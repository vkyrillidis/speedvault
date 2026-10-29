//! Metrics (phase 6 milestone 4): `--metrics`' HTTP responder, the node's
//! `/metrics` and its probes, in servers on threads of this process.

mod common;

use common::*;
use dbnode::memory::Counting;
use dbnode::{Announce, Config};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

/// Counts memory, so that the memory metrics say something.
#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// A server and its metrics address.
fn start_metrics(config: Config, cluster: bool) -> (SocketAddr, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics = TcpListener::bind("127.0.0.1:0").unwrap();
    let (addr, http) = (listener.local_addr().unwrap(), metrics.local_addr().unwrap());
    let cluster = cluster.then(|| Announce { host: "127.0.0.1".into(), port: addr.port() });
    thread::spawn(move || dbnode::serve(listener, Some(metrics), Config { cluster, ..config }).unwrap());
    (addr, http)
}

/// Sends `request` as is and returns the whole response, read to the close.
fn raw(http: SocketAddr, request: &[u8]) -> String {
    let mut s = TcpStream::connect(http).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(request).unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}

/// `GET path`: the status line and the body.
fn get(http: SocketAddr, path: &str) -> (String, String) {
    let response = raw(http, format!("GET {path} HTTP/1.1\r\nHost: test\r\nAccept: */*\r\n\r\n").as_bytes());
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.lines().next().unwrap().to_string();
    let length: usize = head.lines().find_map(|l| l.strip_prefix("Content-Length: ")).unwrap().parse().unwrap();
    assert_eq!(body.len(), length, "{head}");
    (status, body.to_string())
}

/// `/metrics` as samples (`name{labels}` to value), checking the format
/// on the way: every family has HELP then TYPE, and its samples follow.
fn scrape(http: SocketAddr) -> BTreeMap<String, f64> {
    let response = raw(http, b"GET /metrics HTTP/1.1\r\n\r\n");
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(head.contains("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n"), "{head}");
    let mut samples = BTreeMap::new();
    let mut family = String::new();
    let mut lines = body.lines().peekable();
    while let Some(line) = lines.next() {
        if let Some(help) = line.strip_prefix("# HELP ") {
            family = help.split(' ').next().unwrap().to_string();
            let kind = lines.next().unwrap();
            assert!(kind == format!("# TYPE {family} counter") || kind == format!("# TYPE {family} gauge"), "{kind}");
            assert_eq!(family.ends_with("_total"), kind.ends_with("counter"), "{family}");
            continue;
        }
        let (name, value) = line.rsplit_once(' ').unwrap();
        assert!(name.split('{').next() == Some(&family), "{line} outside of {family}");
        let value: f64 = value.parse().unwrap_or_else(|_| panic!("{line}"));
        assert!(samples.insert(name.to_string(), value).is_none(), "{name} twice");
    }
    assert!(body.ends_with('\n'));
    samples
}

fn value(samples: &BTreeMap<String, f64>, name: &str) -> f64 {
    *samples.get(name).unwrap_or_else(|| panic!("no {name} in {samples:#?}"))
}

/// A fresh standalone node: every metric, nothing from cluster mode.
#[test]
fn a_standalone_node_s_metrics() {
    let (_, http) = start_metrics(Config::default(), false);
    let m = scrape(http);
    let names: Vec<&str> = m.keys().map(String::as_str).collect();
    let info = format!("speedvault_info{{version=\"{}\",mode=\"standalone\",node_id=\"\"}}", env!("CARGO_PKG_VERSION"));
    let mut expected = vec![
        &info[..],
        "speedvault_auth_failures_total",
        "speedvault_busy_seconds_total",
        "speedvault_clients",
        "speedvault_commands_processed_total",
        "speedvault_commands_refused_oom_total",
        "speedvault_connections_received_total",
        "speedvault_cpu_seconds_total",
        "speedvault_keys",
        "speedvault_keys_expiring",
        "speedvault_memory_map_growth_bytes",
        "speedvault_memory_max_bytes",
        "speedvault_memory_peak_bytes",
        "speedvault_memory_rss_bytes",
        "speedvault_memory_used_bytes",
        "speedvault_memory_used_ratio",
        "speedvault_uptime_seconds",
    ];
    expected.sort();
    assert_eq!(names, expected);
    assert_eq!(value(&m, &info), 1.0);
    for zero in ["speedvault_auth_failures_total", "speedvault_clients", "speedvault_commands_processed_total", "speedvault_keys", "speedvault_memory_max_bytes", "speedvault_memory_used_ratio"] {
        assert_eq!(value(&m, zero), 0.0, "{zero}");
    }
    assert!(value(&m, "speedvault_memory_used_bytes") > 0.0);
    assert!(value(&m, "speedvault_memory_rss_bytes") > 0.0);
    assert_eq!(get(http, "/ready"), ("HTTP/1.1 200 OK".into(), "ready\n".into()));
    assert_eq!(get(http, "/healthz"), ("HTTP/1.1 200 OK".into(), "ok\n".into()));
}

/// The counters follow the clients' commands, `-OOM` refusals included.
#[test]
fn the_counters_follow_the_traffic() {
    let (addr, http) = start_metrics(Config::default(), false);
    let mut c = Client::connect(addr);
    c.call(&[b"SET", b"a", b"1"], OK);
    c.call(&[b"SET", b"b", b"2", b"EX", b"100"], OK);
    c.call(&[b"GET", b"a"], b"$1\r\n1\r\n");
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"1"], OK);
    c.call(&[b"SET", b"c", b"3"], b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    c.call(&[b"INCR", b"a"], b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    let m = scrape(http);
    assert_eq!(value(&m, "speedvault_commands_processed_total"), 4.0, "SET, SET, GET, CONFIG SET");
    assert_eq!(value(&m, "speedvault_commands_refused_oom_total"), 2.0);
    assert_eq!(value(&m, "speedvault_keys"), 2.0);
    assert_eq!(value(&m, "speedvault_keys_expiring"), 1.0);
    assert_eq!(value(&m, "speedvault_clients"), 1.0);
    assert_eq!(value(&m, "speedvault_connections_received_total"), 1.0);
    assert_eq!(value(&m, "speedvault_memory_max_bytes"), 1.0);
    assert!(value(&m, "speedvault_memory_used_ratio") > 1.0);
    assert!(value(&m, "speedvault_memory_peak_bytes") >= value(&m, "speedvault_memory_used_bytes"));
}

/// Busy time grows with the work the node does, not with the time it
/// waits; CPU time includes it.
#[test]
fn busy_time_counts_work_not_waiting() {
    // Busy-polling on: the thread spins between requests, which mustn't
    // count.
    let (addr, http) = start_metrics(Config { busy_poll: Duration::from_millis(1), ..Config::default() }, false);
    let before = value(&scrape(http), "speedvault_busy_seconds_total");
    let started = Instant::now();
    thread::sleep(Duration::from_millis(300));
    let idle = value(&scrape(http), "speedvault_busy_seconds_total") - before;
    let waited = started.elapsed().as_secs_f64();
    assert!(idle < waited / 10.0, "busy {idle} s of {waited} s idle");

    // Work: large values, so that the node spends real time on them.
    let mut c = Client::connect(addr);
    let value_1m = vec![b'x'; 1 << 20];
    let started = Instant::now();
    for i in 0..200 {
        c.call(&[b"SET", format!("k{i}").as_bytes(), &value_1m], OK);
    }
    let elapsed = started.elapsed().as_secs_f64();
    let m = scrape(http);
    let busy = value(&m, "speedvault_busy_seconds_total") - before - idle;
    assert!(busy > 0.0 && busy < elapsed, "busy {busy} s in {elapsed} s");
    assert!(value(&m, "speedvault_cpu_seconds_total") >= value(&m, "speedvault_busy_seconds_total") * 0.9);
}

/// In cluster mode: the slots, the table's epoch, the moves and redirects;
/// ready once it has a table.
#[test]
fn a_cluster_node_s_metrics() {
    let (addr, http) = start_metrics(Config::default(), true);
    let (status, body) = get(http, "/ready");
    assert_eq!((status.as_str(), body.as_str()), ("HTTP/1.1 503 Service Unavailable", "not ready: no slot table yet\n"));
    let mut c = Client::connect(addr);
    c.cmd(&[b"CLUSTER", b"MYID"]);
    let me = c.read_text();
    let port = addr.port().to_string();
    let other = "b".repeat(40);
    let table: Vec<&[u8]> = vec![
        b"CLUSTER", b"SETTABLE", b"3", b"NODES", b"2", me.as_bytes(), b"127.0.0.1", port.as_bytes(), other.as_bytes(), b"10.0.0.2", b"7000",
        b"RANGES", b"2", b"0", b"99", me.as_bytes(), b"100", b"16383", other.as_bytes(),
    ];
    c.call(&table, OK);
    // "foo" is in slot 12182: another node's.
    c.cmd(&[b"GET", b"foo"]);
    assert!(c.read_line().starts_with(b"-MOVED 12182 "));

    let m = scrape(http);
    let info = format!("speedvault_info{{version=\"{}\",mode=\"cluster\",node_id=\"{me}\"}}", env!("CARGO_PKG_VERSION"));
    assert_eq!(value(&m, &info), 1.0);
    assert_eq!(value(&m, "speedvault_cluster_slots"), 100.0);
    assert_eq!(value(&m, "speedvault_cluster_epoch"), 3.0);
    assert_eq!(value(&m, "speedvault_cluster_leaving"), 0.0);
    assert_eq!(value(&m, "speedvault_cluster_slots_migrating{direction=\"out\"}"), 0.0);
    assert_eq!(value(&m, "speedvault_cluster_slots_migrating{direction=\"in\"}"), 0.0);
    assert_eq!(value(&m, "speedvault_redirects_total{kind=\"moved\"}"), 1.0);
    assert_eq!(value(&m, "speedvault_redirects_total{kind=\"ask\"}"), 0.0);
    assert_eq!(value(&m, "speedvault_redirects_total{kind=\"tryagain\"}"), 0.0);
    for counter in ["keys_sent", "keys_imported", "imports_refused"] {
        assert_eq!(value(&m, &format!("speedvault_migrate_{counter}_total")), 0.0, "{counter}");
    }
    assert_eq!(get(http, "/ready").0, "HTTP/1.1 200 OK");

    // Slots migrating: 5 out of it, 200 into it. (Nothing listens at the
    // other node's address: slot 5's keys wait to go.)
    let table: Vec<&[u8]> = vec![
        b"CLUSTER", b"SETTABLE", b"4", b"NODES", b"2", me.as_bytes(), b"127.0.0.1", port.as_bytes(), other.as_bytes(), b"127.0.0.1", b"1",
        b"RANGES", b"2", b"0", b"99", me.as_bytes(), b"100", b"16383", other.as_bytes(),
        b"MIGRATING", b"2", b"5", me.as_bytes(), other.as_bytes(), b"200", other.as_bytes(), me.as_bytes(),
    ];
    c.call(&table, OK);
    let m = scrape(http);
    assert_eq!(value(&m, "speedvault_cluster_slots_migrating{direction=\"out\"}"), 1.0);
    assert_eq!(value(&m, "speedvault_cluster_slots_migrating{direction=\"in\"}"), 1.0);
    assert_eq!(value(&m, "speedvault_cluster_slots"), 100.0, "its own, the one moving out included");
    assert_eq!(value(&m, "speedvault_cluster_epoch"), 4.0);
}

/// A node joining stuart is ready once stuart acknowledged it.
#[test]
fn a_joining_node_is_not_ready_until_it_joined() {
    // Nothing listens there: it never joins.
    let nowhere = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let (_, http) = start_metrics(Config { stuart: Some(nowhere.to_string()), ..Config::default() }, true);
    assert_eq!(get(http, "/ready"), ("HTTP/1.1 503 Service Unavailable".into(), "not ready: not joined to stuart yet\n".into()));
}

/// What isn't a GET of a known path.
#[test]
fn other_requests() {
    let (_, http) = start_metrics(Config::default(), false);
    assert_eq!(get(http, "/").0, "HTTP/1.1 404 Not Found");
    assert_eq!(get(http, "/metrics/x").0, "HTTP/1.1 404 Not Found");
    let status = |request: &[u8]| raw(http, request).lines().next().unwrap_or_default().to_string();
    assert_eq!(status(b"POST /metrics HTTP/1.1\r\nContent-Length: 0\r\n\r\n"), "HTTP/1.1 405 Method Not Allowed");
    assert_eq!(status(b"*1\r\n$4\r\nPING\r\n\r\n"), "HTTP/1.1 400 Bad Request");
    assert_eq!(status(b"GET /metrics\r\n\r\n"), "HTTP/1.1 400 Bad Request");
    let mut huge = b"GET /metrics HTTP/1.1\r\n".to_vec();
    huge.extend(std::iter::repeat_n(b"X-Pad: 0123456789abcdef\r\n", 500).flatten());
    huge.extend_from_slice(b"\r\n");
    assert_eq!(status(&huge), "HTTP/1.1 400 Bad Request");
    // HEAD: the headers only.
    for path in ["/metrics", "/healthz", "/ready", "/nope"] {
        let head = raw(http, format!("HEAD {path} HTTP/1.1\r\n\r\n").as_bytes());
        assert!(head.starts_with("HTTP/1.1 ") && head.ends_with("\r\n\r\n"), "{path}: {head}");
    }
    // A body nobody reads: the reply still arrives (closing with unread
    // bytes would reset the connection, losing it).
    for _ in 0..5 {
        let mut post = b"POST /metrics HTTP/1.1\r\nContent-Length: 32768\r\n\r\n".to_vec();
        post.extend(std::iter::repeat_n(b'x', 32768));
        assert_eq!(status(&post), "HTTP/1.1 405 Method Not Allowed");
    }
    // A request in pieces, the end split over two reads.
    let mut s = TcpStream::connect(http).unwrap();
    for piece in [&b"GET /hea"[..], b"lthz HTTP/1.0\r\n\r", b"\n"] {
        s.write_all(piece).unwrap();
        thread::sleep(Duration::from_millis(20));
    }
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    assert!(out.starts_with("HTTP/1.1 200 OK\r\n") && out.ends_with("\r\n\r\nok\n"), "{out}");
}

/// A client that sends nothing holds only its own connection, which is
/// closed after 5 seconds.
#[test]
fn a_silent_client_is_closed() {
    let (_, http) = start_metrics(Config::default(), false);
    let mut silent = TcpStream::connect(http).unwrap();
    silent.write_all(b"GET /metr").unwrap();
    // Others are served meanwhile.
    assert_eq!(get(http, "/healthz").0, "HTTP/1.1 200 OK");
    let started = Instant::now();
    silent.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut out = Vec::new();
    silent.read_to_end(&mut out).unwrap();
    assert!(out.is_empty(), "{:?}", String::from_utf8_lossy(&out));
    let waited = started.elapsed();
    assert!(waited > Duration::from_secs(4) && waited < Duration::from_secs(7), "{waited:?}");
}
