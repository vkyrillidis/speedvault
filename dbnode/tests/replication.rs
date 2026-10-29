//! A primary and its replica over real TCP (phase 8 milestone 3): a sync
//! from scratch while writes go on, big collections in parts, reads after
//! `READONLY` (`-LOADING` during the sync), a short drop carried on from
//! the backlog, a long one synced again. Every time, every key the same at
//! both: type, value, absolute expiry. Between the two a proxy, so the
//! test can hold the stream back and cut it.

mod common;

use common::start_cluster;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Counts memory, so that `maxmemory` applies in these tests (for the whole
/// process: every node here).
#[global_allocator]
static ALLOCATOR: dbnode::memory::Counting = dbnode::memory::Counting;

/// A reply, as far as these tests read them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

struct Conn(BufReader<TcpStream>);

impl Conn {
    fn new(addr: SocketAddr) -> Conn {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        Conn(BufReader::new(s))
    }

    fn send(&mut self, args: &[&[u8]]) {
        let mut req = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            req.extend(format!("${}\r\n", a.len()).bytes());
            req.extend(*a);
            req.extend(b"\r\n");
        }
        self.0.get_mut().write_all(&req).unwrap();
    }

    fn read(&mut self) -> Reply {
        let mut line = String::new();
        self.0.read_line(&mut line).unwrap();
        let (kind, rest) = line.trim_end().split_at(1);
        match kind {
            "+" => Reply::Simple(rest.into()),
            "-" => Reply::Error(rest.into()),
            ":" => Reply::Int(rest.parse().unwrap()),
            "$" | "=" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return Reply::Bulk(None);
                }
                let mut body = vec![0; n as usize + 2];
                self.0.read_exact(&mut body).unwrap();
                body.truncate(n as usize);
                Reply::Bulk(Some(body))
            }
            "*" | "~" => {
                let n: i64 = rest.parse().unwrap();
                Reply::Array((0..n.max(0)).map(|_| self.read()).collect())
            }
            "_" => Reply::Bulk(None),
            other => panic!("unexpected reply type {other:?}: {line:?}"),
        }
    }

    fn call(&mut self, args: &[&[u8]]) -> Reply {
        self.send(args);
        self.read()
    }

    fn call_str(&mut self, args: &[&str]) -> Reply {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        self.call(&args)
    }

    fn text(&mut self, args: &[&str]) -> String {
        match self.call_str(args) {
            Reply::Bulk(Some(b)) => String::from_utf8(b).unwrap(),
            Reply::Simple(s) => s,
            other => panic!("{args:?}: {other:?}"),
        }
    }

    fn info(&mut self, field: &str) -> String {
        let text = self.text(&["INFO", "everything"]);
        let prefix = format!("{field}:");
        text.lines().find_map(|l| l.strip_prefix(&prefix)).unwrap_or_else(|| panic!("no {field} in {text}")).to_string()
    }
}

/// Between the replica and its primary: holds back what the primary
/// sends while paused (after a first piece, if asked), and what the
/// replica sends (its acknowledgements) while `paused_back`; cuts every
/// connection on demand.
struct Proxy {
    addr: SocketAddr,
    paused: Arc<AtomicBool>,
    pause_after_first: Arc<AtomicBool>,
    paused_back: Arc<AtomicBool>,
    /// The replica's acknowledgements passed on (`REPLACK`s).
    acks: Arc<AtomicUsize>,
    /// What the primary sends goes slowly: 16 KiB every 20 ms.
    slow: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn new(to: SocketAddr) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (paused, pause_after_first, conns) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)), Arc::default());
        let paused_back = Arc::new(AtomicBool::new(false));
        let acks = Arc::new(AtomicUsize::new(0));
        let slow = Arc::new(AtomicBool::new(false));
        let p = Proxy {
            addr,
            paused: paused.clone(),
            pause_after_first: pause_after_first.clone(),
            paused_back: paused_back.clone(),
            acks: acks.clone(),
            slow: slow.clone(),
            conns: Arc::clone(&conns),
        };
        thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                let Ok(server) = TcpStream::connect(to) else { continue };
                // As the nodes' own connections: nothing small held back.
                let _ = (client.set_nodelay(true), server.set_nodelay(true));
                conns.lock().unwrap().extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);
                let (c2, s2) = (client.try_clone().unwrap(), server.try_clone().unwrap());
                let back = (paused_back.clone(), Arc::new(AtomicBool::new(false)));
                let counted = acks.clone();
                thread::spawn(move || copy(c2, s2, Some(back), Some(counted), None));
                let (paused, after, slow) = (paused.clone(), pause_after_first.clone(), slow.clone());
                thread::spawn(move || copy(server, client, Some((paused, after)), None, Some(slow)));
            }
        });
        p
    }

    fn cut(&self) {
        for c in self.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(Shutdown::Both);
        }
    }

    fn pause(&self, on: bool) {
        self.paused.store(on, Ordering::SeqCst);
    }
}

/// Copies `from` to `to` until either ends; counts the `REPLACK`s it
/// copies into `acks` (each ack fits a read: they're small, and the
/// replica writes each at once).
fn copy(
    mut from: TcpStream,
    mut to: TcpStream,
    pause: Option<(Arc<AtomicBool>, Arc<AtomicBool>)>,
    acks: Option<Arc<AtomicUsize>>,
    slow: Option<Arc<AtomicBool>>,
) {
    let mut buf = vec![0; 64 * 1024];
    loop {
        if let Some((paused, _)) = &pause {
            while paused.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
        }
        let slowly = slow.as_ref().is_some_and(|s| s.load(Ordering::SeqCst));
        let n = match from.read(&mut buf[..if slowly { 16 * 1024 } else { 64 * 1024 }]) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        // Paused while the read waited: held back too.
        if let Some((paused, _)) = &pause {
            while paused.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
        }
        if let Some(acks) = &acks {
            acks.fetch_add(buf[..n].windows(7).filter(|w| w == b"REPLACK").count(), Ordering::SeqCst);
        }
        if to.write_all(&buf[..n]).is_err() {
            break;
        }
        if slowly {
            thread::sleep(Duration::from_millis(20));
        }
        if let Some((paused, after)) = &pause {
            if after.swap(false, Ordering::SeqCst) {
                paused.store(true, Ordering::SeqCst);
            }
        }
    }
    let _ = to.shutdown(Shutdown::Both);
    let _ = from.shutdown(Shutdown::Both);
}

/// Every bulk string of a reply, in order, nested ones too.
fn bulks(reply: &Reply) -> Vec<Vec<u8>> {
    match reply {
        Reply::Bulk(Some(b)) => vec![b.clone()],
        Reply::Array(items) => items.iter().flat_map(bulks).collect(),
        _ => Vec::new(),
    }
}

fn id_of(c: &mut Conn) -> String {
    c.text(&["CLUSTER", "MYID"])
}

/// The table: `p` owns every slot, `r` is its replica; `p_at` is the port
/// the table gives `p` (the proxy's, for the replica).
fn settable(epoch: u64, p: &str, p_at: u16, r: &str, r_port: u16) -> Vec<String> {
    let mut a: Vec<String> = ["CLUSTER", "SETTABLE", &epoch.to_string(), "NODES", "2"].iter().map(|s| s.to_string()).collect();
    a.extend([p.into(), "127.0.0.1".into(), p_at.to_string(), r.into(), "127.0.0.1".into(), r_port.to_string()]);
    a.extend(["RANGES", "1", "0", "16383", p, "REPLICAS", "1", r, p].iter().map(|s| s.to_string()));
    a
}

fn call_vec(c: &mut Conn, args: &[String]) -> Reply {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    c.call_str(&args)
}

/// Every key of a node as a client sees it: its type, value (a hash's and
/// a set's sorted) and absolute expiry.
fn contents(c: &mut Conn) -> Vec<(Vec<u8>, String, Reply, i64)> {
    let Reply::Array(keys) = c.call_str(&["KEYS", "*"]) else { panic!() };
    let mut keys: Vec<Vec<u8>> = keys.into_iter().map(|k| if let Reply::Bulk(Some(k)) = k { k } else { panic!() }).collect();
    keys.sort();
    keys.into_iter()
        .map(|k| {
            let Reply::Simple(kind) = c.call(&[b"TYPE", &k]) else { panic!() };
            let mut value = match kind.as_str() {
                "string" => c.call(&[b"GET", &k]),
                "hash" => c.call(&[b"HGETALL", &k]),
                "set" => c.call(&[b"SMEMBERS", &k]),
                "list" => c.call(&[b"LRANGE", &k, b"0", b"-1"]),
                other => panic!("type {other}"),
            };
            if let Reply::Array(items) = &mut value {
                if kind == "hash" {
                    let mut pairs: Vec<Reply> = items.chunks(2).map(|p| Reply::Array(p.to_vec())).collect();
                    pairs.sort();
                    *items = pairs;
                } else if kind == "set" {
                    items.sort();
                }
            }
            let Reply::Int(at) = c.call(&[b"PEXPIRETIME", &k]) else { panic!() };
            (k, kind, value, at)
        })
        .collect()
}

/// The keys `INFO keyspace` counts: every key, those of slots copied and
/// not served yet too.
fn keyspace(c: &mut Conn) -> i64 {
    let text = c.text(&["INFO", "keyspace"]);
    let Some(line) = text.lines().find_map(|l| l.strip_prefix("db0:keys=")) else { return 0 };
    line.split(',').next().unwrap().parse().unwrap()
}

fn wait_until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Until the replica has applied the primary's whole stream.
fn caught_up(p: &mut Conn, r: &mut Conn) {
    wait_until("the replica catches up", 30, || {
        r.info("master_link_status") == "up" && r.info("slave_repl_offset") == p.info("master_repl_offset")
    });
}

/// xorshift64*.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }
}

/// Random writes over the keys `load` made, until `stop`.
fn writer(addr: SocketAddr, seed: u64, stop: Arc<AtomicBool>) -> thread::JoinHandle<usize> {
    thread::spawn(move || {
        let mut c = Conn::new(addr);
        let mut rng = Rng(seed);
        let mut n = 0;
        while !stop.load(Ordering::SeqCst) {
            let i = rng.below(3000);
            let (s, h, z, l) = (format!("s{i}"), format!("h{}", i % 300), format!("z{}", i % 300), format!("l{}", i % 300));
            let v = rng.below(1_000_000).to_string();
            let args: Vec<String> = match rng.below(16) {
                0 => vec!["SET".into(), s, v],
                1 => vec!["SET".into(), s, v, "PX".into(), (100_000 + rng.below(1000)).to_string()],
                2 => vec!["DEL".into(), s],
                3 => vec!["INCR".into(), format!("n{}", i % 100)],
                4 => vec!["EXPIRE".into(), s, if rng.below(4) == 0 { "-1".into() } else { "200".into() }],
                5 => vec!["HSET".into(), h, format!("f{}", rng.below(50)), v],
                6 => vec!["HDEL".into(), h, format!("f{}", rng.below(50))],
                7 => vec!["SADD".into(), z, v],
                8 => vec!["SPOP".into(), z],
                9 => vec!["RPUSH".into(), l, v],
                10 => vec!["LPOP".into(), l],
                11 => vec!["HSET".into(), "bighash".into(), format!("f{}", rng.below(20_000)), v],
                12 => vec!["HDEL".into(), "bighash".into(), format!("f{}", rng.below(20_000))],
                13 => vec!["RPUSH".into(), "biglist".into(), v],
                14 => vec!["INCRBYFLOAT".into(), format!("f{}", i % 50), "0.5".into()],
                _ => vec!["SINTERSTORE".into(), format!("{{t}}d{}", i % 10), format!("{{t}}a{}", i % 10), format!("{{t}}b{}", i % 10)],
            };
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            c.call_str(&args);
            n += 1;
        }
        n
    })
}

/// Keys of every type, and two collections bigger than a part.
fn load(c: &mut Conn) {
    for i in 0..3000 {
        c.call_str(&["SET", &format!("s{i}"), &format!("v{i}")]);
        if i % 3 == 0 {
            c.call_str(&["PEXPIRE", &format!("s{i}"), "500000"]);
        }
    }
    for i in 0..300 {
        c.call_str(&["HSET", &format!("h{i}"), "a", "1", "b", "2"]);
        c.call_str(&["SADD", &format!("z{i}"), "1", "2", "x"]);
        c.call_str(&["RPUSH", &format!("l{i}"), "1", "2", "3"]);
    }
    for i in 0..10 {
        c.call_str(&["SADD", &format!("{{t}}a{i}"), "1", "2", "3"]);
        c.call_str(&["SADD", &format!("{{t}}b{i}"), "2", "3", "4"]);
    }
    for chunk in 0..20 {
        let mut args = vec!["HSET".to_string(), "bighash".into()];
        let mut list = vec!["RPUSH".to_string(), "biglist".into()];
        for f in 0..1000 {
            args.extend([format!("f{}", chunk * 1000 + f), "v".into()]);
            list.push(format!("e{}", chunk * 1000 + f));
        }
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        c.call_str(&a);
        let a: Vec<&str> = list.iter().map(String::as_str).collect();
        c.call_str(&a);
    }
}

#[test]
fn a_replica_syncs_follows_and_carries_on() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    // The primary alone first, with its keys.
    let mine = ["CLUSTER", "SETTABLE", "1", "NODES", "1", &pid, "127.0.0.1", &p_addr.port().to_string(), "RANGES", "1", "0", "16383", &pid];
    assert_eq!(p.call_str(&mine), Reply::Simple("OK".into()));
    load(&mut p);
    assert_eq!(p.info("repl_backlog_active"), "0", "no stream before a replica");

    // The replica attaches; the proxy holds the stream back after its
    // first piece: the replica is syncing, and says so.
    proxy.pause_after_first.store(true, Ordering::SeqCst);
    assert_eq!(call_vec(&mut p, &settable(2, &pid, p_addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    assert_eq!(call_vec(&mut r, &settable(2, &pid, proxy.addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    wait_until("the replica syncs", 10, || r.info("master_sync_in_progress") == "1");
    assert_eq!(r.call_str(&["READONLY"]), Reply::Simple("OK".into()));
    assert_eq!(r.call_str(&["GET", "s1"]), Reply::Error("LOADING Redis is loading the dataset in memory".into()));
    let Reply::Array(role) = r.call_str(&["ROLE"]) else { panic!() };
    assert_eq!(role[3], Reply::Bulk(Some(b"sync".to_vec())));
    assert_eq!(r.info("master_link_status"), "down");
    // (The primary may have queued the whole copy already: its state is
    // send_bulk only while it hasn't.)
    assert_eq!(p.info("connected_slaves"), "1");

    // Writes all along; the stream goes on.
    let stop = Arc::new(AtomicBool::new(false));
    let w = writer(p_addr, 7, stop.clone());
    thread::sleep(Duration::from_millis(100));
    proxy.pause(false);
    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::SeqCst);
    let writes = w.join().unwrap();
    assert!(writes > 100, "{writes} writes");
    caught_up(&mut p, &mut r);
    assert_eq!(contents(&mut p), contents(&mut r), "after the sync");
    assert_eq!((p.info("sync_full"), p.info("sync_partial_ok")), ("1".into(), "0".into()));
    assert!(p.info("slave0").contains(",state=online,"));
    let Reply::Array(role) = r.call_str(&["ROLE"]) else { panic!() };
    assert_eq!(role[3], Reply::Bulk(Some(b"connected".to_vec())));
    assert_eq!(role[4], Reply::Int(r.info("slave_repl_offset").parse().unwrap()));
    // Its stream is the primary's.
    assert_eq!(r.info("master_replid"), p.info("master_replid"));
    // Reads served, writes and other slots redirected.
    assert_eq!(r.call_str(&["GET", "s2"]), p.call_str(&["GET", "s2"]));
    assert!(matches!(r.call_str(&["SET", "s2", "x"]), Reply::Error(e) if e.starts_with("MOVED ")));
    assert_eq!(r.call_str(&["READWRITE"]), Reply::Simple("OK".into()));
    assert!(matches!(r.call_str(&["GET", "s2"]), Reply::Error(e) if e.starts_with("MOVED ")));
    r.call_str(&["READONLY"]);

    // A short drop: it carries on from the backlog.
    proxy.cut();
    let stop = Arc::new(AtomicBool::new(false));
    let w = writer(p_addr, 8, stop.clone());
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::SeqCst);
    w.join().unwrap();
    caught_up(&mut p, &mut r);
    assert_eq!((p.info("sync_full"), p.info("sync_partial_ok")), ("1".into(), "1".into()), "carried on");
    assert_eq!(contents(&mut p), contents(&mut r), "after carrying on");

    // An expired key stays at the replica, unseen, until the primary's DEL.
    p.call_str(&["SET", "brief", "v", "PX", "300"]);
    caught_up(&mut p, &mut r);
    let size = |c: &mut Conn| c.call_str(&["DBSIZE"]);
    let before = size(&mut r);
    proxy.pause(true);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(r.call_str(&["GET", "brief"]), Reply::Bulk(None), "hidden");
    assert_eq!(size(&mut r), before, "not reclaimed by the replica");
    proxy.pause(false);
    wait_until("the primary's DEL arrives", 10, || size(&mut r) != before);
    caught_up(&mut p, &mut r);

    // A long one: more than the backlog meanwhile, a sync from scratch;
    // keys deleted meanwhile are gone at the replica too.
    proxy.pause(true);
    proxy.cut();
    let big = "x".repeat(100_000);
    for i in 0..20 {
        p.call_str(&["SET", &format!("s{i}"), &big]);
    }
    for i in 100..200 {
        p.call_str(&["DEL", &format!("s{i}")]);
    }
    proxy.pause(false);
    caught_up(&mut p, &mut r);
    assert_eq!((p.info("sync_full"), p.info("sync_partial_err")), ("2".into(), "1".into()), "synced again");
    assert_eq!(contents(&mut p), contents(&mut r), "after syncing again");
    assert_eq!(p.info("db0"), r.info("db0"));
}

#[test]
fn only_its_replicas_may_sync() {
    let (p_addr, x_addr) = (start_cluster(), start_cluster());
    let (mut p, mut x) = (Conn::new(p_addr), Conn::new(x_addr));
    let (pid, xid) = (id_of(&mut p), id_of(&mut x));
    let mine = ["CLUSTER", "SETTABLE", "1", "NODES", "1", &pid, "127.0.0.1", &p_addr.port().to_string(), "RANGES", "1", "0", "16383", &pid];
    assert_eq!(p.call_str(&mine), Reply::Simple("OK".into()));
    let refused = format!("ERR The table doesn't make {xid} a replica of this node");
    assert_eq!(p.call_str(&["REPLSYNC", &xid, "?", "-1"]), Reply::Error(refused));
    assert_eq!(p.call_str(&["REPLSYNC", "nope", "?", "-1"]), Reply::Error("ERR Invalid node id 'nope'".into()));
    assert_eq!(p.call_str(&["REPLSYNC", &xid, "?", "x"]), Reply::Error("ERR Invalid offset 'x'".into()));
    // The connection serves on.
    assert_eq!(p.call_str(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(p.info("repl_backlog_active"), "0");

    // Made its replica (by hand: nothing at x follows, x is no node's
    // replica in its own table), x may sync; it carries on only with this
    // stream's id and an offset the backlog has.
    let table = settable(2, &pid, p_addr.port(), &xid, x_addr.port());
    assert_eq!(call_vec(&mut p, &table), Reply::Simple("OK".into()));
    let replid = p.info("master_replid");
    let line = |args: &[&str]| {
        let mut c = Conn::new(p_addr);
        c.send(&args.iter().map(|a| a.as_bytes()).collect::<Vec<_>>());
        let mut line = String::new();
        c.0.read_line(&mut line).unwrap();
        line.trim_end().to_string()
    };
    assert!(line(&["REPLSYNC", &xid, "?", "-1"]).starts_with(&format!("+FULLRESYNC {replid} ")));
    assert_eq!(p.info("repl_backlog_active"), "1");
    let other = "0".repeat(40);
    assert!(line(&["REPLSYNC", &xid, &other, "0"]).starts_with("+FULLRESYNC "), "another stream");
    assert!(line(&["REPLSYNC", &xid, &replid, "99999999"]).starts_with("+FULLRESYNC "), "an offset beyond it");
    assert_eq!(line(&["REPLSYNC", &xid, &replid, "0"]), format!("+CONTINUE {replid} 0"));
    assert_eq!((p.info("sync_full"), p.info("sync_partial_ok"), p.info("sync_partial_err")), ("3".into(), "1".into(), "2".into()));
    let mut s = Conn::new(common::start());
    assert_eq!(s.call_str(&["REPLSYNC", &xid, "?", "-1"]), Reply::Error("ERR This instance has cluster support disabled".into()));
}

/// The replica acknowledges what it has applied (`REPLACK offset`): after
/// each read that brought something, and every second regardless, during
/// its copy too (offset 0). The primary shows it (`INFO`'s `slave0`,
/// `ROLE`), and notices at once when the replica leaves.
#[test]
fn a_replica_acknowledges() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let mine = ["CLUSTER", "SETTABLE", "1", "NODES", "1", &pid, "127.0.0.1", &p_addr.port().to_string(), "RANGES", "1", "0", "16383", &pid];
    assert_eq!(p.call_str(&mine), Reply::Simple("OK".into()));
    load(&mut p);

    // The copy held back after its first piece: the replica acknowledges
    // every second all the same.
    proxy.pause_after_first.store(true, Ordering::SeqCst);
    assert_eq!(call_vec(&mut p, &settable(2, &pid, p_addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    assert_eq!(call_vec(&mut r, &settable(2, &pid, proxy.addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    wait_until("the replica syncs", 10, || r.info("master_sync_in_progress") == "1");
    let replica = format!("ip=127.0.0.1,port={},state=", r_addr.port());
    for _ in 0..3 {
        thread::sleep(Duration::from_millis(1000));
        let slave0 = p.info("slave0");
        assert!(slave0.starts_with(&replica), "{slave0}");
        assert!(slave0.ends_with(",offset=0,lag=0") || slave0.ends_with(",offset=0,lag=1"), "{slave0}");
    }
    assert_eq!(r.info("master_sync_in_progress"), "1", "still copying");

    // Synced: every write acknowledged.
    proxy.pause(false);
    caught_up(&mut p, &mut r);
    for i in 0..20 {
        assert_eq!(p.call_str(&["SET", "acked", &i.to_string()]), Reply::Simple("OK".into()));
        let offset = p.info("master_repl_offset");
        wait_until("the write acknowledged", 5, || p.info("slave0").contains(&format!(",offset={offset},lag=")));
    }
    let offset = p.info("master_repl_offset");
    let port = r_addr.port().to_string();
    let role = Reply::Array(vec![Reply::Bulk(Some(b"127.0.0.1".to_vec())), Reply::Bulk(Some(port.into_bytes())), Reply::Bulk(Some(offset.clone().into_bytes()))]);
    assert_eq!(p.call_str(&["ROLE"]), Reply::Array(vec![Reply::Bulk(Some(b"master".to_vec())), Reply::Int(offset.parse().unwrap()), Reply::Array(vec![role])]));

    // The replica leaves (no longer one in its table): the primary drops
    // it at once, with nothing to send it.
    let alone = ["CLUSTER", "SETTABLE", "3", "NODES", "1", &rid, "127.0.0.1", &r_addr.port().to_string(), "RANGES", "1", "0", "16383", &rid];
    assert_eq!(r.call_str(&alone), Reply::Simple("OK".into()));
    wait_until("the primary drops the replica", 3, || p.info("connected_slaves") == "0");

    // A replica's connection that sends anything but REPLACK is hung up.
    let mut c = Conn::new(p_addr);
    c.send(&[b"REPLSYNC", rid.as_bytes(), b"?", b"-1"]);
    let mut line = String::new();
    c.0.read_line(&mut line).unwrap();
    assert!(line.starts_with("+FULLRESYNC "), "{line}");
    c.send(&[b"PING"]);
    let mut rest = Vec::new();
    c.0.read_to_end(&mut rest).unwrap();
    wait_until("the primary drops it", 3, || p.info("connected_slaves") == "0");
    // One attaching again: the earlier connection is closed, not just
    // left (its acknowledgements' reader has a copy of the socket).
    let mut first = Conn::new(p_addr);
    first.send(&[b"REPLSYNC", rid.as_bytes(), b"?", b"-1"]);
    let mut line = String::new();
    first.0.read_line(&mut line).unwrap();
    let mut again = Conn::new(p_addr);
    again.send(&[b"REPLSYNC", rid.as_bytes(), b"?", b"-1"]);
    let mut rest = Vec::new();
    first.0.read_to_end(&mut rest).expect("the first connection closed");
}

/// `WAIT`: the replicas that acknowledged the client's last write, once
/// enough have or the timeout passed. A replica acknowledges each read it
/// applied, so a synced one answers within a round trip; a replica still
/// copying doesn't count, as in Redis.
#[test]
fn wait_for_the_replica() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let mine = ["CLUSTER", "SETTABLE", "1", "NODES", "1", &pid, "127.0.0.1", &p_addr.port().to_string(), "RANGES", "1", "0", "16383", &pid];
    assert_eq!(p.call_str(&mine), Reply::Simple("OK".into()));
    load(&mut p);
    assert_eq!(p.call_str(&["WAIT", "1", "50"]), Reply::Int(0), "no replica");

    // Copying: not counted, even for a client that wrote nothing.
    proxy.pause_after_first.store(true, Ordering::SeqCst);
    assert_eq!(call_vec(&mut p, &settable(2, &pid, p_addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    assert_eq!(call_vec(&mut r, &settable(2, &pid, proxy.addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    wait_until("the replica syncs", 10, || r.info("master_sync_in_progress") == "1");
    let mut q = Conn::new(p_addr);
    assert_eq!(q.call_str(&["WAIT", "1", "100"]), Reply::Int(0));
    // A WAIT under way when the copy ends returns then.
    let waiting = thread::spawn(move || {
        let t = Instant::now();
        (q.call_str(&["WAIT", "1", "0"]), t.elapsed())
    });
    thread::sleep(Duration::from_millis(300));
    proxy.pause(false);
    let (got, took) = waiting.join().unwrap();
    assert_eq!(got, Reply::Int(1));
    assert!(took >= Duration::from_millis(300), "{took:?}");
    caught_up(&mut p, &mut r);

    // Synced: each write acknowledged within a round trip, not a second.
    let mut slowest = Duration::ZERO;
    for i in 0..50 {
        p.call_str(&["SET", "w", &i.to_string()]);
        let t = Instant::now();
        assert_eq!(p.call_str(&["WAIT", "1", "0"]), Reply::Int(1));
        slowest = slowest.max(t.elapsed());
    }
    assert!(slowest < Duration::from_millis(250), "WAIT took {slowest:?}");
    // More than there are: the timeout, then those there are.
    let t = Instant::now();
    assert_eq!(p.call_str(&["WAIT", "2", "200"]), Reply::Int(1));
    assert!(t.elapsed() >= Duration::from_millis(200));

    // The stream held back: the write isn't acknowledged in time; a
    // client that wrote nothing since is still answered at once.
    proxy.pause(true);
    p.call_str(&["SET", "held", "1"]);
    let t = Instant::now();
    assert_eq!(p.call_str(&["WAIT", "1", "300"]), Reply::Int(0));
    assert!(t.elapsed() >= Duration::from_millis(300));
    let mut other = Conn::new(p_addr);
    assert_eq!(other.call_str(&["WAIT", "1", "0"]), Reply::Int(1));
    proxy.pause(false);
    assert_eq!(p.call_str(&["WAIT", "1", "0"]), Reply::Int(1));

    let refused = "ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.";
    assert_eq!(r.call_str(&["WAIT", "0", "0"]), Reply::Error(refused.into()));
}

/// Under a stream of writes, the replica's acknowledgements are bundled:
/// one a millisecond at most (and the 1 s heartbeat), not one a read; the
/// last write is still acknowledged within about that.
#[test]
fn acknowledgements_are_bundled() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    assert_eq!(call_vec(&mut p, &settable(1, &pid, p_addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    assert_eq!(call_vec(&mut r, &settable(1, &pid, proxy.addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    caught_up(&mut p, &mut r);
    // Writers unpipelined: each write a read at the replica.
    let before = proxy.acks.load(Ordering::SeqCst);
    let t = Instant::now();
    let writers: Vec<_> = (0..4)
        .map(|w| {
            thread::spawn(move || {
                let mut c = Conn::new(p_addr);
                for i in 0..5000 {
                    assert_eq!(c.call_str(&["SET", &format!("k{w}:{i}"), "v"]), Reply::Simple("OK".into()));
                }
            })
        })
        .collect();
    writers.into_iter().for_each(|w| w.join().unwrap());
    let took = t.elapsed();
    let mut slowest = Duration::ZERO;
    for _ in 0..20 {
        p.call_str(&["SET", "last", "v"]);
        let t = Instant::now();
        assert_eq!(p.call_str(&["WAIT", "1", "0"]), Reply::Int(1));
        slowest = slowest.max(t.elapsed());
    }
    let acks = proxy.acks.load(Ordering::SeqCst) - before;
    let ms = took.as_millis() as usize;
    assert!(acks <= ms + 2 * 20 + 10, "{acks} acknowledgements for 20000 writes in {took:?}");
    assert!(acks >= 20, "{acks}");
    assert!(slowest < Duration::from_millis(50), "WAIT took {slowest:?}");
    caught_up(&mut p, &mut r);
    // Idle, all acknowledged: the 1 s heartbeat alone.
    thread::sleep(Duration::from_millis(100));
    let before = proxy.acks.load(Ordering::SeqCst);
    thread::sleep(Duration::from_millis(1500));
    let idle = proxy.acks.load(Ordering::SeqCst) - before;
    assert!((1..=3).contains(&idle), "{idle} acknowledgements in 1.5 s idle");
}

/// A copy of an empty primary ends at offset 0, where the replica already
/// was: it acknowledges going online all the same, so a `WAIT` returns
/// within a round trip, not at the next second's acknowledgement.
#[test]
fn wait_after_an_empty_copy() {
    // The primary's first PING into the stream (its timer's next tick
    // after the replica attached) is acknowledged too: an attempt in which
    // it came before the WAIT returned proves nothing, so another pair
    // tries again.
    for _ in 0..5 {
        let (p_addr, r_addr) = (start_cluster(), start_cluster());
        let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
        let (pid, rid) = (id_of(&mut p), id_of(&mut r));
        let table = settable(1, &pid, p_addr.port(), &rid, r_addr.port());
        assert_eq!(call_vec(&mut p, &table), Reply::Simple("OK".into()));
        assert_eq!(call_vec(&mut r, &table), Reply::Simple("OK".into()));
        let t = Instant::now();
        assert_eq!(p.call_str(&["WAIT", "1", "3000"]), Reply::Int(1));
        let took = t.elapsed();
        if p.info("master_repl_offset") == "0" {
            assert!(took < Duration::from_millis(600), "WAIT took {took:?}");
            return;
        }
    }
    panic!("every WAIT returned only after the first PING: no acknowledgement on going online");
}

/// The lease: a primary takes writes while its replica has acknowledged
/// within 3 s (Redis's `min-replicas-to-write 1`, `-max-lag 3`), reads
/// all along; a replica gone, its last acknowledgement still counts. A
/// table without the replica lets the primary write alone, and one
/// pairing them again lets it write until the replica first acknowledges.
#[test]
fn the_lease() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let alone = |id: &str, port: u16, epoch: &str| {
        ["CLUSTER", "SETTABLE", epoch, "NODES", "1", id, "127.0.0.1", &port.to_string(), "RANGES", "1", "0", "16383", id]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    };
    let ok = Reply::Simple("OK".into());
    let refused = Reply::Error("NOREPLICAS Not enough good replicas to write.".into());
    assert_eq!(call_vec(&mut p, &alone(&pid, p_addr.port(), "1")), ok);
    assert_eq!(call_vec(&mut p, &settable(2, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    assert_eq!(call_vec(&mut r, &settable(2, &pid, proxy.addr.port(), &rid, r_addr.port())), ok);
    caught_up(&mut p, &mut r);
    assert_eq!(p.call_str(&["SET", "k", "1"]), ok);
    assert_eq!(p.call_str(&["WAIT", "1", "1000"]), Reply::Int(1));
    // Just after an acknowledgement: the ping it answered was sent at most
    // a second ago (one a second), and the lease runs 3 s from then.
    let seen = proxy.acks.load(Ordering::SeqCst);
    wait_until("an acknowledgement", 3, || proxy.acks.load(Ordering::SeqCst) > seen);

    // Its acknowledgements held back: writes for 2-3 s, then refused.
    proxy.paused_back.store(true, Ordering::SeqCst);
    let held = Instant::now();
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(p.call_str(&["SET", "k", "2"]), ok, "within the lease");
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "3"]) == refused);
    let lapsed = held.elapsed();
    assert!(lapsed >= Duration::from_millis(1900) && lapsed < Duration::from_millis(3400), "lapsed after {lapsed:?}");
    for write in [&["DEL", "k"][..], &["INCR", "n"], &["HSET", "h", "f", "v"], &["EXPIRE", "k", "100"], &["FLUSHALL"]] {
        assert_eq!(p.call_str(write), refused, "{write:?}");
    }
    // (The SETs before the refusal went through.)
    assert_eq!(p.call_str(&["GET", "k"]), Reply::Bulk(Some(b"3".to_vec())), "reads go on");
    assert_eq!(p.call_str(&["PING"]), Reply::Simple("PONG".into()));
    let lag: u64 = p.info("slave0").rsplit("lag=").next().unwrap().parse().unwrap();
    assert!(lag >= 1, "lag {lag}");
    // Acknowledging again: writes again.
    proxy.paused_back.store(false, Ordering::SeqCst);
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "4"]) == ok);

    // Synced once, then from scratch again (more than the backlog written
    // while it was cut off), the copy slow, longer than the lease: the
    // pings among its batches, acknowledged, keep the writes going.
    caught_up(&mut p, &mut r);
    proxy.pause(true);
    proxy.cut();
    let big = "x".repeat(100_000);
    for i in 0..40 {
        assert_eq!(p.call_str(&["SET", &format!("big{i}"), &big]), ok);
    }
    proxy.slow.store(true, Ordering::SeqCst);
    proxy.pause(false);
    wait_until("the replica syncs again", 10, || r.info("master_sync_in_progress") == "1");
    let copying = Instant::now();
    while copying.elapsed() < Duration::from_millis(4500) {
        assert_eq!(p.call_str(&["SET", "k", "copying"]), ok, "after {:?} of the copy", copying.elapsed());
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(r.info("master_sync_in_progress"), "1", "still copying");
    assert_eq!(p.info("sync_full"), "2");
    proxy.slow.store(false, Ordering::SeqCst);
    caught_up(&mut p, &mut r);
    // A copy that doesn't move (nothing reaches the replica, so no ping
    // either): the lease runs out. Nothing tells the primary it's there.
    proxy.pause(true);
    proxy.cut();
    for i in 0..20 {
        assert_eq!(p.call_str(&["SET", &format!("big{i}"), &big]), ok);
    }
    proxy.pause_after_first.store(true, Ordering::SeqCst);
    proxy.pause(false);
    wait_until("the replica syncs again", 10, || r.info("master_sync_in_progress") == "1");
    wait_until("the lease runs out", 6, || p.call_str(&["SET", "k", "held"]) == refused);
    proxy.pause(false);
    caught_up(&mut p, &mut r);
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "copied"]) == ok);

    // The replica gone (no longer one in its own table): its last
    // acknowledgement runs out all the same.
    assert_eq!(call_vec(&mut r, &alone(&rid, r_addr.port(), "3")), ok);
    wait_until("the primary drops the replica", 3, || p.info("connected_slaves") == "0");
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "5"]) == refused);

    // stuart takes the replica out: the primary writes alone.
    assert_eq!(call_vec(&mut p, &alone(&pid, p_addr.port(), "4")), ok);
    assert_eq!(p.call_str(&["SET", "k", "6"]), ok);
    // Paired again with a replica that never comes: writes, as before
    // its first acknowledgement, past the lease's 3 s too.
    let mut table = settable(5, &pid, p_addr.port(), &rid, r_addr.port());
    table[2] = "5".into();
    assert_eq!(call_vec(&mut p, &table), ok);
    thread::sleep(Duration::from_millis(3500));
    assert_eq!(p.call_str(&["SET", "k", "7"]), ok);
}

/// `CLUSTER FENCE seconds` (stuart's failover): the replica acknowledges
/// nothing for that long, so its primary's lease runs out, while it goes on
/// applying the stream; it says its offset and whether its copy is
/// complete. Lifted with 0, or ending by itself.
#[test]
fn a_fenced_replica() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let ok = Reply::Simple("OK".into());
    let refused = Reply::Error("NOREPLICAS Not enough good replicas to write.".into());
    assert_eq!(p.call_str(&["CLUSTER", "FENCE", "5"]), Reply::Error("ERR This node is not a replica".into()), "no table");
    assert_eq!(call_vec(&mut p, &settable(1, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    // During a copy: not complete.
    proxy.pause_after_first.store(true, Ordering::SeqCst);
    load(&mut p);
    assert_eq!(call_vec(&mut r, &settable(1, &pid, proxy.addr.port(), &rid, r_addr.port())), ok);
    wait_until("the replica copies", 10, || r.info("master_sync_in_progress") == "1");
    assert_eq!(r.call_str(&["CLUSTER", "FENCE", "0"]), Reply::Array(vec![Reply::Int(0), Reply::Int(0)]));
    proxy.pause(false);
    caught_up(&mut p, &mut r);
    assert_eq!(p.call_str(&["SET", "k", "1"]), ok);
    assert_eq!(p.call_str(&["WAIT", "1", "1000"]), Reply::Int(1));

    let offset = p.info("master_repl_offset").parse::<i64>().unwrap();
    assert_eq!(r.call_str(&["CLUSTER", "FENCE", "30"]), Reply::Array(vec![Reply::Int(offset), Reply::Int(1)]));
    let fenced = Instant::now();
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "2"]) == refused);
    // Within the lease of the last ping it answered, sent before the fence.
    assert!(fenced.elapsed() >= Duration::from_millis(1900) && fenced.elapsed() < Duration::from_millis(3400), "{:?}", fenced.elapsed());
    assert_eq!(p.call_str(&["WAIT", "1", "200"]), Reply::Int(0), "nothing acknowledged");
    // The writes before the lease ran out reached it all the same.
    caught_up(&mut p, &mut r);
    assert_eq!(r.call_str(&["READONLY"]), ok);
    assert_eq!(r.call_str(&["GET", "k"]), Reply::Bulk(Some(b"2".to_vec())));
    // Lifted: acknowledging again.
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "0"]), Reply::Array(_)));
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "3"]) == ok);
    // A fence ends by itself.
    assert_eq!(p.call_str(&["WAIT", "1", "1000"]), Reply::Int(1));
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "4"]), Reply::Array(_)));
    let fenced = Instant::now();
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "4"]) == refused);
    wait_until("the fence ends", 5, || p.call_str(&["SET", "k", "5"]) == ok);
    assert!(fenced.elapsed() >= Duration::from_secs(4), "{:?}", fenced.elapsed());

    for bad in ["-1", "3601", "x"] {
        assert_eq!(r.call_str(&["CLUSTER", "FENCE", bad]), Reply::Error(format!("ERR Invalid seconds '{bad}'")));
    }
    assert_eq!(r.call_str(&["CLUSTER", "FENCE"]), Reply::Error("ERR wrong number of arguments for 'cluster|fence' command".into()));
}

/// A primary stalled past its lease (SIGSTOP: swapped out, a VM paused)
/// takes no write on waking, even one that waited for it in its socket:
/// the write sees the time after the stall, and the acknowledgements its
/// replica sent meanwhile, waiting in its socket too, renew nothing (each
/// answers a ping from before the stall). The replica is fenced a second
/// into the stall, as stuart's failover does.
#[test]
fn a_primary_stalled_past_its_lease_takes_no_write() {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let p_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = unsafe { libc::kill(self.0.id() as libc::pid_t, libc::SIGCONT) };
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let primary = Kill(
        std::process::Command::new(env!("CARGO_BIN_EXE_dbnode"))
            .args([&p_addr.to_string(), "--cluster", "--loglevel", "warning"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let signal = |s: libc::c_int| assert_eq!(unsafe { libc::kill(primary.0.id() as libc::pid_t, s) }, 0);
    wait_until("the primary listens", 10, || TcpStream::connect(p_addr).is_ok());
    let r_addr = start_cluster();
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let ok = Reply::Simple("OK".into());
    assert_eq!(call_vec(&mut p, &settable(1, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    assert_eq!(call_vec(&mut r, &settable(1, &pid, proxy.addr.port(), &rid, r_addr.port())), ok);
    caught_up(&mut p, &mut r);
    assert_eq!(p.call_str(&["SET", "k", "1"]), ok);
    assert_eq!(p.call_str(&["WAIT", "1", "1000"]), Reply::Int(1));

    signal(libc::SIGSTOP);
    let acks = proxy.acks.load(Ordering::SeqCst);
    thread::sleep(Duration::from_millis(1200));
    assert!(proxy.acks.load(Ordering::SeqCst) > acks, "the replica acknowledged into the stopped primary");
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "30"]), Reply::Array(_)));
    thread::sleep(Duration::from_millis(2500));
    // Sent while it's stopped: read only once it runs again.
    p.send(&[b"SET", b"k", b"stalled"]);
    signal(libc::SIGCONT);
    assert_eq!(p.read(), Reply::Error("NOREPLICAS Not enough good replicas to write.".into()));
    assert_eq!(p.call_str(&["GET", "k"]), Reply::Bulk(Some(b"1".to_vec())));
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "0"]), Reply::Array(_)));
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "2"]) == ok);
}

/// The same with first answers waiting: the replica's acknowledgements of
/// pings held back 1.5 s, the primary stopped, then they reach its socket
/// (and the replica is fenced). Read on waking, each renews the lease from
/// its ping's sending, before the stop, not from now: the write that
/// waited is refused.
#[test]
fn a_stalled_primary_answered_late_takes_no_write() {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let p_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = unsafe { libc::kill(self.0.id() as libc::pid_t, libc::SIGCONT) };
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let primary = Kill(
        std::process::Command::new(env!("CARGO_BIN_EXE_dbnode"))
            .args([&p_addr.to_string(), "--cluster", "--loglevel", "warning"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let signal = |s: libc::c_int| assert_eq!(unsafe { libc::kill(primary.0.id() as libc::pid_t, s) }, 0);
    wait_until("the primary listens", 10, || TcpStream::connect(p_addr).is_ok());
    let r_addr = start_cluster();
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let ok = Reply::Simple("OK".into());
    assert_eq!(call_vec(&mut p, &settable(1, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    assert_eq!(call_vec(&mut r, &settable(1, &pid, proxy.addr.port(), &rid, r_addr.port())), ok);
    caught_up(&mut p, &mut r);
    assert_eq!(p.call_str(&["WAIT", "1", "1000"]), Reply::Int(1));

    proxy.paused_back.store(true, Ordering::SeqCst);
    let held = Instant::now();
    thread::sleep(Duration::from_millis(1500));
    signal(libc::SIGSTOP);
    // The answers to the pings of the last 1.5 s reach the stopped primary.
    proxy.paused_back.store(false, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(100));
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "30"]), Reply::Array(_)));
    // Past the lease of the newest ping answered (sent before the stop).
    thread::sleep(Duration::from_millis(5000).saturating_sub(held.elapsed()));
    p.send(&[b"SET", b"k", b"stalled"]);
    signal(libc::SIGCONT);
    assert_eq!(p.read(), Reply::Error("NOREPLICAS Not enough good replicas to write.".into()));
    assert!(matches!(r.call_str(&["CLUSTER", "FENCE", "0"]), Reply::Array(_)));
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "2"]) == ok);
}

/// Two replicas: one acknowledging holds the lease while the other is
/// silent; with both silent it runs out.
#[test]
fn the_lease_with_two_replicas() {
    let (p_addr, a_addr, b_addr) = (start_cluster(), start_cluster(), start_cluster());
    let (mut p, mut a, mut b) = (Conn::new(p_addr), Conn::new(a_addr), Conn::new(b_addr));
    let (pid, aid, bid) = (id_of(&mut p), id_of(&mut a), id_of(&mut b));
    let (to_a, to_b) = (Proxy::new(p_addr), Proxy::new(p_addr));
    let ok = Reply::Simple("OK".into());
    let refused = Reply::Error("NOREPLICAS Not enough good replicas to write.".into());
    // The table, with the primary at `p_at` (a replica's proxy).
    let table = |p_at: u16| -> Vec<String> {
        let (p_at, a_port, b_port) = (p_at.to_string(), a_addr.port().to_string(), b_addr.port().to_string());
        ["CLUSTER", "SETTABLE", "1", "NODES", "3", &pid, "127.0.0.1", &p_at, &aid, "127.0.0.1", &a_port, &bid, "127.0.0.1", &b_port]
            .into_iter()
            .chain(["RANGES", "1", "0", "16383", &pid, "REPLICAS", "2", &aid, &pid, &bid, &pid])
            .map(String::from)
            .collect()
    };
    assert_eq!(call_vec(&mut p, &table(p_addr.port())), ok);
    assert_eq!(call_vec(&mut a, &table(to_a.addr.port())), ok);
    assert_eq!(call_vec(&mut b, &table(to_b.addr.port())), ok);
    caught_up(&mut p, &mut a);
    caught_up(&mut p, &mut b);
    assert_eq!(p.call_str(&["SET", "k", "1"]), ok);
    assert_eq!(p.call_str(&["WAIT", "2", "1000"]), Reply::Int(2));

    // One silent, from the start or after it: the other holds the lease.
    to_b.paused_back.store(true, Ordering::SeqCst);
    let held = Instant::now();
    while held.elapsed() < Duration::from_millis(4500) {
        assert_eq!(p.call_str(&["SET", "k", "2"]), ok, "after {:?}", held.elapsed());
        thread::sleep(Duration::from_millis(50));
    }
    // Both silent: it runs out.
    to_a.paused_back.store(true, Ordering::SeqCst);
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "3"]) == refused);
    to_a.paused_back.store(false, Ordering::SeqCst);
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "4"]) == ok);
}

/// A replica the table marks failed (`FAILED`, stuart found it down): its
/// primary writes without it, at once and for as long as the mark stays;
/// cleared, it holds the lease again. The layout shows it as Redis shows
/// a replica in `FAIL`.
#[test]
fn a_failed_replica() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let ok = Reply::Simple("OK".into());
    let refused = Reply::Error("NOREPLICAS Not enough good replicas to write.".into());
    let failed = |epoch: u64| {
        let mut t = settable(epoch, &pid, p_addr.port(), &rid, r_addr.port());
        t.extend(["FAILED".into(), "1".into(), rid.clone()]);
        t
    };
    assert_eq!(call_vec(&mut p, &settable(1, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    assert_eq!(call_vec(&mut r, &settable(1, &pid, proxy.addr.port(), &rid, r_addr.port())), ok);
    caught_up(&mut p, &mut r);

    proxy.paused_back.store(true, Ordering::SeqCst);
    wait_until("the lease runs out", 5, || p.call_str(&["SET", "k", "1"]) == refused);
    // Marked: writes at once, and past the lease.
    assert_eq!(call_vec(&mut p, &failed(2)), ok);
    assert_eq!(p.call_str(&["SET", "k", "2"]), ok);
    let marked = Instant::now();
    while marked.elapsed() < Duration::from_millis(3500) {
        assert_eq!(p.call_str(&["SET", "k", "3"]), ok, "after {:?}", marked.elapsed());
        thread::sleep(Duration::from_millis(100));
    }
    // What clients see.
    let nodes = p.text(&["CLUSTER", "NODES"]);
    let line = nodes.lines().find(|l| l.starts_with(&rid)).unwrap();
    assert!(line.ends_with(&format!(" slave,fail {pid} 0 0 2 disconnected")), "{line}");
    let line = nodes.lines().find(|l| l.starts_with(&pid)).unwrap();
    assert!(line.contains(" myself,master - 0 0 2 connected 0-16383"), "{line}");
    let slots = p.call_str(&["CLUSTER", "SLOTS"]);
    assert!(!format!("{slots:?}").contains(&r_addr.port().to_string()), "{slots:?}");
    let health: Vec<Vec<u8>> = bulks(&p.call_str(&["CLUSTER", "SHARDS"])).into_iter().filter(|b| [&b"online"[..], b"loading", b"fail"].contains(&b.as_slice())).collect();
    assert_eq!(health, [b"online".to_vec(), b"fail".to_vec()]);
    // Cleared while it still says nothing: as a replica newly paired, its
    // last acknowledgement (from before the mark) forgotten, so writes go
    // on until it acknowledges again.
    assert_eq!(call_vec(&mut p, &settable(3, &pid, p_addr.port(), &rid, r_addr.port())), ok);
    assert_eq!(p.call_str(&["SET", "k", "4"]), ok);
    assert!(!p.text(&["CLUSTER", "NODES"]).contains("fail"));
    // The pairing stayed: it takes the writes once its link is back.
    proxy.paused_back.store(false, Ordering::SeqCst);
    assert_eq!(p.call_str(&["WAIT", "1", "3000"]), Reply::Int(1), "its acknowledgements reach the primary");
    caught_up(&mut p, &mut r);
    assert_eq!(r.call_str(&["READONLY"]), ok);
    assert_eq!(r.call_str(&["GET", "k"]), Reply::Bulk(Some(b"4".to_vec())));

    // Its acknowledgements hold the lease again.
    proxy.paused_back.store(true, Ordering::SeqCst);
    wait_until("the lease runs out again", 5, || p.call_str(&["SET", "k", "5"]) == refused);
    proxy.paused_back.store(false, Ordering::SeqCst);
    wait_until("the lease is back", 3, || p.call_str(&["SET", "k", "6"]) == ok);

    // FAILED's checks.
    let mut t = failed(4);
    *t.last_mut().unwrap() = "0".repeat(40);
    assert_eq!(call_vec(&mut p, &t), Reply::Error(format!("ERR Invalid table: unknown node {}", "0".repeat(40))));
    let mut t = failed(4);
    let n = t.len();
    t[n - 2] = "2".into();
    t.push(rid.clone());
    assert_eq!(call_vec(&mut p, &t), Reply::Error(format!("ERR Invalid table: node {rid} is marked failed twice")));
    t.pop();
    assert_eq!(call_vec(&mut p, &t), Reply::Error("ERR syntax error".into()), "a count past the end");
    assert_eq!(p.text(&["CLUSTER", "INFO"]).lines().find(|l| l.starts_with("cluster_current_epoch")), Some("cluster_current_epoch:3"));
}

/// `CLUSTER SETTABLE` from parts: `nodes` (id, port), `ranges` (start,
/// end, owner), slots moving by snapshot (slot, from, to), replicas
/// (replica, primary).
fn table(epoch: u64, nodes: &[(&str, u16)], ranges: &[(u16, u16, &str)], syncing: &[(u16, &str, &str)], replicas: &[(&str, &str)]) -> Vec<String> {
    let mut a: Vec<String> = vec!["CLUSTER".into(), "SETTABLE".into(), epoch.to_string(), "NODES".into(), nodes.len().to_string()];
    for (id, port) in nodes {
        a.extend([id.to_string(), "127.0.0.1".into(), port.to_string()]);
    }
    a.extend(["RANGES".into(), ranges.len().to_string()]);
    for (s, e, id) in ranges {
        a.extend([s.to_string(), e.to_string(), id.to_string()]);
    }
    if !syncing.is_empty() {
        a.extend(["SYNCING".into(), syncing.len().to_string()]);
        for (slot, from, to) in syncing {
            a.extend([slot.to_string(), from.to_string(), to.to_string()]);
        }
    }
    if !replicas.is_empty() {
        a.extend(["REPLICAS".into(), replicas.len().to_string()]);
        for (r, p) in replicas {
            a.extend([r.to_string(), p.to_string()]);
        }
    }
    a
}

/// Slots moving by snapshot, over TCP: the target pulls them from the
/// source by itself (the table says so), copying what was there and then
/// the writes, while it serves none of them (MOVED to the source, which
/// serves all of it); its replica follows. The table giving it the slots,
/// it serves what it copied; a move called off, it drops them, its replica
/// too.
#[test]
fn slots_move_by_snapshot() {
    let (a_addr, b_addr, r_addr) = (start_cluster(), start_cluster(), start_cluster());
    let (mut a, mut b, mut r) = (Conn::new(a_addr), Conn::new(b_addr), Conn::new(r_addr));
    let (aid, bid, rid) = (id_of(&mut a), id_of(&mut b), id_of(&mut r));
    let nodes = [(&aid[..], a_addr.port()), (&bid[..], b_addr.port()), (&rid[..], r_addr.port())];
    let ok = Reply::Simple("OK".into());
    let all = |t: &[String], conns: &mut [&mut Conn]| {
        for c in conns.iter_mut() {
            assert_eq!(call_vec(c, t), Reply::Simple("OK".into()));
        }
    };
    let pairs = [(&rid[..], &bid[..])];
    all(&table(1, &nodes, &[(0, 16383, &aid)], &[], &pairs), &mut [&mut a, &mut b, &mut r]);
    let slot = |key: &str| -> u16 {
        match Conn::new(a_addr).call_str(&["CLUSTER", "KEYSLOT", key]) {
            Reply::Int(s) => s as u16,
            other => panic!("{other:?}"),
        }
    };
    let (s1, s2, s3) = (slot("{one}"), slot("{two}"), slot("{three}"));
    // Keys before: strings, a hash in parts (more than the chunk), expiries.
    for tag in ["one", "two", "three", "other"] {
        for i in 0..50 {
            assert_eq!(a.call_str(&["SET", &format!("{{{tag}}}k{i}"), &format!("v{i}")]), ok);
        }
        assert_eq!(a.call_str(&["SET", &format!("{{{tag}}}t"), "v", "EX", "1000"]), ok);
    }
    let fields: Vec<String> = (0..10_000).flat_map(|i| [format!("f{i}"), i.to_string()]).collect();
    let mut hset = vec!["HSET", "{one}h"];
    hset.extend(fields.iter().map(String::as_str));
    assert_eq!(a.call_str(&hset), Reply::Int(10_000));

    // s1 and s2 move to b by snapshot.
    all(&table(2, &nodes, &[(0, 16383, &aid)], &[(s1, &aid, &bid), (s2, &aid, &bid)], &pairs), &mut [&mut a, &mut b, &mut r]);
    wait_until("the target streams", 10, || b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" streaming "));
    let syncs = b.text(&["CLUSTER", "SLOTSYNCS"]);
    let (lo, hi) = (s1.min(s2), s1.max(s2));
    assert!(syncs.starts_with(&format!("{aid} streaming {lo},{hi} ")), "{syncs}");
    // Writes meanwhile, at the source, which serves all of it.
    for i in 0..50 {
        assert_eq!(a.call_str(&["INCR", "{one}n"]), Reply::Int(i + 1));
        assert_eq!(a.call_str(&["SET", &format!("{{two}}k{i}"), "w"]), ok);
    }
    assert_eq!(a.call_str(&["DEL", "{one}k0"]), Reply::Int(1));
    assert_eq!(a.call_str(&["HSET", "{one}h", "f0", "changed"]), Reply::Int(0));
    // The target serves none of it; the source isn't listed as having a replica.
    let moved = b.call_str(&["GET", "{one}k1"]);
    assert_eq!(moved, Reply::Error(format!("MOVED {s1} 127.0.0.1:{}", a_addr.port())));
    assert_eq!(a.info("connected_slaves"), "0");
    let count = |c: &mut Conn, s: u16| c.call_str(&["CLUSTER", "COUNTKEYSINSLOT", &s.to_string()]);
    let int = |r: Reply| match r {
        Reply::Int(n) => n,
        other => panic!("{other:?}"),
    };
    let (n1, n2) = (int(count(&mut a, s1)), int(count(&mut a, s2)));
    // INFO keyspace counts every key, those copied too; nothing of another
    // slot (s3) comes.
    wait_until("the target has the writes", 5, || keyspace(&mut b) == n1 + n2);
    wait_until("its replica too", 5, || keyspace(&mut r) == n1 + n2);
    // But as in Redis 8, slots copied and not served yet are hidden: at the
    // target and at its replica.
    for c in [&mut b, &mut r] {
        assert_eq!(c.call_str(&["DBSIZE"]), Reply::Int(0));
        assert_eq!(c.call_str(&["KEYS", "*"]), Reply::Array(vec![]));
        assert_eq!(c.call_str(&["SCAN", "0", "COUNT", "1000"]), Reply::Array(vec![Reply::Bulk(Some(b"0".to_vec())), Reply::Array(vec![])]));
        assert_eq!(count(c, s1), Reply::Int(0));
        assert_eq!(c.call_str(&["CLUSTER", "GETKEYSINSLOT", &s1.to_string(), "10"]), Reply::Array(vec![]));
    }

    // The cutover: the table gives b the slots.
    let mut owned: Vec<(u16, u16, &str)> = Vec::new();
    let mut start = 0u16;
    for s in [lo, hi] {
        if s > start {
            owned.push((start, s - 1, &aid));
        }
        owned.push((s, s, &bid));
        start = s + 1;
    }
    if start <= 16383 {
        owned.push((start, 16383, &aid));
    }
    all(&table(3, &nodes, &owned, &[], &pairs), &mut [&mut a, &mut b, &mut r]);
    wait_until("the link ends", 5, || b.text(&["CLUSTER", "SLOTSYNCS"]).is_empty());
    wait_until("the source deletes them", 5, || count(&mut a, s1) == Reply::Int(0) && count(&mut a, s2) == Reply::Int(0));
    assert_eq!(b.call_str(&["GET", "{one}k1"]), Reply::Bulk(Some(b"v1".to_vec())));
    assert_eq!(b.call_str(&["GET", "{one}k0"]), Reply::Bulk(None));
    assert_eq!(b.call_str(&["GET", "{one}n"]), Reply::Bulk(Some(b"50".to_vec())));
    assert_eq!(b.call_str(&["GET", "{two}k7"]), Reply::Bulk(Some(b"w".to_vec())));
    assert_eq!(b.call_str(&["HLEN", "{one}h"]), Reply::Int(10_000));
    assert_eq!(b.call_str(&["HGET", "{one}h", "f0"]), Reply::Bulk(Some(b"changed".to_vec())));
    assert_eq!(count(&mut b, s1), Reply::Int(n1), "served, so counted");
    assert_eq!(b.call_str(&["DBSIZE"]), Reply::Int(n1 + n2));
    let ttl = b.call_str(&["TTL", "{one}t"]);
    assert!(matches!(ttl, Reply::Int(t) if t > 990), "{ttl:?}");
    // Its replica has them too.
    assert_eq!(r.call_str(&["READONLY"]), ok);
    wait_until("the replica has them", 5, || r.call_str(&["GET", "{one}n"]) == Reply::Bulk(Some(b"50".to_vec())));
    assert_eq!(r.call_str(&["HLEN", "{one}h"]), Reply::Int(10_000));

    // s3 starts moving, then the move is called off: dropped, at the
    // replica too.
    all(&table(4, &nodes, &owned, &[(s3, &aid, &bid)], &pairs), &mut [&mut a, &mut b, &mut r]);
    wait_until("the target streams s3", 10, || b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" streaming "));
    let n3 = int(count(&mut a, s3));
    assert_eq!(keyspace(&mut b), n1 + n2 + n3);
    assert_eq!(b.call_str(&["DBSIZE"]), Reply::Int(n1 + n2), "s3 hidden");
    wait_until("the replica has s3", 5, || keyspace(&mut r) == n1 + n2 + n3);
    all(&table(5, &nodes, &owned, &[], &pairs), &mut [&mut a, &mut b, &mut r]);
    wait_until("dropped", 5, || keyspace(&mut b) == n1 + n2 && b.text(&["CLUSTER", "SLOTSYNCS"]).is_empty());
    wait_until("dropped at the replica", 5, || keyspace(&mut r) == n1 + n2);
    assert_eq!(count(&mut b, s1), Reply::Int(n1), "the slots it took stay");
}

/// The ranges of a table where `moved` are `to`'s and the rest `from`'s.
fn owned_but<'a>(moved: &[u16], from: &'a str, to: &'a str) -> Vec<(u16, u16, &'a str)> {
    let mut moved = moved.to_vec();
    moved.sort_unstable();
    let mut owned = Vec::new();
    let mut start = 0u16;
    for s in moved {
        if s > start {
            owned.push((start, s - 1, from));
        }
        owned.push((s, s, to));
        start = s + 1;
    }
    if start <= 16383 {
        owned.push((start, 16383, from));
    }
    owned
}

/// The offset `CLUSTER SLOTSYNCS` gives the link from `source` (its last
/// field), if it streams or lands.
fn sync_offset(c: &mut Conn, source: &str) -> Option<u64> {
    let text = c.text(&["CLUSTER", "SLOTSYNCS"]);
    let line = text.lines().find(|l| l.starts_with(source))?;
    let fields: Vec<&str> = line.split(' ').collect();
    (fields[1] == "streaming" || fields[1] == "landing").then(|| fields[4].parse().unwrap())
}

fn keyslot(addr: SocketAddr, key: &str) -> u16 {
    match Conn::new(addr).call_str(&["CLUSTER", "KEYSLOT", key]) {
        Reply::Int(s) => s as u16,
        other => panic!("{other:?}"),
    }
}

/// The cutover, as stuart drives it: the source pauses the slot's writes
/// (they wait, reads and other slots go on) and answers its offset; the
/// target reaches it; the table gives the target the slot, the source
/// first: the writes waiting are redirected, a writer that follows the
/// redirect loses nothing, and the source deletes the slot's keys, at its
/// replica too. A pause ends by itself, or at once with 0.
#[test]
fn a_cutover_with_writes_paused() {
    let (a_addr, ra_addr, b_addr) = (start_cluster(), start_cluster(), start_cluster());
    let (mut a, mut ra, mut b) = (Conn::new(a_addr), Conn::new(ra_addr), Conn::new(b_addr));
    let (aid, raid, bid) = (id_of(&mut a), id_of(&mut ra), id_of(&mut b));
    let nodes = [(&aid[..], a_addr.port()), (&raid[..], ra_addr.port()), (&bid[..], b_addr.port())];
    let ok = Reply::Simple("OK".into());
    let all = |t: &[String], conns: &mut [&mut Conn]| {
        for c in conns.iter_mut() {
            assert_eq!(call_vec(c, t), Reply::Simple("OK".into()));
        }
    };
    let pairs = [(&raid[..], &aid[..])];
    let everything = [(0u16, 16383u16, &aid[..])];
    all(&table(1, &nodes, &everything, &[], &pairs), &mut [&mut a, &mut ra, &mut b]);
    let (s1, s2) = (keyslot(a_addr, "{one}"), keyslot(a_addr, "{two}"));
    for i in 0..100 {
        assert_eq!(a.call_str(&["SET", &format!("{{one}}k{i}"), "v"]), ok);
        assert_eq!(a.call_str(&["SET", &format!("{{two}}k{i}"), "v"]), ok);
    }
    let count = |c: &mut Conn, s: u16| c.call_str(&["CLUSTER", "COUNTKEYSINSLOT", &s.to_string()]);
    wait_until("the source's replica has them", 5, || count(&mut ra, s1) == Reply::Int(100));

    all(&table(2, &nodes, &everything, &[(s1, &aid, &bid)], &pairs), &mut [&mut a, &mut ra, &mut b]);
    let pause = |c: &mut Conn, ms: &str, slots: &[u16]| {
        let mut args = vec!["CLUSTER".to_string(), "PAUSESLOTS".into(), ms.into(), bid.clone()];
        args.extend(slots.iter().map(u16::to_string));
        call_vec(c, &args)
    };
    let error = |r: Reply| match r {
        Reply::Error(e) => e,
        other => panic!("{other:?}"),
    };
    assert!(error(pause(&mut a, "100", &[s2])).starts_with(&format!("ERR The table doesn't move slot {s2} ")));
    assert!(error(pause(&mut b, "100", &[s1])).starts_with("ERR The table doesn't move slot"), "only at the source");
    assert!(error(pause(&mut a, "10001", &[s1])).starts_with("ERR Invalid milliseconds"));
    wait_until("the target streams", 10, || sync_offset(&mut b, &aid).is_some());

    // A writer at the source, following redirects: every INCR it saw done
    // must be there once the slot moved.
    let stop = Arc::new(AtomicBool::new(false));
    let incrs = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = Conn::new(a_addr);
            let mut done = 0i64;
            while !stop.load(Ordering::SeqCst) {
                match c.call_str(&["INCR", "{one}n"]) {
                    Reply::Int(n) => {
                        done += 1;
                        assert_eq!(n, done, "no INCR lost or done twice");
                    }
                    Reply::Error(e) if e.starts_with("MOVED ") => c = Conn::new(b_addr),
                    other => panic!("{other:?}"),
                }
            }
            done
        })
    };
    thread::sleep(Duration::from_millis(100));

    let Reply::Int(offset) = pause(&mut a, "2000", &[s1]) else { panic!() };
    let paused_at = Instant::now();
    // Reads go on, and writes to other slots.
    assert_eq!(a.call_str(&["GET", "{one}k1"]), Reply::Bulk(Some(b"v".to_vec())));
    assert_eq!(a.call_str(&["SET", "{two}k1", "w"]), ok);
    assert!(paused_at.elapsed() < Duration::from_millis(500));
    // A write to the slot waits.
    // Sent from here, so that it's at the source before the checks below
    // (a thread may start late under load); its reply read on a thread.
    let mut c = Conn::new(a_addr);
    let started = Instant::now();
    c.send(&[b"SET", b"{one}late", b"v"]);
    let late = thread::spawn(move || (c.read(), started.elapsed()));
    wait_until("the target reaches the offset", 5, || sync_offset(&mut b, &aid).is_some_and(|o| o >= offset as u64));
    thread::sleep(Duration::from_millis(150));
    assert!(!late.is_finished(), "the write waits");

    // The cutover: the source first.
    let cut = table(3, &nodes, &owned_but(&[s1], &aid, &bid), &[], &pairs);
    all(&cut, &mut [&mut a]);
    let (reply, waited) = late.join().unwrap();
    assert_eq!(reply, Reply::Error(format!("MOVED {s1} 127.0.0.1:{}", b_addr.port())));
    assert!(waited >= Duration::from_millis(150), "{waited:?}");
    all(&cut, &mut [&mut b, &mut ra]);
    wait_until("the link ends", 5, || b.text(&["CLUSTER", "SLOTSYNCS"]).is_empty());
    thread::sleep(Duration::from_millis(100));
    stop.store(true, Ordering::SeqCst);
    let done = incrs.join().unwrap();
    assert_eq!(b.call_str(&["GET", "{one}n"]), Reply::Bulk(Some(done.to_string().into_bytes())));
    assert_eq!(b.call_str(&["GET", "{one}k99"]), Reply::Bulk(Some(b"v".to_vec())));
    // The source deletes the slot's keys, its replica too.
    wait_until("the source deletes them", 5, || count(&mut a, s1) == Reply::Int(0));
    wait_until("its replica too", 5, || count(&mut ra, s1) == Reply::Int(0));
    assert_eq!(count(&mut a, s2), Reply::Int(100), "the others stay");

    // A pause ends by itself; 0 ends it at once.
    let owned = owned_but(&[s1], &aid, &bid);
    all(&table(4, &nodes, &owned, &[(s2, &aid, &bid)], &pairs), &mut [&mut a, &mut ra, &mut b]);
    wait_until("the target streams s2", 10, || sync_offset(&mut b, &aid).is_some());
    assert!(matches!(pause(&mut a, "300", &[s2]), Reply::Int(_)));
    let started = Instant::now();
    assert_eq!(a.call_str(&["SET", "{two}x", "v"]), ok);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(200) && waited < Duration::from_secs(2), "{waited:?}");
    assert!(matches!(pause(&mut a, "5000", &[s2]), Reply::Int(_)));
    assert_eq!(pause(&mut a, "0", &[s2]), ok);
    let started = Instant::now();
    assert_eq!(a.call_str(&["SET", "{two}y", "v"]), ok);
    assert!(started.elapsed() < Duration::from_millis(100));
    // A table calling the move off ends a pause too.
    assert!(matches!(pause(&mut a, "5000", &[s2]), Reply::Int(_)));
    all(&table(5, &nodes, &owned, &[], &pairs), &mut [&mut a, &mut ra, &mut b]);
    let started = Instant::now();
    assert_eq!(a.call_str(&["SET", "{two}z", "v"]), ok);
    assert!(started.elapsed() < Duration::from_millis(100));
    wait_until("dropped at the target", 5, || keyspace(&mut b) == 101);
}

/// The handover, as stuart drives it when a primary leaves: the primary
/// pauses every write (they wait, reads go on, nothing expires) and answers
/// its offset; the replica reaches it (`CLUSTER FENCE 0`); the table gives
/// the replica the slots, the replica first: the writes waiting at the old
/// primary are redirected, and a writer that follows the redirect loses
/// nothing. The old primary doesn't trim the slots its replica took: its
/// other replica, taking the table last, would apply the `DEL`s. Nor does
/// the new primary apply what the old one sends once it took the table. A
/// pause ends by itself, or at once with 0.
#[test]
fn a_handover_with_writes_paused() {
    let (p_addr, r_addr, r2_addr) = (start_cluster(), start_cluster(), start_cluster());
    let (mut p, mut r, mut r2) = (Conn::new(p_addr), Conn::new(r_addr), Conn::new(r2_addr));
    let (pid, rid, r2id) = (id_of(&mut p), id_of(&mut r), id_of(&mut r2));
    let ok = Reply::Simple("OK".into());
    let nodes = [(&pid[..], p_addr.port()), (&rid[..], r_addr.port()), (&r2id[..], r2_addr.port())];
    let t = table(1, &nodes, &[(0, 16383, &pid)], &[], &[(&rid, &pid), (&r2id, &pid)]);
    for c in [&mut p, &mut r, &mut r2] {
        assert_eq!(call_vec(c, &t), ok);
    }
    caught_up(&mut p, &mut r);
    caught_up(&mut p, &mut r2);
    let error = |r: Reply| match r {
        Reply::Error(e) => e,
        other => panic!("{other:?}"),
    };
    assert_eq!(error(r.call_str(&["CLUSTER", "PAUSEWRITES", "100"])), "ERR This node is not a primary");
    assert!(error(p.call_str(&["CLUSTER", "PAUSEWRITES", "10001"])).starts_with("ERR Invalid milliseconds"));
    assert!(error(p.call_str(&["CLUSTER", "PAUSEWRITES", "-1"])).starts_with("ERR Invalid milliseconds"));

    // A pause ends by itself; 0 ends it at once.
    assert!(matches!(p.call_str(&["CLUSTER", "PAUSEWRITES", "300"]), Reply::Int(_)));
    let started = Instant::now();
    assert_eq!(p.call_str(&["SET", "x", "v"]), ok);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(200) && waited < Duration::from_secs(2), "{waited:?}");
    assert!(matches!(p.call_str(&["CLUSTER", "PAUSEWRITES", "5000"]), Reply::Int(_)));
    assert_eq!(p.call_str(&["CLUSTER", "PAUSEWRITES", "0"]), ok);
    let started = Instant::now();
    assert_eq!(p.call_str(&["SET", "y", "v"]), ok);
    assert!(started.elapsed() < Duration::from_millis(100));
    // A write without keys waits too.
    assert!(matches!(p.call_str(&["CLUSTER", "PAUSEWRITES", "300"]), Reply::Int(_)));
    let started = Instant::now();
    assert_eq!(p.call_str(&["FLUSHALL"]), ok);
    assert!(started.elapsed() >= Duration::from_millis(200), "{:?}", started.elapsed());

    for i in 0..100 {
        assert_eq!(p.call_str(&["SET", &format!("k{i}"), "v"]), ok);
    }
    // Nothing expires while paused: the stream stays at the offset.
    assert_eq!(p.call_str(&["SET", "short", "v", "PX", "50"]), ok);
    let Reply::Int(at) = p.call_str(&["CLUSTER", "PAUSEWRITES", "1000"]) else { panic!() };
    thread::sleep(Duration::from_millis(400));
    assert_eq!(p.info("master_repl_offset"), at.to_string());
    assert_eq!(p.call_str(&["DBSIZE"]), Reply::Int(101));
    wait_until("it expires after the pause", 5, || p.call_str(&["DBSIZE"]) == Reply::Int(100));
    caught_up(&mut p, &mut r);

    // A writer at the primary, following redirects: every INCR it saw done
    // must be at the replica once it took over.
    let stop = Arc::new(AtomicBool::new(false));
    let incrs = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = Conn::new(p_addr);
            let mut done = 0i64;
            while !stop.load(Ordering::SeqCst) {
                match c.call_str(&["INCR", "n"]) {
                    Reply::Int(n) => {
                        done += 1;
                        assert_eq!(n, done, "no INCR lost or done twice");
                    }
                    Reply::Error(e) if e.starts_with("MOVED ") => c = Conn::new(r_addr),
                    other => panic!("{other:?}"),
                }
            }
            done
        })
    };
    thread::sleep(Duration::from_millis(100));

    let Reply::Int(offset) = p.call_str(&["CLUSTER", "PAUSEWRITES", "5000"]) else { panic!() };
    let paused_at = Instant::now();
    // Reads go on.
    assert_eq!(p.call_str(&["GET", "k1"]), Reply::Bulk(Some(b"v".to_vec())));
    assert!(paused_at.elapsed() < Duration::from_millis(500));
    // A write waits. Sent from here, so that it's at the primary before
    // the checks below; its reply read on a thread.
    let mut c = Conn::new(p_addr);
    let started = Instant::now();
    c.send(&[b"SET", b"late", b"v"]);
    let late = thread::spawn(move || (c.read(), started.elapsed()));
    let fence = |r: &mut Conn| match r.call_str(&["CLUSTER", "FENCE", "0"]) {
        Reply::Array(a) => (a[0].clone(), a[1].clone()),
        other => panic!("{other:?}"),
    };
    wait_until("the replica reaches the offset", 5, || matches!(fence(&mut r), (Reply::Int(o), Reply::Int(1)) if o >= offset));
    thread::sleep(Duration::from_millis(150));
    assert!(!late.is_finished(), "the write waits");
    assert_eq!(fence(&mut r).0, Reply::Int(offset), "nothing more came");

    // The handover: the replica first, the other replica last.
    let over = table(2, &nodes, &[(0, 16383, &rid)], &[], &[(&r2id, &rid)]);
    assert_eq!(call_vec(&mut r, &over), ok);
    assert!(!late.is_finished(), "the write waits until the old primary took the table");
    assert_eq!(call_vec(&mut p, &over), ok);
    let (reply, waited) = late.join().unwrap();
    assert_eq!(reply, Reply::Error(format!("MOVED {} 127.0.0.1:{}", keyslot(p_addr, "late"), r_addr.port())));
    assert!(waited >= Duration::from_millis(150), "{waited:?}");
    thread::sleep(Duration::from_millis(100));
    stop.store(true, Ordering::SeqCst);
    let done = incrs.join().unwrap();
    assert!(done > 0);
    // Time for the old primary to trim, were it to.
    thread::sleep(Duration::from_millis(500));
    assert_eq!(call_vec(&mut r2, &over), ok);
    assert_eq!(r.call_str(&["GET", "n"]), Reply::Bulk(Some(done.to_string().into_bytes())));
    assert_eq!(r.call_str(&["GET", "k99"]), Reply::Bulk(Some(b"v".to_vec())));
    assert_eq!(r.info("role"), "master");
    assert_eq!(p.call_str(&["DBSIZE"]), Reply::Int(101), "not trimmed");
    caught_up(&mut r, &mut r2);
    assert_eq!(r2.call_str(&["READONLY"]), ok);
    assert_eq!(contents(&mut r2), contents(&mut r));
    assert_eq!(keyspace(&mut r2), 101);
}

/// A write without keys waiting for a handover: a table that leaves the
/// primary its slots doesn't end the pause; the one that hands them over
/// does, at once (the write isn't redirected: it has no slot). The
/// promoted replica applies nothing its old primary sends from then on:
/// not that write.
#[test]
fn a_keyless_write_waiting_for_a_handover() {
    let (p_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let ok = Reply::Simple("OK".into());
    let nodes = [(&pid[..], p_addr.port()), (&rid[..], r_addr.port())];
    let t = table(1, &nodes, &[(0, 16383, &pid)], &[], &[(&rid, &pid)]);
    for c in [&mut p, &mut r] {
        assert_eq!(call_vec(c, &t), ok);
    }
    for i in 0..10 {
        assert_eq!(p.call_str(&["SET", &format!("k{i}"), "v"]), ok);
    }
    caught_up(&mut p, &mut r);

    let Reply::Int(offset) = p.call_str(&["CLUSTER", "PAUSEWRITES", "5000"]) else { panic!() };
    let mut c = Conn::new(p_addr);
    let started = Instant::now();
    c.send(&[b"FLUSHALL"]);
    let flush = thread::spawn(move || (c.read(), started.elapsed()));
    // A table that changes nothing for it: the pause goes on.
    let same = table(2, &nodes, &[(0, 16383, &pid)], &[], &[(&rid, &pid)]);
    for c in [&mut r, &mut p] {
        assert_eq!(call_vec(c, &same), ok);
    }
    thread::sleep(Duration::from_millis(200));
    assert!(!flush.is_finished(), "a table leaving it its slots ended the pause");
    wait_until("the replica reaches the offset", 5, || {
        matches!(r.call_str(&["CLUSTER", "FENCE", "0"]), Reply::Array(a) if matches!(a[..], [Reply::Int(o), Reply::Int(1)] if o >= offset))
    });

    let over = table(3, &nodes, &[(0, 16383, &rid)], &[], &[]);
    assert_eq!(call_vec(&mut r, &over), ok);
    let handed = Instant::now();
    assert_eq!(call_vec(&mut p, &over), ok);
    let (reply, _) = flush.join().unwrap();
    assert_eq!(reply, ok);
    assert!(handed.elapsed() < Duration::from_secs(2), "the handover table didn't end the pause: {:?}", handed.elapsed());
    // Time for the old primary's stream to reach the new one, were it read.
    thread::sleep(Duration::from_millis(500));
    assert_eq!(r.info("role"), "master");
    assert_eq!(r.call_str(&["DBSIZE"]), Reply::Int(10), "the new primary applied its old primary's FLUSHALL");
}

/// A slot sync through a proxy: a link cut mid-copy copies again, ending
/// with the source's keys; a target with much to acknowledge isn't paused
/// (`-BEHIND`); a `FLUSHALL` at the target copies again; the cutover with
/// the link cut before `SLOTSDONE` came: requests to the slot wait, then
/// the target serves what it copied.
#[test]
fn a_slot_sync_through_failures() {
    let (a_addr, b_addr) = (start_cluster(), start_cluster());
    let proxy = Proxy::new(a_addr);
    let (mut a, mut b) = (Conn::new(a_addr), Conn::new(b_addr));
    let (aid, bid) = (id_of(&mut a), id_of(&mut b));
    // The target reaches the source through the proxy.
    let nodes = [(&aid[..], proxy.addr.port()), (&bid[..], b_addr.port())];
    let ok = Reply::Simple("OK".into());
    let everything = [(0u16, 16383u16, &aid[..])];
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(1, &nodes, &everything, &[], &[])), ok);
    }
    let s1 = keyslot(a_addr, "{one}");
    let value = "x".repeat(100);
    for chunk in 0..20 {
        let mut args = vec!["MSET".to_string()];
        for i in 0..1000 {
            args.extend([format!("{{one}}k{}", chunk * 1000 + i), value.clone()]);
        }
        assert_eq!(call_vec(&mut a, &args), ok);
    }
    let count = |c: &mut Conn| c.call_str(&["CLUSTER", "COUNTKEYSINSLOT", &s1.to_string()]);
    assert_eq!(count(&mut a), Reply::Int(20_000));

    // Cut mid-copy (slowly: 16 KiB every 20 ms, about 3 s for the slot).
    proxy.slow.store(true, Ordering::SeqCst);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(2, &nodes, &everything, &[(s1, &aid, &bid)], &[])), ok);
    }
    wait_until("the copy is under way", 10, || keyspace(&mut b) > 2000);
    proxy.cut();
    proxy.slow.store(false, Ordering::SeqCst);
    wait_until("copied again", 20, || sync_offset(&mut b, &aid).is_some());
    assert_eq!(keyspace(&mut b), 20_000);
    assert_eq!(a.call_str(&["SET", "{one}k0", "new"]), ok);
    wait_until("the write arrives", 5, || sync_offset(&mut b, &aid).is_some_and(|o| o > 0));

    // Its acknowledgements held back: over 1 MiB behind, not paused.
    proxy.paused_back.store(true, Ordering::SeqCst);
    let big = "y".repeat(100_000);
    for i in 0..15 {
        assert_eq!(a.call_str(&["SET", &format!("{{one}}big{i}"), &big]), ok);
    }
    thread::sleep(Duration::from_millis(100));
    let pause = |c: &mut Conn, ms: &str| call_vec(c, &["CLUSTER".into(), "PAUSESLOTS".into(), ms.into(), bid.clone(), s1.to_string()]);
    match pause(&mut a, "1000") {
        Reply::Error(e) => assert!(e.starts_with("BEHIND "), "{e}"),
        other => panic!("{other:?}"),
    }
    let started = Instant::now();
    assert_eq!(a.call_str(&["SET", "{one}k1", "new"]), ok);
    assert!(started.elapsed() < Duration::from_millis(100), "nothing paused");
    proxy.paused_back.store(false, Ordering::SeqCst);
    wait_until("caught up", 5, || matches!(pause(&mut a, "0"), Reply::Simple(_)) && matches!(pause(&mut a, "1"), Reply::Int(_)));

    // FLUSHALL at the target: copied again.
    assert_eq!(b.call_str(&["FLUSHALL"]), ok);
    wait_until("copied again after FLUSHALL", 20, || sync_offset(&mut b, &aid).is_some() && keyspace(&mut b) == 20_015);

    // The cutover with what the source sends held back, then cut: the
    // target's requests to the slot wait, then it serves what it has.
    wait_until("the target reaches the source", 5, || {
        let Reply::Int(at) = pause(&mut a, "2000") else { return false };
        thread::sleep(Duration::from_millis(20));
        sync_offset(&mut b, &aid).is_some_and(|o| o >= at as u64)
    });
    proxy.pause(true);
    let cut = table(3, &nodes, &owned_but(&[s1], &aid, &bid), &[], &[]);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &cut), ok);
    }
    let mut c = Conn::new(b_addr);
    let started = Instant::now();
    c.send(&[b"GET", b"{one}k0"]);
    let get = thread::spawn(move || (c.read(), started.elapsed()));
    thread::sleep(Duration::from_millis(300));
    assert!(!get.is_finished(), "landing: the request waits");
    assert!(b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" landing "));
    proxy.cut();
    proxy.pause(false);
    let (reply, waited) = get.join().unwrap();
    assert_eq!(reply, Reply::Bulk(Some(b"new".to_vec())));
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    assert_eq!(count(&mut b), Reply::Int(20_015));
    wait_until("the source deletes them", 5, || count(&mut a) == Reply::Int(0));
}

/// Through a proxy, what a target must get exactly: its offset counts the
/// writes, not the source's pings, so it equals the paused source's; a
/// `FLUSHALL` at the source waits for a pause, and reaches the target as
/// the slot's keys dropped, not as a `FLUSHALL` (its own slots' keys
/// stay); a link that fails keeps its copy until the source takes the new
/// one (a cutover may come meanwhile), which then drops what the source
/// deleted in between.
#[test]
fn a_slot_sync_gets_exactly_the_source_s_slot() {
    let (a_addr, b_addr) = (start_cluster(), start_cluster());
    let proxy = Proxy::new(a_addr);
    let (mut a, mut b) = (Conn::new(a_addr), Conn::new(b_addr));
    let (aid, bid) = (id_of(&mut a), id_of(&mut b));
    let nodes = [(&aid[..], proxy.addr.port()), (&bid[..], b_addr.port())];
    let ok = Reply::Simple("OK".into());
    let (s1, own) = (keyslot(a_addr, "{one}"), keyslot(a_addr, "{bee}"));
    // b owns the slot of {bee}, with keys of its own.
    let owned = owned_but(&[own], &aid, &bid);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(1, &nodes, &owned, &[], &[])), ok);
    }
    for i in 0..10 {
        assert_eq!(b.call_str(&["SET", &format!("{{bee}}k{i}"), "mine"]), ok);
    }
    let fill = |a: &mut Conn, n: usize| {
        let mut args = vec!["MSET".to_string()];
        for i in 0..n {
            args.extend([format!("{{one}}k{i}"), "v".into()]);
        }
        assert_eq!(call_vec(a, &args), ok);
    };
    fill(&mut a, 2000);
    let moving = table(2, &nodes, &owned, &[(s1, &aid, &bid)], &[]);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &moving), ok);
    }
    wait_until("the target streams", 10, || sync_offset(&mut b, &aid).is_some());
    assert_eq!(keyspace(&mut b), 2010);
    let pause = |c: &mut Conn, ms: &str| call_vec(c, &["CLUSTER".into(), "PAUSESLOTS".into(), ms.into(), bid.clone(), s1.to_string()]);

    // Writes, then pings (one a second): the offsets agree to the byte.
    for i in 0..20 {
        assert_eq!(a.call_str(&["INCR", &format!("{{one}}n{i}")]), Reply::Int(1));
    }
    thread::sleep(Duration::from_millis(2500));
    let Reply::Int(at) = pause(&mut a, "2000") else { panic!() };
    wait_until("the target reaches the offset", 5, || sync_offset(&mut b, &aid).is_some_and(|o| o >= at as u64));
    assert_eq!(sync_offset(&mut b, &aid), Some(at as u64), "pings aren't counted");

    // FLUSHALL waits for the pause, then drops the slot at the target only.
    let mut c = Conn::new(a_addr);
    let started = Instant::now();
    c.send(&[b"FLUSHALL"]);
    let flush = thread::spawn(move || (c.read(), started.elapsed()));
    thread::sleep(Duration::from_millis(300));
    assert!(!flush.is_finished(), "FLUSHALL waits for the pause");
    assert_eq!(pause(&mut a, "0"), ok);
    let (reply, waited) = flush.join().unwrap();
    assert_eq!(reply, ok);
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    wait_until("the slot's keys dropped at the target", 5, || keyspace(&mut b) == 10);
    assert_eq!(b.call_str(&["GET", "{bee}k3"]), Reply::Bulk(Some(b"mine".to_vec())), "its own keys stay");
    fill(&mut a, 1000);
    wait_until("copied again", 5, || keyspace(&mut b) == 1010);

    // The link fails, and the source's answers are held back: the copy
    // stays while the target connects again. The source deletes half the
    // slot before the target asks again (it waits half a second): the new
    // link's copy doesn't have them, nor sends their DELs, so only
    // dropping what the old link copied keeps them from coming back.
    let mut del = vec!["DEL".to_string()];
    del.extend((0..500).map(|i| format!("{{one}}k{i}")));
    proxy.pause(true);
    proxy.cut();
    assert_eq!(call_vec(&mut a, &del), Reply::Int(500));
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(keyspace(&mut b), 1010, "kept while it connects again");
    proxy.pause(false);
    wait_until("copied again", 20, || sync_offset(&mut b, &aid).is_some() && keyspace(&mut b) == 510);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(keyspace(&mut b), 510, "no leftovers");
}

/// A node the table marks failed doesn't trim the slots it no longer owns:
/// a primary failed over gave them to its replica, which may still apply
/// its stream (it would delete them there). Unmarked, it trims them.
#[test]
fn a_failed_node_does_not_trim() {
    let (a_addr, r_addr) = (start_cluster(), start_cluster());
    let (mut a, mut r) = (Conn::new(a_addr), Conn::new(r_addr));
    let (aid, rid) = (id_of(&mut a), id_of(&mut r));
    let nodes = [(&aid[..], a_addr.port()), (&rid[..], r_addr.port())];
    let ok = Reply::Simple("OK".into());
    assert_eq!(call_vec(&mut a, &table(1, &nodes, &[(0, 16383, &aid)], &[], &[(&rid, &aid)])), ok);
    let s = keyslot(a_addr, "{one}");
    for i in 0..100 {
        assert_eq!(a.call_str(&["SET", &format!("{{one}}k{i}"), "v"]), ok);
    }
    // The replica promoted, a marked failed.
    let mut promoted = table(2, &nodes, &[(0, 16383, &rid)], &[], &[]);
    promoted.extend(["FAILED".into(), "1".into(), aid.clone()]);
    assert_eq!(call_vec(&mut a, &promoted), ok);
    let count = |c: &mut Conn| c.call_str(&["CLUSTER", "COUNTKEYSINSLOT", &s.to_string()]);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(count(&mut a), Reply::Int(100), "a failed node keeps them");
    // Back (unmarked), a primary without slots: it trims them.
    assert_eq!(call_vec(&mut a, &table(3, &nodes, &[(0, 16383, &rid)], &[], &[])), ok);
    wait_until("trimmed", 5, || count(&mut a) == Reply::Int(0));
}

/// A link cut in the middle of the copy while a client writes to the slot:
/// the copy starts again, and at the cutover the target has the slot
/// exactly as the source had it.
#[test]
fn a_link_cut_mid_copy_under_writes() {
    let (a_addr, b_addr) = (start_cluster(), start_cluster());
    let proxy = Proxy::new(a_addr);
    let (mut a, mut b) = (Conn::new(a_addr), Conn::new(b_addr));
    let (aid, bid) = (id_of(&mut a), id_of(&mut b));
    let nodes = [(&aid[..], proxy.addr.port()), (&bid[..], b_addr.port())];
    let ok = Reply::Simple("OK".into());
    let everything = [(0u16, 16383u16, &aid[..])];
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(1, &nodes, &everything, &[], &[])), ok);
    }
    let s1 = keyslot(a_addr, "{one}");
    let value = "x".repeat(100);
    for chunk in 0..20 {
        let mut args = vec!["MSET".to_string()];
        for i in 0..1000 {
            args.extend([format!("{{one}}k{}", chunk * 1000 + i), value.clone()]);
        }
        assert_eq!(call_vec(&mut a, &args), ok);
    }
    let fields: Vec<String> = (0..5000).flat_map(|i| [format!("f{i}"), "v".into()]).collect();
    let mut hset = vec!["HSET".to_string(), "{one}h".into()];
    hset.extend(fields);
    assert_eq!(call_vec(&mut a, &hset), Reply::Int(5000));

    // A writer on the slot all along.
    let stop = Arc::new(AtomicBool::new(false));
    let writes = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut c = Conn::new(a_addr);
            let mut rng = Rng(7);
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                let k = format!("{{one}}k{}", rng.below(25_000));
                let v = rng.below(1000).to_string();
                let args: Vec<String> = match rng.below(6) {
                    0 => vec!["SET".into(), k, v],
                    1 => vec!["DEL".into(), k],
                    2 => vec!["INCR".into(), format!("{{one}}n{}", rng.below(50))],
                    3 => vec!["HSET".into(), "{one}h".into(), format!("f{}", rng.below(6000)), v],
                    4 => vec!["HDEL".into(), "{one}h".into(), format!("f{}", rng.below(6000))],
                    _ => vec!["SET".into(), k, v, "PX".into(), "500000".into()],
                };
                let reply = call_vec(&mut c, &args);
                assert!(!matches!(reply, Reply::Error(_)), "{args:?}: {reply:?}");
                n += 1;
            }
            n
        })
    };
    // Slowly (16 KiB every 20 ms), cut mid-copy, then at full speed.
    proxy.slow.store(true, Ordering::SeqCst);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(2, &nodes, &everything, &[(s1, &aid, &bid)], &[])), ok);
    }
    wait_until("the copy is under way", 10, || keyspace(&mut b) > 2000);
    proxy.cut();
    proxy.slow.store(false, Ordering::SeqCst);
    wait_until("copied again", 20, || sync_offset(&mut b, &aid).is_some());
    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::SeqCst);
    assert!(writes.join().unwrap() > 100);

    // The cutover: paused, the target at the offset, then the table.
    let pause = call_vec(&mut a, &["CLUSTER".into(), "PAUSESLOTS".into(), "5000".into(), bid.clone(), s1.to_string()]);
    let Reply::Int(at) = pause else { panic!("{pause:?}") };
    wait_until("the target reaches the offset", 5, || sync_offset(&mut b, &aid).is_some_and(|o| o >= at as u64));
    let expected = contents(&mut a);
    let cut = table(3, &nodes, &owned_but(&[s1], &aid, &bid), &[], &[]);
    for c in [&mut b, &mut a] {
        assert_eq!(call_vec(c, &cut), ok);
    }
    wait_until("the link ends", 5, || b.text(&["CLUSTER", "SLOTSYNCS"]).is_empty());
    let got = contents(&mut b);
    assert_eq!(got.len(), expected.len());
    assert!(got == expected, "the target's slot differs from the source's");
}

/// A target that takes the table moving slots to it before their source
/// does (stuart pushes it a table it is owed first) is refused by the
/// source, and asks again within milliseconds, not after a failed link's
/// half second: it streams soon after the source takes the table.
#[test]
fn a_target_ahead_of_its_source_syncs_soon() {
    let (a_addr, b_addr) = (start_cluster(), start_cluster());
    let (mut a, mut b) = (Conn::new(a_addr), Conn::new(b_addr));
    let (aid, bid) = (id_of(&mut a), id_of(&mut b));
    let nodes = [(&aid[..], a_addr.port()), (&bid[..], b_addr.port())];
    let one = table(1, &nodes, &[(0, 16383, &aid)], &[], &[]);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &one), Reply::Simple("OK".into()));
    }
    let slot = keyslot(a_addr, "{x}");
    assert_eq!(a.call_str(&["SET", "{x}k", "v"]), Reply::Simple("OK".into()));
    let moving = table(2, &nodes, &[(0, 16383, &aid)], &[(slot, &aid, &bid)], &[]);
    assert_eq!(call_vec(&mut b, &moving), Reply::Simple("OK".into()));
    // Refused meanwhile: the source's table doesn't move the slot.
    thread::sleep(Duration::from_millis(100));
    assert!(b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" connecting "), "{}", b.text(&["CLUSTER", "SLOTSYNCS"]));
    assert_eq!(call_vec(&mut a, &moving), Reply::Simple("OK".into()));
    let taken = Instant::now();
    wait_until("the target streams", 5, || b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" streaming "));
    assert!(taken.elapsed() < Duration::from_millis(250), "streaming {} ms after the source took the table", taken.elapsed().as_millis());
}

/// A target full while it copies waits for room, as phase 5's moves to a
/// full target do: its link stops reading (`full` in `CLUSTER SLOTSYNCS`,
/// `migrate_imports_refused` counts it) and takes no key, the source
/// serves the slot meanwhile; once there's room (the limit lifted) the
/// copy goes on, and the cutover gives the target every key. (Memory is
/// counted for this whole process, so `maxmemory 1` stands for full; the
/// peak under a real limit is `migrate_check full-target`'s.)
#[test]
fn a_full_target_waits_for_room() {
    let (a_addr, b_addr) = (start_cluster(), start_cluster());
    let (mut a, mut b) = (Conn::new(a_addr), Conn::new(b_addr));
    let (aid, bid) = (id_of(&mut a), id_of(&mut b));
    let nodes = [(&aid[..], a_addr.port()), (&bid[..], b_addr.port())];
    let ok = Reply::Simple("OK".into());
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &table(1, &nodes, &[(0, 16383, &aid)], &[], &[])), ok);
    }
    let s = keyslot(a_addr, "{full}");
    for i in 0..2000 {
        assert_eq!(a.call_str(&["SET", &format!("{{full}}k{i}"), "v"]), ok);
    }
    assert_eq!(b.call_str(&["CONFIG", "SET", "maxmemory", "1"]), ok);

    let moving = table(2, &nodes, &[(0, 16383, &aid)], &[(s, &aid, &bid)], &[]);
    for c in [&mut a, &mut b] {
        assert_eq!(call_vec(c, &moving), ok);
    }
    wait_until("the target is full", 10, || b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" full "));
    thread::sleep(Duration::from_millis(500));
    let syncs = b.text(&["CLUSTER", "SLOTSYNCS"]);
    assert!(syncs.starts_with(&format!("{aid} full {s} 0 ")), "still waiting, no key taken: {syncs}");
    assert_eq!(keyspace(&mut b), 0);
    assert_eq!(b.info("migrate_imports_refused"), "1");
    // The source serves the slot meanwhile.
    assert_eq!(a.call_str(&["SET", "{full}new", "v"]), ok);

    assert_eq!(b.call_str(&["CONFIG", "SET", "maxmemory", "0"]), ok);
    wait_until("the target streams", 10, || b.text(&["CLUSTER", "SLOTSYNCS"]).contains(" streaming "));
    let given = table(3, &nodes, &owned_but(&[s], &aid, &bid), &[], &[]);
    for c in [&mut b, &mut a] {
        assert_eq!(call_vec(c, &given), ok);
    }
    wait_until("the link ends", 5, || b.text(&["CLUSTER", "SLOTSYNCS"]).is_empty());
    assert_eq!(b.call_str(&["CLUSTER", "COUNTKEYSINSLOT", &s.to_string()]), Reply::Int(2001));
    assert_eq!(b.call_str(&["GET", "{full}new"]), Reply::Bulk(Some(b"v".to_vec())));
    assert_eq!(b.info("migrate_keys_imported"), "2001");
    assert_eq!(a.info("migrate_keys_sent"), "2001");
}

/// `GET /ready` at `http`: the status line and the body.
fn ready(http: SocketAddr) -> (String, String) {
    let mut s = TcpStream::connect(http).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"GET /ready HTTP/1.1\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_string(), body.to_string())
}

/// A replica is ready (Kubernetes' readiness, `/ready`) only while synced
/// with its primary: not during its copy, nor with its link down.
#[test]
fn a_replica_is_ready_once_synced() {
    let p_addr = start_cluster();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics = TcpListener::bind("127.0.0.1:0").unwrap();
    let (r_addr, http) = (listener.local_addr().unwrap(), metrics.local_addr().unwrap());
    let cluster = Some(dbnode::Announce { host: "127.0.0.1".into(), port: r_addr.port() });
    thread::spawn(move || dbnode::serve(listener, Some(metrics), dbnode::Config { cluster, ..Default::default() }).unwrap());
    let (mut p, mut r) = (Conn::new(p_addr), Conn::new(r_addr));
    let (pid, rid) = (id_of(&mut p), id_of(&mut r));
    let proxy = Proxy::new(p_addr);
    let mine = ["CLUSTER", "SETTABLE", "1", "NODES", "1", &pid, "127.0.0.1", &p_addr.port().to_string(), "RANGES", "1", "0", "16383", &pid];
    assert_eq!(p.call_str(&mine), Reply::Simple("OK".into()));
    load(&mut p);

    proxy.pause_after_first.store(true, Ordering::SeqCst);
    assert_eq!(call_vec(&mut p, &settable(2, &pid, p_addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    assert_eq!(call_vec(&mut r, &settable(2, &pid, proxy.addr.port(), &rid, r_addr.port())), Reply::Simple("OK".into()));
    wait_until("the replica syncs", 10, || r.info("master_sync_in_progress") == "1");
    let not_ready = ("HTTP/1.1 503 Service Unavailable".to_string(), "not ready: a replica not synced with its primary\n".to_string());
    assert_eq!(ready(http), not_ready, "during the copy");

    proxy.pause(false);
    wait_until("the replica is ready", 10, || ready(http).0 == "HTTP/1.1 200 OK");
    assert_eq!(r.info("master_link_status"), "up");

    // The link cut and held: not ready until it's back.
    proxy.pause(true);
    proxy.cut();
    wait_until("the link is down", 10, || r.info("master_link_status") == "down");
    assert_eq!(ready(http), not_ready, "with the link down");
    proxy.pause(false);
    wait_until("the replica is ready again", 10, || ready(http).0 == "HTTP/1.1 200 OK");
}
