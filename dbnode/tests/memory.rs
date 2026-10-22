//! Memory (phase 6 milestone 3): `maxmemory` and the `-OOM` refusals,
//! `INFO memory`, `CONFIG GET/SET maxmemory`, in servers on threads of
//! this process; and the count itself, in the dbnode binary as a process
//! (the count is the process's).

mod common;

use common::*;
use dbnode::memory::Counting;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Counts memory, so that `maxmemory` applies to the servers here.
#[global_allocator]
static ALLOCATOR: Counting = Counting;

const OOM: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";

/// Past `maxmemory`, Redis's `denyoom` commands are refused (after the
/// arity check, not counted as processed); the others run, so that keys
/// can still be read and deleted.
#[test]
fn writes_are_refused_past_maxmemory() {
    let mut c = Client::connect(start());
    c.call(&[b"SET", b"k", b"v"], OK);
    c.call(&[b"SET", b"n", b"1"], OK);
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"1"], OK);
    let processed = |c: &mut Client| -> u64 {
        c.cmd(&[b"INFO", b"stats"]);
        let text = c.read_text();
        text.lines().find_map(|l| l.strip_prefix("total_commands_processed:")).unwrap().parse().unwrap()
    };
    let before = processed(&mut c);
    for cmd in [
        &[&b"SET"[..], b"k", b"w"][..],
        &[b"SETNX", b"new", b"v"],
        &[b"MSET", b"a", b"1", b"b", b"2"],
        &[b"APPEND", b"k", b"x"],
        &[b"INCR", b"n"],
        &[b"DECR", b"n"],
        &[b"INCRBY", b"n", b"5"],
        &[b"DECRBY", b"n", b"5"],
        &[b"set", b"k", b"w", b"NX"],
    ] {
        c.call(cmd, OOM);
    }
    // The command checks come first.
    c.call(&[b"SET", b"k"], b"-ERR wrong number of arguments for 'set' command\r\n");
    assert_eq!(processed(&mut c), before + 1, "only INFO counted");
    c.call(&[b"GET", b"k"], b"$1\r\nv\r\n");
    c.call(&[b"GET", b"n"], b"$1\r\n1\r\n");
    c.call(&[b"EXPIRE", b"n", b"100"], b":1\r\n");
    c.call(&[b"PERSIST", b"n"], b":1\r\n");
    c.call(&[b"GETDEL", b"n"], b"$1\r\n1\r\n");
    c.call(&[b"DEL", b"k"], b":1\r\n");
    c.call(&[b"FLUSHALL"], OK);
    c.call(&[b"SET", b"k", b"v"], OOM);
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"0"], OK);
    c.call(&[b"SET", b"k", b"v"], OK);
    // A limit far above what's used refuses nothing.
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"100gb"], OK);
    c.call(&[b"INCR", b"n"], b":1\r\n");
}

/// Redis 7.0's replies (checked with redis-cli against 7.0.15).
#[test]
fn config_maxmemory() {
    let mut c = Client::connect(start());
    let get = |c: &mut Client, expected: &str| {
        c.call(&[b"CONFIG", b"GET", b"maxmemory"], format!("*2\r\n$9\r\nmaxmemory\r\n${}\r\n{expected}\r\n", expected.len()).as_bytes());
    };
    get(&mut c, "0");
    c.call(&[b"CONFIG", b"GET", b"maxmemory-policy"], b"*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n");
    for (value, bytes) in [("1kb", "1024"), ("2G", "2000000000"), ("100", "100"), ("3mb", "3145728"), ("0", "0")] {
        c.call(&[b"CONFIG", b"SET", b"maxmemory", value.as_bytes()], OK);
        get(&mut c, bytes);
    }
    let bad_value = b"-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value\r\n";
    for value in ["1x", "-1", "1.5mb", "", "0%", "101%"] {
        c.call(&[b"CONFIG", b"SET", b"maxmemory", value.as_bytes()], bad_value);
    }
    c.call(&[b"CONFIG", b"SET", b"MAXMEMORY", b"5", b"maxmemory", b"6"], b"-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - duplicate parameter\r\n");
    c.call(&[b"CONFIG", b"SET", b"foo", b"1"], b"-ERR Unknown option or number of arguments for CONFIG SET - 'foo'\r\n");
    c.call(&[b"CONFIG", b"SET", b"maxmemory"], b"-ERR wrong number of arguments for 'config|set' command\r\n");
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"5", b"x"], b"-ERR syntax error\r\n");
    // Nothing applies unless everything does.
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"7", b"foo", b"1"], b"-ERR Unknown option or number of arguments for CONFIG SET - 'foo'\r\n");
    get(&mut c, "0");
    c.call(&[b"CONFIG", b"SET", b"maxmemory-policy", b"NoEviction", b"maxmemory", b"8"], OK);
    get(&mut c, "8");
    c.call(
        &[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lru"],
        b"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - only noeviction is supported\r\n",
    );
    c.call(&[b"CONFIG", b"GET", b"maxmemory*"], b"*4\r\n$9\r\nmaxmemory\r\n$1\r\n8\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n");
}

fn info_memory(c: &mut Client) -> Vec<(String, String)> {
    c.cmd(&[b"INFO", b"memory"]);
    let text = c.read_text();
    assert!(text.starts_with("# Memory\r\n"), "{text}");
    text.lines().skip(1).filter_map(|l| l.split_once(':')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn field(fields: &[(String, String)], name: &str) -> u64 {
    fields.iter().find(|(k, _)| k == name).unwrap_or_else(|| panic!("no {name} in {fields:?}")).1.parse().unwrap()
}

#[test]
fn info_memory_fields() {
    let mut c = Client::connect(start());
    c.call(&[b"CONFIG", b"SET", b"maxmemory", b"2gb"], OK);
    let fields = info_memory(&mut c);
    let names: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        names,
        [
            "used_memory",
            "used_memory_human",
            "used_memory_rss",
            "used_memory_rss_human",
            "used_memory_peak",
            "used_memory_peak_human",
            "used_memory_startup",
            "used_memory_dataset",
            "maxmemory",
            "maxmemory_human",
            "maxmemory_policy",
            "mem_fragmentation_ratio",
            "mem_allocator",
            "mem_map_growth",
        ]
    );
    assert!(field(&fields, "used_memory") > 0);
    assert!(field(&fields, "used_memory_peak") >= field(&fields, "used_memory"));
    assert_eq!(field(&fields, "maxmemory"), 2 << 30);
    let text = |name: &str| fields.iter().find(|(k, _)| k == name).unwrap().1.clone();
    assert_eq!(text("maxmemory_human"), "2.00G");
    assert_eq!(text("maxmemory_policy"), "noeviction");
    // In the default INFO, after Clients.
    c.cmd(&[b"INFO"]);
    let all = c.read_text();
    assert!(all.contains("connected_clients:1\r\n\r\n# Memory\r\nused_memory:"), "{all}");
}

/// The dbnode binary, killed when dropped.
struct Node {
    child: Child,
    addr: SocketAddr,
}

impl Node {
    fn start(extra: &[&str]) -> Node {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_dbnode"))
            .args([&addr.to_string(), "--loglevel", "warning"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(addr).is_err() {
            assert!(Instant::now() < deadline, "the node didn't start");
            thread::sleep(Duration::from_millis(10));
        }
        Node { child, addr }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Sends `n` requests in one go and reads their `n` replies, each `reply`
/// (empty: any one-line reply).
fn pipeline(c: &mut Client, requests: impl Iterator<Item = Vec<u8>>, reply: &[u8]) {
    let mut n = 0;
    let mut batch = Vec::new();
    for req in requests {
        batch.extend(req);
        n += 1;
    }
    let mut writer = c.0.try_clone().unwrap();
    let sender = thread::spawn(move || writer.write_all(&batch).unwrap());
    for _ in 0..n {
        if reply.is_empty() {
            c.read_line();
        } else {
            c.expect(reply);
        }
    }
    sender.join().unwrap();
}

/// The count follows the keys: up by about what they take, back down once
/// they're deleted (the map gives its room back).
#[test]
fn the_count_follows_the_keys() {
    let node = Node::start(&[]);
    let mut c = Client::connect(node.addr);
    let start = field(&info_memory(&mut c), "used_memory");
    let keys = 100_000;
    let value = [b'x'; 100];
    pipeline(&mut c, (0..keys).map(|i| request(&[b"SET", format!("key:{i:08}").as_bytes(), &value])), OK);
    let full = field(&info_memory(&mut c), "used_memory");
    // 12-byte keys and 100-byte values, with an entry each: Redis 7.0.15
    // takes 16.0 MB for the same (176 bytes a key).
    let per_key = (full - start) / keys;
    assert!((112..=250).contains(&per_key), "{per_key} bytes a key ({start} -> {full})");
    let rss = field(&info_memory(&mut c), "used_memory_rss");
    assert!(rss > full, "rss {rss}, used {full}");

    pipeline(&mut c, (0..keys).map(|i| request(&[b"DEL", format!("key:{i:08}").as_bytes()])), b":1\r\n");
    // The expiry fiber shrinks the map within 100 ms.
    thread::sleep(Duration::from_millis(300));
    let fields = info_memory(&mut c);
    let empty = field(&fields, "used_memory");
    assert!(empty < start + 256 * 1024, "{start} at the start, {empty} after deleting every key");
    assert!(field(&fields, "used_memory_peak") >= full);
}

/// `--maxmemory`: writes are refused once the node is past it, and it
/// doesn't go much further (one request's worth).
#[test]
fn a_node_fills_up_to_its_maxmemory() {
    let node = Node::start(&["--maxmemory", "8mb"]);
    let mut c = Client::connect(node.addr);
    let fields = info_memory(&mut c);
    assert_eq!(field(&fields, "maxmemory"), 8 << 20);
    let value = [b'x'; 1000];
    let mut i = 0;
    loop {
        c.cmd(&[b"SET", format!("key:{i}").as_bytes(), &value]);
        let line = c.read_line();
        if line.starts_with(b"-OOM") {
            break;
        }
        assert_eq!(line, OK, "key {i}");
        i += 1;
        assert!(i < 100_000, "never refused");
    }
    // Refused at the limit, or a little before when the next key would
    // make the map double past it.
    let used = field(&info_memory(&mut c), "used_memory");
    assert!(used > 6 << 20 && used < (8 << 20) + 64 * 1024, "{used}");
    assert!(i > 5000, "refused after {i} keys of 1 KB");
    c.call(&[b"GET", b"key:0"], &[&b"$1000\r\n"[..], &value, b"\r\n"].concat());
    // Deleting makes room again.
    for k in 0..100 {
        c.call(&[b"DEL", format!("key:{k}").as_bytes()], b":1\r\n");
    }
    c.call(&[b"SET", b"again", b"v"], OK);
}

/// `--maxmemory N%` is a share of the container's limit, and refused
/// without one.
#[test]
fn a_share_of_the_container_limit() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_dbnode"))
        .args(["127.0.0.1:0", "--maxmemory", "50%"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    match dbnode::memory::cgroup_limit() {
        None => {
            let out = child.wait_with_output().unwrap();
            assert_eq!(out.status.code(), Some(2));
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(err.contains("--maxmemory 50% needs a memory limit on the container"), "{err}");
        }
        Some(limit) => {
            // Started (and would have exited at once otherwise).
            thread::sleep(Duration::from_millis(300));
            assert!(child.try_wait().unwrap().is_none(), "exited with a {limit}-byte cgroup limit");
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
}

/// With small keys the map is most of the memory, and it doubles in one
/// allocation: writes are refused once that would go past `maxmemory`,
/// even if the node is below it now.
#[test]
fn the_map_does_not_double_past_maxmemory() {
    let max: u64 = 64 << 20;
    let node = Node::start(&["--maxmemory", "64mb"]);
    let mut c = Client::connect(node.addr);
    let mut i = 0u64;
    let used = loop {
        let batch: Vec<u8> = (i..i + 1000).flat_map(|k| request(&[b"SET", format!("{k}").as_bytes(), b"v"])).collect();
        c.send(&batch);
        let replies: Vec<Vec<u8>> = (0..1000).map(|_| c.read_line()).collect();
        if let Some(first) = replies.iter().position(|r| r.starts_with(b"-OOM")) {
            // Once refused, the rest of the batch is refused too.
            assert!(replies[first..].iter().all(|r| r.starts_with(b"-OOM")));
            break field(&info_memory(&mut c), "used_memory");
        }
        assert!(replies.iter().all(|r| r == OK));
        i += 1000;
        assert!(i < 10_000_000, "never refused");
    };
    assert!(used < max - (4 << 20), "refused at {used} of {max}: not ahead of the map's growth");
    let peak = field(&info_memory(&mut c), "used_memory_peak");
    assert!(peak <= max, "peak {peak} past {max}");
}

/// A value that grows (APPEND reallocates it) is counted as it grows; the
/// peak is sampled by the node itself, so it shows a level nobody asked
/// about while it lasted.
#[test]
fn growth_and_peak_are_counted() {
    let node = Node::start(&[]);
    let mut c = Client::connect(node.addr);
    let chunk = [b'x'; 1000];
    // 5 MB of keys, held 300 ms, deleted: no INFO meanwhile.
    let before = field(&info_memory(&mut c), "used_memory");
    pipeline(&mut c, (0..5000).map(|i| request(&[b"SET", format!("k{i}").as_bytes(), &chunk])), OK);
    thread::sleep(Duration::from_millis(300));
    pipeline(&mut c, (0..5000).map(|i| request(&[b"DEL", format!("k{i}").as_bytes()])), b":1\r\n");
    thread::sleep(Duration::from_millis(300));
    let fields = info_memory(&mut c);
    let peak = field(&fields, "used_memory_peak");
    assert!(peak >= before + 5_000_000, "peak {peak} from {before}");

    let start = field(&fields, "used_memory");
    pipeline(&mut c, (0..10_000).map(|_| request(&[b"APPEND", b"big", &chunk])), b"");
    let grown = field(&info_memory(&mut c), "used_memory");
    assert!(grown >= start + 10_000_000, "{start} -> {grown} after appending 10 MB");
}
