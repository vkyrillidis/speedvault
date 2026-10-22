//! Moving a slot between two nodes (phase 5 milestone 3): the source
//! streams the slot's keys to the target while clients go on writing, and
//! carries on after its connection to the target drops. The tables are
//! pushed by hand, as stuart will push them: the target first.

mod common;

use cluster::key_hash_slot;
use common::*;
use dbnode::{Config, Migrate};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Keys with this tag are all in one slot.
const TAG: &str = "{mv}";

fn slot() -> u16 {
    key_hash_slot(TAG.as_bytes())
}

fn key(i: usize) -> Vec<u8> {
    format!("{TAG}{i}").into_bytes()
}

/// A node in cluster mode announcing `announce` (default: where it
/// listens), with `migrate` settings.
fn start_node(migrate: Migrate, announce: Option<u16>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let port = announce.unwrap_or(addr.port());
    let cluster = Some(dbnode::Announce { host: "127.0.0.1".into(), port });
    thread::spawn(move || dbnode::serve(listener, None, Config { cluster, migrate, ..Config::default() }).unwrap());
    addr
}

fn myid(addr: SocketAddr) -> String {
    let mut c = Client::connect(addr);
    c.cmd(&[b"CLUSTER", b"MYID"]);
    c.read_text()
}

/// Two nodes: A owns every slot, B none, and their tables.
struct Pair {
    a: SocketAddr,
    b: SocketAddr,
    a_id: String,
    b_id: String,
    /// The port B is announced at (a proxy's, maybe).
    b_port: u16,
    epoch: u64,
}

impl Pair {
    fn new(migrate: Migrate) -> Pair {
        Pair::with_b_at(migrate, None)
    }

    /// B announced at `b_port` instead of where it listens.
    fn with_b_at(migrate: Migrate, b_port: Option<u16>) -> Pair {
        let a = start_node(migrate.clone(), None);
        let b = start_node(migrate, b_port);
        let mut pair = Pair { a, b, a_id: myid(a), b_id: myid(b), b_port: b_port.unwrap_or(b.port()), epoch: 0 };
        pair.push(&[("0", "16383", "A")], None);
        pair
    }

    /// Pushes a table (ranges' owners as "A" or "B") to B, then A.
    fn push(&mut self, ranges: &[(&str, &str, &str)], migrating: Option<u16>) {
        self.epoch += 1;
        let (a, b) = (self.a_id.clone(), self.b_id.clone());
        let mut args: Vec<Vec<u8>> = ["CLUSTER", "SETTABLE", &self.epoch.to_string(), "NODES", "2"].iter().map(|s| s.as_bytes().to_vec()).collect();
        for (id, port) in [(&a, self.a.port()), (&b, self.b_port)] {
            args.extend([id.as_bytes().to_vec(), b"127.0.0.1".to_vec(), port.to_string().into_bytes()]);
        }
        args.extend([b"RANGES".to_vec(), ranges.len().to_string().into_bytes()]);
        for (start, end, owner) in ranges {
            let id = if *owner == "A" { &a } else { &b };
            args.extend([start.as_bytes().to_vec(), end.as_bytes().to_vec(), id.as_bytes().to_vec()]);
        }
        if let Some(s) = migrating {
            args.extend([b"MIGRATING".to_vec(), b"1".to_vec(), s.to_string().into_bytes(), a.into_bytes(), b.into_bytes()]);
        }
        let args: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
        for addr in [self.b, self.a] {
            Client::connect(addr).call(&args, OK);
        }
    }

    fn start_moving(&mut self) {
        self.push(&[("0", "16383", "A")], Some(slot()));
    }

    /// The table after the move: B owns the slot.
    fn finish(&mut self) {
        let s = slot();
        let (before, after) = ((s - 1).to_string(), (s + 1).to_string());
        let me = s.to_string();
        self.push(&[("0", &before, "A"), (&me, &me, "B"), (&after, "16383", "A")], None);
    }

    /// Waits until A has nothing of the slot pending.
    fn wait_moved(&self, within: Duration) {
        let mut c = Client::connect(self.a);
        let deadline = Instant::now() + within;
        loop {
            c.cmd(&[b"CLUSTER", b"PENDING", slot().to_string().as_bytes()]);
            if c.read_integer() == "0" {
                return;
            }
            assert!(Instant::now() < deadline, "the slot didn't move in {within:?}");
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// A reply, as much as these tests look at.
#[derive(Debug, PartialEq)]
enum Reply {
    Ok,
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Error(String),
}

fn read(c: &mut Client) -> Reply {
    let line = c.read_line();
    let text = String::from_utf8_lossy(&line[1..line.len() - 2]).into_owned();
    match line[0] {
        b'+' => Reply::Ok,
        b':' => Reply::Int(text.parse().unwrap()),
        b'-' => Reply::Error(text),
        b'$' if text == "-1" => Reply::Bulk(None),
        b'$' => {
            let mut body = vec![0; text.parse::<usize>().unwrap() + 2];
            c.0.read_exact(&mut body).unwrap();
            body.truncate(body.len() - 2);
            Reply::Bulk(Some(body))
        }
        _ => panic!("unexpected reply {line:?}"),
    }
}

/// A cluster client for one slot: follows `-ASK` (with `ASKING`), `-MOVED`
/// and `-TRYAGAIN` as redis-py does.
struct Follower {
    a: Client,
    b: Client,
    /// Where the slot is, as far as this client knows.
    at_b: bool,
    redirects: usize,
}

impl Follower {
    fn new(pair: &Pair) -> Follower {
        Follower { a: Client::connect(pair.a), b: Client::connect(pair.b), at_b: false, redirects: 0 }
    }

    fn call(&mut self, args: &[&[u8]]) -> Reply {
        let mut ask = false;
        for _ in 0..100 {
            let c = if self.at_b || ask { &mut self.b } else { &mut self.a };
            if ask {
                c.call(&[b"ASKING"], OK);
            }
            c.cmd(args);
            match read(c) {
                Reply::Error(e) if e.starts_with("ASK ") => {
                    ask = true;
                    self.redirects += 1;
                }
                Reply::Error(e) if e.starts_with("MOVED ") => {
                    self.at_b = !self.at_b;
                    ask = false;
                    self.redirects += 1;
                }
                Reply::Error(e) if e.starts_with("TRYAGAIN") => {
                    ask = false;
                    thread::sleep(Duration::from_millis(1));
                }
                reply => return reply,
            }
        }
        panic!("{args:?}: redirected 100 times");
    }
}

fn get_asking(c: &mut Client, key: &[u8]) -> Reply {
    c.call(&[b"ASKING"], OK);
    c.cmd(&[b"GET", key]);
    read(c)
}

#[test]
fn moves_a_slot_with_its_keys() {
    let mut pair = Pair::new(Migrate { batch: 7, pause: Duration::ZERO, ..Migrate::default() });
    let mut a = Client::connect(pair.a);
    for i in 0..300 {
        a.call(&[b"SET", &key(i), format!("v{i}").as_bytes()], OK);
    }
    a.call(&[b"SET", &key(1000), b"ttl", b"PX", b"600000"], OK);
    a.call(&[b"SET", &key(1001), b"gone", b"PX", b"1"], OK);
    let binary: Vec<u8> = (0..=255).collect();
    a.call(&[b"SET", &key(1002), &binary], OK);
    let big = vec![b'x'; 3 * 1024 * 1024];
    a.call(&[b"SET", &key(1003), &big], OK);
    a.call(&[b"SET", b"elsewhere", b"stays"], OK);
    a.call(&[b"HSET", &key(1004), b"b", b"1", b"a", b"2"], b":2\r\n");
    a.call(&[b"SADD", &key(1005), b"3", b"1", b"x"], b":3\r\n");
    a.call(&[b"RPUSH", &key(1006), b"l1", b"", b"l3"], b":3\r\n");
    a.call(&[b"PEXPIRE", &key(1006), b"600000"], b":1\r\n");
    thread::sleep(Duration::from_millis(5));

    pair.start_moving();
    pair.wait_moved(Duration::from_secs(20));
    a.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", slot().to_string().as_bytes()], b":0\r\n");
    a.call(&[b"DBSIZE"], b":1\r\n");
    a.call(&[b"GET", b"elsewhere"], b"$5\r\nstays\r\n");
    a.call(&[b"GET", &key(5)], format!("-ASK {} 127.0.0.1:{}\r\n", slot(), pair.b_port).as_bytes());

    let mut b = Client::connect(pair.b);
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", slot().to_string().as_bytes()], b":306\r\n");
    // The collections, in their order.
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HGETALL", &key(1004)], b"*4\r\n$1\r\nb\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"SCARD", &key(1005)], b":3\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"LRANGE", &key(1006), b"0", b"-1"], b"*3\r\n$2\r\nl1\r\n$0\r\n\r\n$2\r\nl3\r\n");
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"PTTL", &key(1006)]);
    let Reply::Int(ttl) = read(&mut b) else { panic!() };
    assert!((590_000..=600_000).contains(&ttl), "{ttl}");
    for i in 0..300 {
        assert_eq!(get_asking(&mut b, &key(i)), Reply::Bulk(Some(format!("v{i}").into_bytes())), "key {i}");
    }
    assert_eq!(get_asking(&mut b, &key(1001)), Reply::Bulk(None), "expired at the source: not moved");
    assert_eq!(get_asking(&mut b, &key(1002)), Reply::Bulk(Some(binary)));
    assert_eq!(get_asking(&mut b, &key(1003)), Reply::Bulk(Some(big)));
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"PTTL", &key(1000)]);
    let Reply::Int(ttl) = read(&mut b) else { panic!() };
    assert!((590_000..=600_000).contains(&ttl), "{ttl}");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"TTL", &key(0)], b":-1\r\n");

    pair.finish();
    b.call(&[b"GET", &key(7)], b"$2\r\nv7\r\n");
    a.call(&[b"GET", &key(7)], format!("-MOVED {} 127.0.0.1:{}\r\n", slot(), pair.b_port).as_bytes());
}

/// Counters written through a redirect-following client while their slot
/// moves, in small batches with pauses: none loses a write.
#[test]
fn writes_during_a_move_are_kept() {
    let mut pair = Pair::new(Migrate { batch: 3, pause: Duration::from_micros(300), ..Migrate::default() });
    let mut a = Client::connect(pair.a);
    const KEYS: usize = 200;
    for i in 0..KEYS {
        a.call(&[b"SET", &key(i), b"0"], OK);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..3)
        .map(|w| {
            let mut f = Follower::new(&pair);
            let stop = stop.clone();
            thread::spawn(move || {
                // Writer w owns keys w, w+3, ...: the counts are exact.
                let mine: Vec<usize> = (w..KEYS).step_by(3).collect();
                let mut counts = vec![0i64; KEYS];
                let mut n = 0;
                while !stop.load(Ordering::Relaxed) {
                    let k = mine[n % mine.len()];
                    n += 1;
                    match f.call(&[b"INCR", &key(k)]) {
                        Reply::Int(v) => {
                            counts[k] += 1;
                            assert_eq!(v, counts[k], "key {k}");
                        }
                        other => panic!("INCR {k}: {other:?}"),
                    }
                    // Now and then a key is deleted and starts again.
                    if n % 97 == 0 {
                        assert_eq!(f.call(&[b"DEL", &key(k)]), Reply::Int(1));
                        counts[k] = 0;
                    }
                }
                (counts, f.redirects)
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(50));
    pair.start_moving();
    pair.wait_moved(Duration::from_secs(20));
    thread::sleep(Duration::from_millis(30));
    pair.finish();
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);

    let mut b = Client::connect(pair.b);
    let mut redirects = 0;
    for w in writers {
        let (counts, r) = w.join().unwrap();
        redirects += r;
        for (k, &count) in counts.iter().enumerate() {
            if count > 0 {
                b.call(&[b"GET", &key(k)], format!("${}\r\n{count}\r\n", count.to_string().len()).as_bytes());
            }
        }
    }
    assert!(redirects > 0, "the writers never saw a redirect: the move was too quick to test anything");
    a.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", slot().to_string().as_bytes()], b":0\r\n");
}

/// A TCP proxy in front of B. It can drop the connection instead of passing
/// B's next reply on (the batch landed, the source can't know), and refuse
/// connections for a while.
struct Proxy {
    port: u16,
    drop_reply: Arc<AtomicBool>,
    refuse: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
}

impl Proxy {
    fn start(to: Arc<std::sync::Mutex<Option<SocketAddr>>>) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (drop_reply, refuse, dropped) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
        let (d, r, n) = (drop_reply.clone(), refuse.clone(), dropped.clone());
        thread::spawn(move || {
            for client in listener.incoming() {
                let client = client.unwrap();
                if r.load(Ordering::SeqCst) {
                    continue; // dropped: closed
                }
                let Some(addr) = *to.lock().unwrap() else { continue };
                let server = TcpStream::connect(addr).unwrap();
                let (mut c_in, mut s_out) = (client.try_clone().unwrap(), server.try_clone().unwrap());
                thread::spawn(move || {
                    let mut buf = [0; 65536];
                    while let Ok(n) = c_in.read(&mut buf) {
                        if n == 0 || s_out.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    let _ = s_out.shutdown(std::net::Shutdown::Both);
                });
                let (mut s_in, mut c_out) = (server, client);
                let (d, n) = (d.clone(), n.clone());
                thread::spawn(move || {
                    let mut buf = [0; 65536];
                    while let Ok(len) = s_in.read(&mut buf) {
                        // MYID's reply (a bulk) passes; an IMPORT's (+OK) may not.
                        if len == 0 || (buf[0] == b'+' && d.swap(false, Ordering::SeqCst)) {
                            n.fetch_add(usize::from(len > 0), Ordering::SeqCst);
                            break;
                        }
                        if c_out.write_all(&buf[..len]).is_err() {
                            break;
                        }
                    }
                    let _ = c_out.shutdown(std::net::Shutdown::Both);
                    let _ = s_in.shutdown(std::net::Shutdown::Both);
                });
            }
        });
        Proxy { port, drop_reply, refuse, dropped }
    }
}

/// The connection drops after the target applied a batch, before the source
/// heard: the batch's keys are dirty. The source answers for them, present
/// or not, and once it can reach the target again sends their state then:
/// a key deleted meanwhile is deleted there too, not left as a stale copy.
#[test]
fn a_dropped_connection_resumes() {
    let to = Arc::new(std::sync::Mutex::new(None));
    let proxy = Proxy::start(to.clone());
    let mut pair = Pair::with_b_at(Migrate { batch: 5, pause: Duration::ZERO, ..Migrate::default() }, Some(proxy.port));
    *to.lock().unwrap() = Some(pair.b);
    let mut a = Client::connect(pair.a);
    for i in 0..40 {
        a.call(&[b"SET", &key(i), format!("v{i}").as_bytes()], OK);
    }
    proxy.drop_reply.store(true, Ordering::SeqCst);
    pair.start_moving();
    let deadline = Instant::now() + Duration::from_secs(5);
    while proxy.dropped.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "no batch went through the proxy");
        thread::sleep(Duration::from_millis(1));
    }
    proxy.refuse.store(true, Ordering::SeqCst);
    // The first batch (the first five keys written) landed at B; A still
    // has them, dirty.
    let mut b = Client::connect(pair.b);
    for i in 0..5 {
        assert_eq!(get_asking(&mut b, &key(i)), Reply::Bulk(Some(format!("v{i}").into_bytes())), "key {i} at B");
    }
    a.call(&[b"GET", &key(0)], b"$2\r\nv0\r\n");
    a.call(&[b"DEL", &key(0)], b":1\r\n");
    // Deleted, but dirty: A still answers for it, B's copy is stale.
    a.call(&[b"GET", &key(0)], NIL);
    a.call(&[b"SET", &key(1), b"new"], OK);
    a.call(&[b"CLUSTER", b"PENDING", slot().to_string().as_bytes()], b":40\r\n");
    proxy.refuse.store(false, Ordering::SeqCst);
    pair.wait_moved(Duration::from_secs(10));

    assert_eq!(get_asking(&mut b, &key(0)), Reply::Bulk(None), "deleted while dirty: deleted at B too");
    assert_eq!(get_asking(&mut b, &key(1)), Reply::Bulk(Some(b"new".to_vec())));
    for i in 2..40 {
        assert_eq!(get_asking(&mut b, &key(i)), Reply::Bulk(Some(format!("v{i}").into_bytes())), "key {i} at B");
    }
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", slot().to_string().as_bytes()], b":39\r\n");
    pair.finish();
}

/// B refuses batches until its table imports the slot (here A gets the
/// migration first); A tries again, and the keys stay at A meanwhile.
#[test]
fn a_target_without_the_table_refuses() {
    let mut pair = Pair::new(Migrate::default());
    let mut a = Client::connect(pair.a);
    a.call(&[b"SET", &key(1), b"1"], OK);
    // Only A learns of the move.
    pair.epoch += 1;
    let s = slot().to_string();
    let args: Vec<Vec<u8>> = vec![
        b"CLUSTER".to_vec(), b"SETTABLE".to_vec(), pair.epoch.to_string().into_bytes(), b"NODES".to_vec(), b"2".to_vec(),
        pair.a_id.clone().into_bytes(), b"127.0.0.1".to_vec(), pair.a.port().to_string().into_bytes(),
        pair.b_id.clone().into_bytes(), b"127.0.0.1".to_vec(), pair.b_port.to_string().into_bytes(),
        b"RANGES".to_vec(), b"1".to_vec(), b"0".to_vec(), b"16383".to_vec(), pair.a_id.clone().into_bytes(),
        b"MIGRATING".to_vec(), b"1".to_vec(), s.clone().into_bytes(), pair.a_id.clone().into_bytes(), pair.b_id.clone().into_bytes(),
    ];
    let refs: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
    a.call(&refs, OK);
    thread::sleep(Duration::from_millis(300));
    a.call(&[b"GET", &key(1)], b"$1\r\n1\r\n");
    a.call(&[b"CLUSTER", b"PENDING", s.as_bytes()], b":1\r\n");
    // Now B too, in a newer table; A takes it as well.
    pair.start_moving();
    pair.wait_moved(Duration::from_secs(5));
    let mut b = Client::connect(pair.b);
    assert_eq!(get_asking(&mut b, &key(1)), Reply::Bulk(Some(b"1".to_vec())));
}

/// `INFO cluster`'s field, as a number.
fn info_field(addr: SocketAddr, name: &str) -> u64 {
    let mut c = Client::connect(addr);
    c.cmd(&[b"INFO", b"cluster"]);
    let text = c.read_text();
    text.lines().find_map(|l| l.strip_prefix(&format!("{name}:"))).unwrap_or_else(|| panic!("no {name} in {text}")).parse().unwrap()
}

/// A batch stops at 1 MiB of values, whatever --migrate-batch says.
#[test]
fn batches_stop_at_a_mebibyte() {
    let mut pair = Pair::new(Migrate { batch: 100, pause: Duration::ZERO, ..Migrate::default() });
    let mut a = Client::connect(pair.a);
    let value = vec![b'v'; 600 * 1024];
    for i in 0..3 {
        a.call(&[b"SET", &key(i), &value], OK);
    }
    pair.start_moving();
    pair.wait_moved(Duration::from_secs(10));
    // 600 KiB, then 1.2 MiB: full; then the third alone.
    assert_eq!(info_field(pair.a, "migrate_batches_sent"), 2);
    assert_eq!(info_field(pair.a, "migrate_keys_sent"), 3);
    assert_eq!(info_field(pair.a, "migrate_bytes_sent"), 3 * 600 * 1024);
    assert_eq!(info_field(pair.b, "migrate_keys_imported"), 3);
}

/// A client keeping a node busy (GETs of a key outside the moving slot)
/// until dropped: the source then pauses after each batch.
struct Busy {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Busy {
    /// Returns once the node has served the client: else a first batch
    /// could go before it, unpaused.
    fn start(addr: SocketAddr) -> Busy {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let (served, first) = std::sync::mpsc::channel();
        let thread = thread::spawn(move || {
            // Pipelined, so that the node always has requests waiting: one
            // at a time, a batch's round trip could fall between them.
            let mut c = Client::connect(addr);
            let get = request(&[b"GET", b"elsewhere"]);
            let pipeline = get.repeat(16);
            while !flag.load(Ordering::Relaxed) {
                c.send(&pipeline);
                for _ in 0..16 {
                    c.expect(b"$-1\r\n");
                }
                let _ = served.send(());
            }
        });
        first.recv().unwrap();
        Busy { stop, thread: Some(thread) }
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// `--migrate-pause` holds only while clients are served: without any, a
/// slot moves at once whatever the pause.
#[test]
fn the_pause_is_for_busy_nodes_only() {
    let long = Migrate { batch: 1, pause: Duration::from_secs(60), ..Migrate::default() };
    let mut idle = Pair::new(long.clone());
    let mut a = Client::connect(idle.a);
    for i in 0..3 {
        a.call(&[b"SET", &key(i), b"1"], OK);
    }
    idle.start_moving();
    idle.wait_moved(Duration::from_secs(5));

    let mut busy = Pair::new(long);
    let mut a = Client::connect(busy.a);
    for i in 0..3 {
        a.call(&[b"SET", &key(i), b"1"], OK);
    }
    let _client = Busy::start(busy.a);
    busy.start_moving();
    thread::sleep(Duration::from_millis(500));
    a.call(&[b"CLUSTER", b"PENDING", slot().to_string().as_bytes()], b":2\r\n");
}

/// `INFO cluster` counts the redirects each node sends: `-MOVED`, `-ASK`
/// and `-TRYAGAIN`.
#[test]
fn redirects_are_counted() {
    // One key per batch, then a long pause (a client keeps A busy): some
    // keys move, some stay. (A batch goes without the pause if the client
    // wasn't served since the one before: rarely more than one.)
    let mut pair = Pair::new(Migrate { batch: 1, pause: Duration::from_secs(60), ..Migrate::default() });
    let busy = Busy::start(pair.a);
    let (mut a, mut b) = (Client::connect(pair.a), Client::connect(pair.b));
    const KEYS: usize = 20;
    for i in 0..KEYS {
        a.call(&[b"SET", &key(i), b"v"], OK);
    }
    b.cmd(&[b"GET", &key(1)]);
    assert!(matches!(read(&mut b), Reply::Error(e) if e.starts_with("MOVED ")));
    pair.start_moving();
    let deadline = Instant::now() + Duration::from_secs(5);
    while info_field(pair.a, "migrate_keys_sent") < 1 {
        assert!(Instant::now() < deadline, "no batch sent");
        thread::sleep(Duration::from_millis(5));
    }
    a.cmd(&[b"CLUSTER", b"GETKEYSINSLOT", slot().to_string().as_bytes(), b"100"]);
    let line = a.read_line();
    let left: usize = String::from_utf8_lossy(&line[1..line.len() - 2]).parse().unwrap();
    assert!((1..KEYS).contains(&left), "{left} keys left");
    let here: Vec<Vec<u8>> = (0..left).map(|_| a.read_bulk()).collect();
    let stayed = here[0].clone();
    let gone = (0..KEYS).map(key).find(|k| !here.contains(k)).unwrap();
    for (at_b, args, prefix) in [
        (false, vec![&b"GET"[..], &gone], "ASK "),
        (false, vec![&b"GET"[..], &gone], "ASK "),
        (false, vec![&b"MGET"[..], &stayed, &gone], "TRYAGAIN"),
        (true, vec![&b"GET"[..], &gone], "MOVED "),
    ] {
        let c = if at_b { &mut b } else { &mut a };
        c.cmd(&args);
        let reply = read(c);
        assert!(matches!(&reply, Reply::Error(e) if e.starts_with(prefix)), "{args:?}: {reply:?}");
    }
    // At B after ASKING, the moved key and one missing: TRYAGAIN.
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"MGET", &gone, &key(1000)]);
    assert!(matches!(read(&mut b), Reply::Error(e) if e.starts_with("TRYAGAIN")));
    // Served: not counted.
    a.call(&[b"GET", &stayed], b"$1\r\nv\r\n");
    assert_eq!(info_field(pair.a, "cluster_redirects_ask"), 2);
    assert_eq!(info_field(pair.a, "cluster_redirects_tryagain"), 1);
    assert_eq!(info_field(pair.a, "cluster_redirects_moved"), 0);
    assert_eq!(info_field(pair.b, "cluster_redirects_moved"), 2);
    assert_eq!(info_field(pair.b, "cluster_redirects_tryagain"), 1);
    assert_eq!(info_field(pair.b, "cluster_redirects_ask"), 0);
    drop(busy);
}

/// Another node answering at the target's address (say the target
/// restarted, with a new id) gets nothing: the keys stay.
#[test]
fn keys_go_only_to_the_target() {
    let fake = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = fake.local_addr().unwrap().port();
    let imported = Arc::new(AtomicBool::new(false));
    let seen = imported.clone();
    thread::spawn(move || {
        for conn in fake.incoming() {
            let mut conn = conn.unwrap();
            let seen = seen.clone();
            thread::spawn(move || {
                let mut buf = [0; 65536];
                while let Ok(n) = conn.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let request = &buf[..n];
                    let reply: &[u8] = if request.windows(4).any(|w| w == b"MYID") {
                        b"$40\r\n0123456789012345678901234567890123456789\r\n"
                    } else {
                        seen.store(true, Ordering::SeqCst);
                        b"+OK\r\n"
                    };
                    if conn.write_all(reply).is_err() {
                        break;
                    }
                }
            });
        }
    });
    let mut pair = Pair::with_b_at(Migrate::default(), Some(port));
    let mut a = Client::connect(pair.a);
    a.call(&[b"SET", &key(1), b"1"], OK);
    pair.start_moving();
    thread::sleep(Duration::from_millis(1500));
    assert!(!imported.load(Ordering::SeqCst), "keys went to another node");
    a.call(&[b"GET", &key(1)], b"$1\r\n1\r\n");
    a.call(&[b"CLUSTER", b"PENDING", slot().to_string().as_bytes()], b":1\r\n");
}

/// A move called off (the slot stays here, stuart's answer to a target
/// that died) settles the dirty keys: nothing is pending for them.
#[test]
fn a_move_called_off_settles_dirty_keys() {
    let to = Arc::new(std::sync::Mutex::new(None));
    let proxy = Proxy::start(to.clone());
    let mut pair = Pair::with_b_at(Migrate { batch: 5, pause: Duration::ZERO, ..Migrate::default() }, Some(proxy.port));
    *to.lock().unwrap() = Some(pair.b);
    let mut a = Client::connect(pair.a);
    for i in 0..10 {
        a.call(&[b"SET", &key(i), b"v"], OK);
    }
    proxy.refuse.store(false, Ordering::SeqCst);
    proxy.drop_reply.store(true, Ordering::SeqCst);
    pair.start_moving();
    let deadline = Instant::now() + Duration::from_secs(5);
    while proxy.dropped.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "no batch went through the proxy");
        thread::sleep(Duration::from_millis(1));
    }
    proxy.refuse.store(true, Ordering::SeqCst);
    let s = slot().to_string();
    a.call(&[b"DEL", &key(0)], b":1\r\n");
    a.call(&[b"CLUSTER", b"PENDING", s.as_bytes()], b":10\r\n");
    pair.push(&[("0", "16383", "A")], None);
    a.call(&[b"CLUSTER", b"PENDING", s.as_bytes()], b":9\r\n");
    a.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":9\r\n");
    a.call(&[b"GET", &key(0)], NIL);
    a.call(&[b"GET", &key(1)], b"$1\r\nv\r\n");
}

/// A target that can't be reached doesn't hold up the source's other
/// slots: with a lower slot migrating to a node that is down, a higher one
/// still moves to a node that is up.
#[test]
fn a_dead_target_holds_up_only_its_slots() {
    let a = start_node(Migrate { batch: 5, pause: Duration::ZERO, ..Migrate::default() }, None);
    let b = start_node(Migrate::default(), None);
    let dead = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let (a_id, b_id, c_id) = (myid(a), myid(b), "c".repeat(40));
    let (low, high) = ("{a}", "{x}");
    let (low_slot, high_slot) = (key_hash_slot(low.as_bytes()), key_hash_slot(high.as_bytes()));
    assert!(low_slot < high_slot);
    let table = |epoch: &str, migrating: bool| {
        let mut args: Vec<String> = ["CLUSTER", "SETTABLE", epoch, "NODES", "3"].iter().map(|s| s.to_string()).collect();
        for (id, port) in [(&a_id, a.port()), (&b_id, b.port()), (&c_id, dead)] {
            args.extend([id.clone(), "127.0.0.1".into(), port.to_string()]);
        }
        args.extend(["RANGES", "1", "0", "16383", &a_id].iter().map(|s| s.to_string()));
        if migrating {
            args.extend(["MIGRATING".to_string(), "2".into()]);
            args.extend([low_slot.to_string(), a_id.clone(), c_id.clone()]);
            args.extend([high_slot.to_string(), a_id.clone(), b_id.clone()]);
        }
        args
    };
    let push = |epoch: &str, migrating: bool| {
        let args = table(epoch, migrating);
        let args: Vec<&[u8]> = args.iter().map(|s| s.as_bytes()).collect();
        for addr in [b, a] {
            Client::connect(addr).call(&args, OK);
        }
    };
    push("1", false);
    let mut ca = Client::connect(a);
    for i in 0..20 {
        ca.call(&[b"SET", format!("{low}{i}").as_bytes(), b"v"], OK);
        ca.call(&[b"SET", format!("{high}{i}").as_bytes(), b"v"], OK);
    }
    push("2", true);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        ca.cmd(&[b"CLUSTER", b"PENDING", high_slot.to_string().as_bytes()]);
        if ca.read_integer() == "0" {
            break;
        }
        assert!(Instant::now() < deadline, "the slot to the live node didn't move");
        thread::sleep(Duration::from_millis(10));
    }
    ca.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", low_slot.to_string().as_bytes()], b":20\r\n");
    Client::connect(b).call(&[b"CLUSTER", b"COUNTKEYSINSLOT", high_slot.to_string().as_bytes()], b":20\r\n");
}

#[test]
fn import_and_pending_errors() {
    let pair = Pair::new(Migrate::default());
    let mut b = Client::connect(pair.b);
    let s = slot().to_string();
    let k = key(1);
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k, b"0", b"string", b"v"], format!("-ERR Slot {s} is not being imported here\r\n").as_bytes());
    b.call(&[b"CLUSTER", b"IMPORT", b"16384", b"0"], b"-ERR Invalid slot\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", b"x", b"0"], b"-ERR value is not an integer or out of range\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k, b"0", b"v"], b"-ERR syntax error\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"2", &k, b"0", b"string", b"v"], b"-ERR syntax error\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"-1"], b"-ERR syntax error\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes()], b"-ERR wrong number of arguments for 'cluster|import' command\r\n");
    b.call(&[b"CLUSTER", b"PENDING", b"16384"], b"-ERR Invalid slot\r\n");
    b.call(&[b"CLUSTER", b"PENDING", s.as_bytes()], b":0\r\n");

    let mut pair = pair;
    pair.start_moving();
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", b"other", b"0", b"string", b"v"], format!("-ERR Key 'other' is not in slot {s}\r\n").as_bytes());
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k, b"-2", b"string", b"v"], b"-ERR Invalid ttl '-2'\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k, b"x", b"string", b"v"], b"-ERR Invalid ttl 'x'\r\n");
    // All checked before any is applied.
    let k2 = key(2);
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"2", &k2, b"0", b"string", b"v", b"other", b"0", b"string", b"v"], format!("-ERR Key 'other' is not in slot {s}\r\n").as_bytes());
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":0\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"2", &k, b"5000", b"string", b"v", &k2, b"0", b"string", b"w"], OK);
    b.call(&[b"CLUSTER", b"PENDING", s.as_bytes()], b":2\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"-1", b"string", b""], OK);
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":1\r\n");
    // Types: an unknown one, a malformed collection, then a hash.
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"0", b"zset", b"v"], b"-ERR Invalid type 'zset'\r\n");
    let bad = [&b"-ERR Invalid value for '"[..], &k2, b"'\r\n"].concat();
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"0", b"hash", b"\x01\0\0\0a"], &bad);
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"0", b"list", b""], &bad);
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"0", b"hash", b"\x01\0\0\0f\x01\0\0\0v"], OK);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HGET", &k2, b"f"], b"$1\r\nv\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k2, b"-1", b"string", b""], OK);
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"PTTL", &k]);
    let Reply::Int(ttl) = read(&mut b) else { panic!() };
    assert!((4900..=5000).contains(&ttl), "{ttl}");
    // A standalone node has no CLUSTER at all.
    let mut s = Client::connect(start());
    s.call(&[b"CLUSTER", b"IMPORT", b"0", b"0"], b"-ERR This instance has cluster support disabled\r\n");
}

/// Counts memory, so that `maxmemory` applies in these tests.
#[global_allocator]
static ALLOCATOR: dbnode::memory::Counting = dbnode::memory::Counting;

/// A target past its `maxmemory` refuses the keys (`-OOM`); the source
/// keeps and serves them, and they move once the target has room.
#[test]
fn a_full_target_refuses_keys_until_it_has_room() {
    let mut pair = Pair::new(Migrate::default());
    let s = slot().to_string();
    let mut a = Client::connect(pair.a);
    for i in 0..50 {
        a.call(&[b"SET", &key(i), format!("v{i}").as_bytes()], OK);
    }
    let mut b = Client::connect(pair.b);
    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"1"], OK);
    pair.start_moving();
    let deadline = Instant::now() + Duration::from_secs(10);
    while info_field(pair.b, "migrate_imports_refused") == 0 {
        assert!(Instant::now() < deadline, "the target never refused");
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(200));
    a.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":50\r\n");
    a.call(&[b"GET", &key(3)], b"$2\r\nv3\r\n");
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":0\r\n");
    assert_eq!(info_field(pair.b, "migrate_keys_imported"), 0);
    // Writes the client is sent to the target with are refused there too.
    b.call(&[b"ASKING"], OK);
    b.call(&[b"SET", &key(1000), b"x"], b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");

    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"0"], OK);
    pair.wait_moved(Duration::from_secs(10));
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":50\r\n");

    // Refused as a whole; deletes alone pass, they free memory.
    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"1"], OK);
    let refused = info_field(pair.b, "migrate_imports_refused");
    b.call(
        &[b"CLUSTER", b"IMPORT", s.as_bytes(), b"2", &key(0), b"-1", b"string", b"", &key(99), b"0", b"string", b"v"],
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n",
    );
    assert_eq!(info_field(pair.b, "migrate_imports_refused"), refused + 1);
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":50\r\n");
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &key(0), b"-1", b"string", b""], OK);
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":49\r\n");
}

/// A reply's bytes, whatever its kind (arrays read whole).
fn raw_reply(c: &mut Client) -> Vec<u8> {
    let mut line = c.read_line();
    let n: i64 = String::from_utf8_lossy(&line[1..line.len() - 2]).parse().unwrap_or(0);
    match line[0] {
        b'$' if n >= 0 => {
            let mut body = vec![0; n as usize + 2];
            c.0.read_exact(&mut body).unwrap();
            line.extend(body);
        }
        b'*' | b'%' | b'~' => {
            let n = if line[0] == b'%' { 2 * n } else { n };
            for _ in 0..n.max(0) {
                line.extend(raw_reply(c));
            }
        }
        _ => {}
    }
    line
}

fn raw(c: &mut Client, args: &[&[u8]]) -> Vec<u8> {
    c.cmd(args);
    raw_reply(c)
}

/// The same, after `ASKING`.
fn raw_asking(c: &mut Client, args: &[&[u8]]) -> Vec<u8> {
    c.call(&[b"ASKING"], OK);
    raw(c, args)
}

/// A hash, a set and a list of `n` elements each in the moving slot (the
/// list with a TTL).
fn fill_collections(a: &mut Client, n: usize) {
    for i in 0..n {
        let m = format!("m{i}");
        a.call(&[b"HSET", &key(2000), m.as_bytes(), format!("v{i}").as_bytes()], b":1\r\n");
        a.call(&[b"SADD", &key(2001), m.as_bytes()], b":1\r\n");
        a.call(&[b"RPUSH", &key(2002), m.as_bytes()], format!(":{}\r\n", i + 1).as_bytes());
    }
    a.call(&[b"PEXPIRE", &key(2002), b"600000"], b":1\r\n");
}

/// The collections' contents, in their order, read with `read`.
fn collections(read: &mut dyn FnMut(&[&[u8]]) -> Vec<u8>) -> Vec<Vec<u8>> {
    vec![read(&[b"HGETALL", &key(2000)]), read(&[b"SMEMBERS", &key(2001)]), read(&[b"LRANGE", &key(2002), b"0", b"-1"])]
}

/// Collections larger than `--migrate-chunk` move alone, in parts, and
/// arrive whole: order, encoding and TTL included.
#[test]
fn large_collections_move_in_parts() {
    let mut pair = Pair::new(Migrate { batch: 100, pause: Duration::ZERO, chunk: 3 });
    let mut a = Client::connect(pair.a);
    fill_collections(&mut a, 40);
    a.call(&[b"HSET", &key(2003), b"f", b"v"], b":1\r\n");
    for i in 0..10 {
        a.call(&[b"SET", &key(i), b"v"], OK);
    }
    let before = collections(&mut |args| raw(&mut a, args));
    let encodings: Vec<Vec<u8>> = (2000..2003).map(|k| raw(&mut a, &[b"OBJECT", b"ENCODING", &key(k)])).collect();
    pair.start_moving();
    pair.wait_moved(Duration::from_secs(10));
    let mut b = Client::connect(pair.b);
    assert_eq!(collections(&mut |args| raw_asking(&mut b, args)), before);
    let got: Vec<Vec<u8>> = (2000..2003).map(|k| raw_asking(&mut b, &[b"OBJECT", b"ENCODING", &key(k)])).collect();
    assert_eq!(got, encodings);
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"PTTL", &key(2002)]);
    let Reply::Int(ttl) = read(&mut b) else { panic!() };
    assert!((590_000..=600_000).contains(&ttl), "{ttl}");
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", slot().to_string().as_bytes()], b":14\r\n");
    // 14 parts for each of the three collections of 40; the 11 small keys
    // in batches of 3 elements.
    assert_eq!(info_field(pair.a, "migrate_parts_sent"), 42);
    assert_eq!(info_field(pair.b, "migrate_parts_imported"), 42);
    assert_eq!(info_field(pair.a, "migrate_keys_sent"), 14);
    assert_eq!(info_field(pair.b, "migrate_keys_imported"), 14);
    // Full batches: a collection doesn't cut one short (which key comes
    // first after one is deleted varies).
    assert!((4..=6).contains(&info_field(pair.a, "migrate_batches_sent")));
    pair.finish();
}

/// Waits until `field` of `INFO cluster` at `addr` reaches `at_least`.
fn wait_info(addr: SocketAddr, field: &str, at_least: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while info_field(addr, field) < at_least {
        assert!(Instant::now() < deadline, "{field} never reached {at_least}");
        thread::sleep(Duration::from_millis(2));
    }
}

/// While a collection moves in parts the target shows nothing of it, and
/// writes to it at the source wait; they land at the target afterwards.
#[test]
fn a_collection_in_parts_is_hidden_and_writes_wait() {
    // A client keeps A busy: a long pause after each part.
    let mut pair = Pair::new(Migrate { batch: 100, pause: Duration::from_millis(100), chunk: 10 });
    let busy = Busy::start(pair.a);
    let mut a = Client::connect(pair.a);
    a.call(&[b"PING"], b"+PONG\r\n");
    for i in 0..40 {
        a.call(&[b"HSET", &key(2000), format!("m{i}").as_bytes(), format!("v{i}").as_bytes()], b":1\r\n");
    }
    pair.start_moving();
    wait_info(pair.b, "migrate_parts_imported", 1);
    let mut b = Client::connect(pair.b);
    let s = slot().to_string();
    // Its first part is at B, hidden.
    b.call(&[b"ASKING"], OK);
    b.call(&[b"EXISTS", &key(2000)], b":0\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HLEN", &key(2000)], b":0\r\n");
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":0\r\n");
    b.call(&[b"DBSIZE"], b":0\r\n");
    // A write at A waits for the whole hash to move (three more parts,
    // each after a pause), then is sent on.
    let mut f = Follower::new(&pair);
    let started = Instant::now();
    assert_eq!(f.call(&[b"HSET", &key(2000), b"new", b"1"]), Reply::Int(1));
    assert!(started.elapsed() >= Duration::from_millis(150), "the write didn't wait: {:?}", started.elapsed());
    assert!(f.redirects > 0);
    drop(busy);
    pair.wait_moved(Duration::from_secs(10));
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HLEN", &key(2000)], b":41\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HGET", &key(2000), b"m39"], b"$3\r\nv39\r\n");
    b.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", s.as_bytes()], b":1\r\n");
}

/// A target that fills up part way refuses a part: the source gives the
/// move of that key up and the key can be written again at once; it moves
/// again from the start once the target has room.
#[test]
fn a_target_full_part_way_gives_the_key_back() {
    let mut pair = Pair::new(Migrate { batch: 100, pause: Duration::from_millis(100), chunk: 10 });
    let busy = Busy::start(pair.a);
    let mut a = Client::connect(pair.a);
    for i in 0..40 {
        a.call(&[b"RPUSH", &key(2002), format!("e{i}").as_bytes()], format!(":{}\r\n", i + 1).as_bytes());
    }
    pair.start_moving();
    wait_info(pair.b, "migrate_parts_imported", 1);
    let mut b = Client::connect(pair.b);
    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"1"], OK);
    wait_info(pair.a, "migrate_parts_aborted", 1);
    // Written at A, at once.
    let started = Instant::now();
    a.call(&[b"RPUSH", &key(2002), b"late"], b":41\r\n");
    assert!(started.elapsed() < Duration::from_millis(50), "{:?}", started.elapsed());
    b.call(&[b"ASKING"], OK);
    b.call(&[b"EXISTS", &key(2002)], b":0\r\n");
    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"0"], OK);
    drop(busy);
    pair.wait_moved(Duration::from_secs(10));
    b.call(&[b"ASKING"], OK);
    b.call(&[b"LLEN", &key(2002)], b":41\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"LINDEX", &key(2002), b"-1"], b"$4\r\nlate\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"LINDEX", &key(2002), b"0"], b"$2\r\ne0\r\n");
}

/// The connection drops after the target took a part: the key is dirty,
/// changed at the source, and sent again from the start, whole.
#[test]
fn a_dropped_connection_restarts_a_collection() {
    let to = Arc::new(std::sync::Mutex::new(None));
    let proxy = Proxy::start(to.clone());
    let mut pair = Pair::with_b_at(Migrate { batch: 100, pause: Duration::ZERO, chunk: 10 }, Some(proxy.port));
    *to.lock().unwrap() = Some(pair.b);
    let mut a = Client::connect(pair.a);
    for i in 0..40 {
        a.call(&[b"SADD", &key(2001), format!("e{i}").as_bytes()], b":1\r\n");
    }
    proxy.drop_reply.store(true, Ordering::SeqCst);
    pair.start_moving();
    let deadline = Instant::now() + Duration::from_secs(5);
    while proxy.dropped.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline, "no part went through the proxy");
        thread::sleep(Duration::from_millis(1));
    }
    proxy.refuse.store(true, Ordering::SeqCst);
    let mut b = Client::connect(pair.b);
    assert_eq!(info_field(pair.b, "migrate_parts_imported"), 1);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"EXISTS", &key(2001)], b":0\r\n");
    // Not in flight any more: writable at A.
    a.call(&[b"SREM", &key(2001), b"e0", b"e1"], b":2\r\n");
    a.call(&[b"CLUSTER", b"PENDING", slot().to_string().as_bytes()], b":1\r\n");
    proxy.refuse.store(false, Ordering::SeqCst);
    pair.wait_moved(Duration::from_secs(10));
    b.call(&[b"ASKING"], OK);
    b.call(&[b"SCARD", &key(2001)], b":38\r\n");
    b.call(&[b"ASKING"], OK);
    b.call(&[b"SISMEMBER", &key(2001), b"e0"], b":0\r\n");
}

/// `CLUSTER IMPORTPART` by hand: its checks, a key hidden until its last
/// part, and partial keys dropped by a delete or a table that ends the
/// import.
#[test]
fn import_part_by_hand() {
    let mut pair = Pair::new(Migrate::default());
    let mut b = Client::connect(pair.b);
    let s = slot().to_string();
    let k = key(1);
    let part = |items: &[&str]| -> Vec<u8> { items.iter().flat_map(|i| [&(i.len() as u32).to_le_bytes()[..], i.as_bytes()].concat()).collect() };
    let (p1, p2) = (part(&["a", "1", "b", "2"]), part(&["c", "3"]));
    let send = |b: &mut Client, kind: &[u8], total: &[u8], offset: &[u8], payload: &[u8], ttl: &[u8], expected: &[u8]| {
        b.call(&[b"CLUSTER", b"IMPORTPART", s.as_bytes(), &k, kind, total, offset, payload, ttl], expected);
    };
    let not_here = format!("-ERR Slot {s} is not being imported here\r\n");
    send(&mut b, b"hash", b"3", b"0", &p1, b"0", not_here.as_bytes());
    pair.start_moving();
    let unexpected = [&b"-ERR Unexpected part of '"[..], &k, b"'\r\n"].concat();
    let invalid = [&b"-ERR Invalid value for '"[..], &k, b"'\r\n"].concat();
    b.call(&[b"CLUSTER", b"IMPORTPART", s.as_bytes(), b"other", b"hash", b"3", b"0", &p1, b"0"], format!("-ERR Key 'other' is not in slot {s}\r\n").as_bytes());
    send(&mut b, b"string", b"3", b"0", &p1, b"0", b"-ERR Invalid type 'string'\r\n");
    send(&mut b, b"zset", b"3", b"0", &p1, b"0", b"-ERR Invalid type 'zset'\r\n");
    send(&mut b, b"hash", b"-1", b"0", &p1, b"0", b"-ERR syntax error\r\n");
    send(&mut b, b"hash", b"3", b"x", &p1, b"0", b"-ERR syntax error\r\n");
    send(&mut b, b"hash", b"3", b"0", &p1, b"-5", b"-ERR syntax error\r\n");
    send(&mut b, b"hash", b"3", b"0", &part(&["a"]), b"0", &invalid);
    send(&mut b, b"hash", b"3", b"0", b"", b"0", &invalid);
    send(&mut b, b"hash", b"3", b"2", &p1, b"0", &unexpected);
    send(&mut b, b"hash", b"1", b"0", &p1, b"0", &unexpected);
    b.call(&[b"CLUSTER", b"IMPORTPART"], b"-ERR wrong number of arguments for 'cluster|importpart' command\r\n");
    // Two parts; hidden until the second.
    send(&mut b, b"hash", b"3", b"0", &p1, b"0", OK);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"EXISTS", &k], b":0\r\n");
    send(&mut b, b"hash", b"3", b"1", &p2, b"0", &unexpected);
    send(&mut b, b"set", b"3", b"2", &part(&["c"]), b"0", &unexpected);
    send(&mut b, b"hash", b"4", b"2", &p2, b"0", &unexpected);
    send(&mut b, b"hash", b"3", b"2", &p2, b"9000", OK);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"HGETALL", &k], b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n");
    b.call(&[b"ASKING"], OK);
    b.cmd(&[b"PTTL", &k]);
    let Reply::Int(ttl) = read(&mut b) else { panic!() };
    assert!((8900..=9000).contains(&ttl), "{ttl}");
    // Offset 0 starts again; the finished key stays until the last part.
    send(&mut b, b"list", b"3", b"0", &part(&["x"]), b"0", OK);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"TYPE", &k], b"+hash\r\n");
    // A delete drops the parts.
    b.call(&[b"CLUSTER", b"IMPORT", s.as_bytes(), b"1", &k, b"-1", b"string", b""], OK);
    send(&mut b, b"list", b"3", b"1", &part(&["y", "z"]), b"0", &unexpected);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"EXISTS", &k], b":0\r\n");
    // So does a table that ends the import, even if another starts it again.
    send(&mut b, b"list", b"3", b"0", &part(&["x"]), b"0", OK);
    pair.push(&[("0", "16383", "A")], None);
    pair.start_moving();
    send(&mut b, b"list", b"3", b"1", &part(&["y", "z"]), b"0", &unexpected);
    send(&mut b, b"list", b"3", b"0", &part(&["x", "y", "z"]), b"0", OK);
    b.call(&[b"ASKING"], OK);
    b.call(&[b"LRANGE", &k, b"0", b"-1"], b"*3\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n");
    // A first part is refused if the whole wouldn't fit, estimated from
    // it: one element of a million, with 64 MiB to spare (the tests in
    // this binary share the counting allocator: room for them too).
    b.cmd(&[b"INFO", b"memory"]);
    let info = b.read_text();
    let used: u64 = info.lines().find_map(|l| l.strip_prefix("used_memory:")).unwrap().parse().unwrap();
    let max = (used + (64 << 20)).to_string();
    b.call(&[b"CONFIG", b"SET", b"maxmemory", max.as_bytes()], OK);
    send(&mut b, b"set", b"1000000", b"0", &part(&["x"]), b"0", b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    send(&mut b, b"set", b"2", b"0", &part(&["x"]), b"0", OK);
    send(&mut b, b"set", b"2", b"1", &part(&["y"]), b"0", OK);
    b.call(&[b"CONFIG", b"SET", b"maxmemory", b"0"], OK);
}
