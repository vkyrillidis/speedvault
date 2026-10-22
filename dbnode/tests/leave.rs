//! Leaving the cluster on SIGTERM (phase 6 milestone 2): the dbnode binary
//! as a process, joined to a fake stuart that answers `STUART LEAVE` from a
//! script.

mod common;

use common::*;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// A fake stuart: `+OK` to `STUART JOIN`; to `STUART LEAVE`, the replies of
/// `script` in turn, the last one again and again. Every request goes to
/// the log, as its words.
struct Stuart {
    addr: SocketAddr,
    log: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Stuart {
    fn start(script: &[&'static str]) -> Stuart {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Arc<Mutex<Vec<Vec<String>>>> = Default::default();
        let (script, l) = (script.to_vec(), log.clone());
        thread::spawn(move || {
            let mut leaves = 0;
            for conn in listener.incoming() {
                let Ok(conn) = conn else { return };
                let mut r = BufReader::new(conn.try_clone().unwrap());
                let Some(args) = read_request(&mut r) else { continue };
                let reply = if args.get(1).map(String::as_str) == Some("LEAVE") {
                    leaves += 1;
                    script[(leaves - 1).min(script.len() - 1)]
                } else {
                    "+OK"
                };
                l.lock().unwrap().push(args);
                let _ = (&conn).write_all(format!("{reply}\r\n").as_bytes());
            }
        });
        Stuart { addr, log }
    }

    fn leaves(&self) -> usize {
        self.log.lock().unwrap().iter().filter(|a| a.get(1).map(String::as_str) == Some("LEAVE")).count()
    }

    fn joined(&self) -> Option<String> {
        self.log.lock().unwrap().iter().find(|a| a.get(1).map(String::as_str) == Some("JOIN")).map(|a| a[2].clone())
    }
}

fn read_request(r: &mut BufReader<TcpStream>) -> Option<Vec<String>> {
    let mut head = String::new();
    r.read_line(&mut head).ok()?;
    let n: usize = head.trim_end().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::new();
    for _ in 0..n {
        let mut len = String::new();
        r.read_line(&mut len).ok()?;
        let mut arg = vec![0; len.trim_end().strip_prefix('$')?.parse::<usize>().ok()? + 2];
        r.read_exact(&mut arg).ok()?;
        args.push(String::from_utf8_lossy(&arg[..arg.len() - 2]).into_owned());
    }
    Some(args)
}

/// The dbnode binary in cluster mode, joining `stuart`; killed when dropped.
struct Node {
    child: Child,
    addr: SocketAddr,
}

impl Node {
    fn start(stuart: &Stuart, extra: &[&str]) -> Node {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_dbnode"))
            .args([&addr.to_string(), "--cluster", "--stuart", &stuart.addr.to_string(), "--loglevel", "warning"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let node = Node { child, addr };
        wait_until("the node joined", || stuart.joined().is_some());
        node
    }

    fn signal(&self, signal: libc::c_int) {
        assert_eq!(unsafe { libc::kill(self.child.id() as libc::pid_t, signal) }, 0);
    }

    /// Its exit status, if it exits within `within`.
    fn exited(&mut self, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn info_cluster(&self) -> String {
        let mut c = Client::connect(self.addr);
        c.cmd(&[b"INFO", b"cluster"]);
        String::from_utf8(c.read_bulk()).unwrap()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// SIGTERM: the node asks to leave once a second, serving meanwhile; once
/// stuart answers 0 it serves on for --leave-linger (it owns no slot: it
/// would only redirect), then exits (status 0).
#[test]
fn sigterm_leaves_the_cluster_lingers_then_exits() {
    let stuart = Stuart::start(&[":3", ":1", ":0"]);
    let mut node = Node::start(&stuart, &["--leave-linger", "2"]);
    let id = stuart.joined().unwrap();
    assert!(node.info_cluster().contains("\r\ncluster_leaving:0\r\n"));
    node.signal(libc::SIGTERM);
    wait_until("the first STUART LEAVE", || stuart.leaves() >= 1);
    assert!(node.info_cluster().contains("\r\ncluster_leaving:1\r\n"));
    Client::connect(node.addr).call(&[b"PING"], b"+PONG\r\n");
    wait_until("the third STUART LEAVE", || stuart.leaves() >= 3);
    let left = Instant::now();
    assert!(node.exited(Duration::from_millis(1500)).is_none(), "the node lingers");
    Client::connect(node.addr).call(&[b"PING"], b"+PONG\r\n");
    let status = node.exited(Duration::from_secs(3)).expect("the node exits once it has lingered");
    assert!(status.success(), "{status}");
    assert!(left.elapsed() >= Duration::from_millis(1900), "{:?}", left.elapsed());
    assert_eq!(stuart.leaves(), 3, "once a second until :0");
    let log = stuart.log.lock().unwrap();
    assert!(log.iter().filter(|a| a[1] == "LEAVE").all(|a| a.len() == 3 && a[2] == id));
}

/// A node that stuart lets go while it still owns slots (every node is
/// leaving: a shutdown) exits at once: there is nobody to redirect to.
#[test]
fn a_shutdown_exits_at_once() {
    let stuart = Stuart::start(&[":0"]);
    let mut node = Node::start(&stuart, &["--leave-linger", "60"]);
    let id = stuart.joined().unwrap();
    let (host, port) = (node.addr.ip().to_string(), node.addr.port().to_string());
    let table: [&[u8]; 13] = [
        b"CLUSTER", b"SETTABLE", b"1", b"NODES", b"1", id.as_bytes(), host.as_bytes(), port.as_bytes(),
        b"RANGES", b"1", b"0", b"16383", id.as_bytes(),
    ];
    Client::connect(node.addr).call(&table, b"+OK\r\n");
    node.signal(libc::SIGTERM);
    let status = node.exited(Duration::from_secs(3)).expect("a shutdown exits at once");
    assert!(status.success(), "{status}");
}

/// A second SIGTERM exits at once; so does SIGINT, first or not.
#[test]
fn a_second_signal_exits_at_once() {
    let stuart = Stuart::start(&[":5"]);
    let mut node = Node::start(&stuart, &[]);
    node.signal(libc::SIGTERM);
    wait_until("the first STUART LEAVE", || stuart.leaves() >= 1);
    assert!(node.exited(Duration::from_millis(1500)).is_none(), "the node waits until it has left");
    node.signal(libc::SIGTERM);
    let status = node.exited(Duration::from_secs(2)).expect("a second SIGTERM exits");
    assert!(status.success(), "{status}");

    let stuart = Stuart::start(&[":5"]);
    let mut node = Node::start(&stuart, &[]);
    node.signal(libc::SIGINT);
    assert!(node.exited(Duration::from_secs(2)).is_some(), "SIGINT exits at once");
    assert_eq!(stuart.leaves(), 0);
}

/// With nowhere for its slots to go (stuart refuses: the last node with
/// slots), the node exits at once; with stuart answering but never 0, it
/// exits after --drain-timeout (status 1 both times: keys were lost).
#[test]
fn refused_or_too_long() {
    let stuart = Stuart::start(&["-ERR No other node to move the slots to"]);
    let mut node = Node::start(&stuart, &[]);
    node.signal(libc::SIGTERM);
    let status = node.exited(Duration::from_secs(3)).expect("a refusal exits");
    assert_eq!(status.code(), Some(1));

    let stuart = Stuart::start(&[":5"]);
    let mut node = Node::start(&stuart, &["--drain-timeout", "2"]);
    let t = Instant::now();
    node.signal(libc::SIGTERM);
    let status = node.exited(Duration::from_secs(6)).expect("exits after --drain-timeout");
    assert_eq!(status.code(), Some(1));
    assert!(t.elapsed() >= Duration::from_secs(2), "{:?}", t.elapsed());
    assert!(stuart.leaves() >= 2);
}

/// Without stuart a SIGTERM exits at once, as before.
#[test]
fn without_stuart_sigterm_exits() {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_dbnode"))
        .args([&addr, "--cluster"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Killed when dropped, even if the test fails.
    let mut node = Node { child, addr: addr.parse().unwrap() };
    wait_until("it listens", || TcpStream::connect(&addr).is_ok());
    node.signal(libc::SIGTERM);
    assert!(node.exited(Duration::from_secs(2)).is_some(), "SIGTERM didn't stop it");
}

/// `GET path` at `addr`: the status line and the body.
fn http_get(addr: SocketAddr, path: &str) -> (String, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_string(), body.to_string())
}

/// With `--metrics`: ready once stuart took the join, not ready (and
/// `speedvault_cluster_leaving 1`) once leaving.
#[test]
fn ready_until_leaving() {
    let stuart = Stuart::start(&[":5"]);
    let metrics: SocketAddr = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let node = Node::start(&stuart, &["--metrics", &metrics.to_string()]);
    wait_until("ready", || TcpStream::connect(metrics).is_ok() && http_get(metrics, "/ready").0 == "HTTP/1.1 200 OK");
    assert!(http_get(metrics, "/metrics").1.contains("\nspeedvault_cluster_leaving 0\n"));
    node.signal(libc::SIGTERM);
    wait_until("the first STUART LEAVE", || stuart.leaves() >= 1);
    assert_eq!(http_get(metrics, "/ready"), ("HTTP/1.1 503 Service Unavailable".into(), "not ready: leaving the cluster\n".into()));
    assert!(http_get(metrics, "/metrics").1.contains("\nspeedvault_cluster_leaving 1\n"));
    assert_eq!(http_get(metrics, "/healthz").0, "HTTP/1.1 200 OK", "alive while it leaves");
}
