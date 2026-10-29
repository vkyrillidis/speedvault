//! stuart with real nodes: the stuart binary as a process (so it can be
//! killed and restarted), dbnodes in cluster mode on threads of the test,
//! and a fake node where a test needs one that misbehaves.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// A free port for stuart, below the ephemeral range (32768-60999 on
/// Linux) that every node and fake node of these tests binds from with
/// port 0: a port picked by binding to 0 and let go could be handed to one
/// of them before stuart binds it, and the test would talk to that.
fn free_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let base = 20000 + (std::process::id() % 500) as u16 * 20;
    loop {
        let port = base + NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 2000;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stuart-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The stuart binary on `port`, killed when dropped; its log.
struct Stuart(Child, SocketAddr, PathBuf);

impl Stuart {
    fn start(port: u16, state: &Path) -> Stuart {
        Stuart::start_with(port, state, &[])
    }

    fn start_with(port: u16, state: &Path, extra: &[&str]) -> Stuart {
        // Its log goes next to the state file, and is printed if a test
        // fails while stuart runs.
        let log = std::fs::OpenOptions::new().create(true).append(true).open(state.with_extension("log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_speedvault-stuart"))
            .args([&format!("127.0.0.1:{port}"), "--state", state.to_str().unwrap(), "--loglevel", "notice"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        wait_until("stuart listens", || TcpStream::connect(addr).is_ok());
        let mut stuart = Stuart(child, addr, state.with_extension("log"));
        if let Err(e) = TcpStream::connect(addr) {
            thread::sleep(Duration::from_millis(300));
            let status = stuart.0.try_wait();
            let log = std::fs::read_to_string(&stuart.2).unwrap_or_default();
            panic!("stuart on {addr} answered once, then: {e}; the process: {status:?}; its log: {log}");
        }
        let mut c = stuart.client();
        if let Some(i) = extra.iter().position(|&a| a == "--requirepass-file") {
            let password = std::fs::read_to_string(extra[i + 1]).unwrap();
            assert_eq!(c.text(&["AUTH", password.trim_end()]), "+OK");
        }
        let info = c.text(&["INFO"]);
        let pid = format!("\r\nprocess_id:{}\r\n", stuart.0.id());
        assert!(info.contains(&pid), "another process listens on {addr}: {info:?}");
        stuart
    }

    fn client(&self) -> Client {
        Client::connect(self.1)
    }
}

impl Drop for Stuart {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        if thread::panicking() {
            eprintln!("stuart's log:
{}", std::fs::read_to_string(&self.2).unwrap_or_default());
        }
    }
}

/// A dbnode in cluster mode joining `stuart`, on a thread.
fn node(stuart: SocketAddr) -> SocketAddr {
    node_in(stuart, None)
}

/// A dbnode joining `stuart` from `zone` (`--zone`).
fn node_in(stuart: SocketAddr, zone: Option<&str>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = dbnode::Config {
        cluster: Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() }),
        stuart: Some(stuart.to_string()),
        zone: zone.map(str::to_string),
        ..dbnode::Config::default()
    };
    thread::spawn(move || dbnode::serve(listener, None, config).unwrap());
    addr
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

struct Client(BufReader<TcpStream>);

impl Client {
    fn connect(addr: SocketAddr) -> Client {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        Client(BufReader::new(s))
    }

    /// The raw bytes of the reply.
    fn call(&mut self, args: &[&str]) -> Vec<u8> {
        let mut req = format!("*{}\r\n", args.len());
        for a in args {
            req += &format!("${}\r\n{a}\r\n", a.len());
        }
        self.0.get_mut().write_all(req.as_bytes()).unwrap();
        let mut raw = Vec::new();
        read_reply(&mut self.0, &mut raw);
        raw
    }

    /// The reply as text, for `+`, `-`, `:`, `$` and `=` replies (without
    /// the framing; `txt:` dropped).
    fn text(&mut self, args: &[&str]) -> String {
        let raw = self.call(args);
        let s = String::from_utf8(raw).unwrap();
        let (head, rest) = s.split_once("\r\n").unwrap();
        match &head[..1] {
            "$" | "=" if head != "$-1" => rest[..rest.len() - 2].trim_start_matches("txt:").to_string(),
            _ => head.to_string(),
        }
    }
}

fn read_reply(r: &mut BufReader<TcpStream>, raw: &mut Vec<u8>) {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line).unwrap();
    raw.extend_from_slice(&line);
    let n: i64 = std::str::from_utf8(&line[1..line.len() - 2]).unwrap_or("0").parse().unwrap_or(0);
    match line[0] {
        b'$' | b'=' | b'!' if n >= 0 => {
            let mut body = vec![0; n as usize + 2];
            r.read_exact(&mut body).unwrap();
            raw.extend_from_slice(&body);
        }
        b'*' | b'~' | b'>' => (0..n).for_each(|_| read_reply(r, raw)),
        b'%' => (0..2 * n).for_each(|_| read_reply(r, raw)),
        _ => {}
    }
}

fn epoch_of(c: &mut Client) -> u64 {
    let info = c.text(&["CLUSTER", "INFO"]);
    info.lines().find_map(|l| l.strip_prefix("cluster_current_epoch:")).unwrap().parse().unwrap()
}

/// The `STUART NODES` lines that say `up` and `acked=epoch`.
fn up_and_acked(s: &mut Client, epoch: u64) -> usize {
    let text = s.text(&["STUART", "NODES"]);
    text.lines().filter(|l| l.contains(" up ") && l.contains(&format!(" acked={epoch} "))).count()
}

#[test]
fn a_cluster_forms_grows_and_survives_a_restart() {
    let dir = temp_dir("forms");
    let state = dir.join("state");
    let port = free_port();
    let stuart = Stuart::start(port, &state);
    let mut s = stuart.client();
    assert_eq!(s.call(&["CLUSTER", "SLOTS"]), b"*0\r\n");
    assert_eq!(s.text(&["GET", "foo"]), "-CLUSTERDOWN Hash slot not served");

    // One at a time, so that the join order (the table's) is this order.
    let mut nodes = Vec::new();
    for i in 1..=3 {
        nodes.push(node(stuart.1));
        wait_until("the node joined and is up", || up_and_acked(&mut s, 0) == i);
    }
    // Before CREATE: the nodes, owning nothing.
    let listed = s.text(&["CLUSTER", "NODES"]);
    assert_eq!(listed.lines().count(), 3, "{listed}");
    assert!(listed.lines().all(|l| l.ends_with(" master - 0 0 0 connected")), "{listed}");

    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    assert_eq!(s.text(&["STUART", "CREATE"]), "-ERR The slots were assigned already (table epoch 1)");
    let mut clients: Vec<Client> = nodes.iter().map(|&a| Client::connect(a)).collect();
    wait_until("every node has epoch 1", || clients.iter_mut().all(|c| epoch_of(c) == 1));
    wait_until("stuart knows they took it", || up_and_acked(&mut s, 1) == 3);

    // Everyone gives the same layout: 3 even ranges in join order.
    let slots = s.call(&["CLUSTER", "SLOTS"]);
    for c in &mut clients {
        assert_eq!(c.call(&["CLUSTER", "SLOTS"]), slots);
    }
    let table = s.text(&["STUART", "TABLE"]);
    assert!(table.starts_with("epoch 1\n"), "{table}");
    for (line, range) in table.lines().skip(1).zip(["0-5460", "5461-10922", "10923-16383"]) {
        assert!(line.ends_with(&format!(" master - 0 0 1 connected {range}")), "{table}");
    }
    // foo (12182) is the third node's; bar (5061) the first's.
    let moved_foo = format!("-MOVED 12182 127.0.0.1:{}", nodes[2].port());
    assert_eq!(s.text(&["GET", "foo"]), moved_foo);
    assert_eq!(clients[0].text(&["GET", "foo"]), moved_foo);
    assert_eq!(clients[2].text(&["SET", "foo", "1"]), "+OK");
    assert_eq!(clients[0].text(&["SET", "bar", "2"]), "+OK");
    assert_eq!(s.text(&["SET", "bar", "x"]), format!("-MOVED 5061 127.0.0.1:{}", nodes[0].port()));

    // A fourth node: in the table without slots, a new epoch for everyone.
    let fourth = node(stuart.1);
    clients.push(Client::connect(fourth));
    wait_until("every node has epoch 2", || clients.iter_mut().all(|c| epoch_of(c) == 2));
    let seen = clients[3].text(&["CLUSTER", "NODES"]);
    assert_eq!(seen.lines().count(), 4);
    assert!(seen.lines().any(|l| l.contains("myself,master - 0 0 2 connected") && !l.contains(" connected ")), "{seen}");
    assert_eq!(clients[2].text(&["GET", "foo"]), "1", "the keys stay");

    // stuart restarts, from its state file; a fifth node joins while it's down.
    drop(s);
    drop(stuart);
    let fifth = node(format!("127.0.0.1:{port}").parse().unwrap());
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(clients[0].text(&["GET", "bar"]), "2", "the nodes serve without stuart");
    let stuart = Stuart::start(port, &state);
    let mut s = stuart.client();
    clients.push(Client::connect(fifth));
    wait_until("every node has epoch 3", || clients.iter_mut().all(|c| epoch_of(c) == 3));
    wait_until("all 5 up with epoch 3", || up_and_acked(&mut s, 3) == 5);
    assert_eq!(s.call(&["CLUSTER", "SLOTS"]), clients[4].call(&["CLUSTER", "SLOTS"]));
    let _ = std::fs::remove_dir_all(dir);
}

/// A fake node with id `id`: it drops connections until `until`, then
/// answers `CLUSTER MYID`, `PING`, and `CLUSTER SETTABLE` with `+OK` (or
/// with an error if `refuse`), sending the arguments of each `SETTABLE` to
/// the receiver. Returns its port and the receiver.
fn fake_node(id: &'static str, until: Instant, refuse: bool) -> (String, std::sync::mpsc::Receiver<Vec<String>>) {
    let fake = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = fake.local_addr().unwrap().port().to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for conn in fake.incoming() {
            let conn = conn.unwrap();
            if Instant::now() < until {
                drop(conn);
                continue;
            }
            let tx = tx.clone();
            thread::spawn(move || {
                let mut r = BufReader::new(conn.try_clone().unwrap());
                let mut w = conn;
                loop {
                    let mut head = String::new();
                    if r.read_line(&mut head).unwrap_or(0) == 0 {
                        return;
                    }
                    let n: usize = head[1..head.len() - 2].parse().unwrap();
                    let mut args = Vec::new();
                    for _ in 0..n {
                        let mut len = String::new();
                        r.read_line(&mut len).unwrap();
                        let mut arg = vec![0; len[1..len.len() - 2].parse::<usize>().unwrap() + 2];
                        r.read_exact(&mut arg).unwrap();
                        args.push(String::from_utf8(arg[..arg.len() - 2].to_vec()).unwrap());
                    }
                    let reply = match args[0].as_str() {
                        "CLUSTER" if args[1] == "MYID" => format!("$40\r\n{id}\r\n"),
                        "CLUSTER" => {
                            let _ = tx.send(args.clone());
                            if refuse { "-ERR not today\r\n" } else { "+OK\r\n" }.to_string()
                        }
                        _ => "+PONG\r\n".to_string(),
                    };
                    if w.write_all(reply.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    (port, rx)
}

/// A node that drops stuart's connections for a while still gets the
/// table once it answers.
#[test]
fn a_node_that_missed_the_table_gets_it_later() {
    let dir = temp_dir("missed");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let (port, rx) = fake_node(id, Instant::now() + Duration::from_secs(2), false);
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    let pushed = rx.recv_timeout(Duration::from_secs(10)).expect("no table pushed");
    assert_eq!(pushed[..5], ["CLUSTER", "SETTABLE", "1", "NODES", "1"]);
    assert_eq!(pushed[5..], [id, "127.0.0.1", &port, "RANGES", "1", "0", "16383", id]);
    wait_until("stuart sees it up with epoch 1", || up_and_acked(&mut s, 1) == 1);
    let _ = std::fs::remove_dir_all(dir);
}

/// A node that refuses the table is reported, and isn't sent the same
/// table again on the same connection (nor every tick).
#[test]
fn a_refused_table_is_reported_and_not_pushed_again() {
    let dir = temp_dir("refused");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let id = "abcabcabcabcabcabcabcabcabcabcabcabcabca";
    let (port, rx) = fake_node(id, Instant::now(), true);
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    rx.recv_timeout(Duration::from_secs(10)).expect("no table pushed");
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(rx.try_iter().count(), 0, "pushed again");
    let nodes = s.text(&["STUART", "NODES"]);
    assert!(nodes.contains(" up ") && nodes.contains(" acked=0 "), "{nodes}");
    assert!(nodes.contains("error=\"refused table epoch 1: ERR not today\""), "{nodes}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A node restarted at the same address joins with a new id: it replaces
/// the old one, whose slots are lost.
#[test]
fn a_new_id_at_a_known_address_replaces_the_old_node() {
    let dir = temp_dir("replace");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let (a, b) = ("a".repeat(40), "b".repeat(40));
    assert_eq!(s.text(&["STUART", "JOIN", &a, "127.0.0.1", "1"]), "+OK");
    assert_eq!(s.text(&["STUART", "JOIN", &a, "127.0.0.1", "1"]), "+OK", "again: nothing changes");
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "1"]), "+OK");
    let nodes = s.text(&["STUART", "NODES"]);
    let lines: Vec<&str> = nodes.lines().collect();
    assert_eq!(lines.len(), 2, "{nodes}");
    assert!(lines[0].starts_with(&format!("{b} 127.0.0.1:1 ")) && lines[0].contains(" slots=0"), "{nodes}");
    assert_eq!(lines[1], "unassigned slots: 16384");
    assert!(s.text(&["STUART", "TABLE"]).starts_with("epoch 2\n"));
    assert_eq!(s.text(&["GET", "foo"]), "-CLUSTERDOWN Hash slot not served");
    // The old node's keeper ends: one per node.
    wait_until("one keeper left", || s.text(&["INFO"]).contains("\r\nstuart_keepers:1\r\n"));

    // STUART ASSIGN gives the lost slots to the new node.
    assert_eq!(s.text(&["STUART", "ASSIGN", "0", "16383", &a]), format!("-ERR Unknown node {a}"));
    assert_eq!(s.text(&["STUART", "ASSIGN", "0", "x", &b]), "-ERR Invalid slot range");
    assert_eq!(s.text(&["STUART", "ASSIGN", "0", "16383", "x"]), "-ERR Invalid node id 'x'");
    assert_eq!(s.text(&["STUART", "ASSIGN", "0", "16383"]), "-ERR wrong number of arguments for 'stuart|assign' command");
    assert_eq!(s.text(&["STUART", "ASSIGN", "0", "16383", &b]), "+OK");
    assert!(s.text(&["STUART", "TABLE"]).starts_with("epoch 3\n"));
    assert_eq!(s.text(&["GET", "foo"]), "-MOVED 12182 127.0.0.1:1");
    assert!(!s.text(&["STUART", "NODES"]).contains("unassigned"));
    assert_eq!(
        s.text(&["STUART", "ASSIGN", "5", "5", &b]),
        format!("-ERR Slot 5 is served by {b}: only unassigned slots can be assigned")
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// A node that joins with the name of another (`NAME`, a pod's name) at
/// another address (its pod's new address) replaces it too; the name is in
/// `STUART NODES` and survives a restart of stuart.
#[test]
fn a_new_id_with_a_known_name_replaces_the_old_node() {
    let dir = temp_dir("replace-name");
    let port = free_port();
    let stuart = Stuart::start(port, &dir.join("state"));
    let mut s = stuart.client();
    let (a, b, c) = ("a".repeat(40), "b".repeat(40), "c".repeat(40));
    assert_eq!(s.text(&["STUART", "JOIN", &a, "127.0.0.1", "1", "NAME", "pod-0"]), "+OK");
    assert_eq!(s.text(&["STUART", "JOIN", &c, "127.0.0.1", "3", "name", "pod-1"]), "+OK");
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    assert!(s.text(&["STUART", "NODES"]).starts_with(&format!("{a} 127.0.0.1:1 ")));
    assert!(s.text(&["STUART", "NODES"]).lines().next().unwrap().contains(" slots=8192 name=pod-0"));

    drop((s, stuart));
    let stuart = Stuart::start(port, &dir.join("state"));
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "2", "NAME", "pod-0"]), "+OK");
    let nodes = s.text(&["STUART", "NODES"]);
    let lines: Vec<&str> = nodes.lines().collect();
    assert_eq!(lines.len(), 3, "{nodes}");
    assert!(lines[0].starts_with(&format!("{c} 127.0.0.1:3 ")) && lines[0].contains(" name=pod-1"), "{nodes}");
    assert!(lines[1].starts_with(&format!("{b} 127.0.0.1:2 ")) && lines[1].contains(" slots=0 name=pod-0"), "{nodes}");
    assert_eq!(lines[2], "unassigned slots: 8192");

    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "2", "NAME", "a b"]), "-ERR Invalid name 'a b'");
    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "2", "NAMES", "x"]), "-ERR syntax error");
    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "2", "NAME"]), "-ERR wrong number of arguments for 'stuart|join' command");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn commands_and_errors() {
    let dir = temp_dir("errors");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let id = "a".repeat(40);
    assert_eq!(s.text(&["PING"]), "+PONG");
    assert_eq!(s.text(&["STUART", "CREATE"]), "-ERR No node has joined yet");
    assert_eq!(s.text(&["STUART", "TABLE"]), "$-1");
    assert_eq!(s.text(&["STUART", "JOIN", "x", "127.0.0.1", "1"]), "-ERR Invalid node id 'x'");
    assert_eq!(s.text(&["STUART", "JOIN", &id.to_uppercase(), "127.0.0.1", "1"]), format!("-ERR Invalid node id '{}'", id.to_uppercase()));
    assert_eq!(s.text(&["STUART", "JOIN", &id, "127.0.0.1", "0"]), "-ERR Invalid port '0'");
    assert_eq!(s.text(&["STUART", "JOIN", &id, "127.0.0.1", "x"]), "-ERR Invalid port 'x'");
    assert_eq!(s.text(&["STUART", "JOIN", &id, "a@b", "1"]), "-ERR Invalid host 'a@b'");
    assert_eq!(s.text(&["STUART", "JOIN", &id]), "-ERR wrong number of arguments for 'stuart|join' command");
    assert_eq!(s.text(&["STUART", "FOO"]), "-ERR unknown subcommand 'FOO'. Try STUART HELP.");
    assert_eq!(s.text(&["STUART"]), "-ERR wrong number of arguments for 'stuart' command");
    assert_eq!(s.text(&["CLUSTER", "MYID"]), "-ERR stuart is not a node and has no node id");
    assert_eq!(s.text(&["CLUSTER", "KEYSLOT", "foo"]), ":12182");
    assert_eq!(s.text(&["GET"]), "-ERR wrong number of arguments for 'get' command");
    assert_eq!(s.text(&["DBSIZE"]), "-ERR unknown command 'DBSIZE', with args beginning with: ");
    assert!(s.text(&["INFO"]).contains("\r\nstuart_version:"));
    assert_eq!(s.call(&["COMMAND", "DOCS"]), b"*0\r\n");
    assert!(s.call(&["HELLO", "3"]).starts_with(b"%7\r\n$6\r\nserver\r\n"));
    assert_eq!(s.call(&["COMMAND", "DOCS"]), b"%0\r\n");
    assert_eq!(s.text(&["CLUSTER", "INFO"]).lines().next(), Some("cluster_state:fail"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_bad_state_file_stops_stuart() {
    let dir = temp_dir("bad");
    let state = dir.join("state");
    std::fs::write(&state, "not a state file\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_speedvault-stuart")).args(["127.0.0.1:0", "--state", state.to_str().unwrap()]).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("the first line isn't"), "{err}");
    let out = Command::new(env!("CARGO_BIN_EXE_speedvault-stuart")).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "--state is required");
    let _ = std::fs::remove_dir_all(dir);
}

/// `--create-when 2`: the table appears when the second node joins, once;
/// a third node joins it without slots, and a restart changes nothing.
#[test]
fn create_when_enough_nodes_joined() {
    let dir = temp_dir("create-when");
    let state = dir.join("state");
    let port = free_port();
    let stuart = Stuart::start_with(port, &state, &["--create-when", "2"]);
    let mut s = stuart.client();
    let (a, b, c) = ("a".repeat(40), "b".repeat(40), "c".repeat(40));
    assert_eq!(s.text(&["STUART", "JOIN", &a, "127.0.0.1", "1"]), "+OK");
    assert_eq!(s.text(&["STUART", "TABLE"]), "$-1", "one node: no table yet");
    assert_eq!(s.text(&["STUART", "JOIN", &b, "127.0.0.1", "2"]), "+OK");
    let table = s.text(&["STUART", "TABLE"]);
    assert!(table.starts_with("epoch 1\n"), "{table}");
    assert!(table.contains(" connected 0-8191\n") && table.contains(" connected 8192-16383\n"), "{table}");
    assert_eq!(s.text(&["STUART", "CREATE"]), "-ERR The slots were assigned already (table epoch 1)");
    assert_eq!(s.text(&["STUART", "JOIN", &c, "127.0.0.1", "3"]), "+OK");
    let table = s.text(&["STUART", "TABLE"]);
    assert!(table.starts_with("epoch 2\n") && table.lines().count() == 4, "{table}");
    assert!(table.lines().any(|l| l.starts_with(&c) && l.ends_with(" connected")), "c has no slots: {table}");

    drop(s);
    drop(stuart);
    let stuart = Stuart::start_with(port, &state, &["--create-when", "2"]);
    assert!(stuart.client().text(&["STUART", "TABLE"]).starts_with("epoch 2\n"), "a restart keeps the table");
    drop(stuart);

    // Nodes joined before a restart with --create-when: created at start.
    let state2 = dir.join("state2");
    let stuart = Stuart::start(free_port(), &state2);
    let mut s = stuart.client();
    for (id, p) in [(&a, "1"), (&b, "2")] {
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", p]), "+OK");
    }
    assert_eq!(s.text(&["STUART", "TABLE"]), "$-1");
    drop(s);
    drop(stuart);
    let stuart = Stuart::start_with(free_port(), &state2, &["--create-when", "2"]);
    assert!(stuart.client().text(&["STUART", "TABLE"]).starts_with("epoch 1\n"));
    let _ = std::fs::remove_dir_all(dir);
}

/// A cluster client: sends each request to the node that owns the key's
/// slot as far as it knows, following `-MOVED` (it learns) and `-ASK` (it
/// doesn't, and sends `ASKING` first) as redis-py does, and retrying
/// `-TRYAGAIN`.
struct Follower {
    conns: std::collections::HashMap<String, Client>,
    /// Slot to "host:port", learned from MOVED.
    slots: std::collections::HashMap<u16, String>,
    seed: String,
    redirects: usize,
    /// Each new connection logs in with it first.
    password: Option<&'static str>,
}

impl Follower {
    fn new(seed: SocketAddr) -> Follower {
        Follower { conns: Default::default(), slots: Default::default(), seed: seed.to_string(), redirects: 0, password: None }
    }

    fn conn(&mut self, addr: &str) -> &mut Client {
        let password = self.password;
        self.conns.entry(addr.to_string()).or_insert_with(|| {
            let mut c = Client::connect(addr.parse().unwrap());
            if let Some(password) = password {
                assert_eq!(c.text(&["AUTH", password]), "+OK");
            }
            c
        })
    }

    fn text(&mut self, args: &[&str]) -> String {
        let slot = cluster::key_hash_slot(args[1].as_bytes());
        let mut addr = self.slots.get(&slot).cloned().unwrap_or(self.seed.clone());
        let mut ask = false;
        for _ in 0..50 {
            let c = self.conn(&addr);
            if ask {
                assert_eq!(c.text(&["ASKING"]), "+OK");
            }
            let reply = c.text(args);
            if let Some(to) = reply.strip_prefix("-MOVED ") {
                addr = to.split(' ').nth(1).unwrap().to_string();
                self.slots.insert(slot, addr.clone());
                (ask, self.redirects) = (false, self.redirects + 1);
            } else if let Some(to) = reply.strip_prefix("-ASK ") {
                addr = to.split(' ').nth(1).unwrap().to_string();
                (ask, self.redirects) = (true, self.redirects + 1);
            } else if reply.starts_with("-TRYAGAIN") {
                addr = self.slots.get(&slot).cloned().unwrap_or(self.seed.clone());
                ask = false;
                thread::sleep(Duration::from_millis(1));
            } else {
                return reply;
            }
        }
        panic!("{args:?}: redirected 50 times");
    }
}

/// `n` keys whose slots are in `slots`, a few per slot.
fn keys_in(slots: std::ops::RangeInclusive<u16>, n: usize) -> Vec<String> {
    (0..).map(|i| format!("k{i}")).filter(|k| slots.contains(&cluster::key_hash_slot(k.as_bytes()))).take(n).collect()
}

/// The id a node answers `CLUSTER MYID` with.
fn id_of(addr: SocketAddr) -> String {
    Client::connect(addr).text(&["CLUSTER", "MYID"])
}

/// Two nodes with the slots, a third joining later; then STUART MOVE gives
/// it slots 0-99, keys and all, while a client goes on writing them.
#[test]
fn slots_move_with_their_keys() {
    let dir = temp_dir("move");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "MOVE", "0", "9", &"a".repeat(40)]), "-ERR There is no table yet: STUART CREATE makes it");
    let mut nodes = Vec::new();
    for i in 1..=2 {
        nodes.push(node(stuart.1));
        wait_until("the node joined and is up", || up_and_acked(&mut s, 0) == i);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("both took the table", || up_and_acked(&mut s, 1) == 2);
    let keys = keys_in(0..=99, 300);
    let mut f = Follower::new(stuart.1);
    for k in &keys {
        assert_eq!(f.text(&["SET", k, "0"]), "+OK");
    }
    nodes.push(node(stuart.1));
    wait_until("the third node is up", || up_and_acked(&mut s, 2) == 3);
    let third = id_of(nodes[2]);

    // A writer on the side: INCR on every key, round and round.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (keys, stop, seed) = (keys.clone(), stop.clone(), stuart.1);
        thread::spawn(move || {
            let mut f = Follower::new(seed);
            let mut counts = vec![0u64; keys.len()];
            let mut i = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let k = i % keys.len();
                counts[k] += 1;
                assert_eq!(f.text(&["INCR", &keys[k]]), format!(":{}", counts[k]), "key {}", keys[k]);
                i += 1;
            }
            (counts, f.redirects)
        })
    };
    thread::sleep(Duration::from_millis(100));
    assert_eq!(s.text(&["STUART", "MOVE", "0", "99", &third]), ":100");
    assert_eq!(s.text(&["STUART", "MOVE", "50", "50", &third]), "-ERR Slot 50 is moving already");
    let migrations = s.text(&["STUART", "MIGRATIONS"]);
    assert!(migrations.contains(" -> ") || migrations.contains("queued: "), "{migrations}");
    wait_until("the slots moved", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let table = s.text(&["STUART", "TABLE"]);
    assert!(table.lines().any(|l| l.starts_with(&third) && l.ends_with(" connected 0-99")), "{table}");
    thread::sleep(Duration::from_millis(100));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (counts, redirects) = writer.join().unwrap();
    assert!(redirects >= 100, "the writer followed {redirects} redirects");

    // Every write is at the third node, and nothing of 0-99 is left behind.
    let mut c = Client::connect(nodes[2]);
    for (k, n) in keys.iter().zip(&counts) {
        assert_eq!(c.text(&["GET", k]), n.to_string(), "{k}");
    }
    let mut first = Client::connect(nodes[0]);
    for slot in 0..100 {
        assert_eq!(first.text(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()]), ":0");
    }
    let slot0 = cluster::key_hash_slot(keys[0].as_bytes());
    assert_eq!(first.text(&["GET", &keys[0]]), format!("-MOVED {slot0} 127.0.0.1:{}", nodes[2].port()));
    let listed = s.text(&["STUART", "NODES"]);
    assert!(listed.lines().any(|l| l.starts_with(&third) && l.contains(" slots=100")), "{listed}");
    // The third node's own slots don't move.
    assert_eq!(s.text(&["STUART", "MOVE", "0", "99", &third]), ":0");
    assert_eq!(s.text(&["STUART", "MOVE", "0", "9", "x"]), "-ERR Invalid node id 'x'");
    assert_eq!(s.text(&["STUART", "MOVE", "9", "x", &third]), "-ERR Invalid slot range");
    assert_eq!(s.text(&["STUART", "MOVE", "0", "9"]), "-ERR wrong number of arguments for 'stuart|move' command");
    let _ = std::fs::remove_dir_all(dir);
}

/// stuart killed while slots move: the nodes carry on with the table they
/// have, and the restarted stuart finishes the moves from its state file.
#[test]
fn moves_survive_a_stuart_restart() {
    let dir = temp_dir("move-restart");
    let (port, state) = (free_port(), dir.join("state"));
    // A few slots at a time, so that some still wait when it's killed.
    let stuart = Stuart::start_with(port, &state, &["--moves-per-source", "8"]);
    let mut s = stuart.client();
    let mut nodes = Vec::new();
    for i in 1..=2 {
        nodes.push(node(stuart.1));
        wait_until("the node joined and is up", || up_and_acked(&mut s, 0) == i);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("both took the table", || up_and_acked(&mut s, 1) == 2);
    // Slots 0-399 to the second node, about 2000 keys.
    let keys = keys_in(0..=399, 2000);
    let mut f = Follower::new(stuart.1);
    for k in &keys {
        assert_eq!(f.text(&["SET", k, k]), "+OK");
    }
    let second = id_of(nodes[1]);
    assert_eq!(s.text(&["STUART", "MOVE", "0", "399", &second]), ":400");
    wait_until("some moved", || {
        let t = s.text(&["STUART", "TABLE"]);
        t.lines().any(|l| l.starts_with(&second) && l.contains(" 0-"))
    });
    drop(s);
    drop(stuart);
    let text = std::fs::read_to_string(&state).unwrap();
    assert!(text.contains("\nqueue "), "killed before the end: {text}");
    // The nodes serve meanwhile (through redirects).
    let mut f = Follower::new(nodes[0]);
    for k in keys.iter().step_by(37) {
        assert_eq!(f.text(&["GET", k]), *k);
    }
    let stuart = Stuart::start(port, &state);
    let mut s = stuart.client();
    wait_until("the slots moved", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let table = s.text(&["STUART", "TABLE"]);
    assert!(table.lines().any(|l| l.starts_with(&second) && l.ends_with(" connected 0-399 8192-16383")), "{table}");
    let mut c = Client::connect(nodes[1]);
    for k in &keys {
        assert_eq!(c.text(&["GET", k]), *k);
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// A table a fake node got: its epoch, its moves (as slot, from, to: the
/// MIGRATING section, then SYNCING), when it arrived and when the node
/// acknowledged it.
#[derive(Clone, Debug)]
struct Pushed {
    epoch: u64,
    migrating: Vec<(String, String, String)>,
    got: Instant,
    acked: Instant,
    /// Whether the table gives the node slots, and makes it a replica.
    holds: bool,
    replica: bool,
}

/// A fake node that answers like a dbnode for stuart: `CLUSTER MYID`,
/// `PING`, `CLUSTER SETTABLE`, acknowledging a table `delay(epoch)` after
/// it arrived, and for its moves `CLUSTER SLOTSYNCS` (each source of the last table's moves to
/// it streaming, at offset 100) and `CLUSTER PAUSESLOTS` (`:100`).
/// Every table goes to `log`.
fn slow_node(id: &'static str, delay: fn(u64) -> Duration, log: std::sync::Arc<std::sync::Mutex<Vec<Pushed>>>) -> String {
    slow_node_down(id, delay, log, Default::default())
}

/// The same, down (closing every connection, answering nothing) while
/// `down` is set.
fn slow_node_down(id: &'static str, delay: fn(u64) -> Duration, log: Log, down: std::sync::Arc<std::sync::atomic::AtomicBool>) -> String {
    memory_node(id, delay, log, down, Default::default())
}

/// What a fake node answers to `INFO memory`; empty: `+PONG`, as to
/// anything it doesn't know.
type Mem = std::sync::Arc<std::sync::Mutex<String>>;

/// The same, answering `INFO memory` with `memory`.
fn memory_node(id: &'static str, delay: fn(u64) -> Duration, log: Log, down: std::sync::Arc<std::sync::atomic::AtomicBool>, memory: Mem) -> String {
    lagging_node(id, delay, log, down, memory, Default::default())
}

/// How far a fake node is, and what it was asked: its `CLUSTER SLOTSYNCS`
/// offsets, and its offset in a reply to `CLUSTER FENCE` (a replica's),
/// are 0 while `behind` is set (else 100), so it never reaches a paused
/// node's offset (100); its `CLUSTER FENCE` says its copy isn't complete
/// (still copying) while `copying` is set, or once it answered more than
/// `complete_fences` of them if that's not 0 (`fences` counts them); its
/// `CLUSTER PAUSEWRITES` (a primary's, `:100`) that pause, and those that
/// lift a pause, each also noted in `paused` (when, and whether it
/// paused). `down` as in [`lagging_node`] (with [`handing_cluster`]).
/// From epoch `refuse_from` on (0: never) it refuses each table (`-ERR`),
/// not logged. Its next `behind_pauses` `CLUSTER PAUSESLOTS` (a source's)
/// answer `-BEHIND`, pausing nothing.
#[derive(Clone, Default)]
struct Lag {
    refuse_from: std::sync::Arc<std::sync::atomic::AtomicU64>,
    behind_pauses: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    behind: std::sync::Arc<std::sync::atomic::AtomicBool>,
    copying: std::sync::Arc<std::sync::atomic::AtomicBool>,
    complete_fences: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    fences: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pauses: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    resumes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    paused: std::sync::Arc<std::sync::Mutex<Vec<(Instant, bool)>>>,
    down: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Lag {
    fn behind() -> Lag {
        let lag = Lag::default();
        lag.behind.store(true, std::sync::atomic::Ordering::SeqCst);
        lag
    }

    fn count(n: &std::sync::atomic::AtomicUsize) -> usize {
        n.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The same, as `lag` says.
fn lagging_node(id: &'static str, delay: fn(u64) -> Duration, log: Log, down: std::sync::Arc<std::sync::atomic::AtomicBool>, memory: Mem, lag: Lag) -> String {
    let fake = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = fake.local_addr().unwrap().port().to_string();
    let is_down = move || down.load(std::sync::atomic::Ordering::SeqCst);
    // The last table's moves by snapshot to this node, as (slot, from).
    let syncs: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>> = Default::default();
    thread::spawn(move || {
        for conn in fake.incoming() {
            let conn = conn.unwrap();
            if is_down() {
                continue;
            }
            let log = log.clone();
            let is_down = is_down.clone();
            let memory = memory.clone();
            let syncs = syncs.clone();
            let lag = lag.clone();
            thread::spawn(move || {
                let mut r = BufReader::new(conn.try_clone().unwrap());
                let mut w = conn;
                loop {
                    let mut head = String::new();
                    if r.read_line(&mut head).unwrap_or(0) == 0 {
                        return;
                    }
                    let n: usize = head[1..head.len() - 2].parse().unwrap();
                    let mut args = Vec::new();
                    for _ in 0..n {
                        let mut len = String::new();
                        r.read_line(&mut len).unwrap();
                        let mut arg = vec![0; len[1..len.len() - 2].parse::<usize>().unwrap() + 2];
                        r.read_exact(&mut arg).unwrap();
                        args.push(String::from_utf8(arg[..arg.len() - 2].to_vec()).unwrap());
                    }
                    let reply = match (args[0].as_str(), args.get(1).map(String::as_str)) {
                        ("CLUSTER", Some("MYID")) => format!("$40\r\n{id}\r\n"),
                        ("CLUSTER", Some("SETTABLE")) => {
                            let got = Instant::now();
                            let epoch: u64 = args[2].parse().unwrap();
                            thread::sleep(delay(epoch));
                            let refuse_from = lag.refuse_from.load(std::sync::atomic::Ordering::SeqCst);
                            if refuse_from > 0 && epoch >= refuse_from {
                                if is_down() || w.write_all(b"-ERR refused\r\n").is_err() {
                                    return;
                                }
                                continue;
                            }
                            let section = |word: &str| -> Vec<(String, String, String)> {
                                let Some(i) = args.iter().position(|a| a == word) else { return Vec::new() };
                                let n: usize = args[i + 1].parse().unwrap();
                                args[i + 2..i + 2 + 3 * n].chunks(3).map(|c| (c[0].clone(), c[1].clone(), c[2].clone())).collect()
                            };
                            let mut migrating = section("MIGRATING");
                            let syncing = section("SYNCING");
                            *syncs.lock().unwrap() = syncing.iter().filter(|m| m.2 == id).map(|m| (m.0.clone(), m.1.clone())).collect();
                            migrating.extend(syncing);
                            let holds = section("RANGES").iter().any(|r| r.2 == id);
                            let replica = args.iter().position(|a| a == "REPLICAS").is_some_and(|i| {
                                let n: usize = args[i + 1].parse().unwrap();
                                args[i + 2..i + 2 + 2 * n].chunks(2).any(|c| c[0] == id)
                            });
                            log.lock().unwrap().push(Pushed { epoch, migrating, got, acked: Instant::now(), holds, replica });
                            "+OK\r\n".to_string()
                        }
                        ("CLUSTER", Some("SLOTSYNCS")) => {
                            let syncs = syncs.lock().unwrap();
                            let mut sources: Vec<&String> = syncs.iter().map(|(_, from)| from).collect();
                            sources.sort();
                            sources.dedup();
                            let text: String = sources
                                .iter()
                                .map(|from| {
                                    let slots: Vec<&str> = syncs.iter().filter(|(_, f)| f == *from).map(|(slot, _)| slot.as_str()).collect();
                                    let offset = if lag.behind.load(std::sync::atomic::Ordering::SeqCst) { 0 } else { 100 };
                                    format!("{from} streaming {} 0 {offset}\n", slots.join(","))
                                })
                                .collect();
                            format!("${}\r\n{text}\r\n", text.len())
                        }
                        ("CLUSTER", Some("PAUSESLOTS")) => {
                            let behind = lag.behind_pauses.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |n| n.checked_sub(1));
                            if behind.is_ok() { "-BEHIND 100000\r\n" } else { ":100\r\n" }.to_string()
                        }
                        ("CLUSTER", Some("PAUSEWRITES")) if args[2] == "0" => {
                            lag.resumes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            lag.paused.lock().unwrap().push((Instant::now(), false));
                            "+OK\r\n".to_string()
                        }
                        ("CLUSTER", Some("PAUSEWRITES")) => {
                            lag.pauses.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            lag.paused.lock().unwrap().push((Instant::now(), true));
                            ":100\r\n".to_string()
                        }
                        ("CLUSTER", Some("FENCE")) => {
                            let offset = if lag.behind.load(std::sync::atomic::Ordering::SeqCst) { 0 } else { 100 };
                            let n = lag.fences.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                            let limit = Lag::count(&lag.complete_fences);
                            let complete = !lag.copying.load(std::sync::atomic::Ordering::SeqCst) && (limit == 0 || n <= limit);
                            format!("*2\r\n:{offset}\r\n:{}\r\n", u8::from(complete))
                        }
                        ("INFO", _) if !memory.lock().unwrap().is_empty() => {
                            let text = memory.lock().unwrap().clone();
                            format!("${}\r\n{text}\r\n", text.len())
                        }
                        _ => "+PONG\r\n".to_string(),
                    };
                    if is_down() || w.write_all(reply.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

type Log = std::sync::Arc<std::sync::Mutex<Vec<Pushed>>>;

/// The table with `epoch` as `log` has it.
fn got(log: &Log, epoch: u64) -> Option<Pushed> {
    log.lock().unwrap().iter().find(|p| p.epoch == epoch).cloned()
}

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccc";

/// Fake nodes A, B (and C) joined, the table made: A 0-8191, B the rest
/// (or thirds with C). Their logs.
fn slow_cluster(s: &mut Client, delays: &[fn(u64) -> Duration]) -> Vec<Log> {
    let mut logs = Vec::new();
    for (id, delay) in [A, B, C].into_iter().zip(delays) {
        let log: Log = Default::default();
        let port = slow_node(id, *delay, log.clone());
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
        logs.push(log);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(s, 1) == delays.len());
    logs
}

/// The table starting a move reaches the source first (it accepts the
/// target's copy), the one ending it the target first: the others get each
/// only once that node took it, slow as it is (here half a second per
/// table).
#[test]
fn each_table_reaches_its_first_node_first() {
    let dir = temp_dir("first");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    // Both A and B slow, so that the order shows.
    let slow: fn(u64) -> Duration = |e| if e > 1 { Duration::from_millis(500) } else { Duration::ZERO };
    let logs = slow_cluster(&mut s, &[slow, slow, |_| Duration::ZERO]);
    // 0-5460 are A's: slot 5 moves to B.
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    wait_until("the move is done", || s.text(&["STUART", "MIGRATIONS"]).is_empty() && got(&logs[0], 3).is_some() && got(&logs[2], 3).is_some());
    for (epoch, first) in [(2, 0), (3, 1)] {
        let at_first = got(&logs[first], epoch).unwrap();
        for (other, log) in [("A", &logs[0]), ("B", &logs[1]), ("C", &logs[2])].into_iter().enumerate().filter(|(i, _)| *i != first).map(|(_, o)| o) {
            let p = got(log, epoch).unwrap_or_else(|| panic!("{other} never got epoch {epoch}"));
            assert!(p.got >= at_first.acked, "{other} got epoch {epoch} before node {first} took it");
        }
    }
    assert_eq!(got(&logs[1], 2).unwrap().migrating, [("5".to_string(), A.to_string(), B.to_string())]);
    assert!(got(&logs[1], 3).unwrap().migrating.is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

/// A move ends only once the source has the table that starts it, even if
/// it says nothing is pending before that (it might still take new keys).
#[test]
fn a_move_ends_only_once_the_source_has_it() {
    let dir = temp_dir("source-ack");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let logs = slow_cluster(&mut s, &[|e| if e == 2 { Duration::from_millis(700) } else { Duration::ZERO }, |_| Duration::ZERO]);
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    wait_until("the move is done", || got(&logs[0], 3).is_some());
    let (a2, b3) = (got(&logs[0], 2).unwrap(), got(&logs[1], 3).unwrap());
    assert!(b3.got >= a2.acked, "the move ended before the source took the table that starts it");
    let _ = std::fs::remove_dir_all(dir);
}

/// A node is the source or the target of moves, not both at once: a move
/// out of a node that receives one waits.
#[test]
fn a_node_is_a_source_or_a_target() {
    let dir = temp_dir("one-role");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let slow: fn(u64) -> Duration = |_| Duration::from_millis(20);
    let logs = slow_cluster(&mut s, &[slow, slow, slow]);
    // 5 (A's) to B, then 6000 (B's) to C, and 12000 (C's) to A.
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    assert_eq!(s.text(&["STUART", "MOVE", "6000", "6000", C]), ":1");
    assert_eq!(s.text(&["STUART", "MOVE", "12000", "12000", A]), ":1");
    wait_until("the moves are done", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    thread::sleep(Duration::from_millis(200));
    let log = logs[0].lock().unwrap().clone();
    for p in &log {
        let sources: Vec<&String> = p.migrating.iter().map(|m| &m.1).collect();
        let targets: Vec<&String> = p.migrating.iter().map(|m| &m.2).collect();
        assert!(sources.iter().all(|s| !targets.contains(s)), "epoch {}: {:?}", p.epoch, p.migrating);
    }
    let moved: usize = log.iter().map(|p| p.migrating.len()).sum();
    assert!(moved >= 3, "{log:?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// `slots=N` of each `STUART NODES` line, in order.
fn slot_counts(s: &mut Client) -> Vec<usize> {
    s.text(&["STUART", "NODES"])
        .lines()
        .filter_map(|l| l.split(' ').find_map(|w| w.strip_prefix("slots=")))
        .map(|n| n.parse().unwrap())
        .collect()
}

/// A fourth node joins three: STUART REBALANCE gives it a quarter of the
/// slots; then STUART DRAIN empties the first node. A client writes all
/// along, and every write is found at the end.
#[test]
fn rebalance_then_drain() {
    let dir = temp_dir("rebalance");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "REBALANCE"]), "-ERR There is no table yet: STUART CREATE makes it");
    let mut nodes = Vec::new();
    for i in 1..=3 {
        nodes.push(node(stuart.1));
        wait_until("the node joined and is up", || up_and_acked(&mut s, 0) == i);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 3);
    assert_eq!(s.text(&["STUART", "REBALANCE"]), ":0", "even already");
    let keys: Vec<String> = (0..2000).map(|i| format!("r{i}")).collect();
    let mut f = Follower::new(stuart.1);
    for k in &keys {
        assert_eq!(f.text(&["SET", k, "0"]), "+OK");
    }
    nodes.push(node(stuart.1));
    wait_until("the fourth node is up", || up_and_acked(&mut s, 2) == 4);

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (keys, stop, seed) = (keys.clone(), stop.clone(), stuart.1);
        thread::spawn(move || {
            let mut f = Follower::new(seed);
            let mut counts = vec![0u64; keys.len()];
            let mut i = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let k = (i * 7) % keys.len();
                counts[k] += 1;
                assert_eq!(f.text(&["INCR", &keys[k]]), format!(":{}", counts[k]), "key {}", keys[k]);
                i += 1;
            }
            counts
        })
    };
    assert_eq!(s.text(&["STUART", "REBALANCE"]), ":4096");
    // Planned again while they move: what is still queued.
    let again: usize = s.text(&["STUART", "REBALANCE"])[1..].parse().unwrap();
    assert!(again <= 4096, "{again}");
    let first = id_of(nodes[0]);
    let started = Instant::now();
    wait_until_long("rebalanced", Duration::from_secs(60), || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let rebalanced = started.elapsed();
    assert_eq!(slot_counts(&mut s), [4096, 4096, 4096, 4096]);

    assert_eq!(s.text(&["STUART", "DRAIN", &first]), ":4096");
    wait_until_long("drained", Duration::from_secs(60), || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let counts = slot_counts(&mut s);
    assert_eq!(counts[0], 0);
    assert_eq!(counts.iter().sum::<usize>(), 16384);
    assert!(counts[1..].iter().all(|&n| (5461..=5462).contains(&n)), "{counts:?}");
    thread::sleep(Duration::from_millis(100));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let written = writer.join().unwrap();

    // The drained node trims its keys once it took the last table.
    let mut drained = Client::connect(nodes[0]);
    wait_until("the drained node has no keys", || drained.text(&["DBSIZE"]) == ":0");
    let mut f = Follower::new(nodes[1]);
    for (k, n) in keys.iter().zip(&written) {
        assert_eq!(f.text(&["GET", k]), n.to_string(), "{k}");
    }
    assert_eq!(s.text(&["STUART", "DRAIN", "x"]), "-ERR Invalid node id 'x'");
    assert_eq!(s.text(&["STUART", "LEAVE", "x"]), "-ERR Invalid node id 'x'");
    assert_eq!(s.text(&["STUART", "LEAVE", &"9".repeat(40)]), ":0", "unknown: gone");
    assert_eq!(s.text(&["STUART", "FORGET", &"9".repeat(40)]), format!("-ERR Unknown node {}", "9".repeat(40)));
    assert_eq!(s.text(&["STUART", "FORGET"]), "-ERR wrong number of arguments for 'stuart|forget' command");
    // The drained node is forgotten: it held nothing, nothing is lost.
    assert_eq!(s.text(&["STUART", "FORGET", &first]), ":0");
    assert_eq!(slot_counts(&mut s).len(), 3);
    assert_eq!(s.text(&["STUART", "DRAIN", &"9".repeat(40)]), format!("-ERR Unknown node {}", "9".repeat(40)));
    eprintln!("4096 slots rebalanced in {rebalanced:?}");
    let _ = std::fs::remove_dir_all(dir);
}

fn wait_until_long(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

/// With `--auto` nobody types a STUART command but LEAVE: the first node to
/// join gets every slot, the next two get their share, then the first
/// leaves and is removed. A client writes all along, and every write is
/// found at the end.
#[test]
fn auto_grows_and_shrinks() {
    let dir = temp_dir("auto");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto"]);
    let mut s = stuart.client();
    assert!(s.text(&["INFO"]).contains("\r\nstuart_auto:1\r\n"));
    let mut nodes = vec![node(stuart.1)];
    wait_until("the first node has every slot", || slot_counts(&mut s) == [16384] && up_and_acked(&mut s, 1) == 1);
    let keys: Vec<String> = (0..2000).map(|i| format!("a{i}")).collect();
    let mut f = Follower::new(stuart.1);
    for k in &keys {
        assert_eq!(f.text(&["SET", k, "0"]), "+OK");
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (keys, stop, seed) = (keys.clone(), stop.clone(), stuart.1);
        thread::spawn(move || {
            let mut f = Follower::new(seed);
            let mut counts = vec![0u64; keys.len()];
            let mut i = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let k = (i * 7) % keys.len();
                counts[k] += 1;
                assert_eq!(f.text(&["INCR", &keys[k]]), format!(":{}", counts[k]), "key {}", keys[k]);
                i += 1;
            }
            counts
        })
    };
    nodes.push(node(stuart.1));
    nodes.push(node(stuart.1));
    wait_until_long("three nodes, even", Duration::from_secs(60), || {
        let mut counts = slot_counts(&mut s);
        counts.sort();
        counts == [5461, 5461, 5462] && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });

    let first = id_of(nodes[0]);
    let left: usize = s.text(&["STUART", "LEAVE", &first])[1..].parse().unwrap();
    assert!(left > 5000, "{left}");
    assert!(s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(&first) && l.contains(" leaving")));
    wait_until_long("the first node left", Duration::from_secs(60), || s.text(&["STUART", "LEAVE", &first]) == ":0");
    assert_eq!(slot_counts(&mut s), [8192, 8192]);
    assert!(!s.text(&["STUART", "TABLE"]).contains(&first), "removed from the table");
    thread::sleep(Duration::from_millis(100));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let written = writer.join().unwrap();
    let mut c = Client::connect(nodes[0]);
    assert_eq!(c.text(&["DBSIZE"]), ":0", "the node that left has no keys");
    // It owns nothing in the last table it took: it redirects.
    assert!(c.text(&["GET", &keys[0]]).starts_with("-MOVED "));
    let mut f = Follower::new(nodes[1]);
    for (k, n) in keys.iter().zip(&written) {
        assert_eq!(f.text(&["GET", k]), n.to_string(), "{k}");
    }
    assert_eq!(s.text(&["INFO"]).lines().find(|l| l.starts_with("stuart_nodes:")), Some("stuart_nodes:2"));
    let _ = std::fs::remove_dir_all(dir);
}

/// By snapshot, the moves to a target that goes down are called off and
/// queued again (the source serves the slots all along), and the other
/// moves out of the same source go on; back, it gets them.
#[test]
fn a_target_down_has_its_moves_by_snapshot_called_off() {
    let dir = temp_dir("target-down-sync");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--moves-per-source", "2"]);
    let mut s = stuart.client();
    let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let a_log: Log = Default::default();
    for (id, delay) in [(A, (|_| Duration::ZERO) as fn(u64) -> Duration), (B, |_| Duration::ZERO), (C, |e| if e >= 2 { Duration::from_secs(1) } else { Duration::ZERO })] {
        let log = if id == A { a_log.clone() } else { Default::default() };
        let port = slow_node_down(id, delay, log, if id == C { down.clone() } else { Default::default() });
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 3);
    // Two of A's slots to C, which goes down while it takes that table.
    assert_eq!(s.text(&["STUART", "MOVE", "0", "1", C]), ":2");
    thread::sleep(Duration::from_millis(300));
    down.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(a_log.lock().unwrap().iter().any(|p| p.migrating.iter().any(|m| m.2 == C)), "the moves to C started");
    wait_until("C is down", || s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(C) && l.contains(" down ")));
    assert_eq!(s.text(&["STUART", "MOVE", "2", "5", B]), ":4");
    wait_until_long("A's slots reached B", Duration::from_secs(30), || {
        let t = s.text(&["STUART", "TABLE"]);
        t.lines().any(|l| l.starts_with(B) && l.contains(" 2-5 "))
    });
    let m = s.text(&["STUART", "MIGRATIONS"]);
    assert_eq!(m.lines().filter(|l| l.contains(&format!("-> {C}"))).count(), 0, "{m}");
    assert!(m.contains("queued: 2"), "{m}");
    // C comes back: its moves start and end.
    down.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until_long("C's moves ended", Duration::from_secs(20), || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let t = s.text(&["STUART", "TABLE"]);
    assert!(t.lines().any(|l| l.starts_with(C) && l.contains(" 0-1 ")), "{t}");
    let _ = std::fs::remove_dir_all(dir);
}

/// By snapshot, the cutover waits for the target to reach the offset the
/// paused source answered: a target behind gets no table giving it the
/// slot, and the pauses it misses are tried again (up to five before the
/// move is called off); once it catches up, the slot is its.
#[test]
fn the_cutover_waits_for_the_target_to_catch_up() {
    let dir = temp_dir("catch-up");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let behind = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    for id in [A, B] {
        let b = if id == B { behind.clone() } else { Default::default() };
        let port = lagging_node(id, |_| Duration::ZERO, Default::default(), Default::default(), Default::default(), Lag { behind: b, ..Lag::default() });
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 2);
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    // Two pauses missed (a second each), the slot still A's.
    thread::sleep(Duration::from_millis(2500));
    let m = s.text(&["STUART", "MIGRATIONS"]);
    assert!(m.starts_with(&format!("5 {A} -> {B}")), "{m}");
    behind.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until("the slot moved", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let t = s.text(&["STUART", "TABLE"]);
    assert!(t.lines().any(|l| l.starts_with(B) && l.contains(" 5 ")), "{t}");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(!log.contains("called off"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A source answering `-BEHIND` (its target too far behind to pause for)
/// paused nothing: that isn't a pause missed, however many times; the move
/// goes on, not called off, once the target is close enough.
#[test]
fn a_source_s_behind_is_not_a_missed_pause() {
    let dir = temp_dir("behind");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let a = Lag::default();
    a.behind_pauses.store(7, std::sync::atomic::Ordering::SeqCst);
    for (id, lag) in [(A, a.clone()), (B, Lag::default())] {
        let port = lagging_node(id, |_| Duration::ZERO, Default::default(), Default::default(), Default::default(), lag);
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 2);
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    wait_until("the slot moved", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    assert_eq!(Lag::count(&a.behind_pauses), 0, "A answered -BEHIND each time");
    let t = s.text(&["STUART", "TABLE"]);
    assert!(t.lines().any(|l| l.starts_with(B) && l.contains(" 5 ")), "{t}");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(!log.contains("called off"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A, B and C (fake nodes), slot 5 moved from A to B; B takes the tables
/// from epoch 3 (the cutover) on as `delay`, `down` and `lag` say. Returns
/// once the cutover table is made, B maybe yet to take it.
fn cut_over_to_b(name: &str, delay: fn(u64) -> Duration, down: std::sync::Arc<std::sync::atomic::AtomicBool>, lag: Lag) -> (Stuart, PathBuf, Vec<Log>) {
    let dir = temp_dir(name);
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let mut logs = Vec::new();
    for id in [A, B, C] {
        let log: Log = Default::default();
        let port = if id == B {
            lagging_node(id, delay, log.clone(), down.clone(), Default::default(), lag.clone())
        } else {
            lagging_node(id, |_| Duration::ZERO, log.clone(), Default::default(), Default::default(), Default::default())
        };
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
        logs.push(log);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 3);
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    wait_until("the cutover table", || epoch_of(&mut s) >= 3 && s.text(&["STUART", "MIGRATIONS"]).is_empty());
    (stuart, dir, logs)
}

/// The table after a cutover still waits for its target: here the source
/// starts its next move (a table that goes to the source first) while the
/// target is slow to take the cutover. Were the source to take it, it
/// would send clients to the target, and the target back to it. (The next
/// move is asked for once the cutover table is made: moves the mover can
/// start then start in the cutover table itself.)
#[test]
fn the_next_table_waits_for_the_cutover_s_target() {
    let delay: fn(u64) -> Duration = |e| if e >= 3 { Duration::from_millis(500) } else { Duration::ZERO };
    let (stuart, dir, logs) = cut_over_to_b("cutover-first", delay, Default::default(), Default::default());
    let mut s = stuart.client();
    // Slot 6 (A's too) to C: a table that goes to A first.
    assert_eq!(s.text(&["STUART", "MOVE", "6", "6", C]), ":1");
    wait_until("the moves are done", || epoch_of(&mut s) >= 5 && s.text(&["STUART", "MIGRATIONS"]).is_empty());
    wait_until("A has the last table", || {
        let last = epoch_of(&mut s);
        got(&logs[0], last).is_some()
    });
    let moving = |p: &Pushed| p.migrating.iter().any(|m| m.0 == "5");
    let started = logs[0].lock().unwrap().iter().find(|p| moving(p)).map(|p| p.epoch).expect("A got the move of slot 5");
    let b = logs[1].lock().unwrap().clone();
    for p in logs[0].lock().unwrap().iter().filter(|p| p.epoch > started && !moving(p)) {
        assert!(
            b.iter().any(|q| q.epoch > started && !moving(q) && q.acked <= p.got),
            "A got epoch {} (slot 5 B's) before B took a table giving it slot 5",
            p.epoch
        );
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// `f`'s value once it has one, waiting up to 10 s.
fn wait_until_value<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// When `log`'s node got the table with `epoch`, waiting up to `within`.
fn got_within(log: &Log, epoch: u64, within: Duration) -> Option<Instant> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(p) = got(log, epoch) {
            return Some(p.got);
        }
        thread::sleep(Duration::from_millis(5));
    }
    None
}

/// A target that refuses the cutover table (up, but it never takes it) is
/// waited for 2 s at most (`FIRST_WAIT`): the next table then goes on.
#[test]
fn an_owed_table_is_waited_for_two_seconds_at_most() {
    let lag = Lag::default();
    lag.refuse_from.store(3, std::sync::atomic::Ordering::SeqCst);
    let (stuart, dir, logs) = cut_over_to_b("owed-lapse", |_| Duration::ZERO, Default::default(), lag);
    let mut s = stuart.client();
    let cut = epoch_of(&mut s);
    let asked = Instant::now();
    assert_eq!(s.text(&["STUART", "MOVE", "6", "6", C]), ":1");
    let at = got_within(&logs[0], cut + 1, Duration::from_secs(10)).expect("A got the next table within 10 s");
    let waited = at - asked;
    assert!(waited >= Duration::from_millis(1500), "A got epoch {} {waited:?} after it was made: B owed epoch {cut}", cut + 1);
    assert!(waited <= Duration::from_millis(4000), "A got epoch {} only {waited:?} after it was made", cut + 1);
    let _ = std::fs::remove_dir_all(dir);
}

/// A target down before it took the cutover table isn't waited for: the
/// next table goes on at once.
#[test]
fn an_owed_table_of_a_node_down_is_not_waited_for() {
    let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // B takes 300 ms to answer the cutover table, and by then it's down:
    // it closes the connection without an answer.
    let delay: fn(u64) -> Duration = |e| if e >= 3 { Duration::from_millis(300) } else { Duration::ZERO };
    let (stuart, dir, logs) = cut_over_to_b("owed-down", delay, down.clone(), Lag::default());
    down.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut s = stuart.client();
    let cut = epoch_of(&mut s);
    let asked = Instant::now();
    assert_eq!(s.text(&["STUART", "MOVE", "6", "6", C]), ":1");
    let at = got_within(&logs[0], cut + 1, Duration::from_secs(10)).expect("A got the next table within 10 s");
    assert!(at - asked <= Duration::from_millis(1200), "A got epoch {} {:?} after it was made, B down", cut + 1, at - asked);
    let _ = std::fs::remove_dir_all(dir);
}

/// For the test below: C and E take tables slowly once set.
static SLOW_DEMOTION: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn slow_demotion(_: u64) -> Duration {
    if SLOW_DEMOTION.load(std::sync::atomic::Ordering::SeqCst) {
        Duration::from_millis(300)
    } else {
        Duration::ZERO
    }
}

/// While a primary is being demoted, a node leaving needs room on the
/// other primaries: the one being demoted has none to give (its slots go).
#[test]
fn a_leave_during_a_demotion_has_no_room_on_the_demoted_one() {
    let dir = temp_dir("demote-leave-room");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    // No limit at first, so that the demotion starts; C and E slow to take
    // tables from before it starts, so that it lasts.
    let (_, [a, c, e]) = demoting_cluster(&mut s, mem(100, 0, 50), slow_demotion);
    SLOW_DEMOTION.store(true, std::sync::atomic::Ordering::SeqCst);
    let demoted = wait_until_value("C or E being demoted", || {
        let nodes = s.text(&["STUART", "NODES"]);
        [C, E].into_iter().find(|x| nodes.lines().any(|l| l.starts_with(x) && l.contains(" demoting")))
    });
    let (kept, kept_mem, demoted_mem) = if demoted == C { (E, &e, &c) } else { (C, &c, &e) };
    // 400 bytes of room below 90% on A and on the one being demoted, 500
    // of keys on the other: only A counts.
    *a.lock().unwrap() = mem(500, 1000, 400);
    *demoted_mem.lock().unwrap() = mem(500, 1000, 400);
    *kept_mem.lock().unwrap() = mem(600, 1000, 500);
    wait_until("stuart read their memory", || {
        let nodes = s.text(&["STUART", "NODES"]);
        [(A, 500), (demoted, 500), (kept, 600)].iter().all(|(id, used)| nodes.lines().any(|l| l.starts_with(id) && l.contains(&format!(" used={used} max=1000"))))
    });
    let reply = s.text(&["STUART", "LEAVE", kept]);
    let nodes = s.text(&["STUART", "NODES"]);
    SLOW_DEMOTION.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(nodes.contains(" demoting"), "the demotion ended before the LEAVE: {nodes}");
    assert!(reply.starts_with("-ERR Not enough memory") && reply.contains(" the others have 400 "), "{reply}");
    let _ = std::fs::remove_dir_all(dir);
}

/// For the test below: C takes the tables from this epoch on 300 ms
/// after it got them (0: at once).
static C_SLOW_FROM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn c_slow(epoch: u64) -> Duration {
    let from = C_SLOW_FROM.load(std::sync::atomic::Ordering::SeqCst);
    if from > 0 && epoch >= from {
        Duration::from_millis(300)
    } else {
        Duration::ZERO
    }
}

/// A node that left, lingering, gets the tables that follow in their
/// order, as the others do: a cutover only once its target took it (else
/// it would send clients to a target that doesn't serve the slots yet).
#[test]
fn a_node_that_left_gets_a_cutover_after_its_target() {
    let dir = temp_dir("left-cutover");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    let mut logs = Vec::new();
    for id in [A, B, C] {
        let log: Log = Default::default();
        let delay: fn(u64) -> Duration = if id == C { c_slow } else { |_| Duration::ZERO };
        let port = lagging_node(id, delay, log.clone(), Default::default(), Default::default(), Default::default());
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
        logs.push(log);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 3);
    s.text(&["STUART", "LEAVE", A]);
    wait_until_long("A left", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    let removed = epoch_of(&mut s);
    wait_until("A took the table it was removed with", || got(&logs[0], removed).is_some());
    // B drained to C, slow from now on: a table starting the moves (B's
    // first), then the cutover (C's first).
    C_SLOW_FROM.store(removed + 1, std::sync::atomic::Ordering::SeqCst);
    s.text(&["STUART", "LEAVE", B]);
    let cutover = wait_until_value("the cutover", || {
        let c = logs[2].lock().unwrap().clone();
        c.windows(2).find(|w| w[0].epoch > removed && !w[0].migrating.is_empty() && w[1].migrating.is_empty()).map(|w| w[1].epoch)
    });
    let at = got_within(&logs[0], cutover, Duration::from_secs(10)).expect("A, lingering, got the cutover");
    let c = got(&logs[2], cutover).unwrap();
    C_SLOW_FROM.store(0, std::sync::atomic::Ordering::SeqCst);
    assert!(at >= c.acked, "A got epoch {cutover} {:?} before C, its target, took it", c.acked - at);
    let _ = std::fs::remove_dir_all(dir);
}

/// A node leaving is removed only once it took the table in which it
/// holds nothing (so that it redirects clients for its old slots), slow
/// as it is to take it.
#[test]
fn a_node_leaves_once_it_took_its_last_table() {
    let dir = temp_dir("leave-slow");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    let logs = slow_cluster(&mut s, &[|e| if e >= 2 { Duration::from_millis(300) } else { Duration::ZERO }, |_| Duration::ZERO]);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until_long("A left", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    let epoch = epoch_of(&mut s);
    assert!(got(&logs[0], epoch - 1).is_some(), "A was removed in epoch {epoch} before it took epoch {}", epoch - 1);
    assert_eq!(slot_counts(&mut s), [16384]);
    let _ = std::fs::remove_dir_all(dir);
}

/// A node that left gets the tables that follow while it lingers (here:
/// for good, it never exits), listed in them owning nothing: it advertises
/// where its slots are now, not the table it was removed with.
#[test]
fn a_node_that_left_gets_the_tables_that_follow() {
    let dir = temp_dir("left-tables");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let (a, _b) = (node(stuart.1), node(stuart.1));
    wait_until("both joined", || s.text(&["STUART", "NODES"]).lines().count() == 2);
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    let id_a = id_of(a);
    assert_eq!(s.text(&["STUART", "LEAVE", &id_a]), ":8193");
    wait_until_long("A left", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", &id_a]) == ":0");
    let mut c = Client::connect(a);
    let removed = epoch_of(&mut s);
    wait_until("A took the table it was removed with, and itself", || epoch_of(&mut c) == removed);
    let nodes = c.text(&["CLUSTER", "NODES"]);
    assert_eq!(nodes.lines().count(), 2, "{nodes}");
    assert!(nodes.lines().any(|l| l.starts_with(&id_a) && l.contains("myself,master") && !l.contains("connected 0-")), "{nodes}");
    assert!(nodes.contains("connected 0-16383"), "{nodes}");

    // The next tables reach it too: a node joining, slots moving to it.
    let d = node(stuart.1);
    let id_d = id_of(d);
    wait_until("D joined", || s.text(&["STUART", "NODES"]).contains(&id_d));
    assert_eq!(s.text(&["STUART", "MOVE", "0", "99", &id_d]), ":100");
    wait_until("the move ended", || slot_counts(&mut s).contains(&100));
    wait_until("A took the last table", || epoch_of(&mut c) == epoch_of(&mut s));
    let nodes = c.text(&["CLUSTER", "NODES"]);
    assert_eq!(nodes.lines().count(), 3, "{nodes}");
    assert!(nodes.lines().any(|l| l.starts_with(&id_d) && l.ends_with(" 0-99")), "{nodes}");
    let key = &keys_in(0..=99, 1)[0];
    assert_eq!(c.text(&["GET", key]), format!("-MOVED {} {d}", cluster::key_hash_slot(key.as_bytes())));
    assert!(s.text(&["INFO"]).contains("\r\nstuart_keepers:3\r\n"), "A's keeper lingers");
    let _ = std::fs::remove_dir_all(dir);
}

/// With `--auto`, the slots of a node replaced (a new id at its address:
/// it restarted without its keys) go out again by themselves, and the
/// nodes end even.
#[test]
fn auto_hands_out_lost_slots() {
    let dir = temp_dir("auto-lost");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto"]);
    let mut s = stuart.client();
    let a = slow_node(A, |_| Duration::ZERO, Default::default());
    let b = slow_node(B, |_| Duration::ZERO, Default::default());
    assert_eq!(s.text(&["STUART", "JOIN", A, "127.0.0.1", &a]), "+OK");
    assert_eq!(s.text(&["STUART", "JOIN", B, "127.0.0.1", &b]), "+OK");
    wait_until_long("A and B even", Duration::from_secs(30), || slot_counts(&mut s) == [8192, 8192] && s.text(&["STUART", "MIGRATIONS"]).is_empty());
    // C joins at A's address: A is replaced, its 8192 slots lost. (A's fake
    // still answers there, so C is down; it gets the slots all the same.)
    assert_eq!(s.text(&["STUART", "JOIN", C, "127.0.0.1", &a]), "+OK");
    wait_until_long("the lost slots given out, and the nodes even", Duration::from_secs(30), || {
        let nodes = s.text(&["STUART", "NODES"]);
        !nodes.contains("unassigned") && slot_counts(&mut s) == [8192, 8192] && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    let _ = std::fs::remove_dir_all(dir);
}

/// `--moves-per-source 2 --max-moves 3`: no table has more moves than that.
#[test]
fn moves_at_once_are_limited() {
    let dir = temp_dir("limits");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--moves-per-source", "2", "--max-moves", "3"]);
    let mut s = stuart.client();
    let slow: fn(u64) -> Duration = |_| Duration::from_millis(10);
    let logs = slow_cluster(&mut s, &[slow, slow, slow]);
    // A's 0-9 and C's 12000-12009 to B.
    assert_eq!(s.text(&["STUART", "MOVE", "0", "9", B]), ":10");
    assert_eq!(s.text(&["STUART", "MOVE", "12000", "12009", B]), ":10");
    wait_until("the moves are done", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
    thread::sleep(Duration::from_millis(100));
    let log = logs[1].lock().unwrap().clone();
    let mut most = 0;
    for p in &log {
        let from = |id: &str| p.migrating.iter().filter(|m| m.1 == id).count();
        assert!(from(A) <= 2 && from(C) <= 2, "epoch {}: {:?}", p.epoch, p.migrating);
        assert!(p.migrating.len() <= 3, "epoch {}: {:?}", p.epoch, p.migrating);
        most = most.max(p.migrating.len());
    }
    assert_eq!(most, 3, "the limits are reached, not just kept");
    let _ = std::fs::remove_dir_all(dir);
}

/// A node's `INFO memory`, as much as stuart reads.
fn mem(used: u64, max: u64, dataset: u64) -> String {
    format!("# Memory\r\nused_memory:{used}\r\nused_memory_dataset:{dataset}\r\nmaxmemory:{max}\r\nmaxmemory_policy:noeviction\r\n")
}

/// A fake node answering `INFO memory` with `memory`, joined.
fn join_with_memory(s: &mut Client, id: &'static str, memory: String) -> Mem {
    let m: Mem = std::sync::Arc::new(std::sync::Mutex::new(memory));
    let port = memory_node(id, |_| Duration::ZERO, Default::default(), Default::default(), m.clone());
    assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    m
}

fn sorted_counts(s: &mut Client) -> Vec<usize> {
    let mut counts = slot_counts(s);
    counts.sort();
    counts
}

/// With `--auto`, a node at 90% of its maxmemory gets no slots: the others
/// keep theirs. Once it has room again it gets its share. STUART NODES and
/// INFO say which nodes are full.
#[test]
fn a_full_node_gets_no_slots() {
    let dir = temp_dir("full");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto"]);
    let mut s = stuart.client();
    join_with_memory(&mut s, A, mem(100, 1000, 50));
    join_with_memory(&mut s, B, mem(100, 0, 50));
    wait_until("A and B share the slots", || sorted_counts(&mut s) == [8192, 8192] && s.text(&["STUART", "MIGRATIONS"]).is_empty());
    let c = join_with_memory(&mut s, C, mem(900, 1000, 800));
    wait_until("C is listed full", || s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(C) && l.ends_with(" used=900 max=1000 full")));
    assert!(s.text(&["INFO"]).contains("\r\nstuart_full:1\r\n"));
    // Planned again every second meanwhile: still nothing for C.
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(slot_counts(&mut s), [8192, 8192, 0]);
    assert_eq!(s.text(&["STUART", "MIGRATIONS"]), "");
    // Room again: C gets its share.
    *c.lock().unwrap() = mem(100, 1000, 50);
    wait_until_long("even over three", Duration::from_secs(30), || {
        sorted_counts(&mut s) == [5461, 5461, 5462] && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    assert!(s.text(&["INFO"]).contains("\r\nstuart_full:0\r\n"));
    let _ = std::fs::remove_dir_all(dir);
}

/// A node may leave (or be drained) only if the others have room below
/// 90% of their maxmemory for its keys: else STUART LEAVE is refused, and
/// the node, stopping, exits at once rather than wait to be killed.
#[test]
fn leaving_needs_room_on_the_others() {
    let dir = temp_dir("leave-room");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    join_with_memory(&mut s, A, mem(600, 1000, 500));
    let b = join_with_memory(&mut s, B, mem(500, 1000, 400));
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("both took the table", || up_and_acked(&mut s, 1) == 2);
    let refused = format!("-ERR Not enough memory for node {A}'s keys on the other nodes: they take 500 bytes, the others have 400 below 90% of their maxmemory");
    assert_eq!(s.text(&["STUART", "LEAVE", A]), refused);
    assert_eq!(s.text(&["STUART", "DRAIN", A]), refused);
    assert!(!s.text(&["STUART", "NODES"]).contains(" leaving"));
    assert_eq!(s.text(&["STUART", "MIGRATIONS"]), "");
    // B without a limit: room for anything.
    *b.lock().unwrap() = mem(500, 0, 400);
    wait_until("A may leave", || s.text(&["STUART", "LEAVE", A]) == ":8193");
    wait_until_long("A left", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    // The last node leaving is a shutdown: nowhere to check.
    *b.lock().unwrap() = mem(950, 1000, 900);
    assert_eq!(s.text(&["STUART", "LEAVE", B]), ":0");
    let _ = std::fs::remove_dir_all(dir);
}

/// `GET path` over HTTP: the status line and the body.
fn http_get(addr: SocketAddr, path: &str) -> (String, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_string(), body.to_string())
}

/// `/metrics`' samples, `name{labels}` to value.
fn scrape(addr: SocketAddr) -> std::collections::BTreeMap<String, f64> {
    let (status, body) = http_get(addr, "/metrics");
    assert_eq!(status, "HTTP/1.1 200 OK");
    body.lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let (name, value) = l.rsplit_once(' ').unwrap();
            (name.to_string(), value.parse().unwrap())
        })
        .collect()
}

/// stuart's `/metrics`: the nodes and their state, the table, the moves;
/// per node its slots and memory as the keepers last read it.
#[test]
fn stuart_s_metrics() {
    let dir = temp_dir("metrics");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--metrics", &http.to_string()]);
    let m = scrape(http);
    let info = format!("stuart_info{{version=\"{}\",auto=\"1\"}}", env!("CARGO_PKG_VERSION"));
    let fresh: Vec<(&str, f64)> = vec![
        ("stuart_demotions_total", 0.0),
        ("stuart_failovers_total", 0.0),
        ("stuart_handovers_total", 0.0),
        ("stuart_nodes{state=\"demoting\"}", 0.0),
        ("stuart_nodes{state=\"down\"}", 0.0),
        ("stuart_nodes{state=\"failed\"}", 0.0),
        ("stuart_nodes{state=\"full\"}", 0.0),
        ("stuart_nodes{state=\"leaving\"}", 0.0),
        ("stuart_nodes{state=\"up\"}", 0.0),
        ("stuart_slots_moved_total", 0.0),
        ("stuart_slots_moving", 0.0),
        ("stuart_slots_queued", 0.0),
        ("stuart_slots_unassigned", 16384.0),
        ("stuart_table_epoch", 0.0),
    ];
    for (name, v) in &fresh {
        assert_eq!(m.get(*name), Some(v), "{name}");
    }
    assert_eq!(m.get(&info), Some(&1.0));
    assert_eq!(m.len(), fresh.len() + 2, "and the uptime: {m:#?}");
    assert_eq!(http_get(http, "/ready"), ("HTTP/1.1 200 OK".into(), "ready\n".into()));

    let mut s = stuart.client();
    join_with_memory(&mut s, A, mem(100, 1000, 50));
    let c = join_with_memory(&mut s, C, mem(900, 1000, 800));
    wait_until("A has every slot, C is full", || {
        let m = scrape(http);
        m.get("stuart_nodes{state=\"full\"}") == Some(&1.0) && m.get("stuart_slots_unassigned") == Some(&0.0)
    });
    let m = scrape(http);
    let mut node = |id: &str| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(id)).unwrap().split(' ').nth(1).unwrap().to_string();
    let (a, c_addr) = (node(A), node(C));
    let at = |name: &str, id: &str, addr: &str| format!("{name}{{node=\"{id}\",addr=\"{addr}\"}}");
    assert_eq!(m.get("stuart_nodes{state=\"up\"}"), Some(&2.0));
    assert_eq!(m.get(&at("stuart_node_slots", A, &a)), Some(&16384.0));
    assert_eq!(m.get(&at("stuart_node_slots", C, &c_addr)), Some(&0.0));
    assert_eq!(m.get(&at("stuart_node_up", C, &c_addr)), Some(&1.0));
    assert_eq!(m.get(&at("stuart_node_leaving", C, &c_addr)), Some(&0.0));
    assert_eq!(m.get(&at("stuart_node_memory_used_bytes", C, &c_addr)), Some(&900.0));
    assert_eq!(m.get(&at("stuart_node_memory_max_bytes", A, &a)), Some(&1000.0));
    assert!(m.get("stuart_table_epoch").is_some_and(|&e| e >= 1.0));

    // C has room again: half the slots move to it, and are counted.
    *c.lock().unwrap() = mem(100, 1000, 50);
    wait_until_long("C has its share", Duration::from_secs(30), || {
        let m = scrape(http);
        m.get(&at("stuart_node_slots", C, &c_addr)) == Some(&8192.0) && m.get("stuart_slots_moving") == Some(&0.0)
    });
    let m = scrape(http);
    assert_eq!(m.get("stuart_slots_moved_total"), Some(&8192.0));
    assert_eq!(m.get("stuart_nodes{state=\"full\"}"), Some(&0.0));
    let _ = std::fs::remove_dir_all(dir);
}

/// Sends `signal` (TERM, INT) to stuart.
fn signal(stuart: &Stuart, signal: &str) {
    let status = Command::new("kill").args([&format!("-{signal}"), &stuart.0.id().to_string()]).status().unwrap();
    assert!(status.success());
}

/// How long after `since` stuart exits, and how; within `within`.
fn exits(stuart: &mut Stuart, since: Instant, within: Duration) -> (Duration, std::process::ExitStatus) {
    loop {
        if let Some(status) = stuart.0.try_wait().unwrap() {
            return (since.elapsed(), status);
        }
        assert!(since.elapsed() < within, "stuart still runs {within:?} after the signal");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Two fake nodes joined and holding the slots.
fn two_nodes(name: &str) -> (Stuart, PathBuf) {
    let dir = temp_dir(name);
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    for id in [A, B] {
        let (port, _rx) = fake_node(id, Instant::now(), false);
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("both took the table", || up_and_acked(&mut s, 1) == 2);
    (stuart, dir)
}

/// On SIGTERM stuart serves on for the nodes stopping with it (a namespace
/// deleted: every pod gets SIGTERM at once): once they're all leaving it
/// tells them to stop, and exits a moment later. Stopping alone, it exits
/// once no node started leaving for a while; with nodes still leaving, at
/// most after LINGER_MAX. A second SIGTERM, or SIGINT, exits at once.
#[test]
fn sigterm_lingers_for_the_nodes_stopping_too() {
    // Every node leaving: they're told to stop (0), and stuart goes.
    let (mut stuart, dir) = two_nodes("linger-all");
    signal(&stuart, "TERM");
    let t0 = Instant::now();
    let mut s = stuart.client();
    assert_ne!(s.text(&["STUART", "LEAVE", A]), ":0", "B is still there");
    assert_eq!(s.text(&["STUART", "LEAVE", B]), ":0");
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":0", "asking again");
    let (after, status) = exits(&mut stuart, t0, Duration::from_secs(10));
    assert!(status.success(), "{status}");
    assert!(after >= Duration::from_millis(1400) && after < stuart::LINGER_QUIET, "{after:?}");
    let _ = std::fs::remove_dir_all(dir);

    // stuart alone stopping (a restart): no node leaves, it goes.
    let (mut stuart, dir) = two_nodes("linger-alone");
    signal(&stuart, "TERM");
    let t0 = Instant::now();
    assert_eq!(stuart.client().text(&["PING"]), "+PONG", "it serves on");
    let (after, status) = exits(&mut stuart, t0, Duration::from_secs(10));
    assert!(status.success(), "{status}");
    assert!(after >= stuart::LINGER_QUIET && after < stuart::LINGER_QUIET + Duration::from_secs(1), "{after:?}");
    let _ = std::fs::remove_dir_all(dir);

    // A node leaving, the other staying: at most LINGER_MAX.
    let (mut stuart, dir) = two_nodes("linger-max");
    signal(&stuart, "TERM");
    let t0 = Instant::now();
    assert_ne!(stuart.client().text(&["STUART", "LEAVE", A]), ":0");
    let (after, status) = exits(&mut stuart, t0, Duration::from_secs(25));
    assert!(status.success(), "{status}");
    assert!(after >= stuart::LINGER_MAX && after < stuart::LINGER_MAX + Duration::from_secs(1), "{after:?}");
    let state = std::fs::read_to_string(dir.join("state")).unwrap();
    assert!(state.contains(&format!("leaving {A}")), "the next stuart drains it on: {state}");
    let _ = std::fs::remove_dir_all(dir);

    // Again, or SIGINT: at once.
    for second in ["TERM", "INT"] {
        let (mut stuart, dir) = two_nodes("linger-twice");
        signal(&stuart, "TERM");
        thread::sleep(Duration::from_millis(300));
        let t0 = Instant::now();
        signal(&stuart, second);
        let (after, status) = exits(&mut stuart, t0, Duration::from_secs(10));
        assert!(status.success(), "{status}");
        assert!(after < Duration::from_millis(500), "SIGTERM then SIG{second}: {after:?}");
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// `--replicas 1 --auto`: nodes joining one by one are a primary, its
/// replica (on another zone if there is a primary there), a primary, its
/// replica; the slots go to the primaries; the replicas redirect every key
/// and say so in `ROLE` and `CLUSTER NODES`; a restart keeps the roles.
#[test]
fn replicas_are_assigned_as_nodes_join() {
    let dir = temp_dir("replicas");
    let state = dir.join("state");
    let port = free_port();
    let stuart = Stuart::start_with(port, &state, &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    let mut ids = Vec::new();
    let mut addrs = Vec::new();
    for (i, zone) in ["w1", "w1", "w2", "w1"].into_iter().enumerate() {
        let addr = node_in(stuart.1, Some(zone));
        wait_until("the node joins", || s.text(&["STUART", "NODES"]).lines().filter(|l| l.contains(" up ")).count() == i + 1);
        ids.push(id_of(addr));
        addrs.push(addr);
    }
    // Every node has the last table, and the slots have moved.
    wait_until_long("the slots are even", Duration::from_secs(30), || {
        let epoch = epoch_of(&mut s);
        let nodes = s.text(&["STUART", "NODES"]);
        up_and_acked(&mut s, epoch) == 4 && nodes.contains(" slots=8192 ") && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    let nodes = s.text(&["STUART", "NODES"]);
    let line = |i: usize| nodes.lines().find(|l| l.starts_with(&ids[i])).unwrap().to_string();
    assert!(line(0).contains(" slots=8192 ") && !line(0).contains("replica-of") && line(0).contains(" zone=w1"), "{nodes}");
    assert!(line(1).contains(&format!(" slots=0 replica-of={} zone=w1", ids[0])), "{nodes}");
    assert!(line(2).contains(" slots=8192 ") && !line(2).contains("replica-of"), "{nodes}");
    // Node 2 (w2) is the only primary without one.
    assert!(line(3).contains(&format!(" slots=0 replica-of={} zone=w1", ids[2])), "{nodes}");

    // The replicas, as clients see them.
    let mut b = Client::connect(addrs[1]);
    // It syncs from its primary (phase 8 milestone 3).
    let want = format!("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:", addrs[0].port());
    wait_until("the replica syncs", || String::from_utf8(b.call(&["ROLE"])).unwrap().starts_with(&want));
    let cluster_nodes = b.text(&["CLUSTER", "NODES"]);
    assert!(cluster_nodes.contains(&format!("{} 127.0.0.1:{p}@{p} myself,slave {} ", ids[1], ids[0], p = addrs[1].port())), "{cluster_nodes}");
    assert_eq!(cluster_nodes.matches(" slave ").count(), 1, "{cluster_nodes}");
    assert_eq!(b.text(&["READONLY"]), "+OK");
    let moved = b.text(&["GET", "foo"]);
    assert!(moved.starts_with("-MOVED 12182 127.0.0.1:"), "{moved}");
    let mut a = Client::connect(addrs[0]);
    assert_eq!(a.text(&["ROLE"]), "*3");
    // CLUSTER SLOTS, here and at stuart: each primary with its replica,
    // once stuart has read the replicas' offsets (past 0 since the copy,
    // or the primary's first PING) and pushed them.
    for c in [&mut a, &mut s] {
        wait_until("the replicas in CLUSTER SLOTS", || {
            let slots = String::from_utf8(c.call(&["CLUSTER", "SLOTS"])).unwrap();
            slots.starts_with("*2\r\n*4\r\n") && slots.contains(&ids[1]) && slots.contains(&ids[3])
        });
    }
    let shards = String::from_utf8(s.call(&["CLUSTER", "SHARDS"])).unwrap();
    assert_eq!(shards.matches("$7\r\nreplica\r\n").count(), 2, "{shards}");
    assert_eq!(shards.matches("$7\r\nloading\r\n").count(), 0, "{shards}");

    // The roles are in the state file.
    let epoch = epoch_of(&mut s);
    drop(s);
    drop(stuart);
    let stuart = Stuart::start_with(port, &state, &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    assert_eq!(epoch_of(&mut s), epoch);
    assert!(s.text(&["STUART", "NODES"]).contains(&format!(" replica-of={} zone=w1", ids[0])));

    // JOIN's options.
    let x = "e".repeat(40);
    assert_eq!(s.text(&["STUART", "JOIN", &x, "127.0.0.1", "1", "ZONE", "a", "ZONE", "b"]), "-ERR syntax error");
    assert_eq!(s.text(&["STUART", "JOIN", &x, "127.0.0.1", "1", "NAME", "a", "NAME", "b"]), "-ERR syntax error");
    assert_eq!(s.text(&["STUART", "JOIN", &x, "127.0.0.1", "1", "PLACE", "a"]), "-ERR syntax error");
    assert_eq!(s.text(&["STUART", "JOIN", &x, "127.0.0.1", "1", "ZONE", "a b"]), "-ERR Invalid zone 'a b'");
    assert_eq!(s.text(&["STUART", "JOIN", &x, "127.0.0.1", "1", "zone", "w3", "name", "pod-x"]), "+OK");
    let nodes = s.text(&["STUART", "NODES"]);
    assert!(nodes.lines().any(|l| l.starts_with(&x) && l.contains(" name=pod-x zone=w3")), "{nodes}");
}

/// A dbnode joining `stuart` with the cluster's `password`.
fn node_with_password(stuart: SocketAddr, password: &str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = dbnode::Config {
        cluster: Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() }),
        stuart: Some(stuart.to_string()),
        password: Some(dbnode::auth::Secret(password.as_bytes().to_vec())),
        ..dbnode::Config::default()
    };
    thread::spawn(move || dbnode::serve(listener, None, config).unwrap());
    addr
}

/// One password for the cluster (`--requirepass-file` at stuart, the same
/// at the nodes): stuart's port requires it, a node with another one can't
/// join, and with it every link logs in: stuart's to the nodes (tables,
/// INFO), the replicas' to their primaries, the slot copies' to their
/// sources.
#[test]
fn a_cluster_with_a_password() {
    let dir = temp_dir("password");
    let file = dir.join("password");
    std::fs::write(&file, "s3cret\n").unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--requirepass-file", file.to_str().unwrap()]);
    let mut s = stuart.client();
    assert_eq!(s.text(&["STUART", "NODES"]), "-NOAUTH Authentication required.");
    assert_eq!(s.text(&["AUTH", "s3cret\n"]), "-WRONGPASS invalid username-password pair or user is disabled.");
    assert!(s.text(&["HELLO", "3"]).starts_with("-NOAUTH HELLO must be called with the client already authenticated"));
    assert_eq!(s.text(&["HELLO", "3", "AUTH", "default", "wrong"]), "-WRONGPASS invalid username-password pair or user is disabled.");
    assert_eq!(s.text(&["AUTH", "default", "s3cret"]), "+OK");
    let mut h = stuart.client();
    assert_eq!(String::from_utf8(h.call(&["HELLO", "2", "AUTH", "default", "s3cret"])).unwrap().lines().next(), Some("*14"));
    assert!(!h.text(&["STUART", "NODES"]).starts_with("-NOAUTH"));

    // Another password: refused at the join.
    let stranger = node_with_password(stuart.1, "wrong");
    let mut nodes = Vec::new();
    for i in 1..=4 {
        nodes.push(node_with_password(stuart.1, "s3cret"));
        wait_until("the node joins", || s.text(&["STUART", "NODES"]).lines().filter(|l| l.contains(" up ")).count() == i);
    }
    wait_until_long("the slots are even", Duration::from_secs(30), || {
        let epoch = epoch_of(&mut s);
        let listed = s.text(&["STUART", "NODES"]);
        up_and_acked(&mut s, epoch) == 4 && listed.matches(" slots=8192 ").count() == 2 && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    assert!(!s.text(&["STUART", "NODES"]).contains(&id_of_with(stranger, "wrong")), "the stranger joined");
    let mut c = Client::connect(nodes[0]);
    assert_eq!(c.text(&["CLUSTER", "MYID"]), "-NOAUTH Authentication required.");

    // Writes reach the replicas.
    let mut f = Follower::new(stuart.1);
    f.password = Some("s3cret");
    let keys = keys_in(0..=16383, 200);
    for k in &keys {
        assert_eq!(f.text(&["SET", k, "v"]), "+OK");
    }
    let listed = s.text(&["STUART", "NODES"]);
    let replica = nodes.iter().copied().find(|&n| listed.lines().any(|l| l.starts_with(&id_of_with(n, "s3cret")) && l.contains(" replica-of="))).unwrap();
    let mut r = Client::connect(replica);
    assert_eq!(r.text(&["AUTH", "s3cret"]), "+OK");
    wait_until("the replica is synced", || String::from_utf8(r.call(&["ROLE"])).unwrap().contains("$9\r\nconnected\r\n"));
    assert_eq!(r.text(&["READONLY"]), "+OK");
    let mine: Vec<&String> = keys.iter().filter(|k| !r.text(&["GET", k]).starts_with("-MOVED")).collect();
    assert!(!mine.is_empty());
    wait_until("the writes reached the replica", || mine.iter().all(|k| r.text(&["GET", k]) == "v"));
    let _ = std::fs::remove_dir_all(dir);
}

/// `CLUSTER MYID` at `addr`, logged in with `password`.
fn id_of_with(addr: SocketAddr, password: &str) -> String {
    let mut c = Client::connect(addr);
    assert_eq!(c.text(&["AUTH", password]), "+OK");
    c.text(&["CLUSTER", "MYID"])
}

/// A dbnode joining `stuart` that announces a proxy's address: once the
/// proxy is cut, nobody reaches the node (stuart, clients), while it runs
/// on and its own connections (to its primary) carry on.
struct Behind {
    addr: SocketAddr,
    /// Where the node itself listens: not cut.
    real: SocketAddr,
    cut: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Cut too, but for the replication link (a connection whose first
    /// command is REPLSYNC): stuart and clients can't reach the node, its
    /// replica can.
    isolated: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Each connection's two ends, and whether it is a replication link.
    conns: std::sync::Arc<std::sync::Mutex<Vec<(TcpStream, TcpStream, bool)>>>,
}

impl Behind {
    fn node(stuart: SocketAddr) -> Behind {
        Behind::named(stuart, None)
    }

    fn named(stuart: SocketAddr, name: Option<&str>) -> Behind {
        use std::sync::atomic::Ordering;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let real = listener.local_addr().unwrap();
        let front = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = front.local_addr().unwrap();
        let config = dbnode::Config {
            cluster: Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() }),
            stuart: Some(stuart.to_string()),
            name: name.map(str::to_string),
            ..dbnode::Config::default()
        };
        thread::spawn(move || dbnode::serve(listener, None, config).unwrap());
        let behind = Behind { addr, real, cut: Default::default(), isolated: Default::default(), conns: Default::default() };
        let (cut, isolated, conns) = (behind.cut.clone(), behind.isolated.clone(), behind.conns.clone());
        thread::spawn(move || {
            for client in front.incoming() {
                let Ok(mut client) = client else { continue };
                let (cut, isolated, conns) = (cut.clone(), isolated.clone(), conns.clone());
                thread::spawn(move || {
                    // Its first command says what it is.
                    let mut first = vec![0; 4096];
                    let Ok(n) = client.read(&mut first) else { return };
                    let replication = first[..n].windows(8).any(|w| w == b"REPLSYNC");
                    if cut.load(Ordering::SeqCst) || (isolated.load(Ordering::SeqCst) && !replication) {
                        return;
                    }
                    let Ok(mut server) = TcpStream::connect(real) else { return };
                    if server.write_all(&first[..n]).is_err() {
                        return;
                    }
                    conns.lock().unwrap().push((client.try_clone().unwrap(), server.try_clone().unwrap(), replication));
                    for (mut from, mut to) in [(client.try_clone().unwrap(), server.try_clone().unwrap()), (server, client)] {
                        thread::spawn(move || {
                            let _ = std::io::copy(&mut from, &mut to);
                            let _ = to.shutdown(std::net::Shutdown::Both);
                        });
                    }
                });
            }
        });
        behind
    }

    fn cut(&self, on: bool) {
        self.cut.store(on, std::sync::atomic::Ordering::SeqCst);
        if on {
            for (a, b, _) in self.conns.lock().unwrap().drain(..) {
                let _ = (a.shutdown(std::net::Shutdown::Both), b.shutdown(std::net::Shutdown::Both));
            }
        }
    }

    /// Cuts stuart and the clients off, the replication link kept.
    fn isolate(&self, on: bool) {
        self.isolated.store(on, std::sync::atomic::Ordering::SeqCst);
        if on {
            self.conns.lock().unwrap().retain(|(a, b, replication)| {
                if !replication {
                    let _ = (a.shutdown(std::net::Shutdown::Both), b.shutdown(std::net::Shutdown::Both));
                }
                *replication
            });
        }
    }
}

/// A replica stuart can't reach for 5 s is marked failed (phase 8): in
/// `STUART NODES`, the state file, its primary's `CLUSTER NODES` and
/// stuart's; once it answers again, the mark goes. The pairing stays.
#[test]
fn a_replica_down_is_marked_failed() {
    let dir = temp_dir("failed");
    let state = dir.join("state");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &state, &["--auto", "--replicas", "1", "--metrics", &http.to_string()]);
    let failed = || scrape(http).get("stuart_nodes{state=\"failed\"}").copied();
    let mut s = stuart.client();
    let a = node(stuart.1);
    wait_until("the primary joins", || s.text(&["STUART", "NODES"]).contains(" up "));
    let b = Behind::node(stuart.1);
    let (aid, bid) = (id_of(a), id_of(b.addr));
    wait_until("the replica joins", || s.text(&["STUART", "NODES"]).contains(&format!(" replica-of={aid}")));
    let mut pa = Client::connect(a);
    let want = format!("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:", a.port());
    wait_until("the replica syncs", || String::from_utf8(Client::connect(b.addr).call(&["ROLE"])).unwrap().starts_with(&want));
    let line = |s: &mut Client| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(&bid)).unwrap().to_string();
    assert!(!line(&mut s).contains(" failed"));
    assert_eq!(failed(), Some(0.0));

    b.cut(true);
    let cut = Instant::now();
    wait_until("stuart sees it down", || line(&mut s).contains(" down "));
    wait_until("it is marked failed", || line(&mut s).contains(" failed"));
    assert!(cut.elapsed() >= Duration::from_millis(4900), "marked after {:?}", cut.elapsed());
    assert_eq!(failed(), Some(1.0));
    assert!(std::fs::read_to_string(&state).unwrap().contains(&format!("\nfailed {bid}\n")));
    let fail = format!("{bid} 127.0.0.1:{p}@{p} slave,fail {aid} 0 0 ", p = b.addr.port());
    wait_until("its primary has the mark", || pa.text(&["CLUSTER", "NODES"]).contains(&fail));
    assert!(pa.text(&["CLUSTER", "NODES"]).lines().any(|l| l.starts_with(&fail) && l.ends_with(" disconnected")));
    assert!(s.text(&["CLUSTER", "NODES"]).contains(&fail), "stuart's view too");
    assert_eq!(pa.text(&["SET", "k", "v"]), "+OK");
    assert!(line(&mut s).contains(&format!(" replica-of={aid}")), "the pairing stays");

    b.cut(false);
    wait_until("the mark goes", || !line(&mut s).contains(" failed"));
    wait_until("at its primary too", || !pa.text(&["CLUSTER", "NODES"]).contains("fail"));
    assert!(!std::fs::read_to_string(&state).unwrap().contains("\nfailed "));
    assert_eq!(failed(), Some(0.0));
    assert!(line(&mut s).contains(" up ") && line(&mut s).contains(&format!(" replica-of={aid}")));
}

/// A primary and its replica (the replica joining second, `--replicas 1`),
/// the primary behind a proxy; keys written and acknowledged.
fn primary_and_replica(stuart: &Stuart, name: Option<&str>) -> (Behind, SocketAddr, String, String) {
    let mut s = stuart.client();
    let a = Behind::named(stuart.1, name);
    wait_until("the primary joins", || s.text(&["STUART", "NODES"]).contains(" up "));
    let b = node(stuart.1);
    let (aid, bid) = (id_of(a.addr), id_of(b));
    wait_until("the replica joins", || s.text(&["STUART", "NODES"]).contains(&format!(" replica-of={aid}")));
    let want = format!("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:", a.addr.port());
    wait_until("the replica syncs", || String::from_utf8(Client::connect(b).call(&["ROLE"])).unwrap().starts_with(&want));
    let mut pa = Client::connect(a.real);
    for i in 0..20 {
        assert_eq!(pa.text(&["SET", &format!("{{k}}{i}"), &i.to_string()]), "+OK");
    }
    assert_eq!(pa.text(&["WAIT", "1", "2000"]), ":1");
    (a, b, aid, bid)
}

/// A primary nobody reaches for --fail-after: its replica, fenced, is
/// promoted once the primary's lease has run out; it has the keys. The old
/// one, still running, takes no writes meanwhile; reachable again, it is
/// the new primary's replica.
#[test]
fn a_primary_down_fails_over() {
    let dir = temp_dir("failover");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--fail-after", "2", "--metrics", &http.to_string()]);
    let mut s = stuart.client();
    let (a, b, aid, bid) = primary_and_replica(&stuart, None);
    let line = |s: &mut Client, id: &str| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(id)).unwrap().to_string();

    a.cut(true);
    let cut = Instant::now();
    wait_until_long("the replica is promoted", Duration::from_secs(15), || line(&mut s, &bid).contains(" slots=16384 "));
    let took = cut.elapsed();
    assert!(took >= Duration::from_millis(5000), "promoted after {took:?}: --fail-after 2 and the 3 s lease");
    assert!(!line(&mut s, &bid).contains("replica-of"));
    // As Redis shows a failed-over master: a primary without slots, failed.
    let old = line(&mut s, &aid);
    assert!(!old.contains("replica-of") && old.contains(" failed") && old.contains(" slots=0 "), "{old}");
    let nodes = s.text(&["CLUSTER", "NODES"]);
    assert!(nodes.lines().any(|l| l.starts_with(&aid) && l.contains(" master,fail - 0 0 ") && l.ends_with(" disconnected")), "{nodes}");
    assert_eq!(scrape(http).get("stuart_failovers_total"), Some(&1.0));
    let mut pb = Client::connect(b);
    wait_until("it serves as a primary", || pb.text(&["ROLE"]) == "*3");
    for i in 0..20 {
        assert_eq!(pb.text(&["GET", &format!("{{k}}{i}")]), i.to_string());
    }
    assert_eq!(pb.text(&["SET", "{k}new", "v"]), "+OK", "writes alone: its replica is marked failed");
    // The old primary, cut off, takes no writes: its lease ran out.
    let mut pa = Client::connect(a.real);
    assert_eq!(pa.text(&["SET", "{k}0", "x"]), "-NOREPLICAS Not enough good replicas to write.");

    // Back: the new primary's replica, with its keys.
    a.cut(false);
    wait_until_long("the old primary is a replica", Duration::from_secs(15), || {
        let l = line(&mut s, &aid);
        !l.contains(" failed") && l.contains(&format!(" replica-of={bid}")) && pa.text(&["INFO", "replication"]).contains(&format!("master_port:{}", b.port()))
    });
    wait_until("it syncs", || pa.text(&["INFO", "replication"]).contains("master_link_status:up"));
    assert_eq!(pa.text(&["READONLY"]), "+OK");
    wait_until("it has the new key", || pa.text(&["GET", "{k}new"]) == "v");
    assert_eq!(pa.text(&["GET", "{k}0"]), "0");
}

/// A primary back within the lease's wait: the failover is called off,
/// the fences lifted; it stays the primary and writes again.
#[test]
fn a_primary_back_in_time_is_not_failed_over() {
    let dir = temp_dir("failover-off");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--fail-after", "2", "--metrics", &http.to_string()]);
    let mut s = stuart.client();
    let (a, _b, aid, bid) = primary_and_replica(&stuart, None);
    a.cut(true);
    thread::sleep(Duration::from_millis(3500));
    a.cut(false);
    thread::sleep(Duration::from_secs(4));
    let nodes = s.text(&["STUART", "NODES"]);
    assert!(nodes.lines().any(|l| l.starts_with(&aid) && l.contains(" slots=16384 ") && !l.contains("replica-of")), "{nodes}");
    assert!(nodes.lines().any(|l| l.starts_with(&bid) && l.contains(&format!(" replica-of={aid}"))), "{nodes}");
    assert_eq!(scrape(http).get("stuart_failovers_total"), Some(&0.0));
    let mut pa = Client::connect(a.real);
    wait_until("it writes again: the fence lifted", || pa.text(&["SET", "{k}0", "y"]) == "+OK");
}

/// A pod that joins again under its name (a new id, no keys) replacing a
/// primary: the old one is dead, its replica is promoted at once, without
/// --fail-after or the lease's wait; then the pod joins as the new
/// primary's replica.
#[test]
fn a_primary_rejoining_under_its_name_fails_over_at_once() {
    let dir = temp_dir("failover-rejoin");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    let (_a, b, aid, bid) = primary_and_replica(&stuart, Some("pod-a"));
    let again = Instant::now();
    let c = Behind::named(stuart.1, Some("pod-a"));
    let cid = id_of(c.addr);
    wait_until("the pod joins as the replica", || {
        s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(&cid) && l.contains(&format!(" replica-of={bid}")))
    });
    assert!(again.elapsed() < Duration::from_secs(4), "{:?}: no 5 s detection, no lease wait", again.elapsed());
    let nodes = s.text(&["STUART", "NODES"]);
    assert!(!nodes.contains(&aid), "the old one is gone: {nodes}");
    assert!(nodes.lines().any(|l| l.starts_with(&bid) && l.contains(" slots=16384 ")), "{nodes}");
    let mut pb = Client::connect(b);
    for i in 0..20 {
        assert_eq!(pb.text(&["GET", &format!("{{k}}{i}")]), i.to_string());
    }
}

/// A primary cut off from stuart (and its clients) but not from its
/// replica: stuart can't tell it from a dead one and fails it over, but
/// first fences the replica, so the primary's lease runs out: it takes no
/// write from then on, before the promotion and after. No write is taken
/// by both. Reachable again, it is the new primary's replica.
#[test]
fn a_primary_cut_off_from_stuart_stops_writing_before_the_promotion() {
    let dir = temp_dir("failover-split");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--fail-after", "2"]);
    let mut s = stuart.client();
    let (a, b, aid, bid) = primary_and_replica(&stuart, None);
    let line = |s: &mut Client, id: &str| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(id)).unwrap().to_string();
    let mut pa = Client::connect(a.real);
    a.isolate(true);
    let cut = Instant::now();
    // It writes on, its replica acknowledging, until the fence (--fail-after
    // 2 s after stuart lost it), then refuses within the lease.
    assert_eq!(pa.text(&["SET", "{k}split", "before"]), "+OK");
    // Both watched together: the old primary's last write taken, and the
    // promotion.
    let (mut last_taken, mut promoted) = (Duration::ZERO, None);
    while promoted.is_none() {
        assert!(cut.elapsed() < Duration::from_secs(15), "no promotion");
        if pa.text(&["SET", "{k}split", "x"]) == "+OK" {
            last_taken = cut.elapsed();
        }
        if line(&mut s, &bid).contains(" slots=16384 ") {
            promoted = Some(cut.elapsed());
        }
        thread::sleep(Duration::from_millis(20));
    }
    let promoted = promoted.unwrap();
    assert!(last_taken + Duration::from_millis(100) < promoted, "the old primary took a write {last_taken:?} after the cut, the promotion was at {promoted:?}");
    assert!(promoted >= Duration::from_millis(5000), "{promoted:?}: --fail-after 2 and the 3 s lease");
    for _ in 0..20 {
        assert!(pa.text(&["SET", "{k}split", "after"]).starts_with("-NOREPLICAS"), "the old primary must not write alongside the new");
        thread::sleep(Duration::from_millis(50));
    }
    let mut pb = Client::connect(b);
    assert_eq!(pb.text(&["SET", "{k}new", "v"]), "+OK");
    // Reachable again: the new primary's replica.
    a.isolate(false);
    wait_until_long("the old primary is a replica", Duration::from_secs(15), || line(&mut s, &aid).contains(&format!(" replica-of={bid}")));
    assert_eq!(pa.text(&["READONLY"]), "+OK");
    wait_until("it has the new primary's keys", || pa.text(&["GET", "{k}new"]) == "v");
}

/// Two replicas: the primary down, one is promoted, the other follows it,
/// its fence lifted: it acknowledges the new primary's writes at once
/// (WAIT), not once its fence ran out.
#[test]
fn with_two_replicas_the_other_follows_the_new_primary() {
    let dir = temp_dir("failover-two");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "2", "--fail-after", "2"]);
    let mut s = stuart.client();
    let (a, b, aid, bid) = primary_and_replica(&stuart, None);
    let c = node(stuart.1);
    let cid = id_of(c);
    wait_until("the second replica joins", || s.text(&["STUART", "NODES"]).lines().filter(|l| l.contains(&format!(" replica-of={aid}"))).count() == 2);
    let want = format!("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:", a.addr.port());
    wait_until("it syncs", || String::from_utf8(Client::connect(c).call(&["ROLE"])).unwrap().starts_with(&want));
    a.cut(true);
    let line = |s: &mut Client, id: &str| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(id)).unwrap().to_string();
    wait_until_long("a replica is promoted", Duration::from_secs(15), || {
        line(&mut s, &bid).contains(" slots=16384 ") || line(&mut s, &cid).contains(" slots=16384 ")
    });
    let (new, other, other_id, new_id) =
        if line(&mut s, &bid).contains(" slots=16384 ") { (b, c, cid.clone(), bid.clone()) } else { (c, b, bid.clone(), cid.clone()) };
    wait_until("the other follows it", || line(&mut s, &other_id).contains(&format!(" replica-of={new_id}")));
    let want = format!("*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{}\r\n$9\r\nconnected\r\n:", new.port());
    wait_until("it syncs from it", || String::from_utf8(Client::connect(other).call(&["ROLE"])).unwrap().starts_with(&want));
    let mut pn = Client::connect(new);
    assert_eq!(pn.text(&["SET", "{k}after", "v"]), "+OK");
    let t = Instant::now();
    assert_eq!(pn.text(&["WAIT", "1", "3000"]), ":1", "its fence lifted");
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
}

/// A primary leaving (STUART LEAVE, as on SIGTERM) with a replica: no slot
/// moves; its writes are paused, the replica catches up and is promoted at
/// once, without the lease's wait; the writes that waited are redirected to
/// it, and a writer following the redirect loses nothing. Then the old
/// primary leaves.
#[test]
fn a_leaving_primary_hands_its_slots_to_its_replica() {
    let dir = temp_dir("handover");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--metrics", &http.to_string()]);
    let mut s = stuart.client();
    let (a, b, aid, bid) = primary_and_replica(&stuart, None);
    let line = |s: &mut Client, id: &str| s.text(&["STUART", "NODES"]).lines().find(|l| l.starts_with(id)).map(str::to_string).unwrap_or_default();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let incrs = {
        let (stop, real) = (stop.clone(), a.real);
        thread::spawn(move || {
            let mut c = Client::connect(real);
            let mut done = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let r = c.text(&["INCR", "{k}n"]);
                if r.starts_with(':') {
                    done += 1;
                    assert_eq!(r, format!(":{done}"), "no INCR lost or done twice");
                } else if r.starts_with("-MOVED ") {
                    c = Client::connect(b);
                } else {
                    panic!("{r}");
                }
            }
            done
        })
    };
    thread::sleep(Duration::from_millis(200));

    let left = Instant::now();
    assert_eq!(s.text(&["STUART", "LEAVE", &aid]), ":16385");
    wait_until("the replica owns the slots", || line(&mut s, &bid).contains(" slots=16384 ") && !line(&mut s, &bid).contains("replica-of"));
    let took = left.elapsed();
    assert!(took < Duration::from_secs(2), "{took:?}: no lease wait");
    wait_until("the old primary left", || s.text(&["STUART", "LEAVE", &aid]) == ":0");
    thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let done = incrs.join().unwrap();
    assert!(done > 0);
    let mut pb = Client::connect(b);
    assert_eq!(pb.text(&["GET", "{k}n"]), done.to_string());
    for i in 0..20 {
        assert_eq!(pb.text(&["GET", &format!("{{k}}{i}")]), i.to_string());
    }
    let m = scrape(http);
    assert_eq!((m.get("stuart_handovers_total"), m.get("stuart_failovers_total")), (Some(&1.0), Some(&0.0)));
    assert_eq!(m.get("stuart_slots_moved_total"), Some(&0.0), "no slot moved");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains(&format!("Primary {aid} leaving: handed its slots to replica {bid}")), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A scale-down that leaves more primaries than the nodes need (6 nodes
/// with `--replicas 1`, two replicas leaving: 3 primaries for 4 nodes,
/// two without a replica): one of those two is demoted, its slots move to
/// the others, and it becomes the replica of the other.
const D: &str = "dddddddddddddddddddddddddddddddddddddddd";
const E: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const F: &str = "ffffffffffffffffffffffffffffffffffffffff";

/// The node `id` is a replica of, as STUART NODES says.
fn primary_of(s: &mut Client, id: &str) -> Option<String> {
    let nodes = s.text(&["STUART", "NODES"]);
    let line = nodes.lines().find(|l| l.starts_with(id)).unwrap_or_default().to_string();
    line.split(' ').find_map(|w| w.strip_prefix("replica-of=")).map(str::to_string)
}

/// Fake nodes A to F joined to stuart `--auto --replicas 1`, evened out:
/// A, C and E primaries, B, D and F their replicas; C and E answer `INFO
/// memory` with `primary` and log their tables, taking each `delay(epoch)`.
/// Then F and D leave: three primaries where two do. The logs of C and E,
/// and the memories of A, C and E.
fn demoting_cluster(s: &mut Client, primary: String, delay: fn(u64) -> Duration) -> ([Log; 2], [Mem; 3]) {
    let (logs, mut mems) = ([Log::default(), Log::default()], Vec::new());
    for id in [A, B, C, D, E, F] {
        match id {
            C | E => {
                let m: Mem = std::sync::Arc::new(std::sync::Mutex::new(primary.clone()));
                let port = memory_node(id, delay, logs[usize::from(id == E)].clone(), Default::default(), m.clone());
                assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
                mems.push(m);
            }
            A => mems.push(join_with_memory(s, id, primary.clone())),
            _ => drop(join_with_memory(s, id, mem(100, 0, 50))),
        }
    }
    wait_until_long("even over A, C and E", Duration::from_secs(30), || {
        sorted_counts(s) == [0, 0, 0, 5461, 5461, 5462] && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    assert_eq!([B, D, F].map(|r| primary_of(s, r)), [A, C, E].map(|p| Some(p.to_string())));
    for id in [F, D] {
        assert_eq!(s.text(&["STUART", "LEAVE", id]), ":1");
        wait_until("the replica left", || s.text(&["STUART", "LEAVE", id]) == ":0");
    }
    (logs, mems.try_into().unwrap())
}

/// For the test below: C and E take tables slowly once set.
static SLOW_TABLES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn slow_tables(_: u64) -> Duration {
    if SLOW_TABLES.load(std::sync::atomic::Ordering::SeqCst) {
        Duration::from_millis(300)
    } else {
        Duration::ZERO
    }
}

#[test]
fn a_primary_the_nodes_dont_need_becomes_a_replica() {
    let dir = temp_dir("demote");
    let http: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1", "--metrics", &http.to_string()]);
    let mut s = stuart.client();
    // C and E take each table 300 ms after they got it, from before the
    // replicas leave.
    let (logs, _) = demoting_cluster(&mut s, mem(100, 0, 50), slow_tables);
    SLOW_TABLES.store(true, std::sync::atomic::Ordering::SeqCst);
    // Watched closely: listed demoting meanwhile, and when it became a replica.
    let (mut listed, mut gauge) = (false, false);
    let deadline = Instant::now() + Duration::from_secs(60);
    let (demoted, became) = loop {
        assert!(Instant::now() < deadline, "no demotion within 60 s");
        let nodes = s.text(&["STUART", "NODES"]);
        if nodes.contains(" demoting") && !listed {
            listed = true;
            gauge = scrape(http).get("stuart_nodes{state=\"demoting\"}") == Some(&1.0);
        }
        if let Some(x) = [C, E].into_iter().find(|x| nodes.lines().any(|l| l.starts_with(x) && l.contains(" replica-of="))) {
            break (x, Instant::now());
        }
        thread::sleep(Duration::from_millis(2));
    };
    SLOW_TABLES.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(listed && gauge, "listed demoting in STUART NODES {listed}, in the gauge {gauge}");
    // It became a replica only once it had taken a table that left it no
    // slots (as a primary still).
    let log = logs[usize::from(demoted == E)].lock().unwrap().clone();
    let held = log.iter().filter(|p| p.holds).map(|p| p.epoch).max().unwrap();
    let emptied = log.iter().filter(|p| p.epoch > held && !p.holds && !p.replica).min_by_key(|p| p.epoch).expect("a table leaving it nothing as a primary");
    assert!(emptied.acked <= became, "epoch {} taken {:?} after it became a replica", emptied.epoch, emptied.acked - became);
    let took_replica = || logs[usize::from(demoted == E)].lock().unwrap().iter().any(|p| p.replica);
    wait_until("it took the table making it a replica", took_replica);
    wait_until_long("C or E demoted: two primaries, each with a replica", Duration::from_secs(30), || {
        let nodes = s.text(&["STUART", "NODES"]);
        sorted_counts(&mut s) == [0, 0, 8192, 8192] && !nodes.contains(" demoting") && nodes.lines().filter(|l| l.contains(" replica-of=")).count() == 2
    });
    let kept = if demoted == C { E } else { C };
    assert_eq!(primary_of(&mut s, demoted), Some(kept.to_string()), "the replica of the other one lacking one");
    assert_eq!(primary_of(&mut s, B), Some(A.to_string()), "B stays A's");
    let m = scrape(http);
    assert_eq!(m.get("stuart_demotions_total"), Some(&1.0));
    assert_eq!(m.get("stuart_nodes{state=\"demoting\"}"), Some(&0.0));
    assert!(m.get("stuart_slots_moved_total").is_some_and(|&n| n >= 5461.0));
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains(&format!("Primary {demoted} becomes a replica, the nodes needing fewer primaries")), "{log}");
    assert!(log.contains(&format!("Node {demoted} is now a replica of {kept}")), "{log}");
    // Nothing more to change.
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(scrape(http).get("stuart_demotions_total"), Some(&1.0));
    let _ = std::fs::remove_dir_all(dir);
}

/// A primary is demoted only if the other primaries have room for its
/// keys (the replicas' room doesn't count: they take no slots).
#[test]
fn a_primary_is_demoted_only_if_the_others_have_room() {
    let dir = temp_dir("demote-room");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    // 200 bytes of room each below 90%, 500 of keys: two others have 400.
    let (_, [a, _, _]) = demoting_cluster(&mut s, mem(700, 1000, 500), |_| Duration::ZERO);
    thread::sleep(Duration::from_millis(3000));
    assert!(!s.text(&["STUART", "NODES"]).contains(" demoting"));
    assert_eq!(sorted_counts(&mut s), [0, 5461, 5461, 5462], "D and F gone, the slots where they were");
    *a.lock().unwrap() = mem(100, 1000, 50);
    wait_until_long("C or E demoted", Duration::from_secs(30), || {
        sorted_counts(&mut s) == [0, 0, 8192, 8192] && s.text(&["STUART", "NODES"]).lines().filter(|l| l.contains(" replica-of=")).count() == 2
    });
    let _ = std::fs::remove_dir_all(dir);
}

/// A replica leaving holds nothing: it goes at once, and its primary writes
/// alone from the table without it.
#[test]
fn a_leaving_replica_just_goes() {
    let dir = temp_dir("replica-leaves");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--auto", "--replicas", "1"]);
    let mut s = stuart.client();
    let (a, _b, _aid, bid) = primary_and_replica(&stuart, None);
    let left = Instant::now();
    assert_eq!(s.text(&["STUART", "LEAVE", &bid]), ":1");
    wait_until("it left", || s.text(&["STUART", "LEAVE", &bid]) == ":0");
    assert!(left.elapsed() < Duration::from_secs(2), "{:?}", left.elapsed());
    assert!(!s.text(&["STUART", "NODES"]).contains(&bid));
    let mut pa = Client::connect(a.real);
    wait_until("its primary has no replica", || pa.text(&["INFO", "replication"]).contains("connected_slaves:0"));
    assert_eq!(pa.text(&["SET", "{k}alone", "v"]), "+OK");
    let _ = std::fs::remove_dir_all(dir);
}

/// A replica's keys are copies: it may leave whatever room the others have.
#[test]
fn a_replica_leaves_whatever_room_the_others_have() {
    let dir = temp_dir("replica-room");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1"]);
    let mut s = stuart.client();
    join_with_memory(&mut s, A, mem(600, 1000, 500));
    join_with_memory(&mut s, B, mem(600, 1000, 500));
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("both took the table", || up_and_acked(&mut s, 1) == 2);
    assert!(s.text(&["STUART", "NODES"]).contains(&format!(" replica-of={A}")));
    assert_eq!(s.text(&["STUART", "LEAVE", B]), ":1");
    wait_until("it left", || s.text(&["STUART", "LEAVE", B]) == ":0");
    let _ = std::fs::remove_dir_all(dir);
}

/// Fake nodes A and B, primaries, and C, A's replica (`--replicas 1`),
/// each as `lags` says; the table made. Their logs.
fn handing_cluster(s: &mut Client, lags: [Lag; 3]) -> Vec<Log> {
    handing_cluster_with(s, lags, |_| Duration::ZERO)
}

/// The same, C taking each table after `c_delay(epoch)`.
fn handing_cluster_with(s: &mut Client, lags: [Lag; 3], c_delay: fn(u64) -> Duration) -> Vec<Log> {
    let mut logs = Vec::new();
    for (id, lag) in [A, B, C].into_iter().zip(lags) {
        let log: Log = Default::default();
        let delay = if id == C { c_delay } else { |_| Duration::ZERO };
        let port = lagging_node(id, delay, log.clone(), lag.down.clone(), Default::default(), lag);
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
        logs.push(log);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(s, 1) == 3);
    assert!(s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(C) && l.contains(&format!(" replica-of={A}"))));
    logs
}

/// A replica that missed the paused primary's offset: the pause is lifted,
/// and it is paused for again; this time the replica catches up. The table
/// handing it the slots reaches it first; no slot ever moves.
#[test]
fn a_handover_is_tried_again_and_reaches_the_replica_first() {
    let dir = temp_dir("handover-retry");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1"]);
    let mut s = stuart.client();
    let (a, c) = (Lag::default(), Lag::behind());
    let logs = handing_cluster(&mut s, [a.clone(), Lag::default(), c.clone()]);
    let before = epoch_of(&mut s);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until("the first pause is lifted", || Lag::count(&a.resumes) == 1);
    c.behind.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until("C owns A's slots", || s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(C) && l.split(' ').any(|w| w == "slots=8192")));
    assert_eq!((Lag::count(&a.pauses), Lag::count(&a.resumes)), (2, 1));
    let paused = a.paused.lock().unwrap().clone();
    let (lifted, again) = (paused.iter().find(|p| !p.1).unwrap().0, paused.iter().rfind(|p| p.1).unwrap().0);
    assert!(again.duration_since(lifted) >= Duration::from_millis(100), "paused again {:?} after the pause was lifted", again.duration_since(lifted));
    let handed = before + 1;
    // The state shows the handover before the table is pushed.
    wait_until("A and C got the handover table", || got(&logs[2], handed).is_some() && got(&logs[0], handed).is_some());
    let (at_c, at_a) = (got(&logs[2], handed).unwrap(), got(&logs[0], handed).unwrap());
    assert!(at_c.acked <= at_a.got, "A got the handover table before C took it");
    wait_until("A left", || s.text(&["STUART", "LEAVE", A]) == ":0");
    assert!(logs.iter().all(|l| l.lock().unwrap().iter().all(|p| p.migrating.is_empty())), "no slot moved");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains(&format!("replica {C} didn't reach offset 100 within 1000 ms; trying again")), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A slot of a primary handing over, queued to move meanwhile (`STUART
/// MOVE`): it doesn't move from the primary; once the replica has it, it
/// moves from there.
#[test]
fn a_handing_primary_s_slots_queued_meanwhile_wait() {
    let dir = temp_dir("handover-queued");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1"]);
    let mut s = stuart.client();
    let (a, c) = (Lag::default(), Lag::behind());
    let logs = handing_cluster(&mut s, [a.clone(), Lag::default(), c.clone()]);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until("A paused", || Lag::count(&a.pauses) == 1);
    let slot = s.text(&["STUART", "TABLE"]).lines().find(|l| l.starts_with(A)).and_then(|l| l.split(' ').next_back()?.split('-').next().map(str::to_string)).unwrap();
    assert_eq!(s.text(&["STUART", "MOVE", &slot, &slot, B]), ":1");
    // A missed pause (1 s), and the next one under way: the slot is
    // queued all that time.
    thread::sleep(Duration::from_millis(1500));
    c.behind.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until("C owns A's slots", || holds(&mut s, C, 8192) || holds(&mut s, C, 8191));
    wait_until("the slot moved to B", || s.text(&["STUART", "MIGRATIONS"]).is_empty() && holds(&mut s, C, 8191));
    for log in &logs {
        let moved = log.lock().unwrap().iter().find(|p| p.migrating.iter().any(|m| m.1 == A)).map(|p| p.epoch);
        assert_eq!(moved, None, "a slot moved from A, handing over");
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// A replica that never catches up: after 5 pauses missed, the leaving
/// primary's slots are drained instead, as without replicas.
#[test]
fn a_handover_that_keeps_missing_drains_instead() {
    let dir = temp_dir("handover-drain");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1", "--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    let a = Lag::default();
    handing_cluster(&mut s, [a.clone(), Lag::default(), Lag::behind()]);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until_long("A left, drained", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    assert_eq!((Lag::count(&a.pauses), Lag::count(&a.resumes)), (5, 5));
    let nodes = s.text(&["STUART", "NODES"]);
    assert!(nodes.lines().any(|l| l.starts_with(B) && l.split(' ').any(|w| w == "slots=16384")), "{nodes}");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains("(5 times)); draining them instead, 8192 slots to move"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Whether `STUART NODES` gives `id` `slots` slots.
fn holds(s: &mut Client, id: &str, slots: usize) -> bool {
    s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(id) && l.split(' ').any(|w| w == format!("slots={slots}")))
}

/// A replica still copying (syncing from scratch) is waited for: no
/// pause, nothing drained; after 60 s, the slots are drained instead.
#[test]
fn a_handover_waits_for_a_replica_still_copying() {
    let dir = temp_dir("handover-copying");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1", "--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    let (a, c) = (Lag::default(), Lag::default());
    c.copying.store(true, std::sync::atomic::Ordering::SeqCst);
    handing_cluster(&mut s, [a.clone(), Lag::default(), c]);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    thread::sleep(Duration::from_secs(3));
    assert_eq!(Lag::count(&a.pauses), 0, "paused for a replica still copying");
    assert!(holds(&mut s, A, 8192) && s.text(&["STUART", "MIGRATIONS"]).is_empty(), "drained at once");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains("its replica is still copying; waiting up to 60 s to hand over"), "{log}");
    wait_until_long("A left, drained", Duration::from_secs(80), || s.text(&["STUART", "LEAVE", A]) == ":0");
    assert!(holds(&mut s, B, 16384));
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains("(its replicas are still copying after 60 s); draining them instead"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A replica chosen with a complete copy that, while the primary is
/// paused, says it's copying again (its link restarted from scratch) at
/// the offset: not promoted, the pause lifted; then it's waited for.
#[test]
fn a_replica_copying_again_mid_catch_up_is_not_promoted() {
    let dir = temp_dir("handover-recopy");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1"]);
    let mut s = stuart.client();
    let (a, c) = (Lag::default(), Lag::default());
    handing_cluster(&mut s, [a.clone(), Lag::default(), c.clone()]);
    // Complete when chosen, copying from the next FENCE on.
    c.complete_fences.store(Lag::count(&c.fences) + 1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until("the pause is lifted", || Lag::count(&a.resumes) == 1);
    thread::sleep(Duration::from_millis(1500));
    assert_eq!(Lag::count(&a.pauses), 1, "paused again for a replica copying");
    assert!(holds(&mut s, A, 8192), "{}", s.text(&["STUART", "NODES"]));
    let _ = std::fs::remove_dir_all(dir);
}

/// C slow to take tables from the epoch in `SLOW_*` on (1.8 s, within
/// stuart's 2 s timeout): a replica that has left but isn't removed yet.
static SLOW_LEAVING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);
static SLOW_LEFT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);

fn slow_leaving(epoch: u64) -> Duration {
    if epoch >= SLOW_LEAVING.load(std::sync::atomic::Ordering::SeqCst) { Duration::from_millis(1800) } else { Duration::ZERO }
}

fn slow_left(epoch: u64) -> Duration {
    if epoch >= SLOW_LEFT.load(std::sync::atomic::Ordering::SeqCst) { Duration::from_millis(1800) } else { Duration::ZERO }
}

/// Makes replica C leave and stay leaving a while: D joins (B's replica),
/// C is slow to take that table, and C's LEAVE.
fn c_leaves_slowly(s: &mut Client, slow: &std::sync::atomic::AtomicU64) {
    const D: &str = "dddddddddddddddddddddddddddddddddddddddd";
    slow.store(epoch_of(s) + 1, std::sync::atomic::Ordering::SeqCst);
    let port = lagging_node(D, |_| Duration::ZERO, Default::default(), Default::default(), Default::default(), Lag::default());
    assert_eq!(s.text(&["STUART", "JOIN", D, "127.0.0.1", &port]), "+OK");
    assert_eq!(s.text(&["STUART", "LEAVE", C]), ":1");
}

/// A primary handing over whose replica, still copying, then leaves:
/// no replica can take over, its slots are drained.
#[test]
fn a_handover_whose_replica_leaves_drains_instead() {
    let dir = temp_dir("handover-replica-leaves");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1", "--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    let c = Lag::default();
    c.copying.store(true, std::sync::atomic::Ordering::SeqCst);
    handing_cluster_with(&mut s, [Lag::default(), Lag::default(), c.clone()], slow_leaving);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    c_leaves_slowly(&mut s, &SLOW_LEAVING);
    // It could take over now, were it not leaving.
    c.copying.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until_long("A left, drained", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(!log.contains("handed its slots to replica"), "{log}");
    assert!(log.contains("can't hand its slots to a replica (no replica up, not marked failed nor leaving"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A primary whose only replica is leaving leaves as without replicas:
/// drained.
#[test]
fn a_primary_whose_replica_is_leaving_is_drained() {
    let dir = temp_dir("leave-replica-leaving");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1", "--moves-per-source", "10000", "--max-moves", "10000"]);
    let mut s = stuart.client();
    handing_cluster_with(&mut s, [Lag::default(), Lag::default(), Lag::default()], slow_left);
    c_leaves_slowly(&mut s, &SLOW_LEFT);
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until_long("A left, drained", Duration::from_secs(30), || s.text(&["STUART", "LEAVE", A]) == ":0");
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(log.contains(&format!("Node {A} is leaving: it holds 8192 slots")), "{log}");
    assert!(!log.contains("hands its 8192 slots to a replica"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A primary down while it's to hand over is the failover's: its replica
/// promoted after --fail-after and the lease, nothing paused or drained.
#[test]
fn a_handing_primary_down_fails_over() {
    let dir = temp_dir("handover-down");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--replicas", "1", "--fail-after", "2"]);
    let mut s = stuart.client();
    let a = Lag::default();
    handing_cluster(&mut s, [a.clone(), Lag::default(), Lag::default()]);
    a.down.store(true, std::sync::atomic::Ordering::SeqCst);
    wait_until_long("A is down", Duration::from_secs(10), || s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(A) && l.contains(" down ")));
    assert_eq!(s.text(&["STUART", "LEAVE", A]), ":8193");
    wait_until_long("C owns A's slots", Duration::from_secs(30), || holds(&mut s, C, 8192));
    assert!(holds(&mut s, B, 8192));
    let log = std::fs::read_to_string(&stuart.2).unwrap();
    assert!(!log.contains("can't hand its slots"), "{log}");
    let _ = std::fs::remove_dir_all(dir);
}
