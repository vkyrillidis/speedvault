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
        let child = Command::new(env!("CARGO_BIN_EXE_stuart"))
            .args([&format!("127.0.0.1:{port}"), "--state", state.to_str().unwrap(), "--loglevel", "notice"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        wait_until("stuart listens", || TcpStream::connect(addr).is_ok());
        let stuart = Stuart(child, addr, state.with_extension("log"));
        let info = stuart.client().text(&["INFO"]);
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
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = dbnode::Config {
        cluster: Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() }),
        stuart: Some(stuart.to_string()),
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
    let out = Command::new(env!("CARGO_BIN_EXE_stuart")).args(["127.0.0.1:0", "--state", state.to_str().unwrap()]).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("the first line isn't"), "{err}");
    let out = Command::new(env!("CARGO_BIN_EXE_stuart")).output().unwrap();
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
}

impl Follower {
    fn new(seed: SocketAddr) -> Follower {
        Follower { conns: Default::default(), slots: Default::default(), seed: seed.to_string(), redirects: 0 }
    }

    fn conn(&mut self, addr: &str) -> &mut Client {
        self.conns.entry(addr.to_string()).or_insert_with(|| Client::connect(addr.parse().unwrap()))
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
    let stuart = Stuart::start(port, &state);
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

/// A table a fake node got: its epoch, its MIGRATING section (as slot,
/// from, to), when it arrived and when the node acknowledged it.
#[derive(Clone, Debug)]
struct Pushed {
    epoch: u64,
    migrating: Vec<(String, String, String)>,
    got: Instant,
    acked: Instant,
}

/// A fake node that answers like a dbnode for stuart: `CLUSTER MYID`,
/// `PING`, `CLUSTER PENDING` (always 0: no keys), and `CLUSTER SETTABLE`,
/// acknowledging a table `delay(epoch)` after it arrived. Every table goes
/// to `log`.
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
    let fake = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = fake.local_addr().unwrap().port().to_string();
    let is_down = move || down.load(std::sync::atomic::Ordering::SeqCst);
    thread::spawn(move || {
        for conn in fake.incoming() {
            let conn = conn.unwrap();
            if is_down() {
                continue;
            }
            let log = log.clone();
            let is_down = is_down.clone();
            let memory = memory.clone();
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
                        ("CLUSTER", Some("PENDING")) => ":0\r\n".to_string(),
                        ("CLUSTER", Some("SETTABLE")) => {
                            let got = Instant::now();
                            let epoch: u64 = args[2].parse().unwrap();
                            thread::sleep(delay(epoch));
                            let migrating = match args.iter().position(|a| a == "MIGRATING") {
                                Some(i) => args[i + 2..].chunks(3).map(|c| (c[0].clone(), c[1].clone(), c[2].clone())).collect(),
                                None => Vec::new(),
                            };
                            log.lock().unwrap().push(Pushed { epoch, migrating, got, acked: Instant::now() });
                            "+OK\r\n".to_string()
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

/// A table that starts or ends a move reaches the target first: the
/// source and the others get it only once the target took it, slow as it
/// is (here half a second per table).
#[test]
fn the_target_takes_each_table_first() {
    let dir = temp_dir("first");
    let stuart = Stuart::start(free_port(), &dir.join("state"));
    let mut s = stuart.client();
    let logs = slow_cluster(&mut s, &[|_| Duration::ZERO, |e| if e > 1 { Duration::from_millis(500) } else { Duration::ZERO }, |_| Duration::ZERO]);
    // 0-5460 are A's: slot 5 moves to B.
    assert_eq!(s.text(&["STUART", "MOVE", "5", "5", B]), ":1");
    wait_until("the move is done", || s.text(&["STUART", "MIGRATIONS"]).is_empty() && got(&logs[0], 3).is_some());
    for epoch in [2, 3] {
        let at_b = got(&logs[1], epoch).unwrap();
        for (other, log) in [("A", &logs[0]), ("C", &logs[2])] {
            let p = got(log, epoch).unwrap_or_else(|| panic!("{other} never got epoch {epoch}"));
            assert!(p.got >= at_b.acked, "{other} got epoch {epoch} before B took it");
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

    assert_eq!(Client::connect(nodes[0]).text(&["DBSIZE"]), ":0", "the drained node has no keys");
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

/// A target that goes down holds up only its own moves: a table that must
/// reach it first reaches the others after a while, its moves stop
/// counting against `--moves-per-source`, and other moves out of the same
/// source go on.
#[test]
fn a_target_down_holds_up_only_its_moves() {
    let dir = temp_dir("target-down");
    let stuart = Stuart::start_with(free_port(), &dir.join("state"), &["--moves-per-source", "2"]);
    let mut s = stuart.client();
    let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut logs = Vec::new();
    for (id, delay) in [(A, (|_| Duration::ZERO) as fn(u64) -> Duration), (B, |_| Duration::ZERO), (C, |e| if e >= 2 { Duration::from_secs(1) } else { Duration::ZERO })] {
        let log: Log = Default::default();
        let port = slow_node_down(id, delay, log.clone(), if id == C { down.clone() } else { Default::default() });
        assert_eq!(s.text(&["STUART", "JOIN", id, "127.0.0.1", &port]), "+OK");
        logs.push(log);
    }
    assert_eq!(s.text(&["STUART", "CREATE"]), "+OK");
    wait_until("they took the table", || up_and_acked(&mut s, 1) == 3);
    // Two of A's slots to C, which goes down while it takes that table.
    assert_eq!(s.text(&["STUART", "MOVE", "0", "1", C]), ":2");
    thread::sleep(Duration::from_millis(300));
    down.store(true, std::sync::atomic::Ordering::SeqCst);
    wait_until("C is down", || s.text(&["STUART", "NODES"]).lines().any(|l| l.starts_with(C) && l.contains(" down ")));
    // Four more of A's slots, to B: they move, with C's two still under way.
    assert_eq!(s.text(&["STUART", "MOVE", "2", "5", B]), ":4");
    wait_until_long("A's slots reached B", Duration::from_secs(30), || {
        let t = s.text(&["STUART", "TABLE"]);
        t.lines().any(|l| l.starts_with(B) && l.contains(" 2-5 "))
    });
    let m = s.text(&["STUART", "MIGRATIONS"]);
    assert_eq!(m.lines().filter(|l| l.contains(&format!("-> {C}"))).count(), 2, "{m}");
    for log in &logs[..2] {
        let with_c = log.lock().unwrap().iter().any(|p| p.migrating.iter().any(|m| m.2 == C));
        assert!(with_c, "A and B got the tables C had to take first");
    }
    // C comes back: its moves end.
    down.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_until("C's moves ended", || s.text(&["STUART", "MIGRATIONS"]).is_empty());
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
    wait_until("A left", || s.text(&["STUART", "LEAVE", A]) == ":0");
    let epoch = epoch_of(&mut s);
    assert!(got(&logs[0], epoch - 1).is_some(), "A was removed in epoch {epoch} before it took epoch {}", epoch - 1);
    assert_eq!(slot_counts(&mut s), [16384]);
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
    wait_until("A and B even", || slot_counts(&mut s) == [8192, 8192] && s.text(&["STUART", "MIGRATIONS"]).is_empty());
    // C joins at A's address: A is replaced, its 8192 slots lost. (A's fake
    // still answers there, so C is down; it gets the slots all the same.)
    assert_eq!(s.text(&["STUART", "JOIN", C, "127.0.0.1", &a]), "+OK");
    wait_until("the lost slots given out, and the nodes even", || {
        let nodes = s.text(&["STUART", "NODES"]);
        !nodes.contains("unassigned") && slot_counts(&mut s) == [8192, 8192] && s.text(&["STUART", "MIGRATIONS"]).is_empty()
    });
    let _ = std::fs::remove_dir_all(dir);
}

/// A table that starts moves to B and to C, C going down before it took
/// it: A, the source, gets it once C has been down a while, so that the
/// move to B can end (it waits for the source to take the table).
#[test]
fn a_table_for_a_down_target_reaches_the_others_later() {
    let dir = temp_dir("first-down");
    let state = dir.join("state");
    let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut ports = Vec::new();
    for id in [A, B, C] {
        let delay: fn(u64) -> Duration = if id == C { |e| if e >= 2 { Duration::from_secs(1) } else { Duration::ZERO } } else { |_| Duration::ZERO };
        ports.push(slow_node_down(id, delay, Default::default(), if id == C { down.clone() } else { Default::default() }));
    }
    // Both moves queued before stuart starts: one table starts them.
    let text = format!(
        "stuart-state 1\nnode {A} 127.0.0.1 {}\nnode {B} 127.0.0.1 {}\nnode {C} 127.0.0.1 {}\ntable 1\n\
         range 0 5460 {A}\nrange 5461 10922 {B}\nrange 10923 16383 {C}\nqueue 0 {C}\nqueue 1 {B}\n",
        ports[0], ports[1], ports[2]
    );
    std::fs::write(&state, text).unwrap();
    let stuart = Stuart::start(free_port(), &state);
    let mut s = stuart.client();
    wait_until("both moves started", || s.text(&["STUART", "MIGRATIONS"]).lines().count() == 2);
    thread::sleep(Duration::from_millis(300));
    down.store(true, std::sync::atomic::Ordering::SeqCst);
    let started = Instant::now();
    wait_until("slot 1 is B's", || s.text(&["STUART", "TABLE"]).lines().any(|l| l.starts_with(B) && l.contains(" 1 ")));
    eprintln!("slot 1 moved {:?} after C went down", started.elapsed());
    let m = s.text(&["STUART", "MIGRATIONS"]);
    assert!(m.starts_with(&format!("0 {A} -> {C} ")), "{m}");
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
    wait_until("A left", || s.text(&["STUART", "LEAVE", A]) == ":0");
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
        ("stuart_nodes{state=\"down\"}", 0.0),
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
