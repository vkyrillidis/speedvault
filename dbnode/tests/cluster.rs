//! The dbnode in cluster mode over real TCP: the table it starts with,
//! `CLUSTER SETTABLE` and its epoch rules, and the slot check (`MOVED`,
//! `CROSSSLOT`, `CLUSTERDOWN`); a node the table makes a replica (`ROLE`,
//! `INFO replication`). The errors are Redis 7.0.15's (a cluster with
//! `cluster-require-full-coverage no`), checked with redis-cli.

mod common;

use cluster::key_hash_slot;
use common::*;

/// Another node of the table; nothing listens there.
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccc";

// foo is in slot 12182, bar in 5061.
const MOVED_FOO: &[u8] = b"-MOVED 12182 10.0.0.2:7000\r\n";
const CROSSSLOT: &[u8] = b"-CROSSSLOT Keys in request don't hash to the same slot\r\n";
const CLUSTERDOWN: &[u8] = b"-CLUSTERDOWN Hash slot not served\r\n";

/// A node in cluster mode, a client, and the node's id.
fn node() -> (Client, String) {
    let mut c = Client::connect(start_cluster());
    c.cmd(&[b"CLUSTER", b"MYID"]);
    let id = c.read_text();
    assert_eq!(id.len(), 40);
    assert!(id.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()), "{id}");
    (c, id)
}

/// `CLUSTER SETTABLE epoch NODES ... RANGES ...`.
fn settable(epoch: &str, nodes: &[(&str, &str, &str)], ranges: &[(&str, &str, &str)]) -> Vec<Vec<u8>> {
    let mut args: Vec<Vec<u8>> = vec![b"CLUSTER".to_vec(), b"SETTABLE".to_vec(), epoch.into()];
    args.push(b"NODES".to_vec());
    args.push(nodes.len().to_string().into());
    for (id, host, port) in nodes {
        args.extend([id.as_bytes().to_vec(), host.as_bytes().to_vec(), port.as_bytes().to_vec()]);
    }
    args.push(b"RANGES".to_vec());
    args.push(ranges.len().to_string().into());
    for (start, end, owner) in ranges {
        args.extend([start.as_bytes().to_vec(), end.as_bytes().to_vec(), owner.as_bytes().to_vec()]);
    }
    args
}

fn call_vec(c: &mut Client, args: &[Vec<u8>], expected: &[u8]) {
    let args: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
    c.call(&args, expected);
}

/// The first key `prefix0`, `prefix1`, ... whose slot is in `slots`.
fn key_in(slots: std::ops::RangeInclusive<u16>, prefix: &str) -> Vec<u8> {
    (0..).map(|i| format!("{prefix}{i}").into_bytes()).find(|k| slots.contains(&key_hash_slot(k))).unwrap()
}

/// This node owns 0-8191, B owns 8192-16000, 16001-16383 are unassigned.
fn split(c: &mut Client, me: &str, port: u16) {
    let port = port.to_string();
    let table = settable(
        "1",
        &[(me, "127.0.0.1", &port), (B, "10.0.0.2", "7000")],
        &[("0", "8191", me), ("8192", "16000", B), ("16001", "16383", "-")],
    );
    call_vec(c, &table, OK);
}

#[test]
fn before_a_table_nothing_is_served() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    // At once: only a node joining stuart waits for its first table.
    let asked = std::time::Instant::now();
    c.call(&[b"GET", b"foo"], CLUSTERDOWN);
    assert!(asked.elapsed() < std::time::Duration::from_millis(500), "{:?}", asked.elapsed());
    c.call(&[b"SET", b"bar", b"1"], CLUSTERDOWN);
    c.call(&[b"PING"], b"+PONG\r\n");
    c.call(&[b"DBSIZE"], b":0\r\n");
    c.call(&[b"CLUSTER", b"SLOTS"], b"*0\r\n");
    // It knows itself, owning nothing, as a fresh Redis node does.
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert_eq!(c.read_text(), format!("{me} 127.0.0.1:{port}@{port} myself,master - 0 0 0 connected\n"));
    let shards = format!(
        "*1\r\n*4\r\n$5\r\nslots\r\n*0\r\n$5\r\nnodes\r\n*1\r\n*14\r\n$2\r\nid\r\n$40\r\n{me}\r\n$4\r\nport\r\n:{port}\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n\
         $8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n$6\r\nonline\r\n"
    );
    c.call(&[b"CLUSTER", b"SHARDS"], shards.as_bytes());
    c.cmd(&[b"CLUSTER", b"INFO"]);
    assert_eq!(
        c.read_text(),
        "cluster_state:fail\r\ncluster_slots_assigned:0\r\ncluster_slots_ok:0\r\ncluster_slots_pfail:0\r\n\
         cluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:0\r\ncluster_current_epoch:0\r\n\
         cluster_my_epoch:0\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n\
         total_cluster_links_buffer_limit_exceeded:0\r\n"
    );
}

#[test]
fn keys_are_routed_by_slot() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    split(&mut c, &me, port);
    let unassigned = key_in(16001..=16383, "u");

    c.call(&[b"SET", b"bar", b"1"], OK);
    c.call(&[b"GET", b"bar"], b"$1\r\n1\r\n");
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"TYPE", b"foo"], MOVED_FOO);
    c.call(&[b"DEL", b"foo"], MOVED_FOO);
    c.call(&[b"GET", &unassigned], CLUSTERDOWN);

    // All keys in one slot: the same key twice, or one hash tag.
    c.call(&[b"MGET", b"bar", b"bar"], b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n");
    c.call(&[b"MSET", b"{bar}x", b"2", b"{bar}y", b"3"], OK);
    c.call(&[b"MGET", b"{bar}x", b"{bar}y", b"{bar}z"], b"*3\r\n$1\r\n2\r\n$1\r\n3\r\n$-1\r\n");
    c.call(&[b"EXISTS", b"{bar}x", b"{bar}y"], b":2\r\n");
    c.call(&[b"MGET", b"{foo}x", b"{foo}y"], MOVED_FOO);

    // Every key is checked before MOVED, and as in Redis 8 before an
    // unassigned first key's CLUSTERDOWN.
    c.call(&[b"MGET", b"bar", b"foo"], CROSSSLOT);
    c.call(&[b"MGET", b"foo", b"bar"], CROSSSLOT);
    c.call(&[b"DEL", b"bar", b"{bar}x", b"x"], CROSSSLOT);
    c.call(&[b"MGET", &unassigned, b"foo"], CROSSSLOT);
    c.call(&[b"MGET", &unassigned, &unassigned], CLUSTERDOWN);
    c.call(&[b"MGET", b"bar", &unassigned], CROSSSLOT);
    // MSET's keys are every other argument; its own arity rule comes after.
    c.call(&[b"MSET", b"bar", b"1", b"foo"], CROSSSLOT);
    c.call(&[b"MSET", b"bar", b"foo", b"bar"], b"-ERR wrong number of arguments for 'mset' command\r\n");
    c.call(&[b"MSET", b"bar", b"foo", b"bar", b"1"], OK);
    c.call(&[b"GET", b"bar"], b"$1\r\n1\r\n");
    // The arity check comes first.
    c.call(&[b"GET"], b"-ERR wrong number of arguments for 'get' command\r\n");
    c.call(&[b"GET", b"foo", b"x"], b"-ERR wrong number of arguments for 'get' command\r\n");

    // Keyless commands act on this node.
    c.call(&[b"DBSIZE"], b":3\r\n");
    c.call(&[b"ECHO", b"foo"], b"$3\r\nfoo\r\n");
    c.call(&[b"SELECT", b"0"], OK);
    c.call(&[b"SELECT", b"1"], b"-ERR SELECT is not allowed in cluster mode\r\n");
    c.call(&[b"SELECT", b"x"], b"-ERR value is not an integer or out of range\r\n");

    // A pipeline carries on past a redirect.
    c.send(&[request(&[b"GET", b"foo"]), request(&[b"GET", b"bar"]), request(&[b"INCR", b"foo"])].concat());
    c.expect(&[MOVED_FOO, b"$1\r\n1\r\n", MOVED_FOO].concat());

    // A refused command isn't counted as processed.
    c.cmd(&[b"INFO", b"stats"]);
    let before = c.read_text();
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.cmd(&[b"INFO", b"stats"]);
    let after = c.read_text();
    let processed = |s: &str| -> u64 {
        s.lines().find_map(|l| l.strip_prefix("total_commands_processed:")).unwrap().parse().unwrap()
    };
    assert_eq!(processed(&after), processed(&before) + 1, "only the INFO counts");
}

/// A transaction in cluster mode, as Redis 8 takes it: each command is
/// checked as it's queued (another node's key is redirected, and aborts the
/// transaction), and `EXEC` as one command of all their keys (two slots:
/// `CROSSSLOT`, nothing run, the transaction gone).
#[test]
fn transactions_are_routed_as_a_whole() {
    const EXECABORT: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    split(&mut c, &me, port);
    let other_slot = key_in(0..=100, "k");

    c.call(&[b"MULTI"], OK);
    c.call(&[b"SET", b"{bar}x", b"1"], b"+QUEUED\r\n");
    c.call(&[b"GET", b"{bar}y"], b"+QUEUED\r\n");
    c.call(&[b"PING"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], b"*3\r\n+OK\r\n$-1\r\n+PONG\r\n");

    c.call(&[b"MULTI"], OK);
    c.call(&[b"SET", b"{bar}x", b"2"], b"+QUEUED\r\n");
    c.call(&[b"SET", b"foo", b"2"], MOVED_FOO);
    c.call(&[b"MGET", b"bar", &other_slot], CROSSSLOT);
    c.call(&[b"EXEC"], EXECABORT);

    c.call(&[b"MULTI"], OK);
    c.call(&[b"SET", b"{bar}x", b"3"], b"+QUEUED\r\n");
    c.call(&[b"SET", &other_slot, b"3"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], CROSSSLOT);
    c.call(&[b"EXEC"], b"-ERR EXEC without MULTI\r\n");
    c.call(&[b"MGET", b"{bar}x", b"{bar}x"], b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n");

    // Without keys: served here.
    c.call(&[b"MULTI"], OK);
    c.call(&[b"PING"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], b"*1\r\n+PONG\r\n");

    // WATCH is checked as any command with keys.
    c.call(&[b"WATCH", b"foo"], MOVED_FOO);
    c.call(&[b"WATCH", b"bar", &other_slot], CROSSSLOT);
    c.call(&[b"WATCH", b"bar", b"{bar}x"], OK);
    c.call(&[b"MULTI"], OK);
    c.call(&[b"INCR", b"{bar}x"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], b"*1\r\n:2\r\n");
}

#[test]
fn cluster_commands() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    split(&mut c, &me, port);

    c.call(&[b"CLUSTER", b"KEYSLOT", b"foo"], b":12182\r\n");
    c.call(&[b"cluster", b"keyslot", b"user{42}:name"], b":8000\r\n");
    c.call(&[b"CLUSTER", b"KEYSLOT", b"{user42}:name"], b":14710\r\n");
    c.call(&[b"CLUSTER", b"KEYSLOT"], b"-ERR wrong number of arguments for 'cluster|keyslot' command\r\n");
    c.call(&[b"CLUSTER", b"MYID", b"x"], b"-ERR wrong number of arguments for 'cluster|myid' command\r\n");
    c.call(&[b"CLUSTER", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try CLUSTER HELP.\r\n");

    c.call(&[b"MSET", b"{bar}1", b"a", b"{bar}2", b"b"], OK);
    c.call(&[b"SET", b"bar", b"c"], OK);
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"5061"], b":3\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"12182"], b":0\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"16383"], b":0\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"16384"], b"-ERR Invalid slot\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"-1"], b"-ERR Invalid slot\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"x"], b"-ERR value is not an integer or out of range\r\n");

    // GETKEYSINSLOT: in no particular order, at most `count`.
    c.cmd(&[b"CLUSTER", b"GETKEYSINSLOT", b"5061", b"10"]);
    assert_eq!(c.read_line(), b"*3\r\n");
    let mut keys: Vec<Vec<u8>> = (0..3).map(|_| c.read_bulk()).collect();
    keys.sort();
    assert_eq!(keys, [b"bar".to_vec(), b"{bar}1".to_vec(), b"{bar}2".to_vec()]);
    c.cmd(&[b"CLUSTER", b"GETKEYSINSLOT", b"5061", b"2"]);
    assert_eq!(c.read_line(), b"*2\r\n");
    let two = [c.read_bulk(), c.read_bulk()];
    assert!(two.iter().all(|k| keys.contains(k)) && two[0] != two[1], "{two:?}");
    c.call(&[b"CLUSTER", b"GETKEYSINSLOT", b"5061", b"0"], b"*0\r\n");
    c.call(&[b"CLUSTER", b"GETKEYSINSLOT", b"12182", b"5"], b"*0\r\n");
    c.call(&[b"DEL", b"{bar}1"], b":1\r\n");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"5061"], b":2\r\n");
    for (slot, count) in [("16384", "1"), ("-1", "1"), ("0", "-1")] {
        c.call(&[b"CLUSTER", b"GETKEYSINSLOT", slot.as_bytes(), count.as_bytes()], b"-ERR Invalid slot or number of keys\r\n");
    }
    for (slot, count) in [("x", "1"), ("0", "x"), ("x", "-1"), ("16384", "x")] {
        c.call(&[b"CLUSTER", b"GETKEYSINSLOT", slot.as_bytes(), count.as_bytes()], b"-ERR value is not an integer or out of range\r\n");
    }
    c.call(&[b"CLUSTER", b"GETKEYSINSLOT", b"0"], b"-ERR wrong number of arguments for 'cluster|getkeysinslot' command\r\n");

    for cmd in [&b"READONLY"[..], b"READWRITE", b"ASKING"] {
        c.call(&[cmd], OK);
    }

    let id = &me;
    let slots = format!(
        "*2\r\n*3\r\n:0\r\n:8191\r\n*4\r\n$9\r\n127.0.0.1\r\n:{port}\r\n$40\r\n{id}\r\n*0\r\n\
         *3\r\n:8192\r\n:16000\r\n*4\r\n$8\r\n10.0.0.2\r\n:7000\r\n$40\r\n{B}\r\n*0\r\n"
    );
    c.call(&[b"CLUSTER", b"SLOTS"], slots.as_bytes());
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert_eq!(
        c.read_text(),
        format!(
            "{id} 127.0.0.1:{port}@{port} myself,master - 0 0 1 connected 0-8191\n\
             {B} 10.0.0.2:7000@7000 master - 0 0 1 connected 8192-16000\n"
        )
    );
    c.cmd(&[b"CLUSTER", b"INFO"]);
    let info = c.read_text();
    for line in ["cluster_state:ok\r\n", "cluster_slots_assigned:16001\r\n", "cluster_known_nodes:2\r\n", "cluster_size:2\r\n", "cluster_current_epoch:1\r\n"] {
        assert!(info.contains(line), "{line:?} missing in {info}");
    }

    // Cluster mode shows in HELLO and INFO; in RESP3 too.
    c.cmd(&[b"HELLO", b"3"]);
    assert_eq!(c.read_line(), b"%7\r\n");
    let mut hello = Vec::new();
    for _ in 0..25 {
        hello.extend(c.read_line());
    }
    let needle = b"$4\r\nmode\r\n$7\r\ncluster\r\n$4\r\nrole";
    assert!(hello.windows(needle.len()).any(|w| w == needle), "{}", hello.escape_ascii());
    let info = "# Cluster\r\ncluster_enabled:1\r\nmigrate_keys_sent:0\r\n\
                migrate_keys_imported:0\r\nmigrate_imports_refused:0\r\ncluster_redirects_moved:0\r\ncluster_redirects_ask:0\r\ncluster_redirects_tryagain:0\r\ncluster_leaving:0\r\n";
    c.call(&[b"INFO", b"cluster"], format!("={}\r\ntxt:{info}\r\n", info.len() + 4).as_bytes());
    c.cmd(&[b"INFO", b"server"]);
    assert!(c.read_text().contains("\r\nredis_mode:cluster\r\n"));
    c.cmd(&[b"CLUSTER", b"INFO"]);
    let header = c.read_line();
    assert!(header.starts_with(b"="), "CLUSTER INFO: a verbatim string in RESP3");
    let len: usize = std::str::from_utf8(&header[1..header.len() - 2]).unwrap().parse().unwrap();
    let mut body = vec![0; len + 2];
    std::io::Read::read_exact(&mut c.0, &mut body).unwrap();
    assert!(body.starts_with(b"txt:cluster_state:ok\r\n"));
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert!(c.read_line().starts_with(b"="), "CLUSTER NODES: a verbatim string in RESP3");
}

#[test]
fn settable_epochs() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let nodes = [(&me[..], "127.0.0.1", &port[..]), (B, "10.0.0.2", "7000")];
    let mine_low = [("0", "8191", &me[..]), ("8192", "16383", B)];
    let mine_high = [("0", "8191", B), ("8192", "16383", &me[..])];
    call_vec(&mut c, &settable("5", &nodes, &mine_low), OK);
    c.call(&[b"GET", b"foo"], MOVED_FOO);

    // The same table again: fine, as stuart may push twice.
    call_vec(&mut c, &settable("5", &nodes, &mine_low), OK);
    call_vec(&mut c, &settable("5", &nodes, &mine_high), b"-ERR Table epoch 5 is the current epoch, with a different table\r\n");
    call_vec(&mut c, &settable("4", &nodes, &mine_high), b"-ERR Table epoch 4 is older than the current epoch 5\r\n");
    call_vec(&mut c, &settable("0", &nodes, &mine_high), b"-ERR Table epoch 0 is older than the current epoch 5\r\n");
    let without_me = settable("6", &[(B, "10.0.0.2", "7000")], &[("0", "16383", B)]);
    call_vec(&mut c, &without_me, format!("-ERR The table doesn't list this node ({me})\r\n").as_bytes());
    // Nothing changed.
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"GET", b"bar"], NIL);

    // A newer one moves foo's slot here and bar's away; the epoch may jump.
    call_vec(&mut c, &settable("9", &nodes, &mine_high), OK);
    c.call(&[b"GET", b"foo"], NIL);
    c.call(&[b"GET", b"bar"], b"-MOVED 5061 10.0.0.2:7000\r\n");
    c.cmd(&[b"CLUSTER", b"INFO"]);
    assert!(c.read_text().contains("\r\ncluster_current_epoch:9\r\ncluster_my_epoch:9\r\n"));

    // Another node's address changes, nothing else: a new epoch.
    let moved = [(&me[..], "127.0.0.1", &port[..]), (B, "10.0.0.3", "7001")];
    call_vec(&mut c, &settable("10", &moved, &mine_high), OK);
    c.call(&[b"GET", b"bar"], b"-MOVED 5061 10.0.0.3:7001\r\n");
}

#[test]
fn settable_refuses_bad_tables() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &port[..]), (B, "10.0.0.2", "7000")];
    let ranges = [("0", "8191", me), ("8192", "16383", B)];
    let syntax = b"-ERR syntax error\r\n";
    let s = |args: &[&str]| -> Vec<Vec<u8>> {
        [&["CLUSTER", "SETTABLE"][..], args].concat().iter().map(|a| a.as_bytes().to_vec()).collect()
    };

    // The shape: keywords, counts that fit the arguments, nothing extra.
    call_vec(&mut c, &s(&["1", "NODE", "1", me, "h", "1", "RANGES", "1", "0", "16383", me]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "2", me, "h", "1", "RANGES", "1", "0", "16383", me]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "1", me, "h", "1", "RANGES", "2", "0", "16383", me]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "1", me, "h", "1", "RANGES", "1", "0", "16383", me, "x"]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "-1", me, "h", "1", "RANGES", "1", "0", "16383", me]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "x", me, "h", "1", "RANGES", "1", "0", "16383", me]), syntax);
    call_vec(&mut c, &s(&["1", "NODES", "99999999999999", me, "h", "1", "RANGES", "0"]), syntax);
    call_vec(&mut c, &s(&["1", "nodes", "1", me, "h", "1", "ranges", "0"]), b"-ERR Invalid table: slot 0 is neither assigned nor unassigned\r\n");
    c.call(&[b"CLUSTER", b"SETTABLE", b"1", b"NODES", b"0", b"RANGES"], b"-ERR wrong number of arguments for 'cluster|settable' command\r\n");

    // The values.
    let bad = |field: usize, value: &str| {
        let mut args = settable("1", &nodes, &ranges);
        args[field] = value.as_bytes().to_vec();
        args
    };
    for (field, value, error) in [
        (2, "x", "ERR Invalid epoch 'x'"),
        (2, "-1", "ERR Invalid epoch '-1'"),
        (5, "B", "ERR Invalid node id 'B'"),
        (5, &me.to_uppercase(), &format!("ERR Invalid node id '{}'", me.to_uppercase())),
        (6, "a b", "ERR Invalid host 'a b'"),
        (6, "a@b", "ERR Invalid host 'a@b'"),
        (7, "0", "ERR Invalid port '0'"),
        (7, "65536", "ERR Invalid port '65536'"),
        (7, "x", "ERR Invalid port 'x'"),
        (13, "x", "ERR Invalid slot 'x'"),
        (14, "70000", "ERR Invalid slot '70000'"),
        (15, "dddd", "ERR Invalid node id 'dddd'"),
        (15, C, &format!("ERR Invalid table: unknown node {C}")),
        (14, "8190", "ERR Invalid table: slot 8191 is neither assigned nor unassigned"),
        (14, "8192", "ERR Invalid table: slot 8192 is given more than once"),
        (17, "16384", "ERR Invalid table: invalid slot range 8192-16384"),
        (16, "16384", "ERR Invalid table: invalid slot range 16384-16383"),
    ] {
        call_vec(&mut c, &bad(field, value), format!("-{error}\r\n").as_bytes());
    }
    let same_addr = settable("1", &[nodes[0], (B, "127.0.0.1", &port)], &ranges);
    call_vec(&mut c, &same_addr, format!("-ERR Invalid table: two nodes announce 127.0.0.1:{port}\r\n").as_bytes());
    let dup = settable("1", &[nodes[0], nodes[0]], &[("0", "16383", me)]);
    call_vec(&mut c, &dup, format!("-ERR Invalid table: node {me} is listed twice\r\n").as_bytes());

    // None of them was taken: still no slots.
    c.call(&[b"GET", b"foo"], CLUSTERDOWN);
    c.cmd(&[b"CLUSTER", b"INFO"]);
    assert!(c.read_text().contains("cluster_current_epoch:0\r\n"));
    // A good one is.
    call_vec(&mut c, &settable("1", &nodes, &ranges), OK);
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"GET", b"bar"], NIL);
}

/// One table with every slot unassigned except a few single ones, and a
/// third node without slots.
#[test]
fn single_slots_and_unassigned_slots() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let table = settable(
        "3",
        &[(me, "127.0.0.1", &port), (B, "10.0.0.2", "7000"), (C, "10.0.0.3", "7000")],
        &[("0", "5060", "-"), ("5061", "5061", me), ("5062", "12181", "-"), ("12182", "12182", B), ("12183", "16383", "-")],
    );
    call_vec(&mut c, &table, OK);
    c.call(&[b"SET", b"bar", b"1"], OK);
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"GET", b"a"], CLUSTERDOWN);
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert_eq!(
        c.read_text(),
        format!(
            "{me} 127.0.0.1:{port}@{port} myself,master - 0 0 3 connected 5061\n\
             {B} 10.0.0.2:7000@7000 master - 0 0 3 connected 12182\n\
             {C} 10.0.0.3:7000@7000 master - 0 0 3 connected\n"
        )
    );
    c.cmd(&[b"CLUSTER", b"INFO"]);
    let info = c.read_text();
    assert!(info.contains("cluster_state:ok\r\ncluster_slots_assigned:2\r\n") && info.contains("cluster_known_nodes:3\r\ncluster_size:2\r\n"), "{info}");
}

/// `settable` with a `MIGRATING` section.
fn settable_migrating(epoch: &str, nodes: &[(&str, &str, &str)], ranges: &[(&str, &str, &str)], migrating: &[(&str, &str, &str)]) -> Vec<Vec<u8>> {
    let mut args = settable(epoch, nodes, ranges);
    args.push(b"MIGRATING".to_vec());
    args.push(migrating.len().to_string().into());
    for (slot, from, to) in migrating {
        args.extend([slot.as_bytes().to_vec(), from.as_bytes().to_vec(), to.as_bytes().to_vec()]);
    }
    args
}

const TRYAGAIN: &[u8] = b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n";

/// This node owns 0-8191 and moves bar's slot (5061) to B: it serves the
/// keys it has, sends the others to B (`ASK`), refuses a mix (`TRYAGAIN`).
/// The replies are Redis 7.0.15's in the same state (cluster_difftest.py
/// migrating compares them).
#[test]
fn migrating_slot_at_the_source() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &port[..]), (B, "10.0.0.2", "7000")];
    let ranges = [("0", "8191", me), ("8192", "16383", B)];
    call_vec(&mut c, &settable("1", &nodes, &ranges), OK);
    c.call(&[b"SET", b"bar", b"1"], OK);
    c.call(&[b"SET", b"{bar}e", b"x", b"PX", b"1"], OK);
    std::thread::sleep(std::time::Duration::from_millis(5));
    call_vec(&mut c, &settable_migrating("2", &nodes, &ranges, &[("5061", me, B)]), OK);

    const ASK: &[u8] = b"-ASK 5061 10.0.0.2:7000\r\n";
    c.call(&[b"GET", b"bar"], b"$1\r\n1\r\n");
    c.call(&[b"GET", b"{bar}x"], ASK);
    c.call(&[b"GET", b"{bar}e"], ASK);
    c.call(&[b"MGET", b"bar", b"bar"], b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n");
    c.call(&[b"MGET", b"{bar}x", b"{bar}y"], ASK);
    c.call(&[b"MGET", b"bar", b"{bar}x"], TRYAGAIN);
    c.call(&[b"EXISTS", b"{bar}x", b"bar"], TRYAGAIN);
    c.call(&[b"SET", b"{bar}x", b"1"], ASK);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"GET", b"{bar}x"], ASK);
    c.call(&[b"INCR", b"bar"], b":2\r\n");
    // Other slots are as before; CROSSSLOT still comes first.
    let a = key_in(0..=5000, "a");
    c.call(&[b"SET", &a, b"1"], OK);
    c.call(&[b"GET", b"foo"], b"-MOVED 12182 10.0.0.2:7000\r\n");
    c.call(&[b"MGET", b"{bar}x", &a], CROSSSLOT);
    // The expired key was looked at, not reclaimed (Redis: LOOKUP_NOEXPIRE).
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"5061"], b":2\r\n");
    c.cmd(&[b"CLUSTER", b"NODES"]);
    let text = c.read_text();
    let mine = text.lines().find(|l| l.contains("myself")).unwrap();
    assert!(mine.ends_with(&format!(" connected 0-8191 [5061->-{B}]")), "{text}");
    c.call(&[b"DEL", b"bar"], b":1\r\n");
    c.call(&[b"GET", b"bar"], ASK);

    // The migration ends: B owns the slot.
    let moved = [("0", "5060", me), ("5061", "5061", B), ("5062", "8191", me), ("8192", "16383", B)];
    call_vec(&mut c, &settable("3", &nodes, &moved), OK);
    c.call(&[b"GET", b"bar"], b"-MOVED 5061 10.0.0.2:7000\r\n");
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert!(!c.read_text().contains('['));
}

/// B owns bar's slot and moves it here: only `ASKING` requests are served,
/// several keys only if they're all here; the rest goes to B (`MOVED`).
#[test]
fn migrating_slot_at_the_target() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &port[..]), (B, "10.0.0.2", "7000"), (C, "10.0.0.3", "7000")];
    let ranges = [("0", "8191", B), ("8192", "16383", me)];
    call_vec(&mut c, &settable_migrating("1", &nodes, &ranges, &[("5061", B, me), ("100", B, C)]), OK);

    const MOVED: &[u8] = b"-MOVED 5061 10.0.0.2:7000\r\n";
    c.call(&[b"GET", b"bar"], MOVED);
    c.call(&[b"SET", b"bar", b"1"], MOVED);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"SET", b"bar", b"1"], OK);
    c.call(&[b"GET", b"bar"], MOVED);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"GET", b"bar"], b"$1\r\n1\r\n");
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MGET", b"bar", b"bar"], b"*2\r\n$1\r\n1\r\n$1\r\n1\r\n");
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MGET", b"bar", b"{bar}x"], TRYAGAIN);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MSET", b"{bar}x", b"1", b"{bar}y", b"2"], TRYAGAIN);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"GET", b"{bar}x"], NIL);
    // One key twice is one key, even a missing one.
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MGET", b"{bar}x", b"{bar}x"], b"*2\r\n$-1\r\n$-1\r\n");
    // ASKING holds for the next command only, whatever it is.
    for between in [&[&b"PING"[..]][..], &[b"NOSUCH"], &[b"GET"], &[b"GET", b"foo"]] {
        c.call(&[b"ASKING"], OK);
        c.cmd(between);
        c.read_line();
        c.call(&[b"GET", b"bar"], MOVED);
    }
    c.call(&[b"ASKING"], OK);
    c.call(&[b"ASKING"], OK);
    c.call(&[b"GET", b"bar"], b"$1\r\n1\r\n");
    // Pipelined too.
    c.send(&[request(&[b"ASKING"]), request(&[b"GET", b"bar"]), request(&[b"GET", b"bar"])].concat());
    c.expect(&[OK, b"$1\r\n1\r\n", MOVED].concat());
    // Slot 100 moves from B to C: not this node's business.
    let key100 = key_in(100..=100, "k");
    c.call(&[b"ASKING"], OK);
    c.call(&[&b"GET"[..], &key100], b"-MOVED 100 10.0.0.2:7000\r\n");
    c.cmd(&[b"CLUSTER", b"NODES"]);
    let text = c.read_text();
    let mine = text.lines().find(|l| l.contains("myself")).unwrap();
    assert!(mine.ends_with(&format!(" connected 8192-16383 [5061-<-{B}]")), "{text}");
    c.call(&[b"CLUSTER", b"COUNTKEYSINSLOT", b"5061"], b":1\r\n");
    // In a transaction, ASKING holds until EXEC, as Redis keeps its flag
    // while in MULTI; then it's gone.
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MULTI"], OK);
    c.call(&[b"GET", b"bar"], b"+QUEUED\r\n");
    c.call(&[b"INCR", b"bar"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], b"*2\r\n$1\r\n1\r\n:2\r\n");
    c.call(&[b"GET", b"bar"], MOVED);
    c.call(&[b"MULTI"], OK);
    c.call(&[b"GET", b"bar"], MOVED);
    c.call(&[b"EXEC"], b"-EXECABORT Transaction discarded because of previous errors.\r\n");
    // Several keys, one missing: as a request of them all.
    c.call(&[b"ASKING"], OK);
    c.call(&[b"MULTI"], OK);
    c.call(&[b"GET", b"bar"], b"+QUEUED\r\n");
    c.call(&[b"GET", b"{bar}x"], b"+QUEUED\r\n");
    c.call(&[b"EXEC"], TRYAGAIN);
    // Standalone, ASKING is refused like the other cluster commands.
    let mut s = Client::connect(start());
    s.call(&[b"ASKING"], b"-ERR This instance has cluster support disabled\r\n");
    s.call(&[b"SET", b"bar", b"1"], OK);
}

#[test]
fn settable_migrations_are_checked() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &port[..]), (B, "10.0.0.2", "7000")];
    let ranges = [("0", "8191", me), ("8192", "16383", B)];
    let syntax = b"-ERR syntax error\r\n";
    let mut extra = settable_migrating("1", &nodes, &ranges, &[("5", me, B)]);
    extra.push(b"x".to_vec());
    call_vec(&mut c, &extra, syntax);
    let mut short = settable_migrating("1", &nodes, &ranges, &[("5", me, B)]);
    short.pop();
    call_vec(&mut c, &short, syntax);
    let mut keyword = settable_migrating("1", &nodes, &ranges, &[("5", me, B)]);
    let at = keyword.len() - 5;
    keyword[at] = b"MIGRATE".to_vec();
    call_vec(&mut c, &keyword, syntax);
    for (m, error) in [
        (("8192", me, B), "ERR Invalid table: slot 8192 can only migrate from its owner".to_string()),
        (("5", me, me), "ERR Invalid table: slot 5 migrates to its owner".to_string()),
        (("5", me, C), format!("ERR Invalid table: unknown node {C}")),
        (("16384", me, B), "ERR Invalid table: invalid slot 16384".to_string()),
        (("x", me, B), "ERR Invalid slot 'x'".to_string()),
        (("5", "x", B), "ERR Invalid node id 'x'".to_string()),
    ] {
        call_vec(&mut c, &settable_migrating("1", &nodes, &ranges, &[m]), format!("-{error}\r\n").as_bytes());
    }
    let twice = settable_migrating("1", &nodes, &ranges, &[("5", me, B), ("5", me, B)]);
    call_vec(&mut c, &twice, b"-ERR Invalid table: slot 5 migrates twice\r\n");
    // An empty section is fine, and the same table as none.
    call_vec(&mut c, &settable_migrating("1", &nodes, &ranges, &[]), OK);
    call_vec(&mut c, &settable("1", &nodes, &ranges), OK);
    // The same epoch with a migration added is another table.
    let moving = settable_migrating("1", &nodes, &ranges, &[("5", me, B)]);
    call_vec(&mut c, &moving, b"-ERR Table epoch 1 is the current epoch, with a different table\r\n");
}

/// `--stuart`: the node sends `STUART JOIN id host port` and retries until
/// stuart says `+OK`; then it stops. Here stuart is a listener that refuses
/// the first join.
#[test]
fn joins_stuart_and_retries() {
    use std::io::{BufRead, BufReader, Write};
    let fake = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let stuart = fake.local_addr().unwrap().to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for (i, conn) in fake.incoming().enumerate() {
            let mut conn = conn.unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            // *5, then 5 of $len + value.
            let mut request = String::new();
            for _ in 0..11 {
                reader.read_line(&mut request).unwrap();
            }
            conn.write_all(if i == 0 { b"-ERR not yet\r\n" } else { b"+OK\r\n" }).unwrap();
            tx.send(request).unwrap();
        }
    });
    let addr = start_joining(Some(stuart));
    let mut c = Client::connect(addr);
    c.cmd(&[b"CLUSTER", b"MYID"]);
    let id = c.read_text();
    let port = addr.port();
    let expected = format!("*5\r\n$6\r\nSTUART\r\n$4\r\nJOIN\r\n$40\r\n{id}\r\n$9\r\n127.0.0.1\r\n${}\r\n{port}\r\n", port.to_string().len());
    let timeout = std::time::Duration::from_secs(5);
    assert_eq!(rx.recv_timeout(timeout).unwrap(), expected);
    // Retried a second later, then accepted.
    let start = std::time::Instant::now();
    assert_eq!(rx.recv_timeout(timeout).unwrap(), expected);
    assert!(start.elapsed() >= std::time::Duration::from_millis(500), "{:?}", start.elapsed());
    // No more tries once joined.
    assert!(rx.recv_timeout(std::time::Duration::from_millis(2500)).is_err());
    // Joining changes nothing until stuart sends a table.
    c.call(&[b"GET", b"foo"], CLUSTERDOWN);
}

/// A fake stuart that takes every `STUART JOIN` and sends no table.
fn silent_stuart() -> String {
    use std::io::{BufRead, BufReader, Write};
    let fake = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let stuart = fake.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for conn in fake.incoming() {
            let mut conn = conn.unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut request = String::new();
            for _ in 0..11 {
                reader.read_line(&mut request).unwrap();
            }
            conn.write_all(b"+OK\r\n").unwrap();
        }
    });
    stuart
}

/// A node joining stuart holds a command on a slot it doesn't know of until
/// its first table: then the command is redirected (a client reconnecting
/// to a node restarted at its address gets `MOVED`, as from a restarted
/// Redis node, not `-CLUSTERDOWN`). `CROSSSLOT` doesn't wait, nor does a
/// slot the first table leaves unassigned.
#[test]
fn before_its_first_table_a_joining_node_holds_commands() {
    use std::time::Duration;
    let addr = start_joining(Some(silent_stuart()));
    let mut c = Client::connect(addr);
    c.cmd(&[b"CLUSTER", b"MYID"]);
    let me = c.read_text();
    let asked = std::time::Instant::now();
    c.call(&[b"MGET", b"foo", b"bar"], CROSSSLOT);
    assert!(asked.elapsed() < Duration::from_millis(500), "{:?}", asked.elapsed());
    let mut held = Client::connect(addr);
    held.cmd(&[b"GET", b"foo"]);
    held.0.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut byte = [0];
    let early = std::io::Read::read(&mut held.0, &mut byte);
    assert!(early.is_err(), "answered before the table: {early:?} {byte:?}");
    held.0.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let p = addr.port().to_string();
    let nodes = [(&me[..], "127.0.0.1", &p[..]), (B, "10.0.0.2", "7000")];
    call_vec(&mut c, &settable("1", &nodes, &[("0", "16000", B), ("16001", "16383", "-")]), OK);
    held.expect(MOVED_FOO);
    let unassigned = key_in(16001..=16383, "k");
    let asked = std::time::Instant::now();
    c.call(&[b"GET", &unassigned], CLUSTERDOWN);
    assert!(asked.elapsed() < Duration::from_millis(500), "{:?}", asked.elapsed());
}

/// Without a table FIRST_TABLE_WAIT (3 s) after it started, the node
/// answers `-CLUSTERDOWN` again, at once from then on.
#[test]
fn a_joining_node_waits_for_its_first_table_three_seconds_at_most() {
    use std::time::{Duration, Instant};
    let started = Instant::now();
    let addr = start_joining(Some(silent_stuart()));
    let mut c = Client::connect(addr);
    c.call(&[b"GET", b"foo"], CLUSTERDOWN);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(2900) && waited < Duration::from_millis(4500), "{waited:?}");
    let again = Instant::now();
    c.call(&[b"GET", b"foo"], CLUSTERDOWN);
    assert!(again.elapsed() < Duration::from_millis(200), "{:?}", again.elapsed());
}

/// `settable` with `REPLICAS` (and `MIGRATING` before it, if any).
fn settable_replicas(
    epoch: &str,
    nodes: &[(&str, &str, &str)],
    ranges: &[(&str, &str, &str)],
    migrating: &[(&str, &str, &str)],
    replicas: &[(&str, &str)],
) -> Vec<Vec<u8>> {
    let mut args = if migrating.is_empty() { settable(epoch, nodes, ranges) } else { settable_migrating(epoch, nodes, ranges, migrating) };
    args.push(b"REPLICAS".to_vec());
    args.push(replicas.len().to_string().into());
    for (replica, primary) in replicas {
        args.extend([replica.as_bytes().to_vec(), primary.as_bytes().to_vec()]);
    }
    args
}

/// A replica of B, which owns every slot: it redirects every key to B,
/// after `READONLY` too while it has no data (phase 8 milestone 1); it is
/// a replica in `CLUSTER NODES`, `SHARDS` (loading: it has taken nothing),
/// `ROLE`, `INFO replication` and `HELLO`, and not in `CLUSTER SLOTS` (as
/// Redis lists a replica only once its offset isn't 0). The replies are
/// Redis 7.0.15's for a replica whose primary is down.
#[test]
fn a_replica() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    let p = port.to_string();
    let nodes = [(&me[..], "127.0.0.1", &p[..]), (B, "10.0.0.2", "7000")];
    let all_b = [("0", "16383", B)];
    call_vec(&mut c, &settable_replicas("3", &nodes, &all_b, &[], &[(&me, B)]), OK);

    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"SET", b"foo", b"1"], MOVED_FOO);
    // After READONLY, reads from its copy (empty: nothing synced from a
    // primary nobody runs), as a Redis replica's; writes go to B.
    c.call(&[b"READONLY"], OK);
    c.call(&[b"GET", b"foo"], NIL);
    c.call(&[b"MGET", b"foo", b"foo"], b"*2\r\n$-1\r\n$-1\r\n");
    c.call(&[b"SET", b"foo", b"1"], MOVED_FOO);
    c.call(&[b"READWRITE"], OK);
    c.call(&[b"GET", b"foo"], MOVED_FOO);
    c.call(&[b"READONLY", b"x"], b"-ERR wrong number of arguments for 'readonly' command\r\n");
    c.call(&[b"DBSIZE"], b":0\r\n");

    // Trying to reach B: `connecting`, or `connect` between tries.
    c.cmd(&[b"ROLE"]);
    c.expect(b"*5\r\n$5\r\nslave\r\n$8\r\n10.0.0.2\r\n:7000\r\n");
    let state = c.read_text();
    assert!(state == "connect" || state == "connecting", "{state}");
    c.expect(b":-1\r\n");
    c.call(&[b"ROLE", b"x"], b"-ERR wrong number of arguments for 'role' command\r\n");
    c.cmd(&[b"INFO", b"replication"]);
    let info = c.read_text();
    assert!(
        info.starts_with(
            "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.2\r\nmaster_port:7000\r\nmaster_link_status:down\r\n\
             master_last_io_seconds_ago:-1\r\nmaster_sync_in_progress:0\r\nslave_read_repl_offset:0\r\nslave_repl_offset:0\r\n\
             master_link_down_since_seconds:-1\r\nslave_priority:100\r\nslave_read_only:1\r\nreplica_announced:1\r\n\
             connected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:"
        ),
        "{info}"
    );
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert_eq!(
        c.read_text(),
        format!("{me} 127.0.0.1:{port}@{port} myself,slave {B} 0 0 3 connected\n{B} 10.0.0.2:7000@7000 master - 0 0 3 connected 0-16383\n")
    );
    c.call(&[b"CLUSTER", b"SLOTS"], format!("*1\r\n*3\r\n:0\r\n:16383\r\n*4\r\n$8\r\n10.0.0.2\r\n:7000\r\n$40\r\n{B}\r\n*0\r\n").as_bytes());
    c.cmd(&[b"CLUSTER", b"SHARDS"]);
    let mut shards = vec![0; 4096];
    let n = std::io::Read::read(&mut c.0, &mut shards).unwrap();
    let shards = String::from_utf8_lossy(&shards[..n]).to_string();
    assert!(shards.starts_with("*1\r\n*4\r\n$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n$5\r\nnodes\r\n*2\r\n"), "{shards}");
    assert!(shards.ends_with(&format!("{me}\r\n$4\r\nport\r\n:{port}\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n\
         $4\r\nrole\r\n$7\r\nreplica\r\n$18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n$7\r\nloading\r\n")), "{shards}");
    c.cmd(&[b"HELLO", b"3"]);
    let hello = c.read_line();
    assert_eq!(hello, b"%7\r\n");
    let mut rest = vec![0; 4096];
    let n = std::io::Read::read(&mut c.0, &mut rest).unwrap();
    assert!(rest[..n].ends_with(b"$4\r\nrole\r\n$7\r\nreplica\r\n$7\r\nmodules\r\n*0\r\n"), "{}", rest[..n].escape_ascii());

    // A primary again (no REPLICAS): it serves its slots.
    let mine = [("0", "16383", &me[..])];
    call_vec(&mut c, &settable("4", &nodes, &mine), OK);
    c.call(&[b"GET", b"foo"], b"_\r\n");
    c.call(&[b"ROLE"], b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");
    c.cmd(&[b"INFO", b"replication"]);
    let info = c.read_text();
    // RESP3 now: a verbatim string.
    let info = info.strip_prefix("txt:").unwrap();
    assert!(info.starts_with("# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:"), "{info}");
    assert!(
        info.ends_with(
            "\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\n\
             repl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n"
        ),
        "{info}"
    );
    let replid = info.split("master_replid:").nth(1).unwrap().split("\r\n").next().unwrap();
    assert!(replid.len() == 40 && replid.bytes().all(|b| b.is_ascii_hexdigit()) && replid != me, "{replid}");
}

/// `SHARDS` in `CLUSTER SETTABLE`: the shards as Redis 7.2+ groups them
/// by shard id, here B failed over to this node and still in its shard,
/// first; `CLUSTER MYSHARDID` the node's (its own id before a table, and
/// with the default shards).
#[test]
fn shards_and_myshardid() {
    let (mut c, me) = node();
    let port = c.0.peer_addr().unwrap().port();
    let p = port.to_string();
    let me = &me[..];
    c.call(&[b"CLUSTER", b"MYSHARDID"], format!("$40\r\n{me}\r\n").as_bytes());
    c.call(&[b"CLUSTER", b"MYSHARDID", b"x"], b"-ERR wrong number of arguments for 'cluster|myshardid' command\r\n");
    let nodes = [(B, "10.0.0.2", "7000"), (me, "127.0.0.1", &p[..]), (C, "10.0.0.3", "7000")];
    let all_mine = [("0", "16383", me)];
    let with_shards = |epoch: &str, shards: &[(&str, &str)]| {
        let mut args = settable_replicas(epoch, &nodes, &all_mine, &[], &[(C, me)]);
        args.extend([b"FAILED".to_vec(), b"1".to_vec(), B.as_bytes().to_vec(), b"SHARDS".to_vec(), shards.len().to_string().into()]);
        for (node, shard) in shards {
            args.extend([node.as_bytes().to_vec(), shard.as_bytes().to_vec()]);
        }
        args
    };
    let invalid = |e: String| format!("-ERR Invalid table: {e}\r\n").into_bytes();
    call_vec(&mut c, &with_shards("2", &[(B, B), (me, B)]), &invalid(format!("node {C} is given no shard")));
    call_vec(&mut c, &with_shards("2", &[(B, B), (me, B), (C, C)]), &invalid(format!("replica {C} is not in its primary's shard")));
    let mut odd = with_shards("2", &[(B, B), (me, B), (C, B)]);
    odd.pop();
    call_vec(&mut c, &odd, b"-ERR syntax error\r\n");
    call_vec(&mut c, &with_shards("2", &[(B, B), (me, B), (C, B)]), OK);

    c.call(&[b"CLUSTER", b"MYSHARDID"], format!("$40\r\n{B}\r\n").as_bytes());
    c.cmd(&[b"CLUSTER", b"SHARDS"]);
    let mut shards = vec![0; 4096];
    let n = std::io::Read::read(&mut c.0, &mut shards).unwrap();
    let shards = String::from_utf8_lossy(&shards[..n]).to_string();
    let entry = |id: &str, ip: &str, port: &str, role: &str, health: &str| {
        format!(
            "*14\r\n$2\r\nid\r\n$40\r\n{id}\r\n$4\r\nport\r\n:{port}\r\n$2\r\nip\r\n${}\r\n{ip}\r\n$8\r\nendpoint\r\n${}\r\n{ip}\r\n\
             $4\r\nrole\r\n${}\r\n{role}\r\n$18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n${}\r\n{health}\r\n",
            ip.len(),
            ip.len(),
            role.len(),
            health.len()
        )
    };
    let want = "*1\r\n*4\r\n$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n$5\r\nnodes\r\n*3\r\n".to_string()
        + &entry(B, "10.0.0.2", "7000", "master", "fail")
        + &entry(me, "127.0.0.1", &p, "master", "online")
        + &entry(C, "10.0.0.3", "7000", "replica", "loading");
    assert_eq!(shards, want);

    // The default shards again: each primary its own.
    call_vec(&mut c, &settable_replicas("3", &nodes, &all_mine, &[], &[(C, me)]), OK);
    c.call(&[b"CLUSTER", b"MYSHARDID"], format!("$40\r\n{me}\r\n").as_bytes());
}

#[test]
fn settable_replicas_are_checked() {
    let (mut c, me) = node();
    let p = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &p[..]), (B, "10.0.0.2", "7000"), (C, "10.0.0.3", "7000")];
    let ranges = [("0", "8191", me), ("8192", "16383", B)];
    let invalid = |e: String| format!("-ERR Invalid table: {e}\r\n").into_bytes();
    let refused = |c: &mut Client, replicas: &[(&str, &str)], expected: &[u8]| {
        call_vec(c, &settable_replicas("2", &nodes, &ranges, &[], replicas), expected);
    };
    refused(&mut c, &[(C, C)], &invalid(format!("node {C} is a replica of itself")));
    refused(&mut c, &[(me, C)], &invalid(format!("replica {me} owns slots")));
    refused(&mut c, &[(C, me), (C, B)], &invalid(format!("node {C} is listed as a replica twice")));
    refused(&mut c, &[(C, "dddddddddddddddddddddddddddddddddddddddd")], &invalid("unknown node dddddddddddddddddddddddddddddddddddddddd".into()));
    refused(&mut c, &[(C, "x")], b"-ERR Invalid node id 'x'\r\n");
    // Nothing was taken: the node is still on its own.
    c.cmd(&[b"CLUSTER", b"INFO"]);
    assert!(c.read_text().contains("\r\ncluster_current_epoch:0\r\n"));

    // The shape: pairs, after MIGRATING.
    let mut odd = settable("2", &nodes, &ranges);
    odd.extend([b"REPLICAS".to_vec(), b"1".to_vec(), C.as_bytes().to_vec()]);
    call_vec(&mut c, &odd, b"-ERR syntax error\r\n");
    let mut before = settable_replicas("2", &nodes, &ranges, &[], &[(C, me)]);
    before.extend([b"MIGRATING".to_vec(), b"0".to_vec()]);
    call_vec(&mut c, &before, b"-ERR syntax error\r\n");
    // A slot can't migrate to a replica.
    let to_replica = settable_replicas("2", &nodes, &ranges, &[("5", me, C)], &[(C, B)]);
    call_vec(&mut c, &to_replica, &invalid("slot 5 migrates to a replica".into()));

    // Both, in order; an empty REPLICAS is no replicas.
    call_vec(&mut c, &settable_replicas("2", &nodes, &ranges, &[("5", me, B)], &[(C, me)]), OK);
    call_vec(&mut c, &settable_replicas("3", &nodes, &ranges, &[], &[]), OK);
    c.cmd(&[b"CLUSTER", b"NODES"]);
    assert!(!c.read_text().contains("slave"));
}

/// `CLUSTER SETOFFSETS`: the other nodes' offsets, which decide whether
/// `CLUSTER SLOTS` lists a replica (past 0) and what `SHARDS` shows.
#[test]
fn setoffsets() {
    let (mut c, me) = node();
    let p = c.0.peer_addr().unwrap().port().to_string();
    let me = &me[..];
    let nodes = [(me, "127.0.0.1", &p[..]), (B, "10.0.0.2", "7000"), (C, "10.0.0.3", "7000")];
    let ranges = [("0", "8191", me), ("8192", "16383", B)];
    call_vec(&mut c, &settable_replicas("2", &nodes, &ranges, &[], &[(C, me)]), OK);
    let raw = |c: &mut Client, args: &[&[u8]]| {
        c.cmd(args);
        let mut buf = vec![0; 8192];
        let n = std::io::Read::read(&mut c.0, &mut buf).unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    let replica = format!("*4\r\n$8\r\n10.0.0.3\r\n:7000\r\n$40\r\n{C}\r\n*0\r\n");
    // Before any: C's offset is 0, so it isn't listed, and is loading.
    let slots = raw(&mut c, &[b"CLUSTER", b"SLOTS"]);
    assert!(slots.starts_with("*2\r\n*3\r\n") && !slots.contains(C), "{slots}");
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes(), b"14"], OK);
    let slots = raw(&mut c, &[b"CLUSTER", b"SLOTS"]);
    assert!(slots.starts_with("*2\r\n*4\r\n:0\r\n:8191\r\n") && slots.contains(&replica), "{slots}");
    let shards = raw(&mut c, &[b"CLUSTER", b"SHARDS"]);
    assert!(shards.contains(&format!("{C}\r\n")), "{shards}");
    assert!(shards.contains("$7\r\nreplica\r\n$18\r\nreplication-offset\r\n:14\r\n$6\r\nhealth\r\n$6\r\nonline\r\n"), "{shards}");

    // Refused whole: what was there stays.
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes()], b"-ERR syntax error\r\n");
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes(), b"5", b"x", b"1"], b"-ERR Invalid node id 'x'\r\n");
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes(), b"-1"], b"-ERR Invalid offset '-1'\r\n");
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes(), b"1.5"], b"-ERR Invalid offset '1.5'\r\n");
    assert!(raw(&mut c, &[b"CLUSTER", b"SLOTS"]).contains(&replica));
    assert_eq!(raw(&mut c, &[b"CLUSTER", b"SHARDS"]), shards, "C's offset still 14");

    // Each push replaces the last; a node not in the table is ignored.
    c.call(&[b"CLUSTER", b"SETOFFSETS", b"dddddddddddddddddddddddddddddddddddddddd", b"7"], OK);
    assert!(!raw(&mut c, &[b"CLUSTER", b"SLOTS"]).contains(C));
    c.call(&[b"CLUSTER", b"SETOFFSETS", C.as_bytes(), b"14"], OK);
    c.call(&[b"CLUSTER", b"SETOFFSETS"], OK);
    assert!(!raw(&mut c, &[b"CLUSTER", b"SLOTS"]).contains(C));
}

#[test]
fn role_without_cluster_mode() {
    let mut c = Client::connect(start());
    c.call(&[b"ROLE"], b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");
    c.call(&[b"READONLY"], b"-ERR This instance has cluster support disabled\r\n");
    c.cmd(&[b"INFO", b"replication"]);
    assert!(c.read_text().starts_with("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"));
}
