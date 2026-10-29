//! The dbnode over real TCP connections: a server per test on its own
//! thread, and a blocking client checking the exact bytes of the replies.
//! The expected replies are what Redis 7.0 sends.

mod common;

use common::*;
use dbnode::Config;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn ping_and_echo() {
    let mut c = Client::connect(start());
    c.call(&[b"PING"], b"+PONG\r\n");
    c.call(&[b"ping", b"hi there"], b"$8\r\nhi there\r\n");
    c.call(&[b"PING", b"a", b"b"], b"-ERR wrong number of arguments for 'ping' command\r\n");
    c.call(&[b"ECHO", b""], b"$0\r\n\r\n");
}

#[test]
fn get_set_del() {
    let mut c = Client::connect(start());
    c.call(&[b"GET", b"k"], NIL);
    c.call(&[b"SET", b"k", b"v"], OK);
    c.call(&[b"GET", b"k"], b"$1\r\nv\r\n");
    c.call(&[b"set", b"k", b"w"], OK);
    c.call(&[b"Get", b"k"], b"$1\r\nw\r\n");
    c.call(&[b"SET", b"other", b""], OK);
    c.call(&[b"GET", b"other"], b"$0\r\n\r\n");
    c.call(&[b"DEL", b"k", b"missing", b"other", b"k"], b":2\r\n");
    c.call(&[b"GET", b"k"], NIL);
}

#[test]
fn values_are_binary_safe() {
    let mut c = Client::connect(start());
    let value = b"a\0b\r\nc\xff";
    c.call(&[b"SET", b"\0key\r\n", value], OK);
    c.call(&[b"GET", b"\0key\r\n"], b"$7\r\na\0b\r\nc\xff\r\n");
}

#[test]
fn set_conditions_and_get() {
    let mut c = Client::connect(start());
    c.call(&[b"SET", b"k", b"v", b"XX"], NIL);
    c.call(&[b"SET", b"k", b"v", b"NX"], OK);
    c.call(&[b"SET", b"k", b"w", b"nx"], NIL);
    c.call(&[b"SET", b"k", b"w", b"XX", b"GET"], b"$1\r\nv\r\n");
    c.call(&[b"SET", b"k", b"x", b"NX", b"GET"], b"$1\r\nw\r\n");
    c.call(&[b"SET", b"new", b"x", b"GET"], NIL);
    c.call(&[b"GET", b"k"], b"$1\r\nw\r\n");
    // Options start after the value: these are a key and a value.
    c.call(&[b"SET", b"EX", b"NX"], OK);
    c.call(&[b"GET", b"EX"], b"$2\r\nNX\r\n");
}

#[test]
fn set_option_errors() {
    let mut c = Client::connect(start());
    let syntax = b"-ERR syntax error\r\n";
    for args in [
        &[&b"SET"[..], b"k", b"v", b"NX", b"XX"][..],
        &[b"SET", b"k", b"v", b"FOO"],
        &[b"SET", b"k", b"v", b"EX"],
        &[b"SET", b"k", b"v", b"EX", b"10", b"PX", b"100"],
        &[b"SET", b"k", b"v", b"KEEPTTL", b"EX", b"10"],
        &[b"SET", b"k", b"v", b"PXAT", b"10", b"KEEPTTL"],
        // The syntax is checked before the number.
        &[b"SET", b"k", b"v", b"EX", b"abc", b"FOO"],
    ] {
        c.call(args, syntax);
    }
    c.call(&[b"SET", b"k", b"v", b"EX", b"abc"], b"-ERR value is not an integer or out of range\r\n");
    c.call(&[b"SET", b"k", b"v", b"EX", b"1.5"], b"-ERR value is not an integer or out of range\r\n");
    c.call(&[b"SET", b"k", b"v", b"EX", b"0"], b"-ERR invalid expire time in 'set' command\r\n");
    c.call(&[b"SET", b"k", b"v", b"PX", b"-1"], b"-ERR invalid expire time in 'set' command\r\n");
    // Repeating an option is fine; the last value counts.
    c.call(&[b"SET", b"k", b"v", b"EX", b"1", b"EX", b"100", b"NX", b"NX", b"KEEPTTL"], syntax);
    c.call(&[b"SET", b"k", b"v", b"EX", b"1", b"EX", b"100", b"NX", b"NX"], OK);
    c.call(&[b"GET", b"k"], b"$1\r\nv\r\n");
    // None of the failed SETs did anything.
    c.call(&[b"DEL", b"k"], b":1\r\n");
}

#[test]
fn keys_expire() {
    let mut c = Client::connect(start());
    c.call(&[b"SET", b"short", b"v", b"PX", b"50"], OK);
    c.call(&[b"SET", b"long", b"v", b"EX", b"100"], OK);
    c.call(&[b"SET", b"kept", b"v", b"PX", b"50"], OK);
    c.call(&[b"SET", b"kept", b"w", b"KEEPTTL"], OK);
    c.call(&[b"SET", b"cleared", b"v", b"PX", b"50"], OK);
    c.call(&[b"SET", b"cleared", b"w"], OK);
    c.call(&[b"GET", b"short"], b"$1\r\nv\r\n");
    thread::sleep(Duration::from_millis(150));
    c.call(&[b"GET", b"short"], NIL);
    c.call(&[b"GET", b"kept"], NIL);
    c.call(&[b"GET", b"long"], b"$1\r\nv\r\n");
    c.call(&[b"GET", b"cleared"], b"$1\r\nw\r\n");
}

#[test]
fn counters() {
    let mut c = Client::connect(start());
    c.call(&[b"INCR", b"n"], b":1\r\n");
    c.call(&[b"INCRBY", b"n", b"41"], b":42\r\n");
    c.call(&[b"DECR", b"n"], b":41\r\n");
    c.call(&[b"DECRBY", b"n", b"-9"], b":50\r\n");
    c.call(&[b"GET", b"n"], b"$2\r\n50\r\n");
    c.call(&[b"SET", b"s", b"abc"], OK);
    c.call(&[b"INCR", b"s"], b"-ERR value is not an integer or out of range\r\n");
    c.call(&[b"INCRBY", b"n", b"1.5"], b"-ERR value is not an integer or out of range\r\n");
    // The increment is checked before the key: no WRONGTYPE-style error first.
    c.call(&[b"INCRBY", b"s", b"x"], b"-ERR value is not an integer or out of range\r\n");
    c.call(&[b"SET", b"max", b"9223372036854775807"], OK);
    c.call(&[b"INCR", b"max"], b"-ERR increment or decrement would overflow\r\n");
    c.call(&[b"DECRBY", b"n", b"-9223372036854775808"], b"-ERR decrement would overflow\r\n");
}

#[test]
fn multi_key_commands() {
    let mut c = Client::connect(start());
    c.call(&[b"MSET", b"a", b"1", b"b", b"2", b"a", b"3"], OK);
    c.call(&[b"MGET", b"a", b"missing", b"b"], b"*3\r\n$1\r\n3\r\n$-1\r\n$1\r\n2\r\n");
    c.call(&[b"EXISTS", b"a", b"a", b"missing", b"b"], b":3\r\n");
    c.call(&[b"MSET", b"a", b"1", b"b"], b"-ERR wrong number of arguments for 'mset' command\r\n");
    c.call(&[b"DBSIZE"], b":2\r\n");
}

#[test]
fn string_commands() {
    let mut c = Client::connect(start());
    c.call(&[b"SETNX", b"k", b"v"], b":1\r\n");
    c.call(&[b"SETNX", b"k", b"w"], b":0\r\n");
    c.call(&[b"APPEND", b"k", b"xyz"], b":4\r\n");
    c.call(&[b"APPEND", b"new", b"ab"], b":2\r\n");
    c.call(&[b"STRLEN", b"k"], b":4\r\n");
    c.call(&[b"STRLEN", b"missing"], b":0\r\n");
    c.call(&[b"GETDEL", b"k"], b"$4\r\nvxyz\r\n");
    c.call(&[b"GETDEL", b"k"], NIL);
    c.call(&[b"TYPE", b"new"], b"+string\r\n");
    c.call(&[b"TYPE", b"k"], b"+none\r\n");
}

#[test]
fn expire_commands() {
    let mut c = Client::connect(start());
    c.call(&[b"SET", b"k", b"v"], OK);
    c.call(&[b"TTL", b"k"], b":-1\r\n");
    c.call(&[b"TTL", b"missing"], b":-2\r\n");
    c.call(&[b"EXPIRE", b"k", b"100"], b":1\r\n");
    c.call(&[b"TTL", b"k"], b":100\r\n");
    c.call(&[b"EXPIRE", b"k", b"50", b"GT"], b":0\r\n");
    c.call(&[b"EXPIRE", b"k", b"50", b"lt"], b":1\r\n");
    c.call(&[b"EXPIRE", b"k", b"10", b"NX"], b":0\r\n");
    c.call(&[b"PERSIST", b"k"], b":1\r\n");
    c.call(&[b"PERSIST", b"k"], b":0\r\n");
    c.call(&[b"PEXPIRE", b"k", b"100000"], b":1\r\n");
    c.call(&[b"PEXPIREAT", b"k", b"99999999999999"], b":1\r\n");
    c.call(&[b"EXPIRE", b"missing", b"10"], b":0\r\n");
    // A time in the past deletes the key.
    c.call(&[b"EXPIREAT", b"k", b"1"], b":1\r\n");
    c.call(&[b"EXISTS", b"k"], b":0\r\n");
    c.call(&[b"PTTL", b"k"], b":-2\r\n");
}

#[test]
fn expire_errors_come_in_redis_order() {
    let mut c = Client::connect(start());
    // Unknown option first (cut at a NUL, like Redis's %s), then the
    // conflicts, then the time.
    c.call(&[b"EXPIRE", b"k", b"abc", b"NX", b"F\0OO"], b"-ERR Unsupported option F\r\n");
    c.call(&[b"EXPIRE", b"k", b"abc", b"NX", b"GT"], b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n");
    c.call(&[b"EXPIRE", b"k", b"abc", b"GT", b"LT"], b"-ERR GT and LT options at the same time are not compatible\r\n");
    c.call(&[b"EXPIRE", b"k", b"abc"], b"-ERR value is not an integer or out of range\r\n");
    c.call(&[b"SET", b"k", b"v"], OK);
    c.call(&[b"EXPIRE", b"k", b"9223372036854775807"], b"-ERR invalid expire time in 'expire' command\r\n");
}

#[test]
fn select_only_database_zero() {
    let mut c = Client::connect(start());
    c.call(&[b"SELECT", b"0"], OK);
    c.call(&[b"SELECT", b"1"], b"-ERR DB index is out of range\r\n");
    c.call(&[b"SELECT", b"-1"], b"-ERR DB index is out of range\r\n");
    c.call(&[b"SELECT", b"x"], b"-ERR value is not an integer or out of range\r\n");
    c.call(
        &[b"SELECT", b"99999999999"],
        b"-ERR value is out of range, value must between -2147483648 and 2147483647\r\n",
    );
}

/// Without `--cluster`, as a Redis without `cluster-enabled`: the cluster
/// commands refuse after the subcommand and arity checks, keys aren't
/// checked for slots.
#[test]
fn cluster_support_disabled() {
    let mut c = Client::connect(start());
    let disabled = b"-ERR This instance has cluster support disabled\r\n";
    for args in [
        &[&b"CLUSTER"[..], b"INFO"][..],
        &[b"CLUSTER", b"KEYSLOT", b"foo"],
        &[b"cluster", b"myid"],
        &[b"CLUSTER", b"SLOTS"],
        &[b"CLUSTER", b"SHARDS"],
        &[b"CLUSTER", b"NODES"],
        &[b"CLUSTER", b"COUNTKEYSINSLOT", b"x"],
        &[b"CLUSTER", b"SETTABLE", b"1", b"NODES", b"0", b"RANGES", b"0"],
        &[b"READONLY"],
        &[b"READWRITE"],
        &[b"ASKING"],
    ] {
        c.call(args, disabled);
    }
    c.call(&[b"CLUSTER"], b"-ERR wrong number of arguments for 'cluster' command\r\n");
    c.call(&[b"CLUSTER", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try CLUSTER HELP.\r\n");
    c.call(&[b"CLUSTER", b"KEYSLOT"], b"-ERR wrong number of arguments for 'cluster|keyslot' command\r\n");
    c.call(&[b"READONLY", b"x"], b"-ERR wrong number of arguments for 'readonly' command\r\n");
    c.call(&[b"INFO", b"cluster"], b"$30\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n");
    c.call(&[b"MSET", b"foo", b"1", b"bar", b"2"], OK);
    c.call(&[b"MGET", b"foo", b"bar"], b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n");
}

/// HELLO's reply for connection `id`, as Redis 8.10.2 sends it (without
/// its built-in module, see `tools/compat`).
fn hello_reply(proto: u8, id: &str) -> Vec<u8> {
    let head = if proto == 3 { "%7".to_string() } else { "*14".to_string() };
    format!(
        "{head}\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$6\r\n8.10.2\r\n$5\r\nproto\r\n:{proto}\r\n\
         $2\r\nid\r\n:{id}\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
    )
    .into_bytes()
}


#[test]
fn hello_switches_the_protocol_per_connection() {
    let addr = start();
    let mut c = Client::connect(addr);
    let id = c.id();
    c.call(&[b"HELLO"], &hello_reply(2, &id));
    c.call(&[b"HELLO", b"3"], &hello_reply(3, &id));
    // RESP3 nulls, also inside an array; other replies are unchanged.
    c.call(&[b"GET", b"missing"], b"_\r\n");
    c.call(&[b"MGET", b"missing", b"missing"], b"*2\r\n_\r\n_\r\n");
    c.call(&[b"SET", b"k", b"v", b"NX"], OK);
    c.call(&[b"SET", b"k", b"v", b"NX"], b"_\r\n");
    c.call(&[b"PING"], b"+PONG\r\n");
    // Another connection still speaks RESP2.
    Client::connect(addr).call(&[b"GET", b"missing"], NIL);
    c.call(&[b"HELLO", b"2"], &hello_reply(2, &id));
    c.call(&[b"GET", b"missing"], NIL);
}

#[test]
fn hello_options_and_errors() {
    let mut c = Client::connect(start());
    let id = c.id();
    c.call(&[b"HELLO", b"1"], b"-NOPROTO unsupported protocol version\r\n");
    c.call(&[b"HELLO", b"4"], b"-NOPROTO unsupported protocol version\r\n");
    c.call(&[b"HELLO", b"abc"], b"-ERR Protocol version is not an integer or out of range\r\n");
    c.call(&[b"HELLO", b"3", b"FOO"], b"-ERR Syntax error in HELLO option 'FOO'\r\n");
    c.call(&[b"HELLO", b"3", b"AUTH", b"default"], b"-ERR Syntax error in HELLO option 'AUTH'\r\n");
    c.call(&[b"HELLO", b"3", b"AUTH", b"bob", b"pw"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    c.call(&[b"HELLO", b"3", b"SETNAME", b"a b"], b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n");
    // None of the failures switched the protocol.
    c.call(&[b"GET", b"missing"], NIL);
    c.call(&[b"HELLO", b"3", b"AUTH", b"default", b"any", b"SETNAME", b"conn"], &hello_reply(3, &id));
    c.call(&[b"CLIENT", b"GETNAME"], b"$4\r\nconn\r\n");
}

#[test]
fn client_names_and_ids() {
    let addr = start();
    let (mut a, mut b) = (Client::connect(addr), Client::connect(addr));
    let (id_a, id_b) = (a.id(), b.id());
    assert!(id_b.parse::<u64>().unwrap() > id_a.parse::<u64>().unwrap());
    a.call(&[b"CLIENT", b"GETNAME"], NIL);
    a.call(&[b"client", b"setname", b"first"], OK);
    a.call(&[b"CLIENT", b"GETNAME"], b"$5\r\nfirst\r\n");
    b.call(&[b"CLIENT", b"GETNAME"], NIL);
    a.call(&[b"CLIENT", b"SETNAME", b"a\nb"], b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n");
    a.call(&[b"CLIENT", b"SETNAME", b""], OK);
    a.call(&[b"CLIENT", b"GETNAME"], NIL);
}

#[test]
fn client_list_and_info() {
    let addr = start();
    let (mut a, mut b) = (Client::connect(addr), Client::connect(addr));
    let (id_a, id_b) = (a.id(), b.id());
    b.call(&[b"CLIENT", b"SETNAME", b"bee"], OK);
    a.cmd(&[b"CLIENT", b"LIST"]);
    let list = a.read_text();
    let lines: Vec<&str> = list.lines().collect();
    assert_eq!(lines.len(), 2, "{list}");
    assert!(lines[0].starts_with(&format!("id={id_a} addr=127.0.0.1:")), "{}", lines[0]);
    assert!(lines[0].contains(" name= ") && lines[0].contains(" cmd=client|list "), "{}", lines[0]);
    assert!(lines[1].starts_with(&format!("id={id_b} ")) && lines[1].contains(" name=bee "), "{}", lines[1]);
    assert!(lines[1].contains(" cmd=client|setname ") && lines[1].contains(" resp=2 lib-name= lib-ver= "), "{}", lines[1]);
    assert!(lines[1].contains(" tot-cmds=2 ") && lines[1].ends_with(" avg-pipeline-len-cnt=2"), "{}", lines[1]);
    // Redis 7.2's CLIENT SETINFO, 7.4's NO-TOUCH (flag T).
    b.call(&[b"CLIENT", b"SETINFO", b"lib-name", b"redis-py"], OK);
    b.call(&[b"CLIENT", b"SETINFO", b"LIB-VER", b"5.0.1"], OK);
    b.call(&[b"CLIENT", b"NO-TOUCH", b"on"], OK);
    b.cmd(&[b"CLIENT", b"INFO"]);
    let info = b.read_text();
    assert!(info.contains(" flags=T ") && info.contains(" lib-name=redis-py lib-ver=5.0.1 "), "{info}");
    b.call(&[b"CLIENT", b"SETINFO", b"lib-name", b""], OK);
    b.call(&[b"CLIENT", b"NO-TOUCH", b"OFF"], OK);
    b.cmd(&[b"CLIENT", b"INFO"]);
    let info = b.read_text();
    assert!(info.contains(" flags=N ") && info.contains(" lib-name= lib-ver=5.0.1 "), "{info}");

    a.cmd(&[b"CLIENT", b"LIST", b"ID", id_b.as_bytes()]);
    assert!(a.read_text().starts_with(&format!("id={id_b} ")));
    a.call(&[b"CLIENT", b"LIST", b"TYPE", b"pubsub"], b"$0\r\n\r\n");
    a.call(&[b"CLIENT", b"LIST", b"TYPE", b"nope"], b"-ERR Unknown client type 'nope'\r\n");
    a.call(&[b"CLIENT", b"LIST", b"ID", b"x"], b"-ERR Invalid client ID\r\n");
    a.call(&[b"CLIENT", b"LIST", b"foo"], b"-ERR syntax error\r\n");

    a.call(&[b"HELLO", b"3"], &hello_reply(3, &id_a));
    a.cmd(&[b"CLIENT", b"INFO"]);
    let info = a.read_text();
    assert!(info.starts_with(&format!("txt:id={id_a} ")) && info.contains(" cmd=client|info "), "{info}");
    assert!(info.contains(" resp=3 ") && info.ends_with(" avg-pipeline-len-cnt=8\n"), "{info}");

    // A closed connection leaves the list.
    drop(b);
    thread::sleep(Duration::from_millis(50));
    a.cmd(&[b"CLIENT", b"LIST"]);
    assert_eq!(a.read_text().lines().count(), 1);
}

#[test]
fn client_errors() {
    let mut c = Client::connect(start());
    c.call(&[b"CLIENT"], b"-ERR wrong number of arguments for 'client' command\r\n");
    c.call(&[b"CLIENT", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try CLIENT HELP.\r\n");
    c.call(&[b"client", b"setinfo", b"LIB-NAME", b"x y"], b"-ERR LIB-NAME cannot contain spaces, newlines or special characters.\r\n");
    c.call(&[b"CLIENT", b"SETINFO", b"lib-x", b"x"], b"-ERR Unrecognized option 'lib-x'\r\n");
    c.call(&[b"CLIENT", b"SETINFO", b"lib-name"], b"-ERR wrong number of arguments for 'client|setinfo' command\r\n");
    c.call(&[b"CLIENT", b"NO-TOUCH", b"maybe"], b"-ERR syntax error\r\n");
    c.call(&[b"CLIENT", b"SETNAME"], b"-ERR wrong number of arguments for 'client|setname' command\r\n");
    c.call(&[b"CLIENT", b"ID", b"x"], b"-ERR wrong number of arguments for 'client|id' command\r\n");
}

#[test]
fn command_introspection() {
    let mut c = Client::connect(start());
    c.cmd(&[b"COMMAND", b"COUNT"]);
    let count: usize = c.read_integer().parse().unwrap();
    // COMMAND lists as many as COUNT says, the first being GET.
    c.cmd(&[b"COMMAND"]);
    assert_eq!(c.read_line(), format!("*{count}\r\n").into_bytes());
    c.expect(b"*10\r\n$3\r\nget\r\n:2\r\n");
    drop(c);
    let mut c = Client::connect(start());
    c.call(
        &[b"COMMAND", b"INFO", b"get", b"nosuch", b"MSET"],
        b"*3\r\n\
          *10\r\n$3\r\nget\r\n:2\r\n*0\r\n:1\r\n:1\r\n:1\r\n*0\r\n*0\r\n*0\r\n*0\r\n\
          $-1\r\n\
          *10\r\n$4\r\nmset\r\n:-3\r\n*0\r\n:1\r\n:-1\r\n:2\r\n*0\r\n*0\r\n*0\r\n*0\r\n",
    );
    c.call(&[b"COMMAND", b"DOCS"], b"*0\r\n");
    c.call(&[b"COMMAND", b"FOO"], b"-ERR unknown subcommand 'FOO'. Try COMMAND HELP.\r\n");
}

#[test]
fn config_get() {
    let addr = start();
    let mut c = Client::connect(addr);
    // What redis-benchmark asks on startup.
    c.call(&[b"CONFIG", b"GET", b"save"], b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n");
    c.call(&[b"CONFIG", b"GET", b"appendonly"], b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n");
    // An exact name comes back as sent; a pattern gives the real names; each once.
    c.call(&[b"CONFIG", b"GET", b"SAVE"], b"*2\r\n$4\r\nSAVE\r\n$0\r\n\r\n");
    c.call(
        &[b"CONFIG", b"GET", b"append*", b"save", b"save"],
        b"*4\r\n$10\r\nappendonly\r\n$2\r\nno\r\n$4\r\nsave\r\n$0\r\n\r\n",
    );
    let port = addr.port().to_string();
    c.call(&[b"CONFIG", b"GET", b"po?t"], format!("*2\r\n$4\r\nport\r\n${}\r\n{port}\r\n", port.len()).as_bytes());
    c.call(&[b"CONFIG", b"GET", b"nosuchparam"], b"*0\r\n");
    c.call(&[b"CONFIG", b"GET"], b"-ERR wrong number of arguments for 'config|get' command\r\n");
    c.call(&[b"CONFIG"], b"-ERR wrong number of arguments for 'config' command\r\n");
    c.call(&[b"CONFIG", b"SET", b"save", b""], b"-ERR Unknown option or number of arguments for CONFIG SET - 'save'\r\n");
    c.call(&[b"CONFIG", b"REWRITE"], b"-ERR unknown subcommand 'REWRITE'. Try CONFIG HELP.\r\n");
    let id = c.id();
    c.call(&[b"HELLO", b"3"], &hello_reply(3, &id));
    c.call(&[b"CONFIG", b"GET", b"save"], b"%1\r\n$4\r\nsave\r\n$0\r\n\r\n");
}

#[test]
fn info_sections() {
    let mut c = Client::connect(start());
    c.call(&[b"INFO", b"keyspace"], b"$12\r\n# Keyspace\r\n\r\n");
    c.call(&[b"SET", b"a", b"1"], OK);
    c.call(&[b"SET", b"b", b"2", b"EX", b"100"], OK);
    c.call(&[b"INFO", b"KEYSPACE"], b"$56\r\n# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0,subexpiry=0\r\n\r\n");
    c.call(&[b"INFO", b"nosuchsection"], b"$0\r\n\r\n");

    c.cmd(&[b"INFO"]);
    let all = c.read_text();
    for needle in ["# Server\r\nredis_version:8.10.2\r\n", "\r\n\r\n# Clients\r\nconnected_clients:1\r\n", "\r\nrole:master\r\n", "\r\n# Keyspace\r\n"] {
        assert!(all.contains(needle), "{needle:?} missing in {all}");
    }
    assert!(all.ends_with("avg_ttl=0,subexpiry=0\r\n"), "{all}");

    c.cmd(&[b"INFO", b"stats", b"server"]);
    let two = c.read_text();
    assert!(two.starts_with("# Server\r\n") && two.contains("\r\n\r\n# Stats\r\n") && !two.contains("# Keyspace"), "{two}");

    let id = c.id();
    c.call(&[b"HELLO", b"3"], &hello_reply(3, &id));
    c.call(&[b"INFO", b"keyspace"], b"=60\r\ntxt:# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0,subexpiry=0\r\n\r\n");
}

#[test]
fn idle_connections_are_closed() {
    idle_connections_closed(Duration::ZERO);
}

#[test]
fn idle_connections_are_closed_when_busy_polling() {
    idle_connections_closed(Duration::from_micros(200));
}

fn idle_connections_closed(busy_poll: Duration) {
    let addr = start_with(Config { idle_timeout: Some(Duration::from_millis(300)), busy_poll, ..Config::default() });
    let mut idle = Client::connect(addr);
    idle.call(&[b"PING"], b"+PONG\r\n");
    let mut half_sent = Client::connect(addr);
    half_sent.send(b"*2\r\n$3\r\nGET\r\n");
    // Busy for longer than the timeout, never silent for as long.
    let mut busy = Client::connect(addr);
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(700) {
        busy.call(&[b"PING"], b"+PONG\r\n");
        thread::sleep(Duration::from_millis(100));
    }
    // The other two were closed without a reply.
    idle.expect_closed();
    half_sent.expect_closed();
    // Whole seconds, rounded up.
    busy.call(&[b"CONFIG", b"GET", b"timeout"], b"*2\r\n$7\r\ntimeout\r\n$1\r\n1\r\n");
}

#[test]
fn idle_timeout_waits_long_enough() {
    let addr = start_with(Config { idle_timeout: Some(Duration::from_secs(2)), ..Config::default() });
    let mut c = Client::connect(addr);
    thread::sleep(Duration::from_millis(500));
    c.call(&[b"CONFIG", b"GET", b"timeout"], b"*2\r\n$7\r\ntimeout\r\n$1\r\n2\r\n");
}

#[test]
fn no_idle_timeout_by_default() {
    let addr = start_with(Config { idle_timeout: Some(Duration::ZERO), ..Config::default() });
    let mut c = Client::connect(addr);
    thread::sleep(Duration::from_millis(300));
    c.call(&[b"PING"], b"+PONG\r\n");
    c.call(&[b"CONFIG", b"GET", b"timeout"], b"*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n");
}

/// `--requirepass` (see `dbnode::auth`; the replies are checked against
/// Redis 8.10.2 by difftest's auth cases): nothing but AUTH, HELLO and
/// QUIT before the password, the links of replicas and slot copies
/// included; a client connected while there was no password, or that
/// logged in, stays authenticated when it changes.
#[test]
fn a_password_is_required() {
    let password = Some(dbnode::auth::Secret(b"s3cret".to_vec()));
    let addr = start_with(Config { password, ..Config::default() });
    let mut c = Client::connect(addr);
    const NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";
    c.call(&[b"GET", b"k"], NOAUTH);
    c.call(&[b"CONFIG", b"GET", b"requirepass"], NOAUTH);
    c.call(&[b"GET"], b"-ERR wrong number of arguments for 'get' command\r\n");
    c.call(&[b"REPLSYNC", b"x", b"?", b"-1"], NOAUTH);
    c.call(&[b"SLOTSYNC", b"x", b"1"], NOAUTH);
    c.call(&[b"AUTH", b"secret"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    c.call(&[b"HELLO", b"2", b"AUTH", b"default", b"s3cre"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    c.call(&[b"AUTH", b"default", b"s3cret"], OK);
    c.call(&[b"GET", b"k"], NIL);
    c.call(&[b"INFO", b"stats"], b"");
    let mut stats = vec![0; 4096];
    let n = std::io::Read::read(&mut c.0, &mut stats).unwrap();
    assert!(String::from_utf8_lossy(&stats[..n]).contains("\r\nacl_access_denied_auth:2\r\n"));
    c.call(&[b"CONFIG", b"GET", b"requirepass"], b"*2\r\n$11\r\nrequirepass\r\n$6\r\ns3cret\r\n");
    let mut h = Client::connect(addr);
    h.cmd(&[b"HELLO", b"3", b"AUTH", b"default", b"s3cret", b"SETNAME", b"h"]);
    h.expect(b"%7\r\n$6\r\nserver\r\n");
    let mut h = Client::connect(addr);
    let name_error = b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n";
    h.call(&[b"HELLO", b"2", b"AUTH", b"default", b"s3cret", b"SETNAME", b"a b"], name_error);
    h.call(&[b"PING"], NOAUTH);
    h.cmd(&[b"HELLO", b"2", b"AUTH", b"default", b"s3cret", b"SETNAME", b"h"]);
    h.expect(b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n");

    // Requests are small until then: 10 arguments, 16 KiB each.
    let mut big = Client::connect(addr);
    big.send(b"*11\r\n");
    big.expect(b"-ERR Protocol error: unauthenticated multibulk length\r\n");
    big.expect_closed();
    let mut big = Client::connect(addr);
    big.send(b"*2\r\n$4\r\nAUTH\r\n$16385\r\n");
    big.expect(b"-ERR Protocol error: unauthenticated bulk length\r\n");
    big.expect_closed();
    let mut big = Client::connect(addr);
    let value = vec![b'v'; 20000];
    big.send(&[request(&[b"AUTH", b"s3cret"]), request(&[b"SET", b"k", &value]), request(&[b"STRLEN", b"k"])].concat());
    big.expect(b"+OK\r\n+OK\r\n:20000\r\n");

    // A new password: those logged in stay so, new clients need it.
    c.call(&[b"CONFIG", b"SET", b"requirepass", b"other"], OK);
    c.call(&[b"PING"], b"+PONG\r\n");
    let mut d = Client::connect(addr);
    d.call(&[b"AUTH", b"s3cret"], b"-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    d.call(&[b"AUTH", b"other"], OK);
    // None: everyone may; one connected meanwhile stays authenticated.
    c.call(&[b"CONFIG", b"SET", b"requirepass", b""], OK);
    let mut e = Client::connect(addr);
    e.call(&[b"PING"], b"+PONG\r\n");
    c.call(&[b"CONFIG", b"SET", b"requirepass", b"s3cret"], OK);
    e.call(&[b"PING"], b"+PONG\r\n");
    Client::connect(addr).call(&[b"PING"], NOAUTH);
}

/// In cluster mode the password is the cluster's: set at start only.
#[test]
fn a_cluster_node_s_password_is_set_at_start() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cluster = Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() });
    let password = Some(dbnode::auth::Secret(b"s3cret".to_vec()));
    thread::spawn(move || dbnode::serve(listener, None, Config { cluster, password, ..Config::default() }).unwrap());
    let mut c = Client::connect(addr);
    c.call(&[b"CLUSTER", b"MYID"], b"-NOAUTH Authentication required.\r\n");
    c.call(&[b"AUTH", b"internal connection", b"s3cret"], b"-WRONGPASS invalid internal password\r\n");
    c.call(&[b"AUTH", b"s3cret"], OK);
    c.call(
        &[b"CONFIG", b"SET", b"requirepass", b"x"],
        b"-ERR CONFIG SET failed (possibly related to argument 'requirepass') - in cluster mode the password is the cluster's, set at start (--requirepass, --requirepass-file)\r\n",
    );
    c.call(&[b"CONFIG", b"GET", b"requirepass"], b"*2\r\n$11\r\nrequirepass\r\n$6\r\ns3cret\r\n");
}

/// A client that goes away watching keys, in a transaction, leaves
/// nothing behind: its queued commands never run, others' WATCH goes on.
#[test]
fn a_client_leaving_mid_transaction() {
    let addr = start();
    let mut a = Client::connect(addr);
    a.call(&[b"WATCH", b"k"], OK);
    a.call(&[b"MULTI"], OK);
    a.call(&[b"SET", b"k", b"1"], b"+QUEUED\r\n");
    drop(a);
    let mut b = Client::connect(addr);
    b.call(&[b"WATCH", b"k", b"j"], OK);
    // Until the server has seen a go.
    let mut c = Client::connect(addr);
    let gone = Instant::now();
    loop {
        c.cmd(&[b"CLIENT", b"LIST"]);
        if c.read_text().lines().count() == 2 {
            break;
        }
        assert!(gone.elapsed() < Duration::from_secs(5), "the client stays listed");
        thread::sleep(Duration::from_millis(10));
    }
    let watching = |c: &mut Client| {
        c.cmd(&[b"INFO", b"clients"]);
        let text = c.read_text();
        let field = |name: &str| text.lines().find_map(|l| l.strip_prefix(name)).unwrap().to_string();
        (field("watching_clients:"), field("total_watched_keys:"))
    };
    assert_eq!(watching(&mut c), ("1".into(), "2".into()), "a's watch gone with it");
    c.call(&[b"SET", b"k", b"2"], OK);
    assert_eq!(watching(&mut c), ("0".into(), "0".into()), "b's unwatched once touched");
    b.call(&[b"MULTI"], OK);
    b.call(&[b"PING"], b"+QUEUED\r\n");
    b.call(&[b"EXEC"], b"*-1\r\n");
    c.call(&[b"GET", b"k"], b"$1\r\n2\r\n");
    // Nothing watched now: writes are recorded for nobody.
    b.call(&[b"MULTI"], OK);
    b.call(&[b"PING"], b"+QUEUED\r\n");
    c.call(&[b"SET", b"j", b"1"], OK);
    b.call(&[b"EXEC"], b"*1\r\n+PONG\r\n");
}

#[test]
fn unknown_command() {
    let mut c = Client::connect(start());
    c.call(&[b"FOO"], b"-ERR unknown command 'FOO'\r\n");
    c.call(&[b"foo", b"a", b"b c"], b"-ERR unknown command 'foo', with args beginning with: 'a' 'b c' \r\n");
    c.call(&[b""], b"-ERR unknown command ''\r\n");
    // Newlines in the quoted input become spaces.
    c.call(&[b"BA\r\nR", b"x\ny"], b"-ERR unknown command 'BA  R', with args beginning with: 'x y' \r\n");
    // The connection still works.
    c.call(&[b"PING"], b"+PONG\r\n");
}

#[test]
fn unknown_command_cuts_long_input() {
    let mut c = Client::connect(start());
    let name = [b'N'; 300];
    let long = [b'x'; 300];
    let mut expected = b"-ERR unknown command '".to_vec();
    expected.extend_from_slice(&name[..128]);
    expected.extend_from_slice(b"', with args beginning with: '");
    expected.extend_from_slice(&long[..128]);
    expected.extend_from_slice(b"' \r\n");
    c.call(&[&name, &long, b"y"], &expected);

    // Arguments are listed until 128 bytes of them were (counting quotes
    // and spaces), the last one cut to what's left.
    let args: Vec<Vec<u8>> = (0..30).map(|i| i.to_string().into_bytes()).collect();
    let mut cmd: Vec<&[u8]> = vec![b"FOO"];
    cmd.extend(args.iter().map(Vec::as_slice));
    let mut listed = String::new();
    for arg in &args {
        if listed.len() >= 128 {
            break;
        }
        let arg = std::str::from_utf8(arg).unwrap();
        listed += &format!("'{}' ", &arg[..arg.len().min(128 - listed.len())]);
    }
    let expected = format!("-ERR unknown command 'FOO', with args beginning with: {listed}\r\n");
    c.call(&cmd, expected.as_bytes());
}

#[test]
fn arity_errors() {
    let mut c = Client::connect(start());
    for (args, name) in [
        (&[&b"GET"[..]][..], "get"),
        (&[b"GET", b"a", b"b"], "get"),
        (&[b"set", b"k"], "set"),
        (&[b"DEL"], "del"),
        (&[b"ECHO"], "echo"),
        (&[b"INCR", b"a", b"b"], "incr"),
        (&[b"MGET"], "mget"),
        (&[b"MSET", b"k"], "mset"),
        (&[b"EXPIRE", b"k"], "expire"),
        (&[b"TTL"], "ttl"),
        (&[b"DBSIZE", b"x"], "dbsize"),
        (&[b"SELECT", b"0", b"0"], "select"),
    ] {
        c.call(args, format!("-ERR wrong number of arguments for '{name}' command\r\n").as_bytes());
    }
}

#[test]
fn flushall() {
    let mut c = Client::connect(start());
    c.call(&[b"SET", b"a", b"1"], OK);
    c.call(&[b"SET", b"b", b"2", b"EX", b"100"], OK);
    c.call(&[b"FLUSHALL"], OK);
    c.call(&[b"DEL", b"a", b"b"], b":0\r\n");
    c.call(&[b"flushdb", b"async"], OK);
    c.call(&[b"FLUSHALL", b"SYNC"], OK);
    c.call(&[b"FLUSHALL", b"later"], b"-ERR syntax error\r\n");
    c.call(&[b"FLUSHALL", b"SYNC", b"ASYNC"], b"-ERR syntax error\r\n");
}

#[test]
fn connections_share_the_keyspace() {
    let addr = start();
    let (mut a, mut b) = (Client::connect(addr), Client::connect(addr));
    a.call(&[b"SET", b"k", b"from a"], OK);
    b.call(&[b"GET", b"k"], b"$6\r\nfrom a\r\n");
}

#[test]
fn pipelined_requests_are_answered_in_order() {
    let mut c = Client::connect(start());
    let mut requests = Vec::new();
    let mut replies = Vec::new();
    for i in 0..1000 {
        let (key, value) = (format!("k{i}"), format!("v{i}"));
        requests.extend(request(&[b"SET", key.as_bytes(), value.as_bytes()]));
        requests.extend(request(&[b"GET", key.as_bytes()]));
        replies.extend_from_slice(OK);
        replies.extend(format!("${}\r\n{value}\r\n", value.len()).into_bytes());
    }
    c.send(&requests);
    c.expect(&replies);
}

#[test]
fn requests_split_anywhere_still_work() {
    let mut c = Client::connect(start());
    let mut requests = request(&[b"SET", b"key", b"value"]);
    requests.extend(request(&[b"GET", b"key"]));
    requests.extend_from_slice(b"PING\r\n");
    for byte in &requests {
        c.send(&[*byte]);
        thread::sleep(Duration::from_micros(200));
    }
    c.expect(b"+OK\r\n$5\r\nvalue\r\n+PONG\r\n");
}

#[test]
fn inline_requests() {
    let mut c = Client::connect(start());
    c.send(b"SET k \"two words\"\r\nGET k\nping\r\n\r\n   \r\nECHO 'it\\'s'\r\n");
    c.expect(b"+OK\r\n$9\r\ntwo words\r\n+PONG\r\n$4\r\nit's\r\n");
    // A closing quote must be followed by a space: a protocol error.
    c.send(b"ECHO 'it''s'\r\n");
    c.expect(b"-ERR Protocol error: unbalanced quotes in request\r\n");
    c.expect_closed();
}

#[test]
fn large_values() {
    let mut c = Client::connect(start());
    let value: Vec<u8> = (0..5 << 20).map(|i: u32| (i % 251) as u8).collect();
    c.call(&[b"SET", b"big", &value], OK);
    c.cmd(&[b"GET", b"big"]);
    let mut expected = format!("${}\r\n", value.len()).into_bytes();
    expected.extend_from_slice(&value);
    expected.extend_from_slice(b"\r\n");
    c.expect(&expected);
    // The connection is fine afterwards (its buffer shrank back).
    c.call(&[b"GET", b"missing"], NIL);
}

#[test]
fn many_large_replies_in_one_pipeline() {
    // More reply bytes than one batch buffers: flushed along the way.
    let mut c = Client::connect(start());
    let value = vec![b'v'; 100_000];
    c.call(&[b"SET", b"k", &value], OK);
    let mut requests = Vec::new();
    for _ in 0..50 {
        requests.extend(request(&[b"GET", b"k"]));
    }
    c.send(&requests);
    let mut one = b"$100000\r\n".to_vec();
    one.extend_from_slice(&value);
    one.extend_from_slice(b"\r\n");
    c.expect(&one.repeat(50));
}

#[test]
fn protocol_error_replies_then_closes() {
    let mut c = Client::connect(start());
    // The requests before the bad one are answered.
    c.send(b"PING\r\n*1\r\n$4\r\nPING\r\n*x\r\nPING\r\n");
    c.expect(b"+PONG\r\n+PONG\r\n-ERR Protocol error: invalid multibulk length\r\n");
    c.expect_closed();
}

#[test]
fn quit_replies_then_closes_without_running_the_rest() {
    let addr = start();
    let mut c = Client::connect(addr);
    let mut requests = request(&[b"QUIT"]);
    requests.extend(request(&[b"SET", b"k", b"v"]));
    c.send(&requests);
    c.expect(OK);
    c.expect_closed();
    Client::connect(addr).call(&[b"GET", b"k"], NIL);
}

#[test]
fn empty_requests_get_no_reply() {
    let mut c = Client::connect(start());
    c.send(b"*0\r\n\r\n*-1\r\nPING\r\n");
    c.expect(b"+PONG\r\n");
}

#[test]
fn a_client_vanishing_mid_request_leaves_the_server_running() {
    let addr = start();
    let mut gone = Client::connect(addr);
    gone.send(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\nonly part");
    drop(gone);
    let mut c = Client::connect(addr);
    c.call(&[b"GET", b"k"], NIL);
    c.call(&[b"PING"], b"+PONG\r\n");
}

#[test]
fn many_concurrent_clients() {
    let addr = start();
    let threads: Vec<_> = (0..20)
        .map(|t| {
            thread::spawn(move || {
                let mut c = Client::connect(addr);
                for i in 0..200 {
                    let key = format!("t{t}k{i}");
                    c.call(&[b"SET", key.as_bytes(), key.as_bytes()], OK);
                    c.call(&[b"GET", key.as_bytes()], format!("${}\r\n{key}\r\n", key.len()).as_bytes());
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
}
