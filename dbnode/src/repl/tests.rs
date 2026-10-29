//! The stream checked by applying it: random command sequences run on one
//! node, its stream on another, which never expires keys by itself; then
//! every key is the same at both (value, absolute expiry, encoding).

use super::*;
use crate::client::Client;
use crate::command::{execute, COMMANDS};
use crate::{Config, Node};
use resp::{Encoder, Limits, Parsed, Parser, Protocol};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::time::Instant;
use store::{decode_dump, Dump};

const T0: i64 = 1_700_000_000_000;

/// A node as `serve` makes one, without a reactor: enough for `execute`
/// of the data commands.
fn node(backlog: usize) -> Node {
    node_with(backlog, store::Store::new(), Config::default())
}

fn node_with(backlog: usize, store: store::Store, config: Config) -> Node {
    Node {
        config,
        store: RefCell::new(store),
        clients: Default::default(),
        port: 0,
        started: Instant::now(),
        connections_received: Cell::new(0),
        commands_processed: Cell::new(0),
        commands_refused_oom: Cell::new(0),
        maxmemory: Cell::new(0),
        startup_memory: 0,
        peak_memory: Cell::new(0),
        cluster: None,
        repl: Replication::new(NodeId::from_bytes(&[7; 20]), backlog),
    }
}

fn client() -> Client {
    Client::new(1, None, None, -1)
}

/// Runs each request of `bytes` (RESP) at `node`; the replies.
fn run(node: &Node, client: &Client, bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut parser = Parser::new(Limits::default());
    let (mut at, mut replies) = (0, Vec::new());
    while at < bytes.len() {
        match parser.parse(&bytes[at..]).unwrap() {
            Parsed::Complete { consumed } => {
                let mut out = Encoder::new(Protocol::Resp2);
                execute(&parser.args(&bytes[at..at + consumed]), node, client, &mut out);
                replies.push(out.into_inner());
                at += consumed;
            }
            Parsed::Incomplete => panic!("a command cut short: {:?}", bytes[at..].escape_ascii().to_string()),
        }
    }
    replies
}

fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Encoder::new(Protocol::Resp2);
    out.array(args.len());
    for a in args {
        out.bulk(a);
    }
    out.into_inner()
}

/// The stream as text, a command per line.
fn text(stream: &[u8]) -> Vec<String> {
    let mut parser = Parser::new(Limits::default());
    let (mut at, mut lines) = (0, Vec::new());
    while at < stream.len() {
        let Ok(Parsed::Complete { consumed }) = parser.parse(&stream[at..]) else { panic!("not RESP") };
        let args = parser.args(&stream[at..at + consumed]);
        lines.push(args.iter().map(|a| String::from_utf8_lossy(a).to_string()).collect::<Vec<_>>().join(" "));
        at += consumed;
    }
    lines
}

#[test]
fn writes_go_in_as_their_effects() {
    let a = node(BACKLOG_SIZE);
    let c = client();
    a.store.borrow_mut().set_now(T0);
    // Before the stream starts: nothing, at no cost.
    run(&a, &c, &request(&[b"SET", b"k", b"v"]));
    assert_eq!(a.repl.offset(), 0);
    assert_eq!(a.repl.backlog_info(), (0, 0));
    a.repl.start(&mut a.store.borrow_mut());
    assert_eq!(a.repl.backlog_info(), (1, 0), "as Redis's: the first byte's offset is 1");

    let commands: &[&[&[u8]]] = &[
        &[b"SET", b"k", b"v2"],
        &[b"GET", b"k"],
        &[b"SET", b"t", b"x", b"EX", b"10"],
        &[b"EXPIRE", b"k", b"5", b"NX"],
        &[b"EXPIRE", b"k", b"5", b"NX"],
        &[b"EXPIRE", b"missing", b"5"],
        &[b"INCRBYFLOAT", b"f", b"1.5"],
        &[b"HINCRBYFLOAT", b"h", b"a", b"0.5"],
        &[b"INCR", b"k"],
        &[b"SADD", b"s", b"1"],
        &[b"SPOP", b"s"],
        &[b"SPOP", b"s"],
        &[b"DEL", b"missing"],
        &[b"LMPOP", b"1", b"l", b"LEFT"],
        &[b"SET", b"k"],
        &[b"PEXPIRE", b"t", b"-1"],
        &[b"FLUSHALL"],
    ];
    let replies: Vec<Vec<u8>> = commands.iter().flat_map(|args| run(&a, &c, &request(args))).collect();
    assert!(replies[8].starts_with(b"-ERR value is not an integer"), "INCR of v2");
    let stream = a.repl.since(0).unwrap();
    assert_eq!(stream.len() as u64, a.repl.offset());
    assert_eq!(text(&stream), [
        "SET k v2".to_string(),
        format!("SET t x PXAT {}", T0 + 10_000),
        format!("PEXPIREAT k {}", T0 + 5000),
        "SET f 1.5 KEEPTTL".into(),
        "HSET h a 0.5".into(),
        "SADD s 1".into(),
        "SREM s 1".into(),
        // The empty SPOP and the failing INCR: nothing. A DEL of nothing,
        // an LMPOP of nothing: as sent.
        "DEL missing".into(),
        "LMPOP 1 l LEFT".into(),
        "DEL t".into(),
        "FLUSHALL".into(),
    ]);

    // Keys reclaimed as expired, by a read or by the sweep, as DEL, in
    // order with the writes.
    run(&a, &c, &request(&[b"SET", b"e1", b"v", b"PX", b"5"]));
    run(&a, &c, &request(&[b"SET", b"e2", b"v", b"PX", b"5"]));
    let before = a.repl.offset();
    a.store.borrow_mut().set_now(T0 + 10);
    run(&a, &c, &request(&[b"GET", b"e1"]));
    run(&a, &c, &request(&[b"SET", b"e1", b"again"]));
    crate::expire_sweep(&a);
    assert_eq!(text(&a.repl.since(before).unwrap()), ["DEL e1", "SET e1 again", "DEL e2"]);
    let (first, len) = a.repl.backlog_info();
    assert_eq!((first, len as u64), (1, a.repl.offset()));
}

#[test]
fn a_command_from_the_primary_sees_its_keys() {
    // The replica's clock says the key expired; its primary hasn't said
    // so yet: its INCR goes to the key it has, which keeps its TTL.
    let r = node(BACKLOG_SIZE);
    let c = client();
    r.store.borrow_mut().set_reclaim(false);
    r.store.borrow_mut().set_now(T0);
    run(&r, &c, &request(&[b"SET", b"n", b"5", b"PXAT", (T0 + 10).to_string().as_bytes()]));
    r.store.borrow_mut().set_now(T0 + 100);
    let mut out = Encoder::new(Protocol::Resp2);
    let bytes = request(&[b"INCR", b"n"]);
    let mut parser = Parser::new(Limits::default());
    let Ok(Parsed::Complete { .. }) = parser.parse(&bytes) else { panic!() };
    crate::command::apply(&parser.args(&bytes), &r, &mut out);
    assert_eq!(out.as_bytes(), b":6\r\n");
    assert_eq!(r.store.borrow().now(), T0 + 100, "the clock is back");
    // A client sees it expired, and it stays.
    assert_eq!(run(&r, &c, &request(&[b"GET", b"n"])), [b"$-1\r\n".to_vec()]);
    assert_eq!(r.store.borrow().inspect(b"n").map(|(_, at, _)| at), Some(Some(T0 + 10)));
}

#[test]
fn the_backlog_keeps_the_last_bytes() {
    let r = Replication::new(NodeId::from_bytes(&[1; 20]), 64);
    let mut store = store::Store::new();
    r.start(&mut store);
    let mut all = Vec::new();
    for i in 0..40 {
        let key = format!("key{i}");
        r.append([&b"DEL"[..], key.as_bytes()], Touches::Nothing);
        all.extend(request(&[b"DEL", key.as_bytes()]));
        let offset = r.offset();
        assert_eq!(offset, all.len() as u64);
        let (first, len) = r.backlog_info();
        assert_eq!(len, all.len().min(64));
        assert_eq!(first, offset - len as u64 + 1);
        // What a replica at any offset still in it gets: the rest, exactly.
        for have in offset - len as u64..=offset {
            assert_eq!(r.since(have).unwrap(), all[have as usize..], "{i}: from {have}");
        }
        if len as u64 != offset {
            assert_eq!(r.since(offset - len as u64 - 1), None, "fallen out");
        }
        assert_eq!(r.since(offset + 1), None, "ahead");
    }
    // A command larger than the backlog: its end.
    let big = vec![b'x'; 200];
    r.append([&b"SET"[..], b"k", &big], Touches::Nothing);
    all.extend(request(&[b"SET", b"k", &big]));
    assert_eq!(r.since(r.offset() - 64).unwrap(), all[all.len() - 64..]);
}

#[test]
fn write_flags_are_redis_s() {
    // Redis 8.10.2's COMMAND INFO: the commands flagged `write`, of ours.
    let redis = "set del incr mset decr incrby decrby incrbyfloat setnx getdel append expire pexpire expireat pexpireat \
                 persist flushall flushdb hset hdel hmset hincrby hsetnx hincrbyfloat sadd srem spop smove sunionstore \
                 sinterstore sdiffstore lpush rpush lpop rpop lset linsert lrem ltrim lpushx rpushx lmove rpoplpush lmpop \
                 delex increx lmovem msetex";
    let ours: BTreeSet<&str> = COMMANDS.iter().filter(|c| c.write).map(|c| c.name).collect();
    assert_eq!(ours, redis.split_whitespace().collect());
    assert!(COMMANDS.iter().all(|c| c.subcommands.iter().all(|s| !s.write)));
}

/// xorshift64*, for the random sequences.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

/// A random request over a few keys of each kind (the kinds mix, so some
/// fail with WRONGTYPE), values that are integers or floats or neither,
/// sometimes many at once (so the encodings convert), and every expiry
/// option, times in the past included.
fn random_request(rng: &mut Rng, now: i64) -> Vec<Vec<u8>> {
    let key = |rng: &mut Rng| format!("{}{}", rng.pick(&["k", "h", "s", "l"]), rng.below(4));
    let value = |rng: &mut Rng| match rng.below(5) {
        0 => rng.below(1000).to_string(),
        1 => format!("{}.{}", rng.below(100), rng.below(100)),
        2 => "x".repeat(rng.below(80) as usize),
        _ => format!("v{}", rng.below(300)),
    };
    let many = |rng: &mut Rng| if rng.below(20) == 0 { 140 } else { 1 + rng.below(4) as usize };
    let k = key(rng);
    let mut r: Vec<String> = match rng.below(46) {
        0..=3 => {
            let mut r = vec!["SET".into(), k, value(rng)];
            match rng.below(7) {
                0 => r.extend(["EX".into(), (rng.below(3) + 1).to_string()]),
                1 => r.extend(["PX".into(), (rng.below(40) + 1).to_string()]),
                2 => r.extend(["EXAT".into(), ((now / 1000) + rng.below(3) as i64 - 1).to_string()]),
                3 => r.extend(["PXAT".into(), (now + rng.below(60) as i64 - 20).to_string()]),
                4 => r.push("KEEPTTL".into()),
                _ => {}
            }
            match rng.below(4) {
                0 => r.push("NX".into()),
                1 => r.push("XX".into()),
                _ => {}
            }
            if rng.below(3) == 0 {
                r.push("GET".into());
            }
            r
        }
        4 => vec!["MSET".into(), k, value(rng), key(rng), value(rng)],
        5 => vec!["SETNX".into(), k, value(rng)],
        6 => vec!["APPEND".into(), k, value(rng)],
        7 => vec![rng.pick(&["INCR", "DECR"]).into(), k],
        8 => vec![rng.pick(&["INCRBY", "DECRBY"]).into(), k, value(rng)],
        9 => vec!["INCRBYFLOAT".into(), k, value(rng)],
        10 => vec!["GETDEL".into(), k],
        11 => vec!["DEL".into(), k],
        12 | 13 => {
            let cmd = rng.pick(&["EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"]);
            let when = match cmd {
                "EXPIRE" => rng.below(4) as i64 - 1,
                "PEXPIRE" => rng.below(60) as i64 - 10,
                "EXPIREAT" => now / 1000 + rng.below(3) as i64 - 1,
                _ => now + rng.below(60) as i64 - 20,
            };
            let mut r = vec![cmd.into(), k, when.to_string()];
            if rng.below(2) == 0 {
                r.push(rng.pick(&["NX", "XX", "GT", "LT"]).into());
            }
            r
        }
        14 => vec!["PERSIST".into(), k],
        15 => vec![rng.pick(&["GET", "TTL", "PTTL", "TYPE", "EXISTS", "STRLEN"]).into(), k],
        16 => vec!["MGET".into(), k, key(rng)],
        17 | 18 => {
            let mut r = vec![rng.pick(&["HSET", "HMSET"]).into(), k];
            for _ in 0..many(rng) {
                r.extend([format!("f{}", rng.below(200)), value(rng)]);
            }
            r
        }
        19 => vec!["HDEL".into(), k, format!("f{}", rng.below(200)), format!("f{}", rng.below(200))],
        20 => vec!["HINCRBY".into(), k, format!("f{}", rng.below(5)), value(rng)],
        21 => vec!["HINCRBYFLOAT".into(), k, format!("f{}", rng.below(5)), value(rng)],
        22 => vec!["HSETNX".into(), k, format!("f{}", rng.below(10)), value(rng)],
        23 => vec![rng.pick(&["HGETALL", "HLEN", "HRANDFIELD"]).into(), k],
        24 | 25 => {
            let mut r = vec!["SADD".into(), k];
            for _ in 0..many(rng) {
                r.push(if rng.below(3) == 0 { value(rng) } else { rng.below(600).to_string() });
            }
            r
        }
        26 => vec!["SREM".into(), k, rng.below(600).to_string(), value(rng)],
        27 | 28 => {
            let mut r = vec!["SPOP".into(), k];
            if rng.below(2) == 0 {
                r.push(rng.below(5).to_string());
            }
            r
        }
        29 => vec!["SMOVE".into(), k, key(rng), rng.below(600).to_string()],
        30 => vec![rng.pick(&["SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"]).into(), k, key(rng), key(rng)],
        31 => vec![rng.pick(&["SRANDMEMBER", "SMEMBERS", "SCARD"]).into(), k],
        32 => vec!["SINTERCARD".into(), "2".into(), k, key(rng)],
        33 | 34 => {
            let mut r = vec![rng.pick(&["LPUSH", "RPUSH", "LPUSHX", "RPUSHX"]).into(), k];
            for _ in 0..many(rng) {
                r.push(value(rng));
            }
            r
        }
        35 => {
            let mut r = vec![rng.pick(&["LPOP", "RPOP"]).into(), k];
            if rng.below(2) == 0 {
                r.push(rng.below(4).to_string());
            }
            r
        }
        36 => vec!["LSET".into(), k, (rng.below(6) as i64 - 3).to_string(), value(rng)],
        37 => vec!["LINSERT".into(), k, rng.pick(&["BEFORE", "AFTER"]).into(), value(rng), value(rng)],
        38 => vec!["LREM".into(), k, (rng.below(5) as i64 - 2).to_string(), value(rng)],
        39 => vec!["LTRIM".into(), k, rng.below(3).to_string(), (rng.below(6) as i64 - 2).to_string()],
        40 => vec!["LMOVE".into(), k, key(rng), rng.pick(&["LEFT", "RIGHT"]).into(), rng.pick(&["LEFT", "RIGHT"]).into()],
        41 => vec!["RPOPLPUSH".into(), k, key(rng)],
        42 => vec!["LMPOP".into(), "2".into(), k, key(rng), rng.pick(&["LEFT", "RIGHT"]).into(), "COUNT".into(), (rng.below(3) + 1).to_string()],
        43 => vec![rng.pick(&["LRANGE", "LLEN"]).into(), k, "0".into(), "-1".into()],
        44 => vec!["OBJECT".into(), "ENCODING".into(), k],
        _ if rng.below(40) == 0 => vec!["FLUSHALL".into()],
        _ => vec!["DBSIZE".into()],
    };
    // LLEN takes the key only.
    if r[0] == "LLEN" {
        r.truncate(2);
    }
    r.into_iter().map(String::into_bytes).collect()
}

/// A key's value, expiry and encoding, as [`view`] gives them.
type View = (Vec<Vec<u8>>, Option<i64>, &'static str);

/// The keys and their views, `encoding` or not (a copy rebuilds its
/// values, which may then be encoded otherwise, as from Redis's RDB).
fn views(node: &Node, encoding: bool) -> Vec<(Vec<u8>, View)> {
    let mut keys: Vec<Vec<u8>> = node.store.borrow().keys().map(<[u8]>::to_vec).collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| view(node, &k).map(|(v, at, e)| (k, (v, at, if encoding { e } else { "" }))))
        .collect()
}

/// A key as compared: the value (a hash's fields and a set's members in
/// order, as their order may differ), the absolute expiry, the encoding.
fn view(node: &Node, key: &[u8]) -> Option<View> {
    let store = node.store.borrow();
    let (dump, at, encoding) = store.inspect(key)?;
    let items = match &dump {
        Dump::Str(s) => vec![b"string".to_vec(), s.to_vec()],
        Dump::Elements(kind, bytes, _) => {
            let mut items: Vec<Vec<u8>> = decode_dump(bytes).unwrap().into_iter().map(<[u8]>::to_vec).collect();
            match kind {
                store::Kind::Hash => {
                    let mut pairs: Vec<Vec<u8>> = items.chunks(2).map(|p| [&p[0][..], b"=", &p[1][..]].concat()).collect();
                    pairs.sort();
                    items = pairs;
                }
                store::Kind::Set => items.sort(),
                _ => {}
            }
            items.insert(0, kind.name().to_vec());
            items
        }
    };
    Some((items, at, encoding))
}

fn compare(a: &Node, b: &Node, step: usize) {
    let keys: BTreeSet<Vec<u8>> = a.store.borrow().keys().chain(b.store.borrow().keys()).map(<[u8]>::to_vec).collect();
    for key in keys {
        let (mut va, vb) = (view(a, &key), view(b, &key));
        // INCRBYFLOAT goes to the replica as a SET of its result, which
        // a replica of Redis 8.10.2 too calls `int` if it reads as one,
        // where its primary says `embstr`.
        if let (Some(x), Some(y)) = (&mut va, &vb) {
            if (x.2, y.2) == ("embstr", "int") {
                x.2 = "int";
            }
        }
        assert_eq!(va, vb, "step {step}: key {:?}", key.escape_ascii().to_string());
    }
}

/// The stream's commands that would do something else at the replica if
/// they went in as sent (relative times, random picks, floats).
fn check_rewritten(stream: &[u8]) {
    for line in text(stream) {
        let words: Vec<&str> = line.split(' ').collect();
        let name = words[0].to_ascii_uppercase();
        assert!(!["EXPIRE", "PEXPIRE", "EXPIREAT", "SPOP", "INCRBYFLOAT", "HINCRBYFLOAT", "GETEX"].contains(&name.as_str()), "{line}");
        if name == "SET" {
            // NX, XX and GET are fine as sent; an expiry only as PXAT,
            // which then is all there is.
            let options: Vec<String> = words.iter().skip(3).map(|w| w.to_ascii_uppercase()).collect();
            assert!(options.iter().all(|w| !["EX", "PX", "EXAT"].contains(&w.as_str())), "{line}");
            if options.contains(&"PXAT".to_string()) {
                assert_eq!(options.len(), 2, "{line}");
            }
        }
    }
}

#[test]
fn a_replica_applying_the_stream_has_the_same_keys() {
    for seed in 1..=16u64 {
        let a = node(BACKLOG_SIZE);
        let b = node(BACKLOG_SIZE);
        let c = client();
        a.repl.start(&mut a.store.borrow_mut());
        // The replica's clock never lets a key expire: only the DELs do.
        b.store.borrow_mut().set_now(i64::MIN);
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut now = T0;
        let mut applied = 0;
        for step in 0..10_000 {
            now += rng.below(8) as i64;
            a.store.borrow_mut().set_now(now);
            let req = random_request(&mut rng, now);
            let args: Vec<&[u8]> = req.iter().map(|a| &a[..]).collect();
            run(&a, &c, &request(&args));
            if rng.below(50) == 0 {
                crate::expire_sweep(&a);
            }
            let stream = a.repl.since(applied).expect("in the backlog");
            check_rewritten(&stream);
            for reply in run(&b, &c, &stream) {
                assert!(!reply.starts_with(b"-"), "step {step}: the replica refused {:?} ({:?})", text(&stream), reply.escape_ascii().to_string());
            }
            applied = a.repl.offset();
            if step % 50 == 0 {
                compare(&a, &b, step);
            }
        }
        compare(&a, &b, 10_000);
        assert!(b.repl.offset() == 0, "the replica's own stream didn't start");
        let keys = a.store.borrow().dbsize();
        println!("seed {seed}: {} bytes of stream, {keys} keys at the end", a.repl.offset());
    }
}

/// `random_request` with every key of the command under the hash tag of
/// one of 8 groups (`group`, or one at random): a command's keys in one
/// slot, as cluster mode has them, the groups in slots far apart.
fn tagged(rng: &mut Rng, now: i64, group: Option<u64>) -> Vec<Vec<u8>> {
    let mut req = random_request(rng, now);
    let group = group.unwrap_or_else(|| rng.below(8));
    let bytes = request(&req.iter().map(|a| &a[..]).collect::<Vec<_>>());
    let mut parser = Parser::new(Limits::default());
    let Ok(Parsed::Complete { .. }) = parser.parse(&bytes) else { panic!() };
    let args = parser.args(&bytes);
    let positions: Vec<usize> = match crate::command::find(COMMANDS, &req[0]).and_then(|c| c.key_range(&args)) {
        None => Vec::new(),
        Some((first, last, step)) => {
            let last = if last < 0 { args.len() as i32 + last } else { last };
            (first..=last).step_by(step as usize).map(|i| i as usize).collect()
        }
    };
    for i in positions {
        req[i] = [format!("{{g{group}}}").into_bytes(), req[i].clone()].concat();
    }
    req
}

/// What the primary queued for its replica, taken.
fn take(feed: &Feed) -> Vec<u8> {
    std::mem::take(&mut *feed.out.borrow_mut())
}

/// The copy checked step by step: keys of every kind at a primary, a
/// replica attaching, then in a random order writes, batches of the copy
/// (collections in parts of 8 elements), expiry sweeps, and the replica
/// applying what it was sent; once the copy is done, more of the same.
/// Half the writes during the copy go to the slot being sent, where a
/// write may or may not be forwarded.
/// At the end the replica has the primary's keys, value and absolute
/// expiry (not always the encoding: the copy rebuilds values, as Redis's
/// RDB does).
#[test]
fn a_sync_under_writes_copies_every_key() {
    use crate::repl::{primary, replica};
    for seed in 1..=12u64 {
        let config = Config { migrate: crate::Migrate { chunk: 8, ..crate::Migrate::default() }, ..Config::default() };
        let a = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), config);
        let b = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), Config::default());
        b.store.borrow_mut().set_reclaim(false);
        let c = client();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut now = T0;
        a.store.borrow_mut().set_now(now);
        for _ in 0..2000 {
            let req = tagged(&mut rng, now, None);
            run(&a, &c, &request(&req.iter().map(|a| &a[..]).collect::<Vec<_>>()));
        }
        let group_of = |slot: u32| (0..8).find(|g| u32::from(cluster::key_hash_slot(format!("{{g{g}}}").as_bytes())) == slot);
        let feed = a.repl.attach(&mut a.store.borrow_mut(), NodeId::from_bytes(&[9; 20]), b"?", None);
        let first = take(&feed);
        let line_end = first.windows(2).position(|w| w == b"\r\n").unwrap();
        assert!(first.starts_with(b"+FULLRESYNC "));
        let mut pending = first[line_end + 2..].to_vec();
        let mut staging = replica::Staging::new();
        let mut out = Encoder::new(Protocol::Resp2);
        let mut apply = |pending: &mut Vec<u8>| {
            let mut parser = Parser::new(Limits::default());
            let mut at = 0;
            while let Ok(Parsed::Complete { consumed }) = parser.parse(&pending[at..]) {
                replica::apply(&b, &parser.args(&pending[at..at + consumed]), consumed, &mut staging, &mut out).unwrap();
                at += consumed;
            }
            pending.drain(..at);
        };
        let (mut done, mut batches, mut writes) = (None, 0, 0);
        for step in 0.. {
            now += rng.below(4) as i64;
            a.store.borrow_mut().set_now(now);
            match rng.below(10) {
                0..=4 => {
                    let sending = feed.copy.borrow().as_ref().and_then(|c| group_of(c.slot));
                    let group = sending.filter(|_| rng.below(2) == 0);
                    let req = tagged(&mut rng, now, group);
                    run(&a, &c, &request(&req.iter().map(|a| &a[..]).collect::<Vec<_>>()));
                    writes += usize::from(done.is_none());
                }
                5 | 6 if feed.copying() => {
                    primary::fill(&a, &feed);
                    batches += 1;
                }
                7 => crate::expire_sweep(&a),
                _ => {
                    pending.extend(take(&feed));
                    apply(&mut pending);
                }
            }
            if done.is_none() && !feed.copying() {
                done = Some(step);
                // Now, while keys with an expiry are still there (later,
                // the primary's DEL for each would hide a lost expiry).
                pending.extend(take(&feed));
                apply(&mut pending);
                b.store.borrow_mut().set_now(now);
                assert_eq!(views(&a, false), views(&b, false), "seed {seed}: at the copy's end");
            }
            if done.is_some_and(|d| step > d + 2000) {
                break;
            }
            assert!(step < 200_000, "seed {seed}: the copy never ends");
        }
        // Every expired key reclaimed, its DEL sent; the rest applied. A
        // pass that reclaims nothing has examined every key with an expiry
        // (the budget counts keys examined, round and round).
        loop {
            let mut store = a.store.borrow_mut();
            let all = store.dbsize() + 1;
            if store.expire_cycle(all) == 0 {
                break;
            }
        }
        a.repl.feed(&mut a.store.borrow_mut(), None);
        pending.extend(take(&feed));
        apply(&mut pending);
        assert!(pending.is_empty());
        assert!(staging.is_empty(), "seed {seed}: parts of a key the copy gave up on, held");
        assert_eq!(views(&a, false), views(&b, false), "seed {seed}");
        assert_eq!(b.repl.link.offset(), a.repl.offset(), "seed {seed}: the replica's offset is the primary's");
        println!("seed {seed}: {} keys, {batches} batches, {writes} writes during the copy", a.store.borrow().dbsize());
    }
}

#[test]
fn a_ping_every_ten_seconds_with_replicas() {
    let a = node(BACKLOG_SIZE);
    a.repl.ping();
    assert_eq!(a.repl.offset(), 0, "no replica, no ping");
    let feed = a.repl.attach(&mut a.store.borrow_mut(), NodeId::from_bytes(&[9; 20]), b"?", None);
    a.repl.ping();
    let ping = request(&[b"PING"]);
    assert_eq!(a.repl.offset(), ping.len() as u64);
    a.repl.ping();
    assert_eq!(a.repl.offset(), ping.len() as u64, "not again within 10 s");
    assert!(take(&feed).ends_with(&ping), "to the replica too, copying or not");
}

/// A primary with collections sent in parts of 8, and a replica.
fn copy_pair() -> (Node, Node) {
    let config = Config { migrate: crate::Migrate { chunk: 8, ..crate::Migrate::default() }, ..Config::default() };
    let a = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), config);
    let b = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), Config::default());
    b.store.borrow_mut().set_reclaim(false);
    (a, b)
}

/// The replica attached: its feed, and the stream past `+FULLRESYNC`.
fn attach(a: &Node) -> (Rc<Feed>, Vec<u8>) {
    let feed = a.repl.attach(&mut a.store.borrow_mut(), NodeId::from_bytes(&[9; 20]), b"?", None);
    let first = take(&feed);
    assert!(first.starts_with(b"+FULLRESYNC "));
    let pending = first[first.windows(2).position(|w| w == b"\r\n").unwrap() + 2..].to_vec();
    (feed, pending)
}

/// The rest of the copy, then everything the replica was sent, applied.
fn finish(a: &Node, b: &Node, feed: &Feed, mut pending: Vec<u8>, staging: &mut crate::repl::replica::Staging) {
    while feed.copying() {
        crate::repl::primary::fill(a, feed);
    }
    pending.extend(take(feed));
    let mut out = Encoder::new(Protocol::Resp2);
    let mut parser = Parser::new(Limits::default());
    let mut at = 0;
    while let Ok(Parsed::Complete { consumed }) = parser.parse(&pending[at..]) {
        crate::repl::replica::apply(b, &parser.args(&pending[at..at + consumed]), consumed, staging, &mut out).unwrap();
        at += consumed;
    }
    assert_eq!(at, pending.len());
}

fn hset_many(node: &Node, key: &str, fields: usize) {
    let mut hset: Vec<Vec<u8>> = vec![b"HSET".to_vec(), key.into()];
    for i in 0..fields {
        hset.extend([format!("f{i}").into_bytes(), b"v".to_vec()]);
    }
    run(node, &client(), &request(&hset.iter().map(|a| &a[..]).collect::<Vec<_>>()));
}

/// A collection deleted while it goes in parts: the replica drops what it
/// staged of it, rather than holding it as long as it follows.
#[test]
fn a_collection_deleted_in_parts_leaves_nothing_staged() {
    let (a, b) = copy_pair();
    hset_many(&a, "h", 100);
    let (feed, pending) = attach(&a);
    crate::repl::primary::fill(&a, &feed);
    run(&a, &client(), &request(&[b"DEL", b"h"]));
    let mut staging = crate::repl::replica::Staging::new();
    finish(&a, &b, &feed, pending, &mut staging);
    assert_eq!(b.store.borrow().dbsize(), 0);
    assert!(staging.is_empty(), "the parts of h, held");
}

/// A collection written while it goes in parts (a field already sent
/// deleted, one added) goes again from the start; its expiry with it.
#[test]
fn a_collection_written_in_parts_goes_again() {
    let (a, b) = copy_pair();
    hset_many(&a, "h", 100);
    run(&a, &client(), &request(&[b"PEXPIREAT", b"h", (T0 + 3_600_000).to_string().as_bytes()]));
    let (feed, pending) = attach(&a);
    crate::repl::primary::fill(&a, &feed);
    run(&a, &client(), &request(&[b"HDEL", b"h", b"f0"]));
    run(&a, &client(), &request(&[b"HSET", b"h", b"new", b"v"]));
    finish(&a, &b, &feed, pending, &mut crate::repl::replica::Staging::new());
    assert_eq!(views(&a, false), views(&b, false));
    assert_eq!(view(&b, b"h").unwrap().1, Some(T0 + 3_600_000));
}

/// FLUSHALL while a collection goes in parts, the key made again as
/// large: it goes from its first part, not on from where the old one was.
#[test]
fn a_flushall_in_parts_starts_over() {
    let (a, b) = copy_pair();
    hset_many(&a, "h", 100);
    let (feed, pending) = attach(&a);
    crate::repl::primary::fill(&a, &feed);
    run(&a, &client(), &request(&[b"FLUSHALL"]));
    hset_many(&a, "h", 100);
    finish(&a, &b, &feed, pending, &mut crate::repl::replica::Staging::new());
    assert_eq!(views(&a, false), views(&b, false));
}

/// A write to two keys of the slot being sent, one sent and one not: left
/// out, both sent again as they are then; one gone (emptied), its DEL.
#[test]
fn a_write_across_sent_and_unsent_keys_sends_both_again() {
    let (a, b) = copy_pair();
    for i in 0..10 {
        run(&a, &client(), &request(&[b"RPUSH", format!("{{t}}l{i}").as_bytes(), b"x", b"y"]));
    }
    let (feed, pending) = attach(&a);
    crate::repl::primary::fill(&a, &feed);
    let (sent, unsent): (Vec<Vec<u8>>, Vec<Vec<u8>>) = {
        let copy = feed.copy.borrow();
        let c = copy.as_ref().unwrap();
        (0..10).map(|i| format!("{{t}}l{i}").into_bytes()).partition(|k| c.sent.contains(k))
    };
    assert!(sent.len() >= 2 && unsent.len() >= 2, "the slot half sent: {} sent", sent.len());
    // From one not sent to one sent: the replica has only the second.
    run(&a, &client(), &request(&[b"LMOVE", &unsent[0], &sent[0], b"LEFT", b"RIGHT"]));
    // One sent emptied into one not sent: the first is gone.
    for _ in 0..2 {
        run(&a, &client(), &request(&[b"LMOVE", &sent[1], &unsent[1], b"LEFT", b"RIGHT"]));
    }
    assert_eq!(a.store.borrow().elements(&sent[1]), None);
    finish(&a, &b, &feed, pending, &mut crate::repl::replica::Staging::new());
    assert_eq!(views(&a, false), views(&b, false));
}

/// A slot sync checked as the replica's copy is: keys of every kind in 8
/// groups (slots), 4 of them synced to a target, then in a random order
/// writes to every group, batches of the copy, expiry sweeps and the
/// target applying what it was sent. At the end the target has the
/// source's keys of those slots and none other; and a replica of the
/// target, following the target's own stream, has the target's.
#[test]
fn a_slot_sync_under_writes_copies_its_slots_only() {
    use crate::repl::{primary, replica, slots};
    let slot_of = |g: u64| cluster::key_hash_slot(format!("{{g{g}}}").as_bytes());
    for seed in 1..=12u64 {
        let config = Config { migrate: crate::Migrate { chunk: 8, ..crate::Migrate::default() }, ..Config::default() };
        let a = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), config);
        let b = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), Config::default());
        let r = node_with(BACKLOG_SIZE, store::Store::with_slots(cluster::key_hash_slot), Config::default());
        r.store.borrow_mut().set_reclaim(false);
        let c = client();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut now = T0;
        a.store.borrow_mut().set_now(now);
        b.store.borrow_mut().set_now(now);
        for _ in 0..2000 {
            let req = tagged(&mut rng, now, None);
            run(&a, &c, &request(&req.iter().map(|a| &a[..]).collect::<Vec<_>>()));
        }
        let synced: Vec<u16> = {
            let mut s: Vec<u16> = (0..4).map(slot_of).collect();
            s.sort_unstable();
            s
        };
        b.store.borrow_mut().set_held(synced.clone());
        // The target's replica, attached before (an empty copy).
        let to_r = b.repl.attach(&mut b.store.borrow_mut(), NodeId::from_bytes(&[8; 20]), b"?", None);
        while to_r.copying() {
            primary::fill(&b, &to_r);
        }
        let first = take(&to_r);
        let mut r_pending = first[first.windows(2).position(|w| w == b"\r\n").unwrap() + 2..].to_vec();
        let feed = a.repl.attach_slots(&mut a.store.borrow_mut(), NodeId::from_bytes(&[9; 20]), synced.clone());
        let first = take(&feed);
        assert!(first.starts_with(b"+SYNCING 4\r\n"), "{:?}", first.escape_ascii().to_string());
        let mut pending = first[b"+SYNCING 4\r\n".len()..].to_vec();
        let sync = slots::Sync::for_test(NodeId::from_bytes(&[7; 20]), synced.clone());
        let (mut staging, mut r_staging) = (replica::Staging::new(), replica::Staging::new());
        let mut out = Encoder::new(Protocol::Resp2);
        let mut apply = |pending: &mut Vec<u8>, r_pending: &mut Vec<u8>| {
            let mut parser = Parser::new(Limits::default());
            let mut at = 0;
            while let Ok(Parsed::Complete { consumed }) = parser.parse(&pending[at..]) {
                slots::apply(&b, &sync, &parser.args(&pending[at..at + consumed]), &mut staging, &mut out).unwrap();
                at += consumed;
            }
            pending.drain(..at);
            r_pending.extend(take(&to_r));
            let mut at = 0;
            while let Ok(Parsed::Complete { consumed }) = parser.parse(&r_pending[at..]) {
                replica::apply(&r, &parser.args(&r_pending[at..at + consumed]), consumed, &mut r_staging, &mut out).unwrap();
                at += consumed;
            }
            r_pending.drain(..at);
        };
        let (mut done, mut writes) = (None, 0);
        for step in 0.. {
            now += rng.below(4) as i64;
            a.store.borrow_mut().set_now(now);
            b.store.borrow_mut().set_now(now);
            match rng.below(10) {
                0..=4 => {
                    // Half to a synced group, half to any.
                    let group = (rng.below(2) == 0).then(|| rng.below(4));
                    let req = tagged(&mut rng, now, group);
                    run(&a, &c, &request(&req.iter().map(|a| &a[..]).collect::<Vec<_>>()));
                    writes += usize::from(done.is_none());
                }
                5 | 6 if feed.copying() => {
                    primary::fill(&a, &feed);
                }
                7 => {
                    crate::expire_sweep(&a);
                    crate::expire_sweep(&b);
                }
                _ => {
                    pending.extend(take(&feed));
                    apply(&mut pending, &mut r_pending);
                }
            }
            if done.is_none() && !feed.copying() {
                done = Some(step);
            }
            if done.is_some_and(|d| step > d + 2000) {
                break;
            }
            assert!(step < 200_000, "seed {seed}: the copy never ends");
        }
        loop {
            let mut store = a.store.borrow_mut();
            let all = store.dbsize() + 1;
            if store.expire_cycle(all) == 0 {
                break;
            }
        }
        a.repl.feed(&mut a.store.borrow_mut(), None);
        pending.extend(take(&feed));
        apply(&mut pending, &mut r_pending);
        assert!(pending.is_empty() && staging.is_empty(), "seed {seed}");
        assert_eq!(sync.state(), slots::State::Streaming, "seed {seed}");
        let in_synced = |k: &[u8]| synced.binary_search(&cluster::key_hash_slot(k)).is_ok();
        let theirs: Vec<_> = views(&a, false).into_iter().filter(|(k, _)| in_synced(k)).collect();
        let mine = views(&b, false);
        assert!(mine.iter().all(|(k, _)| in_synced(k)), "seed {seed}: a key of another slot at the target");
        assert_eq!(theirs, mine, "seed {seed}: the target has the source's keys of its slots");
        r.store.borrow_mut().set_now(now);
        assert_eq!(views(&r, false), mine, "seed {seed}: the target's replica has the target's keys");
        assert!(!theirs.is_empty(), "seed {seed}: nothing synced");
        println!("seed {seed}: {} of {} keys synced, {writes} writes during the copy", mine.len(), a.store.borrow().dbsize());
    }
}
