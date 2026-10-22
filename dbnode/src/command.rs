//! The command table: looks a request's command up, checks its arity and
//! runs it, writing the reply. The data commands are here; the connection
//! and server commands (`HELLO`, `CLIENT`, `COMMAND`, `INFO`, `CONFIG`) are
//! in [`crate::admin`], `CLUSTER` and the slot check in [`crate::cluster_mode`].
//!
//! Replies and error texts are Redis 7.0's (`server.c`: `processCommand`;
//! `t_string.c`: `parseExtendedStringArgumentsOrReply`), so a client can't
//! tell the difference.

use crate::admin;
use crate::client::Client;
use crate::cluster_mode::{self, Route};
use crate::migrate;

mod hash;
mod list;
mod scan;
mod set;
use crate::Node;
use resp::{Args, Encoder};
use store::{parse_i64, End, SetOp, ExpireCmd, ExpireFlags, SetCond, SetExpiry, SetOptions, SetReply, Store};

/// What the connection does after a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    /// Write the replies so far, then close (`QUIT`).
    Close,
}

pub(crate) struct Command {
    /// Lower case: matched case-insensitively, and printed like this in
    /// arity errors, as Redis does. A subcommand's is its full name,
    /// `client|setname`.
    pub(crate) name: &'static str,
    /// Redis's convention: `n > 0` exactly n arguments (the name included),
    /// `-n` at least n.
    pub(crate) arity: i32,
    /// Where the keys are, for `COMMAND`: first, last (negative: counted from
    /// the end) and step; `(0, 0, 0)` for none.
    pub(crate) keys: (i32, i32, i32),
    run: Run,
    /// Redis's `denyoom` flag: refused while the node uses more than
    /// `maxmemory` (the commands that may add memory).
    pub(crate) denyoom: bool,
    /// A container command's subcommands (`CLIENT GETNAME`): the second
    /// argument picks one.
    pub(crate) subcommands: &'static [Command],
}

#[derive(Clone, Copy)]
enum Run {
    /// Needs only the keyspace.
    Data(fn(&Args, &mut Store, &mut Encoder) -> Flow),
    /// Needs the node or the connection.
    Conn(fn(&Args, &Node, &Client, &mut Encoder) -> Flow),
}

const NO_KEYS: (i32, i32, i32) = (0, 0, 0);
const KEY: (i32, i32, i32) = (1, 1, 1);
const ALL_KEYS: (i32, i32, i32) = (1, -1, 1);

const fn data(name: &'static str, arity: i32, keys: (i32, i32, i32), run: fn(&Args, &mut Store, &mut Encoder) -> Flow) -> Command {
    Command { name, arity, keys, run: Run::Data(run), denyoom: false, subcommands: &[] }
}

/// `cmd`, flagged `denyoom`.
const fn denyoom(cmd: Command) -> Command {
    Command { denyoom: true, ..cmd }
}

pub(crate) const fn conn(
    name: &'static str,
    arity: i32,
    run: fn(&Args, &Node, &Client, &mut Encoder) -> Flow,
) -> Command {
    Command { name, arity, keys: NO_KEYS, run: Run::Conn(run), denyoom: false, subcommands: &[] }
}

/// A container command. `run` handles it without a subcommand, where its
/// arity allows that (`COMMAND`).
const fn container(
    name: &'static str,
    arity: i32,
    run: fn(&Args, &Node, &Client, &mut Encoder) -> Flow,
    subcommands: &'static [Command],
) -> Command {
    Command { name, arity, keys: NO_KEYS, run: Run::Conn(run), denyoom: false, subcommands }
}

/// Looked up front to back: the hot commands first.
pub(crate) const COMMANDS: &[Command] = &[
    data("get", 2, KEY, get),
    denyoom(data("set", -3, KEY, set)),
    data("del", -2, ALL_KEYS, del),
    data("ping", -1, NO_KEYS, ping),
    denyoom(data("incr", 2, KEY, |args, store, out| incr(args, store, out, 1))),
    data("mget", -2, ALL_KEYS, mget),
    denyoom(data("mset", -3, (1, -1, 2), mset)),
    data("exists", -2, ALL_KEYS, exists),
    data("echo", 2, NO_KEYS, echo),
    denyoom(data("decr", 2, KEY, |args, store, out| incr(args, store, out, -1))),
    denyoom(data("incrby", 3, KEY, incrby)),
    denyoom(data("decrby", 3, KEY, decrby)),
    denyoom(data("setnx", 3, KEY, setnx)),
    data("getdel", 2, KEY, getdel),
    denyoom(data("append", 3, KEY, append)),
    data("strlen", 2, KEY, strlen),
    data("expire", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::Expire)),
    data("pexpire", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::PExpire)),
    data("expireat", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::ExpireAt)),
    data("pexpireat", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::PExpireAt)),
    data("ttl", 2, KEY, ttl),
    data("pttl", 2, KEY, pttl),
    data("persist", 2, KEY, persist),
    data("type", 2, KEY, type_of),
    container("object", -2, admin::unreachable, OBJECT),
    data("dbsize", 1, NO_KEYS, dbsize),
    conn("select", 2, select),
    data("flushall", -1, NO_KEYS, flush),
    data("flushdb", -1, NO_KEYS, flush),
    data("quit", -1, NO_KEYS, quit),
    conn("hello", -1, admin::hello),
    container("client", -2, admin::unreachable, admin::CLIENT),
    container("command", -1, admin::command, admin::COMMAND),
    container("config", -2, admin::unreachable, admin::CONFIG),
    conn("info", -1, admin::info),
    container("cluster", -2, admin::unreachable, cluster_mode::CLUSTER),
    conn("readonly", 1, cluster_mode::accept),
    conn("readwrite", 1, cluster_mode::accept),
    conn("asking", 1, cluster_mode::asking),
    denyoom(data("hset", -4, KEY, |args, store, out| hash::hset(args, store, out, "hset"))),
    data("hget", 3, KEY, hash::hget),
    data("hgetall", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::All)),
    data("hdel", -3, KEY, hash::hdel),
    data("hmget", -3, KEY, hash::hmget),
    denyoom(data("hmset", -4, KEY, |args, store, out| hash::hset(args, store, out, "hmset"))),
    denyoom(data("hincrby", 4, KEY, hash::hincrby)),
    data("hlen", 2, KEY, hash::hlen),
    data("hexists", 3, KEY, hash::hexists),
    denyoom(data("hsetnx", 4, KEY, hash::hsetnx)),
    data("hkeys", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::Keys)),
    data("hvals", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::Values)),
    data("hstrlen", 3, KEY, hash::hstrlen),
    denyoom(data("sadd", -3, KEY, set::sadd)),
    data("srem", -3, KEY, set::srem),
    data("smembers", 2, KEY, set::smembers),
    data("sismember", 3, KEY, set::sismember),
    data("scard", 2, KEY, set::scard),
    data("spop", -2, KEY, set::spop),
    data("srandmember", -2, KEY, set::srandmember),
    data("smismember", -3, KEY, set::smismember),
    data("smove", 4, (1, 2, 1), set::smove),
    data("sunion", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Union)),
    data("sinter", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Inter)),
    data("sdiff", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Diff)),
    denyoom(data("sunionstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Union))),
    denyoom(data("sinterstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Inter))),
    denyoom(data("sdiffstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Diff))),
    denyoom(data("lpush", -3, KEY, |args, store, out| list::push(args, store, out, End::Left, false))),
    denyoom(data("rpush", -3, KEY, |args, store, out| list::push(args, store, out, End::Right, false))),
    data("lpop", -2, KEY, |args, store, out| list::pop(args, store, out, End::Left, "lpop")),
    data("rpop", -2, KEY, |args, store, out| list::pop(args, store, out, End::Right, "rpop")),
    data("lrange", 4, KEY, list::lrange),
    data("llen", 2, KEY, list::llen),
    data("lindex", 3, KEY, list::lindex),
    denyoom(data("lset", 4, KEY, list::lset)),
    denyoom(data("linsert", 5, KEY, list::linsert)),
    data("lrem", 4, KEY, list::lrem),
    data("ltrim", 4, KEY, list::ltrim),
    data("lpos", -3, KEY, list::lpos),
    denyoom(data("lpushx", -3, KEY, |args, store, out| list::push(args, store, out, End::Left, true))),
    denyoom(data("rpushx", -3, KEY, |args, store, out| list::push(args, store, out, End::Right, true))),
    denyoom(data("lmove", 5, (1, 2, 1), list::lmove)),
    denyoom(data("rpoplpush", 3, (1, 2, 1), list::lmove)),
    data("scan", -2, NO_KEYS, scan::scan),
    data("hscan", -3, KEY, scan::hscan),
    data("sscan", -3, KEY, scan::sscan),
    data("keys", 2, NO_KEYS, scan::keys),
];

/// Runs the request `args` (not empty) of `client` and appends its reply to
/// `out`.
pub(crate) fn execute(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    // `ASKING` holds for the next command only, whatever it is.
    let asking = client.asking.take();
    let Some(mut cmd) = find(COMMANDS, &args[0]) else {
        client.last_cmd.set("NULL");
        unknown_command(args, out);
        return Flow::Continue;
    };
    if !cmd.subcommands.is_empty() && args.len() >= 2 {
        match find(cmd.subcommands, &args[1]) {
            Some(sub) => cmd = sub,
            None => {
                client.last_cmd.set("NULL");
                unknown_subcommand(cmd.name, &args[1], out);
                return Flow::Continue;
            }
        }
    }
    client.last_cmd.set(cmd.name);
    let argc = args.len() as i64;
    let arity = i64::from(cmd.arity);
    if (arity > 0 && argc != arity) || argc < -arity {
        arity_error(cmd.name, out);
        return Flow::Continue;
    }
    // In cluster mode, only keys in this node's slots (after the arity
    // check, as in Redis). A refused command doesn't count as processed.
    // One touching keys on their way to another node waits until they've
    // landed (then it's redirected).
    if let Some(cluster) = node.cluster.as_ref().filter(|_| cmd.keys != NO_KEYS) {
        let mut waited = 0;
        loop {
            match cluster.route(args, cmd.keys, &node.store, asking, out) {
                Route::Serve => {
                    cluster.served.set(cluster.served.get() + 1);
                    break;
                }
                Route::Refused => return Flow::Continue,
                Route::Wait => migrate::wait(&mut waited),
            }
        }
    }
    // Then memory, as in Redis: nothing is evicted (`noeviction`).
    if cmd.denyoom && node.out_of_memory() {
        node.commands_refused_oom.set(node.commands_refused_oom.get() + 1);
        out.error(OOM);
        return Flow::Continue;
    }
    node.commands_processed.set(node.commands_processed.get() + 1);
    match cmd.run {
        Run::Data(run) => run(args, &mut node.store.borrow_mut(), out),
        Run::Conn(run) => run(args, node, client, out),
    }
}

/// The command called `name` (case-insensitive); a subcommand is matched by
/// the part of its full name after the `|`.
pub(crate) fn find<'a>(commands: &'a [Command], name: &[u8]) -> Option<&'a Command> {
    commands.iter().find(|c| c.is_named(name))
}

impl Command {
    /// Whether `name` is this command's short name (after the `|` of a
    /// subcommand), case-insensitively. Most entries are told apart by
    /// length alone: every request walks the table this way.
    fn is_named(&self, name: &[u8]) -> bool {
        let full = self.name.as_bytes();
        let Some(start) = full.len().checked_sub(name.len()) else { return false };
        let boundary = if start == 0 { !full.contains(&b'|') } else { full[start - 1] == b'|' };
        boundary && full[start..].eq_ignore_ascii_case(name)
    }
}

/// `-ERR unknown subcommand 'FOO'. Try CLIENT HELP.`
fn unknown_subcommand(container: &str, sub: &[u8], out: &mut Encoder) {
    let upper = container.to_ascii_uppercase();
    out.error_parts(&[b"ERR unknown subcommand '", c_str(sub, 128), b"'. Try ", upper.as_bytes(), b" HELP."]);
}

/// `-ERR unknown command 'FOO', with args beginning with: 'a' 'b' `: the
/// name and the arguments are cut to 128 bytes (C strings in Redis, so also
/// at a NUL), and arguments are listed until 128 bytes of them were.
fn unknown_command(args: &Args, out: &mut Encoder) {
    let mut msg = Vec::with_capacity(64);
    msg.extend_from_slice(b"ERR unknown command '");
    msg.extend_from_slice(c_str(&args[0], 128));
    msg.extend_from_slice(b"', with args beginning with: ");
    let listed_from = msg.len();
    for arg in args.iter().skip(1) {
        let listed = msg.len() - listed_from;
        if listed >= 128 {
            break;
        }
        msg.push(b'\'');
        msg.extend_from_slice(c_str(arg, 128 - listed));
        msg.extend_from_slice(b"' ");
    }
    out.error(&msg);
}

/// `printf("%.*s", max, s)`: up to the first NUL, at most `max` bytes.
pub(crate) fn c_str(s: &[u8], max: usize) -> &[u8] {
    let s = s.iter().position(|&b| b == 0).map_or(s, |nul| &s[..nul]);
    &s[..s.len().min(max)]
}

/// Redis's reply to a `denyoom` command past `maxmemory`.
pub(crate) const OOM: &[u8] = b"OOM command not allowed when used memory > 'maxmemory'.";

pub(crate) fn arity_error(name: &str, out: &mut Encoder) {
    out.error_parts(&[b"ERR wrong number of arguments for '", name.as_bytes(), b"' command"]);
}

pub(crate) fn syntax_error(out: &mut Encoder) {
    out.error(b"ERR syntax error");
}

pub(crate) fn store_error(e: store::Error, out: &mut Encoder) {
    out.error(e.to_string().as_bytes());
}

fn integer_reply(result: Result<i64, store::Error>, out: &mut Encoder) -> Flow {
    match result {
        Ok(n) => out.integer(n),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// An integer argument, or Redis's error for one that isn't.
fn integer_arg(arg: &[u8]) -> Result<i64, store::Error> {
    parse_i64(arg).ok_or(store::Error::NotInteger)
}

fn get(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.get(&args[1]) {
        Ok(Some(value)) => out.bulk(value),
        Ok(None) => out.null(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

fn set(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let opts = match set_options(args) {
        Ok(opts) => opts,
        Err(SetOptionError::Syntax) => {
            syntax_error(out);
            return Flow::Continue;
        }
        Err(SetOptionError::NotInteger) => {
            store_error(store::Error::NotInteger, out);
            return Flow::Continue;
        }
    };
    match store.set(&args[1], &args[2], opts) {
        Ok(SetReply::Ok) => out.ok(),
        Ok(SetReply::NotSet | SetReply::Old(None)) => out.null(),
        Ok(SetReply::Old(Some(old))) => out.bulk(&old),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

enum SetOptionError {
    Syntax,
    NotInteger,
}

/// The options after `SET key value`, with Redis 7.0's rules: `NX` and `XX`
/// exclude each other, `KEEPTTL` and the four expiry options exclude each
/// other, but repeating one is fine (the last value wins). Any other word,
/// or an expiry option without a value, is a syntax error. Only once all
/// options are known is the expiry value read as an integer; whether it's in
/// range is the store's to check.
fn set_options(args: &Args) -> Result<SetOptions, SetOptionError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Unit {
        Ex,
        Px,
        ExAt,
        PxAt,
    }
    impl Unit {
        fn parse(opt: &[u8]) -> Option<Unit> {
            [(b"ex" as &[u8], Unit::Ex), (b"px", Unit::Px), (b"exat", Unit::ExAt), (b"pxat", Unit::PxAt)]
                .into_iter()
                .find(|(name, _)| opt.eq_ignore_ascii_case(name))
                .map(|(_, unit)| unit)
        }
    }
    let mut opts = SetOptions::default();
    let mut keep_ttl = false;
    let mut expiry: Option<(Unit, &[u8])> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = &args[i];
        let next = args.get(i + 1);
        if opt.eq_ignore_ascii_case(b"nx") && opts.cond != SetCond::Xx {
            opts.cond = SetCond::Nx;
        } else if opt.eq_ignore_ascii_case(b"xx") && opts.cond != SetCond::Nx {
            opts.cond = SetCond::Xx;
        } else if opt.eq_ignore_ascii_case(b"get") {
            opts.get = true;
        } else if opt.eq_ignore_ascii_case(b"keepttl") && expiry.is_none() {
            keep_ttl = true;
        } else if let (Some(unit), Some(value)) = (Unit::parse(opt), next) {
            if keep_ttl || expiry.is_some_and(|(u, _)| u != unit) {
                return Err(SetOptionError::Syntax);
            }
            expiry = Some((unit, value));
            i += 1;
        } else {
            return Err(SetOptionError::Syntax);
        }
        i += 1;
    }
    opts.expiry = match expiry {
        None if keep_ttl => SetExpiry::KeepTtl,
        None => SetExpiry::Clear,
        Some((unit, value)) => {
            let n = parse_i64(value).ok_or(SetOptionError::NotInteger)?;
            match unit {
                Unit::Ex => SetExpiry::Ex(n),
                Unit::Px => SetExpiry::Px(n),
                Unit::ExAt => SetExpiry::ExAt(n),
                Unit::PxAt => SetExpiry::PxAt(n),
            }
        }
    };
    Ok(opts)
}

fn del(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let deleted = args.iter().skip(1).filter(|key| store.del(key)).count();
    out.integer(deleted as i64);
    Flow::Continue
}

/// `MGET`: a key holding another type reads as nil, as in Redis.
fn mget(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.array(args.len() - 1);
    for key in args.iter().skip(1) {
        match store.get(key) {
            Ok(Some(value)) => out.bulk(value),
            Ok(None) | Err(_) => out.null(),
        }
    }
    Flow::Continue
}

fn mset(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if args.len().is_multiple_of(2) {
        arity_error("mset", out);
        return Flow::Continue;
    }
    store.mset((1..args.len()).step_by(2).map(|i| (&args[i], &args[i + 1])));
    out.ok();
    Flow::Continue
}

/// Counts repeated keys as often as they're given, as Redis does.
fn exists(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let found = args.iter().skip(1).filter(|key| store.exists(key)).count();
    out.integer(found as i64);
    Flow::Continue
}

fn incr(args: &Args, store: &mut Store, out: &mut Encoder, by: i64) -> Flow {
    integer_reply(store.incr_by(&args[1], by), out)
}

/// The increment is parsed before the key is looked at, as in Redis.
fn incrby(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(integer_arg(&args[2]).and_then(|by| store.incr_by(&args[1], by)), out)
}

fn decrby(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(integer_arg(&args[2]).and_then(|by| store.decr_by(&args[1], by)), out)
}

fn setnx(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(i64::from(store.setnx(&args[1], &args[2])));
    Flow::Continue
}

fn getdel(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.getdel(&args[1]) {
        Ok(Some(value)) => out.bulk(&value),
        Ok(None) => out.null(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

fn append(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.append(&args[1], &args[2]).map(|len| len as i64), out)
}

fn strlen(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.strlen(&args[1]).map(|len| len as i64), out)
}

/// `EXPIRE key time [NX|XX|GT|LT ...]` and its three siblings. In Redis's
/// order: an unknown option, then conflicting options, then a time that
/// isn't an integer; whether the time is in range is the store's to check.
fn expire(args: &Args, store: &mut Store, out: &mut Encoder, cmd: ExpireCmd) -> Flow {
    let mut flags = ExpireFlags::default();
    for opt in args.iter().skip(3) {
        if opt.eq_ignore_ascii_case(b"nx") {
            flags.nx = true;
        } else if opt.eq_ignore_ascii_case(b"xx") {
            flags.xx = true;
        } else if opt.eq_ignore_ascii_case(b"gt") {
            flags.gt = true;
        } else if opt.eq_ignore_ascii_case(b"lt") {
            flags.lt = true;
        } else {
            out.error_parts(&[b"ERR Unsupported option ", c_str(opt, usize::MAX)]);
            return Flow::Continue;
        }
    }
    let result = flags
        .check()
        .and_then(|()| integer_arg(&args[2]))
        .and_then(|when| store.expire(cmd, &args[1], when, flags));
    integer_reply(result.map(i64::from), out)
}

fn ttl(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(store.ttl(&args[1]));
    Flow::Continue
}

fn pttl(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(store.pttl(&args[1]));
    Flow::Continue
}

fn persist(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(i64::from(store.persist(&args[1])));
    Flow::Continue
}

fn type_of(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.simple(store.type_of(&args[1]).as_bytes());
    Flow::Continue
}

/// `OBJECT`'s subcommands. Only `ENCODING` so far: the others report
/// Redis's object sharing and LRU/LFU clocks, which the store doesn't have.
const OBJECT: &[Command] = &[data("object|encoding", 3, (2, 2, 1), object_encoding)];

/// `OBJECT ENCODING key`: nil for a missing key.
fn object_encoding(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.encoding(&args[2]) {
        Some(encoding) => out.bulk(encoding.as_bytes()),
        None => out.null(),
    }
    Flow::Continue
}

fn dbsize(_: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(store.dbsize() as i64);
    Flow::Continue
}

/// Only database 0 exists: any other index is out of range, as it is in a
/// Redis configured with `databases 1`, and not allowed in cluster mode.
/// The index is checked like Redis checks an `int` argument first.
fn select(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    match parse_i64(&args[1]) {
        None => store_error(store::Error::NotInteger, out),
        Some(n) if i32::try_from(n).is_err() => {
            out.error(b"ERR value is out of range, value must between -2147483648 and 2147483647")
        }
        Some(0) => out.ok(),
        Some(_) if node.cluster.is_some() => out.error(b"ERR SELECT is not allowed in cluster mode"),
        Some(_) => out.error(b"ERR DB index is out of range"),
    }
    Flow::Continue
}

fn ping(args: &Args, _: &mut Store, out: &mut Encoder) -> Flow {
    match args.len() {
        1 => out.simple(b"PONG"),
        2 => out.bulk(&args[1]),
        _ => arity_error("ping", out),
    }
    Flow::Continue
}

fn echo(args: &Args, _: &mut Store, out: &mut Encoder) -> Flow {
    out.bulk(&args[1]);
    Flow::Continue
}

/// `FLUSHALL` / `FLUSHDB [SYNC|ASYNC]`. Both flush synchronously: freeing
/// the keys is quick enough so far.
fn flush(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let mode_ok = match args.len() {
        1 => true,
        2 => args[1].eq_ignore_ascii_case(b"sync") || args[1].eq_ignore_ascii_case(b"async"),
        _ => false,
    };
    if mode_ok {
        store.flush();
        out.ok();
    } else {
        syntax_error(out);
    }
    Flow::Continue
}

/// Redis 7.0 accepts `QUIT` with any arguments and stops reading.
fn quit(_: &Args, _: &mut Store, out: &mut Encoder) -> Flow {
    out.ok();
    Flow::Close
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c_str_stops_at_nul_and_max() {
        assert_eq!(c_str(b"abc", 128), b"abc");
        assert_eq!(c_str(b"abcdef", 3), b"abc");
        assert_eq!(c_str(b"ab\0cd", 128), b"ab");
        assert_eq!(c_str(b"abc", 0), b"");
    }

    #[test]
    fn command_names_are_lower_case_and_unique() {
        fn check(commands: &[Command], prefix: &str) {
            for (i, c) in commands.iter().enumerate() {
                assert_eq!(c.name, c.name.to_ascii_lowercase());
                assert!(c.name.starts_with(prefix), "{} lacks the prefix {prefix:?}", c.name);
                assert!(commands[..i].iter().all(|d| d.name != c.name), "{} twice", c.name);
                if !c.subcommands.is_empty() {
                    check(c.subcommands, &format!("{}|", c.name));
                }
            }
        }
        check(COMMANDS, "");
    }

    #[test]
    fn subcommands_are_found_by_their_short_name() {
        let client = find(COMMANDS, b"CLIENT").unwrap();
        assert_eq!(find(client.subcommands, b"SetName").unwrap().name, "client|setname");
        assert!(find(client.subcommands, b"client|setname").is_none());
        assert!(find(client.subcommands, b"etname").is_none());
        assert!(find(COMMANDS, b"et").is_none());
        assert_eq!(find(COMMANDS, b"HSET").unwrap().name, "hset");
        assert!(find(COMMANDS, b"setname").is_none());
    }
}
