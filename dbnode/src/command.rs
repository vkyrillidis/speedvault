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
use crate::multi;
use crate::cluster_mode::{self, ReplicaRead, Route};

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
    /// the end) and step; `(0, 0, 0)` for none, and for a `movable` command.
    pub(crate) keys: (i32, i32, i32),
    /// Redis's `movablekeys`, the one kind there is so far (`SINTERCARD`,
    /// `LMPOP`, `MSETEX`): `args[1]` is how many keys follow it, this many
    /// arguments apart (2 for `MSETEX`'s key and value pairs); 0 if not.
    pub(crate) movable: i32,
    run: Run,
    /// Redis's `denyoom` flag: refused while the node uses more than
    /// `maxmemory` (the commands that may add memory).
    pub(crate) denyoom: bool,
    /// Redis's `write` flag (every `denyoom` command has it): it goes into
    /// the replication stream ([`crate::repl`]).
    pub(crate) write: bool,
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
    Command { name, arity, keys, run: Run::Data(run), denyoom: false, write: false, movable: 0, subcommands: &[] }
}

/// A data command whose keys follow a count at `args[1]`.
const fn movable(name: &'static str, arity: i32, run: fn(&Args, &mut Store, &mut Encoder) -> Flow) -> Command {
    Command { movable: 1, ..data(name, arity, NO_KEYS, run) }
}

/// [`movable`], each key followed by a value.
const fn movable_pairs(name: &'static str, arity: i32, run: fn(&Args, &mut Store, &mut Encoder) -> Flow) -> Command {
    Command { movable: 2, ..data(name, arity, NO_KEYS, run) }
}

/// `cmd`, flagged `denyoom` (and `write`).
const fn denyoom(cmd: Command) -> Command {
    Command { denyoom: true, write: true, ..cmd }
}

/// `cmd`, flagged `write`.
const fn write(cmd: Command) -> Command {
    Command { write: true, ..cmd }
}

pub(crate) const fn conn(
    name: &'static str,
    arity: i32,
    run: fn(&Args, &Node, &Client, &mut Encoder) -> Flow,
) -> Command {
    Command { name, arity, keys: NO_KEYS, run: Run::Conn(run), denyoom: false, write: false, movable: 0, subcommands: &[] }
}

/// A container command. `run` handles it without a subcommand, where its
/// arity allows that (`COMMAND`).
const fn container(
    name: &'static str,
    arity: i32,
    run: fn(&Args, &Node, &Client, &mut Encoder) -> Flow,
    subcommands: &'static [Command],
) -> Command {
    Command { name, arity, keys: NO_KEYS, run: Run::Conn(run), denyoom: false, write: false, movable: 0, subcommands }
}

/// Looked up front to back: the hot commands first.
pub(crate) const COMMANDS: &[Command] = &[
    data("get", 2, KEY, get),
    denyoom(data("set", -3, KEY, set)),
    denyoom(movable_pairs("msetex", -4, msetex)),
    write(data("delex", -2, KEY, delex)),
    data("digest", 2, KEY, digest),
    denyoom(data("increx", -2, KEY, increx)),
    denyoom(data("lmovem", -5, (1, 2, 1), list::lmovem)),
    write(data("del", -2, ALL_KEYS, del)),
    data("ping", -1, NO_KEYS, ping),
    denyoom(data("incr", 2, KEY, |args, store, out| incr(args, store, out, 1))),
    data("mget", -2, ALL_KEYS, mget),
    denyoom(data("mset", -3, (1, -1, 2), mset)),
    data("exists", -2, ALL_KEYS, exists),
    data("echo", 2, NO_KEYS, echo),
    denyoom(data("decr", 2, KEY, |args, store, out| incr(args, store, out, -1))),
    denyoom(data("incrby", 3, KEY, incrby)),
    denyoom(data("decrby", 3, KEY, decrby)),
    denyoom(data("incrbyfloat", 3, KEY, incrbyfloat)),
    denyoom(data("setnx", 3, KEY, setnx)),
    write(data("getdel", 2, KEY, getdel)),
    denyoom(data("append", 3, KEY, append)),
    data("strlen", 2, KEY, strlen),
    write(data("expire", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::Expire))),
    write(data("pexpire", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::PExpire))),
    write(data("expireat", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::ExpireAt))),
    write(data("pexpireat", -3, KEY, |args, store, out| expire(args, store, out, ExpireCmd::PExpireAt))),
    data("ttl", 2, KEY, ttl),
    data("pttl", 2, KEY, pttl),
    data("expiretime", 2, KEY, |args, store, out| expire_time(args, store, out, 1000)),
    data("pexpiretime", 2, KEY, |args, store, out| expire_time(args, store, out, 1)),
    write(data("persist", 2, KEY, persist)),
    data("type", 2, KEY, type_of),
    container("object", -2, admin::unreachable, OBJECT),
    data("dbsize", 1, NO_KEYS, dbsize),
    conn("select", 2, select),
    write(data("flushall", -1, NO_KEYS, flush)),
    write(data("flushdb", -1, NO_KEYS, flush)),
    data("quit", -1, NO_KEYS, quit),
    conn("hello", -1, admin::hello),
    conn("auth", -2, admin::auth),
    conn("multi", 1, multi::multi),
    conn("exec", 1, multi::exec),
    conn("discard", 1, multi::discard),
    Command { keys: ALL_KEYS, ..conn("watch", -2, multi::watch) },
    conn("unwatch", 1, multi::unwatch),
    container("client", -2, admin::unreachable, admin::CLIENT),
    container("command", -1, admin::command, admin::COMMAND),
    container("config", -2, admin::unreachable, admin::CONFIG),
    conn("info", -1, admin::info),
    container("cluster", -2, admin::unreachable, cluster_mode::CLUSTER),
    conn("readonly", 1, |args, node, client, out| cluster_mode::readonly(args, node, client, out, true)),
    conn("readwrite", 1, |args, node, client, out| cluster_mode::readonly(args, node, client, out, false)),
    conn("role", 1, admin::role),
    conn("wait", 3, crate::repl::wait),
    conn("asking", 1, cluster_mode::asking),
    denyoom(data("hset", -4, KEY, |args, store, out| hash::hset(args, store, out, "hset"))),
    data("hget", 3, KEY, hash::hget),
    data("hgetall", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::All)),
    write(data("hdel", -3, KEY, hash::hdel)),
    data("hmget", -3, KEY, hash::hmget),
    denyoom(data("hmset", -4, KEY, |args, store, out| hash::hset(args, store, out, "hmset"))),
    denyoom(data("hincrby", 4, KEY, hash::hincrby)),
    data("hlen", 2, KEY, hash::hlen),
    data("hexists", 3, KEY, hash::hexists),
    denyoom(data("hsetnx", 4, KEY, hash::hsetnx)),
    data("hkeys", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::Keys)),
    data("hvals", 2, KEY, |args, store, out| hash::read(args, store, out, hash::Part::Values)),
    data("hstrlen", 3, KEY, hash::hstrlen),
    denyoom(data("hincrbyfloat", 4, KEY, hash::hincrbyfloat)),
    data("hrandfield", -2, KEY, hash::hrandfield),
    denyoom(data("sadd", -3, KEY, set::sadd)),
    write(data("srem", -3, KEY, set::srem)),
    data("smembers", 2, KEY, set::smembers),
    data("sismember", 3, KEY, set::sismember),
    data("scard", 2, KEY, set::scard),
    write(data("spop", -2, KEY, set::spop)),
    data("srandmember", -2, KEY, set::srandmember),
    data("smismember", -3, KEY, set::smismember),
    write(data("smove", 4, (1, 2, 1), set::smove)),
    data("sunion", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Union)),
    data("sinter", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Inter)),
    data("sdiff", -2, ALL_KEYS, |args, store, out| set::combine(args, store, out, SetOp::Diff)),
    denyoom(data("sunionstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Union))),
    denyoom(data("sinterstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Inter))),
    denyoom(data("sdiffstore", -3, ALL_KEYS, |args, store, out| set::combine_store(args, store, out, SetOp::Diff))),
    movable("sintercard", -3, set::sintercard),
    movable("sunioncard", -3, |args, store, out| set::card(args, store, out, SetOp::Union)),
    movable("sdiffcard", -3, |args, store, out| set::card(args, store, out, SetOp::Diff)),
    denyoom(data("lpush", -3, KEY, |args, store, out| list::push(args, store, out, End::Left, false))),
    denyoom(data("rpush", -3, KEY, |args, store, out| list::push(args, store, out, End::Right, false))),
    write(data("lpop", -2, KEY, |args, store, out| list::pop(args, store, out, End::Left, "lpop"))),
    write(data("rpop", -2, KEY, |args, store, out| list::pop(args, store, out, End::Right, "rpop"))),
    data("lrange", 4, KEY, list::lrange),
    data("llen", 2, KEY, list::llen),
    data("lindex", 3, KEY, list::lindex),
    denyoom(data("lset", 4, KEY, list::lset)),
    denyoom(data("linsert", 5, KEY, list::linsert)),
    write(data("lrem", 4, KEY, list::lrem)),
    write(data("ltrim", 4, KEY, list::ltrim)),
    data("lpos", -3, KEY, list::lpos),
    denyoom(data("lpushx", -3, KEY, |args, store, out| list::push(args, store, out, End::Left, true))),
    denyoom(data("rpushx", -3, KEY, |args, store, out| list::push(args, store, out, End::Right, true))),
    denyoom(data("lmove", 5, (1, 2, 1), list::lmove)),
    denyoom(data("rpoplpush", 3, (1, 2, 1), list::lmove)),
    write(movable("lmpop", -4, list::lmpop)),
    data("scan", -2, NO_KEYS, scan::scan),
    data("hscan", -3, KEY, scan::hscan),
    data("sscan", -3, KEY, scan::sscan),
    data("keys", 2, NO_KEYS, scan::keys),
];

/// Runs the request `args` (not empty) of `client` and appends its reply to
/// `out`; after `MULTI`, queues it instead (see [`multi`]).
pub(crate) fn execute(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    // `ASKING` holds for the next command only, or, after `MULTI`, until
    // the transaction ends, as Redis keeps its flag in a transaction.
    let asking = client.asking.get();
    let flow = check_and_run(args, node, client, out, asking);
    if asking && client.last_cmd.get() != "asking" && client.multi.borrow().is_none() {
        client.asking.set(false);
    }
    multi::touched(node);
    flow
}

/// [`execute`]'s checks, in Redis 8's order (`processCommand`); a command
/// refused flags the transaction, if any. `EXEC` after `MULTI` is checked
/// as the whole transaction.
fn check_and_run(args: &Args, node: &Node, client: &Client, out: &mut Encoder, asking: bool) -> Flow {
    let Some(mut cmd) = find(COMMANDS, &args[0]) else {
        client.last_cmd.set("NULL");
        multi::flag(client);
        unknown_command(args, out);
        return Flow::Continue;
    };
    if !cmd.subcommands.is_empty() && args.len() >= 2 {
        match find(cmd.subcommands, &args[1]) {
            Some(sub) => cmd = sub,
            None => {
                client.last_cmd.set("NULL");
                multi::flag(client);
                unknown_subcommand(cmd.name, &args[1], out);
                return Flow::Continue;
            }
        }
    }
    client.last_cmd.set(cmd.name);
    let argc = args.len() as i64;
    let arity = i64::from(cmd.arity);
    if (arity > 0 && argc != arity) || argc < -arity {
        multi::flag(client);
        arity_error(cmd.name, out);
        return Flow::Continue;
    }
    // Then the password, as in Redis: before the client has logged in,
    // only AUTH, HELLO and QUIT (see `auth`).
    if !client.authenticated.get() && !crate::auth::allowed_before_auth(cmd.name) {
        multi::flag(client);
        out.error(crate::auth::NOAUTH);
        return Flow::Continue;
    }
    let tx = cmd.name == "exec" && client.multi.borrow().is_some();
    let (write, denyoom, keyless_write) = if tx { multi::flags(client) } else { (cmd.write, cmd.denyoom, cmd.write && cmd.key_range(args).is_none()) };
    // In cluster mode, only keys in this node's slots (after the arity
    // check, as in Redis). A refused command doesn't count as processed.
    // One touching keys on their way to another node waits until they've
    // landed (then it's redirected).
    if let Some(cluster) = &node.cluster {
        if tx {
            if !multi::routed(cluster, node, client, asking, write, out) {
                return Flow::Continue;
            }
        } else if let Some(keys) = keys_in(cmd, *args) {
            if !routed(cluster, node, client, keys, asking, write, out) {
                multi::flag(client);
                return Flow::Continue;
            }
        }
        // A write without keys (`FLUSHALL`) waits while slots' writes do
        // (they move by snapshot: see `cluster_mode`).
        if keyless_write {
            let mut waited = 0;
            while cluster.write_paused(None) {
                cluster_mode::wait(&mut waited);
            }
        }
    }
    // Then memory, as in Redis: nothing is evicted (`noeviction`).
    if denyoom && node.out_of_memory() {
        node.commands_refused_oom.set(node.commands_refused_oom.get() + 1);
        return refuse(tx, node, client, OOM, out);
    }
    // Then the lease (phase 8), as Redis's `min-replicas-to-write`: a
    // primary whose replica stopped acknowledging takes no writes.
    if write && node.repl.lease_lapsed() {
        return refuse(tx, node, client, b"NOREPLICAS Not enough good replicas to write.", out);
    }
    if !multi::runs_in_transaction(cmd) && client.multi.borrow().is_some() {
        multi::queue(cmd, args, client, out);
        return Flow::Continue;
    }
    run(cmd, args, node, client, out)
}

/// A command refused for `why` (an error without its `-`); `EXEC` (`tx`)
/// is aborted with it.
fn refuse(tx: bool, node: &Node, client: &Client, why: &[u8], out: &mut Encoder) -> Flow {
    if tx {
        multi::abort(node, client, why, out);
    } else {
        multi::flag(client);
        out.error(why);
    }
    Flow::Continue
}

/// Runs `cmd`, checked, and feeds the replication stream with what it did.
/// Also each command of a transaction, from `EXEC`.
pub(crate) fn run(cmd: &Command, args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    node.commands_processed.set(node.commands_processed.get() + 1);
    let flush = matches!(cmd.name, "flushall" | "flushdb");
    let watched = if flush { multi::present_watched(node, None) } else { Vec::new() };
    let reply_at = out.len();
    let flow = match cmd.run {
        Run::Data(run) => run(args, &mut node.store.borrow_mut(), out),
        Run::Conn(run) => run(args, node, client, out),
    };
    if node.repl.active() {
        let before = node.repl.offset();
        let sent = crate::repl::as_sent(cmd, &out.as_bytes()[reply_at..]).then_some((args, cmd));
        node.repl.feed(&mut node.store.borrow_mut(), sent);
        if node.repl.offset() != before {
            client.woff.set(node.repl.offset());
        }
    }
    if flush {
        // Slots being copied here lost their keys too: copied again.
        crate::repl::slots::flushed(node);
        if out.as_bytes().get(reply_at) != Some(&b'-') {
            multi::deleted(node, watched);
        }
    }
    flow
}

/// The slot check of keys (see [`cluster_mode::Cluster::route`]), waiting
/// while they're on their way: false if refused, the reply written.
pub(crate) fn routed<'a>(
    cluster: &cluster_mode::Cluster,
    node: &Node,
    client: &Client,
    keys: impl Iterator<Item = &'a [u8]> + Clone,
    asking: bool,
    write: bool,
    out: &mut Encoder,
) -> bool {
    let replica_read = match () {
        _ if !client.readonly.get() || write => ReplicaRead::No,
        _ if node.repl.link.loading() => ReplicaRead::Loading,
        _ => ReplicaRead::Serve,
    };
    let mut waited = 0;
    loop {
        match cluster.route(keys.clone(), &node.store, asking, write, replica_read, out) {
            Route::Serve => {
                cluster.served.set(cluster.served.get() + 1);
                return true;
            }
            Route::Refused => return false,
            Route::Wait => cluster_mode::wait(&mut waited),
        }
    }
}

/// `cmd`'s keys in `args`; `None` without any.
fn keys_in<'a>(cmd: &Command, args: Args<'a>) -> Option<impl Iterator<Item = &'a [u8]> + Clone> {
    let (first, last, step) = cmd.key_range(&args)?;
    let last = if last < 0 { args.len() as i32 + last } else { last };
    Some((first..=last).step_by(step as usize).map(move |i| args.get(i as usize).expect("in range")))
}

/// `cmd`'s keys in `args`, none if it has none.
pub(crate) fn keys_of<'a>(cmd: &Command, args: Args<'a>) -> impl Iterator<Item = &'a [u8]> {
    keys_in(cmd, args).into_iter().flatten()
}

/// Runs a command from this node's primary ([`crate::repl::replica`]):
/// a data command, with no slot check and no memory limit, at the time
/// before every expiry; the reply in `out`, which nobody reads.
pub(crate) fn apply(args: &Args, node: &Node, out: &mut Encoder) {
    let Some(cmd) = find(COMMANDS, &args[0]) else {
        unknown_command(args, out);
        return;
    };
    let argc = args.len() as i64;
    let arity = i64::from(cmd.arity);
    if (arity > 0 && argc != arity) || argc < -arity {
        arity_error(cmd.name, out);
        return;
    }
    let Run::Data(run) = cmd.run else {
        out.error(b"ERR not a data command");
        return;
    };
    let mut store = node.store.borrow_mut();
    let now = store.now();
    store.set_now(store::BEFORE_TIME);
    run(args, &mut store, out);
    store.set_now(now);
}

/// The command called `name` (case-insensitive); a subcommand is matched by
/// the part of its full name after the `|`.
pub(crate) fn find<'a>(commands: &'a [Command], name: &[u8]) -> Option<&'a Command> {
    commands.iter().find(|c| c.is_named(name))
}

impl Command {
    /// Where the request's keys are (first, last, step); `None` without
    /// any. A `movable` command's count is read as Redis 7.0 reads it to
    /// find keys: as `string2ll` does (its key spec) or else as `atoi`
    /// does (its `getkeys` procedure); a count below 1 or past the
    /// arguments means no keys, and the command then fails by itself.
    pub(crate) fn key_range(&self, args: &Args) -> Option<(i32, i32, i32)> {
        if self.movable == 0 {
            return (self.keys != NO_KEYS).then_some(self.keys);
        }
        let n = parse_i64(&args[1]).unwrap_or_else(|| i64::from(atoi(&args[1])));
        let step = i64::from(self.movable);
        // The last key must be an argument (Redis's keynum key spec).
        let last = n.checked_sub(1).and_then(|k| k.checked_mul(step)).and_then(|k| k.checked_add(2));
        last.filter(|&last| n >= 1 && last < args.len() as i64).map(|last| (2, last as i32, step as i32))
    }

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

/// C's `atoi`: leading white space, a sign, digits up to anything else;
/// past the range, `strtol`'s saturated `long` cut to an `int`.
fn atoi(s: &[u8]) -> i32 {
    let s = &s[s.iter().take_while(|&&b| b.is_ascii_whitespace() || b == b'').count()..];
    let (neg, s) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    let mut n: i128 = 0;
    for d in s.iter().take_while(|b| b.is_ascii_digit()) {
        n = (n * 10 + i128::from(d - b'0')).min(1 << 64);
    }
    let n = if neg { -n } else { n };
    n.clamp(i64::MIN.into(), i64::MAX.into()) as i64 as i32
}

/// `-ERR unknown subcommand 'FOO'. Try CLIENT HELP.`
fn unknown_subcommand(container: &str, sub: &[u8], out: &mut Encoder) {
    let upper = container.to_ascii_uppercase();
    out.error_parts(&[b"ERR unknown subcommand '", c_str(sub, 128), b"'. Try ", upper.as_bytes(), b" HELP."]);
}

/// `-ERR unknown command 'FOO', with args beginning with: 'a' 'b' `: the
/// name and the arguments are cut to 128 bytes (C strings in Redis, so also
/// at a NUL), and arguments are listed until 128 bytes of them were.
/// Without arguments, the name alone (Redis 7.2 and later).
fn unknown_command(args: &Args, out: &mut Encoder) {
    let mut msg = Vec::with_capacity(64);
    msg.extend_from_slice(b"ERR unknown command '");
    msg.extend_from_slice(c_str(&args[0], 128));
    msg.push(b'\'');
    if args.len() < 2 {
        return out.error(&msg);
    }
    msg.extend_from_slice(b", with args beginning with: ");
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
    let Some((opts, matching)) = set_options_or_reply(args, 3, true, out) else { return Flow::Continue };
    match store.set_if(&args[1], &args[2], opts, matching) {
        Ok(SetReply::Ok) => out.ok(),
        Ok(SetReply::NotSet | SetReply::Old(None)) => out.null(),
        Ok(SetReply::Old(Some(old))) => out.bulk(&old),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// [`set_options`], or the error replied and `None`.
fn set_options_or_reply<'a>(args: &Args<'a>, from: usize, set: bool, out: &mut Encoder) -> Option<(SetOptions, &'a [u8])> {
    match set_options(args, from, set) {
        Ok(parsed) => Some(parsed),
        Err(SetOptionError::Syntax) => {
            syntax_error(out);
            None
        }
        Err(SetOptionError::NotInteger) => {
            store_error(store::Error::NotInteger, out);
            None
        }
    }
}

enum SetOptionError {
    Syntax,
    NotInteger,
}

/// The options of `SET key value` (from `args[from]`), or of `MSETEX`
/// without `set` (which takes no `GET` and no `IF...`), with Redis 8's
/// rules (`parseExtendedStringArgumentsOrReply`): `NX`, `XX`, `IFEQ`,
/// `IFNE`, `IFDEQ` and `IFDNE` exclude each other, `KEEPTTL` and the four
/// expiry options exclude each other, but repeating one is fine (the last
/// value wins). Any other word, or an option without its value, is a
/// syntax error. Only once all options are known is the expiry value read
/// as an integer; whether it's in range is the store's to check. Also the
/// value an `IF...` condition compares with.
fn set_options<'a>(args: &Args<'a>, from: usize, set: bool) -> Result<(SetOptions, &'a [u8]), SetOptionError> {
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
    let mut matching: &[u8] = b"";
    // A condition, unless another one is already there.
    let cond_ok = |opts: &SetOptions, cond: SetCond| opts.cond == SetCond::Always || opts.cond == cond;
    const MATCHING: [(&[u8], SetCond); 4] = [(b"ifeq", SetCond::IfEq), (b"ifne", SetCond::IfNe), (b"ifdeq", SetCond::IfDeq), (b"ifdne", SetCond::IfDne)];
    let mut i = from;
    while i < args.len() {
        let opt = &args[i];
        let next = args.get(i + 1);
        let named = MATCHING.iter().find(|(name, _)| opt.eq_ignore_ascii_case(name)).map(|&(_, c)| c);
        if opt.eq_ignore_ascii_case(b"nx") && cond_ok(&opts, SetCond::Nx) {
            opts.cond = SetCond::Nx;
        } else if opt.eq_ignore_ascii_case(b"xx") && cond_ok(&opts, SetCond::Xx) {
            opts.cond = SetCond::Xx;
        } else if let (true, Some(cond), Some(value)) = (set, named, next) {
            if !cond_ok(&opts, cond) {
                return Err(SetOptionError::Syntax);
            }
            opts.cond = cond;
            matching = value;
            i += 1;
        } else if set && opt.eq_ignore_ascii_case(b"get") {
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
    Ok((opts, matching))
}

/// `MSETEX numkeys key value [key value ...] [NX|XX] [EX|PX|EXAT|PXAT
/// n|KEEPTTL]` (Redis 8.4): 1 if set, 0 if `NX` or `XX` stopped it.
fn msetex(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let n = match parse_i64(&args[1]) {
        Some(n) if (1..=i64::from(i32::MAX)).contains(&n) => n as usize,
        _ => {
            out.error(b"ERR invalid numkeys value");
            return Flow::Continue;
        }
    };
    if 2 * n + 2 > args.len() {
        out.error(b"ERR wrong number of key-value pairs");
        return Flow::Continue;
    }
    let Some((opts, _)) = set_options_or_reply(args, 2 * n + 2, false, out) else { return Flow::Continue };
    let pairs: Vec<(&[u8], &[u8])> = (0..n).map(|k| (&args[2 + 2 * k], &args[3 + 2 * k])).collect();
    integer_reply(store.msetex(&pairs, opts.cond, opts.expiry).map(i64::from), out)
}

/// `DELEX key [IFEQ|IFNE|IFDEQ|IFDNE value]` (Redis 8.4): 1 if the key
/// went, 0 if not.
fn delex(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let cond = match args.len() {
        2 => SetCond::Always,
        4 => match [(&b"ifeq"[..], SetCond::IfEq), (b"ifne", SetCond::IfNe), (b"ifdeq", SetCond::IfDeq), (b"ifdne", SetCond::IfDne)]
            .into_iter()
            .find(|(name, _)| c_str(&args[2], usize::MAX).eq_ignore_ascii_case(name))
        {
            Some((_, cond)) => cond,
            // Redis looks at the key first: the condition only matters if
            // there's a string to compare.
            None => match store.get(&args[1]) {
                Ok(None) => return integer_reply(Ok(0), out),
                Err(_) => return integer_reply(Err(store::Error::DelexNotString), out),
                Ok(Some(_)) => {
                    out.error(b"ERR Invalid condition. Use IFEQ, IFNE, IFDEQ, or IFDNE");
                    return Flow::Continue;
                }
            },
        },
        _ => {
            arity_error("delex", out);
            return Flow::Continue;
        }
    };
    let matching = args.get(3).unwrap_or(b"");
    integer_reply(store.delex(&args[1], cond, matching).map(i64::from), out)
}

/// `INCREX key [options]` (Redis 8.8): `[value, increment]`, integers or
/// doubles.
fn increx(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let opts: Vec<&[u8]> = args.iter().skip(2).collect();
    match store.increx(&args[1], &opts) {
        Ok(store::IncrexReply::Ints(value, delta)) => {
            out.array(2);
            out.integer(value);
            out.integer(delta);
        }
        Ok(store::IncrexReply::Floats(value, delta)) => {
            out.array(2);
            out.double_text(&value);
            out.double_text(&delta);
        }
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `DIGEST key` (Redis 8.4): the string's XXH3 hash in hex, nil without
/// the key.
fn digest(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.digest(&args[1]) {
        Ok(Some(hex)) => out.bulk(&hex),
        Ok(None) => out.null(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
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

fn incrbyfloat(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.incr_by_float(&args[1], &args[2]) {
        Ok(value) => out.bulk(&value),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
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

/// `EXPIRETIME` (`unit` 1000) and `PEXPIRETIME` (1): Redis 7.0's, the
/// seconds rounded to the nearest, as `TTL`'s.
fn expire_time(args: &Args, store: &mut Store, out: &mut Encoder, unit: i64) -> Flow {
    match store.pexpire_time(&args[1]) {
        at if at < 0 => out.integer(at),
        at => out.integer((at + unit / 2) / unit),
    }
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

/// `DBSIZE`, without the slots copied here and not served yet (as Redis 8,
/// whose `INFO keyspace` counts them).
fn dbsize(_: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    out.integer(store.dbsize_served() as i64);
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

    /// `cmd`'s keys in the request `words`.
    fn key_range(cmd: &str, words: &[&str]) -> Option<(i32, i32, i32)> {
        let mut parser = resp::Parser::new(resp::Limits::default());
        let mut buf = format!("*{}\r\n", words.len()).into_bytes();
        for w in words {
            buf.extend_from_slice(format!("${}\r\n{w}\r\n", w.len()).as_bytes());
        }
        assert!(matches!(parser.parse(&buf), Ok(resp::Parsed::Complete { .. })));
        find(COMMANDS, cmd.as_bytes()).unwrap().key_range(&parser.args(&buf))
    }

    #[test]
    fn keys_after_a_count() {
        let range = |words: &[&str]| key_range("sintercard", words);
        assert_eq!(range(&["sintercard", "2", "a", "b"]), Some((2, 3, 1)));
        assert_eq!(range(&["sintercard", "1", "a", "LIMIT", "1"]), Some((2, 2, 1)));
        assert_eq!(range(&["sintercard", "3", "a", "b"]), None, "more than there are");
        assert_eq!(range(&["sintercard", "0", "a"]), None);
        assert_eq!(range(&["sintercard", "-1", "a"]), None);
        assert_eq!(range(&["sintercard", "x", "a"]), None);
        // Not an integer to string2ll: atoi's reading.
        assert_eq!(range(&["sintercard", "2abc", "a", "b"]), Some((2, 3, 1)));
        assert_eq!(range(&["sintercard", " +1", "a", "b"]), Some((2, 2, 1)));
        assert_eq!(key_range("lmpop", &["lmpop", "2", "a", "b", "LEFT"]), Some((2, 3, 1)));
        // A count that reaches the end takes the direction as a key, as
        // both of Redis's readings do: the slot check sees "LEFT" (a
        // CROSSSLOT in cluster mode), else LMPOP fails by itself.
        assert_eq!(key_range("lmpop", &["lmpop", "3", "a", "b", "LEFT"]), Some((2, 4, 1)));
        assert_eq!(find(COMMANDS, b"lmpop").unwrap().keys, NO_KEYS, "COMMAND shows 0 0 0, as Redis");
        assert_eq!(key_range("get", &["get", "k"]), Some(KEY));
        assert_eq!(key_range("ping", &["ping"]), None);
    }

    #[test]
    fn atoi_as_c() {
        assert_eq!(atoi(b"12"), 12);
        assert_eq!(atoi(b" \t-7x"), -7);
        assert_eq!(atoi(b"+3"), 3);
        assert_eq!(atoi(b"x"), 0);
        assert_eq!(atoi(b""), 0);
        assert_eq!(atoi(b"- 1"), 0);
        // strtol saturates, then the long is cut to an int.
        assert_eq!(atoi(b"99999999999999999999"), -1);
        assert_eq!(atoi(b"-99999999999999999999"), 0);
        assert_eq!(atoi(b"4294967298"), 2);
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
