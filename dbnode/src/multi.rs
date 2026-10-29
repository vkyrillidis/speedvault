//! Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, as Redis
//! 8.10.2's (`multi.c`, and `processCommand` around it).
//!
//! After `MULTI`, a command that passes every check of
//! [`command::execute`](crate::command::execute) (it exists, its arity, the
//! password, the slot, memory, the lease) is queued and gets `+QUEUED`; one
//! that fails any of them gets its error and flags the transaction, which
//! `EXEC` then refuses (`-EXECABORT`). `EXEC` is checked as one command
//! made of the whole queue, as Redis's `getNodeByQuery` takes it: all its
//! keys in one slot this node serves, then memory and the lease (refused
//! with `-EXECABORT ... because of: ...`). Then the commands run back to
//! back, their replies in one array. Nothing suspends the fiber in between
//! (`WAIT` answers at once), so no other client runs inside a transaction,
//! and the replication stream has it between `MULTI` and `EXEC`, which a
//! replica applies at once (see `repl`).
//!
//! `WATCH` notes keys. A write that changes one ([`store::Touch`], where
//! Redis calls `signalModifiedKey`), the key expiring (or found expired at
//! `EXEC`), a `FLUSHALL` or the slot leaving while it exists, makes the
//! client's `EXEC` reply nil, and unwatches. A key that had already expired
//! when watched counts only once written again, as in Redis. The store
//! records touches only while a key is watched ([`touched`]).

use crate::client::{Client, Clients};
use crate::cluster_mode::Cluster;
use crate::command::{self, Command, Flow};
use crate::Node;
use resp::{Args, Encoder};
use std::ops::Range;

/// A command queued after `MULTI`, its arguments copied.
pub(crate) struct Queued {
    cmd: &'static Command,
    data: Vec<u8>,
    ranges: Vec<Range<usize>>,
}

impl Queued {
    fn args(&self) -> Args<'_> {
        Args::new(&self.data, &self.ranges)
    }
}

/// A client's transaction, from `MULTI` until `EXEC` or `DISCARD`.
#[derive(Default)]
pub(crate) struct Transaction {
    queued: Vec<Queued>,
    /// While `EXEC` runs them, how many there were (for `CLIENT LIST`).
    running: usize,
    /// A command was refused while queued (Redis's `CLIENT_DIRTY_EXEC`).
    aborted: bool,
}

impl Transaction {
    /// `CLIENT LIST`'s `multi`.
    pub(crate) fn len(&self) -> usize {
        self.queued.len().max(self.running)
    }
}

/// A key a client watches.
pub(crate) struct Watched {
    pub(crate) key: Box<[u8]>,
    /// It had expired, but was still there, when watched.
    pub(crate) expired: bool,
}

/// Whether `cmd` runs at once after `MULTI` instead of being queued.
pub(crate) fn runs_in_transaction(cmd: &Command) -> bool {
    matches!(cmd.name, "exec" | "discard" | "multi" | "watch" | "quit")
}

/// Flags the transaction, if there is one: a command was refused.
pub(crate) fn flag(client: &Client) {
    if let Some(t) = client.multi.borrow_mut().as_mut() {
        t.aborted = true;
    }
}

/// Queues `cmd` and replies `+QUEUED`. A transaction that will be refused
/// keeps nothing more, as in Redis.
pub(crate) fn queue(cmd: &'static Command, args: &Args, client: &Client, out: &mut Encoder) {
    let mut multi = client.multi.borrow_mut();
    let t = multi.as_mut().expect("queued after MULTI");
    if !t.aborted && !client.dirty_cas.get() {
        let mut data = Vec::new();
        let mut ranges = Vec::with_capacity(args.len());
        for arg in args.iter() {
            let start = data.len();
            data.extend_from_slice(arg);
            ranges.push(start..data.len());
        }
        t.queued.push(Queued { cmd, data, ranges });
    }
    out.simple(b"QUEUED");
}

/// What `EXEC` is, as one command: whether it writes, whether it may add
/// memory (`denyoom`), and whether it writes without keys.
pub(crate) fn flags(client: &Client) -> (bool, bool, bool) {
    let multi = client.multi.borrow();
    let queued = multi.as_ref().map_or(&[][..], |t| &t.queued[..]);
    let write = queued.iter().any(|q| q.cmd.write);
    let denyoom = queued.iter().any(|q| q.cmd.denyoom);
    let keyless = queued.iter().any(|q| q.cmd.write && q.cmd.key_range(&q.args()).is_none());
    (write, denyoom, keyless)
}

/// Every key of the queued commands, in order, for `EXEC`'s slot check.
pub(crate) fn keys(client: &Client) -> Vec<Vec<u8>> {
    let multi = client.multi.borrow();
    let queued = multi.as_ref().map_or(&[][..], |t| &t.queued[..]);
    queued.iter().flat_map(|q| command::keys_of(q.cmd, q.args()).map(<[u8]>::to_vec).collect::<Vec<_>>()).collect()
}

/// Ends the transaction and unwatches: `DISCARD`, and `EXEC` once done or
/// refused.
pub(crate) fn discard_transaction(node: &Node, client: &Client) {
    *client.multi.borrow_mut() = None;
    client.dirty_cas.set(false);
    node.clients.unwatch_all(client);
}

/// `EXEC` refused before it ran: `-EXECABORT` with the reason (an error
/// without its `-`).
pub(crate) fn abort(node: &Node, client: &Client, why: &[u8], out: &mut Encoder) {
    discard_transaction(node, client);
    out.error_parts(&[b"EXECABORT Transaction discarded because of: ", why]);
}

pub(crate) fn multi(_: &Args, _: &Node, client: &Client, out: &mut Encoder) -> Flow {
    let mut multi = client.multi.borrow_mut();
    if multi.is_some() {
        out.error(b"ERR MULTI calls can not be nested");
    } else {
        *multi = Some(Transaction::default());
        out.ok();
    }
    Flow::Continue
}

pub(crate) fn discard(_: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    if client.multi.borrow().is_none() {
        out.error(b"ERR DISCARD without MULTI");
    } else {
        discard_transaction(node, client);
        out.ok();
    }
    Flow::Continue
}

pub(crate) fn watch(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    if client.multi.borrow().is_some() {
        out.error(b"ERR WATCH inside MULTI is not allowed");
        return Flow::Continue;
    }
    // Already touched: no point.
    if !client.dirty_cas.get() {
        let store = node.store.borrow();
        for key in args.iter().skip(1) {
            let expired = store.present(key) && !store.contains(key);
            node.clients.watch(client, key, expired);
        }
    }
    out.ok();
    Flow::Continue
}

pub(crate) fn unwatch(_: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    node.clients.unwatch_all(client);
    client.dirty_cas.set(false);
    out.ok();
    Flow::Continue
}

/// `EXEC`, once checked as a whole (see the module docs).
pub(crate) fn exec(_: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    let aborted = match &*client.multi.borrow() {
        None => {
            out.error(b"ERR EXEC without MULTI");
            return Flow::Continue;
        }
        Some(t) => t.aborted,
    };
    // A watched key that has expired since counts as touched.
    if !client.dirty_cas.get() && watched_expired(node, client) {
        client.dirty_cas.set(true);
    }
    if aborted || client.dirty_cas.get() {
        if aborted {
            out.error(b"EXECABORT Transaction discarded because of previous errors.");
        } else {
            out.null_array();
        }
        discard_transaction(node, client);
        return Flow::Continue;
    }
    node.clients.unwatch_all(client);
    let queued = {
        let mut multi = client.multi.borrow_mut();
        let t = multi.as_mut().expect("checked above");
        t.running = t.queued.len();
        std::mem::take(&mut t.queued)
    };
    out.array(queued.len());
    let repl = &node.repl;
    let replicated = repl.active();
    if replicated {
        repl.begin_transaction();
    }
    client.in_exec.set(true);
    for q in &queued {
        // QUIT isn't queued: nothing in here closes the connection.
        command::run(q.cmd, &q.args(), node, client, out);
    }
    client.in_exec.set(false);
    if replicated && repl.end_transaction() {
        client.woff.set(repl.offset());
    }
    discard_transaction(node, client);
    Flow::Continue
}

/// Whether a key `client` watches, not expired when watched, has expired
/// since (Redis's `isWatchedKeyExpired`).
fn watched_expired(node: &Node, client: &Client) -> bool {
    let store = node.store.borrow();
    client.watched.borrow().iter().any(|w| !w.expired && store.present(&w.key) && !store.contains(&w.key))
}

/// After a command (or the expiry timer, or a replica applying its
/// primary's stream): the clients watching a key the store says it touched
/// can't `EXEC` any more. Keeps the store recording touches exactly while
/// some key is watched.
pub(crate) fn touched(node: &Node) {
    let clients = &node.clients;
    if clients.recording.get() {
        let touched = node.store.borrow_mut().take_touched();
        for t in touched {
            touch(clients, &t.key, t.expired);
        }
    }
    let watching = clients.watching();
    if watching != clients.recording.get() {
        node.store.borrow_mut().record_touches(watching);
        clients.recording.set(watching);
    }
}

/// `key` was written (or, `expired`, reclaimed as expired). A key watched
/// already expired takes its deletion as no change, as in Redis.
fn touch(clients: &Clients, key: &[u8], expired: bool) {
    for c in clients.watchers(key) {
        {
            let mut watched = c.watched.borrow_mut();
            let Some(w) = watched.iter_mut().find(|w| &*w.key == key) else { continue };
            if w.expired && expired {
                w.expired = false;
                continue;
            }
        }
        c.dirty_cas.set(true);
        clients.unwatch_all(&c);
    }
}

/// The watched keys that `FLUSHALL`, or a slot leaving (`in_slot`), would
/// delete: those there, expired or not.
pub(crate) fn present_watched(node: &Node, in_slot: Option<u16>) -> Vec<Box<[u8]>> {
    if !node.clients.watching() {
        return Vec::new();
    }
    let store = node.store.borrow();
    let in_slot = |k: &[u8]| in_slot.is_none_or(|s| cluster::key_hash_slot(k) == s);
    node.clients.watched_keys().into_iter().filter(|k| in_slot(k) && store.present(k)).collect()
}

/// `keys` (from [`present_watched`]) were deleted all at once: touched,
/// except for a client that watched one already expired (Redis's
/// `touchAllWatchedKeysInDb`).
pub(crate) fn deleted(node: &Node, keys: Vec<Box<[u8]>>) {
    for key in keys {
        // As a key reclaimed: no change for one watched expired.
        touch(&node.clients, &key, true);
    }
}

/// The slot check of `EXEC` (see [`Cluster::route`]), all the queued keys
/// at once; false if refused, the transaction discarded and the
/// redirection written.
pub(crate) fn routed(cluster: &Cluster, node: &Node, client: &Client, asking: bool, write: bool, out: &mut Encoder) -> bool {
    let keys = keys(client);
    if keys.is_empty() {
        return true;
    }
    let served = command::routed(cluster, node, client, keys.iter().map(|k| &k[..]), asking, write, out);
    if !served {
        discard_transaction(node, client);
    }
    served
}
