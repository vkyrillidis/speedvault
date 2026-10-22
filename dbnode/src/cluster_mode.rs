//! Cluster mode (`--cluster`): the node's slot table, the slot check before
//! every command with keys, and the `CLUSTER` commands.
//!
//! Replies and errors are Redis 7.0.15's in a cluster with
//! `cluster-require-full-coverage no`: a slot no node owns gets
//! `-CLUSTERDOWN Hash slot not served` while the other slots carry on.
//! Without `--cluster`, `CLUSTER`, `READONLY`, `READWRITE` and `ASKING`
//! answer `-ERR This instance has cluster support disabled`, as Redis does.
//!
//! The node starts with a table of its own: itself, owning no slots. stuart
//! replaces it with `CLUSTER SETTABLE` (ours; Redis has none). A table is
//! taken only if its epoch is newer than the current one, so a delayed or
//! repeated push can't roll the layout back.
//!
//! A slot migrating away is emptied by [`crate::migrate`], which sends its
//! keys to the target in batches (`CLUSTER IMPORT`, ours). Until the target
//! has acknowledged a batch, its keys are *in flight*: commands touching
//! them wait, so the copy at the target is the key's last state when this
//! node deletes it. If the connection fails before the acknowledgement,
//! the target may or may not have the batch: its keys are *dirty*, and this
//! node answers for them, whether it still has them or not, until they've
//! been sent again (a deleted one as a delete). Otherwise a key deleted
//! here could be created at the target after `-ASK` and then overwritten by
//! the stale copy.

use crate::client::Client;
use crate::command::{conn, store_error, syntax_error, Command, Flow};
use crate::{notice, verbose, Node};
use ::cluster::{key_hash_slot, reply, NodeId, SlotRange, SlotTable, TableError, SLOTS};
use resp::{Args, Encoder};
use std::cell::{Cell, Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use store::{check_dump, decode_dump, parse_i64, Kind, SetExpiry, SetOptions, Staged, Store};

/// The cluster state of a node in cluster mode.
pub(crate) struct Cluster {
    myself: NodeId,
    state: RefCell<State>,
    /// The keys of slots migrating away that aren't settled; see the module
    /// docs.
    pub(crate) moving: RefCell<Moving>,
    /// Collections arriving in parts (`CLUSTER IMPORTPART`), hidden until
    /// the last part makes them whole.
    staging: RefCell<HashMap<Vec<u8>, Staging>>,
    /// For `INFO cluster`.
    pub(crate) stats: MigrateStats,
    /// Commands with keys served so far: the migrate fiber pauses after a
    /// batch only if clients were served since the last one.
    pub(crate) served: Cell<u64>,
    /// Set once the node started leaving the cluster (SIGTERM, see
    /// `leave`), by the thread that handles the signal.
    pub(crate) leaving: Arc<AtomicBool>,
    /// Set once stuart acknowledged the node's join (`--stuart`), by the
    /// join thread.
    pub(crate) joined: Arc<AtomicBool>,
    /// The slots the current table gives this node, for the leave thread
    /// (a node that has handed them all over lingers before it exits).
    pub(crate) owned: Arc<AtomicUsize>,
    /// With `--stuart`: stuart, which the tables come from.
    pub(crate) stuart: Option<Arc<crate::join::Stuart>>,
}

/// What moving slots has done so far (`INFO cluster`).
#[derive(Default)]
pub(crate) struct MigrateStats {
    /// Keys the targets took (deletes of dirty keys included), in how many
    /// batches, with how many bytes of values.
    pub(crate) keys_sent: Cell<u64>,
    pub(crate) batches_sent: Cell<u64>,
    pub(crate) bytes_sent: Cell<u64>,
    /// Parts of large collections the targets took; such moves given up
    /// (a target refused a part: they start again).
    pub(crate) parts_sent: Cell<u64>,
    pub(crate) parts_aborted: Cell<u64>,
    /// Keys this node took from others with `CLUSTER IMPORT`.
    pub(crate) keys_imported: Cell<u64>,
    /// Parts of collections this node took with `CLUSTER IMPORTPART`.
    pub(crate) parts_imported: Cell<u64>,
    /// `CLUSTER IMPORT` batches refused: they'd take the node past
    /// `maxmemory`.
    pub(crate) imports_refused: Cell<u64>,
    /// Redirects sent to clients: `-MOVED`, `-ASK` and `-TRYAGAIN` (for
    /// counting what a client goes through while slots move).
    pub(crate) moved: Cell<u64>,
    pub(crate) ask: Cell<u64>,
    pub(crate) tryagain: Cell<u64>,
}

fn bump(counter: &Cell<u64>) {
    counter.set(counter.get() + 1);
}

/// See the module docs.
#[derive(Default)]
pub(crate) struct Moving {
    /// Sent, not acknowledged yet: commands touching them wait.
    pub(crate) in_flight: HashSet<Vec<u8>>,
    /// Per slot, sent over a connection that failed.
    pub(crate) dirty: HashMap<u16, HashSet<Vec<u8>>>,
}

/// What the slot check decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Serve,
    /// The reply (a redirect or an error) is written.
    Refused,
    /// A key is in flight to another node: try again once it has landed.
    Wait,
}

struct State {
    table: SlotTable,
    /// This node's index in `table.nodes()`.
    me: usize,
}

impl Cluster {
    /// A node announcing `host:port`, with no slots until a table arrives.
    pub(crate) fn new(myself: NodeId, host: &str, port: u16) -> Result<Cluster, TableError> {
        let node = ::cluster::Node::new(myself, host, port)?;
        let table = SlotTable::new(0, vec![node], &[(SlotRange::new(0, SLOTS as u16 - 1), None)])?;
        Ok(Cluster {
            myself,
            state: RefCell::new(State { table, me: 0 }),
            moving: RefCell::default(),
            staging: RefCell::default(),
            stats: MigrateStats::default(),
            served: Cell::new(0),
            leaving: Arc::default(),
            joined: Arc::default(),
            owned: Arc::default(),
            stuart: None,
        })
    }

    pub(crate) fn myself(&self) -> &NodeId {
        &self.myself
    }

    /// The current table, and this node's index in it.
    pub(crate) fn table(&self) -> (Ref<'_, SlotTable>, usize) {
        let state = self.state.borrow();
        let me = state.me;
        (Ref::map(state, |s| &s.table), me)
    }

    /// Whether this node serves the request's keys, at `keys` (the command
    /// table's first, last, step). If not, writes why and returns false, in
    /// Redis's order (`getNodeByQuery`): the first key's slot is unassigned
    /// (`CLUSTERDOWN`), another key is in another slot (`CROSSSLOT`; the
    /// same key again is fine), or another node owns the slot (`MOVED`).
    ///
    /// While a slot migrates, its keys count. The source (the owner)
    /// serves a request whose keys it all has; one with none of them goes
    /// to the target (`-ASK`), one with some is refused (`-TRYAGAIN`). The
    /// target serves the slot only right after `ASKING` (`asking`), and
    /// refuses a request of several keys it doesn't all have; without
    /// `ASKING` it redirects to the owner (`MOVED`). A key that has expired
    /// counts as missing, but isn't reclaimed here (as in Redis: the
    /// command may not run). At the source, a key in flight makes the
    /// request wait ([`Route::Wait`]), and a dirty key counts as present.
    pub(crate) fn route(&self, args: &Args, keys: (i32, i32, i32), store: &RefCell<Store>, asking: bool, out: &mut Encoder) -> Route {
        let (first, last, step) = keys;
        let last = if last < 0 { args.len() as i32 + last } else { last };
        let state = self.state.borrow();
        let first_key = &args[first as usize];
        let slot = key_hash_slot(first_key);
        let Some(owner) = state.table.owner_index(slot) else {
            out.error(b"CLUSTERDOWN Hash slot not served");
            return Route::Refused;
        };
        let migration = state.table.migration(slot);
        let migrating = migration.filter(|_| owner == state.me);
        let importing = migration.filter(|m| migrating.is_none() && m.to == state.me);
        let moving = migrating.map(|_| self.moving.borrow());
        let (mut missing, mut existing, mut several, mut wait) = (0, 0, false, false);
        let mut i = first;
        while i <= last {
            let key = &args[i as usize];
            if i != first && key != first_key {
                if key_hash_slot(key) != slot {
                    out.error(b"CROSSSLOT Keys in request don't hash to the same slot");
                    return Route::Refused;
                }
                several = true;
            }
            if let Some(moving) = &moving {
                wait |= moving.in_flight.contains(key);
                let dirty = moving.dirty.get(&slot).is_some_and(|d| d.contains(key));
                if dirty || store.borrow().contains(key) {
                    existing += 1;
                } else {
                    missing += 1;
                }
            } else if importing.is_some() {
                if store.borrow().contains(key) {
                    existing += 1;
                } else {
                    missing += 1;
                }
            }
            i += step;
        }
        if wait {
            return Route::Wait;
        }
        if let Some(m) = migrating.filter(|_| missing > 0) {
            if existing > 0 {
                bump(&self.stats.tryagain);
                out.error(b"TRYAGAIN Multiple keys request during rehashing of slot");
            } else {
                bump(&self.stats.ask);
                let to = &state.table.nodes()[m.to];
                out.error(format!("ASK {slot} {}:{}", to.host(), to.port()).as_bytes());
            }
            return Route::Refused;
        }
        if importing.is_some() && asking {
            if several && missing > 0 {
                bump(&self.stats.tryagain);
                out.error(b"TRYAGAIN Multiple keys request during rehashing of slot");
                return Route::Refused;
            }
            return Route::Serve;
        }
        if owner != state.me {
            let node = &state.table.nodes()[owner];
            let (host, port) = (node.host(), node.port());
            bump(&self.stats.moved);
            out.error(format!("MOVED {slot} {host}:{port}").as_bytes());
            return Route::Refused;
        }
        Route::Serve
    }

    /// Replaces the table if `table` is newer and lists this node. A table
    /// with the current epoch is accepted again only unchanged.
    fn install(&self, table: SlotTable) -> Result<(), String> {
        let mut state = self.state.borrow_mut();
        let (epoch, current) = (table.epoch(), state.table.epoch());
        if epoch < current {
            return Err(format!("ERR Table epoch {epoch} is older than the current epoch {current}"));
        }
        if epoch == current {
            return if table == state.table {
                Ok(())
            } else {
                Err(format!("ERR Table epoch {epoch} is the current epoch, with a different table"))
            };
        }
        let Some(me) = table.position(&self.myself) else {
            return Err(format!("ERR The table doesn't list this node ({})", self.myself));
        };
        let mine = table.runs().filter(|(_, owner)| *owner == Some(me)).map(|(r, _)| r.len()).sum::<usize>();
        self.owned.store(mine, Ordering::Relaxed);
        let out = table.migrations().iter().filter(|m| m.from == me).count();
        let ins = table.migrations().iter().filter(|m| m.to == me).count();
        let moving = if out + ins > 0 { format!("; migrating {out} out, {ins} in") } else { String::new() };
        let line = format!("Slot table epoch {epoch}: {} nodes, {mine} of {} assigned slots here{moving}", table.nodes().len(), table.assigned());
        // While slots move, a table per slot moved: not worth a line each.
        if table.migrations().is_empty() && state.table.migrations().is_empty() {
            notice!("{line}");
        } else {
            verbose!("{line}");
        }
        // Dirty keys of a slot no longer migrating from here are settled:
        // the move was called off (this node owns the slot), or the target
        // owns it now, which stuart allows only once nothing was pending.
        self.moving.borrow_mut().dirty.retain(|slot, _| table.migration(*slot).is_some_and(|m| m.from == me));
        // A collection half imported into a slot no longer imported here
        // (the move ended: its source died, or it was called off) is never
        // served.
        self.staging.borrow_mut().retain(|key, _| table.migration(key_hash_slot(key)).is_some_and(|m| m.to == me));
        *state = State { table, me };
        Ok(())
    }
}

/// What a key `CLUSTER IMPORT` adds costs besides its key and value: the
/// map's entry and spare room, the `Rc`, the slot index, the allocator's
/// rounding (measured: 160 bytes for 1M keys of 10 bytes, just after the
/// map doubled; its growth is counted apart).
const IMPORT_ENTRY: usize = 160;
/// What an element of a collection costs besides its bytes in the
/// payload, by type: its boxes and the allocator's rounding, its place in
/// the table (with its hash) or the list, the index. Measured (phase 7
/// milestone 6, 8-byte elements, used_memory over 300k and 1M of them)
/// at 75-110 bytes for a hash's field and value, 46-68 for a set's
/// member, 29-40 for a list's element: the higher, as the table may just
/// have grown.
const fn import_element(kind: Kind) -> usize {
    match kind {
        Kind::Str => 0,
        Kind::Hash => 112,
        Kind::Set => 72,
        Kind::List => 40,
    }
}

/// The memory a key `CLUSTER IMPORT` adds is expected to take.
fn import_cost(kind: Kind, key: &[u8], value: &[u8]) -> u64 {
    let elements = match kind {
        Kind::Str => 0,
        Kind::Hash => store::decode_dump(value).map_or(0, |items| items.len() / 2),
        _ => store::decode_dump(value).map_or(0, |items| items.len()),
    };
    (key.len() + value.len() + IMPORT_ENTRY + elements * import_element(kind)) as u64
}

pub(crate) const CLUSTER: &[Command] = &[
    conn("cluster|myid", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            out.bulk(c.myself.as_bytes());
        }
        Flow::Continue
    }),
    conn("cluster|slots", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            reply::slots(&c.state.borrow().table, out);
        }
        Flow::Continue
    }),
    conn("cluster|shards", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            reply::shards(&c.state.borrow().table, out);
        }
        Flow::Continue
    }),
    conn("cluster|nodes", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            reply::nodes(&c.state.borrow().table, Some(&c.myself), out);
        }
        Flow::Continue
    }),
    conn("cluster|info", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            out.verbatim(b"txt", reply::info_text(&c.state.borrow().table).as_bytes());
        }
        Flow::Continue
    }),
    conn("cluster|keyslot", 3, |args, node, _, out| {
        if enabled(node, out).is_some() {
            out.integer(i64::from(key_hash_slot(&args[2])));
        }
        Flow::Continue
    }),
    conn("cluster|countkeysinslot", 3, count_keys_in_slot),
    conn("cluster|getkeysinslot", 4, get_keys_in_slot),
    conn("cluster|settable", -7, set_table),
    conn("cluster|import", -4, import),
    conn("cluster|importpart", 9, import_part),
    conn("cluster|pending", 3, pending),
];

/// `READONLY` and `READWRITE`: accepted in cluster mode. There are no
/// replicas to read from yet.
pub(crate) fn accept(_: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_some() {
        out.ok();
    }
    Flow::Continue
}

/// `ASKING`: the next command may use a slot this node is importing.
pub(crate) fn asking(_: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_some() {
        client.asking.set(true);
        out.ok();
    }
    Flow::Continue
}

/// The node's cluster state, or Redis's error when it has none.
fn enabled<'a>(node: &'a Node, out: &mut Encoder) -> Option<&'a Cluster> {
    if node.cluster.is_none() {
        out.error(b"ERR This instance has cluster support disabled");
    }
    node.cluster.as_ref()
}

/// `CLUSTER COUNTKEYSINSLOT slot`: from the store's slot index, O(1).
fn count_keys_in_slot(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_none() {
        return Flow::Continue;
    }
    match parse_i64(&args[2]) {
        None => store_error(store::Error::NotInteger, out),
        Some(slot) if !(0..SLOTS as i64).contains(&slot) => out.error(b"ERR Invalid slot"),
        Some(slot) => out.integer(node.store.borrow().count_in_slot(slot as u16) as i64),
    }
    Flow::Continue
}

/// `CLUSTER GETKEYSINSLOT slot count`: up to `count` keys of the slot, in
/// no particular order (expired ones not reclaimed yet included, as Redis
/// lists them). Both numbers are read before either is checked, as in
/// Redis.
fn get_keys_in_slot(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_none() {
        return Flow::Continue;
    }
    match (parse_i64(&args[2]), parse_i64(&args[3])) {
        (None, _) | (_, None) => store_error(store::Error::NotInteger, out),
        (Some(slot), Some(count)) if !(0..SLOTS as i64).contains(&slot) || count < 0 => {
            out.error(b"ERR Invalid slot or number of keys")
        }
        (Some(slot), Some(count)) => {
            let store = node.store.borrow();
            let n = store.count_in_slot(slot as u16).min(usize::try_from(count).unwrap_or(usize::MAX));
            out.array(n);
            for key in store.keys_in_slot(slot as u16).take(n) {
                out.bulk(key);
            }
        }
    }
    Flow::Continue
}

/// `CLUSTER IMPORT slot n [key ttl type value]...` (ours): keys of a slot
/// this node imports, from the node it migrates from. `ttl`: milliseconds
/// left, 0 without expiry, -1 to delete the key (its type and value are
/// ignored). `type` is what `TYPE` says; a collection's value is encoded
/// (see the store's `dump`). All are checked before any is applied.
fn import(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    let Some(slot) = slot_arg(&args[2], out) else { return Flow::Continue };
    let n = match parse_i64(&args[3]) {
        Some(n) if n >= 0 && (n as u64).checked_mul(4).is_some_and(|m| m == args.len() as u64 - 4) => n as usize,
        _ => {
            syntax_error(out);
            return Flow::Continue;
        }
    };
    {
        let (table, me) = c.table();
        if !table.migration(slot).is_some_and(|m| m.to == me) {
            out.error(format!("ERR Slot {slot} is not being imported here").as_bytes());
            return Flow::Continue;
        }
    }
    let mut ttls = Vec::with_capacity(n);
    let mut kinds = Vec::with_capacity(n);
    for k in 0..n {
        let (key, ttl, kind, value) = (&args[4 + 4 * k], &args[5 + 4 * k], &args[6 + 4 * k], &args[7 + 4 * k]);
        if key_hash_slot(key) != slot {
            out.error_parts(&[b"ERR Key '", key, format!("' is not in slot {slot}").as_bytes()]);
            return Flow::Continue;
        }
        match parse_i64(ttl) {
            Some(ttl) if ttl >= -1 => ttls.push(ttl),
            _ => {
                out.error_parts(&[b"ERR Invalid ttl '", ttl, b"'"]);
                return Flow::Continue;
            }
        }
        match Kind::from_name(kind) {
            Some(kind) if ttls[k] == -1 || check_dump(kind, value) => kinds.push(kind),
            Some(_) => {
                out.error_parts(&[b"ERR Invalid value for '", key, b"'"]);
                return Flow::Continue;
            }
            None => {
                out.error_parts(&[b"ERR Invalid type '", kind, b"'"]);
                return Flow::Continue;
            }
        }
    }
    // Refused if the keys would take the node past `maxmemory`: the source
    // keeps them and tries again later (a full node also gets no slots
    // from stuart). Each key costs its bytes and about an entry's worth;
    // deletes cost nothing.
    let max = node.maxmemory.get();
    if max != 0 {
        let adds: u64 = (0..n).filter(|&k| ttls[k] != -1).map(|k| import_cost(kinds[k], &args[4 + 4 * k], &args[7 + 4 * k])).sum();
        let growth = node.store.borrow().growth() as u64;
        if adds > 0 && crate::memory::used() as u64 + adds + growth > max {
            c.stats.imports_refused.set(c.stats.imports_refused.get() + 1);
            out.error(crate::command::OOM);
            return Flow::Continue;
        }
    }
    let mut store = node.store.borrow_mut();
    c.stats.keys_imported.set(c.stats.keys_imported.get() + n as u64);
    let mut staging = c.staging.borrow_mut();
    for (k, ttl) in ttls.into_iter().enumerate() {
        let (key, value) = (&args[4 + 4 * k], &args[7 + 4 * k]);
        // A whole key, or its delete, ends a move of it in parts.
        staging.remove(key);
        if ttl == -1 {
            store.del(key);
        } else if kinds[k] == Kind::Str {
            let expiry = if ttl == 0 { SetExpiry::Clear } else { SetExpiry::Px(ttl) };
            // Can't fail: a positive PX, no GET.
            let _ = store.set(key, value, SetOptions { expiry, ..SetOptions::default() });
        } else {
            store.restore(key, kinds[k], value, (ttl != 0).then_some(ttl));
        }
    }
    out.ok();
    Flow::Continue
}

/// A collection [`import_part`] is building.
struct Staging {
    staged: Staged,
    /// How many elements it will hold.
    total: usize,
}

/// `CLUSTER IMPORTPART slot key type total offset payload ttl` (ours): part
/// of a collection too large for one batch, elements `offset..` of `total`
/// (a hash's fields), encoded as `CLUSTER IMPORT`'s values. Offset 0
/// starts the key again; the others must follow the part before. The key
/// is hidden until the last part, which sets it (replacing any other) with
/// `ttl`: milliseconds left, 0 without expiry. The first part is refused
/// with `-OOM` if the whole collection, estimated from it, wouldn't fit
/// under `maxmemory`, a later one if it doesn't.
fn import_part(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    let Some(slot) = slot_arg(&args[2], out) else { return Flow::Continue };
    let (key, payload) = (&args[3], &args[7]);
    {
        let (table, me) = c.table();
        if !table.migration(slot).is_some_and(|m| m.to == me) {
            out.error(format!("ERR Slot {slot} is not being imported here").as_bytes());
            return Flow::Continue;
        }
    }
    if key_hash_slot(key) != slot {
        out.error_parts(&[b"ERR Key '", key, format!("' is not in slot {slot}").as_bytes()]);
        return Flow::Continue;
    }
    let number = |arg: &[u8]| parse_i64(arg).filter(|&n| n >= 0).map(|n| n as usize);
    let (Some(total), Some(offset), Some(ttl)) = (number(&args[5]), number(&args[6]), number(&args[8])) else {
        syntax_error(out);
        return Flow::Continue;
    };
    let kind = match Kind::from_name(&args[4]) {
        Some(Kind::Str) | None => {
            out.error_parts(&[b"ERR Invalid type '", &args[4], b"'"]);
            return Flow::Continue;
        }
        Some(kind) => kind,
    };
    let count = match decode_dump(payload) {
        Some(items) if check_dump(kind, payload) => items.len() / if kind == Kind::Hash { 2 } else { 1 },
        _ => {
            out.error_parts(&[b"ERR Invalid value for '", key, b"'"]);
            return Flow::Continue;
        }
    };
    let mut staging = c.staging.borrow_mut();
    if offset == 0 {
        staging.remove(key);
    }
    let received = staging.get(key).map_or(0, |s| s.staged.count());
    let follows = staging.get(key).is_none_or(|s| s.staged.kind() == kind && s.total == total);
    if offset != received || !follows || offset + count > total {
        out.error_parts(&[b"ERR Unexpected part of '", key, b"'"]);
        return Flow::Continue;
    }
    let max = node.maxmemory.get();
    if max != 0 {
        // The first part answers for the whole collection.
        let mut adds = import_cost(kind, key, payload);
        if offset == 0 {
            adds = (adds as u128 * total as u128 / count as u128) as u64;
        }
        let growth = node.store.borrow().growth() as u64;
        if crate::memory::used() as u64 + adds + growth > max {
            c.stats.imports_refused.set(c.stats.imports_refused.get() + 1);
            staging.remove(key);
            out.error(crate::command::OOM);
            return Flow::Continue;
        }
    }
    let s = staging.entry(key.to_vec()).or_insert_with(|| Staging { staged: Staged::new(kind), total });
    s.staged.append(payload);
    c.stats.parts_imported.set(c.stats.parts_imported.get() + 1);
    if s.staged.count() == total {
        let s = staging.remove(key).expect("just added to");
        node.store.borrow_mut().restore_staged(key, s.staged, (ttl != 0).then_some(ttl as i64));
        c.stats.keys_imported.set(c.stats.keys_imported.get() + 1);
    }
    out.ok();
    Flow::Continue
}

/// `CLUSTER PENDING slot` (ours): how many keys of the slot this node still
/// answers for: those it has (`COUNTKEYSINSLOT`) and, while it migrates
/// the slot away, the dirty ones it no longer has. 0 at the source of a
/// migration: every key is at the target, and the move can end.
fn pending(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    let Some(slot) = slot_arg(&args[2], out) else { return Flow::Continue };
    let store = node.store.borrow();
    let moving = c.moving.borrow();
    let gone = moving.dirty.get(&slot).map_or(0, |d| d.iter().filter(|k| !store.contains(k)).count());
    out.integer((store.count_in_slot(slot) + gone) as i64);
    Flow::Continue
}

/// A slot number, or Redis's error for one that isn't.
fn slot_arg(arg: &[u8], out: &mut Encoder) -> Option<u16> {
    match parse_i64(arg) {
        Some(slot) if (0..SLOTS as i64).contains(&slot) => Some(slot as u16),
        Some(_) => {
            out.error(b"ERR Invalid slot");
            None
        }
        None => {
            store_error(store::Error::NotInteger, out);
            None
        }
    }
}

/// `CLUSTER SETTABLE epoch NODES n [id host port]... RANGES m [start end
/// id|-]... [MIGRATING k [slot from to]...]`: the whole table, from
/// stuart. The ranges must name every slot once (`-`: unassigned), and the
/// nodes must include this one. A migration names the slot's owner and the
/// node it moves to.
fn set_table(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    match parse_table(args) {
        Ok(table) => match c.install(table) {
            Ok(()) => {
                if let (Some(stuart), Some(addr)) = (&c.stuart, client.addr) {
                    stuart.seen_at(addr.ip());
                }
                out.ok()
            }
            Err(e) => out.error(e.as_bytes()),
        },
        Err(TableArgError::Syntax) => syntax_error(out),
        Err(TableArgError::Invalid(e)) => out.error(e.as_bytes()),
    }
    Flow::Continue
}

enum TableArgError {
    /// The keywords or the counts don't fit the arguments.
    Syntax,
    Invalid(String),
}

fn parse_table(args: &Args) -> Result<SlotTable, TableArgError> {
    let invalid = |what: &str, arg: &[u8]| TableArgError::Invalid(format!("ERR Invalid {what} '{}'", String::from_utf8_lossy(arg)));
    let number = |what: &str, arg: &[u8], max: i64| parse_i64(arg).filter(|n| (0..=max).contains(n)).ok_or_else(|| invalid(what, arg));
    let id = |arg: &[u8]| NodeId::parse(arg).ok_or_else(|| invalid("node id", arg));
    // `KEYWORD count` at `i`, then `count` groups of 3 arguments.
    let section = |i: usize, keyword: &[u8]| -> Result<usize, TableArgError> {
        match (args.get(i), args.get(i + 1)) {
            (Some(word), Some(count)) if word.eq_ignore_ascii_case(keyword) => match parse_i64(count) {
                Some(n) if n >= 0 && (n as usize) <= (args.len() - i - 2) / 3 => Ok(n as usize),
                _ => Err(TableArgError::Syntax),
            },
            _ => Err(TableArgError::Syntax),
        }
    };

    // The shape first, then the values.
    let n = section(3, b"nodes")?;
    let r = 5 + 3 * n;
    let m = section(r, b"ranges")?;
    let g = r + 2 + 3 * m;
    let k = if args.len() == g { 0 } else { section(g, b"migrating")? };
    if args.len() != if args.len() == g { g } else { g + 2 + 3 * k } {
        return Err(TableArgError::Syntax);
    }
    let epoch = number("epoch", &args[2], i64::MAX)? as u64;
    let mut nodes = Vec::with_capacity(n);
    for k in 0..n {
        let i = 5 + 3 * k;
        let port = number("port", &args[i + 2], i64::from(u16::MAX)).and_then(|p| if p == 0 { Err(invalid("port", &args[i + 2])) } else { Ok(p) })?;
        let host = std::str::from_utf8(&args[i + 1]).map_err(|_| invalid("host", &args[i + 1]))?;
        let node = ::cluster::Node::new(id(&args[i])?, host, port as u16).map_err(|_| invalid("host", &args[i + 1]))?;
        nodes.push(node);
    }
    let mut ranges = Vec::with_capacity(m);
    for k in 0..m {
        let i = r + 2 + 3 * k;
        let start = number("slot", &args[i], i64::from(u16::MAX))? as u16;
        let end = number("slot", &args[i + 1], i64::from(u16::MAX))? as u16;
        let owner = if &args[i + 2] == b"-" { None } else { Some(id(&args[i + 2])?) };
        ranges.push((SlotRange::new(start, end), owner));
    }
    let mut migrations = Vec::with_capacity(k);
    for j in 0..k {
        let i = g + 2 + 3 * j;
        let slot = number("slot", &args[i], i64::from(u16::MAX))? as u16;
        migrations.push((slot, id(&args[i + 1])?, id(&args[i + 2])?));
    }
    SlotTable::new(epoch, nodes, &ranges)
        .and_then(|t| t.with_migrations(&migrations))
        .map_err(|e| TableArgError::Invalid(format!("ERR Invalid table: {e}")))
}
