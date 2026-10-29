//! Cluster mode (`--cluster`): the node's slot table, the slot check before
//! every command with keys, and the `CLUSTER` commands.
//!
//! Replies and errors are Redis 8.10.2's in a cluster with
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
//! The table also says whether the node is a primary or a replica (phase
//! 8). A replica owns no slots, so it redirects every key to its owner,
//! writes and reads alike, as a Redis replica does without `READONLY`.
//! After `READONLY` it answers reads of its primary's slots from its copy
//! (possibly behind), as Redis's does: from its empty store before its
//! first sync, and `-LOADING` while it syncs from scratch (agreed
//! 2026-10-23).
//!
//! A table may say a slot migrates key by key (`MIGRATING`), the state
//! Redis's `CLUSTER SETSLOT slot MIGRATING` and `IMPORTING` leave: the
//! replies are Redis's (`-ASK`, `-TRYAGAIN`, `ASKING`, the markers of
//! `CLUSTER NODES`), from the keys each side has (see [`Cluster::route`]).
//! Nothing moves such a slot's keys: phase 5 did, until slots moved by
//! snapshot (phase 8 milestone 6), and stuart makes no such table since.
//!
//! A slot moving by snapshot (phase 8, see [`crate::repl`]) is served by its
//! owner alone until the cutover. Just before, stuart pauses its writes at
//! the owner (`CLUSTER PAUSESLOTS`): they wait, reads go on, and the pause
//! ends by itself, or with the table that gives the slots to the target
//! (then the writes waiting are redirected there). At the target, the
//! slots of that table wait until their sync has ended (`landing`): it
//! serves them only with every write the source took.
//!
//! A primary leaving (SIGTERM) hands its slots to its replica: stuart
//! pauses all its writes (`CLUSTER PAUSEWRITES`, ours, as Redis's `CLIENT
//! PAUSE WRITE`: they wait, reads go on, keys aren't expired actively), waits
//! for the replica to have them all and promotes it. The pause ends by
//! itself, or with the table that leaves the node no slots (then the writes
//! waiting are redirected to their new owner).

use crate::client::Client;
use crate::command::{conn, store_error, syntax_error, Command, Flow};
use crate::{notice, verbose, Node};
use ::cluster::{key_hash_slot, reply, NodeId, SlotRange, SlotTable, TableError, SLOTS};
use reactor::yield_now;
use resp::{Args, Encoder};
use std::cell::{Cell, Ref, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::{parse_i64, Store};

/// The cluster state of a node in cluster mode.
pub(crate) struct Cluster {
    myself: NodeId,
    state: RefCell<State>,
    /// For `INFO cluster`.
    pub(crate) stats: MigrateStats,
    /// Commands with keys served so far: a slot sync's copy pauses after a
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
    /// The nodes' replication offsets as stuart last read them (`CLUSTER
    /// SETOFFSETS`, every second or so), for `CLUSTER SLOTS` and `SHARDS`.
    offsets: RefCell<HashMap<NodeId, u64>>,
    /// Slots whose writes wait (`CLUSTER PAUSESLOTS`): see the module docs.
    pauses: RefCell<Vec<Pause>>,
    /// Every write waits until then (`CLUSTER PAUSEWRITES`): see the module
    /// docs.
    paused: RefCell<Option<WritePause>>,
    /// Slots the table gives this node whose sync from their old owner
    /// hasn't ended yet: every command on them waits (see the module docs).
    pub(crate) landing: RefCell<Vec<u16>>,
    /// This node's replicas that took its slots over (a failover, a
    /// handover): the slots they own aren't trimmed here while its other
    /// replicas may still be applying its stream (they follow the new
    /// primary once they took the table): forgotten once no replica follows
    /// it (`repl::slots::trim_loop`), or once a replica itself.
    pub(crate) successors: RefCell<Vec<NodeId>>,
}

/// Writes to `slots`, moving by snapshot to `target`, wait until `until`.
struct Pause {
    target: NodeId,
    slots: Vec<u16>,
    since: Instant,
    until: Instant,
}

impl Pause {
    /// Logs how long the writes waited (for migrate_bench's cutover
    /// pauses): `how` it ended.
    fn ended(&self, how: &str) {
        let waited = self.until.min(Instant::now()).saturating_duration_since(self.since);
        verbose!("Writes to {} slots moving to {} resumed after {} us ({how})", self.slots.len(), self.target, waited.as_micros());
    }
}

/// Every write waits from `since` until `until` (`CLUSTER PAUSEWRITES`).
struct WritePause {
    since: Instant,
    until: Instant,
}

impl WritePause {
    /// Logs how long the writes waited (for the handover's pause): `how`
    /// it ended.
    fn ended(&self, how: &str) {
        let waited = self.until.min(Instant::now()).saturating_duration_since(self.since);
        verbose!("Writes resumed after {} us ({how})", waited.as_micros());
    }
}

/// The longest `CLUSTER PAUSESLOTS` and `CLUSTER PAUSEWRITES` take.
const PAUSE_MAX: Duration = Duration::from_secs(10);
/// What a slot sync's target may have left to acknowledge for its slots'
/// writes to be paused (Redis 8's `cluster-slot-migration-handoff-max-lag-bytes`).
pub(crate) const PAUSE_LAG: u64 = 1 << 20;

/// What moving slots has done so far (`INFO cluster`).
#[derive(Default)]
pub(crate) struct MigrateStats {
    /// Keys the targets took: those the slots moved away held at their
    /// cutovers.
    pub(crate) keys_sent: Cell<u64>,
    /// Keys this node took: those the slots moved here held once taken.
    pub(crate) keys_imported: Cell<u64>,
    /// Times a slot sync's copy here waited for room under `maxmemory`.
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

/// What a command may do at a replica of the slot's owner: a read after
/// `READONLY` is served, or gets `-LOADING` while the replica syncs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplicaRead {
    No,
    Serve,
    Loading,
}

/// What the slot check decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Serve,
    /// The reply (a redirect or an error) is written.
    Refused,
    /// A write to a slot paused, or a request to a slot landing (see the
    /// module docs): try again a little later ([`wait`]).
    Wait,
}

/// Waits a little for a [`Route::Wait`]: one turn of the reactor at first,
/// a millisecond at a time once that has gone on for a while.
pub(crate) fn wait(waited: &mut u32) {
    *waited += 1;
    if *waited < 1000 {
        yield_now();
    } else {
        reactor::sleep(Duration::from_millis(1));
    }
}

struct State {
    table: SlotTable,
    /// This node's index in `table.nodes()`.
    me: usize,
    /// The replicas the table gives this node and doesn't mark failed:
    /// their acknowledgements hold its lease (phase 8).
    lease: Vec<NodeId>,
}

impl Cluster {
    /// A node announcing `host:port`, with no slots until a table arrives.
    pub(crate) fn new(myself: NodeId, host: &str, port: u16) -> Result<Cluster, TableError> {
        let node = ::cluster::Node::new(myself, host, port)?;
        let table = SlotTable::new(0, vec![node], &[(SlotRange::new(0, SLOTS as u16 - 1), None)])?;
        Ok(Cluster {
            myself,
            state: RefCell::new(State { table, me: 0, lease: Vec::new() }),
            stats: MigrateStats::default(),
            served: Cell::new(0),
            leaving: Arc::default(),
            joined: Arc::default(),
            owned: Arc::default(),
            stuart: None,
            offsets: RefCell::default(),
            pauses: RefCell::default(),
            paused: RefCell::default(),
            landing: RefCell::default(),
            successors: RefCell::default(),
        })
    }

    /// Whether a write to `slot` waits (see the module docs); `None`: a
    /// write without keys (`FLUSHALL`), which waits for any pause.
    pub(crate) fn write_paused(&self, slot: Option<u16>) -> bool {
        if self.all_paused() {
            return true;
        }
        let mut pauses = self.pauses.borrow_mut();
        if pauses.is_empty() {
            return false;
        }
        let now = Instant::now();
        pauses.retain(|p| p.until > now || {
            p.ended("expired");
            false
        });
        pauses.iter().any(|p| slot.is_none_or(|s| p.slots.binary_search(&s).is_ok()))
    }

    /// Whether every write waits (`CLUSTER PAUSEWRITES`); active expiry
    /// waits too.
    pub(crate) fn all_paused(&self) -> bool {
        let mut paused = self.paused.borrow_mut();
        match &*paused {
            None => false,
            Some(p) if p.until > Instant::now() => true,
            Some(p) => {
                p.ended("expired");
                *paused = None;
                false
            }
        }
    }

    pub(crate) fn myself(&self) -> &NodeId {
        &self.myself
    }

    /// This node's primary, if the table makes it a replica.
    pub(crate) fn primary(&self) -> Option<::cluster::Node> {
        let state = self.state.borrow();
        state.table.primary_of(state.me).map(|p| state.table.nodes()[p].clone())
    }

    /// The current table, and this node's index in it.
    pub(crate) fn table(&self) -> (Ref<'_, SlotTable>, usize) {
        let state = self.state.borrow();
        let me = state.me;
        (Ref::map(state, |s| &s.table), me)
    }

    /// Whether this node serves the request's keys, at `keys` (the command
    /// table's first, last, step). If not, writes why and returns false, in
    /// Redis 8's order (`preprocessCommand`, `getNodeByQuery`): a key is in
    /// another slot than the first's (`CROSSSLOT`; the same key again is
    /// fine), the first key's slot is unassigned (`CLUSTERDOWN`), or another
    /// node owns the slot (`MOVED`).
    ///
    /// While a slot migrates key by key (see the module docs), its keys
    /// count. The source (the owner) serves a request whose keys it all
    /// has; one with none of them goes to the target (`-ASK`), one with
    /// some is refused (`-TRYAGAIN`). The target serves the slot only right
    /// after `ASKING` (`asking`), and refuses a request of several keys it
    /// doesn't all have; without `ASKING` it redirects to the owner
    /// (`MOVED`). A key that has expired counts as missing, but isn't
    /// reclaimed here (as in Redis: the command may not run). A `write` to
    /// a slot paused, or any request to a slot landing (moving by snapshot,
    /// see the module docs), waits ([`Route::Wait`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route(
        &self,
        args: &Args,
        keys: (i32, i32, i32),
        store: &RefCell<Store>,
        asking: bool,
        write: bool,
        replica_read: ReplicaRead,
        out: &mut Encoder,
    ) -> Route {
        let (first, last, step) = keys;
        let last = if last < 0 { args.len() as i32 + last } else { last };
        let state = self.state.borrow();
        let first_key = &args[first as usize];
        let slot = key_hash_slot(first_key);
        let Some(owner) = state.table.owner_index(slot) else {
            let crosses = (first..=last).step_by(step as usize).any(|i| key_hash_slot(&args[i as usize]) != slot);
            if crosses {
                out.error(b"CROSSSLOT Keys in request don't hash to the same slot");
            } else {
                out.error(b"CLUSTERDOWN Hash slot not served");
            }
            return Route::Refused;
        };
        if owner == state.me && ((write && self.write_paused(Some(slot))) || self.landing.borrow().contains(&slot)) {
            return Route::Wait;
        }
        // A slot moving by snapshot is its owner's alone: no -ASK, and its
        // target redirects to the owner like any other node.
        let migration = state.table.migration(slot).filter(|m| !m.snapshot);
        let migrating = migration.filter(|_| owner == state.me);
        let importing = migration.filter(|m| migrating.is_none() && m.to == state.me);
        let (mut missing, mut existing, mut several) = (0, 0, false);
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
            if migrating.is_some() || importing.is_some() {
                if store.borrow().contains(key) {
                    existing += 1;
                } else {
                    missing += 1;
                }
            }
            i += step;
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
        if owner != state.me && replica_read != ReplicaRead::No && state.table.primary_of(state.me) == Some(owner) {
            if replica_read == ReplicaRead::Loading {
                out.error(b"LOADING Redis is loading the dataset in memory");
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
        let role = table.primary_of(me).map_or(String::new(), |p| format!(", a replica of {}", table.nodes()[p].id()));
        let line = format!("Slot table epoch {epoch}: {} nodes, {mine} of {} assigned slots here{moving}{role}", table.nodes().len(), table.assigned());
        // While slots move, a table per slot moved: not worth a line each.
        let role_changed = table.primary_of(me).map(|p| table.nodes()[p].id()) != state.table.primary_of(state.me).map(|p| state.table.nodes()[p].id());
        if role_changed || (table.migrations().is_empty() && state.table.migrations().is_empty()) {
            notice!("{line}");
        } else {
            verbose!("{line}");
        }
        // A pause ends with the move: the slots are the target's now, or
        // the move was called off.
        self.pauses.borrow_mut().retain(|p| {
            let to = table.position(&p.target);
            let kept = p.slots.iter().all(|&s| table.migration(s).is_some_and(|m| m.snapshot && m.from == me && Some(m.to) == to));
            if !kept {
                p.ended("table");
            }
            kept
        });
        // A pause of every write ends with the slots handed over.
        if mine == 0 {
            if let Some(p) = self.paused.borrow_mut().take() {
                p.ended("table");
            }
        }
        let mut successors = self.successors.borrow_mut();
        if table.primary_of(me).is_some() {
            successors.clear();
        }
        for r in state.table.replicas_of(state.me).map(|r| *state.table.nodes()[r].id()) {
            let Some(i) = table.position(&r).filter(|_| table.primary_of(me).is_none()) else { continue };
            let took = (0..SLOTS as u16).any(|s| state.table.owner_index(s) == Some(state.me) && table.owner_index(s) == Some(i));
            if took && !successors.contains(&r) {
                successors.push(r);
            }
        }
        drop(successors);
        let lease = table.replicas_of(me).filter(|&r| !table.is_failed(r)).map(|r| *table.nodes()[r].id()).collect();
        *state = State { table, me, lease };
        Ok(())
    }
}

pub(crate) const CLUSTER: &[Command] = &[
    conn("cluster|myid", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            out.bulk(c.myself.as_bytes());
        }
        Flow::Continue
    }),
    // Its shard's id in the table (see `SlotTable::with_shards`); before
    // the node is in one, its own id, as a Redis node starts a shard of
    // its own.
    conn("cluster|myshardid", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            let state = c.state.borrow();
            let shard = state.table.position(&c.myself).and_then(|i| state.table.shard_of(i));
            out.bulk(shard.unwrap_or(c.myself).as_bytes());
        }
        Flow::Continue
    }),
    conn("cluster|slots", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            reply::slots(&c.state.borrow().table, &offsets(node), out);
        }
        Flow::Continue
    }),
    conn("cluster|shards", 2, |_, node, _, out| {
        if let Some(c) = enabled(node, out) {
            reply::shards(&c.state.borrow().table, &offsets(node), out);
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
    conn("cluster|setoffsets", -2, set_offsets),
    conn("cluster|fence", 3, fence),
    conn("cluster|slotsyncs", 2, slot_syncs),
    conn("cluster|pauseslots", -5, pause_slots),
    conn("cluster|pausewrites", 3, pause_writes),
];

/// The nodes' replication offsets as this node knows them: its own (a
/// primary's stream, a replica's copy of its primary's), and the others'
/// as stuart last pushed them, 0 for a node it didn't name.
fn offsets(node: &Node) -> Vec<u64> {
    let c = node.cluster.as_ref().expect("cluster mode");
    let state = c.state.borrow();
    let known = c.offsets.borrow();
    let mut offsets: Vec<u64> = state.table.nodes().iter().map(|n| known.get(n.id()).copied().unwrap_or(0)).collect();
    offsets[state.me] = match state.table.primary_of(state.me) {
        Some(_) => node.repl.link.offset(),
        None => node.repl.offset(),
    };
    offsets
}

/// `READONLY` (`on`) and `READWRITE`: accepted in cluster mode, on any
/// node, as in Redis. See the module docs.
pub(crate) fn readonly(_: &Args, node: &Node, client: &Client, out: &mut Encoder, on: bool) -> Flow {
    if enabled(node, out).is_some() {
        client.readonly.set(on);
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

/// `CLUSTER COUNTKEYSINSLOT slot`: from the store's slot index, O(1); 0
/// for a slot copied here and not served yet, as in Redis 8.
fn count_keys_in_slot(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_none() {
        return Flow::Continue;
    }
    match parse_i64(&args[2]) {
        None => store_error(store::Error::NotInteger, out),
        Some(slot) if !(0..SLOTS as i64).contains(&slot) => out.error(b"ERR Invalid slot"),
        Some(slot) => {
            let store = node.store.borrow();
            let n = if store.is_held(slot as u16) { 0 } else { store.count_in_slot(slot as u16) };
            out.integer(n as i64)
        }
    }
    Flow::Continue
}

/// `CLUSTER GETKEYSINSLOT slot count`: up to `count` keys of the slot, in
/// no particular order (expired ones not reclaimed yet included, as Redis
/// lists them; none of a slot copied here and not served yet). Both
/// numbers are read before either is checked, as in Redis.
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
            let held = store.is_held(slot as u16);
            let n = if held { 0 } else { store.count_in_slot(slot as u16).min(usize::try_from(count).unwrap_or(usize::MAX)) };
            out.array(n);
            for key in store.keys_in_slot(slot as u16).take(n) {
                out.bulk(key);
            }
        }
    }
    Flow::Continue
}

/// `CLUSTER SETTABLE epoch NODES n [id host port]... RANGES m [start end
/// id|-]... [MIGRATING k [slot from to]...] [SYNCING k [slot from to]...]
/// [REPLICAS r [id primary]...] [FAILED f [id]...] [SHARDS s [id shard]...]`:
/// the whole table, from
/// stuart. `MIGRATING` moves a slot key by key (Redis's `SETSLOT` states,
/// see the module docs), `SYNCING` by snapshot (see
/// [`Migration`](::cluster::Migration)). The ranges must name
/// every slot once (`-`: unassigned), and the nodes must include this one.
/// A migration names the slot's owner and the node it moves to; a
/// replica, its primary (see [`SlotTable::with_replicas`]); the nodes
/// marked failed, stuart found down ([`SlotTable::with_failed`]); the
/// shards, if not the default ones ([`SlotTable::with_shards`]).
fn set_table(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    match parse_table(args) {
        Ok(table) => match c.install(table) {
            Ok(()) => {
                node.repl.set_lease(c.state.borrow().lease.clone());
                let sent = node.repl.cut_over(&c.state.borrow().table, &node.store.borrow());
                c.stats.keys_sent.set(c.stats.keys_sent.get() + sent);
                crate::repl::slots::wake(node);
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

/// `CLUSTER SETOFFSETS [id offset]...` (ours): the nodes' replication
/// offsets, from stuart, replacing those it sent before. Not versioned: a
/// late push only shows offsets a second older.
fn set_offsets(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    if !args.len().is_multiple_of(2) {
        syntax_error(out);
        return Flow::Continue;
    }
    let mut offsets = HashMap::with_capacity(args.len() / 2 - 1);
    for i in (2..args.len()).step_by(2) {
        let Some(id) = NodeId::parse(&args[i]) else {
            out.error(format!("ERR Invalid node id '{}'", String::from_utf8_lossy(&args[i])).as_bytes());
            return Flow::Continue;
        };
        let Some(offset) = parse_i64(&args[i + 1]).filter(|&o| o >= 0) else {
            out.error(format!("ERR Invalid offset '{}'", String::from_utf8_lossy(&args[i + 1])).as_bytes());
            return Flow::Continue;
        };
        offsets.insert(id, offset as u64);
    }
    *c.offsets.borrow_mut() = offsets;
    out.ok();
    Flow::Continue
}

/// `CLUSTER FENCE seconds` (ours): stuart failing this replica's primary
/// over. The replica acknowledges nothing for `seconds` (0: again from now
/// on), so the primary's lease runs out, and says where it stands: `[offset,
/// 1 if its copy is complete, else 0]`. A fence ends by itself (a stuart
/// that died meanwhile), or with a table that makes the node a primary.
fn fence(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    if c.primary().is_none() {
        out.error(b"ERR This node is not a replica");
        return Flow::Continue;
    }
    let Some(secs) = parse_i64(&args[2]).filter(|s| (0..=3600).contains(s)) else {
        out.error(format!("ERR Invalid seconds '{}'", String::from_utf8_lossy(&args[2])).as_bytes());
        return Flow::Continue;
    };
    let link = &node.repl.link;
    link.fence(std::time::Duration::from_secs(secs as u64));
    if secs > 0 {
        notice!("Fenced for {secs} s: no acknowledgements to the primary");
    }
    out.array(2);
    out.integer(link.offset() as i64);
    out.integer(i64::from(link.synced() && !link.loading()));
    Flow::Continue
}

/// `CLUSTER PAUSESLOTS ms target slot...` (ours): stuart about to give
/// `slots`, moving by snapshot from this node to `target`, to it. Their
/// writes wait for `ms` (at most 10 s; 0 ends the pause), or until a table
/// ends the move (see the module docs); reads go on. The reply is the
/// offset of the target's sync of exactly those slots: once the target
/// has it, it has every write to them. `-BEHIND n` while the target has
/// more than [`PAUSE_LAG`] bytes to acknowledge (nothing is paused).
fn pause_slots(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    let Some(ms) = parse_i64(&args[2]).filter(|&ms| ms >= 0 && ms as u128 <= PAUSE_MAX.as_millis()) else {
        out.error(format!("ERR Invalid milliseconds '{}'", String::from_utf8_lossy(&args[2])).as_bytes());
        return Flow::Continue;
    };
    let Some(target) = NodeId::parse(&args[3]) else {
        out.error_parts(&[b"ERR Invalid node id '", &args[3], b"'"]);
        return Flow::Continue;
    };
    let mut slots = Vec::with_capacity(args.len() - 4);
    for arg in (4..args.len()).map(|i| &args[i]) {
        match parse_i64(arg).filter(|s| (0..SLOTS as i64).contains(s)) {
            Some(s) => slots.push(s as u16),
            None => {
                out.error_parts(&[b"ERR Invalid slot '", arg, b"'"]);
                return Flow::Continue;
            }
        }
    }
    slots.sort_unstable();
    slots.dedup();
    {
        let (table, me) = c.table();
        let to = table.position(&target);
        if let Some(&slot) = slots.iter().find(|&&s| !table.migration(s).is_some_and(|m| m.snapshot && m.from == me && Some(m.to) == to)) {
            out.error(format!("ERR The table doesn't move slot {slot} from this node to {target}").as_bytes());
            return Flow::Continue;
        }
    }
    let mut pauses = c.pauses.borrow_mut();
    pauses.retain(|p| {
        if p.target == target {
            p.ended("stuart");
        }
        p.target != target
    });
    if ms == 0 {
        out.ok();
        return Flow::Continue;
    }
    let Some(feed) = node.repl.slot_feed(&target, &slots) else {
        out.error(format!("ERR {target} doesn't sync those slots from here").as_bytes());
        return Flow::Continue;
    };
    let Some((offset, lag)) = feed.fed() else {
        out.error(format!("ERR {target} is still copying those slots").as_bytes());
        return Flow::Continue;
    };
    if lag > PAUSE_LAG {
        out.error(format!("BEHIND {lag}").as_bytes());
        return Flow::Continue;
    }
    let since = Instant::now();
    pauses.push(Pause { target, slots, since, until: since + Duration::from_millis(ms as u64) });
    verbose!("Writes to {} slots moving to {target} paused for {ms} ms, at offset {offset}", pauses.last().unwrap().slots.len());
    out.integer(offset as i64);
    Flow::Continue
}

/// `CLUSTER PAUSEWRITES ms` (ours): stuart about to hand this primary's
/// slots to its replica. Every write waits for `ms` (at most 10 s; 0 ends
/// the pause; a new pause replaces the one before), or until a table leaves
/// the node no slots (see the module docs); reads go on. The reply is the
/// node's replication offset: once the replica has it, it has every write.
fn pause_writes(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let Some(c) = enabled(node, out) else { return Flow::Continue };
    let Some(ms) = parse_i64(&args[2]).filter(|&ms| ms >= 0 && ms as u128 <= PAUSE_MAX.as_millis()) else {
        out.error(format!("ERR Invalid milliseconds '{}'", String::from_utf8_lossy(&args[2])).as_bytes());
        return Flow::Continue;
    };
    if c.primary().is_some() {
        out.error(b"ERR This node is not a primary");
        return Flow::Continue;
    }
    if let Some(p) = c.paused.borrow_mut().take() {
        p.ended("stuart");
    }
    if ms == 0 {
        out.ok();
        return Flow::Continue;
    }
    let since = Instant::now();
    *c.paused.borrow_mut() = Some(WritePause { since, until: since + Duration::from_millis(ms as u64) });
    let offset = node.repl.offset();
    verbose!("Writes paused for {ms} ms, at offset {offset}");
    out.integer(offset as i64);
    Flow::Continue
}

/// `CLUSTER SLOTSYNCS` (ours): the slots this node copies from others
/// (moving here by snapshot), a line per link: `source state slots keys
/// offset`, the state `connecting`, `copying`, `streaming`, `full` (waiting
/// for room under `maxmemory`) or `landing`
/// (the table gives this node the slots, their last writes on the way),
/// the slots as ranges (`0-99,150`), the keys copied since the link last
/// started, and the offset applied since its copy.
fn slot_syncs(_: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    if enabled(node, out).is_none() {
        return Flow::Continue;
    }
    let mut text = String::new();
    for s in node.repl.slot_syncs.borrow().iter() {
        let mut ranges: Vec<String> = Vec::new();
        let mut i = 0;
        while i < s.slots.len() {
            let mut j = i;
            while j + 1 < s.slots.len() && s.slots[j + 1] == s.slots[j] + 1 {
                j += 1;
            }
            ranges.push(if i == j { s.slots[i].to_string() } else { format!("{}-{}", s.slots[i], s.slots[j]) });
            i = j + 1;
        }
        text += &format!("{} {} {} {} {}\n", s.source, s.state().name(), ranges.join(","), s.keys(), s.offset());
    }
    out.verbatim(b"txt", text.as_bytes());
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
    // `KEYWORD count` at `i`, then `count` groups of `width` arguments.
    let section = |i: usize, keyword: &[u8], width: usize| -> Result<usize, TableArgError> {
        match (args.get(i), args.get(i + 1)) {
            (Some(word), Some(count)) if word.eq_ignore_ascii_case(keyword) => match parse_i64(count) {
                Some(n) if n >= 0 && (n as usize) <= (args.len() - i - 2) / width => Ok(n as usize),
                _ => Err(TableArgError::Syntax),
            },
            _ => Err(TableArgError::Syntax),
        }
    };
    let optional = |i: usize, keyword: &[u8]| args.get(i).is_some_and(|w| w.eq_ignore_ascii_case(keyword));

    // The shape first, then the values.
    let n = section(3, b"nodes", 3)?;
    let r = 5 + 3 * n;
    let m = section(r, b"ranges", 3)?;
    let g = r + 2 + 3 * m;
    let k = if optional(g, b"migrating") { section(g, b"migrating", 3)? } else { 0 };
    let y = if optional(g, b"migrating") { g + 2 + 3 * k } else { g };
    let w = if optional(y, b"syncing") { section(y, b"syncing", 3)? } else { 0 };
    let p = if optional(y, b"syncing") { y + 2 + 3 * w } else { y };
    let q = if optional(p, b"replicas") { section(p, b"replicas", 2)? } else { 0 };
    let f = if optional(p, b"replicas") { p + 2 + 2 * q } else { p };
    let z = if optional(f, b"failed") { section(f, b"failed", 1)? } else { 0 };
    let h = if optional(f, b"failed") { f + 2 + z } else { f };
    let s = if optional(h, b"shards") { section(h, b"shards", 2)? } else { 0 };
    let end = if optional(h, b"shards") { h + 2 + 2 * s } else { h };
    if args.len() != end {
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
    let mut syncs = Vec::with_capacity(w);
    for j in 0..w {
        let i = y + 2 + 3 * j;
        let slot = number("slot", &args[i], i64::from(u16::MAX))? as u16;
        syncs.push((slot, id(&args[i + 1])?, id(&args[i + 2])?));
    }
    let mut replicas = Vec::with_capacity(q);
    for j in 0..q {
        let i = p + 2 + 2 * j;
        replicas.push((id(&args[i])?, id(&args[i + 1])?));
    }
    let failed = (0..z).map(|j| id(&args[f + 2 + j])).collect::<Result<Vec<_>, _>>()?;
    let shards = (0..s).map(|j| Ok((id(&args[h + 2 + 2 * j])?, id(&args[h + 3 + 2 * j])?))).collect::<Result<Vec<_>, _>>()?;
    SlotTable::new(epoch, nodes, &ranges)
        .and_then(|t| t.with_replicas(&replicas))
        .and_then(|t| t.with_migrations(&migrations))
        .and_then(|t| t.with_syncs(&syncs))
        .and_then(|t| t.with_failed(&failed))
        .and_then(|t| if shards.is_empty() { Ok(t) } else { t.with_shards(&shards) })
        .map_err(|e| TableArgError::Invalid(format!("ERR Invalid table: {e}")))
}
