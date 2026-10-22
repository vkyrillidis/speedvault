//! stuart, the steward of a cluster of dbnodes, on one reactor.
//!
//! Nodes started with `--stuart` join it (`STUART JOIN`); `STUART CREATE`
//! splits the slots over them; a keeper fiber per node pushes the table to
//! its node (`CLUSTER SETTABLE`) until the node has it, and pings it every
//! second. stuart is never in the data path: clients may ask it for the
//! layout (`CLUSTER SLOTS`...) and are redirected to the nodes for keys.
//!
//! The state (the nodes and the table) is written to the state file before
//! stuart acts on it ([`Stuart::change`]), so a restarted stuart carries on
//! where it stopped: it reads the file, and its keepers push the table
//! again.
//!
//! `STUART MOVE` queues slots to move to a node; the mover fiber ([`mover`])
//! starts and ends the moves, a table at a time, and the nodes move the
//! keys between themselves (phase 5). With `--auto` stuart plans the moves
//! itself whenever the nodes change (phase 6): a node that joins gets its
//! share, a node leaving (`STUART LEAVE`) is drained, then removed.
//!
//! The keepers also read each node's memory (`INFO memory`, every second).
//! A node at 90% of its `maxmemory` or more is full: no slot moves to it
//! (the plans leave it out, queued moves to it wait), and a node may leave
//! only if the others have room below 90% for its keys.
//!
//! With `--metrics`, stuart serves `/metrics` and the probes over HTTP
//! ([`metrics`]).
//!
//! On SIGTERM stuart serves on a few seconds for the nodes stopping with
//! it ([`linger`]).

mod conn;
mod keeper;
mod metrics;
mod mover;
pub mod state;

use cluster::NodeId;
use dbnode::{notice, warning};
use reactor::{sleep, spawn, Reactor, TcpListener};
use state::State;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::net;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// How stuart runs.
#[derive(Clone, Debug)]
pub struct Config {
    /// The state file (`--state`).
    pub state: PathBuf,
    /// `--create-when N`: `STUART CREATE` by itself once N nodes have
    /// joined, if there's no table yet (for Docker Compose and Kubernetes,
    /// where nobody types it). `None`: only `STUART CREATE` makes it.
    pub create_when: Option<usize>,
    /// How many slots move at once.
    pub moves: Moves,
    /// `--auto`: keep the slots even over the nodes by itself (see
    /// [`Stuart::replan`]), and make the table at the first join unless
    /// `create_when` says otherwise.
    pub auto: bool,
}

/// How many slots move at once: out of one node (`--moves-per-source`)
/// and in all (`--max-moves`). A node moves its migrating slots' keys one
/// slot after the other, but with more of them migrating its next slot is
/// ready while stuart ends the last one (a table can end and start several
/// moves): REBALANCE of 1M keys onto a fourth node took 3.4 s with 8 per
/// source, 1.8 s with 32, 1.6 s with 64 (tools/bench/migrate_bench.py).
/// Every table carries the moves under way, hence a limit in all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Moves {
    pub per_source: usize,
    pub total: usize,
}

impl Default for Moves {
    fn default() -> Moves {
        Moves { per_source: 32, total: 128 }
    }
}

/// What the fibers share.
pub(crate) struct Stuart {
    pub(crate) state: RefCell<State>,
    path: PathBuf,
    /// Per joined node, what its keeper last saw.
    pub(crate) health: RefCell<HashMap<NodeId, Health>>,
    pub(crate) port: u16,
    pub(crate) started: Instant,
    /// For `HELLO`'s `id`.
    pub(crate) next_client_id: Cell<u64>,
    pub(crate) create_when: Option<usize>,
    /// Keeper fibers running (`INFO`'s `stuart_keepers`): one per node.
    pub(crate) keepers: Cell<usize>,
    /// Per slot moving, what its source last said was pending, and since
    /// when it moves (`STUART MIGRATIONS`; lost on a restart).
    pub(crate) pending: RefCell<HashMap<u16, u64>>,
    pub(crate) moving_since: RefCell<HashMap<u16, Instant>>,
    pub(crate) moves: Moves,
    pub(crate) auto: bool,
    /// Per leaving node that holds nothing any more, the table epoch at
    /// which stuart first saw that: it is removed once it took that table.
    emptied: RefCell<HashMap<NodeId, u64>>,
    /// Slots whose move ended since stuart started.
    pub(crate) slots_moved: Cell<u64>,
}

/// How long a node that must take a table first may be down before the
/// others get the table anyway. The table is for it first so that clients
/// aren't sent back and forth between it and the source; while it's down
/// its slots can't be served either way, and the other moves go on.
const FIRST_WAIT: Duration = Duration::from_secs(2);

/// Past this share of its `maxmemory` a node is full: nothing moves to it.
/// The rest is room for writes while the moves under way end.
const FULL_PERCENT: u64 = 90;

/// A node's memory, from its `INFO memory`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Memory {
    pub(crate) used: u64,
    /// 0: no limit.
    pub(crate) max: u64,
    /// What its keys take: `used_memory_dataset`.
    pub(crate) dataset: u64,
    /// What its next new key would allocate at once (`mem_map_growth`):
    /// counted as used, since past `maxmemory` with it the node refuses
    /// new keys, and keys moving in.
    pub(crate) growth: u64,
}

impl Memory {
    /// The `INFO memory` text's fields; `None` without `used_memory`.
    pub(crate) fn parse(text: &str) -> Option<Memory> {
        let field = |name: &str| text.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix(':')?.trim().parse::<u64>().ok());
        let used = field("used_memory")?;
        let (max, dataset, growth) = (field("maxmemory").unwrap_or(0), field("used_memory_dataset").unwrap_or(used), field("mem_map_growth").unwrap_or(0));
        Some(Memory { used, max, dataset, growth })
    }

    /// Used, with the growth that the next new key needs.
    fn needed(&self) -> u64 {
        self.used.saturating_add(self.growth)
    }

    pub(crate) fn full(&self) -> bool {
        self.max != 0 && self.needed().saturating_mul(100) >= self.max.saturating_mul(FULL_PERCENT)
    }

    /// The bytes it can take before it is full; `None`: no limit.
    pub(crate) fn room(&self) -> Option<u64> {
        (self.max != 0).then(|| (self.max / 100 * FULL_PERCENT).saturating_sub(self.needed()))
    }
}

/// A node's last known condition.
#[derive(Clone, Debug)]
pub(crate) struct Health {
    /// Whether the last exchange with the node worked.
    pub(crate) up: bool,
    /// Since when `up` holds.
    pub(crate) since: Instant,
    /// The newest table epoch the node took.
    pub(crate) acked: u64,
    /// Why the node is down, or refused the table.
    pub(crate) error: Option<String>,
    /// Its memory, as last read (kept while it's down); `None` before
    /// that, or from a node that doesn't say.
    pub(crate) memory: Option<Memory>,
    /// Whether it was asked for its memory since stuart started: until
    /// then nothing moves to it.
    pub(crate) asked: bool,
}

impl Stuart {
    /// Applies `f` to a copy of the state and saves the copy before it
    /// becomes the state: nothing is acted on (replied, pushed) that the
    /// file doesn't hold. If saving fails, nothing changes.
    pub(crate) fn change<R>(&self, f: impl FnOnce(&mut State) -> R) -> io::Result<R> {
        let mut next = self.state.borrow().clone();
        let result = f(&mut next);
        if next != *self.state.borrow() {
            next.save(&self.path)?;
            *self.state.borrow_mut() = next;
        }
        Ok(result)
    }

    /// With `--create-when N`: makes the table once N nodes have joined and
    /// none exists. Once there is a table, never again.
    pub(crate) fn auto_create(&self) {
        let Some(n) = self.create_when.or(self.auto.then_some(1)) else { return };
        let joined = self.state.borrow().nodes().len();
        if joined < n || self.state.borrow().table().is_some() {
            return;
        }
        let why = if self.create_when.is_some() { format!("--create-when {n}") } else { "--auto".to_string() };
        match self.change(|s| s.create()) {
            Ok(Ok(())) => notice!("Created the slot table: 16384 slots over {joined} nodes, epoch 1 ({why})"),
            Ok(Err(e)) => warning!("{why}: {e}"),
            Err(e) => warning!("{why}: can't save the state: {e}"),
        }
    }

    /// After the nodes changed, and every second while nothing moves: with
    /// `--auto`, hands out unassigned slots and plans the moves again (as
    /// `STUART REBALANCE`); without, drains again the leaving nodes that
    /// got slots meanwhile (moves that were under way to them).
    pub(crate) fn replan(&self) {
        if self.state.borrow().table().is_none() {
            return;
        }
        let before = self.state.borrow().queue().to_vec();
        let auto = self.auto;
        let full = self.full_nodes();
        match self.change(|s| if auto { (s.assign_unassigned(), s.rebalance(&full).unwrap_or(0)) } else { (0, s.drain_leaving(&full)) }) {
            Ok((assigned, _)) => {
                let state = self.state.borrow();
                let epoch = state.table().map_or(0, |t| t.epoch());
                if assigned > 0 {
                    notice!("Assigned {assigned} unassigned slots (their keys are gone); table epoch {epoch}");
                }
                if state.queue() != before {
                    notice!("Planned the moves: {} slots to move", state.queue().len());
                }
            }
            Err(e) => warning!("Can't save the state: {e}; the moves wait"),
        }
    }

    /// Removes the leaving nodes that hold nothing any more, once they took
    /// a table in which they hold nothing (so that they redirect clients
    /// for the slots they had), or right away if they are down.
    pub(crate) fn retire(&self) {
        let (epoch, empty) = {
            let state = self.state.borrow();
            let Some(t) = state.table() else { return };
            let empty: Vec<NodeId> = state.leaving().iter().filter(|id| state.holding(id) == 0).copied().collect();
            (t.epoch(), empty)
        };
        for id in empty {
            let since = *self.emptied.borrow_mut().entry(id).or_insert(epoch);
            if !(self.has_taken(&id, since) || !self.is_up(&id)) {
                continue;
            }
            match self.change(|s| s.remove_left(&id)) {
                Ok(true) => {
                    self.emptied.borrow_mut().remove(&id);
                    let epoch = self.state.borrow().table().map_or(0, |t| t.epoch());
                    notice!("Node {id} left the cluster; table epoch {epoch}");
                }
                Ok(false) => {}
                Err(e) => warning!("Can't save the state: {e}; node {id} leaves later"),
            }
        }
    }

    /// Whether node `id` took the table with `epoch` (or a newer one).
    pub(crate) fn has_taken(&self, id: &NodeId, epoch: u64) -> bool {
        self.health.borrow().get(id).is_some_and(|h| h.acked >= epoch)
    }

    pub(crate) fn is_up(&self, id: &NodeId) -> bool {
        self.health.borrow().get(id).is_some_and(|h| h.up)
    }

    /// Whether node `id` may get the table with `epoch` now: it is one of
    /// the nodes that get it first, or those took it (or left).
    pub(crate) fn may_push(&self, id: &NodeId, epoch: u64) -> bool {
        let state = self.state.borrow();
        let first = state.first();
        first.contains(id) || first.iter().all(|f| state.node(f).is_none() || self.has_taken(f, epoch) || self.down_for(f) >= FIRST_WAIT)
    }

    /// How long node `id` has been down (zero if it's up).
    pub(crate) fn down_for(&self, id: &NodeId) -> Duration {
        self.health.borrow().get(id).filter(|h| !h.up).map_or(Duration::ZERO, |h| h.since.elapsed())
    }

    /// Records the outcome of an exchange with node `id`, logging when it
    /// goes up or down.
    pub(crate) fn report(&self, id: &NodeId, up: bool, error: Option<String>) {
        let mut health = self.health.borrow_mut();
        let h = health.entry(*id).or_insert(Health { up: !up, since: Instant::now(), acked: 0, error: None, memory: None, asked: false });
        if h.up != up {
            let addr = self.state.borrow().node(id).map_or("?".to_string(), |n| format!("{}:{}", n.host(), n.port()));
            if up {
                notice!("Node {id} at {addr} is up");
            } else {
                warning!("Node {id} at {addr} is down: {}", error.as_deref().unwrap_or("no reason"));
            }
            h.up = up;
            h.since = Instant::now();
        }
        h.error = error;
    }
}

impl Stuart {
    /// Records node `id`'s memory, logging when it becomes full or has
    /// room again.
    /// A node that doesn't say (`None`) keeps what it last said.
    pub(crate) fn report_memory(&self, id: &NodeId, memory: Option<Memory>) {
        let mut health = self.health.borrow_mut();
        let Some(h) = health.get_mut(id) else { return };
        h.asked = true;
        let Some(m) = memory else { return };
        let (was, is) = (h.memory.is_some_and(|m| m.full()), m.full());
        h.memory = Some(m);
        if was != is {
            if is {
                warning!("Node {id} is full: {} of {} bytes of maxmemory used; no slots move to it", m.used, m.max);
            } else {
                notice!("Node {id} has room again: {} of {} bytes of maxmemory used", m.used, m.max);
            }
        }
    }

    /// The nodes that are full: the plans move nothing to them.
    pub(crate) fn full_nodes(&self) -> Vec<NodeId> {
        self.health.borrow().iter().filter(|(_, h)| h.memory.is_some_and(|m| m.full())).map(|(id, _)| *id).collect()
    }

    /// Whether node `id` may receive slots: stuart knows its memory, and it
    /// isn't full.
    pub(crate) fn may_receive(&self, id: &NodeId) -> bool {
        self.health.borrow().get(id).is_some_and(|h| h.asked && !h.memory.is_some_and(|m| m.full()))
    }

    /// Whether the nodes not leaving, other than `id`, have room (below
    /// 90% of their `maxmemory`) for `id`'s keys, so that it can leave or
    /// be drained; an error says how much is missing. Nodes without a
    /// limit, or that never said, have room for anything. Nothing to check
    /// if there are no others (the cluster is shutting down).
    pub(crate) fn room_for(&self, id: &NodeId) -> Result<(), String> {
        let others: Vec<NodeId> = {
            let state = self.state.borrow();
            state.nodes().iter().map(|n| *n.id()).filter(|n| n != id && !state.leaving().contains(n)).collect()
        };
        let health = self.health.borrow();
        let Some(need) = health.get(id).and_then(|h| h.memory).map(|m| m.dataset) else { return Ok(()) };
        if others.is_empty() || need == 0 {
            return Ok(());
        }
        let mut room = 0u64;
        for other in &others {
            match health.get(other).and_then(|h| h.memory) {
                Some(m) => match m.room() {
                    Some(r) => room += r,
                    None => return Ok(()),
                },
                None => return Ok(()),
            }
        }
        if need <= room {
            return Ok(());
        }
        Err(format!(
            "ERR Not enough memory for node {id}'s keys on the other nodes: they take {need} bytes, the others have {room} below {FULL_PERCENT}% of their maxmemory"
        ))
    }
}

/// Serves on `listener`, and `/metrics` and the probes on `metrics` if
/// given, until the reactor fails; never returns otherwise. Creates the
/// thread's reactor, so the thread must not have one.
pub fn serve(listener: net::TcpListener, metrics: Option<net::TcpListener>, config: Config) -> io::Result<()> {
    let state = State::load(&config.state)?;
    // Fail now, not at the first join, if the file can't be written.
    state.save(&config.state)?;
    let reactor = Reactor::new()?;
    reactor.set_panic_handler(|payload| {
        let message = payload.downcast_ref::<&str>().copied().or_else(|| payload.downcast_ref::<String>().map(String::as_str));
        warning!("A fiber panicked: {}", message.unwrap_or("(no message)"));
    });
    let epoch = state.table().map_or("none".to_string(), |t| t.epoch().to_string());
    notice!("State {}: {} nodes, table epoch {epoch}", config.state.display(), state.nodes().len());
    let ids: Vec<NodeId> = state.nodes().iter().map(|n| *n.id()).collect();
    let stuart = Rc::new(Stuart {
        state: RefCell::new(state),
        path: config.state,
        health: RefCell::new(HashMap::new()),
        port: listener.local_addr()?.port(),
        started: Instant::now(),
        next_client_id: Cell::new(1),
        create_when: config.create_when,
        keepers: Cell::new(0),
        pending: RefCell::new(HashMap::new()),
        moving_since: RefCell::new(HashMap::new()),
        moves: config.moves,
        auto: config.auto,
        emptied: RefCell::new(HashMap::new()),
        slots_moved: Cell::new(0),
    });
    for id in ids {
        let s = stuart.clone();
        reactor.spawn(move || keeper::keep(s, id))?;
    }
    let s = stuart.clone();
    reactor.spawn(move || mover::mover(s))?;
    let s = stuart.clone();
    reactor.spawn(move || linger(s))?;
    if let Some(metrics) = metrics {
        dbnode::http::spawn_server(metrics, stuart.clone())?;
    }
    // Enough nodes may have joined before a restart.
    stuart.auto_create();
    let listener = TcpListener::from_std(listener)?;
    reactor.spawn(move || accept_loop(listener, stuart))?;
    reactor.run()
}

/// Set by the first SIGTERM (stuart's `main`): stuart exits once the
/// nodes stopping with it had time to hear that they can go ([`linger`]).
pub static STOPPING: AtomicBool = AtomicBool::new(false);

/// After SIGTERM, how long stuart waits for a node to start leaving.
pub const LINGER_QUIET: Duration = Duration::from_secs(5);
/// And how long at most it serves on after SIGTERM.
pub const LINGER_MAX: Duration = Duration::from_secs(15);

/// Exits once [`STOPPING`] is set and the nodes stopping too were told to.
/// Deleting a Kubernetes namespace (or `kubectl delete -k`) sends SIGTERM
/// to stuart and every node at once; each node asks to leave once a second
/// and exits at stuart's `0`, which, once every node is leaving, comes at
/// once (a shutdown). Had stuart exited at once, the nodes would ask an
/// absent stuart until their drain timeout. So stuart serves on: until the
/// cluster has been shutting down for a second and a half (each node asked
/// again meanwhile), until no node is leaving [`LINGER_QUIET`] after the
/// signal (stuart alone is stopping: a restart, a reschedule), or for
/// [`LINGER_MAX`] at most (nodes left leaving wait for the next stuart, as
/// for one that died).
fn linger(stuart: Rc<Stuart>) {
    while !STOPPING.load(Ordering::Relaxed) {
        sleep(Duration::from_millis(100));
    }
    notice!(
        "Received SIGTERM: serving on for the nodes stopping too, at most {} s (another SIGTERM or SIGINT exits at once)",
        LINGER_MAX.as_secs()
    );
    let since = Instant::now();
    let mut shutting_since: Option<Instant> = None;
    loop {
        let (nodes, leaving, shutting) = {
            let state = stuart.state.borrow();
            (state.nodes().len(), state.leaving().len(), state.shutting_down())
        };
        let why = if nodes == 0 {
            Some("no nodes".to_string())
        } else if shutting && shutting_since.get_or_insert_with(Instant::now).elapsed() >= Duration::from_millis(1500) {
            Some(format!("every node is leaving ({nodes}) and was told to stop"))
        } else if leaving == 0 && since.elapsed() >= LINGER_QUIET {
            Some("no node is leaving".to_string())
        } else if since.elapsed() >= LINGER_MAX {
            Some(format!("{leaving} of {nodes} nodes still leaving; they wait for stuart to be back"))
        } else {
            None
        };
        if let Some(why) = why {
            notice!("Exiting: {why}");
            std::process::exit(0);
        }
        sleep(Duration::from_millis(100));
    }
}

fn accept_loop(listener: TcpListener, stuart: Rc<Stuart>) {
    loop {
        match listener.accept() {
            Ok((conn, _)) => {
                let _ = conn.set_nodelay(true);
                let stuart = stuart.clone();
                if let Err(e) = spawn(move || conn::handle(conn, &stuart)) {
                    warning!("No fiber for a new connection, closing it: {e}");
                }
            }
            Err(e) => {
                warning!("Accepting a connection failed: {e}");
                sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory() {
        let text = "# Memory\r\nused_memory:900\r\nused_memory_human:900B\r\nused_memory_dataset:700\r\nmaxmemory:1000\r\nmaxmemory_policy:noeviction\r\n";
        let m = Memory::parse(text).unwrap();
        assert_eq!(m, Memory { used: 900, max: 1000, dataset: 700, growth: 0 });
        assert!(m.full(), "90% is full");
        assert_eq!(m.room(), Some(0));
        let m = Memory { used: 899, max: 1000, dataset: 0, growth: 0 };
        assert!(!m.full());
        assert_eq!(m.room(), Some(1));
        // A map that must double for the next key: full below 90% used.
        let m = Memory::parse("used_memory:600
maxmemory:1000
mem_map_growth:300
").unwrap();
        assert_eq!(m.growth, 300);
        assert!(m.full());
        let m = Memory { used: 500, max: 1000, dataset: 0, growth: 300 };
        assert!(!m.full());
        assert_eq!(m.room(), Some(100));
        let unlimited = Memory::parse("used_memory:5\r\nmaxmemory:0\r\n").unwrap();
        assert!(!unlimited.full());
        assert_eq!(unlimited.room(), None);
        assert_eq!(unlimited.dataset, 5, "without the field: all of it");
        assert_eq!(Memory::parse("# Memory\r\n"), None);
        assert_eq!(Memory::parse("PONG"), None);
    }
}

