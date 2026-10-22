//! Moving slots (phase 5): one fiber that ends the moves whose source has
//! nothing of the slot left, and starts the queued ones, one table at a
//! time ([`State::advance`](crate::state::State::advance)).
//!
//! A move ends once both its nodes took the current table (so the source
//! sends new keys of the slot to the target) and the source's `CLUSTER
//! PENDING slot` is 0: every key is at the target. A queued slot starts
//! moving when its source and target are up, the source moves fewer slots
//! than `--moves-per-source` and all fewer than `--max-moves`, and neither
//! node is the other end of a move under way (a node is a source or a
//! target at a time, so "targets first" orders every table). Moves to a
//! target that is down don't count against the limits: they can't end
//! until it's back, and the others go on meanwhile. Nothing starts moving
//! to a full node (90% of its `maxmemory`); while queued moves only wait
//! for full nodes, the fiber plans again every second (`--auto` gives
//! their share to the others).
//!
//! The fiber also removes the leaving nodes that hold nothing any more
//! ([`Stuart::retire`]), and plans again every second while nothing moves
//! ([`Stuart::replan`]).
//!
//! What the nodes do in between, moving the keys, is theirs: see the
//! dbnode's `migrate` module.

use crate::keeper::{self, Link, Reply};
use crate::Stuart;
use cluster::{NodeId, SlotTable};
use dbnode::{notice, verbose, warning};
use reactor::sleep;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// How often the fiber looks while slots move, and while none does.
const BUSY: Duration = Duration::from_millis(2);
const IDLE: Duration = Duration::from_millis(50);
/// How often it plans again while nothing moves.
const REPLAN: Duration = Duration::from_secs(1);

pub(crate) fn mover(stuart: Rc<Stuart>) {
    // Connections to the sources, for CLUSTER PENDING.
    let mut links: HashMap<NodeId, Link> = HashMap::new();
    let mut busy = false;
    // Since the last time nothing moved: slots moved, and since when.
    let (mut moved, mut since) = (0, Instant::now());
    let mut planned = Instant::now();
    loop {
        sleep(if busy { BUSY } else { IDLE });
        stuart.retire();
        let Some(table) = stuart.state.borrow().table().cloned() else { continue };
        let was_busy = busy;
        busy = !table.migrations().is_empty() || !stuart.state.borrow().queue().is_empty();
        if !busy {
            if was_busy {
                notice!("Moves done: {moved} slots moved in {:.1} s", since.elapsed().as_secs_f64());
            }
            (moved, since) = (0, Instant::now());
            links.clear();
            if planned.elapsed() >= REPLAN {
                stuart.replan();
                planned = Instant::now();
            }
            continue;
        }
        let done = finished(&stuart, &table, &mut links);
        let start = startable(&stuart, &table, &done);
        if done.is_empty() && start.is_empty() {
            let blocked = stuart.state.borrow().queue().iter().any(|(_, to)| stuart.is_up(to) && !stuart.may_receive(to));
            if blocked && planned.elapsed() >= REPLAN {
                stuart.replan();
                planned = Instant::now();
            }
            continue;
        }
        match stuart.change(|s| s.advance(&done, &start)) {
            Ok(()) => {
                let epoch = stuart.state.borrow().table().map_or(0, |t| t.epoch());
                moved += done.len();
                stuart.slots_moved.set(stuart.slots_moved.get() + done.len() as u64);
                for slot in &done {
                    let m = table.migration(*slot).expect("a migration");
                    verbose!("Slot {slot} moved to {}; table epoch {epoch}", table.nodes()[m.to].id());
                    stuart.pending.borrow_mut().remove(slot);
                    stuart.moving_since.borrow_mut().remove(slot);
                }
                for slot in &start {
                    let to = stuart.state.borrow().table().and_then(|t| t.migration(*slot).map(|m| *t.nodes()[m.to].id()));
                    if let Some(to) = to {
                        verbose!("Slot {slot} starts moving to {to}; table epoch {epoch}");
                        stuart.moving_since.borrow_mut().insert(*slot, Instant::now());
                    }
                }
            }
            Err(e) => warning!("Can't save the state: {e}; slots wait to move"),
        }
    }
}

/// The moves of `table` that can end: both nodes took it, nothing pending.
fn finished(stuart: &Stuart, table: &SlotTable, links: &mut HashMap<NodeId, Link>) -> Vec<u16> {
    let mut done = Vec::new();
    for m in table.migrations() {
        let (from, to) = (&table.nodes()[m.from], &table.nodes()[m.to]);
        if !(stuart.has_taken(from.id(), table.epoch()) && stuart.has_taken(to.id(), table.epoch())) {
            continue;
        }
        match pending(links, from, m.slot) {
            Ok(n) => {
                stuart.pending.borrow_mut().insert(m.slot, n);
                if n == 0 {
                    done.push(m.slot);
                }
            }
            Err(e) => {
                links.remove(from.id());
                warning!("Can't ask {} about slot {}: {e}", from.id(), m.slot);
            }
        }
    }
    done
}

/// `CLUSTER PENDING slot` at `node`, over a connection kept for it.
fn pending(links: &mut HashMap<NodeId, Link>, node: &cluster::Node, slot: u16) -> io::Result<u64> {
    let addr = (node.host().to_string(), node.port());
    if links.get(node.id()).is_some_and(|l| l.to != addr) {
        links.remove(node.id());
    }
    if !links.contains_key(node.id()) {
        links.insert(*node.id(), keeper::connect(node)?);
    }
    let link = links.get_mut(node.id()).unwrap();
    match keeper::call(link, &[b"CLUSTER", b"PENDING", slot.to_string().as_bytes()])? {
        Reply::Integer(n) if n >= 0 => Ok(n as u64),
        other => Err(io::Error::new(io::ErrorKind::InvalidData, format!("CLUSTER PENDING: {other:?}"))),
    }
}

/// The queued slots that can start moving with the next table, given that
/// the moves of `done` end in it.
fn startable(stuart: &Stuart, table: &SlotTable, done: &[u16]) -> Vec<u16> {
    let state = stuart.state.borrow();
    // The moves under way after this table: sources and targets.
    let mut sources: HashMap<NodeId, usize> = HashMap::new();
    let mut targets: Vec<NodeId> = Vec::new();
    for m in table.migrations() {
        // A move ending still makes its target take the table first.
        let to = *table.nodes()[m.to].id();
        targets.push(to);
        if !done.contains(&m.slot) && stuart.is_up(&to) {
            *sources.entry(*table.nodes()[m.from].id()).or_default() += 1;
        }
    }
    let mut start = Vec::new();
    let mut moving: usize = sources.values().sum();
    for (slot, to) in state.queue() {
        if moving >= stuart.moves.total.max(1) {
            break;
        }
        let Some(from) = table.owner(*slot).map(|n| *n.id()) else { continue };
        if !(stuart.is_up(&from) && stuart.is_up(to) && stuart.may_receive(to)) {
            continue;
        }
        let busy_source = sources.get(&from).copied().unwrap_or(0) >= stuart.moves.per_source.max(1);
        if busy_source || targets.contains(&from) || sources.contains_key(to) {
            continue;
        }
        *sources.entry(from).or_default() += 1;
        targets.push(*to);
        start.push(*slot);
        moving += 1;
    }
    start
}
