//! Moving slots (phase 5; by snapshot since phase 8): one fiber that
//! ends the moves whose target has caught up, and starts the queued ones
//! ([`State::advance`](crate::state::State::advance)).
//!
//! A queued slot starts moving when its source and target are up, the
//! source moves fewer slots than `--moves-per-source` and all fewer than
//! `--max-moves`, and neither node is the other end of a move under way (a
//! node is a source or a target at a time). Nothing starts moving to a
//! full node (90% of its `maxmemory`); while queued moves only wait for
//! full nodes, the fiber plans again every second (`--auto` gives their
//! share to the others).
//!
//! The target copies the slots from the source by itself, and once its
//! link streams (`CLUSTER SLOTSYNCS` at the target, a line per link), the
//! fiber pauses the link's slots' writes at the source (`CLUSTER
//! PAUSESLOTS`, for [`PAUSE`]; it answers the link's offset), waits up to
//! [`CATCH_UP`] for the target to reach that offset, and saves and pushes
//! the table giving the target the slots, to the target first: with every
//! other link that caught up meanwhile ([`CUT_WINDOW`]) and the moves that
//! start, as each save takes milliseconds (an `fsync`). A target more than
//! 1 MiB behind isn't paused (`-BEHIND`): it is asked again later, however
//! long that takes. A move is called off and queued again
//! ([`State::cancel`]) if its source or target is down, or after [`TRIES`]
//! pauses the target didn't catch up within.
//!
//! The fiber also removes the leaving nodes that hold nothing any more
//! ([`Stuart::retire`]), marks the replicas down for a while failed and
//! clears the mark once they're back ([`Stuart::mark_failed`]), and plans
//! again every second while nothing moves ([`Stuart::replan`]).
//!
//! What the nodes do in between, moving the keys, is theirs: see the
//! dbnode's `repl::slots` module.

use crate::keeper::{self, Link, Reply};
use crate::state::State;
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
/// A cutover's pause of the slots' writes at the source: it ends by itself
/// if stuart doesn't end it (Redis 8 waits 10 s, but its source hands over
/// by itself; ours waits for stuart).
const PAUSE: Duration = Duration::from_secs(2);
/// How long the target has to reach the paused source's offset.
const CATCH_UP: Duration = Duration::from_secs(1);
/// The links cut over in one table: those that caught up within this of
/// the first one's pause (which holds writes meanwhile).
const CUT_WINDOW: Duration = Duration::from_millis(20);
/// Pauses the target didn't catch up within before the move is called off.
const TRIES: u32 = 5;
/// After a pause the target missed, and after `-BEHIND`, before the next.
const RETRY_MISSED: Duration = Duration::from_millis(100);
const RETRY_BEHIND: Duration = Duration::from_millis(50);

pub(crate) fn mover(stuart: Rc<Stuart>) {
    // Connections to the sources, for CLUSTER PAUSESLOTS, and to the
    // targets, for CLUSTER SLOTSYNCS.
    let mut links: HashMap<NodeId, Link> = HashMap::new();
    // Per link of a move by snapshot (its slots): pauses missed, and when
    // to try the next.
    let mut tries: HashMap<Vec<u16>, (u32, Instant)> = HashMap::new();
    let mut busy = false;
    // Since the last time nothing moved: slots moved, and since when.
    let (mut moved, mut since) = (0, Instant::now());
    let mut planned = Instant::now();
    loop {
        sleep(if busy { BUSY } else { IDLE });
        stuart.retire();
        stuart.demote();
        stuart.mark_failed();
        let Some(table) = stuart.state.borrow().table().cloned() else { continue };
        let was_busy = busy;
        busy = !table.migrations().is_empty() || !stuart.state.borrow().queue().is_empty();
        if !busy {
            if was_busy {
                notice!("Moves done: {moved} slots moved in {:.1} s", since.elapsed().as_secs_f64());
            }
            (moved, since) = (0, Instant::now());
            links.clear();
            tries.clear();
            if planned.elapsed() >= REPLAN {
                stuart.replan();
                planned = Instant::now();
            }
            continue;
        }
        // The links cut over end in the next table, with the moves that
        // start; moves called off in a table of their own.
        let cuts = match snapshots(&stuart, &table, &mut links, &mut tries) {
            Some(Ended::Cut(cuts)) => cuts,
            Some(Ended::Cancelled) => continue,
            None => Vec::new(),
        };
        let done: Vec<u16> = cuts.iter().flat_map(|c| c.slots.iter().copied()).collect();
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
                for c in &cuts {
                    let ms = c.paused.elapsed().as_millis();
                    verbose!("{} slots moved to {} by snapshot, {ms} ms after the pause; table epoch {epoch}", c.slots.len(), c.target);
                }
                for slot in &done {
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
            // The cuts' pauses lapse; the next turns try again.
            Err(e) => warning!("Can't save the state: {e}; slots wait to move"),
        }
    }
}

/// The connection kept for `node`, made if there's none.
fn link<'a>(stuart: &Stuart, links: &'a mut HashMap<NodeId, Link>, node: &cluster::Node) -> io::Result<&'a mut Link> {
    let addr = (node.host().to_string(), node.port());
    if links.get(node.id()).is_some_and(|l| l.to != addr) {
        links.remove(node.id());
    }
    if !links.contains_key(node.id()) {
        links.insert(*node.id(), keeper::connect(stuart, node)?);
    }
    Ok(links.get_mut(node.id()).unwrap())
}

/// What [`snapshots`] did.
enum Ended {
    /// These links caught up with their paused sources: the next table
    /// gives their slots to their targets.
    Cut(Vec<Cut>),
    Cancelled,
}

/// A link cut over: its slots, its target, when its source paused them.
struct Cut {
    slots: Vec<u16>,
    target: NodeId,
    paused: Instant,
}

/// A target's link pulling slots, as `CLUSTER SLOTSYNCS` says.
struct SyncLine {
    source: NodeId,
    streaming: bool,
    slots: Vec<u16>,
    offset: u64,
}

/// Parses `source state ranges keys offset` lines.
fn sync_lines(text: &str) -> Option<Vec<SyncLine>> {
    let mut lines = Vec::new();
    for line in text.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        let [source, state, ranges, _keys, offset] = words[..] else { return None };
        let mut slots = Vec::new();
        for range in ranges.split(',') {
            let (a, b) = range.split_once('-').unwrap_or((range, range));
            slots.extend(a.parse::<u16>().ok()?..=b.parse::<u16>().ok()?);
        }
        let source = NodeId::parse(source.as_bytes())?;
        lines.push(SyncLine { source, streaming: state == "streaming", slots, offset: offset.parse().ok()? });
    }
    Some(lines)
}

/// `CLUSTER SLOTSYNCS` at `node`.
fn slot_syncs(stuart: &Stuart, links: &mut HashMap<NodeId, Link>, node: &cluster::Node) -> io::Result<Vec<SyncLine>> {
    match keeper::call(link(stuart, links, node)?, &[b"CLUSTER", b"SLOTSYNCS"])? {
        Reply::Bulk(text) => sync_lines(&String::from_utf8_lossy(&text))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CLUSTER SLOTSYNCS: a bad line")),
        other => Err(io::Error::new(io::ErrorKind::InvalidData, format!("CLUSTER SLOTSYNCS: {other:?}"))),
    }
}

/// The moves of `table`: those whose source or target is down
/// are called off; else every link streaming that may be paused now is cut
/// over, while the first pause is younger than [`CUT_WINDOW`] (after the
/// first, a link has what is left of it to catch up, and missing that
/// isn't a try). A link that missed [`TRIES`] pauses is called off.
/// `None`: no new table.
fn snapshots(stuart: &Stuart, table: &SlotTable, links: &mut HashMap<NodeId, Link>, tries: &mut HashMap<Vec<u16>, (u32, Instant)>) -> Option<Ended> {
    let moves: Vec<_> = table.migrations().to_vec();
    tries.retain(|slots, _| slots.iter().all(|s| moves.iter().any(|m| m.slot == *s)));
    if moves.is_empty() {
        return None;
    }
    let id = |i: usize| *table.nodes()[i].id();
    let down: Vec<u16> = moves.iter().filter(|m| !stuart.is_up(&id(m.from)) || !stuart.is_up(&id(m.to))).map(|m| m.slot).collect();
    if !down.is_empty() {
        return cancel(stuart, &down, "their source or target is down");
    }
    let mut targets: Vec<usize> = moves.iter().map(|m| m.to).collect();
    targets.sort_unstable();
    targets.dedup();
    let mut cuts: Vec<Cut> = Vec::new();
    for to in targets {
        let target = &table.nodes()[to];
        if !stuart.has_taken(target.id(), table.epoch()) {
            continue;
        }
        let lines = match slot_syncs(stuart, links, target) {
            Ok(lines) => lines,
            Err(e) => {
                links.remove(target.id());
                warning!("Can't ask {} how its copies stand: {e}", target.id());
                continue;
            }
        };
        for line in lines.into_iter().filter(|l| l.streaming) {
            let Some(from) = table.position(&line.source) else { continue };
            let ours = line.slots.iter().all(|&s| moves.iter().any(|m| m.slot == s && m.from == from && m.to == to));
            if !ours || !stuart.has_taken(&line.source, table.epoch()) {
                continue;
            }
            let (missed, next) = tries.get(&line.slots).copied().unwrap_or((0, Instant::now()));
            if Instant::now() < next {
                continue;
            }
            let budget = match cuts.first().map(|c| c.paused.elapsed()) {
                None => CATCH_UP,
                Some(age) if age < CUT_WINDOW => CUT_WINDOW - age,
                Some(_) => break,
            };
            let source = table.nodes()[from].clone();
            match cut(stuart, links, &source, target, &line.slots, budget) {
                Ok(Some(paused)) => cuts.push(Cut { slots: line.slots, target: *target.id(), paused }),
                Ok(None) => {
                    tries.insert(line.slots, (missed, Instant::now() + RETRY_BEHIND));
                }
                Err(e) if budget < CATCH_UP => verbose!("Slots moving to {}: {e}; with a later table", target.id()),
                Err(e) => {
                    links.remove(source.id());
                    links.remove(target.id());
                    let missed = missed + 1;
                    if missed >= TRIES {
                        return cancel(stuart, &line.slots, &format!("{e} ({missed} times)"));
                    }
                    verbose!("Slots moving to {}: {e}; again later", target.id());
                    tries.insert(line.slots, (missed, Instant::now() + RETRY_MISSED));
                }
            }
        }
    }
    (!cuts.is_empty()).then_some(Ended::Cut(cuts))
}

/// Calls off the moves of `slots` (queued again).
fn cancel(stuart: &Stuart, slots: &[u16], why: &str) -> Option<Ended> {
    match stuart.change(|s: &mut State| s.cancel(slots)) {
        Ok(()) => {
            warning!("{} slots moving called off ({why}): queued again", slots.len());
            Some(Ended::Cancelled)
        }
        Err(e) => {
            warning!("Can't save the state: {e}; moves wait to be called off");
            None
        }
    }
}

/// One try at a cutover: `slots`' writes paused at `source`, and `target`
/// given `budget` (at most [`CATCH_UP`]) to reach the offset it answered. `Ok(Some(when the
/// pause began))`: it did, push the table now; `Ok(None)`: the target is
/// too far behind to pause (asked again later); `Err`: it didn't catch up
/// (the pause ended), or a call failed.
fn cut(stuart: &Stuart, links: &mut HashMap<NodeId, Link>, source: &cluster::Node, target: &cluster::Node, slots: &[u16], budget: Duration) -> io::Result<Option<Instant>> {
    let mut args: Vec<Vec<u8>> = vec![b"CLUSTER".to_vec(), b"PAUSESLOTS".to_vec(), PAUSE.as_millis().to_string().into_bytes(), target.id().as_bytes().to_vec()];
    args.extend(slots.iter().map(|s| s.to_string().into_bytes()));
    let args: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    let started = Instant::now();
    let offset = match keeper::call(link(stuart, links, source)?, &args)? {
        Reply::Integer(o) if o >= 0 => o as u64,
        Reply::Error(e) if e.starts_with("BEHIND ") => return Ok(None),
        other => return Err(io::Error::other(format!("CLUSTER PAUSESLOTS: {other:?}"))),
    };
    loop {
        let reached = slot_syncs(stuart, links, target)?.iter().any(|l| l.source == *source.id() && l.slots == slots && l.offset >= offset);
        if reached {
            return Ok(Some(started));
        }
        if started.elapsed() >= budget {
            break;
        }
        sleep(BUSY);
    }
    let mut end: Vec<&[u8]> = vec![b"CLUSTER", b"PAUSESLOTS", b"0", target.id().as_bytes()];
    let slot_args: Vec<Vec<u8>> = slots.iter().map(|s| s.to_string().into_bytes()).collect();
    end.extend(slot_args.iter().map(Vec::as_slice));
    let _ = keeper::call(link(stuart, links, source)?, &end);
    Err(io::Error::other(format!("the target didn't reach offset {offset} within {} ms", budget.as_millis())))
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
        // A primary handing its slots to a replica moves none meanwhile.
        if state.handing().contains(&from) || state.handing().contains(to) {
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
