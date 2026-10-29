//! Handing a leaving primary's slots to its replica (phase 8): one fiber
//! that, for each primary leaving with a replica that can take over
//! ([`State::leave_handing`](crate::state::State::leave_handing): `STUART
//! LEAVE` from a primary stopping, SIGTERM), promotes that replica, without
//! moving a key.
//!
//! The primary is alive, so there's no lease to wait for: its writes are
//! paused (`CLUSTER PAUSEWRITES`, ours, for [`PAUSE`]; they wait, and it
//! answers its replication offset), the most advanced replica with a
//! complete copy is polled (`CLUSTER FENCE 0`, which fences nothing and
//! says where it stands) until it has that offset, for [`CATCH_UP`] at
//! most, and the table giving it the slots goes to it first
//! ([`State::hand_over`](crate::state::State::hand_over)). The old primary,
//! taking it, redirects the writes that waited; once it took it, it is
//! removed ([`Stuart::retire`]), and its `STUART LEAVE` gets 0. If stuart
//! dies meanwhile, the pause ends by itself; the replica, never promoted,
//! keeps following the primary.
//!
//! A replica that missed the offset is paused for again after
//! [`RETRY_MISSED`] (the pause lifted meanwhile); after [`TRIES`] misses, or
//! with no replica that can take over (up, not marked failed, not leaving),
//! the primary's slots are drained instead, as without replicas
//! ([`State::drain_instead`](crate::state::State::drain_instead)). A
//! replica still copying (syncing from scratch) is waited for, up to
//! [`COPYING_WAIT`]. A primary down is the failover fiber's.

use crate::failover;
use crate::keeper::{self, Link, Reply};
use crate::Stuart;
use cluster::{Node, NodeId};
use dbnode::{notice, warning};
use reactor::sleep;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// The pause of the primary's writes: it ends by itself if stuart doesn't
/// end it (agreed 2026-10-25).
const PAUSE: Duration = Duration::from_secs(10);
/// How long the replica has to reach the paused primary's offset.
const CATCH_UP: Duration = Duration::from_secs(1);
/// How often the replica is polled meanwhile.
const POLL: Duration = Duration::from_millis(2);
/// Pauses the replica missed before the slots are drained instead.
const TRIES: u32 = 5;
/// After a missed pause, before the next.
const RETRY_MISSED: Duration = Duration::from_millis(100);
/// How long a replica still copying is waited for before the slots are
/// drained instead (agreed 2026-10-25), and how often it's asked meanwhile.
const COPYING_WAIT: Duration = Duration::from_secs(60);
const COPYING_RETRY: Duration = Duration::from_millis(500);
const TICK: Duration = Duration::from_millis(20);

/// Per primary handing over: pauses missed, when it was first tried, and
/// when to try next.
struct Try {
    missed: u32,
    since: Instant,
    next: Instant,
    /// Logged once: the replica is still copying.
    copying: bool,
}

/// How one try went.
enum Outcome {
    Done,
    /// No replica has a complete copy yet.
    Copying,
    Missed(String),
    /// None can take over: drain.
    NoReplica(String),
}

pub(crate) fn handover(stuart: Rc<Stuart>) {
    let mut tries: HashMap<NodeId, Try> = HashMap::new();
    loop {
        sleep(TICK);
        let handing: Vec<NodeId> = {
            let state = stuart.state.borrow();
            if state.shutting_down() {
                continue;
            }
            state.handing().to_vec()
        };
        tries.retain(|p, _| handing.contains(p));
        for primary in handing {
            if !stuart.is_up(&primary) {
                continue;
            }
            let now = Instant::now();
            let t = tries.entry(primary).or_insert(Try { missed: 0, since: now, next: now, copying: false });
            if now < t.next {
                continue;
            }
            match hand(&stuart, &primary) {
                Outcome::Done => {
                    tries.remove(&primary);
                }
                Outcome::Copying if t.since.elapsed() >= COPYING_WAIT => {
                    drain(&stuart, &primary, &format!("its replicas are still copying after {} s", COPYING_WAIT.as_secs()));
                    tries.remove(&primary);
                }
                Outcome::Copying => {
                    if !t.copying {
                        notice!("Primary {primary} leaving: its replica is still copying; waiting up to {} s to hand over", COPYING_WAIT.as_secs());
                        t.copying = true;
                    }
                    t.next = Instant::now() + COPYING_RETRY;
                }
                Outcome::Missed(why) => {
                    t.missed += 1;
                    if t.missed >= TRIES {
                        drain(&stuart, &primary, &format!("{why} ({} times)", t.missed));
                        tries.remove(&primary);
                    } else {
                        warning!("Primary {primary} leaving: {why}; trying again");
                        t.next = Instant::now() + RETRY_MISSED;
                    }
                }
                Outcome::NoReplica(why) => {
                    drain(&stuart, &primary, &why);
                    tries.remove(&primary);
                }
            }
        }
    }
}

/// One try at handing `primary`'s slots over.
fn hand(stuart: &Stuart, primary: &NodeId) -> Outcome {
    let (node, candidates) = {
        let state = stuart.state.borrow();
        let Some(node) = state.node(primary).cloned() else { return Outcome::Done };
        let candidates: Vec<Node> = failover::candidates(&state, primary, |id| stuart.is_up(id), |id, epoch| stuart.has_taken(id, epoch))
            .into_iter()
            .filter(|r| !state.leaving().contains(r.id()))
            .collect();
        (node, candidates)
    };
    if candidates.is_empty() {
        return Outcome::NoReplica("no replica up, not marked failed nor leaving, that it took a table with".into());
    }
    let mut stands: Vec<(Node, u64, bool)> = Vec::new();
    for r in &candidates {
        match failover::fence(r, 0) {
            Ok((offset, complete)) => stands.push((r.clone(), offset, complete)),
            Err(e) => warning!("Can't ask replica {} of {primary} where it stands: {e}", r.id()),
        }
    }
    let Some((chosen, _)) = failover::choose(&stands) else {
        return if stands.is_empty() { Outcome::NoReplica("no replica answered".into()) } else { Outcome::Copying };
    };
    let mut p = match keeper::connect(&node) {
        Ok(link) => link,
        Err(e) => return Outcome::Missed(format!("can't reach it: {e}")),
    };
    let paused = Instant::now();
    let offset = match pause(&mut p, PAUSE) {
        Ok(offset) => offset,
        Err(e) => return Outcome::Missed(e.to_string()),
    };
    let reached = keeper::connect(&chosen).and_then(|mut r| {
        loop {
            let (at, complete) = failover::fence_on(&mut r, 0)?;
            if complete && at >= offset {
                return Ok(true);
            }
            if paused.elapsed() >= CATCH_UP {
                return Ok(false);
            }
            sleep(POLL);
        }
    });
    let resume = |p: &mut Link| {
        if let Err(e) = pause(p, Duration::ZERO) {
            warning!("Can't lift the pause of {primary}'s writes: {e}; it ends by itself");
        }
    };
    match reached {
        Ok(true) => {}
        Ok(false) => {
            resume(&mut p);
            return Outcome::Missed(format!("replica {} didn't reach offset {offset} within {} ms", chosen.id(), CATCH_UP.as_millis()));
        }
        Err(e) => {
            resume(&mut p);
            return Outcome::Missed(format!("replica {}: {e}", chosen.id()));
        }
    }
    let caught_up = paused.elapsed();
    match stuart.change(|s| s.hand_over(primary, chosen.id())) {
        Ok(true) => {
            let epoch = stuart.state.borrow().table().map_or(0, |t| t.epoch());
            notice!(
                "Primary {primary} leaving: handed its slots to replica {} (offset {offset}, caught up {} ms after the pause); table epoch {epoch}",
                chosen.id(),
                caught_up.as_millis()
            );
            stuart.handovers.set(stuart.handovers.get() + 1);
            Outcome::Done
        }
        Ok(false) => {
            resume(&mut p);
            Outcome::Missed(format!("{} is no longer its replica, or is leaving", chosen.id()))
        }
        Err(e) => {
            resume(&mut p);
            Outcome::Missed(format!("can't save the state: {e}"))
        }
    }
}

/// `CLUSTER PAUSEWRITES ms` at the primary on `link`: its offset (0 for a
/// pause lifted).
fn pause(link: &mut Link, pause: Duration) -> io::Result<u64> {
    match keeper::call(link, &[b"CLUSTER", b"PAUSEWRITES", pause.as_millis().to_string().as_bytes()])? {
        Reply::Integer(o) if o >= 0 => Ok(o as u64),
        Reply::Simple(_) if pause.is_zero() => Ok(0),
        other => Err(io::Error::other(format!("CLUSTER PAUSEWRITES: {other:?}"))),
    }
}

/// Gives the handover of `primary` up: its slots drain instead.
fn drain(stuart: &Stuart, primary: &NodeId, why: &str) {
    if let Err(e) = stuart.room_for(primary) {
        warning!("Primary {primary} leaving: draining it instead, but {e}");
    }
    let full = stuart.full_nodes();
    match stuart.change(|s| s.drain_instead(primary, &full)) {
        Ok(Ok(n)) => warning!("Primary {primary} leaving: can't hand its slots to a replica ({why}); draining them instead, {n} slots to move"),
        Ok(Err(e)) => warning!("Primary {primary} leaving: can't hand its slots to a replica ({why}), nor drain them: {e}"),
        Err(e) => warning!("Can't save the state: {e}; primary {primary} waits to be drained"),
    }
}
