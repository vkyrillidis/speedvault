//! Failing a primary over to its replica (phase 8): one fiber that, for a
//! primary down for `--fail-after` (or known dead: a pod that rejoined
//! under its name, see [`Stuart::dead`]), promotes its most advanced
//! replica.
//!
//! stuart can't tell a dead primary from one only it can't reach, so the
//! replica is fenced first (`CLUSTER FENCE`: it stops acknowledging), and
//! promoted only once the primary's lease has run out ([`LEASE`] after the
//! fence, plus [`MARGIN`]): a primary that was only cut off has stopped
//! writing by then (`-NOREPLICAS`). A dead one needs no wait. The fence
//! ends by itself after [`FENCE`] (a stuart that dies meanwhile leaves no
//! primary fenced for good).
//!
//! A replica can be promoted if it's up, isn't marked failed, its copy is
//! complete (it says so in its reply to the fence), and its primary has
//! taken the table that last cleared its mark
//! ([`State::promotable`](crate::state::State::promotable)): until then
//! the primary may be writing alone. Of those, the one with the highest
//! offset. If the primary answers again before the promotion, the fences
//! are lifted and nothing changes.

use crate::keeper::{self, Reply};
use crate::Stuart;
use cluster::NodeId;
use dbnode::{notice, warning};
use reactor::sleep;
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// A primary's lease: the dbnode's `repl::LEASE`.
pub(crate) const LEASE: Duration = Duration::from_secs(3);
/// Waited past the lease: the time a fence reply took to get here, and
/// clocks running a little apart.
const MARGIN: Duration = Duration::from_millis(200);
/// How long a fence lasts unless lifted: the lease, the margin, and time to
/// push the promotion.
const FENCE: u64 = 10;
const TICK: Duration = Duration::from_millis(100);

pub(crate) fn failover(stuart: Rc<Stuart>) {
    // Logged once per primary that can't be failed over yet.
    let mut stuck: Vec<NodeId> = Vec::new();
    loop {
        sleep(TICK);
        let due: Vec<(NodeId, bool)> = {
            let state = stuart.state.borrow();
            let Some(t) = state.table() else { continue };
            let dead = stuart.dead.borrow();
            (0..t.nodes().len())
                .filter(|&i| t.primary_of(i).is_none() && t.replicas_of(i).next().is_some())
                .map(|i| *t.nodes()[i].id())
                .filter_map(|p| {
                    let dead = dead.contains(&p);
                    (dead || stuart.down_for(&p) >= stuart.fail_after).then_some((p, dead))
                })
                .collect()
        };
        stuck.retain(|p| due.iter().any(|(d, _)| d == p));
        for (primary, dead) in due {
            match fail_over(&stuart, &primary, dead) {
                Ok(true) => stuck.retain(|p| *p != primary),
                Ok(false) => {}
                Err(why) => {
                    if !stuck.contains(&primary) {
                        warning!("Can't fail primary {primary} over yet: {why}");
                        stuck.push(primary);
                    }
                    if dead {
                        // Its pod waits to join again: let it, the old
                        // node's slots lost, as without replicas.
                        stuart.dead.borrow_mut().remove(&primary);
                        stuart.unpromotable.borrow_mut().insert(primary);
                    }
                }
            }
        }
    }
}

/// One failover of `primary`: `Ok(true)` once promoted, `Ok(false)` if it
/// was called off (the primary answered again), `Err` with why none of its
/// replicas could be promoted.
fn fail_over(stuart: &Stuart, primary: &NodeId, dead: bool) -> Result<bool, String> {
    let candidates = candidates(&stuart.state.borrow(), primary, |id| stuart.is_up(id), |id, epoch| stuart.has_taken(id, epoch));
    if candidates.is_empty() {
        return Err("no replica up, not marked failed, that it took a table with".into());
    }
    // Fenced, each saying its offset and whether its copy is complete.
    let mut fenced: Vec<(cluster::Node, u64, bool)> = Vec::new();
    for r in &candidates {
        match fence(r, FENCE) {
            Ok((offset, complete)) => fenced.push((r.clone(), offset, complete)),
            Err(e) => warning!("Can't fence replica {} of {primary}: {e}", r.id()),
        }
    }
    let fenced_at = Instant::now();
    let lift = |except: Option<&NodeId>| {
        for (r, _, _) in fenced.iter().filter(|(r, _, _)| Some(r.id()) != except) {
            if let Err(e) = fence(r, 0) {
                warning!("Can't lift the fence of replica {}: {e}", r.id());
            }
        }
    };
    let Some((chosen, offset)) = choose(&fenced) else {
        lift(None);
        return Err("no replica with a complete copy answered".into());
    };
    if !dead {
        notice!(
            "Primary {primary} down for {} s: failing it over to {} (offset {offset}) once its lease has run out",
            stuart.down_for(primary).as_secs(),
            chosen.id()
        );
        while fenced_at.elapsed() < LEASE + MARGIN {
            sleep(Duration::from_millis(20));
            if stuart.is_up(primary) {
                notice!("Primary {primary} answers again: not failed over");
                lift(None);
                return Ok(false);
            }
        }
    }
    match stuart.change(|s| s.promote(primary, chosen.id())) {
        Ok(true) => {
            let epoch = stuart.state.borrow().table().map_or(0, |t| t.epoch());
            let why = if dead { "it restarted" } else { "it was down" };
            notice!("Failed primary {primary} over to {} (offset {offset}): {why}; table epoch {epoch}", chosen.id());
            stuart.failovers.set(stuart.failovers.get() + 1);
            stuart.dead.borrow_mut().remove(primary);
            lift(Some(chosen.id()));
            Ok(true)
        }
        Ok(false) => {
            lift(None);
            Err(format!("{} is no longer its replica", chosen.id()))
        }
        Err(e) => {
            lift(None);
            Err(format!("can't save the state: {e}"))
        }
    }
}

/// The replicas of `primary` that may be promoted, as far as the table
/// says: up, not marked failed, and `primary` took the table that last
/// cleared their mark ([`State::promotable`](crate::state::State::promotable)).
/// Whether their copy is complete, their fence's reply says.
pub(crate) fn candidates(
    state: &crate::state::State,
    primary: &NodeId,
    is_up: impl Fn(&NodeId) -> bool,
    has_taken: impl Fn(&NodeId, u64) -> bool,
) -> Vec<cluster::Node> {
    let Some(t) = state.table() else { return Vec::new() };
    let Some(p) = t.position(primary) else { return Vec::new() };
    t.replicas_of(p)
        .map(|r| t.nodes()[r].clone())
        .filter(|r| is_up(r.id()))
        .filter(|r| state.promotable(r.id()).is_some_and(|epoch| has_taken(primary, epoch)))
        .collect()
}

/// Of the replicas fenced, with their offset and whether their copy is
/// complete: the complete one with the highest offset (the first of
/// those, on a tie).
pub(crate) fn choose(fenced: &[(cluster::Node, u64, bool)]) -> Option<(cluster::Node, u64)> {
    let mut best: Option<&(cluster::Node, u64, bool)> = None;
    for f in fenced.iter().filter(|(_, _, complete)| *complete) {
        if best.is_none_or(|b| f.1 > b.1) {
            best = Some(f);
        }
    }
    best.map(|(n, o, _)| (n.clone(), *o))
}

/// `CLUSTER FENCE seconds` at `replica`: its offset, and whether its copy
/// is complete.
pub(crate) fn fence(replica: &cluster::Node, secs: u64) -> io::Result<(u64, bool)> {
    fence_on(&mut keeper::connect(replica)?, secs)
}

/// The same, on a connection to the replica.
pub(crate) fn fence_on(link: &mut keeper::Link, secs: u64) -> io::Result<(u64, bool)> {
    match keeper::call(link, &[b"CLUSTER", b"FENCE", secs.to_string().as_bytes()])? {
        Reply::Array(a) => match a[..] {
            [Reply::Integer(offset), Reply::Integer(complete)] if offset >= 0 => Ok((offset as u64, complete == 1)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "an unexpected reply to CLUSTER FENCE")),
        },
        Reply::Error(e) => Err(io::Error::other(format!("CLUSTER FENCE: {e}"))),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "an unexpected reply to CLUSTER FENCE")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::State;
    use cluster::Node;

    fn id(n: u8) -> NodeId {
        NodeId::from_bytes(&[n; 20])
    }

    #[test]
    fn the_most_advanced_complete_copy() {
        let n = |i: u8| Node::new(id(i), "127.0.0.1", 7000 + u16::from(i)).unwrap();
        let pick = |f: &[(Node, u64, bool)]| choose(f).map(|(node, o)| (node.id().as_bytes()[1], o));
        let hex = |i: u8| format!("{i:02x}").as_bytes()[1];
        assert_eq!(pick(&[]), None);
        assert_eq!(pick(&[(n(1), 50, false)]), None, "a copy under way");
        assert_eq!(pick(&[(n(1), 10, true), (n(2), 30, true), (n(3), 20, true)]), Some((hex(2), 30)));
        assert_eq!(pick(&[(n(1), 10, true), (n(2), 90, false)]), Some((hex(1), 10)), "the most advanced, but copying");
        assert_eq!(pick(&[(n(1), 30, true), (n(2), 30, true)]), Some((hex(1), 30)), "the first on a tie");
    }

    #[test]
    fn who_may_be_promoted() {
        let mut s = State::default();
        s.set_replicas(2);
        for n in 1..=3 {
            s.join(Node::new(id(n), "127.0.0.1", 7000 + u16::from(n)).unwrap());
        }
        s.create().unwrap();
        let ids = |c: Vec<Node>| c.iter().map(|n| *n.id()).collect::<Vec<_>>();
        let all = |_: &NodeId| true;
        let taken = |_: &NodeId, _: u64| true;
        assert_eq!(ids(candidates(&s, &id(1), all, taken)), [id(2), id(3)]);
        assert!(candidates(&s, &id(2), all, taken).is_empty(), "a replica has none");
        assert!(candidates(&s, &id(9), all, taken).is_empty());
        // Down: not a candidate.
        assert_eq!(ids(candidates(&s, &id(1), |n| *n != id(2), taken)), [id(3)]);
        // Marked failed: not; cleared, only once the primary took the table
        // that cleared it.
        s.set_failed(&id(3), true);
        assert_eq!(ids(candidates(&s, &id(1), all, taken)), [id(2)]);
        s.set_failed(&id(3), false);
        let cleared = s.table().unwrap().epoch();
        assert_eq!(ids(candidates(&s, &id(1), all, |p, e| *p == id(1) && e < cleared)), [id(2)], "the primary may write alone still");
        assert_eq!(ids(candidates(&s, &id(1), all, |p, e| *p == id(1) && e <= cleared)), [id(2), id(3)]);
    }
}
