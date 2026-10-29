//! What stuart knows and keeps across restarts: the nodes that joined (and
//! which of them are leaving), and once `STUART CREATE` made it, the slot
//! table, with the slots moving and those waiting to. Changes go
//! through [`State::join`], [`State::create`], [`State::move_slots`],
//! [`State::rebalance`], [`State::leave`], [`State::advance`]... on a copy,
//! which is saved before it replaces the old state and before anything is
//! pushed to the nodes.
//!
//! A slot moves by snapshot (phase 8; phase 5 moved it key by key) in two
//! tables, the first `SYNCING` it (the target copies it while the source
//! serves it), the second giving it to the target ([`State::advance`])
//! once the target has every write. The state says which nodes get the
//! current table first ([`State::first`]): the first table goes to the
//! source first (it accepts the target's copy; unless the table also ends
//! a move from it, see `advance`), the second to the target first: the
//! target serves the slot only once the source, taking the table, has
//! sent it the last writes (the dbnode's `repl` module), so the two never
//! serve it together, and a client the source redirects finds the target
//! waiting for them rather than sending it back. A move that can't end is
//! called off and queued again ([`State::cancel`]). A state file of phase
//! 5 (`migrate` lines, a slot moving key by key) is read as moves by
//! snapshot: the target copies the slots again from scratch.
//!
//! With `--replicas N` (phase 8), some nodes are replicas: the table says
//! of which primary. `STUART CREATE` makes the first `ceil(k / (1 + N))`
//! of the k joined nodes primaries and the others their replicas; a node
//! joining later is a replica of a primary with fewer than N, if there is
//! one, else a primary (and `--auto` gives it slots). A replica goes to
//! the primary on another zone than its own if it can (`ZONE`, the
//! machine it runs on), then the one with the fewest replicas, then the
//! first joined. Only primaries are given slots. A replica whose primary
//! is removed goes to another primary as a node joining would, or becomes
//! a primary (without slots). Lowering N changes no node's role, only
//! what the nodes joining become; raising it leaves primaries beyond the
//! need, demoted with `--auto` (below).
//!
//! The table also marks the nodes stuart found down for a while failed
//! ([`State::set_failed`]): a primary whose replica is marked writes
//! without it (the lease), and clients aren't sent to it. The pairing
//! stays: a replica back soon carries on from its primary's backlog.
//!
//! A primary leaving with a replica that can take over hands its slots to
//! it instead of draining them ([`State::leave_handing`]): its slots stay
//! where they are, out of every plan, until the handover fiber promotes the
//! replica ([`State::hand_over`]), or gives up and drains them as any other
//! node's ([`State::drain_instead`]).
//!
//! Nodes leaving can leave more primaries than `ceil(k / (1 + N))` of the k
//! nodes staying (a scale-down takes the replicas, the last joined, first),
//! with primaries lacking a replica. With `--auto`, once no node is
//! leaving, one primary at a time is demoted ([`State::plan_roles`]): it
//! gets no slots, so the rebalance moves its own to the others, and once it
//! holds nothing it becomes the replica of one lacking a replica
//! ([`State::demote`]). A node joining meanwhile calls it off.

use cluster::{Node, NodeId, SlotRange, SlotTable, SLOTS};
use dbnode::valid_name;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// The state file's first line.
const HEADER: &str = "stuart-state 1";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// The joined nodes, in join order: the table's node order.
    nodes: Vec<Node>,
    /// Nodes leaving (`STUART LEAVE`): their slots move to the others, then
    /// they are removed.
    leaving: Vec<NodeId>,
    /// The leaving primaries whose slots go to one of their replicas: not
    /// drained, and their slots left out of the plans.
    handing: Vec<NodeId>,
    /// The primary being made a replica: it gets no slots, and is given no
    /// replica.
    demoting: Option<NodeId>,
    /// Made by `STUART CREATE`; lists the same nodes as `nodes`. Its
    /// migrations are the slots moving.
    table: Option<SlotTable>,
    /// Slots to move (`STUART MOVE`), with their target, in the order asked.
    queue: Vec<(u16, NodeId)>,
    /// The nodes that must take the current table before the others: the
    /// targets of the moves it starts or ends.
    first: Vec<NodeId>,
    /// The names nodes joined with (`STUART JOIN ... NAME name`): what stays
    /// the same when a node restarts at another address (a pod's name, as
    /// it announces its pod's address).
    names: Vec<(NodeId, String)>,
    /// The zones nodes joined with (`STUART JOIN ... ZONE zone`): the
    /// machine each runs on (a Kubernetes node's name).
    zones: Vec<(NodeId, String)>,
    /// Per replica whose fail mark was cleared, the epoch of the table that
    /// cleared it: its primary may be writing alone until it took that
    /// table, so the replica can't be promoted before ([`State::promotable`]).
    cleared: Vec<(NodeId, u64)>,
    /// Per primary failed over and not back yet, the replica promoted in
    /// its place: back, it is that one's replica, as in Redis Cluster.
    successors: Vec<(NodeId, NodeId)>,
    /// `--replicas N`: how many replicas a primary is given. From the
    /// command line, not the state file ([`State::set_replicas`]).
    replicas: usize,
}

/// What a join changed.
#[derive(Debug, PartialEq, Eq)]
pub struct Joined {
    pub change: Change,
    /// Nodes that announced the address the joining node now announces, or
    /// joined with its name: a node restarted (with a new id, and without
    /// its keys). They are removed, and the slots they owned become
    /// unassigned (lost).
    pub replaced: Vec<(NodeId, usize)>,
    /// The node's primary, if it is a replica.
    pub primary: Option<NodeId>,
}


#[derive(Debug, PartialEq, Eq)]
pub enum Change {
    /// Known with this address already: nothing changes.
    Known,
    /// Known, at another address.
    Moved,
    /// New. With a table, it is added to it without slots.
    Added,
}

impl State {
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn table(&self) -> Option<&SlotTable> {
        self.table.as_ref()
    }

    pub fn node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id() == id)
    }

    /// Slots waiting to move, and where to.
    pub fn queue(&self) -> &[(u16, NodeId)] {
        &self.queue
    }

    /// The nodes that take the current table before the others.
    pub fn first(&self) -> &[NodeId] {
        &self.first
    }

    /// The nodes leaving.
    pub fn leaving(&self) -> &[NodeId] {
        &self.leaving
    }

    /// The leaving primaries that hand their slots to a replica.
    pub fn handing(&self) -> &[NodeId] {
        &self.handing
    }

    /// The primary being made a replica, if any.
    pub fn demoting(&self) -> Option<&NodeId> {
        self.demoting.as_ref()
    }

    /// The primaries not leaving, nor failed (a primary failed over), in
    /// join order.
    fn primaries(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .map(|n| *n.id())
            .filter(|id| !self.leaving.contains(id) && self.primary_of(id).is_none() && !self.is_failed(id))
            .collect()
    }

    /// The primaries that get slots: not the one being demoted.
    fn active(&self) -> Vec<NodeId> {
        self.primaries().into_iter().filter(|id| self.demoting != Some(*id)).collect()
    }

    /// How many primaries there are beyond `ceil(k / (1 + N))` of the k
    /// nodes not leaving (counting the one being demoted); 0 with N = 0.
    pub fn surplus(&self) -> usize {
        if self.table.is_none() || self.replicas == 0 {
            return 0;
        }
        let staying = self.nodes.iter().filter(|n| !self.leaving.contains(n.id())).count();
        self.primaries().len().saturating_sub(staying.div_ceil(1 + self.replicas))
    }

    /// The roles planned again (see the module docs): a demotion no longer
    /// needed is called off; with a surplus of primaries, none being
    /// demoted, no node leaving and no primary failed (failing over, or not
    /// back yet), the primary with the fewest replicas, then the fewest
    /// slots, then the last joined, starts being demoted, if the others
    /// have room for its keys (`roomy`). Whether anything changed.
    ///
    /// A primary lacking a replica for it, and for each of its own, is
    /// certain: with P primaries, R replicas and N each, a surplus means
    /// `P > ceil((P + R) / (1 + N))`, so `(P - 1) N >= R + 1`. Only a
    /// primary starting to leave meanwhile can take that away
    /// ([`State::demote`] then waits).
    pub fn plan_roles(&mut self, roomy: impl Fn(&NodeId) -> bool) -> bool {
        if self.demoting.is_some() {
            if self.surplus() > 0 {
                return false;
            }
            self.demoting = None;
            return true;
        }
        let Some(t) = &self.table else { return false };
        let failed_primary = t.failed().iter().any(|&i| t.primary_of(i).is_none());
        if self.surplus() == 0 || !self.leaving.is_empty() || failed_primary {
            return false;
        }
        let replicas = |id: &NodeId| t.position(id).map_or(0, |i| t.replicas_of(i).count());
        let order = |id: &NodeId| self.nodes.iter().position(|n| n.id() == id).unwrap_or(0);
        let chosen = self
            .primaries()
            .into_iter()
            .filter(|c| roomy(c))
            .min_by_key(|c| (replicas(c), self.holding(c), std::cmp::Reverse(order(c))));
        let Some(chosen) = chosen else { return false };
        self.demoting = Some(chosen);
        true
    }

    /// The primary being demoted, `id`, holding nothing now, becomes a
    /// replica, in a table with the next epoch: of a primary lacking one,
    /// chosen as for a node joining; its own replicas go to other
    /// primaries first, as a removed primary's do. Whether it was done: not
    /// while it holds slots, nor without a primary for it and each of its
    /// replicas (it stays being demoted).
    pub fn demote(&mut self, id: &NodeId) -> bool {
        if self.demoting != Some(*id) || self.holding(id) > 0 || self.primary_of(id).is_some() {
            return false;
        }
        let Some(old) = self.table.clone() else { return false };
        let Some(primary) = self.pick_primary(id, &[]) else { return false };
        let orphans: Vec<NodeId> = replicas(&old).into_iter().filter(|(_, p)| p == id).map(|(r, _)| r).collect();
        let mut pairs: Vec<(NodeId, NodeId)> = replicas(&old).into_iter().filter(|(_, p)| p != id).collect();
        pairs.push((*id, primary));
        // `first` stays: the moves under way are in this table too.
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners(&old), &migrations(&old), &pairs, &failed(&old)));
        for r in &orphans {
            match self.pick_primary(r, &orphans) {
                Some(p) => self.attach(*r, p),
                None => {
                    self.table = Some(old);
                    return false;
                }
            }
        }
        self.demoting = None;
        true
    }

    /// `--replicas N`.
    pub fn set_replicas(&mut self, n: usize) {
        self.replicas = n;
    }

    /// The primary node `id` is a replica of, if it is one.
    pub fn primary_of(&self, id: &NodeId) -> Option<NodeId> {
        let t = self.table.as_ref()?;
        t.primary_of(t.position(id)?).map(|p| *t.nodes()[p].id())
    }

    /// Whether the table marks node `id` failed.
    pub fn is_failed(&self, id: &NodeId) -> bool {
        self.table.as_ref().is_some_and(|t| t.position(id).is_some_and(|i| t.is_failed(i)))
    }

    /// Marks node `id` failed, or clears the mark, in a table with the next
    /// epoch; nothing changes if it is so already (or without a table, or
    /// for a node not in it). Whether the table changed.
    pub fn set_failed(&mut self, id: &NodeId, failed: bool) -> bool {
        let Some(old) = &self.table else { return false };
        if old.position(id).is_none() || self.is_failed(id) == failed {
            return false;
        }
        let mut marked = self::failed(old);
        if failed {
            marked.push(*id);
        } else {
            marked.retain(|m| m != id);
        }
        let epoch = old.epoch() + 1;
        // `first` stays: the moves under way are in this table too.
        self.table = Some(table_from(epoch, self.nodes.clone(), &owners(old), &migrations(old), &replicas(old), &marked));
        self.cleared.retain(|(r, _)| r != id);
        if !failed {
            self.cleared.push((*id, epoch));
        }
        true
    }

    /// A primary failed over (marked failed, holding nothing) answers
    /// again: in a table with the next epoch its mark goes and it becomes
    /// the replica of its successor, as in Redis Cluster, if that is still
    /// a primary not leaving, else of one with fewer than N, as a node
    /// joining would; or else it stays a primary without slots. Whether it
    /// was such a node.
    pub fn back(&mut self, id: &NodeId) -> bool {
        if !self.is_failed(id) || self.primary_of(id).is_some() || self.holding(id) > 0 {
            return false;
        }
        let Some(old) = &self.table else { return false };
        let successor = self.successors.iter().find(|(p, _)| p == id).map(|&(_, s)| s);
        let successor = successor.filter(|s| old.position(s).is_some_and(|i| old.primary_of(i).is_none()) && !self.leaving.contains(s));
        let primary = successor.or_else(|| self.pick_primary(id, &[]));
        self.successors.retain(|(p, _)| p != id);
        let mut pairs = replicas(old);
        pairs.extend(primary.map(|p| (*id, p)));
        let marked: Vec<NodeId> = failed(old).into_iter().filter(|m| m != id).collect();
        let epoch = old.epoch() + 1;
        self.table = Some(table_from(epoch, self.nodes.clone(), &owners(old), &migrations(old), &pairs, &marked));
        self.cleared.retain(|(r, _)| r != id);
        self.cleared.push((*id, epoch));
        true
    }

    /// Whether replica `id` could be promoted: it isn't marked failed, and
    /// its primary has taken the table that last cleared its mark (the
    /// epoch returned: 0 if it never was marked). Until it took it, the
    /// primary may be writing alone.
    pub fn promotable(&self, id: &NodeId) -> Option<u64> {
        self.primary_of(id)?;
        if self.is_failed(id) {
            return None;
        }
        Some(self.cleared.iter().find(|(r, _)| r == id).map_or(0, |&(_, e)| e))
    }

    /// Fails `primary` over to its replica `replica`, in a table with the
    /// next epoch: the replica owns the primary's slots, and the primary's
    /// other replicas follow it; the primary stays a primary without slots,
    /// marked failed, as Redis Cluster shows a failed-over master until it
    /// comes back (then [`State::back`] makes it a replica).
    /// Moves to or from the old primary are called off: one it was the
    /// source of leaves the slot with the new primary, one it was the
    /// target of with its source; a move by snapshot from it is queued
    /// again (the new primary holds the slot whole); queued moves to it are
    /// dropped. The new primary gets the table first. Whether it was done
    /// (`replica` must be `primary`'s replica).
    pub fn promote(&mut self, primary: &NodeId, replica: &NodeId) -> bool {
        if !self.take_over(primary, replica, true) {
            return false;
        }
        self.successors.retain(|(p, _)| p != primary);
        self.successors.push((*primary, *replica));
        true
    }

    /// Leaving primary `primary` hands its slots to its replica `replica`
    /// (see the module docs), in a table with the next epoch, as
    /// [`State::promote`] makes it, but the old primary isn't marked
    /// failed, nor followed when it comes back: it stays a primary without
    /// slots until it is removed, having left. Whether it was done: not to
    /// a replica leaving too (it may have started since it was chosen).
    pub fn hand_over(&mut self, primary: &NodeId, replica: &NodeId) -> bool {
        !self.leaving.contains(replica) && self.take_over(primary, replica, false)
    }

    /// What [`State::promote`] and [`State::hand_over`] share; `failed`
    /// marks the old primary failed.
    fn take_over(&mut self, primary: &NodeId, replica: &NodeId, failed: bool) -> bool {
        let Some(old) = &self.table else { return false };
        if self.primary_of(replica) != Some(*primary) {
            return false;
        }
        let mut owners = owners(old);
        for owner in owners.iter_mut().filter(|o| o.as_ref() == Some(primary)) {
            *owner = Some(*replica);
        }
        let requeue: Vec<(u16, NodeId)> =
            migrations(old).into_iter().filter(|&(_, from, to, snapshot)| snapshot && from == *primary && to != *replica).map(|(slot, _, to, _)| (slot, to)).collect();
        let migrations: Vec<_> = migrations(old).into_iter().filter(|(_, from, to, _)| from != primary && to != primary).collect();
        let pairs: Vec<(NodeId, NodeId)> = replicas(old)
            .into_iter()
            .filter(|(r, _)| r != replica)
            .map(|(r, p)| if p == *primary { (r, *replica) } else { (r, p) })
            .collect();
        let mut marked = self::failed(old);
        if failed && !marked.contains(primary) {
            marked.push(*primary);
        }
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations, &pairs, &marked));
        self.queue.retain(|(_, to)| to != primary);
        self.queue.extend(requeue);
        self.first = vec![*replica];
        self.handing.retain(|p| p != primary);
        if self.demoting == Some(*primary) {
            self.demoting = None;
        }
        self.queue_check();
        true
    }

    /// The zone node `id` joined with, if any.
    pub fn zone(&self, id: &NodeId) -> Option<&str> {
        self.zones.iter().find(|(n, _)| n == id).map(|(_, zone)| zone.as_str())
    }

    /// What node `id` still holds: the slots it owns, those moving to it and
    /// those queued to move to it.
    pub fn holding(&self, id: &NodeId) -> usize {
        let Some(t) = &self.table else { return 0 };
        let owned: usize = t.runs().filter(|(_, o)| o.is_some_and(|i| t.nodes()[i].id() == id)).map(|(r, _)| r.len()).sum();
        let incoming = t.migrations().iter().filter(|m| t.nodes()[m.to].id() == id).count();
        owned + incoming + self.queue.iter().filter(|(_, to)| to == id).count()
    }

    /// What clients see (`CLUSTER SLOTS`...): the table, or before `STUART
    /// CREATE` the joined nodes owning nothing.
    pub fn view(&self) -> SlotTable {
        match &self.table {
            Some(table) => table.clone(),
            None => SlotTable::new(0, self.nodes.clone(), &[(SlotRange::new(0, SLOTS as u16 - 1), None)])
                .expect("joined nodes have distinct ids and addresses"),
        }
    }

    /// The name node `id` joined with, if any.
    pub fn name(&self, id: &NodeId) -> Option<&str> {
        self.names.iter().find(|(n, _)| n == id).map(|(_, name)| name.as_str())
    }

    /// Records that `node` joined, without a name.
    pub fn join(&mut self, node: Node) -> Joined {
        self.join_named(node, None)
    }

    /// Records that `node` joined, with `name` if given (see
    /// [`dbnode::valid_name`]). With a table, any change to the nodes makes a new
    /// one with the next epoch.
    pub fn join_named(&mut self, node: Node, name: Option<&str>) -> Joined {
        self.join_in(node, name, None)
    }

    /// Records that `node` joined, with `name` and `zone` if given. A node
    /// new to a table may become a replica (see the module docs).
    pub fn join_in(&mut self, node: Node, name: Option<&str>, zone: Option<&str>) -> Joined {
        let addr = |n: &Node| (n.host().to_string(), n.port());
        let same_name = |n: &Node| name.is_some() && self.name(n.id()) == name;
        let replaced: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.id() != node.id() && (addr(n) == addr(&node) || same_name(n)))
            .map(|n| *n.id())
            .collect();
        let lost = |id: &NodeId| match &self.table {
            Some(t) => t.runs().filter(|(_, o)| o.is_some_and(|i| t.nodes()[i].id() == id)).map(|(r, _)| r.len()).sum(),
            None => 0,
        };
        let replaced: Vec<(NodeId, usize)> = replaced.iter().map(|id| (*id, lost(id))).collect();
        self.nodes.retain(|n| !replaced.iter().any(|(id, _)| n.id() == id));
        // Names and zones of nodes gone (replaced, left, forgotten) go too.
        let nodes = &self.nodes;
        self.names.retain(|(id, _)| nodes.iter().any(|n| n.id() == id));
        self.zones.retain(|(id, _)| nodes.iter().any(|n| n.id() == id));
        if let Some(name) = name {
            self.names.retain(|(id, _)| id != node.id());
            self.names.push((*node.id(), name.to_string()));
        }
        self.zones.retain(|(id, _)| id != node.id());
        if let Some(zone) = zone {
            self.zones.push((*node.id(), zone.to_string()));
        }
        let id = *node.id();

        let change = match self.nodes.iter_mut().find(|n| n.id() == node.id()) {
            Some(known) if *known == node => Change::Known,
            Some(known) => {
                *known = node;
                Change::Moved
            }
            None => {
                self.nodes.push(node);
                Change::Added
            }
        };
        if change != Change::Known || !replaced.is_empty() {
            self.retable();
        }
        if change == Change::Added {
            if let Some(primary) = self.pick_primary(&id, &[]) {
                self.attach(id, primary);
            }
        }
        Joined { change, replaced, primary: self.primary_of(&id) }
    }

    /// The primary that replica `id` would go to (see the module docs), of
    /// the primaries not leaving, not being demoted and not in `skip`;
    /// `None` if each has N.
    fn pick_primary(&self, id: &NodeId, skip: &[NodeId]) -> Option<NodeId> {
        let t = self.table.as_ref()?;
        let zone = self.zone(id);
        let rank = |p: &NodeId| match (zone, self.zone(p)) {
            (Some(a), Some(b)) if a == b => 2,
            (Some(_), Some(_)) => 0,
            _ => 1,
        };
        (0..t.nodes().len())
            .filter(|&i| t.primary_of(i).is_none() && !t.is_failed(i) && t.nodes()[i].id() != id)
            .map(|i| (i, *t.nodes()[i].id(), t.replicas_of(i).count()))
            .filter(|(_, p, n)| *n < self.replicas && !self.leaving.contains(p) && self.demoting != Some(*p) && !skip.contains(p))
            .min_by_key(|(i, p, n)| (rank(p), *n, *i))
            .map(|(_, p, _)| p)
    }

    /// Makes `id` (owning nothing) a replica of `primary`, in the table as
    /// it is.
    fn attach(&mut self, id: NodeId, primary: NodeId) {
        let Some(t) = self.table.take() else { return };
        let mut list = replicas(&t);
        list.push((id, primary));
        self.table = Some(t.with_replicas(&list).expect("a replica that owns nothing, of a primary"));
    }

    /// The table again, for the current nodes, with the next epoch: slots
    /// of removed nodes become unassigned, except a slot migrating from one,
    /// which goes to its target with the keys that got there. A migration to
    /// a removed node ends with the slot where it was (its keys there are
    /// gone with the node). Moves waiting for a removed node are dropped.
    /// Without a table, only the leaving nodes are checked.
    fn retable(&mut self) {
        let known = |id: &NodeId| self.nodes.iter().any(|n| n.id() == id);
        self.leaving.retain(|id| known(id));
        self.handing.retain(|id| known(id));
        self.demoting = self.demoting.filter(|id| known(id));
        let Some(old) = &self.table else { return };
        let mut owners = owners(old);
        let mut migrations = Vec::new();
        for m in old.migrations() {
            let (from, to) = (*old.nodes()[m.from].id(), *old.nodes()[m.to].id());
            match (known(&from), known(&to)) {
                (true, true) => migrations.push((m.slot, from, to, m.snapshot)),
                (false, true) => owners[usize::from(m.slot)] = Some(to),
                _ => {}
            }
        }
        for owner in owners.iter_mut() {
            *owner = owner.filter(|id| known(id));
        }
        self.queue.retain(|(_, to)| known(to));
        // The targets of the moves still go first; a slot whose source went
        // is its target's now, and the target should know before the others.
        self.first.retain(|id| known(id));
        let (kept, orphans): (Vec<_>, Vec<_>) = replicas(old).into_iter().filter(|(r, _)| known(r)).partition(|(_, p)| known(p));
        let marked: Vec<NodeId> = failed(old).into_iter().filter(|id| known(id)).collect();
        self.cleared.retain(|(id, _)| known(id));
        self.successors.retain(|(p, s)| known(p) && known(s));
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations, &kept, &marked));
        self.queue_check();
        // A replica whose primary went: to another primary, or a primary.
        // Not to one of the others, which have just become primaries too.
        let orphans: Vec<NodeId> = orphans.into_iter().map(|(r, _)| r).collect();
        for r in &orphans {
            if let Some(primary) = self.pick_primary(r, &orphans) {
                self.attach(*r, primary);
            }
        }
    }

    /// Drops queued moves that have nothing left to do: the slot is the
    /// target's already, or no node owns it.
    fn queue_check(&mut self) {
        let Some(t) = &self.table else { return };
        self.queue.retain(|(slot, to)| t.owner(*slot).is_some_and(|n| n.id() != to));
    }

    /// `STUART CREATE`: the slots split evenly over the primaries (the
    /// first `ceil(k / (1 + N))` nodes; the others become their replicas),
    /// in join order, in contiguous ranges, as `redis-cli --cluster create`
    /// splits them (3 nodes: 0-5460, 5461-10922, 10923-16383). Epoch 1.
    pub fn create(&mut self) -> Result<(), String> {
        if let Some(t) = &self.table {
            return Err(format!("ERR The slots were assigned already (table epoch {})", t.epoch()));
        }
        if self.nodes.is_empty() {
            return Err("ERR No node has joined yet".to_string());
        }
        let n = self.nodes.len().div_ceil(1 + self.replicas);
        let per_node = SLOTS as f64 / n as f64;
        let mut ranges = Vec::with_capacity(n);
        let (mut first, mut cursor) = (0usize, 0f64);
        for (i, node) in self.nodes[..n].iter().enumerate() {
            // redis-cli's clusterManagerCommandCreate, rounding included.
            let mut last = (cursor + per_node - 1.0).round() as usize;
            if last >= SLOTS || i == n - 1 {
                last = SLOTS - 1;
            }
            last = last.max(first);
            ranges.push((SlotRange::new(first as u16, last as u16), Some(*node.id())));
            first = last + 1;
            cursor += per_node;
        }
        // More nodes than slots: the last ones get none (the range would
        // start past the end).
        ranges.retain(|(r, _)| usize::from(r.start) < SLOTS && r.start <= r.end);
        let table = SlotTable::new(1, self.nodes.clone(), &ranges).map_err(|e| format!("ERR {e}"))?;
        self.table = Some(table);
        // The rest are replicas, of the primaries with slots: not of each
        // other (those not placed yet look like primaries without slots).
        let rest: Vec<NodeId> = self.nodes[n..].iter().map(|n| *n.id()).collect();
        for &id in &rest {
            let primary = self.pick_primary(&id, &rest).expect("ceil(k / (1 + N)) primaries take k - that replicas");
            self.attach(id, primary);
        }
        Ok(())
    }

    /// `STUART ASSIGN start end id`: gives unassigned slots (a replaced
    /// node's, lost) to node `id`, in a table with the next epoch. Returns
    /// how many. Slots another node serves can't be moved this way: their
    /// keys would stay behind (phase 5 moves slots with their keys).
    pub fn assign(&mut self, range: SlotRange, id: &NodeId) -> Result<usize, String> {
        let Some(old) = &self.table else {
            return Err("ERR There is no table yet: STUART CREATE makes it".to_string());
        };
        if range.start > range.end || usize::from(range.end) >= SLOTS {
            return Err(format!("ERR Invalid slot range {}-{}", range.start, range.end));
        }
        if old.position(id).is_none() {
            return Err(format!("ERR Unknown node {id}"));
        }
        if self.primary_of(id).is_some() {
            return Err(format!("ERR Node {id} is a replica"));
        }
        if let Some(slot) = (range.start..=range.end).find(|&s| old.owner_index(s).is_some()) {
            let owner = old.owner(slot).map(|n| *n.id()).unwrap();
            return Err(format!("ERR Slot {slot} is served by {owner}: only unassigned slots can be assigned"));
        }
        let mut owners = owners(old);
        owners[usize::from(range.start)..=usize::from(range.end)].fill(Some(*id));
        let migrations = migrations(old);
        // `first` stays: the moves under way are in this table too.
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations, &replicas(old), &failed(old)));
        Ok(range.len())
    }

    /// Hands the unassigned slots (a replaced or forgotten node's, whose
    /// keys are gone) to the nodes not leaving, the ones with the fewest
    /// first (counting the moves under way and queued), in runs, in a table
    /// with the next epoch. Returns how many (`--auto` does it).
    pub fn assign_unassigned(&mut self) -> usize {
        let Some(old) = &self.table else { return 0 };
        let active = self.active();
        let free: Vec<u16> = (0..SLOTS as u16).filter(|&s| old.owner_index(s).is_none()).collect();
        if free.is_empty() || active.is_empty() {
            return 0;
        }
        let mut after = owners_after(old);
        for (slot, to) in &self.queue {
            after[usize::from(*slot)] = Some(*to);
        }
        let mut have: Vec<usize> = active.iter().map(|id| after.iter().filter(|o| o.as_ref() == Some(id)).count()).collect();
        let mut want = have.clone();
        for _ in &free {
            let i = (0..want.len()).min_by_key(|&i| (want[i], i)).unwrap();
            want[i] += 1;
        }
        let mut owners = owners(old);
        let mut slots = free.iter();
        for (i, id) in active.iter().enumerate() {
            while have[i] < want[i] {
                let Some(&slot) = slots.next() else { break };
                owners[usize::from(slot)] = Some(*id);
                have[i] += 1;
            }
        }
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations(old), &replicas(old), &failed(old)));
        free.len()
    }

    /// `STUART MOVE start end id`: queues the slots of the range that `id`
    /// doesn't own to move to it. Returns how many. Every slot must be
    /// owned (an unassigned one has no keys to move: `STUART ASSIGN` it),
    /// and none may be moving or queued already.
    pub fn move_slots(&mut self, range: SlotRange, id: &NodeId) -> Result<usize, String> {
        let Some(t) = &self.table else {
            return Err("ERR There is no table yet: STUART CREATE makes it".to_string());
        };
        if range.start > range.end || usize::from(range.end) >= SLOTS {
            return Err(format!("ERR Invalid slot range {}-{}", range.start, range.end));
        }
        if t.position(id).is_none() {
            return Err(format!("ERR Unknown node {id}"));
        }
        if self.leaving.contains(id) {
            return Err(format!("ERR Node {id} is leaving"));
        }
        if self.primary_of(id).is_some() {
            return Err(format!("ERR Node {id} is a replica"));
        }
        if self.demoting == Some(*id) {
            return Err(format!("ERR Node {id} is becoming a replica"));
        }
        let mut added = Vec::new();
        for slot in range.start..=range.end {
            let Some(owner) = t.owner(slot) else {
                return Err(format!("ERR Slot {slot} is unassigned: STUART ASSIGN gives it to a node"));
            };
            if t.migration(slot).is_some() || self.queue.iter().any(|(s, _)| *s == slot) {
                return Err(format!("ERR Slot {slot} is moving already"));
            }
            if owner.id() != id {
                added.push((slot, *id));
            }
        }
        let n = added.len();
        self.queue.extend(added);
        Ok(n)
    }

    /// `STUART REBALANCE`, and what `--auto` does whenever the nodes change:
    /// the queue planned anew, so that once the moves under way and the
    /// queued ones end, the leaving nodes own nothing and each other node
    /// has `assigned / n` slots or one more (the extra ones stay with the
    /// nodes that have the most), moving as few slots as can be. A node
    /// gives away its highest slots and a node receives them in runs, so
    /// ranges stay few. Slots moving now go on to their targets. The nodes
    /// of `full` receive nothing (they may still give): their share goes to
    /// the others. Returns how many slots are queued.
    pub fn rebalance(&mut self, full: &[NodeId]) -> Result<usize, String> {
        let Some(t) = &self.table else {
            return Err("ERR There is no table yet: STUART CREATE makes it".to_string());
        };
        let active = self.active();
        if active.is_empty() {
            return Err("ERR Every node is leaving".to_string());
        }
        self.queue = spread(&self.plan_owners(t), &moving(t), &active, full, Goal::Even);
        Ok(self.queue.len())
    }

    /// `STUART DRAIN id`: queues moves of all of `id`'s slots to the nodes
    /// not leaving, the ones with the fewest first, so that they end as
    /// even as can be without moving anything else. Slots queued to `id`
    /// aren't any more; slots moving to it now get there first (draining
    /// again moves them too). The nodes of `full` receive nothing; slots
    /// that no node can take stay. Returns how many were queued. Not for
    /// the last node.
    pub fn drain(&mut self, id: &NodeId, full: &[NodeId]) -> Result<usize, String> {
        let Some(t) = &self.table else {
            return Err("ERR There is no table yet: STUART CREATE makes it".to_string());
        };
        if t.position(id).is_none() {
            return Err(format!("ERR Unknown node {id}"));
        }
        let others: Vec<NodeId> = self.active().into_iter().filter(|n| n != id).collect();
        if others.is_empty() {
            return Err("ERR No other node to move the slots to".to_string());
        }
        self.queue.retain(|(_, to)| to != id);
        let mut owners = self.plan_owners(t);
        for (slot, to) in &self.queue {
            owners[usize::from(*slot)] = Some(*to);
        }
        let moves = spread(&owners, &moving(t), &others, full, Goal::Fill);
        let n = moves.len();
        self.queue.extend(moves);
        Ok(n)
    }

    /// Drains again the leaving nodes that got slots since (moves that were
    /// under way to them). Returns how many slots were queued.
    pub fn drain_leaving(&mut self, full: &[NodeId]) -> usize {
        let ids: Vec<NodeId> = self.leaving.iter().filter(|id| self.holding(id) > 0 && !self.handing.contains(id)).copied().collect();
        ids.iter().map(|id| self.drain(id, full).unwrap_or(0)).sum()
    }

    /// `STUART LEAVE id`: node `id` is leaving. Its slots move to the other
    /// nodes (as `drain` moves them); once it holds nothing it is removed
    /// ([`State::remove_left`]). Returns what it still holds
    /// ([`State::holding`]) plus one while it is a node: 0 once it is gone,
    /// as for a node stuart doesn't know. Before `CREATE` it goes at once.
    /// Once every node is leaving, the cluster is being shut down (scaled
    /// to zero, `docker compose down`): there's nowhere to keep the keys,
    /// and each gets 0 so that it stops at once.
    pub fn leave(&mut self, id: &NodeId, full: &[NodeId]) -> Result<usize, String> {
        if self.node(id).is_none() {
            return Ok(0);
        }
        if self.table.is_none() {
            self.nodes.retain(|n| n.id() != id);
            self.retable();
            return Ok(0);
        }
        if self.demoting == Some(*id) {
            self.demoting = None;
        }
        if !self.leaving.contains(id) {
            if self.holding(id) > 0 && self.active().iter().any(|n| n != id) {
                self.drain(id, full)?;
            }
            self.leaving.push(*id);
        }
        if self.shutting_down() {
            return Ok(0);
        }
        Ok(self.holding(id) + 1)
    }

    /// `STUART LEAVE id` from a primary with a replica that can take over
    /// (stuart checks that it is up and can be promoted): it is leaving,
    /// but its slots aren't drained; a handover gives them to the replica
    /// (see the module docs). Returns what [`State::leave`] would; a node
    /// that isn't a primary with slots and a replica leaves as there.
    pub fn leave_handing(&mut self, id: &NodeId, full: &[NodeId]) -> Result<usize, String> {
        let t = self.table.as_ref();
        let has_replica = t.and_then(|t| t.position(id).map(|i| t.replicas_of(i).next().is_some())).unwrap_or(false);
        if self.leaving.contains(id) || self.primary_of(id).is_some() || self.holding(id) == 0 || !has_replica {
            return self.leave(id, full);
        }
        if self.demoting == Some(*id) {
            self.demoting = None;
        }
        self.leaving.push(*id);
        self.handing.push(*id);
        // Moves to it are for nobody now; those under way end as they go.
        self.queue.retain(|(_, to)| to != id);
        if self.shutting_down() {
            return Ok(0);
        }
        Ok(self.holding(id) + 1)
    }

    /// A handover of `id`'s slots gave up: they are drained as `STUART
    /// LEAVE` drains a node. Returns how many were queued.
    pub fn drain_instead(&mut self, id: &NodeId, full: &[NodeId]) -> Result<usize, String> {
        if !self.handing.contains(id) {
            return Ok(0);
        }
        self.handing.retain(|p| p != id);
        if self.holding(id) > 0 && self.active().iter().any(|n| n != id) {
            return self.drain(id, full);
        }
        Ok(0)
    }

    /// Every slot's owner once the moves under way have ended, as the plans
    /// see it: the slots of the primaries handing over owned by nobody, so
    /// that no plan moves them, or counts them.
    fn plan_owners(&self, t: &SlotTable) -> Vec<Option<NodeId>> {
        let mut owners = owners_after(t);
        for owner in owners.iter_mut() {
            if owner.is_some_and(|o| self.handing.contains(&o)) {
                *owner = None;
            }
        }
        owners
    }

    /// Whether every node is leaving.
    pub fn shutting_down(&self) -> bool {
        !self.nodes.is_empty() && self.leaving.len() == self.nodes.len()
    }

    /// Removes node `id` if it is leaving and holds nothing, in a table with
    /// the next epoch. Whether it did.
    pub fn remove_left(&mut self, id: &NodeId) -> bool {
        if !self.leaving.contains(id) || self.holding(id) > 0 {
            return false;
        }
        self.nodes.retain(|n| n.id() != id);
        self.retable();
        true
    }

    /// `STUART FORGET id`: node `id` is gone for good (removed without
    /// leaving). It is removed as a replaced node is: its slots become
    /// unassigned (lost), except those migrating from it, which go to their
    /// targets. Returns how many slots were lost.
    pub fn forget(&mut self, id: &NodeId) -> Result<usize, String> {
        if self.node(id).is_none() {
            return Err(format!("ERR Unknown node {id}"));
        }
        let lost = match &self.table {
            Some(t) => (0..SLOTS as u16).filter(|&s| t.owner(s).is_some_and(|n| n.id() == id) && t.migration(s).is_none()).count(),
            None => 0,
        };
        self.nodes.retain(|n| n.id() != id);
        self.retable();
        Ok(lost)
    }

    /// The next table of a move: the migrations of `done` end (the
    /// cutover: their targets own the slots now), and the queued slots
    /// `start` begin to migrate. The targets of the moves ending get the
    /// table first, and the sources of the moves starting (they accept the
    /// targets' copies), but not one that is also the source of a move
    /// ending: it must take the table after that move's target. Nothing if
    /// both are empty.
    pub fn advance(&mut self, done: &[u16], start: &[u16]) {
        let Some(old) = &self.table else { return };
        if done.is_empty() && start.is_empty() {
            return;
        }
        let mut owners = owners(old);
        let mut first = Vec::new();
        let mut handing = Vec::new();
        let mut migrations: Vec<(u16, NodeId, NodeId, bool)> = Vec::new();
        for (slot, from, to, snapshot) in self::migrations(old) {
            if done.contains(&slot) {
                owners[usize::from(slot)] = Some(to);
                first.push(to);
                handing.push(from);
            } else {
                migrations.push((slot, from, to, snapshot));
            }
        }
        for &slot in start {
            let Some(i) = self.queue.iter().position(|(s, _)| *s == slot) else { continue };
            let (_, to) = self.queue.remove(i);
            let Some(from) = owners[usize::from(slot)] else { continue };
            migrations.push((slot, from, to, true));
            if !handing.contains(&from) {
                first.push(from);
            }
        }
        first.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        first.dedup();
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations, &replicas(old), &failed(old)));
        self.first = first;
        self.queue_check();
    }

    /// Calls off the moves by snapshot of `slots` (the slots stay with
    /// their sources, the targets drop what they copied) and queues them
    /// again, last. Nothing if none of them moves so.
    pub fn cancel(&mut self, slots: &[u16]) {
        let Some(old) = &self.table else { return };
        let (cancelled, kept): (Vec<_>, Vec<_>) =
            self::migrations(old).into_iter().partition(|&(slot, _, _, snapshot)| snapshot && slots.contains(&slot));
        if cancelled.is_empty() {
            return;
        }
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners(old), &kept, &replicas(old), &failed(old)));
        self.first = Vec::new();
        for (slot, _, to, _) in cancelled {
            if !self.queue.iter().any(|(s, _)| *s == slot) {
                self.queue.push((slot, to));
            }
        }
        self.queue_check();
    }

    /// The state file's text: a header, a line per node, then the table
    /// (ranges, migrations, replicas, the nodes marked failed), the nodes
    /// it goes to first, and the queue.
    pub fn encode(&self) -> String {
        let mut text = format!("{HEADER}\n");
        for n in &self.nodes {
            text += &format!("node {} {} {}\n", n.id(), n.host(), n.port());
        }
        for n in &self.nodes {
            if let Some(name) = self.name(n.id()) {
                text += &format!("name {} {name}\n", n.id());
            }
        }
        for n in &self.nodes {
            if let Some(zone) = self.zone(n.id()) {
                text += &format!("zone {} {zone}\n", n.id());
            }
        }
        for id in &self.leaving {
            text += &format!("leaving {id}\n");
        }
        for id in &self.handing {
            text += &format!("handing {id}\n");
        }
        if let Some(id) = &self.demoting {
            text += &format!("demoting {id}\n");
        }
        if let Some(t) = &self.table {
            text += &format!("table {}\n", t.epoch());
            for (range, owner) in t.runs() {
                let owner = owner.map_or("-".to_string(), |i| t.nodes()[i].id().to_string());
                text += &format!("range {} {} {owner}\n", range.start, range.end);
            }
            for (slot, from, to, snapshot) in migrations(t) {
                let word = if snapshot { "sync" } else { "migrate" };
                text += &format!("{word} {slot} {from} {to}\n");
            }
            for (replica, primary) in replicas(t) {
                text += &format!("replica {replica} {primary}\n");
            }
            for id in failed(t) {
                text += &format!("failed {id}\n");
            }
            for (id, epoch) in &self.cleared {
                text += &format!("cleared {id} {epoch}\n");
            }
            for (old, new) in &self.successors {
                text += &format!("successor {old} {new}\n");
            }
            for id in &self.first {
                text += &format!("first {id}\n");
            }
            for (slot, to) in &self.queue {
                text += &format!("queue {slot} {to}\n");
            }
        }
        text
    }

    pub fn decode(text: &str) -> Result<State, String> {
        let mut lines = text.lines();
        if lines.next() != Some(HEADER) {
            return Err(format!("the first line isn't {HEADER:?}"));
        }
        let (mut nodes, mut leaving, mut epoch, mut ranges) = (Vec::new(), Vec::new(), None, Vec::new());
        let (mut handing, mut demoting) = (Vec::new(), None);
        let (mut first, mut queue, mut names) = (Vec::new(), Vec::new(), Vec::new());
        let mut syncing = Vec::new();
        let (mut zones, mut replicas, mut failed, mut cleared, mut successors) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let id = |w: &str| NodeId::parse(w.as_bytes()).ok_or(format!("bad node id {w:?}"));
        let num = |w: &str| w.parse::<u64>().map_err(|e| format!("{w:?}: {e}"));
        for (i, line) in lines.enumerate() {
            let words: Vec<&str> = line.split(' ').collect();
            let bad = |e: String| format!("line {}: {e}", i + 2);
            match words[..] {
                ["node", n, host, port] if epoch.is_none() => {
                    let port = u16::try_from(num(port).map_err(bad)?).map_err(|e| bad(e.to_string()))?;
                    nodes.push(Node::new(id(n).map_err(bad)?, host, port).map_err(|e| bad(e.to_string()))?);
                }
                ["name", n, name] if epoch.is_none() && valid_name(name) => names.push((id(n).map_err(bad)?, name.to_string())),
                ["zone", n, zone] if epoch.is_none() && valid_name(zone) => zones.push((id(n).map_err(bad)?, zone.to_string())),
                ["leaving", n] if epoch.is_none() => leaving.push(id(n).map_err(bad)?),
                ["handing", n] if epoch.is_none() => handing.push(id(n).map_err(bad)?),
                ["demoting", n] if epoch.is_none() && demoting.is_none() => demoting = Some(id(n).map_err(bad)?),
                ["table", e] if epoch.is_none() => epoch = Some(num(e).map_err(bad)?),
                ["range", s, e, owner] if epoch.is_some() => {
                    let slot = |w: &str| num(w).and_then(|n| u16::try_from(n).map_err(|e| e.to_string())).map_err(bad);
                    let owner = if owner == "-" { None } else { Some(id(owner).map_err(bad)?) };
                    ranges.push((SlotRange::new(slot(s)?, slot(e)?), owner));
                }
                // Phase 5's `migrate` (key by key): by snapshot now.
                ["migrate" | "sync", s, from, to] if epoch.is_some() => {
                    let slot = num(s).and_then(|n| u16::try_from(n).map_err(|e| e.to_string())).map_err(bad)?;
                    syncing.push((slot, id(from).map_err(bad)?, id(to).map_err(bad)?));
                }
                ["replica", r, p] if epoch.is_some() => replicas.push((id(r).map_err(bad)?, id(p).map_err(bad)?)),
                ["failed", n] if epoch.is_some() => failed.push(id(n).map_err(bad)?),
                ["cleared", n, e] if epoch.is_some() => cleared.push((id(n).map_err(bad)?, num(e).map_err(bad)?)),
                ["successor", p, s] if epoch.is_some() => successors.push((id(p).map_err(bad)?, id(s).map_err(bad)?)),
                ["first", n] if epoch.is_some() => first.push(id(n).map_err(bad)?),
                ["queue", s, to] if epoch.is_some() => {
                    let slot = num(s).and_then(|n| u16::try_from(n).map_err(|e| e.to_string())).map_err(bad)?;
                    queue.push((slot, id(to).map_err(bad)?));
                }
                _ => return Err(bad(format!("unexpected {line:?}"))),
            }
        }
        // Checks the nodes too (ids, addresses), table or not.
        let view = SlotTable::new(epoch.unwrap_or(0), nodes.clone(), &[(SlotRange::new(0, SLOTS as u16 - 1), None)]);
        view.map_err(|e| e.to_string())?;
        if let Some(l) = leaving.iter().find(|l| !nodes.iter().any(|n: &Node| n.id() == *l)) {
            return Err(format!("leaving {l}: not a node"));
        }
        if let Some(h) = handing.iter().find(|h| !leaving.contains(h)) {
            return Err(format!("handing {h}: not leaving"));
        }
        if let Some((n, _)) = names.iter().find(|(n, _)| !nodes.iter().any(|node: &Node| node.id() == n)) {
            return Err(format!("name {n}: not a node"));
        }
        if let Some((n, _)) = zones.iter().find(|(n, _)| !nodes.iter().any(|node: &Node| node.id() == n)) {
            return Err(format!("zone {n}: not a node"));
        }
        let table = match epoch {
            None => None,
            Some(e) => Some(
                SlotTable::new(e, nodes.clone(), &ranges)
                    .and_then(|t| t.with_replicas(&replicas))
                    .and_then(|t| t.with_syncs(&syncing))
                    .and_then(|t| t.with_failed(&failed))
                    .map_err(|e| format!("the table: {e}"))?,
            ),
        };
        if let Some((n, _)) = cleared.iter().find(|(n, _)| !nodes.iter().any(|node: &Node| node.id() == n)) {
            return Err(format!("cleared {n}: not a node"));
        }
        if let Some((p, s)) = successors.iter().find(|(p, s)| [p, s].iter().any(|n| !nodes.iter().any(|node: &Node| node.id() == *n))) {
            return Err(format!("successor {p} {s}: not a node"));
        }
        if let Some(d) = demoting {
            let primary = table.as_ref().is_some_and(|t| t.position(&d).is_some_and(|i| t.primary_of(i).is_none()));
            if leaving.contains(&d) || !primary {
                return Err(format!("demoting {d}: not a primary staying"));
            }
        }
        Ok(State { nodes, leaving, handing, demoting, table, queue, first, names, zones, cleared, successors, replicas: 0 })
    }

    /// Reads the state file; a missing file is a fresh start.
    pub fn load(path: &Path) -> io::Result<State> {
        match fs::read_to_string(path) {
            Ok(text) => State::decode(&text)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {e}", path.display()))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(State::default()),
            Err(e) => Err(e),
        }
    }

    /// Writes the state file so that a crash leaves the old or the new
    /// one, never half of either: a temporary file, flushed to disk, then
    /// renamed over the old one, and the directory flushed.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let tmp = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp)?;
        file.write_all(self.encode().as_bytes())?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
        fs::File::open(dir)?.sync_all()
    }
}

/// How [`spread`] shares the slots out.
enum Goal {
    /// Every node ends with `assigned / n` slots or one more.
    Even,
    /// Only the slots of the other nodes move, each to the node with the
    /// fewest so far (water filling).
    Fill,
}

/// The moves that leave the slots of `owners` with `nodes` only (the other
/// nodes give all theirs), as `goal` says; the slots of `fixed` stay where
/// they are (they are moving). Nodes keep their lowest slots; a receiver
/// takes a run, in `nodes` order. The nodes of `full` receive nothing:
/// what they'd get goes to the others, and slots nobody can take stay.
fn spread(owners: &[Option<NodeId>], fixed: &[bool], nodes: &[NodeId], full: &[NodeId], goal: Goal) -> Vec<(u16, NodeId)> {
    let mine = |id: &NodeId| -> Vec<u16> { (0..SLOTS as u16).filter(|&s| owners[usize::from(s)].as_ref() == Some(id)).collect() };
    let mut have: Vec<(NodeId, usize)> = nodes.iter().map(|id| (*id, mine(id).len())).collect();
    // The slots that leave: the other nodes' (those that can move now).
    let mut leaving: Vec<u16> = (0..SLOTS as u16)
        .filter(|&s| owners[usize::from(s)].is_some_and(|o| !nodes.contains(&o)) && !fixed[usize::from(s)])
        .collect();
    let want: Vec<usize> = match goal {
        Goal::Even => {
            let total = owners.iter().flatten().count();
            let (base, extra) = (total / nodes.len(), total % nodes.len());
            // The extra slots stay with the nodes that have the most.
            let mut order: Vec<usize> = (0..have.len()).collect();
            order.sort_by_key(|&i| std::cmp::Reverse(have[i].1));
            let mut want = vec![base; have.len()];
            for &i in &order[..extra] {
                want[i] += 1;
            }
            // A full node keeps what it has below its share; the rest of
            // its share goes to the open nodes with the fewest.
            let mut short = 0;
            for (i, (id, n)) in have.iter().enumerate() {
                if full.contains(id) && want[i] > *n {
                    short += want[i] - n;
                    want[i] = *n;
                }
            }
            let open: Vec<usize> = (0..have.len()).filter(|&i| !full.contains(&have[i].0)).collect();
            // Ties go to the node that has the most, as the extra slots
            // above: no slot moves just to come back.
            for _ in 0..if open.is_empty() { 0 } else { short } {
                let i = *open.iter().min_by_key(|&&i| (want[i], std::cmp::Reverse(have[i].1), i)).unwrap();
                want[i] += 1;
            }
            // Each node beyond its share gives its highest slots, keeping
            // the moving ones.
            for ((id, _), want) in have.iter().zip(&want) {
                let (moving, free): (Vec<u16>, Vec<u16>) = mine(id).into_iter().partition(|&s| fixed[usize::from(s)]);
                let kept_moving = moving.len();
                leaving.extend(&free[want.saturating_sub(kept_moving).min(free.len())..]);
            }
            leaving.sort_unstable();
            want
        }
        Goal::Fill => {
            let mut want: Vec<usize> = have.iter().map(|(_, n)| *n).collect();
            let open: Vec<usize> = (0..have.len()).filter(|&i| !full.contains(&have[i].0)).collect();
            for _ in 0..if open.is_empty() { 0 } else { leaving.len() } {
                let i = *open.iter().min_by_key(|&&i| (want[i], i)).unwrap();
                want[i] += 1;
            }
            want
        }
    };
    let mut moves = Vec::with_capacity(leaving.len());
    let mut slots = leaving.into_iter();
    for ((id, n), want) in have.iter_mut().zip(&want) {
        while *n < *want {
            let Some(slot) = slots.next() else { break };
            moves.push((slot, *id));
            *n += 1;
        }
    }
    moves
}

/// Every slot's owner once the moves under way have ended.
fn owners_after(t: &SlotTable) -> Vec<Option<NodeId>> {
    let mut owners = owners(t);
    for m in t.migrations() {
        owners[usize::from(m.slot)] = Some(*t.nodes()[m.to].id());
    }
    owners
}

/// Whether each slot is moving.
fn moving(t: &SlotTable) -> Vec<bool> {
    let mut moving = vec![false; SLOTS];
    for m in t.migrations() {
        moving[usize::from(m.slot)] = true;
    }
    moving
}

/// Every slot's owner.
fn owners(t: &SlotTable) -> Vec<Option<NodeId>> {
    (0..SLOTS as u16).map(|s| t.owner(s).map(|n| *n.id())).collect()
}

/// The table's migrations, by id, and whether each is by snapshot.
fn migrations(t: &SlotTable) -> Vec<(u16, NodeId, NodeId, bool)> {
    t.migrations().iter().map(|m| (m.slot, *t.nodes()[m.from].id(), *t.nodes()[m.to].id(), m.snapshot)).collect()
}

/// The table's replicas, by id, as `(replica, primary)`.
fn replicas(t: &SlotTable) -> Vec<(NodeId, NodeId)> {
    t.replicas().iter().map(|r| (*t.nodes()[r.replica].id(), *t.nodes()[r.primary].id())).collect()
}

/// The nodes the table marks failed, by id.
fn failed(t: &SlotTable) -> Vec<NodeId> {
    t.failed().iter().map(|&i| *t.nodes()[i].id()).collect()
}

/// A table from every slot's owner (all of them known nodes), in ranges.
fn table_from(
    epoch: u64,
    nodes: Vec<Node>,
    owners: &[Option<NodeId>],
    migrations: &[(u16, NodeId, NodeId, bool)],
    replicas: &[(NodeId, NodeId)],
    failed: &[NodeId],
) -> SlotTable {
    let by = |snapshot: bool| -> Vec<(u16, NodeId, NodeId)> {
        migrations.iter().filter(|m| m.3 == snapshot).map(|&(slot, from, to, _)| (slot, from, to)).collect()
    };
    // SlotTable::new merges nothing itself, runs do.
    let mut ranges: Vec<(SlotRange, Option<NodeId>)> = Vec::new();
    for (slot, owner) in owners.iter().enumerate() {
        match ranges.last_mut() {
            Some((r, o)) if o == owner => r.end = slot as u16,
            _ => ranges.push((SlotRange::new(slot as u16, slot as u16), *owner)),
        }
    }
    SlotTable::new(epoch, nodes, &ranges)
        .and_then(|t| t.with_replicas(replicas))
        .and_then(|t| t.with_migrations(&by(false)))
        .and_then(|t| t.with_syncs(&by(true)))
        .and_then(|t| t.with_failed(failed))
        .expect("a table of the joined nodes, migrating from the owners, replicas owning nothing, failed nodes known")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> NodeId {
        NodeId::from_bytes(&[n; 20])
    }

    fn node(n: u8) -> Node {
        Node::new(id(n), "10.0.0.1", 7000 + u16::from(n)).unwrap()
    }

    fn ranges(t: &SlotTable) -> Vec<(u16, u16, Option<u8>)> {
        // id(n) is "0n0n...": n is the second hex digit, for n < 10.
        t.runs().map(|(r, o)| (r.start, r.end, o.map(|i| t.nodes()[i].id().as_bytes()[1] - b'0'))).collect()
    }

    fn joined(n: u8) -> State {
        let mut s = State::default();
        for i in 1..=n {
            assert_eq!(s.join(node(i)).change, Change::Added);
        }
        s
    }

    #[test]
    fn create_splits_like_redis_cli() {
        let mut s = joined(3);
        assert_eq!(s.view().nodes().len(), 3);
        assert_eq!(s.view().assigned(), 0);
        s.create().unwrap();
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), 1);
        assert_eq!(ranges(t), [(0, 5460, Some(1)), (5461, 10922, Some(2)), (10923, 16383, Some(3))]);
        assert_eq!(s.create().unwrap_err(), "ERR The slots were assigned already (table epoch 1)");

        for n in [1u8, 2, 4, 5, 7] {
            let mut s = joined(n);
            s.create().unwrap();
            let t = s.table().unwrap();
            assert_eq!(t.assigned(), SLOTS, "{n} nodes");
            let sizes: Vec<usize> = t.ranges_by_node().iter().map(|r| r.iter().map(|r| r.len()).sum()).collect();
            let (min, max) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
            assert!(max - min <= 1, "{n} nodes: {sizes:?}");
            assert!(t.ranges_by_node().iter().all(|r| r.len() == 1), "one range each");
        }
        assert_eq!(State::default().create().unwrap_err(), "ERR No node has joined yet");
    }

    #[test]
    fn joins() {
        let mut s = joined(2);
        assert_eq!(s.join(node(1)), Joined { change: Change::Known, replaced: vec![], primary: None });
        s.create().unwrap();
        assert_eq!(s.join(node(2)).change, Change::Known);
        assert_eq!(s.table().unwrap().epoch(), 1, "nothing changed");

        // A third node: in the table, without slots.
        assert_eq!(s.join(node(3)).change, Change::Added);
        let t = s.table().unwrap();
        assert_eq!((t.epoch(), t.nodes().len()), (2, 3));
        assert_eq!(ranges(t), [(0, 8191, Some(1)), (8192, 16383, Some(2))]);

        // Node 2 moves.
        let moved = Node::new(id(2), "10.0.0.9", 7002).unwrap();
        assert_eq!(s.join(moved.clone()).change, Change::Moved);
        assert_eq!(s.table().unwrap().epoch(), 3);
        assert_eq!(s.table().unwrap().nodes()[1], moved);

        // Node 1 restarted: a new id at its address. Its slots are lost.
        let restarted = Node::new(id(4), "10.0.0.1", 7001).unwrap();
        let j = s.join(restarted);
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(1), 8192)], primary: None });
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), 4);
        assert_eq!(t.nodes().iter().map(|n| *n.id()).collect::<Vec<_>>(), [id(2), id(3), id(4)]);
        assert_eq!(ranges(t), [(0, 8191, None), (8192, 16383, Some(2))]);
        assert_eq!(s.nodes().len(), 3);

        // Before a table, a replaced node just goes.
        let mut s = joined(2);
        let j = s.join(Node::new(id(5), "10.0.0.1", 7002).unwrap());
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(2), 0)], primary: None });
        assert_eq!(s.nodes().iter().map(|n| *n.id()).collect::<Vec<_>>(), [id(1), id(5)]);
        assert!(s.table().is_none());
    }

    /// A node that joins with another's name replaces it, wherever it is
    /// (a pod restarted with a new address); without a name, or with
    /// another one, it doesn't.
    #[test]
    fn joins_with_names() {
        let at = |n: u8, host: &str| Node::new(id(n), host, 7000).unwrap();
        let mut s = State::default();
        s.join_named(at(1, "10.0.0.1"), Some("pod-a"));
        s.join_named(at(2, "10.0.0.2"), Some("pod-b"));
        s.create().unwrap();
        assert_eq!((s.name(&id(1)), s.name(&id(2))), (Some("pod-a"), Some("pod-b")));

        // pod-a again, as it joined: nothing changes.
        assert_eq!(s.join_named(at(1, "10.0.0.1"), Some("pod-a")), Joined { change: Change::Known, replaced: vec![], primary: None });
        // Another node, unnamed or with another name, at another address.
        assert_eq!(s.join(at(3, "10.0.0.3")).replaced, []);
        assert_eq!(s.join_named(at(4, "10.0.0.4"), Some("pod-c")).replaced, []);

        // pod-a restarted at another address: its slots are lost.
        let j = s.join_named(at(5, "10.0.0.9"), Some("pod-a"));
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(1), 8192)], primary: None });
        assert_eq!(ranges(s.table().unwrap())[0], (0, 8191, None));
        assert_eq!((s.name(&id(1)), s.name(&id(5))), (None, Some("pod-a")), "the old node's name goes");
        let ids = s.nodes().iter().map(|n| *n.id()).collect::<Vec<_>>();
        assert_eq!(ids, [id(2), id(3), id(4), id(5)]);

        // The address of a node still counts: pod-d at pod-b's address.
        assert_eq!(s.join_named(at(6, "10.0.0.2"), Some("pod-d")).replaced, [(id(2), 8192)]);
        assert!(s.encode().contains(&format!("\nname {} pod-d\n", id(6))));
        assert!(!s.encode().contains("pod-b"));
    }

    #[test]
    fn names_in_the_state_file() {
        let n = id(1);
        let ok = format!("stuart-state 1\nnode {n} 10.0.0.1 7000\nname {n} pod-a\n");
        assert_eq!(State::decode(&ok).unwrap().name(&n), Some("pod-a"));
        let other = id(2);
        assert!(State::decode(&format!("stuart-state 1\nnode {n} 10.0.0.1 7000\nname {other} pod-a\n")).unwrap_err().contains("not a node"));
        assert!(State::decode(&format!("stuart-state 1\nnode {n} 10.0.0.1 7000\nname {n} \x01\n")).is_err());
        assert!(valid_name("speedvault-0") && !valid_name("") && !valid_name("a b") && !valid_name(&"x".repeat(256)));
    }

    #[test]
    fn assign_gives_lost_slots_to_a_node() {
        let mut s = joined(2);
        let r = SlotRange::new;
        assert_eq!(s.assign(r(0, 10), &id(1)).unwrap_err(), "ERR There is no table yet: STUART CREATE makes it");
        s.create().unwrap();
        // Node 1 restarts as node 3: 0-8191 are lost.
        s.join(Node::new(id(3), "10.0.0.1", 7001).unwrap());
        assert_eq!(s.table().unwrap().epoch(), 2);
        let owned = format!("ERR Slot 8192 is served by {}: only unassigned slots can be assigned", id(2));
        assert_eq!(s.assign(r(8000, 8192), &id(3)).unwrap_err(), owned);
        assert_eq!(s.assign(r(0, 10), &id(1)).unwrap_err(), format!("ERR Unknown node {}", id(1)));
        assert_eq!(s.assign(r(10, 9), &id(3)).unwrap_err(), "ERR Invalid slot range 10-9");
        assert_eq!(s.assign(r(0, 16384), &id(3)).unwrap_err(), "ERR Invalid slot range 0-16384");
        assert_eq!(s.table().unwrap().epoch(), 2, "refused: nothing changed");

        // In two steps, to two nodes; adjacent runs merge.
        assert_eq!(s.assign(r(0, 99), &id(2)), Ok(100));
        assert_eq!(s.assign(r(100, 8191), &id(3)), Ok(8092));
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), 4);
        assert_eq!(ranges(t), [(0, 99, Some(2)), (100, 8191, Some(3)), (8192, 16383, Some(2))]);
        assert_eq!(t.assigned(), SLOTS);
        assert_eq!(s.assign(r(5, 5), &id(3)).unwrap_err(), format!("ERR Slot 5 is served by {}: only unassigned slots can be assigned", id(2)));
        assert_eq!(State::decode(&s.encode()).unwrap(), s);
    }

    #[test]
    fn encode_and_decode() {
        let empty = State::default();
        assert_eq!(empty.encode(), "stuart-state 1\n");
        assert_eq!(State::decode(&empty.encode()).unwrap(), empty);
        let mut s = joined(3);
        assert_eq!(State::decode(&s.encode()).unwrap(), s);
        s.create().unwrap();
        s.join(Node::new(id(4), "10.0.0.1", 7001).unwrap());
        let text = s.encode();
        assert_eq!(State::decode(&text).unwrap(), s);
        assert!(text.contains("\ntable 2\nrange 0 5460 -\nrange 5461 10922 0202"), "{text}");

        for (bad, why) in [
            ("", "first line"),
            ("stuart-state 2\n", "first line"),
            ("stuart-state 1\nnode x 10.0.0.1 1\n", "bad node id"),
            ("stuart-state 1\nnode 0101010101010101010101010101010101010101 10.0.0.1 70000\n", "line 2"),
            ("stuart-state 1\ntable 1\nrange 0 16383 -\nnode 0101010101010101010101010101010101010101 h 1\n", "unexpected"),
            ("stuart-state 1\ntable 1\nrange 0 100 -\n", "the table: slot 101"),
            ("stuart-state 1\nnode 0101010101010101010101010101010101010101 h 1\nnode 0101010101010101010101010101010101010101 h 2\n", "listed twice"),
            ("stuart-state 1\nfoo\n", "unexpected"),
        ] {
            let err = State::decode(bad).unwrap_err();
            assert!(err.contains(why), "{bad:?}: {err}");
        }
    }

    /// (slot, from, to) of the table's migrations, nodes by number.
    fn moving(s: &State) -> Vec<(u16, u8, u8)> {
        let n = |id: &NodeId| id.as_bytes()[1] - b'0';
        migrations(s.table().unwrap()).iter().map(|(slot, f, t, _)| (*slot, n(f), n(t))).collect()
    }

    #[test]
    fn moves_are_queued_started_and_ended() {
        let mut s = joined(2);
        let r = SlotRange::new;
        assert_eq!(s.move_slots(r(0, 10), &id(1)).unwrap_err(), "ERR There is no table yet: STUART CREATE makes it");
        s.create().unwrap();
        s.join(node(3));
        assert_eq!(s.table().unwrap().epoch(), 2);
        // 8190-8193: two of node 1's slots, two of node 2's; to node 2.
        assert_eq!(s.move_slots(r(8190, 8193), &id(2)), Ok(2), "node 2's own slots don't move");
        assert_eq!(s.queue(), [(8190, id(2)), (8191, id(2))]);
        assert_eq!(s.move_slots(r(8191, 8191), &id(3)).unwrap_err(), "ERR Slot 8191 is moving already");
        assert_eq!(s.move_slots(r(0, 0), &id(9)).unwrap_err(), format!("ERR Unknown node {}", id(9)));
        assert_eq!(s.move_slots(r(5, 4), &id(3)).unwrap_err(), "ERR Invalid slot range 5-4");
        assert_eq!(s.move_slots(r(0, 16384), &id(3)).unwrap_err(), "ERR Invalid slot range 0-16384");
        assert_eq!(s.table().unwrap().epoch(), 2, "queueing makes no table");

        // One starts: the table migrates it, its source first.
        s.advance(&[], &[8190]);
        assert_eq!(s.table().unwrap().epoch(), 3);
        assert_eq!(moving(&s), [(8190, 1, 2)]);
        assert_eq!(s.first(), [id(1)]);
        assert_eq!(s.queue(), [(8191, id(2))]);
        assert_eq!(s.table().unwrap().owner(8190).unwrap().id(), &id(1), "the source still owns it");
        // It ends and the next starts, in one table: the cutover's target
        // first (not the source, though it starts a move).
        s.advance(&[8190], &[8191]);
        assert_eq!(s.table().unwrap().epoch(), 4);
        assert_eq!(moving(&s), [(8191, 1, 2)]);
        assert_eq!(ranges(s.table().unwrap()), [(0, 8189, Some(1)), (8190, 8190, Some(2)), (8191, 8191, Some(1)), (8192, 16383, Some(2))]);
        assert_eq!(s.first(), [id(2)]);
        assert!(s.queue().is_empty());
        assert_eq!(State::decode(&s.encode()).unwrap(), s, "saved with its migrations");
        s.advance(&[8191], &[]);
        assert!(moving(&s).is_empty());
        assert_eq!(ranges(s.table().unwrap()), [(0, 8189, Some(1)), (8190, 16383, Some(2))]);
        s.advance(&[], &[]);
        assert_eq!(s.table().unwrap().epoch(), 5, "nothing to do: no table");

        // Several sources go first; a node's own slot is never queued.
        s.move_slots(r(0, 1), &id(3)).unwrap();
        s.move_slots(r(9000, 9000), &id(1)).unwrap();
        s.advance(&[], &[0, 9000]);
        assert_eq!(moving(&s), [(0, 1, 3), (9000, 2, 1)]);
        assert_eq!(s.first(), [id(1), id(2)]);
        let text = s.encode();
        assert!(text.contains("\nsync 0 0101") && text.contains("\nfirst 0101") && text.contains("\nqueue 1 0303"), "{text}");
        assert_eq!(State::decode(&text).unwrap(), s);
    }

    /// A cutover and moves starting by snapshot in one table: the
    /// cutover's target goes first, and the new moves' sources, but not
    /// the cutover's source (it must take the table after its target).
    #[test]
    fn a_cutover_and_starts_in_one_table() {
        let r = SlotRange::new;
        let mut s = State::default();
        for i in 1..=3 {
            s.join(node(i));
        }
        s.create().unwrap();
        s.move_slots(r(0, 1), &id(2)).unwrap();
        s.move_slots(r(11000, 11000), &id(2)).unwrap();
        s.advance(&[], &[0]);
        assert_eq!(s.first(), [id(1)]);
        s.advance(&[0], &[1, 11000]);
        assert_eq!(moving(&s), [(1, 1, 2), (11000, 3, 2)]);
        assert_eq!(s.table().unwrap().owner(0).unwrap().id(), &id(2));
        assert_eq!(s.first(), [id(2), id(3)], "not the cutover's source");
        s.advance(&[1, 11000], &[]);
        assert!(moving(&s).is_empty());
        assert_eq!(s.first(), [id(2)]);
    }

    /// Moves by snapshot: started with the source first, saved as `sync`,
    /// ended by `advance` (the target first), called off and queued
    /// again by `cancel`; a failover of their source queues them again.
    #[test]
    fn moves_by_snapshot() {
        let r = SlotRange::new;
        let mut s = with_replicas(1);
        for i in 1..=4 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(3, 1), (4, 2)]);
        s.move_slots(r(0, 2), &id(2)).unwrap();
        s.advance(&[], &[0, 1, 2]);
        let snapshot = |s: &State| s.table().unwrap().migrations().iter().all(|m| m.snapshot);
        assert_eq!(moving(&s), [(0, 1, 2), (1, 1, 2), (2, 1, 2)]);
        assert!(snapshot(&s));
        assert_eq!(s.first(), [id(1)], "the source first");
        let text = s.encode();
        assert!(text.contains("\nsync 0 0101"), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        // A node removed (here a replica forgotten) leaves them by snapshot.
        assert_eq!(s.forget(&id(4)), Ok(0));
        assert_eq!(moving(&s), [(0, 1, 2), (1, 1, 2), (2, 1, 2)]);
        assert!(snapshot(&s), "still by snapshot after a removal");

        // Two cut over, the target first; one called off and queued again.
        let epoch = s.table().unwrap().epoch();
        s.advance(&[0, 1], &[]);
        assert_eq!(s.table().unwrap().epoch(), epoch + 1);
        assert_eq!(moving(&s), [(2, 1, 2)]);
        assert_eq!(ranges(s.table().unwrap())[..2], [(0, 1, Some(2)), (2, 8191, Some(1))]);
        assert_eq!(s.first(), [id(2)]);
        s.cancel(&[2]);
        assert_eq!(s.table().unwrap().epoch(), epoch + 2);
        assert!(moving(&s).is_empty());
        assert_eq!(s.table().unwrap().owner(2).unwrap().id(), &id(1), "the source keeps it");
        assert_eq!(s.queue(), [(2, id(2))]);
        assert!(s.first().is_empty());

        // Its source failed over: queued again, from the new primary.
        s.advance(&[], &[2]);
        s.move_slots(r(9000, 9000), &id(1)).unwrap();
        s.advance(&[], &[9000]);
        assert_eq!(moving(&s), [(2, 1, 2), (9000, 2, 1)]);
        assert!(s.promote(&id(1), &id(3)));
        assert!(moving(&s).is_empty());
        assert_eq!(s.queue(), [(2, id(2))], "the move from 1 queued again; the one to 1 dropped");
        assert_eq!(s.table().unwrap().owner(2).unwrap().id(), &id(3));
        // A state file of phase 5: its moves key by key read as by snapshot.
        s.advance(&[], &[2]);
        let old = s.encode().replace("\nsync ", "\nmigrate ");
        assert!(old.contains("\nmigrate 2 "), "{old}");
        let mut back = State::decode(&old).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        assert!(snapshot(&back));
    }

    #[test]
    fn nodes_leaving_end_their_moves() {
        let r = SlotRange::new;
        let mut s = joined(3);
        s.create().unwrap();
        // 0-5460 node 1, 5461-10922 node 2, 10923-16383 node 3.
        s.move_slots(r(0, 2), &id(2)).unwrap();
        s.move_slots(r(5461, 5461), &id(3)).unwrap();
        s.advance(&[], &[0, 5461]);
        assert_eq!(moving(&s), [(0, 1, 2), (5461, 2, 3)]);
        // Node 1 restarts (a new id at its address): slot 0, migrating from
        // it, goes to node 2 with what got there; its other slots are lost;
        // its queued moves go too, having nothing to move.
        s.join(Node::new(id(4), "10.0.0.1", 7001).unwrap());
        assert_eq!(moving(&s), [(5461, 2, 3)]);
        let t = s.table().unwrap();
        assert_eq!(t.owner(0).unwrap().id(), &id(2));
        assert!(t.owner(1).is_none() && t.owner(5460).is_none());
        assert!(s.queue().is_empty(), "{:?}", s.queue());
        assert_eq!(s.first(), [id(2)], "slot 0 is node 2's now: it gets the table first");
        // Node 3, a target, restarts: the move ends, the slot stays at node 2.
        s.join(Node::new(id(5), "10.0.0.1", 7003).unwrap());
        assert!(moving(&s).is_empty());
        assert_eq!(s.table().unwrap().owner(5461).unwrap().id(), &id(2));
        // Moves to a node that leaves are dropped.
        s.move_slots(r(5462, 5463), &id(4)).unwrap();
        s.join(Node::new(id(6), "10.0.0.1", 7004).unwrap());
        assert_eq!(s.queue().len(), 2);
        s.join(Node::new(id(7), "10.0.0.1", 7004).unwrap());
        assert_eq!(s.queue().len(), 2, "another node replaced: the moves to node 4 stay");
        s.join(Node::new(id(8), "10.0.0.1", 7001).unwrap());
        assert!(s.queue().is_empty(), "node 4 replaced: its moves go");
        assert_eq!(State::decode(&s.encode()).unwrap(), s);
        assert_eq!(s.move_slots(r(1, 1), &id(2)).unwrap_err(), "ERR Slot 1 is unassigned: STUART ASSIGN gives it to a node");
    }

    /// Per node (by number), how many slots it has once the moves under way
    /// and the queue are done.
    fn after_queue(s: &State) -> Vec<(u8, usize)> {
        let t = s.table().unwrap();
        let mut owners = owners_after(t);
        for (slot, to) in s.queue() {
            owners[usize::from(*slot)] = Some(*to);
        }
        let mut counts: Vec<(u8, usize)> = s.nodes().iter().map(|n| (n.id().as_bytes()[1] - b'0', 0)).collect();
        for o in owners.iter().flatten() {
            counts.iter_mut().find(|(n, _)| *n == o.as_bytes()[1] - b'0').unwrap().1 += 1;
        }
        counts
    }

    #[test]
    fn rebalance_evens_out_with_few_moves() {
        let mut s = joined(3);
        assert_eq!(s.rebalance(&[]).unwrap_err(), "ERR There is no table yet: STUART CREATE makes it");
        s.create().unwrap();
        assert_eq!(s.rebalance(&[]), Ok(0), "even already");
        // A fourth node: each of the three gives a quarter of its slots.
        s.join(node(4));
        assert_eq!(s.rebalance(&[]), Ok(4096));
        assert_eq!(after_queue(&s), [(1, 4096), (2, 4096), (3, 4096), (4, 4096)]);
        // Every slot moves at most once, and each giver gives its highest.
        let mut slots: Vec<u16> = s.queue().iter().map(|(slot, _)| *slot).collect();
        slots.dedup();
        assert_eq!(slots.len(), 4096);
        assert!(s.queue().iter().all(|(_, to)| *to == id(4)));
        assert_eq!(s.queue()[0].0, 4096, "node 1 keeps 0-4095");
        let queued = s.queue().to_vec();
        assert_eq!(s.rebalance(&[]), Ok(4096), "planned again: the same plan");
        assert_eq!(s.queue(), queued);

        // Uneven counts and 5 nodes: the remainder stays with the biggest.
        let mut s = joined(2);
        s.create().unwrap();
        for n in 3..=5 {
            s.join(node(n));
        }
        assert_eq!(s.rebalance(&[]), Ok(16384 - 2 * 3277));
        let counts: Vec<usize> = after_queue(&s).iter().map(|(_, c)| *c).collect();
        assert_eq!(counts, [3277, 3277, 3277, 3277, 3276]);
        // Receivers get runs: few ranges in the end.
        s.advance(&[], &[]);
        let t = s.table().unwrap();
        let mut owners = owners(t);
        for (slot, to) in s.queue() {
            owners[usize::from(*slot)] = Some(*to);
        }
        let ranges = owners.windows(2).filter(|w| w[0] != w[1]).count() + 1;
        assert!(ranges <= 8, "{ranges} ranges");
    }

    #[test]
    fn full_nodes_receive_nothing() {
        // A fourth node joins while node 2 is full: node 2 gives its part
        // like the others (a full node may give), and receives nothing.
        let mut s = joined(3);
        s.create().unwrap();
        s.join(node(4));
        assert_eq!(s.rebalance(&[id(2)]), Ok(4096));
        assert_eq!(after_queue(&s), [(1, 4096), (2, 4096), (3, 4096), (4, 4096)]);
        // The new node is the full one: nothing moves.
        assert_eq!(s.rebalance(&[id(4)]), Ok(0));
        assert_eq!(after_queue(&s), [(1, 5461), (2, 5462), (3, 5461), (4, 0)]);
        // Node 1 leaving, node 2 full: everything goes to node 3.
        let mut s = joined(3);
        s.create().unwrap();
        assert_eq!(s.leave(&id(1), &[id(2)]), Ok(5461 + 1));
        assert_eq!(after_queue(&s), [(1, 0), (2, 5462), (3, 10922)]);
        // And replanned (--auto) the same way.
        assert_eq!(s.rebalance(&[id(2)]), Ok(5461));
        assert_eq!(after_queue(&s), [(1, 0), (2, 5462), (3, 10922)]);
        // Every receiver full: the slots stay where they are.
        let mut s = joined(3);
        s.create().unwrap();
        assert_eq!(s.drain(&id(1), &[id(2), id(3)]), Ok(0));
        assert_eq!(after_queue(&s), [(1, 5461), (2, 5462), (3, 5461)]);
        s.join(node(4));
        assert_eq!(s.rebalance(&[id(4)]), Ok(0));
    }

    #[test]
    fn drain_moves_one_nodes_slots_only() {
        let mut s = joined(3);
        s.create().unwrap();
        s.join(node(4));
        // Node 4 has nothing: draining it moves nothing.
        assert_eq!(s.drain(&id(4), &[]), Ok(0));
        assert_eq!(s.drain(&id(1), &[]), Ok(5461));
        assert!(s.queue().iter().all(|(slot, _)| *slot <= 5460), "only node 1's slots");
        // Node 4 (empty) fills up to the others' level: all go there.
        assert_eq!(after_queue(&s), [(1, 0), (2, 5462), (3, 5461), (4, 5461)]);
        assert_eq!(s.drain(&id(1), &[]), Ok(0), "queued already");
        assert_eq!(s.drain(&id(9), &[]).unwrap_err(), format!("ERR Unknown node {}", id(9)));
        // Two receivers alike: half each.
        let mut s = joined(3);
        s.create().unwrap();
        assert_eq!(s.drain(&id(1), &[]), Ok(5461));
        assert_eq!(after_queue(&s), [(1, 0), (2, 8192), (3, 8192)]);
        let mut one = joined(1);
        one.create().unwrap();
        assert_eq!(one.drain(&id(1), &[]).unwrap_err(), "ERR No other node to move the slots to");
        assert_eq!(one.drain(&id(9), &[]).unwrap_err(), format!("ERR Unknown node {}", id(9)));
    }

    #[test]
    fn plans_leave_moving_slots_alone() {
        let r = SlotRange::new;
        let mut s = joined(3);
        s.create().unwrap();
        // Ten of node 1's slots move to node 2, then a fourth node joins.
        s.move_slots(r(0, 9), &id(2)).unwrap();
        s.advance(&[], &(0..10).collect::<Vec<u16>>());
        s.join(node(4));
        let n = s.rebalance(&[]).unwrap();
        assert!(s.queue().iter().all(|(slot, _)| *slot >= 10), "a moving slot queued again");
        assert_eq!(n, s.queue().len());
        let counts: Vec<usize> = after_queue(&s).iter().map(|(_, c)| *c).collect();
        assert_eq!(counts.iter().sum::<usize>(), SLOTS);
        assert!(counts.iter().all(|&c| c == 4096), "{counts:?}");
        // Once they arrived, planning again changes nothing.
        let queued = s.queue().to_vec();
        s.advance(&(0..10).collect::<Vec<u16>>(), &[]);
        s.rebalance(&[]).unwrap();
        assert_eq!(s.queue(), queued);
    }

    #[test]
    fn a_node_leaves() {
        let r = SlotRange::new;
        let mut s = joined(4);
        s.create().unwrap();
        // 4096 slots each. Node 1 leaves: its slots go to the others.
        assert_eq!(s.leave(&id(1), &[]), Ok(4096 + 1));
        assert_eq!(s.leaving(), [id(1)]);
        assert_eq!(s.leave(&id(1), &[]), Ok(4096 + 1), "asked again: nothing more queued");
        assert_eq!(s.queue().len(), 4096);
        assert_eq!(after_queue(&s), [(1, 0), (2, 5462), (3, 5461), (4, 5461)]);
        assert_eq!(s.move_slots(r(5000, 5000), &id(1)).unwrap_err(), format!("ERR Node {} is leaving", id(1)));
        // Planned as REBALANCE plans (what --auto does): the same.
        s.rebalance(&[]).unwrap();
        assert_eq!(after_queue(&s), [(1, 0), (2, 5462), (3, 5461), (4, 5461)]);
        assert!(s.queue().iter().all(|(slot, _)| *slot < 4096), "only node 1 gives");
        assert_eq!(State::decode(&s.encode()).unwrap(), s, "saved as leaving");
        assert!(s.encode().contains(&format!("\nleaving {}\n", id(1))));
        assert!(!s.remove_left(&id(1)), "it holds slots");

        // The moves start and end; then it holds nothing, but is a node
        // until removed.
        let slots: Vec<u16> = s.queue().iter().map(|(slot, _)| *slot).collect();
        s.advance(&[], &slots);
        assert_eq!(s.leave(&id(1), &[]), Ok(4096 + 1));
        s.advance(&slots, &[]);
        assert_eq!(s.holding(&id(1)), 0);
        assert_eq!(s.leave(&id(1), &[]), Ok(1));
        let epoch = s.table().unwrap().epoch();
        assert!(s.remove_left(&id(1)));
        assert_eq!(s.table().unwrap().epoch(), epoch + 1);
        assert_eq!(s.nodes().len(), 3);
        assert!(s.leaving().is_empty());
        assert_eq!(s.leave(&id(1), &[]), Ok(0), "gone");
        assert!(!s.remove_left(&id(2)), "not leaving");

        // A node that is a target when it starts leaving: what arrives is
        // drained again.
        let mut s = joined(3);
        s.create().unwrap();
        s.join(node(4));
        s.move_slots(r(0, 1), &id(4)).unwrap();
        s.advance(&[], &[0, 1]);
        assert_eq!(s.leave(&id(4), &[]), Ok(2 + 1), "two slots on their way to it");
        assert_eq!(s.drain_leaving(&[]), 0, "nothing to drain yet");
        s.advance(&[0, 1], &[]);
        assert_eq!(s.drain_leaving(&[]), 2);
        assert!(s.queue().iter().all(|(slot, to)| *slot <= 1 && *to != id(4)), "{:?}", s.queue());

        // Once every node is leaving (the last one too), they all get 0: the
        // cluster is shutting down. Without a table a node leaves at once.
        let mut one = joined(1);
        one.create().unwrap();
        assert_eq!(one.leave(&id(1), &[]), Ok(0));
        assert!(one.shutting_down());
        let mut three = joined(3);
        three.create().unwrap();
        assert_eq!(three.leave(&id(1), &[]), Ok(5461 + 1));
        assert_eq!(three.leave(&id(2), &[]), Ok(5462 + 1), "node 1's slots queued to it go to node 3 instead");
        assert!(three.queue().iter().all(|(_, to)| *to == id(3)));
        assert!(!three.shutting_down());
        assert_eq!(three.leave(&id(3), &[]), Ok(0));
        assert_eq!(three.leave(&id(1), &[]), Ok(0));
        let mut two = joined(2);
        assert_eq!(two.leave(&id(1), &[]), Ok(0));
        assert_eq!(two.nodes().len(), 1);
        assert!(State::decode("stuart-state 1\nleaving 0101010101010101010101010101010101010101\n").unwrap_err().contains("not a node"));
    }

    #[test]
    fn forget_and_unassigned_slots() {
        let r = SlotRange::new;
        let mut s = joined(3);
        s.create().unwrap();
        // Slot 0 moves from node 1 to node 2; node 1 is forgotten.
        s.move_slots(r(0, 0), &id(2)).unwrap();
        s.advance(&[], &[0]);
        assert_eq!(s.forget(&id(1)), Ok(5460), "slot 0 goes to its target");
        assert_eq!(s.forget(&id(1)).unwrap_err(), format!("ERR Unknown node {}", id(1)));
        let t = s.table().unwrap();
        assert_eq!(t.owner(0).unwrap().id(), &id(2));
        assert_eq!(t.assigned(), SLOTS - 5460);
        assert!(moving(&s).is_empty());
        // The unassigned slots go to the nodes with the fewest: a node that
        // joins (empty) gets them, in one run; the others need nothing.
        s.join(node(4));
        assert_eq!(s.assign_unassigned(), 5460);
        let t = s.table().unwrap();
        assert_eq!(t.assigned(), SLOTS);
        assert_eq!(ranges(t), [(0, 0, Some(2)), (1, 5460, Some(4)), (5461, 10922, Some(2)), (10923, 16383, Some(3))]);
        assert_eq!(s.assign_unassigned(), 0);
        assert_eq!(s.rebalance(&[]), Ok(1), "5462 5461 5461: one moves");
        // Leaving nodes get none.
        let mut s = joined(3);
        s.create().unwrap();
        s.leave(&id(3), &[]).unwrap();
        s.join(Node::new(id(5), "10.0.0.1", 7001).unwrap());
        assert_eq!(s.assign_unassigned(), 5461);
        assert_eq!(s.table().unwrap().owner(0).unwrap().id(), &id(5));
        assert!(s.queue().iter().all(|(_, to)| *to != id(3)));
    }

    #[test]
    fn decode_checks_migrations() {
        let mut s = joined(2);
        s.create().unwrap();
        s.move_slots(SlotRange::new(0, 0), &id(2)).unwrap();
        s.advance(&[], &[0]);
        let text = s.encode();
        for word in ["sync", "migrate"] {
            let bad = text.replace("sync 0 0101", &format!("{word} 9000 0101"));
            assert!(State::decode(&bad).unwrap_err().contains("slot 9000 can only migrate from its owner"), "{word}");
        }
        let bad = text.replace("\ntable", "\nmigrate 0 x y\ntable");
        assert!(State::decode(&bad).unwrap_err().contains("unexpected"));
    }

    #[test]
    fn save_and_load() {
        let dir = std::env::temp_dir().join(format!("stuart-state-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state");
        let _ = fs::remove_file(&path);
        assert_eq!(State::load(&path).unwrap(), State::default(), "no file: a fresh start");
        let mut s = joined(2);
        s.join_named(node(3), Some("speedvault-2"));
        s.create().unwrap();
        s.save(&path).unwrap();
        assert_eq!(State::load(&path).unwrap(), s);
        assert_eq!(State::load(&path).unwrap().name(&id(3)), Some("speedvault-2"));
        assert!(!path.with_extension("tmp").exists());
        fs::write(&path, "garbage").unwrap();
        assert_eq!(State::load(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// The table's replicas as `(replica, primary)` by node number.
    fn pairs(s: &State) -> Vec<(u8, u8)> {
        let t = s.table().unwrap();
        let n = |i: usize| t.nodes()[i].id().as_bytes()[1] - b'0';
        t.replicas().iter().map(|r| (n(r.replica), n(r.primary))).collect()
    }

    fn with_replicas(n: usize) -> State {
        let mut s = State::default();
        s.set_replicas(n);
        s
    }

    #[test]
    fn create_with_replicas() {
        // 6 nodes, 1 replica each: the first 3 primaries, as redis-cli
        // --cluster-replicas 1 makes them, the others their replicas.
        let mut s = with_replicas(1);
        for i in 1..=6 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(ranges(s.table().unwrap()), [(0, 5460, Some(1)), (5461, 10922, Some(2)), (10923, 16383, Some(3))]);
        assert_eq!(pairs(&s), [(4, 1), (5, 2), (6, 3)]);
        assert_eq!(s.primary_of(&id(4)), Some(id(1)));
        assert_eq!(s.primary_of(&id(1)), None);

        // 5 nodes: 3 primaries (a node more than pairs is a primary), 2 replicas.
        let mut s = with_replicas(1);
        for i in 1..=5 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(ranges(s.table().unwrap()).len(), 3);
        assert_eq!(pairs(&s), [(4, 1), (5, 2)]);

        // 2 replicas each, 7 nodes: 3 primaries; the 4 replicas spread.
        let mut s = with_replicas(2);
        for i in 1..=7 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(4, 1), (5, 2), (6, 3), (7, 1)]);
        // 9: each its 2, none the replica of a node placed after it.
        let mut s = with_replicas(2);
        for i in 1..=9 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(4, 1), (5, 2), (6, 3), (7, 1), (8, 2), (9, 3)]);

        // Zones: a replica goes to a primary on another one.
        let mut s = with_replicas(1);
        for (i, zone) in [(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "c"), (6, "a")] {
            s.join_in(node(i), None, Some(zone));
        }
        s.create().unwrap();
        // 4 (b) skips 3 (b); 5 (c) takes 2, the first left with none; 6 (a)
        // only 3 (b) has none left.
        assert_eq!(pairs(&s), [(4, 1), (5, 2), (6, 3)]);
        // Without zones 3 would go to 1, in its zone.
        let mut s = with_replicas(1);
        for (i, zone) in [(1, "a"), (2, "b"), (3, "a"), (4, "b")] {
            s.join_in(node(i), None, Some(zone));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(3, 2), (4, 1)]);
        // A primary without a zone goes after one in another zone, before
        // one in the same zone.
        let mut s = State::default();
        for (i, zone) in [(1, Some("a")), (2, None), (3, Some("b"))] {
            s.join_in(node(i), None, zone);
        }
        s.create().unwrap();
        s.set_replicas(1);
        s.join_in(node(4), None, Some("a"));
        s.join_in(node(5), None, Some("a"));
        s.join_in(node(6), None, Some("a"));
        assert_eq!(pairs(&s), [(4, 3), (5, 2), (6, 1)]);
        // Same-zone only when nothing else is left.
        let mut s = with_replicas(1);
        for (i, zone) in [(1, "a"), (2, "a")] {
            s.join_in(node(i), None, Some(zone));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(2, 1)]);
        // Without replicas, as before: every node a primary.
        let mut s = joined(4);
        s.create().unwrap();
        assert_eq!(ranges(s.table().unwrap()).len(), 4);
        assert!(pairs(&s).is_empty());
    }

    #[test]
    fn nodes_joining_with_replicas() {
        // As --auto has it: the table at the first join, then one by one.
        let mut s = with_replicas(1);
        s.join(node(1));
        s.create().unwrap();
        let j = s.join(node(2));
        assert_eq!((j.change, j.primary), (Change::Added, Some(id(1))));
        assert_eq!(s.table().unwrap().epoch(), 2);
        // Every primary has its replica: node 3 is a primary.
        assert_eq!(s.join(node(3)).primary, None);
        assert_eq!(s.join(node(4)).primary, Some(id(3)));
        assert_eq!(pairs(&s), [(2, 1), (4, 3)]);
        // Joining again changes nothing.
        assert_eq!(s.join(node(4)), Joined { change: Change::Known, replaced: vec![], primary: Some(id(3)) });
        // The slots go to the primaries only.
        assert_eq!(s.rebalance(&[]).unwrap(), 8192);
        assert!(s.queue().iter().all(|(_, to)| *to == id(3)), "{:?}", s.queue());
        let slots: Vec<u16> = s.queue().iter().map(|(slot, _)| *slot).collect();
        s.advance(&[], &slots);
        s.advance(&slots, &[]);
        assert_eq!(ranges(s.table().unwrap()), [(0, 8191, Some(1)), (8192, 16383, Some(3))]);
        assert_eq!(pairs(&s), [(2, 1), (4, 3)], "the moves keep the replicas");
        let err = format!("ERR Node {} is a replica", id(2));
        assert_eq!(s.move_slots(SlotRange::new(0, 10), &id(2)).unwrap_err(), err);
        assert_eq!(s.drain(&id(1), &[]), Ok(8192));
        assert!(s.queue().iter().all(|(_, to)| *to == id(3)), "drained to primaries only");
        s.queue.clear();

        // A replica restarting (a new id at its address) is a replica
        // again; a replica leaving holds nothing and goes at once.
        let restarted = Node::new(id(5), "10.0.0.1", 7002).unwrap();
        let j = s.join(restarted);
        assert_eq!((j.replaced, j.primary), (vec![(id(2), 0)], Some(id(1))));
        assert_eq!(pairs(&s), [(4, 3), (5, 1)]);
        assert_eq!(s.leave(&id(4), &[]), Ok(1));
        assert!(s.remove_left(&id(4)));
        assert_eq!(pairs(&s), [(5, 1)]);
        // A node joining now fills node 3.
        assert_eq!(s.join(node(6)).primary, Some(id(3)));

        // Unassigned slots go to primaries only.
        s.forget(&id(3)).unwrap();
        // Node 6 lost its primary: node 1 has a replica, so it's a primary.
        assert!(pairs(&s) == [(5, 1)]);
        assert_eq!(s.assign_unassigned(), 8192);
        assert_eq!(ranges(s.table().unwrap()), [(0, 8191, Some(1)), (8192, 16383, Some(6))]);
        let err = format!("ERR Node {} is a replica", id(5));
        assert_eq!(s.assign(SlotRange::new(0, 0), &id(5)).unwrap_err(), err);
    }

    #[test]
    fn replicas_of_a_removed_primary() {
        // Two primaries made without replicas, then 2 wanted: 3, 4 and 5
        // spread. With 1 wanted, when node 1 goes, its replicas 3 and 5
        // have nowhere to go (node 2 has its one; not to each other): both
        // are primaries.
        let mut s = joined(2);
        s.create().unwrap();
        s.set_replicas(2);
        for i in 3..=4 {
            s.join(node(i));
        }
        assert_eq!(pairs(&s), [(3, 1), (4, 2)]);
        s.join(node(5));
        assert_eq!(pairs(&s), [(3, 1), (4, 2), (5, 1)]);
        s.set_replicas(1);
        s.forget(&id(1)).unwrap();
        // 3 and 5: node 2 has its one already; neither goes to the other.
        assert_eq!(pairs(&s), [(4, 2)]);
        // Fewer replicas wanted: the roles there stay; new nodes are primaries
        // until a primary lacks one.
        let mut s = with_replicas(2);
        for i in 1..=3 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(2, 1), (3, 1)]);
        s.set_replicas(1);
        assert_eq!(s.join(node(4)).primary, None);
        assert_eq!(pairs(&s), [(2, 1), (3, 1)]);
        assert_eq!(s.join(node(5)).primary, Some(id(4)));
        // A leaving primary is given no replica.
        s.leave(&id(4), &[]).unwrap();
        s.forget(&id(5)).unwrap();
        assert_eq!(s.join(node(6)).primary, None);
    }

    #[test]
    fn replicas_and_zones_in_the_state_file() {
        let mut s = with_replicas(1);
        s.join_in(node(1), Some("pod-1"), Some("worker-1"));
        s.join_in(node(2), None, Some("worker-2"));
        s.create().unwrap();
        s.join(node(3));
        assert_eq!(pairs(&s), [(2, 1)]);
        assert_eq!(s.zone(&id(1)), Some("worker-1"));
        assert_eq!(s.zone(&id(3)), None);
        let text = s.encode();
        assert!(text.contains(&format!("\nzone {} worker-1\nzone {} worker-2\n", id(1), id(2))), "{text}");
        assert!(text.contains(&format!("\nreplica {} {}\n", id(2), id(1))), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        // Rejoining without a zone drops it.
        s.join_in(node(1), Some("pod-1"), None);
        assert_eq!(s.zone(&id(1)), None);

        let bad = |text: &str| State::decode(text).unwrap_err();
        let base = format!("stuart-state 1\nnode {} 10.0.0.1 7001\nnode {} 10.0.0.1 7002\n", id(1), id(2));
        assert!(bad(&format!("{base}zone {} a\n", id(3))).contains("not a node"));
        assert!(bad(&format!("{base}zone {} a b\n", id(1))).contains("unexpected"));
        let table = format!("{base}table 1\nrange 0 16383 {}\n", id(1));
        assert!(State::decode(&format!("{table}replica {} {}\n", id(2), id(1))).is_ok());
        assert!(bad(&format!("{table}replica {} {}\n", id(1), id(2))).contains("owns slots"));
        assert!(bad(&format!("{table}replica {} {}\n", id(3), id(1))).contains("unknown node"));
        assert!(bad(&format!("{base}replica {} {}\n", id(2), id(1))).contains("unexpected"), "only in a table");
    }

    #[test]
    fn promotion() {
        // 1 and 2 primaries, 3 replica of 1, 4 of 2; then 5 a second
        // replica of 1 (--replicas 2).
        let mut s = with_replicas(1);
        for i in 1..=4 {
            s.join(node(i));
        }
        s.create().unwrap();
        s.set_replicas(2);
        s.join(node(5));
        assert_eq!(pairs(&s), [(3, 1), (4, 2), (5, 1)]);
        // A move out of 1, one into 1, one queued to 1, one queued elsewhere.
        s.move_slots(SlotRange::new(0, 0), &id(2)).unwrap();
        s.move_slots(SlotRange::new(9000, 9000), &id(1)).unwrap();
        s.advance(&[], &[0, 9000]);
        s.move_slots(SlotRange::new(9001, 9001), &id(1)).unwrap();
        s.move_slots(SlotRange::new(1, 1), &id(2)).unwrap();
        assert_eq!(moving(&s), [(0, 1, 2), (9000, 2, 1)]);
        let epoch = s.table().unwrap().epoch();

        assert!(!s.promote(&id(1), &id(4)), "not its replica");
        assert!(!s.promote(&id(3), &id(1)), "a replica isn't failed over");
        assert!(s.promote(&id(1), &id(3)));
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), epoch + 1);
        assert_eq!(ranges(t), [(0, 8191, Some(3)), (8192, 16383, Some(2))], "3 has 1's slots, the moves called off");
        assert!(moving(&s).is_empty());
        assert_eq!(s.queue(), [(1, id(2)), (0, id(2))], "the move queued to 1 dropped, the other kept, the one from 1 queued again");
        // 1 is a primary without slots, marked failed; 5 follows 3.
        assert_eq!(pairs(&s), [(4, 2), (5, 3)]);
        assert!(s.is_failed(&id(1)) && !s.is_failed(&id(3)));
        assert_eq!(s.holding(&id(1)), 0);
        assert!(!s.active().contains(&id(1)), "a failed primary gets no slots");
        assert_eq!(s.first(), [id(3)], "the new primary takes it first");
        assert_eq!(s.primary_of(&id(3)), None);
        // In the state file.
        let mut back = State::decode(&s.encode()).unwrap();
        back.set_replicas(2);
        assert_eq!(back, s);

        // Back: the replica of 3, the one with fewer than 2.
        assert!(!s.back(&id(2)), "not failed over");
        let epoch = s.table().unwrap().epoch();
        assert!(s.back(&id(1)));
        assert_eq!(s.table().unwrap().epoch(), epoch + 1);
        assert_eq!(pairs(&s), [(4, 2), (5, 3), (1, 3)]);
        assert!(!s.is_failed(&id(1)));
        assert_eq!(s.promotable(&id(1)), Some(epoch + 1));
        assert!(!s.back(&id(1)), "once");
        // Its successor's replica even when that one has its N, as in
        // Redis; a node joining meanwhile isn't given the failed primary.
        let mut t = with_replicas(1);
        for i in 1..=2 {
            t.join(node(i));
        }
        t.create().unwrap();
        t.promote(&id(1), &id(2));
        t.join(node(3));
        assert_eq!(pairs(&t), [(3, 2)], "not the replica of the failed primary");
        let mut u = t.clone();
        assert!(t.back(&id(1)));
        assert_eq!(pairs(&t), [(3, 2), (1, 2)]);
        // Its successor leaving, and every other primary full: it stays a
        // primary, to get slots.
        u.leave(&id(2), &[]).unwrap();
        assert!(u.back(&id(1)));
        assert_eq!(u.primary_of(&id(1)), None);
        assert!(u.active().contains(&id(1)));
        // The successor in the state file.
        let mut v = with_replicas(1);
        for i in 1..=2 {
            v.join(node(i));
        }
        v.create().unwrap();
        v.promote(&id(1), &id(2));
        assert!(v.encode().contains(&format!("
successor {} {}
", id(1), id(2))));
        let mut back = State::decode(&v.encode()).unwrap();
        back.set_replicas(1);
        assert_eq!(back, v);
        assert!(State::decode(&v.encode().replace(&format!("successor {} {}", id(1), id(2)), &format!("successor {} {}", id(1), id(7)))).is_err());
    }

    #[test]
    fn a_leaving_primary_hands_over() {
        // 1 and 2 primaries, 3 replica of 1, 4 of 2.
        let mut s = with_replicas(1);
        for i in 1..=4 {
            s.join(node(i));
        }
        s.create().unwrap();
        // A move out of 1 under way, one queued to 1, one queued out of 2.
        s.move_slots(SlotRange::new(0, 0), &id(2)).unwrap();
        s.advance(&[], &[0]);
        s.move_slots(SlotRange::new(9000, 9000), &id(1)).unwrap();
        s.move_slots(SlotRange::new(9001, 9001), &id(1)).unwrap();
        let epoch = s.table().unwrap().epoch();

        // A replica, or a primary without one, leaves as before.
        let mut r = s.clone();
        assert_eq!(r.leave_handing(&id(3), &[]), Ok(1));
        assert!(r.handing().is_empty());
        // Leaving: not drained, nothing moves to it; the move under way goes on.
        assert_eq!(s.leave_handing(&id(1), &[]), Ok(8192 + 1));
        assert_eq!((s.leaving(), s.handing()), (&[id(1)][..], &[id(1)][..]));
        assert!(s.queue().is_empty(), "{:?}", s.queue());
        assert_eq!(moving(&s), [(0, 1, 2)]);
        assert_eq!(s.table().unwrap().epoch(), epoch, "no new table");
        assert_eq!(s.leave_handing(&id(1), &[]), Ok(8192 + 1), "asked again");
        // The plans leave its slots alone: nothing to even out, a drain of
        // 2 moves nothing to it, nor counts its slots.
        assert_eq!(s.rebalance(&[]), Ok(0));
        assert_eq!(s.drain_leaving(&[]), 0);
        assert!(s.queue().is_empty());
        // In the state file.
        let text = s.encode();
        assert!(text.contains(&format!("\nleaving {}\nhanding {}\n", id(1), id(1))), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        assert!(State::decode(&text.replace(&format!("leaving {}\n", id(1)), "")).unwrap_err().contains("not leaving"));

        // Given up: drained as any leaving node.
        let mut d = s.clone();
        assert_eq!(d.drain_instead(&id(1), &[]), Ok(8191), "all but slot 0, moving to 2 already");
        assert!(d.handing().is_empty() && d.leaving() == [id(1)]);
        assert!(d.queue().iter().all(|(_, to)| *to == id(2)));
        assert_eq!(d.drain_instead(&id(1), &[]), Ok(0), "once");

        // Handed over: 3 owns 1's slots, the move out of 1 queued again from
        // 3; 1 is a primary without slots, not failed, no successor; 3 takes
        // the table first; once 1 took it, it can be removed.
        assert!(!s.hand_over(&id(1), &id(4)), "not its replica");
        let mut l = s.clone();
        assert_eq!(l.leave(&id(3), &[]), Ok(1));
        assert!(!l.hand_over(&id(1), &id(3)), "to a replica leaving too");
        assert!(s.hand_over(&id(1), &id(3)));
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), epoch + 1);
        assert_eq!(ranges(t), [(0, 8191, Some(3)), (8192, 16383, Some(2))]);
        assert!(moving(&s).is_empty());
        assert_eq!(s.queue(), [(0, id(2))]);
        assert_eq!(pairs(&s), [(4, 2)]);
        assert!(!s.is_failed(&id(1)) && s.primary_of(&id(1)).is_none());
        assert_eq!(s.first(), [id(3)]);
        assert!(s.handing().is_empty());
        assert_eq!(s.holding(&id(1)), 0);
        assert!(!s.encode().contains("successor"));
        assert!(s.remove_left(&id(1)));
        assert_eq!(s.leave(&id(1), &[]), Ok(0), "gone");
        // 3 has what 1 had: nothing to even out, not even the move queued again.
        assert_eq!(s.rebalance(&[]), Ok(0));
    }

    /// Node `n` leaves and, holding nothing, is removed.
    fn left(s: &mut State, n: u8) {
        s.leave(&id(n), &[]).unwrap();
        assert!(s.remove_left(&id(n)), "node {n} holds nothing");
    }

    /// The moves queued, started and ended.
    fn moved(s: &mut State) {
        let slots: Vec<u16> = s.queue().iter().map(|(slot, _)| *slot).collect();
        s.advance(&[], &slots);
        s.advance(&slots, &[]);
    }

    fn sorted(mut pairs: Vec<(u8, u8)>) -> Vec<(u8, u8)> {
        pairs.sort();
        pairs
    }

    #[test]
    fn primaries_beyond_the_need_are_demoted() {
        // 6 nodes, 1 replica each; a scale-down to 4 takes replicas 6 and 5:
        // 3 primaries where 2 do, two of them without a replica.
        let mut s = with_replicas(1);
        for i in 1..=6 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(s.surplus(), 0);
        assert!(!s.plan_roles(|_| true));
        s.leave(&id(6), &[]).unwrap();
        assert_eq!(s.surplus(), 0, "5 staying: 3 primaries");
        left(&mut s, 6);
        s.leave(&id(5), &[]).unwrap();
        assert_eq!(s.surplus(), 1);
        assert!(!s.plan_roles(|_| true), "not while a node leaves");
        assert!(s.remove_left(&id(5)));
        assert_eq!(pairs(&s), [(4, 1)]);

        // Only one whose keys the others have room for.
        let mut r = s.clone();
        assert!(!r.plan_roles(|_| false));
        assert!(r.plan_roles(|n| *n != id(3)));
        assert_eq!(r.demoting(), Some(&id(2)));
        // A node joining isn't its replica, though first of those lacking one.
        assert_eq!(r.join(node(7)).primary, Some(id(3)));
        // The fewest slots before the last joined: 3 with two more than 2.
        let mut o = s.clone();
        o.move_slots(SlotRange::new(5461, 5462), &id(3)).unwrap();
        moved(&mut o);
        assert!(o.plan_roles(|_| true));
        assert_eq!(o.demoting(), Some(&id(2)));
        // 3, not 2: no replica either, and a slot fewer to move.
        let mut b = s.clone();
        let epoch = s.table().unwrap().epoch();
        assert!(s.plan_roles(|_| true));
        assert_eq!(s.demoting(), Some(&id(3)));
        assert_eq!(s.table().unwrap().epoch(), epoch, "no new table");
        assert!(!s.plan_roles(|_| true), "planned already");
        assert_eq!(s.surplus(), 1, "until it is a replica");
        let err = format!("ERR Node {} is becoming a replica", id(3));
        assert_eq!(s.move_slots(SlotRange::new(0, 0), &id(3)).unwrap_err(), err);
        // In the state file.
        let text = s.encode();
        assert!(text.contains(&format!("\ndemoting {}\n", id(3))), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        let bad = |text: &str| State::decode(text).unwrap_err();
        assert!(bad(&text.replace(&format!("demoting {}", id(3)), &format!("demoting {}", id(4)))).contains("not a primary staying"));
        assert!(bad(&text.replace(&format!("demoting {}", id(3)), &format!("demoting {}", id(9)))).contains("not a primary staying"));
        let twice = text.replace(&format!("demoting {}\n", id(3)), &format!("demoting {}\ndemoting {}\n", id(3), id(2)));
        assert!(bad(&twice).contains("unexpected"));

        // Its slots go to 1 and 2; meanwhile it stays a primary.
        assert!(!s.demote(&id(3)), "holding slots");
        assert_eq!(s.rebalance(&[]), Ok(5461));
        assert!(s.queue().iter().all(|(_, to)| *to != id(3)));
        moved(&mut s);
        assert_eq!(s.holding(&id(3)), 0);
        assert!(!s.demote(&id(2)), "not the one being demoted");
        let epoch = s.table().unwrap().epoch();
        assert!(s.demote(&id(3)));
        assert_eq!(s.table().unwrap().epoch(), epoch + 1);
        assert_eq!(sorted(pairs(&s)), [(3, 2), (4, 1)]);
        assert_eq!((s.demoting(), s.surplus()), (None, 0));
        assert!(!s.plan_roles(|_| true));
        assert_eq!(s.rebalance(&[]), Ok(0), "even");

        // A node joining calls it off: it goes to 2 (not to 3), and with
        // 5 nodes 3 primaries are needed again.
        assert!(b.plan_roles(|_| true));
        // Only the one being demoted is: 8, a primary without slots (1 and
        // 2 have their replica), isn't.
        let mut e = b.clone();
        e.join(node(7));
        assert_eq!(e.join(node(8)).primary, None);
        e.forget(&id(7)).unwrap();
        assert!(!e.demote(&id(8)), "not the one being demoted (2 lacks a replica again)");
        assert_eq!(e.primary_of(&id(8)), None);
        assert_eq!(b.join(node(7)).primary, Some(id(2)));
        assert!(b.plan_roles(|_| true));
        assert_eq!(b.demoting(), None);
        assert_eq!(b.rebalance(&[]), Ok(0), "3 keeps its slots");
        // After a demotion, nodes joining as before: 8 a primary, 9 its replica.
        let mut c = s.clone();
        c.join(node(8));
        c.join(node(9));
        assert_eq!(sorted(pairs(&c)), [(3, 2), (4, 1), (9, 8)]);
        assert_eq!(c.surplus(), 0);
        // Its leaving calls it off; a demotion waits for a failed primary.
        let mut d = b.clone();
        d.forget(&id(7)).unwrap();
        assert!(d.plan_roles(|_| true));
        d.leave(&id(3), &[]).unwrap();
        assert_eq!(d.demoting(), None);
        let mut f = b.clone();
        f.forget(&id(7)).unwrap();
        f.promote(&id(1), &id(4));
        assert_eq!(f.surplus(), 1, "1 failed over is no primary; 2, 3 and 4 are");
        assert!(!f.plan_roles(|_| true), "1 is failed");
    }

    #[test]
    fn a_demoted_primary_s_replicas_find_other_primaries() {
        // 2 replicas each: 9 nodes, then 7, 8 and 9 leave: 3 primaries
        // with a replica each where 2 primaries do (6 nodes).
        let mut s = with_replicas(2);
        for i in 1..=9 {
            s.join(node(i));
        }
        s.create().unwrap();
        for n in [9, 8, 7] {
            left(&mut s, n);
        }
        assert_eq!(sorted(pairs(&s)), [(4, 1), (5, 2), (6, 3)]);
        assert_eq!(s.surplus(), 1);
        let mut f = s.clone();
        assert!(s.plan_roles(|_| true));
        assert_eq!(s.demoting(), Some(&id(3)), "the fewest slots");
        // A node joining doesn't go to it.
        let mut j = s.clone();
        assert_eq!(j.join(node(7)).primary, Some(id(1)));
        assert!(j.plan_roles(|_| true), "7 nodes need 3 primaries: called off");
        assert_eq!(j.demoting(), None);
        // Leaving (handing over to 6), or forgotten: no longer demoted.
        let (mut h, mut g) = (s.clone(), s.clone());
        h.leave_handing(&id(3), &[]).unwrap();
        assert_eq!((h.demoting(), h.handing()), (None, &[id(3)][..]));
        g.forget(&id(3)).unwrap();
        assert_eq!(g.demoting(), None);
        s.rebalance(&[]).unwrap();
        moved(&mut s);
        // 1 starting to leave meanwhile: 3 would go to 2, but 6 has nowhere
        // to go; nothing changes until there is.
        let mut l = s.clone();
        l.leave_handing(&id(1), &[]).unwrap();
        let before = l.clone();
        assert!(!l.demote(&id(3)));
        assert_eq!(l, before);
        assert!(s.demote(&id(3)));
        // 3 goes to 1 (the first of the two with one), then 6 to 2.
        assert_eq!(sorted(pairs(&s)), [(3, 1), (4, 1), (5, 2), (6, 2)]);
        assert_eq!(s.surplus(), 0);

        // Failed over meanwhile: its replica, promoted, isn't demoted.
        assert!(f.plan_roles(|_| true));
        assert!(f.promote(&id(3), &id(6)));
        assert_eq!(f.demoting(), None);

        // Two replicas of its own: neither goes to the other. 12 nodes with
        // 2 replicas each, then 3 wanted: 4 primaries where 3 do.
        let mut s = with_replicas(2);
        for i in 1..=12 {
            s.join(node(i));
        }
        s.create().unwrap();
        // id(n) for n >= 10 isn't "0n0n...": compare ids.
        let replicas_of = |s: &State, p: u8| -> Vec<NodeId> {
            let mut r: Vec<NodeId> = (1..=12).map(id).filter(|r| s.primary_of(r) == Some(id(p))).collect();
            r.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            r
        };
        assert_eq!(replicas_of(&s, 4), [id(8), id(12)]);
        s.set_replicas(3);
        assert_eq!(s.surplus(), 1);
        assert!(s.plan_roles(|_| true));
        assert_eq!(s.demoting(), Some(&id(4)), "the last joined: all the same otherwise");
        s.rebalance(&[]).unwrap();
        moved(&mut s);
        assert!(s.demote(&id(4)));
        for p in 1..=3 {
            assert_eq!(replicas_of(&s, p).len(), 3, "primary {p}");
        }
        assert!(replicas_of(&s, 1).contains(&id(4)));
        assert!(replicas_of(&s, 4).is_empty() && replicas_of(&s, 12).is_empty() && replicas_of(&s, 8).is_empty());
    }

    #[test]
    fn promotable_replicas() {
        let mut s = with_replicas(1);
        for i in 1..=2 {
            s.join(node(i));
        }
        assert_eq!(s.promotable(&id(2)), None, "no table");
        s.create().unwrap();
        assert_eq!(s.promotable(&id(2)), Some(0), "never marked");
        assert_eq!(s.promotable(&id(1)), None, "a primary");
        s.set_failed(&id(2), true);
        assert_eq!(s.promotable(&id(2)), None, "marked failed");
        let e = s.table().unwrap().epoch();
        s.set_failed(&id(2), false);
        assert_eq!(s.promotable(&id(2)), Some(e + 1), "once its primary took the table that cleared the mark");
        // Marked and cleared again: the last clearing counts.
        s.set_failed(&id(2), true);
        s.set_failed(&id(2), false);
        assert_eq!(s.promotable(&id(2)), Some(e + 3));
        // In the state file, and gone with the node.
        let text = s.encode();
        assert!(text.contains(&format!("\ncleared {} {}\n", id(2), e + 3)), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        s.forget(&id(2)).unwrap();
        assert!(!s.encode().contains("\ncleared "));
        let base = format!("stuart-state 1\nnode {} 10.0.0.1 7001\ntable 1\nrange 0 16383 {}\n", id(1), id(1));
        assert!(State::decode(&format!("{base}cleared {} 3\n", id(3))).unwrap_err().contains("not a node"));
        assert!(State::decode(&format!("{base}cleared {} x\n", id(1))).is_err());
    }

    #[test]
    fn failed_marks() {
        let mut s = with_replicas(1);
        assert!(!s.set_failed(&id(2), true), "no table");
        for i in 1..=4 {
            s.join(node(i));
        }
        s.create().unwrap();
        assert_eq!(pairs(&s), [(3, 1), (4, 2)]);
        let epoch = |s: &State| s.table().unwrap().epoch();
        let e = epoch(&s);
        assert!(s.set_failed(&id(3), true));
        assert!(s.is_failed(&id(3)) && !s.is_failed(&id(4)));
        assert_eq!(epoch(&s), e + 1);
        assert!(!s.set_failed(&id(3), true), "marked already");
        assert!(!s.set_failed(&id(9), true), "not a node");
        assert_eq!(epoch(&s), e + 1);
        assert_eq!(pairs(&s), [(3, 1), (4, 2)], "the pairing stays");
        // Kept by the tables after: a move, a node joining.
        s.move_slots(SlotRange::new(0, 9), &id(2)).unwrap();
        s.advance(&[], &[0, 1]);
        assert!(s.is_failed(&id(3)));
        s.join(node(5));
        assert!(s.is_failed(&id(3)));
        // In the state file, after the replicas.
        let text = s.encode();
        assert!(text.contains(&format!("\nreplica {} {}\nfailed {}\n", id(4), id(2), id(3))), "{text}");
        let mut back = State::decode(&text).unwrap();
        back.set_replicas(1);
        assert_eq!(back, s);
        // Cleared.
        let e = epoch(&s);
        assert!(s.set_failed(&id(3), false));
        assert!(!s.is_failed(&id(3)) && epoch(&s) == e + 1);
        assert!(!s.set_failed(&id(3), false));
        assert!(!s.encode().contains("\nfailed "));
        // A node removed takes its mark along.
        s.set_failed(&id(4), true);
        s.forget(&id(4)).unwrap();
        assert!(s.table().unwrap().failed().is_empty());

        let bad = |text: &str| State::decode(text).unwrap_err();
        let base = format!("stuart-state 1\nnode {} 10.0.0.1 7001\nnode {} 10.0.0.1 7002\ntable 1\nrange 0 16383 {}\n", id(1), id(2), id(1));
        assert!(State::decode(&format!("{base}failed {}\n", id(2))).unwrap().is_failed(&id(2)));
        assert!(bad(&format!("{base}failed {}\n", id(3))).contains("unknown node"));
        assert!(bad(&format!("{base}failed {}\nfailed {}\n", id(2), id(2))).contains("marked failed twice"));
    }
}
