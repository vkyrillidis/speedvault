//! What stuart knows and keeps across restarts: the nodes that joined (and
//! which of them are leaving), and once `STUART CREATE` made it, the slot
//! table, with the slots moving (phase 5) and those waiting to. Changes go
//! through [`State::join`], [`State::create`], [`State::move_slots`],
//! [`State::rebalance`], [`State::leave`], [`State::advance`]... on a copy,
//! which is saved before it replaces the old state and before anything is
//! pushed to the nodes.
//!
//! A slot moves in two tables: one where it migrates from its owner to its
//! target, then, once the source has nothing of it left, one where the
//! target owns it. Each must reach the target before the source (and the
//! others), or clients would bounce between the two: the state says which
//! nodes get the current table first ([`State::first`]).

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

    /// The nodes not leaving, in join order: those that get slots.
    fn active(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|n| *n.id()).filter(|id| !self.leaving.contains(id)).collect()
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
        // Names of nodes gone (replaced, left, forgotten) go too.
        let nodes = &self.nodes;
        self.names.retain(|(id, _)| nodes.iter().any(|n| n.id() == id));
        if let Some(name) = name {
            self.names.retain(|(id, _)| id != node.id());
            self.names.push((*node.id(), name.to_string()));
        }

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
        Joined { change, replaced }
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
        let Some(old) = &self.table else { return };
        let mut owners = owners(old);
        let mut migrations = Vec::new();
        for m in old.migrations() {
            let (from, to) = (*old.nodes()[m.from].id(), *old.nodes()[m.to].id());
            match (known(&from), known(&to)) {
                (true, true) => migrations.push((m.slot, from, to)),
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
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations));
        self.queue_check();
    }

    /// Drops queued moves that have nothing left to do: the slot is the
    /// target's already, or no node owns it.
    fn queue_check(&mut self) {
        let Some(t) = &self.table else { return };
        self.queue.retain(|(slot, to)| t.owner(*slot).is_some_and(|n| n.id() != to));
    }

    /// `STUART CREATE`: the slots split evenly over the joined nodes, in
    /// join order, in contiguous ranges, as `redis-cli --cluster create`
    /// splits them (3 nodes: 0-5460, 5461-10922, 10923-16383). Epoch 1.
    pub fn create(&mut self) -> Result<(), String> {
        if let Some(t) = &self.table {
            return Err(format!("ERR The slots were assigned already (table epoch {})", t.epoch()));
        }
        if self.nodes.is_empty() {
            return Err("ERR No node has joined yet".to_string());
        }
        let n = self.nodes.len();
        let per_node = SLOTS as f64 / n as f64;
        let mut ranges = Vec::with_capacity(n);
        let (mut first, mut cursor) = (0usize, 0f64);
        for (i, node) in self.nodes.iter().enumerate() {
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
        if let Some(slot) = (range.start..=range.end).find(|&s| old.owner_index(s).is_some()) {
            let owner = old.owner(slot).map(|n| *n.id()).unwrap();
            return Err(format!("ERR Slot {slot} is served by {owner}: only unassigned slots can be assigned"));
        }
        let mut owners = owners(old);
        owners[usize::from(range.start)..=usize::from(range.end)].fill(Some(*id));
        let migrations = migrations(old);
        // `first` stays: the moves under way are in this table too.
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations));
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
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations(old)));
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
        self.queue = spread(&owners_after(t), &moving(t), &active, full, Goal::Even);
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
        let mut owners = owners_after(t);
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
        let ids: Vec<NodeId> = self.leaving.iter().filter(|id| self.holding(id) > 0).copied().collect();
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

    /// The next table of a move: the migrations of `done` end (their
    /// targets own the slots now), and the queued slots `start` begin to
    /// migrate. Their targets get the table first. Nothing if both are
    /// empty.
    pub fn advance(&mut self, done: &[u16], start: &[u16]) {
        let Some(old) = &self.table else { return };
        if done.is_empty() && start.is_empty() {
            return;
        }
        let mut owners = owners(old);
        let mut first = Vec::new();
        let mut migrations: Vec<(u16, NodeId, NodeId)> = Vec::new();
        for (slot, from, to) in self::migrations(old) {
            if done.contains(&slot) {
                owners[usize::from(slot)] = Some(to);
                first.push(to);
            } else {
                migrations.push((slot, from, to));
            }
        }
        for &slot in start {
            let Some(i) = self.queue.iter().position(|(s, _)| *s == slot) else { continue };
            let (_, to) = self.queue.remove(i);
            let Some(from) = owners[usize::from(slot)] else { continue };
            migrations.push((slot, from, to));
            first.push(to);
        }
        first.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        first.dedup();
        self.table = Some(table_from(old.epoch() + 1, self.nodes.clone(), &owners, &migrations));
        self.first = first;
        self.queue_check();
    }

    /// The state file's text: a header, a line per node, then the table
    /// (ranges, migrations), the nodes it goes to first, and the queue.
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
        for id in &self.leaving {
            text += &format!("leaving {id}\n");
        }
        if let Some(t) = &self.table {
            text += &format!("table {}\n", t.epoch());
            for (range, owner) in t.runs() {
                let owner = owner.map_or("-".to_string(), |i| t.nodes()[i].id().to_string());
                text += &format!("range {} {} {owner}\n", range.start, range.end);
            }
            for (slot, from, to) in migrations(t) {
                text += &format!("migrate {slot} {from} {to}\n");
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
        let (mut migrating, mut first, mut queue, mut names) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
                ["leaving", n] if epoch.is_none() => leaving.push(id(n).map_err(bad)?),
                ["table", e] if epoch.is_none() => epoch = Some(num(e).map_err(bad)?),
                ["range", s, e, owner] if epoch.is_some() => {
                    let slot = |w: &str| num(w).and_then(|n| u16::try_from(n).map_err(|e| e.to_string())).map_err(bad);
                    let owner = if owner == "-" { None } else { Some(id(owner).map_err(bad)?) };
                    ranges.push((SlotRange::new(slot(s)?, slot(e)?), owner));
                }
                ["migrate", s, from, to] if epoch.is_some() => {
                    let slot = num(s).and_then(|n| u16::try_from(n).map_err(|e| e.to_string())).map_err(bad)?;
                    migrating.push((slot, id(from).map_err(bad)?, id(to).map_err(bad)?));
                }
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
        if let Some((n, _)) = names.iter().find(|(n, _)| !nodes.iter().any(|node: &Node| node.id() == n)) {
            return Err(format!("name {n}: not a node"));
        }
        let table = match epoch {
            None => None,
            Some(e) => Some(
                SlotTable::new(e, nodes.clone(), &ranges)
                    .and_then(|t| t.with_migrations(&migrating))
                    .map_err(|e| format!("the table: {e}"))?,
            ),
        };
        Ok(State { nodes, leaving, table, queue, first, names })
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

/// The table's migrations, by id.
fn migrations(t: &SlotTable) -> Vec<(u16, NodeId, NodeId)> {
    t.migrations().iter().map(|m| (m.slot, *t.nodes()[m.from].id(), *t.nodes()[m.to].id())).collect()
}

/// A table from every slot's owner (all of them known nodes), in ranges.
fn table_from(epoch: u64, nodes: Vec<Node>, owners: &[Option<NodeId>], migrations: &[(u16, NodeId, NodeId)]) -> SlotTable {
    // SlotTable::new merges nothing itself, runs do.
    let mut ranges: Vec<(SlotRange, Option<NodeId>)> = Vec::new();
    for (slot, owner) in owners.iter().enumerate() {
        match ranges.last_mut() {
            Some((r, o)) if o == owner => r.end = slot as u16,
            _ => ranges.push((SlotRange::new(slot as u16, slot as u16), *owner)),
        }
    }
    SlotTable::new(epoch, nodes, &ranges)
        .and_then(|t| t.with_migrations(migrations))
        .expect("a table of the joined nodes, migrating from the owners")
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
        assert_eq!(s.join(node(1)), Joined { change: Change::Known, replaced: vec![] });
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
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(1), 8192)] });
        let t = s.table().unwrap();
        assert_eq!(t.epoch(), 4);
        assert_eq!(t.nodes().iter().map(|n| *n.id()).collect::<Vec<_>>(), [id(2), id(3), id(4)]);
        assert_eq!(ranges(t), [(0, 8191, None), (8192, 16383, Some(2))]);
        assert_eq!(s.nodes().len(), 3);

        // Before a table, a replaced node just goes.
        let mut s = joined(2);
        let j = s.join(Node::new(id(5), "10.0.0.1", 7002).unwrap());
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(2), 0)] });
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
        assert_eq!(s.join_named(at(1, "10.0.0.1"), Some("pod-a")), Joined { change: Change::Known, replaced: vec![] });
        // Another node, unnamed or with another name, at another address.
        assert_eq!(s.join(at(3, "10.0.0.3")).replaced, []);
        assert_eq!(s.join_named(at(4, "10.0.0.4"), Some("pod-c")).replaced, []);

        // pod-a restarted at another address: its slots are lost.
        let j = s.join_named(at(5, "10.0.0.9"), Some("pod-a"));
        assert_eq!(j, Joined { change: Change::Added, replaced: vec![(id(1), 8192)] });
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
        migrations(s.table().unwrap()).iter().map(|(slot, f, t)| (*slot, n(f), n(t))).collect()
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

        // One starts: the table migrates it, its target first.
        s.advance(&[], &[8190]);
        assert_eq!(s.table().unwrap().epoch(), 3);
        assert_eq!(moving(&s), [(8190, 1, 2)]);
        assert_eq!(s.first(), [id(2)]);
        assert_eq!(s.queue(), [(8191, id(2))]);
        assert_eq!(s.table().unwrap().owner(8190).unwrap().id(), &id(1), "the source still owns it");
        // It ends and the next starts, in one table.
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

        // Several targets go first; a node's own slot is never queued.
        s.move_slots(r(0, 1), &id(3)).unwrap();
        s.move_slots(r(9000, 9000), &id(1)).unwrap();
        s.advance(&[], &[0, 9000]);
        assert_eq!(moving(&s), [(0, 1, 3), (9000, 2, 1)]);
        assert_eq!(s.first(), [id(1), id(3)]);
        let text = s.encode();
        assert!(text.contains("\nmigrate 0 0101") && text.contains("\nfirst 0101") && text.contains("\nqueue 1 0303"), "{text}");
        assert_eq!(State::decode(&text).unwrap(), s);
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
        assert_eq!(s.first(), [id(2), id(3)], "slot 0 is node 2's now: it gets the table first still");
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
        let bad = text.replace("migrate 0 0101", "migrate 9000 0101");
        assert!(State::decode(&bad).unwrap_err().contains("slot 9000 can only migrate from its owner"));
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
}
