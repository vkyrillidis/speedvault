//! The slot table: which node owns each of the 16384 slots, and the epoch
//! that versions it.

use crate::slot::SLOTS;
use std::fmt;

/// A node's id: 40 lowercase hex characters, Redis's format (Redis makes one
/// from 20 random bytes; so will the dbnode).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId([u8; 40]);

impl NodeId {
    /// `None` unless `text` is exactly 40 of `0-9a-f`.
    pub fn parse(text: &[u8]) -> Option<NodeId> {
        let id: [u8; 40] = text.try_into().ok()?;
        id.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')).then_some(NodeId(id))
    }

    /// The id of 20 bytes, e.g. random ones.
    pub fn from_bytes(bytes: &[u8; 20]) -> NodeId {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut id = [0; 40];
        for (i, &b) in bytes.iter().enumerate() {
            id[2 * i] = HEX[usize::from(b >> 4)];
            id[2 * i + 1] = HEX[usize::from(b & 15)];
        }
        NodeId(id)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        // Only hex digits get in.
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NodeId({})", self.as_str())
    }
}

/// A node as clients see it: its id and the address it announces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    id: NodeId,
    host: String,
    port: u16,
}

impl Node {
    /// Fails unless `host` is printable ASCII without spaces, `@` or `,`:
    /// `CLUSTER NODES` writes it into a space-separated line as
    /// `host:port@cport,hostname`.
    pub fn new(id: NodeId, host: &str, port: u16) -> Result<Node, TableError> {
        let ok = !host.is_empty() && host.bytes().all(|b| b.is_ascii_graphic() && b != b'@' && b != b',');
        if !ok {
            return Err(TableError::BadHost(host.to_string()));
        }
        Ok(Node { id, host: host.to_string(), port })
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Slots `start..=end`, both inclusive as in Redis's replies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

impl SlotRange {
    pub fn new(start: u16, end: u16) -> SlotRange {
        SlotRange { start, end }
    }

    pub fn len(&self) -> usize {
        usize::from(self.end) - usize::from(self.start) + 1
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Why a table was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableError {
    /// `start > end`, or `end` past slot 16383.
    BadRange(SlotRange),
    /// The slot is given twice (owned twice, or owned and unassigned).
    Overlap(u16),
    /// The slot is neither owned nor marked unassigned; the lowest such.
    Gap(u16),
    /// A range names a node that isn't in the node list.
    UnknownNode(NodeId),
    DuplicateNode(NodeId),
    DuplicateAddress(String, u16),
    BadHost(String),
    /// More nodes than slots.
    TooManyNodes(usize),
    /// A migration of a slot past 16383.
    BadSlot(u16),
    /// A migration from a node that doesn't own the slot (or of an
    /// unassigned slot).
    NotTheOwner(u16),
    /// A migration from the owner to itself.
    MigrationToOwner(u16),
    /// Two migrations of one slot.
    DuplicateMigration(u16),
    /// A node listed as a replica twice.
    DuplicateReplica(NodeId),
    /// A replica of itself.
    ReplicaOfItself(NodeId),
    /// A replica of a node that is a replica too: replicas don't chain.
    ReplicaOfReplica(NodeId),
    /// A replica owning slots: only primaries serve them.
    ReplicaOwnsSlots(NodeId),
    /// A slot migrating to a replica.
    ReplicaMigrates(u16),
    /// A node marked failed twice.
    DuplicateFailed(NodeId),
    /// A node given a shard twice.
    DuplicateShard(NodeId),
    /// A node given no shard.
    NoShard(NodeId),
    /// A replica in another shard than its primary's.
    ReplicaOutsideShard(NodeId),
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableError::BadRange(r) => write!(f, "invalid slot range {}-{}", r.start, r.end),
            TableError::Overlap(slot) => write!(f, "slot {slot} is given more than once"),
            TableError::Gap(slot) => write!(f, "slot {slot} is neither assigned nor unassigned"),
            TableError::UnknownNode(id) => write!(f, "unknown node {id}"),
            TableError::DuplicateNode(id) => write!(f, "node {id} is listed twice"),
            TableError::DuplicateAddress(host, port) => write!(f, "two nodes announce {host}:{port}"),
            TableError::BadHost(host) => write!(f, "invalid host '{}'", host.escape_debug()),
            TableError::TooManyNodes(n) => write!(f, "{n} nodes, at most {SLOTS}"),
            TableError::BadSlot(slot) => write!(f, "invalid slot {slot}"),
            TableError::NotTheOwner(slot) => write!(f, "slot {slot} can only migrate from its owner"),
            TableError::MigrationToOwner(slot) => write!(f, "slot {slot} migrates to its owner"),
            TableError::DuplicateMigration(slot) => write!(f, "slot {slot} migrates twice"),
            TableError::DuplicateReplica(id) => write!(f, "node {id} is listed as a replica twice"),
            TableError::ReplicaOfItself(id) => write!(f, "node {id} is a replica of itself"),
            TableError::ReplicaOfReplica(id) => write!(f, "node {id} is a replica of a replica"),
            TableError::ReplicaOwnsSlots(id) => write!(f, "replica {id} owns slots"),
            TableError::ReplicaMigrates(slot) => write!(f, "slot {slot} migrates to a replica"),
            TableError::DuplicateFailed(id) => write!(f, "node {id} is marked failed twice"),
            TableError::DuplicateShard(id) => write!(f, "node {id} is given a shard twice"),
            TableError::NoShard(id) => write!(f, "node {id} is given no shard"),
            TableError::ReplicaOutsideShard(id) => write!(f, "replica {id} is not in its primary's shard"),
        }
    }
}

impl std::error::Error for TableError {}

/// A shard id for a new shard of node `id`: its own, unless `taken`;
/// then one made from it.
fn fresh_shard(id: &NodeId, taken: impl Fn(&NodeId) -> bool) -> NodeId {
    let mut shard = *id;
    let mut k = 0u64;
    while taken(&shard) {
        k += 1;
        // FNV-1a of the id and `k`, as its first 16 characters.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in id.as_bytes().iter().chain(&k.to_le_bytes()) {
            h = (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3);
        }
        shard = *id;
        shard.0[..16].copy_from_slice(format!("{h:016x}").as_bytes());
    }
    shard
}

/// `owner` entry of a slot no node owns.
const UNASSIGNED: u16 = u16::MAX;

/// A slot on its way from the node that owns it to another. The nodes are
/// indexes into [`SlotTable::nodes`]. Two ways:
/// - key by key (phase 5), as Redis's `CLUSTER SETSLOT slot MIGRATING` on
///   `from` and `IMPORTING` on `to` put it: `from` still owns the slot and
///   serves the keys it has, redirecting the others to `to` with `-ASK`;
/// - by snapshot (phase 8, `snapshot`), as Valkey 9's and Redis 8.4's
///   atomic slot migration: `to` copies the slot from `from`, which owns it
///   and serves all of it meanwhile; `to` serves none of it (`-MOVED` to
///   `from`) until a table gives it the slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Migration {
    pub slot: u16,
    pub from: usize,
    pub to: usize,
    pub snapshot: bool,
}

/// A node that copies another (phase 8), as indexes into
/// [`SlotTable::nodes`]. A replica owns no slots; clients may read its
/// primary's slots from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Replica {
    pub replica: usize,
    pub primary: usize,
}

/// The cluster's layout: its nodes, in a fixed order (the order of
/// `CLUSTER SHARDS` and `NODES`), the owner of every slot, the slots
/// being moved, which nodes are replicas of which, and which are marked
/// failed.
#[derive(Clone, PartialEq, Eq)]
pub struct SlotTable {
    epoch: u64,
    nodes: Vec<Node>,
    /// Per slot, the index of its node in `nodes`, or `UNASSIGNED`.
    owner: Box<[u16; SLOTS]>,
    /// By slot; usually empty, and a few at most.
    migrations: Vec<Migration>,
    /// In the order given: a primary's replicas are listed (`CLUSTER
    /// SLOTS`, `SHARDS`) in this order, as Redis lists them in the order
    /// they attached. The nodes not listed are primaries.
    replicas: Vec<Replica>,
    /// The nodes marked failed (phase 8: stuart found them down), by
    /// index, in order.
    failed: Vec<usize>,
    /// Every node's shard, as `(index, shard id)` in order (see
    /// [`SlotTable::with_shards`]); `None`: the default, each primary a
    /// shard with its own id, its replicas after it.
    shards: Option<Vec<(usize, NodeId)>>,
}

impl SlotTable {
    /// Builds a table from ranges that together name every slot exactly
    /// once: with the node that owns them, or `None` for unassigned. Nodes
    /// may own no slots. Ranges may come in any order; adjacent ones with
    /// the same owner merge.
    pub fn new(epoch: u64, nodes: Vec<Node>, ranges: &[(SlotRange, Option<NodeId>)]) -> Result<SlotTable, TableError> {
        if nodes.len() > SLOTS {
            return Err(TableError::TooManyNodes(nodes.len()));
        }
        for (i, node) in nodes.iter().enumerate() {
            for other in &nodes[..i] {
                if other.id == node.id {
                    return Err(TableError::DuplicateNode(node.id));
                }
                if other.host == node.host && other.port == node.port {
                    return Err(TableError::DuplicateAddress(node.host.clone(), node.port));
                }
            }
        }
        // Filled with a marker first, to tell a gap from "unassigned".
        const MISSING: u16 = UNASSIGNED - 1;
        let mut owner: Box<[u16; SLOTS]> = vec![MISSING; SLOTS].into_boxed_slice().try_into().unwrap();
        for &(range, ref id) in ranges {
            if range.start > range.end || usize::from(range.end) >= SLOTS {
                return Err(TableError::BadRange(range));
            }
            let index = match id {
                None => UNASSIGNED,
                Some(id) => match nodes.iter().position(|n| n.id == *id) {
                    Some(i) => i as u16,
                    None => return Err(TableError::UnknownNode(*id)),
                },
            };
            for slot in range.start..=range.end {
                let entry = &mut owner[usize::from(slot)];
                if *entry != MISSING {
                    return Err(TableError::Overlap(slot));
                }
                *entry = index;
            }
        }
        if let Some(slot) = owner.iter().position(|&o| o == MISSING) {
            return Err(TableError::Gap(slot as u16));
        }
        Ok(SlotTable { epoch, nodes, owner, migrations: Vec::new(), replicas: Vec::new(), failed: Vec::new(), shards: None })
    }

    /// The same table with these slots migrating, as `(slot, from, to)`:
    /// each slot once, from the node that owns it to another known node.
    /// Replaces any migrations the table had.
    pub fn with_migrations(mut self, migrations: &[(u16, NodeId, NodeId)]) -> Result<SlotTable, TableError> {
        self.migrations.clear();
        self.add_migrations(migrations, false)
    }

    /// The same table with these slots also moving by snapshot (phase 8;
    /// see [`Migration`]), as `(slot, from, to)`, after
    /// [`with_migrations`](Self::with_migrations): the same rules, and no
    /// slot moving both ways.
    pub fn with_syncs(self, syncs: &[(u16, NodeId, NodeId)]) -> Result<SlotTable, TableError> {
        self.add_migrations(syncs, true)
    }

    fn add_migrations(mut self, migrations: &[(u16, NodeId, NodeId)], snapshot: bool) -> Result<SlotTable, TableError> {
        let mut list = std::mem::take(&mut self.migrations);
        for &(slot, from, to) in migrations {
            if usize::from(slot) >= SLOTS {
                return Err(TableError::BadSlot(slot));
            }
            let from = self.position(&from).ok_or(TableError::UnknownNode(from))?;
            let to = self.position(&to).ok_or(TableError::UnknownNode(to))?;
            if self.owner_index(slot) != Some(from) {
                return Err(TableError::NotTheOwner(slot));
            }
            if to == from {
                return Err(TableError::MigrationToOwner(slot));
            }
            if self.primary_of(to).is_some() {
                return Err(TableError::ReplicaMigrates(slot));
            }
            list.push(Migration { slot, from, to, snapshot });
        }
        list.sort_by_key(|m| m.slot);
        if let Some(w) = list.windows(2).find(|w| w[0].slot == w[1].slot) {
            return Err(TableError::DuplicateMigration(w[0].slot));
        }
        self.migrations = list;
        Ok(self)
    }

    /// The same table with these nodes replicas, as `(replica, primary)`:
    /// each replica once, of a known node that is no replica, owning no
    /// slots and with none migrating to it. Replaces any replicas the
    /// table had, and its shards with the default ones.
    pub fn with_replicas(mut self, replicas: &[(NodeId, NodeId)]) -> Result<SlotTable, TableError> {
        let mut list: Vec<Replica> = Vec::with_capacity(replicas.len());
        for &(replica, primary) in replicas {
            let r = self.position(&replica).ok_or(TableError::UnknownNode(replica))?;
            let p = self.position(&primary).ok_or(TableError::UnknownNode(primary))?;
            if r == p {
                return Err(TableError::ReplicaOfItself(replica));
            }
            if list.iter().any(|x| x.replica == r) {
                return Err(TableError::DuplicateReplica(replica));
            }
            if self.owner.iter().any(|&o| usize::from(o) == r) {
                return Err(TableError::ReplicaOwnsSlots(replica));
            }
            if let Some(m) = self.migrations.iter().find(|m| m.to == r) {
                return Err(TableError::ReplicaMigrates(m.slot));
            }
            list.push(Replica { replica: r, primary: p });
        }
        if let Some(x) = list.iter().find(|x| list.iter().any(|y| y.replica == x.primary)) {
            return Err(TableError::ReplicaOfReplica(*self.nodes[x.replica].id()));
        }
        self.replicas = list;
        self.shards = None;
        Ok(self)
    }

    /// The same table with these nodes marked failed (phase 8): known
    /// nodes, each once. Replaces any the table had. As in Redis Cluster,
    /// a failed node is `fail` in `CLUSTER NODES` and `SHARDS`, and a
    /// failed replica is left out of `SLOTS`; its primary writes without
    /// it (the lease).
    pub fn with_failed(mut self, failed: &[NodeId]) -> Result<SlotTable, TableError> {
        let mut list = Vec::with_capacity(failed.len());
        for id in failed {
            let i = self.position(id).ok_or(TableError::UnknownNode(*id))?;
            if list.contains(&i) {
                return Err(TableError::DuplicateFailed(*id));
            }
            list.push(i);
        }
        list.sort_unstable();
        self.failed = list;
        Ok(self)
    }

    /// The same table with `node` added last, owning no slots: what stuart
    /// gives a node that left the cluster while it lingers, so that it
    /// redirects as the others do (a node's table lists the node). The
    /// other nodes keep their indexes.
    pub fn with_node(mut self, node: Node) -> Result<SlotTable, TableError> {
        if self.nodes.len() == SLOTS {
            return Err(TableError::TooManyNodes(SLOTS + 1));
        }
        if self.position(&node.id).is_some() {
            return Err(TableError::DuplicateNode(node.id));
        }
        if self.nodes.iter().any(|n| n.host == node.host && n.port == node.port) {
            return Err(TableError::DuplicateAddress(node.host, node.port));
        }
        let id = node.id;
        self.nodes.push(node);
        if let Some(mut list) = self.shards.take() {
            let shard = fresh_shard(&id, |s| list.iter().any(|(_, x)| x == s));
            list.push((self.nodes.len() - 1, shard));
            self.shards = Some(list);
        }
        Ok(self)
    }

    /// The same table with its nodes in these shards, as `(node, shard
    /// id)`, after [`with_replicas`](Self::with_replicas): every node
    /// once, a replica in its primary's shard. As Redis 7.2+ groups nodes
    /// by shard id (`CLUSTER SHARDS`, `MYSHARDID`): a replica takes its
    /// primary's, and a primary failed over keeps its own, so it stays in
    /// the shard of the replica that took over. `SHARDS` lists the shards
    /// in the order of their first node here, a shard's nodes in this
    /// order. Without it, each primary is a shard with its own id, its
    /// replicas after it.
    pub fn with_shards(mut self, shards: &[(NodeId, NodeId)]) -> Result<SlotTable, TableError> {
        let mut list: Vec<(usize, NodeId)> = Vec::with_capacity(shards.len());
        for &(id, shard) in shards {
            let i = self.position(&id).ok_or(TableError::UnknownNode(id))?;
            if list.iter().any(|&(j, _)| j == i) {
                return Err(TableError::DuplicateShard(id));
            }
            list.push((i, shard));
        }
        if let Some((_, node)) = self.nodes.iter().enumerate().find(|&(i, _)| !list.iter().any(|&(j, _)| j == i)) {
            return Err(TableError::NoShard(node.id));
        }
        let shard_of = |i: usize| list.iter().find(|&&(j, _)| j == i).map(|&(_, s)| s);
        if let Some(r) = self.replicas.iter().find(|r| shard_of(r.replica) != shard_of(r.primary)) {
            return Err(TableError::ReplicaOutsideShard(self.nodes[r.replica].id));
        }
        self.set_shards(list);
        Ok(self)
    }

    /// The same table with its nodes' shards carried over from `previous`
    /// as Redis's nodes keep their shard ids: a node keeps its shard and
    /// its place in it, a primary failed over included, unless
    /// - it's a replica of a primary in another shard: it moves to the end
    ///   of its primary's (a replica's shard is its primary's);
    /// - it's a primary not marked failed and another such primary of its
    ///   shard owns slots (or neither does, but the other came first): one
    ///   back or handing over without being made a replica. A Redis shard
    ///   serves its slots from one primary; this one gets a shard of its
    ///   own, its replicas with it;
    /// - it's new: a primary gets a shard of its own, a replica its
    ///   primary's, at the end.
    ///
    /// A new shard's id is the node's own, unless a shard has that one.
    pub fn with_shards_after(mut self, previous: &SlotTable) -> SlotTable {
        let before: Vec<(NodeId, NodeId)> = previous.shard_list().into_iter().map(|(i, s)| (previous.nodes[i].id, s)).collect();
        let place = |i: usize| before.iter().position(|(n, _)| *n == self.nodes[i].id);
        let was = |i: usize| place(i).map(|p| before[p].1);
        let owns = |i: usize| self.owner.iter().any(|&o| usize::from(o) == i);
        let primaries: Vec<usize> = (0..self.nodes.len()).filter(|&i| self.primary_of(i).is_none()).collect();
        let mut shard: Vec<Option<NodeId>> = vec![None; self.nodes.len()];
        // The primaries keeping theirs: of a shard's live ones, the one
        // owning slots, else the first.
        let rank = |i: usize| (!owns(i), place(i));
        for &i in &primaries {
            let Some(s) = was(i) else { continue };
            let rival = |j: usize| j != i && !self.is_failed(j) && was(j) == Some(s) && rank(j) < rank(i);
            if self.is_failed(i) || !primaries.iter().any(|&j| rival(j)) {
                shard[i] = Some(s);
            }
        }
        let mut new = Vec::new();
        for &i in &primaries {
            if shard[i].is_none() {
                shard[i] = Some(fresh_shard(&self.nodes[i].id, |s| shard.contains(&Some(*s))));
                new.push(i);
            }
        }
        for r in &self.replicas {
            shard[r.replica] = shard[r.primary];
            if was(r.replica) != shard[r.replica] {
                new.push(r.replica);
            }
        }
        let mut list: Vec<(usize, NodeId)> =
            before.iter().filter_map(|(id, s)| self.position(id).filter(|&i| shard[i] == Some(*s)).map(|i| (i, *s))).collect();
        list.extend(new.into_iter().map(|i| (i, shard[i].unwrap())));
        self.set_shards(list);
        self
    }

    /// Keeps `None` for the default shards, so that equal tables compare
    /// equal.
    fn set_shards(&mut self, list: Vec<(usize, NodeId)>) {
        self.shards = (list != self.default_shards()).then_some(list);
    }

    /// Every node's shard, as `(index, shard id)`, in the order
    /// [`with_shards`](Self::with_shards) describes.
    pub fn shard_list(&self) -> Vec<(usize, NodeId)> {
        self.shards.clone().unwrap_or_else(|| self.default_shards())
    }

    /// Whether the shards are other than the default ones (see
    /// [`with_shards`](Self::with_shards)).
    pub fn has_shards(&self) -> bool {
        self.shards.is_some()
    }

    /// The shards in order, each with its nodes in order.
    pub fn shards(&self) -> Vec<(NodeId, Vec<usize>)> {
        let mut shards: Vec<(NodeId, Vec<usize>)> = Vec::new();
        for (i, s) in self.shard_list() {
            match shards.iter_mut().find(|(x, _)| *x == s) {
                Some((_, nodes)) => nodes.push(i),
                None => shards.push((s, vec![i])),
            }
        }
        shards
    }

    /// Node `i`'s shard id.
    pub fn shard_of(&self, i: usize) -> Option<NodeId> {
        self.shard_list().into_iter().find(|&(j, _)| j == i).map(|(_, s)| s)
    }

    fn default_shards(&self) -> Vec<(usize, NodeId)> {
        let mut list = Vec::with_capacity(self.nodes.len());
        for p in (0..self.nodes.len()).filter(|&i| self.primary_of(i).is_none()) {
            let id = self.nodes[p].id;
            list.push((p, id));
            list.extend(self.replicas_of(p).map(|r| (r, id)));
        }
        list
    }

    /// The indexes of the nodes marked failed, in order.
    pub fn failed(&self) -> &[usize] {
        &self.failed
    }

    /// Whether node `i` is marked failed.
    pub fn is_failed(&self, i: usize) -> bool {
        self.failed.binary_search(&i).is_ok()
    }

    /// The replicas, in the order given.
    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    /// The index of the primary node `i` is a replica of; `None` if it is
    /// a primary.
    pub fn primary_of(&self, i: usize) -> Option<usize> {
        self.replicas.iter().find(|r| r.replica == i).map(|r| r.primary)
    }

    /// The indexes of node `i`'s replicas, in the order given.
    pub fn replicas_of(&self, i: usize) -> impl Iterator<Item = usize> + '_ {
        self.replicas.iter().filter(move |r| r.primary == i).map(|r| r.replica)
    }

    /// The slots migrating, either way, in slot order.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// The migration of `slot`, if it is migrating.
    pub fn migration(&self, slot: u16) -> Option<&Migration> {
        if self.migrations.is_empty() {
            return None;
        }
        self.migrations.binary_search_by_key(&slot, |m| m.slot).ok().map(|i| &self.migrations[i])
    }

    /// A table without nodes, every slot unassigned: a node's view before
    /// stuart has sent it one.
    pub fn empty() -> SlotTable {
        SlotTable::new(0, Vec::new(), &[(SlotRange::new(0, SLOTS as u16 - 1), None)]).unwrap()
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The node owning `slot`, `None` if it is unassigned. Panics if `slot`
    /// isn't below 16384.
    pub fn owner(&self, slot: u16) -> Option<&Node> {
        self.owner_index(slot).map(|i| &self.nodes[i])
    }

    /// The index in [`nodes`](Self::nodes) of the node owning `slot`.
    pub fn owner_index(&self, slot: u16) -> Option<usize> {
        match self.owner[usize::from(slot)] {
            UNASSIGNED => None,
            i => Some(usize::from(i)),
        }
    }

    /// The index of the node with this id.
    pub fn position(&self, id: &NodeId) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == *id)
    }

    /// The number of slots some node owns (`cluster_slots_assigned`).
    pub fn assigned(&self) -> usize {
        self.owner.iter().filter(|&&o| o != UNASSIGNED).count()
    }

    /// The longest runs of slots with the same owner, in slot order, with
    /// the owner's index (`None`: unassigned). They cover all 16384 slots.
    pub fn runs(&self) -> Runs<'_> {
        Runs { owner: &self.owner, next: 0 }
    }

    /// Per node (by index), its slot ranges in slot order.
    pub fn ranges_by_node(&self) -> Vec<Vec<SlotRange>> {
        let mut by_node = vec![Vec::new(); self.nodes.len()];
        for (range, owner) in self.runs() {
            if let Some(i) = owner {
                by_node[i].push(range);
            }
        }
        by_node
    }
}

impl fmt::Debug for SlotTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let runs: Vec<_> = self.runs().collect();
        f.debug_struct("SlotTable")
            .field("epoch", &self.epoch)
            .field("nodes", &self.nodes)
            .field("runs", &runs)
            .field("migrations", &self.migrations)
            .field("replicas", &self.replicas)
            .field("failed", &self.failed)
            .field("shards", &self.shards)
            .finish()
    }
}

/// See [`SlotTable::runs`].
pub struct Runs<'a> {
    owner: &'a [u16; SLOTS],
    next: usize,
}

impl Iterator for Runs<'_> {
    type Item = (SlotRange, Option<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.next;
        let o = *self.owner.get(start)?;
        let len = self.owner[start..].iter().position(|&x| x != o).unwrap_or(SLOTS - start);
        self.next = start + len;
        let owner = (o != UNASSIGNED).then_some(usize::from(o));
        Some((SlotRange::new(start as u16, (start + len - 1) as u16), owner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> NodeId {
        NodeId::from_bytes(&[n; 20])
    }

    fn node(n: u8) -> Node {
        Node::new(id(n), "127.0.0.1", 7000 + u16::from(n)).unwrap()
    }

    fn r(start: u16, end: u16) -> SlotRange {
        SlotRange::new(start, end)
    }

    #[test]
    fn node_ids() {
        let text = b"2f9465cd9028f1f09dd9b69b54d1c95190365d64";
        assert_eq!(NodeId::parse(text).unwrap().as_bytes(), text);
        assert_eq!(NodeId::parse(&text[1..]), None);
        assert_eq!(NodeId::parse(b"2F9465cd9028f1f09dd9b69b54d1c95190365d64"), None);
        assert_eq!(NodeId::parse(b"2g9465cd9028f1f09dd9b69b54d1c95190365d64"), None);
        assert_eq!(NodeId::parse(b"2f9465cd9028f1f09dd9b69b54d1c95190365d645"), None);
        let mut bytes = [0; 20];
        bytes[0] = 0xab;
        bytes[19] = 0x0f;
        assert_eq!(NodeId::from_bytes(&bytes).as_str(), "ab0000000000000000000000000000000000000f");
        assert_eq!(id(1).to_string().len(), 40);
    }

    #[test]
    fn hosts() {
        for ok in ["127.0.0.1", "::1", "node-1.svc.cluster.local"] {
            assert!(Node::new(id(1), ok, 1).is_ok(), "{ok}");
        }
        for bad in ["", "a b", "a@b", "a,b", "a\nb", "a\rb", "\0", "é"] {
            assert_eq!(Node::new(id(1), bad, 1), Err(TableError::BadHost(bad.to_string())), "{bad:?}");
        }
    }

    #[test]
    fn lookup() {
        let t = SlotTable::new(
            7,
            vec![node(1), node(2), node(3)],
            &[(r(5461, 10922), Some(id(2))), (r(0, 5460), Some(id(1))), (r(10923, 16382), Some(id(3))), (r(16383, 16383), None)],
        )
        .unwrap();
        assert_eq!(t.epoch(), 7);
        for (slot, owner) in [(0, Some(0)), (5460, Some(0)), (5461, Some(1)), (10922, Some(1)), (10923, Some(2)), (16382, Some(2)), (16383, None)] {
            assert_eq!(t.owner_index(slot), owner, "slot {slot}");
            assert_eq!(t.owner(slot).map(|n| *n.id()), owner.map(|i| *t.nodes()[i].id()));
        }
        assert_eq!(t.assigned(), 16383);
        assert_eq!(t.position(&id(3)), Some(2));
        assert_eq!(t.position(&id(4)), None);
    }

    #[test]
    #[should_panic]
    fn lookup_past_the_last_slot() {
        SlotTable::empty().owner(16384);
    }

    #[test]
    fn runs_merge_and_cover() {
        let t = SlotTable::new(
            1,
            vec![node(1), node(2), node(3)],
            &[
                (r(0, 0), Some(id(1))),
                (r(1, 99), Some(id(1))),
                (r(100, 100), Some(id(2))),
                (r(101, 999), None),
                (r(1000, 1000), None),
                (r(1001, 2000), Some(id(1))),
                (r(2001, 16383), Some(id(2))),
            ],
        )
        .unwrap();
        let runs: Vec<_> = t.runs().collect();
        assert_eq!(runs, [
            (r(0, 99), Some(0)),
            (r(100, 100), Some(1)),
            (r(101, 1000), None),
            (r(1001, 2000), Some(0)),
            (r(2001, 16383), Some(1)),
        ]);
        assert_eq!(runs.iter().map(|(r, _)| r.len()).sum::<usize>(), SLOTS);
        assert_eq!(t.ranges_by_node(), [vec![r(0, 99), r(1001, 2000)], vec![r(100, 100), r(2001, 16383)], vec![]]);
        assert_eq!(t.assigned(), 16384 - 900);

        let empty = SlotTable::empty();
        assert_eq!(empty.runs().collect::<Vec<_>>(), [(r(0, 16383), None)]);
        assert_eq!(empty.assigned(), 0);
        let full = SlotTable::new(1, vec![node(1)], &[(r(0, 16383), Some(id(1)))]).unwrap();
        assert_eq!(full.runs().collect::<Vec<_>>(), [(r(0, 16383), Some(0))]);
        // A run per slot, alternating owners.
        let ranges: Vec<_> = (0..16384u16).map(|s| (r(s, s), Some(id(1 + (s % 2) as u8)))).collect();
        let striped = SlotTable::new(1, vec![node(1), node(2)], &ranges).unwrap();
        assert_eq!(striped.runs().count(), 16384);
        assert_eq!(striped.runs().last(), Some((r(16383, 16383), Some(1))));
    }

    #[test]
    fn validation() {
        let nodes = || vec![node(1), node(2)];
        let check = |ranges: &[(SlotRange, Option<NodeId>)], err: TableError| {
            assert_eq!(SlotTable::new(1, nodes(), ranges).unwrap_err(), err, "{ranges:?}");
        };
        check(&[(r(0, 16383), Some(id(1))), (r(0, 0), Some(id(2)))], TableError::Overlap(0));
        check(&[(r(0, 8191), Some(id(1))), (r(8191, 16383), Some(id(2)))], TableError::Overlap(8191));
        check(&[(r(0, 8192), Some(id(1))), (r(8000, 8100), None), (r(8193, 16383), Some(id(2)))], TableError::Overlap(8000));
        check(&[(r(0, 16383), None), (r(16383, 16383), None)], TableError::Overlap(16383));
        check(&[(r(0, 8190), Some(id(1))), (r(8192, 16383), Some(id(2)))], TableError::Gap(8191));
        check(&[(r(1, 16383), Some(id(1)))], TableError::Gap(0));
        check(&[(r(0, 16382), Some(id(1)))], TableError::Gap(16383));
        check(&[], TableError::Gap(0));
        check(&[(r(0, 16384), Some(id(1)))], TableError::BadRange(r(0, 16384)));
        check(&[(r(10, 9), Some(id(1)))], TableError::BadRange(r(10, 9)));
        check(&[(r(0, 16383), Some(id(3)))], TableError::UnknownNode(id(3)));

        let dup = SlotTable::new(1, vec![node(1), node(1)], &[(r(0, 16383), None)]);
        assert_eq!(dup.unwrap_err(), TableError::DuplicateNode(id(1)));
        let same_addr = Node::new(id(2), "127.0.0.1", 7001).unwrap();
        let dup = SlotTable::new(1, vec![node(1), same_addr], &[(r(0, 16383), None)]);
        assert_eq!(dup.unwrap_err(), TableError::DuplicateAddress("127.0.0.1".into(), 7001));
        // The same host on another port is another node.
        assert!(SlotTable::new(1, vec![node(1), node(2)], &[(r(0, 16383), None)]).is_ok());
        assert_eq!(TableError::Gap(5).to_string(), "slot 5 is neither assigned nor unassigned");
    }

    #[test]
    fn migrations() {
        let t = SlotTable::new(
            3,
            vec![node(1), node(2), node(3)],
            &[(r(0, 99), Some(id(1))), (r(100, 199), Some(id(2))), (r(200, 16382), Some(id(3))), (r(16383, 16383), None)],
        )
        .unwrap();
        assert!(t.migrations().is_empty());
        assert_eq!(t.migration(5), None);
        let m = t.clone().with_migrations(&[(150, id(2), id(1)), (5, id(1), id(3)), (6, id(1), id(2))]).unwrap();
        assert_eq!(m.migrations(), [
            Migration { slot: 5, from: 0, to: 2, snapshot: false },
            Migration { slot: 6, from: 0, to: 1, snapshot: false },
            Migration { slot: 150, from: 1, to: 0, snapshot: false },
        ]);
        assert_eq!(m.migration(150), Some(&Migration { slot: 150, from: 1, to: 0, snapshot: false }));
        // By snapshot, with them: the same rules, and no slot both ways.
        let s = m.clone().with_syncs(&[(7, id(1), id(2))]).unwrap();
        assert_eq!(s.migration(7), Some(&Migration { slot: 7, from: 0, to: 1, snapshot: true }));
        assert_eq!(s.migrations().len(), 4);
        assert_eq!(m.clone().with_syncs(&[(5, id(1), id(2))]).unwrap_err(), TableError::DuplicateMigration(5));
        assert_eq!(m.clone().with_syncs(&[(150, id(1), id(2))]).unwrap_err(), TableError::NotTheOwner(150));
        assert_eq!(s.clone().with_migrations(&[]).unwrap().migrations(), [], "with_migrations replaces both ways");
        assert_eq!(m.migration(7), None);
        assert_eq!(m.owner_index(5), Some(0), "the source still owns a migrating slot");
        assert_ne!(m, t, "migrations make another table");
        assert_eq!(m.clone().with_migrations(&[]).unwrap(), t);

        let err = |migrations: &[(u16, NodeId, NodeId)]| t.clone().with_migrations(migrations).unwrap_err();
        assert_eq!(err(&[(5, id(2), id(1))]), TableError::NotTheOwner(5));
        assert_eq!(err(&[(16383, id(1), id(2))]), TableError::NotTheOwner(16383), "unassigned");
        assert_eq!(err(&[(5, id(1), id(1))]), TableError::MigrationToOwner(5));
        assert_eq!(err(&[(5, id(1), id(2)), (5, id(1), id(3))]), TableError::DuplicateMigration(5));
        assert_eq!(err(&[(5, id(1), id(4))]), TableError::UnknownNode(id(4)));
        assert_eq!(err(&[(5, id(4), id(1))]), TableError::UnknownNode(id(4)));
        assert_eq!(err(&[(16384, id(1), id(2))]), TableError::BadSlot(16384));
        assert_eq!(TableError::NotTheOwner(5).to_string(), "slot 5 can only migrate from its owner");
    }

    #[test]
    fn replicas() {
        let t = SlotTable::new(
            2,
            vec![node(1), node(2), node(3), node(4), node(5)],
            &[(r(0, 8191), Some(id(1))), (r(8192, 16383), Some(id(2)))],
        )
        .unwrap();
        assert!(t.replicas().is_empty());
        assert_eq!(t.primary_of(2), None);
        // Node 5 before node 3 for node 1: the order given is kept.
        let rt = t.clone().with_replicas(&[(id(5), id(1)), (id(4), id(2)), (id(3), id(1))]).unwrap();
        assert_eq!(rt.replicas(), [Replica { replica: 4, primary: 0 }, Replica { replica: 3, primary: 1 }, Replica { replica: 2, primary: 0 }]);
        assert_eq!(rt.primary_of(4), Some(0));
        assert_eq!(rt.primary_of(2), Some(0));
        assert_eq!(rt.primary_of(0), None);
        assert_eq!(rt.replicas_of(0).collect::<Vec<_>>(), [4, 2]);
        assert_eq!(rt.replicas_of(1).collect::<Vec<_>>(), [3]);
        assert_eq!(rt.replicas_of(2).count(), 0);
        assert_ne!(rt, t, "replicas make another table");
        assert_eq!(rt.clone().with_replicas(&[]).unwrap(), t);
        // A primary may have no slots.
        assert!(t.clone().with_replicas(&[(id(4), id(3))]).is_ok());

        let err = |replicas: &[(NodeId, NodeId)]| t.clone().with_replicas(replicas).unwrap_err();
        assert_eq!(err(&[(id(6), id(1))]), TableError::UnknownNode(id(6)));
        assert_eq!(err(&[(id(3), id(6))]), TableError::UnknownNode(id(6)));
        assert_eq!(err(&[(id(3), id(3))]), TableError::ReplicaOfItself(id(3)));
        assert_eq!(err(&[(id(3), id(1)), (id(3), id(2))]), TableError::DuplicateReplica(id(3)));
        assert_eq!(err(&[(id(1), id(3))]), TableError::ReplicaOwnsSlots(id(1)));
        assert_eq!(err(&[(id(2), id(3))]), TableError::ReplicaOwnsSlots(id(2)));
        assert_eq!(err(&[(id(3), id(1)), (id(4), id(3))]), TableError::ReplicaOfReplica(id(4)));
        assert_eq!(err(&[(id(4), id(3)), (id(3), id(1))]), TableError::ReplicaOfReplica(id(4)), "in either order");

        // Migrations to a replica, either way round.
        let m = t.clone().with_migrations(&[(5, id(1), id(3))]).unwrap();
        assert_eq!(m.clone().with_replicas(&[(id(3), id(2))]).unwrap_err(), TableError::ReplicaMigrates(5));
        assert_eq!(rt.clone().with_migrations(&[(5, id(1), id(5))]).unwrap_err(), TableError::ReplicaMigrates(5));
        assert!(rt.clone().with_migrations(&[(5, id(1), id(2))]).is_ok());
        assert_eq!(TableError::ReplicaOfReplica(id(4)).to_string(), format!("node {} is a replica of a replica", id(4)));
    }

    #[test]
    fn failed() {
        let t = SlotTable::new(2, vec![node(1), node(2), node(3)], &[(r(0, 16383), Some(id(1)))])
            .unwrap()
            .with_replicas(&[(id(3), id(1))])
            .unwrap();
        assert!(t.failed().is_empty());
        assert!(!t.is_failed(2));
        // Kept in node order, whatever the order given.
        let f = t.clone().with_failed(&[id(3), id(2)]).unwrap();
        assert_eq!(f.failed(), [1, 2]);
        assert!(f.is_failed(1) && f.is_failed(2) && !f.is_failed(0));
        assert_ne!(f, t, "marks make another table");
        assert_eq!(f.clone().with_failed(&[]).unwrap(), t);
        // The other parts don't touch the marks.
        let f = f.with_replicas(&[]).unwrap().with_migrations(&[(5, id(1), id(2))]).unwrap();
        assert_eq!(f.failed(), [1, 2]);

        assert_eq!(t.clone().with_failed(&[id(4)]).unwrap_err(), TableError::UnknownNode(id(4)));
        assert_eq!(t.clone().with_failed(&[id(3), id(3)]).unwrap_err(), TableError::DuplicateFailed(id(3)));
        assert_eq!(TableError::DuplicateFailed(id(3)).to_string(), format!("node {} is marked failed twice", id(3)));
    }

    #[test]
    fn with_node() {
        let t = SlotTable::new(7, vec![node(1), node(2), node(3)], &[(r(0, 99), Some(id(2))), (r(100, 16383), Some(id(1)))])
            .unwrap()
            .with_syncs(&[(5, id(2), id(1))])
            .unwrap()
            .with_replicas(&[(id(3), id(1))])
            .unwrap()
            .with_failed(&[id(3)])
            .unwrap();
        let w = t.clone().with_node(node(4)).unwrap();
        assert_eq!(w.epoch(), 7);
        assert_eq!(w.nodes(), [node(1), node(2), node(3), node(4)]);
        assert_eq!(w.position(&id(4)), Some(3));
        assert!(w.runs().eq(t.runs()), "it owns nothing, the others keep their slots");
        assert_eq!((w.migrations(), w.replicas(), w.failed()), (t.migrations(), t.replicas(), t.failed()));
        assert_eq!(w.primary_of(3), None);

        assert_eq!(t.clone().with_node(node(2)).unwrap_err(), TableError::DuplicateNode(id(2)));
        let taken = Node::new(id(4), "127.0.0.1", 7001).unwrap();
        assert_eq!(t.clone().with_node(taken).unwrap_err(), TableError::DuplicateAddress("127.0.0.1".into(), 7001));
    }

    /// Nodes 1 and 2 primaries (0-8191, 8192-16383); 3 and 5 replicas of
    /// 1, 4 of 2.
    fn paired() -> SlotTable {
        SlotTable::new(1, (1..=5).map(node).collect(), &[(r(0, 8191), Some(id(1))), (r(8192, 16383), Some(id(2)))])
            .unwrap()
            .with_replicas(&[(id(3), id(1)), (id(4), id(2)), (id(5), id(1))])
            .unwrap()
    }

    /// The shards as `(shard id, [node...])`, by node number.
    fn shards_of(t: &SlotTable) -> Vec<(NodeId, Vec<u8>)> {
        t.shards().into_iter().map(|(s, nodes)| (s, nodes.into_iter().map(|i| t.nodes()[i].id().as_bytes()[1] - b'0').collect())).collect()
    }

    /// `paired` after 1 failed over to 3: 3 owns 0-8191, 5 follows it.
    fn failed_over(failed: bool) -> SlotTable {
        let t = SlotTable::new(2, (1..=5).map(node).collect(), &[(r(0, 8191), Some(id(3))), (r(8192, 16383), Some(id(2)))])
            .unwrap()
            .with_replicas(&[(id(4), id(2)), (id(5), id(3))])
            .unwrap();
        if failed { t.with_failed(&[id(1)]).unwrap() } else { t }
    }

    #[test]
    fn default_shards() {
        let t = paired();
        assert!(!t.has_shards());
        assert_eq!(shards_of(&t), [(id(1), vec![1, 3, 5]), (id(2), vec![2, 4])]);
        assert_eq!(t.shard_of(2), Some(id(1)), "node 3, a replica of 1");
        // Carried over unchanged, it stays the default.
        assert_eq!(paired().with_shards_after(&t), t);
    }

    #[test]
    fn shards_through_a_failover() {
        // The failed primary keeps its shard and its place, as in Redis:
        // first, before the replica that took over.
        let t = failed_over(true).with_shards_after(&paired());
        assert!(t.has_shards());
        assert_eq!(shards_of(&t), [(id(1), vec![1, 3, 5]), (id(2), vec![2, 4])]);
        // Back as a replica of 3: in place.
        let back = SlotTable::new(3, (1..=5).map(node).collect(), &[(r(0, 8191), Some(id(3))), (r(8192, 16383), Some(id(2)))])
            .unwrap()
            .with_replicas(&[(id(4), id(2)), (id(5), id(3)), (id(1), id(3))])
            .unwrap()
            .with_shards_after(&t);
        assert_eq!(shards_of(&back), [(id(1), vec![1, 3, 5]), (id(2), vec![2, 4])]);
        // Back as a primary without slots (or handing over): a shard of its
        // own, with an id other than its own (its old shard has that).
        let alone = failed_over(false).with_shards_after(&t);
        let s = shards_of(&alone);
        assert_eq!(s[..2], [(id(1), vec![3, 5]), (id(2), vec![2, 4])]);
        assert_eq!(s[2].1, [1]);
        assert_ne!(s[2].0, id(1));
        assert_eq!(s[2].0.as_bytes()[16..], id(1).as_bytes()[16..]);
        // Straight from the start too.
        assert_eq!(failed_over(false).with_shards_after(&paired()), alone);
        // A failed primary is no rival: 3 keeps the shard though it owns
        // nothing now (its slots moved to 2).
        let emptied = SlotTable::new(3, (1..=5).map(node).collect(), &[(r(0, 16383), Some(id(2)))])
            .unwrap()
            .with_replicas(&[(id(4), id(2)), (id(5), id(3))])
            .unwrap()
            .with_failed(&[id(1)])
            .unwrap()
            .with_shards_after(&t);
        assert_eq!(shards_of(&emptied), [(id(1), vec![1, 3, 5]), (id(2), vec![2, 4])]);
    }

    #[test]
    fn shards_of_moved_and_new_nodes() {
        let t = failed_over(true).with_shards_after(&paired());
        // 4 a replica of 3 now: to the end of 3's shard.
        let moved = failed_over(true).with_replicas(&[(id(4), id(3)), (id(5), id(3))]).unwrap().with_shards_after(&t);
        assert_eq!(shards_of(&moved), [(id(1), vec![1, 3, 5, 4]), (id(2), vec![2])]);
        // Node 6 joins (a primary), node 2 is removed: 4 and 5 follow 3.
        let nodes: Vec<Node> = [1, 3, 4, 5, 6].into_iter().map(node).collect();
        let next = SlotTable::new(3, nodes, &[(r(0, 16383), Some(id(3)))])
            .unwrap()
            .with_replicas(&[(id(4), id(3)), (id(5), id(3))])
            .unwrap()
            .with_failed(&[id(1)])
            .unwrap()
            .with_shards_after(&moved);
        assert_eq!(shards_of(&next), [(id(1), vec![1, 3, 5, 4]), (id(6), vec![6])]);
        // A node added for a node leaving gets a shard of its own.
        let lingering = next.clone().with_node(node(7)).unwrap();
        assert_eq!(shards_of(&lingering).last(), Some(&(id(7), vec![7])));
        // `with_replicas` goes back to the default shards.
        assert!(!next.with_replicas(&[]).unwrap().has_shards());
    }

    #[test]
    fn shards_given() {
        let t = failed_over(true);
        let list = [(id(1), id(1)), (id(3), id(1)), (id(5), id(1)), (id(2), id(2)), (id(4), id(2))];
        let given = t.clone().with_shards(&list).unwrap();
        assert_eq!(given, t.clone().with_shards_after(&paired()));
        let ids = |l: Vec<(usize, NodeId)>| -> Vec<(NodeId, NodeId)> { l.into_iter().map(|(i, s)| (*t.nodes()[i].id(), s)).collect() };
        assert_eq!(ids(given.shard_list()), list);
        // The default ones are no shards given.
        let default = [(id(1), id(1)), (id(2), id(2)), (id(4), id(2)), (id(3), id(3)), (id(5), id(3))];
        assert!(!t.clone().with_shards(&default).unwrap().has_shards());
        assert_eq!(t.clone().with_shards(&list[..4]).unwrap_err(), TableError::NoShard(id(4)));
        assert_eq!(t.clone().with_shards(&[list[0], list[0]]).unwrap_err(), TableError::DuplicateShard(id(1)));
        assert_eq!(t.clone().with_shards(&[(id(9), id(1))]).unwrap_err(), TableError::UnknownNode(id(9)));
        let apart = [(id(1), id(1)), (id(3), id(1)), (id(5), id(5)), (id(2), id(2)), (id(4), id(2))];
        assert_eq!(t.with_shards(&apart).unwrap_err(), TableError::ReplicaOutsideShard(id(5)));
    }
}
