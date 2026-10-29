//! The Redis Cluster model shared by the dbnode and stuart: which slot a key
//! belongs to, which node owns each slot, and the replies that tell clients.
//!
//! - [`key_hash_slot`] is Redis's `keyHashSlot`: `CRC16(key) mod 16384`,
//!   hashing only the `{tag}` when the key has one, so stock cluster clients
//!   route keys exactly as they do for Redis.
//! - [`SlotTable`] maps every slot to a node (or to none), with the epoch
//!   stuart versions it by. It is checked when built: every slot is either
//!   owned by exactly one node or explicitly unassigned. Looking a slot up is
//!   one index into a flat 16384-entry array. It may also list slots
//!   migrating from their owner to another node ([`Migration`]), and the
//!   nodes that are replicas of others ([`Replica`], phase 8).
//! - [`reply`] encodes `CLUSTER SLOTS`, `SHARDS` and `NODES` from a table, in
//!   RESP2 and RESP3, as Redis 7.0.15 does (checked byte for byte by
//!   `tools/compat/cluster_compat.sh` against a real Redis Cluster).
//!
//! ```
//! use cluster::{key_hash_slot, Node, NodeId, SlotRange, SlotTable};
//!
//! let a = NodeId::parse(b"2f9465cd9028f1f09dd9b69b54d1c95190365d64").unwrap();
//! let b = NodeId::parse(b"7df638c21e01c25ed7e78895b279ccba6e8335bd").unwrap();
//! let table = SlotTable::new(
//!     1,
//!     vec![Node::new(a, "10.0.0.1", 7000).unwrap(), Node::new(b, "10.0.0.2", 7000).unwrap()],
//!     &[(SlotRange::new(0, 8191), Some(a)), (SlotRange::new(8192, 16383), Some(b))],
//! )
//! .unwrap();
//! assert_eq!(key_hash_slot(b"foo"), 12182);
//! assert_eq!(table.owner(12182).unwrap().host(), "10.0.0.2");
//! ```

pub mod reply;
mod slot;
mod table;

pub use slot::{crc16, key_hash_slot, SLOTS};
pub use table::{Migration, Node, NodeId, Replica, SlotRange, SlotTable, TableError};
