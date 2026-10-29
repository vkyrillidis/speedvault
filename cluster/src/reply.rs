//! The replies to `CLUSTER SLOTS`, `SHARDS`, `NODES` and `INFO`, built from
//! a [`SlotTable`] as Redis 8.10.2 builds them from its view of the cluster
//! (`cluster.c`: `clusterReplyMultiBulkSlots`, `clusterReplyShards`,
//! `clusterGenNodesDescription`, `clusterGenClusterInfoString`).
//!
//! What a Redis node fills in from gossip, a table doesn't have, so it is
//! fixed:
//! - every node's link is `connected`, except a node marked failed's
//!   (`disconnected`), and its ping and pong times 0;
//! - a node's config epoch is the table's epoch;
//! - `NODES` gives the node's own port as its cluster bus port (`@port`):
//!   there is no bus, and the client port at least answers RESP.
//!
//! The nodes' replication offsets are passed in ([`Offsets`]). As in
//! Redis, a replica whose offset is 0 (it has taken nothing from its
//! primary yet) is left out of `SLOTS` (`isReplicaAvailable`) and is
//! `health` `loading` in `SHARDS`; other nodes are `online`.
//!
//! A node the table marks failed (stuart found it down) is shown as Redis
//! 7.0.15 shows a node in `FAIL`: `fail` among its `NODES` flags, its link
//! `disconnected`, `health` `fail` in `SHARDS` (before `loading`), and, a
//! replica, left out of `SLOTS` whatever its offset (`isReplicaAvailable`
//! checks `nodeFailed` first).
//!
//! `SLOTS` lists ranges in slot order, each with its primary, then the
//! primary's replicas; `SHARDS` lists the table's shards in its order
//! (Redis: its hash table's order), each with its nodes in order (as
//! Redis 8.10.2 groups them by shard id: see
//! [`SlotTable::with_shards`]), and `NODES` every node in the table's
//! order, nodes without slots included.
//! Unassigned slots appear nowhere.

use crate::table::{SlotRange, SlotTable};
use crate::NodeId;
use resp::Encoder;
use std::fmt::Write;

/// The nodes' replication offsets, by index in the table; nodes past the
/// end have 0. `&[]`: every node 0, as until replicas copy anything.
pub type Offsets<'a> = &'a [u64];

fn offset(offsets: Offsets, i: usize) -> u64 {
    offsets.get(i).copied().unwrap_or(0)
}

/// Whether `SLOTS` lists replica `i`: once it has taken something,
/// unless it's marked failed.
fn available(table: &SlotTable, offsets: Offsets, i: usize) -> bool {
    !table.is_failed(i) && offset(offsets, i) != 0
}

/// `CLUSTER SLOTS`: per range, `[start, end, [host, port, id, {}], ...]`,
/// the primary first, then its replicas.
pub fn slots(table: &SlotTable, offsets: Offsets, out: &mut Encoder) {
    let owned = table.runs().filter(|(_, owner)| owner.is_some()).count();
    out.array(owned);
    for (range, owner) in table.runs() {
        let Some(i) = owner else { continue };
        let replicas: Vec<usize> = table.replicas_of(i).filter(|&r| available(table, offsets, r)).collect();
        out.array(3 + replicas.len());
        out.integer(i64::from(range.start));
        out.integer(i64::from(range.end));
        for n in std::iter::once(i).chain(replicas) {
            let node = &table.nodes()[n];
            out.array(4);
            out.bulk(node.host().as_bytes());
            out.integer(i64::from(node.port()));
            out.bulk(node.id().as_bytes());
            // Networking metadata (`hostname` when one is announced): none.
            out.map(0);
        }
    }
}

/// `CLUSTER SHARDS`: per shard, `{slots: [start, end, ...], nodes: [{id,
/// port, ip, endpoint, role, replication-offset, health}, ...]}`. As
/// Redis 8.10.2 (`clusterGetMasterFromShard`), the slots are those of the
/// primary of the shard's first node not marked failed: none if all are.
pub fn shards(table: &SlotTable, offsets: Offsets, out: &mut Encoder) {
    let by_node = table.ranges_by_node();
    let shards = table.shards();
    out.array(shards.len());
    for (_, shard) in shards {
        let primary = shard.iter().find(|&&n| !table.is_failed(n)).map(|&n| table.primary_of(n).unwrap_or(n));
        let ranges = primary.map_or(&[][..], |p| &by_node[p][..]);
        out.map(2);
        out.bulk(b"slots");
        out.array(2 * ranges.len());
        for range in ranges {
            out.integer(i64::from(range.start));
            out.integer(i64::from(range.end));
        }
        out.bulk(b"nodes");
        out.array(shard.len());
        for n in shard {
            let node = &table.nodes()[n];
            let replica = table.primary_of(n).is_some();
            out.map(7);
            out.bulk(b"id");
            out.bulk(node.id().as_bytes());
            out.bulk(b"port");
            out.integer(i64::from(node.port()));
            out.bulk(b"ip");
            out.bulk(node.host().as_bytes());
            out.bulk(b"endpoint");
            out.bulk(node.host().as_bytes());
            out.bulk(b"role");
            out.bulk(if replica { b"replica" } else { b"master" });
            out.bulk(b"replication-offset");
            out.integer(offset(offsets, n) as i64);
            out.bulk(b"health");
            let health: &[u8] = if table.is_failed(n) {
                b"fail"
            } else if replica && offset(offsets, n) == 0 {
                b"loading"
            } else {
                b"online"
            };
            out.bulk(health);
        }
    }
}

/// `CLUSTER NODES`: a line per node, `id host:port@port flags primary 0 0
/// epoch connected ranges...`, as a verbatim string (a bulk string in
/// RESP2). The flags are `master` or `slave`, then `,fail` for a node
/// marked failed (whose link is `disconnected`), and `primary` is `-` or a
/// replica's primary. `myself` is the node answering: its flags start with
/// `myself,`. stuart, not being a node, passes `None`.
pub fn nodes(table: &SlotTable, myself: Option<&NodeId>, out: &mut Encoder) {
    out.verbatim(b"txt", nodes_text(table, myself).as_bytes());
}

/// The text of `CLUSTER INFO` (a verbatim string, as for `NODES`). The
/// state is `ok` while some slot is assigned: with
/// `cluster-require-full-coverage no`, Redis also needs a majority of the
/// masters serving slots to be reachable, and a table has them all. There
/// is no bus, so no messages.
pub fn info_text(table: &SlotTable) -> String {
    let assigned = table.assigned();
    let size = table.ranges_by_node().iter().filter(|r| !r.is_empty()).count();
    let epoch = table.epoch();
    let mut text = String::new();
    for (key, value) in [
        ("cluster_state", if assigned > 0 { "ok".to_string() } else { "fail".to_string() }),
        ("cluster_slots_assigned", assigned.to_string()),
        ("cluster_slots_ok", assigned.to_string()),
        ("cluster_slots_pfail", "0".to_string()),
        ("cluster_slots_fail", "0".to_string()),
        ("cluster_known_nodes", table.nodes().len().to_string()),
        ("cluster_size", size.to_string()),
        ("cluster_current_epoch", epoch.to_string()),
        ("cluster_my_epoch", epoch.to_string()),
        ("cluster_stats_messages_sent", "0".to_string()),
        ("cluster_stats_messages_received", "0".to_string()),
        ("total_cluster_links_buffer_limit_exceeded", "0".to_string()),
    ] {
        let _ = write!(text, "{key}:{value}\r\n");
    }
    text
}

/// The text of [`nodes`].
pub fn nodes_text(table: &SlotTable, myself: Option<&NodeId>) -> String {
    let by_node = table.ranges_by_node();
    let mut text = String::new();
    for (i, (node, ranges)) in table.nodes().iter().zip(&by_node).enumerate() {
        let primary = table.primary_of(i).map(|p| table.nodes()[p].id());
        let role = if primary.is_some() { "slave" } else { "master" };
        let me = if Some(node.id()) == myself { "myself," } else { "" };
        let (fail, link) = if table.is_failed(i) { (",fail", "disconnected") } else { ("", "connected") };
        let primary = primary.map_or("-".to_string(), |p| p.to_string());
        let (id, host, port, epoch) = (node.id(), node.host(), node.port(), table.epoch());
        // Writing to a String can't fail.
        let _ = write!(text, "{id} {host}:{port}@{port} {me}{role}{fail} {primary} 0 0 {epoch} {link}");
        for &SlotRange { start, end } in ranges {
            let _ = if start == end { write!(text, " {start}") } else { write!(text, " {start}-{end}") };
        }
        // Only on its own line, a node shows the slots it is migrating
        // away (`[slot->-to]`) and importing (`[slot-<-from]`), in slot
        // order: key by key (as `CLUSTER SETSLOT` makes them); a slot moving
        // by snapshot isn't shown, as with Redis 8.4's atomic migration.
        if Some(node.id()) == myself {
            let me = table.position(node.id());
            for m in table.migrations().iter().filter(|m| !m.snapshot) {
                let (from, to) = (table.nodes()[m.from].id(), table.nodes()[m.to].id());
                let _ = if Some(m.from) == me {
                    write!(text, " [{}->-{to}]", m.slot)
                } else if Some(m.to) == me {
                    write!(text, " [{}-<-{from}]", m.slot)
                } else {
                    Ok(())
                };
            }
        }
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, SlotRange};
    use resp::Protocol;

    // The node ids and order of a 3-node Redis 7.0.15 cluster made by
    // `redis-cli --cluster create`; the expected bytes below are its replies.
    const A: &[u8] = b"2f9465cd9028f1f09dd9b69b54d1c95190365d64";
    const B: &[u8] = b"7df638c21e01c25ed7e78895b279ccba6e8335bd";
    const C: &[u8] = b"0a51616ef7024fb46ab43dac6ef92d925a501108";

    fn id(text: &[u8]) -> NodeId {
        NodeId::parse(text).unwrap()
    }

    fn node(text: &[u8], port: u16) -> Node {
        Node::new(id(text), "127.0.0.1", port).unwrap()
    }

    /// Nodes in Redis's (hash table) order: B, A, C.
    fn three() -> SlotTable {
        SlotTable::new(
            3,
            vec![node(B, 7712), node(A, 7711), node(C, 7713)],
            &[
                (SlotRange::new(0, 5460), Some(id(A))),
                (SlotRange::new(5461, 10922), Some(id(B))),
                (SlotRange::new(10923, 16383), Some(id(C))),
            ],
        )
        .unwrap()
    }

    fn encode(f: impl Fn(&mut Encoder)) -> (String, String) {
        let mut two = Encoder::new(Protocol::Resp2);
        let mut three = Encoder::new(Protocol::Resp3);
        f(&mut two);
        f(&mut three);
        (two.as_bytes().escape_ascii().to_string(), three.as_bytes().escape_ascii().to_string())
    }

    fn check(f: impl Fn(&mut Encoder), resp2: &[u8], resp3: &[u8]) {
        let (two, three) = encode(f);
        assert_eq!(two, resp2.escape_ascii().to_string(), "RESP2");
        assert_eq!(three, resp3.escape_ascii().to_string(), "RESP3");
    }

    #[test]
    fn slots_like_redis() {
        let t = three();
        check(
            |out| slots(&t, &[], out),
            b"*3\r\n*3\r\n:0\r\n:5460\r\n*4\r\n$9\r\n127.0.0.1\r\n:7711\r\n$40\r\n2f9465cd9028f1f09dd9b69b54d1c95190365d64\r\n*0\r\n*3\r\n:5461\r\n:10922\r\n*4\r\n$9\r\n127.0.0.1\r\n:7712\r\n$40\r\n7df638c21e01c25ed7e78895b279ccba6e8335bd\r\n*0\r\n*3\r\n:10923\r\n:16383\r\n*4\r\n$9\r\n127.0.0.1\r\n:7713\r\n$40\r\n0a51616ef7024fb46ab43dac6ef92d925a501108\r\n*0\r\n",
            b"*3\r\n*3\r\n:0\r\n:5460\r\n*4\r\n$9\r\n127.0.0.1\r\n:7711\r\n$40\r\n2f9465cd9028f1f09dd9b69b54d1c95190365d64\r\n%0\r\n*3\r\n:5461\r\n:10922\r\n*4\r\n$9\r\n127.0.0.1\r\n:7712\r\n$40\r\n7df638c21e01c25ed7e78895b279ccba6e8335bd\r\n%0\r\n*3\r\n:10923\r\n:16383\r\n*4\r\n$9\r\n127.0.0.1\r\n:7713\r\n$40\r\n0a51616ef7024fb46ab43dac6ef92d925a501108\r\n%0\r\n",
        );
    }

    #[test]
    fn shards_like_redis() {
        let t = three();
        let shard = |open: &[u8], id: &[u8], port: &[u8], first: &[u8], last: &[u8], node: &[u8]| {
            [
                open, b"$5\r\nslots\r\n*2\r\n:", first, b"\r\n:", last, b"\r\n$5\r\nnodes\r\n*1\r\n", node,
                b"$2\r\nid\r\n$40\r\n", id, b"\r\n$4\r\nport\r\n:", port,
                b"\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n$6\r\nonline\r\n",
            ]
            .concat()
        };
        let all = |open: &[u8], node: &[u8]| {
            [
                &b"*3\r\n"[..],
                &shard(open, B, b"7712", b"5461", b"10922", node),
                &shard(open, A, b"7711", b"0", b"5460", node),
                &shard(open, C, b"7713", b"10923", b"16383", node),
            ]
            .concat()
        };
        check(|out| shards(&t, &[], out), &all(b"*4\r\n", b"*14\r\n"), &all(b"%2\r\n", b"%7\r\n"));
    }

    #[test]
    fn nodes_like_redis() {
        let t = three();
        // Redis's, with our fixed fields: ping/pong 0, the table's epoch,
        // the client port as the bus port.
        let text = "7df638c21e01c25ed7e78895b279ccba6e8335bd 127.0.0.1:7712@7712 master - 0 0 3 connected 5461-10922\n\
                    2f9465cd9028f1f09dd9b69b54d1c95190365d64 127.0.0.1:7711@7711 myself,master - 0 0 3 connected 0-5460\n\
                    0a51616ef7024fb46ab43dac6ef92d925a501108 127.0.0.1:7713@7713 master - 0 0 3 connected 10923-16383\n";
        let resp2 = format!("${}\r\n{text}\r\n", text.len());
        let resp3 = format!("={}\r\ntxt:{text}\r\n", text.len() + 4);
        check(|out| nodes(&t, Some(&id(A)), out), resp2.as_bytes(), resp3.as_bytes());
        assert_eq!(nodes_text(&t, None), text.replace("myself,", ""));
    }

    /// Several ranges per node, single slots, unassigned slots and a node
    /// without slots.
    #[test]
    fn uneven_layout() {
        let t = SlotTable::new(
            9,
            vec![node(A, 7001), node(B, 7002), node(C, 7003)],
            &[
                (SlotRange::new(0, 99), Some(id(A))),
                (SlotRange::new(100, 100), Some(id(B))),
                (SlotRange::new(101, 199), None),
                (SlotRange::new(200, 16382), Some(id(A))),
                (SlotRange::new(16383, 16383), Some(id(B))),
            ],
        )
        .unwrap();
        let (two, three) = encode(|out| slots(&t, &[], out));
        let range = |s: &str, e: &str, port: &str, id: &[u8], meta: &str| {
            format!("*3\r\n:{s}\r\n:{e}\r\n*4\r\n$9\r\n127.0.0.1\r\n:{port}\r\n$40\r\n{}\r\n{meta}\r\n", id.escape_ascii())
        };
        let all = |meta: &str| {
            "*4\r\n".to_string()
                + &range("0", "99", "7001", A, meta)
                + &range("100", "100", "7002", B, meta)
                + &range("200", "16382", "7001", A, meta)
                + &range("16383", "16383", "7002", B, meta)
        };
        assert_eq!(two, all("*0").escape_default().to_string());
        assert_eq!(three, all("%0").escape_default().to_string());

        let (two, three) = encode(|out| shards(&t, &[], out));
        assert!(two.starts_with("*3\r\n*4\r\n$5\r\nslots\r\n*4\r\n:0\r\n:99\r\n:200\r\n:16382\r\n".escape_default().to_string().as_str()), "{two}");
        assert!(two.contains("$5\r\nslots\r\n*4\r\n:100\r\n:100\r\n:16383\r\n:16383\r\n".escape_default().to_string().as_str()), "{two}");
        // C has no slots, but is a shard.
        assert!(two.contains("$5\r\nslots\r\n*0\r\n$5\r\nnodes\r\n*1\r\n*14\r\n$2\r\nid\r\n$40\r\n0a51".escape_default().to_string().as_str()), "{two}");
        assert!(three.contains("%2\r\n$5\r\nslots\r\n*0\r\n$5\r\nnodes\r\n*1\r\n%7\r\n$2\r\nid\r\n$40\r\n0a51".escape_default().to_string().as_str()), "{three}");

        assert_eq!(
            nodes_text(&t, Some(&id(C))),
            "2f9465cd9028f1f09dd9b69b54d1c95190365d64 127.0.0.1:7001@7001 master - 0 0 9 connected 0-99 200-16382\n\
             7df638c21e01c25ed7e78895b279ccba6e8335bd 127.0.0.1:7002@7002 master - 0 0 9 connected 100 16383\n\
             0a51616ef7024fb46ab43dac6ef92d925a501108 127.0.0.1:7003@7003 myself,master - 0 0 9 connected\n"
        );
    }

    /// Redis 7.0.15 after `SETSLOT 100 MIGRATING <B>` and `SETSLOT 5000
    /// IMPORTING <C>` on A (and the reverse on the others): only A's own
    /// line has them, after its ranges.
    #[test]
    fn nodes_with_migrations() {
        let t = three().with_migrations(&[(5000, id(A), id(B)), (100, id(A), id(B)), (12000, id(C), id(A)), (6000, id(B), id(C))]).unwrap();
        let text = nodes_text(&t, Some(&id(A)));
        let mine = text.lines().find(|l| l.contains("myself")).unwrap();
        assert_eq!(
            mine,
            "2f9465cd9028f1f09dd9b69b54d1c95190365d64 127.0.0.1:7711@7711 myself,master - 0 0 3 connected 0-5460 \
             [100->-7df638c21e01c25ed7e78895b279ccba6e8335bd] [5000->-7df638c21e01c25ed7e78895b279ccba6e8335bd] \
             [12000-<-0a51616ef7024fb46ab43dac6ef92d925a501108]"
        );
        assert!(text.lines().filter(|l| !l.contains("myself")).all(|l| !l.contains('[')), "{text}");
        let b = nodes_text(&t, Some(&id(B)));
        assert!(b.contains("connected 5461-10922 [100-<-2f94"), "{b}");
        assert!(b.contains("d64] [5000-<-2f94") && b.contains("d64] [6000->-0a51"), "{b}");
        assert!(!nodes_text(&t, None).contains('['), "stuart's view: no markers");
        // The other replies don't change.
        let (a, b) = (encode(|out| slots(&t, &[], out)), encode(|out| slots(&three(), &[], out)));
        assert_eq!(a, b);
        assert_eq!(encode(|out| shards(&t, &[], out)), encode(|out| shards(&three(), &[], out)));
    }

    #[test]
    fn info() {
        let text = info_text(&three());
        assert!(text.starts_with(
            "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\ncluster_known_nodes:3\r\ncluster_size:3\r\ncluster_current_epoch:3\r\ncluster_my_epoch:3\r\n"
        ), "{text}");
        assert!(text.ends_with("total_cluster_links_buffer_limit_exceeded:0\r\n"));
        let empty = info_text(&SlotTable::empty());
        assert!(empty.starts_with("cluster_state:fail\r\ncluster_slots_assigned:0\r\n"), "{empty}");
        assert!(empty.contains("\r\ncluster_known_nodes:0\r\ncluster_size:0\r\n"), "{empty}");
    }

    // A Redis 7.0.15 cluster of three primaries with a replica each, made
    // by `redis-cli --cluster create ... --cluster-replicas 1`, as 7951 saw
    // it (its NODES order); the expected bytes are its replies.
    const P1: &[u8] = b"653533c4720cbcf1ceb468acbcfec37af239d597";
    const P2: &[u8] = b"5a6862f1475981beaaf267e1a1b65b90a0defa99";
    const P3: &[u8] = b"2abbe717a79a559f4e5f2bdd954bcb3dee70ee42";
    const R2: &[u8] = b"6ea4592ae5d2133de3e05c49a161a9bacfe6f128";
    const R3: &[u8] = b"9609db4753668fcd59244368c8f01dc3ebda5891";
    const R1: &[u8] = b"db96e7014e453a9d1cf65244cdd27fc7b86fb58f";

    fn with_replicas() -> SlotTable {
        SlotTable::new(
            1,
            vec![node(R3, 7955), node(P1, 7951), node(P2, 7952), node(R2, 7954), node(P3, 7953), node(R1, 7956)],
            &[
                (SlotRange::new(0, 5460), Some(id(P1))),
                (SlotRange::new(5461, 10922), Some(id(P2))),
                (SlotRange::new(10923, 16383), Some(id(P3))),
            ],
        )
        .unwrap()
        .with_replicas(&[(id(R3), id(P3)), (id(R2), id(P2)), (id(R1), id(P1))])
        .unwrap()
    }

    fn entry(port: u16, id: &[u8], meta: &str) -> String {
        format!("*4\r\n$9\r\n127.0.0.1\r\n:{port}\r\n$40\r\n{}\r\n{meta}\r\n", std::str::from_utf8(id).unwrap())
    }

    #[test]
    fn slots_with_replicas() {
        let t = with_replicas();
        // Before the replicas have taken anything: the primaries alone.
        assert_eq!(encode(|out| slots(&t, &[], out)), encode(|out| slots(&three_like(&t), &[], out)));
        let range = |s: &str, e: &str, nodes: &[(u16, &[u8])], meta: &str| {
            format!("*{}\r\n:{s}\r\n:{e}\r\n", 2 + nodes.len()) + &nodes.iter().map(|(p, i)| entry(*p, i, meta)).collect::<String>()
        };
        let all = |meta: &str| {
            "*3\r\n".to_string()
                + &range("0", "5460", &[(7951, P1), (7956, R1)], meta)
                + &range("5461", "10922", &[(7952, P2), (7954, R2)], meta)
                + &range("10923", "16383", &[(7953, P3), (7955, R3)], meta)
        };
        // Redis's, once every replica's offset was past 0 (14, 14, 54).
        let offsets = [54, 14, 14, 14, 54, 14];
        check(|out| slots(&t, &offsets, out), all("*0").as_bytes(), all("%0").as_bytes());
        // Only the replicas with an offset.
        let some = [0, 1, 1, 0, 1, 0];
        let (two, _) = encode(|out| slots(&t, &some, out));
        let want = "*3\r\n".to_string()
            + &range("0", "5460", &[(7951, P1)], "*0")
            + &range("5461", "10922", &[(7952, P2)], "*0")
            + &range("10923", "16383", &[(7953, P3)], "*0");
        assert_eq!(two, want.as_bytes().escape_ascii().to_string());
    }

    /// The primaries of `t` alone, for what `SLOTS` shows without replicas.
    fn three_like(t: &SlotTable) -> SlotTable {
        t.clone().with_replicas(&[]).unwrap()
    }

    #[test]
    fn shards_with_replicas() {
        let t = with_replicas();
        let member = |open: &str, id: &[u8], port: u16, role: &str, offset: u64, health: &str| {
            format!(
                "{open}$2\r\nid\r\n$40\r\n{}\r\n$4\r\nport\r\n:{port}\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n\
                 $8\r\nendpoint\r\n$9\r\n127.0.0.1\r\n$4\r\nrole\r\n${}\r\n{role}\r\n\
                 $18\r\nreplication-offset\r\n:{offset}\r\n$6\r\nhealth\r\n${}\r\n{health}\r\n",
                std::str::from_utf8(id).unwrap(),
                role.len(),
                health.len()
            )
        };
        let shard = |map: &str, node: &str, first: &str, last: &str, members: &[(&[u8], u16, &str, u64, &str)]| {
            format!("{map}$5\r\nslots\r\n*2\r\n:{first}\r\n:{last}\r\n$5\r\nnodes\r\n*{}\r\n", members.len())
                + &members.iter().map(|(i, p, r, o, h)| member(node, i, *p, r, *o, h)).collect::<String>()
        };
        // Redis's before any write: every offset 0, the replicas loading.
        let all = |map: &str, node: &str, offsets: [u64; 6]| {
            let health = |o: u64| if o == 0 { "loading" } else { "online" };
            "*3\r\n".to_string()
                + &shard(map, node, "0", "5460", &[(P1, 7951, "master", offsets[1], "online"), (R1, 7956, "replica", offsets[5], health(offsets[5]))])
                + &shard(map, node, "5461", "10922", &[(P2, 7952, "master", offsets[2], "online"), (R2, 7954, "replica", offsets[3], health(offsets[3]))])
                + &shard(map, node, "10923", "16383", &[(P3, 7953, "master", offsets[4], "online"), (R3, 7955, "replica", offsets[0], health(offsets[0]))])
        };
        check(|out| shards(&t, &[], out), all("*4\r\n", "*14\r\n", [0; 6]).as_bytes(), all("%2\r\n", "%7\r\n", [0; 6]).as_bytes());
        // Redis's later: offsets 14 and 54, everyone online.
        let offsets = [54, 14, 14, 14, 54, 14];
        check(|out| shards(&t, &offsets, out), all("*4\r\n", "*14\r\n", offsets).as_bytes(), all("%2\r\n", "%7\r\n", offsets).as_bytes());
    }

    #[test]
    fn nodes_with_replicas() {
        let t = with_replicas();
        // Redis's from 7956 (a replica), in 7951's order, with our fixed
        // fields: ping/pong 0, the table's epoch, the client port as the
        // bus port.
        let text = |me: &[u8]| {
            let line = |i: &[u8], port: u16, rest: &str| {
                let myself = if i == me { "myself," } else { "" };
                format!("{} 127.0.0.1:{port}@{port} {myself}{rest}\n", std::str::from_utf8(i).unwrap())
            };
            let s = |p: &[u8]| std::str::from_utf8(p).unwrap().to_string();
            line(R3, 7955, &format!("slave {} 0 0 1 connected", s(P3)))
                + &line(P1, 7951, "master - 0 0 1 connected 0-5460")
                + &line(P2, 7952, "master - 0 0 1 connected 5461-10922")
                + &line(R2, 7954, &format!("slave {} 0 0 1 connected", s(P2)))
                + &line(P3, 7953, "master - 0 0 1 connected 10923-16383")
                + &line(R1, 7956, &format!("slave {} 0 0 1 connected", s(P1)))
        };
        assert_eq!(nodes_text(&t, Some(&id(R1))), text(R1));
        assert!(nodes_text(&t, Some(&id(R1))).contains(" myself,slave 653533c4720cbcf1ceb468acbcfec37af239d597 0 0 1 connected\n"));
        assert_eq!(nodes_text(&t, Some(&id(P1))), text(P1));
        assert_eq!(nodes_text(&t, None), text(b""));
        // INFO: the replicas are known nodes, not shards serving slots.
        let info = info_text(&t);
        assert!(info.contains("\r\ncluster_known_nodes:6\r\ncluster_size:3\r\n"), "{info}");
    }

    /// Redis 7.0.15 (3+3, `cluster-node-timeout 2000`) once a replica was
    /// SIGKILLed and marked `FAIL`: its line in `NODES` at the others,
    /// `SLOTS` without it, `SHARDS` with `health fail` whatever its offset.
    #[test]
    fn a_failed_replica() {
        let t = with_replicas().with_failed(&[id(R1)]).unwrap();
        let offsets = [54, 14, 14, 14, 54, 14];
        let text = nodes_text(&t, Some(&id(P1)));
        let s = |p: &[u8]| std::str::from_utf8(p).unwrap().to_string();
        let r1 = format!("{} 127.0.0.1:7956@7956 slave,fail {} 0 0 1 disconnected", s(R1), s(P1));
        assert_eq!(text.lines().last(), Some(r1.as_str()));
        assert_eq!(text.replace(",fail", "").replace("disconnected", "connected"), nodes_text(&with_replicas(), Some(&id(P1))), "the other lines as before");
        // SLOTS: the failed replica left out, the others listed.
        let (two, _) = encode(|out| slots(&t, &offsets, out));
        assert!(!two.contains(&s(R1)) && two.contains(&s(R2)) && two.contains(&s(R3)), "{two}");
        assert!(two.starts_with(&format!("*3\r\n*3\r\n:0\r\n:5460\r\n{}*4\r\n:5461", entry(7951, P1, "*0")).escape_default().to_string()), "{two}");
        // SHARDS: fail, before loading too; the rest unchanged.
        for offsets in [&offsets[..], &[]] {
            let (two, three) = encode(|out| shards(&t, offsets, out));
            let (two_before, _) = encode(|out| shards(&with_replicas(), &[54, 14, 14, 14, 54, 14], out));
            let fail = "$6\r\nhealth\r\n$4\r\nfail\r\n".escape_default().to_string();
            assert_eq!(two.matches(&fail).count(), 1, "{two}");
            assert_eq!(three.matches(&fail).count(), 1, "{three}");
            assert!(two.find(&fail) > two.find(&s(R1)), "{two}");
            if offsets.len() == 6 {
                let online = "$6\r\nhealth\r\n$6\r\nonline\r\n".escape_default().to_string();
                assert_eq!(two.replacen(&fail, &online, 1), two_before);
            }
        }
        // A failed primary: master,fail; it stays in SLOTS (it owns them).
        let p = with_replicas().with_failed(&[id(P2)]).unwrap();
        assert!(nodes_text(&p, None).contains(&format!("{} 127.0.0.1:7952@7952 master,fail - 0 0 1 disconnected 5461-10922\n", s(P2))));
        assert_eq!(encode(|out| slots(&p, &offsets, out)), encode(|out| slots(&with_replicas(), &offsets, out)));
        // The marks don't change INFO.
        assert_eq!(info_text(&t), info_text(&with_replicas()));
    }

    /// Redis 8.10.2 (3+3) once primary P2 was SIGKILLed and its replica R2
    /// took over: `SHARDS` lists P2 in its shard still, first, `master`
    /// and `fail`, the slots R2's.
    #[test]
    fn shards_after_a_failover() {
        let before = with_replicas();
        let t = SlotTable::new(
            2,
            before.nodes().to_vec(),
            &[
                (SlotRange::new(0, 5460), Some(id(P1))),
                (SlotRange::new(5461, 10922), Some(id(R2))),
                (SlotRange::new(10923, 16383), Some(id(P3))),
            ],
        )
        .unwrap()
        .with_replicas(&[(id(R3), id(P3)), (id(R1), id(P1))])
        .unwrap()
        .with_failed(&[id(P2)])
        .unwrap()
        .with_shards_after(&before);
        let (two, _) = encode(|out| shards(&t, &[], out));
        let s = |p: &[u8]| std::str::from_utf8(p).unwrap().to_string();
        let shard = format!(
            "$5\r\nslots\r\n*2\r\n:5461\r\n:10922\r\n$5\r\nnodes\r\n*2\r\n*14\r\n$2\r\nid\r\n$40\r\n{}\r\n$4\r\nport\r\n:7952\r\n",
            s(P2)
        )
        .escape_default()
        .to_string();
        assert!(two.starts_with("*3\\r\\n"), "{two}");
        assert!(two.contains(&shard), "{two}");
        let p2 = &two[two.find(&s(P2)).unwrap()..two.find(&s(R2)).unwrap()];
        assert!(p2.contains("master") && p2.contains("fail"), "{p2}");
        let r2 = &two[two.find(&s(R2)).unwrap()..][..300];
        assert!(r2.contains("$6\\r\\nmaster") && r2.contains("online"), "{r2}");
        // Every node of a shard failed: no slots.
        let all = t.clone().with_failed(&[id(P2), id(R2)]).unwrap();
        let (two, _) = encode(|out| shards(&all, &[], out));
        assert!(two.contains(&format!("*0\r\n$5\r\nnodes\r\n*2\r\n*14\r\n$2\r\nid\r\n$40\r\n{}", s(P2)).escape_default().to_string()), "{two}");
        // None either when the first of them owns slots.
        let all = t.clone().with_failed(&[id(P2), id(P1), id(R1)]).unwrap();
        let (two, _) = encode(|out| shards(&all, &[], out));
        assert!(two.contains(&format!("*0\r\n$5\r\nnodes\r\n*2\r\n*14\r\n$2\r\nid\r\n$40\r\n{}", s(P1)).escape_default().to_string()), "{two}");
        // SLOTS and NODES are as without shards.
        let plain = t.clone().with_replicas(&[(id(R3), id(P3)), (id(R1), id(P1))]).unwrap().with_failed(&[id(P2)]).unwrap();
        assert_eq!(encode(|out| slots(&t, &[], out)), encode(|out| slots(&plain, &[], out)));
        assert_eq!(nodes_text(&t, None), nodes_text(&plain, None));
    }

    #[test]
    fn empty_table() {
        let t = SlotTable::empty();
        check(|out| slots(&t, &[], out), b"*0\r\n", b"*0\r\n");
        check(|out| shards(&t, &[], out), b"*0\r\n", b"*0\r\n");
        check(|out| nodes(&t, None, out), b"$0\r\n\r\n", b"=4\r\ntxt:\r\n");
    }
}
