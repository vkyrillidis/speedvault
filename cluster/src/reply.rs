//! The replies to `CLUSTER SLOTS`, `SHARDS`, `NODES` and `INFO`, built from
//! a [`SlotTable`] as Redis 7.0.15 builds them from its view of the cluster
//! (`cluster.c`: `clusterReplyMultiBulkSlots`, `clusterReplyShards`,
//! `clusterGenNodesDescription`, `clusterGenClusterInfoString`).
//!
//! What a Redis node fills in from gossip, a table doesn't have, so it is
//! fixed:
//! - every node is a master, `health` `online`, `replication-offset` 0, its
//!   link `connected`, and its ping and pong times 0;
//! - a node's config epoch is the table's epoch;
//! - `NODES` gives the node's own port as its cluster bus port (`@port`):
//!   there is no bus, and the client port at least answers RESP.
//!
//! `SLOTS` lists ranges in slot order; `SHARDS` and `NODES` list nodes in
//! the table's order (Redis: its hash table's order), nodes without slots
//! included. Unassigned slots appear nowhere.

use crate::table::{SlotRange, SlotTable};
use crate::NodeId;
use resp::Encoder;
use std::fmt::Write;

/// `CLUSTER SLOTS`: per range, `[start, end, [host, port, id, {}]]`.
pub fn slots(table: &SlotTable, out: &mut Encoder) {
    let owned = table.runs().filter(|(_, owner)| owner.is_some()).count();
    out.array(owned);
    for (range, owner) in table.runs() {
        let Some(i) = owner else { continue };
        let node = &table.nodes()[i];
        out.array(3);
        out.integer(i64::from(range.start));
        out.integer(i64::from(range.end));
        out.array(4);
        out.bulk(node.host().as_bytes());
        out.integer(i64::from(node.port()));
        out.bulk(node.id().as_bytes());
        // Networking metadata (`hostname` when one is announced): none.
        out.map(0);
    }
}

/// `CLUSTER SHARDS`: per node, `{slots: [start, end, ...], nodes: [{id,
/// port, ip, endpoint, role, replication-offset, health}]}`.
pub fn shards(table: &SlotTable, out: &mut Encoder) {
    let by_node = table.ranges_by_node();
    out.array(table.nodes().len());
    for (node, ranges) in table.nodes().iter().zip(&by_node) {
        out.map(2);
        out.bulk(b"slots");
        out.array(2 * ranges.len());
        for range in ranges {
            out.integer(i64::from(range.start));
            out.integer(i64::from(range.end));
        }
        out.bulk(b"nodes");
        out.array(1);
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
        out.bulk(b"master");
        out.bulk(b"replication-offset");
        out.integer(0);
        out.bulk(b"health");
        out.bulk(b"online");
    }
}

/// `CLUSTER NODES`: a line per node, `id host:port@port flags - 0 0 epoch
/// connected ranges...`, as a verbatim string (a bulk string in RESP2).
/// `myself` is the node answering: its flags are `myself,master`. stuart,
/// not being a node, passes `None`.
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
    for (node, ranges) in table.nodes().iter().zip(&by_node) {
        let flags = if Some(node.id()) == myself { "myself,master" } else { "master" };
        let (id, host, port, epoch) = (node.id(), node.host(), node.port(), table.epoch());
        // Writing to a String can't fail.
        let _ = write!(text, "{id} {host}:{port}@{port} {flags} - 0 0 {epoch} connected");
        for &SlotRange { start, end } in ranges {
            let _ = if start == end { write!(text, " {start}") } else { write!(text, " {start}-{end}") };
        }
        // Only on its own line, a node shows the slots it is migrating
        // away (`[slot->-to]`) and importing (`[slot-<-from]`), in slot order.
        if Some(node.id()) == myself {
            let me = table.position(node.id());
            for m in table.migrations() {
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
            |out| slots(&t, out),
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
        check(|out| shards(&t, out), &all(b"*4\r\n", b"*14\r\n"), &all(b"%2\r\n", b"%7\r\n"));
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
        let (two, three) = encode(|out| slots(&t, out));
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

        let (two, three) = encode(|out| shards(&t, out));
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
        let (a, b) = (encode(|out| slots(&t, out)), encode(|out| slots(&three(), out)));
        assert_eq!(a, b);
        assert_eq!(encode(|out| shards(&t, out)), encode(|out| shards(&three(), out)));
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

    #[test]
    fn empty_table() {
        let t = SlotTable::empty();
        check(|out| slots(&t, out), b"*0\r\n", b"*0\r\n");
        check(|out| shards(&t, out), b"*0\r\n", b"*0\r\n");
        check(|out| nodes(&t, None, out), b"$0\r\n\r\n", b"=4\r\ntxt:\r\n");
    }
}
