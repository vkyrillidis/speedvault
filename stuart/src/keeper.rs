//! One fiber per joined node: it keeps a connection to the node, makes
//! sure the node there is the one that joined (`CLUSTER MYID`), pushes the
//! table until the node has taken it, and asks the node for its memory
//! and replication offset (`INFO memory replication`) every second, which
//! also tells that it's up. Then it gives the node every node's offset as
//! last read (`CLUSTER SETOFFSETS`, ours), if they changed: so a node's
//! `CLUSTER SLOTS` lists the replicas that have synced, as Redis's gossip
//! lets it, a second or two behind.
//! After a reconnect it pushes again (the node answers `+OK` to a table it
//! has). It ends when the node is no longer in the state (replaced),
//! except after the node left: then it lingers ([`linger`]).
//!
//! A table that starts or ends moves goes to their targets first
//! ([`State::first`](crate::state::State::first)): the other keepers wait
//! until those nodes took it (or left the cluster).

use crate::{Memory, Stuart};
use cluster::{Node, NodeId, SlotTable};
use dbnode::warning;
use reactor::{sleep, TcpStream};
use resp::{Encoder, Protocol};
use std::io::{self, Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// How often a keeper looks for a new table: a move goes through two per
/// slot, each to the target first, then the others.
const TICK: Duration = Duration::from_millis(2);
const PING_EVERY: Duration = Duration::from_secs(1);
/// Wait after a failed connection before the next try.
const RETRY: Duration = Duration::from_secs(1);
/// For connecting, and for each reply.
const TIMEOUT: Duration = Duration::from_secs(2);
/// How long at most a keeper gives tables to a node that left: past its
/// `--leave-linger` (10 s by default), it has exited.
const LINGER_MAX: Duration = Duration::from_secs(600);

/// A connection to a node.
pub(crate) struct Link {
    stream: TcpStream,
    /// The address connected to; the node may move.
    pub(crate) to: (String, u16),
    buf: Vec<u8>,
    /// The newest epoch the node took, or refused, on this connection.
    pushed: u64,
    /// The offsets last given to the node on this connection; `None` once
    /// it refused them (a node too old for `CLUSTER SETOFFSETS`).
    offsets: Option<Vec<(NodeId, u64)>>,
}

pub(crate) fn keep(stuart: Rc<Stuart>, id: NodeId) {
    let mut link: Option<Link> = None;
    let mut next_ping = Instant::now();
    stuart.keepers.set(stuart.keepers.get() + 1);
    loop {
        let Some(node) = stuart.state.borrow().node(&id).cloned() else {
            stuart.health.borrow_mut().remove(&id);
            if stuart.left.borrow_mut().remove(&id) {
                if let Some(l) = link.as_mut() {
                    linger(&stuart, &id, l);
                }
            }
            stuart.keepers.set(stuart.keepers.get() - 1);
            return;
        };
        if link.as_ref().is_some_and(|l| l.to != (node.host().to_string(), node.port())) {
            link = None;
        }
        if link.is_none() {
            // Its memory first: no slot moves to it until stuart knows
            // whether it is full.
            match connect(&node).and_then(|mut l| info(&mut l).map(|i| (l, i))) {
                Ok((l, (memory, offset))) => {
                    link = Some(l);
                    stuart.report(&id, true, None);
                    stuart.report_memory(&id, memory);
                    stuart.report_offset(&id, offset);
                }
                Err(e) => {
                    stuart.report(&id, false, Some(e.to_string()));
                    sleep(RETRY);
                    continue;
                }
            }
            next_ping = Instant::now() + PING_EVERY;
        }
        let l = link.as_mut().unwrap();
        let epoch = stuart.state.borrow().table().map(|t| t.epoch());
        let result = match epoch {
            Some(epoch) if epoch > l.pushed && stuart.may_push(&id, epoch) => {
                let table = stuart.state.borrow().table().cloned().expect("a table");
                push(&stuart, l, &id, &table)
            }
            _ if Instant::now() >= next_ping => {
                next_ping = Instant::now() + PING_EVERY;
                // Up, keeping a refusal of the table as the error.
                let refused = stuart.health.borrow().get(&id).and_then(|h| h.error.clone());
                info(l)
                    .map(|(memory, offset)| {
                        stuart.report(&id, true, refused);
                        stuart.report_memory(&id, memory);
                        stuart.report_offset(&id, offset);
                    })
                    .and_then(|()| push_offsets(&stuart, l, &id))
            }
            _ => Ok(()),
        };
        if let Err(e) = result {
            stuart.report(&id, false, Some(e.to_string()));
            link = None;
        }
        sleep(TICK);
    }
}

/// Node `id` left the cluster (`STUART LEAVE`) and lingers before it
/// exits, redirecting clients (`--leave-linger`): gives it the tables that
/// follow, with it listed owning nothing (a node's table lists it), while
/// it answers on `link`, for [`LINGER_MAX`] at most, or until it joins
/// again (another keeper takes over). Otherwise it would advertise the
/// table it was removed with, and a client that refreshes its slot map
/// from it, as Lettuce may (it prefers the view listing the most nodes it
/// knows), would go by that.
fn linger(stuart: &Stuart, id: &NodeId, link: &mut Link) {
    let Ok(node) = Node::new(*id, &link.to.0, link.to.1) else { return };
    let since = Instant::now();
    let mut next_ping = since + PING_EVERY;
    while since.elapsed() < LINGER_MAX && stuart.state.borrow().node(id).is_none() {
        let epoch = stuart.state.borrow().table().map(|t| t.epoch());
        let result = match epoch {
            Some(epoch) if epoch > link.pushed && stuart.may_push(id, epoch) => {
                // Its address taken by a node that joined: it has gone.
                let Ok(table) = stuart.state.borrow().table().cloned().expect("a table").with_node(node.clone()) else { return };
                push(stuart, link, id, &table)
            }
            // Ends once the node exits.
            _ if Instant::now() >= next_ping => {
                next_ping = Instant::now() + PING_EVERY;
                call(link, &[b"PING"]).map(drop)
            }
            _ => Ok(()),
        };
        if result.is_err() {
            return;
        }
        sleep(TICK);
    }
}

/// Connects and checks that the node there is `node`.
pub(crate) fn connect(node: &Node) -> io::Result<Link> {
    let addr = dbnode::resolve(node.host(), node.port(), TIMEOUT)?;
    let stream = TcpStream::connect_timeout(addr, TIMEOUT)?;
    let _ = stream.set_nodelay(true);
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    let mut link = Link { stream, to: (node.host().to_string(), node.port()), buf: Vec::new(), pushed: 0, offsets: Some(Vec::new()) };
    match call(&mut link, &[b"CLUSTER", b"MYID"])? {
        Reply::Bulk(id) if id == node.id().as_bytes() => Ok(link),
        Reply::Bulk(other) => Err(io::Error::other(format!(
            "another node ({}) answers at {}:{}",
            String::from_utf8_lossy(&other),
            node.host(),
            node.port()
        ))),
        Reply::Error(e) => Err(io::Error::other(format!("CLUSTER MYID: {e}"))),
        Reply::Simple(s) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("CLUSTER MYID: +{s}"))),
        Reply::Integer(n) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("CLUSTER MYID: :{n}"))),
        Reply::Array(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "CLUSTER MYID: an array")),
    }
}

fn push(stuart: &Stuart, link: &mut Link, id: &NodeId, table: &SlotTable) -> io::Result<()> {
    let args = settable_args(table);
    let args: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
    let reply = call(link, &args)?;
    link.pushed = table.epoch();
    let mut health = stuart.health.borrow_mut();
    let h = health.get_mut(id);
    match reply {
        Reply::Simple(_) => {
            if let Some(h) = h {
                h.acked = h.acked.max(table.epoch());
                h.error = None;
            }
        }
        // Not pushed again on this connection: the next table, or a
        // reconnect, tries again.
        Reply::Error(e) => {
            warning!("Node {id} refused table epoch {}: {e}", table.epoch());
            if let Some(h) = h {
                h.error = Some(format!("refused table epoch {}: {e}", table.epoch()));
            }
        }
        Reply::Bulk(_) | Reply::Integer(_) | Reply::Array(_) => {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "an unexpected reply to CLUSTER SETTABLE"))
        }
    }
    Ok(())
}

/// `INFO memory replication`: the node's memory and replication offset,
/// each `None` if it doesn't say (any reply but an error counts as up).
fn info(link: &mut Link) -> io::Result<(Option<Memory>, Option<u64>)> {
    match call(link, &[b"INFO", b"memory", b"replication"])? {
        Reply::Bulk(text) => {
            let text = String::from_utf8_lossy(&text);
            let offset = text.lines().find_map(|l| l.strip_prefix("master_repl_offset:")?.trim().parse().ok());
            Ok((Memory::parse(&text), offset))
        }
        Reply::Simple(_) | Reply::Integer(_) | Reply::Array(_) => Ok((None, None)),
        Reply::Error(e) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("INFO: {e}"))),
    }
}

/// `CLUSTER SETOFFSETS [id offset]...`: the offsets of the table's nodes
/// that are past 0 (the node takes the others as 0), if they changed since
/// the last push on this connection. A refusal is logged once, and the
/// node isn't asked again on this connection.
fn push_offsets(stuart: &Stuart, link: &mut Link, id: &NodeId) -> io::Result<()> {
    let Some(last) = &link.offsets else { return Ok(()) };
    let offsets: Vec<(NodeId, u64)> = {
        let state = stuart.state.borrow();
        let Some(table) = state.table() else { return Ok(()) };
        table.nodes().iter().zip(stuart.offsets(table)).filter(|&(_, o)| o > 0).map(|(n, o)| (*n.id(), o)).collect()
    };
    if *last == offsets {
        return Ok(());
    }
    let mut args: Vec<Vec<u8>> = vec![b"CLUSTER".to_vec(), b"SETOFFSETS".to_vec()];
    for (node, offset) in &offsets {
        args.extend([node.as_bytes().to_vec(), offset.to_string().into_bytes()]);
    }
    let args: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
    match call(link, &args)? {
        Reply::Simple(_) => link.offsets = Some(offsets),
        Reply::Error(e) => {
            warning!("Node {id} refused CLUSTER SETOFFSETS: {e}");
            link.offsets = None;
        }
        Reply::Bulk(_) | Reply::Integer(_) | Reply::Array(_) => {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "an unexpected reply to CLUSTER SETOFFSETS"))
        }
    }
    Ok(())
}

/// `CLUSTER SETTABLE epoch NODES n id host port ... RANGES m start end id|-
/// ... [MIGRATING k slot from to ...] [SYNCING k slot from to ...] [REPLICAS r
/// id primary ...] [FAILED f id ...] [SHARDS s id shard ...]`; `SHARDS` only
/// if they aren't the default ones.
pub(crate) fn settable_args(table: &SlotTable) -> Vec<Vec<u8>> {
    let mut args: Vec<Vec<u8>> = vec![b"CLUSTER".to_vec(), b"SETTABLE".to_vec(), table.epoch().to_string().into()];
    args.push(b"NODES".to_vec());
    args.push(table.nodes().len().to_string().into());
    for n in table.nodes() {
        args.extend([n.id().as_bytes().to_vec(), n.host().as_bytes().to_vec(), n.port().to_string().into()]);
    }
    let runs: Vec<_> = table.runs().collect();
    args.push(b"RANGES".to_vec());
    args.push(runs.len().to_string().into());
    for (range, owner) in runs {
        let owner = owner.map_or(b"-".to_vec(), |i| table.nodes()[i].id().as_bytes().to_vec());
        args.extend([range.start.to_string().into(), range.end.to_string().into(), owner]);
    }
    for (word, snapshot) in [(&b"MIGRATING"[..], false), (b"SYNCING", true)] {
        let moves: Vec<_> = table.migrations().iter().filter(|m| m.snapshot == snapshot).collect();
        if moves.is_empty() {
            continue;
        }
        args.push(word.to_vec());
        args.push(moves.len().to_string().into());
        for m in moves {
            let (from, to) = (table.nodes()[m.from].id(), table.nodes()[m.to].id());
            args.extend([m.slot.to_string().into(), from.as_bytes().to_vec(), to.as_bytes().to_vec()]);
        }
    }
    if !table.replicas().is_empty() {
        args.push(b"REPLICAS".to_vec());
        args.push(table.replicas().len().to_string().into());
        for r in table.replicas() {
            let (replica, primary) = (table.nodes()[r.replica].id(), table.nodes()[r.primary].id());
            args.extend([replica.as_bytes().to_vec(), primary.as_bytes().to_vec()]);
        }
    }
    if !table.failed().is_empty() {
        args.push(b"FAILED".to_vec());
        args.push(table.failed().len().to_string().into());
        args.extend(table.failed().iter().map(|&i| table.nodes()[i].id().as_bytes().to_vec()));
    }
    if table.has_shards() {
        let shards = table.shard_list();
        args.push(b"SHARDS".to_vec());
        args.push(shards.len().to_string().into());
        for (i, shard) in shards {
            args.extend([table.nodes()[i].id().as_bytes().to_vec(), shard.as_bytes().to_vec()]);
        }
    }
    args
}

/// A node's reply, of the kinds the requests above get.
#[derive(Debug)]
pub(crate) enum Reply {
    Simple(String),
    Error(String),
    Bulk(Vec<u8>),
    Integer(i64),
    Array(Vec<Reply>),
}

pub(crate) fn call(link: &mut Link, args: &[&[u8]]) -> io::Result<Reply> {
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(args.len());
    for arg in args {
        req.bulk(arg);
    }
    (&link.stream).write_all(req.as_bytes())?;
    loop {
        if let Some((reply, used)) = parse_reply(&link.buf)? {
            link.buf.drain(..used);
            return Ok(reply);
        }
        if link.buf.len() > 64 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "reply too long"));
        }
        let mut chunk = [0; 4096];
        let n = (&link.stream).read(&mut chunk)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        link.buf.extend_from_slice(&chunk[..n]);
    }
}

/// One `+`, `-`, `:`, `$` or `*` reply from the start of `buf`, and its length;
/// `None` if it isn't complete yet.
fn parse_reply(buf: &[u8]) -> io::Result<Option<(Reply, usize)>> {
    let Some(end) = buf.windows(2).position(|w| w == b"\r\n") else { return Ok(None) };
    let line = String::from_utf8_lossy(&buf[1..end]).into_owned();
    let bad = || io::Error::new(io::ErrorKind::InvalidData, format!("unexpected reply {:?}", buf[..end].escape_ascii().to_string()));
    match buf.first() {
        Some(b'+') => Ok(Some((Reply::Simple(line), end + 2))),
        Some(b'-') => Ok(Some((Reply::Error(line), end + 2))),
        Some(b':') => Ok(Some((Reply::Integer(line.parse().map_err(|_| bad())?), end + 2))),
        Some(b'$') => {
            let len: usize = line.parse().map_err(|_| bad())?;
            let start = end + 2;
            if buf.len() < start + len + 2 {
                return Ok(None);
            }
            Ok(Some((Reply::Bulk(buf[start..start + len].to_vec()), start + len + 2)))
        }
        Some(b'*') => {
            let n: usize = line.parse().map_err(|_| bad())?;
            let (mut items, mut at) = (Vec::with_capacity(n.min(64)), end + 2);
            for _ in 0..n {
                let Some((item, used)) = parse_reply(&buf[at..])? else { return Ok(None) };
                items.push(item);
                at += used;
            }
            Ok(Some((Reply::Array(items), at)))
        }
        _ => Err(bad()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster::SlotRange;

    #[test]
    fn replies() {
        assert!(matches!(parse_reply(b"+OK\r\n").unwrap(), Some((Reply::Simple(s), 5)) if s == "OK"));
        assert!(matches!(parse_reply(b"-ERR x\r\n+").unwrap(), Some((Reply::Error(s), 8)) if s == "ERR x"));
        assert!(matches!(parse_reply(b"$3\r\nab\r\n\r\n").unwrap(), Some((Reply::Bulk(b), 9)) if b == b"ab\r"));
        assert!(parse_reply(b"$3\r\nab").unwrap().is_none());
        assert!(parse_reply(b"+OK").unwrap().is_none());
        assert!(matches!(parse_reply(b":-12\r\n").unwrap(), Some((Reply::Integer(-12), 6))));
        assert!(parse_reply(b":x\r\n").is_err());
        assert!(parse_reply(b"*1\r\n").unwrap().is_none(), "an array not complete yet");
        assert!(matches!(parse_reply(b"*2\r\n:7\r\n:1\r\n+").unwrap(), Some((Reply::Array(a), 12)) if matches!(a[..], [Reply::Integer(7), Reply::Integer(1)])));
        assert!(parse_reply(b"*x\r\n").is_err());
        assert!(parse_reply(b"*1\r\n%1\r\n").is_err());
        assert!(parse_reply(b"$x\r\n").is_err());
    }

    #[test]
    fn settable() {
        let a = NodeId::from_bytes(&[1; 20]);
        let b = NodeId::from_bytes(&[2; 20]);
        let nodes = vec![Node::new(a, "10.0.0.1", 7000).unwrap(), Node::new(b, "10.0.0.2", 7001).unwrap()];
        let t = SlotTable::new(4, nodes, &[(SlotRange::new(0, 99), Some(a)), (SlotRange::new(100, 199), None), (SlotRange::new(200, 16383), Some(b))]).unwrap();
        let text = |t: &SlotTable| -> Vec<String> { settable_args(t).iter().map(|a| String::from_utf8(a.clone()).unwrap()).collect() };
        let moving = t.clone().with_migrations(&[(300, b, a), (5, a, b)]).unwrap();
        let c = NodeId::from_bytes(&[3; 20]);
        let d = NodeId::from_bytes(&[4; 20]);
        let mut nodes = t.nodes().to_vec();
        nodes.extend([Node::new(c, "10.0.0.3", 7002).unwrap(), Node::new(d, "10.0.0.4", 7003).unwrap()]);
        let ranges: Vec<_> = t.runs().map(|(r, o)| (r, o.map(|i| *t.nodes()[i].id()))).collect();
        let paired = SlotTable::new(4, nodes, &ranges).unwrap().with_replicas(&[(d, b), (c, a)]).unwrap().with_migrations(&[(5, a, b)]).unwrap();
        let (a, b, c, d) = (a.to_string(), b.to_string(), c.to_string(), d.to_string());
        assert_eq!(text(&t), [
            "CLUSTER", "SETTABLE", "4", "NODES", "2", &a, "10.0.0.1", "7000", &b, "10.0.0.2", "7001",
            "RANGES", "3", "0", "99", &a, "100", "199", "-", "200", "16383", &b,
        ]);
        assert_eq!(text(&moving)[22..], ["MIGRATING", "2", "5", &a, &b, "300", &b, &a]);
        // By snapshot, after the key-by-key moves (tables stuart no longer
        // makes, but which say so).
        let (ia, ib) = (NodeId::parse(a.as_bytes()).unwrap(), NodeId::parse(b.as_bytes()).unwrap());
        let syncing = moving.clone().with_syncs(&[(250, ib, ia), (7, ia, ib)]).unwrap();
        assert_eq!(text(&syncing)[22..], ["MIGRATING", "2", "5", &a, &b, "300", &b, &a, "SYNCING", "2", "7", &a, &b, "250", &b, &a]);
        let only = t.clone().with_syncs(&[(7, ia, ib)]).unwrap();
        assert_eq!(text(&only)[22..], ["SYNCING", "1", "7", &a, &b]);
        // Replicas last, in the table's order.
        assert_eq!(text(&paired)[28..], ["MIGRATING", "1", "5", &a, &b, "REPLICAS", "2", &d, &b, &c, &a]);
        // The failed nodes after them, in node order.
        let failed = paired.with_failed(&[NodeId::parse(d.as_bytes()).unwrap(), NodeId::parse(c.as_bytes()).unwrap()]).unwrap();
        assert_eq!(text(&failed)[39..], ["FAILED", "2", &c, &d]);
        // The shards last, only if not the default ones: here c's moved
        // to b's shard.
        assert_eq!(text(&failed).len(), 43);
        let id = |s: &str| NodeId::parse(s.as_bytes()).unwrap();
        let shards = [(id(&a), id(&a)), (id(&b), id(&b)), (id(&d), id(&b)), (id(&c), id(&a))];
        let sharded = failed.with_shards(&shards).unwrap();
        assert_eq!(text(&sharded)[43..], ["SHARDS", "4", &a, &a, &b, &b, &d, &b, &c, &a]);
    }
}
