//! Moving the keys of slots migrating away (phase 5): one fiber looks for a
//! slot the table says migrates from this node and still has keys here,
//! and sends them to the slot's new node in batches, `CLUSTER IMPORT slot n
//! key ttl value ...`, over a connection of its own.
//!
//! A batch's keys are in flight until the target acknowledges it (commands
//! touching them wait, see [`crate::cluster_mode`]); then they're deleted
//! here. A batch the target refuses (it doesn't import the slot yet: stuart
//! tells it first, but a table may be late) is sent again a second later.
//! If the connection fails, the keys of the batch are dirty: sent again,
//! first thing, once it's back, with their state then (a key deleted
//! meanwhile as a delete). The node at the other end must be the slot's
//! target (`CLUSTER MYID`): one restarted at its address is another node.
//!
//! Slots are moved one after the other, lowest first. A target that can't
//! be reached, or refuses, isn't tried again for a second: meanwhile the
//! slots migrating to other nodes go on (one connection per target). Once
//! a slot has nothing pending (`CLUSTER PENDING`), stuart gives it to the
//! target.

use crate::cluster_mode::Cluster;
use crate::{unix_ms, verbose, warning, Migrate, Node};
use cluster::NodeId;
use reactor::{sleep, yield_now, TcpStream};
use resp::{Encoder, Protocol};
use store::Kind;
use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// How often the fiber looks for a slot to move: while the table has
/// migrations from here (the next slot's arrives with the table that ends
/// the last one), and while it has none.
const BUSY: Duration = Duration::from_millis(1);
const IDLE: Duration = Duration::from_millis(50);
/// The wait after a failed connection or a refused batch.
const RETRY: Duration = Duration::from_secs(1);
/// For connecting, resolving a name, and each reply.
const TIMEOUT: Duration = Duration::from_secs(2);
/// A batch stops once its values reach this size (with one key at least).
const BATCH_BYTES: usize = 1024 * 1024;

/// The connection to a slot's target.
struct Link {
    stream: TcpStream,
    addr: (String, u16),
    buf: Vec<u8>,
}

/// Waits a little for keys in flight to land: one turn of the reactor at
/// first (a batch takes a round trip), a millisecond at a time once that
/// has gone on for a while (a slow or unreachable target).
pub(crate) fn wait(waited: &mut u32) {
    *waited += 1;
    if *waited < 1000 {
        yield_now();
    } else {
        sleep(Duration::from_millis(1));
    }
}

pub(crate) fn migrate_loop(node: Rc<Node>) {
    let cluster = node.cluster.as_ref().expect("migrate_loop runs in cluster mode");
    let mut links: HashMap<NodeId, Link> = HashMap::new();
    // Targets that failed, and until when they aren't tried again; those
    // whose failure was logged (once, until a batch gets through).
    let mut resting: HashMap<NodeId, Instant> = HashMap::new();
    let mut failing: HashSet<NodeId> = HashSet::new();
    // The slot being moved, for the log.
    let mut current = None;
    // Commands with keys served before the last batch.
    let mut served = cluster.served.get();
    loop {
        let now = Instant::now();
        resting.retain(|_, until| *until > now);
        let Some((slot, target)) = next_slot(&node, cluster, &resting) else {
            if let Some(slot) = current.filter(|&s| !pending_here(&node, cluster, s)) {
                verbose!("Slot {slot} has no keys left here");
                current = None;
            }
            let (table, me) = cluster.table();
            let moving = table.migrations().iter().any(|m| m.from == me);
            drop(table);
            if !moving {
                links.clear();
            }
            sleep(if moving { BUSY } else { IDLE });
            continue;
        };
        if current != Some(slot) {
            if let Some(done) = current.filter(|&s| !pending_here(&node, cluster, s)) {
                verbose!("Slot {done} has no keys left here");
            }
            let n = node.store.borrow().count_in_slot(slot);
            verbose!("Moving slot {slot} ({n} keys) to {} at {}:{}", target.id(), target.host(), target.port());
            current = Some(slot);
            served = cluster.served.get();
        }
        let (id, addr) = (*target.id(), (target.host().to_string(), target.port()));
        if links.get(&id).is_some_and(|l| l.addr != addr) {
            links.remove(&id);
        }
        if let std::collections::hash_map::Entry::Vacant(e) = links.entry(id) {
            match connect(&target) {
                Ok(l) => {
                    e.insert(l);
                }
                Err(err) => {
                    if failing.insert(id) {
                        warning!("Can't reach {id} at {}:{} to move slot {slot}: {err}; retrying every second", addr.0, addr.1);
                    }
                    resting.insert(id, Instant::now() + RETRY);
                    continue;
                }
            }
        }
        match send_batch(&node, cluster, links.get_mut(&id).unwrap(), slot, &node.config.migrate) {
            Ok(Ok(sent)) => {
                failing.remove(&id);
                let pause = node.config.migrate.pause;
                let clients = cluster.served.get() != served;
                served = cluster.served.get();
                if sent == 0 || pause.is_zero() || !clients {
                    yield_now();
                } else {
                    sleep(pause);
                }
            }
            Ok(Err(refused)) => {
                if failing.insert(id) {
                    warning!("{id} refused keys of slot {slot}: {refused}; retrying every second");
                }
                resting.insert(id, Instant::now() + RETRY);
            }
            Err(e) => {
                if failing.insert(id) {
                    warning!("Moving slot {slot} to {id}: {e}; retrying every second");
                }
                links.remove(&id);
                resting.insert(id, Instant::now() + RETRY);
            }
        }
    }
}

/// The lowest slot migrating from here that has keys pending, and its
/// target, leaving out the targets in `resting`.
fn next_slot(node: &Node, cluster: &Cluster, resting: &HashMap<NodeId, Instant>) -> Option<(u16, cluster::Node)> {
    let (table, me) = cluster.table();
    table
        .migrations()
        .iter()
        .filter(|m| m.from == me && !resting.contains_key(table.nodes()[m.to].id()))
        .find(|m| pending_here(node, cluster, m.slot))
        .map(|m| (m.slot, table.nodes()[m.to].clone()))
}

/// Whether `slot` still has keys here, or dirty ones.
fn pending_here(node: &Node, cluster: &Cluster, slot: u16) -> bool {
    node.store.borrow().count_in_slot(slot) > 0 || cluster.moving.borrow().dirty.get(&slot).is_some_and(|d| !d.is_empty())
}

/// Sends one batch of `slot`'s keys: the dirty ones first, then others.
/// `Ok(Ok(n))`: the target took `n` keys (0: there were only expired ones,
/// now reclaimed), deleted here. `Ok(Err(..))`: it refused them, nothing
/// changed. `Err`: the connection failed, the batch is dirty.
fn send_batch(node: &Node, cluster: &Cluster, link: &mut Link, slot: u16, config: &Migrate) -> io::Result<Result<usize, String>> {
    let mut body = Encoder::new(Protocol::Resp2);
    let mut sent: Vec<Vec<u8>> = Vec::new();
    let mut was_dirty: HashSet<Vec<u8>> = HashSet::new();
    let mut bytes = 0;
    {
        let mut store = node.store.borrow_mut();
        store.set_now(unix_ms());
        let mut moving = cluster.moving.borrow_mut();
        let chunk = config.chunk.max(1);
        let mut elements = 0;
        let full = |n: usize, bytes: usize, elements: usize| n >= config.batch.max(1) || bytes >= BATCH_BYTES || elements >= chunk;
        // A collection larger than a batch goes alone, in parts, when it
        // comes first; after others, it waits for a batch of its own.
        let mut big = None;
        if let Some(dirty) = moving.dirty.get_mut(&slot) {
            let keys: Vec<Vec<u8>> = dirty.iter().take(config.batch.max(1)).cloned().collect();
            for key in keys {
                if full(sent.len(), bytes, elements) {
                    break;
                }
                let n = store.elements(&key).unwrap_or(1);
                if n > chunk {
                    if sent.is_empty() {
                        big = Some(key);
                        break;
                    }
                    continue;
                }
                elements += n;
                match store.dump(&key) {
                    Some((value, ttl)) => {
                        entry(&mut body, &key, ttl.unwrap_or(0), value.kind(), value.payload());
                        bytes += value.payload().len();
                    }
                    None => entry(&mut body, &key, -1, Kind::Str, b""),
                }
                dirty.remove(&key);
                was_dirty.insert(key.clone());
                sent.push(key);
            }
        }
        let mut expired = Vec::new();
        let fresh: Vec<Vec<u8>> = store
            .keys_in_slot(slot)
            .filter(|k| !was_dirty.contains(*k))
            .take(config.batch.max(1).saturating_sub(sent.len()))
            .map(<[u8]>::to_vec)
            .collect();
        for key in fresh.into_iter().filter(|_| big.is_none()) {
            if full(sent.len(), bytes, elements) {
                break;
            }
            let n = store.elements(&key).unwrap_or(1);
            if n > chunk {
                if sent.is_empty() {
                    big = Some(key);
                    break;
                }
                continue;
            }
            elements += n;
            match store.dump(&key) {
                Some((value, ttl)) => {
                    entry(&mut body, &key, ttl.unwrap_or(0), value.kind(), value.payload());
                    bytes += value.payload().len();
                    sent.push(key);
                }
                None => expired.push(key),
            }
        }
        // Expired, never sent: reclaimed rather than moved.
        for key in &expired {
            store.exists(key);
        }
        if let Some(key) = big.filter(|_| sent.is_empty()) {
            drop((store, moving));
            return send_big(node, cluster, link, slot, key, config);
        }
        if sent.is_empty() {
            return Ok(Ok(0));
        }
        moving.in_flight.extend(sent.iter().cloned());
    }

    let mut req = Encoder::new(Protocol::Resp2);
    req.array(4 + 4 * sent.len());
    for arg in [&b"CLUSTER"[..], b"IMPORT", slot.to_string().as_bytes(), sent.len().to_string().as_bytes()] {
        req.bulk(arg);
    }
    let result = (&link.stream).write_all(req.as_bytes()).and_then(|()| (&link.stream).write_all(body.as_bytes())).and_then(|()| read_reply(link));
    let mut moving = cluster.moving.borrow_mut();
    moving.in_flight.clear();
    match result {
        Ok(Reply::Simple(_)) => {
            let mut store = node.store.borrow_mut();
            for key in &sent {
                store.del(key);
            }
            let stats = &cluster.stats;
            stats.keys_sent.set(stats.keys_sent.get() + sent.len() as u64);
            stats.batches_sent.set(stats.batches_sent.get() + 1);
            stats.bytes_sent.set(stats.bytes_sent.get() + bytes as u64);
            Ok(Ok(sent.len()))
        }
        // Nothing was applied: the keys are as before, the dirty ones still
        // dirty (an earlier batch may have landed).
        Ok(Reply::Error(e)) => {
            moving.dirty.entry(slot).or_default().extend(was_dirty);
            Ok(Err(e))
        }
        Ok(Reply::Bulk(_)) => {
            moving.dirty.entry(slot).or_default().extend(sent);
            Err(io::Error::new(io::ErrorKind::InvalidData, "a bulk reply to CLUSTER IMPORT"))
        }
        Err(e) => {
            moving.dirty.entry(slot).or_default().extend(sent);
            Err(e)
        }
    }
}

/// Sends a collection too large for a batch, in parts of `config.chunk`
/// elements (and at most [`BATCH_BYTES`]), one `CLUSTER IMPORTPART` a round
/// trip, pausing between them as between batches. The key is in flight
/// from the first part until the target has taken the last one; then it's
/// deleted here and freed a part's worth at a time. A key gone meanwhile
/// (expired) is sent as a delete. Refused part way (the target filled up,
/// or lost the import), the move of the key is given up: it isn't in
/// flight any more, it's dirty (the target may hold parts of it, which a
/// later attempt or delete replaces), and it's sent again from the start.
/// Returns as [`send_batch`] does.
fn send_big(node: &Node, cluster: &Cluster, link: &mut Link, slot: u16, key: Vec<u8>, config: &Migrate) -> io::Result<Result<usize, String>> {
    let was_dirty = {
        let mut moving = cluster.moving.borrow_mut();
        moving.in_flight.insert(key.clone());
        moving.dirty.get_mut(&slot).is_some_and(|d| d.remove(&key))
    };
    // Ends the move of the key: not in flight; dirty if it may be at the
    // target in any form.
    let settle = |dirty: bool| {
        let mut moving = cluster.moving.borrow_mut();
        moving.in_flight.remove(&key);
        if dirty {
            moving.dirty.entry(slot).or_default().insert(key.clone());
        }
    };
    let stats = &cluster.stats;
    let mut offset = 0;
    // Commands with keys served before the last part.
    let mut served = cluster.served.get();
    loop {
        let part = {
            let mut store = node.store.borrow_mut();
            store.set_now(unix_ms());
            store.dump_part(&key, offset, config.chunk.max(1), BATCH_BYTES)
        };
        let mut req = Encoder::new(Protocol::Resp2);
        let last = match &part {
            // Expired (nothing else changes a key in flight): a delete.
            None => {
                req.array(8);
                for arg in [&b"CLUSTER"[..], b"IMPORT", slot.to_string().as_bytes(), b"1", &key, b"-1", b"string", b""] {
                    req.bulk(arg);
                }
                true
            }
            Some(part) => {
                req.array(9);
                let ttl = part.ttl.unwrap_or(0).to_string();
                let (total, at) = (part.total.to_string(), offset.to_string());
                for arg in [&b"CLUSTER"[..], b"IMPORTPART", slot.to_string().as_bytes(), &key, part.kind.name(), total.as_bytes(), at.as_bytes(), &part.payload, ttl.as_bytes()] {
                    req.bulk(arg);
                }
                offset + part.count == part.total
            }
        };
        let result = (&link.stream).write_all(req.as_bytes()).and_then(|()| read_reply(link));
        match result {
            Ok(Reply::Simple(_)) => {}
            Ok(Reply::Error(e)) => {
                settle(was_dirty || offset > 0);
                stats.parts_aborted.set(stats.parts_aborted.get() + 1);
                return Ok(Err(e));
            }
            Ok(Reply::Bulk(_)) => {
                settle(true);
                return Err(io::Error::new(io::ErrorKind::InvalidData, "a bulk reply to CLUSTER IMPORTPART"));
            }
            Err(e) => {
                settle(true);
                return Err(e);
            }
        }
        if let Some(part) = &part {
            offset += part.count;
            stats.parts_sent.set(stats.parts_sent.get() + 1);
            stats.bytes_sent.set(stats.bytes_sent.get() + part.payload.len() as u64);
        }
        if last {
            let garbage = node.store.borrow_mut().take(&key);
            settle(false);
            stats.keys_sent.set(stats.keys_sent.get() + 1);
            if let Some(mut garbage) = garbage {
                while !garbage.free(config.chunk.max(1)) {
                    yield_now();
                }
            }
            return Ok(Ok(1));
        }
        let clients = cluster.served.get() != served;
        served = cluster.served.get();
        if config.pause.is_zero() || !clients {
            yield_now();
        } else {
            sleep(config.pause);
        }
    }
}

/// One `key ttl type value` of `CLUSTER IMPORT`.
fn entry(body: &mut Encoder, key: &[u8], ttl: i64, kind: Kind, value: &[u8]) {
    body.bulk(key);
    body.bulk(ttl.to_string().as_bytes());
    body.bulk(kind.name());
    body.bulk(value);
}

/// Connects to `target` and checks that it is that node.
fn connect(target: &cluster::Node) -> io::Result<Link> {
    let addr = crate::resolve(target.host(), target.port(), TIMEOUT)?;
    let stream = TcpStream::connect_timeout(addr, TIMEOUT)?;
    let _ = stream.set_nodelay(true);
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    let mut link = Link { stream, addr: (target.host().to_string(), target.port()), buf: Vec::new() };
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(2);
    req.bulk(b"CLUSTER");
    req.bulk(b"MYID");
    (&link.stream).write_all(req.as_bytes())?;
    match read_reply(&mut link)? {
        Reply::Bulk(id) if id == target.id().as_bytes() => Ok(link),
        Reply::Bulk(other) => Err(io::Error::other(format!("another node ({}) answers there", String::from_utf8_lossy(&other)))),
        Reply::Error(e) => Err(io::Error::other(format!("CLUSTER MYID: {e}"))),
        Reply::Simple(s) => Err(io::Error::new(io::ErrorKind::InvalidData, format!("CLUSTER MYID: +{s}"))),
    }
}

/// A reply of the kinds the requests above get.
#[derive(Debug)]
enum Reply {
    Simple(String),
    Error(String),
    Bulk(Vec<u8>),
}

fn read_reply(link: &mut Link) -> io::Result<Reply> {
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

/// One `+`, `-` or `$` reply from the start of `buf`, and its length;
/// `None` if it isn't complete yet.
fn parse_reply(buf: &[u8]) -> io::Result<Option<(Reply, usize)>> {
    let Some(end) = buf.windows(2).position(|w| w == b"\r\n") else { return Ok(None) };
    let line = String::from_utf8_lossy(&buf[1..end]).into_owned();
    let bad = || io::Error::new(io::ErrorKind::InvalidData, format!("unexpected reply {:?}", buf[..end].escape_ascii().to_string()));
    match buf.first() {
        Some(b'+') => Ok(Some((Reply::Simple(line), end + 2))),
        Some(b'-') => Ok(Some((Reply::Error(line), end + 2))),
        Some(b'$') => {
            let len: usize = line.parse().map_err(|_| bad())?;
            let start = end + 2;
            if buf.len() < start + len + 2 {
                return Ok(None);
            }
            Ok(Some((Reply::Bulk(buf[start..start + len].to_vec()), start + len + 2)))
        }
        _ => Err(bad()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies() {
        assert!(matches!(parse_reply(b"+OK\r\n").unwrap(), Some((Reply::Simple(s), 5)) if s == "OK"));
        assert!(matches!(parse_reply(b"-ERR x\r\n+").unwrap(), Some((Reply::Error(s), 8)) if s == "ERR x"));
        assert!(matches!(parse_reply(b"$3\r\nab\r\n\r\n").unwrap(), Some((Reply::Bulk(b), 9)) if b == b"ab\r"));
        assert!(parse_reply(b"$3\r\nab").unwrap().is_none());
        assert!(parse_reply(b"+OK").unwrap().is_none());
        assert!(parse_reply(b":1\r\n").is_err());
    }
}
