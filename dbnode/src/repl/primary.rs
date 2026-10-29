//! A primary's side of replication: `REPLSYNC` from a replica turns its
//! connection into the replica's feed, which a loop here sends, copying the
//! keys batch by batch while the replica syncs from scratch. See the
//! [module docs](super).

use super::Feed;
use crate::{unix_ms, verbose, warning, Node};
use cluster::{NodeId, SLOTS};
use reactor::{sleep, yield_now, TcpStream};
use resp::{Args, Encoder, Limits, Parsed, Parser, Protocol};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::rc::Rc;
use std::time::Instant;

/// A batch of the copy stops at this many keys (a slot sync's at
/// `--migrate-batch`) or bytes of values (one key at
/// least): the replica's output is filled again once it's sent. Unit tests
/// take a few keys at a time, so that writes come between batches.
const BATCH_KEYS: usize = if cfg!(test) { 5 } else { 1000 };
const BATCH_BYTES: usize = 1 << 20;

/// `REPLSYNC id replid offset`: `None` (the reply written) unless it may
/// go on, as a feed: the node is a primary in cluster mode and the table
/// makes `id` its replica.
pub(crate) fn attach(args: &Args, node: &Node, out: &mut Encoder) -> Option<Rc<Feed>> {
    let Some(cluster) = &node.cluster else {
        out.error(b"ERR This instance has cluster support disabled");
        return None;
    };
    let Some(id) = NodeId::parse(&args[1]) else {
        out.error_parts(&[b"ERR Invalid node id '", &args[1], b"'"]);
        return None;
    };
    let offset = match &args[3][..] {
        b"-1" => None,
        o => match store::parse_i64(o) {
            Some(o) if o >= 0 => Some(o as u64),
            _ => {
                out.error_parts(&[b"ERR Invalid offset '", o, b"'"]);
                return None;
            }
        },
    };
    let mine = {
        let (table, me) = cluster.table();
        table.position(&id).is_some_and(|i| table.primary_of(i) == Some(me))
    };
    if !mine {
        out.error_parts(&[b"ERR The table doesn't make ", id.as_bytes(), b" a replica of this node"]);
        return None;
    }
    let feed = node.repl.attach(&mut node.store.borrow_mut(), id, &args[2], offset);
    let how = if feed.copying() { "syncs from scratch" } else { "carries on from the backlog" };
    crate::notice!("Replica {id} attached: it {how}");
    Some(feed)
}

/// `SLOTSYNC id slot...`: `None` (the reply written) unless it may go on,
/// as a feed of those slots: the node is in cluster mode and the table
/// moves each of them by snapshot from this node to `id`.
pub(crate) fn attach_slots(args: &Args, node: &Node, out: &mut Encoder) -> Option<Rc<Feed>> {
    let Some(cluster) = &node.cluster else {
        out.error(b"ERR This instance has cluster support disabled");
        return None;
    };
    let Some(id) = NodeId::parse(&args[1]) else {
        out.error_parts(&[b"ERR Invalid node id '", &args[1], b"'"]);
        return None;
    };
    let mut slots = Vec::with_capacity(args.len() - 2);
    for arg in (2..args.len()).map(|i| &args[i]) {
        match store::parse_i64(arg).filter(|s| (0..SLOTS as i64).contains(s)) {
            Some(s) => slots.push(s as u16),
            None => {
                out.error_parts(&[b"ERR Invalid slot '", arg, b"'"]);
                return None;
            }
        }
    }
    {
        let (table, me) = cluster.table();
        let to = table.position(&id);
        if let Some(&slot) = slots.iter().find(|&&s| !table.migration(s).is_some_and(|m| m.snapshot && m.from == me && Some(m.to) == to)) {
            out.error(format!("ERR The table doesn't move slot {slot} from this node to {id}").as_bytes());
            return None;
        }
    }
    let feed = node.repl.attach_slots(&mut node.store.borrow_mut(), id, slots);
    let n = feed.slots.as_ref().map_or(0, Vec::len);
    verbose!("Node {id} syncs {n} slots from here");
    Some(feed)
}

/// Sends `feed` over `conn` until the connection fails or the feed is
/// dropped; the feed is detached however it ends. Meanwhile a fiber of its
/// own reads the replica's acknowledgements ([`acks`]), on a copy of the
/// socket: shut down when this ends, so that it ends too.
pub(crate) fn serve(conn: &TcpStream, node: &Node, feed: Rc<Feed>) -> io::Result<()> {
    struct Detach<'a>(&'a Node, &'a Rc<Feed>, &'a TcpStream);
    impl Drop for Detach<'_> {
        fn drop(&mut self) {
            self.0.repl.detach(self.1);
            let _ = self.2.shutdown(Shutdown::Both);
        }
    }
    let _detach = Detach(node, &feed, conn);
    let reader = TcpStream::from_std(conn.get_ref().try_clone()?)?;
    let f = feed.clone();
    reactor::spawn(move || acks(reader, f))?;
    let started = Instant::now();
    let mut chunk = Vec::new();
    // Commands with keys served before the last batch (a slot sync).
    let mut served = node.cluster.as_ref().map_or(0, |c| c.served.get());
    loop {
        if feed.dropped.get() {
            return Err(io::Error::other("its output went past the limit, or it attached again"));
        }
        if feed.gone.get() {
            return Err(io::Error::other("the replica hung up"));
        }
        let ping = feed.ping_if_due(Instant::now());
        std::mem::swap(&mut chunk, &mut *feed.out.borrow_mut());
        if !chunk.is_empty() {
            // Other fibers append meanwhile: the output isn't borrowed.
            let mut conn = conn;
            conn.write_all(&chunk)?;
            feed.wrote(chunk.len());
            chunk.clear();
            continue;
        }
        if feed.copying() {
            // A batch, and a ping after it; the next batch once little
            // enough is unacknowledged (Feed::may_send), so that a ping
            // (the lease) never waits long behind the batches, however slow
            // the link. One batch at a time costs a fast link half its
            // speed.
            if !feed.may_send() {
                feed.notify.wait_until(Some(ping));
                continue;
            }
            node.store.borrow_mut().set_now(unix_ms());
            if fill(node, &feed) {
                let what = if feed.is_replica() { "Replica" } else { "Node (its slots)" };
                verbose!("{what} {} has every key, in {:.1} s", feed.id, started.elapsed().as_secs_f64());
            }
            feed.ping(Instant::now());
            // Clients go between batches; a slot sync, a move, also pauses
            // for `--migrate-pause` if they were served meanwhile, as
            // phase 5's moves did, so that it takes less of a busy source.
            match &node.cluster {
                Some(c) if feed.slots.is_some() && !node.config.migrate.pause.is_zero() && c.served.get() != served => {
                    served = c.served.get();
                    sleep(node.config.migrate.pause);
                }
                _ => yield_now(),
            }
            continue;
        }
        // A slot sync ended (`SLOTSDONE` sent above).
        if feed.done.get() {
            return Ok(());
        }
        feed.notify.wait_until(Some(ping));
    }
}

/// Puts the next batch of the copy into the feed's output (the keys live
/// at the store's time); at the end, `REPLSYNCED offset`, and the feed gets
/// the stream from there. Returns whether that was the end.
pub(super) fn fill(node: &Node, feed: &Feed) -> bool {
    let chunk = node.config.migrate.chunk.max(1);
    let store = node.store.borrow();
    let mut copy = feed.copy.borrow_mut();
    let Some(c) = copy.as_mut() else { return false };
    let mut batch = Encoder::new(Protocol::Resp2);
    let (mut keys, mut bytes) = (0, 0);
    let most = if feed.slots.is_some() { BATCH_KEYS.min(node.config.migrate.batch.max(1)) } else { BATCH_KEYS };
    while keys < most && bytes < BATCH_BYTES {
        // A collection in parts goes on first.
        if let Some((key, from)) = c.big.take() {
            match store.dump_part(&key, from, chunk, BATCH_BYTES) {
                Some(part) => {
                    let at = store.expire_at(&key).flatten();
                    let last = from + part.count >= part.total;
                    frame(&mut batch, &key, at, part.kind.name(), from == 0, last, &part.payload);
                    bytes += part.payload.len();
                    keys += 1;
                    if last {
                        c.sent.insert(key);
                    } else {
                        c.big = Some((key, from + part.count));
                    }
                }
                // Gone (expired, deleted): what was sent of it goes.
                None => {
                    delete(&mut batch, &key);
                    c.sent.insert(key);
                }
            }
            continue;
        }
        let (key, again) = match c.again.pop_front() {
            Some(key) => (key, true),
            None if c.next < c.keys.len() => {
                c.next += 1;
                (std::mem::take(&mut c.keys[c.next - 1]), false)
            }
            None => {
                // The slot is done: the next one with keys.
                if c.loaded {
                    c.sent.clear();
                    c.keys.clear();
                    c.next = 0;
                    c.slot += 1;
                }
                while (c.slot as usize) < SLOTS && (!feed.wants(c.slot as u16) || store.count_in_slot(c.slot as u16) == 0) {
                    c.slot += 1;
                }
                if c.slot as usize >= SLOTS {
                    let offset = node.repl.offset();
                    drop(copy);
                    let mut out = feed.out.borrow_mut();
                    out.extend_from_slice(batch.as_bytes());
                    let mut end = Encoder::new(Protocol::Resp2);
                    if feed.is_replica() {
                        end.array(2);
                        end.bulk(b"REPLSYNCED");
                        end.bulk(offset.to_string().as_bytes());
                    } else {
                        end.array(1);
                        end.bulk(b"SLOTSSYNCED");
                    }
                    out.extend_from_slice(end.as_bytes());
                    *feed.copy.borrow_mut() = None;
                    return true;
                }
                c.keys = store.keys_in_slot(c.slot as u16).map(<[u8]>::to_vec).collect();
                c.loaded = true;
                continue;
            }
        };
        if c.sent.contains(&key) {
            continue;
        }
        match store.elements(&key) {
            // Gone: never sent, or sent before a write touched it.
            None => {
                if again {
                    delete(&mut batch, &key);
                }
                c.sent.insert(key);
            }
            Some(n) if n > chunk => c.big = Some((key, 0)),
            Some(_) => {
                let (value, _) = store.dump(&key).expect("live, as elements says");
                let at = store.expire_at(&key).flatten();
                frame(&mut batch, &key, at, value.kind().name(), true, true, value.payload());
                bytes += value.payload().len();
                keys += 1;
                c.sent.insert(key);
            }
        }
    }
    feed.out.borrow_mut().extend_from_slice(batch.as_bytes());
    false
}

/// Reads `REPLACK offset|- ping` from the replica until the connection
/// ends (or it sends anything else), then hangs the feed up.
fn acks(conn: TcpStream, feed: Rc<Feed>) {
    let mut buf = vec![0; 4096];
    let mut end = 0;
    let mut parser = Parser::new(Limits::default());
    'read: loop {
        let mut at = 0;
        while let Ok(Parsed::Complete { consumed }) = parser.parse(&buf[at..end]) {
            let args = parser.args(&buf[at..end]);
            let offset = match args.get(1) {
                Some(a) if a[..] == b"-"[..] => Some(None),
                Some(o) => store::parse_i64(o).filter(|&o| o >= 0).map(|o| Some(o as u64)),
                None => None,
            };
            let ping = args.get(2).and_then(store::parse_i64).filter(|&p| p >= 0);
            match (args.len(), offset, ping) {
                (3, Some(offset), Some(ping)) if args[0].eq_ignore_ascii_case(b"REPLACK") => feed.ack(offset, ping as u64),
                _ => {
                    warning!("Replica {} sent something else than REPLACK: hanging up", feed.id);
                    break 'read;
                }
            }
            at += consumed;
        }
        buf.copy_within(at..end, 0);
        end -= at;
        if end == buf.len() {
            break;
        }
        match (&conn).read(&mut buf[end..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => end += n,
        }
    }
    feed.hang_up();
}

/// `REPLKEY key at kind first last payload`.
fn frame(out: &mut Encoder, key: &[u8], at: Option<i64>, kind: &[u8], first: bool, last: bool, payload: &[u8]) {
    let at = at.map_or("-".to_string(), |a| a.to_string());
    out.array(7);
    out.bulk(b"REPLKEY");
    out.bulk(key);
    out.bulk(at.as_bytes());
    out.bulk(kind);
    out.bulk(if first { b"1" } else { b"0" });
    out.bulk(if last { b"1" } else { b"0" });
    out.bulk(payload);
}

fn delete(out: &mut Encoder, key: &[u8]) {
    out.array(2);
    out.bulk(b"DEL");
    out.bulk(key);
}
