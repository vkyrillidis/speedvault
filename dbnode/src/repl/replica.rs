//! A replica's side of replication: while the table makes this node a
//! replica, a fiber keeps a link to its primary, syncs (from scratch, or
//! carrying on from the backlog after a short drop) and applies the
//! stream. See the [module docs](super).
//!
//! The store doesn't reclaim expired keys meanwhile (the primary's `DEL`s
//! do), and every command of the stream runs with the time before every
//! expiry (`store::BEFORE_TIME`): a key the primary has is there for its
//! writes. During a sync from scratch the store starts empty and reads get
//! `-LOADING` (agreed 2026-10-23): a half copy is never served.

use crate::command;
use crate::{notice, verbose, warning, Node};
use cluster::NodeId;
use reactor::{sleep, TcpStream};
use resp::{Encoder, Limits, Parsed, Parser, Protocol};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};
use store::{Kind, Staged};

/// For connecting, resolving the primary's name, and each read: the table
/// is looked at again between reads.
const TIMEOUT: Duration = Duration::from_secs(1);
/// Redis's `repl-timeout`: nothing from the primary this long (it pings
/// every 10 s), the link is down.
const SILENCE: Duration = Duration::from_secs(60);
/// The wait before connecting again.
const RETRY: Duration = Duration::from_millis(500);
const READ_SIZE: usize = 64 * 1024;
/// An acknowledgement at least this often, whatever came (Redis's
/// replicas send `REPLCONF ACK` every second): the primary's lease.
const ACK_EVERY: Duration = Duration::from_secs(1);
/// What came is acknowledged at most this often, the reads in between
/// bundled into one acknowledgement (for `WAIT`, which waits up to this
/// longer): under load a thousand a second at most, not one a read.
const ACK_GAP: Duration = Duration::from_millis(1);

/// Collections arriving in parts during a sync, with their expiry.
pub(super) type Staging = HashMap<Vec<u8>, (Staged, Option<i64>)>;

/// Where the link stands, as `ROLE` says it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum State {
    /// Not a replica.
    #[default]
    Off,
    /// Connecting or connected, the sync not started.
    Connecting,
    /// Syncing from scratch.
    Copying,
    /// Applying the stream.
    Online,
    /// Lost, or never up: connecting again.
    Down,
}

/// The link to the primary, for `ROLE`, `INFO replication` and reads.
#[derive(Default)]
pub(crate) struct Link {
    state: Cell<State>,
    /// The primary's stream id and how much of it was applied, once
    /// synced.
    replid: Cell<Option<NodeId>>,
    offset: Cell<u64>,
    /// The stream a sync from scratch copies (its `+FULLRESYNC`), the
    /// link's once it ends.
    syncing: Cell<Option<NodeId>>,
    last_io: Cell<Option<Instant>>,
    down_since: Cell<Option<Instant>>,
    /// Until when it acknowledges nothing (`CLUSTER FENCE`, from stuart's
    /// failover): its primary's lease runs out.
    fenced: Cell<Option<Instant>>,
    /// The last `REPLPING` from the primary: acknowledged with the offset.
    ping: Cell<u64>,
}

impl Link {
    /// `CLUSTER FENCE`: no acknowledgement for `d` (zero: from now on
    /// again). The stream is still applied meanwhile.
    pub(crate) fn fence(&self, d: Duration) {
        self.fenced.set((!d.is_zero()).then(|| Instant::now() + d));
    }

    fn is_fenced(&self) -> bool {
        self.fenced.get().is_some_and(|until| Instant::now() < until)
    }

    pub(crate) fn state(&self) -> State {
        self.state.get()
    }

    /// Reads get `-LOADING`.
    pub(crate) fn loading(&self) -> bool {
        self.state.get() == State::Copying
    }

    /// The stream applied (`slave_repl_offset`): 0 before a sync ends.
    pub(crate) fn offset(&self) -> u64 {
        self.offset.get()
    }

    /// Whether a sync ever ended (`ROLE`'s offset is -1 before).
    pub(crate) fn synced(&self) -> bool {
        self.replid.get().is_some()
    }

    pub(crate) fn replid(&self) -> Option<NodeId> {
        self.replid.get()
    }

    /// Seconds since the primary last sent something; -1 without a link.
    pub(crate) fn last_io_secs(&self) -> i64 {
        match (self.state.get(), self.last_io.get()) {
            (State::Copying | State::Online, Some(t)) => t.elapsed().as_secs() as i64,
            _ => -1,
        }
    }

    /// Seconds the link has been down; -1 if it never was up.
    pub(crate) fn down_secs(&self) -> i64 {
        self.down_since.get().map_or(-1, |t| t.elapsed().as_secs() as i64)
    }

    fn set(&self, state: State) {
        if state == State::Down && self.state.get() == State::Online {
            self.down_since.set(Some(Instant::now()));
        }
        if state == State::Online {
            self.down_since.set(None);
        }
        self.state.set(state);
    }
}

/// The fiber: see the module docs. Runs as long as the node, in cluster
/// mode.
pub(crate) fn replica_loop(node: Rc<Node>) {
    let cluster = node.cluster.as_ref().expect("replica_loop runs in cluster mode");
    let link = &node.repl.link;
    let mut failing = false;
    loop {
        let Some(primary) = cluster.primary() else {
            if link.state() != State::Off {
                node.store.borrow_mut().set_reclaim(true);
                link.set(State::Off);
                link.fenced.set(None);
            }
            sleep(Duration::from_millis(100));
            continue;
        };
        node.store.borrow_mut().set_reclaim(false);
        if link.state() == State::Off {
            link.set(State::Connecting);
        }
        match follow(&node, &primary) {
            Ok(()) => failing = false,
            Err(e) => {
                if !failing {
                    warning!("Replication from {}:{} failed: {e}; trying again", primary.host(), primary.port());
                }
                failing = true;
                link.set(State::Down);
                sleep(RETRY);
            }
        }
    }
}

/// One link to `primary`: syncs, then applies the stream until the link
/// fails (`Err`) or the table changes the node's primary (`Ok`).
fn follow(node: &Node, primary: &cluster::Node) -> io::Result<()> {
    let cluster = node.cluster.as_ref().unwrap();
    let link = &node.repl.link;
    let addr = crate::resolve(primary.host(), primary.port(), TIMEOUT)?;
    let conn = TcpStream::connect_timeout(addr, TIMEOUT)?;
    let _ = conn.set_nodelay(true);
    conn.set_read_timeout(Some(TIMEOUT))?;
    let (replid, offset) = match link.replid() {
        Some(id) => (id.to_string(), link.offset().to_string()),
        None => ("?".to_string(), "-1".to_string()),
    };
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(4);
    for arg in [&b"REPLSYNC"[..], cluster.myself().as_bytes(), replid.as_bytes(), offset.as_bytes()] {
        req.bulk(arg);
    }
    (&conn).write_all(req.as_bytes())?;
    // From here on, silence for too long ends the link.
    link.last_io.set(Some(Instant::now()));

    let mut buf = vec![0; READ_SIZE];
    let mut end = 0;
    let mut start;
    // The reply line first.
    let line = loop {
        if let Some(i) = buf[..end].windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&buf[..i]).into_owned();
            start = i + 2;
            break line;
        }
        if end == buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no reply line"));
        }
        end += read(&conn, &mut buf[end..], node)?;
    };
    let words: Vec<&str> = line.split(' ').collect();
    let id = words.get(1).and_then(|w| NodeId::parse(w.as_bytes()));
    match (words[0], id, words.get(2).and_then(|o| o.parse::<u64>().ok())) {
        ("+FULLRESYNC", Some(id), Some(_)) => {
            notice!("Syncing from scratch from {}:{} (its stream {id})", primary.host(), primary.port());
            node.store.borrow_mut().flush();
            link.replid.set(None);
            link.syncing.set(Some(id));
            link.offset.set(0);
            link.set(State::Copying);
        }
        ("+CONTINUE", Some(id), Some(o)) if Some(id) == link.replid() && o == link.offset() => {
            notice!("Carrying on from {}:{} at offset {o}", primary.host(), primary.port());
            link.set(State::Online);
        }
        _ => return Err(io::Error::other(format!("REPLSYNC: {line}"))),
    }
    link.last_io.set(Some(Instant::now()));
    ack(&conn, link)?;
    // When the last acknowledgement went, and the offset and ping it said.
    let (mut acked, mut acked_offset) = (Instant::now(), (link.offset(), link.ping.get()));

    let mut parser = Parser::new(Limits::default());
    let mut staging = Staging::new();
    let mut out = Encoder::new(Protocol::Resp2);
    loop {
        // Before anything read is applied: once the table made this node a
        // primary (a failover, a handover), what its old primary sends
        // since (the slots it gave up trimmed, a write without keys that
        // waited) isn't for it.
        if cluster.primary().is_none_or(|p| p.id() != primary.id()) {
            verbose!("The table changed this node's primary: leaving {}:{}", primary.host(), primary.port());
            return Ok(());
        }
        let before = link.state();
        loop {
            match parser.parse(&buf[start..end]) {
                Ok(Parsed::Complete { consumed }) => {
                    let args = parser.args(&buf[start..end]);
                    if !args.is_empty() {
                        apply(node, &args, consumed, &mut staging, &mut out)?;
                    }
                    start += consumed;
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }
        }
        // The copy's end and a new ping at once (the lease, and a copy's
        // next batch, wait for it); what came, once applied (for WAIT), at
        // most every ACK_GAP, so a read waits no longer than that while
        // some is unacknowledged; and anyway every second.
        let since = acked.elapsed();
        let now = (link.offset(), link.ping.get());
        let pinged = now.1 != acked_offset.1;
        if link.state() != before || pinged || (now != acked_offset && since >= ACK_GAP) || since >= ACK_EVERY {
            ack(&conn, link)?;
            (acked, acked_offset) = (Instant::now(), now);
        }
        let wait = if (link.offset(), link.ping.get()) == acked_offset { TIMEOUT } else { ACK_GAP.saturating_sub(acked.elapsed()).max(Duration::from_micros(10)) };
        conn.set_read_timeout(Some(wait))?;
        if start == end {
            (start, end) = (0, 0);
        } else if buf.len() - end < READ_SIZE {
            buf.copy_within(start..end, 0);
            (start, end) = (0, end - start);
            if buf.len() - end < READ_SIZE {
                buf.resize(buf.len().max(end + READ_SIZE) * 2, 0);
            }
        }
        end += read(&conn, &mut buf[end..], node)?;
    }
}

/// `REPLACK offset ping` (ours, Redis's `REPLCONF ACK`): the stream
/// applied (`-` during a copy), and the last `REPLPING` received, which
/// renews the primary's lease from when it sent it.
fn ack(conn: &TcpStream, link: &Link) -> io::Result<()> {
    if link.is_fenced() {
        return Ok(());
    }
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(3);
    req.bulk(b"REPLACK");
    if link.state() == State::Online {
        req.bulk(link.offset().to_string().as_bytes());
    } else {
        req.bulk(b"-");
    }
    req.bulk(link.ping.get().to_string().as_bytes());
    (&*conn).write_all(req.as_bytes())
}

/// Reads what the primary sent; 0 bytes after [`TIMEOUT`] without any,
/// so that the caller looks at the table again; nothing for [`SILENCE`]
/// is an error.
fn read(conn: &TcpStream, buf: &mut [u8], node: &Node) -> io::Result<usize> {
    let link = &node.repl.link;
    match (&*conn).read(buf) {
        Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
        Ok(n) => {
            link.last_io.set(Some(Instant::now()));
            Ok(n)
        }
        Err(e) if e.kind() == io::ErrorKind::TimedOut => {
            if link.last_io.get().is_some_and(|t| t.elapsed() > SILENCE) {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "nothing from the primary for 60 s"));
            }
            Ok(0)
        }
        Err(e) => Err(e),
    }
}

/// One command from the primary, `len` bytes of the stream. The offset
/// counts what comes once the copy is done: the copy's REPLSYNCED gives
/// the offset it ends at.
pub(super) fn apply(node: &Node, args: &resp::Args, len: usize, staging: &mut Staging, out: &mut Encoder) -> io::Result<()> {
    let link = &node.repl.link;
    // Outside the stream: no offset.
    if args[0].eq_ignore_ascii_case(b"REPLPING") {
        let seq = args.get(1).and_then(store::parse_i64).filter(|&s| s > 0 && args.len() == 2);
        let seq = seq.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "a bad REPLPING from the primary"))?;
        link.ping.set(seq as u64);
        return Ok(());
    }
    let online = link.state() == State::Online;
    execute(node, args, staging, out)?;
    if online {
        link.offset.set(link.offset() + len as u64);
    }
    Ok(())
}

/// `REPLKEY key at kind first last payload`: a key copied, or a part of
/// one (a collection in parts is staged until its last). Whether that was
/// the last part.
pub(super) fn replkey(node: &Node, args: &resp::Args, staging: &mut Staging) -> io::Result<bool> {
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{what} from the primary"));
    if args.len() != 7 {
        return Err(bad("a bad REPLKEY"));
    }
    let key = &args[1];
    let at = match &args[2][..] {
        b"-" => None,
        at => Some(store::parse_i64(at).ok_or_else(|| bad("a bad expiry"))?),
    };
    let kind = Kind::from_name(&args[3]).ok_or_else(|| bad("a bad type"))?;
    if kind != Kind::Str && !store::check_dump(kind, &args[6]) {
        return Err(bad("a bad value"));
    }
    if &args[4] == b"1" {
        staging.insert(key.to_vec(), (Staged::new(kind), at));
    }
    let Some((staged, _)) = staging.get_mut(key) else { return Err(bad("a part without its first")) };
    staged.append(&args[6]);
    if &args[5] == b"1" {
        let (staged, at) = staging.remove(key).unwrap();
        node.store.borrow_mut().restore_staged_at(key, staged, at);
        return Ok(true);
    }
    Ok(false)
}

fn execute(node: &Node, args: &resp::Args, staging: &mut Staging, out: &mut Encoder) -> io::Result<()> {
    let link = &node.repl.link;
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{what} from the primary"));
    if args[0].eq_ignore_ascii_case(b"REPLKEY") {
        replkey(node, args, staging)?;
        return Ok(());
    }
    // A slot the primary copied from another node and gave up: its keys go.
    if args[0].eq_ignore_ascii_case(b"REPLDROPSLOT") {
        let slot = args.get(1).and_then(store::parse_i64).filter(|s| (0..cluster::SLOTS as i64).contains(s) && args.len() == 2);
        let slot = slot.ok_or_else(|| bad("a bad REPLDROPSLOT"))? as u16;
        staging.retain(|key, _| cluster::key_hash_slot(key) != slot);
        node.store.borrow_mut().drop_slot(slot);
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"REPLSYNCED") {
        let offset = args.get(1).and_then(store::parse_i64).filter(|&o| o >= 0).ok_or_else(|| bad("a bad REPLSYNCED"))?;
        link.offset.set(offset as u64);
        link.replid.set(link.syncing.take());
        // A key deleted while it went in parts: its parts, never finished.
        staging.clear();
        link.set(State::Online);
        notice!("Synced: {} keys, at offset {offset}", node.store.borrow().dbsize());
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"FLUSHALL") || args[0].eq_ignore_ascii_case(b"FLUSHDB") {
        staging.clear();
    }
    out.clear();
    command::apply(args, node, out);
    if out.as_bytes().first() == Some(&b'-') {
        verbose!("A command from the primary failed: {}", String::from_utf8_lossy(out.as_bytes()).trim_end());
    }
    Ok(())
}

