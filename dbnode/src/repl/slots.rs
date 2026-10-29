//! A node's side of slots moving to it by snapshot (phase 8; see the
//! [module docs](super)): while the table moves slots from another node to
//! this one, a link per source pulls them (`SLOTSYNC`), their keys and
//! then the writes to them, and applies them here, though this node serves
//! none of them yet (the table gives them to the source: `-MOVED` there),
//! and into its own stream, for its replicas. Their expired keys aren't
//! reclaimed meanwhile (the source's `DEL`s come), as at a replica.
//!
//! When the table gives this node the slots (the cutover), they are
//! *landing* until the source's `SLOTSDONE` (see the [module docs](super)):
//! requests to them wait, so they are served with every write the source
//! took; then the link ends. A table that no longer moves them here (the
//! move called off) ends the link and drops them, here and at the replicas
//! (`REPLDROPSLOT`). A link that fails starts again from scratch, dropping
//! what it copied once the source takes the new one (`+SYNCING`): until
//! then it is kept, in case the cutover comes meanwhile (stuart saw this
//! node reach the paused source's offset), and this node then serves what
//! it has.
//!
//! A frame that would take this node past `maxmemory` waits, unread, until
//! there is room (the link is `full`): the source's copy waits for it too
//! (its flow control), and stuart doesn't cut such a link over, as phase
//! 5's moves to a full target wait. Slots landing never wait: they are this
//! node's.

use super::replica::{replkey, Staging};
use crate::command;
use crate::{notice, verbose, warning, Node};
use cluster::{NodeId, SlotTable, SLOTS};
use reactor::{sleep, yield_now, TcpStream};
use resp::{Args, Encoder, Limits, Parsed, Parser, Protocol};
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// How often the table is looked at for moves starting or ending.
const TICK: Duration = Duration::from_millis(20);
/// For connecting, and each read: the link checks whether it should end
/// between reads.
const TIMEOUT: Duration = Duration::from_secs(1);
/// Nothing from the source this long (it pings every second): the link
/// failed.
const SILENCE: Duration = Duration::from_secs(10);
const RETRY: Duration = Duration::from_millis(500);
/// A source whose table doesn't move the slots here yet (it takes the
/// tables after this node, see [`NotYet`]): asked again this often, for
/// [`NOT_YET_FOR`] before a refusal counts as a failure.
const NOT_YET: Duration = Duration::from_millis(5);
const NOT_YET_FOR: Duration = Duration::from_secs(1);
const READ_SIZE: usize = 64 * 1024;
/// An acknowledgement at least this often (the source's pings are
/// answered at once).
const ACK_EVERY: Duration = Duration::from_secs(1);
/// How often a full link looks for room.
const FULL_TICK: Duration = Duration::from_millis(100);
/// How often [`trim_loop`] looks for slots given up, and how many of their
/// keys it deletes at a time.
const TRIM_TICK: Duration = Duration::from_millis(100);
const TRIM_BATCH: usize = 1000;

/// Where a link stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    Connecting,
    /// The keys being copied.
    Copying,
    /// Copied: the writes as they come.
    Streaming,
    /// The table gives this node the slots; the last writes on the way.
    Landing,
    /// Waiting for room under `maxmemory`.
    Full,
}

impl State {
    pub(crate) fn name(self) -> &'static str {
        match self {
            State::Connecting => "connecting",
            State::Copying => "copying",
            State::Streaming => "streaming",
            State::Landing => "landing",
            State::Full => "full",
        }
    }
}

/// A link pulling slots from a source.
pub(crate) struct Sync {
    pub(crate) source: NodeId,
    /// In order.
    pub(crate) slots: Vec<u16>,
    state: Cell<State>,
    /// The last `REPLPING` from the source.
    ping: Cell<u64>,
    /// Keys copied (whole), since the link last started.
    keys: Cell<u64>,
    /// The bytes of writes applied since the copy (the source's offset
    /// for this link, which it pauses at): acknowledged.
    offset: Cell<u64>,
    /// The table no longer moves these slots here: the link ends.
    stop: Cell<bool>,
    /// The table gives this node the slots: they wait for `SLOTSDONE`.
    landing: Cell<bool>,
    /// `SLOTSDONE` came: the source wrote to them for the last time.
    done: Cell<bool>,
    /// The link ended (`SLOTSDONE`, or it failed while landing).
    ended: Cell<bool>,
    /// The slots' keys were flushed here: the link starts again.
    restart: Cell<bool>,
    /// Waiting for room ([`State::Full`]).
    full: Cell<bool>,
}

impl Sync {
    fn new(source: NodeId, slots: Vec<u16>, state: State) -> Sync {
        Sync {
            source,
            slots,
            state: Cell::new(state),
            ping: Cell::new(0),
            keys: Cell::new(0),
            offset: Cell::new(0),
            stop: Cell::new(false),
            landing: Cell::new(false),
            done: Cell::new(false),
            ended: Cell::new(false),
            restart: Cell::new(false),
            full: Cell::new(false),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(source: NodeId, slots: Vec<u16>) -> Sync {
        Sync::new(source, slots, State::Copying)
    }

    pub(crate) fn state(&self) -> State {
        if self.landing.get() {
            State::Landing
        } else if self.full.get() {
            State::Full
        } else {
            self.state.get()
        }
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset.get()
    }

    pub(crate) fn keys(&self) -> u64 {
        self.keys.get()
    }
}

/// The fiber starting and ending the links as the table says. Runs as long
/// as the node, in cluster mode.
/// Woken by each new table ([`wake`]).
pub(crate) fn sync_loop(node: Rc<Node>) {
    loop {
        node.repl.slots_wake.wait_until(Some(Instant::now() + TICK));
        reconcile(&node);
    }
}

/// A new table: the links are looked at now (a cutover's requests wait).
pub(crate) fn wake(node: &Node) {
    node.repl.slots_wake.notify();
}

/// `FLUSHALL` here: the slots being copied lost their keys, so their links
/// start again (those landing excepted: the slots are this node's).
pub(crate) fn flushed(node: &Node) {
    for s in node.repl.slot_syncs.borrow().iter().filter(|s| !s.landing.get()) {
        s.restart.set(true);
    }
}

/// The slots each source moves to this node by snapshot, by source.
fn wanted(table: &SlotTable, me: usize) -> Vec<(NodeId, Vec<u16>)> {
    let mut by_source: Vec<(NodeId, Vec<u16>)> = Vec::new();
    for m in table.migrations().iter().filter(|m| m.snapshot && m.to == me) {
        let source = *table.nodes()[m.from].id();
        match by_source.iter_mut().find(|(s, _)| *s == source) {
            Some((_, slots)) => slots.push(m.slot),
            None => by_source.push((source, vec![m.slot])),
        }
    }
    by_source
}

/// Ends the links the table no longer wants (keeping the slots it now gives
/// this node once their last writes came, dropping the others), and starts
/// one for the slots it moves here that no link pulls yet.
fn reconcile(node: &Rc<Node>) {
    let Some(cluster) = &node.cluster else { return };
    let (ended, taken, started) = {
        let (table, me) = cluster.table();
        let still = |source: &NodeId, slot: u16| {
            table.migration(slot).is_some_and(|m| m.snapshot && m.to == me && table.nodes()[m.from].id() == source)
        };
        let given = |s: &Sync| s.slots.iter().all(|&slot| table.owner_index(slot) == Some(me) && table.migration(slot).is_none());
        let mut syncs = node.repl.slot_syncs.borrow_mut();
        let (mut ended, mut taken) = (Vec::new(), Vec::new());
        let mut landing = cluster.landing.borrow_mut();
        syncs.retain(|s| {
            if given(s) {
                if !s.landing.replace(true) {
                    landing.extend_from_slice(&s.slots);
                }
                if s.ended.get() {
                    landing.retain(|slot| !s.slots.contains(slot));
                    taken.push(s.clone());
                    return false;
                }
                return true;
            }
            if s.slots.iter().all(|&slot| still(&s.source, slot)) {
                return true;
            }
            s.stop.set(true);
            landing.retain(|slot| !s.slots.contains(slot));
            let given_up: Vec<u16> = s.slots.iter().copied().filter(|&slot| table.owner_index(slot) != Some(me)).collect();
            ended.push((s.clone(), given_up));
            false
        });
        drop(landing);
        let mut started = Vec::new();
        for (source, slots) in wanted(&table, me) {
            let uncovered: Vec<u16> =
                slots.into_iter().filter(|slot| !syncs.iter().any(|s| s.source == source && s.slots.contains(slot))).collect();
            if !uncovered.is_empty() {
                let sync = Rc::new(Sync::new(source, uncovered, State::Connecting));
                syncs.push(sync.clone());
                started.push(sync);
            }
        }
        // Held (hidden, expired keys kept): the slots copied here; at a
        // replica, those its primary copies.
        let held: Vec<u16> = match table.primary_of(me) {
            Some(primary) => table.migrations().iter().filter(|m| m.snapshot && m.to == primary).map(|m| m.slot).collect(),
            None => syncs.iter().flat_map(|s| s.slots.iter().copied()).collect(),
        };
        node.store.borrow_mut().set_held(held);
        (ended, taken, started)
    };
    for sync in taken {
        // As `migrate_keys_imported`: the keys the slots came with.
        let keys: u64 = sync.slots.iter().map(|&s| node.store.borrow().count_in_slot(s) as u64).sum();
        cluster.stats.keys_imported.set(cluster.stats.keys_imported.get() + keys);
        if sync.done.get() {
            notice!("Took {} slots from {}: they are this node's", sync.slots.len(), sync.source);
        } else {
            warning!("Took {} slots from {} without their last writes (the link failed): serving what was copied", sync.slots.len(), sync.source);
        }
    }
    for (sync, given_up) in ended {
        if !given_up.is_empty() {
            notice!("No longer copying {} slots from {}: dropped", given_up.len(), sync.source);
            drop_slots(node, &given_up);
        }
    }
    for sync in started {
        verbose!("Copying {} slots from {}", sync.slots.len(), sync.source);
        let n = node.clone();
        if let Err(e) = reactor::spawn(move || pull(n, sync)) {
            warning!("No fiber to copy slots: {e}");
        }
    }
}

/// The fiber deleting the keys of the slots a primary has given up (moved
/// to another node by snapshot, its keys left here at the cutover; or any
/// left otherwise), [`TRIM_BATCH`] at a time between clients, each as a
/// `DEL` into the stream for the replicas: as Redis 8 trims a slot it
/// handed over in the background. A slot is given up when the table gives
/// it to another node, and no move of it from here or to here; an
/// unassigned slot's keys stay (a table may give it back: `--auto` hands
/// lost slots out). Nor does a node the table marks failed trim, nor
/// trim the slots one of its replicas took over (a failover, a handover)
/// while a replica still follows its stream (a `DEL` there would delete
/// keys the new primary has). Runs as long as the node, in cluster mode.
pub(crate) fn trim_loop(node: Rc<Node>) {
    loop {
        sleep(TRIM_TICK);
        if let Some(cluster) = &node.cluster {
            if !node.repl.feeds.borrow().iter().any(|f| f.is_replica() && f.live()) {
                cluster.successors.borrow_mut().clear();
            }
        }
        let given_up = |slot: u16| {
            let Some(cluster) = &node.cluster else { return false };
            let (table, me) = cluster.table();
            table.primary_of(me).is_none()
                && !table.is_failed(me)
                && table.owner_index(slot).is_some_and(|owner| owner != me && !cluster.successors.borrow().contains(table.nodes()[owner].id()))
                && table.migration(slot).is_none_or(|m| m.from != me && m.to != me)
                && node.store.borrow().count_in_slot(slot) > 0
                && !node.store.borrow().is_held(slot)
        };
        for slot in 0..SLOTS as u16 {
            if !given_up(slot) {
                continue;
            }
            let mut deleted = 0;
            while given_up(slot) {
                let watched = crate::multi::present_watched(&node, Some(slot));
                let keys = node.store.borrow_mut().drop_slot_part(slot, TRIM_BATCH);
                crate::multi::deleted(&node, watched);
                for key in &keys {
                    node.repl.relay_del(key);
                }
                deleted += keys.len();
                yield_now();
            }
            verbose!("Slot {slot} given up: {deleted} keys deleted");
        }
    }
}

/// `slots`' keys go, here and at the replicas.
fn drop_slots(node: &Node, slots: &[u16]) {
    for &slot in slots {
        let watched = crate::multi::present_watched(node, Some(slot));
        node.store.borrow_mut().drop_slot(slot);
        crate::multi::deleted(node, watched);
        node.repl.relay_drop(slot);
    }
}

/// The link of `sync`, again after each failure, until the table no longer
/// wants it, or `SLOTSDONE`. A failure while landing ends it (the slots are
/// this node's).
fn pull(node: Rc<Node>, sync: Rc<Sync>) {
    let mut failing = false;
    let started = Instant::now();
    while !sync.stop.get() {
        match link(&node, &sync) {
            Ok(()) => break,
            Err(e) => {
                if sync.stop.get() {
                    return;
                }
                if e.get_ref().is_some_and(|e| e.is::<NotYet>()) && started.elapsed() < NOT_YET_FOR {
                    sleep(NOT_YET);
                    continue;
                }
                if sync.landing.get() {
                    warning!("Copying {} slots from {} failed while they land: {e}", sync.slots.len(), sync.source);
                    break;
                }
                if !failing {
                    warning!("Copying {} slots from {} failed: {e}; again from scratch", sync.slots.len(), sync.source);
                }
                failing = true;
                sync.state.set(State::Connecting);
                sleep(RETRY);
                if sync.landing.get() {
                    break;
                }
            }
        }
    }
    sync.ended.set(true);
    reconcile(&node);
}

/// One link: `SLOTSYNC`, then what the source sends, applied, until the
/// link fails (`Err`) or the table no longer wants it (`Ok`).
fn link(node: &Node, sync: &Sync) -> io::Result<()> {
    let cluster = node.cluster.as_ref().unwrap();
    let source = {
        let (table, _) = cluster.table();
        let i = table.position(&sync.source).ok_or_else(|| io::Error::other("the source left the table"))?;
        table.nodes()[i].clone()
    };
    let addr = crate::resolve(source.host(), source.port(), TIMEOUT)?;
    let conn = TcpStream::connect_timeout(addr, TIMEOUT)?;
    let _ = conn.set_nodelay(true);
    conn.set_read_timeout(Some(TIMEOUT))?;
    let mut req = Encoder::new(Protocol::Resp2);
    // Logged in first, in the same write: the cluster's one password.
    let login = node.password.borrow().clone();
    if let Some(password) = &login {
        crate::auth::login(&mut req, password);
    }
    req.array(2 + sync.slots.len());
    req.bulk(b"SLOTSYNC");
    req.bulk(cluster.myself().as_bytes());
    for slot in &sync.slots {
        req.bulk(slot.to_string().as_bytes());
    }
    (&conn).write_all(req.as_bytes())?;
    let mut last_io = Instant::now();

    let mut buf = vec![0; READ_SIZE];
    let mut end = 0;
    let mut start;
    let mut auth_reply = login.is_some();
    let line = loop {
        if let Some(i) = buf[..end].windows(2).position(|w| w == b"\r\n") {
            start = i + 2;
            let line = String::from_utf8_lossy(&buf[..i]).into_owned();
            if std::mem::take(&mut auth_reply) {
                if line != "+OK" {
                    return Err(io::Error::other(format!("AUTH: {line}")));
                }
                buf.copy_within(start..end, 0);
                end -= start;
                continue;
            }
            break line;
        }
        if end == buf.len() || sync.stop.get() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no reply line"));
        }
        end += read(&conn, &mut buf[end..], &mut last_io)?;
    };
    if line.starts_with("-ERR The table doesn't move slot ") {
        return Err(io::Error::other(NotYet(line)));
    }
    if !line.starts_with("+SYNCING ") {
        return Err(io::Error::other(format!("SLOTSYNC: {line}")));
    }
    // From scratch: what an earlier link copied, or a move before left.
    let leftover: Vec<u16> = sync.slots.iter().copied().filter(|&s| node.store.borrow().count_in_slot(s) > 0).collect();
    drop_slots(node, &leftover);
    sync.restart.set(false);
    sync.state.set(State::Copying);
    sync.keys.set(0);
    sync.offset.set(0);
    ack(&conn, sync)?;
    let (mut acked, mut acked_ping) = (Instant::now(), sync.ping.get());

    let mut parser = Parser::new(Limits::default());
    let mut staging = Staging::new();
    let mut out = Encoder::new(Protocol::Resp2);
    loop {
        if sync.stop.get() || sync.done.get() {
            return Ok(());
        }
        if sync.restart.get() {
            return Err(io::Error::other("this node was flushed"));
        }
        let before = sync.state();
        loop {
            match parser.parse(&buf[start..end]) {
                Ok(Parsed::Complete { consumed }) => {
                    wait_for_room(node, &conn, sync, consumed)?;
                    if sync.stop.get() || sync.restart.get() {
                        break;
                    }
                    let args = parser.args(&buf[start..end]);
                    // The offset: the writes after the copy, pings left out.
                    let counts = sync.state.get() == State::Streaming && !args.is_empty() && !args[0].eq_ignore_ascii_case(b"REPLPING");
                    if !args.is_empty() {
                        apply(node, sync, &args, &mut staging, &mut out)?;
                    }
                    start += consumed;
                    if counts && !sync.done.get() {
                        sync.offset.set(sync.offset.get() + consumed as u64);
                    }
                    if sync.stop.get() || sync.done.get() {
                        return Ok(());
                    }
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }
        }
        // A new ping at once (the source's copy waits for it), the copy's
        // end too, and anyway every second; while landing, what came.
        if sync.state() != before || sync.ping.get() != acked_ping || acked.elapsed() >= ACK_EVERY || sync.landing.get() {
            ack(&conn, sync)?;
            (acked, acked_ping) = (Instant::now(), sync.ping.get());
        }
        if start == end {
            (start, end) = (0, 0);
        } else if buf.len() - end < READ_SIZE {
            buf.copy_within(start..end, 0);
            (start, end) = (0, end - start);
            if buf.len() - end < READ_SIZE {
                buf.resize(buf.len().max(end + READ_SIZE) * 2, 0);
            }
        }
        end += read(&conn, &mut buf[end..], &mut last_io)?;
    }
}

/// The source refused `SLOTSYNC` as its table doesn't move the slots here:
/// it hasn't taken the table this node has yet, which may reach this node
/// first (with a cutover this node is owed, or that goes to the cutover's
/// targets first). Not a failure for [`NOT_YET_FOR`].
#[derive(Debug)]
struct NotYet(String);

impl std::fmt::Display for NotYet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SLOTSYNC: {}", self.0)
    }
}

impl std::error::Error for NotYet {}

/// Whether `bytes` more would take the node past `maxmemory` (with the
/// store's next table doubling, as writes count it).
fn no_room(node: &Node, bytes: usize) -> bool {
    let max = node.maxmemory.get();
    max != 0 && (crate::memory::used() + bytes + node.store.borrow().growth()) as u64 > max
}

/// Holds the next frame (`bytes`) until there's room for it, acknowledging
/// meanwhile; not while the slots land, nor once the link should end.
fn wait_for_room(node: &Node, conn: &TcpStream, sync: &Sync, bytes: usize) -> io::Result<()> {
    if sync.landing.get() || !no_room(node, bytes) {
        return Ok(());
    }
    let cluster = node.cluster.as_ref().unwrap();
    cluster.stats.imports_refused.set(cluster.stats.imports_refused.get() + 1);
    warning!("Copying {} slots from {}: full (maxmemory), waiting for room", sync.slots.len(), sync.source);
    sync.full.set(true);
    let mut acked = Instant::now();
    while no_room(node, bytes) && !sync.landing.get() && !sync.stop.get() && !sync.restart.get() {
        sleep(FULL_TICK);
        if acked.elapsed() >= ACK_EVERY {
            ack(conn, sync)?;
            acked = Instant::now();
        }
    }
    sync.full.set(false);
    notice!("Copying {} slots from {}: room again, going on", sync.slots.len(), sync.source);
    Ok(())
}

/// What the source sent, `0` bytes after [`TIMEOUT`] without any; nothing
/// for [`SILENCE`] is an error.
fn read(conn: &TcpStream, buf: &mut [u8], last_io: &mut Instant) -> io::Result<usize> {
    match (&*conn).read(buf) {
        Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
        Ok(n) => {
            *last_io = Instant::now();
            Ok(n)
        }
        Err(e) if e.kind() == io::ErrorKind::TimedOut => {
            if last_io.elapsed() > SILENCE {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "nothing from the source for 10 s"));
            }
            Ok(0)
        }
        Err(e) => Err(e),
    }
}

/// `REPLACK -|offset ping`: `-` while copying, and the last ping.
fn ack(conn: &TcpStream, sync: &Sync) -> io::Result<()> {
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(3);
    req.bulk(b"REPLACK");
    let offset = sync.offset.get().to_string();
    req.bulk(if sync.state.get() == State::Streaming { offset.as_bytes() } else { b"-" });
    req.bulk(sync.ping.get().to_string().as_bytes());
    (&*conn).write_all(req.as_bytes())
}

/// One frame from the source: a ping, a key (or a part), the copy's end,
/// the slots dropped (`FLUSHALL` there), or a write to them; applied, and
/// into this node's stream.
pub(super) fn apply(node: &Node, sync: &Sync, args: &Args, staging: &mut Staging, out: &mut Encoder) -> io::Result<()> {
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{what} from the source"));
    if args[0].eq_ignore_ascii_case(b"REPLPING") {
        let seq = args.get(1).and_then(store::parse_i64).filter(|&s| s > 0 && args.len() == 2);
        sync.ping.set(seq.ok_or_else(|| bad("a bad REPLPING"))? as u64);
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"REPLKEY") {
        if !sync.slots.contains(&cluster::key_hash_slot(&args[1])) {
            return Err(bad("a key of another slot"));
        }
        if replkey(node, args, staging)? {
            sync.keys.set(sync.keys.get() + 1);
        }
        node.repl.relay_key(args);
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"SLOTSSYNCED") {
        // A key deleted while it went in parts: its parts, never finished.
        staging.clear();
        sync.state.set(State::Streaming);
        verbose!("Copied {} slots from {}: {} keys", sync.slots.len(), sync.source, sync.keys());
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"SLOTSDONE") {
        if sync.state.get() != State::Streaming {
            return Err(bad("SLOTSDONE during the copy"));
        }
        sync.done.set(true);
        return Ok(());
    }
    if args[0].eq_ignore_ascii_case(b"SLOTDROP") {
        staging.clear();
        drop_slots(node, &sync.slots);
        return Ok(());
    }
    out.clear();
    command::apply(args, node, out);
    if out.as_bytes().first() == Some(&b'-') {
        verbose!("A command from {} failed: {}", sync.source, String::from_utf8_lossy(out.as_bytes()).trim_end());
    }
    if node.repl.active() {
        let sent = command::find(command::COMMANDS, &args[0]).filter(|cmd| super::as_sent(cmd, out.as_bytes())).map(|cmd| (args, cmd));
        node.repl.feed(&mut node.store.borrow_mut(), sent);
    }
    Ok(())
}
