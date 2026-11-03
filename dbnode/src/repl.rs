//! Replication (phase 8): the stream every write goes into, and its way to
//! the replicas.
//!
//! # The stream
//!
//! What every write did, as commands in RESP, in order, as Redis's
//! replication stream is. A replica that applies it ends up with the same
//! keys, byte for byte.
//!
//! What goes in is a write's effect, not always the command as sent: the
//! store records an [`Effect`] where they differ (`SPOP` as `SREM`, an
//! expiry relative to now as the absolute time, `INCRBYFLOAT` as the value
//! written; see the `store` docs), and every key it reclaims as expired, as
//! `DEL`: a replica never expires keys by itself. Every other write that
//! succeeded goes in as sent: it does the same wherever it runs. A write
//! that failed (an error reply) changed nothing and stays out; one that
//! changed nothing without failing (`DEL` of a missing key) goes in, as it
//! does nothing at the replica either. With replicas attached, a `PING`
//! goes in every 10 s, as Redis's `repl-ping-replica-period`.
//!
//! The stream has an id (`master_replid`, new at every start) and an
//! offset, the bytes written so far (`master_repl_offset`); the last of
//! them, 1 MiB by default, stay in the backlog, a ring buffer, from which a
//! replica that dropped off briefly carries on.
//!
//! Nothing is recorded until the first replica attaches
//! ([`Replication::start`]): until then writes cost nothing more, as in a
//! Redis without replicas, which has no backlog either.
//!
//! # Syncing a replica
//!
//! A replica connects and sends `REPLSYNC <its id> <replid> <offset>`
//! (ours, Redis's `PSYNC` in spirit). If the id is this stream's and the
//! backlog still holds what follows the offset, the replica carries on
//! (`+CONTINUE replid offset`, then those bytes, then the stream as it
//! goes). Else it syncs from scratch (`+FULLRESYNC replid offset`): this
//! node sends its keys slot by slot, in batches, with no fork and no
//! snapshot held anywhere (`REPLKEY key at kind first last payload`: `at`
//! the absolute expiry or `-`; a collection larger than
//! `--migrate-chunk` elements goes in parts, `first`/`last` marking the
//! ends), and meanwhile the writes the replica must see:
//!
//! ```text
//! slots:  0 ......... s-1 | s (the one being sent) | s+1 ........ 16383
//!         sent: forwarded | forwarded if all its   | not sent yet: left
//!                         | keys were sent, else   | out (their keys go
//!                         | left out and its keys  | later, as they are
//!                         | sent again, as they    | then)
//!                         | are then               |
//! ```
//!
//! A command's keys are all in one slot (cluster mode refuses others), so
//! a write is forwarded whole or not at all. Last comes `REPLSYNCED
//! offset`: from there the replica gets the stream itself, and counts it.
//! The node never waits for a replica: it appends to the replica's output,
//! which a fiber of its own sends, filling it with the next batch when it
//! runs low ([`primary`]); a replica whose output passes
//! [`OUTPUT_LIMIT`] is dropped (Redis's `client-output-buffer-limit
//! replica`), and comes back, carrying on or from scratch.
//!
//! The replica's side is [`replica`].
//!
//! # Syncing slots
//!
//! A slot moving by snapshot (phase 8) is copied the same way to its
//! target, which sends `SLOTSYNC <its id> <slot>...`: a feed like a
//! replica's ([`Feed::slots`]), whose copy takes only those slots and ends
//! with `SLOTSSYNCED`, and which is forwarded only the commands of those
//! slots (no `PING`; a `FLUSHALL` becomes `SLOTDROP`: their keys are gone).
//! Its offset counts the bytes forwarded after `SLOTSSYNCED` (pings left
//! out), which the target acknowledges (`REPLACK offset ping`); stuart
//! pauses the slots' writes here (`CLUSTER PAUSESLOTS`, which answers the
//! offset), waits for the target to reach it, and pushes the table giving
//! the target the slots, the target first. Taking it, this node stops
//! serving them and ends the feed with `SLOTSDONE`
//! ([`Replication::cut_over`]): whatever it took before is in the feed
//! ahead of that, so the target, which serves the slots only once it has
//! it, misses no write even if the pause ran out before the table came,
//! and the two never serve them together. The target's side is [`slots`].

pub(crate) mod primary;
pub(crate) mod replica;
pub(crate) mod slots;

use crate::command::Command;
use cluster::{key_hash_slot, NodeId};
use reactor::Notify;
use resp::Args;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};
use store::{Effect, Store};

/// Redis's default `repl-backlog-size`.
pub(crate) const BACKLOG_SIZE: usize = 1 << 20;
/// Redis's `client-output-buffer-limit replica` hard limit.
pub(crate) const OUTPUT_LIMIT: usize = 256 << 20;
/// Redis's default `repl-ping-replica-period`.
pub(crate) const PING_EVERY: Duration = Duration::from_secs(10);
/// The lease, the plan's `L`: a primary with a replica takes writes only
/// while one has acknowledged within this (Redis's `min-replicas-to-write
/// 1`, `min-replicas-max-lag 3`).
pub(crate) const LEASE: Duration = Duration::from_secs(3);
/// How often a primary sends each replica a `REPLPING seq`, outside the
/// stream: the replica's acknowledgement of one renews the lease from when
/// it was sent.
pub(crate) const PING_FEED: Duration = Duration::from_secs(1);

pub(crate) struct Replication {
    /// `master_replid`.
    pub(crate) replid: NodeId,
    active: Cell<bool>,
    /// `master_repl_offset`: the stream's bytes so far.
    offset: Cell<u64>,
    backlog: RefCell<Backlog>,
    /// A command's bytes before they go to the backlog (kept, reused).
    scratch: RefCell<Vec<u8>>,
    /// The replicas attached.
    pub(crate) feeds: RefCell<Vec<Rc<Feed>>>,
    last_ping: Cell<Option<Instant>>,
    /// This node's own copying, when it is a replica.
    pub(crate) link: replica::Link,
    /// The clients waiting for the replicas' acknowledgements, and when
    /// each replica last acknowledged.
    pub(crate) acks: Rc<Acks>,
    /// The links copying slots moving here by snapshot ([`slots`]).
    pub(crate) slot_syncs: RefCell<Vec<Rc<slots::Sync>>>,
    /// Wakes the fiber that starts and ends them ([`slots::wake`]).
    pub(crate) slots_wake: Notify,
    /// `INFO stats`' `sync_full`, `sync_partial_ok`, `sync_partial_err`:
    /// replicas synced from scratch, carrying on, and asking to carry on
    /// when they couldn't.
    pub(crate) syncs: [Cell<u64>; 3],
    /// While `EXEC` runs a transaction: whether its `MULTI` is in the
    /// stream yet (written before its first command, if any).
    transaction: Cell<Option<bool>>,
}

/// A replica attached: what it is sent.
pub(crate) struct Feed {
    pub(crate) id: NodeId,
    /// What waits to be sent.
    pub(crate) out: RefCell<Vec<u8>>,
    /// Wakes the fiber that sends it.
    notify: Notify,
    /// While it syncs from scratch: how far.
    copy: RefCell<Option<Copy>>,
    /// Its output went past [`OUTPUT_LIMIT`], or it attached again: it's
    /// being dropped.
    dropped: Cell<bool>,
    /// Its connection ended, as the fiber reading its acknowledgements saw.
    gone: Cell<bool>,
    /// The offset it last acknowledged as applied (`REPLACK offset`):
    /// `None` until it's past its copy, during which it acknowledges
    /// without one (`REPLACK`).
    acked: Cell<Option<u64>>,
    /// When it last acknowledged, either way (`INFO`'s lag).
    acked_at: Cell<Option<Instant>>,
    /// The `REPLPING`s sent and not acknowledged yet, with when and how
    /// far into the connection (bytes): an acknowledgement of one holds the
    /// lease from then, not from when it came (it may have waited in the
    /// socket while this node stalled).
    pings: RefCell<VecDeque<(u64, Instant, u64)>>,
    /// The last `REPLPING` sent, and when the next is due.
    ping: Cell<(u64, Option<Instant>)>,
    /// Bytes written to the connection.
    written: Cell<u64>,
    /// How the connection drains, from the pings acknowledged: see
    /// [`Feed::window`].
    flow: Cell<Flow>,
    /// What its acknowledgements go to.
    acks: Rc<Acks>,
    /// A slot sync's (`SLOTSYNC`) slots, in order: it is sent only theirs
    /// (see the module docs). `None`: a replica.
    pub(crate) slots: Option<Vec<u16>>,
    /// A slot sync's offset: the bytes forwarded after its copy.
    fed: Cell<u64>,
    /// A slot sync ended with `SLOTSDONE` (the target owns its slots now):
    /// the connection closes once that is sent.
    pub(crate) done: Cell<bool>,
}

/// How fast a feed's connection drains, measured from its pings'
/// acknowledgements.
#[derive(Clone, Copy, Default)]
struct Flow {
    /// How far into the connection the replica has read (the last ping it
    /// acknowledged), and when that came.
    acked: u64,
    at: Option<Instant>,
    /// Bytes a second; 0 before it is measured.
    rate: f64,
}

/// What a copy may have in flight, unacknowledged: what the link drains in
/// this long, so that a ping waits no longer behind the copy's batches
/// (the lease runs from its sending). Without a measure yet, [`WINDOW_FIRST`].
const DRAIN: Duration = Duration::from_millis(250);
const WINDOW_FIRST: u64 = 1 << 20;
const WINDOW_MIN: u64 = 256 << 10;
const WINDOW_MAX: u64 = 64 << 20;

/// What the feeds share with the node: the clients waiting for
/// acknowledgements (`WAIT`), each with its own [`Notify`] (one waiter
/// each), and when each replica last acknowledged a ping (the `REPLPING`'s
/// sending time), which outlives its connection; and when the lease ends,
/// the one thing a write reads, with
/// the reactor's turn time ([`reactor::turn_time`]): a primary stalled past
/// its lease (stopped, swapped out) takes no write from the fibers woken
/// after it. A lookup and a clock read per write instead cost a primary
/// 6-10% of its writes, the clock read alone 3% (tools/bench/ack_bench.sh).
#[derive(Default)]
pub(crate) struct Acks {
    waiting: RefCell<Vec<Rc<Notify>>>,
    last: RefCell<HashMap<NodeId, Instant>>,
    /// The replicas the table gives the node and doesn't mark failed:
    /// their acknowledgements hold its lease.
    lease: RefCell<Vec<NodeId>>,
    /// [`LEASE`] after the newest acknowledgement of one of them; `None`
    /// before any (or without them): the lease holds.
    ends: Cell<Option<Instant>>,
}

impl Acks {
    /// An acknowledgement of `replica`: the clients waiting are woken; one
    /// of a ping sent at `pinged` holds the lease from then.
    fn record(&self, replica: NodeId, pinged: Option<Instant>) {
        for n in self.waiting.borrow().iter() {
            n.notify();
        }
        let Some(t) = pinged else { return };
        let newer = self.last.borrow().get(&replica).is_none_or(|&last| last < t);
        if newer {
            self.last.borrow_mut().insert(replica, t);
        }
        if self.lease.borrow().contains(&replica) && self.ends.get().is_none_or(|e| e < t + LEASE) {
            self.ends.set(Some(t + LEASE));
        }
    }

    /// `n` is woken by each acknowledgement until the guard is dropped.
    fn enter(&self, n: Rc<Notify>) -> Entered<'_> {
        self.waiting.borrow_mut().push(n.clone());
        Entered(self, n)
    }
}

struct Entered<'a>(&'a Acks, Rc<Notify>);

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        self.0.waiting.borrow_mut().retain(|n| !Rc::ptr_eq(n, &self.1));
    }
}

/// What a slot sync gets for a `FLUSHALL`: its slots' keys are gone.
const SLOTDROP: &[u8] = b"*1\r\n$8\r\nSLOTDROP\r\n";
/// A slot sync's last frame: the target owns the slots now.
const SLOTSDONE: &[u8] = b"*1\r\n$9\r\nSLOTSDONE\r\n";

/// What a command of the stream touches, for a replica still copying.
#[derive(Clone, Copy)]
enum Touches<'a> {
    /// These keys (of one slot).
    Keys(&'a [&'a [u8]]),
    /// Every key: `FLUSHALL`, `FLUSHDB`.
    All,
    /// Every key of a slot: `REPLDROPSLOT`.
    Slot(u16),
    /// No key: `PING`.
    Nothing,
}

/// How far a sync from scratch got: see the module docs.
struct Copy {
    /// The slot being sent; the ones below are.
    slot: u32,
    /// Whether its keys were taken (not at the very start).
    loaded: bool,
    /// Its keys when it was started, and how many of them were taken.
    keys: Vec<Vec<u8>>,
    next: usize,
    /// Its keys a write touched before they were all sent: sent again,
    /// as they are then (a delete if gone).
    again: VecDeque<Vec<u8>>,
    /// Its keys sent (a collection in parts once the last part is).
    sent: HashSet<Vec<u8>>,
    /// A collection being sent in parts: its key and the next element.
    big: Option<(Vec<u8>, usize)>,
}

impl Copy {
    fn new() -> Copy {
        Copy { slot: 0, loaded: false, keys: Vec::new(), next: 0, again: VecDeque::new(), sent: HashSet::new(), big: None }
    }
}

impl Feed {
    fn new(id: NodeId, out: Vec<u8>, copy: bool, acks: Rc<Acks>, slots: Option<Vec<u16>>) -> Feed {
        Feed {
            id,
            out: RefCell::new(out),
            notify: Notify::new(),
            copy: RefCell::new(copy.then(Copy::new)),
            dropped: Cell::new(false),
            gone: Cell::new(false),
            acked: Cell::new(None),
            acked_at: Cell::new(None),
            pings: RefCell::new(VecDeque::new()),
            ping: Cell::new((0, None)),
            written: Cell::new(0),
            flow: Cell::new(Flow::default()),
            acks,
            slots,
            fed: Cell::new(0),
            done: Cell::new(false),
        }
    }

    /// A slot sync's offset, and how much of it the target hasn't
    /// acknowledged; `None` before its copy ended and was acknowledged.
    pub(crate) fn fed(&self) -> Option<(u64, u64)> {
        let acked = self.acked.get().filter(|_| !self.copying())?;
        Some((self.fed.get(), self.fed.get().saturating_sub(acked)))
    }

    /// Whether it still sends: not dropped, not hung up, not ended.
    pub(crate) fn live(&self) -> bool {
        !self.dropped.get() && !self.gone.get() && !self.done.get()
    }

    /// A replica's feed, not a slot sync's.
    pub(crate) fn is_replica(&self) -> bool {
        self.slots.is_none()
    }

    /// Whether it is sent `slot`'s keys: all of them, a replica.
    pub(crate) fn wants(&self, slot: u16) -> bool {
        self.slots.as_ref().is_none_or(|s| s.binary_search(&slot).is_ok())
    }

    /// Queues the next `REPLPING` if it's due ([`PING_FEED`]); returns
    /// when the one after is due.
    pub(crate) fn ping_if_due(&self, now: Instant) -> Instant {
        match self.ping.get().1 {
            Some(due) if now < due => due,
            _ => self.ping(now),
        }
    }

    /// Queues the next `REPLPING`, noting when; returns when the one after
    /// is due.
    pub(crate) fn ping(&self, now: Instant) -> Instant {
        let seq = self.ping.get().0 + 1;
        let mut out = resp::Encoder::new(resp::Protocol::Resp2);
        out.array(2);
        out.bulk(b"REPLPING");
        out.bulk(seq.to_string().as_bytes());
        self.out.borrow_mut().extend_from_slice(out.as_bytes());
        let at = self.queued();
        let mut pings = self.pings.borrow_mut();
        // A replica that never acknowledges: the oldest go (an
        // acknowledgement of one of them renews nothing).
        if pings.len() >= 64 {
            pings.pop_front();
        }
        pings.push_back((seq, now, at));
        self.ping.set((seq, Some(now + PING_FEED)));
        now + PING_FEED
    }

    /// `REPLACK offset|- ping` came: the offset applied (`-` during the
    /// copy) and the last `REPLPING` it got (0: none yet); recorded, and
    /// the clients waiting woken.
    pub(crate) fn ack(&self, offset: Option<u64>, ping: u64) {
        if offset.is_some() {
            self.acked.set(offset);
        }
        let now = Instant::now();
        self.acked_at.set(Some(now));
        let mut pings = self.pings.borrow_mut();
        while pings.front().is_some_and(|&(seq, _, _)| seq < ping) {
            pings.pop_front();
        }
        let pinged = pings.front().filter(|&&(seq, _, _)| seq == ping).map(|&(_, t, at)| (t, at));
        if pinged.is_some() {
            pings.pop_front();
        }
        drop(pings);
        if let Some((_, at)) = pinged {
            let mut flow = self.flow.get();
            if let Some(then) = flow.at.filter(|&then| now > then) {
                let rate = (at.saturating_sub(flow.acked)) as f64 / (now - then).as_secs_f64();
                flow.rate = if flow.rate == 0.0 { rate } else { (flow.rate + rate) / 2.0 };
            }
            (flow.acked, flow.at) = (at, Some(now));
            self.flow.set(flow);
        }
        self.acks.record(self.id, pinged.map(|(t, _)| t));
        // A copy waits for its pings (see `primary::serve`).
        if self.copying() {
            self.notify.notify();
        }
    }

    /// Bytes queued for the connection: written, or waiting to be.
    fn queued(&self) -> u64 {
        self.written.get() + self.out.borrow().len() as u64
    }

    /// `n` more bytes went out.
    pub(crate) fn wrote(&self, n: usize) {
        self.written.set(self.written.get() + n as u64);
    }

    /// Whether a copy may queue its next batch: what it queued that the
    /// replica hasn't acknowledged is within what the link drains in
    /// [`DRAIN`], so a ping never waits behind much more. On a fast link
    /// the copy streams, on a slow one it goes a batch at a time.
    pub(crate) fn may_send(&self) -> bool {
        let flow = self.flow.get();
        let window = if flow.rate == 0.0 { WINDOW_FIRST } else { ((flow.rate * DRAIN.as_secs_f64()) as u64).clamp(WINDOW_MIN, WINDOW_MAX) };
        self.queued().saturating_sub(flow.acked) <= window
    }

    /// The offset it last acknowledged (0 before one), and how long ago it
    /// last acknowledged; `None` before anything.
    pub(crate) fn acked(&self) -> Option<(u64, Duration)> {
        self.acked_at.get().map(|t| (self.acked.get().unwrap_or(0), t.elapsed()))
    }

    /// Past its copy, as the replica acknowledged (Redis's `online`): a
    /// copy queued but not taken yet doesn't count.
    pub(crate) fn online(&self) -> bool {
        self.acked.get().is_some()
    }

    /// Its connection ended: the fiber sending to it stops.
    pub(crate) fn hang_up(&self) {
        self.gone.set(true);
        self.notify.notify();
    }

    /// Whether it syncs from scratch still.
    pub(crate) fn copying(&self) -> bool {
        self.copy.borrow().is_some()
    }

    fn push(&self, bytes: &[u8]) {
        if self.dropped.get() {
            return;
        }
        let mut out = self.out.borrow_mut();
        out.extend_from_slice(bytes);
        if out.len() > OUTPUT_LIMIT {
            self.dropped.set(true);
            *out = Vec::new();
        }
        self.notify.notify();
    }

    /// A command of the stream (`bytes`), as this replica must see it:
    /// see the module docs.
    fn forward(&self, bytes: &[u8], touches: Touches) {
        // A slot sync: its slots' commands alone, and nothing after
        // `SLOTSDONE` (this node deletes their keys then).
        let mut bytes = bytes;
        if self.done.get() {
            return;
        }
        if self.slots.is_some() {
            match touches {
                Touches::Nothing => return,
                Touches::Keys(keys) if keys.first().is_some_and(|k| !self.wants(key_hash_slot(k))) => return,
                Touches::Slot(slot) if !self.wants(slot) => return,
                Touches::Keys(_) | Touches::Slot(_) => {}
                Touches::All => bytes = SLOTDROP,
            }
        }
        {
            let mut copy = self.copy.borrow_mut();
            if let Some(c) = copy.as_mut() {
                let keys = match touches {
                    Touches::Keys(keys) => keys,
                    // Whatever was sent is gone; so is what's to send.
                    Touches::All => {
                        c.again.clear();
                        c.big = None;
                        &[]
                    }
                    // Not sent yet: nothing to drop there.
                    Touches::Slot(slot) if u32::from(slot) > c.slot => return,
                    Touches::Slot(_) | Touches::Nothing => &[],
                };
                match keys.first().map(|k| u32::from(key_hash_slot(k))) {
                    None => {}
                    Some(slot) if slot > c.slot => return,
                    Some(slot) if slot == c.slot => {
                        let big = c.big.as_ref().map(|(b, _)| b.clone());
                        let in_parts = |k: &[u8]| big.as_deref() == Some(k);
                        if !keys.iter().all(|k| c.sent.contains(*k) && !in_parts(k)) {
                            for k in keys {
                                if in_parts(k) {
                                    c.big = None;
                                }
                                c.sent.remove(*k);
                                c.again.push_back(k.to_vec());
                            }
                            return;
                        }
                    }
                    Some(_) => {}
                }
            } else if self.slots.is_some() {
                self.fed.set(self.fed.get() + bytes.len() as u64);
            }
        }
        self.push(bytes);
    }
}

impl Replication {
    pub(crate) fn new(replid: NodeId, backlog: usize) -> Replication {
        Replication {
            replid,
            active: Cell::new(false),
            offset: Cell::new(0),
            backlog: RefCell::new(Backlog::new(backlog)),
            scratch: RefCell::new(Vec::new()),
            feeds: RefCell::new(Vec::new()),
            last_ping: Cell::new(None),
            link: replica::Link::default(),
            acks: Rc::default(),
            slot_syncs: RefCell::new(Vec::new()),
            slots_wake: Notify::new(),
            syncs: Default::default(),
            transaction: Cell::new(None),
        }
    }

    /// Records the stream from now on: the store records its effects.
    pub(crate) fn start(&self, store: &mut Store) {
        if !self.active.replace(true) {
            store.record_effects(true);
        }
    }

    pub(crate) fn active(&self) -> bool {
        self.active.get()
    }

    pub(crate) fn offset(&self) -> u64 {
        self.offset.get()
    }

    /// The stream after its first `have` bytes, if the backlog still holds
    /// all of it: what a replica that has `have` bytes needs.
    pub(crate) fn since(&self, have: u64) -> Option<Vec<u8>> {
        let missing = self.offset.get().checked_sub(have)?;
        self.backlog.borrow().last(usize::try_from(missing).ok()?)
    }

    /// `INFO replication`'s `repl_backlog_first_byte_offset` and
    /// `repl_backlog_histlen`: the offset of its first byte (counting from
    /// 1, 0 before it starts) and how many it holds.
    pub(crate) fn backlog_info(&self) -> (u64, usize) {
        let len = self.backlog.borrow().len;
        if self.active() {
            (self.offset.get() - len as u64 + 1, len)
        } else {
            (0, 0)
        }
    }

    pub(crate) fn backlog_size(&self) -> usize {
        self.backlog.borrow().buf.len()
    }

    /// After a command ran (`sent`: the request of a write that didn't
    /// fail, and its command): the store's effects, then the command as
    /// sent unless it recorded its own. Also after an expiry sweep, with
    /// `None`.
    pub(crate) fn feed(&self, store: &mut Store, sent: Option<(&Args, &Command)>) {
        let mut told = false;
        if store.has_effects() {
            for effect in store.take_effects() {
                match effect {
                    Effect::Expired(key) => self.append([&b"DEL"[..], &key[..]], Touches::Keys(&[&key[..]])),
                    Effect::As(args) => {
                        told = true;
                        let key = args.get(1).cloned().unwrap_or_default();
                        self.append(args.iter().map(|a| &a[..]), Touches::Keys(&[&key[..]]));
                    }
                    Effect::Nothing => told = true,
                }
            }
        }
        if let Some((args, cmd)) = sent.filter(|_| !told) {
            // Every key, a destination before a count (ZUNIONSTORE) too.
            let keys: Vec<&[u8]> = crate::command::keys_of(cmd, *args).collect();
            self.append(args.iter(), if keys.is_empty() { Touches::All } else { Touches::Keys(&keys) });
        }
    }

    /// A key copied from another node (a slot moving here by snapshot, see
    /// [`slots`]), once all of it is here: into the stream as a copy sends
    /// it (`REPLKEY ...`, a collection larger than `chunk` in parts, one
    /// after the other), or `DEL` if it's gone already (expired: this node
    /// keeps it until the source's `DEL`, serving neither meanwhile), for
    /// the replicas. Not part by part as it came: a replica whose copy passed
    /// the key's slot, or that came online, between two of them would get
    /// a part without its first. All at once, each replica gets it whole or
    /// not at all, as a write of the key.
    pub(crate) fn relay_key(&self, store: &Store, key: &[u8], chunk: usize) {
        if !self.active() {
            return;
        }
        let touches = Touches::Keys(&[key]);
        let Some(at) = store.expire_at(key) else {
            self.append([&b"DEL"[..], key], touches);
            return;
        };
        let at = at.map_or("-".to_string(), |a| a.to_string());
        let frame = |kind: store::Kind, first: bool, last: bool, payload: &[u8]| {
            let flag = |b: bool| if b { &b"1"[..] } else { &b"0"[..] };
            self.append([&b"REPLKEY"[..], key, at.as_bytes(), kind.name(), flag(first), flag(last), payload], touches);
        };
        if store.elements(key).is_some_and(|n| n > chunk) {
            let mut from = 0;
            while let Some(part) = store.dump_part(key, from, chunk, primary::BATCH_BYTES) {
                let last = from + part.count >= part.total;
                frame(part.kind, from == 0, last, &part.payload);
                if last {
                    break;
                }
                from += part.count;
            }
        } else if let Some((value, _)) = store.dump(key) {
            frame(value.kind(), true, true, value.payload());
        }
    }

    /// A key of a slot this node gave up, deleted: `DEL key`, for the
    /// replicas (no slot sync wants it).
    pub(crate) fn relay_del(&self, key: &[u8]) {
        if self.active() {
            self.append([&b"DEL"[..], key], Touches::Keys(&[key]));
        }
    }

    /// A slot copied from another node and given up: `REPLDROPSLOT slot`,
    /// so that the replicas drop it too.
    pub(crate) fn relay_drop(&self, slot: u16) {
        if self.active() {
            self.append([&b"REPLDROPSLOT"[..], slot.to_string().as_bytes()], Touches::Slot(slot));
        }
    }

    /// A `PING` into the stream every [`PING_EVERY`] while replicas are
    /// attached (from the node's timer).
    pub(crate) fn ping(&self) {
        if self.feeds.borrow().is_empty() {
            return;
        }
        let now = Instant::now();
        if self.last_ping.get().is_none_or(|t| now.duration_since(t) >= PING_EVERY) {
            self.last_ping.set(Some(now));
            self.append([&b"PING"[..]], Touches::Nothing);
        }
    }

    /// A transaction starts (`EXEC`): what it writes goes into the stream
    /// between `MULTI` and `EXEC`, which a replica applies at once (and a
    /// slot sync never gets: its target serves nothing meanwhile).
    pub(crate) fn begin_transaction(&self) {
        self.transaction.set(Some(false));
    }

    /// The transaction ends: whether it wrote anything (then its `EXEC`
    /// went into the stream too).
    pub(crate) fn end_transaction(&self) -> bool {
        let wrote = self.transaction.take() == Some(true);
        if wrote {
            self.push([&b"EXEC"[..]], Touches::Nothing);
        }
        wrote
    }

    /// One command into the stream, and to the replicas; in a transaction,
    /// after its `MULTI`.
    fn append<'a>(&self, args: impl IntoIterator<Item = &'a [u8], IntoIter: ExactSizeIterator>, touches: Touches) {
        if self.transaction.get() == Some(false) {
            self.transaction.set(Some(true));
            self.push([&b"MULTI"[..]], Touches::Nothing);
        }
        self.push(args, touches);
    }

    fn push<'a>(&self, args: impl IntoIterator<Item = &'a [u8], IntoIter: ExactSizeIterator>, touches: Touches) {
        let args = args.into_iter();
        let mut bytes = self.scratch.borrow_mut();
        bytes.clear();
        let mut out = resp::Encoder::with_buffer(std::mem::take(&mut *bytes), resp::Protocol::Resp2);
        out.array(args.len());
        for arg in args {
            out.bulk(arg);
        }
        *bytes = out.into_inner();
        self.backlog.borrow_mut().append(&bytes);
        self.offset.set(self.offset.get() + bytes.len() as u64);
        for feed in self.feeds.borrow().iter() {
            feed.forward(&bytes, touches);
        }
    }

    /// A replica attaching (`REPLSYNC`): carries on from `offset` if this
    /// stream's backlog has what follows, else syncs from scratch. The
    /// reply line goes first in its output.
    pub(crate) fn attach(&self, store: &mut Store, id: NodeId, replid: &[u8], offset: Option<u64>) -> Rc<Feed> {
        self.start(store);
        // One feed per replica: an earlier connection of it is dropped.
        self.feeds.borrow_mut().retain(|f| {
            if f.id == id && f.is_replica() {
                f.dropped.set(true);
                f.notify.notify();
            }
            f.id != id || !f.is_replica()
        });
        let rest = offset.filter(|_| replid == self.replid.as_bytes()).and_then(|o| self.since(o));
        let count = |i: usize| self.syncs[i].set(self.syncs[i].get() + 1);
        match (&rest, offset) {
            (Some(_), _) => count(1),
            (None, Some(_)) => {
                count(2);
                count(0);
            }
            (None, None) => count(0),
        }
        let feed = match rest {
            Some(rest) => {
                let mut out = format!("+CONTINUE {} {}\r\n", self.replid, offset.unwrap()).into_bytes();
                out.extend_from_slice(&rest);
                Feed::new(id, out, false, self.acks.clone(), None)
            }
            None => Feed::new(id, format!("+FULLRESYNC {} {}\r\n", self.replid, self.offset()).into_bytes(), true, self.acks.clone(), None),
        };
        let feed = Rc::new(feed);
        self.feeds.borrow_mut().push(feed.clone());
        feed
    }

    /// A slot sync attaching (`SLOTSYNC`): its slots copied from scratch,
    /// then their commands. An earlier sync of any of them by the same node
    /// is dropped.
    pub(crate) fn attach_slots(&self, store: &mut Store, id: NodeId, mut slots: Vec<u16>) -> Rc<Feed> {
        self.start(store);
        slots.sort_unstable();
        slots.dedup();
        self.feeds.borrow_mut().retain(|f| {
            let overlaps = f.id == id && f.slots.as_ref().is_some_and(|s| s.iter().any(|x| slots.binary_search(x).is_ok()));
            if overlaps {
                f.dropped.set(true);
                f.notify.notify();
            }
            !overlaps
        });
        let feed = Rc::new(Feed::new(id, format!("+SYNCING {}\r\n", slots.len()).into_bytes(), true, self.acks.clone(), Some(slots)));
        self.feeds.borrow_mut().push(feed.clone());
        feed
    }

    /// The live slot sync of node `id` whose slots are exactly `slots`.
    pub(crate) fn slot_feed(&self, id: &NodeId, slots: &[u16]) -> Option<Rc<Feed>> {
        self.feeds.borrow().iter().find(|f| f.id == *id && f.live() && f.slots.as_deref() == Some(slots)).cloned()
    }

    /// A new table: each slot sync whose node it gives every one of the
    /// sync's slots (the cutover) ends with `SLOTSDONE`, after all that this
    /// node wrote to them (it serves them no more). One still copying can't
    /// end so: it is dropped (the target's link fails; stuart doesn't cut a
    /// copy over). Returns how many keys the slots ended so hold (`INFO`'s
    /// `migrate_keys_sent`).
    pub(crate) fn cut_over(&self, table: &cluster::SlotTable, store: &Store) -> u64 {
        let mut keys = 0;
        for f in self.feeds.borrow().iter().filter(|f| f.live()) {
            let Some(slots) = &f.slots else { continue };
            let Some(to) = table.position(&f.id) else { continue };
            if !slots.iter().all(|&s| table.owner_index(s) == Some(to) && table.migration(s).is_none()) {
                continue;
            }
            if f.copying() {
                crate::warning!("Node {} took {} slots before it had copied them: its sync is dropped", f.id, slots.len());
                f.dropped.set(true);
                f.notify.notify();
                continue;
            }
            f.push(SLOTSDONE);
            f.done.set(true);
            keys += slots.iter().map(|&s| store.count_in_slot(s) as u64).sum::<u64>();
            crate::verbose!("Node {} has {} slots now: their sync ends at offset {}", f.id, slots.len(), f.fed.get());
        }
        keys
    }

    /// The feed is gone (its connection ended).
    pub(crate) fn detach(&self, feed: &Rc<Feed>) {
        self.feeds.borrow_mut().retain(|f| !Rc::ptr_eq(f, feed));
    }

    /// How many replicas past their copy have acknowledged `offset`
    /// (Redis's `replicationCountAcksByOffset`).
    pub(crate) fn acked_by(&self, offset: u64) -> usize {
        self.feeds.borrow().iter().filter(|f| f.is_replica() && !f.dropped.get() && !f.gone.get() && f.acked.get().is_some_and(|a| a >= offset)).count()
    }

    /// Whether the lease ran out, so that this primary takes no writes.
    /// It holds without replicas (in the lease: those the table gives it
    /// and doesn't mark failed), or before any of them first acknowledged
    /// (nothing could be promoted yet); else while one acknowledged within
    /// [`LEASE`].
    pub(crate) fn lease_lapsed(&self) -> bool {
        self.acks.ends.get().is_some_and(|e| reactor::turn_time() >= e)
    }

    /// A new table: `replicas` hold the lease (see [`Acks::lease`]). The
    /// acknowledgements of the others are forgotten, so that one paired
    /// again (or no longer marked failed) starts as a new one.
    pub(crate) fn set_lease(&self, replicas: Vec<NodeId>) {
        self.acks.last.borrow_mut().retain(|id, _| replicas.contains(id));
        let newest = replicas.iter().filter_map(|r| self.acks.last.borrow().get(r).copied()).max();
        self.acks.ends.set(newest.map(|t| t + LEASE));
        *self.acks.lease.borrow_mut() = replicas;
    }
}

/// `WAIT numreplicas timeout`, as Redis 7.0's: the number of replicas
/// that have acknowledged the client's last write (its `woff`), once
/// `numreplicas` have or `timeout` ms passed (0, or past what a clock
/// holds: no timeout). The replicas acknowledge each read they applied,
/// so this takes a round trip, not Redis's `REPLCONF GETACK` sent into
/// the stream.
pub(crate) fn wait(args: &Args, node: &crate::Node, client: &crate::client::Client, out: &mut resp::Encoder) -> crate::command::Flow {
    use crate::command::Flow;
    if node.cluster.as_ref().is_some_and(|c| c.primary().is_some()) {
        out.error(b"ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return Flow::Continue;
    }
    let Some(wanted) = store::parse_i64(&args[1]) else {
        out.error(b"ERR value is not an integer or out of range");
        return Flow::Continue;
    };
    let timeout = match store::parse_i64(&args[2]) {
        None => return error(out, b"ERR timeout is not an integer or out of range"),
        Some(t) if t < 0 => return error(out, b"ERR timeout is negative"),
        Some(t) => t as u64,
    };
    let repl = &node.repl;
    let woff = client.woff.get();
    let mut acked = repl.acked_by(woff);
    // In a transaction, no waiting: the count as it is (Redis denies
    // blocking in EXEC).
    if (acked as i64) < wanted && !client.in_exec.get() {
        let deadline = (timeout > 0).then(|| Instant::now().checked_add(Duration::from_millis(timeout))).flatten();
        let me = Rc::new(Notify::new());
        let _entered = repl.acks.enter(me.clone());
        loop {
            let woken = me.wait_until(deadline);
            acked = repl.acked_by(woff);
            if acked as i64 >= wanted || !woken {
                break;
            }
        }
    }
    out.integer(acked as i64);
    Flow::Continue
}

fn error(out: &mut resp::Encoder, e: &[u8]) -> crate::command::Flow {
    out.error(e);
    crate::command::Flow::Continue
}

/// Whether the command `cmd` that wrote `reply` goes into the stream as
/// sent (if it recorded no effect of its own): a write that didn't fail.
pub(crate) fn as_sent(cmd: &Command, reply: &[u8]) -> bool {
    cmd.write && !matches!(reply.first(), Some(b'-' | b'!'))
}

/// The last bytes of the stream, up to its size, in a ring.
struct Backlog {
    buf: Box<[u8]>,
    /// Where the next byte goes.
    end: usize,
    /// How many bytes it holds (`repl_backlog_histlen`).
    len: usize,
}

impl Backlog {
    fn new(size: usize) -> Backlog {
        Backlog { buf: vec![0; size].into_boxed_slice(), end: 0, len: 0 }
    }

    fn append(&mut self, mut bytes: &[u8]) {
        let size = self.buf.len();
        if bytes.len() > size {
            bytes = &bytes[bytes.len() - size..];
        }
        let first = bytes.len().min(size - self.end);
        self.buf[self.end..self.end + first].copy_from_slice(&bytes[..first]);
        self.buf[..bytes.len() - first].copy_from_slice(&bytes[first..]);
        self.end = (self.end + bytes.len()) % size;
        self.len = (self.len + bytes.len()).min(size);
    }

    /// The last `n` bytes, if it holds that many.
    fn last(&self, n: usize) -> Option<Vec<u8>> {
        if n > self.len {
            return None;
        }
        let size = self.buf.len();
        let start = (self.end + size - n) % size;
        let mut out = Vec::with_capacity(n);
        if start + n <= size {
            out.extend_from_slice(&self.buf[start..start + n]);
        } else {
            out.extend_from_slice(&self.buf[start..]);
            out.extend_from_slice(&self.buf[..n - (size - start)]);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests;
