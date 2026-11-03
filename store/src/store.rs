//! The keyspace: keys to values, with Redis 7's string commands and expiry.
//!
//! One `Store` per core, owned by that core's event loop: no locks, every
//! method takes `&mut self`.
//!
//! # Keys
//!
//! A key and its value are one heap block, a [`Kv`] (see `kv` for what it
//! holds), and the blocks sit in [`SLOTS`] tables, built like a large
//! hash's ([`Indexed`]): a dense `Vec` of the blocks' pointers and an index
//! of `u32` positions into it. In cluster mode ([`Store::with_slots`]) a
//! table is a slot's: `CLUSTER COUNTKEYSINSLOT` is its length, and moving
//! a slot to another node walks its table only, as Redis 8 keeps a dict
//! per slot. A standalone store spreads its keys over as many tables by the
//! top bits of their hash. The store doesn't know how keys map to slots:
//! the dbnode passes the function (`cluster::key_hash_slot`).
//!
//! So a key costs its block, a pointer in its table's `Vec` and two to four
//! `u32`s of its index (a `HashMap` of `Rc` keys cost about 200 bytes), and
//! a table grows on its own: doubling one rehashes 1/16384 of the keys,
//! where the one map of every key used to stop the node for 100s of ms.
//! The price is a fixed 0.8 MB of empty tables.
//!
//! # Time
//!
//! Expiry times are absolute unix times in milliseconds, as in Redis. The
//! store doesn't read a clock: the dbnode sets the time with
//! [`Store::set_now`] (once per batch of commands, the way Redis caches the
//! time per command), and tests move it by hand instead of sleeping.
//!
//! # Expiry
//!
//! A key whose time has come is invisible: every lookup checks the expiry
//! and deletes the key (lazy expiry, as in Redis: expired while
//! `now > expire_at`, so at `now == expire_at` it's alive with a PTTL of 0).
//!
//! So that keys nobody reads again are reclaimed too, every key with an
//! expiry also sits in a dense `Vec`, by where it is (its table and its
//! position there; the block knows its place in the `Vec`). Removal is a
//! `swap_remove` that fixes the place of the key moved into the hole, and a
//! key moved in its table (by a `swap_remove` there) has its entry fixed.
//! [`Store::expire_cycle`] walks that `Vec` round-robin from a cursor and
//! deletes the expired keys it meets, at most `budget` of them per call.
//! Compared with Redis's random sampling it's deterministic: every expiring
//! key is examined within `ceil(n / budget)` calls. Compared with a min-heap
//! by expiry time it never holds stale entries: refreshing a TTL (the
//! common case for sessions and caches) only writes the entry, with no heap
//! entry to leave behind or compact later, and keys without a TTL cost
//! nothing. The price is that the sweep also looks at keys that aren't due
//! yet; a sweep reads one block per key examined, without hashing.
//!
//! # Effects
//!
//! For a replica (phase 8), what a write did must be told as commands that
//! do the same wherever they run, at any time. Most writes do that as sent
//! (`SET`, `HSET`, `LMPOP`, `SMOVE`...); the others record their
//! [`Effect`] once [`Store::record_effects`] turned recording on, as
//! Redis's commands rewrite what they propagate: `SPOP` as the `SREM` of
//! the members it took; an expiry relative to now (`EXPIRE`, `SET ... EX`)
//! as the absolute time, `PEXPIREAT` or `SET ... PXAT`, and as `DEL` when
//! that time had come; `INCRBYFLOAT` and `HINCRBYFLOAT` as the value they
//! wrote. A key reclaimed because it expired, by a lookup or by
//! [`Store::expire_cycle`], is recorded too (`DEL`): a replica doesn't
//! expire keys by itself, its primary tells it, in order with the writes.
//!
//! A replica's store doesn't reclaim ([`Store::set_reclaim`]): a key whose
//! time has come is invisible to lookups but stays until its primary's
//! `DEL`, and `expire_cycle` does nothing, as in a Redis replica. Applying
//! the stream, the replica sets the time to [`BEFORE_TIME`], before every
//! expiry: the keys its primary hasn't deleted are there for its writes.
//!
//! # Touches
//!
//! For `WATCH`, what a key's writes are: while [`Store::record_touches`]
//! has it on, every write that changed a key records it, as Redis's
//! commands call `signalModifiedKey`: whenever the command did something,
//! even the same value again (`SET`, `HSET`, `INCRBY 0`, `APPEND ""`,
//! `LSET`, `LTRIM`), and not when it did nothing (`SADD` of a member there,
//! a `SET NX` that found the key, `DEL` of a missing key, `LREM` without a
//! match, `PERSIST` without an expiry). A key reclaimed as expired is
//! recorded too, as expired ([`Touch::expired`]). Off, it costs a branch.
//!
//! # Memory
//!
//! A `Vec` never gives back the room it grew, so after many deletions
//! (keys expired) the expiry index would keep its largest size.
//! [`Store::shrink`], called from a timer, shrinks it once it's less than a
//! tenth full (as Redis resizes its dicts in `serverCron`), to twice what
//! it holds. A table shrinks as keys leave it (see `table`), and gives back
//! everything once empty (its slot moved away).

use crate::float::{self, LongDouble, Number};
use crate::int::{format_i64, parse_i64};
use crate::kv::{Kv, Value};
use crate::table::Indexed;
use crate::Error;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem;

/// A time before every expiry: see the module docs.
pub const BEFORE_TIME: i64 = i64::MIN;
/// Redis's default `proto-max-bulk-len`: the longest string `APPEND` makes.
pub const DEFAULT_MAX_STRING: usize = 512 * 1024 * 1024;
/// [`Store::shrink`] leaves an index holding fewer entries than this alone:
/// the room it keeps is small, and shrinking it would come back soon.
const SHRINK_MIN: usize = 1024;

/// The number of slots of a Redis Cluster.
pub const SLOTS: usize = 16384;

/// Where a key is: its table, and its position there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Loc {
    table: u32,
    pos: u32,
}

/// The tables of keys; see the module docs.
struct Keys {
    tables: Box<[Indexed<Kv>]>,
    /// A key's slot, below [`SLOTS`]: the table of a key in cluster mode.
    slot_of: Option<fn(&[u8]) -> u16>,
    /// SipHash, randomly keyed per store: slower than a plain hash but,
    /// like Redis's keyed SipHash, it keeps clients from choosing keys
    /// that collide.
    hasher: RandomState,
    /// How many keys, in all the tables.
    len: usize,
    /// The table that held the most keys when one was last added: the
    /// largest to grow next, for [`Store::growth`].
    largest: usize,
}

impl Keys {
    fn new(slot_of: Option<fn(&[u8]) -> u16>) -> Keys {
        let tables = (0..SLOTS).map(|_| Indexed::default()).collect();
        Keys { tables, slot_of, hasher: RandomState::new(), len: 0, largest: 0 }
    }

    /// The table of `key`, and its hash there.
    fn locate(&self, key: &[u8]) -> (usize, u32) {
        let hash = self.hasher.hash_one(key);
        // The top bits standalone: the table's index takes the low ones.
        let table = self.slot_of.map_or((hash >> (64 - SLOTS.trailing_zeros())) as usize, |slot_of| usize::from(slot_of(key)));
        (table, hash as u32)
    }

    /// Where `key` is, expired or not.
    fn find(&self, key: &[u8]) -> Option<Loc> {
        let (table, hash) = self.locate(key);
        let pos = self.tables[table].find(hash, key)?;
        Some(Loc { table: table as u32, pos: pos as u32 })
    }

    fn at(&self, loc: Loc) -> &Kv {
        self.tables[loc.table as usize].get(loc.pos as usize).expect("a key where it was")
    }

    fn at_mut(&mut self, loc: Loc) -> &mut Kv {
        self.tables[loc.table as usize].get_mut(loc.pos as usize).expect("a key where it was")
    }

    fn get(&self, key: &[u8]) -> Option<&Kv> {
        self.find(key).map(|loc| self.at(loc))
    }

    fn iter(&self) -> impl Iterator<Item = &Kv> {
        self.tables.iter().flat_map(|t| t.entries())
    }
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Keys").field("len", &self.len).field("slots", &self.slot_of.is_some()).finish()
    }
}

/// Expiry option of [`Store::set`]. The numbers are as the client sent them;
/// the store checks them like Redis does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetExpiry {
    /// No option: the key loses any TTL it had.
    #[default]
    Clear,
    /// `KEEPTTL`
    KeepTtl,
    /// `EX seconds`
    Ex(i64),
    /// `PX milliseconds`
    Px(i64),
    /// `EXAT unix-seconds`
    ExAt(i64),
    /// `PXAT unix-milliseconds`
    PxAt(i64),
}

/// Condition option of [`Store::set`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetCond {
    #[default]
    Always,
    /// `NX`: only if the key doesn't exist.
    Nx,
    /// `XX`: only if the key exists.
    Xx,
    /// `IFEQ value` (Redis 8.4): only if the key holds this string; the
    /// value is given to [`Store::set_if`].
    IfEq,
    /// `IFNE value`: unless the key holds this string (a missing key is
    /// set).
    IfNe,
    /// `IFDEQ digest`: only if the string's [`digest`](Store::digest) is
    /// this one (16 hex digits, either case).
    IfDeq,
    /// `IFDNE digest`: unless it is (a missing key is set).
    IfDne,
}

impl SetCond {
    /// Whether it compares the key's value (`IFEQ` and the others).
    fn matches(self) -> bool {
        matches!(self, SetCond::IfEq | SetCond::IfNe | SetCond::IfDeq | SetCond::IfDne)
    }
}

/// The options of `SET`, parsed by the dbnode (which also rejects the
/// combinations Redis calls a syntax error, like `NX XX` or `EX KEEPTTL`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetOptions {
    pub expiry: SetExpiry,
    pub cond: SetCond,
    /// `GET`: reply with the old value.
    pub get: bool,
}

/// What `SET` replies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetReply {
    /// `+OK`
    Ok,
    /// Nil: the `NX` / `XX` condition failed.
    NotSet,
    /// With `GET`: the old value, nil if there was none. Also when the
    /// condition failed and nothing was set, as in Redis.
    Old(Option<Vec<u8>>),
}

/// Which expire command: decides the unit, whether the time is relative to
/// now, and the name in error messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpireCmd {
    Expire,
    PExpire,
    ExpireAt,
    PExpireAt,
}

impl ExpireCmd {
    fn name(self) -> &'static str {
        match self {
            ExpireCmd::Expire => "expire",
            ExpireCmd::PExpire => "pexpire",
            ExpireCmd::ExpireAt => "expireat",
            ExpireCmd::PExpireAt => "pexpireat",
        }
    }
}

/// The `NX` / `XX` / `GT` / `LT` options of the expire commands. The store
/// rejects the combinations Redis rejects; an unknown option is the
/// dbnode's to report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExpireFlags {
    /// Only if the key has no expiry.
    pub nx: bool,
    /// Only if the key has an expiry.
    pub xx: bool,
    /// Only if the new expiry is later; no expiry counts as infinitely late.
    pub gt: bool,
    /// Only if the new expiry is earlier; no expiry counts as infinitely late.
    pub lt: bool,
}

impl ExpireFlags {
    /// Rejects the combinations Redis rejects. [`Store::expire`] checks this
    /// itself; it's public because Redis checks it before parsing the time,
    /// so the dbnode needs it before it has a time to pass.
    pub fn check(self) -> Result<(), Error> {
        if self.nx && (self.xx || self.gt || self.lt) {
            return Err(Error::ExpireNxConflict);
        }
        if self.gt && self.lt {
            return Err(Error::ExpireGtLtConflict);
        }
        Ok(())
    }
}

/// The keyspace of one core. See the [module docs](self) for the design.
#[derive(Debug)]
pub struct Store {
    keys: Keys,
    /// Every key with an expiry, in no order; see the module docs.
    expiring: Vec<Loc>,
    /// Where `expire_cycle` continues.
    cursor: usize,
    /// Unix ms.
    now: i64,
    /// The longest string `APPEND` makes.
    max_string: usize,
    /// For `SPOP`, `SRANDMEMBER`: xorshift64*, seeded at random.
    rng: u64,
    /// While recording (see the module docs), the effects since they were
    /// last taken.
    effects: Option<Vec<Effect>>,
    /// While recording (see the module docs), the keys touched since they
    /// were last taken.
    touched: Option<Vec<Touch>>,
    /// Whether expired keys are deleted (not at a replica).
    reclaim: bool,
    /// Slots whose expired keys are hidden but not deleted, as at a
    /// replica, whatever `reclaim` says: in cluster mode, the slots a
    /// node copies from another (moving by snapshot), whose keys the
    /// other expires. Their keys aren't counted or listed either (`DBSIZE`,
    /// `KEYS`, `SCAN`), as Redis 8 hides a slot it imports. In order;
    /// usually empty.
    held: Vec<u16>,
}

/// Whether `value` satisfies an `IFEQ`, `IFNE`, `IFDEQ` or `IFDNE`
/// condition with `matching`: the value compared byte for byte, the digest
/// as hex in either case (after checking it's 16 bytes).
fn value_matches(value: &[u8], cond: SetCond, matching: &[u8]) -> Result<bool, Error> {
    Ok(match cond {
        SetCond::IfEq => value == matching,
        SetCond::IfNe => value != matching,
        SetCond::IfDeq | SetCond::IfDne => {
            if matching.len() != 16 {
                return Err(Error::DigestLength);
            }
            let same = crate::xxh3::digest(value).eq_ignore_ascii_case(matching);
            same == (cond == SetCond::IfDeq)
        }
        _ => unreachable!("a condition on the value"),
    })
}

/// `INCREX`'s reply: the new value and the increment applied, integers or
/// floats (written as `INCRBYFLOAT` writes them).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IncrexReply {
    Ints(i64, i64),
    Floats(Vec<u8>, Vec<u8>),
}

/// A key a write changed, or reclaimed as expired; see the [module
/// docs](self).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Touch {
    pub key: Box<[u8]>,
    /// It had expired and was deleted (by a lookup or `expire_cycle`).
    pub expired: bool,
}

/// What a write did, when it isn't what the command says as sent, or a key
/// reclaimed as expired; see the [module docs](self).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// The key had expired and is gone: `DEL key`.
    Expired(Vec<u8>),
    /// The command did this, as a command's arguments (the name first).
    As(Vec<Vec<u8>>),
    /// The command, which records its effect, changed nothing.
    Nothing,
}

impl Default for Store {
    fn default() -> Store {
        // A randomly keyed SipHash of anything is a random seed; `| 1` as
        // xorshift's state must not be 0.
        let rng = std::collections::hash_map::RandomState::new().hash_one(0u8) | 1;
        Store {
            keys: Keys::new(None),
            expiring: Vec::new(),
            cursor: 0,
            now: 0,
            max_string: DEFAULT_MAX_STRING,
            rng,
            effects: None,
            touched: None,
            reclaim: true,
            held: Vec::new(),
        }
    }
}

impl Store {
    pub fn new() -> Store {
        Store::default()
    }

    /// A store that keeps its keys by slot (see the module docs), for
    /// cluster mode. `slot_of` gives a key's slot, below [`SLOTS`].
    pub fn with_slots(slot_of: fn(&[u8]) -> u16) -> Store {
        Store { keys: Keys::new(Some(slot_of)), ..Store::default() }
    }

    /// Sets the current time, unix ms. Expiry is judged against it.
    pub fn set_now(&mut self, now_ms: i64) {
        self.now = now_ms;
    }

    pub fn now(&self) -> i64 {
        self.now
    }

    /// Whether expired keys are deleted, by lookups and by
    /// [`expire_cycle`](Self::expire_cycle) (the default), or only hidden
    /// (a replica's store). See the module docs.
    pub fn set_reclaim(&mut self, on: bool) {
        self.reclaim = on;
    }

    /// The slots whose expired keys aren't deleted (see `held`), replacing
    /// those given before.
    pub fn set_held(&mut self, mut slots: Vec<u16>) {
        slots.sort_unstable();
        slots.dedup();
        self.held = slots;
    }

    /// Whether `slot` is held (see `held`).
    pub fn is_held(&self, slot: u16) -> bool {
        !self.held.is_empty() && self.held.binary_search(&slot).is_ok()
    }

    /// Whether the keys of `table`, expired, may be deleted.
    fn reclaims(&self, table: u32) -> bool {
        self.reclaim && !self.table_held(table)
    }

    /// Whether `table` is a held slot's.
    fn table_held(&self, table: u32) -> bool {
        !self.held.is_empty() && self.has_slots() && self.is_held(table as u16)
    }

    /// Records the writes' effects from now on, or stops (and drops those
    /// not taken). See the module docs.
    pub fn record_effects(&mut self, on: bool) {
        self.effects = on.then(Vec::new);
    }

    /// The effects recorded since the last call, in order.
    pub fn take_effects(&mut self) -> Vec<Effect> {
        match &mut self.effects {
            Some(effects) if !effects.is_empty() => mem::take(effects),
            _ => Vec::new(),
        }
    }

    /// Whether effects are recorded and some wait to be taken: cheaper
    /// than [`take_effects`](Self::take_effects) for every command.
    pub fn has_effects(&self) -> bool {
        self.effects.as_ref().is_some_and(|e| !e.is_empty())
    }

    /// Says that the write command being run changed nothing and needs no
    /// replicating (`GEORADIUS` without `STORE`, flagged a write as in
    /// Redis, which doesn't propagate it either).
    pub fn wrote_nothing(&mut self) {
        self.effect(|| Effect::Nothing);
    }

    /// Records `effect` if recording.
    fn effect(&mut self, effect: impl FnOnce() -> Effect) {
        if let Some(effects) = &mut self.effects {
            effects.push(effect());
        }
    }

    /// Records the keys writes touch from now on, or stops (and drops those
    /// not taken). See the module docs.
    pub fn record_touches(&mut self, on: bool) {
        self.touched = on.then(Vec::new);
    }

    /// The keys touched since the last call, in order.
    pub fn take_touched(&mut self) -> Vec<Touch> {
        match &mut self.touched {
            Some(touched) if !touched.is_empty() => mem::take(touched),
            _ => Vec::new(),
        }
    }

    /// Records that a write changed `key`, if recording.
    fn touch(&mut self, key: &[u8]) {
        if let Some(touched) = &mut self.touched {
            touched.push(Touch { key: key.into(), expired: false });
        }
    }

    /// Records that `key` was reclaimed as expired, if recording.
    fn touch_expired(&mut self, key: &[u8]) {
        if let Some(touched) = &mut self.touched {
            touched.push(Touch { key: key.into(), expired: true });
        }
    }

    /// Redis's `proto-max-bulk-len` as far as the store is concerned: the
    /// longest string `APPEND` may make (default 512 MiB). The dbnode
    /// enforces it on request sizes.
    pub fn set_max_string_len(&mut self, len: usize) {
        self.max_string = len;
    }

    /// `GET`. The value is borrowed: copy it straight into the reply.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_str()?)),
            None => Ok(None),
        }
    }

    /// `MGET`: like `GET`, except a value of another type reads as nil
    /// instead of failing. (The dbnode can also do this key by key with
    /// `get(key).ok().flatten()`, writing each reply as it goes.)
    pub fn mget(&mut self, keys: &[&[u8]]) -> Vec<Option<&[u8]>> {
        // Expire first, then borrow: the borrows must all live at once.
        for key in keys {
            self.live(key);
        }
        keys.iter().map(|key| self.keys.get(key).and_then(|e| e.as_str().ok())).collect()
    }

    /// `SET` with its options; see [`SetReply`] for what it returns.
    pub fn set(&mut self, key: &[u8], value: &[u8], opts: SetOptions) -> Result<SetReply, Error> {
        self.set_if(key, value, opts, b"")
    }

    /// [`set`](Self::set) with an `IFEQ`, `IFNE`, `IFDEQ` or `IFDNE`
    /// condition (`opts.cond`), `matching` the value or digest it names.
    pub fn set_if(&mut self, key: &[u8], value: &[u8], opts: SetOptions, matching: &[u8]) -> Result<SetReply, Error> {
        // As in Redis: a bad expiry first, then the type check of GET, then
        // the condition: whether the key exists, then its type and value.
        let at = self.expire_at_for(opts.expiry, "set")?;
        let now = self.now;
        // With an expiry the effect is the absolute time, or a delete.
        let recorded = at.is_some() && self.effects.is_some();
        let entry = self.live(key);
        if opts.get {
            if let Some(e) = &entry {
                e.as_str()?;
            }
        }
        let mut blocked = match opts.cond {
            SetCond::Always | SetCond::IfNe | SetCond::IfDne => false,
            SetCond::Nx => entry.is_some(),
            SetCond::Xx | SetCond::IfEq | SetCond::IfDeq => entry.is_none(),
        };
        if let Some(e) = entry.as_ref().filter(|_| opts.cond.matches()) {
            blocked = !value_matches(e.as_str()?, opts.cond, matching)?;
        }
        if blocked {
            let reply = match entry {
                _ if !opts.get => SetReply::NotSet,
                Some(e) => SetReply::Old(Some(e.as_str()?.to_vec())),
                None => SetReply::Old(None),
            };
            if recorded {
                self.effect(|| Effect::Nothing);
            }
            return Ok(reply);
        }
        let existed = entry.is_some();
        let old = match entry {
            None => {
                // An expiry in the past (PXAT 1) still replies OK, but leaves
                // no key behind.
                if at.is_none_or(|at| at > now) {
                    self.insert(key, value, at);
                    self.touch(key);
                }
                None
            }
            Some(e) => {
                // The type was checked above.
                let old = if opts.get { Some(e.as_str()?.to_vec()) } else { None };
                e.set_str(value);
                let has_ttl = e.has_ttl();
                match (opts.expiry, at) {
                    (SetExpiry::KeepTtl, _) => {}
                    (_, None) if has_ttl => self.set_ttl(key, None),
                    (_, None) => {}
                    (_, Some(at)) if at <= now => {
                        self.remove(key);
                    }
                    (_, Some(at)) if has_ttl => e.set_expire_at(at),
                    (_, Some(at)) => self.set_ttl(key, Some(at)),
                }
                self.touch(key);
                old
            }
        };
        if let Some(at) = at.filter(|_| recorded) {
            self.effect(|| match () {
                _ if at > now => Effect::As(vec![b"SET".to_vec(), key.to_vec(), value.to_vec(), b"PXAT".to_vec(), at.to_string().into()]),
                _ if existed => Effect::As(vec![b"DEL".to_vec(), key.to_vec()]),
                _ => Effect::Nothing,
            });
        }
        Ok(if opts.get { SetReply::Old(old) } else { SetReply::Ok })
    }

    /// `DIGEST`: the string's XXH3 hash, as 16 lowercase hex digits (Redis
    /// 8.4); `None` for a missing key.
    pub fn digest(&mut self, key: &[u8]) -> Result<Option<[u8; 16]>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(crate::xxh3::digest(e.as_str()?))),
            None => Ok(None),
        }
    }

    /// `DELEX key [IFEQ|IFNE|IFDEQ|IFDNE value]` (Redis 8.4), `cond` the
    /// condition (`Always`: none, a plain `DEL`): whether the key went. A
    /// missing key is no error; with a condition, a key of another type is.
    pub fn delex(&mut self, key: &[u8], cond: SetCond, matching: &[u8]) -> Result<bool, Error> {
        if !cond.matches() {
            return Ok(self.del(key));
        }
        let Some(e) = self.live(key) else { return Ok(false) };
        let value = e.as_str().map_err(|_| Error::DelexNotString)?;
        if !value_matches(value, cond, matching)? {
            return Ok(false);
        }
        self.remove(key);
        self.touch(key);
        Ok(true)
    }

    /// `MSETEX numkeys key value ... [NX|XX] [EX|PX|EXAT|PXAT n|KEEPTTL]`
    /// (Redis 8.4): every pair set, with one expiry, unless `NX` finds a
    /// key or `XX` misses one (then none is, and it's false). The expiry is
    /// checked first, then every key, then they're set.
    pub fn msetex(&mut self, pairs: &[(&[u8], &[u8])], cond: SetCond, expiry: SetExpiry) -> Result<bool, Error> {
        self.expire_at_for(expiry, "msetex")?;
        for (key, _) in pairs {
            let found = self.live(key).is_some();
            if (cond == SetCond::Nx && found) || (cond == SetCond::Xx && !found) {
                return Ok(false);
            }
        }
        for (key, value) in pairs {
            self.set(key, value, SetOptions { expiry, ..SetOptions::default() }).map_err(|e| match e {
                Error::InvalidExpireTime(_) => Error::InvalidExpireTime("msetex"),
                e => e,
            })?;
        }
        Ok(true)
    }

    /// `INCREX key [BYINT n|BYFLOAT f] [LBOUND l] [UBOUND u] [SATURATE]
    /// [EX|PX|EXAT|PXAT t|PERSIST] [ENX]` (Redis 8.8), `opts` what follows
    /// the key. Adds to the integer (or float) at `key`, 0 if missing; a
    /// result past the bounds (or the type's range) leaves the key as it is
    /// and replies the value and 0, unless `SATURATE` caps it. The TTL is
    /// kept unless an expiry or `PERSIST` says otherwise; `ENX` sets the
    /// expiry only on a key without one. The effect is a `SET` of the
    /// result (`PXAT`, or `KEEPTTL`), a `DEL` if the time had come, nothing
    /// if refused. Parsed and checked in Redis's order
    /// (`parseIncrExArgumentsOrReply`, `increxCommand`).
    pub fn increx(&mut self, key: &[u8], opts: &[&[u8]]) -> Result<IncrexReply, Error> {
        enum Expiry {
            Keep,
            Persist,
            /// `None` if its number isn't an integer: an error once the
            /// bounds and ENX are checked, as in Redis.
            At(Option<SetExpiry>),
        }
        let (mut by_int, mut by_float) = (None, None);
        let (mut lower, mut upper): (Option<&[u8]>, Option<&[u8]>) = (None, None);
        let (mut saturate, mut enx) = (false, false);
        let mut expiry = Expiry::Keep;
        let mut i = 0;
        while i < opts.len() {
            let (opt, next) = (opts[i], opts.get(i + 1).copied());
            let is = |name: &[u8]| opt.eq_ignore_ascii_case(name);
            let unit: Option<fn(i64) -> SetExpiry> = [&b"ex"[..], b"px", b"exat", b"pxat"]
                .iter()
                .position(|name| is(name))
                .map(|n| [SetExpiry::Ex, SetExpiry::Px, SetExpiry::ExAt, SetExpiry::PxAt][n]);
            let no_expiry = matches!(expiry, Expiry::Keep);
            match next {
                Some(n) if is(b"byint") && by_int.is_none() && by_float.is_none() => {
                    by_int = Some(parse_i64(n).ok_or(Error::IncrexNotInteger)?);
                    i += 1;
                }
                Some(n) if is(b"byfloat") && by_int.is_none() && by_float.is_none() => {
                    by_float = Some(match float::parse(n) {
                        None => return Err(Error::IncrexNotFloat),
                        Some(Number::Infinite(_)) => return Err(Error::IncrexInfinite),
                        Some(Number::Finite(f)) => f,
                    });
                    i += 1;
                }
                Some(n) if is(b"lbound") && lower.is_none() => {
                    lower = Some(n);
                    i += 1;
                }
                Some(n) if is(b"ubound") && upper.is_none() => {
                    upper = Some(n);
                    i += 1;
                }
                _ if is(b"saturate") && !saturate => saturate = true,
                _ if is(b"enx") && !enx && !matches!(expiry, Expiry::Persist) => enx = true,
                _ if is(b"persist") && no_expiry && !enx => expiry = Expiry::Persist,
                Some(n) if unit.is_some() && no_expiry => {
                    expiry = Expiry::At(parse_i64(n).map(unit.expect("matched")));
                    i += 1;
                }
                _ => return Err(Error::Syntax),
            }
            i += 1;
        }
        // The bounds, by the increment's kind; then ENX; then the expiry.
        enum Bounds {
            Ints(i64, i64),
            Floats(Number, Number),
        }
        let bounds = if by_float.is_some() {
            let parse = |b: Option<&[u8]>, name, default| match b {
                None => Ok(Number::Finite(default)),
                Some(b) => float::parse(b).ok_or(Error::BoundNotFloat(name)),
            };
            let (lb, ub) = (parse(lower, "LBOUND", LongDouble::MAX.neg())?, parse(upper, "UBOUND", LongDouble::MAX)?);
            if lb > ub {
                return Err(Error::BoundsReversed);
            }
            Bounds::Floats(lb, ub)
        } else {
            let parse = |b: Option<&[u8]>, name, default| b.map_or(Ok(default), |b| parse_i64(b).ok_or(Error::BoundNotInteger(name)));
            let (lb, ub) = (parse(lower, "LBOUND", i64::MIN)?, parse(upper, "UBOUND", i64::MAX)?);
            if lb > ub {
                return Err(Error::BoundsReversed);
            }
            Bounds::Ints(lb, ub)
        };
        if enx && matches!(expiry, Expiry::Keep) {
            return Err(Error::EnxWithoutExpiry);
        }
        let at = match expiry {
            Expiry::At(None) => return Err(Error::NotInteger),
            Expiry::At(Some(e)) => self.expire_at_for(e, "increx")?,
            _ => None,
        };

        let now = self.now;
        let (found, has_ttl, current) = match self.live(key) {
            Some(e) => (true, e.has_ttl(), Some(e.as_str()?.to_vec())),
            None => (false, false, None),
        };
        let (reply, written) = match bounds {
            Bounds::Floats(lb, ub) => {
                let old = match current.as_deref().map(float::parse) {
                    None => LongDouble::ZERO,
                    Some(None) => return Err(Error::NotFloat),
                    Some(Some(Number::Infinite(_))) => return Err(Error::ValueInfinite),
                    Some(Some(Number::Finite(v))) => v,
                };
                let incr = by_float.expect("a float increment");
                let sum = old.add(incr);
                let out_of_range = sum.is_none_or(|v| Number::Finite(v) > ub || Number::Finite(v) < lb);
                let value = if !out_of_range {
                    Number::Finite(sum.expect("in range"))
                } else if !saturate {
                    let mut v = Vec::new();
                    old.write(&mut v);
                    self.effect(|| Effect::Nothing);
                    return Ok(IncrexReply::Floats(v, b"0".to_vec()));
                } else if sum.is_none() {
                    if incr.is_negative() { lb } else { ub }
                } else if sum.is_some_and(|v| Number::Finite(v) > ub) {
                    ub
                } else {
                    lb
                };
                let Number::Finite(value) = value else { return Err(Error::AppliedInfinite) };
                let delta = value.add(old.neg()).ok_or(Error::AppliedInfinite)?;
                let (mut v, mut d) = (Vec::new(), Vec::new());
                value.write(&mut v);
                delta.write(&mut d);
                (IncrexReply::Floats(v.clone(), d), v)
            }
            Bounds::Ints(lb, ub) => {
                let old = match current.as_deref().map(parse_i64) {
                    None => 0,
                    Some(None) => return Err(Error::NotInteger),
                    Some(Some(v)) => v,
                };
                let incr = by_int.unwrap_or(1);
                let sum = old.checked_add(incr);
                let value = match sum {
                    Some(v) if v <= ub && v >= lb => v,
                    _ if !saturate => {
                        self.effect(|| Effect::Nothing);
                        return Ok(IncrexReply::Ints(old, 0));
                    }
                    None if incr >= 0 => ub,
                    None => lb,
                    Some(v) if v > ub => ub,
                    Some(_) => lb,
                };
                let delta = value.checked_sub(old).ok_or(Error::AppliedOverflow)?;
                (IncrexReply::Ints(value, delta), value.to_string().into_bytes())
            }
        };
        let set_expiry = at.filter(|_| !enx || !has_ttl);
        if set_expiry.is_some_and(|at| at <= now) {
            // The time has come: the key goes (if it was there).
            if found {
                self.remove(key);
                self.touch(key);
            }
            self.effect(|| if found { Effect::As(vec![b"DEL".to_vec(), key.to_vec()]) } else { Effect::Nothing });
            return Ok(reply);
        }
        match self.live(key) {
            Some(e) => e.set_str(&written),
            None => self.insert(key, &written, None),
        }
        if matches!(reply, IncrexReply::Floats(..)) {
            self.live(key).expect("just written").mark_float();
        }
        let mut effect = vec![b"SET".to_vec(), key.to_vec(), written];
        if matches!(expiry, Expiry::Persist) {
            self.set_ttl(key, None);
        } else if let Some(at) = set_expiry {
            self.set_ttl(key, Some(at));
            effect.extend([b"PXAT".to_vec(), at.to_string().into_bytes()]);
        } else {
            effect.push(b"KEEPTTL".to_vec());
        }
        self.touch(key);
        self.effect(|| Effect::As(effect));
        Ok(reply)
    }

    /// The absolute expiry time `SET` (or `cmd`) asks for, checked like
    /// Redis does: the number must be positive and the time must fit an
    /// `i64`.
    fn expire_at_for(&self, expiry: SetExpiry, cmd: &'static str) -> Result<Option<i64>, Error> {
        let (n, unit, base) = match expiry {
            SetExpiry::Clear | SetExpiry::KeepTtl => return Ok(None),
            SetExpiry::Ex(n) => (n, 1000, self.now),
            SetExpiry::Px(n) => (n, 1, self.now),
            SetExpiry::ExAt(n) => (n, 1000, 0),
            SetExpiry::PxAt(n) => (n, 1, 0),
        };
        let at = n.checked_mul(unit).and_then(|ms| ms.checked_add(base)).filter(|_| n > 0);
        at.map(Some).ok_or(Error::InvalidExpireTime(cmd))
    }

    /// `MSET`: plain `SET`s, the last one wins for a repeated key.
    pub fn mset<'a>(&mut self, pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) {
        for (key, value) in pairs {
            // A plain SET can't fail: no expiry to check, no GET.
            let _ = self.set(key, value, SetOptions::default());
        }
    }

    /// `SETNX`: true if the key was set (it didn't exist).
    pub fn setnx(&mut self, key: &[u8], value: &[u8]) -> bool {
        let opts = SetOptions { cond: SetCond::Nx, ..SetOptions::default() };
        self.set(key, value, opts) == Ok(SetReply::Ok)
    }

    /// `DEL` of one key: true if it existed. `DEL k1 k2 ...` counts the trues.
    pub fn del(&mut self, key: &[u8]) -> bool {
        // An expired key is deleted either way, but doesn't count.
        let found = self.live(key).is_some() && self.remove(key).is_some();
        if found {
            self.touch(key);
        }
        found
    }

    /// `EXISTS` of one key. `EXISTS k1 k2 ...` counts the trues, repeats
    /// included.
    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.live(key).is_some()
    }

    /// Whether `key` exists and hasn't expired, without reclaiming it if it
    /// has: Redis's lookup with `LOOKUP_NOEXPIRE`, which the slot check of
    /// a migrating slot uses (the command may be redirected, not run).
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.get(key).is_some_and(|e| self.now <= e.expire_at())
    }

    /// Whether `key` is in the keyspace, expired or not (Redis's `dbFind`):
    /// whether a `FLUSHALL` touches it, and with [`contains`](Self::contains)
    /// whether it has expired without being reclaimed.
    pub fn present(&self, key: &[u8]) -> bool {
        self.keys.get(key).is_some()
    }

    /// A live key's value and the milliseconds it has left (`None` without
    /// expiry; at least 1, a key at its last millisecond counts as 1), for
    /// copying it to another node. Like [`contains`](Self::contains), an
    /// expired key is `None` and isn't reclaimed.
    pub fn peek(&self, key: &[u8]) -> Option<(&[u8], Option<i64>)> {
        let e = self.keys.get(key).filter(|e| self.now <= e.expire_at())?;
        let ttl = e.has_ttl().then(|| (e.expire_at() - self.now).max(1));
        // Only strings exist outside the tests.
        Some((e.as_str().ok()?, ttl))
    }

    /// `GETDEL`: the value, now deleted. A value of another type stays.
    pub fn getdel(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.live(key) {
            None => return Ok(None),
            Some(e) => {
                e.as_str()?;
            }
        }
        self.touch(key);
        match self.remove(key).map(Kv::into_value) {
            Some(Value::Str(s)) => Ok(Some(s)),
            _ => unreachable!("checked above"),
        }
    }

    /// `APPEND`: the new length. Creates the key if missing; keeps the TTL.
    /// The string moves out of its block to a `Vec` of its own, which grows
    /// by doubling: appending stays linear.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<usize, Error> {
        let max = self.max_string;
        let len = match self.live(key) {
            Some(e) => {
                if e.as_str()?.len() + value.len() > max {
                    return Err(Error::StringTooLong);
                }
                let s = e.str_buf()?;
                s.extend_from_slice(value);
                s.len()
            }
            None => {
                self.insert(key, value, None);
                value.len()
            }
        };
        self.touch(key);
        Ok(len)
    }

    /// `STRLEN`: 0 for a missing key.
    pub fn strlen(&mut self, key: &[u8]) -> Result<usize, Error> {
        match self.live(key) {
            Some(e) => Ok(e.as_str()?.len()),
            None => Ok(0),
        }
    }

    /// `INCRBY` (and `INCR` with 1): the new value. A missing key counts as
    /// 0; the TTL is kept.
    pub fn incr_by(&mut self, key: &[u8], by: i64) -> Result<i64, Error> {
        let mut buf = [0; 20];
        let new = match self.live(key) {
            Some(e) => {
                let old = parse_i64(e.as_str()?).ok_or(Error::NotInteger)?;
                let new = old.checked_add(by).ok_or(Error::Overflow)?;
                e.set_str(format_i64(&mut buf, new));
                new
            }
            None => {
                self.insert(key, format_i64(&mut buf, by), None);
                by
            }
        };
        self.touch(key);
        Ok(new)
    }

    /// `INCRBYFLOAT`: the new value, as written. In Redis's order: the
    /// key's type, its value, then the increment; the TTL is kept. As in
    /// Redis, the result is a string to `OBJECT ENCODING` (`embstr` or
    /// `raw`) even when it reads as an integer.
    pub fn incr_by_float(&mut self, key: &[u8], incr: &[u8]) -> Result<Vec<u8>, Error> {
        let old = match self.live(key) {
            Some(e) => float::parse(e.as_str()?).ok_or(Error::NotFloat)?,
            None => Number::Finite(LongDouble::ZERO),
        };
        let incr = float::parse(incr).ok_or(Error::NotFloat)?;
        let mut new = Vec::new();
        old.add(incr).ok_or(Error::NotFinite)?.write(&mut new);
        match self.live(key) {
            Some(e) => e.set_str(&new),
            None => self.insert(key, &new, None),
        }
        self.live(key).expect("just written").mark_float();
        self.touch(key);
        self.effect(|| Effect::As(vec![b"SET".to_vec(), key.to_vec(), new.clone(), b"KEEPTTL".to_vec()]));
        Ok(new)
    }

    /// `DECRBY` (and `DECR` with 1).
    pub fn decr_by(&mut self, key: &[u8], by: i64) -> Result<i64, Error> {
        if by == i64::MIN {
            return Err(Error::DecrementOverflow);
        }
        self.incr_by(key, -by)
    }

    /// `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`: true if the expiry was
    /// set. A time not in the future deletes the key (and counts as set).
    pub fn expire(&mut self, cmd: ExpireCmd, key: &[u8], when: i64, flags: ExpireFlags) -> Result<bool, Error> {
        flags.check()?;
        let invalid = Error::InvalidExpireTime(cmd.name());
        let (unit, base) = match cmd {
            ExpireCmd::Expire => (1000, self.now),
            ExpireCmd::PExpire => (1, self.now),
            ExpireCmd::ExpireAt => (1000, 0),
            ExpireCmd::PExpireAt => (1, 0),
        };
        // Negative times are fine (they delete the key), overflow is not.
        let at = when.checked_mul(unit).and_then(|ms| ms.checked_add(base)).ok_or(invalid)?;

        let now = self.now;
        let Some(e) = self.live(key) else {
            self.effect(|| Effect::Nothing);
            return Ok(false);
        };
        let current = e.has_ttl().then(|| e.expire_at());
        let refused = (flags.nx && current.is_some())
            || (flags.xx && current.is_none())
            || (flags.gt && current.is_none_or(|cur| at <= cur))
            || (flags.lt && current.is_some_and(|cur| at >= cur));
        if refused {
            self.effect(|| Effect::Nothing);
            return Ok(false);
        }
        if at <= now {
            self.remove(key);
            self.effect(|| Effect::As(vec![b"DEL".to_vec(), key.to_vec()]));
        } else {
            self.set_ttl(key, Some(at));
            self.effect(|| Effect::As(vec![b"PEXPIREAT".to_vec(), key.to_vec(), at.to_string().into()]));
        }
        self.touch(key);
        Ok(true)
    }

    /// `PTTL`: ms left, -1 without expiry, -2 if the key doesn't exist.
    pub fn pttl(&mut self, key: &[u8]) -> i64 {
        let now = self.now;
        match self.live(key) {
            None => -2,
            Some(e) if !e.has_ttl() => -1,
            // `live` leaves only keys with now <= expire_at: never negative.
            Some(e) => e.expire_at().saturating_sub(now),
        }
    }

    /// `PEXPIRETIME`: the absolute expiry, unix ms; -1 without expiry, -2
    /// if the key doesn't exist.
    pub fn pexpire_time(&mut self, key: &[u8]) -> i64 {
        match self.live(key) {
            None => -2,
            Some(e) if !e.has_ttl() => -1,
            Some(e) => e.expire_at(),
        }
    }

    /// `TTL`: like `PTTL`, in seconds, rounded to the nearest like Redis.
    pub fn ttl(&mut self, key: &[u8]) -> i64 {
        match self.pttl(key) {
            ms if ms < 0 => ms,
            ms => ms.saturating_add(500) / 1000,
        }
    }

    /// `PERSIST`: true if the key had an expiry, now removed.
    pub fn persist(&mut self, key: &[u8]) -> bool {
        match self.live(key) {
            Some(e) if e.has_ttl() => {
                self.set_ttl(key, None);
                self.touch(key);
                true
            }
            _ => false,
        }
    }

    /// `TYPE`: `"string"`, `"hash"`, `"set"`, `"list"`, `"zset"`, or `"none"` for a
    /// missing key.
    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        self.live(key).map_or("none", |e| e.type_name())
    }

    /// `OBJECT ENCODING`: `None` for a missing key.
    pub fn encoding(&mut self, key: &[u8]) -> Option<&'static str> {
        self.live(key).map(|e| e.encoding())
    }

    /// Every key, expired keys not reclaimed yet included, those of held
    /// slots too (`INFO keyspace`).
    pub fn dbsize(&self) -> usize {
        self.keys.len
    }

    /// `DBSIZE`: like [`dbsize`](Self::dbsize), without the held slots'
    /// keys (see [`set_held`](Self::set_held)).
    pub fn dbsize_served(&self) -> usize {
        let held: usize = if self.has_slots() { self.held.iter().map(|&s| self.keys.tables[usize::from(s)].len()).sum() } else { 0 };
        self.keys.len - held
    }

    /// Every key, in no particular order; like [`dbsize`](Self::dbsize),
    /// expired keys not reclaimed yet included.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.keys.iter().map(Kv::key)
    }

    /// Whether the keys are kept by slot ([`Store::with_slots`]).
    pub fn has_slots(&self) -> bool {
        self.keys.slot_of.is_some()
    }

    /// The table of `slot`. Panics without slots, or if `slot` isn't below
    /// [`SLOTS`].
    fn slot(&self, slot: u16) -> &Indexed<Kv> {
        assert!(self.has_slots(), "a store without the slot index");
        &self.keys.tables[usize::from(slot)]
    }

    /// `CLUSTER COUNTKEYSINSLOT`: like [`dbsize`](Self::dbsize), expired
    /// keys not reclaimed yet included. Panics without the slot index, or if
    /// `slot` isn't below [`SLOTS`].
    pub fn count_in_slot(&self, slot: u16) -> usize {
        self.slot(slot).len()
    }

    /// The keys of `slot`, in no particular order, expired ones not
    /// reclaimed yet included (`CLUSTER GETKEYSINSLOT`). Panics as
    /// [`count_in_slot`](Self::count_in_slot) does.
    pub fn keys_in_slot(&self, slot: u16) -> impl Iterator<Item = &[u8]> {
        self.slot(slot).entries().iter().map(Kv::key)
    }

    /// Deletes up to `n` keys of `slot`, expired or not, recording no
    /// effect, and returns them: a slot given up, emptied in pieces. Panics
    /// as [`count_in_slot`](Self::count_in_slot) does.
    pub fn drop_slot_part(&mut self, slot: u16, n: usize) -> Vec<Box<[u8]>> {
        let len = self.slot(slot).len();
        let mut keys = Vec::with_capacity(n.min(len));
        // From the end: nothing moves.
        for pos in (len.saturating_sub(n)..len).rev() {
            keys.push(self.remove_at(Loc { table: u32::from(slot), pos: pos as u32 }).key().into());
        }
        keys
    }

    /// Deletes every key of `slot`, expired or not, recording no effect: a
    /// slot copied from another node and given up. Returns how many.
    /// Panics as [`count_in_slot`](Self::count_in_slot) does.
    pub fn drop_slot(&mut self, slot: u16) -> usize {
        let len = self.slot(slot).len();
        for pos in (0..len).rev() {
            self.remove_at(Loc { table: u32::from(slot), pos: pos as u32 });
        }
        len
    }

    /// `FLUSHDB` / `FLUSHALL`. Frees the memory too.
    pub fn flush(&mut self) {
        self.keys = Keys::new(self.keys.slot_of);
        self.expiring = Vec::new();
        self.cursor = 0;
    }

    /// Active expiry: examines up to `budget` keys with an expiry, continuing
    /// where the last call stopped, and deletes the expired ones. Returns how
    /// many it deleted. Call it from a timer; if it deleted a good part of
    /// `budget`, more are probably due and calling again soon pays off.
    pub fn expire_cycle(&mut self, budget: usize) -> usize {
        let mut removed = 0;
        if !self.reclaim {
            return 0;
        }
        for _ in 0..budget {
            if self.expiring.is_empty() {
                break;
            }
            if self.cursor >= self.expiring.len() {
                self.cursor = 0;
            }
            let loc = self.expiring[self.cursor];
            if self.now > self.keys.at(loc).expire_at() && self.reclaims(loc.table) {
                // The last key moves into this place: examine it next.
                let kv = self.remove_at(loc);
                self.touch_expired(kv.key());
                self.effect(|| Effect::Expired(kv.key().to_vec()));
                removed += 1;
            } else {
                self.cursor += 1;
            }
        }
        removed
    }

    /// Gives back the room of the expiry index once it's less than a tenth
    /// full (see the [module docs](self)). Call it from a timer. Returns
    /// whether it shrank anything.
    pub fn shrink(&mut self) -> bool {
        let sparse = |len: usize, capacity: usize| capacity > SHRINK_MIN && len * 10 < capacity;
        if sparse(self.expiring.len(), self.expiring.capacity()) {
            self.expiring.shrink_to(self.expiring.len() * 2);
            return true;
        }
        false
    }

    /// The most the next new key may allocate at once if it makes a table
    /// grow (its `Vec` or its index twice the size, while the old one is
    /// still there), and the expiry index with it. So that a node can
    /// refuse the write instead of jumping past its limit: Redis refuses to
    /// expand a dict past `maxmemory` for the same reason. Which table the
    /// key goes to isn't known, so the tables count as the largest one,
    /// full (see `Keys::largest`): a few kB, where one map of every key
    /// doubled by hundreds of MB.
    pub fn growth(&self) -> usize {
        let mut bytes = self.keys.tables[self.keys.largest].growth_bound();
        if !self.expiring.is_empty() && self.expiring.len() == self.expiring.capacity() {
            bytes += self.expiring.capacity() * 2 * mem::size_of::<Loc>();
        }
        bytes
    }

    /// How many keys have an expiry (expired ones not reclaimed included).
    pub fn expiring_len(&self) -> usize {
        self.expiring.len()
    }

    /// The block of `key` unless it has expired; an expired one is deleted.
    /// Every command looks its key up through here.
    fn live(&mut self, key: &[u8]) -> Option<&mut Kv> {
        let loc = self.keys.find(key)?;
        if self.now > self.keys.at(loc).expire_at() {
            if self.reclaims(loc.table) {
                self.remove_at(loc);
                self.touch_expired(key);
                self.effect(|| Effect::Expired(key.to_vec()));
            }
            return None;
        }
        Some(self.keys.at_mut(loc))
    }

    /// Adds a string key that doesn't exist (expired counts as existing:
    /// check with `live` first).
    fn insert(&mut self, key: &[u8], value: &[u8], at: Option<i64>) {
        self.insert_kv(key, at, |hash, ttl| Kv::new_str(hash, key, value, ttl));
    }

    /// [`insert`](Self::insert) of any value.
    fn insert_value(&mut self, key: &[u8], value: Value, at: Option<i64>) {
        self.insert_kv(key, at, |hash, ttl| Kv::new(hash, key, value, ttl));
    }

    /// Adds the block `make` makes from the key's hash and its expiry.
    fn insert_kv(&mut self, key: &[u8], at: Option<i64>, make: impl FnOnce(u32, Option<(i64, u32)>) -> Kv) {
        let (table, hash) = self.keys.locate(key);
        debug_assert!(self.keys.tables[table].find(hash, key).is_none(), "insert of an existing key");
        let ttl = at.map(|at| (at, self.next_ttl_slot()));
        let pos = self.keys.tables[table].push(make(hash, ttl));
        if ttl.is_some() {
            self.expiring.push(Loc { table: table as u32, pos: pos as u32 });
        }
        self.keys.len += 1;
        if self.keys.tables[table].len() > self.keys.tables[self.keys.largest].len() {
            self.keys.largest = table;
        }
    }

    /// Deletes `key`, expired or not, and its place in the expiry index.
    fn remove(&mut self, key: &[u8]) -> Option<Kv> {
        let loc = self.keys.find(key)?;
        Some(self.remove_at(loc))
    }

    /// [`remove`](Self::remove) of the key at `loc`. The last key of its
    /// table moves into its place, the last of the expiry index into its
    /// place there.
    fn remove_at(&mut self, loc: Loc) -> Kv {
        let table = &mut self.keys.tables[loc.table as usize];
        let kv = table.remove_swap_at(loc.pos as usize);
        if let Some(slot) = table.get(loc.pos as usize).and_then(Kv::ttl_slot) {
            self.expiring[slot as usize].pos = loc.pos;
        }
        if let Some(slot) = kv.ttl_slot() {
            self.unlink(slot);
        }
        self.keys.len -= 1;
        kv
    }

    /// Sets (`Some`) or clears (`None`) the expiry of an existing key.
    fn set_ttl(&mut self, key: &[u8], at: Option<i64>) {
        let loc = self.keys.find(key).expect("set_ttl of a missing key");
        let slot = self.next_ttl_slot();
        let kv = self.keys.at_mut(loc);
        match (at, kv.ttl_slot()) {
            (Some(at), Some(_)) => kv.set_expire_at(at),
            (Some(at), None) => {
                kv.set_ttl(Some((at, slot)));
                self.expiring.push(loc);
            }
            (None, Some(slot)) => {
                kv.set_ttl(None);
                self.unlink(slot);
            }
            (None, None) => {}
        }
    }

    /// A random number below `n` (> 0).
    fn random_below(&mut self, n: usize) -> usize {
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let r = self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d);
        ((u128::from(r) * n as u128) >> 64) as usize
    }

    /// The place in the expiry index of the next key to get a TTL.
    fn next_ttl_slot(&self) -> u32 {
        u32::try_from(self.expiring.len()).expect("too many keys with a TTL")
    }

    /// Removes `slot` from the expiry index; the last key moves into it.
    fn unlink(&mut self, slot: u32) {
        self.expiring.swap_remove(slot as usize);
        if let Some(&moved) = self.expiring.get(slot as usize) {
            self.keys.at_mut(moved).set_ttl_slot(slot);
        }
    }
}

mod hash;
mod set;
pub use set::SetOp;
mod list;
pub use list::{End, LposOptions};
mod dump;
mod scan;
mod zset;
pub use zset::{Aggregate, Combine, Scored, ZaddFlags, Zadded};
pub use dump::{check as check_dump, decode as decode_dump, Dump, Garbage, Kind, Part, Staged};
#[cfg(test)]
mod tests;
