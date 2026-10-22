//! The keyspace: keys to values, with Redis 7's string commands and expiry.
//!
//! One `Store` per core, owned by that core's event loop: no locks, every
//! method takes `&mut self`. The keys are `Rc<[u8]>`, shared between the map
//! and the expiry index, which makes the store `!Send`: create it on the
//! thread that uses it.
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
//! expiry also sits in a dense `Vec` (the entry knows its slot there; removal
//! is a `swap_remove` that fixes the slot of the key moved into the hole).
//! [`Store::expire_cycle`] walks that `Vec` round-robin from a cursor and
//! deletes the expired keys it meets, at most `budget` of them per call.
//! Compared with Redis's random sampling it's deterministic: every expiring
//! key is examined within `ceil(n / budget)` calls. Compared with a min-heap
//! by expiry time it never holds stale entries: refreshing a TTL (the
//! common case for sessions and caches) only writes the entry, with no heap
//! entry to leave behind or compact later, and keys without a TTL cost
//! nothing. The price is that the sweep also looks at keys that aren't due
//! yet; a sweep costs one map lookup per key examined.
//!
//! # Keys by slot
//!
//! In cluster mode ([`Store::with_slots`]) every key also sits in the list
//! of its slot, built like the expiry index: a dense `Vec` per slot, the
//! entry knows its place in it, removal is a `swap_remove`. So `CLUSTER
//! COUNTKEYSINSLOT` is O(1), and moving a slot to another node walks its
//! keys only. The store doesn't know how keys map to slots: the dbnode
//! passes the function (`cluster::key_hash_slot`). A key costs one more
//! `Rc` in a `Vec` (16 bytes, plus the `Vec`'s spare room), and the entry
//! keeps its place in a field that fills padding: an `Entry` stays 40 bytes.
//!
//! A standalone store keeps one such list of every key (phase 7), so that
//! `SCAN` can walk it: see [`Store::scan`].
//!
//! # Memory
//!
//! A `HashMap` or a `Vec` never gives back the room it grew, so after many
//! deletions (a slot moved away, keys expired) the store would keep its
//! largest size. [`Store::shrink`], called from a timer, shrinks the map and
//! the expiry index once they're less than a tenth full (as Redis resizes
//! its dicts in `serverCron`), to twice what they hold; a slot's list
//! shrinks as keys leave it, and goes once it's empty. Shrinking the map
//! rehashes what is left in one go, as growing it does.

use crate::collections::{Hash, List, Set};
use crate::int::{parse_i64, write_i64};
use crate::Error;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::mem;
use std::rc::Rc;

/// The `expire_at` of a key without expiry: never `< now`, so the expiry
/// check on every lookup needs no extra branch.
const NEVER: i64 = i64::MAX;
/// The `slot` of a key without expiry.
const NO_SLOT: u32 = u32::MAX;
/// Redis's default `proto-max-bulk-len`: the longest string `APPEND` makes.
pub const DEFAULT_MAX_STRING: usize = 512 * 1024 * 1024;
/// [`Store::shrink`] leaves tables holding fewer entries than this alone:
/// the room they keep is small, and shrinking them would come back soon.
const SHRINK_MIN: usize = 1024;

/// A value; commands meeting the wrong type fail with [`Error::WrongType`].
/// The collections are boxed so that a `Value` stays the size of a `Vec`
/// and an [`Entry`] 40 bytes.
#[derive(Debug)]
enum Value {
    /// Binary-safe. A `Vec` rather than a `Box<[u8]>` so `APPEND` and
    /// `INCR` grow and rewrite in place.
    Str(Vec<u8>),
    Hash(Box<Hash>),
    Set(Box<Set>),
    List(Box<List>),
}

impl Value {
    fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::List(_) => "list",
        }
    }

    /// `OBJECT ENCODING`: what Redis 7.0.15 would call it. A string is
    /// `int` if it reads as one, `embstr` up to 44 bytes, else `raw`
    /// (Redis also makes an `embstr` `raw` once it's modified in place, by
    /// `APPEND` or `SETRANGE`, which this doesn't track).
    fn encoding(&self) -> &'static str {
        match self {
            Value::Str(s) if s.len() <= 20 && parse_i64(s).is_some() => "int",
            Value::Str(s) if s.len() <= 44 => "embstr",
            Value::Str(_) => "raw",
            Value::Hash(h) => h.encoding(),
            Value::Set(s) => s.encoding(),
            Value::List(l) => l.encoding(),
        }
    }

    fn as_str(&self) -> Result<&Vec<u8>, Error> {
        match self {
            Value::Str(s) => Ok(s),
            _ => Err(Error::WrongType),
        }
    }

    fn as_str_mut(&mut self) -> Result<&mut Vec<u8>, Error> {
        match self {
            Value::Str(s) => Ok(s),
            _ => Err(Error::WrongType),
        }
    }

    /// Makes this the string `value`, reusing the buffer when it fits
    /// without wasting much: an overwrite then costs no allocation.
    fn assign(&mut self, value: &[u8]) {
        match self {
            Value::Str(buf) if buf.capacity() >= value.len() && buf.capacity() <= 2 * value.len() + 64 => {
                buf.clear();
                buf.extend_from_slice(value);
            }
            _ => *self = Value::Str(value.to_vec()),
        }
    }
}

#[derive(Debug)]
struct Entry {
    value: Value,
    /// Unix ms; [`NEVER`] without expiry.
    expire_at: i64,
    /// Index in `Store::expiring`; [`NO_SLOT`] without expiry.
    slot: u32,
    /// Index in its slot's list of [`Slots::keys`] (the one list of a
    /// standalone store).
    pos: u32,
}

/// The number of slots of a Redis Cluster.
pub const SLOTS: usize = 16384;

/// The keys of every slot; see the module docs. Without `slot_of` (a
/// standalone node) one list holds every key, for `SCAN`, built by the
/// first `SCAN` (until then the store pays nothing for it).
struct Slots {
    /// A key's slot, below [`SLOTS`].
    slot_of: Option<fn(&[u8]) -> u16>,
    keys: Vec<Vec<Rc<[u8]>>>,
    /// Whether the lists are kept: from the start by slot, from the first
    /// `SCAN` standalone.
    active: bool,
}

impl Slots {
    fn new(slot_of: Option<fn(&[u8]) -> u16>) -> Slots {
        let lists = if slot_of.is_some() { SLOTS } else { 1 };
        Slots { slot_of, keys: (0..lists).map(|_| Vec::new()).collect(), active: slot_of.is_some() }
    }

    /// The list `key` belongs in.
    fn list_of(&self, key: &[u8]) -> usize {
        self.slot_of.map_or(0, |slot_of| usize::from(slot_of(key)))
    }
}

impl std::fmt::Debug for Slots {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let keys: usize = self.keys.iter().map(Vec::len).sum();
        f.debug_struct("Slots").field("keys", &keys).finish()
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
///
/// The map uses std's default hasher (SipHash, randomly keyed per process):
/// slower than a plain hash but, like Redis's keyed SipHash, it keeps
/// clients from choosing keys that collide. Revisit when profiling says so.
#[derive(Debug)]
pub struct Store {
    map: HashMap<Rc<[u8]>, Entry>,
    /// Every key with an expiry, in no order; see the module docs.
    expiring: Vec<Rc<[u8]>>,
    /// Where `expire_cycle` continues.
    cursor: usize,
    /// Unix ms.
    now: i64,
    /// The longest string `APPEND` makes.
    max_string: usize,
    /// The keys by slot, in cluster mode.
    slots: Slots,
    /// The map's largest `capacity` seen by [`Store::shrink`] since it last
    /// shrank: deletions leave tombstones, which `capacity` subtracts, but
    /// the table keeps its size.
    map_room: usize,
    /// For `SPOP`, `SRANDMEMBER`: xorshift64*, seeded at random.
    rng: u64,
}

impl Default for Store {
    fn default() -> Store {
        // A randomly keyed SipHash of anything is a random seed; `| 1` as
        // xorshift's state must not be 0.
        let rng = std::collections::hash_map::RandomState::new().hash_one(0u8) | 1;
        Store {
            map: HashMap::new(),
            expiring: Vec::new(),
            cursor: 0,
            now: 0,
            max_string: DEFAULT_MAX_STRING,
            slots: Slots::new(None),
            map_room: 0,
            rng,
        }
    }
}

impl Store {
    pub fn new() -> Store {
        Store::default()
    }

    /// A store that also lists its keys by slot (see the module docs), for
    /// cluster mode. `slot_of` gives a key's slot, below [`SLOTS`].
    pub fn with_slots(slot_of: fn(&[u8]) -> u16) -> Store {
        Store { slots: Slots::new(Some(slot_of)), ..Store::default() }
    }

    /// Sets the current time, unix ms. Expiry is judged against it.
    pub fn set_now(&mut self, now_ms: i64) {
        self.now = now_ms;
    }

    pub fn now(&self) -> i64 {
        self.now
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
            Some(e) => Ok(Some(e.value.as_str()?)),
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
        keys.iter().map(|key| self.map.get(*key).and_then(|e| e.value.as_str().ok()).map(|s| &s[..])).collect()
    }

    /// `SET` with its options; see [`SetReply`] for what it returns.
    pub fn set(&mut self, key: &[u8], value: &[u8], opts: SetOptions) -> Result<SetReply, Error> {
        // As in Redis: a bad expiry first, then the type check of GET, then
        // the condition.
        let at = self.expire_at_for_set(opts.expiry)?;
        let now = self.now;
        let entry = self.live(key);
        if opts.get {
            if let Some(e) = &entry {
                e.value.as_str()?;
            }
        }
        let blocked = match opts.cond {
            SetCond::Always => false,
            SetCond::Nx => entry.is_some(),
            SetCond::Xx => entry.is_none(),
        };
        if blocked {
            return Ok(match entry {
                _ if !opts.get => SetReply::NotSet,
                Some(e) => SetReply::Old(Some(e.value.as_str()?.clone())),
                None => SetReply::Old(None),
            });
        }
        let old = match entry {
            None => {
                // An expiry in the past (PXAT 1) still replies OK, but leaves
                // no key behind.
                if at.is_none_or(|at| at > now) {
                    self.insert(key, value.to_vec(), at);
                }
                None
            }
            Some(e) => {
                let old = if opts.get {
                    match mem::replace(&mut e.value, Value::Str(value.to_vec())) {
                        Value::Str(old) => Some(old),
                        _ => unreachable!("type checked above"),
                    }
                } else {
                    e.value.assign(value);
                    None
                };
                let has_ttl = e.slot != NO_SLOT;
                match (opts.expiry, at) {
                    (SetExpiry::KeepTtl, _) => {}
                    (_, None) if has_ttl => self.set_ttl(key, None),
                    (_, None) => {}
                    (_, Some(at)) if at <= now => {
                        self.remove(key);
                    }
                    (_, Some(at)) if has_ttl => e.expire_at = at,
                    (_, Some(at)) => self.set_ttl(key, Some(at)),
                }
                old
            }
        };
        Ok(if opts.get { SetReply::Old(old) } else { SetReply::Ok })
    }

    /// The absolute expiry time `SET` asks for, checked like Redis does: the
    /// number must be positive and the time must fit an `i64`.
    fn expire_at_for_set(&self, expiry: SetExpiry) -> Result<Option<i64>, Error> {
        let (n, unit, base) = match expiry {
            SetExpiry::Clear | SetExpiry::KeepTtl => return Ok(None),
            SetExpiry::Ex(n) => (n, 1000, self.now),
            SetExpiry::Px(n) => (n, 1, self.now),
            SetExpiry::ExAt(n) => (n, 1000, 0),
            SetExpiry::PxAt(n) => (n, 1, 0),
        };
        let at = n.checked_mul(unit).and_then(|ms| ms.checked_add(base)).filter(|_| n > 0);
        at.map(Some).ok_or(Error::InvalidExpireTime("set"))
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
        self.live(key).is_some() && self.remove(key).is_some()
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
        self.map.get(key).is_some_and(|e| self.now <= e.expire_at)
    }

    /// A live key's value and the milliseconds it has left (`None` without
    /// expiry; at least 1, a key at its last millisecond counts as 1), for
    /// copying it to another node. Like [`contains`](Self::contains), an
    /// expired key is `None` and isn't reclaimed.
    pub fn peek(&self, key: &[u8]) -> Option<(&[u8], Option<i64>)> {
        let e = self.map.get(key).filter(|e| self.now <= e.expire_at)?;
        let ttl = (e.slot != NO_SLOT).then(|| (e.expire_at - self.now).max(1));
        // Only strings exist outside the tests.
        Some((e.value.as_str().ok()?, ttl))
    }

    /// `GETDEL`: the value, now deleted. A value of another type stays.
    pub fn getdel(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.live(key) {
            None => return Ok(None),
            Some(e) => {
                e.value.as_str()?;
            }
        }
        match self.remove(key).map(|e| e.value) {
            Some(Value::Str(s)) => Ok(Some(s)),
            _ => unreachable!("checked above"),
        }
    }

    /// `APPEND`: the new length. Creates the key if missing; keeps the TTL.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<usize, Error> {
        let max = self.max_string;
        match self.live(key) {
            Some(e) => {
                let s = e.value.as_str_mut()?;
                if s.len() + value.len() > max {
                    return Err(Error::StringTooLong);
                }
                s.extend_from_slice(value);
                Ok(s.len())
            }
            None => {
                self.insert(key, value.to_vec(), None);
                Ok(value.len())
            }
        }
    }

    /// `STRLEN`: 0 for a missing key.
    pub fn strlen(&mut self, key: &[u8]) -> Result<usize, Error> {
        match self.live(key) {
            Some(e) => Ok(e.value.as_str()?.len()),
            None => Ok(0),
        }
    }

    /// `INCRBY` (and `INCR` with 1): the new value. A missing key counts as
    /// 0; the TTL is kept.
    pub fn incr_by(&mut self, key: &[u8], by: i64) -> Result<i64, Error> {
        match self.live(key) {
            Some(e) => {
                let s = e.value.as_str_mut()?;
                let old = parse_i64(s).ok_or(Error::NotInteger)?;
                let new = old.checked_add(by).ok_or(Error::Overflow)?;
                write_i64(s, new);
                Ok(new)
            }
            None => {
                let mut s = Vec::with_capacity(20);
                write_i64(&mut s, by);
                self.insert(key, s, None);
                Ok(by)
            }
        }
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
        let Some(e) = self.live(key) else { return Ok(false) };
        let current = (e.slot != NO_SLOT).then_some(e.expire_at);
        let refused = (flags.nx && current.is_some())
            || (flags.xx && current.is_none())
            || (flags.gt && current.is_none_or(|cur| at <= cur))
            || (flags.lt && current.is_some_and(|cur| at >= cur));
        if refused {
            return Ok(false);
        }
        if at <= now {
            self.remove(key);
        } else {
            self.set_ttl(key, Some(at));
        }
        Ok(true)
    }

    /// `PTTL`: ms left, -1 without expiry, -2 if the key doesn't exist.
    pub fn pttl(&mut self, key: &[u8]) -> i64 {
        let now = self.now;
        match self.live(key) {
            None => -2,
            Some(e) if e.slot == NO_SLOT => -1,
            // `live` leaves only keys with now <= expire_at: never negative.
            Some(e) => e.expire_at.saturating_sub(now),
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
            Some(e) if e.slot != NO_SLOT => {
                self.set_ttl(key, None);
                true
            }
            _ => false,
        }
    }

    /// `TYPE`: `"string"`, `"hash"`, `"set"`, `"list"`, or `"none"` for a
    /// missing key.
    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        self.live(key).map_or("none", |e| e.value.type_name())
    }

    /// `OBJECT ENCODING`: `None` for a missing key.
    pub fn encoding(&mut self, key: &[u8]) -> Option<&'static str> {
        self.live(key).map(|e| e.value.encoding())
    }

    /// `DBSIZE`. Like Redis, counts expired keys not reclaimed yet.
    pub fn dbsize(&self) -> usize {
        self.map.len()
    }

    /// Every key, in no particular order; like [`dbsize`](Self::dbsize),
    /// expired keys not reclaimed yet included.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.map.keys().map(|k| &**k)
    }

    /// Whether the keys are listed by slot ([`Store::with_slots`]).
    pub fn has_slots(&self) -> bool {
        self.slots.slot_of.is_some()
    }

    /// `CLUSTER COUNTKEYSINSLOT`: like [`dbsize`](Self::dbsize), expired
    /// keys not reclaimed yet included. Panics without the slot index, or if
    /// `slot` isn't below [`SLOTS`].
    pub fn count_in_slot(&self, slot: u16) -> usize {
        assert!(self.has_slots(), "a store without the slot index");
        self.slots.keys[usize::from(slot)].len()
    }

    /// The keys of `slot`, in no particular order, expired ones not
    /// reclaimed yet included (`CLUSTER GETKEYSINSLOT`). Panics as
    /// [`count_in_slot`](Self::count_in_slot) does.
    pub fn keys_in_slot(&self, slot: u16) -> impl Iterator<Item = &[u8]> {
        assert!(self.has_slots(), "a store without the slot index");
        self.slots.keys[usize::from(slot)].iter().map(|k| &**k)
    }

    /// `FLUSHDB` / `FLUSHALL`. Frees the memory too.
    pub fn flush(&mut self) {
        self.map = HashMap::new();
        self.map_room = 0;
        self.expiring = Vec::new();
        self.cursor = 0;
        self.slots = Slots::new(self.slots.slot_of);
    }

    /// Active expiry: examines up to `budget` keys with an expiry, continuing
    /// where the last call stopped, and deletes the expired ones. Returns how
    /// many it deleted. Call it from a timer; if it deleted a good part of
    /// `budget`, more are probably due and calling again soon pays off.
    pub fn expire_cycle(&mut self, budget: usize) -> usize {
        let mut removed = 0;
        for _ in 0..budget {
            if self.expiring.is_empty() {
                break;
            }
            if self.cursor >= self.expiring.len() {
                self.cursor = 0;
            }
            let key = &self.expiring[self.cursor];
            if self.now > self.map[&**key].expire_at {
                // The last key moves into this slot: examine it next.
                let key = Rc::clone(key);
                self.remove(&key);
                removed += 1;
            } else {
                self.cursor += 1;
            }
        }
        removed
    }

    /// Gives back the room of the map and the expiry index once they're
    /// less than a tenth full (see the [module docs](self)). Call it from a
    /// timer. Returns whether it shrank anything.
    pub fn shrink(&mut self) -> bool {
        let sparse = |len: usize, capacity: usize| capacity > SHRINK_MIN && len * 10 < capacity;
        let mut shrank = false;
        self.map_room = self.map_room.max(self.map.capacity());
        if sparse(self.map.len(), self.map_room) {
            self.map.shrink_to(self.map.len() * 2);
            self.map_room = self.map.capacity();
            shrank = true;
        }
        if sparse(self.expiring.len(), self.expiring.capacity()) {
            self.expiring.shrink_to(self.expiring.len() * 2);
            shrank = true;
        }
        shrank
    }

    /// The bytes the next new key would allocate at once if it makes the
    /// map grow (a table twice the size, while the old one is still
    /// there), and the expiry index with it; 0 if it fits. So that a node
    /// can refuse the write instead of jumping past its limit: Redis
    /// refuses to expand a dict past `maxmemory` for the same reason. An
    /// estimate: std doesn't tell the table's size, and a map full of
    /// tombstones is cleaned in place rather than grown.
    pub fn growth(&self) -> usize {
        let mut bytes = 0;
        let (len, capacity) = (self.map.len(), self.map.capacity());
        let room = self.map_room.max(capacity);
        if len >= capacity && (len + 1) * 2 > room {
            // hashbrown: buckets a power of two, filled to 7/8; a control
            // byte per bucket.
            let buckets = ((len.max(room) + 1) * 8 / 7).next_power_of_two();
            bytes += buckets * (mem::size_of::<(Rc<[u8]>, Entry)>() + 1);
        }
        if !self.expiring.is_empty() && self.expiring.len() == self.expiring.capacity() {
            bytes += self.expiring.capacity() * 2 * mem::size_of::<Rc<[u8]>>();
        }
        bytes
    }

    /// How many keys have an expiry (expired ones not reclaimed included).
    pub fn expiring_len(&self) -> usize {
        self.expiring.len()
    }

    /// The entry of `key` unless it has expired; an expired one is deleted.
    /// Every command looks its key up through here.
    fn live(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let now = self.now;
        let entry: *mut Entry = self.map.get_mut(key)?;
        // SAFETY: `entry` comes from a `&mut` into the map, which isn't
        // touched again before the pointer becomes a `&mut` with the same
        // lifetime, except on the path that removes the entry, which doesn't
        // use the pointer after that. Safe Rust can't say this yet (returning
        // the borrow on one path and mutating the map on the other is NLL
        // "problem case #3"); a second lookup would cost a hash per GET.
        unsafe {
            if now > (*entry).expire_at {
                self.remove(key);
                return None;
            }
            Some(&mut *entry)
        }
    }

    /// Adds a key that doesn't exist (expired counts as existing: check
    /// with `live` first).
    fn insert(&mut self, key: &[u8], value: Vec<u8>, at: Option<i64>) {
        self.insert_value(key, Value::Str(value), at);
    }

    /// [`insert`](Self::insert) of any value.
    fn insert_value(&mut self, key: &[u8], value: Value, at: Option<i64>) {
        let key: Rc<[u8]> = Rc::from(key);
        let (expire_at, slot) = match at {
            Some(at) => (at, self.push_expiring(Rc::clone(&key))),
            None => (NEVER, NO_SLOT),
        };
        let pos = if self.slots.active {
            let i = self.slots.list_of(&key);
            let list = &mut self.slots.keys[i];
            list.push(Rc::clone(&key));
            (list.len() - 1) as u32
        } else {
            0
        };
        let old = self.map.insert(key, Entry { value, expire_at, slot, pos });
        debug_assert!(old.is_none(), "insert of an existing key");
    }

    /// Deletes `key`, expired or not, and its places in the expiry and slot
    /// indexes.
    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.map.remove(key)?;
        if entry.slot != NO_SLOT {
            self.unlink(entry.slot);
        }
        if !self.slots.active {
            return Some(entry);
        }
        let i = self.slots.list_of(key);
        let list = &mut self.slots.keys[i];
        list.swap_remove(entry.pos as usize);
        if let Some(moved) = list.get(entry.pos as usize) {
            self.map.get_mut(&**moved).expect("slot index out of sync").pos = entry.pos;
        }
        // Room given back as the slot empties (it moved away): halved at a
        // quarter full, freed when empty.
        if list.is_empty() {
            *list = Vec::new();
        } else if list.len() * 4 <= list.capacity() && list.capacity() > 16 {
            list.shrink_to(list.len() * 2);
        }
        Some(entry)
    }

    /// Builds the key list of a standalone store if it has none yet (see
    /// [`Slots`]): one pass over the map.
    fn index_keys(&mut self) {
        if self.slots.active {
            return;
        }
        let list = &mut self.slots.keys[0];
        list.reserve_exact(self.map.len());
        for (key, e) in self.map.iter_mut() {
            e.pos = list.len() as u32;
            list.push(Rc::clone(key));
        }
        self.slots.active = true;
    }

    /// Sets (`Some`) or clears (`None`) the expiry of an existing key.
    fn set_ttl(&mut self, key: &[u8], at: Option<i64>) {
        let e = self.map.get_mut(key).expect("set_ttl of a missing key");
        match at {
            Some(at) if e.slot != NO_SLOT => e.expire_at = at,
            Some(at) => {
                // The index shares the map's key; std has no lookup that
                // hands out the key and a `&mut` to the value at once.
                let shared = Rc::clone(self.map.get_key_value(key).unwrap().0);
                let slot = self.push_expiring(shared);
                let e = self.map.get_mut(key).unwrap();
                e.expire_at = at;
                e.slot = slot;
            }
            None if e.slot != NO_SLOT => {
                let slot = mem::replace(&mut e.slot, NO_SLOT);
                e.expire_at = NEVER;
                self.unlink(slot);
            }
            None => {}
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

    fn push_expiring(&mut self, key: Rc<[u8]>) -> u32 {
        let slot = u32::try_from(self.expiring.len()).ok().filter(|&s| s != NO_SLOT).expect("too many keys with a TTL");
        self.expiring.push(key);
        slot
    }

    /// Removes `slot` from the expiry index; the last key moves into it.
    fn unlink(&mut self, slot: u32) {
        self.expiring.swap_remove(slot as usize);
        if let Some(moved) = self.expiring.get(slot as usize) {
            self.map.get_mut(&**moved).expect("expiry index out of sync").slot = slot;
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
pub use dump::{check as check_dump, decode as decode_dump, Dump, Garbage, Kind, Part, Staged};
#[cfg(test)]
mod tests;
