//! One heap block per key of the keyspace: its hash, its expiry if it has
//! one, the key, and the value, found through a single 8-byte pointer.
//!
//! A key used to cost a `HashMap` bucket (the key's `Rc` and a 40-byte
//! entry), the `Rc`'s allocation, the value's `Vec` and its allocation, and
//! the `Rc` again in the expiry and slot indexes. Here it's one block, in a
//! table of its slot ([`Indexed`](crate::table::Indexed)), as Redis 8 keeps
//! a key and its value in one `kvobj`:
//!
//! ```text
//! ┌───────┬──────────┬─────────────────────┬──────┬──────┬─────┬──────────────┐
//! │ flags │ hash u32 │ expire_at i64, slot │ klen │ vlen │ key │ value        │
//! │       │          │ u32 (with a TTL)    │      │      │     │ or a pointer │
//! └───────┴──────────┴─────────────────────┴──────┴──────┴─────┴──────────────┘
//! ```
//!
//! The lengths are LEB128 (1 byte each up to 127). A string up to
//! [`EMBED_MAX`] bytes sits in the block (`embstr`, or `int` if it reads as
//! one); a longer one, or one `APPEND` grew, sits in a `Vec` of its own,
//! with room to grow (`raw`), and the block holds a pointer to it, as it
//! does to a hash, a set, a list or a sorted set; `vlen` is then left out. The expiry
//! (absolute unix ms, and the key's place in the store's expiry index) is
//! there only for a key with a TTL: setting the first one, or removing it,
//! makes a new block.
//!
//! A value of a new length makes a new block as well (the old one is
//! freed): it's one allocation, as the value's own `Vec` was, and an
//! overwrite of the same length (a counter, a fixed-size record) is written
//! in place. The block's size is read back from its header, which makes
//! the pointer thin; that needs `unsafe` to allocate and free, all in this
//! module, as in [`block`](crate::block).

use crate::collections::{Hash, List, Set};
use crate::zset::ZSet;
use crate::int::parse_i64;
use crate::table::Hashed;
use crate::Error;
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::fmt;
use std::mem::{self, ManuallyDrop};
use std::ptr::{self, NonNull};

/// The `expire_at` of a key without expiry: never `< now`, so the expiry
/// check on every lookup needs no extra branch.
pub(crate) const NEVER: i64 = i64::MAX;
/// The longest string kept in the block. Longer ones keep their `Vec`:
/// moving a large value in (a `RESTORE`, a replica's copy) costs no copy,
/// and an overwrite of another length doesn't copy the key along.
pub(crate) const EMBED_MAX: usize = 4096;
/// Redis 8's `OBJECT ENCODING` of a string: `embstr` while the key and the
/// value together take at most this many bytes. Redis 8 embeds a short
/// value with its key in one 64-byte cache line: a 16-byte header, the key
/// with 3 bytes of its own and the value with 4 (`kvobjSet`).
const EMBSTR_KEY_AND_VALUE: usize = 64 - 16 - 3 - 4;

/// The kind of value, in the low bits of the flags.
const KIND: u8 = 0b111;
const STR: u8 = 0;
const HASH: u8 = 1;
const SET: u8 = 2;
const LIST: u8 = 3;
const ZSET: u8 = 4;
/// A string in a `Vec` of its own: the block holds a pointer.
const RAW: u8 = 1 << 3;
/// The expiry is in the header.
const TTL: u8 = 1 << 4;
/// A string `INCRBYFLOAT` wrote: never `int` to `OBJECT ENCODING`, even if
/// it reads as one, as in Redis. A string written otherwise loses it.
const FLOAT: u8 = 1 << 5;

const HASH_AT: usize = 1;
const EXPIRE_AT: usize = 5;
const TTL_SLOT_AT: usize = 13;
const TTL_SIZE: usize = 12;
const PTR: usize = mem::size_of::<usize>();

/// A value outside the store: what goes into a [`Kv`] and comes out of it.
/// The collections are boxed, as the block holds a pointer to them.
#[derive(Debug)]
pub(crate) enum Value {
    Str(Vec<u8>),
    Hash(Box<Hash>),
    Set(Box<Set>),
    List(Box<List>),
    ZSet(Box<ZSet>),
}

/// A value in the store, borrowed.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Val<'a> {
    Str(&'a [u8]),
    Hash(&'a Hash),
    Set(&'a Set),
    List(&'a List),
    ZSet(&'a ZSet),
}

/// What a new block holds after its key.
enum Body<'a> {
    /// A string, in the block.
    Inline(&'a [u8]),
    /// A pointer: to a `Vec<u8>` (with [`RAW`]) or to a collection.
    Ptr(usize),
}

/// An owned block: allocated by [`Kv::new`], freed on drop with its value.
pub(crate) struct Kv(NonNull<u8>);

fn len_size(mut n: usize) -> usize {
    let mut size = 1;
    while n >= 0x80 {
        n >>= 7;
        size += 1;
    }
    size
}

fn layout(size: usize) -> Layout {
    Layout::from_size_align(size, 1).expect("a block smaller than isize::MAX")
}

/// Writes `bytes` at `*at` of the block at `ptr`, and moves `*at` past them.
///
/// # Safety
///
/// The block must have room for them.
unsafe fn put(ptr: *mut u8, at: &mut usize, bytes: &[u8]) {
    unsafe { ptr.add(*at).copy_from_nonoverlapping(bytes.as_ptr(), bytes.len()) };
    *at += bytes.len();
}

/// Writes `n` as LEB128 at `*at`; see [`put`].
unsafe fn put_len(ptr: *mut u8, at: &mut usize, mut n: usize) {
    let mut bytes = [0u8; 10];
    let mut i = 0;
    while n >= 0x80 {
        bytes[i] = (n as u8) | 0x80;
        n >>= 7;
        i += 1;
    }
    bytes[i] = n as u8;
    unsafe { put(ptr, at, &bytes[..=i]) }
}

impl Kv {
    /// A block for `key`, whose hash is `hash`, holding `value`, with an
    /// expiry (`expire_at`, and the key's place in the expiry index) if
    /// `ttl` says so.
    pub fn new(hash: u32, key: &[u8], value: Value, ttl: Option<(i64, u32)>) -> Kv {
        let (kind, body) = match value {
            Value::Str(s) if s.len() <= EMBED_MAX => return Kv::build(STR, hash, ttl, key, Body::Inline(&s)),
            Value::Str(s) => (STR | RAW, Box::into_raw(Box::new(s)) as usize),
            Value::Hash(h) => (HASH, Box::into_raw(h) as usize),
            Value::Set(s) => (SET, Box::into_raw(s) as usize),
            Value::List(l) => (LIST, Box::into_raw(l) as usize),
            Value::ZSet(z) => (ZSET, Box::into_raw(z) as usize),
        };
        Kv::build(kind, hash, ttl, key, Body::Ptr(body))
    }

    /// [`new`](Self::new) of a string, copied in.
    pub fn new_str(hash: u32, key: &[u8], value: &[u8], ttl: Option<(i64, u32)>) -> Kv {
        if value.len() <= EMBED_MAX {
            Kv::build(STR, hash, ttl, key, Body::Inline(value))
        } else {
            Kv::new(hash, key, Value::Str(value.to_vec()), ttl)
        }
    }

    /// A block holding `body`, which it then owns. `kind` is the flags
    /// without [`TTL`].
    fn build(kind: u8, hash: u32, ttl: Option<(i64, u32)>, key: &[u8], body: Body) -> Kv {
        let flags = kind | if ttl.is_some() { TTL } else { 0 };
        let value_size = match body {
            Body::Inline(s) => len_size(s.len()) + s.len(),
            Body::Ptr(_) => PTR,
        };
        let ttl_size = if ttl.is_some() { TTL_SIZE } else { 0 };
        let size = 1 + 4 + ttl_size + len_size(key.len()) + key.len() + value_size;
        // SAFETY: `size` is at least 7, so the layout isn't zero-sized; the
        // writes below fill exactly the `size` bytes allocated, in the order
        // `header` reads them back.
        unsafe {
            let ptr = alloc(layout(size));
            if ptr.is_null() {
                handle_alloc_error(layout(size));
            }
            let mut at = 0;
            put(ptr, &mut at, &[flags]);
            put(ptr, &mut at, &hash.to_ne_bytes());
            if let Some((expire_at, slot)) = ttl {
                put(ptr, &mut at, &expire_at.to_ne_bytes());
                put(ptr, &mut at, &slot.to_ne_bytes());
            }
            put_len(ptr, &mut at, key.len());
            if let Body::Inline(s) = body {
                put_len(ptr, &mut at, s.len());
            }
            put(ptr, &mut at, key);
            match body {
                Body::Inline(s) => put(ptr, &mut at, s),
                Body::Ptr(p) => put(ptr, &mut at, &p.to_ne_bytes()),
            }
            debug_assert_eq!(at, size);
            Kv(NonNull::new_unchecked(ptr))
        }
    }

    fn flags(&self) -> u8 {
        // SAFETY: the first byte of every block.
        unsafe { *self.0.as_ptr() }
    }

    fn kind(&self) -> u8 {
        self.flags() & KIND
    }

    /// Whether the value is behind a pointer.
    fn boxed(&self) -> bool {
        self.flags() & (KIND | RAW) != STR
    }

    /// The size of the fixed part of the header: the flags, the hash, and
    /// the expiry if there's one.
    fn fixed_size(&self) -> usize {
        1 + 4 + if self.flags() & TTL != 0 { TTL_SIZE } else { 0 }
    }

    /// Where the key starts, the key's length, and the value's (the
    /// pointer's when boxed). Every lookup reads it once per key compared:
    /// the lengths below 128 take one byte, and one branch.
    #[inline]
    fn header(&self) -> (usize, usize, usize) {
        let mut at = self.fixed_size();
        let mut read = || {
            let mut len = 0;
            let mut shift = 0;
            loop {
                // SAFETY: the header was written whole by `build`, and these
                // reads stop at its last length byte (one without 0x80).
                let b = unsafe { *self.0.as_ptr().add(at) };
                at += 1;
                len |= ((b & 0x7f) as usize) << shift;
                if b < 0x80 {
                    return len;
                }
                shift += 7;
            }
        };
        let k = read();
        let v = if self.boxed() { PTR } else { read() };
        (at, k, v)
    }

    /// Its size, the header included.
    fn size(&self) -> usize {
        let (at, k, v) = self.header();
        at + k + v
    }

    /// The key and the bytes after it (the value or the pointer).
    #[inline]
    fn parts(&self) -> (&[u8], &[u8]) {
        let (at, k, v) = self.header();
        // SAFETY: `build` wrote the key at `at..at + k` and what follows it at
        // `at + k..at + k + v`, which live as long as `self`.
        unsafe {
            let ptr = self.0.as_ptr().add(at);
            (std::slice::from_raw_parts(ptr, k), std::slice::from_raw_parts(ptr.add(k), v))
        }
    }

    /// The pointer to the value, of a boxed one.
    fn ptr(&self) -> *mut u8 {
        debug_assert!(self.boxed());
        let p = self.parts().1;
        usize::from_ne_bytes(p.try_into().expect("a pointer")) as *mut u8
    }

    /// Reads `N` bytes of the fixed header at `at`.
    #[inline]
    fn read<const N: usize>(&self, at: usize) -> [u8; N] {
        assert!(at + N <= self.fixed_size(), "in the fixed header");
        // SAFETY: inside the fixed header, checked above, which `build`
        // wrote whole; unaligned, as the block has an alignment of 1.
        unsafe { self.0.as_ptr().add(at).cast::<[u8; N]>().read_unaligned() }
    }

    /// Writes `bytes` over the block at `at`.
    fn write(&mut self, at: usize, bytes: &[u8]) {
        assert!(at + bytes.len() <= self.size());
        // SAFETY: inside the block, checked above; `&mut self` means nothing
        // else reads it.
        unsafe { self.0.as_ptr().add(at).copy_from_nonoverlapping(bytes.as_ptr(), bytes.len()) }
    }

    pub fn key(&self) -> &[u8] {
        self.parts().0
    }

    pub fn has_ttl(&self) -> bool {
        self.flags() & TTL != 0
    }

    /// Unix ms; [`NEVER`] without expiry.
    pub fn expire_at(&self) -> i64 {
        if self.has_ttl() {
            i64::from_ne_bytes(self.read(EXPIRE_AT))
        } else {
            NEVER
        }
    }

    /// The key's place in the expiry index, with a TTL.
    pub fn ttl_slot(&self) -> Option<u32> {
        self.has_ttl().then(|| u32::from_ne_bytes(self.read(TTL_SLOT_AT)))
    }

    /// Moves the expiry of a key that has one.
    pub fn set_expire_at(&mut self, at: i64) {
        assert!(self.has_ttl(), "set_expire_at without a TTL");
        self.write(EXPIRE_AT, &at.to_ne_bytes());
    }

    /// Moves a key with a TTL to place `slot` in the expiry index.
    pub fn set_ttl_slot(&mut self, slot: u32) {
        assert!(self.has_ttl(), "set_ttl_slot without a TTL");
        self.write(TTL_SLOT_AT, &slot.to_ne_bytes());
    }

    /// Gives the key an expiry (and its place in the expiry index), or
    /// takes it away: a new block, the value moves over.
    pub fn set_ttl(&mut self, ttl: Option<(i64, u32)>) {
        let kind = self.flags() & !TTL;
        let (key, rest) = self.parts();
        let body = if self.boxed() { Body::Ptr(self.ptr() as usize) } else { Body::Inline(rest) };
        let new = Kv::build(kind, self.hash(), ttl, key, body);
        // The value is the new block's now: free the old block only.
        mem::replace(self, new).free_block();
    }

    /// Frees the block, not the value it points to.
    fn free_block(self) {
        let this = ManuallyDrop::new(self);
        let size = this.size();
        // SAFETY: allocated by `build` with this same layout; `this` isn't
        // dropped, so it's freed only here.
        unsafe { dealloc(this.0.as_ptr(), layout(size)) }
    }

    pub fn value(&self) -> Val<'_> {
        let p = if self.boxed() { self.ptr() } else { ptr::null_mut() };
        // SAFETY: a boxed value's pointer came from `Box::into_raw` of the
        // type its kind says, and the block owns it while it lives.
        unsafe {
            match self.kind() {
                STR if p.is_null() => Val::Str(self.parts().1),
                STR => Val::Str(&*p.cast::<Vec<u8>>()),
                HASH => Val::Hash(&*p.cast::<Hash>()),
                SET => Val::Set(&*p.cast::<Set>()),
                LIST => Val::List(&*p.cast::<List>()),
                _ => Val::ZSet(&*p.cast::<ZSet>()),
            }
        }
    }

    /// The pointer to a boxed value of `kind`, or a wrong-type error.
    fn ptr_of(&mut self, kind: u8) -> Result<*mut u8, Error> {
        if self.kind() == kind {
            Ok(self.ptr())
        } else {
            Err(Error::WrongType)
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self.kind() {
            STR => "string",
            HASH => "hash",
            SET => "set",
            LIST => "list",
            _ => "zset",
        }
    }

    /// `OBJECT ENCODING`, as Redis 8.10 would say it: a string is `int` if
    /// it reads as one (not after `INCRBYFLOAT`), `embstr` while it fits a
    /// cache line with its key ([`EMBSTR_KEY_AND_VALUE`]), `raw` past that or
    /// once `APPEND` grew it (Redis makes it `raw` too, whatever it holds).
    pub fn encoding(&self) -> &'static str {
        match self.value() {
            Val::Str(_) if self.flags() & RAW != 0 => "raw",
            Val::Str(s) if self.flags() & FLOAT == 0 && s.len() <= 20 && parse_i64(s).is_some() => "int",
            Val::Str(s) if self.key().len() + s.len() <= EMBSTR_KEY_AND_VALUE => "embstr",
            Val::Str(_) => "raw",
            Val::Hash(h) => h.encoding(),
            Val::Set(s) => s.encoding(),
            Val::List(l) => l.encoding(),
            Val::ZSet(z) => z.encoding(),
        }
    }

    pub fn as_str(&self) -> Result<&[u8], Error> {
        match self.value() {
            Val::Str(s) => Ok(s),
            _ => Err(Error::WrongType),
        }
    }

    pub fn as_hash(&self) -> Result<&Hash, Error> {
        match self.value() {
            Val::Hash(h) => Ok(h),
            _ => Err(Error::WrongType),
        }
    }

    pub fn as_set(&self) -> Result<&Set, Error> {
        match self.value() {
            Val::Set(s) => Ok(s),
            _ => Err(Error::WrongType),
        }
    }

    pub fn as_list(&self) -> Result<&List, Error> {
        match self.value() {
            Val::List(l) => Ok(l),
            _ => Err(Error::WrongType),
        }
    }

    pub fn as_zset(&self) -> Result<&ZSet, Error> {
        match self.value() {
            Val::ZSet(z) => Ok(z),
            _ => Err(Error::WrongType),
        }
    }

    pub fn as_hash_mut(&mut self) -> Result<&mut Hash, Error> {
        // SAFETY: as in `value`; `&mut self` makes the borrow unique.
        self.ptr_of(HASH).map(|p| unsafe { &mut *p.cast::<Hash>() })
    }

    pub fn as_set_mut(&mut self) -> Result<&mut Set, Error> {
        // SAFETY: as in `as_hash_mut`.
        self.ptr_of(SET).map(|p| unsafe { &mut *p.cast::<Set>() })
    }

    pub fn as_list_mut(&mut self) -> Result<&mut List, Error> {
        // SAFETY: as in `as_hash_mut`.
        self.ptr_of(LIST).map(|p| unsafe { &mut *p.cast::<List>() })
    }

    pub fn as_zset_mut(&mut self) -> Result<&mut ZSet, Error> {
        // SAFETY: as in `as_hash_mut`.
        self.ptr_of(ZSET).map(|p| unsafe { &mut *p.cast::<ZSet>() })
    }

    /// Makes the value the string `value`, whatever it was: written over
    /// the old one when it's as long, else a new block (or the `raw`
    /// string's buffer, reused when it fits without wasting much).
    pub fn set_str(&mut self, value: &[u8]) {
        match self.flags() & (KIND | RAW) {
            STR => {
                let (at, k, v) = self.header();
                if v == value.len() {
                    self.write(at + k, value);
                    self.write(0, &[self.flags() & !FLOAT]);
                    return;
                }
            }
            kind if kind == STR | RAW && value.len() > EMBED_MAX => {
                // SAFETY: as in `as_hash_mut`.
                let buf = unsafe { &mut *self.ptr().cast::<Vec<u8>>() };
                if buf.capacity() >= value.len() && buf.capacity() <= 2 * value.len() + 64 {
                    buf.clear();
                    buf.extend_from_slice(value);
                    return;
                }
            }
            _ => {}
        }
        let new = Kv::new_str(self.hash(), self.key(), value, self.ttl_of());
        *self = new;
    }

    /// Marks the string as `INCRBYFLOAT`'s (see [`FLOAT`]).
    pub fn mark_float(&mut self) {
        self.write(0, &[self.flags() | FLOAT]);
    }

    /// The `ttl` of a new block keeping this one's expiry.
    fn ttl_of(&self) -> Option<(i64, u32)> {
        self.ttl_slot().map(|slot| (self.expire_at(), slot))
    }

    /// The string as a `Vec` to grow (`APPEND`): a string in the block
    /// moves to one of its own first, and is `raw` from then on.
    pub fn str_buf(&mut self) -> Result<&mut Vec<u8>, Error> {
        match self.flags() & (KIND | RAW) {
            STR => {
                let s = self.as_str()?.to_vec();
                let p = Box::into_raw(Box::new(s)) as usize;
                let new = Kv::build(STR | RAW, self.hash(), self.ttl_of(), self.key(), Body::Ptr(p));
                *self = new;
            }
            kind if kind == STR | RAW => {}
            _ => return Err(Error::WrongType),
        }
        // SAFETY: as in `as_hash_mut`.
        Ok(unsafe { &mut *self.ptr().cast::<Vec<u8>>() })
    }

    /// The value, taken out; the block goes.
    pub fn into_value(self) -> Value {
        let value = if self.boxed() {
            let p = self.ptr();
            // SAFETY: as in `value`; the block is freed without its value
            // below, so the value has one owner, the box made here.
            unsafe {
                match self.kind() {
                    STR => Value::Str(*Box::from_raw(p.cast::<Vec<u8>>())),
                    HASH => Value::Hash(Box::from_raw(p.cast::<Hash>())),
                    SET => Value::Set(Box::from_raw(p.cast::<Set>())),
                    LIST => Value::List(Box::from_raw(p.cast::<List>())),
                    _ => Value::ZSet(Box::from_raw(p.cast::<ZSet>())),
                }
            }
        } else {
            Value::Str(self.parts().1.to_vec())
        };
        self.free_block();
        value
    }
}

impl Hashed for Kv {
    fn hash(&self) -> u32 {
        u32::from_ne_bytes(self.read(HASH_AT))
    }

    fn key(&self) -> &[u8] {
        Kv::key(self)
    }
}

impl Kv {
    pub fn hash(&self) -> u32 {
        Hashed::hash(self)
    }
}

impl Drop for Kv {
    fn drop(&mut self) {
        if self.boxed() {
            // Takes the value out, and drops it with the block.
            // SAFETY: `self` is read out once and never used again: the
            // `ptr::read` copy owns the block from here.
            drop(unsafe { ptr::read(self) }.into_value());
        } else {
            let size = self.size();
            // SAFETY: allocated by `build` with this same layout, freed only
            // here.
            unsafe { dealloc(self.0.as_ptr(), layout(size)) }
        }
    }
}

impl fmt::Debug for Kv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Kv");
        d.field("key", &String::from_utf8_lossy(self.key()));
        match self.value() {
            Val::Str(s) => d.field("value", &String::from_utf8_lossy(s)),
            v => d.field("value", &v),
        };
        d.field("expire_at", &self.ttl_of()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7) as u8).collect()
    }

    #[test]
    fn keeps_its_parts() {
        for (k, v) in [(0, 0), (1, 0), (16, 16), (127, 128), (128, 127), (300, 4096), (5, 4097), (0, 70_000)] {
            for ttl in [None, Some((1_700_000_000_000, 42))] {
                let (key, value) = (bytes(k), bytes(v));
                let kv = Kv::new(0xdead_beef, &key, Value::Str(value.clone()), ttl);
                assert_eq!(kv.hash(), 0xdead_beef);
                assert_eq!(kv.key(), &key[..]);
                assert_eq!(kv.as_str().unwrap(), &value[..]);
                assert_eq!(kv.ttl_of(), ttl);
                assert_eq!(kv.expire_at(), ttl.map_or(NEVER, |t| t.0));
                assert_eq!(kv.boxed(), v > EMBED_MAX);
            }
        }
    }

    #[test]
    fn a_key_of_16_with_a_value_of_16_is_39_bytes() {
        let kv = Kv::new(1, &[b'k'; 16], Value::Str(vec![b'v'; 16]), None);
        assert_eq!(kv.size(), 39);
    }

    #[test]
    fn moves_its_value_through_ttl_changes() {
        let mut kv = Kv::new(7, b"key", Value::Str(b"value".to_vec()), None);
        kv.set_ttl(Some((1000, 3)));
        assert_eq!((kv.expire_at(), kv.ttl_slot()), (1000, Some(3)));
        kv.set_expire_at(2000);
        kv.set_ttl_slot(9);
        assert_eq!((kv.expire_at(), kv.ttl_slot()), (2000, Some(9)));
        assert_eq!((kv.key(), kv.as_str().unwrap()), (&b"key"[..], &b"value"[..]));
        kv.set_ttl(None);
        assert_eq!((kv.expire_at(), kv.ttl_slot()), (NEVER, None));
        assert_eq!((kv.key(), kv.as_str().unwrap(), kv.hash()), (&b"key"[..], &b"value"[..], 7));

        let mut h = Box::<Hash>::default();
        h.insert(b"f", b"v");
        let mut kv = Kv::new(7, b"key", Value::Hash(h), None);
        kv.set_ttl(Some((5, 0)));
        kv.as_hash_mut().unwrap().insert(b"g", b"w");
        kv.set_ttl(None);
        assert_eq!(kv.as_hash().unwrap().len(), 2);
        assert_eq!(kv.as_str(), Err(Error::WrongType));
        assert_eq!(kv.type_name(), "hash");

        // The fifth kind: the flags' kind bits don't run into RAW, TTL or FLOAT.
        let mut z = Box::<ZSet>::default();
        z.insert(b"m", 1.5);
        let mut kv = Kv::new(7, b"key", Value::ZSet(z), None);
        assert_eq!((kv.type_name(), kv.encoding(), kv.has_ttl()), ("zset", "listpack", false));
        kv.set_ttl(Some((5, 0)));
        assert!(kv.has_ttl());
        kv.as_zset_mut().unwrap().insert(b"n", -1.0);
        kv.set_ttl(None);
        assert_eq!(kv.as_zset().unwrap().iter(0, 9, false).collect::<Vec<_>>(), [(&b"n"[..], -1.0), (b"m", 1.5)]);
        assert_eq!((kv.as_str(), kv.as_hash().err(), kv.as_list().err()), (Err(Error::WrongType), Some(Error::WrongType), Some(Error::WrongType)));
        assert_eq!(kv.as_set_mut().err(), Some(Error::WrongType));
        let Value::ZSet(z) = kv.into_value() else { panic!("a sorted set") };
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn strings_change_in_place_or_move() {
        let mut kv = Kv::new(7, b"n", Value::Str(b"10".to_vec()), Some((99, 1)));
        assert_eq!(kv.encoding(), "int");
        let block = kv.0;
        kv.set_str(b"11");
        assert_eq!(kv.0, block, "same length: in place");
        kv.set_str(b"hello world");
        assert_eq!((kv.as_str().unwrap(), kv.encoding(), kv.ttl_of()), (&b"hello world"[..], "embstr", Some((99, 1))));
        kv.set_str(&[b'x'; 45]);
        assert_eq!(kv.encoding(), "raw");

        // APPEND makes it raw for good, until a SET.
        kv.set_str(b"12");
        kv.str_buf().unwrap().extend_from_slice(b"34");
        assert_eq!((kv.as_str().unwrap(), kv.encoding(), kv.ttl_of()), (&b"1234"[..], "raw", Some((99, 1))));
        kv.str_buf().unwrap().extend_from_slice(b"5");
        assert_eq!(kv.as_str().unwrap(), b"12345");
        kv.set_str(b"6");
        assert_eq!(kv.encoding(), "int");

        // A long raw string reuses its buffer.
        kv.set_str(&bytes(10_000));
        kv.set_str(&bytes(9_000));
        assert_eq!(kv.as_str().unwrap(), &bytes(9_000)[..]);
        assert!(matches!(kv.into_value(), Value::Str(s) if s == bytes(9_000)));
    }

    #[test]
    fn a_string_over_any_value() {
        let mut kv = Kv::new(7, b"k", Value::List(Box::default()), None);
        assert_eq!(kv.str_buf().err(), Some(Error::WrongType));
        kv.set_str(b"now a string");
        assert_eq!(kv.as_str().unwrap(), b"now a string");
    }

    #[test]
    fn is_one_pointer() {
        assert_eq!(mem::size_of::<Kv>(), 8);
        assert_eq!(mem::size_of::<Option<Kv>>(), 8);
    }
}
