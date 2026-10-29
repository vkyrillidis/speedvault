//! An insertion-ordered hash table: the members of a hash or a set, and
//! (as [`Indexed`]) the keys of one slot of the keyspace.
//!
//! The entries sit in a `Vec`, in the order they were added; an index of
//! positions into it (open addressing, linear probing) finds a key. Each
//! entry is one heap block holding its hash and key (a [`Block`] for a
//! hash's field and value, or a set's member with an empty value; a `Kv`
//! for a key and its value), so an entry costs one allocation and an
//! 8-byte pointer, the index holds only `u32` positions, no key is stored
//! twice, and growing the index never hashes a key again. Iterating walks
//! the `Vec`.
//!
//! Removal ([`Indexed::remove_swap_at`]) moves the last entry into the
//! hole, O(1): only collections past the sizes where Redis keeps them
//! compact are tables (see `collections`), and their order is as
//! unspecified as Redis's; so is the keyspace's.
//!
//! A place in the index holds a position, below half the index's length
//! (it's at most half full), so the bits above are free: they hold the
//! entry's hash there, a tag. A probe compares tags and reads an entry only
//! when they match (by chance 1 in 2^27 with the smallest index, 1 in 2^7
//! at 16M entries), so a key that isn't there costs no entry read, nor a key
//! past the others in its probe run.
//!
//! A table of up to [`SCAN_MAX`] entries has no index: comparing the hashes
//! of a few entries is cheaper than probing, and saves the index's memory.

use crate::block::Block;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem;

/// Tables up to this many entries are searched without an index.
const SCAN_MAX: usize = 8;
/// An empty place in the index.
const EMPTY: u32 = u32::MAX;

/// What an [`Indexed`] holds: an entry that knows its hash and its key.
pub(crate) trait Hashed {
    fn hash(&self) -> u32;
    fn key(&self) -> &[u8];
}

impl Hashed for Block {
    fn hash(&self) -> u32 {
        Block::hash(self)
    }

    fn key(&self) -> &[u8] {
        Block::key(self)
    }
}

/// The entries and their index, without a hasher: the caller hashes the
/// keys (the keyspace hashes a key once, to choose its table and to find
/// it there).
#[derive(Debug)]
pub(crate) struct Indexed<E> {
    entries: Vec<E>,
    /// Positions into `entries` in the bits below its length, the tag in
    /// the bits above (see the module docs), [`EMPTY`] where none; a power
    /// of two long, at most half full. Empty while the table has no more
    /// than [`SCAN_MAX`] entries.
    index: Vec<u32>,
}

impl<E> Default for Indexed<E> {
    fn default() -> Indexed<E> {
        Indexed { entries: Vec::new(), index: Vec::new() }
    }
}

impl<E: Hashed> Indexed<E> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[E] {
        &self.entries
    }

    pub fn get(&self, pos: usize) -> Option<&E> {
        self.entries.get(pos)
    }

    pub fn get_mut(&mut self, pos: usize) -> Option<&mut E> {
        self.entries.get_mut(pos)
    }

    /// The bits of a place in the index that hold the position, and those
    /// of a hash that choose the home place.
    fn mask(&self) -> u32 {
        (self.index.len() - 1) as u32
    }

    /// The position in `entries` of `key`, whose hash is `hash`.
    pub fn find(&self, hash: u32, key: &[u8]) -> Option<usize> {
        let found = |e: &E| e.hash() == hash && e.key() == key;
        if self.index.is_empty() {
            return self.entries.iter().position(found);
        }
        let mask = self.mask();
        let tag = hash & !mask;
        let mut i = hash & mask;
        loop {
            let place = self.index[i as usize];
            if place == EMPTY {
                return None;
            }
            if place & !mask == tag {
                let pos = (place & mask) as usize;
                if found(&self.entries[pos]) {
                    return Some(pos);
                }
            }
            i = (i + 1) & mask;
        }
    }

    /// The place in the index that holds position `pos`.
    fn place_of(&self, pos: usize) -> usize {
        let mask = self.mask();
        let mut i = self.entries[pos].hash() & mask;
        // An empty place's position bits are all ones: never a position.
        while (self.index[i as usize] & mask) as usize != pos {
            debug_assert_ne!(self.index[i as usize], EMPTY, "index out of sync");
            i = (i + 1) & mask;
        }
        i as usize
    }

    /// Adds `entry`, whose key isn't there, at the end; returns its
    /// position.
    pub fn push(&mut self, entry: E) -> usize {
        let pos = u32::try_from(self.entries.len()).ok().filter(|&p| p != EMPTY).expect("too many entries");
        let hash = entry.hash();
        self.entries.push(entry);
        if self.index.is_empty() {
            if self.entries.len() > SCAN_MAX {
                self.rebuild(32);
            }
        } else if self.entries.len() * 2 > self.index.len() {
            self.rebuild(self.index.len() * 2);
        } else {
            self.place(hash, pos);
        }
        pos as usize
    }

    /// The most a push may allocate at once while the table is this long
    /// (a `Vec` or an index twice the size, while the old one is still
    /// there): what it allocates when both are full. A table no longer
    /// than this one grows by no more.
    pub fn growth_bound(&self) -> usize {
        let len = self.entries.len().max(self.entries.capacity());
        (len * 2).max(4) * mem::size_of::<E>() + (self.index.len() * 2).max(32) * mem::size_of::<u32>()
    }

    /// Puts position `pos`, of an entry with `hash`, in the index.
    fn place(&mut self, hash: u32, pos: u32) {
        let mask = self.mask();
        debug_assert!(pos < mask / 2 + 1, "at most half full");
        let mut i = (hash & mask) as usize;
        while self.index[i] != EMPTY {
            i = (i + 1) & mask as usize;
        }
        self.index[i] = (hash & !mask) | pos;
    }

    /// Builds the index again with `len` places (a power of two), or none
    /// if the table is small enough to go without.
    fn rebuild(&mut self, len: usize) {
        if self.entries.len() <= SCAN_MAX {
            self.index = Vec::new();
            return;
        }
        self.index = vec![EMPTY; len];
        for pos in 0..self.entries.len() {
            self.place(self.entries[pos].hash(), pos as u32);
        }
    }

    /// Empties place `i` of the index, moving the entries probed past it
    /// back (backward-shift deletion: no tombstones).
    fn unplace(&mut self, mut i: usize) {
        let mask = self.mask();
        let mut j = i;
        loop {
            j = (j + 1) & mask as usize;
            let place = self.index[j];
            if place == EMPTY {
                break;
            }
            // The tag holds the hash's high bits only: the home is read
            // from the entry.
            let home = (self.entries[(place & mask) as usize].hash() & mask) as usize;
            // The entry at j may move to i unless its home lies cyclically
            // in (i, j]: then it would sit before its home.
            let stays = if i <= j { i < home && home <= j } else { i < home || home <= j };
            if !stays {
                self.index[i] = place;
                i = j;
            }
        }
        self.index[i] = EMPTY;
    }

    /// Removes the entry at `pos` and returns it; the last entry takes its
    /// place.
    pub fn remove_swap_at(&mut self, pos: usize) -> E {
        let last = self.entries.len() - 1;
        if !self.index.is_empty() {
            let place = self.place_of(pos);
            self.unplace(place);
            if pos != last {
                let moved = self.place_of(last);
                self.index[moved] = (self.index[moved] & !self.mask()) | pos as u32;
            }
        }
        let entry = self.entries.swap_remove(pos);
        self.after_remove();
        entry
    }

    /// Drops the index once the table is small, shrinks it (and the
    /// entries) once they're a quarter full; an empty table gives back
    /// all its room.
    fn after_remove(&mut self) {
        let len = self.entries.len();
        if !self.index.is_empty() && (len <= SCAN_MAX || len * 8 < self.index.len()) {
            self.rebuild((len * 2).next_power_of_two().max(32));
        }
        if len == 0 {
            self.entries = Vec::new();
        } else if self.entries.capacity() > 16 && len * 4 < self.entries.capacity() {
            self.entries.shrink_to(len * 2);
        }
    }

    /// Drops the last `n` entries, leaving the table unusable (the index
    /// goes first): for freeing a table taken out of the store a piece at
    /// a time. True once it's empty.
    pub fn free_last(&mut self, n: usize) -> bool {
        self.index = Vec::new();
        self.entries.truncate(self.entries.len().saturating_sub(n));
        if self.entries.is_empty() {
            self.entries = Vec::new();
        }
        self.entries.is_empty()
    }

    /// The index and the entries agree: every entry is found where it is,
    /// and the index holds each position once.
    #[cfg(test)]
    pub fn check(&self) {
        for (pos, e) in self.entries.iter().enumerate() {
            assert_eq!(self.find(e.hash(), e.key()), Some(pos));
        }
        if !self.index.is_empty() {
            let mask = self.mask();
            for &place in self.index.iter().filter(|&&p| p != EMPTY) {
                assert_eq!(place & !mask, self.entries[(place & mask) as usize].hash() & !mask, "tag of {place:#x}");
            }
            let mut placed: Vec<u32> = self.index.iter().filter(|&&p| p != EMPTY).map(|&p| p & mask).collect();
            placed.sort_unstable();
            assert_eq!(placed, (0..self.entries.len() as u32).collect::<Vec<_>>());
            assert!(self.entries.len() * 2 <= self.index.len());
        } else {
            assert!(self.entries.len() <= SCAN_MAX);
        }
    }
}

/// The table of a hash or a set: [`Indexed`] blocks, and a hasher of its
/// own.
#[derive(Debug)]
pub struct Table {
    inner: Indexed<Block>,
    hasher: RandomState,
}

impl Default for Table {
    fn default() -> Table {
        Table { inner: Indexed::default(), hasher: RandomState::new() }
    }
}

impl Table {
    #[cfg(test)]
    pub fn new() -> Table {
        Table::default()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    fn hash(&self, key: &[u8]) -> u32 {
        self.hasher.hash_one(key) as u32
    }

    /// The position of `key` in the order, if it's there.
    pub fn position(&self, key: &[u8]) -> Option<usize> {
        self.inner.find(self.hash(key), key)
    }

    /// The value of `key`.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let pos = self.position(key)?;
        Some(self.inner.entries[pos].value())
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.position(key).is_some()
    }

    /// The key and value at position `pos` in the order.
    pub fn get_index(&self, pos: usize) -> Option<(&[u8], &[u8])> {
        self.inner.get(pos).map(Block::parts)
    }

    /// Adds `key` at the end, or replaces its value where it is (keeping
    /// its place, as a listpack does). True if it's new.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        let hash = self.hash(key);
        if let Some(pos) = self.inner.find(hash, key) {
            let b = &mut self.inner.entries[pos];
            if b.value().len() == value.len() {
                b.overwrite_value(value);
            } else {
                *b = Block::new(hash, key, value);
            }
            return false;
        }
        self.inner.push(Block::new(hash, key, value));
        true
    }

    /// Removes `key`; the last entry takes its place (O(1)). True if it
    /// was there.
    pub fn remove_swap(&mut self, key: &[u8]) -> bool {
        match self.position(key) {
            Some(pos) => {
                self.remove_swap_at(pos);
                true
            }
            None => false,
        }
    }

    /// Removes the entry at `pos` and returns its key; the last entry
    /// takes its place.
    pub fn remove_swap_at(&mut self, pos: usize) -> Box<[u8]> {
        self.inner.remove_swap_at(pos).key().into()
    }

    /// See [`Indexed::free_last`].
    pub fn free_last(&mut self, n: usize) -> bool {
        self.inner.free_last(n)
    }

    /// The entries in order.
    #[cfg(test)]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> + DoubleEndedIterator {
        self.inner.entries.iter().map(Block::parts)
    }

    #[cfg(test)]
    pub fn values(&self) -> impl ExactSizeIterator<Item = &[u8]> + DoubleEndedIterator {
        self.inner.entries.iter().map(Block::value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn key(n: u32) -> Vec<u8> {
        format!("k{n}").into_bytes()
    }

    fn val(n: u32) -> Vec<u8> {
        n.to_string().into_bytes()
    }

    fn check(t: &Table) {
        t.inner.check();
    }

    fn numbers(t: &Table) -> Vec<u32> {
        t.values().map(|v| std::str::from_utf8(v).unwrap().parse().unwrap()).collect()
    }

    #[test]
    fn keeps_insertion_order_and_replaces_in_place() {
        let mut t = Table::new();
        for n in 0..100 {
            assert!(t.insert(&key(n), &val(n)));
        }
        assert!(!t.insert(&key(5), b"500"));
        check(&t);
        let mut want: Vec<u32> = (0..100).collect();
        want[5] = 500;
        assert_eq!(numbers(&t), want);
        assert_eq!(t.get(b"k7"), Some(&b"7"[..]));
        assert_eq!(t.get(b"nope"), None);
        // The same length is written over, another one is a new block.
        t.insert(b"k7", b"8");
        t.insert(b"k7", b"70");
        assert_eq!(t.get_index(7), Some((&b"k7"[..], &b"70"[..])));
        t.insert(b"k7", b"7");
        assert_eq!(t.get_index(7), Some((&b"k7"[..], &b"7"[..])));
        check(&t);
    }

    #[test]
    fn swap_removal_moves_the_last_entry() {
        let mut t = Table::new();
        for n in 0..10 {
            t.insert(&key(n), &val(n));
        }
        assert!(t.remove_swap(b"k2"));
        assert!(!t.remove_swap(b"k2"));
        assert_eq!(numbers(&t), [0, 1, 9, 3, 4, 5, 6, 7, 8]);
        assert_eq!(&*t.remove_swap_at(0), b"k0");
        check(&t);
    }

    /// Random inserts and removals against a `HashMap`, through the
    /// index growing, shrinking and going away.
    #[test]
    fn matches_a_hash_map() {
        let mut t = Table::new();
        let mut m: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for round in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Up to 2000 keys early on, then fewer, so the table shrinks.
            let range = if round < 10_000 { 2000 } else { 50 };
            let k = key((x % range) as u32);
            match x >> 60 {
                0..=7 => assert_eq!(t.insert(&k, &val(round)), m.insert(k.clone(), val(round)).is_none()),
                8..=12 => assert_eq!(t.remove_swap(&k), m.remove(&k).is_some()),
                _ => assert_eq!(t.get(&k), m.get(&k).map(|v| &v[..])),
            }
            if round % 97 == 0 {
                check(&t);
            }
            assert_eq!(t.len(), m.len());
        }
        check(&t);
        for (k, v) in t.iter() {
            assert_eq!(m.get(k).map(|v| &v[..]), Some(v));
        }
    }

    #[test]
    fn colliding_homes_survive_removals() {
        // Every key in one table of 32 places lands somewhere; removals
        // from the middle of probe runs must keep the rest findable.
        let mut t = Table::new();
        for n in 0..16 {
            t.insert(&key(n), &val(n));
        }
        for n in [3, 0, 15, 7, 8, 1] {
            t.remove_swap(&key(n));
            check(&t);
        }
        for n in [2, 4, 5, 6, 9, 10, 11, 12, 13, 14] {
            assert_eq!(t.get(&key(n)), Some(&val(n)[..]));
        }
    }

    /// Two keys whose hashes are equal (1 in 2^32 of pairs, so made up
    /// here) are told apart by their bytes, with and without an index.
    #[test]
    fn same_hash_other_key() {
        let mut t = Table::new();
        for n in [4, 100] {
            for i in 0..n {
                t.insert(&key(i), &val(i));
            }
            let h = t.hash(&key(3));
            assert_eq!(t.inner.find(h, &key(3)), Some(3));
            assert_eq!(t.inner.find(h, b"another"), None);
        }
    }

    #[test]
    fn frees_from_the_end() {
        let mut t = Table::new();
        for n in 0..100 {
            t.insert(&key(n), &val(n));
        }
        assert!(!t.free_last(60));
        assert_eq!(t.len(), 40);
        assert!(t.free_last(60));
    }

    /// What a push allocates, at most what `growth_bound` said.
    #[test]
    fn bounds_its_growth() {
        let mut t: Indexed<Block> = Indexed::default();
        for n in 0..300u32 {
            let (capacity, places) = (t.entries.capacity(), t.index.len());
            let bound = t.growth_bound();
            t.push(Block::new(n, &key(n), b""));
            let mut grown = 0;
            if t.entries.capacity() != capacity {
                grown += t.entries.capacity() * mem::size_of::<Block>();
            }
            if t.index.len() != places {
                grown += t.index.len() * 4;
            }
            assert!(grown <= bound, "push {n}: {grown} of {bound}");
            if t.entries.len() == 9 || t.entries.len() == 17 {
                assert_eq!(grown, bound, "both full at {n}");
            }
        }
    }
}
