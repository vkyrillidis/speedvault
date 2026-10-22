//! An insertion-ordered hash table: the members of a hash or a set.
//!
//! The entries sit in a `Vec`, in the order they were added; an index of
//! positions into it (open addressing, linear probing) finds a key. Each
//! entry keeps its hash, so the index holds only `u32` positions and no key
//! is stored twice, and growing the index never hashes a key again.
//! Iterating walks the `Vec`.
//!
//! Removal ([`Table::remove_swap`]) moves the last entry into the hole,
//! O(1): only collections past the sizes where Redis keeps them compact are
//! tables (see `collections`), and their order is as unspecified as Redis's.
//!
//! A table of up to [`SCAN_MAX`] entries has no index: comparing the hashes
//! of a few entries is cheaper than probing, and saves the index's memory.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

/// Tables up to this many entries are searched without an index.
const SCAN_MAX: usize = 8;
/// An empty place in the index.
const EMPTY: u32 = u32::MAX;

#[derive(Debug)]
struct Slot<V> {
    hash: u64,
    key: Box<[u8]>,
    value: V,
}

#[derive(Debug)]
pub struct Table<V> {
    entries: Vec<Slot<V>>,
    /// Positions into `entries`, [`EMPTY`] where none; a power of two long,
    /// at most half full. Empty while the table has no more than
    /// [`SCAN_MAX`] entries.
    index: Vec<u32>,
    hasher: RandomState,
}

impl<V> Default for Table<V> {
    fn default() -> Table<V> {
        Table { entries: Vec::new(), index: Vec::new(), hasher: RandomState::new() }
    }
}

impl<V> Table<V> {
    #[cfg(test)]
    pub fn new() -> Table<V> {
        Table::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// The position of `key` in the order, if it's there.
    pub fn position(&self, key: &[u8]) -> Option<usize> {
        self.find(self.hash(key), key).map(|(_, pos)| pos)
    }

    /// The place in the index (`usize::MAX` without an index) and the
    /// position in `entries` of `key`.
    fn find(&self, hash: u64, key: &[u8]) -> Option<(usize, usize)> {
        if self.index.is_empty() {
            let pos = self.entries.iter().position(|s| s.hash == hash && *s.key == *key)?;
            return Some((usize::MAX, pos));
        }
        let mask = self.index.len() - 1;
        let mut i = hash as usize & mask;
        loop {
            let pos = self.index[i];
            if pos == EMPTY {
                return None;
            }
            let s = &self.entries[pos as usize];
            if s.hash == hash && *s.key == *key {
                return Some((i, pos as usize));
            }
            i = (i + 1) & mask;
        }
    }

    /// The place in the index that holds position `pos`.
    fn place_of(&self, pos: usize) -> usize {
        let mask = self.index.len() - 1;
        let mut i = self.entries[pos].hash as usize & mask;
        while self.index[i] as usize != pos {
            debug_assert_ne!(self.index[i], EMPTY, "index out of sync");
            i = (i + 1) & mask;
        }
        i
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let (_, pos) = self.find(self.hash(key), key)?;
        Some(&self.entries[pos].value)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.position(key).is_some()
    }

    /// The entry at position `pos` in the order.
    pub fn get_index(&self, pos: usize) -> Option<(&[u8], &V)> {
        self.entries.get(pos).map(|s| (&*s.key, &s.value))
    }

    /// Adds `key` at the end, or replaces its value where it is (keeping
    /// its place, as a listpack does). Returns the old value.
    pub fn insert(&mut self, key: &[u8], value: V) -> Option<V> {
        let hash = self.hash(key);
        if let Some((_, pos)) = self.find(hash, key) {
            return Some(std::mem::replace(&mut self.entries[pos].value, value));
        }
        let pos = u32::try_from(self.entries.len()).ok().filter(|&p| p != EMPTY).expect("too many entries");
        self.entries.push(Slot { hash, key: key.into(), value });
        if self.index.is_empty() {
            if self.entries.len() > SCAN_MAX {
                self.rebuild(32);
            }
        } else if self.entries.len() * 2 > self.index.len() {
            self.rebuild(self.index.len() * 2);
        } else {
            self.place(hash, pos);
        }
        None
    }

    /// Puts position `pos`, of an entry with `hash`, in the index.
    fn place(&mut self, hash: u64, pos: u32) {
        let mask = self.index.len() - 1;
        let mut i = hash as usize & mask;
        while self.index[i] != EMPTY {
            i = (i + 1) & mask;
        }
        self.index[i] = pos;
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
            self.place(self.entries[pos].hash, pos as u32);
        }
    }

    /// Empties place `i` of the index, moving the entries probed past it
    /// back (backward-shift deletion: no tombstones).
    fn unplace(&mut self, mut i: usize) {
        let mask = self.index.len() - 1;
        let mut j = i;
        loop {
            j = (j + 1) & mask;
            let pos = self.index[j];
            if pos == EMPTY {
                break;
            }
            let home = self.entries[pos as usize].hash as usize & mask;
            // The entry at j may move to i unless its home lies cyclically
            // in (i, j]: then it would sit before its home.
            let stays = if i <= j { i < home && home <= j } else { i < home || home <= j };
            if !stays {
                self.index[i] = pos;
                i = j;
            }
        }
        self.index[i] = EMPTY;
    }

    /// Removes `key`; the last entry takes its place (O(1)).
    pub fn remove_swap(&mut self, key: &[u8]) -> Option<V> {
        let (_, pos) = self.find(self.hash(key), key)?;
        Some(self.remove_swap_at(pos).1)
    }

    /// Removes the entry at `pos`; the last entry takes its place.
    pub fn remove_swap_at(&mut self, pos: usize) -> (Box<[u8]>, V) {
        let last = self.entries.len() - 1;
        if !self.index.is_empty() {
            let place = self.place_of(pos);
            self.unplace(place);
            if pos != last {
                let moved = self.place_of(last);
                self.index[moved] = pos as u32;
            }
        }
        let slot = self.entries.swap_remove(pos);
        self.after_remove();
        (slot.key, slot.value)
    }

    /// Drops the index once the table is small, shrinks it (and the
    /// entries) once they're a quarter full.
    fn after_remove(&mut self) {
        let len = self.entries.len();
        if !self.index.is_empty() && (len <= SCAN_MAX || len * 8 < self.index.len()) {
            self.rebuild((len * 2).next_power_of_two().max(32));
        }
        if self.entries.capacity() > 16 && len * 4 < self.entries.capacity() {
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

    /// The entries in order.
    #[cfg(test)]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&[u8], &V)> + DoubleEndedIterator {
        self.entries.iter().map(|s| (&*s.key, &s.value))
    }

    #[cfg(test)]
    pub fn values(&self) -> impl ExactSizeIterator<Item = &V> + DoubleEndedIterator {
        self.entries.iter().map(|s| &s.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn key(n: u32) -> Vec<u8> {
        format!("k{n}").into_bytes()
    }

    /// The index and the entries agree: every entry is found where it is,
    /// and the index holds each position once.
    fn check<V>(t: &Table<V>) {
        for (pos, s) in t.entries.iter().enumerate() {
            assert_eq!(t.position(&s.key), Some(pos));
        }
        if !t.index.is_empty() {
            let mut placed: Vec<u32> = t.index.iter().copied().filter(|&p| p != EMPTY).collect();
            placed.sort_unstable();
            assert_eq!(placed, (0..t.entries.len() as u32).collect::<Vec<_>>());
            assert!(t.entries.len() * 2 <= t.index.len());
        } else {
            assert!(t.entries.len() <= SCAN_MAX);
        }
    }

    #[test]
    fn keeps_insertion_order_and_replaces_in_place() {
        let mut t = Table::new();
        for n in 0..100 {
            assert_eq!(t.insert(&key(n), n), None);
        }
        assert_eq!(t.insert(&key(5), 500), Some(5));
        check(&t);
        let order: Vec<u32> = t.values().copied().collect();
        let mut want: Vec<u32> = (0..100).collect();
        want[5] = 500;
        assert_eq!(order, want);
        assert_eq!(t.get(b"k7"), Some(&7));
        assert_eq!(t.get(b"nope"), None);
        t.insert(b"k7", 70);
        assert_eq!(t.get_index(7), Some((&b"k7"[..], &70)));
    }

    #[test]
    fn swap_removal_moves_the_last_entry() {
        let mut t = Table::new();
        for n in 0..10 {
            t.insert(&key(n), n);
        }
        assert_eq!(t.remove_swap(b"k2"), Some(2));
        assert_eq!(t.values().copied().collect::<Vec<_>>(), [0, 1, 9, 3, 4, 5, 6, 7, 8]);
        check(&t);
    }

    /// Random inserts and removals against a `HashMap`, through the
    /// index growing, shrinking and going away.
    #[test]
    fn matches_a_hash_map() {
        let mut t = Table::new();
        let mut m = HashMap::new();
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for round in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Up to 2000 keys early on, then fewer, so the table shrinks.
            let range = if round < 10_000 { 2000 } else { 50 };
            let k = key((x % range) as u32);
            match x >> 60 {
                0..=7 => assert_eq!(t.insert(&k, round), m.insert(k.clone(), round)),
                8..=12 => assert_eq!(t.remove_swap(&k), m.remove(&k)),
                _ => assert_eq!(t.get(&k), m.get(&k)),
            }
            if round % 97 == 0 {
                check(&t);
            }
            assert_eq!(t.len(), m.len());
        }
        check(&t);
        for (k, v) in t.iter() {
            assert_eq!(m.get(k), Some(v));
        }
    }

    #[test]
    fn colliding_homes_survive_removals() {
        // Every key in one table of 32 places lands somewhere; removals
        // from the middle of probe runs must keep the rest findable.
        let mut t = Table::new();
        for n in 0..16 {
            t.insert(&key(n), n);
        }
        for n in [3, 0, 15, 7, 8, 1] {
            t.remove_swap(&key(n));
            check(&t);
        }
        for n in [2, 4, 5, 6, 9, 10, 11, 12, 13, 14] {
            assert_eq!(t.get(&key(n)), Some(&n));
        }
    }
}
