//! The values that hold many elements: hashes, sets, lists.
//!
//! Each knows the encoding Redis 7.0.15 would have given it, because that
//! decides the order Redis replies in (see the phase 7 plan in the README):
//! a hash Redis would still keep as a listpack keeps its insertion order on
//! deletion, a set Redis would keep as an intset is read out in numeric
//! order. Redis converts one way only (a listpack that grew into a hashtable
//! stays one after shrinking), and so do these.

use crate::int::parse_i64;
use crate::packed::{At, Packed};
use crate::table::Table;
use std::cmp::Ordering;
use std::collections::VecDeque;

/// Redis's `hash-max-listpack-entries`: 512 built in (the `redis.conf` shipped
/// with Redis sets 128; a server started without one has 512).
pub const HASH_MAX_LISTPACK_ENTRIES: usize = 512;
/// Redis's `hash-max-listpack-value`.
pub const HASH_MAX_LISTPACK_VALUE: usize = 64;
/// Redis's `set-max-intset-entries`.
pub const SET_MAX_INTSET_ENTRIES: usize = 512;

/// A hash: packed while Redis would keep it as a listpack (so in Redis's
/// order, and a few bytes per field), a table once Redis would have
/// converted it to a hashtable.
#[derive(Debug)]
pub struct Hash {
    repr: HashRepr,
}

#[derive(Debug)]
enum HashRepr {
    /// Fields and values in turn.
    Packed(Packed),
    Table(Table),
}

impl Default for Hash {
    fn default() -> Hash {
        Hash { repr: HashRepr::Packed(Packed::default()) }
    }
}

/// Where a field of a packed hash is, and its value.
fn find_pair(p: &Packed, field: &[u8]) -> Option<(At, At)> {
    let mut items = p.iter();
    while let Some((f, bytes)) = items.next() {
        let (v, _) = items.next().expect("fields and values in turn");
        if bytes == field {
            return Some((f, v));
        }
    }
    None
}

impl Hash {
    pub fn len(&self) -> usize {
        match &self.repr {
            HashRepr::Packed(p) => p.len() / 2,
            HashRepr::Table(t) => t.len(),
        }
    }

    /// Once true, the store deletes the key.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether Redis would have converted it to a hashtable (which it
    /// scans incrementally, in no order).
    pub fn is_big(&self) -> bool {
        matches!(self.repr, HashRepr::Table(_))
    }

    pub fn encoding(&self) -> &'static str {
        if self.is_big() {
            "hashtable"
        } else {
            "listpack"
        }
    }

    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match &self.repr {
            HashRepr::Packed(p) => find_pair(p, field).map(|(_, v)| p.get(v)),
            HashRepr::Table(t) => t.get(field),
        }
    }

    /// To a table, as Redis converts a listpack to a hashtable: once, in
    /// the order the fields were.
    fn convert(&mut self) {
        if let HashRepr::Packed(p) = &self.repr {
            let mut t = Table::default();
            let mut items = p.iter();
            while let Some((_, f)) = items.next() {
                let (_, v) = items.next().expect("fields and values in turn");
                t.insert(f, v);
            }
            self.repr = HashRepr::Table(t);
        }
    }

    /// Converts to a hashtable if any of `args` is longer than a listpack
    /// takes, as Redis's `hashTypeTryConversion` does before a write.
    pub(crate) fn note_lengths(&mut self, args: &[&[u8]]) {
        if args.iter().any(|a| a.len() > HASH_MAX_LISTPACK_VALUE) {
            self.convert();
        }
    }

    /// Sets `field`; true if it's new.
    pub(crate) fn insert(&mut self, field: &[u8], value: &[u8]) -> bool {
        if field.len() > HASH_MAX_LISTPACK_VALUE || value.len() > HASH_MAX_LISTPACK_VALUE {
            self.convert();
        }
        let new = match &mut self.repr {
            HashRepr::Packed(p) => match find_pair(p, field) {
                Some((_, v)) => {
                    p.replace(v, value);
                    false
                }
                None => {
                    p.push(field);
                    p.push(value);
                    true
                }
            },
            HashRepr::Table(t) => t.insert(field, value),
        };
        if self.len() > HASH_MAX_LISTPACK_ENTRIES {
            self.convert();
        }
        new
    }

    /// Deletes `field`; true if it was there.
    pub(crate) fn remove(&mut self, field: &[u8]) -> bool {
        match &mut self.repr {
            HashRepr::Packed(p) => match find_pair(p, field) {
                Some((f, v)) => {
                    p.remove_range(f.whole().start..v.whole().end, 2);
                    true
                }
                None => false,
            },
            HashRepr::Table(t) => t.remove_swap(field),
        }
    }

    /// The fields and values in Redis's order (for a big hash, the order
    /// of the table, which Redis leaves unspecified).
    pub fn iter(&self) -> HashIter<'_> {
        self.iter_from(0)
    }

    /// [`iter`](Self::iter) from the `from`th field on.
    pub fn iter_from(&self, from: usize) -> HashIter<'_> {
        HashIter(match &self.repr {
            HashRepr::Packed(p) => {
                let mut items = p.iter();
                if from > 0 {
                    items.nth(2 * from - 1);
                }
                IterRepr::Packed(items)
            }
            HashRepr::Table(t) => IterRepr::Table(t, from.min(t.len())),
        })
    }

    /// Frees up to `n` fields of a hash taken out of the store, leaving it
    /// unusable; true once it's all freed.
    pub(crate) fn free_last(&mut self, n: usize) -> bool {
        match &mut self.repr {
            HashRepr::Packed(p) => {
                *p = Packed::default();
                true
            }
            HashRepr::Table(t) => t.free_last(n),
        }
    }
}

/// [`Hash::iter`]: fields and values.
pub struct HashIter<'a>(IterRepr<'a>);

enum IterRepr<'a> {
    Packed(crate::packed::Iter<'a>),
    /// The table and the next position.
    Table(&'a Table, usize),
}

impl<'a> Iterator for HashIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<(&'a [u8], &'a [u8])> {
        match &mut self.0 {
            IterRepr::Packed(items) => {
                let (_, f) = items.next()?;
                let (_, v) = items.next().expect("fields and values in turn");
                Some((f, v))
            }
            IterRepr::Table(t, pos) => {
                let (f, v) = t.get_index(*pos)?;
                *pos += 1;
                Some((f, v))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match &self.0 {
            IterRepr::Packed(items) => items.len() / 2,
            IterRepr::Table(t, pos) => t.len() - pos,
        };
        (n, Some(n))
    }
}

impl ExactSizeIterator for HashIter<'_> {}

/// Small sets of other members are packed too while they have at most this
/// many (Redis 7.2's `set-max-listpack-entries`; 7.0.15 has no listpack
/// sets, so they say `hashtable` as 7.0.15 would).
pub const SET_MAX_PACKED_ENTRIES: usize = 128;
/// ... of at most this many bytes each (Redis 7.2's
/// `set-max-listpack-value`).
pub const SET_MAX_PACKED_VALUE: usize = 64;

/// A set: an intset while Redis would keep one (canonical integers, read out
/// in numeric order), packed while small, a table past that.
#[derive(Debug)]
pub struct Set {
    repr: SetRepr,
}

#[derive(Debug)]
enum SetRepr {
    /// Integers in numeric order, as their canonical decimal strings.
    Ints(Packed),
    /// Up to [`SET_MAX_PACKED_ENTRIES`] members, in the order added.
    Small(Packed),
    Table(Table),
}

impl Default for Set {
    fn default() -> Set {
        Set { repr: SetRepr::Ints(Packed::default()) }
    }
}

/// Canonical integer strings (as `parse_i64` takes them) by their value,
/// without parsing them.
fn cmp_ints(a: &[u8], b: &[u8]) -> Ordering {
    match (a.first() == Some(&b'-'), b.first() == Some(&b'-')) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => a.len().cmp(&b.len()).then_with(|| a.cmp(b)),
        (true, true) => b.len().cmp(&a.len()).then_with(|| b.cmp(a)),
    }
}

impl Set {
    pub fn len(&self) -> usize {
        match &self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => p.len(),
            SetRepr::Table(t) => t.len(),
        }
    }

    /// Once true, the store deletes the key.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether Redis would have converted it to a hashtable (which it
    /// scans incrementally, in no order).
    pub fn is_big(&self) -> bool {
        !matches!(self.repr, SetRepr::Ints(_))
    }

    pub fn encoding(&self) -> &'static str {
        if self.is_big() {
            "hashtable"
        } else {
            "intset"
        }
    }

    /// Where `member` is in an intset, or where it would go.
    fn find_int(p: &Packed, member: &[u8]) -> Result<At, usize> {
        for (at, m) in p.iter() {
            match cmp_ints(m, member) {
                Ordering::Less => {}
                Ordering::Equal => return Ok(at),
                Ordering::Greater => return Err(at.whole().start),
            }
        }
        Err(p.bytes())
    }

    fn find(p: &Packed, member: &[u8]) -> Option<At> {
        p.iter().find(|(_, m)| *m == member).map(|(at, _)| at)
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.repr {
            SetRepr::Ints(p) => parse_i64(member).is_some() && Set::find_int(p, member).is_ok(),
            SetRepr::Small(p) => Set::find(p, member).is_some(),
            SetRepr::Table(t) => t.contains(member),
        }
    }

    /// To the packed set if it fits, else to the table, in the order the
    /// members are now (Redis converts an intset to a hashtable; 7.2 to a
    /// listpack while small).
    fn convert(&mut self, small: bool) {
        let members: Vec<Box<[u8]>> = self.iter().map(Box::from).collect();
        self.repr = if small {
            let mut p = Packed::default();
            members.iter().for_each(|m| p.push(m));
            SetRepr::Small(p)
        } else {
            let mut t = Table::default();
            members.iter().for_each(|m| {
                t.insert(m, b"");
            });
            SetRepr::Table(t)
        };
    }

    /// Adds `member`; true if it's new.
    pub fn insert(&mut self, member: &[u8]) -> bool {
        if let SetRepr::Ints(p) = &mut self.repr {
            // An intset holds what Redis's string2ll takes: canonical i64s.
            if parse_i64(member).is_some() {
                match Set::find_int(p, member) {
                    Ok(_) => return false,
                    Err(at) if p.len() < SET_MAX_INTSET_ENTRIES => {
                        p.insert_at(at, member);
                        return true;
                    }
                    Err(_) => {}
                }
            }
            let fits = self.len() < SET_MAX_PACKED_ENTRIES
                && member.len() <= SET_MAX_PACKED_VALUE
                && self.iter().all(|m| m.len() <= SET_MAX_PACKED_VALUE);
            self.convert(fits);
        }
        if let SetRepr::Small(p) = &mut self.repr {
            if Set::find(p, member).is_some() {
                return false;
            }
            if p.len() < SET_MAX_PACKED_ENTRIES && member.len() <= SET_MAX_PACKED_VALUE {
                p.push(member);
                return true;
            }
            self.convert(false);
        }
        match &mut self.repr {
            SetRepr::Table(t) => t.insert(member, b""),
            _ => unreachable!("converted above"),
        }
    }

    /// Removes `member`; true if it was there.
    pub(crate) fn remove(&mut self, member: &[u8]) -> bool {
        let found = match &self.repr {
            SetRepr::Ints(p) => parse_i64(member).and_then(|_| Set::find_int(p, member).ok()),
            SetRepr::Small(p) => Set::find(p, member),
            SetRepr::Table(_) => None,
        };
        match &mut self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => match found {
                Some(at) => {
                    p.remove_range(at.whole(), 1);
                    true
                }
                None => false,
            },
            SetRepr::Table(t) => t.remove_swap(member),
        }
    }

    /// The member at `pos` (below `len`), for picking one at random and
    /// for `SSCAN`.
    pub(crate) fn member_at(&self, pos: usize) -> &[u8] {
        match &self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => p.iter().nth(pos).expect("a position below len").1,
            SetRepr::Table(t) => t.get_index(pos).expect("a position below len").0,
        }
    }

    /// Removes and returns the member at `pos` (below `len`).
    pub(crate) fn take_at(&mut self, pos: usize) -> Box<[u8]> {
        match &mut self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => {
                let (at, m) = p.iter().nth(pos).expect("a position below len");
                let m = Box::from(m);
                p.remove_range(at.whole(), 1);
                m
            }
            SetRepr::Table(t) => t.remove_swap_at(pos),
        }
    }

    /// The members in Redis's order: ascending numbers while an intset,
    /// otherwise the order they're held in (which Redis leaves unspecified).
    pub fn members(&self) -> Vec<&[u8]> {
        self.iter().collect()
    }

    /// [`members`](Self::members), one at a time.
    pub fn iter(&self) -> SetIter<'_> {
        SetIter(match &self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => SetIterRepr::Packed(p.iter()),
            SetRepr::Table(t) => SetIterRepr::Table(t, 0),
        })
    }

    /// Frees up to `n` members of a set taken out of the store, leaving it
    /// unusable; true once it's all freed.
    pub(crate) fn free_last(&mut self, n: usize) -> bool {
        match &mut self.repr {
            SetRepr::Ints(p) | SetRepr::Small(p) => {
                *p = Packed::default();
                true
            }
            SetRepr::Table(t) => t.free_last(n),
        }
    }
}

/// [`Set::iter`].
pub struct SetIter<'a>(SetIterRepr<'a>);

enum SetIterRepr<'a> {
    Packed(crate::packed::Iter<'a>),
    /// The table and the next position.
    Table(&'a Table, usize),
}

impl<'a> Iterator for SetIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match &mut self.0 {
            SetIterRepr::Packed(items) => items.next().map(|(_, m)| m),
            SetIterRepr::Table(t, pos) => {
                let (m, _) = t.get_index(*pos)?;
                *pos += 1;
                Some(m)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match &self.0 {
            SetIterRepr::Packed(items) => items.len(),
            SetIterRepr::Table(t, pos) => t.len() - pos,
        };
        (n, Some(n))
    }
}

impl ExactSizeIterator for SetIter<'_> {}

/// The bytes a list node holds before the next element starts a new one:
/// Redis's default `list-max-listpack-size -2`, 8 KiB. An element larger
/// than that has a node of its own.
pub const LIST_NODE_BYTES: usize = 8192;

/// A list: packed nodes of up to [`LIST_NODE_BYTES`], as Redis's quicklist
/// (Redis 7.0.15 always calls a list that). Pushing and popping at either
/// end is O(1) (a node's bytes move, as in a listpack); reaching an
/// element by its position walks the nodes from the nearer end.
#[derive(Debug, Default)]
pub struct List {
    nodes: VecDeque<Packed>,
    len: usize,
}

impl List {
    pub fn len(&self) -> usize {
        self.len
    }

    /// Once true, the store deletes the key.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn encoding(&self) -> &'static str {
        "quicklist"
    }

    /// The element at `index`, counted from the end if negative (as
    /// `LINDEX` takes it).
    pub fn get(&self, index: i64) -> Option<&[u8]> {
        let (n, at) = self.locate(self.resolve(index)?);
        Some(self.nodes[n].get(at))
    }

    /// `index` as a position, if it's in the list.
    pub(crate) fn resolve(&self, index: i64) -> Option<usize> {
        let len = self.len as i64;
        let i = if index < 0 { index.checked_add(len)? } else { index };
        (0..len).contains(&i).then_some(i as usize)
    }

    /// `start..=stop` as Redis's `LRANGE` and `LTRIM` read them (negative
    /// from the end, clamped); `None` if it holds nothing.
    pub fn range(&self, start: i64, stop: i64) -> Option<(usize, usize)> {
        let len = self.len as i64;
        let start = if start < 0 { (start.saturating_add(len)).max(0) } else { start };
        let stop = if stop < 0 { stop.saturating_add(len) } else { stop.min(len - 1) };
        (start <= stop && start < len).then_some((start as usize, stop as usize))
    }

    /// The elements at positions `from..=to`.
    pub fn slice(&self, from: usize, to: usize) -> ListIter<'_> {
        let mut it = self.iter_from(from);
        it.left = to + 1 - from;
        it
    }

    /// The elements in order.
    pub fn iter(&self) -> ListIter<'_> {
        self.iter_from(0)
    }

    /// The elements from position `from` (at most the length) on.
    pub fn iter_from(&self, from: usize) -> ListIter<'_> {
        if from >= self.len {
            return ListIter { nodes: &self.nodes, node: self.nodes.len(), inner: None, left: 0 };
        }
        let (n, within) = self.node_of(from);
        let mut inner = self.nodes[n].iter();
        if within > 0 {
            inner.nth(within - 1);
        }
        ListIter { nodes: &self.nodes, node: n, inner: Some(inner), left: self.len - from }
    }

    /// The elements from the last to the first.
    pub fn iter_rev(&self) -> impl Iterator<Item = &[u8]> {
        self.nodes.iter().rev().flat_map(|node| node.iter().rev().map(|(_, e)| e))
    }

    /// The node holding position `i` (below the length), and `i` within
    /// it: counted from the nearer end.
    fn node_of(&self, i: usize) -> (usize, usize) {
        if i < self.len / 2 {
            let mut i = i;
            for (n, node) in self.nodes.iter().enumerate() {
                if i < node.len() {
                    return (n, i);
                }
                i -= node.len();
            }
        } else {
            let mut from_end = self.len - 1 - i;
            for (n, node) in self.nodes.iter().enumerate().rev() {
                if from_end < node.len() {
                    return (n, node.len() - 1 - from_end);
                }
                from_end -= node.len();
            }
        }
        unreachable!("a position below the length")
    }

    /// The node and place of position `i` (below the length).
    fn locate(&self, i: usize) -> (usize, At) {
        let (n, within) = self.node_of(i);
        let node = &self.nodes[n];
        let at = if within < node.len() / 2 {
            node.iter().nth(within)
        } else {
            node.iter().nth_back(node.len() - 1 - within)
        };
        (n, at.expect("within the node").0)
    }

    /// Whether `node` has room for an element of `size` bytes.
    fn room(node: &Packed, size: usize) -> bool {
        node.len() == 0 || node.bytes() + size + 2 <= LIST_NODE_BYTES
    }

    pub(crate) fn push_back(&mut self, item: &[u8]) {
        match self.nodes.back_mut() {
            Some(node) if List::room(node, item.len()) => node.push(item),
            _ => {
                let mut node = Packed::default();
                node.push(item);
                self.nodes.push_back(node);
            }
        }
        self.len += 1;
    }

    pub(crate) fn push_front(&mut self, item: &[u8]) {
        match self.nodes.front_mut() {
            Some(node) if List::room(node, item.len()) => node.insert_at(0, item),
            _ => {
                let mut node = Packed::default();
                node.push(item);
                self.nodes.push_front(node);
            }
        }
        self.len += 1;
    }

    pub(crate) fn pop_front(&mut self) -> Option<Box<[u8]>> {
        let node = self.nodes.front_mut()?;
        let (at, e) = node.iter().next().expect("no empty node");
        let e = Box::from(e);
        node.remove_range(at.whole(), 1);
        if node.len() == 0 {
            self.nodes.pop_front();
        }
        self.len -= 1;
        Some(e)
    }

    pub(crate) fn pop_back(&mut self) -> Option<Box<[u8]>> {
        let node = self.nodes.back_mut()?;
        let (at, e) = node.iter().next_back().expect("no empty node");
        let e = Box::from(e);
        node.remove_range(at.whole(), 1);
        if node.len() == 0 {
            self.nodes.pop_back();
        }
        self.len -= 1;
        Some(e)
    }

    /// Sets position `i` (below the length) to `value`.
    pub(crate) fn set(&mut self, i: usize, value: &[u8]) {
        let (n, at) = self.locate(i);
        self.nodes[n].replace(at, value);
        self.split_if_large(n);
    }

    /// Inserts `item` before position `i` (the length: at the end).
    pub(crate) fn insert(&mut self, i: usize, item: &[u8]) {
        if i == self.len {
            return self.push_back(item);
        }
        let (n, at) = self.locate(i);
        self.nodes[n].insert_at(at.whole().start, item);
        self.len += 1;
        self.split_if_large(n);
    }

    /// Cuts node `n` in two halves if it has grown past twice a node's size
    /// (an insertion or a longer element in its middle).
    fn split_if_large(&mut self, n: usize) {
        let node = &self.nodes[n];
        if node.bytes() <= 2 * LIST_NODE_BYTES || node.len() < 2 {
            return;
        }
        let half = node.len() / 2;
        let (mut first, mut second) = (Packed::default(), Packed::default());
        for (k, (_, e)) in node.iter().enumerate() {
            if k < half { first.push(e) } else { second.push(e) }
        }
        self.nodes[n] = first;
        self.nodes.insert(n + 1, second);
    }

    /// Removes the elements equal to `element`, at most `limit`, the first
    /// ones or (`from_tail`) the last ones: how many.
    pub(crate) fn remove_matching(&mut self, element: &[u8], limit: usize, from_tail: bool) -> usize {
        let mut removed = 0;
        let order: Vec<usize> = if from_tail { (0..self.nodes.len()).rev().collect() } else { (0..self.nodes.len()).collect() };
        for n in order {
            if removed >= limit {
                break;
            }
            let node = &self.nodes[n];
            let mut hits: Vec<usize> = node.iter().enumerate().filter(|(_, (_, e))| *e == element).map(|(k, _)| k).collect();
            if hits.is_empty() {
                continue;
            }
            let take = hits.len().min(limit - removed);
            if from_tail {
                hits.drain(..hits.len() - take);
            } else {
                hits.truncate(take);
            }
            let mut kept = Packed::default();
            let mut next = hits.iter().peekable();
            for (k, (_, e)) in node.iter().enumerate() {
                if next.peek() == Some(&&k) {
                    next.next();
                } else {
                    kept.push(e);
                }
            }
            self.nodes[n] = kept;
            removed += take;
        }
        self.nodes.retain(|node| node.len() > 0);
        self.len -= removed;
        removed
    }

    /// Keeps positions `from..=to` (below the length) only.
    pub(crate) fn trim(&mut self, from: usize, to: usize) {
        let back = self.len - 1 - to;
        self.drop_back(back);
        self.drop_front(from);
    }

    pub(crate) fn clear(&mut self) {
        self.nodes = VecDeque::new();
        self.len = 0;
    }

    /// Removes the first `n` elements (at most the length).
    fn drop_front(&mut self, mut n: usize) {
        self.len -= n;
        while n > 0 {
            let node = self.nodes.front_mut().expect("n within the length");
            if node.len() <= n {
                n -= node.len();
                self.nodes.pop_front();
            } else {
                let (at, _) = node.iter().nth(n).expect("within the node");
                node.remove_range(0..at.whole().start, n);
                n = 0;
            }
        }
    }

    /// Removes the last `n` elements (at most the length).
    fn drop_back(&mut self, mut n: usize) {
        self.len -= n;
        while n > 0 {
            let node = self.nodes.back_mut().expect("n within the length");
            if node.len() <= n {
                n -= node.len();
                self.nodes.pop_back();
            } else {
                let (at, _) = node.iter().nth_back(n - 1).expect("within the node");
                let end = node.bytes();
                node.remove_range(at.whole().start..end, n);
                n = 0;
            }
        }
    }

    /// Frees up to `n` elements, a node at a time, of a list taken out of
    /// the store; true once it's all freed.
    pub(crate) fn free_last(&mut self, n: usize) -> bool {
        let mut freed = 0;
        while freed < n.max(1) {
            match self.nodes.pop_back() {
                Some(node) => freed += node.len(),
                None => break,
            }
        }
        if self.nodes.is_empty() {
            self.nodes = VecDeque::new();
        }
        self.nodes.is_empty()
    }
}

/// [`List::iter`], [`List::slice`]: the elements in order.
pub struct ListIter<'a> {
    nodes: &'a VecDeque<Packed>,
    node: usize,
    inner: Option<crate::packed::Iter<'a>>,
    left: usize,
}

impl<'a> Iterator for ListIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        loop {
            if let Some((_, e)) = self.inner.as_mut().and_then(Iterator::next) {
                self.left -= 1;
                return Some(e);
            }
            self.node += 1;
            self.inner = Some(self.nodes.get(self.node)?.iter());
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl ExactSizeIterator for ListIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hash_converts_once_like_redis() {
        let mut h = Hash::default();
        for n in 0..HASH_MAX_LISTPACK_ENTRIES {
            assert!(h.insert(format!("f{n}").as_bytes(), b"v"));
        }
        assert_eq!(h.encoding(), "listpack");
        assert!(!h.insert(b"f0", b"w"), "an update isn't new");
        h.insert(b"one-more", b"v");
        assert_eq!(h.encoding(), "hashtable");
        h.remove(b"one-more");
        assert_eq!(h.encoding(), "hashtable", "Redis never converts back");

        let mut h = Hash::default();
        h.insert(b"f", &[b'x'; HASH_MAX_LISTPACK_VALUE]);
        assert_eq!(h.encoding(), "listpack");
        h.insert(b"g", &[b'x'; HASH_MAX_LISTPACK_VALUE + 1]);
        assert_eq!(h.encoding(), "hashtable");
    }

    #[test]
    fn a_small_hash_keeps_insertion_order_on_deletion() {
        let mut h = Hash::default();
        for f in [&b"c"[..], b"a", b"d", b"b"] {
            h.insert(f, f);
        }
        h.remove(b"a");
        h.insert(b"c", b"C");
        let got: Vec<(&[u8], &[u8])> = h.iter().collect();
        assert_eq!(got, [(&b"c"[..], &b"C"[..]), (b"d", b"d"), (b"b", b"b")]);
    }

    #[test]
    fn an_intset_reads_out_sorted() {
        let mut s = Set::default();
        for m in [&b"10"[..], b"-3", b"7", b"100"] {
            assert!(s.insert(m));
        }
        assert!(!s.insert(b"7"));
        assert_eq!(s.encoding(), "intset");
        assert_eq!(s.members(), [&b"-3"[..], b"7", b"10", b"100"]);
        s.remove(b"-3");
        assert_eq!(s.members(), [&b"7"[..], b"10", b"100"]);
        // Not canonical integers: Redis's string2ll refuses them.
        for m in [&b"07"[..], b"+1", b" 1", b"a"] {
            let mut s = Set::default();
            s.insert(b"1");
            s.insert(m);
            assert_eq!(s.encoding(), "hashtable", "{m:?}");
        }
    }

    #[test]
    fn an_intset_converts_past_its_size() {
        let mut s = Set::default();
        for n in 0..SET_MAX_INTSET_ENTRIES {
            s.insert(n.to_string().as_bytes());
        }
        assert_eq!(s.encoding(), "intset");
        s.insert(b"0");
        assert_eq!(s.encoding(), "intset", "a member already there adds nothing");
        s.insert(b"100000");
        assert_eq!(s.encoding(), "hashtable");
    }

    #[test]
    fn integers_compare_by_value() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut nums: Vec<i64> = vec![0, 1, -1, 9, 10, -9, -10, i64::MAX, i64::MIN, 99, 100, -99, -100];
        for _ in 0..2000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            nums.push((x as i64) >> (x % 60));
        }
        for &a in &nums {
            for &b in nums.iter().take(40) {
                let (sa, sb) = (a.to_string(), b.to_string());
                assert_eq!(cmp_ints(sa.as_bytes(), sb.as_bytes()), a.cmp(&b), "{a} {b}");
            }
        }
    }

    fn repr(s: &Set) -> &'static str {
        match s.repr {
            SetRepr::Ints(_) => "ints",
            SetRepr::Small(_) => "small",
            SetRepr::Table(_) => "table",
        }
    }

    #[test]
    fn sets_change_form_where_redis_would() {
        // Integers: an intset up to 512, then the table.
        let mut s = Set::default();
        for n in (0..512).rev() {
            s.insert(format!("{}", n * 7 - 1000).as_bytes());
        }
        assert_eq!((repr(&s), s.encoding()), ("ints", "intset"));
        let sorted: Vec<i64> = s.members().iter().map(|m| std::str::from_utf8(m).unwrap().parse().unwrap()).collect();
        assert!(sorted.windows(2).all(|w| w[0] < w[1]), "numeric order");
        s.insert(b"5000");
        assert_eq!((repr(&s), s.encoding()), ("table", "hashtable"));
        // A few integers, then a string: packed, called a hashtable.
        let mut s = Set::default();
        for m in [&b"3"[..], b"1", b"2"] {
            s.insert(m);
        }
        s.insert(b"x");
        assert_eq!((repr(&s), s.encoding()), ("small", "hashtable"));
        assert_eq!(s.members(), [&b"1"[..], b"2", b"3", b"x"], "the intset's order, then the new one");
        assert!(s.contains(b"2") && s.contains(b"x") && !s.contains(b"02"));
        // Up to 128 members of up to 64 bytes.
        for n in 0..124 {
            s.insert(format!("m{n}").as_bytes());
        }
        assert_eq!((s.len(), repr(&s)), (128, "small"));
        assert!(!s.insert(b"m0"), "a member already there changes nothing");
        assert_eq!(repr(&s), "small");
        s.insert(b"one more");
        assert_eq!((s.len(), repr(&s)), (129, "table"));
        let mut s = Set::default();
        s.insert(b"x");
        s.insert(&[b'y'; SET_MAX_PACKED_VALUE]);
        assert_eq!(repr(&s), "small");
        s.insert(&[b'y'; SET_MAX_PACKED_VALUE + 1]);
        assert_eq!(repr(&s), "table");
        // An intset meeting a long member goes straight to the table.
        let mut s = Set::default();
        s.insert(b"1");
        s.insert(&[b'y'; SET_MAX_PACKED_VALUE + 1]);
        assert_eq!(repr(&s), "table");
    }

    /// Random adds and removes against a `HashSet`, through every form.
    #[test]
    fn a_set_matches_a_hash_set() {
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        for round in 0..300 {
            let mut s = Set::default();
            let mut m: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for _ in 0..(round * 5) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // Mostly integers; strings and long members now and then.
                let member: Vec<u8> = match x % 50 {
                    0 if round % 3 == 0 => format!("s{}", x % 7).into_bytes(),
                    1 if round % 7 == 0 => vec![b'L'; 65 + (x % 3) as usize],
                    _ => format!("{}", (x >> 8) as i64 % 2000 - 1000).into_bytes(),
                };
                if x & 0x300 == 0 {
                    assert_eq!(s.remove(&member), m.remove(&member));
                } else {
                    assert_eq!(s.insert(&member), m.insert(member.clone()));
                }
                assert_eq!(s.len(), m.len());
            }
            let mut got: Vec<Vec<u8>> = s.members().iter().map(|m| m.to_vec()).collect();
            if !s.is_big() {
                let nums: Vec<i64> = got.iter().map(|m| std::str::from_utf8(m).unwrap().parse().unwrap()).collect();
                assert!(nums.windows(2).all(|w| w[0] < w[1]), "an intset in numeric order");
            }
            got.sort();
            let mut want: Vec<Vec<u8>> = m.into_iter().collect();
            want.sort();
            assert_eq!(got, want, "round {round} ({})", repr(&s));
            for (pos, member) in s.members().iter().enumerate() {
                assert_eq!(s.member_at(pos), *member);
            }
        }
    }

    /// The list's nodes: none empty, the counts adding up.
    fn check_nodes(l: &List) {
        assert!(l.nodes.iter().all(|n| n.len() > 0), "an empty node");
        assert_eq!(l.nodes.iter().map(Packed::len).sum::<usize>(), l.len());
    }

    /// Random operations on a list against a `VecDeque`, with elements
    /// from empty to larger than a node, through many nodes.
    #[test]
    fn a_list_matches_a_deque() {
        let mut l = List::default();
        let mut m: VecDeque<Vec<u8>> = VecDeque::new();
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut most = 0;
        for round in 0..30_000u32 {
            let r = next();
            let size = match r % 100 {
                0 => 3000 + (r >> 8) as usize % 20_000,
                1..=20 => (r >> 8) as usize % 200,
                _ => (r >> 8) as usize % 12,
            };
            // Few distinct contents, so that LREM finds some.
            let e: Vec<u8> = vec![b'a' + (r >> 20) as u8 % 4; size];
            let pos = |len: usize| if len == 0 { 0 } else { (r >> 32) as usize % len };
            match (r >> 40) % 16 {
                0..=3 => {
                    l.push_back(&e);
                    m.push_back(e);
                }
                4..=6 => {
                    l.push_front(&e);
                    m.push_front(e);
                }
                7 => assert_eq!(l.pop_front().map(Vec::from), m.pop_front()),
                8 => assert_eq!(l.pop_back().map(Vec::from), m.pop_back()),
                9 if !m.is_empty() => {
                    let i = pos(m.len());
                    l.set(i, &e);
                    m[i] = e;
                }
                10 => {
                    let i = pos(m.len() + 1);
                    l.insert(i, &e);
                    m.insert(i, e);
                }
                11 => {
                    let limit = 1 + (r >> 50) as usize % 3;
                    let from_tail = r & 1 == 1;
                    let mut removed = 0;
                    if from_tail {
                        let mut i = m.len();
                        while i > 0 && removed < limit {
                            i -= 1;
                            if m[i] == e {
                                m.remove(i);
                                removed += 1;
                            }
                        }
                    } else {
                        let mut i = 0;
                        while i < m.len() && removed < limit {
                            if m[i] == e {
                                m.remove(i);
                                removed += 1;
                            } else {
                                i += 1;
                            }
                        }
                    }
                    assert_eq!(l.remove_matching(&e, limit, from_tail), removed);
                }
                12 if m.len() > 200 => {
                    let (from, to) = (pos(m.len() / 4), m.len() - 1 - pos(m.len() / 4));
                    l.trim(from, to);
                    m.truncate(to + 1);
                    m.drain(..from);
                }
                _ if !m.is_empty() => {
                    let i = pos(m.len());
                    assert_eq!(l.get(i as i64), Some(&m[i][..]), "round {round}");
                    let got: Vec<&[u8]> = l.iter_from(i).take(5).collect();
                    let want: Vec<&[u8]> = m.range(i..).take(5).map(|e| &e[..]).collect();
                    assert_eq!(got, want);
                }
                _ => {}
            }
            assert_eq!(l.len(), m.len());
            most = most.max(l.nodes.len());
            if round % 499 == 0 {
                check_nodes(&l);
                assert!(l.iter().eq(m.iter().map(|e| &e[..])));
                assert!(l.iter_rev().eq(m.iter().rev().map(|e| &e[..])));
                assert_eq!(l.iter().len(), m.len());
            }
        }
        check_nodes(&l);
        assert!(most > 20, "the list had {most} nodes at most");
        assert!(l.iter().eq(m.iter().map(|e| &e[..])));
    }
}
