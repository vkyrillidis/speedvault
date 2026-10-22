//! Byte strings back to back in one buffer: the compact encoding of small
//! collections and of a list's nodes (Redis's listpack, in spirit).
//!
//! Each element is its length (LEB128), its bytes, and its length again
//! written to be read backwards, so the buffer can be walked from either
//! end (`RPOP` takes the last element without walking the rest). An
//! element costs its bytes and two to ten bytes of length, instead of a
//! heap allocation of its own (at least 32 bytes with glibc's malloc) and a
//! pointer and length to find it. Finding one by its contents is a walk,
//! which is what Redis does too while a collection is small.

/// The strings, in order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Packed {
    bytes: Vec<u8>,
    len: usize,
}

/// Where an element is: its first byte, its bytes, and the byte after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct At {
    start: usize,
    data: usize,
    end: usize,
    next: usize,
}

impl At {
    /// The byte range of the element with its lengths.
    pub(crate) fn whole(self) -> std::ops::Range<usize> {
        self.start..self.next
    }
}

fn len_size(mut n: usize) -> usize {
    let mut size = 1;
    while n >= 0x80 {
        n >>= 7;
        size += 1;
    }
    size
}

/// Appends an element as it's stored: length, bytes, length backwards.
fn put(out: &mut Vec<u8>, item: &[u8]) {
    let mut n = item.len();
    let from = out.len();
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    let head = out.len() - from;
    out.extend_from_slice(item);
    // The same groups, highest first, all but the highest flagged: read
    // from the end, the lowest comes first and a flag says another precedes.
    for k in (0..head).rev() {
        let group = out[from + k] & 0x7f;
        out.push(if k == head - 1 { group } else { group | 0x80 });
    }
}

/// The stored size of an element of `n` bytes.
fn entry_size(n: usize) -> usize {
    n + 2 * len_size(n)
}

impl Packed {
    /// How many strings.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The bytes it holds, lengths included.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Room for `more` bytes: a little extra, not the doubling a `Vec`
    /// does by itself (a small collection stays small).
    fn reserve(&mut self, more: usize) {
        if self.bytes.capacity() - self.bytes.len() < more {
            self.bytes.reserve_exact(more.max(self.bytes.len() / 8));
        }
    }

    /// Adds `item` at the end.
    pub(crate) fn push(&mut self, item: &[u8]) {
        self.reserve(entry_size(item.len()));
        put(&mut self.bytes, item);
        self.len += 1;
    }

    /// Inserts `item` at byte offset `at` (an element's start, or the end).
    pub(crate) fn insert_at(&mut self, at: usize, item: &[u8]) {
        if at == self.bytes.len() {
            return self.push(item);
        }
        // Appended, then rotated into place: one move of the bytes after
        // `at`, no allocation besides the buffer's.
        let size = entry_size(item.len());
        self.reserve(size);
        put(&mut self.bytes, item);
        self.bytes[at..].rotate_right(size);
        self.len += 1;
    }

    /// Replaces the element at `at` with `item`.
    pub(crate) fn replace(&mut self, at: At, item: &[u8]) {
        if at.end - at.data == item.len() {
            self.bytes[at.data..at.end].copy_from_slice(item);
            return;
        }
        let mut entry = Vec::with_capacity(entry_size(item.len()));
        put(&mut entry, item);
        self.reserve(entry.len().saturating_sub(at.next - at.start));
        self.bytes.splice(at.whole(), entry);
    }

    /// Removes the elements in the byte range `range` (whole elements,
    /// `count` of them).
    pub(crate) fn remove_range(&mut self, range: std::ops::Range<usize>, count: usize) {
        self.bytes.drain(range);
        self.len -= count;
        if self.bytes.capacity() > 64 && self.bytes.len() * 2 < self.bytes.capacity() {
            self.bytes.shrink_to(self.bytes.len() + self.bytes.len() / 8);
        }
    }

    /// The element starting at byte offset `start`.
    pub(crate) fn at(&self, start: usize) -> At {
        let (mut n, mut shift, mut i) = (0usize, 0, start);
        loop {
            let b = self.bytes[i];
            n |= usize::from(b & 0x7f) << shift;
            i += 1;
            if b < 0x80 {
                break;
            }
            shift += 7;
        }
        let end = i + n;
        At { start, data: i, end, next: end + (i - start) }
    }

    /// The element ending just before byte offset `next`.
    pub(crate) fn before(&self, next: usize) -> At {
        let (mut n, mut shift, mut i) = (0usize, 0, next);
        loop {
            i -= 1;
            let b = self.bytes[i];
            n |= usize::from(b & 0x7f) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
        }
        let size = next - i;
        let end = i;
        let data = end - n;
        At { start: data - size, data, end, next }
    }

    /// The bytes of the element at `at`.
    pub(crate) fn get(&self, at: At) -> &[u8] {
        &self.bytes[at.data..at.end]
    }

    /// The elements in order, with where they are.
    pub(crate) fn iter(&self) -> Iter<'_> {
        Iter { packed: self, front: 0, back: self.bytes.len(), left: self.len }
    }
}

/// [`Packed::iter`], from both ends.
#[derive(Clone)]
pub(crate) struct Iter<'a> {
    packed: &'a Packed,
    front: usize,
    back: usize,
    left: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (At, &'a [u8]);

    fn next(&mut self) -> Option<(At, &'a [u8])> {
        if self.left == 0 {
            return None;
        }
        let at = self.packed.at(self.front);
        self.front = at.next;
        self.left -= 1;
        Some((at, self.packed.get(at)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let at = self.packed.before(self.back);
        self.back = at.start;
        self.left -= 1;
        Some((at, self.packed.get(at)))
    }
}

impl ExactSizeIterator for Iter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(p: &Packed) -> Vec<Vec<u8>> {
        p.iter().map(|(_, b)| b.to_vec()).collect()
    }

    fn items_back(p: &Packed) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = p.iter().rev().map(|(_, b)| b.to_vec()).collect();
        v.reverse();
        v
    }

    #[test]
    fn push_insert_replace_remove() {
        let mut p = Packed::default();
        let long = vec![b'x'; 300];
        p.push(b"a");
        p.push(&long);
        p.push(b"");
        assert_eq!(p.len(), 3);
        assert_eq!(p.bytes(), 3 + 304 + 2, "300 takes two length bytes, each way");
        assert_eq!(items(&p), [b"a".to_vec(), long.clone(), vec![]]);
        assert_eq!(items_back(&p), items(&p));
        let ats: Vec<At> = p.iter().map(|(at, _)| at).collect();
        p.replace(ats[0], b"b");
        p.replace(ats[2], b"tail");
        assert_eq!(items(&p), [b"b".to_vec(), long.clone(), b"tail".to_vec()]);
        let second = p.iter().nth(1).unwrap().0;
        p.replace(second, b"short");
        assert_eq!(items(&p), [b"b".to_vec(), b"short".to_vec(), b"tail".to_vec()]);
        let second = p.iter().nth(1).unwrap().0;
        p.insert_at(second.whole().start, &long);
        p.insert_at(0, b"first");
        p.insert_at(p.bytes(), b"last");
        let want = [b"first".to_vec(), b"b".to_vec(), long.clone(), b"short".to_vec(), b"tail".to_vec(), b"last".to_vec()];
        assert_eq!(items(&p), want);
        assert_eq!(items_back(&p), want);
        let (b, l) = (p.iter().nth(1).unwrap().0, p.iter().nth(2).unwrap().0);
        p.remove_range(b.whole().start..l.whole().end, 2);
        assert_eq!(items(&p), [b"first".to_vec(), b"short".to_vec(), b"tail".to_vec(), b"last".to_vec()]);
        assert_eq!(p.iter().len(), 4);
        // Both ends at once meet in the middle.
        let mut it = p.iter();
        assert_eq!(it.next().unwrap().1, b"first");
        assert_eq!(it.next_back().unwrap().1, b"last");
        assert_eq!(it.next_back().unwrap().1, b"tail");
        assert_eq!(it.next().unwrap().1, b"short");
        assert!(it.next().is_none() && it.next_back().is_none());
    }

    #[test]
    fn lengths_of_every_size_both_ways() {
        let mut p = Packed::default();
        let sizes = [0, 1, 127, 128, 129, 255, 16383, 16384, 100_000, 2_097_152];
        for n in sizes {
            p.push(&vec![7; n]);
        }
        let lens: Vec<usize> = p.iter().map(|(_, b)| b.len()).collect();
        assert_eq!(lens, sizes);
        let mut back: Vec<usize> = p.iter().rev().map(|(_, b)| b.len()).collect();
        back.reverse();
        assert_eq!(back, sizes);
        assert_eq!(p.bytes(), sizes.iter().map(|&n| entry_size(n)).sum::<usize>());
    }
}
