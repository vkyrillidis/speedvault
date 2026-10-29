//! One heap block holding an entry of a large hash or set: its hash, its
//! key and its value, found through a single 8-byte pointer.
//!
//! A `Box<[u8]>` per key and per value cost a hash's field two heap blocks
//! and a 40-byte slot (a hash, two pointers with their lengths). Here the
//! slot is the pointer alone and the lengths sit at the front of the block:
//!
//! ```text
//! ┌──────────┬──────┬──────┬───────┬────────┐
//! │ hash u32 │ klen │ vlen │ key   │ value  │   klen, vlen: LEB128 (1 byte
//! └──────────┴──────┴──────┴───────┴────────┘   each up to 127)
//! ```
//!
//! A set's members are keys with an empty value. The block's size is read
//! back from its header, which is what makes the pointer thin: Rust's
//! `Box<[u8]>` keeps the length beside the pointer instead. That needs
//! `unsafe` to allocate and free; it's all in this module.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::fmt;
use std::ptr::NonNull;

const HASH: usize = 4;

/// An owned block: allocated by [`Block::new`], freed on drop.
pub(crate) struct Block(NonNull<u8>);

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

impl Block {
    pub(crate) fn new(hash: u32, key: &[u8], value: &[u8]) -> Block {
        let size = HASH + len_size(key.len()) + len_size(value.len()) + key.len() + value.len();
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(&hash.to_ne_bytes());
        for mut n in [key.len(), value.len()] {
            while n >= 0x80 {
                bytes.push((n as u8) | 0x80);
                n >>= 7;
            }
            bytes.push(n as u8);
        }
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        debug_assert_eq!(bytes.len(), size);
        // SAFETY: `size` is at least 6, so the layout isn't zero-sized; the
        // copy writes exactly the `size` bytes allocated.
        unsafe {
            let ptr = alloc(layout(size));
            let Some(ptr) = NonNull::new(ptr) else { handle_alloc_error(layout(size)) };
            ptr.as_ptr().copy_from_nonoverlapping(bytes.as_ptr(), size);
            Block(ptr)
        }
    }

    /// The header: where the key starts, and the key's and value's lengths.
    fn header(&self) -> (usize, usize, usize) {
        let mut at = HASH;
        let mut lens = [0; 2];
        for len in &mut lens {
            let mut shift = 0;
            loop {
                // SAFETY: the header was written whole by `new`, and these
                // reads stop at its last length byte (one without 0x80).
                let b = unsafe { *self.0.as_ptr().add(at) };
                at += 1;
                *len |= ((b & 0x7f) as usize) << shift;
                if b < 0x80 {
                    break;
                }
                shift += 7;
            }
        }
        (at, lens[0], lens[1])
    }

    /// All its bytes, the header included.
    fn bytes(&self) -> &[u8] {
        let (at, k, v) = self.header();
        // SAFETY: `new` allocated and wrote `at + k + v` bytes, which live as
        // long as `self`.
        unsafe { std::slice::from_raw_parts(self.0.as_ptr(), at + k + v) }
    }

    pub(crate) fn hash(&self) -> u32 {
        // SAFETY: the first `HASH` bytes are the hash; unaligned, as the
        // block has an alignment of 1.
        unsafe { self.0.as_ptr().cast::<u32>().read_unaligned() }
    }

    /// The key and the value.
    pub(crate) fn parts(&self) -> (&[u8], &[u8]) {
        let (at, k, _) = self.header();
        self.bytes()[at..].split_at(k)
    }

    pub(crate) fn key(&self) -> &[u8] {
        self.parts().0
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.parts().1
    }

    /// Writes `value` over the value, which must be as long.
    pub(crate) fn overwrite_value(&mut self, value: &[u8]) {
        let (at, k, v) = self.header();
        assert_eq!(v, value.len(), "a value of the same length");
        // SAFETY: the value's bytes are `at + k .. at + k + v` of the block,
        // and `&mut self` means nothing else is reading them.
        unsafe { self.0.as_ptr().add(at + k).copy_from_nonoverlapping(value.as_ptr(), v) }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let size = self.bytes().len();
        // SAFETY: allocated by `new` with this same layout, freed only here.
        unsafe { dealloc(self.0.as_ptr(), layout(size)) }
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (k, v) = self.parts();
        f.debug_struct("Block").field("key", &String::from_utf8_lossy(k)).field("value", &String::from_utf8_lossy(v)).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_its_parts() {
        for (k, v) in [(0, 0), (1, 0), (8, 8), (127, 128), (128, 127), (300, 20_000), (0, 70_000)] {
            let key: Vec<u8> = (0..k).map(|i| i as u8).collect();
            let value: Vec<u8> = (0..v).map(|i| (i * 7) as u8).collect();
            let b = Block::new(0xdead_beef, &key, &value);
            assert_eq!(b.hash(), 0xdead_beef);
            assert_eq!(b.parts(), (&key[..], &value[..]));
            assert_eq!(b.bytes().len(), HASH + len_size(k) + len_size(v) + k + v);
        }
    }

    #[test]
    fn overwrites_a_value_in_place() {
        let mut b = Block::new(7, b"field", b"12345");
        b.overwrite_value(b"54321");
        assert_eq!(b.parts(), (&b"field"[..], &b"54321"[..]));
        assert_eq!(b.hash(), 7);
    }

    #[test]
    #[should_panic]
    fn refuses_a_value_of_another_length() {
        Block::new(7, b"field", b"1").overwrite_value(b"22");
    }

    #[test]
    fn is_one_pointer() {
        assert_eq!(std::mem::size_of::<Block>(), 8);
        assert_eq!(std::mem::size_of::<Option<Block>>(), 8);
    }
}
