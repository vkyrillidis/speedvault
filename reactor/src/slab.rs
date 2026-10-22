//! A slab: values in reusable slots, addressed by [`Token`]s.
//!
//! Removing a value bumps its slot's generation, so tokens handed out for the
//! old value stop matching, even after the slot is reused. Lookups are one
//! array index and one comparison.

use crate::Token;

pub(crate) struct Slab<T> {
    entries: Vec<Entry<T>>,
    /// Indices of empty entries, reused last-in first-out.
    free: Vec<u32>,
    len: usize,
}

struct Entry<T> {
    generation: u32,
    value: Option<T>,
}

impl<T> Slab<T> {
    pub(crate) fn new() -> Slab<T> {
        Slab { entries: Vec::new(), free: Vec::new(), len: 0 }
    }

    pub(crate) fn insert(&mut self, value: T) -> Token {
        self.len += 1;
        if let Some(index) = self.free.pop() {
            let entry = &mut self.entries[index as usize];
            entry.value = Some(value);
            return Token::new(index, entry.generation);
        }
        let index = u32::try_from(self.entries.len()).expect("slab full");
        self.entries.push(Entry { generation: 0, value: Some(value) });
        Token::new(index, 0)
    }

    /// The value `token` was handed out for, if it is still there.
    pub(crate) fn get_mut(&mut self, token: Token) -> Option<&mut T> {
        let entry = self.entries.get_mut(token.index() as usize)?;
        if entry.generation != token.generation() {
            return None;
        }
        entry.value.as_mut()
    }

    pub(crate) fn contains(&self, token: Token) -> bool {
        self.entries
            .get(token.index() as usize)
            .is_some_and(|e| e.generation == token.generation() && e.value.is_some())
    }

    pub(crate) fn remove(&mut self, token: Token) -> Option<T> {
        let entry = self.entries.get_mut(token.index() as usize)?;
        if entry.generation != token.generation() {
            return None;
        }
        let value = entry.value.take()?;
        // Wraps after 2^32 reuses of one slot; a token that old is long gone.
        entry.generation = entry.generation.wrapping_add(1);
        self.free.push(token.index());
        self.len -= 1;
        Some(value)
    }

    /// Removes all values (bumping their generations, like `remove`).
    pub(crate) fn drain(&mut self) -> Vec<T> {
        let mut values = Vec::with_capacity(self.len);
        for (index, entry) in self.entries.iter_mut().enumerate() {
            if let Some(value) = entry.value.take() {
                entry.generation = entry.generation.wrapping_add(1);
                self.free.push(index as u32);
                values.push(value);
            }
        }
        self.len = 0;
        values
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let mut slab = Slab::new();
        let a = slab.insert("a");
        let b = slab.insert("b");
        assert_eq!(slab.len(), 2);
        assert_eq!(slab.get_mut(a), Some(&mut "a"));
        assert_eq!(slab.remove(b), Some("b"));
        assert_eq!(slab.get_mut(b), None);
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn reused_slot_rejects_the_old_token() {
        let mut slab = Slab::new();
        let old = slab.insert(1);
        slab.remove(old);
        let new = slab.insert(2);
        assert_eq!(new.index(), old.index(), "slot is reused");
        assert_ne!(new.generation(), old.generation());
        assert_eq!(slab.get_mut(old), None, "stale token must not see the new value");
        assert_eq!(slab.remove(old), None);
        assert_eq!(slab.get_mut(new), Some(&mut 2));
    }

    #[test]
    fn double_remove_is_harmless() {
        let mut slab = Slab::new();
        let t = slab.insert(1);
        assert_eq!(slab.remove(t), Some(1));
        assert_eq!(slab.remove(t), None);
        assert_eq!(slab.len(), 0);
        // The slot went onto the free list once only.
        let a = slab.insert(2);
        let b = slab.insert(3);
        assert_ne!(a.index(), b.index());
    }

    #[test]
    fn unknown_index_is_none() {
        let mut slab: Slab<u8> = Slab::new();
        assert_eq!(slab.get_mut(Token::new(5, 0)), None);
        assert_eq!(slab.remove(Token::new(5, 0)), None);
    }

    #[test]
    fn drain_empties_and_invalidates() {
        let mut slab = Slab::new();
        let a = slab.insert(1);
        let b = slab.insert(2);
        let mut values = slab.drain();
        values.sort();
        assert_eq!(values, [1, 2]);
        assert_eq!(slab.len(), 0);
        assert_eq!(slab.get_mut(a), None);
        assert_eq!(slab.get_mut(b), None);
        slab.insert(3);
        slab.insert(4);
        assert_eq!(slab.len(), 2);
    }
}
