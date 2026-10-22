//! A pool of fiber stacks, so that starting a fiber doesn't need an
//! mmap/munmap pair (~1.5 µs) but just takes a stack off a list (~47 ns
//! for the whole fiber, see benches/switch.rs).
//!
//! ```text
//!   pool.get() ──▶ Fiber::new(stack, f) ──▶ ... ──▶ fiber.into_stack() ──▶ pool.put(stack)
//!       ▲                                                                       │
//!       └─────────────────────────── free list ◀────────────────────────────────┘
//! ```

use crate::stack::page_size;
use crate::Stack;
use std::io;

/// Keeps up to `max_free` unused stacks of one size around for reuse.
pub struct StackPool {
    free: Vec<Stack>,
    /// Usable size of the pool's stacks, rounded up to whole pages like
    /// `Stack::new` does, so it equals `size()` of the stacks we hand out.
    stack_size: usize,
    max_free: usize,
}

impl StackPool {
    /// Creates an empty pool handing out stacks with at least `stack_size`
    /// usable bytes, and keeping at most `max_free` unused ones.
    ///
    /// Nothing is allocated up front.
    pub fn new(stack_size: usize, max_free: usize) -> StackPool {
        let page = page_size();
        StackPool {
            free: Vec::with_capacity(max_free),
            stack_size: stack_size.max(1).div_ceil(page) * page,
            max_free,
        }
    }

    /// Takes a stack off the free list, or allocates a new one if the list
    /// is empty.
    pub fn get(&mut self) -> io::Result<Stack> {
        if let Some(stack) = self.free.pop() {
            Ok(stack)
        } else {
            Stack::new(self.stack_size)
        }
    }

    /// Gives a stack back to the pool. It is kept for reuse if the pool has
    /// room and the stack has the pool's size; otherwise it is dropped (and
    /// so unmapped).
    ///
    /// Careful: `Stack::new` rounds the size up to whole pages, so a stack
    /// from `get()` may be bigger than the `stack_size` given to `new()`.
    pub fn put(&mut self, stack: Stack) {
        if self.free.len() < self.max_free && stack.size() == self.stack_size {
            self.free.push(stack);
        }
    }

    /// Number of unused stacks currently in the pool.
    pub fn free_count(&self) -> usize {
        self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{suspend, Fiber, State};

    #[test]
    fn new_pool_is_empty() {
        let pool = StackPool::new(64 * 1024, 4);
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn get_on_empty_pool_allocates() {
        let mut pool = StackPool::new(64 * 1024, 4);
        let stack = pool.get().unwrap();
        assert!(stack.size() >= 64 * 1024);
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn put_then_get_reuses_the_same_stack() {
        let mut pool = StackPool::new(64 * 1024, 4);
        let stack = pool.get().unwrap();
        let top = stack.top();
        pool.put(stack);
        assert_eq!(pool.free_count(), 1);
        let again = pool.get().unwrap();
        assert_eq!(again.top(), top, "expected the same mapping back");
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn put_beyond_max_free_drops_the_stack() {
        let mut pool = StackPool::new(64 * 1024, 2);
        let stacks: Vec<Stack> = (0..3).map(|_| pool.get().unwrap()).collect();
        for s in stacks {
            pool.put(s);
        }
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn put_of_a_different_size_drops_the_stack() {
        let mut pool = StackPool::new(64 * 1024, 4);
        pool.put(Stack::new(128 * 1024).unwrap());
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn size_that_is_not_a_page_multiple_is_still_reused() {
        // 10_000 bytes is rounded up to whole pages by Stack::new.
        let mut pool = StackPool::new(10_000, 4);
        let stack = pool.get().unwrap();
        assert_eq!(stack.size() % page_size(), 0);
        pool.put(stack);
        assert_eq!(pool.free_count(), 1);
    }

    #[test]
    fn stack_survives_a_round_trip_through_a_fiber() {
        let mut pool = StackPool::new(64 * 1024, 4);
        for _ in 0..3 {
            let mut f = Fiber::new(pool.get().unwrap(), suspend);
            f.resume().unwrap();
            assert_eq!(f.state(), State::Suspended);
            pool.put(f.into_stack()); // cancels the suspended fiber first
            assert_eq!(pool.free_count(), 1);
        }
    }
}
