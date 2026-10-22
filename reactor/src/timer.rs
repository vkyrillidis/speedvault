//! Deadlines of waiting fibers: a min-heap ordered by deadline, plus a slab
//! saying which heap entries still count.
//!
//! Cancelling a timer only removes its slab slot; the heap entry stays and is
//! skipped when it reaches the top (its token no longer matches). Removing
//! from the middle of a binary heap would cost O(n). So that cancelled
//! entries can't pile up (short waits with long timeouts, cancelled every
//! time), the heap is rebuilt once they outnumber the live ones.

use crate::slab::Slab;
use crate::Token;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

/// Below this size the heap is never compacted: not worth it.
const MIN_COMPACT: usize = 64;

pub(crate) struct Timers {
    /// (deadline, sequence number, timer token). The sequence number makes
    /// timers with equal deadlines fire in the order they were added.
    heap: BinaryHeap<Reverse<(Instant, u64, u64)>>,
    /// Per live timer: the task to wake.
    live: Slab<Token>,
    seq: u64,
}

impl Timers {
    pub(crate) fn new() -> Timers {
        Timers { heap: BinaryHeap::new(), live: Slab::new(), seq: 0 }
    }

    /// Wakes `task` at `deadline`, unless cancelled before.
    pub(crate) fn add(&mut self, deadline: Instant, task: Token) -> Token {
        let timer = self.live.insert(task);
        self.seq += 1;
        self.heap.push(Reverse((deadline, self.seq, timer.to_u64())));
        timer
    }

    /// Harmless if the timer has fired or was cancelled already.
    pub(crate) fn cancel(&mut self, timer: Token) {
        if self.live.remove(timer).is_some() {
            self.compact_if_mostly_cancelled();
        }
    }

    /// The earliest deadline of a live timer.
    pub(crate) fn next_deadline(&mut self) -> Option<Instant> {
        while let Some(Reverse((deadline, _, timer))) = self.heap.peek() {
            if self.live.contains(Token::from_u64(*timer)) {
                return Some(*deadline);
            }
            self.heap.pop();
        }
        None
    }

    /// Removes every live timer due at `now` and calls `wake` with its task,
    /// earliest first.
    pub(crate) fn expire(&mut self, now: Instant, mut wake: impl FnMut(Token)) {
        while let Some(Reverse((deadline, _, timer))) = self.heap.peek() {
            if *deadline > now {
                break;
            }
            let timer = Token::from_u64(*timer);
            self.heap.pop();
            if let Some(task) = self.live.remove(timer) {
                wake(task);
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.live.len() == 0
    }

    /// Number of live timers.
    pub(crate) fn len(&self) -> usize {
        self.live.len()
    }

    /// Heap entries, cancelled ones included.
    #[cfg(test)]
    fn heap_len(&self) -> usize {
        self.heap.len()
    }

    fn compact_if_mostly_cancelled(&mut self) {
        if self.heap.len() >= MIN_COMPACT && self.heap.len() > 2 * self.live.len() {
            let live = &self.live;
            self.heap.retain(|Reverse((_, _, timer))| live.contains(Token::from_u64(*timer)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn task(i: u32) -> Token {
        Token::new(i, 0)
    }

    fn expired(timers: &mut Timers, now: Instant) -> Vec<u32> {
        let mut woken = Vec::new();
        timers.expire(now, |t| woken.push(t.index()));
        woken
    }

    #[test]
    fn fire_in_deadline_order_then_insertion_order() {
        let t0 = Instant::now();
        let ms = Duration::from_millis;
        let mut timers = Timers::new();
        timers.add(t0 + ms(30), task(3));
        timers.add(t0 + ms(10), task(1));
        timers.add(t0 + ms(20), task(2));
        timers.add(t0 + ms(10), task(11)); // same deadline as task 1, added later
        assert_eq!(timers.next_deadline(), Some(t0 + ms(10)));
        assert_eq!(expired(&mut timers, t0 + ms(20)), [1, 11, 2]);
        assert_eq!(timers.next_deadline(), Some(t0 + ms(30)));
        assert_eq!(expired(&mut timers, t0 + ms(29)), [] as [u32; 0]);
        assert_eq!(expired(&mut timers, t0 + ms(30)), [3]);
        assert!(timers.is_empty());
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn cancelled_timers_never_fire() {
        let t0 = Instant::now();
        let mut timers = Timers::new();
        let a = timers.add(t0, task(1));
        timers.add(t0 + Duration::from_secs(1), task(2));
        timers.cancel(a);
        timers.cancel(a); // twice is harmless
        assert_eq!(timers.len(), 1);
        assert_eq!(timers.next_deadline(), Some(t0 + Duration::from_secs(1)), "skips the cancelled one");
        assert_eq!(expired(&mut timers, t0 + Duration::from_secs(1)), [2]);
    }

    #[test]
    fn a_reused_slot_does_not_revive_a_cancelled_entry() {
        let t0 = Instant::now();
        let mut timers = Timers::new();
        let old = timers.add(t0, task(1));
        timers.cancel(old);
        // Same slab slot, new generation, later deadline.
        let new = timers.add(t0 + Duration::from_secs(1), task(2));
        assert_eq!(new.index(), old.index());
        assert_eq!(expired(&mut timers, t0), [] as [u32; 0], "the old heap entry is stale");
        assert_eq!(expired(&mut timers, t0 + Duration::from_secs(1)), [2]);
    }

    #[test]
    fn heap_is_compacted_when_mostly_cancelled() {
        // A wait with a long timeout that always ends early: arm, cancel,
        // repeat. The heap must not grow without bound.
        let far = Instant::now() + Duration::from_secs(3600);
        let mut timers = Timers::new();
        let keep = timers.add(far, task(0));
        for i in 0..100_000 {
            let t = timers.add(far, task(i));
            timers.cancel(t);
        }
        assert!(timers.heap_len() <= MIN_COMPACT, "heap has {} entries", timers.heap_len());
        assert_eq!(timers.len(), 1);
        timers.cancel(keep);
        assert!(timers.is_empty());
    }
}
