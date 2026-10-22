//! Shared by the integration tests: a tiny deterministic random number
//! generator (xorshift), so the tests need no dependencies.

#![allow(dead_code)] // each test file uses a different part

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    /// Bytes biased towards the ones the protocol cares about.
    pub fn byte(&mut self) -> u8 {
        const SPECIAL: &[u8] = b"\r\n*$:\"'\\ \t\x0b\0-0123456789xa";
        if self.chance(50) {
            SPECIAL[self.below(SPECIAL.len())]
        } else {
            self.next() as u8
        }
    }
}
