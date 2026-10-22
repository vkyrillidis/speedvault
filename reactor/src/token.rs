//! The 64 bits a registration carries through epoll.
//!
//! ```text
//!  bit 63            32 31                 0
//!     ┌────────────────┬───────────────────┐
//!     │   generation   │    slot index     │
//!     └────────────────┴───────────────────┘
//! ```
//!
//! The index says which slot of the reactor the event is for. The generation
//! is bumped every time a slot is freed, so an event that still carries the
//! old generation (the socket was closed after epoll reported it) no longer
//! matches the slot and is ignored, where a pointer in `epoll_data` would
//! point at freed memory.

/// A slot index and generation packed into one `u64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token(u64);

impl Token {
    /// Packs `index` into the low 32 bits and `generation` into the high 32.
    pub fn new(index: u32, generation: u32) -> Token {
        Token((generation as u64) << 32 | index as u64)
    }

    /// The slot index (low 32 bits).
    pub fn index(self) -> u32 {
        (self.0 & 0xFFFF_FFFF) as u32
    }

    /// The generation (high 32 bits).
    pub fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// The raw value to store in `epoll_event.u64`.
    pub fn to_u64(self) -> u64 {
        self.0
    }

    /// Rebuilds a token from the raw value epoll hands back.
    pub fn from_u64(raw: u64) -> Token {
        Token(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_index_and_generation() {
        let t = Token::new(5, 7);
        assert_eq!(t.index(), 5);
        assert_eq!(t.generation(), 7);
    }

    #[test]
    fn layout_is_generation_high_index_low() {
        assert_eq!(Token::new(1, 2).to_u64(), 0x0000_0002_0000_0001);
    }

    #[test]
    fn extreme_values_survive() {
        let t = Token::new(u32::MAX, u32::MAX);
        assert_eq!(t.index(), u32::MAX);
        assert_eq!(t.generation(), u32::MAX);
        assert_eq!(t.to_u64(), u64::MAX);
    }

    #[test]
    fn fields_do_not_bleed_into_each_other() {
        let only_index = Token::new(u32::MAX, 0);
        assert_eq!(only_index.generation(), 0);
        let only_generation = Token::new(0, u32::MAX);
        assert_eq!(only_generation.index(), 0);
    }

    #[test]
    fn raw_value_round_trips() {
        let t = Token::new(123_456, 42);
        assert_eq!(Token::from_u64(t.to_u64()), t);
    }

    #[test]
    fn a_new_generation_is_a_different_token() {
        // Same slot, reused after the old socket was closed: an event carrying
        // the old token must not be mistaken for the new one.
        assert_ne!(Token::new(3, 1), Token::new(3, 2));
    }
}
