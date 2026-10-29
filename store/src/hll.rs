//! Redis's HyperLogLog, as far as `SUNIONCARD ... APPROX` needs it: the
//! registers an element sets and the cardinality they estimate, computed
//! exactly as Redis 8 computes them (`hyperloglog.c`), so that the
//! estimate is Redis's to the unit.
//!
//! 16384 registers (`HLL_P` 14); an element's 64-bit MurmurHash64A (seed
//! `0xadc83b19`) picks a register with its low 14 bits, and the register
//! keeps the longest run of zero bits plus one seen in the other 50. The
//! estimate is Otmar Ertl's ("New cardinality estimation algorithms for
//! HyperLogLog sketches", arXiv:1702.01284), from the registers' histogram.
//! Redis keeps registers in 6 bits, sparse or dense; their values, and so
//! the estimate, are the same as these.

const P: u32 = 14;
const Q: u32 = 64 - P;
const REGISTERS: usize = 1 << P;
/// 0.5 / ln(2).
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

/// MurmurHash2, 64-bit (`MurmurHash64A`), reading little-endian words as
/// Redis's endian-neutral version does.
pub(crate) fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let (words, tail) = key.as_chunks::<8>();
    for word in words {
        let mut k = u64::from_le_bytes(*word);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        for (i, &b) in tail.iter().enumerate() {
            h ^= u64::from(b) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// The registers of one HyperLogLog.
pub(crate) struct Hll {
    registers: Box<[u8; REGISTERS]>,
}

impl Hll {
    pub(crate) fn new() -> Hll {
        Hll { registers: Box::new([0; REGISTERS]) }
    }

    /// Adds `element` (Redis's `hllAdd`): its register keeps the larger of
    /// its value and the element's run (`hllPatLen`).
    pub(crate) fn add(&mut self, element: &[u8]) {
        let hash = murmur64a(element, 0xadc8_3b19);
        let index = (hash & (REGISTERS as u64 - 1)) as usize;
        let run = ((hash >> P) | (1 << Q)).trailing_zeros() as u8 + 1;
        let register = &mut self.registers[index];
        *register = (*register).max(run);
    }

    /// The estimated cardinality (Redis's `hllCount`).
    pub(crate) fn count(&self) -> u64 {
        let m = REGISTERS as f64;
        let mut histogram = [0u32; 64];
        for &r in self.registers.iter() {
            histogram[usize::from(r)] += 1;
        }
        let mut z = m * tau((m - f64::from(histogram[Q as usize + 1])) / m);
        for j in (1..=Q as usize).rev() {
            z += f64::from(histogram[j]);
            z *= 0.5;
        }
        z += m * sigma(f64::from(histogram[0]) / m);
        // llroundl: half away from zero, as f64::round.
        (ALPHA_INF * m * m / z).round() as u64
    }
}

/// Ertl's sigma (`hllSigma`).
fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            return z;
        }
    }
}

/// Ertl's tau (`hllTau`).
fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        // pow(1 - x, 2) is the square, correctly rounded, as a product is.
        z -= (1.0 - x) * (1.0 - x) * y;
        if z_prime == z {
            return z / 3.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur_matches_the_reference() {
        // Vectors of the reference C implementation (MurmurHash2.cpp):
        // empty, tails of 1 to 3 bytes, a whole word with tails of 4 to 6.
        for (key, seed, hash) in [
            (&b""[..], 0x9747b28c, 0x8397626cd6895052),
            (b"T", 0, 0x7a307fb51b9741cd),
            (b"Th", 0xffffffffffffffff, 0xde60778698f36157),
            (b"The", 0, 0x27ba74c43db50f9f),
            (b"The quick br", 0, 0x84cee032a4391865),
            (b"The quick bro", 0, 0xad423398ec714cf2),
            (b"The quick brow", 0, 0x870042965902b2c6),
        ] {
            assert_eq!(murmur64a(key, seed), hash, "{key:?}");
        }
    }

    #[test]
    fn counts_close_to_the_truth() {
        let mut h = Hll::new();
        assert_eq!(h.count(), 0);
        for n in 0..100_000u32 {
            h.add(n.to_string().as_bytes());
        }
        let c = h.count() as f64;
        assert!((c - 100_000.0).abs() < 2_000.0, "{c}");
        // Adding the same elements again changes nothing.
        let before = h.count();
        for n in 0..1000u32 {
            h.add(n.to_string().as_bytes());
        }
        assert_eq!(h.count(), before);
    }
}
