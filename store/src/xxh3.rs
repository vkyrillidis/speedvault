//! XXH3, 64-bit, unseeded (`XXH3_64bits`), as Redis 8 hashes a string for
//! `DIGEST` and the `IFDEQ`/`IFDNE` conditions of `SET` and `DELEX`: a port
//! of the scalar code of xxHash 0.8 (`deps/xxhash/xxhash.h` in Redis), with
//! its default secret. Inputs of up to 16, 128 and 240 bytes each have a
//! path of their own; longer ones are read in 64-byte stripes into eight
//! accumulators, scrambled after every 1024 bytes, then merged.

const PRIME32_1: u64 = 0x9E37_79B1;
const PRIME32_2: u64 = 0x85EB_CA77;
const PRIME32_3: u64 = 0xC2B2_AE3D;
const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;
const PRIME_MX1: u64 = 0x1656_6791_9E37_79F9;
const PRIME_MX2: u64 = 0x9FB2_1C65_1E98_DF25;

const SECRET: [u8; 192] = [
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, //
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, //
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21, //
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, //
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3, //
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8, //
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d, //
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64, //
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb, //
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e, //
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce, //
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e, //
];
/// The smallest secret the 129-to-240-byte path reads from.
const SECRET_SIZE_MIN: usize = 136;
const STRIPE_LEN: usize = 64;
const SECRET_CONSUME_RATE: usize = 8;

fn read32(b: &[u8], at: usize) -> u64 {
    u64::from(u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes")))
}

fn read64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

/// The low 64 bits of the 128-bit product XOR'd with the high ones.
fn mul128_fold64(lhs: u64, rhs: u64) -> u64 {
    let product = u128::from(lhs) * u128::from(rhs);
    (product as u64) ^ ((product >> 64) as u64)
}

fn xorshift64(v: u64, shift: u32) -> u64 {
    v ^ (v >> shift)
}

fn avalanche(h: u64) -> u64 {
    xorshift64(xorshift64(h, 37).wrapping_mul(PRIME_MX1), 32)
}

fn xxh64_avalanche(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(PRIME64_2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME64_3);
    h ^ (h >> 32)
}

fn rrmxmx(mut h: u64, len: u64) -> u64 {
    h ^= h.rotate_left(49) ^ h.rotate_left(24);
    h = h.wrapping_mul(PRIME_MX2);
    h ^= (h >> 35).wrapping_add(len);
    h = h.wrapping_mul(PRIME_MX2);
    xorshift64(h, 28)
}

fn mix16(input: &[u8], at: usize, secret_at: usize) -> u64 {
    mul128_fold64(read64(input, at) ^ read64(&SECRET, secret_at), read64(input, at + 8) ^ read64(&SECRET, secret_at + 8))
}

/// `XXH3_64bits`.
pub(crate) fn xxh3_64(input: &[u8]) -> u64 {
    let len = input.len();
    match len {
        0 => xxh64_avalanche(read64(&SECRET, 56) ^ read64(&SECRET, 64)),
        1..=3 => {
            let combined = (u64::from(input[0]) << 16) | (u64::from(input[len >> 1]) << 24) | u64::from(input[len - 1]) | ((len as u64) << 8);
            xxh64_avalanche(combined ^ (read32(&SECRET, 0) ^ read32(&SECRET, 4)))
        }
        4..=8 => {
            let input64 = read32(input, len - 4).wrapping_add(read32(input, 0) << 32);
            rrmxmx(input64 ^ (read64(&SECRET, 8) ^ read64(&SECRET, 16)), len as u64)
        }
        9..=16 => {
            let lo = read64(input, 0) ^ (read64(&SECRET, 24) ^ read64(&SECRET, 32));
            let hi = read64(input, len - 8) ^ (read64(&SECRET, 40) ^ read64(&SECRET, 48));
            avalanche((len as u64).wrapping_add(lo.swap_bytes()).wrapping_add(hi).wrapping_add(mul128_fold64(lo, hi)))
        }
        17..=128 => {
            let mut acc = (len as u64).wrapping_mul(PRIME64_1);
            // From the outside in: 16 bytes at each end, for every 32 of length.
            for i in (0..=(len - 1) / 32).rev() {
                acc = acc.wrapping_add(mix16(input, 16 * i, 32 * i));
                acc = acc.wrapping_add(mix16(input, len - 16 * (i + 1), 32 * i + 16));
            }
            avalanche(acc)
        }
        129..=240 => {
            let mut acc = (len as u64).wrapping_mul(PRIME64_1);
            for i in 0..8 {
                acc = acc.wrapping_add(mix16(input, 16 * i, 16 * i));
            }
            let mut acc_end = mix16(input, len - 16, SECRET_SIZE_MIN - 17);
            let acc = avalanche(acc);
            for i in 8..len / 16 {
                acc_end = acc_end.wrapping_add(mix16(input, 16 * i, 16 * (i - 8) + 3));
            }
            avalanche(acc.wrapping_add(acc_end))
        }
        _ => hash_long(input),
    }
}

/// One 64-byte stripe into the accumulators (`XXH3_accumulate_512`).
fn accumulate_512(acc: &mut [u64; 8], input: &[u8], at: usize, secret_at: usize) {
    for lane in 0..8 {
        let data = read64(input, at + 8 * lane);
        let key = data ^ read64(&SECRET, secret_at + 8 * lane);
        acc[lane ^ 1] = acc[lane ^ 1].wrapping_add(data);
        acc[lane] = (key & 0xFFFF_FFFF).wrapping_mul(key >> 32).wrapping_add(acc[lane]);
    }
}

fn scramble(acc: &mut [u64; 8], secret_at: usize) {
    for (lane, a) in acc.iter_mut().enumerate() {
        *a = (xorshift64(*a, 47) ^ read64(&SECRET, secret_at + 8 * lane)).wrapping_mul(PRIME32_1);
    }
}

/// Inputs past 240 bytes (`XXH3_hashLong_64b`).
fn hash_long(input: &[u8]) -> u64 {
    let len = input.len();
    let mut acc = [PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1];
    let stripes_per_block = (SECRET.len() - STRIPE_LEN) / SECRET_CONSUME_RATE;
    let block_len = STRIPE_LEN * stripes_per_block;
    let blocks = (len - 1) / block_len;
    for n in 0..blocks {
        for s in 0..stripes_per_block {
            accumulate_512(&mut acc, input, n * block_len + s * STRIPE_LEN, s * SECRET_CONSUME_RATE);
        }
        scramble(&mut acc, SECRET.len() - STRIPE_LEN);
    }
    let stripes = ((len - 1) - block_len * blocks) / STRIPE_LEN;
    for s in 0..stripes {
        accumulate_512(&mut acc, input, blocks * block_len + s * STRIPE_LEN, s * SECRET_CONSUME_RATE);
    }
    // The last stripe, against a secret not aligned with the others.
    accumulate_512(&mut acc, input, len - STRIPE_LEN, SECRET.len() - STRIPE_LEN - 7);
    let mut result = (len as u64).wrapping_mul(PRIME64_1);
    for i in 0..4 {
        result = result.wrapping_add(mul128_fold64(acc[2 * i] ^ read64(&SECRET, 11 + 16 * i), acc[2 * i + 1] ^ read64(&SECRET, 11 + 16 * i + 8)));
    }
    avalanche(result)
}

/// Redis's digest of a string: the hash as 16 lowercase hex digits.
pub(crate) fn digest(value: &[u8]) -> [u8; 16] {
    let mut out = [0; 16];
    let hex = format!("{:016x}", xxh3_64(value));
    out.copy_from_slice(hex.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_values() {
        // xxHash's own: XXH3_64bits of the empty input.
        assert_eq!(xxh3_64(b""), 0x2D06_8005_38D3_94C2);
    }
}
