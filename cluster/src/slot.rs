//! Key to slot: CRC16 and hash tags, as Redis's `cluster.c` (`keyHashSlot`)
//! and `crc16.c` compute them.

/// The number of hash slots in a Redis Cluster.
pub const SLOTS: usize = 16384;

/// CRC16-XMODEM (polynomial 0x1021, initial value 0, no reflection), one
/// table lookup per byte.
const TABLE: [u16; 256] = {
    let mut t = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
            bit += 1;
        }
        t[i] = crc;
        i += 1;
    }
    t
};

/// Slicing by 8: `SLICES[k][b]` is the CRC of byte `b` followed by `k`
/// zero bytes. The CRC is linear, so 8 bytes' CRC is the XOR of their 8
/// lookups (the running CRC folded into the first two): 8 independent loads
/// instead of a chain of 8 dependent ones. `SLICES[0]` is `TABLE`.
const SLICES: [[u16; 256]; 8] = {
    let mut t = [[0; 256]; 8];
    t[0] = TABLE;
    let mut k = 1;
    while k < 8 {
        let mut b = 0;
        while b < 256 {
            let prev = t[k - 1][b];
            t[k][b] = (prev << 8) ^ TABLE[(prev >> 8) as usize];
            b += 1;
        }
        k += 1;
    }
    t
};

/// CRC16-XMODEM of `data`, Redis's `crc16`: `crc16(b"123456789")` is 0x31C3.
/// Runs on every keyed command in cluster mode, so 8 bytes at a time.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    let (chunks, tail) = data.as_chunks::<8>();
    for c in chunks {
        let s = &SLICES;
        let (x0, x1) = (c[0] ^ (crc >> 8) as u8, c[1] ^ crc as u8);
        crc = s[7][usize::from(x0)]
            ^ s[6][usize::from(x1)]
            ^ s[5][usize::from(c[2])]
            ^ s[4][usize::from(c[3])]
            ^ s[3][usize::from(c[4])]
            ^ s[2][usize::from(c[5])]
            ^ s[1][usize::from(c[6])]
            ^ s[0][usize::from(c[7])];
    }
    for &b in tail {
        crc = (crc << 8) ^ TABLE[usize::from((crc >> 8) as u8 ^ b)];
    }
    crc
}

/// The slot of `key`, `0..16384`; binary safe.
///
/// If the key has a `{` and a `}` somewhere after it with at least one byte
/// in between, only the bytes between the first `{` and the first `}` after
/// it are hashed, so `{user42}:name` and `{user42}:email` share a slot.
/// Otherwise (no `{`, no `}` after it, or `{}`) the whole key is hashed:
/// `{}a{b}` hashes all six bytes, `{{a}}` hashes `{a`.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let hashed = match find(key, b'{') {
        Some(open) => match find(&key[open + 1..], b'}') {
            Some(len) if len > 0 => &key[open + 1..open + 1 + len],
            _ => key,
        },
        None => key,
    };
    crc16(hashed) & (SLOTS as u16 - 1)
}

/// The first `byte` in `data`, 8 bytes at a time: a byte of `x` is zero
/// where `data` has `byte`, and `(x - 0x01..) & !x & 0x80..` sets the top
/// bit of the lowest such byte (and maybe of bytes above it, never below).
fn find(data: &[u8], byte: u8) -> Option<usize> {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGHS: u64 = 0x8080_8080_8080_8080;
    let pattern = ONES * u64::from(byte);
    let (chunks, tail) = data.as_chunks::<8>();
    for (i, chunk) in chunks.iter().enumerate() {
        let x = u64::from_le_bytes(*chunk) ^ pattern;
        let found = x.wrapping_sub(ONES) & !x & HIGHS;
        if found != 0 {
            return Some(8 * i + (found.trailing_zeros() / 8) as usize);
        }
    }
    tail.iter().position(|&b| b == byte).map(|p| 8 * chunks.len() + p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_check_value() {
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(crc16(b""), 0);
        // Bit by bit, without the table.
        let slow = |data: &[u8]| {
            let mut crc = 0u16;
            for &b in data {
                crc ^= u16::from(b) << 8;
                for _ in 0..8 {
                    crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
                }
            }
            crc
        };
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(crc16(&all), slow(&all));
        assert_eq!(crc16(b"\xff\x00\x80\x7f"), slow(b"\xff\x00\x80\x7f"));
        // Every length around the 8-byte chunks, and every start in `all`
        // (so the bytes under each slice table vary).
        for start in 0..64 {
            for len in 0..=40 {
                let data = &all[start..start + len];
                assert_eq!(crc16(data), slow(data), "start {start}, len {len}");
            }
        }
    }

    #[test]
    fn find_is_position() {
        // Every placement of one or two '{' (and bytes that look close:
        // 0x7a, 0x7c, 0xfb, 0x00) in buffers around the 8-byte chunks.
        let fillers = [b'a', 0x7a, 0x7c, 0xfb, 0x00, 0xff];
        for len in 0..=33 {
            for &fill in &fillers {
                let mut data = vec![fill; len];
                assert_eq!(find(&data, b'{'), None, "len {len}");
                for i in 0..len {
                    data[i] = b'{';
                    assert_eq!(find(&data, b'{'), Some(i), "len {len}, at {i}");
                    for j in i + 1..len {
                        data[j] = b'{';
                        assert_eq!(find(&data, b'{'), Some(i), "len {len}, at {i} and {j}");
                        data[j] = fill;
                    }
                    data[i] = fill;
                }
            }
        }
        for byte in 0..=255u8 {
            let data: Vec<u8> = (0..=255).collect();
            assert_eq!(find(&data, byte), Some(usize::from(byte)));
        }
    }

    /// The slots Redis 7.0.15's `CLUSTER KEYSLOT` gives.
    #[test]
    fn known_slots() {
        let cases: &[(&[u8], u16)] = &[
            (b"foo", 12182),
            (b"bar", 5061),
            (b"123456789", 12739),
            (b"", 0),
            (b"a", 15495),
            (b"user{42}:name", 8000),
            (b"{user42}:email", 14710),
        ];
        for &(key, slot) in cases {
            assert_eq!(key_hash_slot(key), slot, "{}", key.escape_ascii());
        }
    }

    #[test]
    fn hash_tags() {
        // (key, what gets hashed, Redis's slot)
        let cases: &[(&[u8], &[u8], u16)] = &[
            (b"{}", b"{}", 15257),           // empty tag: the whole key
            (b"{", b"{", 4092),              // no '}'
            (b"}{", b"}{", 12793),           // '}' only before the '{'
            (b"{a}{b}", b"a", 15495),        // the first tag
            (b"a{b", b"a{b", 13340),         // unclosed
            (b"{{a}}", b"{a", 10276),        // up to the first '}'
            (b"{}a{b}", b"{}a{b}", 16175),   // the first '{' decides, even if empty
            (b"{a", b"{a", 10276),
            (b"{a}", b"a", 15495),
            (b"x{a}y", b"a", 15495),
            (b"\0{\xff\r\n}\0", b"\xff\r\n", crc16(b"\xff\r\n") & 0x3fff),
        ];
        for &(key, hashed, slot) in cases {
            assert_eq!(key_hash_slot(key), crc16(hashed) & 0x3fff, "{}", key.escape_ascii());
            assert_eq!(key_hash_slot(key), slot, "{}", key.escape_ascii());
        }
        assert_eq!(key_hash_slot(b"{user42}:name"), key_hash_slot(b"{user42}:email"));
    }
}
