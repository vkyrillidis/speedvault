//! Integers as Redis reads and writes them in string values.

/// Parses a 64-bit integer the way Redis's `string2ll` does, which is what
/// decides whether `INCR` accepts a value (and how the dbnode should parse
/// integer arguments): optional `-`, then digits, no leading zeros, no `+`,
/// no spaces, at most 20 bytes, within `i64`. `"0"` is fine, `"-0"` is not.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (negative, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    match digits.first() {
        Some(b'1'..=b'9') => {}
        _ => return None,
    }
    let mut v: u64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    if negative {
        // -(2^63) is i64::MIN; its magnitude doesn't fit an i64.
        (v <= 1 << 63).then(|| (v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

/// Replaces the contents of `out` with `n` in decimal, keeping its buffer.
pub(crate) fn write_i64(out: &mut Vec<u8>, n: i64) {
    out.clear();
    out.extend_from_slice(format_i64(&mut [0; 20], n));
}

/// `n` in decimal, written at the end of `buf` (20 bytes fit `i64::MIN`).
pub(crate) fn format_i64(buf: &mut [u8; 20], n: i64) -> &[u8] {
    let mut i = buf.len();
    let mut v = n.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if n < 0 {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_what_redis_accepts() {
        // Checked against Redis 7.0.15 with SET n <s> + INCR n.
        for (s, v) in [
            ("0", 0),
            ("1", 1),
            ("-1", -1),
            ("123", 123),
            ("9223372036854775807", i64::MAX),
            ("-9223372036854775808", i64::MIN),
        ] {
            assert_eq!(parse_i64(s.as_bytes()), Some(v), "{s}");
        }
    }

    #[test]
    fn parse_rejects_what_redis_rejects() {
        // Checked against Redis 7.0.15: each one makes INCR fail with
        // "ERR value is not an integer or out of range".
        for s in [
            "", " 1", "1 ", "+1", "-0", "01", "-01", "00000000000000000001", "-0000000000000000001", "1.0",
            "abc", "-", "9223372036854775808", "-9223372036854775809", "99999999999999999999",
            "184467440737095516160", "1e3", "0x10", "1-",
        ] {
            assert_eq!(parse_i64(s.as_bytes()), None, "{s:?}");
        }
    }

    #[test]
    fn write_round_trips() {
        let mut out = b"garbage that is longer".to_vec();
        for n in [0, 1, -1, 9, 10, -10, 4711, i64::MAX, i64::MIN] {
            write_i64(&mut out, n);
            assert_eq!(out, n.to_string().as_bytes());
            assert_eq!(parse_i64(&out), Some(n));
        }
    }
}
