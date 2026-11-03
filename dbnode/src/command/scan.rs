//! `SCAN`, `HSCAN`, `SSCAN`, `KEYS` (Redis 7.0's `db.c`: `scanGenericCommand`):
//! parsing and replies; the store walks.

use super::{c_str, store_error, syntax_error, Flow};
use resp::{Args, Encoder};
use store::{parse_i64, Store};

/// A cursor as Redis 8's `string2ull` reads it (a C string, so up to a
/// NUL): an integer as `string2ll` takes it, refused if negative; else as
/// `strtoull` reads it, whitespace first, an optional sign (a negative one
/// wraps), then digits to the end.
fn cursor(arg: &[u8]) -> Option<u64> {
    let arg = c_str(arg, usize::MAX);
    if let Some(n) = parse_i64(arg) {
        return u64::try_from(n).ok();
    }
    // C's isspace: Rust's ASCII whitespace and the vertical tab.
    let skip = arg.iter().take_while(|&&b| b.is_ascii_whitespace() || b == 0x0b).count();
    let trimmed = &arg[skip..];
    let (negative, digits) = match trimmed.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, trimmed),
    };
    if digits.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    Some(if negative { n.wrapping_neg() } else { n })
}

struct Options<'a> {
    count: usize,
    pattern: Option<&'a [u8]>,
    kind: Option<&'a [u8]>,
    /// `HSCAN ... NOVALUES`: the fields only.
    no_values: bool,
}

/// Which command reads the options: `SCAN` takes `TYPE`, `HSCAN` `NOVALUES`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scanning {
    Keys,
    Hash,
    Other,
}

/// The options from `args[from..]`, as `scanGenericCommand` reads them.
/// `None` once the error is replied.
fn options<'a>(args: &Args<'a>, from: usize, scanning: Scanning, out: &mut Encoder) -> Option<Options<'a>> {
    let with_type = scanning == Scanning::Keys;
    let mut opts = Options { count: 10, pattern: None, kind: None, no_values: false };
    let mut i = from;
    while i < args.len() {
        // NOVALUES stands alone, and may come last.
        if args[i].eq_ignore_ascii_case(b"novalues") {
            if scanning != Scanning::Hash {
                out.error(b"ERR NOVALUES option can only be used in HSCAN");
                return None;
            }
            opts.no_values = true;
            i += 1;
            continue;
        }
        let (opt, value) = (args.get(i)?, args.get(i + 1));
        match value {
            Some(v) if opt.eq_ignore_ascii_case(b"count") => match parse_i64(v) {
                None => {
                    store_error(store::Error::NotInteger, out);
                    return None;
                }
                Some(n) if n < 1 => {
                    syntax_error(out);
                    return None;
                }
                Some(n) => opts.count = usize::try_from(n).unwrap_or(usize::MAX),
            },
            Some(v) if opt.eq_ignore_ascii_case(b"match") => opts.pattern = Some(v),
            Some(v) if with_type && opt.eq_ignore_ascii_case(b"type") => opts.kind = Some(v),
            _ => {
                syntax_error(out);
                return None;
            }
        }
        i += 2;
    }
    Some(opts)
}

fn invalid_cursor(out: &mut Encoder) -> Flow {
    out.error(b"ERR invalid cursor");
    Flow::Continue
}

/// `[cursor, [elements]]`.
fn reply<'e>(next: u64, len: usize, elements: impl Iterator<Item = &'e [u8]>, out: &mut Encoder) {
    out.array(2);
    out.bulk(next.to_string().as_bytes());
    out.array(len);
    for e in elements {
        out.bulk(e);
    }
}

pub(super) fn scan(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(c) = cursor(&args[1]) else { return invalid_cursor(out) };
    let Some(opts) = options(args, 2, Scanning::Keys, out) else { return Flow::Continue };
    let (next, keys) = store.scan(c, opts.count, opts.pattern, opts.kind);
    reply(next, keys.len(), keys.iter().map(|k| &k[..]), out);
    Flow::Continue
}

/// The reply for a missing key: done, nothing.
fn empty_scan(out: &mut Encoder) -> Flow {
    reply(0, 0, std::iter::empty(), out);
    Flow::Continue
}

/// `HSCAN`: as Redis, the cursor, then the key (missing: an empty reply,
/// whatever the options), then the options.
pub(super) fn hscan(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(c) = cursor(&args[2]) else { return invalid_cursor(out) };
    match store.hash(&args[1]) {
        Err(e) => {
            store_error(e, out);
            return Flow::Continue;
        }
        Ok(None) => return empty_scan(out),
        Ok(Some(_)) => {}
    }
    let Some(opts) = options(args, 3, Scanning::Hash, out) else { return Flow::Continue };
    match store.hscan(&args[1], c, opts.count, opts.pattern) {
        Ok((next, pairs)) if opts.no_values => reply(next, pairs.len(), pairs.iter().map(|(f, _)| &f[..]), out),
        Ok((next, pairs)) => reply(next, 2 * pairs.len(), pairs.iter().flat_map(|(f, v)| [&f[..], &v[..]]), out),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `ZSCAN`, as `HSCAN`: each member with its score, written as Redis
/// writes it: as packed (`d2string`) while the set is, `%.17Lg` once large.
pub(super) fn zscan(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(c) = cursor(&args[2]) else { return invalid_cursor(out) };
    match store.zset_at(&args[1]) {
        Err(e) => {
            store_error(e, out);
            return Flow::Continue;
        }
        Ok(None) => return empty_scan(out),
        Ok(Some(_)) => {}
    }
    let Some(opts) = options(args, 3, Scanning::Other, out) else { return Flow::Continue };
    match store.zscan(&args[1], c, opts.count, opts.pattern) {
        Ok((next, members, big)) => {
            let scores: Vec<Vec<u8>> = members
                .iter()
                .map(|&(_, s)| if big { resp::format_double(s, &mut [0; 32]).to_vec() } else { store::double_to_bytes(s) })
                .collect();
            reply(next, 2 * members.len(), members.iter().zip(&scores).flat_map(|((m, _), s)| [&m[..], &s[..]]), out);
        }
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `SSCAN`, as `HSCAN`.
pub(super) fn sscan(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(c) = cursor(&args[2]) else { return invalid_cursor(out) };
    match store.set_at(&args[1]) {
        Err(e) => {
            store_error(e, out);
            return Flow::Continue;
        }
        Ok(None) => return empty_scan(out),
        Ok(Some(_)) => {}
    }
    let Some(opts) = options(args, 3, Scanning::Other, out) else { return Flow::Continue };
    match store.sscan(&args[1], c, opts.count, opts.pattern) {
        Ok((next, members)) => reply(next, members.len(), members.iter().map(|m| &m[..]), out),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn keys(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let keys = store.keys_matching(&args[1]);
    out.array(keys.len());
    for k in keys {
        out.bulk(k);
    }
    Flow::Continue
}

#[cfg(test)]
mod tests {
    use super::cursor;

    #[test]
    fn cursors_as_redis_8_reads_them() {
        // Redis 8.10.2: string2ll, negatives refused; else strtoull.
        assert_eq!(cursor(b"0"), Some(0));
        assert_eq!(cursor(b"00"), Some(0));
        assert_eq!(cursor(b"-0"), Some(0));
        assert_eq!(cursor(b""), None);
        assert_eq!(cursor(b"+7"), Some(7));
        assert_eq!(cursor(b"-1"), None);
        assert_eq!(cursor(b"-9223372036854775808"), None);
        assert_eq!(cursor(b"9223372036854775808"), Some(1 << 63));
        assert_eq!(cursor(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(cursor(b"18446744073709551616"), None);
        assert_eq!(cursor(b" 1"), Some(1));
        assert_eq!(cursor(b"\t\x0b\n1"), Some(1));
        assert_eq!(cursor(b" -5"), Some(5u64.wrapping_neg()));
        assert_eq!(cursor(b"- 5"), None);
        assert_eq!(cursor(b"5\0abc"), Some(5));
        assert_eq!(cursor(b"1 "), None);
        assert_eq!(cursor(b"0x10"), None);
        assert_eq!(cursor(b"x"), None);
        assert_eq!(cursor(b"-"), None);
    }
}
