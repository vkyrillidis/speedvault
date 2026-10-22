//! `SCAN`, `HSCAN`, `SSCAN`, `KEYS` (Redis 7.0's `db.c`: `scanGenericCommand`):
//! parsing and replies; the store walks.

use super::{store_error, syntax_error, Flow};
use resp::{Args, Encoder};
use store::{parse_i64, Store};

/// A cursor as Redis's `strtoul` reads it: decimal, an optional sign (a
/// negative one wraps), no spaces; the empty string is 0.
fn cursor(arg: &[u8]) -> Option<u64> {
    let (negative, digits) = match arg.split_first() {
        None => return Some(0),
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        Some(_) => (false, arg),
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
}

/// The options from `args[from..]`; `TYPE` only if `with_type` (`SCAN`).
/// `None` once the error is replied.
fn options<'a>(args: &Args<'a>, from: usize, with_type: bool, out: &mut Encoder) -> Option<Options<'a>> {
    let mut opts = Options { count: 10, pattern: None, kind: None };
    let mut i = from;
    while i < args.len() {
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
    let Some(opts) = options(args, 2, true, out) else { return Flow::Continue };
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
    let Some(opts) = options(args, 3, false, out) else { return Flow::Continue };
    match store.hscan(&args[1], c, opts.count, opts.pattern) {
        Ok((next, pairs)) => reply(next, 2 * pairs.len(), pairs.iter().flat_map(|(f, v)| [&f[..], &v[..]]), out),
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
    let Some(opts) = options(args, 3, false, out) else { return Flow::Continue };
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
    fn cursors_as_strtoul_reads_them() {
        assert_eq!(cursor(b"0"), Some(0));
        assert_eq!(cursor(b""), Some(0));
        assert_eq!(cursor(b"+7"), Some(7));
        assert_eq!(cursor(b"-1"), Some(u64::MAX));
        assert_eq!(cursor(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(cursor(b"18446744073709551616"), None);
        assert_eq!(cursor(b" 1"), None);
        assert_eq!(cursor(b"1 "), None);
        assert_eq!(cursor(b"x"), None);
        assert_eq!(cursor(b"-"), None);
    }
}
