//! The list commands' parsing and replies (Redis 7.0's `t_list.c`); the
//! store does the work. As in Redis, arguments are parsed before the key
//! is looked up, so a bad argument beats `WRONGTYPE`.

use super::{arity_error, integer_arg, integer_reply, store_error, syntax_error, Flow};
use resp::{Args, Encoder};
use store::{parse_i64, End, List, LposOptions, Store};

const MUST_BE_POSITIVE: &[u8] = b"ERR value is out of range, must be positive";

/// `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`.
pub(super) fn push(args: &Args, store: &mut Store, out: &mut Encoder, end: End, only_existing: bool) -> Flow {
    let elements: Vec<&[u8]> = args.iter().skip(2).collect();
    integer_reply(store.push(&args[1], end, &elements, only_existing).map(|n| n as i64), out)
}

/// `LPOP key [count]`, `RPOP key [count]`.
pub(super) fn pop(args: &Args, store: &mut Store, out: &mut Encoder, end: End, name: &str) -> Flow {
    let count = match args.len() {
        2 => None,
        3 => match parse_i64(&args[2]) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => {
                out.error(MUST_BE_POSITIVE);
                return Flow::Continue;
            }
        },
        _ => {
            arity_error(name, out);
            return Flow::Continue;
        }
    };
    match (store.pop(&args[1], end, count.unwrap_or(1)), count) {
        (Ok(None), None) => out.null(),
        (Ok(None), Some(_)) => out.null_array(),
        (Ok(Some(popped)), None) => out.bulk(&popped[0]),
        (Ok(Some(popped)), Some(_)) => {
            out.array(popped.len());
            for e in popped {
                out.bulk(&e);
            }
        }
        (Err(e), _) => store_error(e, out),
    }
    Flow::Continue
}

/// The list at `args[1]`, or the reply for a key of another type.
fn list<'s>(args: &Args, store: &'s mut Store, out: &mut Encoder) -> Result<Option<&'s List>, ()> {
    store.list_at(&args[1]).map_err(|e| store_error(e, out))
}

pub(super) fn llen(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(l) = list(args, store, out) {
        out.integer(l.map_or(0, |l| l.len() as i64));
    }
    Flow::Continue
}

pub(super) fn lindex(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let index = match integer_arg(&args[2]) {
        Ok(i) => i,
        Err(e) => {
            store_error(e, out);
            return Flow::Continue;
        }
    };
    if let Ok(l) = list(args, store, out) {
        match l.and_then(|l| l.get(index)) {
            Some(e) => out.bulk(e),
            None => out.null(),
        }
    }
    Flow::Continue
}

/// Two integer arguments, from `args[i]`, or the reply for a bad one.
fn two_integers(args: &Args, i: usize, out: &mut Encoder) -> Option<(i64, i64)> {
    match (integer_arg(&args[i]), integer_arg(&args[i + 1])) {
        (Ok(a), Ok(b)) => Some((a, b)),
        (Err(e), _) | (_, Err(e)) => {
            store_error(e, out);
            None
        }
    }
}

pub(super) fn lrange(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some((start, stop)) = two_integers(args, 2, out) else { return Flow::Continue };
    if let Ok(l) = list(args, store, out) {
        match l.and_then(|l| l.range(start, stop).map(|(from, to)| l.slice(from, to))) {
            Some(elements) => {
                out.array(elements.len());
                for e in elements {
                    out.bulk(e);
                }
            }
            None => out.array(0),
        }
    }
    Flow::Continue
}

pub(super) fn ltrim(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some((start, stop)) = two_integers(args, 2, out) else { return Flow::Continue };
    match store.ltrim(&args[1], start, stop) {
        Ok(()) => out.ok(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn lset(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match integer_arg(&args[2]).and_then(|index| store.lset(&args[1], index, &args[3])) {
        Ok(()) => out.ok(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn linsert(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let after = if args[2].eq_ignore_ascii_case(b"after") {
        true
    } else if args[2].eq_ignore_ascii_case(b"before") {
        false
    } else {
        syntax_error(out);
        return Flow::Continue;
    };
    integer_reply(store.linsert(&args[1], after, &args[3], &args[4]), out)
}

pub(super) fn lrem(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(integer_arg(&args[2]).and_then(|count| store.lrem(&args[1], count, &args[3])).map(|n| n as i64), out)
}

/// `LPOS key element [RANK rank] [COUNT num] [MAXLEN len]`.
pub(super) fn lpos(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let mut opts = LposOptions::default();
    let mut with_count = false;
    let mut i = 3;
    while i < args.len() {
        let opt = &args[i];
        let Some(arg) = args.get(i + 1) else {
            syntax_error(out);
            return Flow::Continue;
        };
        let n = parse_i64(arg);
        if opt.eq_ignore_ascii_case(b"rank") {
            match n {
                None => return error(store::Error::NotInteger.to_string().as_bytes(), out),
                // Redis 7.0.15 negates it (overflowing, still negative),
                // so every match from the tail qualifies: as -1.
                Some(i64::MIN) => opts.rank = -1,
                Some(0) => return error(b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list", out),
                Some(r) => opts.rank = r,
            }
        } else if opt.eq_ignore_ascii_case(b"count") {
            match n {
                Some(c) if c >= 0 => opts.count = c as usize,
                _ => return error(b"ERR COUNT can't be negative", out),
            }
            with_count = true;
        } else if opt.eq_ignore_ascii_case(b"maxlen") {
            match n {
                Some(m) if m >= 0 => opts.maxlen = m as usize,
                _ => return error(b"ERR MAXLEN can't be negative", out),
            }
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
        i += 2;
    }
    if !with_count {
        opts.count = 1;
    }
    match store.lpos(&args[1], &args[2], opts) {
        Ok(found) if with_count => {
            out.array(found.len());
            for p in found {
                out.integer(p as i64);
            }
        }
        Ok(found) => match found.first() {
            Some(&p) => out.integer(p as i64),
            None => out.null(),
        },
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

fn error(message: &[u8], out: &mut Encoder) -> Flow {
    out.error(message);
    Flow::Continue
}

fn end_arg(arg: &[u8]) -> Option<End> {
    if arg.eq_ignore_ascii_case(b"left") {
        Some(End::Left)
    } else if arg.eq_ignore_ascii_case(b"right") {
        Some(End::Right)
    } else {
        None
    }
}

/// `LMOVE src dst LEFT|RIGHT LEFT|RIGHT`; `RPOPLPUSH src dst`.
pub(super) fn lmove(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let ends = if args.len() == 3 { Some((End::Right, End::Left)) } else { end_arg(&args[3]).zip(end_arg(&args[4])) };
    let Some((from, to)) = ends else {
        syntax_error(out);
        return Flow::Continue;
    };
    match store.lmove(&args[1], &args[2], from, to) {
        Ok(Some(e)) => out.bulk(&e),
        Ok(None) => out.null(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}
