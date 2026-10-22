//! The set commands' parsing and replies (Redis 7.0's `t_set.c`); the
//! store does the work.

use super::{integer_reply, store_error, syntax_error, Flow};
use resp::{Args, Encoder};
use store::{parse_i64, Set, SetOp, Store};

fn members_of<'a>(args: &'a Args, from: usize) -> Vec<&'a [u8]> {
    args.iter().skip(from).collect()
}

pub(super) fn sadd(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.sadd(&args[1], &members_of(args, 2)).map(|n| n as i64), out)
}

pub(super) fn srem(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.srem(&args[1], &members_of(args, 2)).map(|n| n as i64), out)
}

/// The set at `args[1]`, or the reply for a key of another type.
fn set<'s>(args: &Args, store: &'s mut Store, out: &mut Encoder) -> Result<Option<&'s Set>, ()> {
    store.set_at(&args[1]).map_err(|e| store_error(e, out))
}

/// A set reply (an array in RESP2), in the set's order.
fn reply_members(set: Option<&Set>, out: &mut Encoder) {
    let members = set.map_or(Vec::new(), Set::members);
    out.set(members.len());
    for m in members {
        out.bulk(m);
    }
}

pub(super) fn smembers(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(s) = set(args, store, out) {
        reply_members(s, out);
    }
    Flow::Continue
}

pub(super) fn scard(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(s) = set(args, store, out) {
        out.integer(s.map_or(0, |s| s.len() as i64));
    }
    Flow::Continue
}

pub(super) fn sismember(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(s) = set(args, store, out) {
        out.integer(i64::from(s.is_some_and(|s| s.contains(&args[2]))));
    }
    Flow::Continue
}

pub(super) fn smismember(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(s) = set(args, store, out) {
        out.array(args.len() - 2);
        for m in args.iter().skip(2) {
            out.integer(i64::from(s.is_some_and(|s| s.contains(m))));
        }
    }
    Flow::Continue
}

/// `SPOP key [count]`.
pub(super) fn spop(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match args.len() {
        2 => match store.spop(&args[1]) {
            Ok(Some(m)) => out.bulk(&m),
            Ok(None) => out.null(),
            Err(e) => store_error(e, out),
        },
        3 => match parse_i64(&args[2]) {
            None | Some(i64::MIN..0) => out.error(b"ERR value is out of range, must be positive"),
            Some(n) => match store.spop_count(&args[1], n as usize) {
                Ok(members) => {
                    out.set(members.len());
                    for m in members {
                        out.bulk(&m);
                    }
                }
                Err(e) => store_error(e, out),
            },
        },
        _ => syntax_error(out),
    }
    Flow::Continue
}

/// `SRANDMEMBER key [count]`.
pub(super) fn srandmember(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match args.len() {
        2 => match store.srandmember(&args[1]) {
            Ok(Some(m)) => out.bulk(&m),
            Ok(None) => out.null(),
            Err(e) => store_error(e, out),
        },
        3 => match parse_i64(&args[2]) {
            None => store_error(store::Error::NotInteger, out),
            // Redis takes -LONG_MAX..LONG_MAX.
            Some(i64::MIN) => out.error(
                b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
            ),
            Some(n) => match store.srandmember_count(&args[1], n) {
                Ok(members) => {
                    out.array(members.len());
                    for m in members {
                        out.bulk(&m);
                    }
                }
                Err(e) => store_error(e, out),
            },
        },
        _ => syntax_error(out),
    }
    Flow::Continue
}

pub(super) fn smove(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.smove(&args[1], &args[2], &args[3]).map(i64::from), out)
}

/// `SUNION`, `SINTER`, `SDIFF`.
pub(super) fn combine(args: &Args, store: &mut Store, out: &mut Encoder, op: SetOp) -> Flow {
    match store.combine(op, &members_of(args, 1)) {
        Ok(result) => reply_members(Some(&result), out),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`.
pub(super) fn combine_store(args: &Args, store: &mut Store, out: &mut Encoder, op: SetOp) -> Flow {
    integer_reply(store.combine_store(op, &args[1], &members_of(args, 2)).map(|n| n as i64), out)
}
