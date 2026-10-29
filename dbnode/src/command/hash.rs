//! The hash commands' parsing and replies (Redis 7.0's `t_hash.c`); the
//! store does the work.

use super::{arity_error, integer_arg, integer_reply, store_error, syntax_error, Flow};
use resp::{Args, Encoder, Protocol};
use store::{parse_i64, Hash, Store};

/// `HSET key field value [field value ...]`: the number of new fields;
/// `HMSET`, the same with `+OK`.
pub(super) fn hset(args: &Args, store: &mut Store, out: &mut Encoder, name: &str) -> Flow {
    if !args.len().is_multiple_of(2) {
        arity_error(name, out);
        return Flow::Continue;
    }
    let pairs: Vec<&[u8]> = args.iter().skip(2).collect();
    match store.hset(&args[1], &pairs) {
        Ok(_) if name == "hmset" => out.ok(),
        Ok(new) => out.integer(new as i64),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn hsetnx(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    integer_reply(store.hsetnx(&args[1], &args[2], &args[3]).map(i64::from), out)
}

/// The hash at `args[1]`, or the reply for a key of another type.
fn hash<'s>(args: &Args, store: &'s mut Store, out: &mut Encoder) -> Result<Option<&'s Hash>, ()> {
    store.hash(&args[1]).map_err(|e| store_error(e, out))
}

pub(super) fn hget(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match hash(args, store, out) {
        Ok(Some(h)) => match h.get(&args[2]) {
            Some(v) => out.bulk(v),
            None => out.null(),
        },
        Ok(None) => out.null(),
        Err(()) => {}
    }
    Flow::Continue
}

pub(super) fn hmget(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(h) = hash(args, store, out) {
        out.array(args.len() - 2);
        for field in args.iter().skip(2) {
            match h.and_then(|h| h.get(field)) {
                Some(v) => out.bulk(v),
                None => out.null(),
            }
        }
    }
    Flow::Continue
}

pub(super) fn hdel(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let fields: Vec<&[u8]> = args.iter().skip(2).collect();
    integer_reply(store.hdel(&args[1], &fields).map(|n| n as i64), out)
}

pub(super) fn hincrby(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match integer_arg(&args[3]) {
        Ok(by) => integer_reply(store.hincr_by(&args[1], &args[2], by), out),
        Err(e) => {
            store_error(e, out);
            Flow::Continue
        }
    }
}

pub(super) fn hincrbyfloat(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match store.hincr_by_float(&args[1], &args[2], &args[3]) {
        Ok(value) => out.bulk(&value),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `HRANDFIELD key [count [WITHVALUES]]`. The count is read before the key
/// is looked up, as in Redis. With values, RESP3 gets a pair per field.
pub(super) fn hrandfield(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if args.len() == 2 {
        match store.hrandfield(&args[1]) {
            Ok(Some(field)) => out.bulk(&field),
            Ok(None) => out.null(),
            Err(e) => store_error(e, out),
        }
        return Flow::Continue;
    }
    let count = match parse_i64(&args[2]) {
        None => return integer_reply(Err(store::Error::NotInteger), out),
        // Redis takes -LONG_MAX..LONG_MAX.
        Some(i64::MIN) => {
            out.error(b"ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807");
            return Flow::Continue;
        }
        Some(n) => n,
    };
    let with_values = args.len() == 4;
    if args.len() > 4 || (with_values && !args[3].eq_ignore_ascii_case(b"withvalues")) {
        syntax_error(out);
        return Flow::Continue;
    }
    // So that twice the count still fits.
    if with_values && !(-(i64::MAX / 2)..=i64::MAX / 2).contains(&count) {
        out.error(b"ERR value is out of range");
        return Flow::Continue;
    }
    match store.hrandfield_count(&args[1], count) {
        Ok(pairs) => {
            let nested = with_values && out.protocol() == Protocol::Resp3;
            out.array(if with_values && !nested { 2 * pairs.len() } else { pairs.len() });
            for (f, v) in pairs {
                if nested {
                    out.array(2);
                }
                out.bulk(&f);
                if with_values {
                    out.bulk(&v);
                }
            }
        }
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn hlen(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(h) = hash(args, store, out) {
        out.integer(h.map_or(0, |h| h.len() as i64));
    }
    Flow::Continue
}

pub(super) fn hexists(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(h) = hash(args, store, out) {
        out.integer(i64::from(h.is_some_and(|h| h.get(&args[2]).is_some())));
    }
    Flow::Continue
}

pub(super) fn hstrlen(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(h) = hash(args, store, out) {
        out.integer(h.and_then(|h| h.get(&args[2])).map_or(0, |v| v.len() as i64));
    }
    Flow::Continue
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Part {
    Keys,
    Values,
    /// Both: a map in RESP3.
    All,
}

/// `HKEYS`, `HVALS`, `HGETALL`: in the hash's order (Redis's, while it's
/// small; see the store's `collections`).
pub(super) fn read(args: &Args, store: &mut Store, out: &mut Encoder, part: Part) -> Flow {
    if let Ok(h) = hash(args, store, out) {
        let len = h.map_or(0, Hash::len);
        if part == Part::All {
            out.map(len);
        } else {
            out.array(len);
        }
        for (f, v) in h.into_iter().flat_map(Hash::iter) {
            if part != Part::Values {
                out.bulk(f);
            }
            if part != Part::Keys {
                out.bulk(v);
            }
        }
    }
    Flow::Continue
}
