//! The sorted set commands' parsing and replies (Redis 8.10's `t_zset.c`);
//! the store does the work.
//!
//! Scores are replied as Redis's `addReplyDouble` writes them (`d2string`):
//! a bulk string in RESP2, a double in RESP3. With `WITHSCORES`, RESP2 has
//! one flat array, RESP3 an array of [member, score] pairs.

use super::{c_str, integer_reply, store_error, syntax_error, Flow};
use resp::{Args, Encoder, Protocol};
use store::{parse_i64, Aggregate, Bounds, Combine, LexBound, LexRange, ScoreRange, SetOp, Store, ZSet, ZaddFlags};

/// Redis's `strcasecmp` of an argument with an option's name: a C string,
/// so it ends at a NUL.
pub(super) fn is(arg: &[u8], name: &[u8]) -> bool {
    c_str(arg, usize::MAX).eq_ignore_ascii_case(name)
}

pub(super) fn score(out: &mut Encoder, s: f64) {
    out.double_text(&store::double_to_bytes(s));
}

/// The header of `n` members, with scores or not.
fn members_header(out: &mut Encoder, n: usize, with_scores: bool) {
    let flat = with_scores && out.protocol() == Protocol::Resp2;
    out.array(if flat { 2 * n } else { n });
}

/// A member, with its score (as a pair in RESP3) or not.
fn member(out: &mut Encoder, m: &[u8], s: f64, with_scores: bool) {
    if with_scores && out.protocol() == Protocol::Resp3 {
        out.array(2);
    }
    out.bulk(m);
    if with_scores {
        score(out, s);
    }
}

/// An integer argument, as `getLongFromObjectOrReply`.
fn long(arg: &[u8], out: &mut Encoder) -> Option<i64> {
    let n = parse_i64(arg);
    if n.is_none() {
        store_error(store::Error::NotInteger, out);
    }
    n
}

/// The sorted set at `key`, or the reply for a key of another type.
pub(super) fn zset<'s>(key: &[u8], store: &'s mut Store, out: &mut Encoder) -> Result<Option<&'s ZSet>, ()> {
    store.zset_at(key).map_err(|e| store_error(e, out))
}

/// `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`,
/// and `ZINCRBY key increment member` (`incr`).
pub(super) fn zadd(args: &Args, store: &mut Store, out: &mut Encoder, incr: bool) -> Flow {
    let mut flags = ZaddFlags { incr, ..ZaddFlags::default() };
    let mut ch = false;
    // Options come before the first score, in any order.
    let mut at = 2;
    while at < args.len() {
        let opt = &args[at];
        if is(opt, b"nx") {
            flags.nx = true;
        } else if is(opt, b"xx") {
            flags.xx = true;
        } else if is(opt, b"ch") {
            ch = true;
        } else if is(opt, b"incr") {
            flags.incr = true;
        } else if is(opt, b"gt") {
            flags.gt = true;
        } else if is(opt, b"lt") {
            flags.lt = true;
        } else {
            break;
        }
        at += 1;
    }
    let rest = args.len() - at;
    if rest == 0 || !rest.is_multiple_of(2) {
        syntax_error(out);
        return Flow::Continue;
    }
    if flags.nx && flags.xx {
        out.error(b"ERR XX and NX options at the same time are not compatible");
        return Flow::Continue;
    }
    if [flags.gt, flags.lt, flags.nx].into_iter().filter(|&on| on).count() > 1 {
        out.error(b"ERR GT, LT, and/or NX options at the same time are not compatible");
        return Flow::Continue;
    }
    if flags.incr && rest > 2 {
        out.error(b"ERR INCR option supports a single increment-element pair");
        return Flow::Continue;
    }
    // Every score first: the command does all or nothing.
    let mut pairs = Vec::with_capacity(rest / 2);
    for i in (at..args.len()).step_by(2) {
        match store::parse_double(&args[i]) {
            Some(s) => pairs.push((s, &args[i + 1][..])),
            None => {
                store_error(store::Error::NotFloat, out);
                return Flow::Continue;
            }
        }
    }
    match store.zadd(&args[1], flags, &pairs) {
        Ok(done) if flags.incr => {
            if done.processed > 0 {
                score(out, done.score);
            } else {
                out.null();
            }
        }
        Ok(done) => out.integer((if ch { done.added + done.updated } else { done.added }) as i64),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

pub(super) fn zrem(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let members: Vec<&[u8]> = args.iter().skip(2).collect();
    integer_reply(store.zrem(&args[1], &members).map(|n| n as i64), out)
}

pub(super) fn zcard(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(z) = zset(&args[1], store, out) {
        out.integer(z.map_or(0, |z| z.len() as i64));
    }
    Flow::Continue
}

pub(super) fn zscore(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    match zset(&args[1], store, out) {
        Ok(Some(z)) => match z.score(&args[2]) {
            Some(s) => score(out, s),
            None => out.null(),
        },
        Ok(None) => out.null(),
        Err(()) => {}
    }
    Flow::Continue
}

pub(super) fn zmscore(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if let Ok(z) = zset(&args[1], store, out) {
        out.array(args.len() - 2);
        for m in args.iter().skip(2) {
            match z.and_then(|z| z.score(m)) {
                Some(s) => score(out, s),
                None => out.null(),
            }
        }
    }
    Flow::Continue
}

/// `ZRANK key member [WITHSCORE]`, `ZREVRANK` (`rev`).
pub(super) fn zrank(args: &Args, store: &mut Store, out: &mut Encoder, rev: bool) -> Flow {
    if args.len() > 4 {
        let name = if rev { "zrevrank" } else { "zrank" };
        out.error(format!("ERR wrong number of arguments for '{name}' command").as_bytes());
        return Flow::Continue;
    }
    let with_score = args.len() == 4;
    if with_score && !is(&args[3], b"withscore") {
        syntax_error(out);
        return Flow::Continue;
    }
    let found = match zset(&args[1], store, out) {
        Err(()) => return Flow::Continue,
        Ok(z) => z.and_then(|z| z.rank(&args[2]).map(|(r, s)| (if rev { z.len() - 1 - r } else { r }, s))),
    };
    match found {
        Some((rank, s)) => {
            if with_score {
                out.array(2);
            }
            out.integer(rank as i64);
            if with_score {
                score(out, s);
            }
        }
        None if with_score => out.null_array(),
        None => out.null(),
    }
    Flow::Continue
}

/// A score range's ends, as `zslParseRange`: `(` makes one exclusive.
fn score_range(min: &[u8], max: &[u8]) -> Option<ScoreRange> {
    let end = |arg: &[u8]| match arg.split_first() {
        Some((b'(', rest)) => store::parse_double_bound(rest).map(|v| (v, true)),
        _ => store::parse_double_bound(arg).map(|v| (v, false)),
    };
    let (min, minex) = end(min)?;
    let (max, maxex) = end(max)?;
    Some(ScoreRange { min, max, minex, maxex })
}

/// A lex range's ends, as `zslParseLexRange`: `-`, `+` (C strings: what
/// follows a NUL doesn't count), or a member after `(` or `[`.
fn lex_range(min: &[u8], max: &[u8]) -> Option<LexRange> {
    let end = |arg: &[u8]| match arg.split_first() {
        Some((b'+', rest)) if rest.first().is_none_or(|&b| b == 0) => Some((LexBound::Max, true)),
        Some((b'-', rest)) if rest.first().is_none_or(|&b| b == 0) => Some((LexBound::Min, true)),
        Some((b'(', rest)) => Some((LexBound::Value(rest.to_vec()), true)),
        Some((b'[', rest)) => Some((LexBound::Value(rest.to_vec()), false)),
        _ => None,
    };
    let (min, minex) = end(min)?;
    let (max, maxex) = end(max)?;
    Some(LexRange { min, max, minex, maxex })
}

const NOT_FLOAT: &[u8] = b"ERR min or max is not a float";
const NOT_LEX: &[u8] = b"ERR min or max not valid string range item";

#[derive(Clone, Copy, PartialEq, Eq)]
enum By {
    Rank,
    Score,
    Lex,
}

/// The ranks a `ZRANGE` picks: from `start`, `count` of them, forwards or
/// backwards, and an upper bound for `ZRANGESTORE` to size its set by.
struct Picked {
    start: usize,
    count: usize,
    rev: bool,
    hint: usize,
}

/// The ranks of a range by rank, as `genericZrangebyrankCommand`.
fn by_rank(z: &ZSet, start: i64, end: i64, rev: bool) -> Picked {
    let len = z.len() as i64;
    let start = if start < 0 { (len + start).max(0) } else { start };
    let end = if end < 0 { len + end } else { end };
    if start > end || start >= len {
        return Picked { start: 0, count: 0, rev, hint: 0 };
    }
    let end = end.min(len - 1);
    let count = (end - start + 1) as usize;
    let first = if rev { (len - 1 - start) as usize } else { start as usize };
    Picked { start: first, count, rev, hint: count }
}

/// The ranks of a range by score or member, as `genericZrangebyscoreCommand`
/// and `genericZrangebylexCommand`: from the first in range (the last one
/// backwards), past `offset` members, then while in range, `limit` at most
/// (negative: no limit).
fn by_bounds(z: &ZSet, b: &impl Bounds, offset: i64, limit: i64, rev: bool) -> Picked {
    let none = Picked { start: 0, count: 0, rev, hint: 0 };
    if offset < 0 || (offset > 0 && offset >= z.len() as i64) {
        return none;
    }
    let found = if rev { z.range_last(b) } else { z.range_first(b) };
    let Some(edge) = found else { return none };
    let offset = offset as usize;
    let start = if rev {
        match edge.checked_sub(offset) {
            Some(s) => s,
            None => return none,
        }
    } else if edge + offset < z.len() {
        edge + offset
    } else {
        return none;
    };
    let limit = if limit < 0 { usize::MAX } else { limit as usize };
    let count = z
        .iter(start, limit, rev)
        .take_while(|(m, s)| if rev { b.gte_min(*s, m) } else { b.lte_max(*s, m) })
        .count();
    Picked { start, count, rev, hint: 0 }
}

/// `ZRANGE` and the commands it grew out of: `fixed_by` and `fixed_rev`
/// for `ZRANGEBYSCORE`, `ZREVRANGE` and the like (which then take no
/// `BYSCORE`, `BYLEX` or `REV`); `store` for `ZRANGESTORE dst src ...`.
fn range(args: &Args, store: &mut Store, out: &mut Encoder, fixed_by: Option<By>, fixed_rev: Option<bool>, dst: Option<&[u8]>) -> Flow {
    let src = if dst.is_some() { 2 } else { 1 };
    let (mut by, mut rev) = (fixed_by, fixed_rev);
    let (mut with_scores, mut offset, mut limit) = (false, 0, -1);
    let mut j = src + 3;
    while j < args.len() {
        let left = args.len() - j - 1;
        let arg = &args[j];
        if dst.is_none() && is(arg, b"withscores") {
            with_scores = true;
        } else if is(arg, b"limit") && left >= 2 {
            let Some(o) = long(&args[j + 1], out) else { return Flow::Continue };
            let Some(l) = long(&args[j + 2], out) else { return Flow::Continue };
            (offset, limit) = (o, l);
            j += 2;
        // Each once, and only where the command leaves it open.
        } else if rev.is_none() && is(arg, b"rev") {
            rev = Some(true);
        } else if by.is_none() && is(arg, b"bylex") {
            by = Some(By::Lex);
        } else if by.is_none() && is(arg, b"byscore") {
            by = Some(By::Score);
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
        j += 1;
    }
    let by = by.unwrap_or(By::Rank);
    let rev = rev.unwrap_or(false);
    if limit != -1 && by == By::Rank {
        out.error(b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return Flow::Continue;
    }
    if with_scores && by == By::Lex {
        out.error(b"ERR syntax error, WITHSCORES not supported in combination with BYLEX");
        return Flow::Continue;
    }
    // Backwards by score or member, the range is given max first.
    let (lo, hi) = if rev && by != By::Rank { (&args[src + 2], &args[src + 1]) } else { (&args[src + 1], &args[src + 2]) };
    enum Spec {
        Rank(i64, i64),
        Score(ScoreRange),
        Lex(LexRange),
    }
    let spec = match by {
        By::Rank => {
            let Some(a) = long(lo, out) else { return Flow::Continue };
            let Some(b) = long(hi, out) else { return Flow::Continue };
            Spec::Rank(a, b)
        }
        By::Score => match score_range(lo, hi) {
            Some(r) => Spec::Score(r),
            None => {
                out.error(NOT_FLOAT);
                return Flow::Continue;
            }
        },
        By::Lex => match lex_range(lo, hi) {
            Some(r) => Spec::Lex(r),
            None => {
                out.error(NOT_LEX);
                return Flow::Continue;
            }
        },
    };
    let z = match zset(&args[src], store, out) {
        Err(()) => return Flow::Continue,
        Ok(None) => {
            match dst {
                Some(dst) => out.integer(store.zstore(dst, &[], 0) as i64),
                None => out.array(0),
            }
            return Flow::Continue;
        }
        Ok(Some(z)) => z,
    };
    let picked = match &spec {
        Spec::Rank(a, b) => by_rank(z, *a, *b, rev),
        Spec::Score(r) => by_bounds(z, r, offset, limit, rev),
        Spec::Lex(r) => by_bounds(z, r, offset, limit, rev),
    };
    let it = z.iter(picked.start, picked.count, picked.rev);
    match dst {
        Some(dst) => {
            let members: Vec<(Vec<u8>, f64)> = it.map(|(m, s)| (m.to_vec(), s)).collect();
            let members: Vec<(&[u8], f64)> = members.iter().map(|(m, s)| (&m[..], *s)).collect();
            out.integer(store.zstore(dst, &members, picked.hint) as i64);
        }
        None => {
            members_header(out, picked.count, with_scores);
            for (m, s) in it {
                member(out, m, s, with_scores);
            }
        }
    }
    Flow::Continue
}

pub(super) fn zrange(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    range(args, store, out, None, None, None)
}

pub(super) fn zrangestore(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    range(args, store, out, None, None, Some(&args[1]))
}

pub(super) fn zrevrange(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    range(args, store, out, Some(By::Rank), Some(true), None)
}

pub(super) fn zrangebyscore(args: &Args, store: &mut Store, out: &mut Encoder, rev: bool) -> Flow {
    range(args, store, out, Some(By::Score), Some(rev), None)
}

pub(super) fn zrangebylex(args: &Args, store: &mut Store, out: &mut Encoder, rev: bool) -> Flow {
    range(args, store, out, Some(By::Lex), Some(rev), None)
}

pub(super) fn zcount(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(r) = score_range(&args[2], &args[3]) else {
        out.error(NOT_FLOAT);
        return Flow::Continue;
    };
    if let Ok(z) = zset(&args[1], store, out) {
        out.integer(z.map_or(0, |z| z.range_count(&r)) as i64);
    }
    Flow::Continue
}

pub(super) fn zlexcount(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(r) = lex_range(&args[2], &args[3]) else {
        out.error(NOT_LEX);
        return Flow::Continue;
    };
    if let Ok(z) = zset(&args[1], store, out) {
        out.integer(z.map_or(0, |z| z.range_count(&r)) as i64);
    }
    Flow::Continue
}

/// `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`: the range
/// parsed first, then the key.
fn zremrange(args: &Args, store: &mut Store, out: &mut Encoder, by: By) -> Flow {
    let (from, count) = match by {
        By::Rank => {
            let Some(start) = long(&args[2], out) else { return Flow::Continue };
            let Some(end) = long(&args[3], out) else { return Flow::Continue };
            match zset(&args[1], store, out) {
                Err(()) => return Flow::Continue,
                Ok(None) => (0, 0),
                Ok(Some(z)) => {
                    let p = by_rank(z, start, end, false);
                    (p.start, p.count)
                }
            }
        }
        By::Score => {
            let Some(r) = score_range(&args[2], &args[3]) else {
                out.error(NOT_FLOAT);
                return Flow::Continue;
            };
            match zset(&args[1], store, out) {
                Err(()) => return Flow::Continue,
                Ok(z) => z.and_then(|z| z.range_first(&r).map(|f| (f, z.range_count(&r)))).unwrap_or((0, 0)),
            }
        }
        By::Lex => {
            let Some(r) = lex_range(&args[2], &args[3]) else {
                out.error(NOT_LEX);
                return Flow::Continue;
            };
            match zset(&args[1], store, out) {
                Err(()) => return Flow::Continue,
                Ok(z) => z.and_then(|z| z.range_first(&r).map(|f| (f, z.range_count(&r)))).unwrap_or((0, 0)),
            }
        }
    };
    integer_reply(store.zrem_ranks(&args[1], from, count).map(|n| n as i64), out)
}

pub(super) fn zremrangebyrank(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    zremrange(args, store, out, By::Rank)
}

pub(super) fn zremrangebyscore(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    zremrange(args, store, out, By::Score)
}

pub(super) fn zremrangebylex(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    zremrange(args, store, out, By::Lex)
}

/// `ZPOPMIN key [count]`, `ZPOPMAX` (`max`).
pub(super) fn zpop(args: &Args, store: &mut Store, out: &mut Encoder, max: bool) -> Flow {
    if args.len() > 3 {
        syntax_error(out);
        return Flow::Continue;
    }
    let count = match args.get(2) {
        None => None,
        // One error for both, as `getPositiveLongFromObjectOrReply`.
        Some(arg) => match parse_i64(arg) {
            None | Some(i64::MIN..0) => {
                out.error(b"ERR value is out of range, must be positive");
                return Flow::Continue;
            }
            Some(n) => Some(n as usize),
        },
    };
    match store.zpop(&args[1], max, count.unwrap_or(1)) {
        Ok(Some(popped)) if count != Some(0) => {
            // With a count, RESP3 nests the pairs.
            let nested = count.is_some() && out.protocol() == Protocol::Resp3;
            out.array(if nested { popped.len() } else { 2 * popped.len() });
            for (m, s) in popped {
                if nested {
                    out.array(2);
                }
                out.bulk(&m);
                score(out, s);
            }
        }
        Ok(_) => out.array(0),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `ZRANDMEMBER key [count [WITHSCORES]]`.
pub(super) fn zrandmember(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    if args.len() == 2 {
        match store.zrandmember(&args[1]) {
            Ok(Some(m)) => out.bulk(&m),
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
    let with_scores = args.len() == 4;
    if args.len() > 4 || (with_scores && !is(&args[3], b"withscores")) {
        syntax_error(out);
        return Flow::Continue;
    }
    // So that twice the count still fits.
    if with_scores && !(-(i64::MAX / 2)..=i64::MAX / 2).contains(&count) {
        out.error(b"ERR value is out of range");
        return Flow::Continue;
    }
    match store.zrandmember_count(&args[1], count) {
        Ok(picked) => {
            members_header(out, picked.len(), with_scores);
            for (m, s) in picked {
                member(out, &m, s, with_scores);
            }
        }
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `ZUNION`, `ZINTER`, `ZDIFF numkeys key ... [WEIGHTS w ...] [AGGREGATE
/// SUM|MIN|MAX|COUNT] [WITHSCORES]`, and their `STORE` forms (`dst`: the
/// command's name, `args[1]` the destination), as
/// `zunionInterDiffGenericCommand`: the count, the keys' types, then the
/// options.
pub(super) fn combine(args: &Args, store: &mut Store, out: &mut Encoder, op: SetOp, dst: Option<&str>) -> Flow {
    let name = match (dst, op) {
        (Some(name), _) => name,
        (None, SetOp::Union) => "zunion",
        (None, SetOp::Inter) => "zinter",
        (None, SetOp::Diff) => "zdiff",
    };
    let Some(keys) = numkeys(args, if dst.is_some() { 2 } else { 1 }, name, store, out) else { return Flow::Continue };
    let mut opts = Combine::default();
    let mut with_scores = false;
    let mut j = keys.len() + if dst.is_some() { 3 } else { 2 };
    while j < args.len() {
        let left = args.len() - j;
        if op != SetOp::Diff && left > keys.len() && is(&args[j], b"weights") {
            opts.weights.clear();
            for w in args.iter().skip(j + 1).take(keys.len()) {
                match store::parse_double(w) {
                    Some(w) => opts.weights.push(w),
                    None => {
                        out.error(b"ERR weight value is not a float");
                        return Flow::Continue;
                    }
                }
            }
            j += 1 + keys.len();
        } else if op != SetOp::Diff && left >= 2 && is(&args[j], b"aggregate") {
            let a = &args[j + 1];
            opts.aggregate = if is(a, b"sum") {
                Aggregate::Sum
            } else if is(a, b"min") {
                Aggregate::Min
            } else if is(a, b"max") {
                Aggregate::Max
            } else if is(a, b"count") {
                Aggregate::Count
            } else {
                syntax_error(out);
                return Flow::Continue;
            };
            j += 2;
        } else if dst.is_none() && is(&args[j], b"withscores") {
            with_scores = true;
            j += 1;
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
    }
    let z = store.zcombine(&keys, op, &opts);
    match dst {
        Some(_) => out.integer(store.zstore_set(&args[1], z) as i64),
        None => {
            members_header(out, z.len(), with_scores);
            for (m, s) in z.iter(0, usize::MAX, false) {
                member(out, m, s, with_scores);
            }
        }
    }
    Flow::Continue
}

/// The keys after a count at `args[at]`, read as Redis reads them (an
/// integer of at least 1, no more than there are arguments), and checked
/// for their types; `None` once the error is replied.
fn numkeys<'a>(args: &Args<'a>, at: usize, name: &str, store: &mut Store, out: &mut Encoder) -> Option<Vec<&'a [u8]>> {
    let n = long(&args[at], out)?;
    if n < 1 {
        out.error(format!("ERR at least 1 input key is needed for '{name}' command").as_bytes());
        return None;
    }
    if n as u64 > (args.len() - at - 1) as u64 {
        syntax_error(out);
        return None;
    }
    let keys: Vec<&[u8]> = args.iter().skip(at + 1).take(n as usize).collect();
    if let Err(e) = store.zcheck(&keys) {
        store_error(e, out);
        return None;
    }
    Some(keys)
}

/// `ZINTERCARD numkeys key ... [LIMIT limit]`.
pub(super) fn zintercard(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Some(keys) = numkeys(args, 1, "zintercard", store, out) else { return Flow::Continue };
    let mut limit = 0;
    let mut j = keys.len() + 2;
    while j < args.len() {
        if args.len() - j >= 2 && is(&args[j], b"limit") {
            match parse_i64(&args[j + 1]) {
                Some(n) if n >= 0 => limit = n as usize,
                _ => {
                    out.error(b"ERR LIMIT can't be negative");
                    return Flow::Continue;
                }
            }
            j += 2;
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
    }
    out.integer(store.zintercard(&keys, limit) as i64);
    Flow::Continue
}

/// `ZMPOP numkeys key ... MIN|MAX [COUNT count]`.
pub(super) fn zmpop(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let numkeys = match parse_i64(&args[1]) {
        Some(n) if n >= 1 => n as u64,
        _ => {
            out.error(b"ERR numkeys should be greater than 0");
            return Flow::Continue;
        }
    };
    let at = numkeys.saturating_add(2);
    let end = (at < args.len() as u64).then(|| &args[at as usize]);
    let max = match end {
        Some(e) if is(e, b"min") => false,
        Some(e) if is(e, b"max") => true,
        _ => {
            syntax_error(out);
            return Flow::Continue;
        }
    };
    let mut count = None;
    let mut i = at as usize + 1;
    while i < args.len() {
        if count.is_none() && is(&args[i], b"count") && i + 1 < args.len() {
            i += 1;
            match parse_i64(&args[i]) {
                Some(n) if n >= 1 => count = Some(n as usize),
                _ => {
                    out.error(b"ERR count should be greater than 0");
                    return Flow::Continue;
                }
            }
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
        i += 1;
    }
    let keys: Vec<&[u8]> = args.iter().skip(2).take(numkeys as usize).collect();
    match store.zmpop(&keys, max, count.unwrap_or(1)) {
        Ok(Some((key, popped))) => {
            out.array(2);
            out.bulk(keys[key]);
            out.array(popped.len());
            for (m, s) in popped {
                out.array(2);
                out.bulk(&m);
                score(out, s);
            }
        }
        Ok(None) => out.null_array(),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}
