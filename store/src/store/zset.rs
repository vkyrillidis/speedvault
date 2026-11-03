//! The sorted set commands (Redis 8.10's `t_zset.c`).
//!
//! A sorted set emptied by a command loses its key, as in Redis. The reads
//! work on [`Store::zset_at`]'s set; the writes are here.

use super::{SetOp, Store, Value};
use crate::collections::Set;
use crate::kv::Val;
use crate::zset::ZSet;
use crate::Error;
use std::collections::HashMap;

/// How `ZUNION` and `ZINTER` combine a member's scores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
    /// Redis 8: each set's weight once per member, whatever its score.
    Count,
}

impl Aggregate {
    /// Redis's `zuiWeightedScore`.
    fn weighted(self, score: f64, weight: f64) -> f64 {
        if self == Aggregate::Count { weight } else { weight * score }
    }

    /// Redis's `zunionInterAggregate`: a NaN sum is 0; MIN and MAX keep
    /// what they have on a tie (so the first of `0` and `-0` stays).
    fn combine(self, target: f64, value: f64) -> f64 {
        match self {
            Aggregate::Sum | Aggregate::Count => {
                let t = target + value;
                if t.is_nan() { 0.0 } else { t }
            }
            Aggregate::Min => if value < target { value } else { target },
            Aggregate::Max => if value > target { value } else { target },
        }
    }
}

/// An input of `ZUNION`, `ZINTER`, `ZDIFF`: a sorted set, or a set whose
/// members score 1, or nothing.
#[derive(Clone, Copy)]
enum Source<'a> {
    Missing,
    Set(&'a Set),
    ZSet(&'a ZSet),
}

impl Source<'_> {
    fn len(&self) -> usize {
        match self {
            Source::Missing => 0,
            Source::Set(s) => s.len(),
            Source::ZSet(z) => z.len(),
        }
    }

    /// The members and their scores in Redis's `zuiNext` order: a sorted set
    /// from its end, a set as it iterates.
    fn for_each(&self, mut f: impl FnMut(&[u8], f64) -> bool) {
        match self {
            Source::Missing => {}
            Source::Set(s) => {
                for m in s.iter() {
                    if !f(m, 1.0) {
                        break;
                    }
                }
            }
            Source::ZSet(z) => {
                for (m, s) in z.iter(z.len().saturating_sub(1), usize::MAX, true) {
                    if !f(m, s) {
                        break;
                    }
                }
            }
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            Source::Missing => None,
            Source::Set(s) => s.contains(member).then_some(1.0),
            Source::ZSet(z) => z.score(member),
        }
    }

    /// The same key given twice.
    fn same(&self, other: &Source) -> bool {
        match (self, other) {
            (Source::Set(a), Source::Set(b)) => std::ptr::eq(*a, *b),
            (Source::ZSet(a), Source::ZSet(b)) => std::ptr::eq(*a, *b),
            _ => false,
        }
    }
}

/// `ZUNION`'s and `ZINTER`'s options.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Combine {
    /// One per key; empty: all 1.
    pub weights: Vec<f64>,
    pub aggregate: Aggregate,
}

/// `ZADD`'s options; `incr` for `ZINCRBY` too.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ZaddFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub incr: bool,
}

/// What `ZADD` did.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Zadded {
    pub added: usize,
    pub updated: usize,
    /// Pairs not skipped (by `NX`, `XX`, `GT`, `LT`): `INCR` replies the
    /// score if there's one, else nil.
    pub processed: usize,
    /// The last processed pair's score.
    pub score: f64,
}

/// A member taken out, with its score.
pub type Scored = (Box<[u8]>, f64);

impl Store {
    /// The sorted set at `key`, `None` if there's none.
    pub fn zset_at(&mut self, key: &[u8]) -> Result<Option<&ZSet>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_zset()?)),
            None => Ok(None),
        }
    }

    /// `ZADD` (and `ZINCRBY`): `pairs` of a score (not NaN) and a member,
    /// one at most with `INCR`, as Redis's `zsetAdd` takes them.
    pub fn zadd(&mut self, key: &[u8], flags: ZaddFlags, pairs: &[(f64, &[u8])]) -> Result<Zadded, Error> {
        let mut done = Zadded::default();
        match self.live(key) {
            Some(e) => e.as_zset_mut()?.expect(pairs.len()),
            None if flags.xx => return Ok(done),
            None => {
                let z = ZSet::with_hint(pairs.len(), pairs[0].1.len());
                self.insert_value(key, Value::ZSet(Box::new(z)), None);
            }
        }
        let z = self.live(key).expect("there").as_zset_mut().expect("a sorted set");
        let mut result = Ok(());
        for &(score, member) in pairs {
            match z.score(member) {
                Some(_) if flags.nx => {}
                Some(current) => {
                    let score = if flags.incr { score + current } else { score };
                    if score.is_nan() {
                        result = Err(Error::ScoreNan);
                        break;
                    }
                    if (flags.lt && score >= current) || (flags.gt && score <= current) {
                        continue;
                    }
                    done.processed += 1;
                    done.score = score;
                    if score != current {
                        z.insert(member, score);
                        done.updated += 1;
                    }
                }
                None if !flags.xx => {
                    z.insert(member, score);
                    done.added += 1;
                    done.processed += 1;
                    done.score = score;
                }
                None => {}
            }
        }
        if done.added + done.updated > 0 {
            self.touch(key);
        }
        result.map(|()| done)
    }

    /// `ZREM`: how many of `members` were there.
    pub fn zrem(&mut self, key: &[u8], members: &[&[u8]]) -> Result<usize, Error> {
        let Some(e) = self.live(key) else { return Ok(0) };
        let z = e.as_zset_mut()?;
        let mut removed = 0;
        for m in members {
            removed += usize::from(z.remove(m).is_some());
            if z.is_empty() {
                break;
            }
        }
        if z.is_empty() {
            self.remove(key);
        }
        if removed > 0 {
            self.touch(key);
        }
        Ok(removed)
    }

    /// Removes `count` members from rank `from` on (`ZREMRANGEBY*`, which
    /// find the ranks first); how many went.
    pub fn zrem_ranks(&mut self, key: &[u8], from: usize, count: usize) -> Result<usize, Error> {
        let Some(e) = self.live(key) else { return Ok(0) };
        let z = e.as_zset_mut()?;
        let count = count.min(z.len().saturating_sub(from));
        for _ in 0..count {
            z.remove_at(from);
        }
        if z.is_empty() {
            self.remove(key);
        }
        if count > 0 {
            self.touch(key);
        }
        Ok(count)
    }

    /// `ZPOPMIN`, `ZPOPMAX` (`max`): up to `count` members from that end;
    /// `None` without the key.
    pub fn zpop(&mut self, key: &[u8], max: bool, count: usize) -> Result<Option<Vec<Scored>>, Error> {
        let Some(e) = self.live(key) else { return Ok(None) };
        let z = e.as_zset_mut()?;
        let n = count.min(z.len());
        let popped: Vec<Scored> = (0..n).map(|_| z.remove_at(if max { z.len() - 1 } else { 0 })).collect();
        if z.is_empty() {
            self.remove(key);
        }
        if n > 0 {
            self.touch(key);
        }
        Ok(Some(popped))
    }

    /// `ZRANGESTORE`'s write: `dst` becomes a sorted set of `members` (in
    /// order, `hint` of them expected, as Redis sizes it), or goes if there
    /// are none. How many it holds.
    pub fn zstore(&mut self, dst: &[u8], members: &[(&[u8], f64)], hint: usize) -> usize {
        if members.is_empty() {
            if self.remove(dst).is_some() {
                self.touch(dst);
            }
            return 0;
        }
        let mut z = ZSet::with_hint(hint, 0);
        for &(m, s) in members {
            z.insert(m, s);
        }
        let n = z.len();
        self.remove(dst);
        self.insert_value(dst, Value::ZSet(Box::new(z)), None);
        self.touch(dst);
        n
    }

    /// Looks `keys` up in order, as `zunionInterDiffGenericCommand` does
    /// before it reads its options: the first key of another type than a
    /// set or a sorted set is an error.
    pub fn zcheck(&mut self, keys: &[&[u8]]) -> Result<(), Error> {
        for key in keys {
            if let Some(e) = self.live(key) {
                if !matches!(e.value(), Val::Set(_) | Val::ZSet(_)) {
                    return Err(Error::WrongType);
                }
            }
        }
        Ok(())
    }

    /// The inputs, after [`zcheck`](Self::zcheck): live keys only (a
    /// replica's expired ones aren't reclaimed, so `live` alone won't do).
    fn sources(&self, keys: &[&[u8]]) -> Vec<Source<'_>> {
        keys.iter()
            .map(|key| match self.keys.get(key).filter(|e| self.now <= e.expire_at()).map(|e| e.value()) {
                Some(Val::Set(s)) => Source::Set(s),
                Some(Val::ZSet(z)) => Source::ZSet(z),
                _ => Source::Missing,
            })
            .collect()
    }

    /// `ZUNION`, `ZINTER`, `ZDIFF` of `keys` (checked with [`zcheck`]):
    /// the result, large whatever its size, as Redis builds it (a reply
    /// keeps its `-0` scores; [`zstore_set`](Self::zstore_set) packs it if
    /// it fits).
    ///
    /// [`zcheck`]: Self::zcheck
    pub fn zcombine(&self, keys: &[&[u8]], op: SetOp, opts: &Combine) -> ZSet {
        let mut out = ZSet::with_hint(usize::MAX, 0);
        let mut src: Vec<(Source, f64)> =
            self.sources(keys).into_iter().enumerate().map(|(i, s)| (s, opts.weights.get(i).copied().unwrap_or(1.0))).collect();
        let agg = opts.aggregate;
        match op {
            SetOp::Inter => {
                // Smallest first (a stable sort: ties keep their order, as
                // glibc's qsort does for a few inputs), so the scores add up
                // in Redis's order.
                src.sort_by_key(|(s, _)| s.len());
                if src[0].0.len() == 0 {
                    return out;
                }
                let (first, w0) = src[0];
                first.for_each(|m, score| {
                    let mut total = agg.weighted(score, w0);
                    if total.is_nan() {
                        total = 0.0;
                    }
                    for (s, w) in &src[1..] {
                        let found = if s.same(&first) { Some(score) } else { s.score(m) };
                        let Some(value) = found else { return true };
                        total = agg.combine(total, agg.weighted(value, *w));
                    }
                    out.insert(m, total);
                    true
                });
            }
            SetOp::Union => {
                src.sort_by_key(|(s, _)| s.len());
                let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
                for (s, w) in &src {
                    s.for_each(|m, score| {
                        let mut value = agg.weighted(score, *w);
                        if value.is_nan() {
                            value = 0.0;
                        }
                        match scores.get_mut(m) {
                            Some(total) => *total = agg.combine(*total, value),
                            None => {
                                scores.insert(m.to_vec(), value);
                            }
                        }
                        true
                    });
                }
                for (m, s) in scores {
                    out.insert(&m, s);
                }
            }
            SetOp::Diff => {
                let first = src[0].0;
                // A key given twice takes everything away.
                if src[1..].iter().any(|(s, _)| s.same(&first)) {
                    return out;
                }
                first.for_each(|m, score| {
                    if src[1..].iter().all(|(s, _)| s.score(m).is_none()) {
                        out.insert(m, score);
                    }
                    true
                });
            }
        }
        out
    }

    /// `ZINTERCARD`: how many members every one of `keys` (checked) holds,
    /// counting no further than `limit` (0: no limit).
    pub fn zintercard(&self, keys: &[&[u8]], limit: usize) -> usize {
        let mut src = self.sources(keys);
        src.sort_by_key(Source::len);
        let first = src[0];
        let mut n = 0;
        first.for_each(|m, _| {
            if src[1..].iter().all(|s| s.same(&first) || s.score(m).is_some()) {
                n += 1;
                if limit > 0 && n >= limit {
                    return false;
                }
            }
            true
        });
        n
    }

    /// A `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE` result into `dst`:
    /// packed if it fits, or `dst` deleted if it's empty. Its size.
    pub fn zstore_set(&mut self, dst: &[u8], mut z: ZSet) -> usize {
        if z.is_empty() {
            if self.remove(dst).is_some() {
                self.touch(dst);
            }
            return 0;
        }
        z.shrink_if_fits();
        let n = z.len();
        self.remove(dst);
        self.insert_value(dst, Value::ZSet(Box::new(z)), None);
        self.touch(dst);
        n
    }

    /// `ZMPOP`: from the first of `keys` that exists (a key of another type
    /// before it is an error), up to `count` members from one end; which key
    /// it was and what came out, `None` if none exists.
    pub fn zmpop(&mut self, keys: &[&[u8]], max: bool, count: usize) -> Result<Option<(usize, Vec<Scored>)>, Error> {
        for (i, key) in keys.iter().enumerate() {
            if self.zset_at(key)?.is_some() {
                return Ok(self.zpop(key, max, count)?.map(|popped| (i, popped)));
            }
        }
        Ok(None)
    }

    /// `ZRANDMEMBER key`: a random member.
    pub fn zrandmember(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let len = match self.live(key) {
            None => return Ok(None),
            Some(e) => e.as_zset()?.len(),
        };
        let rank = self.random_below(len);
        let z = self.live(key).expect("looked up above").as_zset()?;
        Ok(z.get(rank).map(|(m, _)| m.to_vec()))
    }

    /// `ZRANDMEMBER key count`: `count` > 0 distinct members (all of them,
    /// last first, if it covers the set: Redis walks a sorted set from its
    /// end there; in order while the set is packed, as Redis's listpack
    /// sampling; in a random order once large),
    /// `count` < 0, `-count` members that may repeat. With their scores.
    pub fn zrandmember_count(&mut self, key: &[u8], count: i64) -> Result<Vec<(Vec<u8>, f64)>, Error> {
        let (len, big) = match self.live(key) {
            None => return Ok(Vec::new()),
            Some(e) => {
                let z = e.as_zset()?;
                (z.len(), z.is_big())
            }
        };
        let ranks: Vec<usize> = if count < 0 {
            (0..count.unsigned_abs()).map(|_| self.random_below(len)).collect()
        } else if count as u64 >= len as u64 {
            (0..len).rev().collect()
        } else {
            // A partial Fisher-Yates over the ranks.
            let mut all: Vec<usize> = (0..len).collect();
            for i in 0..count as usize {
                let j = i + self.random_below(len - i);
                all.swap(i, j);
            }
            all.truncate(count as usize);
            if !big {
                all.sort_unstable();
            }
            all
        };
        let z = self.live(key).expect("looked up above").as_zset()?;
        Ok(ranks
            .into_iter()
            .map(|r| {
                let (m, s) = z.get(r).expect("a rank below the length");
                (m.to_vec(), s)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check, store};
    use super::super::{Kind, Staged};
    use super::{SetOp, Store};
    use crate::{End, Error, ExpireCmd};

    /// A sorted set's dump payload: members and their scores.
    fn payload(pairs: &[(&[u8], f64)]) -> Vec<u8> {
        let mut out = Vec::new();
        for &(m, s) in pairs {
            for item in [m, &s.to_bits().to_le_bytes()[..]] {
                out.extend_from_slice(&(item.len() as u32).to_le_bytes());
                out.extend_from_slice(item);
            }
        }
        out
    }

    fn members(s: &mut Store, key: &[u8]) -> Vec<(Vec<u8>, u64)> {
        let z = s.zset_at(key).unwrap().unwrap();
        z.iter(0, usize::MAX, false).map(|(m, sc)| (m.to_vec(), sc.to_bits())).collect()
    }

    #[test]
    fn a_sorted_set_is_a_type_of_its_own() {
        let mut s = store();
        let p = payload(&[(b"b", 2.0), (b"a", 1.0), (b"c", f64::NEG_INFINITY)]);
        assert!(crate::check_dump(Kind::ZSet, &p));
        s.restore(b"z", Kind::ZSet, &p, None);
        assert_eq!(s.type_of(b"z"), "zset");
        assert_eq!(s.encoding(b"z"), Some("listpack"));
        assert_eq!(
            members(&mut s, b"z"),
            [(b"c".to_vec(), f64::NEG_INFINITY.to_bits()), (b"a".to_vec(), 1f64.to_bits()), (b"b".to_vec(), 2f64.to_bits())]
        );
        assert_eq!(s.elements(b"z"), Some(3));
        // Every other type's commands refuse it.
        assert_eq!(s.get(b"z"), Err(Error::WrongType));
        assert_eq!(s.append(b"z", b"x"), Err(Error::WrongType));
        assert_eq!(s.incr_by(b"z", 1), Err(Error::WrongType));
        assert_eq!(s.hset(b"z", &[b"f", b"v"]), Err(Error::WrongType));
        assert_eq!(s.sadd(b"z", &[b"m"]), Err(Error::WrongType));
        assert_eq!(s.push(b"z", End::Left, &[b"x"], false), Err(Error::WrongType));
        assert!(s.set_at(b"z").is_err() && s.hash(b"z").is_err() && s.list_at(b"z").is_err());
        // And it refuses theirs.
        s.set(b"str", b"v", Default::default()).unwrap();
        s.sadd(b"set", &[b"m"]).unwrap();
        assert_eq!(s.zset_at(b"str").err(), Some(Error::WrongType));
        assert_eq!(s.zset_at(b"set").err(), Some(Error::WrongType));
        assert!(s.zset_at(b"nothing").unwrap().is_none());
        // SCAN TYPE finds it.
        let (cursor, keys) = s.scan(0, 100, None, Some(b"ZSET"));
        assert_eq!((cursor, keys), (0, vec![b"z".to_vec()]));
        // The generic commands.
        assert!(s.exists(b"z"));
        assert!(s.expire(ExpireCmd::PExpire, b"z", 100, Default::default()).unwrap());
        assert_eq!(s.pttl(b"z"), 100);
        assert_eq!(s.type_of(b"z"), "zset", "a TTL keeps the value");
        assert_eq!(members(&mut s, b"z").len(), 3);
        check(&s);
        s.set_now(s.now + 101);
        assert_eq!(s.type_of(b"z"), "none");
        s.restore(b"z", Kind::ZSet, &p, None);
        assert!(s.del(b"z"));
        assert!(!s.exists(b"z"));
        check(&s);
    }

    #[test]
    fn its_encoding_comes_from_its_contents() {
        let mut s = store();
        let big: Vec<(Vec<u8>, f64)> = (0..129).map(|i| (format!("m{i}").into_bytes(), f64::from(i))).collect();
        let pairs: Vec<(&[u8], f64)> = big.iter().map(|(m, sc)| (&m[..], *sc)).collect();
        s.restore(b"big", Kind::ZSet, &payload(&pairs), None);
        assert_eq!(s.encoding(b"big"), Some("skiplist"));
        s.restore(b"small", Kind::ZSet, &payload(&pairs[..128]), None);
        assert_eq!(s.encoding(b"small"), Some("listpack"));
        let long = [b'x'; 65];
        s.restore(b"long", Kind::ZSet, &payload(&[(&long, 1.0)]), None);
        assert_eq!(s.encoding(b"long"), Some("skiplist"));
    }

    #[test]
    fn a_dump_checks_its_scores() {
        let ok = payload(&[(b"m", 1.5), (b"", -0.0)]);
        assert!(crate::check_dump(Kind::ZSet, &ok));
        assert!(!crate::check_dump(Kind::ZSet, b""), "no empty collection");
        assert!(!crate::check_dump(Kind::ZSet, &payload(&[(b"m", f64::NAN)])), "no NaN");
        let mut odd = payload(&[(b"m", 1.0)]);
        odd.extend_from_slice(b"\x01\0\0\0x");
        assert!(!crate::check_dump(Kind::ZSet, &odd), "pairs only");
        assert!(!crate::check_dump(Kind::ZSet, b"\x01\0\0\0m\x07\0\0\0abcdefg"), "8-byte scores");
        assert_eq!(Kind::from_name(b"zset"), Some(Kind::ZSet));
        assert_eq!(Kind::ZSet.name(), b"zset");
    }

    #[test]
    fn copies_whole_and_in_parts_bit_for_bit() {
        let mut a = store();
        let mut b = store();
        let pairs: Vec<(Vec<u8>, f64)> = (0..1000)
            .map(|i| (format!("m{}", i * 7919 % 1000).into_bytes(), [0.0, -0.0, 1e-310, f64::INFINITY, 2.5][i % 5]))
            .collect();
        let refs: Vec<(&[u8], f64)> = pairs.iter().map(|(m, sc)| (&m[..], *sc)).collect();
        a.restore(b"z", Kind::ZSet, &payload(&refs), Some(5000));
        a.restore(b"small", Kind::ZSet, &payload(&refs[..5]), None);
        for key in [&b"z"[..], b"small"] {
            let (dump, ttl) = a.dump(key).unwrap();
            assert_eq!(dump.kind(), Kind::ZSet);
            b.restore(key, dump.kind(), dump.payload(), ttl);
            assert_eq!(members(&mut b, key), members(&mut a, key));
            assert_eq!(b.encoding(key), a.encoding(key));
        }
        assert_eq!(b.pttl(b"z"), 5000);
        // In parts of 3 members.
        let mut c = store();
        let mut staged: Option<Staged> = None;
        let mut parts = 0;
        loop {
            let at = staged.as_ref().map_or(0, Staged::count);
            let part = a.dump_part(b"z", at, 3, usize::MAX).unwrap();
            assert!(crate::check_dump(part.kind, &part.payload));
            let st = staged.get_or_insert_with(|| Staged::new(part.kind));
            st.append(&part.payload);
            parts += 1;
            if st.count() == part.total {
                c.restore_staged(b"z", staged.take().unwrap(), part.ttl);
                break;
            }
        }
        assert_eq!(parts, 334);
        assert_eq!(members(&mut c, b"z"), members(&mut a, b"z"));
        // Taken out and freed in pieces.
        let mut g = a.take(b"z").unwrap();
        let mut rounds = 1;
        while !g.free(100) {
            rounds += 1;
        }
        assert_eq!(rounds, 10);
        assert_eq!(a.type_of(b"z"), "none");
    }

    fn add(s: &mut Store, key: &[u8], flags: super::ZaddFlags, pairs: &[(f64, &str)]) -> Result<super::Zadded, Error> {
        let pairs: Vec<(f64, &[u8])> = pairs.iter().map(|&(sc, m)| (sc, m.as_bytes())).collect();
        s.zadd(key, flags, &pairs)
    }

    fn names(s: &mut Store, key: &[u8]) -> Vec<(String, f64)> {
        let z = s.zset_at(key).unwrap().unwrap();
        z.iter(0, usize::MAX, false).map(|(m, sc)| (String::from_utf8_lossy(m).into(), sc)).collect()
    }

    #[test]
    fn zadd_as_redis_zset_add() {
        use super::ZaddFlags as F;
        let mut s = store();
        s.record_touches(true);
        let r = add(&mut s, b"z", F::default(), &[(1.0, "a"), (2.0, "b"), (1.0, "a")]).unwrap();
        assert_eq!((r.added, r.updated, r.processed), (2, 0, 3));
        assert_eq!(s.take_touched().len(), 1);
        // XX on a missing key creates nothing.
        let r = add(&mut s, b"none", F { xx: true, ..F::default() }, &[(1.0, "a")]).unwrap();
        assert_eq!(r, super::Zadded::default());
        assert!(!s.exists(b"none") && s.take_touched().is_empty());
        // NX skips what's there, adds what isn't.
        let r = add(&mut s, b"z", F { nx: true, ..F::default() }, &[(9.0, "a"), (3.0, "c")]).unwrap();
        assert_eq!((r.added, r.updated, r.processed), (1, 0, 1));
        // XX updates only; an equal score is processed but not updated.
        let r = add(&mut s, b"z", F { xx: true, ..F::default() }, &[(5.0, "a"), (2.0, "b"), (1.0, "new")]).unwrap();
        assert_eq!((r.added, r.updated, r.processed), (0, 1, 2));
        // GT and LT compare with the score there; a new member is added anyway.
        let r = add(&mut s, b"z", F { gt: true, ..F::default() }, &[(4.0, "a"), (6.0, "b"), (0.0, "d")]).unwrap();
        assert_eq!((r.added, r.updated, r.processed), (1, 1, 2));
        let r = add(&mut s, b"z", F { lt: true, ..F::default() }, &[(7.0, "a"), (-1.0, "c")]).unwrap();
        assert_eq!((r.added, r.updated, r.processed), (0, 1, 1));
        assert_eq!(names(&mut s, b"z"), [("c".into(), -1.0), ("d".into(), 0.0), ("a".into(), 5.0), ("b".into(), 6.0)]);
        // INCR adds to the score, and replies the last processed one.
        let r = add(&mut s, b"z", F { incr: true, ..F::default() }, &[(2.5, "a")]).unwrap();
        assert_eq!((r.updated, r.score), (1, 7.5));
        let r = add(&mut s, b"z", F { incr: true, gt: true, ..F::default() }, &[(-1.0, "a")]).unwrap();
        assert_eq!(r.processed, 0, "GT refuses a decrement");
        // inf + -inf is NaN: refused, nothing changed.
        add(&mut s, b"z", F::default(), &[(f64::INFINITY, "i")]).unwrap();
        s.take_touched();
        assert_eq!(add(&mut s, b"z", F { incr: true, ..F::default() }, &[(f64::NEG_INFINITY, "i")]), Err(Error::ScoreNan));
        assert_eq!(s.zset_at(b"z").unwrap().unwrap().score(b"i"), Some(f64::INFINITY));
        assert!(s.take_touched().is_empty());
        s.set(b"str", b"v", Default::default()).unwrap();
        assert_eq!(add(&mut s, b"str", F::default(), &[(1.0, "a")]), Err(Error::WrongType));
    }

    #[test]
    fn zadd_sizes_a_new_set_as_redis() {
        let mut s = store();
        let long = "x".repeat(65);
        add(&mut s, b"a", Default::default(), &[(1.0, long.as_str()), (1.0, "b")]).unwrap();
        assert_eq!(s.encoding(b"a"), Some("skiplist"), "the first member decides");
        add(&mut s, b"b", Default::default(), &[(1.0, "b"), (1.0, long.as_str())]).unwrap();
        assert_eq!(s.encoding(b"b"), Some("skiplist"), "converted by the long one");
        let many: Vec<(f64, &str)> = (0..129).map(|_| (1.0, "same")).collect();
        add(&mut s, b"c", Default::default(), &many).unwrap();
        assert_eq!((s.encoding(b"c"), s.zset_at(b"c").unwrap().unwrap().len()), (Some("skiplist"), 1), "counted by its pairs");
        add(&mut s, b"d", Default::default(), &[(1.0, "a")]).unwrap();
        add(&mut s, b"d", Default::default(), &many[..128]).unwrap();
        assert_eq!(s.encoding(b"d"), Some("listpack"));
        add(&mut s, b"d", Default::default(), &many).unwrap();
        assert_eq!(s.encoding(b"d"), Some("skiplist"));
    }

    #[test]
    fn removals_delete_an_emptied_key() {
        let mut s = store();
        s.record_touches(true);
        add(&mut s, b"z", Default::default(), &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]).unwrap();
        s.take_touched();
        assert_eq!(s.zrem(b"z", &[b"a", b"x"]), Ok(1));
        assert_eq!(s.zrem(b"z", &[b"x"]), Ok(0));
        assert_eq!(s.take_touched().len(), 1, "only the removal that removed");
        assert_eq!(s.zrem_ranks(b"z", 1, 5), Ok(2));
        assert_eq!(names(&mut s, b"z"), [("b".into(), 2.0)]);
        assert_eq!(s.zrem_ranks(b"z", 0, 0), Ok(0));
        assert_eq!(s.zrem_ranks(b"z", 0, 1), Ok(1));
        assert!(!s.exists(b"z"));
        assert_eq!(s.zrem(b"z", &[b"a"]), Ok(0));
        assert_eq!(s.zrem_ranks(b"z", 0, 1), Ok(0));
        add(&mut s, b"p", Default::default(), &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).unwrap();
        assert_eq!(s.zpop(b"p", true, 1).unwrap().unwrap(), [(Box::from(&b"c"[..]), 3.0)]);
        assert_eq!(s.zpop(b"p", false, 0).unwrap().unwrap(), []);
        assert_eq!(s.zpop(b"p", false, 9).unwrap().unwrap(), [(Box::from(&b"a"[..]), 1.0), (Box::from(&b"b"[..]), 2.0)]);
        assert!(!s.exists(b"p"));
        assert_eq!(s.zpop(b"p", false, 1), Ok(None));
        s.set(b"str", b"v", Default::default()).unwrap();
        assert_eq!(s.zrem(b"str", &[b"a"]), Err(Error::WrongType));
        assert_eq!(s.zpop(b"str", false, 0), Err(Error::WrongType));
        assert_eq!(s.zrem_ranks(b"str", 0, 1), Err(Error::WrongType));
        check(&s);
    }

    #[test]
    fn zstore_replaces_or_deletes() {
        let mut s = store();
        s.set(b"d", b"v", Default::default()).unwrap();
        s.expire(ExpireCmd::Expire, b"d", 100, Default::default()).unwrap();
        assert_eq!(s.zstore(b"d", &[(b"a", 1.0), (b"b", -0.0)], 2), 2);
        assert_eq!((s.type_of(b"d"), s.pttl(b"d")), ("zset", -1), "a new value, no TTL");
        assert_eq!(s.zset_at(b"d").unwrap().unwrap().score(b"b").map(f64::to_bits), Some(0f64.to_bits()), "packed: -0 is 0");
        assert_eq!(s.zstore(b"d", &[], 0), 0);
        assert!(!s.exists(b"d"));
        let many: Vec<(Vec<u8>, f64)> = (0..10).map(|i| (format!("m{i}").into_bytes(), f64::from(i))).collect();
        let refs: Vec<(&[u8], f64)> = many.iter().map(|(m, sc)| (&m[..], *sc)).collect();
        s.zstore(b"e", &refs, 129);
        assert_eq!(s.encoding(b"e"), Some("skiplist"), "sized by the hint");
        s.zstore(b"f", &refs, 0);
        assert_eq!(s.encoding(b"f"), Some("listpack"));
        check(&s);
    }

    #[test]
    fn zrandmember_picks() {
        let mut s = store();
        assert_eq!(s.zrandmember(b"z"), Ok(None));
        assert_eq!(s.zrandmember_count(b"z", 3), Ok(vec![]));
        add(&mut s, b"z", Default::default(), &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")]).unwrap();
        let all: Vec<Vec<u8>> = s.zrandmember_count(b"z", 9).unwrap().into_iter().map(|(m, _)| m).collect();
        assert_eq!(all, [b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()], "all of it, from the end");
        for _ in 0..50 {
            let picked = s.zrandmember_count(b"z", 3).unwrap();
            assert_eq!(picked.len(), 3);
            assert!(picked.windows(2).all(|w| w[0].1 < w[1].1), "distinct, in order while packed");
            assert_eq!(s.zrandmember_count(b"z", -7).unwrap().len(), 7);
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            seen.insert(s.zrandmember(b"z").unwrap().unwrap());
        }
        assert_eq!(seen.len(), 4);
    }

    fn combined(s: &Store, keys: &[&str], op: SetOp, weights: &[f64], aggregate: super::Aggregate) -> Vec<(String, u64)> {
        let keys: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
        let opts = super::Combine { weights: weights.to_vec(), aggregate };
        let z = s.zcombine(&keys, op, &opts);
        assert_eq!(z.encoding(), "skiplist", "built large, as Redis builds it");
        z.iter(0, usize::MAX, false).map(|(m, sc)| (String::from_utf8_lossy(m).into(), sc.to_bits())).collect()
    }

    fn bits(pairs: &[(&str, f64)]) -> Vec<(String, u64)> {
        pairs.iter().map(|&(m, sc)| (m.to_string(), sc.to_bits())).collect()
    }

    #[test]
    fn union_inter_diff_as_redis() {
        use super::Aggregate as A;
        let mut s = store();
        add(&mut s, b"a", Default::default(), &[(1.0, "x"), (2.0, "y"), (3.0, "z")]).unwrap();
        add(&mut s, b"b", Default::default(), &[(4.0, "y"), (5.0, "w")]).unwrap();
        s.sadd(b"s", &[b"y", b"q"]).unwrap();
        s.zcheck(&[b"a", b"b", b"s", b"none"]).unwrap();
        assert_eq!(combined(&s, &["a", "b"], SetOp::Union, &[], A::Sum), bits(&[("x", 1.0), ("z", 3.0), ("w", 5.0), ("y", 6.0)]));
        assert_eq!(combined(&s, &["a", "s"], SetOp::Union, &[2.0, 10.0], A::Sum), bits(&[("x", 2.0), ("z", 6.0), ("q", 10.0), ("y", 14.0)]));
        assert_eq!(combined(&s, &["a", "b"], SetOp::Union, &[], A::Min), bits(&[("x", 1.0), ("y", 2.0), ("z", 3.0), ("w", 5.0)]));
        assert_eq!(combined(&s, &["a", "b"], SetOp::Union, &[], A::Max), bits(&[("x", 1.0), ("z", 3.0), ("y", 4.0), ("w", 5.0)]));
        assert_eq!(combined(&s, &["a", "b", "s"], SetOp::Union, &[1.0, 2.0, 0.5], A::Count), bits(&[("q", 0.5), ("x", 1.0), ("z", 1.0), ("w", 2.0), ("y", 3.5)]));
        assert_eq!(combined(&s, &["a", "b", "s"], SetOp::Inter, &[], A::Sum), bits(&[("y", 7.0)]));
        assert_eq!(combined(&s, &["a", "none"], SetOp::Inter, &[], A::Sum), []);
        assert_eq!(combined(&s, &["a", "a"], SetOp::Inter, &[1.0, -1.0], A::Max), bits(&[("x", 1.0), ("y", 2.0), ("z", 3.0)]));
        assert_eq!(combined(&s, &["a", "b"], SetOp::Diff, &[], A::Sum), bits(&[("x", 1.0), ("z", 3.0)]));
        assert_eq!(combined(&s, &["a", "a"], SetOp::Diff, &[], A::Sum), [], "a key twice takes everything");
        assert_eq!(combined(&s, &["s", "a"], SetOp::Diff, &[], A::Sum), bits(&[("q", 1.0)]));
        assert_eq!(combined(&s, &["none", "a"], SetOp::Diff, &[], A::Sum), []);
        // NaN is 0: inf * 0, and inf + -inf.
        add(&mut s, b"i", Default::default(), &[(f64::INFINITY, "m")]).unwrap();
        assert_eq!(combined(&s, &["i"], SetOp::Union, &[0.0], A::Sum), bits(&[("m", 0.0)]));
        assert_eq!(combined(&s, &["i", "i"], SetOp::Union, &[1.0, -1.0], A::Sum), bits(&[("m", 0.0)]));
        assert_eq!(combined(&s, &["i", "i"], SetOp::Inter, &[1.0, -1.0], A::Sum), bits(&[("m", 0.0)]));
        // -0 kept in a result; MIN and MAX keep the first of 0 and -0, the
        // smallest input first, ties in the order given.
        add(&mut s, b"m1", Default::default(), &[(0.0, "x")]).unwrap();
        add(&mut s, b"m2", Default::default(), &[(0.0, "x")]).unwrap();
        assert_eq!(combined(&s, &["m1"], SetOp::Union, &[-1.0], A::Sum), bits(&[("x", -0.0)]));
        assert_eq!(combined(&s, &["m1", "m2"], SetOp::Union, &[-1.0, 1.0], A::Min), bits(&[("x", -0.0)]));
        assert_eq!(combined(&s, &["m1", "m2"], SetOp::Union, &[1.0, -1.0], A::Min), bits(&[("x", 0.0)]));
        assert_eq!(combined(&s, &["m1", "m2"], SetOp::Union, &[1.0, -1.0], A::Max), bits(&[("x", 0.0)]));
        add(&mut s, b"m2", Default::default(), &[(1.0, "y")]).unwrap();
        assert_eq!(combined(&s, &["m2", "m1"], SetOp::Inter, &[1.0, -1.0], A::Min), bits(&[("x", -0.0)]), "m1 the smaller, so first");
        // Sums in that order: (0.1 + 0.2) + 0.3 isn't 0.1 + (0.2 + 0.3).
        for (k, v) in [("f1", 0.1), ("f2", 0.2), ("f3", 0.3)] {
            add(&mut s, k.as_bytes(), Default::default(), &[(v, "x")]).unwrap();
        }
        assert_eq!(combined(&s, &["f1", "f2", "f3"], SetOp::Union, &[], A::Sum), bits(&[("x", 0.1 + 0.2 + 0.3)]));
        assert_eq!(combined(&s, &["f3", "f2", "f1"], SetOp::Union, &[], A::Sum), bits(&[("x", 0.3 + 0.2 + 0.1)]));
        add(&mut s, b"f1", Default::default(), &[(1.0, "p"), (2.0, "q")]).unwrap();
        assert_eq!(combined(&s, &["f1", "f2", "f3"], SetOp::Union, &[], A::Sum)[0], ("x".to_string(), (0.2 + 0.3 + 0.1f64).to_bits()));
        assert_eq!(combined(&s, &["f1", "f2", "f3"], SetOp::Inter, &[], A::Sum), bits(&[("x", 0.2 + 0.3 + 0.1)]));
        // ZINTERCARD, and its limit.
        let keys = [&b"a"[..], b"b", b"s"];
        assert_eq!(s.zintercard(&keys, 0), 1);
        assert_eq!(s.zintercard(&[b"a"], 2), 2);
        assert_eq!(s.zintercard(&[b"a", b"a"], 0), 3);
        assert_eq!(s.zintercard(&[b"a", b"none"], 0), 0);
        s.set(b"str", b"v", Default::default()).unwrap();
        assert_eq!(s.zcheck(&[b"a", b"str"]), Err(Error::WrongType));
    }

    #[test]
    fn stored_results_pack_or_delete() {
        let mut s = store();
        add(&mut s, b"a", Default::default(), &[(0.0, "x"), (2.0, "y")]).unwrap();
        let z = s.zcombine(&[b"a"], SetOp::Union, &super::Combine { weights: vec![-1.0], aggregate: Default::default() });
        assert_eq!(s.zstore_set(b"d", z), 2);
        assert_eq!(s.encoding(b"d"), Some("listpack"));
        assert_eq!(s.zset_at(b"d").unwrap().unwrap().score(b"x").map(f64::to_bits), Some(0f64.to_bits()), "-0 is 0 once packed");
        let z = s.zcombine(&[b"none"], SetOp::Union, &Default::default());
        assert_eq!(s.zstore_set(b"d", z), 0);
        assert!(!s.exists(b"d"));
        let many: Vec<(f64, String)> = (0..129).map(|i| (f64::from(i), format!("m{i}"))).collect();
        let pairs: Vec<(f64, &str)> = many.iter().map(|(sc, m)| (*sc, m.as_str())).collect();
        add(&mut s, b"big", Default::default(), &pairs).unwrap();
        let z = s.zcombine(&[b"big"], SetOp::Union, &Default::default());
        s.zstore_set(b"d", z);
        assert_eq!(s.encoding(b"d"), Some("skiplist"));
        // ZMPOP: the first key that exists.
        assert_eq!(s.zmpop(&[b"none", b"a"], true, 5).unwrap(), Some((1, vec![(Box::from(&b"y"[..]), 2.0), (Box::from(&b"x"[..]), 0.0)])));
        assert!(!s.exists(b"a"));
        assert_eq!(s.zmpop(&[b"none", b"a"], false, 1).unwrap(), None);
        s.set(b"str", b"v", Default::default()).unwrap();
        assert_eq!(s.zmpop(&[b"str", b"big"], false, 1), Err(Error::WrongType));
        check(&s);
    }
}
