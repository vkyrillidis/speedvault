//! `SCAN`, `HSCAN`, `SSCAN` and `KEYS`.
//!
//! A cursor must guarantee what Redis's does: an element present for the
//! whole scan is returned at least once (maybe more); one added or removed
//! meanwhile may or may not be. The keys sit in the dense `Vec`s of their
//! tables, a hash's or set's members in its table's `Vec`; removal is a
//! `swap_remove`, which moves the last element
//! into the hole. Walked from the end, that keeps the guarantee: the
//! element moved is either already returned (then it may come again) or
//! not reached yet (then it still will be), and new elements land at the
//! end, behind the walk. The cursor says where the walk is.
//!
//! Collections Redis keeps compact (a listpack hash, an intset) it returns
//! whole in one reply with cursor 0, in their order; so do these.

use super::Store;
use crate::kv::Val;
use crate::glob::glob_match;
use crate::Error;

/// A position within a list meaning "from its end": a fresh list.
const FRESH: u64 = 0xffff_ffff;

/// `(list, left)` from a `SCAN` cursor: the list being walked and how many
/// of its positions are still to come (from 0 up), or [`FRESH`].
fn decode(cursor: u64) -> (usize, u64) {
    if cursor == 0 {
        (0, FRESH)
    } else {
        ((cursor >> 32) as usize, cursor & FRESH)
    }
}

/// Never 0, the cursor of a finished scan.
fn encode(list: usize, left: u64) -> u64 {
    ((list as u64) << 32) | left
}

/// Whether `s` matches `pattern` (`None`, or a lone `*`: everything).
fn matching(pattern: Option<&[u8]>, s: &[u8]) -> bool {
    match pattern {
        None | Some(b"*") => true,
        Some(p) => glob_match(p, s),
    }
}

/// Walks a list from the end, from where `left` says, picking up to
/// `count`; returns the new `left` (0: done) and what it picked.
fn walk<T>(len: usize, left: u64, count: usize, mut pick: impl FnMut(usize) -> T) -> (u64, Vec<T>) {
    let mut left = if left == FRESH { len } else { (left as usize).min(len) };
    let mut picked = Vec::new();
    while left > 0 && picked.len() < count {
        left -= 1;
        picked.push(pick(left));
    }
    (left as u64, picked)
}

impl Store {
    /// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`: the next
    /// cursor (0 when done) and the keys, about `count` of them examined.
    /// Expired keys are reclaimed rather than returned.
    pub fn scan(&mut self, cursor: u64, count: usize, pattern: Option<&[u8]>, kind: Option<&[u8]>) -> (u64, Vec<Vec<u8>>) {
        let (mut list, mut left) = decode(cursor);
        let mut picked = Vec::new();
        let lists = &self.keys.tables;
        while list < lists.len() && picked.len() < count {
            // A held slot's keys aren't listed.
            if self.table_held(list as u32) {
                list += 1;
                left = FRESH;
                continue;
            }
            let keys = lists[list].entries();
            let (now_left, mut more) = walk(keys.len(), left, count - picked.len(), |i| keys[i].key().to_vec());
            picked.append(&mut more);
            if now_left == 0 {
                list += 1;
                left = FRESH;
            } else {
                left = now_left;
            }
        }
        let next = if list >= lists.len() { 0 } else { encode(list, left) };
        let mut keys = Vec::with_capacity(picked.len());
        for key in picked {
            if !matching(pattern, &key) {
                continue;
            }
            let Some(e) = self.live(&key) else { continue };
            if kind.is_some_and(|k| !k.eq_ignore_ascii_case(e.type_name().as_bytes())) {
                continue;
            }
            keys.push(key);
        }
        (next, keys)
    }

    /// `KEYS pattern`: every live key matching, in no particular order.
    /// Expired keys are skipped, not reclaimed, as in Redis; so are the held
    /// slots' keys.
    pub fn keys_matching(&self, pattern: &[u8]) -> Vec<&[u8]> {
        let now = self.now;
        let served = self.keys.tables.iter().enumerate().filter(|&(t, _)| !self.table_held(t as u32));
        served.flat_map(|(_, t)| t.entries()).filter(|e| now <= e.expire_at() && matching(Some(pattern), e.key())).map(|e| e.key()).collect()
    }

    /// `HSCAN key cursor [MATCH pattern] [COUNT count]`: the next cursor
    /// and the fields with their values. (0, nothing) without the key.
    #[allow(clippy::type_complexity)]
    pub fn hscan(&mut self, key: &[u8], cursor: u64, count: usize, pattern: Option<&[u8]>) -> Result<(u64, Vec<(Vec<u8>, Vec<u8>)>), Error> {
        let Some(e) = self.live(key) else { return Ok((0, Vec::new())) };
        let Val::Hash(h) = e.value() else { return Err(Error::WrongType) };
        let pair = |(f, v): (&[u8], &[u8])| (f.to_vec(), v.to_vec());
        let keep = |(f, _): &(Vec<u8>, Vec<u8>)| matching(pattern, f);
        if !h.is_big() {
            return Ok((0, h.iter().map(pair).filter(keep).collect()));
        }
        let (left, picked) = walk(h.len(), cursor_left(cursor), count, |i| h.iter_from(i).next().map(pair).expect("below len"));
        Ok((left, picked.into_iter().filter(keep).collect()))
    }

    /// `SSCAN key cursor [MATCH pattern] [COUNT count]`.
    pub fn sscan(&mut self, key: &[u8], cursor: u64, count: usize, pattern: Option<&[u8]>) -> Result<(u64, Vec<Vec<u8>>), Error> {
        let Some(e) = self.live(key) else { return Ok((0, Vec::new())) };
        let Val::Set(s) = e.value() else { return Err(Error::WrongType) };
        if !s.is_big() {
            return Ok((0, s.members().into_iter().filter(|m| matching(pattern, m)).map(<[u8]>::to_vec).collect()));
        }
        let (left, picked) = walk(s.len(), cursor_left(cursor), count, |i| s.member_at(i).to_vec());
        Ok((left, picked.into_iter().filter(|m| matching(pattern, m)).collect()))
    }
}

/// A collection's cursor: how many positions are left; 0 starts afresh.
fn cursor_left(cursor: u64) -> u64 {
    if cursor == 0 {
        FRESH
    } else {
        cursor
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check, put_other, slotted, store};
    use crate::{Error, Store};
    use std::collections::HashSet;

    /// Scans to the end, `count` at a time, calling `between` after each
    /// call; every key returned, repeats included.
    fn scan_all(s: &mut Store, count: usize, mut between: impl FnMut(&mut Store, usize)) -> Vec<Vec<u8>> {
        let mut cursor = 0;
        let mut all = Vec::new();
        for round in 0.. {
            let (next, mut keys) = s.scan(cursor, count, None, None);
            all.append(&mut keys);
            if next == 0 {
                break;
            }
            between(s, round);
            cursor = next;
        }
        all
    }

    fn key(n: usize) -> Vec<u8> {
        format!("k{n}").into_bytes()
    }

    #[test]
    fn every_key_once_when_nothing_changes() {
        for mut s in [store(), slotted()] {
            for n in 0..1000 {
                s.set(&key(n), b"v", Default::default()).unwrap();
            }
            let all = scan_all(&mut s, 7, |_, _| {});
            assert_eq!(all.len(), 1000);
            assert_eq!(all.iter().collect::<HashSet<_>>().len(), 1000);
        }
        assert_eq!(store().scan(0, 10, None, None), (0, Vec::new()));
    }

    /// The guarantee: keys there throughout come at least once, whatever
    /// is deleted or added between calls.
    #[test]
    fn keys_there_throughout_come_at_least_once() {
        for mut s in [store(), slotted()] {
            for n in 0..2000 {
                s.set(&key(n), b"v", Default::default()).unwrap();
            }
            // Keys 0..1000 stay; the others go, and new ones come.
            let mut next_new = 5000;
            let all = scan_all(&mut s, 13, |s, round| {
                for i in 0..10 {
                    s.del(&key(1000 + (round * 37 + i * 101) % 1000));
                    s.set(&key(next_new), b"v", Default::default()).unwrap();
                    next_new += 1;
                }
            });
            let seen: HashSet<Vec<u8>> = all.into_iter().collect();
            for n in 0..1000 {
                assert!(seen.contains(&key(n)), "key {n} missed");
            }
        }
    }

    #[test]
    fn a_standalone_store_spreads_its_keys_over_the_tables() {
        let mut s = store();
        for n in 0..1000 {
            s.set(&key(n), b"v", Default::default()).unwrap();
        }
        check(&s);
        let used = s.keys.tables.iter().filter(|t| t.len() > 0).count();
        assert!(used > 900, "{used} tables for 1000 keys");
        assert_eq!(s.scan(0, 2000, None, None).1.len(), 1000);
        for n in 0..500 {
            s.del(&key(n));
            s.set(&key(1000 + n), b"v", Default::default()).unwrap();
        }
        check(&s);
        assert_eq!(s.scan(0, 2000, None, None).1.len(), 1000);
        s.flush();
        check(&s);
        assert_eq!(s.scan(0, 10, None, None), (0, Vec::new()), "empty tables, all walked");
    }

    #[test]
    fn filters() {
        let mut s = store();
        s.set(b"user:1", b"v", Default::default()).unwrap();
        s.set(b"user:2", b"v", Default::default()).unwrap();
        s.set(b"other", b"v", Default::default()).unwrap();
        s.set(b"", b"empty", Default::default()).unwrap();
        put_other(&mut s, b"user:list");
        let mut got = s.scan(0, 100, Some(b"user:*"), None).1;
        got.sort();
        assert_eq!(got, [&b"user:1"[..], b"user:2", b"user:list"]);
        assert_eq!(s.scan(0, 100, Some(b"user:*"), Some(b"LIST")).1, [b"user:list".to_vec()]);
        assert_eq!(s.scan(0, 100, Some(b"*"), None).1.len(), 5, "a lone * takes the empty key too");
        let mut keys = s.keys_matching(b"user:?");
        keys.sort();
        assert_eq!(keys, [&b"user:1"[..], b"user:2"]);
        assert_eq!(s.keys_matching(b"*").len(), 5);
        // Expired keys: reclaimed by SCAN, skipped by KEYS.
        s.expire(crate::ExpireCmd::PExpire, b"other", 5, Default::default()).unwrap();
        let now = s.now();
        s.set_now(now + 10);
        assert_eq!(s.keys_matching(b"other"), Vec::<&[u8]>::new());
        assert_eq!(s.dbsize(), 5);
        assert!(s.scan(0, 100, Some(b"other"), None).1.is_empty());
        assert_eq!(s.dbsize(), 4);
    }

    #[test]
    fn small_collections_come_whole() {
        let mut s = store();
        s.hset(b"h", &[b"b", b"1", b"a", b"2"]).unwrap();
        assert_eq!(s.hscan(b"h", 0, 1, None).unwrap(), (0, vec![(b"b".to_vec(), b"1".to_vec()), (b"a".to_vec(), b"2".to_vec())]));
        assert_eq!(s.hscan(b"h", 0, 1, Some(b"a")).unwrap().1.len(), 1);
        s.sadd(b"s", &[b"3", b"1", b"2"]).unwrap();
        assert_eq!(s.sscan(b"s", 0, 1, None).unwrap(), (0, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]));
        assert_eq!(s.hscan(b"nope", 0, 1, None).unwrap(), (0, Vec::new()));
        assert_eq!(s.sscan(b"h", 0, 1, None), Err(Error::WrongType));
        assert_eq!(s.hscan(b"s", 0, 1, None), Err(Error::WrongType));
    }

    #[test]
    fn big_collections_walk_with_the_guarantee() {
        let mut s = store();
        let members: Vec<Vec<u8>> = (0..1000).map(|n| format!("m{n}").into_bytes()).collect();
        let refs: Vec<&[u8]> = members.iter().map(|m| &m[..]).collect();
        s.sadd(b"s", &refs).unwrap();
        let (mut cursor, mut seen) = (0, HashSet::new());
        let mut round = 0;
        loop {
            let (next, got) = s.sscan(b"s", cursor, 10, None).unwrap();
            seen.extend(got);
            if next == 0 {
                break;
            }
            // Members 0..500 stay; the others go, new ones come.
            let gone = format!("m{}", 500 + (round * 7) % 500).into_bytes();
            let new = format!("new{round}").into_bytes();
            s.srem(b"s", &[&gone]).unwrap();
            s.sadd(b"s", &[&new]).unwrap();
            round += 1;
            cursor = next;
        }
        for m in &members[..500] {
            assert!(seen.contains(m), "{m:?} missed");
        }
        // A small set of strings: packed, but a hashtable to Redis 7.0.15,
        // so scanned a few at a time. Its removals shift the members after
        // the hole down, which the walk from the end allows too.
        let small: Vec<Vec<u8>> = (0..100).map(|n| format!("p{n}").into_bytes()).collect();
        let refs: Vec<&[u8]> = small.iter().map(|m| &m[..]).collect();
        s.sadd(b"p", &refs).unwrap();
        assert_eq!(s.encoding(b"p"), Some("hashtable"));
        let (mut cursor, mut seen, mut round) = (0, HashSet::new(), 0);
        loop {
            let (next, got) = s.sscan(b"p", cursor, 3, None).unwrap();
            seen.extend(got);
            if next == 0 {
                break;
            }
            let gone = format!("p{}", 50 + (round * 13) % 50).into_bytes();
            s.srem(b"p", &[&gone]).unwrap();
            s.sadd(b"p", &[format!("q{round}").as_bytes()]).unwrap();
            round += 1;
            cursor = next;
        }
        assert!(round > 10);
        for m in &small[..50] {
            assert!(seen.contains(m), "{m:?} missed");
        }
        // A big hash, the same way, without changes.
        let pairs: Vec<&[u8]> = members.iter().flat_map(|m| [&m[..], b"v"]).collect();
        s.hset(b"h", &pairs).unwrap();
        let (mut cursor, mut fields) = (0, Vec::new());
        loop {
            let (next, got) = s.hscan(b"h", cursor, 10, None).unwrap();
            fields.extend(got.into_iter().map(|(f, _)| f));
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(fields.len(), 1000);
        assert_eq!(fields.into_iter().collect::<HashSet<_>>().len(), 1000);
    }
}
