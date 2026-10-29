//! The set commands (Redis 7.0's `t_set.c`).
//!
//! A set emptied by a command loses its key, as in Redis. Results that
//! Redis builds as a new set (`SUNION`, `SINTER`, `SDIFF`, the `STORE`
//! variants) are built as a [`Set`] here too, from empty, so they come out
//! an intset (in numeric order) exactly when Redis's would.

use super::{Effect, Store, Value};
use crate::collections::Set;
use crate::Error;


/// Which of `SUNION`, `SINTER`, `SDIFF`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Inter,
    Diff,
}

impl Store {
    /// The set at `key`, `None` if there's none (`SMEMBERS`, `SCARD`, ...).
    pub fn set_at(&mut self, key: &[u8]) -> Result<Option<&Set>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_set()?)),
            None => Ok(None),
        }
    }

    /// `SADD`: how many members are new.
    pub fn sadd(&mut self, key: &[u8], members: &[&[u8]]) -> Result<usize, Error> {
        match self.live(key) {
            Some(e) => {
                let s = e.as_set_mut()?;
                Ok(members.iter().filter(|m| s.insert(m)).count())
            }
            None => {
                let mut s = Set::default();
                let added = members.iter().filter(|m| s.insert(m)).count();
                self.insert_value(key, Value::Set(Box::new(s)), None);
                Ok(added)
            }
        }
    }

    /// `SREM`: how many of `members` were there.
    pub fn srem(&mut self, key: &[u8], members: &[&[u8]]) -> Result<usize, Error> {
        let Some(e) = self.live(key) else { return Ok(0) };
        let s = e.as_set_mut()?;
        let removed = members.iter().filter(|m| s.remove(m)).count();
        if s.is_empty() {
            self.remove(key);
        }
        Ok(removed)
    }

    /// `SPOP key`: a random member, now removed.
    pub fn spop(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
        let len = match self.live(key) {
            None => {
                self.effect(|| Effect::Nothing);
                return Ok(None);
            }
            Some(e) => e.as_set()?.len(),
        };
        let pos = self.random_below(len);
        let s = self.live(key).expect("looked up above").as_set_mut()?;
        let member = s.take_at(pos);
        if s.is_empty() {
            self.remove(key);
        }
        self.effect(|| Effect::As(vec![b"SREM".to_vec(), key.to_vec(), member.to_vec()]));
        Ok(Some(member))
    }

    /// `SPOP key count`: up to `count` random members, now removed. All of
    /// them, in the set's order, when `count` covers the set, as in Redis.
    pub fn spop_count(&mut self, key: &[u8], count: usize) -> Result<Vec<Box<[u8]>>, Error> {
        let len = match self.live(key) {
            None => {
                self.effect(|| Effect::Nothing);
                return Ok(Vec::new());
            }
            Some(e) => e.as_set()?.len(),
        };
        let popped: Vec<Box<[u8]>> = if count >= len {
            match self.remove(key).map(|e| e.into_value()) {
                Some(Value::Set(s)) => s.members().into_iter().map(Box::from).collect(),
                _ => unreachable!("type checked above"),
            }
        } else {
            let mut popped = Vec::with_capacity(count);
            for left in (len - count + 1..=len).rev() {
                let pos = self.random_below(left);
                let s = self.live(key).expect("not emptied").as_set_mut()?;
                popped.push(s.take_at(pos));
            }
            popped
        };
        self.effect(|| match popped.is_empty() {
            true => Effect::Nothing,
            false => Effect::As([b"SREM".to_vec(), key.to_vec()].into_iter().chain(popped.iter().map(|m| m.to_vec())).collect()),
        });
        Ok(popped)
    }

    /// `SRANDMEMBER key`: a random member.
    pub fn srandmember(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let len = match self.live(key) {
            None => return Ok(None),
            Some(e) => e.as_set()?.len(),
        };
        let pos = self.random_below(len);
        Ok(Some(self.live(key).expect("looked up above").as_set()?.member_at(pos).to_vec()))
    }

    /// `SRANDMEMBER key count`: `count` > 0 distinct members (all of them,
    /// in the set's order, if it covers the set); `count` < 0, `-count`
    /// members that may repeat.
    pub fn srandmember_count(&mut self, key: &[u8], count: i64) -> Result<Vec<Vec<u8>>, Error> {
        let len = match self.live(key) {
            None => return Ok(Vec::new()),
            Some(e) => e.as_set()?.len(),
        };
        let mut picks: Vec<usize> = Vec::new();
        if count < 0 {
            let n = count.unsigned_abs() as usize;
            for _ in 0..n {
                picks.push(self.random_below(len));
            }
        } else if count as u64 >= len as u64 {
            let s = self.live(key).expect("looked up above").as_set()?;
            return Ok(s.members().into_iter().map(<[u8]>::to_vec).collect());
        } else {
            // A partial Fisher-Yates over the positions.
            let mut all: Vec<usize> = (0..len).collect();
            for i in 0..count as usize {
                let j = i + self.random_below(len - i);
                all.swap(i, j);
            }
            all.truncate(count as usize);
            picks = all;
        }
        let s = self.live(key).expect("looked up above").as_set()?;
        Ok(picks.into_iter().map(|pos| s.member_at(pos).to_vec()).collect())
    }

    /// `SMOVE src dst member`: true if it moved (or `src` is `dst` and holds
    /// it).
    pub fn smove(&mut self, src: &[u8], dst: &[u8], member: &[u8]) -> Result<bool, Error> {
        // Redis's order: a missing source first, then both types.
        let Some(e) = self.live(src) else { return Ok(false) };
        let has = e.as_set()?.contains(member);
        if let Some(e) = self.live(dst) {
            e.as_set()?;
        }
        if src == dst || !has {
            return Ok(has);
        }
        self.srem(src, &[member])?;
        self.sadd(dst, &[member])?;
        Ok(true)
    }

    /// `SUNION`, `SINTER`, `SDIFF` of `keys` (at least one). Every key's
    /// type is checked first, missing keys or not (Redis 7.0.15).
    pub fn combine(&mut self, op: SetOp, keys: &[&[u8]]) -> Result<Set, Error> {
        if self.check_sets(keys)? && op == SetOp::Inter {
            return Ok(Set::default());
        }
        let sets = self.sets(keys);
        let mut out = Set::default();
        match op {
            SetOp::Union => {
                for s in sets.iter().flatten() {
                    for m in s.members() {
                        out.insert(m);
                    }
                }
            }
            SetOp::Inter => {
                let mut sets: Vec<&Set> = sets.into_iter().flatten().collect();
                // As Redis: walk the smallest, look the members up in the rest.
                sets.sort_by_key(|s| s.len());
                for m in sets[0].members() {
                    if sets[1..].iter().all(|s| s.contains(m)) {
                        out.insert(m);
                    }
                }
            }
            SetOp::Diff => {
                if let Some(first) = sets[0] {
                    for m in first.members() {
                        if !sets[1..].iter().flatten().any(|s| s.contains(m)) {
                            out.insert(m);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// `SINTERCARD`: the size of the intersection of `keys` (at least
    /// one), counted up to `limit` (0: no limit). Types checked as `SINTER`.
    pub fn sintercard(&mut self, keys: &[&[u8]], limit: usize) -> Result<usize, Error> {
        if self.check_sets(keys)? {
            return Ok(0);
        }
        let mut sets: Vec<&Set> = self.sets(keys).into_iter().flatten().collect();
        sets.sort_by_key(|s| s.len());
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(sets[0].iter().filter(|m| sets[1..].iter().all(|s| s.contains(m))).take(limit).count())
    }

    /// Checks that each of `keys` that exists holds a set (expiring the
    /// keys due), missing keys or not, as Redis 7.0.15: whether one is
    /// missing.
    fn check_sets(&mut self, keys: &[&[u8]]) -> Result<bool, Error> {
        let mut missing = false;
        for key in keys {
            match self.live(key) {
                None => missing = true,
                Some(e) => {
                    e.as_set()?;
                }
            }
        }
        Ok(missing)
    }

    /// The sets at `keys`, after [`check_sets`](Self::check_sets): every
    /// one borrowed at once.
    fn sets(&self, keys: &[&[u8]]) -> Vec<Option<&Set>> {
        keys.iter().map(|k| self.keys.get(k).map(|e| e.as_set().expect("checked"))).collect()
    }

    /// `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`: the result replaces
    /// whatever `dst` held (no TTL); an empty one deletes `dst`. Its size.
    pub fn combine_store(&mut self, op: SetOp, dst: &[u8], keys: &[&[u8]]) -> Result<usize, Error> {
        let result = self.combine(op, keys)?;
        let len = result.len();
        self.del(dst);
        if len > 0 {
            self.insert_value(dst, Value::Set(Box::new(result)), None);
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{put_other, store};
    use super::SetOp;
    use crate::Error;

    fn members(s: &mut crate::Store, key: &[u8]) -> Vec<String> {
        let set = s.set_at(key).unwrap();
        set.map_or(Vec::new(), |s| s.members().iter().map(|m| String::from_utf8_lossy(m).into()).collect())
    }

    #[test]
    fn sadd_srem_and_the_emptied_key() {
        let mut s = store();
        assert_eq!(s.sadd(b"s", &[b"3", b"1", b"2", b"1"]), Ok(3));
        assert_eq!(members(&mut s, b"s"), ["1", "2", "3"]);
        assert_eq!(s.encoding(b"s"), Some("intset"));
        assert_eq!(s.sadd(b"s", &[b"a", b"3"]), Ok(1));
        assert_eq!(s.encoding(b"s"), Some("hashtable"));
        assert_eq!(s.srem(b"s", &[b"a", b"x"]), Ok(1));
        assert_eq!(s.srem(b"s", &[b"1", b"2", b"3"]), Ok(3));
        assert!(!s.exists(b"s"));
        assert_eq!(s.srem(b"s", &[b"1"]), Ok(0));
    }

    #[test]
    fn spop_takes_everything_once() {
        let mut s = store();
        let all: Vec<Vec<u8>> = (0..50).map(|n| format!("m{n}").into_bytes()).collect();
        let refs: Vec<&[u8]> = all.iter().map(|m| &m[..]).collect();
        s.sadd(b"s", &refs).unwrap();
        let mut got: Vec<Vec<u8>> = s.spop_count(b"s", 20).unwrap().into_iter().map(Vec::from).collect();
        assert_eq!(got.len(), 20);
        while let Some(m) = s.spop(b"s").unwrap() {
            got.push(m.into());
        }
        assert!(!s.exists(b"s"));
        got.sort();
        let mut want = all.clone();
        want.sort();
        assert_eq!(got, want);
        assert_eq!(s.spop_count(b"s", 3), Ok(Vec::new()));
    }

    #[test]
    fn spop_count_covering_the_set_gives_its_order() {
        let mut s = store();
        s.sadd(b"s", &[b"9", b"-1", b"5"]).unwrap();
        let got: Vec<Vec<u8>> = s.spop_count(b"s", 5).unwrap().into_iter().map(Vec::from).collect();
        assert_eq!(got, [b"-1".to_vec(), b"5".to_vec(), b"9".to_vec()]);
        assert!(!s.exists(b"s"));
    }

    #[test]
    fn srandmember() {
        let mut s = store();
        s.sadd(b"s", &[b"a", b"b", b"c", b"d"]).unwrap();
        let picks = s.srandmember_count(b"s", 3).unwrap();
        assert_eq!(picks.len(), 3);
        let mut distinct = picks.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), 3);
        assert_eq!(s.srandmember_count(b"s", 10).unwrap().len(), 4);
        assert_eq!(s.srandmember_count(b"s", -10).unwrap().len(), 10);
        assert_eq!(s.srandmember_count(b"s", 0).unwrap().len(), 0);
        assert!(s.srandmember(b"s").unwrap().is_some());
        assert_eq!(s.set_at(b"s").unwrap().unwrap().len(), 4, "nothing removed");
        assert_eq!(s.srandmember(b"nope"), Ok(None));
    }

    #[test]
    fn smove() {
        let mut s = store();
        s.sadd(b"a", &[b"x", b"y"]).unwrap();
        assert_eq!(s.smove(b"a", b"b", b"x"), Ok(true));
        assert_eq!(s.smove(b"a", b"b", b"x"), Ok(false));
        assert_eq!(s.smove(b"a", b"a", b"y"), Ok(true));
        assert_eq!(s.smove(b"a", b"b", b"y"), Ok(true));
        assert!(!s.exists(b"a"));
        assert_eq!(members(&mut s, b"b").len(), 2);
        put_other(&mut s, b"l");
        assert_eq!(s.smove(b"nope", b"l", b"x"), Ok(false), "a missing source comes first");
        assert_eq!(s.smove(b"b", b"l", b"x"), Err(Error::WrongType));
        assert_eq!(s.smove(b"l", b"b", b"x"), Err(Error::WrongType));
    }

    #[test]
    fn combine() {
        let mut s = store();
        s.sadd(b"a", &[b"1", b"2", b"3"]).unwrap();
        s.sadd(b"b", &[b"3", b"4"]).unwrap();
        put_other(&mut s, b"l");
        let names = |set: crate::collections::Set| -> Vec<Vec<u8>> { set.members().iter().map(|m| m.to_vec()).collect() };
        assert_eq!(names(s.combine(SetOp::Union, &[b"b", b"a", b"nope"]).unwrap()), [b"1", b"2", b"3", b"4"].map(|m| m.to_vec()));
        assert_eq!(names(s.combine(SetOp::Inter, &[b"a", b"b"]).unwrap()), [b"3".to_vec()]);
        assert_eq!(names(s.combine(SetOp::Diff, &[b"a", b"b"]).unwrap()), [b"1".to_vec(), b"2".to_vec()]);
        assert!(s.combine(SetOp::Inter, &[b"a", b"nope"]).unwrap().members().is_empty());
        assert!(s.combine(SetOp::Inter, &[b"a", b"nope", b"l"]).is_err(), "Redis checks every type");
        assert!(s.combine(SetOp::Union, &[b"a", b"nope", b"l"]).is_err());
        assert!(s.combine(SetOp::Diff, &[b"nope", b"l"]).is_err());
        assert_eq!(s.combine_store(SetOp::Union, b"l", &[b"a", b"b"]), Ok(4));
        assert_eq!(s.type_of(b"l"), "set");
        assert_eq!(s.combine_store(SetOp::Inter, b"l", &[b"a", b"nope"]), Ok(0));
        assert!(!s.exists(b"l"));
    }

    #[test]
    fn sintercard() {
        let mut s = store();
        s.sadd(b"a", &[b"a", b"b", b"c", b"d"]).unwrap();
        s.sadd(b"b", &[b"b", b"c", b"d", b"e"]).unwrap();
        put_other(&mut s, b"l");
        assert_eq!(s.sintercard(&[b"a", b"b"], 0), Ok(3));
        assert_eq!(s.sintercard(&[b"a", b"b"], 2), Ok(2));
        assert_eq!(s.sintercard(&[b"a", b"b"], 9), Ok(3));
        assert_eq!(s.sintercard(&[b"a"], 0), Ok(4));
        assert_eq!(s.sintercard(&[b"a", b"a"], 0), Ok(4));
        assert_eq!(s.sintercard(&[b"nope"], 0), Ok(0));
        assert_eq!(s.sintercard(&[b"nope", b"l"], 0), Err(Error::WrongType), "Redis checks every type");
        assert_eq!(s.sintercard(&[b"l", b"nope"], 0), Err(Error::WrongType));
    }
}
