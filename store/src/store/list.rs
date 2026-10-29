//! The list commands (Redis 7.0's `t_list.c`), without the blocking ones.
//!
//! A list emptied by a command loses its key, as in Redis.

use super::{Store, Value};
use crate::collections::List;
use crate::Error;

/// Which of the keys a pop took from, and the elements, in pop order.
type Popped = (usize, Vec<Box<[u8]>>);


/// Which end of a list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// The head: `LPUSH`, `LPOP`, `LEFT`.
    Left,
    /// The tail.
    Right,
}

/// `LPOS`'s options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LposOptions {
    /// Which match to start from: 1 the first, -1 the first from the end.
    /// Never 0.
    pub rank: i64,
    /// How many matches to return; 0 all of them.
    pub count: usize,
    /// How many elements to look at; 0 all of them.
    pub maxlen: usize,
}

impl Default for LposOptions {
    fn default() -> LposOptions {
        LposOptions { rank: 1, count: 1, maxlen: 0 }
    }
}

impl Store {
    /// The list at `key`, `None` if there's none (`LRANGE`, `LINDEX`, ...).
    pub fn list_at(&mut self, key: &[u8]) -> Result<Option<&List>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_list()?)),
            None => Ok(None),
        }
    }

    /// The list at `key` to change, `None` if there's none.
    fn list_mut(&mut self, key: &[u8]) -> Result<Option<&mut List>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_list_mut()?)),
            None => Ok(None),
        }
    }

    /// Deletes `key` if its list is empty.
    fn drop_if_empty(&mut self, key: &[u8]) {
        if self.list_at(key).ok().flatten().is_some_and(List::is_empty) {
            self.remove(key);
        }
    }

    /// `LPUSH`, `RPUSH` (and with `only_existing` `LPUSHX`, `RPUSHX`): the
    /// new length, 0 if `only_existing` and there's no list.
    pub fn push(&mut self, key: &[u8], end: End, elements: &[&[u8]], only_existing: bool) -> Result<usize, Error> {
        if self.list_mut(key)?.is_none() {
            if only_existing {
                return Ok(0);
            }
            self.insert_value(key, Value::List(Box::default()), None);
        }
        let l = self.list_mut(key)?.expect("made above");
        l.grow(elements.iter().copied());
        for e in elements {
            match end {
                End::Left => l.push_front(e),
                End::Right => l.push_back(e),
            }
        }
        Ok(l.len())
    }

    /// `LPOP`, `RPOP` with a count (`None`: one): up to `count` elements.
    /// `None` without a list.
    pub fn pop(&mut self, key: &[u8], end: End, count: usize) -> Result<Option<Vec<Box<[u8]>>>, Error> {
        let Some(l) = self.list_mut(key)? else { return Ok(None) };
        let n = count.min(l.len());
        let popped: Vec<Box<[u8]>> = match end {
            End::Left => (0..n).map(|_| l.pop_front().expect("counted")).collect(),
            End::Right => (0..n).map(|_| l.pop_back().expect("counted")).collect(),
        };
        l.shrink();
        self.drop_if_empty(key);
        Ok(Some(popped))
    }

    /// `LMPOP`: up to `count` elements from the first of `keys` holding a
    /// list, and which key that was; `None` if none does. A key of another
    /// type before that one fails, any after it isn't looked at.
    pub fn mpop(&mut self, keys: &[&[u8]], end: End, count: usize) -> Result<Option<Popped>, Error> {
        for (i, key) in keys.iter().enumerate() {
            if self.list_at(key)?.is_some() {
                return Ok(self.pop(key, end, count)?.map(|popped| (i, popped)));
            }
        }
        Ok(None)
    }

    /// `LSET`.
    pub fn lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> Result<(), Error> {
        let l = self.list_mut(key)?.ok_or(Error::NoSuchKey)?;
        // As Redis: the encoding converts first, even out of range.
        l.grow([value]);
        let i = l.resolve(index).ok_or(Error::IndexOutOfRange)?;
        l.set(i, value);
        l.shrink();
        Ok(())
    }

    /// `LINSERT key BEFORE|AFTER pivot element` (`after`): the new length,
    /// -1 without the pivot, 0 without the list.
    pub fn linsert(&mut self, key: &[u8], after: bool, pivot: &[u8], element: &[u8]) -> Result<i64, Error> {
        let Some(l) = self.list_mut(key)? else { return Ok(0) };
        // As Redis: the encoding converts first, even without the pivot.
        l.grow([element]);
        let Some(i) = l.iter().position(|e| e == pivot) else { return Ok(-1) };
        l.insert_by(i, after, element);
        Ok(l.len() as i64)
    }

    /// `LREM key count element`: how many were removed; `count` > 0 the
    /// first ones from the head, < 0 from the tail, 0 all.
    pub fn lrem(&mut self, key: &[u8], count: i64, element: &[u8]) -> Result<usize, Error> {
        let Some(l) = self.list_mut(key)? else { return Ok(0) };
        let limit = if count == 0 { usize::MAX } else { count.unsigned_abs().try_into().unwrap_or(usize::MAX) };
        let removed = l.remove_matching(element, limit, count < 0);
        if removed > 0 {
            l.shrink();
        }
        self.drop_if_empty(key);
        Ok(removed)
    }

    /// `LTRIM`: keeps `start..=stop` (as `LRANGE` reads them).
    pub fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> Result<(), Error> {
        let Some(l) = self.list_mut(key)? else { return Ok(()) };
        match l.range(start, stop) {
            Some((from, to)) => l.trim(from, to),
            None => l.clear(),
        }
        l.shrink();
        self.drop_if_empty(key);
        Ok(())
    }

    /// `LPOS`: the positions of the matches (at most `count`, all if 0).
    pub fn lpos(&mut self, key: &[u8], element: &[u8], opts: LposOptions) -> Result<Vec<usize>, Error> {
        let Some(l) = self.list_at(key)? else { return Ok(Vec::new()) };
        let len = l.len();
        let maxlen = if opts.maxlen == 0 { len } else { opts.maxlen.min(len) };
        let want = if opts.count == 0 { usize::MAX } else { opts.count };
        let mut skip = opts.rank.unsigned_abs() - 1;
        let mut found = Vec::new();
        let positions: Box<dyn Iterator<Item = (usize, &[u8])>> = if opts.rank > 0 {
            Box::new(l.iter().take(maxlen).enumerate())
        } else {
            Box::new(l.iter_rev().take(maxlen).enumerate().map(|(k, e)| (len - 1 - k, e)))
        };
        for (i, e) in positions {
            if e == element {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                found.push(i);
                if found.len() >= want {
                    break;
                }
            }
        }
        Ok(found)
    }

    /// `LMOVE src dst from to` (`RPOPLPUSH` is `Right`, `Left`): the element
    /// moved, `None` without a source list.
    pub fn lmove(&mut self, src: &[u8], dst: &[u8], from: End, to: End) -> Result<Option<Vec<u8>>, Error> {
        // Redis's order: a missing source first, then both types.
        if self.list_mut(src)?.is_none() {
            return Ok(None);
        }
        self.list_at(dst)?;
        if src == dst {
            // In place: the key, and its TTL, never go away.
            let l = self.list_mut(src)?.expect("checked above");
            let e = match from {
                End::Left => l.pop_front(),
                End::Right => l.pop_back(),
            }
            .expect("a list is never empty");
            let copy = e.to_vec();
            l.grow([&e[..]]);
            match to {
                End::Left => l.push_front(&e),
                End::Right => l.push_back(&e),
            }
            l.shrink();
            return Ok(Some(copy));
        }
        let mut popped = self.pop(src, from, 1)?.expect("checked above");
        let element = popped.pop().expect("a list is never empty");
        self.push(dst, to, &[&element], false)?;
        Ok(Some(element.into()))
    }
}

impl Store {
    /// `LMOVEM src dst LEFT|RIGHT LEFT|RIGHT [COUNT|EXACTLY n OBO|BULK]`
    /// (Redis 8.10): up to `count` elements (exactly, or none, with
    /// `exactly`) moved from `src`'s `from` end to `dst`'s `to` end, in the
    /// order they now have in `dst`, which is also the reply's; `None` if
    /// nothing moved. The block keeps its order in the source (`BULK`), or
    /// each element is pushed as popped (`obo`). As Redis's
    /// `lmovemMoveAndReply`: popped one by one, the destination's encoding
    /// converted once for the block; the source converts back only when the
    /// destination is another key.
    #[allow(clippy::too_many_arguments)]
    pub fn lmovem(&mut self, src: &[u8], dst: &[u8], from: End, to: End, count: usize, exactly: bool, obo: bool) -> Result<Option<Vec<Box<[u8]>>>, Error> {
        let len = self.list_at(src)?.map_or(0, List::len);
        let n = match exactly {
            true if len < count => return Ok(None),
            _ => count.min(len),
        };
        if n == 0 {
            return Ok(None);
        }
        self.list_at(dst)?;
        let l = self.list_mut(src)?.expect("not empty");
        let mut moved: Vec<Box<[u8]>> = (0..n).map(|_| if from == End::Left { l.pop_front() } else { l.pop_back() }.expect("counted")).collect();
        if if obo { to == End::Left } else { from == End::Right } {
            moved.reverse();
        }
        if src != dst && self.list_mut(dst)?.is_none() {
            self.insert_value(dst, Value::List(Box::default()), None);
        }
        let d = self.list_mut(dst)?.expect("made above");
        d.grow(moved.iter().map(|e| &e[..]));
        if to == End::Left {
            moved.iter().rev().for_each(|e| d.push_front(e));
        } else {
            moved.iter().for_each(|e| d.push_back(e));
        }
        if src != dst {
            self.list_mut(src)?.expect("still there").shrink();
            self.drop_if_empty(src);
        }
        Ok(Some(moved))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{put_other, store};
    use super::{End, LposOptions};
    use crate::{Error, Store};

    fn items(s: &mut Store, key: &[u8]) -> Vec<String> {
        let l = s.list_at(key).unwrap();
        l.map_or(Vec::new(), |l| l.slice(0, l.len() - 1).map(|e| String::from_utf8_lossy(e).into()).collect())
    }

    #[test]
    fn push_pop_and_the_emptied_key() {
        let mut s = store();
        assert_eq!(s.push(b"l", End::Left, &[b"a", b"b"], false), Ok(2));
        assert_eq!(s.push(b"l", End::Right, &[b"c"], false), Ok(3));
        assert_eq!(items(&mut s, b"l"), ["b", "a", "c"]);
        assert_eq!(s.push(b"x", End::Left, &[b"a"], true), Ok(0));
        assert!(!s.exists(b"x"));
        let popped = s.pop(b"l", End::Right, 2).unwrap().unwrap();
        assert_eq!(popped, [Box::from(&b"c"[..]), Box::from(&b"a"[..])]);
        assert_eq!(s.pop(b"l", End::Left, 5).unwrap().unwrap().len(), 1);
        assert!(!s.exists(b"l"));
        assert_eq!(s.pop(b"l", End::Left, 1), Ok(None));
    }

    #[test]
    fn mpop() {
        let mut s = store();
        s.push(b"a", End::Right, &[b"1", b"2", b"3"], false).unwrap();
        s.push(b"b", End::Right, &[b"x"], false).unwrap();
        s.set(b"o", b"v", Default::default()).unwrap();
        let one = |e: &str| Box::from(e.as_bytes());
        assert_eq!(s.mpop(&[b"nope", b"a", b"b"], End::Right, 2), Ok(Some((1, vec![one("3"), one("2")]))));
        assert_eq!(s.mpop(&[b"a", b"b"], End::Left, 10), Ok(Some((0, vec![one("1")]))));
        assert!(!s.exists(b"a"));
        assert_eq!(s.mpop(&[b"b", b"o"], End::Left, 1), Ok(Some((0, vec![one("x")]))), "a type after isn't looked at");
        assert_eq!(s.mpop(&[b"nope", b"b"], End::Left, 1), Ok(None));
        assert_eq!(s.mpop(&[b"nope", b"o", b"b"], End::Left, 1), Err(Error::WrongType));
    }

    #[test]
    fn ranges_like_redis() {
        let mut s = store();
        s.push(b"l", End::Right, &[b"0", b"1", b"2", b"3", b"4"], false).unwrap();
        let l = s.list_at(b"l").unwrap().unwrap();
        assert_eq!(l.range(0, -1), Some((0, 4)));
        assert_eq!(l.range(-2, 100), Some((3, 4)));
        assert_eq!(l.range(-100, 1), Some((0, 1)));
        assert_eq!(l.range(3, 2), None);
        assert_eq!(l.range(5, 10), None);
        assert_eq!(l.range(0, -6), None);
        assert_eq!(l.range(i64::MIN, i64::MAX), Some((0, 4)));
        assert_eq!(l.get(-1), Some(&b"4"[..]));
        assert_eq!(l.get(5), None);
        assert_eq!(l.get(i64::MIN), None);
        s.ltrim(b"l", 1, -2).unwrap();
        assert_eq!(items(&mut s, b"l"), ["1", "2", "3"]);
        s.ltrim(b"l", 5, 6).unwrap();
        assert!(!s.exists(b"l"));
    }

    #[test]
    fn lset_linsert_lrem() {
        let mut s = store();
        assert_eq!(s.lset(b"l", 0, b"x"), Err(Error::NoSuchKey));
        s.push(b"l", End::Right, &[b"a", b"b", b"a", b"c", b"a"], false).unwrap();
        assert_eq!(s.lset(b"l", -1, b"z"), Ok(()));
        assert_eq!(s.lset(b"l", 5, b"z"), Err(Error::IndexOutOfRange));
        assert_eq!(s.linsert(b"l", true, b"c", b"d"), Ok(6));
        assert_eq!(s.linsert(b"l", false, b"q", b"d"), Ok(-1));
        assert_eq!(s.linsert(b"nope", false, b"q", b"d"), Ok(0));
        assert_eq!(items(&mut s, b"l"), ["a", "b", "a", "c", "d", "z"]);
        assert_eq!(s.lrem(b"l", -1, b"a"), Ok(1));
        assert_eq!(items(&mut s, b"l"), ["a", "b", "c", "d", "z"]);
        assert_eq!(s.lrem(b"l", 0, b"a"), Ok(1));
        for e in ["b", "c", "d", "z"] {
            s.lrem(b"l", 1, e.as_bytes()).unwrap();
        }
        assert!(!s.exists(b"l"));
    }

    #[test]
    fn lpos() {
        let mut s = store();
        s.push(b"l", End::Right, &[b"a", b"b", b"a", b"c", b"a"], false).unwrap();
        let pos = |s: &mut Store, rank, count, maxlen| s.lpos(b"l", b"a", LposOptions { rank, count, maxlen }).unwrap();
        assert_eq!(pos(&mut s, 1, 1, 0), [0]);
        assert_eq!(pos(&mut s, 2, 1, 0), [2]);
        assert_eq!(pos(&mut s, -1, 0, 0), [4, 2, 0]);
        assert_eq!(pos(&mut s, 1, 0, 2), [0]);
        assert_eq!(pos(&mut s, -2, 2, 0), [2, 0]);
        assert_eq!(pos(&mut s, 4, 1, 0), Vec::<usize>::new());
    }

    #[test]
    fn lmove() {
        let mut s = store();
        s.push(b"a", End::Right, &[b"1", b"2"], false).unwrap();
        s.expire(crate::ExpireCmd::PExpire, b"a", 5000, Default::default()).unwrap();
        assert_eq!(s.lmove(b"a", b"a", End::Left, End::Right), Ok(Some(b"1".to_vec())));
        assert_eq!(items(&mut s, b"a"), ["2", "1"]);
        assert_eq!(s.pttl(b"a"), 5000, "rotated in place, the TTL kept");
        s.push(b"one", End::Right, &[b"x"], false).unwrap();
        s.expire(crate::ExpireCmd::PExpire, b"one", 5000, Default::default()).unwrap();
        s.lmove(b"one", b"one", End::Right, End::Left).unwrap();
        assert_eq!(s.pttl(b"one"), 5000, "a list of one too");
        assert_eq!(s.lmove(b"a", b"b", End::Right, End::Left), Ok(Some(b"1".to_vec())));
        assert_eq!(s.lmove(b"a", b"b", End::Right, End::Left), Ok(Some(b"2".to_vec())));
        assert!(!s.exists(b"a"));
        assert_eq!(items(&mut s, b"b"), ["2", "1"]);
        put_other(&mut s, b"o");
        s.set(b"str", b"v", Default::default()).unwrap();
        assert_eq!(s.lmove(b"nope", b"str", End::Left, End::Left), Ok(None));
        assert_eq!(s.lmove(b"b", b"str", End::Left, End::Left), Err(Error::WrongType));
        assert_eq!(items(&mut s, b"b").len(), 2, "nothing popped");
    }
}
