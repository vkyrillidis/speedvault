//! A sorted set: members ordered by score, then by their bytes.
//!
//! Small ones are packed (Redis's listpack encoding): each member followed
//! by its score's 8 bytes, in order, found by a walk. Past
//! [`ZSET_MAX_LISTPACK_ENTRIES`] members, or with a member longer than
//! [`ZSET_MAX_LISTPACK_VALUE`] bytes, it converts, one way as in Redis, to
//! a table of members and scores (to find a member's score) beside a
//! counted B+ tree of (score, member) (the order and the ranks): Redis's
//! `skiplist` encoding, which keeps a dict beside a skiplist.
//!
//! Each member of a large set is stored once, in the table's block (its
//! hash, the member, the score); the tree's entries hold the score again
//! and the block's address, which stays put while the table moves its
//! blocks about. That's the one place a member is reached through a raw
//! pointer: the tree's entries are removed before their blocks are freed.
//!
//! The order a client sees is the same in both encodings, so the encoding
//! changes no reply, only `OBJECT ENCODING` and the memory.

use crate::block::Block;
use crate::packed::{At, Packed};
use crate::table::Table;
use crate::ztree::{cmp, Ranked, Tree};
use std::cmp::Ordering;
use std::ptr::NonNull;

/// Redis's `zset-max-listpack-entries`.
pub const ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
/// Redis's `zset-max-listpack-value`.
pub const ZSET_MAX_LISTPACK_VALUE: usize = 64;

/// A sorted set.
#[derive(Debug)]
pub struct ZSet {
    repr: Repr,
}

#[derive(Debug)]
enum Repr {
    /// Members and their scores in turn, in order.
    Packed(Packed),
    Big(Box<Big>),
}

#[derive(Debug, Default)]
struct Big {
    /// Member to score (8 bytes).
    members: Table,
    order: Tree<Entry>,
}

/// An entry of the tree: a score, and the block of the table that holds
/// the member.
#[derive(Clone, Copy, Debug)]
struct Entry {
    score: f64,
    block: NonNull<u8>,
}

impl Ranked for Entry {
    fn score(&self) -> f64 {
        self.score
    }

    fn member(&self) -> &[u8] {
        // SAFETY: an entry's block lives in the set's table for as long as
        // the entry is in the tree (`Big` removes the entry first), and a
        // block's member is never written over.
        unsafe { Block::key_at(self.block) }
    }
}

fn score_bytes(score: f64) -> [u8; 8] {
    score.to_bits().to_le_bytes()
}

fn read_score(bytes: &[u8]) -> f64 {
    f64::from_bits(u64::from_le_bytes(bytes.try_into().expect("a score is 8 bytes")))
}

impl Default for ZSet {
    fn default() -> ZSet {
        ZSet { repr: Repr::Packed(Packed::default()) }
    }
}

/// A packed set's pairs: where each member starts, the member, the score.
fn pairs(p: &Packed) -> impl DoubleEndedIterator<Item = (At, &[u8], f64)> + '_ {
    PairIter(p.iter())
}

struct PairIter<'a>(crate::packed::Iter<'a>);

impl<'a> Iterator for PairIter<'a> {
    type Item = (At, &'a [u8], f64);

    fn next(&mut self) -> Option<Self::Item> {
        let (at, member) = self.0.next()?;
        let (_, score) = self.0.next().expect("members and scores in turn");
        Some((at, member, read_score(score)))
    }
}

impl DoubleEndedIterator for PairIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (_, score) = self.0.next_back()?;
        let (at, member) = self.0.next_back().expect("members and scores in turn");
        Some((at, member, read_score(score)))
    }
}

impl ZSet {
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Packed(p) => p.len() / 2,
            Repr::Big(b) => b.order.len(),
        }
    }

    /// Once true, the store deletes the key.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding(&self) -> &'static str {
        match self.repr {
            Repr::Packed(_) => "listpack",
            Repr::Big(_) => "skiplist",
        }
    }

    /// An empty set for about `size` members of about `member_len` bytes,
    /// as Redis's `zsetTypeCreate` makes it: packed if they'd fit.
    #[allow(dead_code, reason = "for the sorted set commands, phase 9 milestone 2")]
    pub(crate) fn with_hint(size: usize, member_len: usize) -> ZSet {
        if size <= ZSET_MAX_LISTPACK_ENTRIES && member_len <= ZSET_MAX_LISTPACK_VALUE {
            ZSet::default()
        } else {
            ZSet { repr: Repr::Big(Box::default()) }
        }
    }

    /// Converts ahead of `size` members if they couldn't be packed
    /// (Redis's `zsetTypeMaybeConvert`: `ZADD` counts its pairs, new or not).
    #[allow(dead_code, reason = "for the sorted set commands, phase 9 milestone 2")]
    pub(crate) fn expect(&mut self, size: usize) {
        if matches!(self.repr, Repr::Packed(_)) && size > ZSET_MAX_LISTPACK_ENTRIES {
            self.convert();
        }
    }

    /// To the large encoding.
    fn convert(&mut self) {
        let Repr::Packed(p) = &self.repr else { return };
        let mut big = Big::default();
        for (_, member, score) in pairs(p) {
            let block = big.members.push_new(member, &score_bytes(score));
            big.order.insert(Entry { score, block });
        }
        self.repr = Repr::Big(Box::new(big));
    }

    /// The score of `member`.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match &self.repr {
            Repr::Packed(p) => pairs(p).find(|(_, m, _)| *m == member).map(|(.., s)| s),
            Repr::Big(b) => b.members.get(member).map(read_score),
        }
    }

    /// Gives `member` the score `score` (not NaN), adding it if it's new
    /// (true then). A score equal to the one it has (`-0` to `0` too) leaves
    /// it as it is, as in Redis.
    pub(crate) fn insert(&mut self, member: &[u8], score: f64) -> bool {
        debug_assert!(!score.is_nan());
        if let Repr::Packed(p) = &mut self.repr {
            let found = pairs(p).find(|(_, m, _)| *m == member).map(|(at, _, s)| (at, s));
            if let Some((at, current)) = found {
                if current == score {
                    return false;
                }
                let next = p.at(at.whole().end);
                p.remove_range(at.whole().start..next.whole().end, 2);
                Self::insert_packed(p, member, score);
                return false;
            }
            if p.len() / 2 < ZSET_MAX_LISTPACK_ENTRIES && member.len() <= ZSET_MAX_LISTPACK_VALUE {
                Self::insert_packed(p, member, score);
                return true;
            }
            self.convert();
        }
        let Repr::Big(b) = &mut self.repr else { unreachable!("converted above") };
        match b.members.position(member) {
            Some(pos) => {
                let current = read_score(b.members.value_at(pos));
                if current == score {
                    return false;
                }
                let e = b.order.remove(current, member).expect("in the tree");
                // The block stays where it is: 8 bytes over 8.
                b.members.overwrite_at(pos, &score_bytes(score));
                b.order.insert(Entry { score, block: e.block });
                false
            }
            None => {
                let block = b.members.push_new(member, &score_bytes(score));
                b.order.insert(Entry { score, block });
                true
            }
        }
    }

    /// Inserts a new pair at its place in a packed set.
    fn insert_packed(p: &mut Packed, member: &[u8], score: f64) {
        let at = pairs(p).find(|(_, m, s)| cmp(&(*s, *m), score, member) == Ordering::Greater).map(|(at, ..)| at.whole().start);
        let at = at.unwrap_or(p.bytes());
        p.insert_at(at, member);
        let after = p.at(at).whole().end;
        p.insert_at(after, &score_bytes(score));
    }

    /// Removes `member`; its score if it was there.
    #[allow(dead_code, reason = "for the sorted set commands, phase 9 milestone 2")]
    pub(crate) fn remove(&mut self, member: &[u8]) -> Option<f64> {
        match &mut self.repr {
            Repr::Packed(p) => {
                let (at, _, score) = pairs(p).find(|(_, m, _)| *m == member)?;
                let next = p.at(at.whole().end);
                p.remove_range(at.whole().start..next.whole().end, 2);
                Some(score)
            }
            Repr::Big(b) => {
                let pos = b.members.position(member)?;
                let score = read_score(b.members.value_at(pos));
                // The tree's entry first: it points at the block.
                b.order.remove(score, member).expect("in the tree");
                b.members.remove_swap_at(pos);
                Some(score)
            }
        }
    }

    /// Removes the member at `rank` (below the length), and returns it
    /// with its score.
    #[allow(dead_code, reason = "for the sorted set commands, phase 9 milestone 2")]
    pub(crate) fn remove_at(&mut self, rank: usize) -> (Box<[u8]>, f64) {
        match &mut self.repr {
            Repr::Packed(p) => {
                let (at, member, score) = pairs(p).nth(rank).expect("a rank below the length");
                let member = Box::from(member);
                let next = p.at(at.whole().end);
                p.remove_range(at.whole().start..next.whole().end, 2);
                (member, score)
            }
            Repr::Big(b) => {
                let e = b.order.remove_at(rank);
                let member: Box<[u8]> = e.member().into();
                b.members.remove_swap(&member);
                (member, e.score)
            }
        }
    }

    /// The rank of `member` (its place in the order from 0), and its score.
    pub fn rank(&self, member: &[u8]) -> Option<(usize, f64)> {
        match &self.repr {
            Repr::Packed(p) => pairs(p).enumerate().find(|(_, (_, m, _))| *m == member).map(|(r, (.., s))| (r, s)),
            Repr::Big(b) => {
                let score = read_score(b.members.get(member)?);
                Some((b.order.rank(score, member).expect("in the tree"), score))
            }
        }
    }

    /// The member at `rank`, and its score.
    pub fn get(&self, rank: usize) -> Option<(&[u8], f64)> {
        self.iter(rank, 1, false).next()
    }

    /// The rank of the first member for which `pred(score, member)`
    /// holds, the length if none: `pred` must be false and then true along
    /// the order.
    pub fn first_where(&self, pred: impl Fn(f64, &[u8]) -> bool) -> usize {
        match &self.repr {
            Repr::Packed(p) => pairs(p).take_while(|(_, m, s)| !pred(*s, m)).count(),
            Repr::Big(b) => b.order.first_where(|e| pred(e.score, e.member())),
        }
    }

    /// `count` members from `rank` on, with their scores, forwards or
    /// (`rev`) backwards: as many as there are.
    pub fn iter(&self, rank: usize, count: usize, rev: bool) -> ZIter<'_> {
        let len = self.len();
        let left = if rank < len { count.min(if rev { rank + 1 } else { len - rank }) } else { 0 };
        ZIter(match &self.repr {
            Repr::Packed(p) => {
                let mut it = PairIter(p.iter());
                if left > 0 {
                    if rev {
                        for _ in 0..len - 1 - rank {
                            it.next_back();
                        }
                    } else {
                        for _ in 0..rank {
                            it.next();
                        }
                    }
                }
                IterRepr::Packed(it, left, rev)
            }
            Repr::Big(b) => IterRepr::Tree(b.order.iter(rank, left, rev)),
        })
    }

    /// Frees up to `n` members of a set taken out of the store, leaving it
    /// unusable; true once it's all freed.
    pub(crate) fn free_last(&mut self, n: usize) -> bool {
        let done = match &mut self.repr {
            Repr::Packed(_) => true,
            // The tree's entries point at the blocks freed here, but
            // nothing reads them again: the tree goes as a whole below.
            Repr::Big(b) => b.members.free_last(n),
        };
        if done {
            self.repr = Repr::Packed(Packed::default());
        }
        done
    }
}

impl Ranked for (f64, &[u8]) {
    fn score(&self) -> f64 {
        self.0
    }

    fn member(&self) -> &[u8] {
        self.1
    }
}

/// [`ZSet::iter`]: members with their scores.
pub struct ZIter<'a>(IterRepr<'a>);

enum IterRepr<'a> {
    /// The pairs left on the way, how many to take, and the direction.
    Packed(PairIter<'a>, usize, bool),
    Tree(crate::ztree::Iter<'a, Entry>),
}

impl<'a> Iterator for ZIter<'a> {
    type Item = (&'a [u8], f64);

    fn next(&mut self) -> Option<(&'a [u8], f64)> {
        match &mut self.0 {
            IterRepr::Packed(it, left, rev) => {
                if *left == 0 {
                    return None;
                }
                *left -= 1;
                let (_, m, s) = if *rev { it.next_back() } else { it.next() }?;
                Some((m, s))
            }
            IterRepr::Tree(it) => {
                let e = it.next()?;
                // SAFETY: as in `Entry::member`; the set is borrowed for 'a.
                Some((unsafe { Block::key_at(e.block) }, e.score))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match &self.0 {
            IterRepr::Packed(_, left, _) => *left,
            IterRepr::Tree(it) => it.len(),
        };
        (n, Some(n))
    }
}

impl ExactSizeIterator for ZIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// The model: (score, member) pairs in order.
    fn sorted(model: &std::collections::HashMap<Vec<u8>, f64>) -> Vec<(Vec<u8>, f64)> {
        let mut v: Vec<(Vec<u8>, f64)> = model.iter().map(|(m, &s)| (m.clone(), s)).collect();
        v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
        v
    }

    fn check(z: &ZSet, model: &std::collections::HashMap<Vec<u8>, f64>, rng: &mut Rng) {
        let want = sorted(model);
        assert_eq!(z.len(), want.len());
        let got: Vec<(Vec<u8>, f64)> = z.iter(0, usize::MAX, false).map(|(m, s)| (m.to_vec(), s)).collect();
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(g.0, w.0);
            assert_eq!(g.1.to_bits(), w.1.to_bits(), "scores kept bit for bit");
        }
        if let Repr::Big(b) = &z.repr {
            b.order.check();
            assert_eq!(b.members.len(), b.order.len());
        }
        for _ in 0..10.min(want.len()) {
            let r = rng.below(want.len() as u64) as usize;
            let (m, s) = &want[r];
            assert_eq!(z.score(m).map(f64::to_bits), Some(s.to_bits()));
            assert_eq!(z.rank(m).map(|(r, _)| r), Some(r));
            assert_eq!(z.get(r).map(|(m, _)| m.to_vec()), Some(m.clone()));
            let n = rng.below(20) as usize;
            let back: Vec<Vec<u8>> = z.iter(r, n, true).map(|(m, _)| m.to_vec()).collect();
            let want_back: Vec<Vec<u8>> = want[(r + 1).saturating_sub(n)..=r].iter().rev().map(|(m, _)| m.clone()).collect();
            assert_eq!(back, want_back);
            let bound = *s;
            assert_eq!(z.first_where(|sc, _| sc >= bound), want.iter().filter(|(_, sc)| *sc < bound).count());
        }
        assert_eq!(z.score(b"missing"), None);
        assert_eq!(z.rank(b"missing"), None);
    }

    /// Random adds, updates and removals against a `HashMap`, packed and
    /// through the conversion; the encoding as Redis's.
    #[test]
    fn agrees_with_a_model() {
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        for round in 0..6 {
            let mut z = ZSet::default();
            let mut model = std::collections::HashMap::new();
            let (members, steps) = if round < 3 { (100, 3000) } else { (3000, 20_000) };
            for step in 0..steps {
                let m = format!("member-{}", rng.below(members)).into_bytes();
                let score = match rng.below(4) {
                    0 => 0.0,
                    1 => -0.0,
                    _ => (rng.below(50) as f64 - 25.0) / 2.0,
                };
                match rng.below(10) {
                    0..=5 => {
                        let new = !model.contains_key(&m);
                        // An equal score keeps the old one, -0 or 0.
                        if model.get(&m).is_none_or(|&old: &f64| old != score) {
                            model.insert(m.clone(), score);
                        }
                        assert_eq!(z.insert(&m, score), new);
                    }
                    6..=7 => assert_eq!(z.remove(&m).map(f64::to_bits), model.remove(&m).map(f64::to_bits)),
                    _ if !model.is_empty() => {
                        let r = rng.below(model.len() as u64) as usize;
                        let want = sorted(&model)[r].clone();
                        let (m, s) = z.remove_at(r);
                        assert_eq!((m.to_vec(), s.to_bits()), (want.0.clone(), want.1.to_bits()));
                        model.remove(&want.0);
                    }
                    _ => {}
                }
                if step % 97 == 0 {
                    check(&z, &model, &mut rng);
                }
            }
            check(&z, &model, &mut rng);
            if round >= 3 {
                assert_eq!(z.encoding(), "skiplist", "past 128 members");
            }
            while !z.is_empty() {
                z.remove_at(0);
            }
        }
    }

    #[test]
    fn encodings_as_redis() {
        let mut z = ZSet::default();
        for i in 0..128 {
            z.insert(format!("m{i}").as_bytes(), i as f64);
        }
        assert_eq!(z.encoding(), "listpack");
        z.insert(b"m0", 5.0);
        assert_eq!(z.encoding(), "listpack", "an update adds nothing");
        z.insert(b"m128", 1.0);
        assert_eq!(z.encoding(), "skiplist");
        for i in 0..120 {
            z.remove(format!("m{i}").as_bytes());
        }
        assert_eq!(z.encoding(), "skiplist", "one way");

        let mut z = ZSet::default();
        z.insert(&[b'x'; 64], 1.0);
        assert_eq!(z.encoding(), "listpack");
        z.insert(&[b'y'; 65], 1.0);
        assert_eq!(z.encoding(), "skiplist");
        assert_eq!(z.iter(0, 10, false).map(|(m, _)| m[0]).collect::<Vec<_>>(), [b'x', b'y']);

        assert_eq!(ZSet::with_hint(128, 64).encoding(), "listpack");
        assert_eq!(ZSet::with_hint(129, 1).encoding(), "skiplist");
        assert_eq!(ZSet::with_hint(1, 65).encoding(), "skiplist");
        let mut z = ZSet::default();
        z.insert(b"a", 1.0);
        z.expect(128);
        assert_eq!(z.encoding(), "listpack");
        z.expect(129);
        assert_eq!(z.encoding(), "skiplist");
        assert_eq!(z.score(b"a"), Some(1.0));
    }

    #[test]
    fn ties_order_by_member_bytes() {
        for big in [false, true] {
            let mut z = if big { ZSet::with_hint(1000, 1) } else { ZSet::default() };
            for m in [&b"b"[..], b"a", b"ab", b"", b"\xff", b"B"] {
                z.insert(m, 1.0);
            }
            z.insert(b"z", -0.0);
            z.insert(b"y", f64::NEG_INFINITY);
            z.insert(b"x", f64::INFINITY);
            let order: Vec<&[u8]> = z.iter(0, 100, false).map(|(m, _)| m).collect();
            assert_eq!(order, [&b"y"[..], b"z", b"", b"B", b"a", b"ab", b"b", b"\xff", b"x"], "big: {big}");
            assert_eq!(z.score(b"z").unwrap().to_bits(), (-0.0f64).to_bits());
            z.insert(b"z", 0.0);
            assert_eq!(z.score(b"z").unwrap().to_bits(), (-0.0f64).to_bits(), "0 equals -0: no update");
        }
    }

    #[test]
    fn frees_in_pieces() {
        let mut z = ZSet::with_hint(1000, 1);
        for i in 0..10_000 {
            z.insert(format!("{i}").as_bytes(), i as f64);
        }
        let mut rounds = 0;
        while !z.free_last(1000) {
            rounds += 1;
        }
        assert_eq!(rounds, 9);
    }
}
