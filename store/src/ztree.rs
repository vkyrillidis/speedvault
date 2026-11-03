//! A counted B+ tree: the order of a large sorted set, by score and then
//! member bytes, with the number of entries under each child kept in its
//! parent so that a rank (`ZRANK`, `ZRANGE` by index) is O(log n).
//!
//! Redis keeps the same order in a skiplist: a node per member (its own
//! allocation: member pointer, score, back pointer, 1.33 levels on
//! average of 16 bytes each). Here a leaf holds up to [`MAX`] entries side
//! by side, each a score and a pointer to its member, and an inner node up
//! to [`MAX`] children with their counts and their first entries (to choose
//! a child without visiting it).
//!
//! Nodes stay between [`MIN`] and [`MAX`] wide (the root, the first leaf
//! and the last excepted): a full
//! leaf splits in two before it takes another entry, an inner node that
//! grows past [`MAX`] splits after, one that shrinks below [`MIN`]
//! merges with a neighbour, or takes entries from it if the two wouldn't
//! fit one node. All leaves are at the same depth.
//!
//! No node's vectors ever double past what it can hold: a leaf has room
//! for [`MAX`] entries (1 KiB) from its split on, an inner node for
//! [`MAX`] + 1 children from its making. (Splitting a leaf once it held
//! 65 doubled it to 128 places, which both halves kept: a third of the
//! tree's memory was empty places, 45 bytes a member of a set of 1M.)
//!
//! A full last leaf taking an entry past its end keeps its entries and
//! the new one starts a leaf of its own, and the same at the start of the
//! first leaf: scores that only grow (timestamps) or only shrink fill the
//! leaves, where halves would leave every one of them half empty for good.
//! Only there: anywhere else, scores chosen to land at the end of full
//! leaves could make a leaf of one entry each.

use std::cmp::Ordering;

/// The widest a node gets: entries in a leaf, children in an inner node.
const MAX: usize = 64;
/// The narrowest a node other than the root gets.
const MIN: usize = MAX / 4;

/// What the tree orders: a score that's never NaN, and a member.
pub(crate) trait Ranked: Copy {
    fn score(&self) -> f64;
    fn member(&self) -> &[u8];
}

/// `e` against (`score`, `member`): scores as numbers (`-0` equal to `0`),
/// then members as bytes.
pub(crate) fn cmp<E: Ranked>(e: &E, score: f64, member: &[u8]) -> Ordering {
    e.score().partial_cmp(&score).expect("scores are never NaN").then_with(|| e.member().cmp(member))
}

#[derive(Debug)]
enum Node<E> {
    Leaf(Vec<E>),
    Inner(Box<Inner<E>>),
}

#[derive(Debug)]
struct Inner<E> {
    children: Vec<Node<E>>,
    /// How many entries under each child.
    counts: Vec<usize>,
    /// Each child's first entry.
    firsts: Vec<E>,
}

impl<E> Inner<E> {
    /// Empty, with room for the [`MAX`] + 1 children it holds before it
    /// splits, so none of its vectors ever grows.
    fn with_room() -> Inner<E> {
        Inner { children: Vec::with_capacity(MAX + 1), counts: Vec::with_capacity(MAX + 1), firsts: Vec::with_capacity(MAX + 1) }
    }
}

impl<E: Ranked> Node<E> {
    /// Entries in a leaf, children in an inner node.
    fn width(&self) -> usize {
        match self {
            Node::Leaf(v) => v.len(),
            Node::Inner(n) => n.children.len(),
        }
    }

    /// How many entries it holds.
    fn count(&self) -> usize {
        match self {
            Node::Leaf(v) => v.len(),
            Node::Inner(n) => n.counts.iter().sum(),
        }
    }

    fn first(&self) -> E {
        match self {
            Node::Leaf(v) => v[0],
            Node::Inner(n) => n.firsts[0],
        }
    }

    /// Adds `e` (not there yet); a node split off on the right if it grew
    /// too wide. `first`, `last`: whether the node is on the tree's first
    /// or last path.
    fn insert(&mut self, e: E, first: bool, last: bool) -> Option<Node<E>> {
        match self {
            Node::Leaf(v) => {
                let pos = v.partition_point(|x| cmp(x, e.score(), e.member()) == Ordering::Less);
                if v.len() < MAX {
                    // A leaf grows by doubling from empty: to MAX at most.
                    v.insert(pos, e);
                    return None;
                }
                let mut right = Vec::with_capacity(MAX);
                if last && pos == MAX {
                    // Past the end of the last leaf: it stays full.
                    right.push(e);
                    return Some(Node::Leaf(right));
                }
                if first && pos == 0 {
                    // Before the start of the first one: the same.
                    right.append(v);
                    v.push(e);
                    return Some(Node::Leaf(right));
                }
                // Full: in two first, then the entry into its half.
                let half = MAX / 2;
                right.extend(v.drain(half..));
                if pos <= half {
                    v.insert(pos, e);
                } else {
                    right.insert(pos - half, e);
                }
                Some(Node::Leaf(right))
            }
            Node::Inner(n) => {
                let i = n.child_for(e.score(), e.member()).unwrap_or(0);
                let (first, last) = (first && i == 0, last && i + 1 == n.children.len());
                let split = n.children[i].insert(e, first, last);
                n.counts[i] += 1;
                n.firsts[i] = n.children[i].first();
                if let Some(right) = split {
                    n.counts[i] = n.children[i].count();
                    n.counts.insert(i + 1, right.count());
                    n.firsts.insert(i + 1, right.first());
                    n.children.insert(i + 1, right);
                }
                (n.children.len() > MAX).then(|| {
                    let half = n.children.len() / 2;
                    let mut right = Inner::with_room();
                    right.children.extend(n.children.drain(half..));
                    right.counts.extend(n.counts.drain(half..));
                    right.firsts.extend(n.firsts.drain(half..));
                    Node::Inner(Box::new(right))
                })
            }
        }
    }

    /// Removes the entry (`score`, `member`) if it's there.
    fn remove(&mut self, score: f64, member: &[u8]) -> Option<E> {
        match self {
            Node::Leaf(v) => {
                let pos = v.partition_point(|x| cmp(x, score, member) == Ordering::Less);
                (pos < v.len() && cmp(&v[pos], score, member) == Ordering::Equal).then(|| v.remove(pos))
            }
            Node::Inner(n) => {
                let i = n.child_for(score, member)?;
                let removed = n.children[i].remove(score, member)?;
                n.counts[i] -= 1;
                n.after_removal(i);
                Some(removed)
            }
        }
    }

    /// Removes the entry at `rank` (below the count).
    fn remove_at(&mut self, rank: usize) -> E {
        match self {
            Node::Leaf(v) => v.remove(rank),
            Node::Inner(n) => {
                let (i, rest) = n.child_at(rank);
                let removed = n.children[i].remove_at(rest);
                n.counts[i] -= 1;
                n.after_removal(i);
                removed
            }
        }
    }
}

impl<E: Ranked> Inner<E> {
    /// The child that holds (`score`, `member`) if anything does: the last
    /// whose first entry isn't past it. `None` if it's before them all.
    fn child_for(&self, score: f64, member: &[u8]) -> Option<usize> {
        self.firsts.partition_point(|f| cmp(f, score, member) != Ordering::Greater).checked_sub(1)
    }

    /// The child that holds the entry at `rank`, and its rank there.
    fn child_at(&self, mut rank: usize) -> (usize, usize) {
        for (i, &c) in self.counts.iter().enumerate() {
            if rank < c {
                return (i, rank);
            }
            rank -= c;
        }
        panic!("a rank below the count");
    }

    /// After child `i` lost an entry: its first entry again, and if it
    /// got too narrow, merged with a neighbour or evened out with it.
    fn after_removal(&mut self, i: usize) {
        if self.children[i].width() >= MIN || self.children.len() == 1 {
            if self.children[i].width() > 0 {
                self.firsts[i] = self.children[i].first();
            }
            return;
        }
        let a = if i + 1 < self.children.len() { i } else { i - 1 };
        let b = a + 1;
        let (left, right) = self.children.split_at_mut(b);
        let (left, right) = (&mut left[a], &mut right[0]);
        if left.width() + right.width() <= MAX {
            match (left, right) {
                (Node::Leaf(l), Node::Leaf(r)) => l.append(r),
                (Node::Inner(l), Node::Inner(r)) => {
                    l.children.append(&mut r.children);
                    l.counts.append(&mut r.counts);
                    l.firsts.append(&mut r.firsts);
                }
                _ => unreachable!("leaves all at one depth"),
            }
            self.counts[a] += self.counts[b];
            self.children.remove(b);
            self.counts.remove(b);
            self.firsts.remove(b);
            self.firsts[a] = self.children[a].first();
            return;
        }
        // Too many for one node: half each.
        let half = (left.width() + right.width()) / 2;
        match (left, right) {
            (Node::Leaf(l), Node::Leaf(r)) => {
                if l.len() > half {
                    let moved: Vec<E> = l.drain(half..).collect();
                    r.splice(0..0, moved);
                } else {
                    let take = half - l.len();
                    l.extend(r.drain(..take));
                }
            }
            (Node::Inner(l), Node::Inner(r)) => {
                if l.children.len() > half {
                    let c: Vec<Node<E>> = l.children.drain(half..).collect();
                    r.children.splice(0..0, c);
                    let c: Vec<usize> = l.counts.drain(half..).collect();
                    r.counts.splice(0..0, c);
                    let c: Vec<E> = l.firsts.drain(half..).collect();
                    r.firsts.splice(0..0, c);
                } else {
                    let take = half - l.children.len();
                    l.children.extend(r.children.drain(..take));
                    l.counts.extend(r.counts.drain(..take));
                    l.firsts.extend(r.firsts.drain(..take));
                }
            }
            _ => unreachable!("leaves all at one depth"),
        }
        self.counts[a] = self.children[a].count();
        self.counts[b] = self.children[b].count();
        self.firsts[a] = self.children[a].first();
        self.firsts[b] = self.children[b].first();
    }
}

/// The tree.
#[derive(Debug)]
pub(crate) struct Tree<E> {
    root: Node<E>,
    len: usize,
}

impl<E> Default for Tree<E> {
    fn default() -> Tree<E> {
        Tree { root: Node::Leaf(Vec::new()), len: 0 }
    }
}

impl<E: Ranked> Tree<E> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Adds `e`, which isn't there.
    pub(crate) fn insert(&mut self, e: E) {
        if let Some(right) = self.root.insert(e, true, true) {
            let left = std::mem::replace(&mut self.root, Node::Leaf(Vec::new()));
            let mut root = Inner::with_room();
            root.counts.extend([left.count(), right.count()]);
            root.firsts.extend([left.first(), right.first()]);
            root.children.extend([left, right]);
            self.root = Node::Inner(Box::new(root));
        }
        self.len += 1;
    }

    /// The root again after a removal: an inner node with one child gives
    /// way to it.
    fn shrink(&mut self) {
        while let Node::Inner(n) = &mut self.root {
            if n.children.len() > 1 {
                break;
            }
            self.root = n.children.pop().expect("one child");
        }
    }

    /// Removes (`score`, `member`) if it's there.
    pub(crate) fn remove(&mut self, score: f64, member: &[u8]) -> Option<E> {
        let removed = self.root.remove(score, member)?;
        self.len -= 1;
        self.shrink();
        Some(removed)
    }

    /// Removes the entry at `rank` (below [`len`](Self::len)).
    pub(crate) fn remove_at(&mut self, rank: usize) -> E {
        assert!(rank < self.len, "a rank below the length");
        let removed = self.root.remove_at(rank);
        self.len -= 1;
        self.shrink();
        removed
    }

    /// The rank of (`score`, `member`), if it's there.
    pub(crate) fn rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        let mut node = &self.root;
        let mut rank = 0;
        loop {
            match node {
                Node::Leaf(v) => {
                    let pos = v.partition_point(|x| cmp(x, score, member) == Ordering::Less);
                    return (pos < v.len() && cmp(&v[pos], score, member) == Ordering::Equal).then_some(rank + pos);
                }
                Node::Inner(n) => {
                    let i = n.child_for(score, member)?;
                    rank += n.counts[..i].iter().sum::<usize>();
                    node = &n.children[i];
                }
            }
        }
    }

    /// The entry at `rank`.
    #[cfg(test)]
    pub(crate) fn get(&self, rank: usize) -> Option<&E> {
        if rank >= self.len {
            return None;
        }
        let (mut node, mut rank) = (&self.root, rank);
        loop {
            match node {
                Node::Leaf(v) => return v.get(rank),
                Node::Inner(n) => {
                    let (i, rest) = n.child_at(rank);
                    (node, rank) = (&n.children[i], rest);
                }
            }
        }
    }

    /// The rank of the first entry for which `pred` holds, `len` if none:
    /// `pred` is false and then true along the order.
    pub(crate) fn first_where(&self, pred: impl Fn(&E) -> bool) -> usize {
        let (mut node, mut rank) = (&self.root, 0);
        loop {
            match node {
                Node::Leaf(v) => return rank + v.partition_point(|x| !pred(x)),
                Node::Inner(n) => {
                    let i = n.firsts.partition_point(|f| !pred(f)).saturating_sub(1);
                    rank += n.counts[..i].iter().sum::<usize>();
                    node = &n.children[i];
                }
            }
        }
    }

    /// `count` entries from `rank` on, forwards or (`rev`) backwards: as
    /// many as there are.
    pub(crate) fn iter(&self, rank: usize, count: usize, rev: bool) -> Iter<'_, E> {
        let left = if rank < self.len { count.min(if rev { rank + 1 } else { self.len - rank }) } else { 0 };
        let mut it = Iter { stack: Vec::new(), leaf: &[], pos: 0, left, rev };
        if left > 0 {
            let (mut node, mut rank) = (&self.root, rank);
            loop {
                match node {
                    Node::Leaf(v) => {
                        (it.leaf, it.pos) = (v, rank);
                        break;
                    }
                    Node::Inner(n) => {
                        let (i, rest) = n.child_at(rank);
                        it.stack.push((&**n, i));
                        (node, rank) = (&n.children[i], rest);
                    }
                }
            }
        }
        it
    }

    /// The invariants: order, counts, first entries, widths, depth.
    #[cfg(test)]
    pub(crate) fn check(&self) {
        fn walk<E: Ranked>(node: &Node<E>, root: bool, edge: (bool, bool), depth: usize, leaves: &mut Vec<usize>, out: &mut Vec<E>) {
            // The first leaf and the last may be narrower (not empty).
            let outer_leaf = (edge.0 || edge.1) && matches!(node, Node::Leaf(_));
            if !root && !outer_leaf {
                assert!(node.width() >= MIN, "a node too narrow: {}", node.width());
            }
            assert!(root || node.width() >= 1, "an empty node");
            assert!(node.width() <= MAX);
            match node {
                Node::Leaf(v) => {
                    leaves.push(depth);
                    out.extend_from_slice(v);
                }
                Node::Inner(n) => {
                    assert!(n.children.len() >= 2 || !root, "a root with one child");
                    assert_eq!(n.children.len(), n.counts.len());
                    assert_eq!(n.children.len(), n.firsts.len());
                    for (i, c) in n.children.iter().enumerate() {
                        assert_eq!(c.count(), n.counts[i]);
                        let f = c.first();
                        assert_eq!(cmp(&n.firsts[i], f.score(), f.member()), Ordering::Equal);
                        walk(c, false, (edge.0 && i == 0, edge.1 && i + 1 == n.children.len()), depth + 1, leaves, out);
                    }
                }
            }
        }
        let (mut leaves, mut out) = (Vec::new(), Vec::new());
        walk(&self.root, true, (true, true), 0, &mut leaves, &mut out);
        assert!(leaves.windows(2).all(|w| w[0] == w[1]), "leaves at one depth");
        assert_eq!(out.len(), self.len);
        for w in out.windows(2) {
            assert_eq!(cmp(&w[0], w[1].score(), w[1].member()), Ordering::Less, "in order");
        }
    }
}

/// [`Tree::iter`].
pub(crate) struct Iter<'a, E> {
    /// The inner nodes down to the leaf, with the child taken in each.
    stack: Vec<(&'a Inner<E>, usize)>,
    leaf: &'a [E],
    /// The next entry's place in the leaf.
    pos: usize,
    left: usize,
    rev: bool,
}

impl<E> Iter<'_, E> {
    /// To the next leaf in the direction of travel.
    fn step_leaf(&mut self) {
        // Up to the first node with a child further on.
        while let Some((n, i)) = self.stack.pop() {
            let next = if self.rev { i.checked_sub(1) } else { Some(i + 1).filter(|&j| j < n.children.len()) };
            if let Some(j) = next {
                self.stack.push((n, j));
                let mut node = &n.children[j];
                // And down its nearest edge.
                loop {
                    match node {
                        Node::Leaf(v) => {
                            self.leaf = v;
                            self.pos = if self.rev { v.len() - 1 } else { 0 };
                            return;
                        }
                        Node::Inner(m) => {
                            let k = if self.rev { m.children.len() - 1 } else { 0 };
                            self.stack.push((&**m, k));
                            node = &m.children[k];
                        }
                    }
                }
            }
        }
        unreachable!("an entry left to visit");
    }
}

impl<E: Copy> Iterator for Iter<'_, E> {
    type Item = E;

    fn next(&mut self) -> Option<E> {
        if self.left == 0 {
            return None;
        }
        let e = self.leaf[self.pos];
        self.left -= 1;
        if self.left > 0 {
            if self.rev {
                if self.pos == 0 {
                    self.step_leaf();
                } else {
                    self.pos -= 1;
                }
            } else {
                self.pos += 1;
                if self.pos == self.leaf.len() {
                    self.step_leaf();
                }
            }
        }
        Some(e)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl<E: Copy> ExactSizeIterator for Iter<'_, E> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct T(f64, &'static [u8]);

    impl Ranked for T {
        fn score(&self) -> f64 {
            self.0
        }
        fn member(&self) -> &[u8] {
            self.1
        }
    }

    /// The model's key: the score's order as an integer, then the member.
    fn key(score: f64, member: &[u8]) -> (i64, Vec<u8>) {
        let b = score.to_bits() as i64;
        (if b < 0 { i64::MIN - b } else { b }, member.to_vec())
    }

    fn member(n: u64) -> &'static [u8] {
        Box::leak(format!("m{n}").into_bytes().into_boxed_slice())
    }

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// Entries, the places of the leaves, and the bytes of the inner nodes'
    /// vectors (their capacities).
    fn room<E>(n: &Node<E>) -> (usize, usize, usize) {
        match n {
            Node::Leaf(v) => (v.len(), v.capacity(), 0),
            Node::Inner(i) => {
                let own = i.children.capacity() * std::mem::size_of::<Node<E>>()
                    + i.counts.capacity() * 8
                    + i.firsts.capacity() * std::mem::size_of::<E>();
                i.children.iter().map(room).fold((0, 0, own), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
            }
        }
    }

    #[test]
    fn leaves_fill_and_never_outgrow_their_places() {
        // No leaf past MAX places; random scores fill leaves to about 70%,
        // scores that only grow or only shrink to all but the last one.
        // The bytes of tree a member costs (an entry of 16 bytes, as a
        // sorted set's) printed (cargo test -- --nocapture).
        for (order, least) in [("random", 0.6), ("ascending", 0.95), ("descending", 0.95)] {
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
            let mut tree: Tree<T> = Tree::default();
            let n = 200_000u64;
            let members: Vec<&'static [u8]> = (0..n).map(member).collect();
            for i in 0..n {
                let score = match order {
                    "random" => rng.below(n) as f64,
                    "ascending" => i as f64,
                    _ => (n - i) as f64,
                };
                tree.insert(T(score, members[i as usize]));
            }
            tree.check();
            let (len, places, inner) = room(&tree.root);
            assert_eq!(len as u64, n);
            assert!(fn_max_places(&tree.root) <= MAX, "a leaf past {MAX} places");
            let fill = len as f64 / places as f64;
            assert!(fill >= least, "{order}: leaves {:.0}% full", 100.0 * fill);
            let per = (places * 16 + inner * 16 / std::mem::size_of::<T>()) as f64 / n as f64;
            println!("{order}: leaves {:.0}% full, {per:.1} bytes of tree a member", 100.0 * fill);
            // And back down: every other entry out, in order.
            let all: Vec<T> = tree.iter(0, usize::MAX, false).collect();
            for e in all.iter().step_by(2) {
                assert!(tree.remove(e.0, e.1).is_some());
            }
            tree.check();
            assert_eq!(tree.len() as u64, n / 2);
        }
    }

    /// The most places of any leaf.
    fn fn_max_places<E>(n: &Node<E>) -> usize {
        match n {
            Node::Leaf(v) => v.capacity(),
            Node::Inner(i) => i.children.iter().map(fn_max_places).max().unwrap_or(0),
        }
    }

    /// Random inserts and removals (by key and by rank) against a sorted
    /// `Vec`, through growth and back down to empty; ranks, lookups, the
    /// predicate search and both iterators checked as it goes.
    #[test]
    fn agrees_with_a_model() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut tree: Tree<T> = Tree::default();
        let mut model: Vec<((i64, Vec<u8>), T)> = Vec::new();
        let phases = [(20_000, 85), (15_000, 30), (10_000, 50), (30_000, 0)];
        for &(steps, insert_pct) in &phases {
            for step in 0..steps {
                if model.is_empty() || rng.below(100) < insert_pct {
                    // Few distinct scores, so members break many ties.
                    let score = (rng.below(400) as f64 - 200.0) / 4.0;
                    let score = if score == 0.0 && rng.below(2) == 0 { -0.0 } else { score };
                    let m = member(rng.below(1_000_000));
                    let k = key(score, m);
                    if let Err(at) = model.binary_search_by(|(mk, _)| mk.cmp(&k)) {
                        model.insert(at, (k, T(score, m)));
                        tree.insert(T(score, m));
                    }
                } else if rng.below(2) == 0 {
                    let i = rng.below(model.len() as u64) as usize;
                    let (_, e) = model.remove(i);
                    let removed = tree.remove(e.0, e.1).expect("there");
                    assert_eq!(removed.1, e.1);
                    assert!(tree.remove(e.0, e.1).is_none(), "gone");
                } else {
                    let i = rng.below(model.len() as u64) as usize;
                    let (k, _) = model.remove(i);
                    let removed = tree.remove_at(i);
                    assert_eq!(key(removed.0, removed.1), k);
                }
                assert_eq!(tree.len(), model.len());
                if step % 499 == 0 {
                    check_against(&tree, &model, &mut rng);
                }
            }
        }
        while let Some((_, e)) = model.pop() {
            assert_eq!(tree.remove(e.0, e.1).map(|r| r.1), Some(e.1));
        }
        assert_eq!(tree.len(), 0);
        tree.check();
    }

    fn check_against(tree: &Tree<T>, model: &[((i64, Vec<u8>), T)], rng: &mut Rng) {
        tree.check();
        let all: Vec<(i64, Vec<u8>)> = model.iter().map(|(k, _)| k.clone()).collect();
        let got: Vec<(i64, Vec<u8>)> = tree.iter(0, usize::MAX, false).map(|e| key(e.0, e.1)).collect();
        assert_eq!(got, all);
        let back: Vec<(i64, Vec<u8>)> = tree.iter(tree.len().saturating_sub(1), usize::MAX, true).map(|e| key(e.0, e.1)).collect();
        assert_eq!(back, all.iter().rev().cloned().collect::<Vec<_>>());
        for _ in 0..20 {
            if all.is_empty() {
                break;
            }
            let r = rng.below(all.len() as u64) as usize;
            let e = tree.get(r).unwrap();
            assert_eq!(key(e.0, e.1), all[r]);
            assert_eq!(tree.rank(e.0, e.1), Some(r));
            let n = rng.below(150) as usize;
            let fw: Vec<_> = tree.iter(r, n, false).map(|e| key(e.0, e.1)).collect();
            assert_eq!(fw, all[r..(r + n).min(all.len())]);
            let bw: Vec<_> = tree.iter(r, n, true).map(|e| key(e.0, e.1)).collect();
            let want: Vec<_> = all[(r + 1).saturating_sub(n)..=r].iter().rev().cloned().collect();
            assert_eq!(bw, want);
            let s = (rng.below(400) as f64 - 200.0) / 4.0;
            assert_eq!(tree.first_where(|e| e.0 >= s), model.iter().filter(|(_, e)| e.0 < s).count());
            assert_eq!(tree.first_where(|e| e.0 > s), model.iter().filter(|(_, e)| e.0 <= s).count());
        }
        assert_eq!(tree.rank(1e9, b"nope"), None);
        assert_eq!(tree.rank(-1e9, b"nope"), None);
        assert!(tree.get(all.len()).is_none());
        assert_eq!(tree.iter(all.len(), 5, false).count(), 0);
        assert_eq!(tree.first_where(|_| false), all.len());
        assert_eq!(tree.first_where(|_| true), 0);
    }

    /// Four levels (past 64^3 / 2 entries), in order and in reverse, then
    /// emptied from random ranks.
    #[test]
    fn deep_trees() {
        let mut rng = Rng(77);
        for rev in [false, true] {
            let mut tree: Tree<T> = Tree::default();
            let n = 300_000u64;
            for i in 0..n {
                let i = if rev { n - 1 - i } else { i };
                tree.insert(T(i as f64, member(0)));
            }
            tree.check();
            assert_eq!(tree.iter(0, usize::MAX, false).map(|e| e.0 as u64).collect::<Vec<_>>(), (0..n).collect::<Vec<_>>());
            assert_eq!(tree.rank(123_456.0, b"m0"), Some(123_456));
            let mut left: Vec<u64> = (0..n).collect();
            while !left.is_empty() {
                let r = rng.below(left.len() as u64) as usize;
                assert_eq!(tree.remove_at(r).0 as u64, left.remove(r));
                if left.len().is_multiple_of(50_000) {
                    tree.check();
                }
            }
            tree.check();
        }
    }
}
