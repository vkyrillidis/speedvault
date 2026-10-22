//! A key's value as bytes and back, for moving it to another node
//! (`CLUSTER IMPORT`).
//!
//! A string travels as it is. A collection travels as its elements in
//! order (a hash's fields and values in turn), each as a 4-byte
//! little-endian length and its bytes. The receiving node builds it again
//! from empty, so the encoding comes out of the contents, as it does when
//! Redis loads a value with `RESTORE`.

use super::{Store, Value, NO_SLOT};

/// A value's type on the wire: what `TYPE` says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Str,
    Hash,
    Set,
    List,
}

impl Kind {
    pub fn name(self) -> &'static [u8] {
        match self {
            Kind::Str => b"string",
            Kind::Hash => b"hash",
            Kind::Set => b"set",
            Kind::List => b"list",
        }
    }

    pub fn from_name(name: &[u8]) -> Option<Kind> {
        [Kind::Str, Kind::Hash, Kind::Set, Kind::List].into_iter().find(|k| k.name() == name)
    }
}

/// A value ready to send: a string borrowed, a collection encoded.
#[derive(Debug, PartialEq, Eq)]
pub enum Dump<'a> {
    Str(&'a [u8]),
    /// The encoded elements, and how many there are.
    Elements(Kind, Vec<u8>, usize),
}

impl Dump<'_> {
    pub fn kind(&self) -> Kind {
        match self {
            Dump::Str(_) => Kind::Str,
            Dump::Elements(kind, ..) => *kind,
        }
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            Dump::Str(s) => s,
            Dump::Elements(_, bytes, _) => bytes,
        }
    }

    /// How many elements: 1 for a string.
    pub fn elements(&self) -> usize {
        match self {
            Dump::Str(_) => 1,
            Dump::Elements(.., n) => *n,
        }
    }
}

fn encode<'a>(items: impl Iterator<Item = &'a [u8]>) -> (Vec<u8>, usize) {
    let mut out = Vec::new();
    let mut n = 0;
    for item in items {
        out.extend_from_slice(&(item.len() as u32).to_le_bytes());
        out.extend_from_slice(item);
        n += 1;
    }
    (out, n)
}

/// The elements of an encoded collection; `None` if it's malformed.
pub fn decode(mut bytes: &[u8]) -> Option<Vec<&[u8]>> {
    let mut items = Vec::new();
    while !bytes.is_empty() {
        let (len, rest) = bytes.split_first_chunk::<4>()?;
        let len = u32::from_le_bytes(*len) as usize;
        if rest.len() < len {
            return None;
        }
        items.push(&rest[..len]);
        bytes = &rest[len..];
    }
    Some(items)
}

/// Whether `payload` is a valid value of `kind`: a collection's elements
/// decode, a hash's pair up, and none is empty.
pub fn check(kind: Kind, payload: &[u8]) -> bool {
    if kind == Kind::Str {
        return true;
    }
    match decode(payload) {
        Some(items) => !items.is_empty() && (kind != Kind::Hash || items.len().is_multiple_of(2)),
        None => false,
    }
}

impl Store {
    /// A live key's value and the milliseconds it has left (`None` without
    /// expiry; at least 1), for copying it to another node. Like
    /// [`contains`](Self::contains), an expired key is `None` and isn't
    /// reclaimed.
    pub fn dump(&self, key: &[u8]) -> Option<(Dump<'_>, Option<i64>)> {
        let e = self.map.get(key).filter(|e| self.now <= e.expire_at)?;
        let ttl = (e.slot != NO_SLOT).then(|| (e.expire_at - self.now).max(1));
        let dump = match &e.value {
            Value::Str(s) => Dump::Str(s),
            Value::Hash(h) => {
                let (bytes, n) = encode(h.iter().flat_map(|(f, v)| [f, v]));
                Dump::Elements(Kind::Hash, bytes, n)
            }
            Value::Set(s) => {
                let (bytes, n) = encode(s.iter());
                Dump::Elements(Kind::Set, bytes, n)
            }
            Value::List(l) => {
                let (bytes, n) = encode(l.iter());
                Dump::Elements(Kind::List, bytes, n)
            }
        };
        Some((dump, ttl))
    }

    /// Replaces `key` with the value `payload` of `kind` (checked with
    /// [`check`]), expiring in `ttl` ms if given (> 0).
    pub fn restore(&mut self, key: &[u8], kind: Kind, payload: &[u8], ttl: Option<i64>) {
        let mut staged = Staged::new(kind);
        staged.append(payload);
        self.restore_staged(key, staged, ttl);
    }

    /// Replaces `key` with the value built in `staged`, expiring in `ttl`
    /// ms if given (> 0). O(1): the value moves in whole.
    pub fn restore_staged(&mut self, key: &[u8], staged: Staged, ttl: Option<i64>) {
        self.remove(key);
        let at = ttl.map(|ttl| self.now.saturating_add(ttl));
        self.insert_value(key, staged.value, at);
    }

    /// How many elements a live key holds (a string: 1); expired is `None`,
    /// not reclaimed. O(1), so a move can tell a key to send in parts.
    pub fn elements(&self, key: &[u8]) -> Option<usize> {
        let e = self.map.get(key).filter(|e| self.now <= e.expire_at)?;
        Some(match &e.value {
            Value::Str(_) => 1,
            Value::Hash(h) => h.len(),
            Value::Set(s) => s.len(),
            Value::List(l) => l.len(),
        })
    }

    /// Elements `from..` of a live collection, as many as fit in `max`
    /// elements and `max_bytes` (one at least), encoded as [`dump`]'s
    /// payload; `None` for a string or a missing or expired key. The
    /// positions are the order the collection holds its elements in, so
    /// the parts of a key that doesn't change in between make it whole.
    ///
    /// [`dump`]: Self::dump
    pub fn dump_part(&self, key: &[u8], from: usize, max: usize, max_bytes: usize) -> Option<Part> {
        let e = self.map.get(key).filter(|e| self.now <= e.expire_at)?;
        let ttl = (e.slot != NO_SLOT).then(|| (e.expire_at - self.now).max(1));
        let mut part = Part { kind: Kind::Str, payload: Vec::new(), count: 0, total: 0, ttl };
        // Adds one element; false once the part is full.
        let add = |part: &mut Part, items: &[&[u8]]| {
            for item in items {
                part.payload.extend_from_slice(&(item.len() as u32).to_le_bytes());
                part.payload.extend_from_slice(item);
            }
            part.count += 1;
            part.count < max.max(1) && part.payload.len() < max_bytes
        };
        match &e.value {
            Value::Str(_) => return None,
            Value::Hash(h) => {
                (part.kind, part.total) = (Kind::Hash, h.len());
                for (f, v) in h.iter_from(from) {
                    if !add(&mut part, &[f, v]) {
                        break;
                    }
                }
            }
            Value::Set(s) => {
                (part.kind, part.total) = (Kind::Set, s.len());
                for m in s.iter().skip(from) {
                    if !add(&mut part, &[m]) {
                        break;
                    }
                }
            }
            Value::List(l) => {
                (part.kind, part.total) = (Kind::List, l.len());
                for item in l.iter_from(from) {
                    if !add(&mut part, &[item]) {
                        break;
                    }
                }
            }
        }
        Some(part)
    }

    /// Removes `key` and hands its value over, to be freed a piece at a
    /// time ([`Garbage::free`]): dropping a collection of millions of
    /// elements at once would hold the node up.
    pub fn take(&mut self, key: &[u8]) -> Option<Garbage> {
        self.remove(key).map(|e| Garbage(e.value))
    }
}

/// Part of a collection, from [`Store::dump_part`].
#[derive(Debug, PartialEq, Eq)]
pub struct Part {
    pub kind: Kind,
    /// The elements, encoded as [`Dump`]'s payload.
    pub payload: Vec<u8>,
    /// How many elements it holds (a hash's fields).
    pub count: usize,
    /// How many the collection holds.
    pub total: usize,
    /// The milliseconds the key has left, `None` without expiry.
    pub ttl: Option<i64>,
}

/// A collection being built from parts (each checked with [`check`]), for
/// [`Store::restore_staged`] once complete. It's outside the store
/// meanwhile: no command sees it.
#[derive(Debug)]
pub struct Staged {
    value: Value,
    count: usize,
}

impl Staged {
    pub fn new(kind: Kind) -> Staged {
        let value = match kind {
            Kind::Str => Value::Str(Vec::new()),
            Kind::Hash => Value::Hash(Box::default()),
            Kind::Set => Value::Set(Box::default()),
            Kind::List => Value::List(Box::default()),
        };
        Staged { value, count: 0 }
    }

    pub fn kind(&self) -> Kind {
        match self.value {
            Value::Str(_) => Kind::Str,
            Value::Hash(_) => Kind::Hash,
            Value::Set(_) => Kind::Set,
            Value::List(_) => Kind::List,
        }
    }

    /// The elements appended so far (a hash's fields).
    pub fn count(&self) -> usize {
        self.count
    }

    /// Adds the elements of `payload` (checked); a string's payload is the
    /// whole string.
    pub fn append(&mut self, payload: &[u8]) {
        match &mut self.value {
            Value::Str(s) => {
                s.extend_from_slice(payload);
                self.count = 1;
            }
            Value::Hash(h) => {
                let items = decode(payload).expect("checked");
                // As Redis's hashTypeTryConversion: before inserting.
                h.note_lengths(&items);
                for pair in items.chunks(2) {
                    h.insert(pair[0], pair[1]);
                }
                self.count += items.len() / 2;
            }
            Value::Set(s) => {
                for m in decode(payload).expect("checked") {
                    s.insert(m);
                    self.count += 1;
                }
            }
            Value::List(l) => {
                let items = decode(payload).expect("checked");
                self.count += items.len();
                for item in items {
                    l.push_back(item);
                }
            }
        }
    }
}

/// A value taken out of the store ([`Store::take`]), freed a piece at a
/// time.
pub struct Garbage(Value);

impl Garbage {
    /// Frees up to `n` elements (a string at once); true once it's all
    /// freed.
    pub fn free(&mut self, n: usize) -> bool {
        match &mut self.0 {
            Value::Str(s) => {
                *s = Vec::new();
                true
            }
            Value::Hash(h) => h.free_last(n),
            Value::Set(s) => s.free_last(n),
            Value::List(l) => l.free_last(n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::store;
    use super::*;
    use crate::End;

    /// Dumps `key` from one store and restores it into another.
    fn copy(from: &Store, to: &mut Store, key: &[u8]) {
        let (dump, ttl) = from.dump(key).expect("a live key");
        assert!(check(dump.kind(), dump.payload()));
        to.restore(key, dump.kind(), dump.payload(), ttl);
    }

    #[test]
    fn every_type_survives_the_trip() {
        let mut a = store();
        let mut b = store();
        a.set(b"s", b"str", Default::default()).unwrap();
        a.hset(b"h", &[b"b", b"1", b"a", b"2"]).unwrap();
        a.sadd(b"i", &[b"3", b"1"]).unwrap();
        a.sadd(b"t", &[b"x", b"y"]).unwrap();
        a.push(b"l", End::Right, &[b"1", b"", b"3"], false).unwrap();
        a.expire(crate::ExpireCmd::PExpire, b"l", 5000, Default::default()).unwrap();
        b.set(b"h", b"old", Default::default()).unwrap();
        for key in [&b"s"[..], b"h", b"i", b"t", b"l"] {
            copy(&a, &mut b, key);
            assert_eq!(b.type_of(key), a.type_of(key));
            assert_eq!(b.encoding(key), a.encoding(key));
        }
        assert_eq!(b.get(b"s"), Ok(Some(&b"str"[..])));
        let h: Vec<(&[u8], &[u8])> = b.hash(b"h").unwrap().unwrap().iter().collect();
        assert_eq!(h, [(&b"b"[..], &b"1"[..]), (b"a", b"2")]);
        assert_eq!(b.set_at(b"i").unwrap().unwrap().members(), [&b"1"[..], b"3"]);
        let l = b.list_at(b"l").unwrap().unwrap();
        assert_eq!(l.slice(0, 2).collect::<Vec<_>>(), [&b"1"[..], b"", b"3"]);
        assert_eq!(b.pttl(b"l"), 5000);
        assert_eq!(b.pttl(b"h"), -1);
    }

    #[test]
    fn a_big_hash_stays_big() {
        let mut a = store();
        let mut b = store();
        a.hset(b"h", &[b"f", &[b'x'; 100]]).unwrap();
        copy(&a, &mut b, b"h");
        assert_eq!(b.encoding(b"h"), Some("hashtable"));
    }

    /// Copies `key` part by part, `max` elements a part.
    fn copy_in_parts(from: &Store, to: &mut Store, key: &[u8], max: usize) -> usize {
        let mut staged: Option<Staged> = None;
        let mut parts = 0;
        loop {
            let at = staged.as_ref().map_or(0, Staged::count);
            let part = from.dump_part(key, at, max, usize::MAX).expect("a live collection");
            assert!(check(part.kind, &part.payload));
            let s = staged.get_or_insert_with(|| Staged::new(part.kind));
            s.append(&part.payload);
            parts += 1;
            assert!(part.count > 0 && s.count() <= part.total, "{} of {} after {parts} parts", s.count(), part.total);
            if s.count() == part.total {
                to.restore_staged(key, staged.take().unwrap(), part.ttl);
                return parts;
            }
        }
    }

    #[test]
    fn parts_make_the_whole() {
        let mut a = store();
        let mut b = store();
        let fields: Vec<Vec<u8>> = (0..1000).map(|i| format!("f{i}").into_bytes()).collect();
        for f in &fields {
            a.hset(b"h", &[f, b"v"]).unwrap();
            a.sadd(b"s", &[f]).unwrap();
            a.push(b"l", End::Right, &[f], false).unwrap();
        }
        a.hset(b"small", &[b"b", b"1", b"a", b"2"]).unwrap();
        a.expire(crate::ExpireCmd::PExpire, b"l", 5000, Default::default()).unwrap();
        assert_eq!(a.elements(b"h"), Some(1000));
        assert_eq!(a.elements(b"nothing"), None);
        a.set(b"str", b"x", Default::default()).unwrap();
        assert_eq!(a.elements(b"str"), Some(1));
        assert_eq!(a.dump_part(b"str", 0, 10, 100), None);
        assert_eq!(copy_in_parts(&a, &mut b, b"h", 3), 334);
        assert_eq!(copy_in_parts(&a, &mut b, b"s", 7), 143);
        assert_eq!(copy_in_parts(&a, &mut b, b"l", 1000), 1);
        assert_eq!(copy_in_parts(&a, &mut b, b"small", 1), 2);
        for key in [&b"h"[..], b"s", b"l", b"small"] {
            assert_eq!(b.dump(key).unwrap().0, a.dump(key).unwrap().0, "{key:?}, order included");
            assert_eq!(b.encoding(key), a.encoding(key));
        }
        assert_eq!(b.pttl(b"l"), 5000);
        // A part stops at max_bytes too, with one element at least.
        let part = a.dump_part(b"l", 10, 100, 1).unwrap();
        assert_eq!((part.count, part.total), (1, 1000));
        assert_eq!(decode(&part.payload).unwrap(), [&b"f10"[..]]);
        assert_eq!(a.dump_part(b"l", 1000, 100, 100).unwrap().count, 0);
    }

    #[test]
    fn garbage_is_freed_in_pieces() {
        let mut a = store();
        // Past the compact sizes (freed at once: a few bytes); the list in
        // nodes of 8 KiB, freed a node at a time (80 elements of 100 bytes).
        for i in 0..600 {
            let m = format!("m{i}");
            a.hset(b"h", &[m.as_bytes(), b"v"]).unwrap();
            a.sadd(b"s", &[m.as_bytes()]).unwrap();
            a.push(b"l", End::Right, &[format!("{m:>100}").as_bytes()], false).unwrap();
        }
        a.set(b"str", b"x", Default::default()).unwrap();
        for (key, want) in [(&b"h"[..], 3), (b"s", 3), (b"l", 3)] {
            let mut g = a.take(key).unwrap();
            assert!(!a.exists(key));
            let mut rounds = 1;
            while !g.free(200) {
                rounds += 1;
            }
            assert_eq!(rounds, want, "{key:?}");
        }
        a.hset(b"small", &[b"f", b"v"]).unwrap();
        assert!(a.take(b"small").unwrap().free(1));
        assert!(a.take(b"str").unwrap().free(1));
        assert!(a.take(b"str").is_none());
        assert_eq!(a.dbsize(), 0);
    }

    #[test]
    fn malformed_payloads() {
        assert!(check(Kind::Str, b""));
        assert!(!check(Kind::List, b""), "no empty collection");
        assert!(!check(Kind::List, b"\x05\0\0\0abc"));
        assert!(!check(Kind::List, b"\x01\0\0"));
        assert!(check(Kind::List, b"\x01\0\0\0a"));
        assert!(!check(Kind::Hash, b"\x01\0\0\0a"), "a field without a value");
        assert!(check(Kind::Hash, b"\x01\0\0\0a\0\0\0\0"));
        assert_eq!(Kind::from_name(b"set"), Some(Kind::Set));
        assert_eq!(Kind::from_name(b"zset"), None);
    }
}
