//! The sorted set commands (Redis 8.10's `t_zset.c`).
//!
//! A sorted set emptied by a command loses its key, as in Redis.

use super::Store;
use crate::zset::ZSet;
use crate::Error;

impl Store {
    /// The sorted set at `key`, `None` if there's none.
    pub fn zset_at(&mut self, key: &[u8]) -> Result<Option<&ZSet>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.as_zset()?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check, store};
    use super::super::{Kind, Staged};
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

    fn members(s: &mut super::Store, key: &[u8]) -> Vec<(Vec<u8>, u64)> {
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
}
