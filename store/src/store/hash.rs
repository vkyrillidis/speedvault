//! The hash commands (Redis 7.0's `t_hash.c`).
//!
//! A hash emptied by a command loses its key, as in Redis. The reads hand
//! out the [`Hash`] itself; the dbnode writes the reply from it.

use super::{Store, Value};
use crate::collections::Hash;
use crate::int::{parse_i64, write_i64};
use crate::Error;

impl Value {
    fn as_hash(&self) -> Result<&Hash, Error> {
        match self {
            Value::Hash(h) => Ok(h),
            _ => Err(Error::WrongType),
        }
    }

    fn as_hash_mut(&mut self) -> Result<&mut Hash, Error> {
        match self {
            Value::Hash(h) => Ok(h),
            _ => Err(Error::WrongType),
        }
    }
}

impl Store {
    /// The hash at `key`, `None` if there's none (`HGET`, `HGETALL`, ...).
    pub fn hash(&mut self, key: &[u8]) -> Result<Option<&Hash>, Error> {
        match self.live(key) {
            Some(e) => Ok(Some(e.value.as_hash()?)),
            None => Ok(None),
        }
    }

    /// The hash at `key`, created empty if missing. The caller must leave
    /// something in it.
    fn hash_or_create(&mut self, key: &[u8]) -> Result<&mut Hash, Error> {
        if self.live(key).is_none() {
            self.insert_value(key, Value::Hash(Box::default()), None);
        }
        self.live(key).expect("just made").value.as_hash_mut()
    }

    /// `HSET key field value [field value ...]` (and `HMSET`), `pairs` the
    /// fields and values in turn (an even number, at least two): how many
    /// fields are new.
    pub fn hset(&mut self, key: &[u8], pairs: &[&[u8]]) -> Result<usize, Error> {
        debug_assert!(!pairs.is_empty() && pairs.len().is_multiple_of(2));
        let h = self.hash_or_create(key)?;
        // As Redis's hashTypeTryConversion: long arguments convert the hash
        // before anything is set.
        h.note_lengths(pairs);
        let (pairs, _) = pairs.as_chunks::<2>();
        Ok(pairs.iter().filter(|[f, v]| h.insert(f, v)).count())
    }

    /// `HSETNX`: true if the field was new and is now set.
    pub fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Result<bool, Error> {
        let h = self.hash_or_create(key)?;
        if h.get(field).is_some() {
            // Redis 7.0.15: nothing set, nothing converted.
            return Ok(false);
        }
        h.note_lengths(&[field, value]);
        Ok(h.insert(field, value))
    }

    /// `HDEL`: how many of `fields` were there.
    pub fn hdel(&mut self, key: &[u8], fields: &[&[u8]]) -> Result<usize, Error> {
        let Some(e) = self.live(key) else { return Ok(0) };
        let h = e.value.as_hash_mut()?;
        let removed = fields.iter().filter(|f| h.remove(f)).count();
        if h.is_empty() {
            self.remove(key);
        }
        Ok(removed)
    }

    /// `HINCRBY`: the new value. A missing field counts as 0.
    pub fn hincr_by(&mut self, key: &[u8], field: &[u8], by: i64) -> Result<i64, Error> {
        if let Some(e) = self.live(key) {
            // Look before creating: a non-integer field fails without
            // leaving an empty hash behind.
            if let Some(v) = e.value.as_hash()?.get(field) {
                parse_i64(v).ok_or(Error::HashNotInteger)?.checked_add(by).ok_or(Error::Overflow)?;
            }
        }
        let h = self.hash_or_create(key)?;
        let old = h.get(field).map_or(0, |v| parse_i64(v).expect("checked above"));
        let new = old + by;
        let mut buf = Vec::with_capacity(20);
        write_i64(&mut buf, new);
        h.insert(field, &buf);
        Ok(new)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{put_other, store};
    use crate::Error;

    fn pairs(h: &crate::collections::Hash) -> Vec<(String, String)> {
        h.iter().map(|(f, v)| (String::from_utf8_lossy(f).into(), String::from_utf8_lossy(v).into())).collect()
    }

    #[test]
    fn hset_counts_new_fields_and_keeps_order() {
        let mut s = store();
        assert_eq!(s.hset(b"h", &[b"b", b"1", b"a", b"2"]), Ok(2));
        assert_eq!(s.hset(b"h", &[b"a", b"3", b"c", b"4", b"c", b"5"]), Ok(1));
        let h = s.hash(b"h").unwrap().unwrap();
        assert_eq!(pairs(h), [("b".into(), "1".into()), ("a".into(), "3".into()), ("c".into(), "5".into())]);
        assert_eq!(s.type_of(b"h"), "hash");
        assert_eq!(s.encoding(b"h"), Some("listpack"));
    }

    #[test]
    fn hdel_deletes_the_emptied_key() {
        let mut s = store();
        s.hset(b"h", &[b"a", b"1", b"b", b"2"]).unwrap();
        assert_eq!(s.hdel(b"h", &[b"a", b"x", b"a"]), Ok(1));
        assert!(s.exists(b"h"));
        assert_eq!(s.hdel(b"h", &[b"b"]), Ok(1));
        assert!(!s.exists(b"h"));
        assert_eq!(s.hdel(b"h", &[b"b"]), Ok(0));
        assert_eq!(s.dbsize(), 0);
    }

    #[test]
    fn wrong_types() {
        let mut s = store();
        put_other(&mut s, b"l");
        assert_eq!(s.hset(b"l", &[b"a", b"1"]), Err(Error::WrongType));
        assert_eq!(s.hsetnx(b"l", b"a", b"1"), Err(Error::WrongType));
        assert_eq!(s.hdel(b"l", &[b"a"]), Err(Error::WrongType));
        assert_eq!(s.hincr_by(b"l", b"a", 1), Err(Error::WrongType));
        assert!(s.hash(b"l").is_err());
        s.hset(b"h", &[b"a", b"1"]).unwrap();
        assert_eq!(s.get(b"h"), Err(Error::WrongType));
        assert_eq!(s.incr_by(b"h", 1), Err(Error::WrongType));
    }

    #[test]
    fn hsetnx_converts_like_redis() {
        let mut s = store();
        assert_eq!(s.hsetnx(b"h", b"f", b"v"), Ok(true));
        assert_eq!(s.hsetnx(b"h", b"f", &[b'x'; 65]), Ok(false));
        // Redis 7.0.15: nothing set, nothing converted.
        assert_eq!(s.encoding(b"h"), Some("listpack"));
        assert_eq!(s.hash(b"h").unwrap().unwrap().get(b"f"), Some(&b"v"[..]));
        assert_eq!(s.hsetnx(b"h", b"g", &[b'x'; 65]), Ok(true));
        assert_eq!(s.encoding(b"h"), Some("hashtable"));
    }

    #[test]
    fn hincrby() {
        let mut s = store();
        assert_eq!(s.hincr_by(b"h", b"n", 5), Ok(5));
        assert_eq!(s.hincr_by(b"h", b"n", -7), Ok(-2));
        s.hset(b"h", &[b"s", b"x", b"m", b"9223372036854775807"]).unwrap();
        assert_eq!(s.hincr_by(b"h", b"s", 1), Err(Error::HashNotInteger));
        assert_eq!(s.hincr_by(b"h", b"m", 1), Err(Error::Overflow));
        assert_eq!(s.hash(b"h").unwrap().unwrap().get(b"m"), Some(&b"9223372036854775807"[..]));
    }

    #[test]
    fn a_hash_expires_like_any_key() {
        let mut s = store();
        s.hset(b"h", &[b"a", b"1"]).unwrap();
        s.expire(crate::ExpireCmd::PExpire, b"h", 10, Default::default()).unwrap();
        let now = s.now();
        s.set_now(now + 11);
        assert!(s.hash(b"h").unwrap().is_none());
        assert_eq!(s.dbsize(), 0);
    }
}
