//! Cases marked "Redis:" were checked against Redis 7.0.15 with redis-cli.

use super::*;

const T0: i64 = 1_700_000_000_000;

pub(super) fn store() -> Store {
    let mut s = Store::new();
    s.set_now(T0);
    s
}

/// A key holding a non-string (a list of one), for the WRONGTYPE paths.
pub(super) fn put_other(s: &mut Store, key: &[u8]) {
    let mut list = List::default();
    list.push_back(b"x");
    s.insert_value(key, Value::List(Box::new(list)), None);
}

/// A store with the slot index; a key's slot is its first byte.
pub(super) fn slotted() -> Store {
    let mut s = Store::with_slots(first_byte);
    s.set_now(T0);
    s
}

fn first_byte(key: &[u8]) -> u16 {
    key.first().map_or(0, |&b| u16::from(b))
}

/// The expiry index and the map agree.
pub(super) fn check(s: &Store) {
    for (i, key) in s.expiring.iter().enumerate() {
        let e = s.map.get(&**key).expect("indexed key not in the map");
        assert_eq!(e.slot as usize, i, "slot of {key:?}");
        assert_ne!(e.expire_at, NEVER);
    }
    let with_ttl = s.map.values().filter(|e| e.slot != NO_SLOT).count();
    assert_eq!(with_ttl, s.expiring.len());
    for e in s.map.values().filter(|e| e.slot == NO_SLOT) {
        assert_eq!(e.expire_at, NEVER);
    }
    // The slot index and the map agree: every key once, in its slot.
    // Standalone: one list of every key, once a SCAN built it.
    let slots = &s.slots;
    if !slots.active {
        assert!(slots.keys.iter().all(Vec::is_empty));
        return;
    }
    let mut listed = 0;
    for (slot, keys) in slots.keys.iter().enumerate() {
        for (i, key) in keys.iter().enumerate() {
            let e = s.map.get(&**key).expect("key in the slot index not in the map");
            assert_eq!(e.pos as usize, i, "place of {key:?}");
            assert_eq!(slots.list_of(key), slot, "slot of {key:?}");
            assert!(Rc::ptr_eq(key, s.map.get_key_value(&**key).unwrap().0), "the index shares the map's key");
        }
        listed += keys.len();
    }
    assert_eq!(listed, s.map.len());
}

fn set(s: &mut Store, key: &str, value: &str) {
    assert_eq!(s.set(key.as_bytes(), value.as_bytes(), SetOptions::default()), Ok(SetReply::Ok));
}

fn set_opts(s: &mut Store, key: &str, value: &str, opts: SetOptions) -> Result<SetReply, Error> {
    s.set(key.as_bytes(), value.as_bytes(), opts)
}

fn get(s: &mut Store, key: &str) -> Option<String> {
    s.get(key.as_bytes()).unwrap().map(|v| String::from_utf8(v.to_vec()).unwrap())
}

fn ex(expiry: SetExpiry) -> SetOptions {
    SetOptions { expiry, ..SetOptions::default() }
}

fn old(v: &str) -> SetReply {
    SetReply::Old(Some(v.as_bytes().to_vec()))
}

const FLAGS: ExpireFlags = ExpireFlags { nx: false, xx: false, gt: false, lt: false };

// ---- GET / SET ----

#[test]
fn get_and_set() {
    let mut s = store();
    assert_eq!(get(&mut s, "k"), None);
    set(&mut s, "k", "v");
    assert_eq!(get(&mut s, "k").as_deref(), Some("v"));
    set(&mut s, "k", "longer value");
    assert_eq!(get(&mut s, "k").as_deref(), Some("longer value"));
    set(&mut s, "k", "");
    assert_eq!(get(&mut s, "k").as_deref(), Some(""), "empty is not missing");
    assert_eq!(s.dbsize(), 1);
}

#[test]
fn binary_safe() {
    let mut s = store();
    let key = b"\0k\xff\r\n";
    let value = b"\0\x01\xff\r\n\0";
    s.set(key, value, SetOptions::default()).unwrap();
    assert_eq!(s.get(key), Ok(Some(&value[..])));
    assert_eq!(s.get(b"\0k\xff\r"), Ok(None));
    assert_eq!(s.strlen(key), Ok(6));
}

#[test]
fn overwrite_reuses_or_replaces_buffer() {
    let mut s = store();
    set(&mut s, "k", &"x".repeat(1000));
    set(&mut s, "k", "small");
    assert_eq!(get(&mut s, "k").as_deref(), Some("small"));
    set(&mut s, "k", "small2");
    assert_eq!(get(&mut s, "k").as_deref(), Some("small2"));
}

#[test]
fn get_wrong_type() {
    // Redis: GET on a list is WRONGTYPE.
    let mut s = store();
    put_other(&mut s, b"l");
    assert_eq!(s.get(b"l"), Err(Error::WrongType));
}

#[test]
fn set_clears_or_keeps_ttl() {
    // Redis: SET k w drops the TTL, SET k w KEEPTTL keeps it.
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert_eq!(s.ttl(b"k"), 100);
    set(&mut s, "k", "w");
    assert_eq!(s.ttl(b"k"), -1);
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert_eq!(set_opts(&mut s, "k", "w", ex(SetExpiry::KeepTtl)), Ok(SetReply::Ok));
    assert_eq!(s.ttl(b"k"), 100);
    assert_eq!(get(&mut s, "k").as_deref(), Some("w"));
    // KEEPTTL on a key without TTL, and on a new key.
    set_opts(&mut s, "n", "v", ex(SetExpiry::KeepTtl)).unwrap();
    assert_eq!(s.ttl(b"n"), -1);
    check(&s);
}

#[test]
fn set_expiry_kinds() {
    let mut s = store();
    set_opts(&mut s, "ex", "v", ex(SetExpiry::Ex(10))).unwrap();
    set_opts(&mut s, "px", "v", ex(SetExpiry::Px(1500))).unwrap();
    set_opts(&mut s, "exat", "v", ex(SetExpiry::ExAt(T0 / 1000 + 20))).unwrap();
    set_opts(&mut s, "pxat", "v", ex(SetExpiry::PxAt(T0 + 2500))).unwrap();
    assert_eq!(s.pttl(b"ex"), 10_000);
    assert_eq!(s.pttl(b"px"), 1500);
    assert_eq!(s.pttl(b"exat"), 20_000);
    assert_eq!(s.pttl(b"pxat"), 2500);
    // A new expiry on a key that has one, and a different kind.
    set_opts(&mut s, "ex", "v", ex(SetExpiry::Px(7))).unwrap();
    assert_eq!(s.pttl(b"ex"), 7);
    check(&s);
}

#[test]
fn set_invalid_expiry() {
    // Redis: EX 0, EX -1, PX 0, EXAT 0, EX 9223372036854775 (overflows when
    // adding now), EX 9223372036854776 (overflows * 1000) and
    // PX 9223372036854775807 fail; PXAT 9223372036854775807 is fine.
    let mut s = store();
    set(&mut s, "k", "old");
    let invalid = Err(Error::InvalidExpireTime("set"));
    for e in [
        SetExpiry::Ex(0),
        SetExpiry::Ex(-1),
        SetExpiry::Px(0),
        SetExpiry::Px(-5),
        SetExpiry::ExAt(0),
        SetExpiry::PxAt(0),
        SetExpiry::PxAt(-1),
        SetExpiry::Ex(9223372036854775),
        SetExpiry::Ex(9223372036854776),
        SetExpiry::Px(i64::MAX),
        SetExpiry::ExAt(i64::MAX / 1000 + 1),
    ] {
        assert_eq!(set_opts(&mut s, "k", "new", ex(e)), invalid, "{e:?}");
        // The expiry is checked before GET's type check, and nothing changes.
        let opts = SetOptions { expiry: e, get: true, ..SetOptions::default() };
        assert_eq!(set_opts(&mut s, "k", "new", opts), invalid);
    }
    assert_eq!(get(&mut s, "k").as_deref(), Some("old"));
    assert_eq!(set_opts(&mut s, "k", "v", ex(SetExpiry::PxAt(i64::MAX))), Ok(SetReply::Ok));
    assert_eq!(s.pttl(b"k"), i64::MAX - T0);
    assert_eq!(set_opts(&mut s, "k", "v", ex(SetExpiry::ExAt(i64::MAX / 1000))), Ok(SetReply::Ok));
    assert_eq!(Error::InvalidExpireTime("set").to_string(), "ERR invalid expire time in 'set' command");
}

#[test]
fn set_expiry_in_the_past() {
    // Redis: SET k v PXAT 1 replies OK and leaves no key.
    let mut s = store();
    assert_eq!(set_opts(&mut s, "k", "v", ex(SetExpiry::PxAt(1))), Ok(SetReply::Ok));
    assert_eq!(s.dbsize(), 0);
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(5))).unwrap();
    let opts = SetOptions { expiry: SetExpiry::PxAt(T0), get: true, ..SetOptions::default() };
    assert_eq!(set_opts(&mut s, "k", "w", opts), Ok(old("v")));
    assert_eq!(s.dbsize(), 0);
    assert_eq!(s.expiring_len(), 0);
    // Exactly now is in the past too (checkAlreadyExpired: when <= now).
    set(&mut s, "k", "v");
    set_opts(&mut s, "k", "v", ex(SetExpiry::PxAt(T0 + 1))).unwrap();
    assert!(s.exists(b"k"));
    check(&s);
}

#[test]
fn set_nx_xx() {
    let mut s = store();
    let nx = SetOptions { cond: SetCond::Nx, ..SetOptions::default() };
    let xx = SetOptions { cond: SetCond::Xx, ..SetOptions::default() };
    assert_eq!(set_opts(&mut s, "k", "1", xx), Ok(SetReply::NotSet));
    assert_eq!(s.dbsize(), 0);
    assert_eq!(set_opts(&mut s, "k", "1", nx), Ok(SetReply::Ok));
    assert_eq!(set_opts(&mut s, "k", "2", nx), Ok(SetReply::NotSet));
    assert_eq!(get(&mut s, "k").as_deref(), Some("1"));
    assert_eq!(set_opts(&mut s, "k", "3", xx), Ok(SetReply::Ok));
    assert_eq!(get(&mut s, "k").as_deref(), Some("3"));
    // NX with an expiry that isn't applied because the condition fails.
    let nx_ex = SetOptions { cond: SetCond::Nx, expiry: SetExpiry::Ex(5), get: false };
    assert_eq!(set_opts(&mut s, "k", "4", nx_ex), Ok(SetReply::NotSet));
    assert_eq!(s.ttl(b"k"), -1);
    // An expired key counts as missing.
    set_opts(&mut s, "e", "old", ex(SetExpiry::Px(10))).unwrap();
    s.set_now(T0 + 11);
    assert_eq!(set_opts(&mut s, "e", "x", xx), Ok(SetReply::NotSet));
    assert_eq!(set_opts(&mut s, "e", "new", nx), Ok(SetReply::Ok));
    assert_eq!(s.ttl(b"e"), -1);
    check(&s);
}

#[test]
fn set_get_option() {
    // Redis: SET k v NX GET on an existing key replies the old value and
    // sets nothing; on a missing key nil, and sets. XX GET on a missing key:
    // nil, not set. SET k x XX GET: old value, set.
    let mut s = store();
    let get = |cond| SetOptions { cond, get: true, ..SetOptions::default() };
    assert_eq!(set_opts(&mut s, "k", "v", get(SetCond::Always)), Ok(SetReply::Old(None)));
    assert_eq!(set_opts(&mut s, "k", "w", get(SetCond::Nx)), Ok(old("v")));
    assert_eq!(self::get(&mut s, "k").as_deref(), Some("v"));
    assert_eq!(set_opts(&mut s, "k2", "v", get(SetCond::Nx)), Ok(SetReply::Old(None)));
    assert_eq!(self::get(&mut s, "k2").as_deref(), Some("v"));
    assert_eq!(set_opts(&mut s, "k3", "v", get(SetCond::Xx)), Ok(SetReply::Old(None)));
    assert!(!s.exists(b"k3"));
    assert_eq!(set_opts(&mut s, "k", "x", get(SetCond::Xx)), Ok(old("v")));
    assert_eq!(self::get(&mut s, "k").as_deref(), Some("x"));
    assert_eq!(set_opts(&mut s, "k", "y", get(SetCond::Always)), Ok(old("x")));
    assert_eq!(self::get(&mut s, "k").as_deref(), Some("y"));
}

#[test]
fn set_over_other_type() {
    // Redis: SET l v GET and SET l v NX GET on a list are WRONGTYPE and
    // change nothing; a plain SET l v overwrites, TYPE l is then string.
    let mut s = store();
    put_other(&mut s, b"l");
    for cond in [SetCond::Always, SetCond::Nx, SetCond::Xx] {
        let opts = SetOptions { cond, get: true, ..SetOptions::default() };
        assert_eq!(set_opts(&mut s, "l", "v", opts), Err(Error::WrongType));
        assert_eq!(s.type_of(b"l"), "list");
    }
    set(&mut s, "l", "v");
    assert_eq!(s.type_of(b"l"), "string");
    assert_eq!(get(&mut s, "l").as_deref(), Some("v"));
}

#[test]
fn mset_and_setnx() {
    // Redis: MSET a 1 a 2 leaves 2; SETNX on an existing key is 0.
    let mut s = store();
    set_opts(&mut s, "a", "0", ex(SetExpiry::Ex(10))).unwrap();
    s.mset([(&b"a"[..], &b"1"[..]), (b"b", b"x"), (b"a", b"2")]);
    assert_eq!(get(&mut s, "a").as_deref(), Some("2"));
    assert_eq!(get(&mut s, "b").as_deref(), Some("x"));
    assert_eq!(s.ttl(b"a"), -1, "MSET clears the TTL");
    assert!(!s.setnx(b"a", b"z"));
    assert!(s.setnx(b"zz", b"z"));
    assert_eq!(get(&mut s, "a").as_deref(), Some("2"));
    assert_eq!(get(&mut s, "zz").as_deref(), Some("z"));
    put_other(&mut s, b"l");
    assert!(!s.setnx(b"l", b"z"));
    check(&s);
}

#[test]
fn mget() {
    // Redis: MGET k list missing gives value, nil, nil.
    let mut s = store();
    set(&mut s, "k", "x");
    put_other(&mut s, b"l");
    set_opts(&mut s, "e", "v", ex(SetExpiry::Px(1))).unwrap();
    s.set_now(T0 + 2);
    let got = s.mget(&[b"k", b"l", b"nope", b"e", b"k"]);
    assert_eq!(got, vec![Some(&b"x"[..]), None, None, None, Some(&b"x"[..])]);
    assert_eq!(s.dbsize(), 2, "the expired key is gone");
}

// ---- DEL / EXISTS / GETDEL ----

#[test]
fn del_and_exists() {
    // Redis: EXISTS k k nope k counts 3.
    let mut s = store();
    set(&mut s, "k", "v");
    put_other(&mut s, b"l");
    let n = [&b"k"[..], b"k", b"nope", b"k"].iter().filter(|k| s.exists(k)).count();
    assert_eq!(n, 3);
    assert!(s.exists(b"l"));
    assert!(s.del(b"k"));
    assert!(!s.del(b"k"));
    assert!(s.del(b"l"), "DEL deletes any type");
    assert!(!s.exists(b"k"));
    assert_eq!(s.dbsize(), 0);
}

#[test]
fn del_expired_counts_zero() {
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Px(5))).unwrap();
    set_opts(&mut s, "k2", "v", ex(SetExpiry::Px(500))).unwrap();
    s.set_now(T0 + 6);
    assert!(!s.del(b"k"));
    assert_eq!(s.dbsize(), 1);
    assert!(s.del(b"k2"));
    assert_eq!(s.dbsize(), 0);
    assert_eq!(s.expiring_len(), 0);
    check(&s);
}

#[test]
fn getdel() {
    // Redis: GETDEL returns the value and deletes; nil when missing;
    // WRONGTYPE on a list, which stays.
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert_eq!(s.getdel(b"k"), Ok(Some(b"v".to_vec())));
    assert!(!s.exists(b"k"));
    assert_eq!(s.getdel(b"k"), Ok(None));
    put_other(&mut s, b"l");
    assert_eq!(s.getdel(b"l"), Err(Error::WrongType));
    assert!(s.exists(b"l"));
    check(&s);
}

// ---- APPEND / STRLEN ----

#[test]
fn append_and_strlen() {
    // Redis: APPEND ap hello = 5, APPEND ap " world" = 11; APPEND of "" to a
    // missing key creates an empty string; STRLEN of a missing key is 0.
    let mut s = store();
    assert_eq!(s.append(b"ap", b"hello"), Ok(5));
    assert_eq!(s.append(b"ap", b" world"), Ok(11));
    assert_eq!(get(&mut s, "ap").as_deref(), Some("hello world"));
    assert_eq!(s.strlen(b"ap"), Ok(11));
    assert_eq!(s.strlen(b"nope"), Ok(0));
    assert_eq!(s.append(b"e", b""), Ok(0));
    assert!(s.exists(b"e"));
    assert_eq!(get(&mut s, "e").as_deref(), Some(""));
}

#[test]
fn append_too_long() {
    // Redis with proto-max-bulk-len 1mb: APPEND up to exactly 1048576 bytes
    // works, one more byte is refused and changes nothing.
    let mut s = store();
    s.set_max_string_len(10);
    assert_eq!(s.append(b"k", b"12345678"), Ok(8));
    assert_eq!(s.append(b"k", b"901"), Err(Error::StringTooLong));
    assert_eq!(s.append(b"k", b"90"), Ok(10));
    assert_eq!(s.append(b"k", b"1"), Err(Error::StringTooLong));
    assert_eq!(s.append(b"k", b""), Ok(10));
    assert_eq!(s.strlen(b"k"), Ok(10));
    assert_eq!(Error::StringTooLong.to_string(), "ERR string exceeds maximum allowed size (proto-max-bulk-len)");
}

#[test]
fn append_keeps_ttl() {
    // Redis: APPEND doesn't touch the TTL.
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert_eq!(s.append(b"k", b"x"), Ok(2));
    assert_eq!(s.ttl(b"k"), 100);
}

#[test]
fn append_strlen_wrong_type() {
    // Redis: APPEND and STRLEN on a list are WRONGTYPE.
    let mut s = store();
    put_other(&mut s, b"l");
    assert_eq!(s.append(b"l", b"x"), Err(Error::WrongType));
    assert_eq!(s.strlen(b"l"), Err(Error::WrongType));
}

// ---- INCR family ----

#[test]
fn incr_decr() {
    let mut s = store();
    assert_eq!(s.incr_by(b"c", 1), Ok(1));
    assert_eq!(s.incr_by(b"c", 10), Ok(11));
    assert_eq!(s.decr_by(b"c", 1), Ok(10));
    assert_eq!(s.decr_by(b"c", 15), Ok(-5));
    assert_eq!(get(&mut s, "c").as_deref(), Some("-5"));
    assert_eq!(s.decr_by(b"d", 3), Ok(-3));
    assert_eq!(s.incr_by(b"z", 0), Ok(0));
    assert_eq!(get(&mut s, "z").as_deref(), Some("0"));
    set(&mut s, "n", "0");
    assert_eq!(s.incr_by(b"n", 1), Ok(1));
    set(&mut s, "n", "-1");
    assert_eq!(s.incr_by(b"n", -9223372036854775807), Ok(i64::MIN));
    assert_eq!(get(&mut s, "n").as_deref(), Some("-9223372036854775808"));
}

#[test]
fn incr_not_an_integer() {
    // Redis: each of these makes INCR fail and leaves the value alone.
    let mut s = store();
    for v in ["abc", " 1", "+1", "-0", "01", "00000000000000000001", "1.0", "", "9223372036854775808"] {
        set(&mut s, "n", v);
        assert_eq!(s.incr_by(b"n", 1), Err(Error::NotInteger), "{v:?}");
        assert_eq!(s.decr_by(b"n", 1), Err(Error::NotInteger), "{v:?}");
        assert_eq!(get(&mut s, "n").as_deref(), Some(v));
    }
    assert_eq!(Error::NotInteger.to_string(), "ERR value is not an integer or out of range");
}

#[test]
fn incr_overflow() {
    // Redis: INCR at i64::MAX, DECR and INCRBY -1 at i64::MIN overflow;
    // DECRBY n -9223372036854775808 is "decrement would overflow" whatever
    // n holds; DECRBY 5 -9223372036854775807 overflows the normal way.
    let mut s = store();
    set(&mut s, "n", "9223372036854775807");
    assert_eq!(s.incr_by(b"n", 1), Err(Error::Overflow));
    set(&mut s, "n", "-9223372036854775808");
    assert_eq!(s.decr_by(b"n", 1), Err(Error::Overflow));
    assert_eq!(s.incr_by(b"n", -1), Err(Error::Overflow));
    assert_eq!(get(&mut s, "n").as_deref(), Some("-9223372036854775808"));
    assert_eq!(s.decr_by(b"n", i64::MIN), Err(Error::DecrementOverflow));
    assert_eq!(s.decr_by(b"missing", i64::MIN), Err(Error::DecrementOverflow));
    assert!(!s.exists(b"missing"));
    set(&mut s, "n", "5");
    assert_eq!(s.decr_by(b"n", -9223372036854775807), Err(Error::Overflow));
    assert_eq!(s.incr_by(b"n", i64::MAX - 5), Ok(i64::MAX));
    assert_eq!(Error::Overflow.to_string(), "ERR increment or decrement would overflow");
    assert_eq!(Error::DecrementOverflow.to_string(), "ERR decrement would overflow");
}

#[test]
fn incr_keeps_ttl_and_type() {
    // Redis: INCR keeps the TTL; INCR on a list is WRONGTYPE.
    let mut s = store();
    s.incr_by(b"c", 1).unwrap();
    s.expire(ExpireCmd::Expire, b"c", 100, FLAGS).unwrap();
    assert_eq!(s.incr_by(b"c", 1), Ok(2));
    assert_eq!(s.ttl(b"c"), 100);
    assert_eq!(s.type_of(b"c"), "string");
    put_other(&mut s, b"l");
    assert_eq!(s.incr_by(b"l", 1), Err(Error::WrongType));
    assert_eq!(s.decr_by(b"l", 1), Err(Error::WrongType));
}

#[test]
fn incr_after_expiry_starts_at_zero() {
    let mut s = store();
    set_opts(&mut s, "c", "41", ex(SetExpiry::Px(10))).unwrap();
    s.set_now(T0 + 11);
    assert_eq!(s.incr_by(b"c", 1), Ok(1));
    assert_eq!(s.ttl(b"c"), -1);
    check(&s);
}

// ---- EXPIRE family, TTL, PERSIST ----

#[test]
fn expire_missing_key() {
    // Redis: EXPIRE nope 10 is 0, with any valid flags too.
    let mut s = store();
    assert_eq!(s.expire(ExpireCmd::Expire, b"nope", 10, FLAGS), Ok(false));
    let xx_gt = ExpireFlags { xx: true, gt: true, ..FLAGS };
    assert_eq!(s.expire(ExpireCmd::Expire, b"nope", 10, xx_gt), Ok(false));
    assert_eq!(s.expire(ExpireCmd::Expire, b"nope", -10, FLAGS), Ok(false));
    assert_eq!(s.dbsize(), 0);
}

#[test]
fn expire_flag_conflicts() {
    // Redis: NX with XX, GT or LT; GT with LT; checked before the key.
    let mut s = store();
    let nx = ExpireFlags { nx: true, ..FLAGS };
    for f in [ExpireFlags { xx: true, ..nx }, ExpireFlags { gt: true, ..nx }, ExpireFlags { lt: true, ..nx }] {
        assert_eq!(s.expire(ExpireCmd::Expire, b"nope", 10, f), Err(Error::ExpireNxConflict));
    }
    let all = ExpireFlags { nx: true, xx: true, gt: true, lt: true };
    assert_eq!(s.expire(ExpireCmd::Expire, b"nope", 10, all), Err(Error::ExpireNxConflict));
    let gt_lt = ExpireFlags { gt: true, lt: true, ..FLAGS };
    assert_eq!(s.expire(ExpireCmd::PExpire, b"nope", 10, gt_lt), Err(Error::ExpireGtLtConflict));
    assert_eq!(
        Error::ExpireNxConflict.to_string(),
        "ERR NX and XX, GT or LT options at the same time are not compatible"
    );
    assert_eq!(Error::ExpireGtLtConflict.to_string(), "ERR GT and LT options at the same time are not compatible");
}

#[test]
fn expire_overflow() {
    // Redis: EXPIRE nope 9223372036854776 and -9223372036854776, PEXPIRE nope
    // 9223372036854775807 and EXPIREAT nope 9223372036854776 are invalid;
    // EXPIRE nope -9223372036854775 and PEXPIREAT nope 9223372036854775807
    // are fine (0: no such key).
    let mut s = store();
    let cases = [
        (ExpireCmd::Expire, 9223372036854776, Err(Error::InvalidExpireTime("expire"))),
        (ExpireCmd::Expire, -9223372036854776, Err(Error::InvalidExpireTime("expire"))),
        (ExpireCmd::Expire, -9223372036854775, Ok(false)),
        (ExpireCmd::PExpire, i64::MAX, Err(Error::InvalidExpireTime("pexpire"))),
        (ExpireCmd::PExpire, i64::MAX - T0, Ok(false)),
        (ExpireCmd::PExpire, i64::MIN, Ok(false)),
        (ExpireCmd::ExpireAt, 9223372036854776, Err(Error::InvalidExpireTime("expireat"))),
        (ExpireCmd::ExpireAt, 9223372036854775, Ok(false)),
        (ExpireCmd::PExpireAt, i64::MAX, Ok(false)),
    ];
    for (cmd, when, want) in cases {
        assert_eq!(s.expire(cmd, b"nope", when, FLAGS), want, "{cmd:?} {when}");
    }
    // The time is checked even when the key exists, and nothing changes.
    set(&mut s, "k", "v");
    assert_eq!(s.expire(ExpireCmd::Expire, b"k", i64::MAX, FLAGS), Err(Error::InvalidExpireTime("expire")));
    assert_eq!(s.ttl(b"k"), -1);
    assert_eq!(s.expire(ExpireCmd::PExpireAt, b"k", i64::MAX, FLAGS), Ok(true));
    assert_eq!(s.pttl(b"k"), i64::MAX - T0);
    assert_eq!(Error::InvalidExpireTime("pexpireat").to_string(), "ERR invalid expire time in 'pexpireat' command");
}

#[test]
fn expire_units_and_bases() {
    let mut s = store();
    for k in ["a", "b", "c", "d"] {
        set(&mut s, k, "v");
    }
    assert_eq!(s.expire(ExpireCmd::Expire, b"a", 10, FLAGS), Ok(true));
    assert_eq!(s.expire(ExpireCmd::PExpire, b"b", 10, FLAGS), Ok(true));
    assert_eq!(s.expire(ExpireCmd::ExpireAt, b"c", T0 / 1000 + 10, FLAGS), Ok(true));
    assert_eq!(s.expire(ExpireCmd::PExpireAt, b"d", T0 + 10, FLAGS), Ok(true));
    assert_eq!([s.pttl(b"a"), s.pttl(b"b"), s.pttl(b"c"), s.pttl(b"d")], [10_000, 10, 10_000, 10]);
    check(&s);
}

#[test]
fn expire_flags() {
    // Redis, in this order, on a key without TTL: XX 0, GT 0, LT 1 (no TTL
    // counts as infinite), NX 0, GT 200 1, GT 150 0, LT 100 1, LT 150 0,
    // XX LT 150 0; TTL is then 100.
    let mut s = store();
    set(&mut s, "k", "v");
    let f = |nx, xx, gt, lt| ExpireFlags { nx, xx, gt, lt };
    let steps = [
        (100, f(false, true, false, false), false),
        (100, f(false, false, true, false), false),
        (100, f(false, false, false, true), true),
        (50, f(true, false, false, false), false),
        (200, f(false, false, true, false), true),
        (150, f(false, false, true, false), false),
        (100, f(false, false, false, true), true),
        (150, f(false, false, false, true), false),
        (150, f(false, true, false, true), false),
        (100, f(false, false, true, false), false), // GT with an equal time
        (100, f(false, false, false, true), false), // LT with an equal time
    ];
    for (secs, flags, want) in steps {
        assert_eq!(s.expire(ExpireCmd::Expire, b"k", secs, flags), Ok(want), "{secs} {flags:?}");
    }
    assert_eq!(s.ttl(b"k"), 100);
    // NX on a key without TTL, XX on one with.
    set(&mut s, "n", "v");
    assert_eq!(s.expire(ExpireCmd::Expire, b"n", 30, f(true, false, false, false)), Ok(true));
    assert_eq!(s.expire(ExpireCmd::Expire, b"n", 40, f(false, true, false, false)), Ok(true));
    assert_eq!(s.ttl(b"n"), 40);
    // Redis: EXPIRE k -1 GT on a key without TTL is 0 and keeps it.
    set(&mut s, "g", "v");
    assert_eq!(s.expire(ExpireCmd::Expire, b"g", -1, f(false, false, true, false)), Ok(false));
    assert_eq!(s.ttl(b"g"), -1);
    check(&s);
}

#[test]
fn expire_in_the_past_deletes() {
    // Redis: EXPIRE k 0, EXPIRE k -5 LT and EXPIREAT k 1 reply 1 and delete.
    let mut s = store();
    set(&mut s, "k", "v");
    assert_eq!(s.expire(ExpireCmd::Expire, b"k", 0, FLAGS), Ok(true));
    assert!(!s.exists(b"k"));
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert_eq!(s.expire(ExpireCmd::Expire, b"k", -5, ExpireFlags { lt: true, ..FLAGS }), Ok(true));
    assert!(!s.exists(b"k"));
    set(&mut s, "k", "v");
    assert_eq!(s.expire(ExpireCmd::ExpireAt, b"k", 1, FLAGS), Ok(true));
    assert!(!s.exists(b"k"));
    // Exactly now counts as the past; now + 1 ms doesn't.
    set(&mut s, "k", "v");
    assert_eq!(s.expire(ExpireCmd::PExpireAt, b"k", T0, FLAGS), Ok(true));
    assert!(!s.exists(b"k"));
    set(&mut s, "k", "v");
    assert_eq!(s.expire(ExpireCmd::PExpire, b"k", 1, FLAGS), Ok(true));
    assert!(s.exists(b"k"));
    assert_eq!(s.dbsize(), 1);
    check(&s);
}

#[test]
fn ttl_and_pttl() {
    // Redis: TTL/PTTL of a missing key -2, without expiry -1; TTL rounds
    // (pttl + 500) / 1000.
    let mut s = store();
    assert_eq!((s.ttl(b"nope"), s.pttl(b"nope")), (-2, -2));
    set(&mut s, "k", "v");
    assert_eq!((s.ttl(b"k"), s.pttl(b"k")), (-1, -1));
    for (ms, secs) in [(1499, 1), (1500, 2), (499, 0), (500, 1), (1, 0), (100_000, 100)] {
        s.expire(ExpireCmd::PExpire, b"k", ms, FLAGS).unwrap();
        assert_eq!(s.pttl(b"k"), ms);
        assert_eq!(s.ttl(b"k"), secs, "{ms} ms");
    }
    put_other(&mut s, b"l");
    assert_eq!(s.ttl(b"l"), -1, "TTL works on any type");
}

#[test]
fn persist() {
    // Redis: PERSIST 1 when it removed a TTL, else 0 (also when missing).
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Ex(100))).unwrap();
    assert!(s.persist(b"k"));
    assert_eq!(s.ttl(b"k"), -1);
    assert!(!s.persist(b"k"));
    assert!(!s.persist(b"nope"));
    s.set_now(T0 + 1_000_000);
    assert!(s.exists(b"k"), "a persisted key doesn't expire");
    assert_eq!(s.expiring_len(), 0);
    check(&s);
}

#[test]
fn type_dbsize_flush() {
    // Redis: TYPE nope is none, TYPE k string; FLUSHDB empties.
    let mut s = store();
    assert_eq!(s.type_of(b"nope"), "none");
    set(&mut s, "k", "v");
    set_opts(&mut s, "e", "v", ex(SetExpiry::Ex(1))).unwrap();
    assert_eq!(s.type_of(b"k"), "string");
    assert_eq!(s.dbsize(), 2);
    s.flush();
    assert_eq!(s.dbsize(), 0);
    assert_eq!(s.expiring_len(), 0);
    assert_eq!(get(&mut s, "k"), None);
    s.set_now(T0 + 5000);
    assert_eq!(s.expire_cycle(10), 0);
    set_opts(&mut s, "e", "v", ex(SetExpiry::Ex(1))).unwrap();
    check(&s);
}

// ---- Expiry ----

#[test]
fn lazy_expiry() {
    let mut s = store();
    set_opts(&mut s, "k", "v", ex(SetExpiry::Px(100))).unwrap();
    s.set_now(T0 + 100);
    // Redis: expired only once now > expire_at; until then PTTL may be 0.
    assert_eq!(get(&mut s, "k").as_deref(), Some("v"));
    assert_eq!(s.pttl(b"k"), 0);
    s.set_now(T0 + 101);
    assert_eq!(s.dbsize(), 1, "DBSIZE counts it until it's reclaimed");
    assert_eq!(get(&mut s, "k"), None);
    assert_eq!(s.dbsize(), 0, "reading it reclaimed it");
    assert_eq!(s.expiring_len(), 0);
    check(&s);
}

#[test]
fn every_command_sees_expiry() {
    let mut s = store();
    let fresh = |s: &mut Store| {
        s.set_now(T0);
        set_opts(s, "k", "41", ex(SetExpiry::Px(1))).unwrap();
        s.set_now(T0 + 2);
    };
    fresh(&mut s);
    assert_eq!(s.strlen(b"k"), Ok(0));
    fresh(&mut s);
    assert_eq!(s.append(b"k", b"x"), Ok(1));
    assert_eq!(s.ttl(b"k"), -1);
    fresh(&mut s);
    assert_eq!(s.getdel(b"k"), Ok(None));
    fresh(&mut s);
    assert_eq!(s.ttl(b"k"), -2);
    fresh(&mut s);
    assert_eq!(s.type_of(b"k"), "none");
    fresh(&mut s);
    assert!(!s.persist(b"k"));
    fresh(&mut s);
    assert_eq!(s.expire(ExpireCmd::Expire, b"k", 10, FLAGS), Ok(false));
    fresh(&mut s);
    assert!(!s.exists(b"k"));
    fresh(&mut s);
    let get = SetOptions { get: true, ..SetOptions::default() };
    assert_eq!(set_opts(&mut s, "k", "v", get), Ok(SetReply::Old(None)));
    assert_eq!(s.ttl(b"k"), -1);
    assert_eq!(s.dbsize(), 1);
    check(&s);
}

#[test]
fn active_expiry_respects_budget_and_completes() {
    let mut s = store();
    for i in 0..1000 {
        let key = format!("e{i}");
        set_opts(&mut s, &key, "v", ex(SetExpiry::Px(10 + i % 7))).unwrap();
        set(&mut s, &format!("p{i}"), "v");
        set_opts(&mut s, &format!("l{i}"), "v", ex(SetExpiry::Ex(3600))).unwrap();
    }
    assert_eq!(s.expire_cycle(100), 0, "nothing due yet");
    s.set_now(T0 + 100);
    let mut calls = 0;
    let mut total = 0;
    loop {
        let removed = s.expire_cycle(50);
        assert!(removed <= 50);
        total += removed;
        calls += 1;
        check(&s);
        if total == 1000 {
            break;
        }
        // 2000 keys with a TTL, 50 a call: every key is seen within 40 calls.
        assert!(calls <= 40, "only {total} reclaimed after {calls} calls");
    }
    assert_eq!(s.dbsize(), 2000);
    assert_eq!(s.expiring_len(), 1000);
    assert_eq!(s.expire_cycle(5000), 0);
    assert_eq!(s.ttl(b"l3"), 3600);
}

#[test]
fn active_expiry_budget_is_exact() {
    let mut s = store();
    for i in 0..100 {
        set_opts(&mut s, &format!("e{i}"), "v", ex(SetExpiry::Px(1))).unwrap();
    }
    s.set_now(T0 + 2);
    assert_eq!(s.expire_cycle(10), 10);
    assert_eq!(s.dbsize(), 90);
    assert_eq!(s.expire_cycle(0), 0);
    assert_eq!(s.expire_cycle(1000), 90);
    assert_eq!(s.dbsize(), 0);
    check(&s);
}

#[test]
fn active_expiry_cursor_moves_on() {
    // A single due key behind many that aren't: a small budget still gets
    // there, rather than looking at the same keys again and again.
    let mut s = store();
    for i in 0..100 {
        set_opts(&mut s, &format!("l{i}"), "v", ex(SetExpiry::Ex(3600))).unwrap();
    }
    set_opts(&mut s, "due", "v", ex(SetExpiry::Px(1))).unwrap();
    s.set_now(T0 + 2);
    let calls = (1..=30).find(|_| s.expire_cycle(5) == 1).expect("never reclaimed");
    assert!(calls <= 21);
    assert_eq!(s.dbsize(), 100);
    // And it keeps going round.
    s.set_now(T0 + 3_600_001);
    let mut total = 0;
    for _ in 0..25 {
        total += s.expire_cycle(5);
    }
    assert_eq!(total, 100);
    check(&s);
}

/// Random operations against a simple model, checking the expiry index
/// after every step.
#[test]
fn model_check() {
    model_check_on(store());
}

/// The same with the slot index, which `check` verifies at every step.
#[test]
fn model_check_with_slots() {
    model_check_on(slotted());
}

fn model_check_on(mut s: Store) {
    let mut model: HashMap<Vec<u8>, (Vec<u8>, Option<i64>)> = HashMap::new();
    let mut now = T0;
    let mut rng = 0x2545f4914f6cdd1du64;
    let mut next = |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    for step in 0..20_000 {
        // Over a few slots (the first byte), several keys in each.
        let key = format!("{}{}", ["a", "b", "c", "d"][next(4) as usize], next(10)).into_bytes();
        let value = format!("v{step}").into_bytes();
        let alive = |m: &HashMap<Vec<u8>, (Vec<u8>, Option<i64>)>, k: &[u8], now| {
            m.get(k).filter(|(_, at)| at.is_none_or(|at| now <= at)).cloned()
        };
        match next(10) {
            0 => {
                s.set(&key, &value, SetOptions::default()).unwrap();
                model.insert(key.clone(), (value, None));
            }
            1 => {
                let ms = next(50) as i64 + 1;
                s.set(&key, &value, ex(SetExpiry::Px(ms))).unwrap();
                model.insert(key.clone(), (value, Some(now + ms)));
            }
            2 => {
                let was = alive(&model, &key, now).is_some();
                assert_eq!(s.del(&key), was);
                model.remove(&key);
            }
            3 => {
                let ms = next(60) as i64 - 10;
                let was = alive(&model, &key, now);
                assert_eq!(s.expire(ExpireCmd::PExpire, &key, ms, FLAGS), Ok(was.is_some()));
                if let Some((v, _)) = was {
                    if ms <= 0 {
                        model.remove(&key);
                    } else {
                        model.insert(key.clone(), (v, Some(now + ms)));
                    }
                }
            }
            4 => {
                let was = alive(&model, &key, now);
                assert_eq!(s.persist(&key), was.as_ref().is_some_and(|(_, at)| at.is_some()));
                if let Some((v, _)) = was {
                    model.insert(key.clone(), (v, None));
                }
            }
            5 => {
                now += next(5) as i64;
                s.set_now(now);
            }
            6 if next(50) == 0 => {
                s.flush();
                model.clear();
            }
            6 => {
                s.expire_cycle(next(8) as usize);
            }
            7 => {
                let opts = SetOptions { expiry: SetExpiry::KeepTtl, get: true, ..SetOptions::default() };
                let was = alive(&model, &key, now);
                let want = SetReply::Old(was.as_ref().map(|(v, _)| v.clone()));
                assert_eq!(s.set(&key, &value, opts), Ok(want));
                model.insert(key.clone(), (value, was.and_then(|(_, at)| at)));
            }
            _ => {
                let want = alive(&model, &key, now);
                assert_eq!(s.get(&key).unwrap().map(|v| v.to_vec()), want.as_ref().map(|(v, _)| v.clone()));
                let pttl = match want {
                    None => -2,
                    Some((_, None)) => -1,
                    Some((_, Some(at))) => at - now,
                };
                assert_eq!(s.pttl(&key), pttl);
            }
        }
        check(&s);
        assert!(s.dbsize() >= model.keys().filter(|k| alive(&model, k, now).is_some()).count());
    }
    // Everything due gets reclaimed in the end, nothing else.
    now += 1000;
    s.set_now(now);
    while s.expire_cycle(7) > 0 {}
    s.expire_cycle(1000);
    let live = model.iter().filter(|(_, (_, at))| at.is_none_or(|at| now <= at)).count();
    assert_eq!(s.dbsize(), live);
    check(&s);
}

#[test]
fn keys_lists_every_key() {
    let mut s = store();
    assert_eq!(s.keys().count(), 0);
    set(&mut s, "a", "1");
    set_opts(&mut s, "b", "2", ex(SetExpiry::Px(5))).unwrap();
    put_other(&mut s, b"\0c");
    s.set_now(T0 + 6);
    // b has expired but isn't reclaimed: listed, as DBSIZE counts it.
    let mut keys: Vec<&[u8]> = s.keys().collect();
    keys.sort();
    assert_eq!(keys, [&b"\0c"[..], b"a", b"b"]);
    s.flush();
    assert_eq!(s.keys().count(), 0);
}

// ---- Keys by slot ----

#[test]
fn keys_by_slot() {
    let mut s = slotted();
    assert!(s.has_slots());
    assert!(!store().has_slots());
    let slot = |s: &Store, n: u8| {
        let mut keys: Vec<Vec<u8>> = s.keys_in_slot(u16::from(n)).map(<[u8]>::to_vec).collect();
        keys.sort();
        keys
    };
    for key in ["a1", "a2", "a3", "b1"] {
        set(&mut s, key, "v");
    }
    set(&mut s, "a2", "again");
    assert_eq!((s.count_in_slot(u16::from(b'a')), s.count_in_slot(u16::from(b'b')), s.count_in_slot(0)), (3, 1, 0));
    assert_eq!(slot(&s, b'a'), [b"a1".to_vec(), b"a2".to_vec(), b"a3".to_vec()]);
    // The first of a slot goes: the last moves into its place.
    assert!(s.del(b"a1"));
    check(&s);
    assert_eq!(slot(&s, b'a'), [b"a2".to_vec(), b"a3".to_vec()]);
    // Every way a key leaves.
    assert_eq!(s.getdel(b"a2"), Ok(Some(b"again".to_vec())));
    set_opts(&mut s, "b2", "v", ex(SetExpiry::Px(5))).unwrap();
    set_opts(&mut s, "b3", "v", ex(SetExpiry::Px(5))).unwrap();
    set_opts(&mut s, "b4", "v", ex(SetExpiry::Px(5))).unwrap();
    assert_eq!(s.count_in_slot(u16::from(b'b')), 4);
    s.set_now(T0 + 6);
    assert_eq!(s.count_in_slot(u16::from(b'b')), 4, "expired keys count until reclaimed, as in DBSIZE");
    assert_eq!(get(&mut s, "b2"), None);
    assert_eq!(s.expire_cycle(10), 2);
    assert_eq!(slot(&s, b'b'), [b"b1".to_vec()]);
    assert_eq!(s.expire(ExpireCmd::PExpire, b"b1", -1, FLAGS), Ok(true));
    assert_eq!(set_opts(&mut s, "a3", "v", ex(SetExpiry::PxAt(1))), Ok(SetReply::Ok), "an expiry in the past deletes");
    assert_eq!((s.count_in_slot(u16::from(b'a')), s.count_in_slot(u16::from(b'b'))), (0, 0));
    check(&s);
    // A new key, after the slot was emptied, and every kind of insert.
    set_opts(&mut s, "c1", "v", ex(SetExpiry::Ex(10))).unwrap();
    assert!(s.setnx(b"c2", b"v"));
    s.append(b"c3", b"v").unwrap();
    s.incr_by(b"c4", 1).unwrap();
    s.mset([(&b"c5"[..], &b"v"[..]), (b"", b"empty key")]);
    assert_eq!((s.count_in_slot(u16::from(b'c')), s.count_in_slot(0)), (5, 1));
    check(&s);
    s.flush();
    assert_eq!(s.count_in_slot(u16::from(b'c')), 0);
    set(&mut s, "c1", "v");
    assert_eq!(slot(&s, b'c'), [b"c1".to_vec()]);
    check(&s);
}

#[test]
#[should_panic(expected = "without the slot index")]
fn count_in_slot_needs_the_index() {
    store().count_in_slot(0);
}

#[test]
fn entry_stays_small() {
    // `pos` fills padding: the slot index costs the map nothing.
    assert_eq!(mem::size_of::<Entry>(), 40);
}

#[test]
fn contains_doesnt_reclaim() {
    let mut s = slotted();
    set(&mut s, "a1", "v");
    set_opts(&mut s, "a2", "v", ex(SetExpiry::Px(5))).unwrap();
    assert!(s.contains(b"a1") && s.contains(b"a2") && !s.contains(b"a3"));
    s.set_now(T0 + 5);
    assert!(s.contains(b"a2"), "alive while now == expire_at");
    s.set_now(T0 + 6);
    assert!(!s.contains(b"a2"));
    assert_eq!((s.dbsize(), s.count_in_slot(u16::from(b'a'))), (2, 2), "expired, not reclaimed");
    assert!(!s.exists(b"a2"));
    assert_eq!(s.dbsize(), 1);
    check(&s);
}

#[test]
fn peek_gives_value_and_ttl() {
    let mut s = store();
    set(&mut s, "a", "1");
    set_opts(&mut s, "b", "2", ex(SetExpiry::Px(5))).unwrap();
    assert_eq!(s.peek(b"a"), Some((&b"1"[..], None)));
    assert_eq!(s.peek(b"b"), Some((&b"2"[..], Some(5))));
    assert_eq!(s.peek(b"c"), None);
    s.set_now(T0 + 5);
    assert_eq!(s.peek(b"b"), Some((&b"2"[..], Some(1))), "the last millisecond counts as 1");
    s.set_now(T0 + 6);
    assert_eq!(s.peek(b"b"), None);
    assert_eq!(s.dbsize(), 2, "not reclaimed");
    put_other(&mut s, b"o");
    assert_eq!(s.peek(b"o"), None);
}

#[test]
fn room_is_given_back_after_deletions() {
    let mut s = slotted();
    // Two slots: 'a' keeps 50 keys, 'b' loses all of its.
    for i in 0..20_000 {
        let at = if i % 2 == 0 { SetExpiry::Px(60_000) } else { SetExpiry::Clear };
        let opts = SetOptions { expiry: at, ..SetOptions::default() };
        s.set(format!("a{i}").as_bytes(), b"v", opts).unwrap();
        s.set(format!("b{i}").as_bytes(), b"v", opts).unwrap();
    }
    let (map_cap, exp_cap) = (s.map.capacity(), s.expiring.capacity());
    assert!(map_cap >= 40_000 && exp_cap >= 20_000);
    assert!(!s.shrink(), "a full store doesn't shrink");
    assert_eq!(s.map_room, map_cap);
    for i in 0..20_000 {
        s.del(format!("b{i}").as_bytes());
        if i >= 50 {
            s.del(format!("a{i}").as_bytes());
        }
    }
    let lists = &s.slots.keys;
    assert_eq!(lists[usize::from(b'b')].capacity(), 0, "an empty slot's list is freed");
    assert!(lists[usize::from(b'a')].capacity() <= 200, "{}", lists[usize::from(b'a')].capacity());
    // Deleted entries leave tombstones, which take from what `capacity`
    // reports, but the table keeps its size until asked.
    assert!(s.map.capacity() > map_cap / 2, "{} of {map_cap}", s.map.capacity());
    assert!(s.shrink());
    assert!(s.map.capacity() < 1000, "{}", s.map.capacity());
    assert!(s.expiring.capacity() <= 50, "{}", s.expiring.capacity());
    check(&s);
    assert_eq!(s.dbsize(), 50);
    for i in 0..50 {
        assert_eq!(s.get(format!("a{i}").as_bytes()), Ok(Some(&b"v"[..])));
    }
    assert_eq!(s.count_in_slot(u16::from(b'a')), 50);
    // Small tables are left alone.
    let mut small = store();
    for i in 0..500 {
        small.set(format!("k{i}").as_bytes(), b"v", SetOptions::default()).unwrap();
    }
    for i in 0..500 {
        small.del(format!("k{i}").as_bytes());
    }
    assert!(!small.shrink());
}

#[test]
fn growth_says_when_the_map_would_double() {
    let mut s = store();
    assert!(s.growth() > 0, "an empty map allocates on the first key");
    let mut i = 0;
    while s.map.len() < 1000 || s.map.len() < s.map.capacity() {
        if s.map.len() >= 1000 {
            assert_eq!(s.growth(), 0, "room for {} more", s.map.capacity() - s.map.len());
        }
        s.set(format!("k{i}").as_bytes(), b"v", SetOptions::default()).unwrap();
        i += 1;
    }
    let (capacity, growth) = (s.map.capacity(), s.growth());
    let bucket = mem::size_of::<(Rc<[u8]>, Entry)>() + 1;
    // The next table: twice the buckets of this one.
    let buckets = (capacity * 8 / 7).next_power_of_two();
    assert_eq!(growth, 2 * buckets * bucket, "{capacity} items in {buckets} buckets");
    s.set(b"one more", b"v", SetOptions::default()).unwrap();
    assert!(s.map.capacity() > capacity);
    assert_eq!(s.growth(), 0);
}

#[test]
fn encodings_are_redis_s() {
    // Redis: OBJECT ENCODING after SET, INCR and APPEND.
    let mut s = store();
    for (value, encoding) in [
        (&b"12345"[..], "int"),
        (b"-9223372036854775808", "int"),
        (b"9223372036854775808", "embstr"),
        (b"007", "embstr"),
        (b"-0", "embstr"),
        (&[b'x'; 44], "embstr"),
        (&[b'x'; 45], "raw"),
    ] {
        s.set(b"k", value, SetOptions::default()).unwrap();
        assert_eq!(s.encoding(b"k"), Some(encoding), "{value:?}");
    }
    s.incr_by(b"n", 1).unwrap();
    assert_eq!(s.encoding(b"n"), Some("int"));
    assert_eq!(s.encoding(b"nope"), None);
    put_other(&mut s, b"l");
    assert_eq!(s.encoding(b"l"), Some("quicklist"));
}
