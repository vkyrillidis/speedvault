//! The geo commands (Redis 8.10's `geo.c`): parsing, checks in Redis's
//! order, and replies. A geo key is a sorted set whose scores are geohashes
//! ([`store::geohash`]), so `GEOADD` is a `ZADD` and the rest read and
//! write sorted sets.
//!
//! Positions are replied as `addReplyDouble` writes them (the scores'
//! format), distances with 4 decimals as `fixedpoint_d2string` does (a
//! bulk string in RESP3 too).

use super::zset::{is, score, zset};
use super::{c_str, store_error, syntax_error, Flow};
use resp::{Args, Encoder};
use store::geohash::{self, Extent, Shape, LAT_MAX, LAT_MIN, LONG_MAX, LONG_MIN};
use store::{parse_i64, Store, ZSet, ZaddFlags};

/// A double argument, as `getDoubleFromObjectOrReply` (`msg`: its error,
/// else Redis's usual one).
fn float(arg: &[u8], msg: Option<&[u8]>, out: &mut Encoder) -> Option<f64> {
    let v = store::parse_double(arg);
    if v.is_none() {
        match msg {
            Some(msg) => out.error(msg),
            None => store_error(store::Error::NotFloat, out),
        }
    }
    v
}

/// `extractLongLatOrReply`: both read, then checked against the limits.
// Redis's comparisons as written: `!(a..=b).contains(&x)` would take a NaN
// as out of range, these don't.
#[allow(clippy::manual_range_contains)]
fn long_lat(lon: &[u8], lat: &[u8], out: &mut Encoder) -> Option<[f64; 2]> {
    let x = float(lon, None, out)?;
    let y = float(lat, None, out)?;
    if x < LONG_MIN || x > LONG_MAX || y < LAT_MIN || y > LAT_MAX {
        // `%f`: Rust's `{:.6}` writes the same exact decimals (and `-0`).
        out.error(format!("ERR invalid longitude,latitude pair {x:.6},{y:.6}").as_bytes());
        return None;
    }
    Some([x, y])
}

/// `extractUnitOrReply`: a unit's size in meters.
fn unit(arg: &[u8], out: &mut Encoder) -> Option<f64> {
    let size = [(&b"m"[..], 1.0), (b"km", 1000.0), (b"ft", 0.3048), (b"mi", 1609.34)].into_iter().find(|(name, _)| is(arg, name));
    if size.is_none() {
        out.error(b"ERR unsupported unit provided. please use M, KM, FT, MI");
    }
    size.map(|(_, s)| s)
}

/// `extractDistanceOrReply`: a radius and its unit's size.
fn radius(num: &[u8], u: &[u8], out: &mut Encoder) -> Option<(f64, f64)> {
    let r = float(num, Some(b"ERR need numeric radius"), out)?;
    if r < 0.0 {
        out.error(b"ERR radius cannot be negative");
        return None;
    }
    Some((r, unit(u, out)?))
}

/// `extractBoxOrReply`: width, height and the unit's size.
fn bbox(w: &[u8], h: &[u8], u: &[u8], out: &mut Encoder) -> Option<(f64, f64, f64)> {
    let w = float(w, Some(b"ERR need numeric width"), out)?;
    let h = float(h, Some(b"ERR need numeric height"), out)?;
    if h < 0.0 || w < 0.0 {
        out.error(b"ERR height or width cannot be negative");
        return None;
    }
    Some((w, h, unit(u, out)?))
}

/// `longLatFromMember`, replying Redis's error for a missing member.
fn member_point(z: &ZSet, member: &[u8], out: &mut Encoder) -> Option<[f64; 2]> {
    let p = z.score(member).map(geohash::decode_score);
    if p.is_none() {
        out.error(b"ERR could not decode requested zset member");
    }
    p
}

/// `GEOADD key [NX|XX] [CH] longitude latitude member [...]`: every point
/// checked, then a `ZADD` of their geohashes.
pub(super) fn geoadd(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let mut flags = ZaddFlags::default();
    let mut ch = false;
    let mut at = 2;
    while at < args.len() {
        let opt = &args[at];
        if is(opt, b"nx") {
            flags.nx = true;
        } else if is(opt, b"xx") {
            flags.xx = true;
        } else if is(opt, b"ch") {
            ch = true;
        } else {
            break;
        }
        at += 1;
    }
    let rest = args.len() - at;
    // No point at all fails in the ZADD it becomes, with the same error.
    if !rest.is_multiple_of(3) || (flags.xx && flags.nx) || rest == 0 {
        syntax_error(out);
        return Flow::Continue;
    }
    let mut pairs = Vec::with_capacity(rest / 3);
    for i in (at..args.len()).step_by(3) {
        let Some([x, y]) = long_lat(&args[i], &args[i + 1], out) else { return Flow::Continue };
        pairs.push((geohash::point_score(x, y), &args[i + 2][..]));
    }
    match store.zadd(&args[1], flags, &pairs) {
        Ok(done) => out.integer((if ch { done.added + done.updated } else { done.added }) as i64),
        Err(e) => store_error(e, out),
    }
    Flow::Continue
}

/// `GEOPOS key [member ...]`: [longitude, latitude] each, nil if missing.
pub(super) fn geopos(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Ok(z) = zset(&args[1], store, out) else { return Flow::Continue };
    out.array(args.len() - 2);
    for m in args.iter().skip(2) {
        match z.and_then(|z| z.score(m)) {
            Some(s) => {
                let [x, y] = geohash::decode_score(s);
                out.array(2);
                score(out, x);
                score(out, y);
            }
            None => out.null_array(),
        }
    }
    Flow::Continue
}

/// `GEOHASH key [member ...]`: 11-character geohashes, nil if missing.
pub(super) fn geohash(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let Ok(z) = zset(&args[1], store, out) else { return Flow::Continue };
    out.array(args.len() - 2);
    for m in args.iter().skip(2) {
        match z.and_then(|z| z.score(m)) {
            Some(s) => out.bulk(&geohash::geohash_string(s)),
            None => out.null(),
        }
    }
    Flow::Continue
}

/// `GEODIST key member1 member2 [unit]`: the unit first, then the key; nil
/// if either member is missing.
pub(super) fn geodist(args: &Args, store: &mut Store, out: &mut Encoder) -> Flow {
    let to_meters = match args.len() {
        4 => 1.0,
        5 => match unit(&args[4], out) {
            Some(u) => u,
            None => return Flow::Continue,
        },
        _ => {
            syntax_error(out);
            return Flow::Continue;
        }
    };
    let z = match zset(&args[1], store, out) {
        Err(()) => return Flow::Continue,
        Ok(None) => {
            out.null();
            return Flow::Continue;
        }
        Ok(Some(z)) => z,
    };
    match (z.score(&args[2]), z.score(&args[3])) {
        (Some(a), Some(b)) => {
            let [x1, y1] = geohash::decode_score(a);
            let [x2, y2] = geohash::decode_score(b);
            out.bulk(geohash::distance_text(geohash::distance(x1, y1, x2, y2) / to_meters).as_bytes());
        }
        _ => out.null(),
    }
    Flow::Continue
}

/// Which search command (`georadiusGeneric`'s flags).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Form {
    /// `GEORADIUS key longitude latitude radius unit ...`.
    Radius,
    /// `GEORADIUSBYMEMBER key member radius unit ...`.
    ByMember,
    /// `GEOSEARCH key ...`.
    Search,
    /// `GEOSEARCHSTORE destination source ...`.
    SearchStore,
}

/// `GEORADIUS`, `GEORADIUSBYMEMBER` (`ro`: their `_RO` forms, which take
/// no `STORE`), `GEOSEARCH` and `GEOSEARCHSTORE`, as `georadiusGeneric`:
/// the key's type, the centre and the extent of the older forms, the
/// options in order (each checked as it's read), the checks across them,
/// then the search.
pub(super) fn search(args: &Args, store: &mut Store, out: &mut Encoder, kind: Form, ro: bool) -> Flow {
    let src = if kind == Form::SearchStore { 2 } else { 1 };
    let Ok(z) = zset(&args[src], store, out) else { return Flow::Continue };
    let mut shape = Shape { xy: [0.0; 2], conversion: 0.0, extent: Extent::Radius(0.0) };
    let mut store_key: Option<&[u8]> = None;
    let base = match kind {
        Form::Radius => {
            let Some(xy) = long_lat(&args[2], &args[3], out) else { return Flow::Continue };
            let Some((r, conversion)) = radius(&args[4], &args[5], out) else { return Flow::Continue };
            shape = Shape { xy, conversion, extent: Extent::Radius(r) };
            6
        }
        // Without the key, the radius isn't read: only the options, for
        // the reply's form.
        Form::ByMember => {
            if let Some(z) = z {
                let Some(xy) = member_point(z, &args[2], out) else { return Flow::Continue };
                let Some((r, conversion)) = radius(&args[3], &args[4], out) else { return Flow::Continue };
                shape = Shape { xy, conversion, extent: Extent::Radius(r) };
            }
            5
        }
        Form::Search => 2,
        Form::SearchStore => {
            store_key = Some(&args[1]);
            3
        }
    };
    let geosearch = matches!(kind, Form::Search | Form::SearchStore);
    let (mut with_dist, mut with_hash, mut with_coord) = (false, false, false);
    let (mut from_member, mut from_loc, mut by_radius, mut by_box) = (false, false, false, false);
    // `Some(desc)` for ASC / DESC.
    let mut sort: Option<bool> = None;
    let mut any = false;
    let mut count: i64 = 0;
    let mut store_dist = false;
    let remaining = args.len().saturating_sub(base);
    let mut i = 0;
    while i < remaining {
        let at = base + i;
        let arg = &args[at];
        if is(arg, b"withdist") {
            with_dist = true;
        } else if is(arg, b"withhash") {
            with_hash = true;
        } else if is(arg, b"withcoord") {
            with_coord = true;
        } else if is(arg, b"any") {
            any = true;
        } else if is(arg, b"asc") {
            sort = Some(false);
        } else if is(arg, b"desc") {
            sort = Some(true);
        } else if is(arg, b"count") && i + 1 < remaining {
            let Some(n) = parse_i64(&args[at + 1]) else {
                store_error(store::Error::NotInteger, out);
                return Flow::Continue;
            };
            if n <= 0 {
                out.error(b"ERR COUNT must be > 0");
                return Flow::Continue;
            }
            count = n;
            i += 1;
        } else if (is(arg, b"store") || is(arg, b"storedist")) && i + 1 < remaining && !ro && !geosearch {
            store_key = Some(&args[at + 1]);
            store_dist = is(arg, b"storedist");
            i += 1;
        } else if is(arg, b"storedist") && kind == Form::SearchStore {
            store_dist = true;
        } else if is(arg, b"frommember") && i + 1 < remaining && geosearch && !from_loc {
            // Without the key, read on for the reply's form.
            if let Some(z) = z {
                let Some(xy) = member_point(z, &args[at + 1], out) else { return Flow::Continue };
                shape.xy = xy;
            }
            from_member = true;
            i += 1;
        } else if is(arg, b"fromlonlat") && i + 2 < remaining && geosearch && !from_member {
            let Some(xy) = long_lat(&args[at + 1], &args[at + 2], out) else { return Flow::Continue };
            shape.xy = xy;
            from_loc = true;
            i += 2;
        } else if is(arg, b"byradius") && i + 2 < remaining && geosearch && !by_box {
            let Some((r, conversion)) = radius(&args[at + 1], &args[at + 2], out) else { return Flow::Continue };
            shape.conversion = conversion;
            shape.extent = Extent::Radius(r);
            by_radius = true;
            i += 2;
        } else if is(arg, b"bybox") && i + 3 < remaining && geosearch && !by_radius {
            let Some((width, height, conversion)) = bbox(&args[at + 1], &args[at + 2], &args[at + 3], out) else { return Flow::Continue };
            shape.conversion = conversion;
            shape.extent = Extent::Box { width, height };
            by_box = true;
            i += 3;
        } else {
            syntax_error(out);
            return Flow::Continue;
        }
        i += 1;
    }
    if store_key.is_some() && (with_dist || with_hash || with_coord) {
        let what = if kind == Form::SearchStore { "GEOSEARCHSTORE" } else { "STORE option in GEORADIUS" };
        out.error(format!("ERR {what} is not compatible with WITHDIST, WITHHASH and WITHCOORD options").as_bytes());
        return Flow::Continue;
    }
    let name = c_str(&args[0], usize::MAX);
    if geosearch && !(from_member || from_loc) {
        out.error_parts(&[b"ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for ", name]);
        return Flow::Continue;
    }
    if geosearch && !(by_radius || by_box) {
        out.error_parts(&[b"ERR exactly one of BYRADIUS and BYBOX can be specified for ", name]);
        return Flow::Continue;
    }
    if any && count == 0 {
        out.error(b"ERR the ANY argument requires COUNT argument");
        return Flow::Continue;
    }
    let Some(z) = z else {
        match store_key {
            Some(dst) => out.integer(store.zstore(dst, &[], 0) as i64),
            None => {
                store.wrote_nothing();
                out.array(0);
            }
        }
        return Flow::Continue;
    };
    // COUNT without an order means the nearest: ASC (not with ANY).
    if count != 0 && sort.is_none() && !any {
        sort = Some(false);
    }
    let count = count as usize;
    let mut points = geohash::search(z, &shape, if any { count } else { 0 });
    let returned = if count == 0 || points.len() < count { points.len() } else { count };
    if let Some(desc) = sort {
        geohash::sort(&mut points, desc, returned);
    }
    let points = &points[..returned];
    let conversion = shape.conversion;
    let Some(dst) = store_key else {
        let options = usize::from(with_dist) + usize::from(with_hash) + usize::from(with_coord);
        out.array(returned);
        for p in points {
            if options > 0 {
                out.array(options + 1);
            }
            out.bulk(p.member);
            if with_dist {
                out.bulk(geohash::distance_text(p.dist / conversion).as_bytes());
            }
            if with_hash {
                out.integer(geohash::score_to_i64(p.score));
            }
            if with_coord {
                out.array(2);
                score(out, p.longitude);
                score(out, p.latitude);
            }
        }
        // Read-only after all: nothing for the replicas.
        store.wrote_nothing();
        return Flow::Continue;
    };
    let members: Vec<(Vec<u8>, f64)> =
        points.iter().map(|p| (p.member.to_vec(), if store_dist { p.dist / conversion } else { p.score })).collect();
    let members: Vec<(&[u8], f64)> = members.iter().map(|(m, s)| (&m[..], *s)).collect();
    out.integer(store.zstore_large(dst, &members) as i64);
    Flow::Continue
}
