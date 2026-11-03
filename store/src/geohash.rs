//! Geo points as sorted set scores, and the search around them: Redis
//! 8.10's `geohash.c` and `geohash_helper.c` ported line by line, and the
//! scan of `geo.c`'s `geoGetPointsInRange`, whose order is a reply's
//! without `ASC` or `DESC`.
//!
//! A point is a longitude in [-180, 180] and a latitude in [-85.05112878,
//! 85.05112878], each cut into 26 bits and interleaved, the latitude's in
//! the even places: the 52-bit result is the member's score. A search picks
//! a cell size from the area's size, then scans the cell of the centre and
//! its 8 neighbours (those the area can reach) as score ranges, keeping the
//! points inside the circle (haversine) or the box.
//!
//! Every operation is done as the C does it, in the same order, so that
//! the last bits of a distance are the same: `sin`, `cos`, `asin` and
//! `sqrt` are the platform's libm through Rust's `f64` methods, glibc on
//! Linux as in Redis's build. Where the C converts a double to an integer
//! out of range (a score that was never a geohash), the x86-64 result is
//! reproduced ([`score_bits`], [`score_to_i64`]).

use crate::zset::{Bounds, ScoreRange, ZSet};
use std::f64::consts::PI;

/// `GEO_STEP_MAX`: 26 bits each, 52 in all.
pub const STEP_MAX: u8 = 26;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;
pub const LONG_MIN: f64 = -180.0;
pub const LONG_MAX: f64 = 180.0;

/// `EARTH_RADIUS_IN_METERS`: the quadratic mean radius of WGS-84.
const EARTH_RADIUS: f64 = 6372797.560856;
const MERCATOR_MAX: f64 = 20037726.37;
/// `D_R`.
const D_R: f64 = PI / 180.0;

fn deg_rad(ang: f64) -> f64 {
    ang * D_R
}

fn rad_deg(ang: f64) -> f64 {
    ang / D_R
}

/// `GeoHashBits`: the cell `bits` of `step` bits per coordinate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HashBits {
    pub bits: u64,
    pub step: u8,
}

impl HashBits {
    /// `HASHISZERO`: a neighbour left out.
    fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

/// `GeoHashRange`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Range {
    pub min: f64,
    pub max: f64,
}

impl Range {
    /// `RANGEISZERO`.
    fn is_zero(self) -> bool {
        self.max == 0.0 && self.min == 0.0
    }
}

/// `GeoHashArea`: a cell and what it covers.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Area {
    pub hash: HashBits,
    pub longitude: Range,
    pub latitude: Range,
}

/// `GeoHashNeighbors`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Neighbors {
    north: HashBits,
    east: HashBits,
    west: HashBits,
    south: HashBits,
    north_east: HashBits,
    south_east: HashBits,
    north_west: HashBits,
    south_west: HashBits,
}

/// `geohashGetCoordRange`: the longitudes and latitudes a score covers.
pub const LONG_RANGE: Range = Range { min: LONG_MIN, max: LONG_MAX };
pub const LAT_RANGE: Range = Range { min: LAT_MIN, max: LAT_MAX };

/// `interleave64`: `x`'s bits in the even places, `y`'s in the odd.
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let mut x = u64::from(xlo);
    let mut y = u64::from(ylo);
    for k in (0..5).rev() {
        x = (x | (x << S[k])) & B[k];
        y = (y | (y << S[k])) & B[k];
    }
    x | (y << 1)
}

/// `deinterleave64`: the even places in the low 32 bits, the odd ones in
/// the high 32.
fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] =
        [0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF, 0x00000000FFFFFFFF];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    let mut x = interleaved;
    let mut y = interleaved >> 1;
    for k in 0..6 {
        x = (x | (x >> S[k])) & B[k];
        y = (y | (y >> S[k])) & B[k];
    }
    x | (y << 32)
}

/// `geohashEncode`: the cell of `step` bits holding the point, `None` for
/// a point outside the ranges or Redis's limits.
pub fn encode(long_range: Range, lat_range: Range, longitude: f64, latitude: f64, step: u8) -> Option<HashBits> {
    if step > 32 || step == 0 || lat_range.is_zero() || long_range.is_zero() {
        return None;
    }
    if longitude > LONG_MAX || longitude < LONG_MIN || latitude > LAT_MAX || latitude < LAT_MIN {
        return None;
    }
    if latitude < lat_range.min || latitude > lat_range.max || longitude < long_range.min || longitude > long_range.max {
        return None;
    }
    let mut lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    let mut long_offset = (longitude - long_range.min) / (long_range.max - long_range.min);
    lat_offset *= (1u64 << step) as f64;
    long_offset *= (1u64 << step) as f64;
    // C's conversion to uint32_t: in range here (at most 2^step).
    Some(HashBits { bits: interleave64(lat_offset as u32, long_offset as u32), step })
}

/// `geohashEncodeWGS84`.
pub fn encode_wgs84(longitude: f64, latitude: f64, step: u8) -> Option<HashBits> {
    encode(LONG_RANGE, LAT_RANGE, longitude, latitude, step)
}

/// `geohashDecode`: what the cell covers.
pub fn decode(long_range: Range, lat_range: Range, hash: HashBits) -> Option<Area> {
    if hash.is_zero() || lat_range.is_zero() || long_range.is_zero() {
        return None;
    }
    let step = hash.step;
    let hash_sep = deinterleave64(hash.bits);
    let lat_scale = lat_range.max - lat_range.min;
    let long_scale = long_range.max - long_range.min;
    let ilato = hash_sep as u32;
    let ilono = (hash_sep >> 32) as u32;
    let cells = (1u64 << step) as f64;
    // `ilato + 1` is an unsigned int's sum: it wraps.
    Some(Area {
        hash,
        latitude: Range {
            min: lat_range.min + (f64::from(ilato) * 1.0 / cells) * lat_scale,
            max: lat_range.min + (f64::from(ilato.wrapping_add(1)) * 1.0 / cells) * lat_scale,
        },
        longitude: Range {
            min: long_range.min + (f64::from(ilono) * 1.0 / cells) * long_scale,
            max: long_range.min + (f64::from(ilono.wrapping_add(1)) * 1.0 / cells) * long_scale,
        },
    })
}

/// `geohashDecodeAreaToLongLat`: the cell's centre, within the limits.
fn area_to_long_lat(area: &Area) -> [f64; 2] {
    // As the C's two `if`s each (a NaN would stay NaN).
    let x = ((area.longitude.min + area.longitude.max) / 2.0).clamp(LONG_MIN, LONG_MAX);
    let y = ((area.latitude.min + area.latitude.max) / 2.0).clamp(LAT_MIN, LAT_MAX);
    [x, y]
}

/// `geohashDecodeToLongLatWGS84`.
fn decode_to_long_lat(hash: HashBits) -> Option<[f64; 2]> {
    decode(LONG_RANGE, LAT_RANGE, hash).map(|a| area_to_long_lat(&a))
}

/// C's `(uint64_t)` of a double, as gcc does it on x86-64: below 2^63 a
/// signed conversion (a negative score wraps around; below -2^63, or NaN,
/// `0x8000000000000000`), else one of the score less 2^63 with the top bit
/// flipped (2^64 and past, infinity, give 0).
pub fn score_bits(score: f64) -> u64 {
    const TWO63: f64 = 9223372036854775808.0;
    if score < TWO63 {
        cvttsd2si(score) as u64
    } else {
        (cvttsd2si(score - TWO63) as u64) ^ (1 << 63)
    }
}

/// x86-64's `cvttsd2si`: truncated, `i64::MIN` out of range or for NaN.
fn cvttsd2si(x: f64) -> i64 {
    if x > -9223372036854777856.0 && x < 9223372036854775808.0 { x as i64 } else { i64::MIN }
}

/// C's `(long long)` of a score (`WITHHASH`'s `addReplyLongLong`).
pub fn score_to_i64(score: f64) -> i64 {
    cvttsd2si(score)
}

/// `decodeGeohash`: the point a score stands for (a score that was never a
/// geohash still decodes, to some point within the limits).
pub fn decode_score(score: f64) -> [f64; 2] {
    decode_to_long_lat(HashBits { bits: score_bits(score), step: STEP_MAX }).expect("the step isn't 0")
}

/// `geohashAlign52Bits`.
fn align52(hash: HashBits) -> u64 {
    hash.bits << (52 - u32::from(hash.step) * 2)
}

/// `GEOADD`'s score for a point (checked against the limits).
pub fn point_score(longitude: f64, latitude: f64) -> f64 {
    align52(encode_wgs84(longitude, latitude, STEP_MAX).expect("a point within the limits")) as f64
}

/// `GEOHASH`'s 11 characters for a score: the point re-encoded with the
/// usual latitudes of [-90, 90], the last character always `0`.
pub fn geohash_string(score: f64) -> [u8; 11] {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let [x, y] = decode_score(score);
    let r = (Range { min: -180.0, max: 180.0 }, Range { min: -90.0, max: 90.0 });
    let bits = encode(r.0, r.1, x, y, 26).map_or(0, |h| h.bits);
    let mut buf = [b'0'; 11];
    for (i, c) in buf.iter_mut().enumerate().take(10) {
        *c = ALPHABET[((bits >> (52 - (i + 1) * 5)) & 0x1f) as usize];
    }
    buf
}

/// `geohash_move_x`: a cell east (`d` > 0) or west.
fn move_x(hash: &mut HashBits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let y = hash.bits & 0x5555555555555555;
    let zz = 0x5555555555555555u64 >> (64 - u32::from(hash.step) * 2);
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaaaaaaaaaaaaaau64 >> (64 - u32::from(hash.step) * 2);
    hash.bits = x | y;
}

/// `geohash_move_y`: a cell north (`d` > 0) or south.
fn move_y(hash: &mut HashBits, d: i8) {
    if d == 0 {
        return;
    }
    let x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let mut y = hash.bits & 0x5555555555555555;
    let zz = 0xaaaaaaaaaaaaaaaau64 >> (64 - u32::from(hash.step) * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555u64 >> (64 - u32::from(hash.step) * 2);
    hash.bits = x | y;
}

/// `geohashNeighbors`.
fn neighbors(hash: HashBits) -> Neighbors {
    let moved = |dx: i8, dy: i8| {
        let mut h = hash;
        move_x(&mut h, dx);
        move_y(&mut h, dy);
        h
    };
    Neighbors {
        east: moved(1, 0),
        west: moved(-1, 0),
        south: moved(0, -1),
        north: moved(0, 1),
        north_west: moved(-1, 1),
        north_east: moved(1, 1),
        south_east: moved(1, -1),
        south_west: moved(-1, -1),
    }
}

/// `geohashEstimateStepsByRadius`: the bits per coordinate of the cells a
/// search of `range_meters` around latitude `lat` scans.
fn estimate_steps_by_radius(mut range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }
    step -= 2;
    // Wider towards the poles.
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// What a search covers (`GeoShape`'s `type` and `t`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Extent {
    /// `BYRADIUS`, in the unit.
    Radius(f64),
    /// `BYBOX`, in the unit.
    Box { width: f64, height: f64 },
}

/// `GeoShape`: a search's centre (longitude, latitude), its extent, and
/// the unit's size in meters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Shape {
    pub xy: [f64; 2],
    pub conversion: f64,
    pub extent: Extent,
}

/// `geohashBoundingBox`: min longitude, min latitude, max longitude, max
/// latitude (the edges bend away from the equator: the wider of the two
/// latitudes is taken).
fn bounding_box(shape: &Shape) -> [f64; 4] {
    let [longitude, latitude] = shape.xy;
    let (height, width) = match shape.extent {
        Extent::Radius(r) => (shape.conversion * r, shape.conversion * r),
        Extent::Box { width, height } => (shape.conversion * (height / 2.0), shape.conversion * (width / 2.0)),
    };
    let lat_delta = rad_deg(height / EARTH_RADIUS);
    let long_delta_top = rad_deg(width / EARTH_RADIUS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom = rad_deg(width / EARTH_RADIUS / deg_rad(latitude - lat_delta).cos());
    let southern = latitude < 0.0;
    [
        if southern { longitude - long_delta_bottom } else { longitude - long_delta_top },
        latitude - lat_delta,
        if southern { longitude + long_delta_bottom } else { longitude + long_delta_top },
        latitude + lat_delta,
    ]
}

/// `GeoHashRadius`: the centre's cell, its neighbours (those left out
/// zeroed) and what the centre's cell covers.
struct Cells {
    hash: HashBits,
    neighbors: Neighbors,
}

/// `geohashCalculateAreasByShapeWGS84`.
fn areas_by_shape(shape: &Shape) -> Cells {
    let [min_lon, min_lat, max_lon, max_lat] = bounding_box(shape);
    let [longitude, latitude] = shape.xy;
    let radius_meters = match shape.extent {
        Extent::Radius(r) => r,
        Extent::Box { width, height } => ((width / 2.0) * (width / 2.0) + (height / 2.0) * (height / 2.0)).sqrt(),
    } * shape.conversion;
    let mut steps = estimate_steps_by_radius(radius_meters, latitude);
    // The centre is within the limits: it was checked, or is a member's.
    let cell = |steps| {
        let hash = encode(LONG_RANGE, LAT_RANGE, longitude, latitude, steps).unwrap_or_default();
        let area = decode(LONG_RANGE, LAT_RANGE, hash).unwrap_or_default();
        (hash, neighbors(hash), area)
    };
    let (mut hash, mut neighbors, mut area) = cell(steps);
    // A step too fine for the area near a cell's edge: one coarser.
    let decrease_step = {
        let side = |h| decode(LONG_RANGE, LAT_RANGE, h).unwrap_or_default();
        let (north, south, east, west) = (side(neighbors.north), side(neighbors.south), side(neighbors.east), side(neighbors.west));
        north.latitude.max < max_lat || south.latitude.min > min_lat || east.longitude.max < max_lon || west.longitude.min > min_lon
    };
    if steps > 1 && decrease_step {
        steps -= 1;
        (hash, neighbors, area) = cell(steps);
    }
    // The neighbours the area can't reach are left out.
    if steps >= 2 {
        let zero = HashBits::default();
        if area.latitude.min < min_lat {
            (neighbors.south, neighbors.south_west, neighbors.south_east) = (zero, zero, zero);
        }
        if area.latitude.max > max_lat {
            (neighbors.north, neighbors.north_east, neighbors.north_west) = (zero, zero, zero);
        }
        if area.longitude.min < min_lon {
            (neighbors.west, neighbors.south_west, neighbors.north_west) = (zero, zero, zero);
        }
        if area.longitude.max > max_lon {
            (neighbors.east, neighbors.south_east, neighbors.north_east) = (zero, zero, zero);
        }
    }
    Cells { hash, neighbors }
}

/// `geohashGetLatDistance`.
fn lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS * (deg_rad(lat2d) - deg_rad(lat1d)).abs()
}

/// `geohashGetDistance`: the haversine distance in meters.
pub fn distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = deg_rad(lon1d);
    let lon2r = deg_rad(lon2d);
    let v = ((lon2r - lon1r) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1d, lat2d);
    }
    let lat1r = deg_rad(lat1d);
    let lat2r = deg_rad(lat2d);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS * a.sqrt().asin()
}

/// `geohashGetDistanceIfInRectangle`: the distance of (x2, y2) from the
/// box's centre (x1, y1), if inside.
fn distance_if_in_rectangle(width_m: f64, height_m: f64, x1: f64, y1: f64, x2: f64, y2: f64) -> Option<f64> {
    if lat_distance(y2, y1) > height_m / 2.0 {
        return None;
    }
    if distance(x2, y2, x1, y2) > width_m / 2.0 {
        return None;
    }
    Some(distance(x1, y1, x2, y2))
}

/// `geoWithinShape`: the point of `score` and its distance from the
/// centre, if inside. (A NaN distance is inside, as `>` says.)
fn within(shape: &Shape, score: f64) -> Option<([f64; 2], f64)> {
    let xy = decode_score(score);
    let [x1, y1] = shape.xy;
    let distance = match shape.extent {
        Extent::Radius(r) => {
            let d = distance(x1, y1, xy[0], xy[1]);
            if d > r * shape.conversion {
                return None;
            }
            d
        }
        Extent::Box { width, height } => {
            distance_if_in_rectangle(width * shape.conversion, height * shape.conversion, x1, y1, xy[0], xy[1])?
        }
    };
    Some((xy, distance))
}

/// `geoPoint`: a member found, with its point, its distance from the
/// centre (meters) and its score.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Point<'a> {
    pub longitude: f64,
    pub latitude: f64,
    pub dist: f64,
    pub score: f64,
    pub member: &'a [u8],
}

/// `geoGetPointsInRange`: the members scored in [`min`, `max`) inside the
/// shape, in order, onto `found`; stops at `limit` found in all (0: no
/// limit).
fn points_in_range<'a>(z: &'a ZSet, min: u64, max: u64, shape: &Shape, found: &mut Vec<Point<'a>>, limit: usize) {
    let range = ScoreRange { min: min as f64, max: max as f64, minex: false, maxex: true };
    let Some(first) = z.range_first(&range) else { return };
    for (member, score) in z.iter(first, usize::MAX, false) {
        if !range.lte_max(score, member) {
            break;
        }
        if let Some(([longitude, latitude], dist)) = within(shape, score) {
            found.push(Point { longitude, latitude, dist, score, member });
        }
        if !found.is_empty() && limit > 0 && found.len() >= limit {
            break;
        }
    }
}

/// `membersOfAllNeighbors` after `geohashCalculateAreasByShapeWGS84`: the
/// members of `z` inside `shape`, in Redis's scan order (the centre's cell,
/// then north, south, east, west, north-east, north-west, south-east,
/// south-west, each by score), `limit` at most (0: all; `ANY`).
pub fn search<'a>(z: &'a ZSet, shape: &Shape, limit: usize) -> Vec<Point<'a>> {
    let Cells { hash, neighbors: n } = areas_by_shape(shape);
    let cells = [hash, n.north, n.south, n.east, n.west, n.north_east, n.north_west, n.south_east, n.south_west];
    let mut found = Vec::new();
    // As Redis: 0 doubles as "none yet", so a neighbour the same as the
    // centre's cell is scanned again.
    let mut last_processed = 0;
    for (i, &cell) in cells.iter().enumerate() {
        if cell.is_zero() {
            continue;
        }
        // Neighbours of a huge radius can be the same cell.
        if last_processed != 0 && cell == cells[last_processed] {
            continue;
        }
        if !found.is_empty() && limit > 0 && found.len() >= limit {
            break;
        }
        let min = align52(cell);
        let max = align52(HashBits { bits: cell.bits + 1, step: cell.step });
        points_in_range(z, min, max, shape, &mut found, limit);
        last_processed = i;
    }
    found
}

/// `sort_gp_asc`, a C comparator: 1, 0 or -1 (with a NaN, -1).
fn cmp_asc(a: &Point, b: &Point) -> i32 {
    if a.dist > b.dist {
        1
    } else if a.dist == b.dist {
        0
    } else {
        -1
    }
}

/// `ASC` (or `DESC`) on found points, as Redis: `qsort` of all of them
/// when `returned` covers them, else Redis's `pqsort`, which sorts only
/// enough for the first `returned`.
pub fn sort(points: &mut [Point], desc: bool, returned: usize) {
    let cmp = |a: &Point, b: &Point| if desc { -cmp_asc(a, b) } else { cmp_asc(a, b) };
    if returned >= points.len() {
        msort(points, &cmp);
    } else if returned > 0 {
        pqsort(points, &cmp, 0, returned - 1);
    }
}

/// glibc 2.39's `qsort` (`msort.c`): a top-down merge sort, the first half
/// of `n / 2`; on a tie the left one goes first. The same order as any
/// stable sort for a consistent comparator, and still glibc's with a NaN
/// distance, which isn't one.
fn msort<T: Copy>(v: &mut [T], cmp: &impl Fn(&T, &T) -> i32) {
    fn go<T: Copy>(v: &mut [T], tmp: &mut Vec<T>, cmp: &impl Fn(&T, &T) -> i32) {
        let n = v.len();
        if n <= 1 {
            return;
        }
        let n1 = n / 2;
        go(&mut v[..n1], tmp, cmp);
        go(&mut v[n1..], tmp, cmp);
        tmp.clear();
        let (mut i, mut j) = (0, n1);
        while i < n1 && j < n {
            if cmp(&v[i], &v[j]) <= 0 {
                tmp.push(v[i]);
                i += 1;
            } else {
                tmp.push(v[j]);
                j += 1;
            }
        }
        tmp.extend_from_slice(&v[i..n1]);
        // What's left of the second half is in place already.
        let done = tmp.len();
        v[..done].copy_from_slice(tmp);
    }
    let mut tmp = Vec::with_capacity(v.len());
    go(v, &mut tmp, cmp);
}

/// Redis's `pqsort.c` (NetBSD's Bentley-McIlroy quicksort, sorting only
/// the parts that overlap elements `lrange..=rrange`), on indices instead
/// of bytes. Not stable: ported as it is, so ties land where Redis's do.
fn pqsort<T: Copy>(v: &mut [T], cmp: &impl Fn(&T, &T) -> i32, lrange: usize, rrange: usize) {
    fn med3<T>(v: &[T], a: usize, b: usize, c: usize, cmp: &impl Fn(&T, &T) -> i32) -> usize {
        if cmp(&v[a], &v[b]) < 0 {
            if cmp(&v[b], &v[c]) < 0 {
                b
            } else if cmp(&v[a], &v[c]) < 0 {
                c
            } else {
                a
            }
        } else if cmp(&v[b], &v[c]) > 0 {
            b
        } else if cmp(&v[a], &v[c]) < 0 {
            a
        } else {
            c
        }
    }
    /// `vecswap`: `n` elements from `a` with `n` from `b`.
    fn vecswap<T>(v: &mut [T], a: usize, b: usize, n: usize) {
        for k in 0..n {
            v.swap(a + k, b + k);
        }
    }
    // The C's byte pointers become indices: `lrange` the first element's
    // first byte, `rrange` the last one's last byte; a part [l, r] is
    // skipped if both ends are before it or both after.
    let outside = |l: usize, n: usize| (lrange < l && rrange < l) || (lrange >= l + n && rrange >= l + n);
    let mut a = 0;
    let mut n = v.len();
    loop {
        if n < 7 {
            for pm in a + 1..a + n {
                let mut pl = pm;
                while pl > a && cmp(&v[pl - 1], &v[pl]) > 0 {
                    v.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return;
        }
        let mut pm = a + n / 2;
        if n > 7 {
            let mut pl = a;
            let mut pn = a + n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(v, pl, pl + d, pl + 2 * d, cmp);
                pm = med3(v, pm - d, pm, pm + d, cmp);
                pn = med3(v, pn - 2 * d, pn - d, pn, cmp);
            }
            pm = med3(v, pl, pm, pn, cmp);
        }
        v.swap(a, pm);
        let (mut pa, mut pb) = (a + 1, a + 1);
        // `pc` may go one below `a + 1` (to `a`): kept as a signed index.
        let (mut pc, mut pd) = ((a + n - 1) as isize, a + n - 1);
        loop {
            while pb as isize <= pc {
                let r = cmp(&v[pb], &v[a]);
                if r > 0 {
                    break;
                }
                if r == 0 {
                    v.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
            }
            while pb as isize <= pc {
                let r = cmp(&v[pc as usize], &v[a]);
                if r < 0 {
                    break;
                }
                if r == 0 {
                    v.swap(pc as usize, pd);
                    pd -= 1;
                }
                pc -= 1;
            }
            if pb as isize > pc {
                break;
            }
            v.swap(pb, pc as usize);
            pb += 1;
            pc -= 1;
        }
        let pn = a + n;
        let r = (pa - a).min(pb - pa);
        vecswap(v, a, pb - r, r);
        let r = (pd as isize - pc) as usize;
        let r2 = r.min(pn - pd - 1);
        vecswap(v, pb, pn - r2, r2);
        let left = pb - pa;
        if left > 1 && !outside(a, left) {
            pqsort_part(v, cmp, a, left, lrange, rrange);
        }
        if r > 1 {
            a = pn - r;
            n = r;
            if !outside(a, n) {
                continue;
            }
        }
        return;
    }
}

/// [`pqsort`]'s recursion on `v[a..a + n]` (the same ranges, the whole
/// slice's indices).
fn pqsort_part<T: Copy>(v: &mut [T], cmp: &impl Fn(&T, &T) -> i32, a: usize, n: usize, lrange: usize, rrange: usize) {
    // Shifted so the part starts at 0; the ranges shift with it (they may
    // fall before it: then they're "before", as in the C).
    let part = &mut v[a..a + n];
    let shift = |x: usize| x.checked_sub(a);
    match (shift(lrange), shift(rrange)) {
        (Some(l), Some(r)) => pqsort(part, cmp, l, r),
        // `lrange` before the part, `rrange` in or after it.
        // Then every part has `lrange` before it, and is skipped only if
        // `rrange` is before it too: as with `lrange` 0, for which
        // `lrange < l` holds for every part but the first, which `rrange`
        // (in or past it) keeps anyway.
        (None, Some(r)) => pqsort(part, cmp, 0, r),
        // Both before: the caller skips such a part.
        _ => {}
    }
}

/// `fixedpoint_d2string(..., 4)`: a distance with 4 decimals, rounded as
/// `llrint` (to even); out of `long long`'s range (or NaN), x86's
/// `LLONG_MIN`.
pub fn distance_text(d: f64) -> String {
    if d == 0.0 {
        return "0.0000".to_string();
    }
    let x = (d * 10000.0).round_ties_even();
    let s = cvttsd2si(x);
    let sign = if s < 0 { "-" } else { "" };
    let value = s.unsigned_abs();
    format!("{sign}{}.{:04}", value / 10000, value % 10000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn ordering(a: &Point, b: &Point) -> Ordering {
        a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal)
    }

    #[test]
    fn interleave_round_trips() {
        for (x, y) in [(0u32, 0u32), (1, 0), (0, 1), (0x3ffffff, 0x3ffffff), (0x1234567, 0x2abcdef), (u32::MAX, 0)] {
            let i = interleave64(x, y);
            assert_eq!(deinterleave64(i), u64::from(x) | (u64::from(y) << 32));
        }
        assert_eq!(interleave64(1, 0), 1);
        assert_eq!(interleave64(0, 1), 2);
    }

    #[test]
    fn scores_as_redis() {
        // GEOADD's scores, seen with Redis 8.10.2 (ZRANGE ... WITHSCORES).
        assert_eq!(point_score(-180.0, -85.05112878), 0.0);
        assert_eq!(point_score(0.0, 0.0), 3377699720527872.0);
        assert_eq!(point_score(180.0, 85.05112878), 13510798882111488.0);
        // GEOPOS of a score, as Redis 8.10.2 replied.
        let pos = |s: f64| decode_score(s);
        assert_eq!(pos(3471579339700058.0), [1.806325614452362, 41.235889392604285]);
        assert_eq!(pos(-1.0), [180.0, 85.05112878]);
        assert_eq!(pos(-0.5), [-179.99999731779099, -85.05112751263943]);
        assert_eq!(pos(-9.3e18), [180.0, -85.05112751263943]);
        assert_eq!(pos(f64::NEG_INFINITY), [180.0, -85.05112751263943]);
        assert_eq!(pos(f64::INFINITY), [-179.99999731779099, -85.05112751263943]);
        assert_eq!(pos(1e19), [180.0, 85.05112878]);
        assert_eq!(pos(2e19), [-179.99999731779099, -85.05112751263943]);
        assert_eq!(pos(4503599627370496.0), [-179.99999731779099, 85.05112878]);
        assert_eq!(pos(4503599627370495.5), [179.99999731779099, 85.05112751263943]);
        assert_eq!(pos(9007199254740992.0), [180.0, -85.05112751263943]);
        assert_eq!(&geohash_string(3471579339700058.0), b"sp3694g26q0");
        assert_eq!(&geohash_string(-1.0), b"bp05b5048p0");
        assert_eq!(&geohash_string(0.0), b"00bh0hbj200");
        assert_eq!(&geohash_string(4503599627370495.5), b"zzpgzgpfxz0");
    }

    #[test]
    fn casts_as_x86() {
        assert_eq!(score_bits(-1.0), u64::MAX);
        assert_eq!(score_bits(-0.5), 0);
        assert_eq!(score_bits(-1e19), 1 << 63);
        assert_eq!(score_bits(f64::NAN), 0);
        assert_eq!(score_bits(f64::INFINITY), 0);
        assert_eq!(score_bits(1e19), 10_000_000_000_000_000_000);
        assert_eq!(score_bits(18446744073709549568.0), 18446744073709549568);
        assert_eq!(score_bits(18446744073709551616.0), 0);
        assert_eq!(score_to_i64(-1.5), -1);
        assert_eq!(score_to_i64(1e19), i64::MIN);
        assert_eq!(score_to_i64(f64::NEG_INFINITY), i64::MIN);
    }

    #[test]
    fn distances_as_redis() {
        // GEODIST of (180, 85.05) and (-180, -85.05), and of it and (0, 0)
        // in km, from Redis 8.10.2.
        let a = decode_score(13510798882111488.0);
        let b = decode_score(0.0);
        let c = decode_score(3377699720527872.0);
        assert_eq!(distance_text(distance(a[0], a[1], b[0], b[1])), "18919844.5891");
        assert_eq!(distance_text(distance(a[0], a[1], c[0], c[1]) / 1000.0), "10560.8115");
        assert_eq!(distance_text(0.0), "0.0000");
        assert_eq!(distance_text(-0.0), "0.0000");
        assert_eq!(distance_text(0.00005), "0.0000", "llrint: half to even");
        // Exact ties once scaled: to the even neighbour.
        assert_eq!(distance_text(0.09375), "0.0938");
        assert_eq!(distance_text(0.03125), "0.0312");
        assert_eq!(distance_text(0.00015), "0.0001", "1.4999999999999998 scaled");
        assert_eq!(distance_text(0.7775), "0.7775");
        assert_eq!(distance_text(12.5), "12.5000");
        assert_eq!(distance_text(-3.25), "-3.2500");
        assert_eq!(distance_text(f64::NAN), "-922337203685477.5808");
        assert_eq!(distance_text(1e30), "-922337203685477.5808");
    }

    #[test]
    fn steps_by_radius() {
        assert_eq!(estimate_steps_by_radius(0.0, 0.0), 26);
        assert_eq!(estimate_steps_by_radius(1.0, 0.0), 24);
        assert_eq!(estimate_steps_by_radius(1000.0, 0.0), 14);
        assert_eq!(estimate_steps_by_radius(1000.0, 70.0), 13);
        assert_eq!(estimate_steps_by_radius(1000.0, -81.0), 12);
        assert_eq!(estimate_steps_by_radius(1e9, 0.0), 1);
    }

    #[test]
    fn neighbours_move_one_cell() {
        let h = encode_wgs84(10.0, 20.0, 5).unwrap();
        let n = neighbors(h);
        let area = |h| decode(LONG_RANGE, LAT_RANGE, h).unwrap();
        let (c, e, nn) = (area(h), area(n.east), area(n.north));
        assert_eq!(e.longitude.min, c.longitude.max);
        assert_eq!(e.latitude, c.latitude);
        assert_eq!(nn.latitude.min, c.latitude.max);
        assert_eq!(area(n.south_west).longitude.max, c.longitude.min);
        assert_eq!(area(n.south_west).latitude.max, c.latitude.min);
        // West of the first column wraps to the last.
        let w = neighbors(encode_wgs84(-180.0, 0.0, 3).unwrap()).west;
        assert_eq!(area(w).longitude.max, 180.0);
    }

    fn points(dists: &[f64]) -> Vec<Point<'static>> {
        dists.iter().enumerate().map(|(i, &d)| Point { longitude: i as f64, latitude: 0.0, dist: d, score: i as f64, member: b"" }).collect()
    }

    #[test]
    fn msort_is_stable() {
        let mut v = points(&[3.0, 1.0, 3.0, 2.0, 1.0, 0.0, 3.0, 1.0, 2.0]);
        sort(&mut v, false, usize::MAX);
        let order: Vec<f64> = v.iter().map(|p| p.score).collect();
        assert_eq!(order, [5.0, 1.0, 4.0, 7.0, 3.0, 8.0, 0.0, 2.0, 6.0]);
        sort(&mut v, true, usize::MAX);
        let order: Vec<f64> = v.iter().map(|p| p.score).collect();
        assert_eq!(order, [0.0, 2.0, 6.0, 3.0, 8.0, 1.0, 4.0, 7.0, 5.0], "ties keep their order backwards too");
    }

    /// xorshift64 in [0, 1).
    fn unit(seed: &mut u64) -> f64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        (*seed >> 11) as f64 / (1u64 << 53) as f64
    }

    #[test]
    fn a_search_finds_every_point_inside() {
        // Redis's cells cover the area: whatever a scan of every member
        // finds inside, the search finds, in both hemispheres, up to high
        // latitudes, where the box's edges bend most. Only away from the
        // limits: the cells don't wrap over the latitude limits, and a
        // point at longitude 180 or latitude 85.05112878 has an offset of
        // 2^26, past the 52 bits, in no cell (Redis 8.10.2 misses those
        // too; see below and the geo_limits difftest case).
        let mut seed = 99u64;
        for (lon0, lat0) in [(10.0, 70.0), (-120.0, -72.0), (100.0, 60.0), (0.0, 0.5), (-60.0, -40.0), (140.0, -65.0)] {
            let mut z = ZSet::default();
            let names: Vec<String> = (0..2000).map(|i| format!("p{i}")).collect();
            for name in &names {
                let mut lon = lon0 + (unit(&mut seed) - 0.5) * 30.0_f64;
                if lon >= LONG_MAX {
                    lon -= 360.0;
                }
                let lat = (lat0 + (unit(&mut seed) - 0.5) * 10.0_f64).clamp(LAT_MIN, LAT_MAX - 1e-6);
                z.insert(name.as_bytes(), point_score(lon, lat));
            }
            for _ in 0..300 {
                let xy = [(lon0 + (unit(&mut seed) - 0.5) * 20.0).clamp(LONG_MIN, LONG_MAX - 1e-6), (lat0 + (unit(&mut seed) - 0.5) * 6.0).clamp(LAT_MIN, LAT_MAX - 1e-6)];
                let size = 10f64.powf(3.0 + unit(&mut seed) * 2.5);
                let extent = if unit(&mut seed) < 0.5 {
                    Extent::Radius(size)
                } else {
                    Extent::Box { width: size * (0.2 + unit(&mut seed) * 2.0), height: size * (0.2 + unit(&mut seed) * 2.0) }
                };
                let shape = Shape { xy, conversion: 1.0, extent };
                let mut found: Vec<&[u8]> = search(&z, &shape, 0).iter().map(|p| p.member).collect();
                let mut inside: Vec<&[u8]> = z.iter(0, usize::MAX, false).filter(|(_, s)| within(&shape, *s).is_some()).map(|(m, _)| m).collect();
                found.sort();
                inside.sort();
                assert_eq!(found, inside, "around {xy:?}, {extent:?}");
            }
        }
    }

    #[test]
    fn a_point_at_the_limits_is_never_found() {
        // As Redis 8.10.2: past the 52 bits, outside every cell.
        let mut z = ZSet::default();
        z.insert(b"east", point_score(180.0, 10.0));
        z.insert(b"north", point_score(10.0, LAT_MAX));
        z.insert(b"in", point_score(179.9, 10.0));
        assert!(point_score(180.0, 10.0) >= (1u64 << 52) as f64);
        let shape = Shape { xy: [179.95, 10.0], conversion: 1000.0, extent: Extent::Radius(50.0) };
        let found: Vec<&[u8]> = search(&z, &shape, 0).iter().map(|p| p.member).collect();
        assert_eq!(found, [b"in"]);
        let shape = Shape { xy: [10.0, 85.0], conversion: 1000.0, extent: Extent::Radius(100.0) };
        assert!(search(&z, &shape, 0).is_empty());
    }

    #[test]
    fn pqsort_sorts_the_first_ones() {
        let mut seed = 7u64;
        for n in 1..300usize {
            for k in [1, 2, 3, n / 2, n.saturating_sub(1)] {
                if k == 0 || k >= n {
                    continue;
                }
                let dists: Vec<f64> = (0..n)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        (seed % 50) as f64
                    })
                    .collect();
                let mut v = points(&dists);
                sort(&mut v, false, k);
                let mut all = dists.clone();
                all.sort_by(f64::total_cmp);
                let got: Vec<f64> = v[..k].iter().map(|p| p.dist).collect();
                assert_eq!(got, all[..k], "n {n} k {k}");
                assert!(v[..k].windows(2).all(|w| ordering(&w[0], &w[1]) != Ordering::Greater));
                let mut scores: Vec<f64> = v.iter().map(|p| p.score).collect();
                scores.sort_by(f64::total_cmp);
                assert_eq!(scores, (0..n).map(|i| i as f64).collect::<Vec<_>>(), "a permutation");
            }
        }
    }
}
