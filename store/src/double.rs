//! Doubles as Redis 8.10 reads and writes them: the scores of sorted sets.
//!
//! Reading. Redis 8 reads a double with `fast_float_strtod`: a port of
//! fast_float that handles up to 19 significant digits with a decimal
//! exponent in [-342, 308], and falls back to glibc's `strtod` for the
//! rest (hex floats, longer mantissas, wider exponents, `inf`, leading
//! white space). Both are correctly rounded, so the value is Rust's
//! `f64::from_str`'s; what differs is the range error. Only `strtod`
//! reports one (`ERANGE`), so `ZADD z 9e308 m` stores `inf` and
//! `ZADD z 1e400 m` is refused.
//! - [`parse`] is `string2d` (scores): the whole string, no leading white
//!   space, not NaN, and refused on `ERANGE` with an infinite or zero result.
//! - [`parse_bound`] is what `zslParseRange` does with a range bound: it
//!   reads up to the first NUL byte (the C string ends there), `strtod`
//!   skips white space first, an empty string is 0 (`strtod` reads nothing
//!   and stops on the NUL), and range errors don't matter.
//!
//! Writing. [`write`] is `d2string`: `nan`, `inf`, `-inf`, `-0`, an integer
//! below 2^62 as one, anything else by `fpconv_dtoa`, a Grisu2 (Florian
//! Loitsch's algorithm, night-shift's C version, ported here as it is).
//! Grisu2 round-trips but isn't always the shortest, and has its own rules
//! for when to use an exponent, so Rust's formatting won't do.

/// What [`read`] made of a string.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Read {
    /// A value, and whether glibc's `strtod` would have set `ERANGE` with
    /// an infinite or zero result (only when Redis falls back to it).
    Value(f64, bool),
    /// Not a number (NaN included: both callers refuse it).
    Invalid,
}

/// Reads all of `s`, which has no leading white space, as Redis 8's
/// `fast_float_strtod` would.
fn read(s: &[u8]) -> Read {
    let (neg, body) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    if body.eq_ignore_ascii_case(b"inf") || body.eq_ignore_ascii_case(b"infinity") {
        return Read::Value(if neg { f64::NEG_INFINITY } else { f64::INFINITY }, false);
    }
    if let [b'0', b'x' | b'X', hex @ ..] = body {
        return hex_float(neg, hex);
    }
    decimal(neg, body)
}

/// `[digits][.digits][(e|E)[+|-]digits]` with a digit before the exponent.
fn decimal(neg: bool, s: &[u8]) -> Read {
    let mut i = 0;
    let digits = |i: &mut usize| {
        let from = *i;
        while *i < s.len() && s[*i].is_ascii_digit() {
            *i += 1;
        }
        from..*i
    };
    let int = digits(&mut i);
    let frac = if i < s.len() && s[i] == b'.' {
        i += 1;
        digits(&mut i)
    } else {
        i..i
    };
    if int.is_empty() && frac.is_empty() {
        return Read::Invalid;
    }
    let mut exp: i64 = 0;
    if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        i += 1;
        let neg_exp = match s.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let e = digits(&mut i);
        if e.is_empty() {
            return Read::Invalid;
        }
        // As fast_float: digits stop counting once past 2^28.
        for &d in &s[e] {
            if exp < 0x1000_0000 {
                exp = exp * 10 + i64::from(d - b'0');
            }
        }
        if neg_exp {
            exp = -exp;
        }
    }
    if i != s.len() {
        return Read::Invalid;
    }
    // The grammar is a subset of what Rust reads, and ASCII.
    let text = std::str::from_utf8(s).expect("ASCII");
    let value: f64 = text.parse().expect("a decimal Rust reads");
    let value = if neg { -value } else { value };
    // fast_float's own path: at most 19 digits once the leading zeros are
    // skipped, and the exponent, counted from the last digit, in range.
    let all = int.len() + frac.len();
    let significant = if all > 19 {
        let leading = s[..i].iter().take_while(|&&c| c == b'0' || c == b'.').filter(|&&c| c == b'0').count();
        all - leading
    } else {
        all
    };
    let exponent = exp - frac.len() as i64;
    let fast = significant <= 19 && (-342..=308).contains(&exponent);
    let nonzero = s[int].iter().chain(&s[frac]).any(|&d| d != b'0');
    let erange = !fast && (value.is_infinite() || (value == 0.0 && nonzero));
    Read::Value(value, erange)
}

/// The hex digits after `0x`: `[hex][.hex][(p|P)[+|-]digits]`, as glibc's
/// `strtod` reads them, rounded to nearest, ties to even.
fn hex_float(neg: bool, s: &[u8]) -> Read {
    let (mut mant, mut exp2, mut sticky, mut any, mut seen_point) = (0u64, 0i64, false, false, false);
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        if c == b'.' && !seen_point {
            seen_point = true;
        } else if let Some(d) = char::from(c).to_digit(16) {
            any = true;
            if mant >> 60 == 0 {
                mant = (mant << 4) | u64::from(d);
                if seen_point {
                    exp2 -= 4;
                }
            } else {
                // Past 64 bits: only whether anything is left matters.
                sticky |= d != 0;
                if !seen_point {
                    exp2 += 4;
                }
            }
        } else {
            break;
        }
        i += 1;
    }
    if !any {
        // `strtod` reads the `0` and stops at the `x`.
        return Read::Invalid;
    }
    if i < s.len() {
        if s[i] != b'p' && s[i] != b'P' {
            return Read::Invalid;
        }
        i += 1;
        let neg_exp = match s.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let digits = &s[i..];
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Read::Invalid;
        }
        let e = digits.iter().fold(0i64, |e, d| (e * 10 + i64::from(d - b'0')).min(1 << 40));
        exp2 += if neg_exp { -e } else { e };
    }
    let (value, erange) = from_bits(mant, exp2, sticky);
    Read::Value(if neg { -value } else { value }, erange)
}

/// The double nearest `(mant + f) * 2^exp2`, `0 < f < 1` exactly when
/// `sticky`; and whether that's a range error (zero or infinite).
fn from_bits(mant: u64, exp2: i64, sticky: bool) -> (f64, bool) {
    if mant == 0 {
        return (0.0, false);
    }
    let lz = mant.leading_zeros();
    let m = mant << lz;
    let e = exp2 - i64::from(lz);
    // The exponent of the leading bit; below -1022 the subnormals keep fewer.
    let top = e + 63;
    let shift = 11 + (-1022 - top).max(0);
    let (kept, up) = if shift > 64 {
        (0, false)
    } else if shift == 64 {
        (0, m > 1 << 63 || (m == 1 << 63 && sticky))
    } else {
        let kept = m >> shift;
        let rem = m & ((1u64 << shift) - 1);
        let half = 1u64 << (shift - 1);
        (kept, rem > half || (rem == half && (sticky || kept & 1 == 1)))
    };
    let mut kept = kept + u64::from(up);
    let mut q = e + shift;
    if kept == 1 << 53 {
        kept >>= 1;
        q += 1;
    }
    if kept == 0 {
        return (0.0, true);
    }
    if kept >= 1 << 52 {
        let biased = q + 52 + 1023;
        if biased >= 2047 {
            return (f64::INFINITY, true);
        }
        (f64::from_bits(((biased as u64) << 52) | (kept & ((1 << 52) - 1))), false)
    } else {
        debug_assert_eq!(q, -1074, "a subnormal");
        (f64::from_bits(kept), false)
    }
}

/// C's `isspace` in the C locale.
fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// A score, as Redis's `string2d` reads it; `None` if it isn't one
/// ("value is not a valid float").
pub fn parse(s: &[u8]) -> Option<f64> {
    // `string2d` refuses a leading space, which `strtod` would skip: the
    // grammar of `read` has no white space anywhere.
    match read(s) {
        Read::Value(v, false) if !v.is_nan() => Some(v),
        _ => None,
    }
}

/// A range bound after its `(`, as `zslParseRange` reads it; `None` if
/// it isn't one.
pub fn parse_bound(s: &[u8]) -> Option<f64> {
    let s = &s[..s.iter().position(|&c| c == 0).unwrap_or(s.len())];
    if s.is_empty() {
        return Some(0.0);
    }
    let start = s.iter().position(|&c| !is_c_space(c))?;
    match read(&s[start..]) {
        Read::Value(v, _) if !v.is_nan() => Some(v),
        _ => None,
    }
}

/// Writes `d` as Redis's `d2string` does.
pub fn write(d: f64, out: &mut Vec<u8>) {
    if d.is_nan() {
        out.extend_from_slice(b"nan");
    } else if d.is_infinite() {
        out.extend_from_slice(if d < 0.0 { b"-inf" } else { b"inf" });
    } else if d == 0.0 {
        out.extend_from_slice(if d.is_sign_negative() { b"-0" } else { b"0" });
    } else if let Some(n) = as_integer(d) {
        out.extend_from_slice(n.to_string().as_bytes());
    } else {
        fpconv_dtoa(d, out);
    }
}

/// [`write`], as a new string.
pub fn to_bytes(d: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    write(d, &mut out);
    out
}

/// Redis's `double2ll`: `d` as an integer, if it's one within ±2^62.
fn as_integer(d: f64) -> Option<i64> {
    let limit = (i64::MAX / 2) as f64;
    if !(-limit..=limit).contains(&d) {
        return None;
    }
    let n = d as i64;
    (n as f64 == d).then_some(n)
}

// fpconv_dtoa, as night-shift wrote it.

#[derive(Clone, Copy)]
struct Fp {
    frac: u64,
    exp: i32,
}

const FRACMASK: u64 = 0x000F_FFFF_FFFF_FFFF;
const EXPMASK: u64 = 0x7FF0_0000_0000_0000;
const HIDDENBIT: u64 = 0x0010_0000_0000_0000;
const SIGNMASK: u64 = 0x8000_0000_0000_0000;
const EXPBIAS: i32 = 1023 + 52;

const TENS: [u64; 20] = [
    10000000000000000000,
    1000000000000000000,
    100000000000000000,
    10000000000000000,
    1000000000000000,
    100000000000000,
    10000000000000,
    1000000000000,
    100000000000,
    10000000000,
    1000000000,
    100000000,
    10000000,
    1000000,
    100000,
    10000,
    1000,
    100,
    10,
    1,
];

const NPOWERS: i32 = 87;
const STEPPOWERS: i32 = 8;
const FIRSTPOWER: i32 = -348;
const EXPMAX: i32 = -32;
const EXPMIN: i32 = -60;

#[rustfmt::skip]
const POWERS_TEN: [(u64, i32); 87] = [
    (18054884314459144840, -1220), (13451937075301367670, -1193),
    (10022474136428063862, -1166), (14934650266808366570, -1140),
    (11127181549972568877, -1113), (16580792590934885855, -1087),
    (12353653155963782858, -1060), (18408377700990114895, -1034),
    (13715310171984221708, -1007), (10218702384817765436, -980),
    (15227053142812498563, -954),  (11345038669416679861, -927),
    (16905424996341287883, -901),  (12595523146049147757, -874),
    (9384396036005875287,  -847),  (13983839803942852151, -821),
    (10418772551374772303, -794),  (15525180923007089351, -768),
    (11567161174868858868, -741),  (17236413322193710309, -715),
    (12842128665889583758, -688),  (9568131466127621947,  -661),
    (14257626930069360058, -635),  (10622759856335341974, -608),
    (15829145694278690180, -582),  (11793632577567316726, -555),
    (17573882009934360870, -529),  (13093562431584567480, -502),
    (9755464219737475723,  -475),  (14536774485912137811, -449),
    (10830740992659433045, -422),  (16139061738043178685, -396),
    (12024538023802026127, -369),  (17917957937422433684, -343),
    (13349918974505688015, -316),  (9946464728195732843,  -289),
    (14821387422376473014, -263),  (11042794154864902060, -236),
    (16455045573212060422, -210),  (12259964326927110867, -183),
    (18268770466636286478, -157),  (13611294676837538539, -130),
    (10141204801825835212, -103),  (15111572745182864684, -77),
    (11258999068426240000, -50),   (16777216000000000000, -24),
    (12500000000000000000,   3),   (9313225746154785156,   30),
    (13877787807814456755,  56),   (10339757656912845936,  83),
    (15407439555097886824, 109),   (11479437019748901445, 136),
    (17105694144590052135, 162),   (12744735289059618216, 189),
    (9495567745759798747,  216),   (14149498560666738074, 242),
    (10542197943230523224, 269),   (15709099088952724970, 295),
    (11704190886730495818, 322),   (17440603504673385349, 348),
    (12994262207056124023, 375),   (9681479787123295682,  402),
    (14426529090290212157, 428),   (10748601772107342003, 455),
    (16016664761464807395, 481),   (11933345169920330789, 508),
    (17782069995880619868, 534),   (13248674568444952270, 561),
    (9871031767461413346,  588),   (14708983551653345445, 614),
    (10959046745042015199, 641),   (16330252207878254650, 667),
    (12166986024289022870, 694),   (18130221999122236476, 720),
    (13508068024458167312, 747),   (10064294952495520794, 774),
    (14996968138956309548, 800),   (11173611982879273257, 827),
    (16649979327439178909, 853),   (12405201291620119593, 880),
    (9242595204427927429,  907),   (13772540099066387757, 933),
    (10261342003245940623, 960),   (15290591125556738113, 986),
    (11392378155556871081, 1013),  (16975966327722178521, 1039),
    (12648080533535911531, 1066),
];

fn find_cachedpow10(exp: i32) -> (Fp, i32) {
    let one_log_ten = 0.30102999566398114;
    let approx = (f64::from(-(exp + NPOWERS)) * one_log_ten) as i32;
    let mut idx = (approx - FIRSTPOWER) / STEPPOWERS;
    loop {
        let current = exp + POWERS_TEN[idx as usize].1 + 64;
        if current < EXPMIN {
            idx += 1;
            continue;
        }
        if current > EXPMAX {
            idx -= 1;
            continue;
        }
        let (frac, exp) = POWERS_TEN[idx as usize];
        return (Fp { frac, exp }, FIRSTPOWER + idx * STEPPOWERS);
    }
}

fn build_fp(d: f64) -> Fp {
    let bits = d.to_bits();
    let mut fp = Fp { frac: bits & FRACMASK, exp: ((bits & EXPMASK) >> 52) as i32 };
    if fp.exp != 0 {
        fp.frac += HIDDENBIT;
        fp.exp -= EXPBIAS;
    } else {
        fp.exp = -EXPBIAS + 1;
    }
    fp
}

fn normalize(fp: &mut Fp) {
    while fp.frac & HIDDENBIT == 0 {
        fp.frac <<= 1;
        fp.exp -= 1;
    }
    let shift = 64 - 52 - 1;
    fp.frac <<= shift;
    fp.exp -= shift;
}

fn normalized_boundaries(fp: Fp) -> (Fp, Fp) {
    let mut upper = Fp { frac: (fp.frac << 1) + 1, exp: fp.exp - 1 };
    while upper.frac & (HIDDENBIT << 1) == 0 {
        upper.frac <<= 1;
        upper.exp -= 1;
    }
    let u_shift = 64 - 52 - 2;
    upper.frac <<= u_shift;
    upper.exp -= u_shift;
    let l_shift = if fp.frac == HIDDENBIT { 2 } else { 1 };
    let mut lower = Fp { frac: (fp.frac << l_shift) - 1, exp: fp.exp - l_shift };
    lower.frac <<= lower.exp - upper.exp;
    lower.exp = upper.exp;
    (lower, upper)
}

fn multiply(a: Fp, b: Fp) -> Fp {
    let lomask = 0x0000_0000_FFFF_FFFFu64;
    let ah_bl = (a.frac >> 32).wrapping_mul(b.frac & lomask);
    let al_bh = (a.frac & lomask).wrapping_mul(b.frac >> 32);
    let al_bl = (a.frac & lomask).wrapping_mul(b.frac & lomask);
    let ah_bh = (a.frac >> 32).wrapping_mul(b.frac >> 32);
    let mut tmp = (ah_bl & lomask).wrapping_add(al_bh & lomask).wrapping_add(al_bl >> 32);
    // Round up.
    tmp = tmp.wrapping_add(1 << 31);
    Fp {
        frac: ah_bh.wrapping_add(ah_bl >> 32).wrapping_add(al_bh >> 32).wrapping_add(tmp >> 32),
        exp: a.exp + b.exp + 64,
    }
}

fn round_digit(digits: &mut [u8], ndigits: usize, delta: u64, mut rem: u64, kappa: u64, frac: u64) {
    while rem < frac
        && delta.wrapping_sub(rem) >= kappa
        && (rem.wrapping_add(kappa) < frac || frac.wrapping_sub(rem) > rem.wrapping_add(kappa).wrapping_sub(frac))
    {
        digits[ndigits - 1] -= 1;
        rem = rem.wrapping_add(kappa);
    }
}

fn generate_digits(fp: Fp, upper: Fp, lower: Fp, digits: &mut [u8; 18], k: &mut i32) -> usize {
    let wfrac = upper.frac.wrapping_sub(fp.frac);
    let mut delta = upper.frac.wrapping_sub(lower.frac);
    let one = Fp { frac: 1u64 << -upper.exp, exp: upper.exp };
    let mut part1 = upper.frac >> -one.exp;
    let mut part2 = upper.frac & (one.frac - 1);
    let mut idx = 0;
    let mut kappa = 10;
    // 1000000000
    let mut divp = 10;
    while kappa > 0 {
        let div = TENS[divp];
        let digit = part1 / div;
        if digit != 0 || idx != 0 {
            digits[idx] = digit as u8 + b'0';
            idx += 1;
        }
        part1 -= digit * div;
        kappa -= 1;
        let tmp = (part1 << -one.exp).wrapping_add(part2);
        if tmp <= delta {
            *k += kappa;
            round_digit(digits, idx, delta, tmp, div << -one.exp, wfrac);
            return idx;
        }
        divp += 1;
    }
    // 10
    let mut unit = 18;
    loop {
        part2 = part2.wrapping_mul(10);
        delta = delta.wrapping_mul(10);
        kappa -= 1;
        let digit = part2 >> -one.exp;
        if digit != 0 || idx != 0 {
            digits[idx] = digit as u8 + b'0';
            idx += 1;
        }
        part2 &= one.frac - 1;
        if part2 < delta {
            *k += kappa;
            round_digit(digits, idx, delta, part2, one.frac, wfrac.wrapping_mul(TENS[unit]));
            return idx;
        }
        unit -= 1;
    }
}

fn grisu2(d: f64, digits: &mut [u8; 18]) -> (usize, i32) {
    let mut w = build_fp(d);
    let (lower, upper) = normalized_boundaries(w);
    normalize(&mut w);
    let (cp, k) = find_cachedpow10(upper.exp);
    let w = multiply(w, cp);
    let mut upper = multiply(upper, cp);
    let mut lower = multiply(lower, cp);
    lower.frac = lower.frac.wrapping_add(1);
    upper.frac = upper.frac.wrapping_sub(1);
    let mut kk = -k;
    let n = generate_digits(w, upper, lower, digits, &mut kk);
    (n, kk)
}

fn emit_digits(digits: &[u8], mut ndigits: usize, out: &mut Vec<u8>, k: i32, neg: bool) {
    let nd = ndigits as i32;
    let mut exp = (k + nd - 1).abs();
    // Write a plain integer.
    if k >= 0 && exp < nd + 7 {
        out.extend_from_slice(&digits[..ndigits]);
        out.resize(out.len() + k as usize, b'0');
        return;
    }
    // Write a decimal without scientific notation.
    if k < 0 && (k > -7 || exp < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            // Below 1: a leading zero.
            out.extend_from_slice(b"0.");
            out.resize(out.len() + (-offset) as usize, b'0');
            out.extend_from_slice(&digits[..ndigits]);
        } else {
            let offset = offset as usize;
            out.extend_from_slice(&digits[..offset]);
            out.push(b'.');
            out.extend_from_slice(&digits[offset..ndigits]);
        }
        return;
    }
    // Write a decimal with scientific notation.
    ndigits = ndigits.min(18 - usize::from(neg));
    out.push(digits[0]);
    if ndigits > 1 {
        out.push(b'.');
        out.extend_from_slice(&digits[1..ndigits]);
    }
    out.push(b'e');
    out.push(if k + ndigits as i32 - 1 < 0 { b'-' } else { b'+' });
    let mut cent = 0;
    if exp > 99 {
        cent = exp / 100;
        out.push(cent as u8 + b'0');
        exp -= cent * 100;
    }
    if exp > 9 {
        let dec = exp / 10;
        out.push(dec as u8 + b'0');
        exp -= dec * 10;
    } else if cent != 0 {
        out.push(b'0');
    }
    out.push((exp % 10) as u8 + b'0');
}

fn fpconv_dtoa(d: f64, out: &mut Vec<u8>) {
    let bits = d.to_bits();
    let neg = bits & SIGNMASK != 0;
    if neg {
        out.push(b'-');
    }
    // filter_special
    if d == 0.0 {
        out.push(b'0');
        return;
    }
    if bits & EXPMASK == EXPMASK {
        out.extend_from_slice(if bits & FRACMASK != 0 { b"nan" } else { b"inf" });
        return;
    }
    let mut digits = [0u8; 18];
    let (n, k) = grisu2(d, &mut digits);
    emit_digits(&digits, n, out, k, neg);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(d: f64) -> String {
        String::from_utf8(to_bytes(d)).unwrap()
    }

    #[test]
    fn writes_as_redis() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-17.0, "-17"),
            (1.5, "1.5"),
            (0.1, "0.1"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (0.000001, "0.000001"),
            (123456.789, "123456.789"),
            (4611686018427387904.0, "4611686018427387904"),
            (4611686018427387904.0 * 2.0, "9223372036854776000"),
            (1e21, "1e+21"),
            (1e20, "1e+20"),
            (123456789012345680000.0, "123456789012345680000"),
            (1e22, "1e+22"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (5e-324, "5e-324"),
            (2.2250738585072014e-308, "2.2250738585072014e-308"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            (3.25159, "3.25159"),
            (-0.5, "-0.5"),
            (1e100, "1e+100"),
            (1.2345e-100, "1.2345e-100"),
        ];
        for &(d, want) in cases {
            assert_eq!(w(d), want, "{d:e}");
        }
    }

    #[test]
    fn written_reads_back() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let d = f64::from_bits(x);
            if d.is_nan() {
                continue;
            }
            let s = to_bytes(d);
            let back = parse(&s).or_else(|| parse_bound(&s)).unwrap_or_else(|| panic!("{}", String::from_utf8_lossy(&s)));
            assert_eq!(back.to_bits(), d.to_bits(), "{}", String::from_utf8_lossy(&s));
        }
    }

    #[test]
    fn reads_as_redis() {
        let inf = f64::INFINITY;
        let ok: &[(&str, f64)] = &[
            ("1", 1.0),
            ("-1.5", -1.5),
            ("+.5", 0.5),
            ("5.", 5.0),
            ("1e3", 1000.0),
            ("1E+3", 1000.0),
            ("inf", inf),
            ("-INF", -inf),
            ("+Infinity", inf),
            ("0x10", 16.0),
            ("0x1.8p1", 3.0),
            ("-0X.8", -0.5),
            ("0x1p-1074", 5e-324),
            // A tie at the 53rd bit, then a 1 past the 16 hex digits kept:
            // rounded up; without it, to even.
            ("0x1.0000000000000801p0", 1.0000000000000002),
            ("0x1.00000000000008p0", 1.0),
            ("0x1.00000000000018p0", 1.0000000000000004),
            // fast_float's own range: overflow and underflow aren't errors.
            ("9e308", inf),
            ("1e-340", 0.0),
            ("0e999", 0.0),
            ("1e-310", 1e-310),
            ("00000000000000000000000000001.5", 1.5),
        ];
        for &(s, want) in ok {
            assert_eq!(parse(s.as_bytes()).map(f64::to_bits), Some(want.to_bits()), "{s}");
        }
        let bad = [
            "", " 1", "\t1", "1 ", "abc", "1e", "1e+", ".", "-", "nan", "NaN", "nan(1)", "0x", "0xg", "0x1p", "infinit",
            "infinityy", "1.2.3", "1\x001",
            // strtod's ERANGE, past fast_float's range.
            "1e400", "-1e400", "1e-400", "0x1p99999", "0x1p-1080", "123456789012345678901e300",
        ];
        for s in bad {
            assert_eq!(parse(s.as_bytes()), None, "{s:?}");
        }
    }

    #[test]
    fn reads_bounds_as_redis() {
        let ok: &[(&[u8], f64)] =
            &[(b"", 0.0), (b"\0junk", 0.0), (b"1\0junk", 1.0), (b"  2", 2.0), (b"\n-3", -3.0), (b"1e400", f64::INFINITY), (b"1e-400", 0.0)];
        for &(s, want) in ok {
            assert_eq!(parse_bound(s).map(f64::to_bits), Some(want.to_bits()), "{s:?}");
        }
        for s in [&b" "[..], b"1 ", b"x", b"nan", b"  ", b" \0"] {
            assert_eq!(parse_bound(s), None, "{s:?}");
        }
    }

    /// Compares with Redis's own C code: `SPEEDVAULT_DOUBLE_ORACLE` names a
    /// file of lines `hex(input) TAB parse TAB bound TAB write`, where the
    /// results are `-` (refused) or the double's bits in hex followed by
    /// what `d2string` wrote of it.
    #[test]
    #[ignore]
    fn against_redis() {
        let Ok(path) = std::env::var("SPEEDVAULT_DOUBLE_ORACLE") else { return };
        let text = std::fs::read_to_string(path).unwrap();
        let mut n = 0;
        for line in text.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let input: Vec<u8> = (0..f[0].len()).step_by(2).map(|i| u8::from_str_radix(&f[0][i..i + 2], 16).unwrap()).collect();
            let show = |r: Option<f64>| match r {
                None => "-".to_string(),
                Some(d) => format!("{:016x} {}", d.to_bits(), w(d)),
            };
            assert_eq!(show(parse(&input)), f[1], "parse {:?}", String::from_utf8_lossy(&input));
            assert_eq!(show(parse_bound(&input)), f[2], "bound {:?}", String::from_utf8_lossy(&input));
            n += 1;
        }
        eprintln!("{n} inputs as Redis");
    }
}
