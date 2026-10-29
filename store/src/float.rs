//! `long double` as Redis 7.0 on x86_64 uses it for `INCRBYFLOAT` and
//! `HINCRBYFLOAT`: the x87's 80-bit extended format (a 64-bit significand,
//! exponents from 2^-16445, subnormals included, to 2^16383), emulated so
//! that every platform replies what a Redis on x86_64 does. Reading is
//! glibc's `strtold` (correctly rounded) behind Redis's `string2ld`; adding
//! is the FPU's (round to nearest, ties to even); writing is `%.17Lf` with
//! Redis's trimming (`ld2string`, `LD_STR_HUMAN`).

use std::cmp::Ordering;

/// A finite value, `mant * 2^exp`: normalised (the top bit of `mant` set)
/// except at the smallest exponent, where the subnormals and zero are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LongDouble {
    neg: bool,
    mant: u64,
    exp: i32,
}

/// The exponent of the subnormals: 2^-16382, the smallest normal's, with
/// the 63 bits of fraction.
const MIN_EXP: i32 = -16445;
/// The largest normal's: 2^16383 with the 63 bits of fraction.
const MAX_EXP: i32 = 16320;
/// Redis's `MAX_LONG_DOUBLE_CHARS`: longer strings aren't read.
const MAX_CHARS: usize = 5 * 1024;
const TEN_TO_17: u128 = 100_000_000_000_000_000;

/// What `string2ld` reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Number {
    Finite(LongDouble),
    /// `inf` or `infinity` (a value out of range isn't read at all).
    Infinite,
}

impl LongDouble {
    pub(crate) const ZERO: LongDouble = LongDouble { neg: false, mant: 0, exp: MIN_EXP };

    /// `self + other`; `None` if the sum overflows to infinity.
    pub(crate) fn add(self, other: LongDouble) -> Option<LongDouble> {
        // Order by magnitude: `(exp, mant)` does that for this layout.
        let (a, b) = if (self.exp, self.mant) >= (other.exp, other.mant) { (self, other) } else { (other, self) };
        if b.mant == 0 {
            // x + 0 is x, and -0 + -0 is -0 (round to nearest).
            let neg = if a.mant == 0 { a.neg && b.neg } else { a.neg };
            return Some(LongDouble { neg, ..a });
        }
        // 64 guard bits under `a`'s significand, then a sticky bit for
        // what's shifted out of `b`'s.
        let big = u128::from(a.mant) << 64;
        let d = (a.exp - b.exp) as u32;
        let (small, sticky) = if d >= 128 {
            (0, true)
        } else {
            let s = u128::from(b.mant) << 64;
            (s >> d, d > 0 && s & ((1u128 << d) - 1) != 0)
        };
        let e = a.exp - 64;
        if a.neg == b.neg {
            let (sum, carry) = big.overflowing_add(small);
            if carry {
                return round(a.neg, (sum >> 1) | 1 << 127, e + 1, sticky || sum & 1 != 0);
            }
            round(a.neg, sum, e, sticky)
        } else {
            // With a sticky bit, `b` was a little more than `small`: the
            // difference lies strictly between `big - small - 1` and the
            // next integer. Bits are dropped only when `a` is far larger,
            // so this doesn't underflow.
            let diff = big - small - u128::from(sticky);
            if diff == 0 && !sticky {
                return Some(LongDouble::ZERO);
            }
            round(a.neg, diff, e, sticky)
        }
    }

    /// `%.17Lf`, trailing zeros (and then a trailing `.`) removed, `-0`
    /// written `0`: Redis's `ld2string` in `LD_STR_HUMAN` mode.
    pub(crate) fn write(self, out: &mut Vec<u8>) {
        out.clear();
        let (int, mut frac) = if self.exp >= 0 {
            let mut int = Big::from_u64(self.mant);
            int.shl(self.exp as usize);
            (int, 0)
        } else {
            let f = self.exp.unsigned_abs();
            let (int, frac) = if f >= 64 { (0, self.mant) } else { (self.mant >> f, self.mant & ((1 << f) - 1)) };
            // The fraction times 10^17 fits in 121 bits: rounded to an
            // integer, ties to even, as glibc's printf.
            let p = u128::from(frac) * TEN_TO_17;
            let digits = if f >= 129 {
                0
            } else if f == 128 {
                u128::from(p > 1 << 127)
            } else {
                let kept = p >> f;
                let rem = p & ((1u128 << f) - 1);
                let half = 1u128 << (f - 1);
                kept + u128::from(rem > half || (rem == half && kept & 1 == 1))
            };
            if digits == TEN_TO_17 {
                (Big::from_u64(int + 1), 0)
            } else {
                (Big::from_u64(int), digits as u64)
            }
        };
        if self.neg {
            out.push(b'-');
        }
        int.write_decimal(out);
        if frac != 0 {
            let mut digits = 17;
            while frac % 10 == 0 {
                frac /= 10;
                digits -= 1;
            }
            out.push(b'.');
            let start = out.len();
            out.resize(start + digits, b'0');
            for i in (start..start + digits).rev() {
                out[i] = b'0' + (frac % 10) as u8;
                frac /= 10;
            }
        }
        if out == b"-0" {
            out.remove(0);
        }
    }
}

impl Number {
    /// `self + other`; `None` if it's infinite.
    pub(crate) fn add(self, other: Number) -> Option<LongDouble> {
        match (self, other) {
            (Number::Finite(a), Number::Finite(b)) => a.add(b),
            _ => None,
        }
    }
}

/// The value nearest `(q + f) * 2^e`, `0 <= f < 1` and `f > 0` exactly when
/// `sticky`, ties to even; `None` past the largest. With `sticky`, `q`
/// must have more than 65 bits, so that `f` lies below the rounding bit.
fn round(neg: bool, q: u128, e: i32, sticky: bool) -> Option<LongDouble> {
    let bits = 128 - q.leading_zeros() as i32;
    debug_assert!(!sticky || bits > 65);
    // Drop down to 64 bits, or down to the subnormals' exponent.
    let shift = (bits - 64).max(MIN_EXP.saturating_sub(e));
    let (mut mant, mut exp) = if shift <= 0 {
        ((q << -shift) as u64, e + shift)
    } else {
        let up = if shift > 128 {
            false
        } else if shift == 128 {
            q > 1 << 127 || (q == 1 << 127 && sticky)
        } else {
            let rem = q & ((1u128 << shift) - 1);
            let half = 1u128 << (shift - 1);
            match rem.cmp(&half) {
                Ordering::Greater => true,
                Ordering::Equal => sticky || (q >> shift) & 1 == 1,
                Ordering::Less => false,
            }
        };
        let kept = if shift >= 128 { 0 } else { q >> shift };
        let kept = kept + u128::from(up);
        if kept == 1 << 64 {
            (1 << 63, e + shift + 1)
        } else {
            (kept as u64, e + shift)
        }
    };
    if exp > MAX_EXP {
        return None;
    }
    if mant == 0 {
        (mant, exp) = (0, MIN_EXP);
    }
    Some(LongDouble { neg, mant, exp })
}

/// Reads `s` as Redis's `string2ld` does: `None` for the empty string, one
/// of `MAX_CHARS` bytes or more, one starting with a space, anything
/// `strtold` doesn't read whole (a NUL byte included), NaN, and a value
/// out of range (`strtold`'s `ERANGE` with
/// infinity or zero; a subnormal is fine).
pub(crate) fn parse(s: &[u8]) -> Option<Number> {
    // strtold skips leading white space but Redis refuses it, so the
    // grammar below has none.
    if s.is_empty() || s.len() >= MAX_CHARS {
        return None;
    }
    let (neg, s) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    if s.eq_ignore_ascii_case(b"inf") || s.eq_ignore_ascii_case(b"infinity") {
        return Some(Number::Infinite);
    }
    let value = match s {
        [b'0', b'x' | b'X', hex @ ..] => parse_hex(neg, hex),
        _ => parse_decimal(neg, s),
    }?;
    value.map(Number::Finite)
}

/// The digits of `s` in base `radix`, then an optional fraction: the
/// digits' value, how many came after the point, and the rest of `s`.
/// `None` without any digit.
fn mantissa(s: &[u8], radix: u32) -> Option<(Big, usize, &[u8])> {
    let mut big = Big::default();
    let (mut any, mut frac_digits, mut seen_point) = (false, 0, false);
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'.' && !seen_point {
            seen_point = true;
        } else if let Some(d) = char::from(s[i]).to_digit(radix) {
            any = true;
            big.mul_small(radix);
            big.add_small(d);
            frac_digits += usize::from(seen_point);
        } else {
            break;
        }
        i += 1;
    }
    any.then_some((big, frac_digits, &s[i..]))
}

/// An exponent: `marker`, an optional sign, digits (saturating), and all
/// of `s` taken. `Some(0)` for an empty `s`.
fn exponent(s: &[u8], markers: &[u8; 2]) -> Option<i64> {
    let Some((&m, rest)) = s.split_first() else { return Some(0) };
    if !markers.contains(&m) {
        return None;
    }
    let (neg, digits) = match rest.split_first() {
        Some((b'-', d)) => (true, d),
        Some((b'+', d)) => (false, d),
        _ => (false, rest),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let n = digits.iter().fold(0i64, |n, d| (n * 10 + i64::from(d - b'0')).min(1 << 40));
    Some(if neg { -n } else { n })
}

/// The outer `None`: not a number; the inner: out of range.
fn parse_decimal(neg: bool, s: &[u8]) -> Option<Option<LongDouble>> {
    let (mut d, frac_digits, rest) = mantissa(s, 10)?;
    let k = exponent(rest, b"eE")? - frac_digits as i64;
    if d.is_zero() {
        return Some(Some(LongDouble { neg, ..LongDouble::ZERO }));
    }
    // Bounds first, so the numbers stay small: below 10^-4952 rounds to
    // zero (the smallest subnormal is 3.6e-4951), 10^4933 and up is past
    // the largest (1.19e4932).
    let magnitude = k + d.decimal_len_estimate() as i64;
    if magnitude > 4934 {
        return Some(None);
    }
    if magnitude < -4953 {
        return Some(round_or_underflow(neg, 0, MIN_EXP - 2, true));
    }
    if k >= 0 {
        d.mul_pow10(k as usize);
        let (q, e, sticky) = d.top_bits();
        return Some(round(neg, q, e, sticky));
    }
    // d / 10^-k, with 66 to 68 bits of quotient and the rest as a sticky bit.
    let mut b = Big::from_u64(1);
    b.mul_pow10(k.unsigned_abs() as usize);
    let s = b.bit_len() as i64 + 67 - d.bit_len() as i64;
    if s >= 0 {
        d.shl(s as usize);
    } else {
        b.shl(s.unsigned_abs() as usize);
    }
    let q = d.div_bits(&mut b, 68);
    Some(round_or_underflow(neg, q, -s as i32, !d.is_zero()))
}

/// The outer `None`: not a number; the inner: out of range.
fn parse_hex(neg: bool, s: &[u8]) -> Option<Option<LongDouble>> {
    // `0x` with no hex digit: strtold reads the `0` and stops at the `x`.
    let (h, frac_digits, rest) = mantissa(s, 16)?;
    let p = exponent(rest, b"pP")?;
    if h.is_zero() {
        return Some(Some(LongDouble { neg, ..LongDouble::ZERO }));
    }
    let e = p - 4 * frac_digits as i64;
    let magnitude = e + h.bit_len() as i64;
    if magnitude > i64::from(MAX_EXP) + 66 {
        return Some(None);
    }
    if magnitude < i64::from(MIN_EXP) - 2 {
        return Some(round_or_underflow(neg, 0, MIN_EXP - 2, true));
    }
    let (q, shift, sticky) = h.top_bits();
    Some(round_or_underflow(neg, q, (e + i64::from(shift)) as i32, sticky))
}

/// `round`, with a nonzero value that rounds to zero out of range too.
fn round_or_underflow(neg: bool, q: u128, e: i32, sticky: bool) -> Option<LongDouble> {
    if q == 0 {
        // Only for values far below the smallest subnormal.
        return None;
    }
    round(neg, q, e, sticky).filter(|v| v.mant != 0)
}

/// A non-negative integer: 32-bit limbs, least significant first, no
/// leading zero limbs. Only as much as the conversions need.
#[derive(Clone, Debug, Default)]
struct Big(Vec<u32>);

impl Big {
    fn from_u64(n: u64) -> Big {
        let mut b = Big(vec![n as u32, (n >> 32) as u32]);
        b.trim();
        b
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn bit_len(&self) -> usize {
        self.0.last().map_or(0, |top| self.0.len() * 32 - top.leading_zeros() as usize)
    }

    /// At most one more than the number of decimal digits, never fewer.
    fn decimal_len_estimate(&self) -> usize {
        // log10(2) < 0.30103
        self.bit_len() * 30103 / 100_000 + 1
    }

    fn mul_small(&mut self, m: u32) {
        let mut carry = 0u64;
        for limb in &mut self.0 {
            let v = u64::from(*limb) * u64::from(m) + carry;
            *limb = v as u32;
            carry = v >> 32;
        }
        if carry != 0 {
            self.0.push(carry as u32);
        }
    }

    fn add_small(&mut self, a: u32) {
        let mut carry = a;
        for limb in &mut self.0 {
            let (v, c) = limb.overflowing_add(carry);
            *limb = v;
            if !c {
                return;
            }
            carry = 1;
        }
        if carry != 0 {
            self.0.push(carry);
        }
    }

    fn mul_pow10(&mut self, mut n: usize) {
        while n >= 9 {
            self.mul_small(1_000_000_000);
            n -= 9;
        }
        self.mul_small(10u32.pow(n as u32));
    }

    fn shl(&mut self, bits: usize) {
        if self.is_zero() {
            return;
        }
        let (limbs, bits) = (bits / 32, bits % 32);
        if bits != 0 {
            let mut carry = 0;
            for limb in &mut self.0 {
                let v = (*limb << bits) | carry;
                carry = *limb >> (32 - bits);
                *limb = v;
            }
            if carry != 0 {
                self.0.push(carry);
            }
        }
        self.0.splice(0..0, std::iter::repeat_n(0, limbs));
    }

    fn shr1(&mut self) {
        let mut carry = 0;
        for limb in self.0.iter_mut().rev() {
            let v = (*limb >> 1) | carry;
            carry = *limb << 31;
            *limb = v;
        }
        self.trim();
    }

    fn cmp(&self, other: &Big) -> Ordering {
        self.0.len().cmp(&other.0.len()).then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }

    /// `self -= other`, `other` not larger.
    fn sub(&mut self, other: &Big) {
        let mut borrow = false;
        for (i, limb) in self.0.iter_mut().enumerate() {
            let o = other.0.get(i).copied().unwrap_or(0);
            let (v, b1) = limb.overflowing_sub(o);
            let (v, b2) = v.overflowing_sub(u32::from(borrow));
            *limb = v;
            borrow = b1 || b2;
        }
        debug_assert!(!borrow);
        self.trim();
    }

    /// `self / d`, known to be below `2^bits` (at most 127), by binary long
    /// division; `self` keeps the remainder. Clobbers `d`.
    fn div_bits(&mut self, d: &mut Big, bits: u32) -> u128 {
        let mut q = 0u128;
        d.shl(bits as usize);
        for i in (0..=bits).rev() {
            if self.cmp(d) != Ordering::Less {
                self.sub(d);
                q |= 1 << i;
            }
            d.shr1();
        }
        q
    }

    /// The top 128 bits (all of them if fewer), the exponent of their
    /// lowest, and whether any bit below them is set.
    fn top_bits(&self) -> (u128, i32, bool) {
        let len = self.bit_len();
        let shift = len.saturating_sub(128);
        let (limbs, bits) = (shift / 32, shift % 32);
        let mut q = 0u128;
        // Five limbs cover 128 bits at any offset.
        for (i, &limb) in self.0.iter().enumerate().skip(limbs).take(5) {
            let at = (i - limbs) * 32;
            let v = u128::from(limb);
            if at >= bits + 128 {
                break;
            } else if at >= bits {
                q |= v << (at - bits);
            } else {
                q |= v >> (bits - at);
            }
        }
        let sticky = self.0[..limbs].iter().any(|&l| l != 0) || (bits > 0 && self.0[limbs] & ((1 << bits) - 1) != 0);
        (q, shift as i32, sticky)
    }

    fn write_decimal(mut self, out: &mut Vec<u8>) {
        if self.is_zero() {
            out.push(b'0');
            return;
        }
        let mut chunks = Vec::new();
        while !self.is_zero() {
            let mut rem = 0u64;
            for limb in self.0.iter_mut().rev() {
                let v = (rem << 32) | u64::from(*limb);
                *limb = (v / 1_000_000_000) as u32;
                rem = v % 1_000_000_000;
            }
            self.trim();
            chunks.push(rem as u32);
        }
        let top = chunks.pop().expect("not zero");
        out.extend_from_slice(top.to_string().as_bytes());
        for c in chunks.iter().rev() {
            out.extend_from_slice(format!("{c:09}").as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(s: &str) -> LongDouble {
        match parse(s.as_bytes()) {
            Some(Number::Finite(v)) => v,
            other => panic!("{s:?}: {other:?}"),
        }
    }

    fn text(v: LongDouble) -> String {
        let mut out = Vec::new();
        v.write(&mut out);
        String::from_utf8(out).unwrap()
    }

    fn sum(a: &str, b: &str) -> String {
        text(num(a).add(num(b)).expect("finite"))
    }

    /// Replies of Redis 7.0.15 on x86_64 to `SET x a`, `INCRBYFLOAT x b`.
    #[test]
    fn sums_as_redis_writes_them() {
        assert_eq!(sum("10.5", "0.1"), "10.6");
        assert_eq!(sum("10.6", "5.0e3"), "5010.60000000000000009");
        assert_eq!(sum("5010.60000000000000009", "-5010.6"), "0");
        assert_eq!(sum("3.0", "1.0"), "4");
        assert_eq!(sum("4", "1.5e-20"), "4");
        assert_eq!(sum("4", "0x10"), "20");
        assert_eq!(sum("0", "0x1p3"), "8");
        assert_eq!(sum("0", "0x1.8p-1"), "0.75");
        assert_eq!(sum("0", ".5"), "0.5");
        assert_eq!(sum("0", "5."), "5");
        assert_eq!(sum("0", "+5"), "5");
        assert_eq!(sum("0", "-0"), "0");
        assert_eq!(sum("0", "1e-17"), "0.00000000000000001");
        assert_eq!(sum("0", "5e-18"), "0");
        assert_eq!(sum("0", "1.5e-17"), "0.00000000000000002");
        assert_eq!(sum("0", "2.5e-17"), "0.00000000000000002");
        // Exact ties at the 17th decimal go to even.
        assert_eq!(sum("0", "0x1p-18"), "0.00000381469726562");
        assert_eq!(sum("0", "0x3p-18"), "0.00001144409179688");
        assert_eq!(sum("0", "00012"), "12");
        assert_eq!(sum("0", "1e-4940"), "0");
        assert_eq!(sum("0", "1e-4950"), "0");
        assert_eq!(sum("-1e-20", "0"), "0");
        assert_eq!(sum("1.00000000000000000005", "0"), "1");
        assert_eq!(sum("4", "10000000000000000000000"), "10000000000000000000000");
        assert_eq!(sum("10000000000000000000000", "1.123456789012345678901"), "10000000000000000000000");
        assert_eq!(
            sum("1.7976931348623157e308", "1.7976931348623157e308"),
            "359538626972463139991842093548208868096772893888658356970628841530353257046248792709403737216774171128857586838851041378617416726902272110715794556052955234146997955372576789671236326588091188869859074894581254961338834769297758245552436667197907705664206565009503223382416235440319971852753604933726679072768"
        );
        assert_eq!(
            sum("1", "1e300"),
            "1000000000000000000008997324079559193870523944273290747938260082321265646596180935755849152083750497190350372508614274835903592556184672983913096260520748646287327135641843653294084255107606016789726652932370030551382947620994540294772781889620606179267611627097410650567187386105690089424915104006144"
        );
        let max = sum("0", "1e4932");
        assert_eq!(max.len(), 4933);
        assert!(max.starts_with("10000000000000000000060189493879638913238377742545331147737850413525330194866087224075333277794"));
        assert!(max.ends_with("1086538418113791886622720"));
    }

    #[test]
    fn what_redis_does_not_read() {
        for s in [
            "", " 1", "1 ", "abc", "nan", "-nan", "NaN(1)", "1_0", "1e", "1e+", "0x", "0xg", "0x1p", ".", "-", "e5",
            "1e-5000", "1.2e4932", "1e99999999999999999999", "0x1p16384", "0x1p-16500", "--1", "infinit",
        ] {
            assert_eq!(parse(s.as_bytes()), None, "{s:?}");
        }
        let long = "1".repeat(MAX_CHARS);
        assert_eq!(parse(long.as_bytes()), None);
        // 1, too long to read; one byte shorter it's fine.
        assert_eq!(parse(format!("{}1", "0".repeat(MAX_CHARS - 1)).as_bytes()), None);
        assert_eq!(parse(format!("{}1", "0".repeat(MAX_CHARS - 2)).as_bytes()), Some(Number::Finite(num("1"))));
        // An overflow, not the length.
        assert_eq!(parse("1".repeat(MAX_CHARS - 1).as_bytes()), None);
    }

    #[test]
    fn infinities_and_zeros() {
        for s in ["inf", "-inf", "INF", "Infinity", "+infinity"] {
            assert_eq!(parse(s.as_bytes()), Some(Number::Infinite), "{s}");
        }
        assert_eq!(parse(b"0e99999999999"), Some(Number::Finite(LongDouble::ZERO)));
        assert_eq!(text(num("-0").add(num("-0")).unwrap()), "0");
        assert!(num("-0").add(num("-0")).unwrap().neg);
        assert!(!num("-1").add(num("1")).unwrap().neg);
        // Redis checks that strtold read every byte.
        assert_eq!(parse(b"1.5\0abc"), None);
        assert_eq!(parse(b"1.5\0"), None);
    }

    #[test]
    fn the_extremes() {
        let max = num("0x0.ffffffffffffffffp16384");
        assert_eq!((max.mant, max.exp), (u64::MAX, MAX_EXP));
        assert_eq!(max.add(max), None);
        assert_eq!(max.add(LongDouble { neg: true, ..max }), Some(LongDouble::ZERO));
        // 2^-16445, the smallest subnormal, and half of it, which ties to
        // even: zero, out of range.
        let tiny = num("0x1p-16445");
        assert_eq!((tiny.mant, tiny.exp), (1, MIN_EXP));
        assert_eq!(parse(b"0x1p-16446"), None);
        assert_eq!(num("0x3p-16447"), tiny);
        assert_eq!(tiny.add(tiny).unwrap().mant, 2);
        assert_eq!(text(tiny), "0");
        // Subnormals that add up to a normal.
        let half = num("0x1p-16383");
        assert_eq!((half.mant, half.exp), (1 << 62, MIN_EXP));
        let normal = half.add(half).unwrap();
        assert_eq!((normal.mant, normal.exp), (1 << 63, MIN_EXP));
        // Ties to even when adding: 1 + 2^-64 is a tie, 1 + 3 * 2^-65 not.
        assert_eq!(num("1").add(num("0x1p-64")), Some(num("1")));
        assert_eq!(num("1").add(num("0x3p-65")).unwrap().mant, (1 << 63) + 1);
        assert_eq!(num("0x1.0000000000000002p0").add(num("0x1p-64")).unwrap().mant, (1 << 63) + 2);
        // Far apart: the smaller is only a sticky bit.
        assert_eq!(num("1").add(num("-0x1p-200")), Some(num("1")));
        assert_eq!(num("1").add(num("0x1p-200")), Some(num("1")));
        // Just past the halfway point below 1: rounds down.
        assert_eq!(num("1").add(num("-0x1.000000000000002p-65")).unwrap().mant, u64::MAX);
        // The same with a bit shifted out of the smaller one: only the
        // sticky bit says it's past halfway (Redis: 18446744073709551615;
        // an exact -0.5 ties to even, 18446744073709551616).
        assert_eq!(sum("18446744073709551616", "-0x1.0000000000000002p-1"), "18446744073709551615");
        assert_eq!(sum("18446744073709551616", "-0.5"), "18446744073709551616");
    }

    #[test]
    fn decimal_reading_is_correctly_rounded() {
        // 0.1 in 64 bits: 0xcccccccccccccccd * 2^-67 (glibc's strtold).
        let v = num("0.1");
        assert_eq!((v.mant, v.exp), (0xcccc_cccc_cccc_cccd, -67));
        // A tie in decimal, beyond the first 66 bits: 1 + 2^-64 rounds to
        // 1, a hair above it rounds up.
        assert_eq!(num("1.0000000000000000000542101086242752217003726400434970855712890625"), num("1"));
        assert_eq!(num("1.0000000000000000000542101086242752217003726400434970855712890626").mant, (1 << 63) + 1);
        assert_eq!(num("1.0000000000000000001084202172485504434007452800869941711425781250").mant, (1 << 63) + 1);
        assert_eq!(num("1.6250e3"), num("1625"));
    }
}
