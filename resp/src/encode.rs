//! Reply encoding for RESP2 and RESP3.
//!
//! RESP3 adds types RESP2 lacks; each falls back to what Redis 7.0 sends a
//! RESP2 client (checked with `DEBUG PROTOCOL <type>` against Redis 7.0.15):
//!
//! | reply        | RESP3              | RESP2                    |
//! |--------------|--------------------|--------------------------|
//! | null         | `_`                | `$-1`                    |
//! | null array   | `_`                | `*-1`                    |
//! | map of n     | `%n`               | `*2n`                    |
//! | set of n     | `~n`               | `*n`                     |
//! | push of n    | `>n`               | `*n` (as pub/sub does)   |
//! | double       | `,3.141`           | bulk `3.141`             |
//! | boolean      | `#t` / `#f`        | `:1` / `:0`              |
//! | verbatim     | `=n` `txt:...`     | bulk without `txt:`      |
//! | big number   | `(123...`          | bulk                     |
//! | bulk error   | `!n` ...           | `-...`                   |

use std::fmt::Write as _;

/// The protocol a connection speaks: RESP2 until the client sends `HELLO 3`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    #[default]
    Resp2,
    Resp3,
}

/// Appends replies to an output buffer. One per connection: replies to a
/// batch of pipelined requests accumulate here and go out in one write.
#[derive(Debug, Default)]
pub struct Encoder {
    out: Vec<u8>,
    protocol: Protocol,
}

impl Encoder {
    pub fn new(protocol: Protocol) -> Encoder {
        Encoder { out: Vec::new(), protocol }
    }

    /// Appends to `out`, keeping what it holds.
    pub fn with_buffer(out: Vec<u8>, protocol: Protocol) -> Encoder {
        Encoder { out, protocol }
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// For `HELLO`: replies from now on use `protocol`.
    pub fn set_protocol(&mut self, protocol: Protocol) {
        self.protocol = protocol;
    }

    /// The encoded replies, ready to write.
    pub fn as_bytes(&self) -> &[u8] {
        &self.out
    }

    /// The buffer itself, e.g. to append a pre-encoded reply or to drain the
    /// part a short write sent.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.out
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.out
    }

    pub fn len(&self) -> usize {
        self.out.len()
    }

    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }

    /// Empties the buffer after it was written, keeping its capacity.
    pub fn clear(&mut self) {
        self.out.clear();
    }

    /// `+OK`.
    pub fn ok(&mut self) {
        self.out.extend_from_slice(b"+OK\r\n");
    }

    /// A simple string, `+text`. `\r` and `\n` would end the reply early, so
    /// they become spaces.
    pub fn simple(&mut self, text: &[u8]) {
        self.line_parts(b'+', &[text]);
    }

    /// An error, `-ERR message` (the code, e.g. `ERR` or `WRONGTYPE`, is part
    /// of `message`). Like Redis, `\r` and `\n` become spaces: messages may
    /// quote client input.
    pub fn error(&mut self, message: &[u8]) {
        self.line_parts(b'-', &[message]);
    }

    /// An error put together from pieces, to quote client input without
    /// allocating: `error_parts(&[b"ERR unknown command '", name, b"'"])`.
    pub fn error_parts(&mut self, parts: &[&[u8]]) {
        self.line_parts(b'-', parts);
    }

    /// `:n`.
    pub fn integer(&mut self, n: i64) {
        let mut buf = [0; 20];
        let digits = format_integer(n, &mut buf);
        self.out.reserve(digits.len() + 3);
        self.out.push(b':');
        self.out.extend_from_slice(digits);
        self.out.extend_from_slice(b"\r\n");
    }

    /// A bulk string, `$len\r\nvalue\r\n`; binary safe.
    pub fn bulk(&mut self, value: &[u8]) {
        self.out.reserve(value.len() + 25);
        self.header(b'$', value.len());
        self.out.extend_from_slice(value);
        self.out.extend_from_slice(b"\r\n");
    }

    /// A missing value: `$-1` in RESP2, `_` in RESP3.
    pub fn null(&mut self) {
        self.out.extend_from_slice(match self.protocol {
            Protocol::Resp2 => b"$-1\r\n",
            Protocol::Resp3 => b"_\r\n",
        });
    }

    /// A missing array (e.g. `BLPOP` timing out): `*-1` in RESP2, `_` in RESP3.
    pub fn null_array(&mut self) {
        self.out.extend_from_slice(match self.protocol {
            Protocol::Resp2 => b"*-1\r\n",
            Protocol::Resp3 => b"_\r\n",
        });
    }

    /// An array of `len` elements; encode them next.
    pub fn array(&mut self, len: usize) {
        self.header(b'*', len);
    }

    /// A map of `len` key/value pairs; encode key, value, key, value... next.
    /// RESP2 gets a flat array of `2 * len` elements.
    pub fn map(&mut self, len: usize) {
        match self.protocol {
            Protocol::Resp2 => self.header(b'*', 2 * len),
            Protocol::Resp3 => self.header(b'%', len),
        }
    }

    /// A set of `len` elements; an array in RESP2.
    pub fn set(&mut self, len: usize) {
        match self.protocol {
            Protocol::Resp2 => self.header(b'*', len),
            Protocol::Resp3 => self.header(b'~', len),
        }
    }

    /// An out-of-band push of `len` elements (pub/sub, invalidation). RESP2
    /// has no push type; Redis sends pub/sub messages to RESP2 clients as
    /// arrays, so that's what RESP2 gets.
    pub fn push(&mut self, len: usize) {
        match self.protocol {
            Protocol::Resp2 => self.header(b'*', len),
            Protocol::Resp3 => self.header(b'>', len),
        }
    }

    /// A double formatted like Redis 7.0's `%.17g` (`1.1` is
    /// `1.1000000000000001`, `inf`, `-inf`): `,` in RESP3, a bulk string in RESP2.
    pub fn double(&mut self, d: f64) {
        let mut buf = [0; 32];
        let text = format_double(d, &mut buf);
        match self.protocol {
            Protocol::Resp2 => self.bulk(text),
            Protocol::Resp3 => self.line_parts(b',', &[text]),
        }
    }

    /// A double already written (Redis's `long double`s, as `INCREX`
    /// replies them): `,` in RESP3, a bulk string in RESP2.
    pub fn double_text(&mut self, text: &[u8]) {
        match self.protocol {
            Protocol::Resp2 => self.bulk(text),
            Protocol::Resp3 => self.line_parts(b',', &[text]),
        }
    }

    /// `#t` / `#f` in RESP3, `:1` / `:0` in RESP2.
    pub fn boolean(&mut self, b: bool) {
        self.out.extend_from_slice(match (self.protocol, b) {
            (Protocol::Resp2, true) => b":1\r\n",
            (Protocol::Resp2, false) => b":0\r\n",
            (Protocol::Resp3, true) => b"#t\r\n",
            (Protocol::Resp3, false) => b"#f\r\n",
        });
    }

    /// A verbatim string with a three-letter format (`txt`, `mkd`), as
    /// `INFO` and `CLIENT LIST` reply: `=len\r\ntxt:text\r\n` in RESP3, the
    /// bare text as a bulk string in RESP2.
    pub fn verbatim(&mut self, format: &[u8; 3], text: &[u8]) {
        match self.protocol {
            Protocol::Resp2 => self.bulk(text),
            Protocol::Resp3 => {
                self.out.reserve(text.len() + 29);
                self.header(b'=', text.len() + 4);
                self.out.extend_from_slice(format);
                self.out.push(b':');
                self.out.extend_from_slice(text);
                self.out.extend_from_slice(b"\r\n");
            }
        }
    }

    /// An integer too big for `i64`, given as its decimal digits: `(digits`
    /// in RESP3, a bulk string in RESP2.
    pub fn big_number(&mut self, digits: &[u8]) {
        match self.protocol {
            Protocol::Resp2 => self.bulk(digits),
            Protocol::Resp3 => self.line_parts(b'(', &[digits]),
        }
    }

    /// An error that may contain any bytes: `!len\r\nmessage\r\n` in RESP3;
    /// in RESP2 a plain error, with `\r` and `\n` turned into spaces.
    pub fn bulk_error(&mut self, message: &[u8]) {
        match self.protocol {
            Protocol::Resp2 => self.error(message),
            Protocol::Resp3 => {
                self.out.reserve(message.len() + 25);
                self.header(b'!', message.len());
                self.out.extend_from_slice(message);
                self.out.extend_from_slice(b"\r\n");
            }
        }
    }

    /// `<kind><len>\r\n`.
    fn header(&mut self, kind: u8, len: usize) {
        if len < 10 {
            self.out.extend_from_slice(&[kind, b'0' + len as u8, b'\r', b'\n']);
        } else {
            let mut buf = [0; 20];
            let start = format_u64(len as u64, &mut buf);
            self.out.reserve(23);
            self.out.push(kind);
            self.out.extend_from_slice(&buf[start..]);
            self.out.extend_from_slice(b"\r\n");
        }
    }

    /// `<kind><parts...>\r\n` with `\r` and `\n` in the parts made spaces.
    fn line_parts(&mut self, kind: u8, parts: &[&[u8]]) {
        self.out.push(kind);
        let start = self.out.len();
        for part in parts {
            self.out.extend_from_slice(part);
        }
        for b in &mut self.out[start..] {
            if *b == b'\r' || *b == b'\n' {
                *b = b' ';
            }
        }
        self.out.extend_from_slice(b"\r\n");
    }
}

/// "00", "01", ... "99": two digits per division.
const PAIRS: [u8; 200] = {
    let mut t = [0; 200];
    let mut i = 0;
    while i < 100 {
        t[2 * i] = b'0' + (i / 10) as u8;
        t[2 * i + 1] = b'0' + (i % 10) as u8;
        i += 1;
    }
    t
};

/// Writes `v`'s digits at the end of `buf`; returns where they start.
fn format_u64(mut v: u64, buf: &mut [u8; 20]) -> usize {
    let mut i = buf.len();
    while v >= 100 {
        let d = (v % 100) as usize * 2;
        v /= 100;
        i -= 2;
        buf[i..i + 2].copy_from_slice(&PAIRS[d..d + 2]);
    }
    if v >= 10 {
        let d = v as usize * 2;
        i -= 2;
        buf[i..i + 2].copy_from_slice(&PAIRS[d..d + 2]);
    } else {
        i -= 1;
        buf[i] = b'0' + v as u8;
    }
    i
}

/// `n` in decimal, without allocating; for replies and for storing integers
/// as strings (`INCR`). Returns the part of `buf` holding the text.
pub fn format_integer(n: i64, buf: &mut [u8; 20]) -> &[u8] {
    // 19 digits at most, so the 20th byte is free for the sign.
    let mut start = format_u64(n.unsigned_abs(), buf);
    if n < 0 {
        start -= 1;
        buf[start] = b'-';
    }
    &buf[start..]
}

/// `d` as C's `printf("%.17g")` formats it, which is what Redis 7.0 replies
/// (`ZSCORE`): 17 significant digits, trailing zeros removed, an exponent
/// below 1e-4 or from 1e17 on. Infinities are `inf` / `-inf`, NaN `nan` /
/// `-nan` (glibc). Returns the part of `buf` holding the text.
pub fn format_double(d: f64, buf: &mut [u8; 32]) -> &[u8] {
    if !d.is_finite() {
        let text: &[u8] = match (d.is_nan(), d.is_sign_negative()) {
            (true, false) => b"nan",
            (true, true) => b"-nan",
            (false, false) => b"inf",
            (false, true) => b"-inf",
        };
        buf[..text.len()].copy_from_slice(text);
        return &buf[..text.len()];
    }
    // Rust rounds exactly as glibc does (correctly, from the binary value): "-d.dddddddddddddddde-x".
    let mut sci = Cursor { buf: [0; 32], len: 0 };
    write!(sci, "{:.16e}", d.abs()).expect("fits in 32 bytes");
    let sci = &sci.buf[..sci.len];
    let e = sci.iter().position(|&b| b == b'e').expect("an exponent");
    let mut digits = [0u8; 17];
    digits[0] = sci[0];
    digits[1..].copy_from_slice(&sci[2..e]);
    let exp = std::str::from_utf8(&sci[e + 1..]).ok().and_then(|s| s.parse::<i32>().ok()).expect("an exponent");
    let mut n = 17;
    while n > 1 && digits[n - 1] == b'0' {
        n -= 1;
    }

    let mut len = 0;
    let mut put = |bytes: &[u8]| {
        buf[len..len + bytes.len()].copy_from_slice(bytes);
        len += bytes.len();
    };
    if d.is_sign_negative() {
        put(b"-");
    }
    if !(-4..17).contains(&exp) {
        put(&digits[..1]);
        if n > 1 {
            put(b".");
            put(&digits[1..n]);
        }
        put(if exp < 0 { b"e-" } else { b"e+" });
        let mut ebuf = [0; 20];
        let start = format_u64(u64::from(exp.unsigned_abs()), &mut ebuf);
        if ebuf.len() - start < 2 {
            put(b"0");
        }
        put(&ebuf[start..]);
    } else if exp >= 0 {
        let int_len = exp as usize + 1;
        put(&digits[..int_len]);
        if n > int_len {
            put(b".");
            put(&digits[int_len..n]);
        }
    } else {
        put(b"0.");
        for _ in 0..-exp - 1 {
            put(b"0");
        }
        put(&digits[..n]);
    }
    &buf[..len]
}

/// A `fmt::Write` into a fixed buffer, so formatting doesn't allocate.
struct Cursor {
    buf: [u8; 32],
    len: usize,
}

impl std::fmt::Write for Cursor {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        self.buf.get_mut(self.len..end).ok_or(std::fmt::Error)?.copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn both(f: impl Fn(&mut Encoder)) -> (Vec<u8>, Vec<u8>) {
        let mut two = Encoder::new(Protocol::Resp2);
        let mut three = Encoder::new(Protocol::Resp3);
        f(&mut two);
        f(&mut three);
        (two.into_inner(), three.into_inner())
    }

    fn check(f: impl Fn(&mut Encoder), resp2: &[u8], resp3: &[u8]) {
        let (two, three) = both(f);
        assert_eq!(two.escape_ascii().to_string(), resp2.escape_ascii().to_string(), "RESP2");
        assert_eq!(three.escape_ascii().to_string(), resp3.escape_ascii().to_string(), "RESP3");
    }

    #[test]
    fn simple_strings_and_errors() {
        check(|e| e.ok(), b"+OK\r\n", b"+OK\r\n");
        check(|e| e.simple(b"PONG"), b"+PONG\r\n", b"+PONG\r\n");
        check(|e| e.simple(b"a\r\nb"), b"+a  b\r\n", b"+a  b\r\n");
        check(|e| e.error(b"ERR syntax error"), b"-ERR syntax error\r\n", b"-ERR syntax error\r\n");
        check(|e| e.error(b"ERR a\nb\rc"), b"-ERR a b c\r\n", b"-ERR a b c\r\n");
        check(|e| e.error_parts(&[b"ERR unknown command '", b"x\r\ny", b"'"]),
              b"-ERR unknown command 'x  y'\r\n", b"-ERR unknown command 'x  y'\r\n");
        check(|e| e.error_parts(&[]), b"-\r\n", b"-\r\n");
    }

    #[test]
    fn integers() {
        check(|e| e.integer(0), b":0\r\n", b":0\r\n");
        check(|e| e.integer(12345), b":12345\r\n", b":12345\r\n");
        check(|e| e.integer(-1), b":-1\r\n", b":-1\r\n");
        check(|e| e.integer(i64::MAX), b":9223372036854775807\r\n", b":9223372036854775807\r\n");
        check(|e| e.integer(i64::MIN), b":-9223372036854775808\r\n", b":-9223372036854775808\r\n");
    }

    #[test]
    fn format_integer_matches_to_string() {
        let mut values = vec![0, 1, 9, 10, 11, 99, 100, 101, 999, 1000, i64::MAX, i64::MIN, i64::MIN + 1];
        let mut p = 1i64;
        while let Some(next) = p.checked_mul(10) {
            values.extend([p - 1, p, p + 1, -p, -p - 1, -p + 1]);
            p = next;
        }
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            values.push(x as i64);
            values.push((x >> (x % 64)) as i64);
        }
        for v in values {
            let mut buf = [0; 20];
            assert_eq!(format_integer(v, &mut buf), v.to_string().as_bytes());
        }
    }

    #[test]
    fn bulk_strings() {
        check(|e| e.bulk(b"Hello World"), b"$11\r\nHello World\r\n", b"$11\r\nHello World\r\n");
        check(|e| e.bulk(b""), b"$0\r\n\r\n", b"$0\r\n\r\n");
        check(|e| e.bulk(b"\r\n\0"), b"$3\r\n\r\n\0\r\n", b"$3\r\n\r\n\0\r\n");
        let big = vec![b'x'; 123456];
        let (two, _) = both(|e| e.bulk(&big));
        assert_eq!(&two[..10], b"$123456\r\nx");
        assert_eq!(two.len(), 9 + 123456 + 2);
    }

    #[test]
    fn nulls() {
        // As Redis 7.0.15 replies to GET of a missing key, and to BLPOP timing out.
        check(|e| e.null(), b"$-1\r\n", b"_\r\n");
        check(|e| e.null_array(), b"*-1\r\n", b"_\r\n");
    }

    #[test]
    fn aggregates() {
        // As Redis 7.0.15 replies to DEBUG PROTOCOL array / set / map / push.
        check(|e| { e.array(3); e.integer(0); e.integer(1); e.integer(2) },
              b"*3\r\n:0\r\n:1\r\n:2\r\n", b"*3\r\n:0\r\n:1\r\n:2\r\n");
        check(|e| { e.set(3); e.integer(0); e.integer(1); e.integer(2) },
              b"*3\r\n:0\r\n:1\r\n:2\r\n", b"~3\r\n:0\r\n:1\r\n:2\r\n");
        check(|e| { e.map(3); for i in 0..3 { e.integer(i); e.boolean(i == 1) } },
              b"*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n", b"%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n");
        check(|e| { e.push(2); e.bulk(b"server-cpu-usage"); e.integer(42) },
              b"*2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n", b">2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n");
        check(|e| e.array(0), b"*0\r\n", b"*0\r\n");
        check(|e| e.array(10), b"*10\r\n", b"*10\r\n");
        check(|e| e.map(5), b"*10\r\n", b"%5\r\n");
        check(|e| e.set(1_000_000), b"*1000000\r\n", b"~1000000\r\n");
        check(|e| e.push(12), b"*12\r\n", b">12\r\n");
    }

    #[test]
    fn booleans() {
        check(|e| e.boolean(true), b":1\r\n", b"#t\r\n");
        check(|e| e.boolean(false), b":0\r\n", b"#f\r\n");
    }

    #[test]
    fn verbatim_big_number_bulk_error() {
        // As Redis 7.0.15 replies to DEBUG PROTOCOL verbatim / bignum.
        check(|e| e.verbatim(b"txt", b"This is a verbatim\nstring"),
              b"$25\r\nThis is a verbatim\nstring\r\n", b"=29\r\ntxt:This is a verbatim\nstring\r\n");
        check(|e| e.big_number(b"1234567999999999999999999999999999999"),
              b"$37\r\n1234567999999999999999999999999999999\r\n", b"(1234567999999999999999999999999999999\r\n");
        check(|e| e.bulk_error(b"SYNTAX invalid\r\nsyntax"),
              b"-SYNTAX invalid  syntax\r\n", b"!22\r\nSYNTAX invalid\r\nsyntax\r\n");
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.141 is what DEBUG PROTOCOL double sends, not pi
    fn doubles() {
        // As Redis 7.0.15 replies to ZSCORE (and DEBUG PROTOCOL double for 3.141).
        check(|e| e.double(3.141), b"$5\r\n3.141\r\n", b",3.141\r\n");
        check(|e| e.double(1.1), b"$18\r\n1.1000000000000001\r\n", b",1.1000000000000001\r\n");
        check(|e| e.double(f64::INFINITY), b"$3\r\ninf\r\n", b",inf\r\n");
        check(|e| e.double(f64::NEG_INFINITY), b"$4\r\n-inf\r\n", b",-inf\r\n");
        check(|e| e.double(0.0), b"$1\r\n0\r\n", b",0\r\n");
    }

    #[test]
    fn format_double_like_printf_17g() {
        // Expected texts from Redis 7.0.15 (ZSCORE) and Python's '%.17g'.
        let cases: &[(f64, &str)] = &[
            (1.1, "1.1000000000000001"),
            (1e300, "1.0000000000000001e+300"),
            (0.0001, "0.0001"),
            (123456789012345678.0, "1.2345678901234568e+17"),
            (0.00001, "1.0000000000000001e-05"),
            (3.0, "3"),
            (1e16, "10000000000000000"),
            (1e17, "1e+17"),
            (2.5e-5, "2.5000000000000001e-05"),
            (-0.0, "-0"),
            (-1.5, "-1.5"),
            (0.1, "0.10000000000000001"),
            (1.0 / 3.0, "0.33333333333333331"),
            (100.0, "100"),
            (12345678901234567.0, "12345678901234568"),
            (0.00012345, "0.00012344999999999999"),
            (5e-324, "4.9406564584124654e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (1e100, "1e+100"),
            (123.456, "123.456"),
            (-2.0f64.powi(70), "-1.1805916207174113e+21"),
            (f64::NAN, "nan"),
            (-f64::NAN, "-nan"),
        ];
        for &(d, want) in cases {
            let mut buf = [0; 32];
            assert_eq!(std::str::from_utf8(format_double(d, &mut buf)).unwrap(), want, "{d:e}");
        }
    }

    #[test]
    fn format_double_round_trips() {
        // 17 significant digits always read back as the same double.
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..100_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let d = f64::from_bits(x);
            if !d.is_finite() {
                continue;
            }
            let mut buf = [0; 32];
            let text = std::str::from_utf8(format_double(d, &mut buf)).unwrap();
            assert_eq!(text.parse::<f64>().unwrap().to_bits(), d.to_bits(), "{text}");
        }
    }

    #[test]
    fn protocol_switch_and_buffer() {
        let mut e = Encoder::default();
        assert_eq!(e.protocol(), Protocol::Resp2);
        e.null();
        e.set_protocol(Protocol::Resp3);
        e.null();
        assert_eq!(e.as_bytes(), b"$-1\r\n_\r\n");
        assert_eq!(e.len(), 8);
        e.clear();
        assert!(e.is_empty());
        e.buffer_mut().extend_from_slice(b"+QUEUED\r\n");
        let mut e = Encoder::with_buffer(e.into_inner(), Protocol::Resp3);
        e.ok();
        assert_eq!(e.as_bytes(), b"+QUEUED\r\n+OK\r\n");
    }
}
