//! Request parsing: multibulk (`*2\r\n$3\r\nGET\r\n$1\r\nk\r\n`) and inline
//! (`GET k\r\n`, what telnet sends) requests, like Redis 7.0.
//!
//! Redis is lenient in ways clients may rely on, and strict in ways a
//! differential test sees, so both are copied:
//! - a line ends at the first `\r`; the byte after it is skipped unchecked,
//!   and so are the two bytes after a bulk value (`$4\r\nPINGxx` is `PING`);
//! - `*0`, `*-1` (any count <= 0) and blank inline lines are no command;
//! - "too big ... string" errors fire only while no line end has arrived and
//!   more than `max_inline_len` bytes are waiting, so whether a long line is
//!   accepted depends on how it was split into reads, as it does in Redis;
//! - after an error Redis replies `-ERR <message>` and closes the connection.
//!
//! One Redis quirk is not copied: it looks for line ends with `strchr`, so a
//! NUL byte before the line end makes it wait (and eventually fail with "too
//! big ..."). Here a NUL is an ordinary byte, so `*1\0\r\n` fails at once with
//! "invalid multibulk length" and an inline `ECHO a\0b` echoes `a\0b`.

use std::fmt;
use std::ops::{Index, Range};

use crate::Encoder;

/// Size limits for requests. The defaults are Redis 7.0's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest accepted multibulk count (`*N`); above it the request
    /// fails with "invalid multibulk length". Redis 7.0 allows `INT_MAX`.
    /// Nothing is allocated up front for it: the argument list grows with
    /// the arguments that actually arrive.
    pub max_args: usize,
    /// The largest accepted bulk length (`$N`), Redis's `proto-max-bulk-len`
    /// (512 MiB); above it the request fails with "invalid bulk length".
    pub max_bulk_len: usize,
    /// How many bytes may wait without a line end before the request fails
    /// with "too big inline request" / "too big mbulk count string" / "too
    /// big bulk count string" (Redis's `PROTO_INLINE_MAX_SIZE`, 64 KiB).
    pub max_inline_len: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits { max_args: i32::MAX as usize, max_bulk_len: 512 << 20, max_inline_len: 64 << 10 }
    }
}

/// What [`Parser::parse`] found at the start of the buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parsed {
    /// A whole request, `consumed` bytes long. [`Parser::args`] has its
    /// arguments; when there are none (`*0`, a blank inline line) there is
    /// nothing to execute and nothing to reply: skip the bytes and go on.
    Complete { consumed: usize },
    /// The request isn't complete yet. Nothing is consumed; call again with
    /// the same bytes and more appended.
    Incomplete,
}

/// A malformed request. Redis replies `-ERR <message>` ([`ProtocolError::encode`])
/// and closes the connection after the reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// `*` is not followed by a number, or the number exceeds `max_args`.
    InvalidMultibulkLength,
    /// `$` is not followed by a number >= 0, or the number exceeds `max_bulk_len`.
    InvalidBulkLength,
    /// An argument line of a multibulk request doesn't start with `$`.
    ExpectedDollar(u8),
    /// No `\n` in more than `max_inline_len` bytes of an inline request.
    TooBigInlineRequest,
    /// No `\r` in more than `max_inline_len` bytes after `*`.
    TooBigMultibulkCount,
    /// No `\r` in more than `max_inline_len` bytes where `$` was expected.
    TooBigBulkCount,
    /// An inline request has an unterminated quote, or a closing quote
    /// followed by something other than a space.
    UnbalancedQuotes,
}

impl ProtocolError {
    /// Appends the error reply Redis sends: `-ERR Protocol error: ...\r\n`.
    /// The same in RESP2 and RESP3.
    pub fn encode(&self, out: &mut Encoder) {
        let text: &[u8] = match self {
            ProtocolError::InvalidMultibulkLength => b"invalid multibulk length",
            ProtocolError::InvalidBulkLength => b"invalid bulk length",
            ProtocolError::ExpectedDollar(got) => {
                // The byte goes out raw, like Redis's "%c"; error() turns \r and \n into spaces as Redis does.
                return out.error_parts(&[b"ERR Protocol error: expected '$', got '", &[*got], b"'"]);
            }
            ProtocolError::TooBigInlineRequest => b"too big inline request",
            ProtocolError::TooBigMultibulkCount => b"too big mbulk count string",
            ProtocolError::TooBigBulkCount => b"too big bulk count string",
            ProtocolError::UnbalancedQuotes => b"unbalanced quotes in request",
        };
        out.error_parts(&[b"ERR Protocol error: ", text]);
    }
}

/// The message without the `ERR ` prefix, for logs. A non-printable byte in
/// "expected '$'" is escaped here (but not in [`ProtocolError::encode`]).
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            ProtocolError::InvalidMultibulkLength => "invalid multibulk length",
            ProtocolError::InvalidBulkLength => "invalid bulk length",
            ProtocolError::ExpectedDollar(got) => {
                return write!(f, "Protocol error: expected '$', got '{}'", got.escape_ascii());
            }
            ProtocolError::TooBigInlineRequest => "too big inline request",
            ProtocolError::TooBigMultibulkCount => "too big mbulk count string",
            ProtocolError::TooBigBulkCount => "too big bulk count string",
            ProtocolError::UnbalancedQuotes => "unbalanced quotes in request",
        };
        write!(f, "Protocol error: {text}")
    }
}

impl std::error::Error for ProtocolError {}

/// `bulk_len` before the `$N` line of the next argument has been read.
const NO_BULK: usize = usize::MAX;

/// Parses requests one at a time from the front of a buffer, resuming where
/// it stopped when a request arrives in pieces.
///
/// The caller owns the buffer and drives the parser:
/// - Call [`parse`](Parser::parse) with the bytes starting at the first
///   unparsed request. On `Complete { consumed }`, read the arguments with
///   [`args`](Parser::args), execute, and continue with the bytes after
///   `consumed`. Several requests in one read (pipelining) come out one per
///   call.
/// - On `Incomplete`, read more and call again with the same request start
///   and the new bytes appended. The parser keeps its place (arguments read
///   so far, how far it searched for a line end), so a large request read in
///   many pieces costs no re-parsing. The buffer may move or be compacted in
///   between, since everything is an offset from its start, but the bytes
///   already passed must stay as they were.
/// - On `Err`, reply with [`ProtocolError::encode`] and close the connection.
///   The parser is reset; to reuse it, start at a new request.
///
/// Steady state, parsing allocates nothing: the argument list and the inline
/// scratch buffer are kept and reused.
#[derive(Debug)]
pub struct Parser {
    limits: Limits,
    /// Byte ranges of the arguments read so far: into the caller's buffer for
    /// multibulk requests, into `scratch` for inline ones (`inline`).
    args: Vec<Range<usize>>,
    /// Inline arguments after removing quotes and escapes. Inline requests
    /// are rare (telnet, scripts), so copying them is fine.
    scratch: Vec<u8>,
    inline: bool,
    /// Multibulk arguments still to read; 0 between requests.
    remaining: usize,
    /// Length of the argument being read, or NO_BULK before its `$N` line.
    bulk_len: usize,
    /// Offset of the next unparsed byte of a multibulk request.
    pos: usize,
    /// Bytes from here back to the current line start contain no line end:
    /// the next search starts here instead of rescanning them.
    scanned: usize,
}

impl Parser {
    pub fn new(limits: Limits) -> Parser {
        Parser {
            limits,
            args: Vec::new(),
            scratch: Vec::new(),
            inline: false,
            remaining: 0,
            bulk_len: NO_BULK,
            pos: 0,
            scanned: 0,
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Parses the request at the start of `buf`. See [`Parser`] for how to
    /// drive it.
    pub fn parse(&mut self, buf: &[u8]) -> Result<Parsed, ProtocolError> {
        debug_assert!(buf.len() >= self.pos && buf.len() >= self.scanned, "buffer shrank between calls");
        let result = if self.remaining > 0 { self.multibulk_args(buf) } else { self.start(buf) };
        if result.is_err() {
            self.reset();
        }
        result
    }

    /// Forgets a partly parsed request, e.g. when the connection is reused.
    pub fn reset(&mut self) {
        self.args.clear();
        self.inline = false;
        self.remaining = 0;
        self.bulk_len = NO_BULK;
        self.pos = 0;
        self.scanned = 0;
    }

    /// The arguments of the request the last `Complete` returned. `buf` must
    /// be the buffer passed to that `parse` call (or one starting at the
    /// same byte). Empty for `*0` and blank inline lines.
    pub fn args<'a>(&'a self, buf: &'a [u8]) -> Args<'a> {
        let data = if self.inline { &self.scratch[..] } else { buf };
        Args { data, ranges: &self.args }
    }

    /// A new request: `*` starts a multibulk request, anything else an inline one.
    fn start(&mut self, buf: &[u8]) -> Result<Parsed, ProtocolError> {
        self.args.clear();
        self.inline = false;
        match buf.first() {
            None => Ok(Parsed::Incomplete),
            Some(b'*') => self.multibulk_count(buf),
            Some(_) => self.inline_request(buf),
        }
    }

    fn multibulk_count(&mut self, buf: &[u8]) -> Result<Parsed, ProtocolError> {
        let Some(cr) = self.find_line(buf, 0, ProtocolError::TooBigMultibulkCount)? else {
            return Ok(Parsed::Incomplete);
        };
        let count = match parse_i64(&buf[1..cr]) {
            Some(n) if n <= 0 => return Ok(Parsed::Complete { consumed: cr + 2 }),
            Some(n) if n as u64 <= self.limits.max_args as u64 => n as usize,
            _ => return Err(ProtocolError::InvalidMultibulkLength),
        };
        // Reserve for small requests only: the count is the client's claim, the bytes aren't here yet.
        self.args.reserve(count.min(1024));
        self.remaining = count;
        self.pos = cr + 2;
        self.multibulk_args(buf)
    }

    fn multibulk_args(&mut self, buf: &[u8]) -> Result<Parsed, ProtocolError> {
        while self.remaining > 0 {
            if self.bulk_len == NO_BULK {
                let start = self.pos;
                if let Some((len, next)) = bulk_len_fast(buf, start) {
                    if len <= self.limits.max_bulk_len as u64 {
                        self.bulk_len = len as usize;
                        self.pos = next;
                        self.scanned = 0;
                    }
                }
            }
            if self.bulk_len == NO_BULK {
                let start = self.pos;
                let Some(cr) = self.find_line(buf, start, ProtocolError::TooBigBulkCount)? else {
                    return Ok(Parsed::Incomplete);
                };
                if buf[start] != b'$' {
                    return Err(ProtocolError::ExpectedDollar(buf[start]));
                }
                self.bulk_len = match parse_i64(&buf[start + 1..cr]) {
                    Some(n) if n >= 0 && n as u64 <= self.limits.max_bulk_len as u64 => n as usize,
                    _ => return Err(ProtocolError::InvalidBulkLength),
                };
                self.pos = cr + 2;
            }
            // The value and two more bytes, which Redis skips without checking that they are "\r\n".
            let available = buf.len() - self.pos;
            if available < 2 || available - 2 < self.bulk_len {
                return Ok(Parsed::Incomplete);
            }
            self.args.push(self.pos..self.pos + self.bulk_len);
            self.pos += self.bulk_len + 2;
            self.bulk_len = NO_BULK;
            self.remaining -= 1;
        }
        let consumed = self.pos;
        self.pos = 0;
        Ok(Parsed::Complete { consumed })
    }

    /// Finds the `\r` ending the line at `start`, like Redis: the first `\r`,
    /// with at least one byte (meant to be `\n`, not checked) after it.
    /// Without any `\r`, more than `max_inline_len` bytes is `too_big`.
    fn find_line(&mut self, buf: &[u8], start: usize, too_big: ProtocolError) -> Result<Option<usize>, ProtocolError> {
        let from = self.scanned.max(start);
        match buf[from..].iter().position(|&b| b == b'\r') {
            Some(i) if from + i + 1 < buf.len() => {
                self.scanned = 0;
                Ok(Some(from + i))
            }
            Some(i) => {
                self.scanned = from + i;
                Ok(None)
            }
            None if buf.len() - start > self.limits.max_inline_len => Err(too_big),
            None => {
                self.scanned = buf.len();
                Ok(None)
            }
        }
    }

    /// One line up to `\n` (a `\r` before it is dropped), split into
    /// arguments like Redis's `sdssplitargs`.
    fn inline_request(&mut self, buf: &[u8]) -> Result<Parsed, ProtocolError> {
        let from = self.scanned;
        let Some(i) = buf[from..].iter().position(|&b| b == b'\n') else {
            if buf.len() > self.limits.max_inline_len {
                return Err(ProtocolError::TooBigInlineRequest);
            }
            self.scanned = buf.len();
            return Ok(Parsed::Incomplete);
        };
        let newline = from + i;
        self.scanned = 0;
        let end = if newline > 0 && buf[newline - 1] == b'\r' { newline - 1 } else { newline };
        split_args(&buf[..end], &mut self.scratch, &mut self.args)?;
        self.inline = true;
        Ok(Parsed::Complete { consumed: newline + 1 })
    }
}

/// The common case of a `$N\r\n` line in one pass: `$`, 1 to 18 digits
/// without a leading zero (or just `0`), `\r` and one more byte. Returns N
/// and the offset after the line; anything else is left to the general path.
#[inline]
fn bulk_len_fast(buf: &[u8], start: usize) -> Option<(u64, usize)> {
    let line = buf.get(start..)?;
    if line.first() != Some(&b'$') {
        return None;
    }
    let mut len = 0u64;
    let mut i = 1;
    while let Some(&b) = line.get(i) {
        if !b.is_ascii_digit() || i > 18 {
            break;
        }
        len = len * 10 + u64::from(b - b'0');
        i += 1;
    }
    let well_formed = i > 1 && (line[1] != b'0' || i == 2) && line.get(i) == Some(&b'\r') && i + 1 < line.len();
    well_formed.then_some((len, start + i + 2))
}

/// The arguments of a parsed request; `args[0]` is the command name.
#[derive(Clone, Copy, Debug)]
pub struct Args<'a> {
    data: &'a [u8],
    ranges: &'a [Range<usize>],
}

impl<'a> Args<'a> {
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn get(&self, i: usize) -> Option<&'a [u8]> {
        self.ranges.get(i).map(|r| &self.data[r.clone()])
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a [u8]> + 'a {
        let data = self.data;
        self.ranges.iter().map(move |r| &data[r.clone()])
    }
}

impl Index<usize> for Args<'_> {
    type Output = [u8];

    fn index(&self, i: usize) -> &[u8] {
        &self.data[self.ranges[i].clone()]
    }
}

/// C's `isspace` in the C locale.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn hex_value(b: Option<&u8>) -> Option<u8> {
    match *b? {
        b @ b'0'..=b'9' => Some(b - b'0'),
        b @ b'a'..=b'f' => Some(b - b'a' + 10),
        b @ b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Redis's `sdssplitargs`: arguments separated by whitespace; `"..."` with
/// the escapes `\n \r \t \b \a \xHH` (any other `\c` is `c`); `'...'` where
/// only `\'` is an escape. Quotes may start mid-argument (`a"b c"` is `ab c`)
/// but a closing quote must be followed by whitespace or the end. Note the
/// asymmetry, copied from Redis: leading blanks are anything `isspace`, but
/// an unquoted argument ends only at space, tab, `\r` or `\n` (`a\x0bb` is
/// one argument).
fn split_args(line: &[u8], out: &mut Vec<u8>, args: &mut Vec<Range<usize>>) -> Result<(), ProtocolError> {
    out.clear();
    let mut p = 0;
    loop {
        while p < line.len() && is_space(line[p]) {
            p += 1;
        }
        if p == line.len() {
            return Ok(());
        }
        let start = out.len();
        let (mut in_double, mut in_single) = (false, false);
        loop {
            let c = line.get(p).copied();
            let mut done = false;
            if in_double {
                match c {
                    None => return Err(ProtocolError::UnbalancedQuotes),
                    Some(b'\\') if line.get(p + 1) == Some(&b'x') => {
                        match (hex_value(line.get(p + 2)), hex_value(line.get(p + 3))) {
                            (Some(hi), Some(lo)) => {
                                out.push(hi * 16 + lo);
                                p += 3;
                            }
                            _ => {
                                p += 1;
                                out.push(b'x');
                            }
                        }
                    }
                    Some(b'\\') if p + 1 < line.len() => {
                        p += 1;
                        out.push(match line[p] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => other,
                        });
                    }
                    Some(b'"') => {
                        if p + 1 < line.len() && !is_space(line[p + 1]) {
                            return Err(ProtocolError::UnbalancedQuotes);
                        }
                        done = true;
                    }
                    Some(b) => out.push(b),
                }
            } else if in_single {
                match c {
                    None => return Err(ProtocolError::UnbalancedQuotes),
                    Some(b'\\') if line.get(p + 1) == Some(&b'\'') => {
                        p += 1;
                        out.push(b'\'');
                    }
                    Some(b'\'') => {
                        if p + 1 < line.len() && !is_space(line[p + 1]) {
                            return Err(ProtocolError::UnbalancedQuotes);
                        }
                        done = true;
                    }
                    Some(b) => out.push(b),
                }
            } else {
                match c {
                    None | Some(b' ' | b'\n' | b'\r' | b'\t') => done = true,
                    Some(b'"') => in_double = true,
                    Some(b'\'') => in_single = true,
                    Some(b) => out.push(b),
                }
            }
            if p < line.len() {
                p += 1;
            }
            if done {
                break;
            }
        }
        args.push(start..out.len());
    }
}

/// Redis's `string2ll`: an optional `-`, then digits without a leading zero
/// (`0` itself is fine, `-0` is not), no `+`, no blanks, fits in an `i64`.
fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (negative, digits) = match s[0] {
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if !matches!(digits.first(), Some(b'1'..=b'9')) {
        return None;
    }
    let mut v: u64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    if negative {
        (v <= 1 << 63).then(|| (v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes consumed and arguments of a complete request, None if incomplete.
    type One = Result<Option<(usize, Vec<Vec<u8>>)>, ProtocolError>;

    /// Parses one request from `input` in one go.
    fn parse_one(input: &[u8]) -> One {
        parse_one_with(input, Limits::default())
    }

    fn parse_one_with(input: &[u8], limits: Limits) -> One {
        let mut p = Parser::new(limits);
        Ok(match p.parse(input)? {
            Parsed::Complete { consumed } => Some((consumed, p.args(input).iter().map(<[u8]>::to_vec).collect())),
            Parsed::Incomplete => None,
        })
    }

    fn args(list: &[&[u8]]) -> Vec<Vec<u8>> {
        list.iter().map(|a| a.to_vec()).collect()
    }

    fn inline(line: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
        let (consumed, a) = parse_one(line)?.expect("complete");
        assert_eq!(consumed, line.len());
        Ok(a)
    }

    #[test]
    fn multibulk() {
        let req = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        assert_eq!(parse_one(req), Ok(Some((req.len(), args(&[b"GET", b"key"])))));
    }

    #[test]
    fn multibulk_is_binary_safe() {
        let req = b"*2\r\n$4\r\nECHO\r\n$6\r\n\0\r\n\r\xff\n\r\n";
        assert_eq!(parse_one(req), Ok(Some((req.len(), args(&[b"ECHO", b"\0\r\n\r\xff\n"])))));
    }

    #[test]
    fn empty_bulk_string() {
        let req = b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n";
        assert_eq!(parse_one(req), Ok(Some((req.len(), args(&[b"ECHO", b""])))));
    }

    #[test]
    fn args_index_into_the_buffer() {
        let req = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut p = Parser::new(Limits::default());
        assert_eq!(p.parse(req), Ok(Parsed::Complete { consumed: req.len() }));
        let a = p.args(req);
        assert_eq!(a.len(), 2);
        assert_eq!(&a[0], b"GET");
        assert_eq!(a.get(1), Some(&b"key"[..]));
        assert_eq!(a.get(2), None);
        assert_eq!(a[1].as_ptr(), req[17..].as_ptr());
    }

    #[test]
    fn counts_of_zero_or_less_are_no_command() {
        for req in [&b"*0\r\n"[..], b"*-1\r\n", b"*-5\r\n"] {
            assert_eq!(parse_one(req), Ok(Some((req.len(), vec![]))));
        }
    }

    #[test]
    fn trailing_bytes_after_lines_and_values_are_not_checked() {
        // Checked against Redis 7.0.15: all three are PING.
        for req in [&b"*1\r\n$4\r\nPINGxx"[..], b"*1\rX$4\r\nPING\r\n", b"*1\r\n$4\rXPING\r\n"] {
            assert_eq!(parse_one(req), Ok(Some((req.len(), args(&[b"PING"])))), "{}", req.escape_ascii());
        }
    }

    #[test]
    fn a_line_needs_a_byte_after_its_cr() {
        assert_eq!(parse_one(b"*1\r"), Ok(None));
        assert_eq!(parse_one(b"*1\r\n$4\r"), Ok(None));
        assert_eq!(parse_one(b"*1\r\n$4\r\nPING\r"), Ok(None));
        assert_eq!(parse_one(b"*1\r\n$4\r\nPING"), Ok(None));
        assert_eq!(parse_one(b""), Ok(None));
        assert_eq!(parse_one(b"*"), Ok(None));
    }

    #[test]
    fn bad_multibulk_counts() {
        // Each checked against Redis 7.0.15.
        for req in [
            &b"*abc\r\n"[..],
            b"*01\r\n",
            b"*+1\r\n",
            b"*-0\r\n",
            b"* 1\r\n",
            b"*\r\n",
            b"*1 \r\n",
            b"*2147483648\r\n",
            b"*99999999999999999999\r\n",
            b"*-9223372036854775809\r\n",
            b"*1\0\r\n",
        ] {
            assert_eq!(parse_one(req), Err(ProtocolError::InvalidMultibulkLength), "{}", req.escape_ascii());
        }
        assert_eq!(parse_one(b"*2147483647\r\n"), Ok(None));
        assert_eq!(parse_one(b"*-9223372036854775808\r\n"), Ok(Some((23, vec![]))));
    }

    #[test]
    fn max_args() {
        let limits = Limits { max_args: 2, ..Limits::default() };
        assert_eq!(parse_one_with(b"*2\r\n", limits), Ok(None));
        assert_eq!(parse_one_with(b"*3\r\n", limits), Err(ProtocolError::InvalidMultibulkLength));
    }

    #[test]
    fn bad_bulk_lengths() {
        for req in [
            &b"*1\r\n$-1\r\n"[..],
            b"*1\r\n$abc\r\n",
            b"*1\r\n$\r\n",
            b"*1\r\n$01\r\n",
            b"*1\r\n$0000\r\n",
            b"*1\r\n$1 \r\n",
            b"*1\r\n$536870913\r\n",
            b"*1\r\n$1000000000000000000\r\n",
            b"*1\r\n$99999999999999999999\r\n",
        ] {
            assert_eq!(parse_one(req), Err(ProtocolError::InvalidBulkLength), "{}", req.escape_ascii());
        }
        assert_eq!(parse_one(b"*1\r\n$536870912\r\n"), Ok(None));
        let limits = Limits { max_bulk_len: 3, ..Limits::default() };
        assert_eq!(parse_one_with(b"*1\r\n$3\r\nabc\r\n", limits), Ok(Some((13, args(&[b"abc"])))));
        assert_eq!(parse_one_with(b"*1\r\n$4\r\n", limits), Err(ProtocolError::InvalidBulkLength));
    }

    #[test]
    fn expected_dollar() {
        assert_eq!(parse_one(b"*1\r\nX4\r\nPING\r\n"), Err(ProtocolError::ExpectedDollar(b'X')));
        assert_eq!(parse_one(b"*1\r\n\r\n"), Err(ProtocolError::ExpectedDollar(b'\r')));
        assert_eq!(parse_one(b"*2\r\n$1\r\na\r\n:1\r\n"), Err(ProtocolError::ExpectedDollar(b':')));
        // The line must be complete before the '$' is checked, as in Redis.
        assert_eq!(parse_one(b"*1\r\nX"), Ok(None));
    }

    #[test]
    fn too_big_without_a_line_end() {
        let limits = Limits { max_inline_len: 10, ..Limits::default() };
        // More than 10 bytes counting from the '*' or '$', as Redis counts from the line start.
        assert_eq!(parse_one_with(b"*123456789", limits), Ok(None));
        assert_eq!(parse_one_with(b"*1234567890", limits), Err(ProtocolError::TooBigMultibulkCount));
        assert_eq!(parse_one_with(b"*1\r\n$123456789", limits), Ok(None));
        assert_eq!(parse_one_with(b"*1\r\n$1234567890", limits), Err(ProtocolError::TooBigBulkCount));
        assert_eq!(parse_one_with(b"*1\r\nxxxxxxxxxxx", limits), Err(ProtocolError::TooBigBulkCount));
        assert_eq!(parse_one_with(b"aaaaaaaaaa", limits), Ok(None));
        assert_eq!(parse_one_with(b"aaaaaaaaaaa", limits), Err(ProtocolError::TooBigInlineRequest));
        // A line end that has arrived counts, however far away.
        assert_eq!(parse_one_with(b"*00000000000000\r\n", limits), Err(ProtocolError::InvalidMultibulkLength));
        assert_eq!(parse_one_with(b"ECHO aaaaaaaaaaaaaaaa\n", limits), Ok(Some((22, args(&[b"ECHO", b"aaaaaaaaaaaaaaaa"])))));
        // A trailing \r means the line end is arriving: wait however long it is.
        assert_eq!(parse_one_with(b"*00000000000000\r", limits), Ok(None));
    }

    #[test]
    fn too_big_with_redis_defaults() {
        // Checked against Redis 7.0.15: 64 KiB wait, one byte more fails.
        let mut req = vec![b'a'; 65536];
        assert_eq!(parse_one(&req), Ok(None));
        req.push(b'a');
        assert_eq!(parse_one(&req), Err(ProtocolError::TooBigInlineRequest));
        let mut req = b"*1\r\n".to_vec();
        req.resize(4 + 65536, b'x');
        assert_eq!(parse_one(&req), Ok(None));
        req.push(b'x');
        assert_eq!(parse_one(&req), Err(ProtocolError::TooBigBulkCount));
    }

    #[test]
    fn inline_splitting() {
        assert_eq!(inline(b"GET key\r\n"), Ok(args(&[b"GET", b"key"])));
        assert_eq!(inline(b"GET key\n"), Ok(args(&[b"GET", b"key"])));
        assert_eq!(inline(b"  SET\t a  b \r\n"), Ok(args(&[b"SET", b"a", b"b"])));
        assert_eq!(inline(b"ECHO a\r\r\n"), Ok(args(&[b"ECHO", b"a"])));
        assert_eq!(inline(b"ECHO a\x0bb\r\n"), Ok(args(&[b"ECHO", b"a\x0bb"])));
        assert_eq!(inline(b"ECHO \x0b\x0cb\r\n"), Ok(args(&[b"ECHO", b"b"])));
        assert_eq!(inline(b"ECHO a\0b\r\n"), Ok(args(&[b"ECHO", b"a\0b"])));
        assert_eq!(inline(b"ECHO \xff\xfe\n"), Ok(args(&[b"ECHO", b"\xff\xfe"])));
    }

    #[test]
    fn inline_quotes() {
        // Each checked against Redis 7.0.15 with ECHO.
        assert_eq!(inline(b"ECHO \"a b\"\r\n"), Ok(args(&[b"ECHO", b"a b"])));
        assert_eq!(inline(b"ECHO \"\"\r\n"), Ok(args(&[b"ECHO", b""])));
        assert_eq!(inline(b"ECHO ''\r\n"), Ok(args(&[b"ECHO", b""])));
        assert_eq!(inline(b"ECHO a\"b c\"\r\n"), Ok(args(&[b"ECHO", b"ab c"])));
        assert_eq!(inline(b"ECHO \"a\" b\r\n"), Ok(args(&[b"ECHO", b"a", b"b"])));
        assert_eq!(inline(b"ECHO \"a\"\x0b\r\n"), Ok(args(&[b"ECHO", b"a"])));
        assert_eq!(inline(b"ECHO 'a'\t\r\n"), Ok(args(&[b"ECHO", b"a"])));
        assert_eq!(inline(b"ECHO 'it\\'s'\r\n"), Ok(args(&[b"ECHO", b"it's"])));
        assert_eq!(inline(b"ECHO 'a\\nb'\r\n"), Ok(args(&[b"ECHO", b"a\\nb"])));
        assert_eq!(inline(b"ECHO \"\\x41\\x4a\\n\\q\\xZZ\\\"\"\r\n"), Ok(args(&[b"ECHO", b"AJ\nqxZZ\""])));
        assert_eq!(inline(b"ECHO \"\\r\\t\\b\\a\\\\\"\r\n"), Ok(args(&[b"ECHO", b"\r\t\x08\x07\\"])));
        assert_eq!(inline(b"ECHO \"\\x4\"\r\n"), Ok(args(&[b"ECHO", b"x4"])));
        assert_eq!(inline(b"ECHO \"\\xff\\x00\"\r\n"), Ok(args(&[b"ECHO", b"\xff\0"])));
        assert_eq!(inline(b"ECHO \"\\xAb\\xFF\\x9e\"\r\n"), Ok(args(&[b"ECHO", b"\xab\xff\x9e"])));
    }

    #[test]
    fn inline_unbalanced_quotes() {
        // Each checked against Redis 7.0.15.
        for line in [
            &b"ECHO \"abc\r\n"[..],
            b"ECHO 'abc\r\n",
            b"ECHO \"a\"b\r\n",
            b"ECHO 'a'b\r\n",
            b"ECHO a'b c'd\r\n",
            b"ECHO \"a\\\"\r\n",
            b"ECHO \"a\\\r\n",
            b"PING \"a\n",
        ] {
            assert_eq!(parse_one(line), Err(ProtocolError::UnbalancedQuotes), "{}", line.escape_ascii());
        }
    }

    #[test]
    fn blank_inline_lines_are_no_command() {
        for line in [&b"\r\n"[..], b"\n", b"   \r\n", b"\t\x0b\x0c\n"] {
            assert_eq!(parse_one(line), Ok(Some((line.len(), vec![]))), "{}", line.escape_ascii());
        }
    }

    #[test]
    fn inline_waits_for_the_newline() {
        assert_eq!(parse_one(b"PING"), Ok(None));
        assert_eq!(parse_one(b"PING\r"), Ok(None));
        assert_eq!(parse_one(b"\r"), Ok(None));
    }

    #[test]
    fn pipeline_one_request_per_call() {
        let input = b"PING\r\n*0\r\n*1\r\n$4\r\nPING\r\n\nGET 'k'\n*1\r\n$1\r\n";
        let mut p = Parser::new(Limits::default());
        let mut start = 0;
        let mut seen = Vec::new();
        while let Parsed::Complete { consumed } = p.parse(&input[start..]).unwrap() {
            seen.push(p.args(&input[start..]).iter().map(<[u8]>::to_vec).collect::<Vec<_>>());
            start += consumed;
        }
        assert_eq!(seen, vec![args(&[b"PING"]), vec![], args(&[b"PING"]), vec![], args(&[b"GET", b"k"])]);
        assert_eq!(&input[start..], b"*1\r\n$1\r\n");
    }

    #[test]
    fn resumes_a_request_read_in_pieces() {
        let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
        let mut p = Parser::new(Limits::default());
        for end in 0..req.len() {
            assert_eq!(p.parse(&req[..end]), Ok(Parsed::Incomplete), "at {end}");
        }
        assert_eq!(p.parse(req), Ok(Parsed::Complete { consumed: req.len() }));
        let a: Vec<_> = p.args(req).iter().collect();
        assert_eq!(a, [&b"SET"[..], b"k", b"value"]);
        // And the parser is ready for the next request.
        assert_eq!(p.parse(b"PING\n"), Ok(Parsed::Complete { consumed: 5 }));
        assert_eq!(p.args(b"PING\n").iter().collect::<Vec<_>>(), [b"PING"]);
    }

    #[test]
    fn the_buffer_may_move_between_calls() {
        let req = b"*2\r\n$4\r\nECHO\r\n$3\r\nabc\r\n";
        let mut p = Parser::new(Limits::default());
        let first = req[..12].to_vec();
        assert_eq!(p.parse(&first), Ok(Parsed::Incomplete));
        drop(first);
        let second = req.to_vec();
        assert_eq!(p.parse(&second), Ok(Parsed::Complete { consumed: req.len() }));
        assert_eq!(&p.args(&second)[1], b"abc");
    }

    #[test]
    fn errors_reset_the_parser() {
        let mut p = Parser::new(Limits::default());
        assert_eq!(p.parse(b"*2\r\n$1\r\na\r\n"), Ok(Parsed::Incomplete));
        assert_eq!(p.parse(b"*2\r\n$1\r\na\r\n!"), Ok(Parsed::Incomplete));
        assert_eq!(p.parse(b"*2\r\n$1\r\na\r\n!\r\n"), Err(ProtocolError::ExpectedDollar(b'!')));
        assert_eq!(p.parse(b"PING\n"), Ok(Parsed::Complete { consumed: 5 }));
        assert_eq!(p.args(b"PING\n").len(), 1);
    }

    #[test]
    fn reset_forgets_a_partial_request() {
        let mut p = Parser::new(Limits::default());
        assert_eq!(p.parse(b"*2\r\n$1\r\na\r\n"), Ok(Parsed::Incomplete));
        p.reset();
        assert_eq!(p.parse(b"*1\r\n$1\r\nb\r\n"), Ok(Parsed::Complete { consumed: 11 }));
        assert_eq!(p.args(b"*1\r\n$1\r\nb\r\n").len(), 1);
    }

    #[test]
    fn inline_args_survive_until_the_next_parse() {
        let mut p = Parser::new(Limits::default());
        assert_eq!(p.parse(b"SET \"a b\" c\n"), Ok(Parsed::Complete { consumed: 12 }));
        let a = p.args(b"ignored for inline requests");
        assert_eq!(a.iter().collect::<Vec<_>>(), [&b"SET"[..], b"a b", b"c"]);
    }

    #[test]
    fn integers_like_string2ll() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"7"), Some(7));
        assert_eq!(parse_i64(b"-7"), Some(-7));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [&b""[..], b"-", b"+1", b"01", b"-0", b"00", b" 1", b"1 ", b"1a", b"9223372036854775808",
                    b"-9223372036854775809", b"18446744073709551616", b"-18446744073709551616", b"184467440737095516160"] {
            assert_eq!(parse_i64(bad), None, "{}", bad.escape_ascii());
        }
    }

    #[test]
    fn error_replies() {
        // Each reply checked byte for byte against Redis 7.0.15.
        let cases: [(ProtocolError, &[u8]); 9] = [
            (ProtocolError::InvalidMultibulkLength, b"-ERR Protocol error: invalid multibulk length\r\n"),
            (ProtocolError::InvalidBulkLength, b"-ERR Protocol error: invalid bulk length\r\n"),
            (ProtocolError::ExpectedDollar(b'X'), b"-ERR Protocol error: expected '$', got 'X'\r\n"),
            (ProtocolError::ExpectedDollar(b'\r'), b"-ERR Protocol error: expected '$', got ' '\r\n"),
            (ProtocolError::ExpectedDollar(b'\n'), b"-ERR Protocol error: expected '$', got ' '\r\n"),
            (ProtocolError::ExpectedDollar(0xff), b"-ERR Protocol error: expected '$', got '\xff'\r\n"),
            (ProtocolError::TooBigInlineRequest, b"-ERR Protocol error: too big inline request\r\n"),
            (ProtocolError::TooBigMultibulkCount, b"-ERR Protocol error: too big mbulk count string\r\n"),
            (ProtocolError::TooBigBulkCount, b"-ERR Protocol error: too big bulk count string\r\n"),
        ];
        for (e, want) in cases {
            for proto in [crate::Protocol::Resp2, crate::Protocol::Resp3] {
                let mut enc = Encoder::new(proto);
                e.encode(&mut enc);
                assert_eq!(enc.as_bytes().escape_ascii().to_string(), want.escape_ascii().to_string());
            }
        }
        let mut enc = Encoder::new(crate::Protocol::Resp2);
        ProtocolError::UnbalancedQuotes.encode(&mut enc);
        assert_eq!(enc.as_bytes(), b"-ERR Protocol error: unbalanced quotes in request\r\n");
    }

    #[test]
    fn error_display() {
        assert_eq!(ProtocolError::InvalidBulkLength.to_string(), "Protocol error: invalid bulk length");
        assert_eq!(ProtocolError::ExpectedDollar(b'\n').to_string(), "Protocol error: expected '$', got '\\n'");
    }
}
