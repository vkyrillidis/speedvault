//! Property tests: request streams cut into reads every which way must parse
//! into the same commands (and the same error) as when they arrive whole.
//! A tiny xorshift generator keeps the tests deterministic and dependency free.

mod common;

use common::Rng;
use resp::{Limits, Parsed, Parser, ProtocolError};

type Command = Vec<Vec<u8>>;

/// What a connection gets out of a stream: the commands, then either the
/// error that ends it or how many bytes of an incomplete request are left.
type Outcome = (Vec<Command>, Result<usize, ProtocolError>);

/// Feeds `stream` in reads of the given sizes, like a connection would,
/// parsing after each read. With `fresh`, every call gets a new parser
/// (re-parsing from the request start), which must change nothing.
/// `compact` occasionally moves the unparsed bytes to the buffer start.
fn drive(stream: &[u8], reads: &[usize], limits: Limits, fresh: bool, compact: Option<&mut Rng>) -> Outcome {
    let mut parser = Parser::new(limits);
    let mut buf = Vec::new();
    let mut start = 0;
    let mut commands = Vec::new();
    let mut fed = 0;
    let mut compact = compact;
    for &n in reads.iter().chain(std::iter::once(&usize::MAX)) {
        let end = fed + n.min(stream.len() - fed);
        buf.extend_from_slice(&stream[fed..end]);
        fed = end;
        loop {
            if fresh {
                parser = Parser::new(limits);
            }
            match parser.parse(&buf[start..]) {
                Ok(Parsed::Complete { consumed }) => {
                    assert!(consumed > 0 && consumed <= buf.len() - start);
                    commands.push(parser.args(&buf[start..]).iter().map(<[u8]>::to_vec).collect());
                    start += consumed;
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => return (commands, Err(e)),
            }
        }
        if let Some(rng) = compact.as_deref_mut() {
            if rng.chance(30) {
                buf.drain(..start);
                start = 0;
            }
        }
        if fed == stream.len() {
            break;
        }
    }
    (commands, Ok(buf.len() - start))
}

fn whole(stream: &[u8]) -> Outcome {
    drive(stream, &[stream.len()], Limits::default(), false, None)
}

fn random_reads(rng: &mut Rng, len: usize) -> Vec<usize> {
    let mut reads = Vec::new();
    let mut total = 0;
    while total < len {
        let n = match rng.below(4) {
            0 => 1,
            1 => 1 + rng.below(4),
            2 => 1 + rng.below(32),
            _ => 1 + rng.below(len),
        };
        reads.push(n);
        total += n;
    }
    reads
}

fn multibulk(rng: &mut Rng, out: &mut Vec<u8>) -> Command {
    let count = rng.below(6);
    out.extend_from_slice(format!("*{count}\r\n").as_bytes());
    let mut command = Vec::new();
    for _ in 0..count {
        let len = if rng.chance(5) { 1000 + rng.below(5000) } else { rng.below(20) };
        let arg: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        out.extend_from_slice(format!("${len}\r\n").as_bytes());
        out.extend_from_slice(&arg);
        out.extend_from_slice(b"\r\n");
        command.push(arg);
    }
    command
}

/// An inline request with bare, double-quoted and single-quoted arguments.
fn inline(rng: &mut Rng, out: &mut Vec<u8>) -> Command {
    const BARE: &[u8] = b"abcXYZ019*$:-_\0\xff";
    let count = rng.below(5);
    let mut command = Vec::new();
    if rng.chance(30) {
        out.extend_from_slice(&b" \t\x0b\x0c"[..1 + rng.below(4)]);
    }
    for i in 0..count {
        if i > 0 {
            out.extend_from_slice(&b" \t  "[..1 + rng.below(4)]);
        }
        let mut arg = Vec::new();
        match rng.below(3) {
            0 => {
                for j in 0..1 + rng.below(8) {
                    let b = match BARE[rng.below(BARE.len())] {
                        b'*' if i == 0 && j == 0 => b'a', // a leading '*' would make it a multibulk request
                        b => b,
                    };
                    arg.push(b);
                    out.push(b);
                }
            }
            1 => {
                out.push(b'"');
                for _ in 0..rng.below(8) {
                    let b = rng.byte();
                    arg.push(b);
                    match b {
                        b'"' | b'\\' => out.extend_from_slice(&[b'\\', b]),
                        b'\n' if rng.chance(50) => out.extend_from_slice(b"\\n"),
                        b'a'..=b'z' | b'0'..=b'9' | b' ' => out.push(b),
                        _ => out.extend_from_slice(format!("\\x{b:02x}").as_bytes()),
                    }
                }
                out.push(b'"');
            }
            _ => {
                out.push(b'\'');
                for _ in 0..rng.below(8) {
                    let b = b"ab \\'"[rng.below(5)];
                    if b == b'\\' {
                        continue; // a backslash before the closing quote would escape it
                    }
                    arg.push(b);
                    if b == b'\'' {
                        out.push(b'\\');
                    }
                    out.push(b);
                }
                out.push(b'\'');
            }
        }
        command.push(arg);
    }
    if rng.chance(30) {
        out.extend_from_slice(b"  ");
    }
    out.extend_from_slice(if rng.chance(50) { b"\r\n" } else { b"\n" });
    command
}

fn valid_stream(rng: &mut Rng) -> (Vec<u8>, Vec<Command>) {
    let mut stream = Vec::new();
    let mut commands = Vec::new();
    for _ in 0..1 + rng.below(12) {
        let command = if rng.chance(70) { multibulk(rng, &mut stream) } else { inline(rng, &mut stream) };
        commands.push(command);
    }
    (stream, commands)
}

#[test]
fn valid_streams_split_anywhere_give_the_same_commands() {
    let mut rng = Rng(0x1234_5678_9ABC_DEF1);
    for round in 0..2000 {
        let (stream, expected) = valid_stream(&mut rng);
        let want: Outcome = (expected, Ok(0));
        assert_eq!(whole(&stream), want, "round {round}: {}", stream.escape_ascii());
        let reads = random_reads(&mut rng, stream.len());
        let mut compact_rng = Rng(rng.next() | 1);
        assert_eq!(drive(&stream, &reads, Limits::default(), false, Some(&mut compact_rng)), want, "round {round}");
        assert_eq!(drive(&stream, &reads, Limits::default(), true, None), want, "round {round}, fresh parsers");
        if round % 10 == 0 && stream.len() < 2000 {
            for cut in 0..stream.len() {
                assert_eq!(drive(&stream, &[cut], Limits::default(), false, None), want, "round {round}, cut {cut}");
            }
            assert_eq!(drive(&stream, &vec![1; stream.len()], Limits::default(), false, None), want, "round {round}");
        }
    }
}

#[test]
fn a_truncated_stream_leaves_the_partial_request() {
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..300 {
        let (stream, expected) = valid_stream(&mut rng);
        // Cut inside the last request: everything before it comes out, the rest waits.
        let mut boundaries = vec![0];
        let mut parser = Parser::new(Limits::default());
        while let Ok(Parsed::Complete { consumed }) = parser.parse(&stream[*boundaries.last().unwrap()..]) {
            boundaries.push(boundaries.last().unwrap() + consumed);
        }
        assert_eq!(*boundaries.last().unwrap(), stream.len());
        let last_start = boundaries[boundaries.len() - 2];
        let cut = last_start + rng.below(stream.len() - last_start);
        let reads = random_reads(&mut rng, cut);
        let got = drive(&stream[..cut], &reads, Limits::default(), false, None);
        assert_eq!(got, (expected[..expected.len() - 1].to_vec(), Ok(cut - last_start)));
    }
}

#[test]
fn garbage_gives_the_same_outcome_however_it_is_split() {
    let mut rng = Rng(0x0BAD_C0DE_0BAD_C0DE);
    let mut errors = 0;
    for round in 0..20_000 {
        let len = rng.below(64);
        let mut stream: Vec<u8> = Vec::new();
        // Mostly garbage after a valid-looking prefix, so the parser gets deep before failing.
        if rng.chance(50) {
            let prefix: &[u8] = [&b"*"[..], b"*2\r\n", b"*2\r\n$", b"*1\r\n$3\r\n", b"GET \"", b"SET '"][rng.below(6)];
            stream.extend_from_slice(prefix);
        }
        stream.extend((0..len).map(|_| rng.byte()));
        let want = whole(&stream);
        errors += want.1.is_err() as usize;
        let reads = random_reads(&mut rng, stream.len());
        assert_eq!(drive(&stream, &reads, Limits::default(), false, None), want, "round {round}: {}", stream.escape_ascii());
        assert_eq!(drive(&stream, &reads, Limits::default(), true, None), want, "round {round}, fresh");
        assert_eq!(drive(&stream, &vec![1; stream.len()], Limits::default(), false, None), want, "round {round}");
    }
    assert!(errors > 1000, "the garbage should hit the error paths ({errors} errors)");
}

#[test]
fn garbage_with_tiny_limits_never_panics() {
    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    let limits = Limits { max_args: 3, max_bulk_len: 5, max_inline_len: 4 };
    let mut outcomes = [0usize; 3];
    for _ in 0..20_000 {
        let stream: Vec<u8> = (0..rng.below(48)).map(|_| rng.byte()).collect();
        let reads = random_reads(&mut rng, stream.len());
        let (commands, end) = drive(&stream, &reads, limits, false, None);
        assert!(commands.iter().flatten().all(|a| a.len() <= stream.len()));
        outcomes[match end {
            Ok(0) => 0,
            Ok(_) => 1,
            Err(_) => 2,
        }] += 1;
    }
    assert!(outcomes.iter().all(|&n| n > 100), "{outcomes:?}");
}
