//! Cost of parsing requests and encoding replies. Run with
//! `cargo bench -p resp`.
//!
//! Each case runs in a tight loop over a request already in memory, so the
//! numbers are the protocol's share of a request, without any I/O.

use resp::{Encoder, Limits, Parsed, Parser, Protocol};
use std::hint::black_box;
use std::time::Instant;

const ITERATIONS: u32 = 2_000_000;

fn main() {
    println!("{ITERATIONS} iterations, best of 5\n");

    let get = b"*2\r\n$3\r\nGET\r\n$8\r\nkey:1234\r\n";
    let set = [&b"*3\r\n$3\r\nSET\r\n$8\r\nkey:1234\r\n$64\r\n"[..], &[b'v'; 64], b"\r\n"].concat();
    let inline = b"GET key:1234\r\n";
    let mut pipeline = Vec::new();
    for i in 0..16 {
        pipeline.extend_from_slice(if i % 2 == 0 { get } else { &set[..] });
    }

    let mut parser = Parser::new(Limits::default());
    report("parse GET", 1, || parse_all(&mut parser, get));
    report("parse SET, 64-byte value", 1, || parse_all(&mut parser, &set));
    report("parse inline GET", 1, || parse_all(&mut parser, inline));
    report("parse 16-command pipeline", 16, || parse_all(&mut parser, &pipeline));
    report("parse SET in 2 reads", 1, || {
        let cut = set.len() / 2;
        assert_eq!(parser.parse(black_box(&set[..cut])), Ok(Parsed::Incomplete));
        parse_all(&mut parser, &set)
    });

    let value = [b'v'; 64];
    for protocol in [Protocol::Resp2, Protocol::Resp3] {
        println!();
        let mut out = Encoder::new(protocol);
        report(&format!("{protocol:?}: encode +OK"), 1, || encode(&mut out, |e| e.ok()));
        report(&format!("{protocol:?}: encode 64-byte bulk"), 1, || encode(&mut out, |e| e.bulk(black_box(&value))));
        report(&format!("{protocol:?}: encode null"), 1, || encode(&mut out, |e| e.null()));
        report(&format!("{protocol:?}: encode integer 1234567"), 1, || {
            encode(&mut out, |e| e.integer(black_box(1234567)))
        });
        report(&format!("{protocol:?}: encode double 1.5"), 1, || encode(&mut out, |e| e.double(black_box(1.5))));
        report(&format!("{protocol:?}: encode 16 replies"), 16, || {
            encode(&mut out, |e| {
                for i in 0..16 {
                    if i % 2 == 0 {
                        e.bulk(black_box(&value));
                    } else {
                        e.ok();
                    }
                }
            })
        });
    }
}

/// Parses every request in `buf`; returns the number of arguments seen.
fn parse_all(parser: &mut Parser, buf: &[u8]) -> usize {
    let buf = black_box(buf);
    let mut start = 0;
    let mut args = 0;
    while let Ok(Parsed::Complete { consumed }) = parser.parse(&buf[start..]) {
        args += black_box(parser.args(&buf[start..])).len();
        start += consumed;
    }
    args
}

/// Encodes into a buffer that is cleared once it holds 64 KiB, as a connection
/// would after writing a batch.
fn encode(out: &mut Encoder, f: impl Fn(&mut Encoder)) -> usize {
    if out.len() > 64 * 1024 {
        out.clear();
    }
    f(out);
    black_box(out.as_bytes()).len()
}

fn report(name: &str, per_round: u32, mut round: impl FnMut() -> usize) {
    let best = (0..5)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..ITERATIONS {
                black_box(round());
            }
            start.elapsed().as_nanos() as f64
        })
        .fold(f64::INFINITY, f64::min);
    let per_op = best / f64::from(ITERATIONS) / f64::from(per_round);
    println!("{name:36} {per_op:6.1} ns/op");
}
