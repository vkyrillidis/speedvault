//! Differential test against a real Redis: random streams of `ECHO`
//! requests, valid or with garbage at the end, go to Redis, and its replies
//! must be exactly what our parser plus a one-command executor produce:
//! the echoes, then for a protocol error `-ERR Protocol error: ...` and a
//! closed connection, or else nothing more and an open connection.
//!
//! Needs a server, so it's ignored by default:
//!
//! ```sh
//! redis-server --port 6391 --save '' --appendonly no --daemonize yes
//! cargo test -p resp --test redis_diff -- --ignored
//! redis-cli -p 6391 shutdown nosave
//! ```
//!
//! `RESP_REDIS_ADDR` overrides the address (default `127.0.0.1:6391`).

mod common;

use common::Rng;
use resp::{Encoder, Limits, Parsed, Parser, Protocol};
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// What Redis should send back for `stream`, and whether it then closes
/// the connection. `None` if the stream holds a request other than a valid
/// `ECHO`, whose reply we don't model.
fn expected(stream: &[u8]) -> Option<(Vec<u8>, bool)> {
    let mut parser = Parser::new(Limits::default());
    let mut out = Encoder::new(Protocol::Resp2);
    let mut start = 0;
    loop {
        match parser.parse(&stream[start..]) {
            Ok(Parsed::Complete { consumed }) => {
                let args = parser.args(&stream[start..]);
                if !args.is_empty() {
                    if args.len() != 2 || !args[0].eq_ignore_ascii_case(b"ECHO") {
                        return None;
                    }
                    out.bulk(&args[1]);
                }
                start += consumed;
            }
            Ok(Parsed::Incomplete) => return Some((out.into_inner(), false)),
            Err(e) => {
                e.encode(&mut out);
                return Some((out.into_inner(), true));
            }
        }
    }
}

/// Sends `stream` and collects the reply: everything up to the close, or
/// what arrived before Redis went quiet.
fn ask_redis(addr: &str, stream: &[u8], want_len: usize) -> (Vec<u8>, bool) {
    let mut conn = TcpStream::connect(addr).expect("a Redis server at RESP_REDIS_ADDR (see the file comment)");
    conn.write_all(stream).unwrap();
    let mut reply = Vec::new();
    let mut buf = [0; 65536];
    loop {
        // Generous until the expected bytes are in, then just long enough to see extra bytes or a close.
        let wait = if reply.len() < want_len { 2000 } else { 50 };
        conn.set_read_timeout(Some(Duration::from_millis(wait))).unwrap();
        match conn.read(&mut buf) {
            Ok(0) => return (reply, true),
            Ok(n) => reply.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => return (reply, false),
            Err(e) if e.kind() == ErrorKind::ConnectionReset => return (reply, true),
            Err(e) => panic!("{e}"),
        }
    }
}

fn echo_request(rng: &mut Rng, out: &mut Vec<u8>) {
    let name: &[u8] = [&b"ECHO"[..], b"echo", b"EcHo"][rng.below(3)];
    let arg: Vec<u8> = (0..rng.below(12)).map(|_| rng.byte()).collect();
    match rng.below(3) {
        0 => {
            out.extend_from_slice(format!("*2\r\n${}\r\n", name.len()).as_bytes());
            out.extend_from_slice(name);
            out.extend_from_slice(format!("\r\n${}\r\n", arg.len()).as_bytes());
            out.extend_from_slice(&arg);
            out.extend_from_slice(b"\r\n");
        }
        1 => {
            out.extend_from_slice(name);
            out.extend_from_slice(b" \"");
            for &b in &arg {
                match b {
                    b'"' | b'\\' => out.extend_from_slice(&[b'\\', b]),
                    b'a'..=b'z' | b'0'..=b'9' => out.push(b),
                    _ => out.extend_from_slice(format!("\\x{b:02X}").as_bytes()),
                }
            }
            out.extend_from_slice(b"\"\r\n");
        }
        _ => {
            out.extend_from_slice(name);
            out.extend_from_slice(b" x");
            out.extend(arg.iter().filter(|b| b.is_ascii_alphanumeric()));
            out.extend_from_slice(b"\n");
        }
    }
}

#[test]
#[ignore = "needs a Redis server, see the file comment"]
fn replies_match_redis() {
    let addr = std::env::var("RESP_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6391".into());
    let mut rng = Rng(0xFEED_FACE_DEAD_BEEF);
    let (mut checked, mut errors, mut open) = (0, 0, 0);
    while checked < 600 {
        let mut stream = Vec::new();
        for _ in 0..rng.below(4) {
            echo_request(&mut rng, &mut stream);
        }
        if rng.chance(80) {
            const PREFIXES: [&[u8]; 10] =
                [b"", b"*", b"*2\r\n", b"*2\r\n$", b"*1\r\n$4\r\n", b"ECHO \"", b"ECHO '", b"*1\r\n", b"ECHO \"a\"", b"*1\r\n$1"];
            stream.extend_from_slice(PREFIXES[rng.below(PREFIXES.len())]);
            // No NUL: Redis stops looking for a line end at one (see parse.rs), a difference we chose.
            stream.extend((0..rng.below(24)).map(|_| rng.byte()).filter(|&b| b != 0));
            if rng.chance(60) {
                stream.extend_from_slice(b"\r\n"); // ends a line, so more of the garbage gets judged
            }
        }
        let Some((want, closes)) = expected(&stream) else { continue };
        let (got, closed) = ask_redis(&addr, &stream, want.len());
        assert_eq!(
            (got.escape_ascii().to_string(), closed),
            (want.escape_ascii().to_string(), closes),
            "request: {}",
            stream.escape_ascii()
        );
        checked += 1;
        errors += closes as usize;
        open += !closes as usize;
    }
    println!("{checked} streams: {errors} protocol errors, {open} left open");
    assert!(errors > 150 && open > 150);
}
