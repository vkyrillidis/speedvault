//! The Redis protocol (RESP2 and RESP3): request parsing and reply encoding.
//!
//! A connection reads bytes into a buffer and hands them to a [`Parser`],
//! which cuts off one request at a time (pipelining) and remembers how far it
//! got when a request is still incomplete (partial reads). The arguments are
//! byte ranges into the caller's buffer: parsing a multibulk request copies
//! nothing and, once the parser's vectors have grown, allocates nothing.
//!
//! Replies go through an [`Encoder`], which appends them to a `Vec<u8>` in
//! the protocol the connection speaks (RESP2 until `HELLO 3`), so one batch of
//! pipelined replies leaves in a single `write`.
//!
//! ```
//! use resp::{Encoder, Limits, Parsed, Parser, Protocol};
//!
//! let mut parser = Parser::new(Limits::default());
//! let mut replies = Encoder::new(Protocol::Resp2);
//! let input = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n*1\r\n$4\r\nPI";
//! let mut start = 0;
//! loop {
//!     match parser.parse(&input[start..]).unwrap() {
//!         Parsed::Complete { consumed } => {
//!             let args = parser.args(&input[start..]);
//!             if !args.is_empty() {
//!                 replies.bulk(&args[0]); // execute the command here
//!             }
//!             start += consumed;
//!         }
//!         Parsed::Incomplete => break, // read more, call again from `start`
//!     }
//! }
//! assert_eq!(replies.as_bytes(), b"$3\r\nGET\r\n$4\r\nPING\r\n");
//! assert_eq!(start, 26); // "*1\r\n$4\r\nPI" waits for the rest
//! ```
//!
//! Behaviour, error texts and limits follow Redis 7.0 (`networking.c`:
//! `processMultibulkBuffer`, `processInlineBuffer`; `sds.c`: `sdssplitargs`)
//! and were checked against a Redis 7.0.15 server byte for byte.

mod encode;
mod parse;

pub use encode::{format_double, format_integer, Encoder, Protocol};
pub use parse::{Args, Limits, Parsed, Parser, ProtocolError};
