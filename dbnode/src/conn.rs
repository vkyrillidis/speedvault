//! One client connection: read requests, run them, write the replies.
//!
//! The requests that one `read` brought in are run back to back and their
//! replies leave in one `write`, so a client pipelining N requests costs one
//! read and one write, not N of each. That write waits until the other
//! connections ready in the same turn of the reactor have run, so the
//! replies of a turn leave together. A request split over several reads
//! stays in the buffer until the rest arrives; the buffer grows for requests
//! larger than itself.

use crate::command::{self, Flow};
use crate::{unix_ms, verbose, warning, Node};
use reactor::TcpStream;
use resp::{Encoder, Limits, Parsed, Parser, Protocol, ProtocolError};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::time::Instant;

/// Initial size of the read buffer, and the least room a `read` gets.
const READ_SIZE: usize = 16 * 1024;
/// A read buffer that grew past this for a large request goes back to
/// `READ_SIZE` once it's empty again.
const KEEP_BUFFER: usize = 4 * READ_SIZE;
/// Replies are written before the batch is done once they reach this size,
/// so a long pipeline of large values doesn't pile up in memory.
const FLUSH_AT: usize = 64 * 1024;

/// Why a connection ended without an I/O error.
enum Closed {
    ByClient,
    Quit,
    Idle,
    ProtocolError(ProtocolError),
    /// It was a replica's (`REPLSYNC`), now gone.
    Replica,
}

pub(crate) fn handle(conn: TcpStream, node: &Node) {
    let peer = conn.peer_addr().map_or_else(|_| "?".to_string(), |a| a.to_string());
    verbose!("Accepted {peer}");
    match serve(&conn, node) {
        Ok(Closed::ByClient) => verbose!("Client {peer} closed the connection"),
        Ok(Closed::Quit) => verbose!("Client {peer} sent QUIT"),
        Ok(Closed::Idle) => verbose!("Closing idle client {peer}"),
        Ok(Closed::ProtocolError(e)) => verbose!("{e} from client {peer}, closing it"),
        Ok(Closed::Replica) => verbose!("Replica {peer} went away"),
        // A client going away mid-write is routine.
        Err(e) if matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe) => {
            verbose!("Client {peer} went away: {e}")
        }
        Err(e) => warning!("Connection from {peer} failed: {e}"),
    }
}

fn serve(mut conn: &TcpStream, node: &Node) -> io::Result<Closed> {
    // Listed in CLIENT LIST until this function returns, however it returns.
    let registration = node.clients.register(conn.peer_addr().ok(), conn.local_addr().ok(), conn.as_raw_fd());
    let client = &*registration.client;
    conn.set_read_timeout(node.config.idle_timeout)?;
    let mut parser = Parser::new(Limits::default());
    let mut out = Encoder::new(Protocol::Resp2);
    let mut buf = vec![0; READ_SIZE];
    // Unparsed bytes: buf[start..end]. A request is always parsed from its
    // first byte, so `start` only moves past whole requests.
    let (mut start, mut end) = (0, 0);
    loop {
        node.store.borrow_mut().set_now(unix_ms());
        client.last_active.set(Instant::now());
        let mut closed = None;
        while closed.is_none() {
            match parser.parse(&buf[start..end]) {
                Ok(Parsed::Complete { consumed }) => {
                    let args = parser.args(&buf[start..end]);
                    // A replica asking for the stream: the connection is
                    // its feed from here on (see `repl`).
                    if args.len() == 4 && args[0].eq_ignore_ascii_case(b"REPLSYNC") {
                        if let Some(feed) = crate::repl::primary::attach(&args, node, &mut out) {
                            conn.write_all(out.as_bytes())?;
                            crate::repl::primary::serve(conn, node, feed)?;
                            return Ok(Closed::Replica);
                        }
                        start += consumed;
                        continue;
                    }
                    // A node syncing slots from here (a slot moving by
                    // snapshot): the same, a feed of those slots.
                    if args.len() >= 3 && args[0].eq_ignore_ascii_case(b"SLOTSYNC") {
                        if let Some(feed) = crate::repl::primary::attach_slots(&args, node, &mut out) {
                            conn.write_all(out.as_bytes())?;
                            crate::repl::primary::serve(conn, node, feed)?;
                            return Ok(Closed::Replica);
                        }
                        start += consumed;
                        continue;
                    }
                    // No arguments (`*0`, a blank line): nothing to run, no reply.
                    if !args.is_empty() && command::execute(&args, node, client, &mut out) == Flow::Close {
                        closed = Some(Closed::Quit);
                    }
                    start += consumed;
                    if out.len() >= FLUSH_AT {
                        conn.write_all(out.as_bytes())?;
                        out.clear();
                    }
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => {
                    // Like Redis: the error, then the connection is closed.
                    e.encode(&mut out);
                    closed = Some(Closed::ProtocolError(e));
                }
            }
        }
        if !out.is_empty() {
            // Write once the other connections that were ready have run
            // too, like Redis writes all replies just before it sleeps. A
            // reply written at once would wake its client while the server
            // still works through the others, and a client woken for every
            // reply costs both sides more than the batch: GET with 50
            // clients took 4.7 us of kernel time per request instead of 3.7.
            reactor::yield_now();
            conn.write_all(out.as_bytes())?;
            out.clear();
        }
        if let Some(closed) = closed {
            return Ok(closed);
        }

        // Room for the next read.
        if start == end {
            (start, end) = (0, 0);
            if buf.len() > KEEP_BUFFER {
                buf = vec![0; READ_SIZE];
            }
        } else if buf.len() - end < READ_SIZE {
            if start > 0 {
                buf.copy_within(start..end, 0);
                (start, end) = (0, end - start);
            }
            if buf.len() - end < READ_SIZE {
                buf.resize(buf.len().max(end + READ_SIZE) * 2, 0);
            }
        }
        let n = match conn.read(&mut buf[end..]) {
            Ok(n) => n,
            // Idle for longer than the timeout: closed without a word, as
            // Redis does. A request half sent counts as idle too.
            Err(e) if e.kind() == io::ErrorKind::TimedOut => return Ok(Closed::Idle),
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Ok(Closed::ByClient);
        }
        end += n;
    }
}
