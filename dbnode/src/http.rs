//! A small HTTP/1.1 responder for Prometheus and Kubernetes' probes
//! (`--metrics HOST:PORT`), on the node's reactor, used by the node and by
//! stuart:
//!
//! - `GET /metrics`: the [`Site`]'s metrics, in Prometheus' text format
//!   (version 0.0.4);
//! - `GET /healthz`: `200` while the reactor answers (a liveness probe);
//! - `GET /ready`: `200` if the site is [ready](Site::ready), else `503`
//!   with the reason (a readiness probe).
//!
//! It reads one request's head (the request line and headers, 8 KiB at
//! most, within 5 seconds), answers and closes the connection: a scrape a
//! second doesn't need keep-alive, and it keeps this small. Only `GET`
//! (and `HEAD`) are served. No fiber blocks: a slow client only holds its
//! own.

use crate::warning;
use reactor::{sleep, spawn, TcpListener, TcpStream};
use std::net::Shutdown;
use std::fmt::{Display, Write as _};
use std::io::{self, Read, Write};
use std::net;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// How long a client may take to send its request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The most a request's head may take.
const MAX_HEAD: usize = 8 * 1024;
/// After the reply, what the client sent beyond the head (a body, the rest
/// of a request too large) is read and dropped, this much at most, for this
/// long at most: closing with unread bytes would reset the connection, and
/// the client could lose the reply.
const DRAIN_MAX: usize = 64 * 1024;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// What a server serves over HTTP.
pub trait Site {
    /// Appends the `/metrics` page (see [`family`] and [`sample`]).
    fn metrics(&self, out: &mut String);
    /// Whether it should get traffic (`/ready`); the error says why not.
    fn ready(&self) -> Result<(), String>;
}

/// Serves `site` on `listener`, in a fiber of the thread's reactor.
///
/// # Panics
/// If the thread has no reactor.
pub fn spawn_server(listener: net::TcpListener, site: Rc<dyn Site>) -> io::Result<()> {
    let listener = TcpListener::from_std(listener)?;
    spawn(move || loop {
        match listener.accept() {
            Ok((conn, _)) => {
                let site = site.clone();
                if let Err(e) = spawn(move || handle(&conn, &*site)) {
                    warning!("No fiber for a new metrics connection, closing it: {e}");
                }
            }
            Err(e) => {
                warning!("Accepting a metrics connection failed: {e}");
                sleep(Duration::from_millis(10));
            }
        }
    })
}

/// A reply: status line, content type, body.
struct Reply {
    status: &'static str,
    kind: &'static str,
    body: String,
    head_only: bool,
}

fn handle(mut conn: &TcpStream, site: &dyn Site) {
    let reply = match read_head(conn) {
        Ok(Some(head)) => respond(&head, site),
        // Closed before a whole request, or too slow: nothing to answer.
        Ok(None) => return,
        Err(e) if e.kind() == io::ErrorKind::InvalidData => text("400 Bad Request", "bad request\n"),
        Err(_) => return,
    };
    let mut out = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.status,
        reply.kind,
        reply.body.len()
    );
    if !reply.head_only {
        out.push_str(&reply.body);
    }
    if conn.write_all(out.as_bytes()).is_ok() && conn.shutdown(Shutdown::Write).is_ok() {
        drain(conn);
    }
}

/// Reads until the client closes (it has the whole reply), for a while.
fn drain(mut conn: &TcpStream) {
    let deadline = Instant::now() + DRAIN_TIMEOUT;
    let (mut buf, mut drained) = ([0; 4096], 0);
    while drained < DRAIN_MAX {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || conn.set_read_timeout(Some(left)).is_err() {
            return;
        }
        match conn.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => drained += n,
        }
    }
}

/// The request's head, up to the empty line. `None` if the client closed
/// the connection or took too long; `InvalidData` if it is too large.
fn read_head(mut conn: &TcpStream) -> io::Result<Option<Vec<u8>>> {
    let deadline = Instant::now() + REQUEST_TIMEOUT;
    let mut head = Vec::with_capacity(512);
    let mut buf = [0; 1024];
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Ok(None);
        }
        conn.set_read_timeout(Some(left))?;
        let n = match conn.read(&mut buf) {
            Ok(0) => return Ok(None),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::TimedOut => return Ok(None),
            Err(e) => return Err(e),
        };
        // The end may straddle two reads.
        let from = head.len().saturating_sub(3);
        head.extend_from_slice(&buf[..n]);
        if let Some(end) = head[from..].windows(4).position(|w| w == b"\r\n\r\n") {
            head.truncate(from + end);
            return Ok(Some(head));
        }
        if head.len() > MAX_HEAD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "request head too large"));
        }
    }
}

fn respond(head: &[u8], site: &dyn Site) -> Reply {
    let line = head.split(|&b| b == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line.strip_suffix(b"\r").unwrap_or(line));
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return text("400 Bad Request", "bad request\n");
    };
    if !version.starts_with("HTTP/1.") || !target.starts_with('/') {
        return text("400 Bad Request", "bad request\n");
    }
    let head_only = match method {
        "GET" => false,
        "HEAD" => true,
        _ => return text("405 Method Not Allowed", "only GET\n"),
    };
    let path = target.split('?').next().unwrap_or(target);
    let mut reply = match path {
        "/metrics" => {
            let mut body = String::with_capacity(4096);
            site.metrics(&mut body);
            Reply { status: "200 OK", kind: "text/plain; version=0.0.4; charset=utf-8", body, head_only }
        }
        "/healthz" => text("200 OK", "ok\n"),
        "/ready" => match site.ready() {
            Ok(()) => text("200 OK", "ready\n"),
            Err(why) => text("503 Service Unavailable", &format!("not ready: {why}\n")),
        },
        _ => text("404 Not Found", "not found: try /metrics, /healthz or /ready\n"),
    };
    reply.head_only = head_only;
    reply
}

fn text(status: &'static str, body: &str) -> Reply {
    Reply { status, kind: "text/plain; charset=utf-8", body: body.to_string(), head_only: false }
}

/// The kind of a metric family.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Counter,
    Gauge,
}

/// Starts a metric family: its `# HELP` and `# TYPE` lines. Its samples
/// follow, with [`sample`].
pub fn family(out: &mut String, name: &str, kind: Kind, help: &str) {
    let kind = match kind {
        Kind::Counter => "counter",
        Kind::Gauge => "gauge",
    };
    let _ = write!(out, "# HELP {name} {}\n# TYPE {name} {kind}\n", help.replace('\\', "\\\\").replace('\n', "\\n"));
}

/// One sample: `name{label="value",...} value`.
pub fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: impl Display) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (label, v)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{label}=\"");
            for c in v.chars() {
                match c {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        out.push('}');
    }
    let _ = writeln!(out, " {value}");
}

/// A family of one sample without labels.
pub fn metric(out: &mut String, name: &str, kind: Kind, help: &str, value: impl Display) {
    family(out, name, kind, help);
    sample(out, name, &[], value);
}

/// Seconds, as Prometheus wants durations.
pub fn seconds(d: Duration) -> f64 {
    d.as_secs_f64()
}

/// The CPU time the process has used, user and system, all threads.
pub fn cpu_time() -> Duration {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Duration::ZERO;
    }
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl Site for Fake {
        fn metrics(&self, out: &mut String) {
            metric(out, "x_total", Kind::Counter, "Things.", 3);
        }
        fn ready(&self) -> Result<(), String> {
            Err("leaving".into())
        }
    }

    fn reply(head: &str) -> (String, String) {
        let r = respond(head.as_bytes(), &Fake);
        (r.status.to_string(), if r.head_only { String::new() } else { r.body })
    }

    #[test]
    fn requests() {
        let metrics = "# HELP x_total Things.\n# TYPE x_total counter\nx_total 3\n";
        assert_eq!(reply("GET /metrics HTTP/1.1\r\nHost: a"), ("200 OK".into(), metrics.into()));
        assert_eq!(reply("GET /metrics?x=1 HTTP/1.0"), ("200 OK".into(), metrics.into()));
        assert_eq!(reply("HEAD /metrics HTTP/1.1").1, "");
        assert_eq!(reply("GET /healthz HTTP/1.1"), ("200 OK".into(), "ok\n".into()));
        assert_eq!(reply("GET /ready HTTP/1.1"), ("503 Service Unavailable".into(), "not ready: leaving\n".into()));
        assert_eq!(reply("GET / HTTP/1.1").0, "404 Not Found");
        assert_eq!(reply("POST /metrics HTTP/1.1").0, "405 Method Not Allowed");
        for bad in ["", "GET", "GET /metrics", "GET /metrics HTTP/2", "GET metrics HTTP/1.1", "GET /metrics HTTP/1.1 x", "\u{0}\u{1}"] {
            assert_eq!(reply(bad).0, "400 Bad Request", "{bad:?}");
        }
    }

    #[test]
    fn format() {
        let mut out = String::new();
        family(&mut out, "a", Kind::Gauge, "Line\none \\ two.");
        sample(&mut out, "a", &[("node", "x\"y\\z\nw"), ("addr", "h:1")], 0.5);
        sample(&mut out, "a", &[], 7);
        assert_eq!(out, "# HELP a Line\\none \\\\ two.\n# TYPE a gauge\na{node=\"x\\\"y\\\\z\\nw\",addr=\"h:1\"} 0.5\na 7\n");
    }
}
