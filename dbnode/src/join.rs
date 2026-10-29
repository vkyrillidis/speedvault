//! Joining stuart (`--stuart HOST:PORT`): a thread of its own sends
//! `STUART JOIN id host port [NAME name] [ZONE zone]` once a second until stuart
//! acknowledges it. It uses blocking sockets and resolves stuart's name again on every try
//! (a Kubernetes Service may not resolve yet when the pod starts), so
//! neither a slow DNS nor an absent stuart holds up the reactor: the node
//! serves `CLUSTERDOWN` meanwhile. Once joined, stuart drives: it pings the
//! node and pushes the slot table with `CLUSTER SETTABLE`.

use crate::{notice, warning, Announce};
use cluster::NodeId;
use resp::{Encoder, Protocol};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const RETRY: Duration = Duration::from_secs(1);
/// For connecting, and for each read and write.
const TIMEOUT: Duration = Duration::from_secs(2);

/// stuart, as a node reaches it: its `HOST:PORT` (`--stuart`), and where it
/// was last seen. Shared by the node's join and leave threads and its
/// connections (stuart's tables say where it is).
pub(crate) struct Stuart {
    pub(crate) addr: String,
    /// The address it last answered at, or its tables came from (with its
    /// port).
    seen: Mutex<Option<SocketAddr>>,
}

impl Stuart {
    pub(crate) fn new(addr: String) -> Stuart {
        Stuart { addr, seen: Mutex::new(None) }
    }

    /// A table came from stuart at `ip` (`CLUSTER SETTABLE`, which only
    /// stuart sends): stuart is there, at its port. Keeps the address
    /// current when stuart moves (its pod rescheduled), as the node itself
    /// calls stuart only to join and to leave.
    pub(crate) fn seen_at(&self, ip: IpAddr) {
        // main checked that it's HOST:PORT.
        if let Some(port) = self.addr.rsplit_once(':').and_then(|(_, p)| p.parse().ok()) {
            *self.seen.lock().unwrap_or_else(|e| e.into_inner()) = Some(SocketAddr::new(ip, port));
        }
    }

    /// Sends one request to stuart, over a connection of its own (stuart's
    /// name resolved again), and returns its one-line reply (`+OK`, `:3`,
    /// `-ERR ...`). Also for leaving (see `leave`). If the name doesn't
    /// resolve, or none of its addresses answers, the address stuart was
    /// last seen at: on Kubernetes stuart's Service may be deleted along
    /// with the nodes (by `kubectl delete -k`, before the pods get their
    /// SIGTERM), while stuart serves on at its pod's address for the nodes
    /// leaving.
    pub(crate) fn call(&self, args: &[&[u8]]) -> io::Result<String> {
        let seen = *self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let mut addrs: Vec<SocketAddr> = match self.addr.to_socket_addrs() {
            Ok(addrs) => addrs.collect(),
            Err(e) if seen.is_none() => return Err(e),
            Err(_) => Vec::new(),
        };
        if let Some(seen) = seen.filter(|s| !addrs.contains(s)) {
            addrs.push(seen);
        }
        let mut last = io::Error::new(io::ErrorKind::NotFound, "the name has no address");
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, TIMEOUT) {
                Ok(conn) => {
                    let reply = request(conn, args)?;
                    *self.seen.lock().unwrap_or_else(|e| e.into_inner()) = Some(addr);
                    return Ok(reply);
                }
                Err(e) => last = e,
            }
        }
        Err(last)
    }
}

/// Starts joining `stuart` in the background; `joined` is set once it has.
pub(crate) fn spawn(
    stuart: Arc<Stuart>,
    id: NodeId,
    announce: Announce,
    name: Option<String>,
    zone: Option<String>,
    joined: Arc<AtomicBool>,
) -> io::Result<()> {
    thread::Builder::new()
        .name("join".into())
        .spawn(move || join(&stuart, &id, &announce, name.as_deref(), zone.as_deref(), &joined))
        .map(drop)
}

fn join(stuart: &Stuart, id: &NodeId, announce: &Announce, name: Option<&str>, zone: Option<&str>, joined: &AtomicBool) {
    let mut failures = 0u64;
    loop {
        match join_once(stuart, id, announce, name, zone) {
            Ok(()) => {
                joined.store(true, Ordering::Relaxed);
                notice!("Joined stuart at {}", stuart.addr);
                return;
            }
            // The first failure, then one line a minute.
            Err(e) => {
                if failures.is_multiple_of(60) {
                    warning!("Can't join stuart at {} yet ({e}); retrying every second", stuart.addr);
                }
                failures += 1;
            }
        }
        thread::sleep(RETRY);
    }
}

fn join_once(stuart: &Stuart, id: &NodeId, announce: &Announce, name: Option<&str>, zone: Option<&str>) -> io::Result<()> {
    let port = announce.port.to_string();
    let mut args: Vec<&[u8]> = vec![b"STUART", b"JOIN", id.as_bytes(), announce.host.as_bytes(), port.as_bytes()];
    if let Some(name) = name {
        args.extend([b"NAME".as_slice(), name.as_bytes()]);
    }
    if let Some(zone) = zone {
        args.extend([b"ZONE".as_slice(), zone.as_bytes()]);
    }
    let reply = stuart.call(&args)?;
    match reply.as_bytes().first() {
        Some(b'+') => Ok(()),
        Some(b'-') => Err(io::Error::other(format!("stuart says: {}", &reply[1..]))),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unexpected reply {reply:?}"))),
    }
}

fn request(mut conn: TcpStream, args: &[&[u8]]) -> io::Result<String> {
    conn.set_read_timeout(Some(TIMEOUT))?;
    conn.set_write_timeout(Some(TIMEOUT))?;
    let mut req = Encoder::new(Protocol::Resp2);
    req.array(args.len());
    for arg in args {
        req.bulk(arg);
    }
    conn.write_all(req.as_bytes())?;

    let mut line = Vec::new();
    let mut byte = [0];
    while !line.ends_with(b"\r\n") {
        if line.len() > 1024 || conn.read(&mut byte)? == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "no reply"));
        }
        line.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&line[..line.len() - 2]).into_owned())
}
