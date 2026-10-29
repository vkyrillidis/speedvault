//! What the node knows about each connection, for `CLIENT` and `INFO`.
//!
//! A connection registers a [`Client`] when it starts and the guard
//! [`Registration`] removes it when the connection ends, however it ends.

use crate::multi::{Transaction, Watched};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::rc::Rc;
use std::time::Instant;

/// One connection, as `CLIENT LIST` shows it.
pub(crate) struct Client {
    /// Unique per node, from 1 up, never reused (`CLIENT ID`).
    pub(crate) id: u64,
    pub(crate) addr: Option<SocketAddr>,
    pub(crate) laddr: Option<SocketAddr>,
    pub(crate) fd: RawFd,
    pub(crate) created: Instant,
    /// When the last batch of requests arrived.
    pub(crate) last_active: Cell<Instant>,
    /// `CLIENT SETNAME`; never empty (setting "" removes the name).
    pub(crate) name: RefCell<Option<Vec<u8>>>,
    /// Full name of the last command looked up (`client|list`), "NULL" if
    /// the last request named no known command, as in Redis.
    pub(crate) last_cmd: Cell<&'static str>,
    /// 2 or 3, after `HELLO`.
    pub(crate) resp: Cell<u8>,
    /// `ASKING` was the last command: the next one may use a slot this
    /// node is importing (Redis's `CLIENT_ASKING`). Any other command
    /// clears it.
    pub(crate) asking: Cell<bool>,
    /// After `READONLY`, until `READWRITE`: the client may read a
    /// replica's primary's slots from it.
    pub(crate) readonly: Cell<bool>,
    /// The stream's offset after this client's last command that went
    /// into it (Redis's `woff`): what `WAIT` waits for the replicas to
    /// acknowledge.
    pub(crate) woff: Cell<u64>,
    /// `CLIENT SETINFO lib-name` and `lib-ver`; never empty.
    pub(crate) lib_name: RefCell<Option<Vec<u8>>>,
    pub(crate) lib_ver: RefCell<Option<Vec<u8>>>,
    /// `CLIENT NO-TOUCH on` (flag `T`). The store keeps no access times,
    /// so it changes nothing else.
    pub(crate) no_touch: Cell<bool>,
    /// It may run commands: it logged in (`AUTH`), or connected while
    /// the node had no password (see [`crate::auth`]). False until the
    /// connection says.
    pub(crate) authenticated: Cell<bool>,
    /// After `MULTI`, until `EXEC` or `DISCARD` (see [`crate::multi`]).
    pub(crate) multi: RefCell<Option<Transaction>>,
    /// While `EXEC` runs the queued commands.
    pub(crate) in_exec: Cell<bool>,
    /// The keys `WATCH`ed, and whether one was touched since (Redis's
    /// `CLIENT_DIRTY_CAS`: its keys are unwatched then).
    pub(crate) watched: RefCell<Vec<Watched>>,
    pub(crate) dirty_cas: Cell<bool>,
    /// Bytes read and written, commands run, reads that brought requests
    /// and the commands they brought (Redis 8's `tot-net-in`, `tot-net-out`,
    /// `tot-cmds`, `read-events`, `avg-pipeline-len-sum` and `-cnt`).
    pub(crate) net_in: Cell<u64>,
    pub(crate) net_out: Cell<u64>,
    pub(crate) cmds: Cell<u64>,
    pub(crate) read_events: Cell<u64>,
    pub(crate) pipelined: Cell<u64>,
    pub(crate) pipelines: Cell<u64>,
}

impl Client {
    /// A client just connected, with id `id`.
    pub(crate) fn new(id: u64, addr: Option<SocketAddr>, laddr: Option<SocketAddr>, fd: RawFd) -> Client {
        let now = Instant::now();
        Client {
            id,
            addr,
            laddr,
            fd,
            created: now,
            last_active: Cell::new(now),
            name: RefCell::new(None),
            last_cmd: Cell::new("NULL"),
            resp: Cell::new(2),
            asking: Cell::new(false),
            readonly: Cell::new(false),
            woff: Cell::new(0),
            lib_name: RefCell::new(None),
            lib_ver: RefCell::new(None),
            no_touch: Cell::new(false),
            authenticated: Cell::new(false),
            multi: RefCell::new(None),
            in_exec: Cell::new(false),
            watched: RefCell::new(Vec::new()),
            dirty_cas: Cell::new(false),
            net_in: Cell::new(0),
            net_out: Cell::new(0),
            cmds: Cell::new(0),
            read_events: Cell::new(0),
            pipelined: Cell::new(0),
            pipelines: Cell::new(0),
        }
    }

    /// The name, after checking it as Redis does: "" removes it, and only
    /// printable ASCII without spaces is allowed. Returns false (and
    /// changes nothing) if the name is invalid.
    pub(crate) fn set_name(&self, name: &[u8]) -> bool {
        if !valid_name(name) {
            return false;
        }
        *self.name.borrow_mut() = (!name.is_empty()).then(|| name.to_vec());
        true
    }

    /// One `CLIENT LIST` line, newline included, in Redis 8.10's field
    /// order. The memory and buffer fields Redis tracks per client are 0
    /// here; there are no I/O threads.
    pub(crate) fn info_line(&self, now: Instant, out: &mut String) {
        let addr = |a: Option<SocketAddr>| a.map_or_else(|| "?:0".to_string(), |a| a.to_string());
        // Names and library attributes are printable ASCII (checked).
        let text = |v: &RefCell<Option<Vec<u8>>>| v.borrow().as_deref().map_or(String::new(), |n| String::from_utf8_lossy(n).into_owned());
        // Redis's letters, in its order; N for none.
        let mut flags = String::new();
        for (on, letter) in [
            (self.multi.borrow().is_some(), 'x'),
            (self.dirty_cas.get(), 'd'),
            (self.readonly.get(), 'r'),
            (self.no_touch.get(), 'T'),
        ] {
            if on {
                flags.push(letter);
            }
        }
        if flags.is_empty() {
            flags.push('N');
        }
        let multi = self.multi.borrow().as_ref().map_or(-1, |t| t.len() as i64);
        let _ = writeln!(
            out,
            "id={} addr={} laddr={} fd={} name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi={} watch={} \
             qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 omem-shared=0 omem-unshared=0 \
             tot-mem=0 events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={} io-thread=0 \
             tot-net-in={} tot-net-out={} tot-cmds={} read-events={} avg-pipeline-len-sum={} avg-pipeline-len-cnt={}",
            self.id,
            addr(self.addr),
            addr(self.laddr),
            self.fd,
            text(&self.name),
            now.duration_since(self.created).as_secs(),
            now.duration_since(self.last_active.get()).as_secs(),
            flags,
            multi,
            self.watched.borrow().len(),
            self.last_cmd.get(),
            self.resp.get(),
            text(&self.lib_name),
            text(&self.lib_ver),
            self.net_in.get(),
            self.net_out.get(),
            self.cmds.get(),
            self.read_events.get(),
            self.pipelined.get(),
            self.pipelines.get(),
        );
    }

    /// `n` more bytes written to the client.
    pub(crate) fn wrote(&self, n: usize) {
        self.net_out.set(self.net_out.get() + n as u64);
    }
}

/// Whether `name` may be a client's name: printable ASCII without spaces
/// ("" too: it removes the name).
pub(crate) fn valid_name(name: &[u8]) -> bool {
    name.iter().all(|&b| (b'!'..=b'~').contains(&b))
}

/// Every connected client, by id (so `CLIENT LIST` is in connection order),
/// and who watches which key (see [`crate::multi`]).
#[derive(Default)]
pub(crate) struct Clients {
    by_id: RefCell<BTreeMap<u64, Rc<Client>>>,
    last_id: Cell<u64>,
    /// Per key watched, the ids of the clients watching it.
    watches: RefCell<HashMap<Box<[u8]>, Vec<u64>>>,
    /// Whether the store records touches (see `multi::touched`).
    pub(crate) recording: Cell<bool>,
}

impl Clients {
    pub(crate) fn register(&self, addr: Option<SocketAddr>, laddr: Option<SocketAddr>, fd: RawFd) -> Registration<'_> {
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        let client = Rc::new(Client::new(id, addr, laddr, fd));
        self.by_id.borrow_mut().insert(id, client.clone());
        Registration { clients: self, client }
    }

    pub(crate) fn len(&self) -> usize {
        self.by_id.borrow().len()
    }

    /// A snapshot, so the caller may run code that registers or removes
    /// clients meanwhile.
    pub(crate) fn all(&self) -> Vec<Rc<Client>> {
        self.by_id.borrow().values().cloned().collect()
    }

    /// `client` watches `key` (`expired`: it had expired, but was there).
    /// A key is watched once.
    pub(crate) fn watch(&self, client: &Client, key: &[u8], expired: bool) {
        let mut watched = client.watched.borrow_mut();
        if watched.iter().any(|w| &*w.key == key) {
            return;
        }
        watched.push(Watched { key: key.into(), expired });
        self.watches.borrow_mut().entry(key.into()).or_default().push(client.id);
    }

    /// `client` watches nothing any more.
    pub(crate) fn unwatch_all(&self, client: &Client) {
        let mut watched = client.watched.borrow_mut();
        if watched.is_empty() {
            return;
        }
        let mut watches = self.watches.borrow_mut();
        for w in watched.drain(..) {
            if let Some(ids) = watches.get_mut(&w.key) {
                ids.retain(|&id| id != client.id);
                if ids.is_empty() {
                    watches.remove(&w.key);
                }
            }
        }
    }

    /// Whether any client watches a key.
    pub(crate) fn watching(&self) -> bool {
        !self.watches.borrow().is_empty()
    }

    /// The clients watching `key`.
    pub(crate) fn watchers(&self, key: &[u8]) -> Vec<Rc<Client>> {
        let watches = self.watches.borrow();
        let Some(ids) = watches.get(key) else { return Vec::new() };
        let by_id = self.by_id.borrow();
        ids.iter().filter_map(|id| by_id.get(id).cloned()).collect()
    }

    /// How many clients watch keys, and how many keys are watched (`INFO`'s
    /// `watching_clients`, `total_watched_keys`).
    pub(crate) fn watch_counts(&self) -> (usize, usize) {
        let watches = self.watches.borrow();
        let clients: std::collections::HashSet<u64> = watches.values().flatten().copied().collect();
        (clients.len(), watches.len())
    }

    /// Every key watched.
    pub(crate) fn watched_keys(&self) -> Vec<Box<[u8]>> {
        self.watches.borrow().keys().cloned().collect()
    }
}

/// Keeps a client registered while it lives.
pub(crate) struct Registration<'a> {
    clients: &'a Clients,
    pub(crate) client: Rc<Client>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.clients.unwatch_all(&self.client);
        self.clients.by_id.borrow_mut().remove(&self.client.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_count_up_and_registrations_end_with_their_guard() {
        let clients = Clients::default();
        let a = clients.register(None, None, 7);
        let b = clients.register(None, None, 8);
        assert_eq!((a.client.id, b.client.id), (1, 2));
        drop(a);
        assert_eq!(clients.all().iter().map(|c| c.id).collect::<Vec<_>>(), [2]);
        assert_eq!(clients.register(None, None, 9).client.id, 3, "ids are never reused");
    }

    #[test]
    fn names_are_checked_like_redis() {
        let clients = Clients::default();
        let r = clients.register(None, None, 1);
        assert!(r.client.set_name(b"conn-1"));
        assert_eq!(r.client.name.borrow().as_deref(), Some(&b"conn-1"[..]));
        assert!(!r.client.set_name(b"a b"));
        assert!(!r.client.set_name(b"a\nb"));
        assert!(!r.client.set_name(b"caf\xc3\xa9"));
        assert_eq!(r.client.name.borrow().as_deref(), Some(&b"conn-1"[..]), "a rejected name changes nothing");
        assert!(r.client.set_name(b""));
        assert!(r.client.name.borrow().is_none());
    }
}
