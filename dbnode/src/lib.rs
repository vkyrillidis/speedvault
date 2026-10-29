//! The dbnode: a Redis-compatible server on one reactor.
//!
//! One fiber accepts connections and starts a fiber per connection
//! ([`conn`]); the connection fibers parse requests with `resp`, run them
//! against the node's [`Store`] ([`command`]) and write the replies of each
//! batch of pipelined requests in one `write`. A third fiber reclaims
//! expired keys nobody reads again. In cluster mode ([`cluster_mode`]) a
//! command runs only if this node owns its keys' slot, and more fibers
//! replicate ([`repl`]): to and from replicas, and the slots moving to this
//! node by snapshot from their owners.
//! With `--metrics`, more fibers serve `/metrics` and the probes over HTTP
//! ([`http`], [`metrics`]).
//!
//! Everything runs on the calling thread: the store is owned by it, without
//! locks. A fiber only borrows the store while it runs a command, never
//! across a suspension, so the borrows can't overlap.

mod admin;
mod client;
mod cluster_mode;
mod command;
mod conn;
pub mod http;
mod join;
mod leave;
pub mod log;
pub mod memory;
mod metrics;
mod repl;

use client::Clients;
use cluster_mode::Cluster;
use reactor::{sleep, spawn, PanicPayload, Reactor, TcpListener};
use std::cell::{Cell, RefCell};
use std::io::{self, Read};
use std::net;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use store::Store;

/// How often the expiry fiber runs (Redis's default `hz 10`).
const EXPIRE_INTERVAL: Duration = Duration::from_millis(100);
/// Keys with an expiry examined per sweep.
const EXPIRE_BUDGET: usize = 200;
/// Sweeps per run at most: while a quarter of a sweep was expired, more keys
/// are probably due, so it goes on, but no longer than this.
const EXPIRE_MAX_SWEEPS: usize = 16;

/// How a node runs. The default is Redis's defaults.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Close a connection that sent nothing for this long, silently, as
    /// Redis's `timeout` does. `None` (or zero): never.
    pub idle_timeout: Option<Duration>,
    /// Poll for I/O without sleeping for this long before the thread
    /// sleeps (see [`Reactor::set_busy_poll`]). Zero (the default, like
    /// Redis): sleep at once.
    pub busy_poll: Duration,
    /// Cluster mode (`--cluster`), announcing this address to clients (in
    /// `MOVED` and `CLUSTER SLOTS`). `None`: standalone.
    pub cluster: Option<Announce>,
    /// In cluster mode, stuart's `host:port` (`--stuart`): joined in the
    /// background until it answers.
    pub stuart: Option<String>,
    /// With `stuart`: the name it joins with (`--name`), which stays the same
    /// when the node restarts at another address; stuart takes a node
    /// joining with another's name as its replacement.
    pub name: Option<String>,
    /// With `stuart`: the machine the node runs on (`--zone`; on
    /// Kubernetes, its node's name from the Downward API). stuart puts a
    /// replica on another zone than its primary where it can.
    pub zone: Option<String>,
    /// How a slot migrating away is sent.
    pub migrate: Migrate,
    /// With `stuart`: after a SIGTERM, how long the node tries to leave the
    /// cluster before it exits anyway (`--drain-timeout`; see `leave`).
    /// Zero: the default, 10 minutes.
    pub drain_timeout: Duration,
    /// With `stuart`: once it has left, how long the node serves on (only
    /// redirects: it owns no slot) before it exits, so that clients that
    /// refresh their slot map now and then learn where its slots went
    /// (`--leave-linger`; see `leave`). `None`: the default, 10 s.
    pub leave_linger: Option<Duration>,
    /// `maxmemory` in bytes (`--maxmemory`, resolved): past it, commands
    /// that may add memory are refused, and so are keys moving here. Zero
    /// (the default, like Redis): no limit.
    pub maxmemory: u64,
}

/// Whether `name` can be a node's name for stuart (`--name`): printable,
/// without spaces, at most 255 bytes (a Kubernetes pod name has at most
/// 253).
pub fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 255 && name.bytes().all(|b| b.is_ascii_graphic())
}

/// `--drain-timeout`'s default: a full node's slots move in seconds; this
/// leaves room for a stuart that is down a while.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(600);

/// `--leave-linger`'s default: Lettuce 7 refreshes its slot map on a
/// redirect at most once in 5 s, so a node that exits as soon as its last
/// slot has gone leaves it sending that slot's commands to a closed
/// connection, where they time out.
pub const LEAVE_LINGER: Duration = Duration::from_secs(10);

/// How the source of a slot sync (a move by snapshot) sends the copy of
/// the slots' keys to their new node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migrate {
    /// Keys per batch at most (`--migrate-batch`); batches also stop at
    /// 1 MiB of values, with at least one key.
    pub batch: usize,
    /// Pause after a batch if clients were served since the one before
    /// (`--migrate-pause`), so that a move takes less of a busy node's
    /// time; with no clients, or zero, the next batch goes once the clients
    /// waiting have run.
    pub pause: Duration,
    /// Elements per batch at most (`--migrate-chunk`; a string is one): a
    /// hash, set or list with more goes alone, in parts of this many (a
    /// replica's full sync too).
    pub chunk: usize,
}

impl Default for Migrate {
    fn default() -> Migrate {
        Migrate { batch: 100, pause: Duration::from_micros(50), chunk: 4096 }
    }
}

/// The address clients reach a node at, as Redis's `cluster-announce-ip`
/// and `cluster-announce-port` give it. An IPv6 host comes without brackets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announce {
    pub host: String,
    pub port: u16,
}

/// What the fibers of one node share.
pub(crate) struct Node {
    pub(crate) config: Config,
    pub(crate) store: RefCell<Store>,
    pub(crate) clients: Clients,
    /// The port it listens on, for `INFO` and `CONFIG GET port`.
    pub(crate) port: u16,
    pub(crate) started: Instant,
    pub(crate) connections_received: Cell<u64>,
    pub(crate) commands_processed: Cell<u64>,
    /// Commands refused with `-OOM`.
    pub(crate) commands_refused_oom: Cell<u64>,
    /// `maxmemory`, which `CONFIG SET` changes; 0: none.
    pub(crate) maxmemory: Cell<u64>,
    /// `used_memory` once the node was set up (`used_memory_startup`), and
    /// the most seen since (sampled, see `memory`).
    pub(crate) startup_memory: usize,
    pub(crate) peak_memory: Cell<usize>,
    /// In cluster mode, the slot table and the node's id.
    pub(crate) cluster: Option<Cluster>,
    /// The replication stream ([`repl`]); its id, `master_replid`, is 40
    /// random hex digits, new at every start, as Redis makes one.
    pub(crate) repl: repl::Replication,
}

/// Serves clients on `listener`, and `/metrics` and the probes on
/// `metrics` if given (see [`http`]), until the reactor fails; never
/// returns otherwise. Creates the thread's reactor, so the thread must not
/// have one.
pub fn serve(listener: net::TcpListener, metrics: Option<net::TcpListener>, mut config: Config) -> io::Result<()> {
    config.idle_timeout = config.idle_timeout.filter(|t| !t.is_zero());
    let reactor = Reactor::new()?;
    reactor.set_busy_poll(config.busy_poll);
    reactor.set_panic_handler(|payload| {
        warning!("A connection panicked and was closed: {}", panic_message(&payload));
    });
    let cluster = match &config.cluster {
        None if config.stuart.is_some() => {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "joining stuart needs cluster mode"));
        }
        None => None,
        Some(announce) => {
            let mut cluster = Cluster::new(random_node_id()?, &announce.host, announce.port)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            notice!(
                "Cluster mode: node id {}, announced as {}:{}; no slots until a table arrives",
                cluster.myself(),
                announce.host,
                announce.port
            );
            if let Some(stuart) = &config.stuart {
                let stuart = Arc::new(join::Stuart::new(stuart.clone()));
                cluster.stuart = Some(stuart.clone());
                let (name, zone) = (config.name.clone(), config.zone.clone());
                join::spawn(stuart.clone(), *cluster.myself(), announce.clone(), name, zone, cluster.joined.clone())?;
                let id = *cluster.myself();
                let (leaving, owned) = (cluster.leaving.clone(), cluster.owned.clone());
                let timeout = if config.drain_timeout.is_zero() { DRAIN_TIMEOUT } else { config.drain_timeout };
                let linger = config.leave_linger.unwrap_or(LEAVE_LINGER);
                on_sigterm(Box::new(move || {
                    notice!("Received SIGTERM: leaving the cluster, then exiting (another SIGTERM or SIGINT exits at once)");
                    if let Err(e) = leave::spawn(stuart.clone(), id, timeout, linger, leaving.clone(), owned.clone()) {
                        warning!("Can't start leaving the cluster: {e}; exiting");
                        std::process::exit(1);
                    }
                }));
            }
            Some(cluster)
        }
    };
    // Cluster mode lists the keys by slot, to count and move them.
    let store = if cluster.is_some() { Store::with_slots(cluster::key_hash_slot) } else { Store::new() };
    let maxmemory = config.maxmemory;
    let node = Rc::new(Node {
        config,
        store: RefCell::new(store),
        clients: Clients::default(),
        port: listener.local_addr()?.port(),
        started: Instant::now(),
        connections_received: Cell::new(0),
        commands_processed: Cell::new(0),
        commands_refused_oom: Cell::new(0),
        maxmemory: Cell::new(maxmemory),
        startup_memory: memory::used(),
        peak_memory: Cell::new(memory::used()),
        cluster,
        repl: repl::Replication::new(random_node_id()?, repl::BACKLOG_SIZE),
    });
    let listener = TcpListener::from_std(listener)?;

    let n = node.clone();
    reactor.spawn(move || accept_loop(listener, n))?;
    if node.cluster.is_some() {
        let n = node.clone();
        reactor.spawn(move || repl::replica::replica_loop(n))?;
        let n = node.clone();
        reactor.spawn(move || repl::slots::sync_loop(n))?;
        let n = node.clone();
        reactor.spawn(move || repl::slots::trim_loop(n))?;
    }
    if let Some(metrics) = metrics {
        http::spawn_server(metrics, node.clone())?;
    }
    reactor.spawn(move || expire_loop(node))?;
    reactor.run()
}

impl Node {
    /// Whether the node uses more than `maxmemory`, or would once the
    /// store grows a table of keys for one more key: `denyoom` commands are
    /// refused (Redis's `noeviction`). A table doubles in one allocation,
    /// larger than the key (a few kB; the expiry index of 1M keys: 16 MB).
    pub(crate) fn out_of_memory(&self) -> bool {
        let max = self.maxmemory.get();
        max != 0 && (memory::used() + self.store.borrow().growth()) as u64 > max
    }

    /// `used_memory`, sampling the peak.
    pub(crate) fn used_memory(&self) -> usize {
        let used = memory::used();
        if used > self.peak_memory.get() {
            self.peak_memory.set(used);
        }
        used
    }
}

fn accept_loop(listener: TcpListener, node: Rc<Node>) {
    loop {
        match listener.accept() {
            Ok((conn, _)) => {
                node.connections_received.set(node.connections_received.get() + 1);
                // Replies leave as soon as a batch is done; Nagle would hold
                // back the next small one.
                let _ = conn.set_nodelay(true);
                let node = node.clone();
                if let Err(e) = spawn(move || conn::handle(conn, &node)) {
                    warning!("No fiber for a new connection, closing it: {e}");
                }
            }
            // EMFILE, ENOBUFS, ECONNABORTED...: retry after a pause rather
            // than spin on an error that won't clear at once.
            Err(e) => {
                warning!("Accepting a connection failed: {e}");
                sleep(Duration::from_millis(10));
            }
        }
    }
}

/// Active expiry: the store only sweeps when asked to. Also gives memory
/// back after many deletions, and samples the peak.
fn expire_loop(node: Rc<Node>) {
    loop {
        sleep(EXPIRE_INTERVAL);
        node.used_memory();
        node.store.borrow_mut().shrink();
        node.store.borrow_mut().set_now(unix_ms());
        expire_sweep(&node);
        node.repl.ping();
    }
}

/// The sweeps of one turn of the expiry timer; the keys reclaimed go into
/// the replication stream.
fn expire_sweep(node: &Node) {
    // Not while writes wait for a handover (as Redis's `CLIENT PAUSE
    // WRITE`): the replica is to catch up with a stream that stays put.
    if node.cluster.as_ref().is_some_and(|c| c.all_paused()) {
        return;
    }
    let mut store = node.store.borrow_mut();
    for _ in 0..EXPIRE_MAX_SWEEPS {
        if store.expire_cycle(EXPIRE_BUDGET) < EXPIRE_BUDGET / 4 {
            break;
        }
    }
    if node.repl.active() {
        node.repl.feed(&mut store, None);
    }
}

/// A new node id from 20 random bytes, as Redis makes one.
fn random_node_id() -> io::Result<cluster::NodeId> {
    let mut bytes = [0; 20];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(cluster::NodeId::from_bytes(&bytes))
}

/// The time the store judges expiry by.
pub(crate) fn unix_ms() -> i64 {
    // Before 1970 only with a broken clock; 0 keeps the store working.
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

fn panic_message(payload: &PanicPayload) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("(no message)")
}

/// The address of `host`: an IP address as it is; a name is looked up on a
/// thread of its own, the calling fiber waiting up to `timeout`, so that a
/// slow DNS doesn't hold up the reactor (nodes on Docker Compose or
/// Kubernetes announce names, and a removed container's or pod's name can
/// take seconds to fail).
pub fn resolve(host: &str, port: u16, timeout: Duration) -> io::Result<net::SocketAddr> {
    if let Ok(ip) = host.parse::<net::IpAddr>() {
        return Ok(net::SocketAddr::new(ip, port));
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let name = (host.to_string(), port);
    std::thread::Builder::new().name("resolve".into()).spawn(move || {
        let _ = tx.send(net::ToSocketAddrs::to_socket_addrs(&name).map(|mut addrs| addrs.next()));
    })?;
    let deadline = Instant::now() + timeout;
    loop {
        match rx.try_recv() {
            Ok(Ok(Some(addr))) => return Ok(addr),
            Ok(Ok(None)) => return Err(io::Error::new(io::ErrorKind::NotFound, format!("{host} has no address"))),
            Ok(Err(e)) => return Err(e),
            Err(std::sync::mpsc::TryRecvError::Empty) if Instant::now() < deadline => sleep(Duration::from_millis(1)),
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, format!("looking up {host} took too long"))),
        }
    }
}

/// What the first SIGTERM does instead of exiting, if set: a node that
/// joined stuart leaves the cluster (see `leave`); stuart serves on for
/// the nodes stopping with it.
pub type SigtermHook = Box<dyn FnOnce() + Send>;
static ON_SIGTERM: Mutex<Option<SigtermHook>> = Mutex::new(None);

/// Sets what the first SIGTERM does instead of exiting (see
/// [`exit_on_signals`]); it runs on the signal thread, and must not block.
pub fn on_sigterm(hook: SigtermHook) {
    *ON_SIGTERM.lock().unwrap_or_else(|e| e.into_inner()) = Some(hook);
}

/// Exits the process, with a log line, on SIGTERM (`docker stop`, systemd)
/// or SIGINT (Ctrl-C). There's nothing to save: the node persists nothing,
/// and stuart writes its state before acting on it. A node that joined
/// stuart leaves the cluster on the first SIGTERM instead, and exits once
/// it has, and stuart lingers for the nodes stopping with it (a second
/// SIGTERM, or SIGINT, exits at once).
///
/// The signals are blocked in this thread before any other thread exists,
/// so every thread inherits the mask, and one thread waits for them with
/// `sigwait`: no handler runs in the middle of the reactor, and the exit
/// can log like any other code. Without this, a server running as PID 1
/// in a container ignores SIGTERM, and `docker stop` waits 10 s to kill it.
pub fn exit_on_signals() -> io::Result<()> {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        let rc = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        std::thread::Builder::new().name("signals".into()).spawn(move || loop {
            let mut signal = 0;
            if libc::sigwait(&set, &mut signal) != 0 {
                return;
            }
            let hook = if signal == libc::SIGTERM { ON_SIGTERM.lock().unwrap_or_else(|e| e.into_inner()).take() } else { None };
            match hook {
                Some(hook) => {
                    hook();
                }
                None => {
                    let name = if signal == libc::SIGTERM { "SIGTERM" } else { "SIGINT" };
                    notice!("Received {name}, exiting");
                    std::process::exit(0);
                }
            }
        })?;
    }
    Ok(())
}

