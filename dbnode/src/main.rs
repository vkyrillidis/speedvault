//! `dbnode [PORT | HOST:PORT] [--timeout SECONDS] [--busy-poll USEC] [--loglevel LEVEL]
//! [--maxmemory BYTES | N%] [--metrics PORT | HOST:PORT] [--cluster [--announce HOST:PORT] [--stuart HOST:PORT [--name NAME] [--zone ZONE] [--drain-timeout SECONDS] [--leave-linger SECONDS]]
//! [--migrate-batch KEYS] [--migrate-pause USEC] [--migrate-chunk ELEMENTS]]`: serves on 127.0.0.1:7000 by default.
//! Exits on SIGTERM or SIGINT; with `--stuart`, the first SIGTERM leaves the cluster first.

use dbnode::log::{self, Level};
use dbnode::memory::{Counting, Human, Limit};
use dbnode::{notice, valid_name, warning, Announce, Config, Migrate, DRAIN_TIMEOUT, LEAVE_LINGER};
use std::net::{SocketAddr, TcpListener};
use std::process::ExitCode;
use std::time::Duration;

/// Counts the memory in use, for `maxmemory` and `INFO memory`.
#[global_allocator]
static ALLOCATOR: Counting = Counting;

const DEFAULT_ADDR: &str = "127.0.0.1:7000";
/// Enough for a client that answers at once to send its next request before
/// the server sleeps: 1.37x Redis's GET/s at -P 1 instead of 0.87x (WSL2).
const DEFAULT_BUSY_POLL: Duration = Duration::from_micros(20);
const USAGE: &str = "usage: speedvault-server [PORT | HOST:PORT] [--timeout SECONDS] [--busy-poll USEC] [--loglevel LEVEL]
              [--maxmemory BYTES | N%] [--metrics PORT | HOST:PORT]
              [--cluster [--announce HOST:PORT] [--stuart HOST:PORT [--name NAME] [--zone ZONE] [--drain-timeout SECONDS] [--leave-linger SECONDS]]
                         [--migrate-batch KEYS] [--migrate-pause USEC] [--migrate-chunk ELEMENTS]]
  PORT | HOST:PORT    where to listen (default 127.0.0.1:7000; a bare port listens on all interfaces)
  --timeout SECONDS   close connections idle this long (default 0: never), like Redis's timeout
  --busy-poll USEC    poll this long for requests before sleeping (default 20; 0: sleep at once);
                      faster replies, but a busy server keeps its CPU at 100%
  --loglevel LEVEL    warning, notice (default) or verbose (every connection), like Redis's loglevel
  --maxmemory BYTES | N%  past this much memory, refuse writes that add memory (-OOM, Redis's
                      noeviction) and keys moving here; a size (2gb, 500mb, bytes) or a share of
                      the container's memory limit (75%; refused without one). Default 0: no limit
  --metrics PORT | HOST:PORT  serve GET /metrics (Prometheus), /healthz and /ready over HTTP there
                      (a bare port listens on all interfaces). Default: off
  --cluster           cluster mode (Redis Cluster protocol): serve the slots a table pushed with
                      CLUSTER SETTABLE assigns, redirect the others with MOVED
  --announce HOST:PORT  the address clients reach this node at (default: where it listens;
                      required when it listens on all interfaces)
  --stuart HOST:PORT  join stuart there, retrying every second until it answers; stuart then
                      pushes the slot table. The first SIGTERM leaves the cluster (STUART LEAVE:
                      the slots move to the other nodes), then exits; SIGINT exits at once
  --name NAME         with --stuart: a name that stays when the node restarts at another address
                      (a pod's name, announcing its pod's address); stuart takes a node joining
                      with the name of another as its replacement, as one at its address
  --zone ZONE         with --stuart: the machine the node runs on (a Kubernetes node's name);
                      stuart puts a replica on another zone than its primary where it can
  --drain-timeout SECONDS  after that SIGTERM, exit anyway after this long (default 600)
  --leave-linger SECONDS  once it has left, redirect clients this long before exiting, so that
                      they learn where its slots went (default 10; 0: exit at once)
  --migrate-batch KEYS  keys per batch of a slot's copy when it moves to another node
                      (default 100; a batch also stops at 1 MiB of values)
  --migrate-pause USEC  pause after such a batch if clients were served meanwhile (default 50;
                      0: only let the waiting clients run)
  --migrate-chunk ELEMENTS  elements per batch (a string is one); a larger hash, set or list
                      goes in parts of this many (default 4096)
  --help, --version";

/// What the command line asks for (made once: its size doesn't matter).
#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
enum Command {
    Serve {
        addr: SocketAddr,
        timeout: Option<Duration>,
        busy_poll: Duration,
        level: Level,
        cluster: Option<Announce>,
        stuart: Option<String>,
        name: Option<String>,
        zone: Option<String>,
        migrate: Migrate,
        drain_timeout: Duration,
        leave_linger: Duration,
        maxmemory: Limit,
        metrics: Option<SocketAddr>,
    },
    Help,
    Version,
    /// Arguments that parse but don't fit together.
    Invalid(&'static str),
}

fn main() -> ExitCode {
    let (addr, timeout, busy_poll, level, cluster, stuart, name, zone, migrate, drain_timeout, leave_linger, maxmemory, metrics) = match parse_args(std::env::args().skip(1)) {
        Some(Command::Serve { addr, timeout, busy_poll, level, cluster, stuart, name, zone, migrate, drain_timeout, leave_linger, maxmemory, metrics }) => {
            (addr, timeout, busy_poll, level, cluster, stuart, name, zone, migrate, drain_timeout, leave_linger, maxmemory, metrics)
        }
        Some(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Some(Command::Version) => {
            println!("speedvault-server {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Some(Command::Invalid(why)) => {
            eprintln!("speedvault-server: {why}\n{USAGE}");
            return ExitCode::from(2);
        }
        None => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    log::set_level(level);
    let maxmemory = match maxmemory.resolve() {
        Ok(bytes) => bytes,
        Err(why) => {
            eprintln!("speedvault-server: {why}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = dbnode::exit_on_signals() {
        warning!("Can't handle SIGTERM/SIGINT: {e}");
    }
    let listener = match TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            warning!("Can't listen on {addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let metrics = match metrics.map(TcpListener::bind).transpose() {
        Ok(m) => m,
        Err(e) => {
            warning!("Can't serve metrics on {}: {e}", metrics.unwrap());
            return ExitCode::FAILURE;
        }
    };
    let idle = timeout.filter(|t| !t.is_zero()).map_or("never".to_string(), |t| format!("{}s", t.as_secs()));
    let max = if maxmemory == 0 { "none".to_string() } else { format!("{} ({maxmemory} bytes)", Human(maxmemory)) };
    let http = metrics.as_ref().and_then(|m| m.local_addr().ok()).map_or(String::new(), |a| format!(", metrics on http://{a}/metrics"));
    notice!(
        "speedvault-server {} (pid {}) ready on {addr}; idle timeout {idle}, busy-poll {} us, maxmemory {max}{http}",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        busy_poll.as_micros()
    );
    let error = dbnode::serve(listener, metrics, Config { idle_timeout: timeout, busy_poll, cluster, stuart, name, zone, migrate, drain_timeout, leave_linger: Some(leave_linger), maxmemory });
    warning!("Server stopped: {}", error.err().map_or("no error".to_string(), |e| e.to_string()));
    ExitCode::FAILURE
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Option<Command> {
    let mut addr = None;
    let (mut timeout, mut busy_poll, mut level) = (None, DEFAULT_BUSY_POLL, Level::Notice);
    let (mut cluster, mut announce, mut stuart, mut name, mut zone) = (false, None, None, None, None);
    let (mut migrate, mut drain_timeout, mut leave_linger, mut maxmemory, mut metrics) = (Migrate::default(), None, None, Limit::Bytes(0), None);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Some(Command::Help),
            "--version" | "-v" => return Some(Command::Version),
            "--timeout" => timeout = Some(Duration::from_secs(args.next()?.parse().ok()?)),
            "--busy-poll" => busy_poll = Duration::from_micros(args.next()?.parse().ok()?),
            "--loglevel" => level = Level::parse(&args.next()?)?,
            "--cluster" => cluster = true,
            "--announce" => announce = Some(parse_announce(&args.next()?)?),
            // Resolved by the join thread, on every try.
            "--stuart" => stuart = Some(args.next().filter(|s| parse_announce(s).is_some())?),
            "--name" => name = Some(args.next().filter(|n| valid_name(n))?),
            "--zone" => zone = Some(args.next().filter(|z| valid_name(z))?),
            "--migrate-batch" => migrate.batch = args.next()?.parse().ok().filter(|&n| n > 0)?,
            "--migrate-pause" => migrate.pause = Duration::from_micros(args.next()?.parse().ok()?),
            "--migrate-chunk" => migrate.chunk = args.next()?.parse().ok().filter(|&n| n > 0)?,
            "--maxmemory" => maxmemory = Limit::parse(&args.next()?)?,
            "--metrics" => metrics = Some(parse_addr(&args.next()?)?),
            "--drain-timeout" => drain_timeout = Some(Duration::from_secs(args.next()?.parse().ok().filter(|&n| n > 0)?)),
            "--leave-linger" => leave_linger = Some(Duration::from_secs(args.next()?.parse().ok()?)),
            _ if addr.is_none() && !arg.starts_with('-') => addr = Some(parse_addr(&arg)?),
            _ => return None,
        }
    }
    let addr = addr.unwrap_or_else(|| DEFAULT_ADDR.parse().unwrap());
    if stuart.is_some() && !cluster {
        return Some(Command::Invalid("--stuart needs --cluster"));
    }
    if drain_timeout.is_some() && stuart.is_none() {
        return Some(Command::Invalid("--drain-timeout needs --stuart"));
    }
    if name.is_some() && stuart.is_none() {
        return Some(Command::Invalid("--name needs --stuart"));
    }
    if zone.is_some() && stuart.is_none() {
        return Some(Command::Invalid("--zone needs --stuart"));
    }
    if leave_linger.is_some() && stuart.is_none() {
        return Some(Command::Invalid("--leave-linger needs --stuart"));
    }
    let drain_timeout = drain_timeout.unwrap_or(DRAIN_TIMEOUT);
    let leave_linger = leave_linger.unwrap_or(LEAVE_LINGER);
    let cluster = match (cluster, announce) {
        (false, None) => None,
        (false, Some(_)) => return Some(Command::Invalid("--announce needs --cluster")),
        (true, Some(announce)) => Some(announce),
        (true, None) if addr.ip().is_unspecified() => {
            return Some(Command::Invalid("--cluster on all interfaces needs --announce HOST:PORT"))
        }
        (true, None) => Some(Announce { host: addr.ip().to_string(), port: addr.port() }),
    };
    Some(Command::Serve { addr, timeout, busy_poll, level, cluster, stuart, name, zone, migrate, drain_timeout, leave_linger, maxmemory, metrics })
}

/// `host:port`, or `[v6 address]:port`; a host that `CLUSTER NODES` can
/// print (see `cluster::Node::new`) and a port that isn't 0.
fn parse_announce(arg: &str) -> Option<Announce> {
    let (host, port) = arg.rsplit_once(':')?;
    let host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    let port = port.parse::<u16>().ok().filter(|&p| p != 0)?;
    cluster::Node::new(cluster::NodeId::from_bytes(&[0; 20]), host, port).ok()?;
    Some(Announce { host: host.to_string(), port })
}

/// A bare port listens on all interfaces.
fn parse_addr(arg: &str) -> Option<SocketAddr> {
    match arg.parse::<u16>() {
        Ok(port) => Some(SocketAddr::from(([0, 0, 0, 0], port))),
        Err(_) => arg.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Option<Command> {
        parse_args(args.iter().map(|a| a.to_string()))
    }

    fn serve(addr: &str, timeout: Option<u64>, level: Level) -> Option<Command> {
        let timeout = timeout.map(Duration::from_secs);
        let addr = addr.parse().unwrap();
        Some(Command::Serve { addr, timeout, busy_poll: DEFAULT_BUSY_POLL, level, cluster: None, stuart: None, name: None, zone: None, migrate: Migrate::default(), drain_timeout: DRAIN_TIMEOUT, leave_linger: LEAVE_LINGER, maxmemory: Limit::Bytes(0), metrics: None })
    }

    fn announced(args: &[&str]) -> Option<Announce> {
        match parse(args) {
            Some(Command::Serve { cluster, .. }) => cluster,
            other => panic!("{args:?}: {other:?}"),
        }
    }

    #[test]
    fn cluster_arguments() {
        let at = |host: &str, port| Some(Announce { host: host.to_string(), port });
        assert_eq!(announced(&["--cluster"]), at("127.0.0.1", 7000));
        assert_eq!(announced(&["10.0.0.5:7001", "--cluster"]), at("10.0.0.5", 7001));
        assert_eq!(announced(&["7001", "--cluster", "--announce", "10.0.0.5:7001"]), at("10.0.0.5", 7001));
        assert_eq!(announced(&["--announce", "node-1.svc:6379", "--cluster"]), at("node-1.svc", 6379));
        assert_eq!(announced(&["--cluster", "--announce", "[::1]:7000"]), at("::1", 7000));
        assert_eq!(announced(&["--cluster", "--announce", "::1:7000"]), at("::1", 7000));
        assert_eq!(announced(&["7001"]), None);
        assert_eq!(parse(&["7001", "--cluster"]), Some(Command::Invalid("--cluster on all interfaces needs --announce HOST:PORT")));
        assert_eq!(parse(&["--announce", "10.0.0.5:7001"]), Some(Command::Invalid("--announce needs --cluster")));
        for bad in ["10.0.0.5", "10.0.0.5:0", "10.0.0.5:70000", ":7000", "a b:7000", "a@b:7000", "a,b:7000", "[]:7000"] {
            assert_eq!(parse(&["--cluster", "--announce", bad]), None, "{bad}");
        }
        assert_eq!(parse(&["--cluster", "--announce"]), None);

        let joins = |args: &[&str]| match parse(args) {
            Some(Command::Serve { stuart, .. }) => stuart,
            other => panic!("{args:?}: {other:?}"),
        };
        assert_eq!(joins(&["--cluster", "--stuart", "10.0.0.1:7600"]), Some("10.0.0.1:7600".to_string()));
        assert_eq!(joins(&["--cluster", "--stuart", "stuart.db.svc:7600"]), Some("stuart.db.svc:7600".to_string()));
        assert_eq!(joins(&["--cluster"]), None);
        assert_eq!(parse(&["--stuart", "10.0.0.1:7600"]), Some(Command::Invalid("--stuart needs --cluster")));
        for bad in ["10.0.0.1", "10.0.0.1:0", "a b:7600"] {
            assert_eq!(parse(&["--cluster", "--stuart", bad]), None, "{bad}");
        }

        let linger = |args: &[&str]| match parse(args) {
            Some(Command::Serve { leave_linger, .. }) => leave_linger,
            other => panic!("{args:?}: {other:?}"),
        };
        assert_eq!(linger(&["--cluster", "--stuart", "10.0.0.1:7600"]), LEAVE_LINGER);
        assert_eq!(linger(&["--cluster", "--stuart", "10.0.0.1:7600", "--leave-linger", "0"]), Duration::ZERO);
        assert_eq!(linger(&["--cluster", "--stuart", "10.0.0.1:7600", "--leave-linger", "30"]), Duration::from_secs(30));
        assert_eq!(parse(&["--cluster", "--stuart", "10.0.0.1:7600", "--leave-linger", "-1"]), None);
        assert_eq!(parse(&["--cluster", "--leave-linger", "5"]), Some(Command::Invalid("--leave-linger needs --stuart")));

        let name = |args: &[&str]| match parse(args) {
            Some(Command::Serve { name, .. }) => name,
            other => panic!("{args:?}: {other:?}"),
        };
        assert_eq!(name(&["--cluster", "--stuart", "10.0.0.1:7600"]), None);
        assert_eq!(name(&["--cluster", "--stuart", "10.0.0.1:7600", "--name", "speedvault-3"]), Some("speedvault-3".into()));
        assert_eq!(parse(&["--cluster", "--stuart", "10.0.0.1:7600", "--name", "a b"]), None);
        assert_eq!(parse(&["--cluster", "--stuart", "10.0.0.1:7600", "--name", ""]), None);
        assert_eq!(parse(&["--cluster", "--name", "x"]), Some(Command::Invalid("--name needs --stuart")));

        let zone = |args: &[&str]| match parse(args) {
            Some(Command::Serve { zone, .. }) => zone,
            other => panic!("{args:?}: {other:?}"),
        };
        assert_eq!(zone(&["--cluster", "--stuart", "10.0.0.1:7600"]), None);
        assert_eq!(zone(&["--cluster", "--stuart", "10.0.0.1:7600", "--zone", "kind-worker2"]), Some("kind-worker2".into()));
        assert_eq!(parse(&["--cluster", "--stuart", "10.0.0.1:7600", "--zone", "a b"]), None);
        assert_eq!(parse(&["--cluster", "--stuart", "10.0.0.1:7600", "--zone"]), None);
        assert_eq!(parse(&["--cluster", "--zone", "x"]), Some(Command::Invalid("--zone needs --stuart")));

        let migrate = |args: &[&str]| match parse(args) {
            Some(Command::Serve { migrate, .. }) => migrate,
            other => panic!("{args:?}: {other:?}"),
        };
        assert_eq!(migrate(&["--cluster"]), Migrate { batch: 100, pause: Duration::from_micros(50), chunk: 4096 });
        assert_eq!(
            migrate(&["--cluster", "--migrate-batch", "7", "--migrate-pause", "250", "--migrate-chunk", "9"]),
            Migrate { batch: 7, pause: Duration::from_micros(250), chunk: 9 }
        );
        for bad in [&["--migrate-batch", "0"][..], &["--migrate-batch", "x"], &["--migrate-pause", "-1"], &["--migrate-batch"], &["--migrate-chunk", "0"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn arguments() {
        assert_eq!(parse(&[]), serve("127.0.0.1:7000", None, Level::Notice));
        assert_eq!(parse(&["8080"]), serve("0.0.0.0:8080", None, Level::Notice));
        assert_eq!(
            parse(&["--loglevel", "verbose", "127.0.0.1:9", "--timeout", "30"]),
            serve("127.0.0.1:9", Some(30), Level::Verbose)
        );
        assert_eq!(parse(&["--timeout", "0"]), serve("127.0.0.1:7000", Some(0), Level::Notice));
        assert_eq!(parse(&["7000", "--help"]), Some(Command::Help));
        assert_eq!(parse(&["--version"]), Some(Command::Version));
        let Some(Command::Serve { busy_poll, .. }) = parse(&["--busy-poll", "0"]) else { panic!() };
        assert_eq!(busy_poll, Duration::ZERO);
        let Some(Command::Serve { busy_poll, .. }) = parse(&["--busy-poll", "150", "7001"]) else { panic!() };
        assert_eq!(busy_poll, Duration::from_micros(150));
        let Some(Command::Serve { maxmemory, .. }) = parse(&["--maxmemory", "2gb"]) else { panic!() };
        assert_eq!(maxmemory, Limit::Bytes(2 << 30));
        let Some(Command::Serve { maxmemory, .. }) = parse(&["--maxmemory", "75%"]) else { panic!() };
        assert_eq!(maxmemory, Limit::Share(75));
        let Some(Command::Serve { metrics, .. }) = parse(&["--metrics", "9121"]) else { panic!() };
        assert_eq!(metrics, Some("0.0.0.0:9121".parse().unwrap()));
        let Some(Command::Serve { metrics, .. }) = parse(&["--metrics", "127.0.0.1:9121"]) else { panic!() };
        assert_eq!(metrics, Some("127.0.0.1:9121".parse().unwrap()));
        for bad in [&["--metrics"][..], &["--metrics", "x"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
        for bad in [&["--maxmemory"][..], &["--maxmemory", "1.5gb"], &["--maxmemory", "0%"], &["--maxmemory", "-1"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
        for bad in [&["--timeout"][..], &["--timeout", "-1"], &["--busy-poll"], &["--busy-poll", "1.5"], &["--loglevel", "debug"], &["--nope"], &["1", "2"], &["nohost"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }
}
