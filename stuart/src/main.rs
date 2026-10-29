//! `stuart [PORT | HOST:PORT] --state FILE [--auto] [--create-when N] [--replicas N] [--fail-after SECONDS]
//! [--moves-per-source N] [--max-moves N] [--loglevel LEVEL] [--metrics PORT | HOST:PORT] [--requirepass-file PATH | --requirepass PASSWORD]`: serves on 127.0.0.1:7600 by default. Exits on
//! SIGINT, and on SIGTERM once the nodes stopping with it were told to (at most 15 s; see `linger`).

use dbnode::auth;
use dbnode::log::{self, Level};
use dbnode::{notice, warning};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use stuart::{Config, Moves};

const DEFAULT_ADDR: &str = "127.0.0.1:7600";
const USAGE: &str = "usage: speedvault-stuart [PORT | HOST:PORT] --state FILE [--auto] [--create-when N] [--replicas N] [--fail-after SECONDS]
              [--moves-per-source N] [--max-moves N] [--loglevel LEVEL] [--metrics PORT | HOST:PORT]
              [--requirepass-file PATH | --requirepass PASSWORD]
  PORT | HOST:PORT    where to listen (default 127.0.0.1:7600; a bare port listens on all interfaces)
  --state FILE        the nodes and the slot table, written before stuart acts on them and read at start
  --auto              keep the slots even over the nodes by itself: nodes joining get slots, leaving
                      ones are drained (a primary with a replica hands over instead); the table is
                      made at the first join (or per --create-when); with --replicas, a primary
                      the nodes don't need becomes a replica
  --create-when N     STUART CREATE by itself once N nodes have joined (if there's no table yet)
  --replicas N        replicas per primary (default 0): STUART CREATE makes ceil(k / (1 + N)) of the
                      k nodes primaries, the others their replicas; a node joining later fills a
                      primary that has fewer than N, else is a primary; raising N later demotes
                      primaries with --auto
  --fail-after SECONDS  a node stuart can't reach this long is failed (default 5): a replica is
                      marked failed (its primary writes without it), a primary's replica promoted
  --moves-per-source N  slots moving out of one node at once (default 128)
  --max-moves N       slots moving at once in all (default 512)
  --loglevel LEVEL    warning, notice (default) or verbose, like Redis's loglevel
  --metrics PORT | HOST:PORT  serve GET /metrics (Prometheus), /healthz and /ready over HTTP there
                      (a bare port listens on all interfaces). Default: off; it needs no password
  --requirepass-file PATH  the cluster's password (the file's contents, one line end dropped: a
                      mounted Kubernetes Secret), the nodes' --requirepass: stuart's port requires
                      it (AUTH), and stuart logs in with it to the nodes
  --requirepass PASSWORD  the same, given here (seen by anyone who can list processes)
  --help, --version";

#[derive(Debug, PartialEq)]
enum Command {
    Serve {
        addr: SocketAddr,
        state: PathBuf,
        create_when: Option<usize>,
        replicas: usize,
        fail_after: Duration,
        moves: Moves,
        auto: bool,
        level: Level,
        metrics: Option<SocketAddr>,
        requirepass: Option<String>,
        requirepass_file: Option<PathBuf>,
    },
    Help,
    Version,
}

fn main() -> ExitCode {
    let (addr, state, create_when, replicas, fail_after, moves, auto, level, metrics, requirepass, requirepass_file) = match parse_args(std::env::args().skip(1)) {
        Some(Command::Serve { addr, state, create_when, replicas, fail_after, moves, auto, level, metrics, requirepass, requirepass_file }) => {
            (addr, state, create_when, replicas, fail_after, moves, auto, level, metrics, requirepass, requirepass_file)
        }
        Some(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Some(Command::Version) => {
            println!("speedvault-stuart {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        None => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    log::set_level(level);
    let password = match auth::password(requirepass, requirepass_file.as_deref()) {
        Ok(password) => password,
        Err(why) => {
            eprintln!("speedvault-stuart: {why}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = dbnode::exit_on_signals() {
        warning!("Can't handle SIGTERM/SIGINT: {e}");
    }
    // The first SIGTERM: serve on for the nodes stopping too (see linger).
    dbnode::on_sigterm(Box::new(|| stuart::STOPPING.store(true, std::sync::atomic::Ordering::Relaxed)));
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
    let http = metrics.as_ref().and_then(|m| m.local_addr().ok()).map_or(String::new(), |a| format!("; metrics on http://{a}/metrics"));
    let auth = if password.is_some() { "; password required" } else { "" };
    notice!("speedvault-stuart {} (pid {}) ready on {addr}{auth}{http}", env!("CARGO_PKG_VERSION"), std::process::id());
    let error = stuart::serve(listener, metrics, Config { state, create_when, moves, auto, replicas, fail_after, password });
    warning!("stuart stopped: {}", error.err().map_or("no error".to_string(), |e| e.to_string()));
    ExitCode::FAILURE
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Option<Command> {
    let (mut addr, mut state, mut create_when, mut level) = (None, None, None, Level::Notice);
    let (mut moves, mut auto, mut metrics, mut replicas) = (Moves::default(), false, None, 0);
    let mut fail_after = stuart::FAIL_AFTER;
    let (mut requirepass, mut requirepass_file) = (None, None);
    let positive = |arg: Option<String>| arg?.parse::<usize>().ok().filter(|&n| n > 0);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Some(Command::Help),
            "--version" | "-v" => return Some(Command::Version),
            "--state" => state = Some(PathBuf::from(args.next()?)),
            "--auto" => auto = true,
            "--create-when" => create_when = Some(args.next()?.parse::<usize>().ok().filter(|&n| n > 0)?),
            "--replicas" => replicas = args.next()?.parse::<usize>().ok()?,
            "--fail-after" => fail_after = Duration::from_secs(positive(args.next())? as u64),
            "--loglevel" => level = Level::parse(&args.next()?)?,
            "--moves-per-source" => moves.per_source = positive(args.next())?,
            "--max-moves" => moves.total = positive(args.next())?,
            "--metrics" => metrics = Some(parse_addr(&args.next()?)?),
            "--requirepass" => requirepass = Some(args.next()?),
            "--requirepass-file" => requirepass_file = Some(PathBuf::from(args.next()?)),
            _ if addr.is_none() && !arg.starts_with('-') => addr = Some(parse_addr(&arg)?),
            _ => return None,
        }
    }
    let addr = addr.unwrap_or_else(|| DEFAULT_ADDR.parse().unwrap());
    if requirepass.is_some() && requirepass_file.is_some() {
        return None;
    }
    Some(Command::Serve { addr, state: state?, create_when, replicas, fail_after, moves, auto, level, metrics, requirepass, requirepass_file })
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

    #[test]
    fn arguments() {
        let serve = |addr: &str, state: &str, level| {
            Some(Command::Serve {
                addr: addr.parse().unwrap(),
                state: state.into(),
                create_when: None,
                replicas: 0,
                fail_after: stuart::FAIL_AFTER,
                moves: Moves::default(),
                auto: false,
                level,
                metrics: None,
                requirepass: None,
                requirepass_file: None,
            })
        };
        assert_eq!(parse(&["--state", "s"]), serve("127.0.0.1:7600", "s", Level::Notice));
        assert_eq!(parse(&["7601", "--state", "/var/lib/stuart"]), serve("0.0.0.0:7601", "/var/lib/stuart", Level::Notice));
        assert_eq!(parse(&["--loglevel", "verbose", "10.0.0.1:7600", "--state", "s"]), serve("10.0.0.1:7600", "s", Level::Verbose));
        assert_eq!(parse(&["--help"]), Some(Command::Help));
        assert_eq!(parse(&["--version"]), Some(Command::Version));
        let Some(Command::Serve { create_when, .. }) = parse(&["--state", "s", "--create-when", "3"]) else { panic!() };
        assert_eq!(create_when, Some(3));
        let Some(Command::Serve { auto, .. }) = parse(&["--auto", "--state", "s"]) else { panic!() };
        assert!(auto);
        let Some(Command::Serve { metrics, .. }) = parse(&["--state", "s", "--metrics", "9121"]) else { panic!() };
        assert_eq!(metrics, Some("0.0.0.0:9121".parse().unwrap()));
        assert_eq!(parse(&["--state", "s", "--metrics"]), None);
        let Some(Command::Serve { requirepass, requirepass_file, .. }) = parse(&["--state", "s", "--requirepass", "pw"]) else { panic!() };
        assert_eq!((requirepass.as_deref(), requirepass_file), (Some("pw"), None));
        let Some(Command::Serve { requirepass, requirepass_file, .. }) = parse(&["--state", "s", "--requirepass-file", "/p"]) else { panic!() };
        assert_eq!((requirepass, requirepass_file), (None, Some(PathBuf::from("/p"))));
        assert_eq!(parse(&["--state", "s", "--requirepass", "a", "--requirepass-file", "b"]), None);
        assert_eq!(parse(&["--state", "s", "--requirepass"]), None);
        for bad in ["0", "-1", "x", ""] {
            assert_eq!(parse(&["--state", "s", "--create-when", bad]), None, "{bad:?}");
        }
        assert_eq!(parse(&["--state", "s", "--create-when"]), None);
        let Some(Command::Serve { replicas, .. }) = parse(&["--state", "s", "--replicas", "2"]) else { panic!() };
        assert_eq!(replicas, 2);
        let Some(Command::Serve { replicas, .. }) = parse(&["--state", "s", "--replicas", "0"]) else { panic!() };
        assert_eq!(replicas, 0);
        for bad in ["-1", "x", ""] {
            assert_eq!(parse(&["--state", "s", "--replicas", bad]), None, "{bad:?}");
        }
        let Some(Command::Serve { moves, .. }) = parse(&["--state", "s", "--moves-per-source", "2", "--max-moves", "5"]) else { panic!() };
        assert_eq!(moves, Moves { per_source: 2, total: 5 });
        assert_eq!(Moves::default(), Moves { per_source: 128, total: 512 });
        assert_eq!(parse(&["--state", "s", "--key-by-key"]), None, "phase 5's moves are gone");
        for bad in ["0", "x"] {
            assert_eq!(parse(&["--state", "s", "--max-moves", bad]), None, "{bad:?}");
            assert_eq!(parse(&["--state", "s", "--moves-per-source", bad]), None, "{bad:?}");
        }
        for bad in [&[][..], &["7600"], &["--state"], &["--state", "s", "--loglevel", "debug"], &["--state", "s", "x"], &["--nope"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }
}
