//! `stuart [PORT | HOST:PORT] --state FILE [--auto] [--create-when N] [--moves-per-source N]
//! [--max-moves N] [--loglevel LEVEL] [--metrics PORT | HOST:PORT]`: serves on 127.0.0.1:7600 by default. Exits on
//! SIGINT, and on SIGTERM once the nodes stopping with it were told to (at most 15 s; see `linger`).

use dbnode::log::{self, Level};
use dbnode::{notice, warning};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;
use stuart::{Config, Moves};

const DEFAULT_ADDR: &str = "127.0.0.1:7600";
const USAGE: &str = "usage: stuart [PORT | HOST:PORT] --state FILE [--auto] [--create-when N] [--moves-per-source N]
              [--max-moves N] [--loglevel LEVEL] [--metrics PORT | HOST:PORT]
  PORT | HOST:PORT    where to listen (default 127.0.0.1:7600; a bare port listens on all interfaces)
  --state FILE        the nodes and the slot table, written before stuart acts on them and read at start
  --auto              keep the slots even over the nodes by itself: nodes joining get slots, leaving
                      ones are drained; the table is made at the first join (or per --create-when)
  --create-when N     STUART CREATE by itself once N nodes have joined (if there's no table yet)
  --moves-per-source N  slots moving out of one node at once (default 32)
  --max-moves N       slots moving at once in all (default 128)
  --loglevel LEVEL    warning, notice (default) or verbose, like Redis's loglevel
  --metrics PORT | HOST:PORT  serve GET /metrics (Prometheus), /healthz and /ready over HTTP there
                      (a bare port listens on all interfaces). Default: off
  --help, --version";

#[derive(Debug, PartialEq)]
enum Command {
    Serve { addr: SocketAddr, state: PathBuf, create_when: Option<usize>, moves: Moves, auto: bool, level: Level, metrics: Option<SocketAddr> },
    Help,
    Version,
}

fn main() -> ExitCode {
    let (addr, state, create_when, moves, auto, level, metrics) = match parse_args(std::env::args().skip(1)) {
        Some(Command::Serve { addr, state, create_when, moves, auto, level, metrics }) => (addr, state, create_when, moves, auto, level, metrics),
        Some(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Some(Command::Version) => {
            println!("stuart {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        None => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    log::set_level(level);
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
    notice!("stuart {} (pid {}) ready on {addr}{http}", env!("CARGO_PKG_VERSION"), std::process::id());
    let error = stuart::serve(listener, metrics, Config { state, create_when, moves, auto });
    warning!("stuart stopped: {}", error.err().map_or("no error".to_string(), |e| e.to_string()));
    ExitCode::FAILURE
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Option<Command> {
    let (mut addr, mut state, mut create_when, mut level) = (None, None, None, Level::Notice);
    let (mut moves, mut auto, mut metrics) = (Moves::default(), false, None);
    let positive = |arg: Option<String>| arg?.parse::<usize>().ok().filter(|&n| n > 0);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Some(Command::Help),
            "--version" | "-v" => return Some(Command::Version),
            "--state" => state = Some(PathBuf::from(args.next()?)),
            "--auto" => auto = true,
            "--create-when" => create_when = Some(args.next()?.parse::<usize>().ok().filter(|&n| n > 0)?),
            "--loglevel" => level = Level::parse(&args.next()?)?,
            "--moves-per-source" => moves.per_source = positive(args.next())?,
            "--max-moves" => moves.total = positive(args.next())?,
            "--metrics" => metrics = Some(parse_addr(&args.next()?)?),
            _ if addr.is_none() && !arg.starts_with('-') => addr = Some(parse_addr(&arg)?),
            _ => return None,
        }
    }
    let addr = addr.unwrap_or_else(|| DEFAULT_ADDR.parse().unwrap());
    Some(Command::Serve { addr, state: state?, create_when, moves, auto, level, metrics })
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
            Some(Command::Serve { addr: addr.parse().unwrap(), state: state.into(), create_when: None, moves: Moves::default(), auto: false, level, metrics: None })
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
        for bad in ["0", "-1", "x", ""] {
            assert_eq!(parse(&["--state", "s", "--create-when", bad]), None, "{bad:?}");
        }
        assert_eq!(parse(&["--state", "s", "--create-when"]), None);
        let Some(Command::Serve { moves, .. }) = parse(&["--state", "s", "--moves-per-source", "2", "--max-moves", "5"]) else { panic!() };
        assert_eq!(moves, Moves { per_source: 2, total: 5 });
        assert_eq!(Moves::default(), Moves { per_source: 32, total: 128 });
        for bad in ["0", "x"] {
            assert_eq!(parse(&["--state", "s", "--max-moves", bad]), None, "{bad:?}");
            assert_eq!(parse(&["--state", "s", "--moves-per-source", bad]), None, "{bad:?}");
        }
        for bad in [&[][..], &["7600"], &["--state"], &["--state", "s", "--loglevel", "debug"], &["--state", "s", "x"], &["--nope"]] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }
}
