//! Connection and server commands: `HELLO`, `CLIENT`, `COMMAND`, `INFO`,
//! `CONFIG`. These are what clients send on connect (see
//! `tools/compat/README.md`); replies and errors are Redis 7.0's.
//!
//! The node presents itself as Redis 7.0.15 (`HELLO`'s `server` and
//! `version`, `INFO`'s `redis_version`): clients pick features by them, and
//! 7.0.15 is the behaviour the node copies. `INFO` adds a
//! `speedvault_version` line so the real server can still be told apart.

use crate::client::Client;
use crate::command::{c_str, conn, find, syntax_error, Command, Flow, COMMANDS};
use crate::memory::{self, Human, Limit};
use crate::{warning, Node};
use resp::{Args, Encoder};
use std::fmt::Write;
use std::time::Instant;
use store::parse_i64;

/// The Redis version whose behaviour the node copies.
const REDIS_VERSION: &str = "7.0.15";

pub(crate) const CLIENT: &[Command] = &[
    conn("client|id", 2, |_, _, client, out| {
        out.integer(client.id as i64);
        Flow::Continue
    }),
    conn("client|getname", 2, |_, _, client, out| {
        match &*client.name.borrow() {
            Some(name) => out.bulk(name),
            None => out.null(),
        }
        Flow::Continue
    }),
    conn("client|setname", 3, |args, _, client, out| {
        if set_name(client, &args[2], out) {
            out.ok();
        }
        Flow::Continue
    }),
    conn("client|info", 2, |_, _, client, out| {
        let mut line = String::new();
        client.info_line(Instant::now(), &mut line);
        out.verbatim(b"txt", line.as_bytes());
        Flow::Continue
    }),
    conn("client|list", -2, client_list),
];

pub(crate) const COMMAND: &[Command] = &[
    conn("command|count", 2, |_, _, _, out| {
        out.integer(COMMANDS.len() as i64);
        Flow::Continue
    }),
    conn("command|info", -2, command_info),
    conn("command|docs", -2, |_, _, _, out| {
        // No documentation to give: an empty map. redis-cli then shows no
        // hints, as it would after an error, but without the error.
        out.map(0);
        Flow::Continue
    }),
];

pub(crate) const CONFIG: &[Command] = &[conn("config|get", -3, config_get), conn("config|set", -4, config_set)];

/// The `run` of a container whose arity always requires a subcommand.
pub(crate) fn unreachable(_: &Args, _: &Node, _: &Client, _: &mut Encoder) -> Flow {
    unreachable!("the arity check requires a subcommand")
}

/// Sets the client name, or replies Redis's error and returns false.
fn set_name(client: &Client, name: &[u8], out: &mut Encoder) -> bool {
    let ok = client.set_name(name);
    if !ok {
        out.error(b"ERR Client names cannot contain spaces, newlines or special characters.");
    }
    ok
}

/// `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
///
/// Every option is checked before anything changes. There are no users or
/// passwords: like a Redis whose `default` user has `nopass`, `AUTH default`
/// with any password succeeds and other users don't exist.
pub(crate) fn hello(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    let mut proto = None;
    if args.len() >= 2 {
        match parse_i64(&args[1]) {
            None => {
                out.error(b"ERR Protocol version is not an integer or out of range");
                return Flow::Continue;
            }
            Some(v @ (2 | 3)) => proto = Some(v as u8),
            Some(_) => {
                out.error(b"NOPROTO unsupported protocol version");
                return Flow::Continue;
            }
        }
    }
    let (mut user, mut name) = (None, None);
    let mut i = 2;
    while i < args.len() {
        let (opt, more) = (&args[i], args.len() - 1 - i);
        if opt.eq_ignore_ascii_case(b"auth") && more >= 2 {
            user = Some(&args[i + 1]);
            i += 3;
        } else if opt.eq_ignore_ascii_case(b"setname") && more >= 1 {
            name = Some(&args[i + 1]);
            i += 2;
        } else {
            out.error_parts(&[b"ERR Syntax error in HELLO option '", c_str(opt, usize::MAX), b"'"]);
            return Flow::Continue;
        }
    }
    if user.is_some_and(|u| u != b"default") {
        out.error(b"WRONGPASS invalid username-password pair or user is disabled.");
        return Flow::Continue;
    }
    if let Some(name) = name {
        if !set_name(client, name, out) {
            return Flow::Continue;
        }
    }
    if let Some(proto) = proto {
        client.resp.set(proto);
        out.set_protocol(if proto == 3 { resp::Protocol::Resp3 } else { resp::Protocol::Resp2 });
    }
    out.map(7);
    for (key, value) in [("server", "redis"), ("version", REDIS_VERSION)] {
        out.bulk(key.as_bytes());
        out.bulk(value.as_bytes());
    }
    out.bulk(b"proto");
    out.integer(i64::from(client.resp.get()));
    out.bulk(b"id");
    out.integer(client.id as i64);
    let role = if node.cluster.as_ref().and_then(|c| c.primary()).is_some() { "replica" } else { "master" };
    for (key, value) in [("mode", mode(node)), ("role", role)] {
        out.bulk(key.as_bytes());
        out.bulk(value.as_bytes());
    }
    out.bulk(b"modules");
    out.array(0);
    Flow::Continue
}

/// The replicas attached, as the table has them: host, port, and whether
/// they sync from scratch still.
/// The replicas attached and in the table: host, port, whether not online,
/// and their last acknowledgement (offset, seconds ago), 0 and 0 before it.
fn replicas(node: &Node) -> Vec<(String, u16, bool, u64, u64)> {
    let Some(c) = &node.cluster else { return Vec::new() };
    let (table, _) = c.table();
    node.repl
        .feeds
        .borrow()
        .iter()
        .filter(|f| f.is_replica())
        .filter_map(|f| table.position(&f.id).map(|i| (&table.nodes()[i], f)))
        .map(|(n, f)| {
            let (offset, ago) = f.acked().map_or((0, 0), |(o, ago)| (o, ago.as_secs()));
            (n.host().to_string(), n.port(), !f.online(), offset, ago)
        })
        .collect()
}

/// `ROLE`, as Redis 7.0 answers it: a primary `[master, offset, [[host,
/// port, offset]...]]`, each replica's offset as it last acknowledged it;
/// a replica `[slave, host, port, state, offset]`, the offset -1 before
/// its first sync.
pub(crate) fn role(_: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    match node.cluster.as_ref().and_then(|c| c.primary()) {
        None => {
            let replicas = replicas(node);
            out.array(3);
            out.bulk(b"master");
            out.integer(node.repl.offset() as i64);
            out.array(replicas.len());
            for (host, port, _, offset, _) in replicas {
                out.array(3);
                out.bulk(host.as_bytes());
                out.bulk(port.to_string().as_bytes());
                out.bulk(offset.to_string().as_bytes());
            }
        }
        Some(primary) => {
            use crate::repl::replica::State;
            let link = &node.repl.link;
            out.array(5);
            out.bulk(b"slave");
            out.bulk(primary.host().as_bytes());
            out.integer(i64::from(primary.port()));
            out.bulk(match link.state() {
                State::Off | State::Down => b"connect".as_slice(),
                State::Connecting => b"connecting",
                State::Copying => b"sync",
                State::Online => b"connected",
            });
            out.integer(if link.synced() { link.offset() as i64 } else { -1 });
        }
    }
    Flow::Continue
}

/// `standalone` or `cluster`, for `HELLO` and `INFO`.
fn mode(node: &Node) -> &'static str {
    if node.cluster.is_some() {
        "cluster"
    } else {
        "standalone"
    }
}

/// `CLIENT LIST [TYPE normal|master|replica|pubsub] [ID id [id ...]]`.
/// Every client is a normal one.
fn client_list(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let mut clients = node.clients.all();
    if args.len() == 4 && args[2].eq_ignore_ascii_case(b"type") {
        let ty = &args[3];
        if !ty.eq_ignore_ascii_case(b"normal") {
            let known = [&b"master"[..], b"replica", b"slave", b"pubsub"];
            if !known.iter().any(|k| ty.eq_ignore_ascii_case(k)) {
                out.error_parts(&[b"ERR Unknown client type '", c_str(ty, usize::MAX), b"'"]);
                return Flow::Continue;
            }
            clients.clear();
        }
    } else if args.len() > 3 && args[2].eq_ignore_ascii_case(b"id") {
        let mut ids = Vec::new();
        for arg in args.iter().skip(3) {
            match parse_i64(arg) {
                Some(id) if id > 0 => ids.push(id as u64),
                _ => {
                    out.error(b"ERR Invalid client ID");
                    return Flow::Continue;
                }
            }
        }
        clients.retain(|c| ids.contains(&c.id));
    } else if args.len() != 2 {
        syntax_error(out);
        return Flow::Continue;
    }
    let now = Instant::now();
    let mut text = String::new();
    for c in clients {
        c.info_line(now, &mut text);
    }
    out.verbatim(b"txt", text.as_bytes());
    Flow::Continue
}

/// `COMMAND`: every command's info.
pub(crate) fn command(_: &Args, _: &Node, _: &Client, out: &mut Encoder) -> Flow {
    out.array(COMMANDS.len());
    for cmd in COMMANDS {
        command_info_of(cmd, out);
    }
    Flow::Continue
}

/// `COMMAND INFO [name ...]`: nil for an unknown name; all commands without
/// names.
fn command_info(args: &Args, node: &Node, client: &Client, out: &mut Encoder) -> Flow {
    if args.len() == 2 {
        return command(args, node, client, out);
    }
    out.array(args.len() - 2);
    for name in args.iter().skip(2) {
        match find(COMMANDS, name) {
            Some(cmd) => command_info_of(cmd, out),
            None => out.null(),
        }
    }
    Flow::Continue
}

/// Redis 7's ten fields: name, arity, flags, first key, last key, key step,
/// ACL categories, tips, key specs, subcommands. The node tracks no flags,
/// categories, tips or key specs; those are empty.
fn command_info_of(cmd: &Command, out: &mut Encoder) {
    out.array(10);
    out.bulk(cmd.name.as_bytes());
    out.integer(i64::from(cmd.arity));
    out.set(0);
    let (first, last, step) = cmd.keys;
    out.integer(i64::from(first));
    out.integer(i64::from(last));
    out.integer(i64::from(step));
    out.set(0);
    out.set(0);
    out.array(0);
    out.array(cmd.subcommands.len());
    for sub in cmd.subcommands {
        command_info_of(sub, out);
    }
}

/// The parameters `CONFIG GET` knows. `CONFIG SET` changes only
/// `maxmemory` (and accepts `maxmemory-policy noeviction`).
fn config_params(node: &Node) -> [(&'static str, String); 8] {
    // Whole seconds, as Redis has it; rounded up so that a timeout under a
    // second doesn't read as 0, "never".
    let timeout = node.config.idle_timeout.map_or(0, |t| t.as_millis().div_ceil(1000));
    [
        ("appendonly", "no".to_string()),
        ("databases", "1".to_string()),
        ("maxmemory", node.maxmemory.get().to_string()),
        ("maxmemory-policy", "noeviction".to_string()),
        ("port", node.port.to_string()),
        ("proto-max-bulk-len", resp::Limits::default().max_bulk_len.to_string()),
        ("save", String::new()),
        ("timeout", timeout.to_string()),
    ]
}

/// `CONFIG GET parameter [parameter ...]`. A glob pattern (`*`, `?`)
/// matches canonical names; an exact name comes back spelled as the client
/// sent it, as in Redis. Each parameter is listed once.
fn config_get(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let params = config_params(node);
    let mut found: Vec<(&[u8], &str)> = Vec::new();
    for pattern in args.iter().skip(2) {
        let is_glob = pattern.iter().any(|&b| b == b'*' || b == b'?');
        for (name, value) in &params {
            let hit = if is_glob { glob_match(pattern, name.as_bytes()) } else { pattern.eq_ignore_ascii_case(name.as_bytes()) };
            if hit && !found.iter().any(|(n, _)| n.eq_ignore_ascii_case(name.as_bytes())) {
                found.push((if is_glob { name.as_bytes() } else { pattern }, value));
            }
        }
    }
    out.map(found.len());
    for (name, value) in found {
        out.bulk(name);
        out.bulk(value.as_bytes());
    }
    Flow::Continue
}

/// `CONFIG SET parameter value [parameter value ...]`: every pair is
/// checked before any applies, as in Redis. `maxmemory` takes a size
/// (`100mb`, as Redis does) or, ours, a share of the container's memory
/// limit (`75%`); `maxmemory-policy` only `noeviction`, the one policy.
fn config_set(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    // An odd count past the arity check is a syntax error in Redis 7.0.
    if !args.len().is_multiple_of(2) {
        syntax_error(out);
        return Flow::Continue;
    }
    let failed = |out: &mut Encoder, name: &[u8], why: &str| {
        out.error_parts(&[b"ERR CONFIG SET failed (possibly related to argument '", name, b"') - ", why.as_bytes()]);
    };
    let mut maxmemory = None;
    let mut seen: Vec<&[u8]> = Vec::new();
    for i in (2..args.len()).step_by(2) {
        let (name, value) = (&args[i], &args[i + 1]);
        let known = [&b"maxmemory"[..], b"maxmemory-policy"].into_iter().find(|k| name.eq_ignore_ascii_case(k));
        let Some(known) = known else {
            out.error_parts(&[b"ERR Unknown option or number of arguments for CONFIG SET - '", c_str(name, usize::MAX), b"'"]);
            return Flow::Continue;
        };
        if seen.contains(&known) {
            failed(out, known, "duplicate parameter");
            return Flow::Continue;
        }
        seen.push(known);
        if known == b"maxmemory" {
            let limit = std::str::from_utf8(value).ok().and_then(Limit::parse);
            match limit.map(Limit::resolve) {
                None => {
                    failed(out, known, "argument must be a memory value");
                    return Flow::Continue;
                }
                Some(Err(why)) => {
                    failed(out, known, &why);
                    return Flow::Continue;
                }
                Some(Ok(bytes)) => maxmemory = Some(bytes),
            }
        } else if !value.eq_ignore_ascii_case(b"noeviction") {
            failed(out, known, "only noeviction is supported");
            return Flow::Continue;
        }
    }
    if let Some(bytes) = maxmemory {
        node.maxmemory.set(bytes);
        let used = node.used_memory() as u64;
        if bytes != 0 && bytes < used {
            warning!("WARNING: the new maxmemory value set via CONFIG SET ({bytes}) is smaller than the current memory usage ({used}). This will result in writes being refused (noeviction).");
        }
    }
    out.ok();
    Flow::Continue
}

/// Case-insensitive glob with `*` and `?`; any other byte matches itself.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some((b'*', rest)) => (0..=text.len()).any(|i| glob_match(rest, &text[i..])),
        Some((&p, rest)) => match text.split_first() {
            Some((&t, text)) if p == b'?' || p.eq_ignore_ascii_case(&t) => glob_match(rest, text),
            _ => false,
        },
    }
}

/// The sections `INFO` knows, in output order.
const INFO_SECTIONS: [&str; 8] = ["server", "clients", "memory", "persistence", "stats", "replication", "cluster", "keyspace"];

/// `INFO [section ...]`: no section, `default`, `all` or `everything` give
/// every section; unknown names are skipped (an empty reply if nothing is
/// left), as in Redis.
pub(crate) fn info(args: &Args, node: &Node, _: &Client, out: &mut Encoder) -> Flow {
    let all = args.len() == 1
        || args.iter().skip(1).any(|a| [&b"default"[..], b"all", b"everything"].iter().any(|k| a.eq_ignore_ascii_case(k)));
    let mut text = String::new();
    for section in INFO_SECTIONS {
        if all || args.iter().skip(1).any(|a| a.eq_ignore_ascii_case(section.as_bytes())) {
            if !text.is_empty() {
                text.push_str("\r\n");
            }
            info_section(section, node, &mut text);
        }
    }
    out.verbatim(b"txt", text.as_bytes());
    Flow::Continue
}

fn info_section(section: &str, node: &Node, text: &mut String) {
    match section {
        "server" => {
            text.push_str("# Server\r\n");
            let uptime = node.started.elapsed().as_secs();
            field(text, "redis_version", REDIS_VERSION);
            field(text, "speedvault_version", env!("CARGO_PKG_VERSION"));
            field(text, "redis_mode", mode(node));
            field(text, "os", format_args!("{} {}", std::env::consts::OS, std::env::consts::ARCH));
            field(text, "arch_bits", usize::BITS);
            field(text, "multiplexing_api", if cfg!(target_os = "linux") { "epoll" } else { "kqueue" });
            field(text, "process_id", std::process::id());
            field(text, "tcp_port", node.port);
            field(text, "uptime_in_seconds", uptime);
            field(text, "uptime_in_days", uptime / 86400);
            field(text, "hz", 10);
        }
        "clients" => {
            text.push_str("# Clients\r\n");
            field(text, "connected_clients", node.clients.len());
        }
        "memory" => {
            // Redis's fields, as far as the node keeps them (see `memory`).
            text.push_str("# Memory\r\n");
            let (used, rss, max) = (node.used_memory() as u64, memory::rss() as u64, node.maxmemory.get());
            let peak = node.peak_memory.get() as u64;
            field(text, "used_memory", used);
            field(text, "used_memory_human", Human(used));
            field(text, "used_memory_rss", rss);
            field(text, "used_memory_rss_human", Human(rss));
            field(text, "used_memory_peak", peak);
            field(text, "used_memory_peak_human", Human(peak));
            field(text, "used_memory_startup", node.startup_memory);
            field(text, "used_memory_dataset", used.saturating_sub(node.startup_memory as u64));
            field(text, "maxmemory", max);
            field(text, "maxmemory_human", Human(max));
            field(text, "maxmemory_policy", "noeviction");
            let ratio = if used == 0 { 0.0 } else { rss as f64 / used as f64 };
            field(text, "mem_fragmentation_ratio", format_args!("{ratio:.2}"));
            field(text, "mem_allocator", "libc");
            // Ours: what the next new key would allocate at once when the
            // map must double (0 while it has room). Past maxmemory with
            // it, new keys are refused: stuart counts it as used.
            field(text, "mem_map_growth", node.store.borrow().growth());
        }
        "persistence" => {
            // Nothing is persisted; clients check `loading` before using a node.
            text.push_str("# Persistence\r\n");
            field(text, "loading", 0);
            field(text, "aof_enabled", 0);
        }
        "stats" => {
            text.push_str("# Stats\r\n");
            field(text, "total_connections_received", node.connections_received.get());
            field(text, "total_commands_processed", node.commands_processed.get());
            let syncs = &node.repl.syncs;
            field(text, "sync_full", syncs[0].get());
            field(text, "sync_partial_ok", syncs[1].get());
            field(text, "sync_partial_err", syncs[2].get());
        }
        "replication" => {
            // Redis 7.0's fields. A primary has no backlog until the
            // stream starts (its first replica); `slaveN`'s offset and lag
            // (seconds) are its last acknowledgement's.
            text.push_str("# Replication\r\n");
            let link = &node.repl.link;
            let primary = node.cluster.as_ref().and_then(|c| c.primary());
            match &primary {
                None => field(text, "role", "master"),
                Some(primary) => {
                    use crate::repl::replica::State;
                    let up = link.state() == State::Online;
                    field(text, "role", "slave");
                    field(text, "master_host", primary.host());
                    field(text, "master_port", primary.port());
                    field(text, "master_link_status", if up { "up" } else { "down" });
                    field(text, "master_last_io_seconds_ago", link.last_io_secs());
                    field(text, "master_sync_in_progress", u8::from(link.loading()));
                    field(text, "slave_read_repl_offset", link.offset());
                    field(text, "slave_repl_offset", link.offset());
                    if !up {
                        field(text, "master_link_down_since_seconds", link.down_secs());
                    }
                    field(text, "slave_priority", 100);
                    field(text, "slave_read_only", 1);
                    field(text, "replica_announced", 1);
                }
            }
            let replicas = replicas(node);
            field(text, "connected_slaves", replicas.len());
            for (i, (host, port, copying, offset, lag)) in replicas.iter().enumerate() {
                let state = if *copying { "send_bulk" } else { "online" };
                field(text, &format!("slave{i}"), format_args!("ip={host},port={port},state={state},offset={offset},lag={lag}"));
            }
            field(text, "master_failover_state", "no-failover");
            let repl = &node.repl;
            let (first, histlen) = repl.backlog_info();
            // A replica's stream is its primary's.
            let (replid, offset) = match (&primary, link.replid()) {
                (Some(_), Some(id)) => (id, link.offset()),
                _ => (repl.replid, repl.offset()),
            };
            field(text, "master_replid", replid);
            field(text, "master_replid2", "0".repeat(40));
            field(text, "master_repl_offset", offset);
            field(text, "second_repl_offset", -1);
            field(text, "repl_backlog_active", u8::from(repl.active()));
            field(text, "repl_backlog_size", repl.backlog_size());
            field(text, "repl_backlog_first_byte_offset", first);
            field(text, "repl_backlog_histlen", histlen);
        }
        "cluster" => {
            text.push_str("# Cluster\r\n");
            field(text, "cluster_enabled", u8::from(node.cluster.is_some()));
            // Ours: moving slots (phase 5).
            if let Some(c) = &node.cluster {
                field(text, "migrate_keys_sent", c.stats.keys_sent.get());
                field(text, "migrate_keys_imported", c.stats.keys_imported.get());
                field(text, "migrate_imports_refused", c.stats.imports_refused.get());
                field(text, "cluster_redirects_moved", c.stats.moved.get());
                field(text, "cluster_redirects_ask", c.stats.ask.get());
                field(text, "cluster_redirects_tryagain", c.stats.tryagain.get());
                field(text, "cluster_leaving", u64::from(c.leaving.load(std::sync::atomic::Ordering::Relaxed)));
            }
        }
        "keyspace" => {
            text.push_str("# Keyspace\r\n");
            let store = node.store.borrow();
            // Redis lists a database only when it has keys. It also keeps
            // an estimated average TTL; the node doesn't (0, as in a Redis
            // that hasn't sampled any yet).
            if store.dbsize() > 0 {
                let keys = store.dbsize();
                field(text, "db0", format_args!("keys={keys},expires={},avg_ttl=0", store.expiring_len()));
            }
        }
        _ => unreachable!("not in INFO_SECTIONS: {section}"),
    }
}

/// One `key:value` line of `INFO`.
fn field(text: &mut String, key: &str, value: impl std::fmt::Display) {
    let _ = write!(text, "{key}:{value}\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob() {
        assert!(glob_match(b"*", b"save"));
        assert!(glob_match(b"append*", b"appendonly"));
        assert!(glob_match(b"SA?E", b"save"));
        assert!(glob_match(b"*max*len", b"proto-max-bulk-len"));
        assert!(!glob_match(b"append*", b"save"));
        assert!(!glob_match(b"sav", b"save"));
        assert!(!glob_match(b"?", b""));
    }
}
