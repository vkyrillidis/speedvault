//! One client connection: the `STUART` commands, the layout as clients ask
//! for it (`CLUSTER SLOTS SHARDS NODES INFO KEYSLOT`), and what clients send
//! on connect (`HELLO`, `PING`, `COMMAND`, `CLIENT`, `INFO`).
//!
//! stuart holds no keys: a data command is redirected with `-MOVED` to the
//! node owning its first key, as a node owning none would redirect it, so
//! stuart can be a cluster client's seed (`redis-cli -c -p 7600 GET k`).

use crate::state::State;
use crate::{keeper, Stuart};
use cluster::{key_hash_slot, reply, Node, NodeId};
use dbnode::{notice, verbose, warning};
use reactor::TcpStream;
use resp::{Args, Encoder, Limits, Parsed, Parser, Protocol};
use std::io::{self, Read, Write};
use std::rc::Rc;
use std::time::Instant;

/// The dbnode's data commands; the first key is the first argument.
const KEYED: &[&str] = &[
    "get", "set", "del", "incr", "mget", "mset", "exists", "decr", "incrby", "decrby", "setnx", "getdel", "append",
    "strlen", "expire", "pexpire", "expireat", "pexpireat", "ttl", "pttl", "persist", "type",
];

#[derive(PartialEq, Eq)]
enum Flow {
    Continue,
    Close,
}

pub(crate) fn handle(conn: TcpStream, stuart: &Rc<Stuart>) {
    let peer = conn.peer_addr().map_or_else(|_| "?".to_string(), |a| a.to_string());
    verbose!("Accepted {peer}");
    match serve(&conn, stuart) {
        Ok(()) => verbose!("Client {peer} closed the connection"),
        Err(e) => verbose!("Client {peer}: {e}"),
    }
}

fn serve(mut conn: &TcpStream, stuart: &Rc<Stuart>) -> io::Result<()> {
    let id = stuart.next_client_id.get();
    stuart.next_client_id.set(id + 1);
    let mut parser = Parser::new(Limits::default());
    let mut out = Encoder::new(Protocol::Resp2);
    let mut buf = vec![0; 16 * 1024];
    let (mut start, mut end) = (0, 0);
    loop {
        let mut close = false;
        while !close {
            match parser.parse(&buf[start..end]) {
                Ok(Parsed::Complete { consumed }) => {
                    let args = parser.args(&buf[start..end]);
                    if !args.is_empty() && execute(&args, stuart, id, &mut out) == Flow::Close {
                        close = true;
                    }
                    start += consumed;
                }
                Ok(Parsed::Incomplete) => break,
                Err(e) => {
                    e.encode(&mut out);
                    close = true;
                }
            }
        }
        if !out.is_empty() {
            conn.write_all(out.as_bytes())?;
            out.clear();
        }
        if close {
            return Ok(());
        }
        if start == end {
            (start, end) = (0, 0);
        } else if buf.len() - end < 4096 {
            buf.copy_within(start..end, 0);
            (start, end) = (0, end - start);
            if buf.len() - end < 4096 {
                buf.resize(buf.len() * 2, 0);
            }
        }
        let n = conn.read(&mut buf[end..])?;
        if n == 0 {
            return Ok(());
        }
        end += n;
    }
}

fn execute(args: &Args, stuart: &Rc<Stuart>, client: u64, out: &mut Encoder) -> Flow {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let sub = args.get(1).map(|s| String::from_utf8_lossy(s).to_ascii_lowercase());
    match (name.as_str(), sub.as_deref(), args.len()) {
        ("ping", _, 1) => out.simple(b"PONG"),
        ("ping", _, 2) | ("echo", _, 2) => out.bulk(&args[1]),
        ("quit", _, _) => {
            out.ok();
            return Flow::Close;
        }
        ("hello", _, _) => hello(args, client, out),
        ("command", Some("docs"), _) => out.map(0),
        ("command", Some("count"), 2) => out.integer(0),
        ("command", _, _) => out.array(0),
        ("client", Some("setname" | "setinfo"), _) => out.ok(),
        ("client", Some("id"), 2) => out.integer(client as i64),
        ("info", _, _) => info(stuart, out),
        ("cluster", Some("slots"), 2) => {
            let view = stuart.state.borrow().view();
            reply::slots(&view, &stuart.offsets(&view), out)
        }
        ("cluster", Some("shards"), 2) => {
            let view = stuart.state.borrow().view();
            reply::shards(&view, &stuart.offsets(&view), out)
        }
        ("cluster", Some("nodes"), 2) => reply::nodes(&stuart.state.borrow().view(), None, out),
        ("cluster", Some("info"), 2) => out.verbatim(b"txt", reply::info_text(&stuart.state.borrow().view()).as_bytes()),
        ("cluster", Some("keyslot"), 3) => out.integer(i64::from(key_hash_slot(&args[2]))),
        ("cluster", Some("myid"), 2) => out.error(b"ERR stuart is not a node and has no node id"),
        ("stuart", Some("join"), 5 | 7 | 9) => join(args, stuart, out),
        ("stuart", Some("create"), 2) => create(stuart, out),
        ("stuart", Some("assign"), 5) => assign(args, stuart, out),
        ("stuart", Some("move"), 5) => move_slots(args, stuart, out),
        ("stuart", Some("rebalance"), 2) => {
            let full = stuart.full_nodes();
            queue_moves(stuart, out, "rebalance", |s| s.rebalance(&full))
        }
        ("stuart", Some("drain"), 3) => match NodeId::parse(&args[2]) {
            Some(id) => match stuart.room_for(&id) {
                Ok(()) => {
                    let full = stuart.full_nodes();
                    queue_moves(stuart, out, &format!("drain {id}"), |s| s.drain(&id, &full))
                }
                Err(e) => {
                    warning!("STUART DRAIN {id} refused: {e}");
                    out.error(e.as_bytes())
                }
            },
            None => out.error_parts(&[b"ERR Invalid node id '", &args[2], b"'"]),
        },
        ("stuart", Some("leave"), 3) => match NodeId::parse(&args[2]) {
            Some(id) => leave(stuart, &id, out),
            None => out.error_parts(&[b"ERR Invalid node id '", &args[2], b"'"]),
        },
        ("stuart", Some("forget"), 3) => match NodeId::parse(&args[2]) {
            Some(id) => forget(stuart, &id, out),
            None => out.error_parts(&[b"ERR Invalid node id '", &args[2], b"'"]),
        },
        ("stuart", Some("migrations"), 2) => migrations(stuart, out),
        ("stuart", Some("nodes"), 2) => nodes(stuart, out),
        ("stuart", Some("table"), 2) => match stuart.state.borrow().table() {
            Some(t) => out.verbatim(b"txt", format!("epoch {}\n{}", t.epoch(), reply::nodes_text(t, None)).as_bytes()),
            None => out.null(),
        },
        (cmd, _, n) if KEYED.contains(&cmd) && n >= 2 => {
            let slot = key_hash_slot(&args[1]);
            match stuart.state.borrow().view().owner(slot) {
                Some(node) => out.error(format!("MOVED {slot} {}:{}", node.host(), node.port()).as_bytes()),
                None => out.error(b"CLUSTERDOWN Hash slot not served"),
            }
        }
        ("cluster" | "stuart" | "client", Some(sub), _) => {
            let known: &[&str] = match name.as_str() {
                "cluster" => &["slots", "shards", "nodes", "info", "keyslot", "myid"],
                "stuart" => &["join", "create", "assign", "move", "rebalance", "drain", "leave", "forget", "migrations", "nodes", "table"],
                _ => &["setname", "setinfo", "id"],
            };
            if known.contains(&sub) {
                out.error(format!("ERR wrong number of arguments for '{name}|{sub}' command").as_bytes());
            } else {
                let upper = name.to_ascii_uppercase();
                out.error_parts(&[b"ERR unknown subcommand '", &args[1], b"'. Try ", upper.as_bytes(), b" HELP."]);
            }
        }
        ("ping" | "echo" | "cluster" | "stuart" | "client", _, _) => {
            out.error(format!("ERR wrong number of arguments for '{name}' command").as_bytes())
        }
        _ if KEYED.contains(&name.as_str()) => {
            out.error(format!("ERR wrong number of arguments for '{name}' command").as_bytes())
        }
        _ => {
            let mut msg = b"ERR unknown command '".to_vec();
            msg.extend_from_slice(&args[0]);
            msg.extend_from_slice(b"', with args beginning with: ");
            for arg in args.iter().skip(1) {
                msg.push(b'\'');
                msg.extend_from_slice(arg);
                msg.extend_from_slice(b"' ");
            }
            out.error(&msg);
        }
    }
    Flow::Continue
}

/// `HELLO [2|3]`, answered as the dbnode answers it (clients pick features
/// by `version`); `INFO` tells stuart apart.
fn hello(args: &Args, client: u64, out: &mut Encoder) {
    match args.get(1) {
        None => {}
        Some(b"2") => out.set_protocol(Protocol::Resp2),
        Some(b"3") => out.set_protocol(Protocol::Resp3),
        Some(_) => return out.error(b"NOPROTO unsupported protocol version"),
    }
    if args.len() > 2 {
        return out.error_parts(&[b"ERR Syntax error in HELLO option '", &args[2], b"'"]);
    }
    let proto = if out.protocol() == Protocol::Resp3 { 3 } else { 2 };
    out.map(7);
    for (key, value) in [("server", "redis"), ("version", "7.0.15")] {
        out.bulk(key.as_bytes());
        out.bulk(value.as_bytes());
    }
    out.bulk(b"proto");
    out.integer(proto);
    out.bulk(b"id");
    out.integer(client as i64);
    for (key, value) in [("mode", "cluster"), ("role", "master")] {
        out.bulk(key.as_bytes());
        out.bulk(value.as_bytes());
    }
    out.bulk(b"modules");
    out.array(0);
}

fn info(stuart: &Stuart, out: &mut Encoder) {
    let uptime = stuart.started.elapsed().as_secs();
    let state = stuart.state.borrow();
    let epoch = state.table().map_or(0, |t| t.epoch());
    let text = format!(
        "# Server\r\nredis_version:7.0.15\r\nstuart_version:{}\r\nredis_mode:cluster\r\nprocess_id:{}\r\ntcp_port:{}\r\n\
         uptime_in_seconds:{uptime}\r\n\r\n# Cluster\r\ncluster_enabled:1\r\nstuart_nodes:{}\r\nstuart_keepers:{}\r\n\
         stuart_table_epoch:{epoch}\r\nstuart_auto:{}\r\nstuart_leaving:{}\r\nstuart_full:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        stuart.port,
        state.nodes().len(),
        stuart.keepers.get(),
        u8::from(stuart.auto),
        state.leaving().len(),
        stuart.full_nodes().len(),
    );
    out.verbatim(b"txt", text.as_bytes());
}

/// `STUART JOIN id host port [NAME name] [ZONE zone]`: from a node starting
/// with `--stuart` (and `--name`, `--zone`).
fn join(args: &Args, stuart: &Rc<Stuart>, out: &mut Encoder) {
    let (mut name, mut zone) = (None, None);
    for i in (5..args.len()).step_by(2) {
        let (key, value) = (&args[i], &args[i + 1]);
        let (slot, what) = if key.eq_ignore_ascii_case(b"name") {
            (&mut name, &b"name"[..])
        } else if key.eq_ignore_ascii_case(b"zone") {
            (&mut zone, &b"zone"[..])
        } else {
            return out.error(b"ERR syntax error");
        };
        if slot.is_some() {
            return out.error(b"ERR syntax error");
        }
        match std::str::from_utf8(value) {
            Ok(value) if dbnode::valid_name(value) => *slot = Some(value),
            _ => return out.error_parts(&[b"ERR Invalid ", what, b" '", value, b"'"]),
        }
    }
    let Some(id) = NodeId::parse(&args[2]) else {
        return out.error_parts(&[b"ERR Invalid node id '", &args[2], b"'"]);
    };
    let port = std::str::from_utf8(&args[4]).ok().and_then(|p| p.parse::<u16>().ok()).filter(|&p| p != 0);
    let Some(port) = port else {
        return out.error_parts(&[b"ERR Invalid port '", &args[4], b"'"]);
    };
    let node = std::str::from_utf8(&args[3]).ok().and_then(|host| Node::new(id, host, port).ok());
    let Some(node) = node else {
        return out.error_parts(&[b"ERR Invalid host '", &args[3], b"'"]);
    };
    let addr = format!("{}:{}", node.host(), node.port());
    let named = name.map_or(String::new(), |n| format!(" ({n})"));
    // Why a node it replaces would be: known before the join changes it.
    let same_name: Vec<NodeId> = {
        let s = stuart.state.borrow();
        s.nodes().iter().map(|n| *n.id()).filter(|n| name.is_some() && s.name(n) == name).collect()
    };
    // It replaces a primary with replicas (its pod restarted, with a new
    // id and no keys): the old one is dead. Its replica is promoted first
    // (no wait: nothing is left to fence), then the pod's next try joins,
    // replacing a replica.
    let dying: Vec<NodeId> = {
        let s = stuart.state.borrow();
        let replaced = |n: &Node| n.id() != &id && ((n.host(), n.port()) == (node.host(), node.port()) || same_name.contains(n.id()));
        s.table()
            .map(|t| {
                (0..t.nodes().len())
                    .filter(|&i| t.primary_of(i).is_none() && t.replicas_of(i).next().is_some() && replaced(&t.nodes()[i]))
                    .map(|i| *t.nodes()[i].id())
                    .filter(|p| !stuart.unpromotable.borrow().contains(p))
                    .collect()
            })
            .unwrap_or_default()
    };
    if let Some(p) = dying.first() {
        if stuart.dead.borrow_mut().insert(*p) {
            notice!("Node {id} at {addr}{named} replaces primary {p}: failing it over to its replica first");
        }
        return out.error(format!("TRYAGAIN Failing node {p} over to its replica first").as_bytes());
    }
    let joined = match stuart.change(|s| s.join_in(node, name, zone)) {
        Ok(joined) => joined,
        Err(e) => {
            warning!("Can't save the state: {e}");
            return out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    };
    for (old, lost) in &joined.replaced {
        let why = if same_name.contains(old) { "joined with the same name" } else { "announced the same address" };
        warning!("Node {id} at {addr}{named} replaces node {old}, which {why}: {lost} slots lost (unassigned)");
    }
    let epoch = stuart.state.borrow().table().map_or(String::new(), |t| format!("; table epoch {}", t.epoch()));
    match joined.change {
        crate::state::Change::Known => verbose!("Node {id} at {addr}{named} joined again"),
        crate::state::Change::Moved => notice!("Node {id}{named} moved to {addr}{epoch}"),
        crate::state::Change::Added => {
            let role = joined.primary.map_or(String::new(), |p| format!(" as a replica of {p}"));
            notice!("Node {id} joined at {addr}{named}{role}{epoch}");
            let s = stuart.clone();
            if let Err(e) = reactor::spawn(move || keeper::keep(s, id)) {
                warning!("No fiber to keep node {id}: {e}");
            }
        }
    }
    // The join stands even if the automatic CREATE can't be saved.
    stuart.auto_create();
    if joined.change == crate::state::Change::Added || !joined.replaced.is_empty() {
        stuart.replan();
    }
    out.ok();
}

/// `STUART LEAVE id`: from a node that is stopping (or by hand). Replies
/// what it still holds, plus one while it is a node: 0 once it has left.
/// A primary with a replica that can take over hands its slots to it (see
/// `handover`); else they are drained, refused if the other nodes haven't
/// the memory for its keys: the node then stops at once, rather than wait
/// for room until it's killed. A replica's keys are copies: it just goes.
fn leave(stuart: &Stuart, id: &NodeId, out: &mut Encoder) {
    let was = stuart.state.borrow().node(id).is_some() && !stuart.state.borrow().leaving().contains(id);
    let replica = stuart.state.borrow().primary_of(id).is_some();
    let hand = was && stuart.can_hand_over(id);
    if was && !replica && !hand && stuart.state.borrow().table().is_some() {
        if let Err(e) = stuart.room_for(id) {
            warning!("Node {id} may not leave, its keys would be lost: {e}");
            return out.error(e.as_bytes());
        }
    }
    let full = stuart.full_nodes();
    match stuart.change(|s| if hand { s.leave_handing(id, &full) } else { s.leave(id, &full) }) {
        Ok(Ok(n)) => {
            if was {
                if n == 0 && stuart.state.borrow().shutting_down() {
                    warning!("Node {id} is leaving, and so is every other: the cluster is shutting down, the keys go with the nodes");
                } else if n == 0 {
                    notice!("Node {id} left (there is no table yet)");
                } else if hand {
                    notice!("Primary {id} is leaving: it hands its {} slots to a replica", n - 1);
                } else if replica {
                    notice!("Replica {id} is leaving");
                } else {
                    notice!("Node {id} is leaving: it holds {} slots", n - 1);
                    stuart.replan();
                }
            }
            out.integer(n as i64);
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART FORGET id`: a node that is gone for good. Replies how many slots
/// were lost.
fn forget(stuart: &Stuart, id: &NodeId, out: &mut Encoder) {
    match stuart.change(|s| s.forget(id)) {
        Ok(Ok(lost)) => {
            let epoch = stuart.state.borrow().table().map_or(String::new(), |t| format!("; table epoch {}", t.epoch()));
            warning!("Forgot node {id}: {lost} slots lost (unassigned){epoch}");
            stuart.replan();
            out.integer(lost as i64);
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART CREATE`: the slots split over the nodes that joined.
fn create(stuart: &Stuart, out: &mut Encoder) {
    match stuart.change(|s| s.create()) {
        Ok(Ok(())) => {
            let n = stuart.state.borrow().nodes().len();
            notice!("Created the slot table: 16384 slots over {n} nodes, epoch 1");
            out.ok();
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART ASSIGN start end id`: unassigned (lost) slots to a node.
fn assign(args: &Args, stuart: &Stuart, out: &mut Encoder) {
    let slot = |arg: &[u8]| std::str::from_utf8(arg).ok().and_then(|s| s.parse::<u16>().ok());
    let (Some(start), Some(end)) = (slot(&args[2]), slot(&args[3])) else {
        return out.error(b"ERR Invalid slot range");
    };
    let Some(id) = NodeId::parse(&args[4]) else {
        return out.error_parts(&[b"ERR Invalid node id '", &args[4], b"'"]);
    };
    match stuart.change(|s| s.assign(cluster::SlotRange::new(start, end), &id)) {
        Ok(Ok(n)) => {
            let epoch = stuart.state.borrow().table().map_or(0, |t| t.epoch());
            notice!("Assigned slots {start}-{end} ({n}) to node {id}; table epoch {epoch}");
            out.ok();
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART MOVE start end id`: the slots of the range to move to node `id`,
/// with their keys. Replies how many were queued (those `id` owns already
/// don't move).
fn move_slots(args: &Args, stuart: &Stuart, out: &mut Encoder) {
    let slot = |arg: &[u8]| std::str::from_utf8(arg).ok().and_then(|s| s.parse::<u16>().ok());
    let (Some(start), Some(end)) = (slot(&args[2]), slot(&args[3])) else {
        return out.error(b"ERR Invalid slot range");
    };
    let Some(id) = NodeId::parse(&args[4]) else {
        return out.error_parts(&[b"ERR Invalid node id '", &args[4], b"'"]);
    };
    match stuart.change(|s| s.move_slots(cluster::SlotRange::new(start, end), &id)) {
        Ok(Ok(n)) => {
            notice!("Slots {start}-{end} to move to node {id}: {n} queued");
            out.integer(n as i64);
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART REBALANCE` and `STUART DRAIN id`: the moves `plan` queues;
/// replies how many slots will move.
fn queue_moves(stuart: &Stuart, out: &mut Encoder, what: &str, plan: impl FnOnce(&mut State) -> Result<usize, String>) {
    match stuart.change(plan) {
        Ok(Ok(n)) => {
            notice!("STUART {}: {n} slots to move", what.to_uppercase());
            out.integer(n as i64);
        }
        Ok(Err(e)) => out.error(e.as_bytes()),
        Err(e) => {
            warning!("Can't save the state: {e}");
            out.error(format!("ERR Can't save the state: {e}").as_bytes());
        }
    }
}

/// `STUART MIGRATIONS`: a line per slot moving, `slot from -> to
/// for=Ns`, then `queued: N` when slots wait to move.
fn migrations(stuart: &Stuart, out: &mut Encoder) {
    let state = stuart.state.borrow();
    let mut text = String::new();
    if let Some(t) = state.table() {
        let since = stuart.moving_since.borrow();
        for m in t.migrations() {
            let (from, to) = (t.nodes()[m.from].id(), t.nodes()[m.to].id());
            let age = since.get(&m.slot).map_or("?".to_string(), |s| s.elapsed().as_secs().to_string());
            text += &format!("{} {from} -> {to} for={age}s\n", m.slot);
        }
    }
    if !state.queue().is_empty() {
        text += &format!("queued: {}\n", state.queue().len());
    }
    out.verbatim(b"txt", text.as_bytes());
}

/// `STUART NODES`: a line per joined node, `id host:port up|down since=Ns
/// acked=E slots=N [replica-of=ID] [name=NAME] [zone=ZONE] [used=B max=B]
/// [full] [leaving] [demoting] [failed] [error=...]`, and the slots no node owns, if
/// any.
fn nodes(stuart: &Stuart, out: &mut Encoder) {
    let state = stuart.state.borrow();
    let health = stuart.health.borrow();
    let view = state.view();
    let by_node = view.ranges_by_node();
    let now = Instant::now();
    let mut text = String::new();
    for (node, ranges) in view.nodes().iter().zip(&by_node) {
        let slots: usize = ranges.iter().map(|r| r.len()).sum();
        let (status, since, acked, error) = match health.get(node.id()) {
            Some(h) => (if h.up { "up" } else { "down" }, now.duration_since(h.since).as_secs(), h.acked, h.error.clone()),
            None => ("unknown", 0, 0, None),
        };
        text += &format!("{} {}:{} {status} since={since}s acked={acked} slots={slots}", node.id(), node.host(), node.port());
        if let Some(primary) = state.primary_of(node.id()) {
            text += &format!(" replica-of={primary}");
        }
        if let Some(name) = state.name(node.id()) {
            text += &format!(" name={name}");
        }
        if let Some(zone) = state.zone(node.id()) {
            text += &format!(" zone={zone}");
        }
        if let Some(m) = health.get(node.id()).and_then(|h| h.memory) {
            text += &format!(" used={} max={}", m.used, m.max);
            if m.full() {
                text += " full";
            }
        }
        if state.leaving().contains(node.id()) {
            text += " leaving";
        }
        if state.demoting() == Some(node.id()) {
            text += " demoting";
        }
        if state.is_failed(node.id()) {
            text += " failed";
        }
        if let Some(e) = error {
            text += &format!(" error={e:?}");
        }
        text.push('\n');
    }
    if state.table().is_some() && view.assigned() < cluster::SLOTS {
        text += &format!("unassigned slots: {}\n", cluster::SLOTS - view.assigned());
    }
    out.verbatim(b"txt", text.as_bytes());
}
