//! The Rust side of `tools/compat/cluster_compat.py`, over stdin and stdout.
//!
//! `cluster_compat keyslot`: reads one hex-encoded key per line, prints its
//! slot per line.
//!
//! `cluster_compat replies`: reads a layout,
//!
//! ```text
//! epoch 3
//! node <id> <host> <port>        (one per node, in reply order)
//! range <start> <end> <id | ->   (every slot exactly once; - is unassigned)
//! replica <id> <primary id>      (optional, in the order the replies list them)
//! offset <id> <n>                (optional: a replication offset; 0 if not given)
//! myself <id>                    (optional)
//! ```
//!
//! and prints `<reply> <protocol> <hex bytes>` for `slots`, `shards` and
//! `nodes` in `resp2` and `resp3`, or `error <message>`.

use cluster::{key_hash_slot, reply, Node, NodeId, SlotRange, SlotTable};
use resp::{Encoder, Protocol};
use std::io::{self, BufRead, Write};

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let lines: Vec<String> = io::stdin().lock().lines().map(|l| l.unwrap()).collect();
    let mut out = io::BufWriter::new(io::stdout().lock());
    match mode.as_str() {
        "keyslot" => {
            for line in &lines {
                writeln!(out, "{}", key_hash_slot(&unhex(line.trim()))).unwrap();
            }
        }
        "replies" => match replies(&lines) {
            Ok(text) => out.write_all(text.as_bytes()).unwrap(),
            Err(e) => writeln!(out, "error {e}").unwrap(),
        },
        _ => {
            eprintln!("usage: cluster_compat keyslot|replies < input");
            std::process::exit(2);
        }
    }
}

fn replies(lines: &[String]) -> Result<String, String> {
    let (mut epoch, mut nodes, mut ranges, mut myself) = (0, Vec::new(), Vec::new(), None);
    let (mut replicas, mut offsets) = (Vec::new(), Vec::new());
    let id = |text: &str| NodeId::parse(text.as_bytes()).ok_or(format!("bad node id {text}"));
    let num = |text: &str| text.parse::<u64>().map_err(|e| format!("{text}: {e}"));
    for line in lines {
        let words: Vec<&str> = line.split_whitespace().collect();
        match words[..] {
            [] => {}
            ["epoch", n] => epoch = num(n)?,
            ["node", i, host, port] => nodes.push(Node::new(id(i)?, host, num(port)? as u16).map_err(|e| e.to_string())?),
            ["range", s, e, owner] => {
                let owner = if owner == "-" { None } else { Some(id(owner)?) };
                ranges.push((SlotRange::new(num(s)? as u16, num(e)? as u16), owner));
            }
            ["replica", r, p] => replicas.push((id(r)?, id(p)?)),
            ["offset", i, n] => offsets.push((id(i)?, num(n)?)),
            ["myself", i] => myself = Some(id(i)?),
            _ => return Err(format!("bad line: {line}")),
        }
    }
    let table = SlotTable::new(epoch, nodes, &ranges).and_then(|t| t.with_replicas(&replicas)).map_err(|e| e.to_string())?;
    let mut by_index = vec![0; table.nodes().len()];
    for (i, n) in offsets {
        by_index[table.position(&i).ok_or(format!("offset of an unknown node {i}"))?] = n;
    }
    let mut text = String::new();
    for (name, protocol) in [("resp2", Protocol::Resp2), ("resp3", Protocol::Resp3)] {
        let encode = |f: &dyn Fn(&mut Encoder)| {
            let mut out = Encoder::new(protocol);
            f(&mut out);
            hex(out.as_bytes())
        };
        text += &format!("slots {name} {}\n", encode(&|out| reply::slots(&table, &by_index, out)));
        text += &format!("shards {name} {}\n", encode(&|out| reply::shards(&table, &by_index, out)));
        text += &format!("nodes {name} {}\n", encode(&|out| reply::nodes(&table, myself.as_ref(), out)));
    }
    Ok(text)
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
