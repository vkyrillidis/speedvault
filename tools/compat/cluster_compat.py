#!/usr/bin/env python3
"""The cluster crate against a real Redis Cluster (run by cluster_compat.sh).

1. CLUSTER KEYSLOT: random keys (binary ones, and many with braces) go to
   Redis pipelined, and through `cluster_compat keyslot` to
   cluster::key_hash_slot; every slot must match.
2. CLUSTER SLOTS / SHARDS / NODES, raw RESP2 and RESP3 bytes, for each
   layout below. The layout is set up in Redis (ADDSLOTS, MEET), and the same
   layout, with Redis's node ids, addresses and node order, is given to
   `cluster_compat replies`. SLOTS and SHARDS must be byte for byte the same,
   SHARDS' shards in any order (Redis 8 lists them in its hash table's
   order of their random shard ids).
   NODES too, after masking what a table doesn't have (see reply.rs): the
   bus port, ping and pong times, and the config epoch. Two layouts have
   replicas (CLUSTER REPLICATE): with nothing written, their replication
   offsets stay 0 (the servers run with a long repl-ping-replica-period),
   as ours are until replicas copy data: left out of SLOTS, `loading` in
   SHARDS.
3. With --dbnode: our node in cluster mode in a Redis node's place
   (announcing its address, given its view with CLUSTER SETTABLE), a
   replica's too. The same requests to both must get the same replies,
   node ids masked: MOVED, CROSSSLOT, CLUSTERDOWN, the CLUSTER commands.

    python3 cluster_compat.py --bin PATH --ports 7801,7802,7803,7804 [--keys N] [--dbnode PATH]

The first three ports are the cluster `redis-cli --cluster create` made;
the fourth is a cluster-enabled node outside it. Each layout but the first
resets every node (CLUSTER RESET HARD), so the nodes get new ids.
"""

from __future__ import annotations

import argparse
import random
import re
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple

from resp import Conn, cmd, parse

Ranges = List[Tuple[int, int]]


def call(port: int, *args):
    with Conn("127.0.0.1", port, timeout=5) as c:
        return c.call(*args)


def raw_reply(port: int, protocol: int, *args) -> bytes:
    with Conn("127.0.0.1", port, timeout=5) as c:
        if protocol == 3:
            c.call("HELLO", "3")
        c.send(cmd(*args))
        raw, _ = c.read_reply()
        return raw


def random_key(rng: random.Random) -> bytes:
    kind = rng.randrange(6)
    n = rng.randrange(0, 24)
    if kind == 0:  # any bytes
        return bytes(rng.randrange(256) for _ in range(n))
    if kind == 1:  # printable
        return bytes(rng.randrange(32, 127) for _ in range(n))
    # Mostly braces and a few other bytes, so every tag case comes up.
    alphabet = b"{}{}{}ab\x00\xff\r\n "
    return bytes(rng.choice(alphabet) for _ in range(n))


def check_keyslot(binary: str, port: int, count: int, seed: int) -> int:
    rng = random.Random(seed)
    fixed = [b"", b"foo", b"bar", b"123456789", b"{}", b"{", b"}{", b"{a}{b}", b"a{b", b"{{a}}", b"{}a{b}",
             b"{user42}:name", b"x" * 1000, b"{" + b"\xff" * 300 + b"}"]
    keys = fixed + [random_key(rng) for _ in range(count - len(fixed))]
    theirs = []
    with Conn("127.0.0.1", port, timeout=10) as c:
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            c.send(b"".join(cmd("CLUSTER", "KEYSLOT", k) for k in batch))
            theirs += [r.value for _, r in c.read_replies(len(batch))]
    out = subprocess.run([binary, "keyslot"], input="".join(k.hex() + "\n" for k in keys),
                         capture_output=True, text=True, check=True).stdout
    ours = [int(line) for line in out.split()]
    bad = [(k, t, o) for k, t, o in zip(keys, theirs, ours) if t != o]
    for k, t, o in bad[:10]:
        print(f"  KEYSLOT {k!r}: redis {t}, ours {o}")
    braced = sum(1 for k in keys if b"{" in k)
    print(f"keyslot: {len(keys)} keys ({braced} with '{{'), {len(bad)} differences")
    return 1 if bad or len(ours) != len(keys) else 0


class Node:
    def __init__(self, port: int):
        self.port = port
        self.id = call(port, "CLUSTER", "MYID").value.decode()


def nodes_view(port: int) -> List[List[str]]:
    text = call(port, "CLUSTER", "NODES").value.decode()
    return [line.split() for line in text.splitlines()]


def slots_text(ranges: Ranges) -> List[str]:
    return [str(s) if s == e else f"{s}-{e}" for s, e in ranges]


def wait_for(port: int, nodes: List[Node], layout: Dict[int, Ranges], timeout: float = 30,
             replicas: Optional[Dict[int, int]] = None) -> List[List[str]]:
    """Until `port` knows every node, connected, with the layout's slots,
    and the replicas (port -> primary port) as replicas."""
    want = {n.id: slots_text(layout.get(n.port, [])) for n in nodes}
    by_port = {n.port: n.id for n in nodes}
    primaries = {by_port[r]: by_port[p] for r, p in (replicas or {}).items()}
    deadline = time.time() + timeout
    while True:
        view = nodes_view(port)
        got = {f[0]: f[8:] for f in view if f[7] == "connected" and "handshake" not in f[2] and "fail" not in f[2]}
        roles = {f[0]: f[3] for f in view if "slave" in f[2]}
        if got == want and len(view) == len(nodes) and roles == primaries:
            return view
        if time.time() > deadline:
            raise SystemExit(f"cluster didn't converge on {port}: {view}")
        time.sleep(0.2)


def replica_order(port: int) -> List[Tuple[str, str]]:
    """(replica id, primary id) in the order `port` lists them in CLUSTER
    SHARDS: each primary's in the order they attached (gossip's, at `port`)."""
    pairs = []
    for shard in call(port, "CLUSTER", "SHARDS").value:
        fields = shard.value
        nodes = fields[[f.value for f in fields[::2]].index(b"nodes") * 2 + 1].value
        ids = []
        for n in nodes:
            kv = {k.value: v.value for k, v in zip(n.value[::2], n.value[1::2])}
            ids.append((kv[b"id"].decode(), kv[b"role"]))
        primary = next(i for i, role in ids if role == b"master")
        pairs += [(i, primary) for i, role in ids if role == b"replica"]
    return pairs


def table_input(view: List[List[str]], layout: Dict[int, Ranges], by_id: Dict[str, Node], myself: str,
                replicas: Optional[List[Tuple[str, str]]] = None) -> str:
    """The layout for `cluster_compat replies`: Redis's ids, addresses and
    order, our own slot ranges (not parsed back from Redis), and the
    replicas as given (in Redis's order)."""
    lines = ["epoch 1"]
    owner = [None] * 16384
    for f in view:
        host, port = f[1].split("@")[0].rsplit(":", 1)
        lines.append(f"node {f[0]} {host} {port}")
        for s, e in layout.get(by_id[f[0]].port, []):
            for slot in range(s, e + 1):
                owner[slot] = f[0]
    start = 0
    for slot in range(1, 16385):
        if slot == 16384 or owner[slot] != owner[start]:
            lines.append(f"range {start} {slot - 1} {owner[start] or '-'}")
            start = slot
    lines += [f"replica {r} {p}" for r, p in replicas or []]
    lines.append(f"myself {myself}")
    return "\n".join(lines) + "\n"


NODES_LINE = re.compile(r"^(\S+ \S+?)@\d+ (\S+ \S+) \d+ \d+ \d+ (.*)$")


def mask_nodes(raw: bytes) -> Tuple[bytes, str]:
    """(the reply's type and length check, its text with live fields masked)."""
    kind = raw[:1]
    head, _, rest = raw.partition(b"\r\n")
    n = int(head[1:])
    body = rest[:n]
    framing_ok = rest[n:] == b"\r\n" and (kind == b"$" or body.startswith(b"txt:"))
    if kind == b"=":
        body = body[4:]
    lines = []
    for line in body.decode().split("\n"):
        m = NODES_LINE.match(line)
        lines.append(f"{m.group(1)}@BUS {m.group(2)} PING PONG EPOCH {m.group(3)}" if m else line)
    return kind + (b"ok" if framing_ok else b"BAD FRAMING"), "\n".join(lines)


def sorted_shards(raw: bytes, mask=lambda b: b) -> bytes:
    """A CLUSTER SHARDS reply with its shards sorted, each byte for byte
    after `mask`."""
    reply, _ = parse(raw)
    if reply.kind != "*":
        return mask(raw)
    return raw[:raw.index(b"\r\n")] + b"".join(sorted(mask(e.encode()) for e in reply.value))


def compare_layout(name: str, binary: str, nodes: List[Node], layout: Dict[int, Ranges], ask: List[int],
                   replicas: Optional[Dict[int, int]] = None) -> int:
    by_id = {n.id: n for n in nodes}
    failures = 0
    for port in ask:
        view = wait_for(port, nodes, layout, replicas=replicas)
        myself = next(f[0] for f in view if "myself" in f[2])
        ours = {}
        pairs = replica_order(port) if replicas else []
        out = subprocess.run([binary, "replies"], input=table_input(view, layout, by_id, myself, pairs),
                             capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            if line.startswith("error"):
                raise SystemExit(f"{name}: our table was refused: {line}")
            reply, proto, data = line.split()
            ours[(reply, proto)] = bytes.fromhex(data)
        for reply in ("slots", "shards", "nodes"):
            for proto in (2, 3):
                theirs = raw_reply(port, proto, "CLUSTER", reply.upper())
                mine = ours[(reply, f"resp{proto}")]
                if reply == "nodes":
                    same = mask_nodes(theirs) == mask_nodes(mine)
                elif reply == "shards":
                    same = sorted_shards(theirs) == sorted_shards(mine)
                else:
                    same = theirs == mine
                status = "same" if same else "DIFFERENT"
                print(f"{name}: CLUSTER {reply.upper()} RESP{proto} asking {port}: {status} ({len(theirs)} bytes)")
                if not same:
                    failures += 1
                    print(f"  redis: {theirs[:600]!r}")
                    print(f"  ours:  {mine[:600]!r}")
                    if reply == "nodes":
                        print(f"  masked redis: {mask_nodes(theirs)!r}\n  masked ours:  {mask_nodes(mine)!r}")
    return failures


HELLO_ID = re.compile(rb"\$2\r\nid\r\n:\d+\r\n")
# HELLO's last field: Redis 8 lists its built-in modules (vectorset), we
# have none.
HELLO_MODULES = re.compile(rb"\$7\r\nmodules\r\n.*\Z", re.S)
# The INFO cluster lines only our node has (moving slots, redirects,
# leaving: phases 5 and 6).
OUR_INFO = re.compile(rb"(migrate_|cluster_redirects_|cluster_leaving)[a-z_]*:\d+\r\n")


def drop_our_info(raw: bytes) -> bytes:
    """An INFO reply (bulk or verbatim) without OUR_INFO's lines, framed
    again."""
    kind, _, rest = raw.partition(b"\r\n")
    n = int(kind[1:])
    body = OUR_INFO.sub(b"", rest[:n])
    return kind[:1] + str(len(body)).encode() + b"\r\n" + body + rest[n:]


def owner_port(layout: Dict[int, Ranges], slot: int) -> Optional[int]:
    return next((p for p, rs in layout.items() if any(s <= slot <= e for s, e in rs)), None)


def keys_by_owner(port: int, layout: Dict[int, Ranges], here: int) -> Dict[str, List[bytes]]:
    """A few keys per kind: in `here`'s slots, another node's, unassigned."""
    found: Dict[str, List[bytes]] = {"here": [], "other": [], "unassigned": []}
    with Conn("127.0.0.1", port, timeout=5) as c:
        for i in range(5000):
            key = f"k{i}".encode()
            owner = owner_port(layout, c.call("CLUSTER", "KEYSLOT", key).value)
            kind = "unassigned" if owner is None else "here" if owner == here else "other"
            if len(found[kind]) < 3:
                found[kind].append(key)
            if all(len(v) == 3 for v in found.values()):
                break
    return found


def node_probes(keys: Dict[str, List[bytes]], slot_of) -> List[Tuple]:
    probes: List[Tuple] = [("PING",), ("ECHO", "x"), ("DBSIZE",), ("SELECT", "0"), ("SELECT", "1"), ("SELECT", "x"),
                           ("CLUSTER",), ("CLUSTER", "FOO"), ("CLUSTER", "KEYSLOT"), ("CLUSTER", "KEYSLOT", "foo"),
                           ("CLUSTER", "KEYSLOT", "{user42}:name"), ("CLUSTER", "MYID"), ("CLUSTER", "MYID", "x"),
                           ("CLUSTER", "SLOTS"), ("CLUSTER", "SHARDS"), ("CLUSTER", "NODES"),
                           ("CLUSTER", "COUNTKEYSINSLOT", "16384"), ("CLUSTER", "COUNTKEYSINSLOT", "-1"),
                           ("CLUSTER", "COUNTKEYSINSLOT", "x"), ("READONLY",), ("READWRITE",), ("ASKING",),
                           ("READONLY", "x"), ("INFO", "cluster"), ("GET",), ("MSET", "a")]
    for kind, ks in keys.items():
        if not ks:
            continue
        k = ks[0]
        probes += [("SET", k, "v"), ("GET", k), ("INCR", k), ("TYPE", k), ("TTL", k), ("EXPIRE", k, "100"),
                   ("MGET", k, k), ("MGET", b"{" + k + b"}1", b"{" + k + b"}2"), ("DEL", k, k), ("EXISTS", k),
                   ("GET", k, "x"), ("MSET", k, "1", k, "2"), ("MSET", k, "1", k)]
        for kind2, ks2 in keys.items():
            if ks2 and ks2[-1] != k:
                probes += [("MGET", k, ks2[-1]), ("MSET", k, "1", ks2[-1], "2"), ("DEL", ks2[-1], k)]
    # Keys in this node's slots, counted by slot (also one of another's).
    for k in keys["here"] + keys["other"][:1]:
        probes += [("SET", k, "1"), ("SET", b"{" + k + b"}x", "1"), ("CLUSTER", "COUNTKEYSINSLOT", str(slot_of(k)))]
    return probes


def compare_node(name: str, dbnode: str, node_port: int, nodes: List[Node], layout: Dict[int, Ranges], here: int,
                 replicas: Optional[Dict[int, int]] = None) -> int:
    """Our node in Redis node `here`'s place: announcing its address and
    given its view as a table (our id in place of its id). The same
    requests to both must get the same replies, apart from the node ids.
    With `here` a replica, no request writes (writes would be replicated:
    the offsets would move off 0)."""
    view = wait_for(here, nodes, layout, replicas=replicas)
    # A Redis master that just joined reports `fail` (and refuses keys with
    # "CLUSTERDOWN The cluster is down") for a rejoin delay of up to 5 s
    # after the layout settled; a table has no such phase.
    want = "ok" if any(layout.values()) else "fail"
    deadline = time.time() + 30
    while f"cluster_state:{want}\r\n" not in call(here, "CLUSTER", "INFO").value.decode():
        if time.time() > deadline:
            raise SystemExit(f"{name}: {here} never reported cluster_state:{want}")
        time.sleep(0.2)
    proc = subprocess.Popen([dbnode, f"127.0.0.1:{node_port}", "--cluster", "--announce", f"127.0.0.1:{here}",
                             "--loglevel", "warning"])
    try:
        for _ in range(50):
            try:
                ours = call(node_port, "CLUSTER", "MYID").value
                break
            except OSError:
                time.sleep(0.1)
        theirs = call(here, "CLUSTER", "MYID").value
        info = call(here, "CLUSTER", "INFO").value.decode()
        epoch = re.search(r"cluster_current_epoch:(\d+)", info).group(1)
        by_id = {n.id: n for n in nodes}
        pairs = replica_order(here) if replicas else []
        table = table_input(view, layout, by_id, theirs.decode(), pairs).replace(theirs.decode(), ours.decode())
        args = ["CLUSTER", "SETTABLE", epoch, "NODES", str(sum(1 for l in table.splitlines() if l.startswith("node ")))]
        for line in table.splitlines():
            if line.startswith("node "):
                args += line.split()[1:]
        ranges = [line.split()[1:] for line in table.splitlines() if line.startswith("range ")]
        args += ["RANGES", str(len(ranges))] + [w for r in ranges for w in r]
        pairs = [line.split()[1:] for line in table.splitlines() if line.startswith("replica ")]
        if pairs:
            args += ["REPLICAS", str(len(pairs))] + [w for p in pairs for w in p]
        reply = call(node_port, *args)
        if reply.kind != "+":
            raise SystemExit(f"{name}: our node refused the table: {reply!r}")
        if not replicas:
            call(here, "FLUSHALL")
            call(node_port, "FLUSHALL")

        def mask(raw: bytes) -> bytes:
            raw = HELLO_ID.sub(b"$2\r\nid\r\n:ID\r\n", raw.replace(theirs, b"<MYID>").replace(ours, b"<MYID>"))
            raw = HELLO_MODULES.sub(b"$7\r\nmodules\r\n<masked>", raw)
            if raw[:1] in b"$=" and b" myself," in raw:
                kind, text = mask_nodes(raw)
                return kind + text.encode()
            if raw[:1] in b"$=" and b"# Cluster\r\ncluster_enabled:" in raw:
                return drop_our_info(raw)
            return raw

        failures = 0
        probes = node_probes(keys_by_owner(here, layout, here), lambda k: call(here, "CLUSTER", "KEYSLOT", k).value)
        # At a replica every key is redirected: no write runs. (What it
        # would serve after READONLY is left to milestone 3: the probes
        # send READWRITE right after it.)
        # CLUSTER INFO: the lines a table determines.
        probes.append(("CLUSTER", "INFO"))
        for proto in (2, 3):
            with Conn("127.0.0.1", here, timeout=5) as r, Conn("127.0.0.1", node_port, timeout=5) as o:
                if proto == 3:
                    r.call("HELLO", "3")
                    o.call("HELLO", "3")
                same = 0
                for p in probes + [("HELLO", str(proto))]:
                    r.send(cmd(*p))
                    o.send(cmd(*p))
                    rr, _ = r.read_reply()
                    oo, _ = o.read_reply()
                    if p == ("CLUSTER", "INFO"):
                        keep = lambda raw: b"\r\n".join(raw.split(b"\r\n")[1:9])  # state .. current_epoch
                        a_, b_ = keep(rr), keep(oo)
                    elif p == ("CLUSTER", "SHARDS"):
                        a_, b_ = sorted_shards(rr, mask), sorted_shards(oo, mask)
                    else:
                        a_, b_ = mask(rr), mask(oo)
                    if a_ == b_:
                        same += 1
                    else:
                        failures += 1
                        print(f"{name} RESP{proto}: {' '.join(map(str, p))}: DIFFERENT\n  redis: {rr[:300]!r}\n  ours:  {oo[:300]!r}")
                print(f"{name}: our node in {here}'s place, RESP{proto}: {same}/{len(probes) + 1} replies the same")
        return failures
    finally:
        proc.terminate()
        proc.wait()


def setup(ports: List[int], layout: Dict[int, Ranges], replicas: Optional[Dict[int, int]] = None) -> List[Node]:
    """Resets every node and builds `layout` (port -> ranges) from scratch,
    with `replicas` (port -> primary port) replicating, in that order."""
    for p in ports:
        # Only if there are keys: a primary with a backlog would propagate
        # it, moving its replication offset off 0.
        if call(p, "ROLE").value[0].value == b"master" and call(p, "DBSIZE").value:
            call(p, "FLUSHALL")
        call(p, "CLUSTER", "RESET", "HARD")
    for i, p in enumerate(ports):
        call(p, "CLUSTER", "SET-CONFIG-EPOCH", str(i + 1))
        for s, e in layout.get(p, []):
            call(p, "CLUSTER", "ADDSLOTSRANGE", str(s), str(e))
    for p in ports[1:]:
        call(ports[0], "CLUSTER", "MEET", "127.0.0.1", str(p))
    nodes = [Node(p) for p in ports]
    by_port = {n.port: n.id for n in nodes}
    for r, p in (replicas or {}).items():
        # Once the replica knows its primary.
        deadline = time.time() + 30
        while by_port[p] not in call(r, "CLUSTER", "NODES").value.decode():
            if time.time() > deadline:
                raise SystemExit(f"{r} never learned of {p}")
            time.sleep(0.1)
        reply = call(r, "CLUSTER", "REPLICATE", by_port[p])
        if reply.kind != "+":
            raise SystemExit(f"CLUSTER REPLICATE on {r}: {reply!r}")
    return nodes


def offsets_zero(ports: List[int]) -> bool:
    """Whether every node's replication offset is still 0 (nothing was
    replicated: the comparisons with replicas assume it)."""
    for p in ports:
        info = call(p, "INFO", "replication").value.decode()
        if re.search(r"^(master_repl_offset|slave_repl_offset):(?!0\r$)", info, re.M):
            return False
    return True


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="the cluster_compat example binary")
    ap.add_argument("--ports", required=True, help="4 ports: the 3-node cluster, then a spare node")
    ap.add_argument("--keys", type=int, default=20000)
    ap.add_argument("--dbnode", help="also run our node in cluster mode in a Redis node's place (target/release/speedvault-server)")
    ap.add_argument("--node-port", type=int, default=7811, help="where our node listens [7811]")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args(argv)
    ports = [int(p) for p in args.ports.split(",")]
    a, b, c, d = ports

    failures = check_keyslot(args.bin, a, args.keys, args.seed)
    created = [Node(p) for p in (a, b, c)]

    # 1. What `redis-cli --cluster create` made: three even ranges.
    even = {a: [(0, 5460)], b: [(5461, 10922)], c: [(10923, 16383)]}
    failures += compare_layout("create", args.bin, created, even, [a, b])
    if args.dbnode:
        failures += compare_node("create", args.dbnode, args.node_port, created, even, a)

    # 2. Interleaved single slots (runs that merge across them), several
    #    ranges per node, unassigned slots and a node without slots.
    uneven = {
        a: [(s, s) for s in range(0, 98, 2)] + [(200, 5000), (16383, 16383)],
        b: [(s, s) for s in range(1, 98, 2)] + [(99, 100), (5001, 10000)],
        c: [(98, 98), (10001, 16000)],
    }
    nodes = setup(ports, uneven)
    failures += compare_layout("uneven", args.bin, nodes, uneven, [a, d])
    if args.dbnode:
        failures += compare_node("uneven", args.dbnode, args.node_port, nodes, uneven, a)
        failures += compare_node("uneven", args.dbnode, args.node_port, nodes, uneven, d)

    # 3. No slots assigned at all.
    nodes = setup(ports, {})
    failures += compare_layout("unassigned", args.bin, nodes, {}, [a])
    if args.dbnode:
        failures += compare_node("unassigned", args.dbnode, args.node_port, nodes, {}, a)

    # 4. Replicas (phase 8): a and b with a replica each, asked at a
    #    primary and at the replicas, our node in a replica's place; then a
    #    with two, b with none. Nothing is written (see offsets_zero).
    pairs = {c: a, d: b}
    halves = {a: [(0, 8191)], b: [(8192, 16383)]}
    nodes = setup(ports, halves, pairs)
    failures += compare_layout("replicas", args.bin, nodes, halves, [a, c, d], pairs)
    if args.dbnode:
        failures += compare_node("replicas", args.dbnode, args.node_port, nodes, halves, c, pairs)
    two = {c: a, d: a}
    nodes = setup(ports, halves, two)
    failures += compare_layout("two replicas", args.bin, nodes, halves, [b, d], two)
    if args.dbnode:
        failures += compare_node("two replicas", args.dbnode, args.node_port, nodes, halves, d, two)
    if not offsets_zero(ports):
        print("replicas: a replication offset moved off 0 during the checks; they don't hold")
        failures += 1

    print("OK" if failures == 0 else f"FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
