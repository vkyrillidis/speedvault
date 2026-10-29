#!/usr/bin/env python3
"""Memory of a primary and its replica (phase 8 milestone 10; run by
memory_bench.sh).

    python3 memory_bench.py --bin DIR [--base-port 7940] [--also NAME=SERVER ...] [--out FILE]

Datasets, each loaded into a new primary with a replica attached (pipelined
straight to the primary, which owns every slot):
- strings: 1M keys `key:%012d` of 16 B values (as redis-benchmark's);
- collections: 10,000 each of hashes (100 fields, field and value 8 B),
  sets (100 members of 9 B) and lists (100 elements of 8 B), small enough
  for Redis's listpacks; and 10 each of hashes and sets of 100,000, past
  them. 5M elements in all.
Servers: ours (stuart --auto --replicas 1), Redis 7.0.15 and each --also
NAME=SERVER, a redis-server-like binary (Redis 8.10.2, Valkey 9.1.2),
standalone (REPLICAOF) and in cluster mode as ours runs (the primary owns
every slot, the replica follows by CLUSTER REPLICATE); their defaults but
diskless sync without its delay.

Once the replica has the primary's offset and keys: `INFO memory`'s
used_memory (what the allocator handed out, as each server counts it; ours
counts glibc's usable size of every block) and the process's RSS
(/proc/PID/status VmRSS), primary and replica, and used_memory's growth
over the empty node divided by the keys (strings) or the elements
(collections). Prints Markdown tables (and writes them to --out).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Callable, Dict, Iterator, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, wait_for  # noqa: E402
from resp import Conn  # noqa: E402

HOST = "127.0.0.1"
REDIS_SERVER = os.environ.get("REDIS_SERVER", "redis-server")


def call(port: int, *args):
    with Conn(HOST, port, timeout=60) as c:
        return c.call(*args).value


def field(port: int, section: str, name: str) -> str:
    text = call(port, "INFO", section).decode()
    m = re.search(rf"^{name}:(.*?)\r?$", text, re.M)
    return m.group(1) if m else ""


def rss(pid: int) -> int:
    with open(f"/proc/{pid}/status") as f:
        return next(int(l.split()[1]) for l in f if l.startswith("VmRSS:")) * 1024


def resp(*args: bytes) -> bytes:
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


def strings() -> Iterator[bytes]:
    for i in range(1000000):
        yield resp(b"SET", b"key:%012d" % i, b"%016d" % i)


def collections() -> Iterator[bytes]:
    for k in range(10000):
        items = [b"%08d" % i for i in range(100)]
        yield resp(b"HSET", b"h:%05d" % k, *[x for i in items for x in (i, i)])
        yield resp(b"SADD", b"s:%05d" % k, *[b"m" + i for i in items])
        yield resp(b"RPUSH", b"l:%05d" % k, *items)
    for k in range(10):
        for start in range(0, 100000, 1000):
            items = [b"%08d" % i for i in range(start, start + 1000)]
            yield resp(b"HSET", b"bh:%02d" % k, *[x for i in items for x in (i, i)])
            yield resp(b"SADD", b"bs:%02d" % k, *[b"m" + i for i in items])


# (name, commands, how many keys, how many units for the per-unit figure, unit)
DATASETS: List[Tuple[str, Callable[[], Iterator[bytes]], int, int, str]] = [
    ("strings", strings, 1000000, 1000000, "key"),
    ("collections", collections, 30020, 5000000, "element"),
]


def load(port: int, commands: Iterator[bytes]):
    """Sends the commands pipelined, 1,000 at a time, and reads the
    replies: no error allowed."""
    with socket.create_connection((HOST, port)) as s:
        f = s.makefile("rb")
        batch: List[bytes] = []

        def flush():
            s.sendall(b"".join(batch))
            for _ in batch:
                line = f.readline()
                if not line or line.startswith(b"-"):
                    raise SystemExit(f"{port}: {line!r} while loading")
            batch.clear()
        for c in commands:
            batch.append(c)
            if len(batch) == 1000:
                flush()
        if batch:
            flush()


def synced(primary: int, replica: int, keys: int) -> bool:
    return (field(replica, "replication", "master_link_status") == "up"
            and field(primary, "replication", "master_repl_offset") == field(replica, "replication", "slave_repl_offset")
            and call(replica, "DBSIZE") == keys)


def measure(primary: int, replica: int, pids: Tuple[int, int], data) -> Dict[str, int]:
    _, commands, keys, _, _ = data
    empty = int(field(primary, "memory", "used_memory"))
    load(primary, commands())
    if call(primary, "DBSIZE") != keys:
        raise SystemExit(f"{primary}: {call(primary, 'DBSIZE')} keys, not {keys}")
    wait_for("the replica to catch up", lambda: synced(primary, replica, keys), timeout=300, every=0.1)
    time.sleep(1)  # Redis updates its RSS figure in its cron
    return {"empty": empty, "used": int(field(primary, "memory", "used_memory")),
            "replica_used": int(field(replica, "memory", "used_memory")), "rss": rss(pids[0]), "replica_rss": rss(pids[1])}


def ours(args, data) -> Dict[str, int]:
    base = args.base_port
    primary, replica = base + 1, base + 2
    cluster = Cluster(args.bin, base, [], ["--auto", "--replicas", "1"])
    try:
        cluster.start_node(primary)
        wait_for("the primary to own every slot",
                 lambda: b"\ncluster_slots_assigned:16384\r" in call(primary, "CLUSTER", "INFO"), timeout=30)
        cluster.start_node(replica)
        wait_for("the replica's link up", lambda: field(replica, "replication", "master_link_status") == "up", timeout=30)
        return measure(primary, replica, (cluster.nodes[primary].pid, cluster.nodes[replica].pid), data)
    finally:
        cluster.stop(keep=False)


def redis(args, server: str, cluster: bool, data) -> Dict[str, int]:
    primary, replica = args.base_port + 11, args.base_port + 12
    tmp = tempfile.mkdtemp(prefix="memory_bench.")
    procs: List[subprocess.Popen] = []

    def start(port: int) -> subprocess.Popen:
        d = os.path.join(tmp, str(port))
        os.mkdir(d)
        p = subprocess.Popen([server, "--port", str(port), "--dir", d, "--save", "", "--appendonly", "no",
                              "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0", "--loglevel", "warning"]
                             + (["--cluster-enabled", "yes", "--cluster-config-file", os.path.join(d, "nodes.conf")]
                                if cluster else []),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)
        wait_for(f"{server} {port}", lambda: call(port, "PING") == b"PONG")
        return p

    try:
        p, r = start(primary), start(replica)
        if cluster:
            call(primary, "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
            wait_for("the cluster to be ok", lambda: b"cluster_state:ok" in call(primary, "CLUSTER", "INFO"), timeout=30)
            me = call(primary, "CLUSTER", "MYID").decode()
            call(replica, "CLUSTER", "MEET", HOST, str(primary))
            wait_for("the replica to know the primary", lambda: me.encode() in call(replica, "CLUSTER", "NODES"), timeout=30)
            call(replica, "CLUSTER", "REPLICATE", me)
        else:
            call(replica, "REPLICAOF", HOST, str(primary))
        wait_for("the replica's link up", lambda: field(replica, "replication", "master_link_status") == "up", timeout=30)
        return measure(primary, replica, (p.pid, r.pid), data)
    finally:
        for p in procs:
            p.kill()
            p.wait()
        shutil.rmtree(tmp, ignore_errors=True)


def version(server: str) -> str:
    out = subprocess.run([server, "--version"], capture_output=True, text=True, check=True).stdout
    m = re.search(r"v=(\S+)", out)
    return m.group(1) if m else out.strip()


def mb(b: int) -> str:
    return f"{b / 1e6:,.0f}"


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="the directory with release dbnode and stuart")
    ap.add_argument("--base-port", type=int, default=7940)
    ap.add_argument("--also", action="append", default=[], metavar="NAME=SERVER",
                    help="also a redis-server-like binary")
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    servers = [(f"Redis {version(REDIS_SERVER)}", REDIS_SERVER)]
    for a in args.also:
        name, sep, path = a.partition("=")
        if not sep:
            ap.error(f"--also takes NAME=SERVER, not {a}")
        servers.append((name, path))
    setups: List[Tuple[str, Callable]] = [("ours", lambda d: ours(args, d))]
    for name, server in servers:
        setups += [(name, lambda d, s=server: redis(args, s, False, d)),
                   (f"{name} cluster", lambda d, s=server: redis(args, s, True, d))]
    lines: List[str] = []
    for data in DATASETS:
        name, _, keys, units, unit = data
        what = f"{keys:,} keys" + (f", {units:,} {unit}s" if unit != "key" else "")
        head = ["", f"{name}: {what}", "",
                f"| server | used_memory (MB) | B per {unit} | RSS (MB) | replica used_memory (MB) | replica RSS (MB) | "
                f"used_memory / ours |", "|---|---|---|---|---|---|---|"]
        print("\n".join(head), flush=True)
        lines += head
        ours_used: Optional[int] = None
        for label, run in setups:
            r = run(data)
            ours_used = ours_used or r["used"]
            line = (f"| {label} | {mb(r['used'])} | {(r['used'] - r['empty']) / units:.0f} | {mb(r['rss'])} | "
                    f"{mb(r['replica_used'])} | {mb(r['replica_rss'])} | {r['used'] / ours_used:.2f}x |")
            lines.append(line)
            print(line, flush=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            f.write("\n".join(lines).lstrip("\n") + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
