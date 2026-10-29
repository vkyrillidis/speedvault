#!/usr/bin/env python3
"""What a replica costs its primary (phase 8 milestones 4 and 10; run by
ack_bench.sh).

    python3 ack_bench.py --bin DIR [--base-bin DIR] [--base-port 7990] [--runs 5]
                         [--also NAME=SERVER ...] [--out FILE]

redis-benchmark SET, GET, INCR and HSET on random keys (-r 1000000, 16 B
values, -c 50; in that order, so GET finds keys) at a primary, unpipelined
and with -P 16, in these set-ups:
- ours alone: stuart --auto and one node (no stream);
- ours with a replica: stuart --auto --replicas 1, the second node its
  replica, acknowledging what it applied after every read and at least
  every second, which the primary reads on a fiber of its own (--bin);
- the same with --base-bin, a build from before the acknowledgements, if
  given: the difference is what they cost;
- Redis 7.0.15 alone, and with a replica (REPLICAOF; it acknowledges once
  a second), with their defaults but diskless sync without its delay;
- the same for each --also NAME=SERVER, a redis-server-like binary (Redis
  8.10.2, Valkey 9.1.2);
- each of those in cluster mode too, as ours runs: the primary owns every
  slot, the replica follows it by CLUSTER REPLICATE.
The primary is pinned to CPU 2, the replica to 4, stuart to 14,
redis-benchmark to 8 and 10. Each figure is the median of --runs runs; a
run during which the wall clock stepped (WSL2, see clockguard.py) is run
again. With a replica, the replica's offset is checked against the
primary's after each set-up, and ours must have no write refused (the
lease). Prints Markdown tables (and writes them to --out).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Callable, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, wait_for  # noqa: E402
from resp import Conn  # noqa: E402

HOST = "127.0.0.1"
PRIMARY_CPU, REPLICA_CPU, STUART_CPU = 2, 4, 14
CLIENT_CPUS = "8,10"
REDIS_SERVER = os.environ.get("REDIS_SERVER", "redis-server")
COMMANDS = ["SET", "GET", "INCR", "HSET"]
# One redis-benchmark run per pipeline depth: (-P, -n per command).
DEPTHS = [(1, 1000000), (16, 4000000)]
TESTS = [f"{c}, -P {p}" for p, _ in DEPTHS for c in COMMANDS]


def call(port: int, *args):
    with Conn(HOST, port, timeout=10) as c:
        return c.call(*args).value


def field(port: int, name: str) -> str:
    text = call(port, "INFO", "everything").decode()
    m = re.search(rf"^{name}:(.*?)\r?$", text, re.M)
    return m.group(1) if m else ""


def version(server: str) -> str:
    out = subprocess.run([server, "--version"], capture_output=True, text=True, check=True).stdout
    m = re.search(r"v=(\S+)", out)
    return m.group(1) if m else out.strip()


def offset_ms() -> float:
    return (time.time() - time.monotonic()) * 1000.0


# Requests sent, per port: a run again after a clock step sends them again.
SENT: Dict[int, int] = {}


def bench(port: int, depth: int, n: int) -> Dict[str, float]:
    """requests/s of each command in one redis-benchmark run, again while
    the clock steps."""
    for _ in range(5):
        SENT[port] = SENT.get(port, 0) + n * len(COMMANDS)
        before = offset_ms()
        out = subprocess.run(["taskset", "-c", CLIENT_CPUS, "redis-benchmark", "-h", HOST, "-p", str(port),
                              "-t", ",".join(c.lower() for c in COMMANDS), "-r", "1000000", "-d", "16", "-c", "50",
                              "-P", str(depth), "-n", str(n), "--csv"],
                             capture_output=True, text=True, check=True).stdout
        if abs(offset_ms() - before) > 5.0:
            continue
        got = {}
        for line in out.splitlines():
            cells = [c.strip('"') for c in line.split(",")]
            name = cells[0].split(" ")[0]
            if name in COMMANDS:
                got[f"{name}, -P {depth}"] = float(cells[1])
        if len(got) != len(COMMANDS):
            raise SystemExit(f"redis-benchmark gave {sorted(got)}:\n{out}")
        return got
    raise SystemExit("the clock kept stepping")


def runs(port: int, n: int) -> Dict[str, float]:
    samples: Dict[str, List[float]] = {t: [] for t in TESTS}
    for depth, count in DEPTHS:
        for _ in range(n):
            for name, value in bench(port, depth, count).items():
                samples[name].append(value)
    return {name: statistics.median(v) for name, v in samples.items()}


def synced(primary: int, replica: int, master: str, slave: str) -> Callable[[], bool]:
    return lambda: field(primary, master) == field(replica, slave) != ""


def ours(bin_dir: str, base: int, n: int, replica: bool) -> Dict[str, float]:
    primary, other = base + 1, base + 2
    stuart = ["--auto"] + (["--replicas", "1"] if replica else [])
    cluster = Cluster(bin_dir, base, [], stuart, cpus={base: STUART_CPU, primary: PRIMARY_CPU, other: REPLICA_CPU})
    try:
        cluster.start_node(primary)
        wait_for("the primary to own every slot", lambda: b"\ncluster_slots_assigned:16384\r" in call(primary, "CLUSTER", "INFO"), timeout=30)
        if replica:
            cluster.start_node(other)
            wait_for("the replica to sync", lambda: field(other, "master_link_status") == "up" and field(other, "master_sync_in_progress") == "0", timeout=30)
        SENT.pop(primary, None)
        result = runs(primary, n)
        if replica:
            wait_for("the replica to catch up", synced(primary, other, "master_repl_offset", "slave_repl_offset"), timeout=30)
            # Every write was taken, the lease never lapsed: a refused one
            # isn't counted as processed (redis-benchmark's CONFIG GETs
            # and this script's INFOs are, a few).
            done = int(field(primary, "total_commands_processed"))
            want = SENT.pop(primary)
            if not want <= done < want + 1000:
                raise SystemExit(f"{bin_dir}: {done} commands processed for {want} sent")
        return result
    finally:
        cluster.stop(keep=False)


def redis(server: str, base: int, n: int, replica: bool, cluster: bool) -> Dict[str, float]:
    primary, other = base + 11, base + 12
    tmp = tempfile.mkdtemp(prefix="ack_bench.")
    procs: List[subprocess.Popen] = []

    def start(port: int, cpu: int):
        d = os.path.join(tmp, str(port))
        os.mkdir(d)
        procs.append(subprocess.Popen(["taskset", "-c", str(cpu), server, "--port", str(port), "--dir", d, "--save", "",
                                       "--appendonly", "no", "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0",
                                       "--loglevel", "warning"]
                                      + (["--cluster-enabled", "yes", "--cluster-config-file", os.path.join(d, "nodes.conf")]
                                         if cluster else []),
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_for(f"{server} {port}", lambda: call(port, "PING") == b"PONG")

    try:
        start(primary, PRIMARY_CPU)
        if cluster:
            call(primary, "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
            wait_for("the cluster to be ok", lambda: b"cluster_state:ok" in call(primary, "CLUSTER", "INFO"), timeout=30)
        if replica:
            start(other, REPLICA_CPU)
            if cluster:
                me = call(primary, "CLUSTER", "MYID").decode()
                call(other, "CLUSTER", "MEET", HOST, str(primary))
                wait_for("the replica to know the primary", lambda: me.encode() in call(other, "CLUSTER", "NODES"), timeout=30)
                call(other, "CLUSTER", "REPLICATE", me)
            else:
                call(other, "REPLICAOF", HOST, str(primary))
            wait_for("the replica to sync", lambda: field(other, "master_link_status") == "up", timeout=30)
        result = runs(primary, n)
        if replica:
            wait_for(f"the {server} replica to catch up", synced(primary, other, "master_repl_offset", "slave_repl_offset"), timeout=30)
        return result
    finally:
        for p in procs:
            p.kill()
            p.wait()
        shutil.rmtree(tmp, ignore_errors=True)


def table(title: str, names: List[str], rows: List[tuple]) -> List[str]:
    lines = [f"| {title} | " + " | ".join(n.split(",")[0] for n in names) + " |", "|---" * (1 + len(names)) + "|"]
    lines += [f"| {label} | " + " | ".join(cells) + " |" for label, cells in rows]
    return lines + [""]


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="the directory with release dbnode and stuart")
    ap.add_argument("--base-bin", help="the same, built from before the acknowledgements")
    ap.add_argument("--base-port", type=int, default=7990)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--also", action="append", default=[], metavar="NAME=SERVER",
                    help="also a redis-server-like binary, alone and with a replica")
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    servers = [(f"Redis {version(REDIS_SERVER)}", REDIS_SERVER)]
    for a in args.also:
        name, sep, path = a.partition("=")
        if not sep:
            ap.error(f"--also takes NAME=SERVER, not {a}")
        servers.append((name, path))
    rows: List[tuple] = [("ours alone", ours(args.bin, args.base_port, args.runs, False)),
                         ("ours with a replica", ours(args.bin, args.base_port, args.runs, True))]
    if args.base_bin:
        rows.append(("ours with a replica, before acknowledgements", ours(args.base_bin, args.base_port, args.runs, True)))
    servers = [(name + kind, server, cluster) for name, server in servers
               for kind, cluster in [("", False), (" cluster", True)]]
    for name, server, cluster in servers:
        rows += [(f"{name} alone", redis(server, args.base_port, args.runs, False, cluster)),
                 (f"{name} with a replica", redis(server, args.base_port, args.runs, True, cluster))]
    by = dict(rows)
    lines = [f"Requests/s, median of {args.runs} runs; -c 50, -r 1000000, 16 B values.", ""]
    for depth, _ in DEPTHS:
        names = [t for t in TESTS if t.endswith(f"-P {depth}")]
        lines += table(f"-P {depth} (req/s)", names, [(label, [f"{r[n]:,.0f}" for n in names]) for label, r in rows])
        lines += table(f"-P {depth}: with a replica / alone", names,
                       [(s, [f"{by[s + ' with a replica'][n] / by[s + ' alone'][n]:.2f}x" for n in names])
                        for s in ["ours"] + [name for name, _, _ in servers]])
        lines += table(f"-P {depth}: ours / them", names,
                       [(f"{name} {how}", [f"{by['ours ' + how][n] / by[name + ' ' + how][n]:.2f}x" for n in names])
                        for name, _, _ in servers for how in ["alone", "with a replica"]])
    if args.base_bin:
        with_acks, before = by["ours with a replica"], by["ours with a replica, before acknowledgements"]
        lines.append("With acknowledgements against before: " + ", ".join(f"{n} {with_acks[n] / before[n]:.3f}x" for n in TESTS))
    text = "\n".join(lines).rstrip("\n") + "\n"
    print(text)
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
