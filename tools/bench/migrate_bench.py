#!/usr/bin/env python3
"""Moving slots, measured (phase 5 milestone 6, by snapshot since phase 8
milestone 6; run by migrate_bench.sh).

    python3 migrate_bench.py PART... --bin DIR [--base-port 7900] [--out FILE]

Slots move by snapshot (phase 5's moves key by key are gone; the results
of 2026-10-25 compare the two).

speed      how fast keys move with no client load: two nodes, keys of 16 B,
           1 KiB and 16 KiB values in 8 or 1024 slots of the first, STUART
           MOVE of those slots to the second (for --migrate-batch 100 and
           1000); keys/s and MB/s (values) from the MOVE to the last
           table, the target's used_memory after (and per key moved).
impact     what a move costs the clients of its two nodes: 4M keys of 16 B
           moving out of 1024 slots with each --migrate-batch /
           --migrate-pause pair (the copy's batches and the pause after
           each while clients were served), while redis-benchmark (-c 50,
           unpipelined) SETs and GETs keys of a slot that stays at the source, and SETs
           keys of a slot of the target's, in 0.3 s runs for 4 s or until
           the move ends: requests/s and p99, against the same without a
           move, and the keys/s moved meanwhile.
rebalance  STUART REBALANCE of 4M keys of 16 B from three nodes onto a
           fourth (a quarter of them move) for --moves-per-source /
           --max-moves pairs, with redis-benchmark SETting keys of a slot
           the first node keeps: the time it takes, requests/s and p99
           during it, the new node's used_memory after.
drain      stuart --auto and four nodes holding 200k keys of 100 B (as
           migrate_check's sigterm), STUART LEAVE of the first, for
           --moves-per-source / --max-moves pairs: the time
           until it holds nothing (4096 slots, few keys: what a move costs
           besides its keys).

In every part a redis-py RedisCluster client SETs keys of random slots one
at a time while the slots move: its p99 and worst latency show what a
cutover's pause and the redirect after cost a client writing to a moving
slot. The nodes and stuart log verbosely, and the source's log gives each
cutover's pause (from CLUSTER PAUSESLOTS to the table that ends it): p50 /
max, and how many cutovers.

Each process is pinned: stuart to CPU 14, the nodes to 2, 4, 6 and 12,
redis-benchmark to 8 and 10, the RedisCluster client to 0. Prints Markdown
tables (and writes them to --out). Times are monotonic; a redis-benchmark
run during which the wall clock stepped (WSL2, see clockguard.py) is left
out.
"""

from __future__ import annotations

import argparse
import glob
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, key_slot, text, wait_even, wait_for  # noqa: E402
from redis.cluster import ClusterNode, RedisCluster  # noqa: E402

HOST = "127.0.0.1"
NODE_CPUS = [2, 4, 6, 12]
STUART_CPU = 14
CLIENT_CPUS = [8, 10]
WRITER_CPU = 0

_tags: Dict[int, str] = {}


def tag(slot: int) -> str:
    """A hash tag whose keys land in `slot`."""
    if not _tags:
        i = 0
        while len(_tags) < 16384:
            t = f"t{i}"
            _tags.setdefault(key_slot(t.encode()), t)
            i += 1
    return _tags[slot]


def fill(port: int, slots: List[int], n: int, size: int, prefix: str = "m"):
    """n keys with `size`-byte values, spread over `slots`, straight to the
    node at `port` (pipelined, 10k at a time)."""
    value = b"v" * size
    sock = socket.create_connection((HOST, port))
    tags = [tag(s) for s in slots]
    for start in range(0, n, 10000):
        end = min(n, start + 10000)
        parts = []
        for i in range(start, end):
            key = f"{{{tags[i % len(tags)]}}}{prefix}{i}".encode()
            parts.append(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len(key), key, size, value))
        sock.sendall(b"".join(parts))
        want, got = 5 * (end - start), 0
        while got < want:
            chunk = sock.recv(1 << 20)
            if not chunk:
                raise RuntimeError("the node closed the connection")
            got += len(chunk)
    sock.close()


def bench_runs(port: int, slot: int, cmds: List[str], cpu: int, until, size: int = 16) -> Dict[str, List[Tuple[float, float]]]:
    """redis-benchmark runs of 100k requests against `port`, keys of
    `slot`, the commands in turn, until `until()`: (requests/s, p99 ms)
    of each run, by command."""
    key = f"{{{tag(slot)}}}b:__rand_int__"
    rows: Dict[str, List[Tuple[float, float]]] = {cmd: [] for cmd in cmds}
    i = 0
    while not until():
        cmd = cmds[i % len(cmds)]
        i += 1
        args = ["SET", key, "v" * size] if cmd == "SET" else ["GET", key]
        offset = clock_offset()
        out = subprocess.run(["taskset", "-c", str(cpu), "redis-benchmark", "-h", HOST, "-p", str(port), "-n", "100000",
                              "-c", "50", "-r", "100000", "--csv", *args], capture_output=True, text=True).stdout
        if abs(clock_offset() - offset) > 0.005:
            continue  # the wall clock stepped (WSL2): redis-benchmark's times are wrong
        line = out.strip().splitlines()[-1].replace('"', "").split(",")
        rows[cmd].append((float(line[-7]), float(line[-2])))
    return rows


def clock_offset() -> float:
    """Wall minus monotonic clock, s (see clockguard.py)."""
    return time.time() - time.monotonic()


def window(seconds: float):
    end = time.monotonic() + seconds
    return lambda: time.monotonic() >= end


def summary(rows: List[Tuple[float, float]]) -> str:
    """Mean requests/s and the worst p99 of the runs."""
    if not rows:
        return "-"
    rps = sum(r for r, _ in rows) / len(rows)
    return f"{rps / 1000:.0f}k / {max(p for _, p in rows):.2f}"


def parallel(*jobs):
    """Runs the functions at once; returns their results."""
    results: List[Optional[object]] = [None] * len(jobs)

    def run(i, f):
        results[i] = f()
    threads = [threading.Thread(target=run, args=(i, f)) for i, f in enumerate(jobs)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def percentile(values: List[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * p))]


class Writer:
    """A RedisCluster client SETting keys of random slots, one at a time,
    from start() to stop(): each SET's latency, and the errors."""

    def __init__(self, port: int):
        self.port = port
        self.latencies: List[float] = []
        self.errors = 0
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run)

    def start(self):
        self.thread.start()

    def run(self):
        try:
            os.sched_setaffinity(0, {WRITER_CPU})
        except (AttributeError, OSError):
            pass
        rc = RedisCluster(startup_nodes=[ClusterNode(HOST, self.port)], socket_timeout=10)
        rnd = random.Random(1)
        i = 0
        while not self.done.is_set():
            key = f"{{{tag(rnd.randrange(16384))}}}w{i}"
            i += 1
            t0 = time.monotonic()
            try:
                rc.set(key, "v")
            except Exception:  # noqa: BLE001 - counted
                self.errors += 1
                continue
            self.latencies.append(time.monotonic() - t0)
        rc.close()

    def stop(self) -> str:
        """p99 / max ms, errors."""
        self.done.set()
        self.thread.join()
        if not self.latencies:
            return "-"
        p99, worst = percentile(self.latencies, 0.99) * 1000, max(self.latencies) * 1000
        errors = f", {self.errors} errors" if self.errors else ""
        return f"{p99:.2f} / {worst:.1f}{errors}"


def cluster(args, n: int, migrate: List[str], stuart: Optional[List[str]] = None,
            auto: bool = False) -> Tuple[Cluster, List[int], List[str]]:
    cpus = {args.base_port: STUART_CPU}
    cpus.update({args.base_port + 1 + i: NODE_CPUS[i] for i in range(4)})
    stuart = list(stuart or [])
    if auto:
        stuart.append("--auto")
    # The pauses come from the sources' logs, the tries from stuart's.
    migrate = [*migrate, "--loglevel", "verbose"]
    stuart.extend(["--loglevel", "verbose"])
    c = Cluster(args.bin, args.base_port, migrate, stuart, cpus)
    ports = [args.base_port + 1 + i for i in range(n)]
    ids = [c.start_node(p) for p in ports]
    if auto:
        wait_even(c, n)
    else:
        c.create(n)
    return c, ports, ids


def keys_sent(c: Cluster, ports: List[int]) -> int:
    return sum(c.info(p).get("migrate_keys_sent", 0) for p in ports)


def used_memory(c: Cluster, port: int) -> int:
    return int(c.node(port).info("memory")["used_memory"])


def mark(c: Cluster) -> Dict[str, int]:
    """Where each log ends now (the pauses after this count)."""
    return {f: os.path.getsize(f) for f in glob.glob(os.path.join(c.dir, "*.log"))}


def pauses(c: Cluster, since: Dict[str, int]) -> Tuple[List[float], Dict[str, int]]:
    """The cutover pauses (ms) the nodes logged since `since`, and counts:
    cutovers, pauses that expired or stuart ended (missed), links that
    started again, pauses stuart missed."""
    found: List[float] = []
    counts = {"table": 0, "missed": 0, "restarts": 0}
    for f in glob.glob(os.path.join(c.dir, "*.log")):
        with open(f, "rb") as log:
            log.seek(since.get(f, 0))
            for line in log.read().decode(errors="replace").splitlines():
                m = re.search(r"resumed after (\d+) us \((\w+)\)", line)
                if m:
                    if m.group(2) == "table":
                        found.append(int(m.group(1)) / 1000)
                        counts["table"] += 1
                    else:
                        counts["missed"] += 1
                elif "again from scratch" in line:
                    counts["restarts"] += 1
    return found, counts


def pause_cell(found: List[float], counts: Dict[str, int]) -> str:
    """p50 / max ms (cutovers, and what went wrong)."""
    if not found:
        return "-"
    extra = "".join(f", {counts[k]} {k}" for k in ("missed", "restarts") if counts[k])
    return f"{percentile(found, 0.5):.1f} / {max(found):.1f} ({len(found)}{extra})"


def move_time(args, slots: int, n: int, size: int, batch: int) -> Tuple[float, str, str, str]:
    """Seconds from STUART MOVE of `slots` slots holding n keys to the
    last table; the pauses, the writer, the target's memory."""
    c, ports, ids = cluster(args, 2, ["--migrate-batch", str(batch)])
    try:
        if n:
            fill(ports[0], list(range(slots)), n, size)
        before = used_memory(c, ports[1])
        logs = mark(c)
        writer = Writer(ports[0])
        writer.start()
        t0 = time.monotonic()
        c.stuart().execute_command("STUART", "MOVE", 0, slots - 1, ids[1])
        c.wait_settled(600)
        t = time.monotonic() - t0
        client = writer.stop()
        moved = keys_sent(c, ports)
        assert moved >= n, f"{moved} keys moved, not {n}"
        grown = used_memory(c, ports[1]) - before
        memory = f"{grown / 1e6:.0f} MB ({grown / n:.0f} B)" if n else ""
        cell = pause_cell(*pauses(c, logs))
    finally:
        c.stop(False)
    return t, cell, client, memory


def part_speed(args, out: List[str]):
    out += ["", "**Moving speed** (no load but one RedisCluster client): slots from one node to another, STUART MOVE to the "
            "last table; the client's SET p99 / max (ms); the target's used_memory grown (per key)", "",
            "| `--migrate-batch` | values | keys | slots | time | keys/s | MB/s | pause p50 / max ms (cutovers) | client p99 / max | target memory |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for size, n, slots in [(0, 0, 1024), (16, 1_000_000, 8), (16, 1_000_000, 1024), (1024, 300_000, 8),
                           (1024, 300_000, 1024), (16384, 30_000, 8)]:
        for batch in ((100,) if n == 0 else args.batches):
            t, cell, client, memory = move_time(args, slots, n, size, batch)
            if n:
                row = (f"| {batch} | {size} B | {n:,} | {slots} | {t:.2f} s | {n / t / 1000:.0f}k | {n * size / t / 1e6:.0f} | "
                       f"{cell} | {client} | {memory} |")
            else:
                row = f"| {batch} | none | 0 | {slots} | {t:.2f} s | | | {cell} | {client} | |"
            print(row, flush=True)
            out.append(row)


def part_impact(args, out: List[str]):
    n = 4_000_000
    out += ["", f"**Clients during a move**: {n // 1_000_000}M keys of 16 B moving out of 1024 slots; redis-benchmark `-c 50` "
            "on keys of a slot staying at the source (SET and GET in turn) and of one at the target (SET), 0.3 s runs for "
            "4 s or until the move ends: mean requests/s / worst p99 (ms); the RedisCluster client's SET p99 / max (ms) "
            "over the whole move", "",
            "| `--migrate-batch` | `--migrate-pause` | moved keys/s | source SET | source GET | target SET | pause p50 / max ms (cutovers) | client p99 / max |",
            "|---|---|---|---|---|---|---|---|"]
    stay, there = 2000, 9000  # slots at the source (0-8191) and the target
    baseline_done = False
    for batch, pause in args.impact:
        c, ports, ids = cluster(args, 2, ["--migrate-batch", str(batch), "--migrate-pause", str(pause)])
        try:
            fill(ports[0], list(range(1024)), n, 16)
            if not baseline_done:
                src, dst = parallel(lambda: bench_runs(ports[0], stay, ["SET", "GET"], CLIENT_CPUS[0], window(4)),
                                    lambda: bench_runs(ports[1], there, ["SET"], CLIENT_CPUS[1], window(4)))
                row = f"| no move | | | {summary(src['SET'])} | {summary(src['GET'])} | {summary(dst['SET'])} | | |"
                print(row, flush=True)
                out.append(row)
                baseline_done = True
            logs = mark(c)
            writer = Writer(ports[0])
            writer.start()
            k0, t0 = keys_sent(c, ports), time.monotonic()
            c.stuart().execute_command("STUART", "MOVE", 0, 1023, ids[1])
            end = window(4)
            until = lambda: end() or not c.migrations()  # noqa: E731
            src, dst = parallel(lambda: bench_runs(ports[0], stay, ["SET", "GET"], CLIENT_CPUS[0], until),
                                lambda: bench_runs(ports[1], there, ["SET"], CLIENT_CPUS[1], until))
            # Its keys count at the cutovers: the rate over the whole move.
            c.wait_settled(600)
            rate = (keys_sent(c, ports) - k0) / (time.monotonic() - t0)
            client = writer.stop()
            row = (f"| {batch} | {pause} µs | {rate / 1000:.0f}k | {summary(src['SET'])} | {summary(src['GET'])} | {summary(dst['SET'])} | "
                   f"{pause_cell(*pauses(c, logs))} | {client} |")
        finally:
            c.stop(False)
        print(row, flush=True)
        out.append(row)


def part_rebalance(args, out: List[str]):
    n = 4_000_000
    out += ["", f"**REBALANCE** of {n // 1_000_000}M keys of 16 B from three nodes onto a fourth (1M keys and 4096 slots "
            "move); redis-benchmark `-c 50` SET on a slot the first node keeps: mean requests/s / worst p99 (ms); the "
            "RedisCluster client's SET p99 / max (ms); the new node's used_memory after", "",
            "| `--moves-per-source` | `--max-moves` | time | keys/s | clients before | clients during | "
            "pause p50 / max ms (cutovers) | client p99 / max | target memory |", "|---|---|---|---|---|---|---|---|---|"]
    for per_source, total in args.rebalance:
        c, ports, ids = cluster(args, 3, [], ["--moves-per-source", str(per_source), "--max-moves", str(total)])
        try:
            owners = c.owners()
            for p in ports:
                fill(p, [s for s in range(16384) if owners[s] == p], n // 3, 16)
            before = bench_runs(ports[0], 0, ["SET"], CLIENT_CPUS[0], window(2))["SET"]
            c.start_node(args.base_port + 4)
            ports.append(args.base_port + 4)
            logs = mark(c)
            writer = Writer(ports[0])
            writer.start()
            done = threading.Event()
            t0 = time.monotonic()
            c.stuart().execute_command("STUART", "REBALANCE")

            def settle():
                c.wait_settled(600)
                done.set()
                return time.monotonic() - t0
            t, during = parallel(settle, lambda: bench_runs(ports[0], 0, ["SET"], CLIENT_CPUS[0], done.is_set)["SET"])
            client = writer.stop()
            moved = keys_sent(c, ports)
            memory = used_memory(c, ports[-1])
            row = (f"| {per_source} | {total} | {t:.2f} s | {moved / t / 1000:.0f}k | {summary(before)} | "
                   f"{summary(during)} | {pause_cell(*pauses(c, logs))} | {client} | {memory / 1e6:.0f} MB |")
        finally:
            c.stop(False)
        print(row, flush=True)
        out.append(row)


def part_drain(args, out: List[str]):
    n = 200_000
    out += ["", f"**Drain** (as migrate_check's sigterm): stuart `--auto`, four nodes, {n // 1000}k keys of 100 B; STUART LEAVE "
            "of the first, until it holds nothing (4096 slots, 50k keys move); the RedisCluster client's SET p99 / max (ms)", "",
            "| `--moves-per-source` | `--max-moves` | time | slots/s | pause p50 / max ms (cutovers) | client p99 / max |",
            "|---|---|---|---|---|---|"]
    for per_source, total in args.drain:
        for _ in range(args.drain_runs):
            c, ports, ids = cluster(args, 4, [], ["--moves-per-source", str(per_source), "--max-moves", str(total)], auto=True)
            try:
                c.fill(n)
                logs = mark(c)
                writer = Writer(ports[1])
                writer.start()
                t0 = time.monotonic()
                c.stuart().execute_command("STUART", "LEAVE", ids[0])
                wait_for("the first node to leave", lambda: c.stuart().execute_command("STUART", "LEAVE", ids[0]) == 0, 120, 0.005)
                t = time.monotonic() - t0
                client = writer.stop()
                row = f"| {per_source} | {total} | {t:.2f} s | {4096 / t:.0f} | {pause_cell(*pauses(c, logs))} | {client} |"
            finally:
                c.stop(False)
            print(row, flush=True)
            out.append(row)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parts", nargs="+", choices=["speed", "impact", "rebalance", "drain", "all"])
    ap.add_argument("--bin", required=True)
    ap.add_argument("--base-port", type=int, default=7900)
    ap.add_argument("--out")
    ap.add_argument("--batches", default="100,1000", help="speed: --migrate-batch values")
    ap.add_argument("--impact", default="100/50,100/100,1000/100,1000/1000",
                    help="impact: --migrate-batch/--migrate-pause pairs")
    ap.add_argument("--rebalance", default="32/128,128/512",
                    help="rebalance: --moves-per-source/--max-moves pairs")
    ap.add_argument("--drain", default="32/128,128/512", help="drain: --moves-per-source/--max-moves pairs")
    ap.add_argument("--drain-runs", type=int, default=3)
    args = ap.parse_args(argv)
    args.batches = [int(b) for b in args.batches.split(",")]
    args.impact = [tuple(map(int, p.split("/"))) for p in args.impact.split(",")]
    args.rebalance = [tuple(map(int, p.split("/"))) for p in args.rebalance.split(",")]
    args.drain = [tuple(map(int, p.split("/"))) for p in args.drain.split(",")]
    parts = ["speed", "impact", "rebalance", "drain"] if "all" in args.parts else args.parts
    out: List[str] = []
    for part in parts:
        {"speed": part_speed, "impact": part_impact, "rebalance": part_rebalance, "drain": part_drain}[part](args, out)
    if args.out:
        with open(args.out, "w", newline="") as f:
            f.write("\n".join(out).lstrip("\n") + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
