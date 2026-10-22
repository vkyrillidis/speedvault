#!/usr/bin/env python3
"""Moving slots, measured (phase 5 milestone 6; run by migrate_bench.sh).

    python3 migrate_bench.py PART... --bin DIR [--base-port 7900] [--out FILE]

speed      how fast keys move with no client load: two nodes, keys of 16 B,
           1 KiB and 16 KiB values in 1024 slots of the first, STUART MOVE
           of the 1024 slots to the second, for --migrate-batch 10, 100,
           1000; keys/s and MB/s (values) from the MOVE to the last table.
impact     what a move costs the clients of its two nodes: 2M keys of 16 B
           moving (as above) with each --migrate-batch / --migrate-pause
           pair, while redis-benchmark (-c 50, unpipelined) SETs and GETs
           keys of a slot that stays at the source, and SETs keys of a slot
           of the target's, in 0.3 s runs for 4 s or until the move ends:
           requests/s and p99, against the same without a move, and the
           keys/s moved meanwhile.
rebalance  STUART REBALANCE of 4M keys of 16 B from three nodes onto a
           fourth (a quarter of them move) for --moves-per-source /
           --max-moves pairs, with redis-benchmark SETting keys of a slot
           the first node keeps: the time it takes, and requests/s and p99
           during it.

Each process is pinned: stuart to CPU 14, the nodes to 2, 4, 6 and 12,
redis-benchmark to 8 and 10. Prints Markdown tables (and writes them to
--out). Times are monotonic; a redis-benchmark run during which the wall
clock stepped (WSL2, see clockguard.py) is left out.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, key_slot, text, wait_for  # noqa: E402

HOST = "127.0.0.1"
NODE_CPUS = [2, 4, 6, 12]
STUART_CPU = 14
CLIENT_CPUS = [8, 10]

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


def cluster(args, n: int, migrate: List[str], stuart: Optional[List[str]] = None) -> Tuple[Cluster, List[int], List[str]]:
    cpus = {args.base_port: STUART_CPU}
    cpus.update({args.base_port + 1 + i: NODE_CPUS[i] for i in range(4)})
    c = Cluster(args.bin, args.base_port, migrate, stuart, cpus)
    ports = [args.base_port + 1 + i for i in range(n)]
    ids = [c.start_node(p) for p in ports]
    c.create(n)
    return c, ports, ids


def keys_sent(c: Cluster, ports: List[int]) -> int:
    return sum(c.info(p).get("migrate_keys_sent", 0) for p in ports)


def move_time(args, slots: int, n: int, size: int, batch: int) -> float:
    """Seconds from STUART MOVE of `slots` slots holding n keys to the
    last table."""
    c, ports, ids = cluster(args, 2, ["--migrate-batch", str(batch)])
    try:
        if n:
            fill(ports[0], list(range(slots)), n, size)
        t0 = time.monotonic()
        c.stuart().execute_command("STUART", "MOVE", 0, slots - 1, ids[1])
        c.wait_settled(600)
        t = time.monotonic() - t0
        moved = keys_sent(c, ports)
        assert moved == n, f"{moved} keys moved, not {n}"
    finally:
        c.stop(False)
    return t


def part_speed(args, out: List[str]):
    out += ["", "**Moving speed** (no clients): slots from one node to another, STUART MOVE to the last table", "",
            "| values | keys | slots | `--migrate-batch` | time | keys/s | MB/s |", "|---|---|---|---|---|---|---|"]
    for size, n, slots in [(0, 0, 1024), (16, 1_000_000, 8), (16, 1_000_000, 1024), (1024, 300_000, 8),
                           (1024, 300_000, 1024), (16384, 30_000, 8)]:
        for batch in ((100,) if n == 0 else (10, 100, 1000)):
            t = move_time(args, slots, n, size, batch)
            if n:
                row = f"| {size} B | {n:,} | {slots} | {batch} | {t:.2f} s | {n / t / 1000:.0f}k | {n * size / t / 1e6:.0f} |"
            else:
                row = f"| none | 0 | {slots} | {batch} | {t:.2f} s | | |"
            print(row, flush=True)
            out.append(row)


def part_impact(args, out: List[str]):
    n = 4_000_000
    out += ["", f"**Clients during a move**: {n // 1_000_000}M keys of 16 B moving out of 1024 slots; redis-benchmark `-c 50` "
            "on keys of a slot staying at the source (SET and GET in turn) and of one at the target (SET), 0.3 s runs for "
            "4 s or until the move ends; mean requests/s / worst p99 (ms)", "",
            "| `--migrate-batch` | `--migrate-pause` | moved keys/s | source SET | source GET | target SET |",
            "|---|---|---|---|---|---|"]
    stay, there = 2000, 9000  # slots at the source (0-8191) and the target
    baseline_done = False
    for batch, pause in args.impact:
        c, ports, ids = cluster(args, 2, ["--migrate-batch", str(batch), "--migrate-pause", str(pause)])
        try:
            fill(ports[0], list(range(1024)), n, 16)
            if not baseline_done:
                src, dst = parallel(lambda: bench_runs(ports[0], stay, ["SET", "GET"], CLIENT_CPUS[0], window(4)),
                                    lambda: bench_runs(ports[1], there, ["SET"], CLIENT_CPUS[1], window(4)))
                row = f"| no move | | | {summary(src['SET'])} | {summary(src['GET'])} | {summary(dst['SET'])} |"
                print(row, flush=True)
                out.append(row)
                baseline_done = True
            c.stuart().execute_command("STUART", "MOVE", 0, 1023, ids[1])
            wait_for("keys to move", lambda: keys_sent(c, ports) > 0, 10, 0.01)
            k0, t0 = keys_sent(c, ports), time.monotonic()
            end = window(4)
            until = lambda: end() or not c.migrations()  # noqa: E731
            src, dst = parallel(lambda: bench_runs(ports[0], stay, ["SET", "GET"], CLIENT_CPUS[0], until),
                                lambda: bench_runs(ports[1], there, ["SET"], CLIENT_CPUS[1], until))
            rate = (keys_sent(c, ports) - k0) / (time.monotonic() - t0)
            row = f"| {batch} | {pause} µs | {rate / 1000:.0f}k | {summary(src['SET'])} | {summary(src['GET'])} | {summary(dst['SET'])} |"
        finally:
            c.stop(False)
        print(row, flush=True)
        out.append(row)


def part_rebalance(args, out: List[str]):
    n = 4_000_000
    out += ["", f"**REBALANCE** of {n // 1_000_000}M keys of 16 B from three nodes onto a fourth (1M keys and 4096 slots "
            "move); redis-benchmark `-c 50` SET on a slot the first node keeps: mean requests/s / worst p99 (ms)", "",
            "| `--moves-per-source` | `--max-moves` | time | keys/s | clients before | clients during |", "|---|---|---|---|---|---|"]
    for per_source, total in args.rebalance:
        c, ports, ids = cluster(args, 3, [], ["--moves-per-source", str(per_source), "--max-moves", str(total)])
        try:
            owners = c.owners()
            for p in ports:
                fill(p, [s for s in range(16384) if owners[s] == p], n // 3, 16)
            before = bench_runs(ports[0], 0, ["SET"], CLIENT_CPUS[0], window(2))["SET"]
            c.start_node(args.base_port + 4)
            ports.append(args.base_port + 4)
            done = threading.Event()
            t0 = time.monotonic()
            c.stuart().execute_command("STUART", "REBALANCE")

            def settle():
                c.wait_settled(600)
                done.set()
                return time.monotonic() - t0
            t, during = parallel(settle, lambda: bench_runs(ports[0], 0, ["SET"], CLIENT_CPUS[0], done.is_set)["SET"])
            moved = keys_sent(c, ports)
            row = (f"| {per_source} | {total} | {t:.2f} s | {moved / t / 1000:.0f}k | {summary(before)} | {summary(during)} |")
        finally:
            c.stop(False)
        print(row, flush=True)
        out.append(row)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parts", nargs="+", choices=["speed", "impact", "rebalance", "all"])
    ap.add_argument("--bin", required=True)
    ap.add_argument("--base-port", type=int, default=7900)
    ap.add_argument("--out")
    ap.add_argument("--impact", default="10/0,100/0,1000/0,100/50,100/100,100/300,100/1000,1000/100,1000/300,1000/1000",
                    help="impact: --migrate-batch/--migrate-pause pairs")
    ap.add_argument("--rebalance", default="1/64,8/64,16/64,32/128,32/256,64/256",
                    help="rebalance: --moves-per-source/--max-moves pairs")
    args = ap.parse_args(argv)
    args.impact = [tuple(map(int, p.split("/"))) for p in args.impact.split(",")]
    args.rebalance = [tuple(map(int, p.split("/"))) for p in args.rebalance.split(",")]
    parts = ["speed", "impact", "rebalance"] if "all" in args.parts else args.parts
    out: List[str] = []
    for part in parts:
        {"speed": part_speed, "impact": part_impact, "rebalance": part_rebalance}[part](args, out)
    if args.out:
        with open(args.out, "w", newline="") as f:
            f.write("\n".join(out).lstrip("\n") + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
