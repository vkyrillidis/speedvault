#!/usr/bin/env python3
"""Stalls while the keyspace grows (run by stall_bench.sh).

    python3 stall_bench.py [--keys 10000000] [--batch 100] [--base-port 7960] [--out FILE] NAME=BINARY ...

Each server in turn, alone on CPU 2 (the client on CPU 4), gets --keys new
keys `key:%012d` of 16 B values from one connection, --batch SETs at a
time (pipelined), and each batch's round trip is timed (the monotonic
clock: WSL2's wall clock steps). A server whose keyspace grows in one go
(a hash table doubling, all of it rehashed at once) stops for as long as
that takes, and the batch that waited shows it; one that grows bit by bit
(an incremental rehash, or many small tables) doesn't.

BINARY is a dbnode (started standalone) or a redis-server-like binary
(`--save '' --appendonly no`). Prints a Markdown table: keys/s, the
batch round trips' p50, p99.9 and max, how many took over 10 ms, and the
three slowest with the number of keys when they came; and writes it to
--out.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
from typing import List, Tuple

HOST = "127.0.0.1"
VALUE = b"0123456789abcdef"


def start(binary: str, port: int, logdir: str) -> subprocess.Popen:
    if "dbnode" in os.path.basename(binary):
        args = [binary, f"{HOST}:{port}", "--loglevel", "warning"]
    else:
        args = [binary, "--port", str(port), "--save", "", "--appendonly", "no"]
    log = open(os.path.join(logdir, f"{port}.log"), "wb")
    proc = subprocess.Popen(["taskset", "-c", "2", *args], stdout=log, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            socket.create_connection((HOST, port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise SystemExit(f"{binary} didn't start on {port}")


def command(*args: bytes) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


def fill(port: int, keys: int, batch: int) -> Tuple[float, List[Tuple[float, int]]]:
    """Seconds in all, and each batch's (round trip in ms, keys before it)."""
    s = socket.create_connection((HOST, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    want = b"+OK\r\n" * batch
    trips = []
    start = time.monotonic()
    for first in range(0, keys, batch):
        n = min(batch, keys - first)
        req = b"".join(command(b"SET", b"key:%012d" % i, VALUE) for i in range(first, first + n))
        t = time.monotonic()
        s.sendall(req)
        got = b""
        while len(got) < 5 * n:
            chunk = s.recv(65536)
            if not chunk:
                raise SystemExit(f"port {port} closed the connection")
            got += chunk
        trips.append(((time.monotonic() - t) * 1000, first))
        if got != want[: 5 * n]:
            raise SystemExit(f"port {port}: unexpected reply {got[:80]!r}")
    total = time.monotonic() - start
    s.close()
    return total, trips


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keys", type=int, default=10_000_000)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--base-port", type=int, default=7960)
    ap.add_argument("--out")
    ap.add_argument("servers", nargs="+", metavar="NAME=BINARY")
    a = ap.parse_args()

    rows = []
    with tempfile.TemporaryDirectory() as logdir:
        for i, spec in enumerate(a.servers):
            name, binary = spec.split("=", 1)
            port = a.base_port + i
            print(f">> {name}: {a.keys:,} keys", file=sys.stderr)
            proc = start(binary, port, logdir)
            try:
                total, trips = fill(port, a.keys, a.batch)
            finally:
                proc.kill()
                proc.wait()
            ms = sorted(t for t, _ in trips)
            slowest = sorted(trips, reverse=True)[:3]
            rows.append(
                f"| {name} | {a.keys / total:,.0f} | {ms[len(ms) // 2]:.2f} | {ms[int(len(ms) * 0.999)]:.2f} | {ms[-1]:.1f} "
                f"| {sum(t > 10 for t in ms)} | " + ", ".join(f"{t:.0f} ms at {k / 1e6:.2f}M" for t, k in slowest) + " |"
            )
            print(rows[-1], file=sys.stderr)

    text = "\n".join([
        f"stalls while filling {a.keys:,} keys, {a.batch} SETs a batch, one connection:",
        "",
        "| server | keys/s | batch p50 (ms) | p99.9 (ms) | max (ms) | batches > 10 ms | slowest |",
        "|---|---:|---:|---:|---:|---:|---|",
        *rows,
    ])
    print(text)
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
