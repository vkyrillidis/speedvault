#!/usr/bin/env python3
"""A replica's sync, measured (phase 8 milestones 3 and 10; run by
sync_bench.sh).

    python3 sync_bench.py --bin DIR [--keys 1000000,10000000] [--cut-keys 1000000]
                          [--base-port 7950] [--also NAME=SERVER ...] [--out FILE]

For each key count: a primary loaded with that many keys (`key:%012d`,
16 B values, as redis-benchmark's), then a replica attached: ours (stuart
--auto --replicas 1: the second node to join is the first's replica),
Redis 7.0.15's and each --also NAME=SERVER's, a redis-server-like binary
(Redis 8.10.2, Valkey 9.1.2) (REPLICAOF; their defaults but diskless sync
without the 5 s wait for more replicas). Once with no clients, once with
redis-benchmark SETting random keys of those (-c 50, unpipelined) at the
primary meanwhile.

Measured: the time from the replica's start to its link up (`INFO
replication`: master_link_status up, no sync in progress), and every
50 ms the memory of the primary, with Redis's forked child, and of the
replica, as Pss (/proc/PID/smaps_rollup: a page the child shares with
its parent counts once, split between them, so a fork shows only what
copy-on-write copied). Then both stopped writing, the replica's DBSIZE
and offset are checked against the primary's.

Then a cut, with --cut-keys keys: the replica reaches its primary through
a proxy here (ours: the primary announces the proxy's port; theirs:
REPLICAOF the proxy), which, a second after the replica's link is up,
closes every connection and turns new ones away for 1 s. Measured: the
time from the proxy's reopening to the replica's link up again, and
whether that was a partial resync (`INFO stats` sync_partial_ok) or a
full one (sync_full): with no clients, and with the SETs going on (more
than the 1 MiB backlog, ours and Redis's default, piles up in a second).
The proxy carries ours' stuart connection too (stuart waits 5 s before
calling a primary failed; the primary's write lease lasts 3 s).

The primary is pinned to CPU 2, the replica to 4, stuart to 14,
redis-benchmark to 8 and 10. Prints a Markdown table (and writes it to
--out).
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
import threading
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, wait_for  # noqa: E402
from resp import Conn  # noqa: E402

HOST = "127.0.0.1"
PRIMARY_CPU, REPLICA_CPU, STUART_CPU = 2, 4, 14
CLIENT_CPUS = "8,10"
REDIS_SERVER = os.environ.get("REDIS_SERVER", "redis-server")
EVERY = 0.05


def pss(pid: int) -> int:
    """The Pss of `pid` and its children, in bytes (0 once it's gone)."""
    total = 0
    for p in [pid, *children(pid)]:
        try:
            with open(f"/proc/{p}/smaps_rollup") as f:
                total += next(int(l.split()[1]) for l in f if l.startswith("Pss:")) * 1024
        except (OSError, StopIteration):
            pass
    return total


def children(pid: int) -> List[int]:
    found = []
    try:
        for task in os.listdir(f"/proc/{pid}/task"):
            with open(f"/proc/{pid}/task/{task}/children") as f:
                found += [int(c) for c in f.read().split()]
    except OSError:
        pass
    return found


def info(port: int, section: str = "replication") -> str:
    with Conn(HOST, port, timeout=30) as c:
        return c.call("INFO", section).value.decode()


def field(text: str, name: str) -> str:
    m = re.search(rf"^{name}:(.*?)\r?$", text, re.M)
    return m.group(1) if m else ""


def call(port: int, *args):
    with Conn(HOST, port, timeout=60) as c:
        return c.call(*args).value


def load(port: int, n: int):
    """SETs key:%012d to a 16 B value for 0..n, pipelined."""
    chunk = 10000
    with socket.create_connection((HOST, port)) as s:
        for start in range(0, n, chunk):
            end = min(n, start + chunk)
            s.sendall(b"".join(b"*3\r\n$3\r\nSET\r\n$16\r\nkey:%012d\r\n$16\r\n%016d\r\n" % (i, i) for i in range(start, end)))
            want, got = 5 * (end - start), 0
            while got < want:
                data = s.recv(1 << 20)
                if not data:
                    raise SystemExit(f"{port}: the connection closed while loading")
                if b"-" in data:
                    raise SystemExit(f"{port}: an error while loading: {data[:200]!r}")
                got += len(data)


class Sampler(threading.Thread):
    """Peak Pss of the primary and the replica (whose pid may come later)."""

    def __init__(self, primary: int, replica):
        super().__init__(daemon=True)
        self.primary, self.replica = primary, replica
        self.peak_primary = self.peak_replica = 0
        self.done = threading.Event()

    def run(self):
        while not self.done.is_set():
            self.peak_primary = max(self.peak_primary, pss(self.primary))
            pid = self.replica()
            if pid:
                self.peak_replica = max(self.peak_replica, pss(pid))
            time.sleep(EVERY)


def writer(port: int, n: int) -> subprocess.Popen:
    return subprocess.Popen(["taskset", "-c", CLIENT_CPUS, "redis-benchmark", "-h", HOST, "-p", str(port), "-t", "set",
                             "-r", str(n), "-d", "16", "-c", "50", "-n", "1000000000", "-q"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class Proxy:
    """TCP from `port` to `target`; cut() drops every connection and turns
    new ones away for a while."""

    def __init__(self, port: int, target: int):
        self.target = target
        self.server = socket.create_server((HOST, port))
        self.closed_until = 0.0
        self.pairs: List[Tuple[socket.socket, socket.socket]] = []
        self.lock = threading.Lock()
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                c, _ = self.server.accept()
            except OSError:
                return
            if time.monotonic() < self.closed_until:
                c.close()
                continue
            try:
                u = socket.create_connection((HOST, self.target))
            except OSError:
                c.close()
                continue
            for s in (c, u):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self.lock:
                self.pairs.append((c, u))
            for a, b in ((c, u), (u, c)):
                threading.Thread(target=self.pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def pipe(a: socket.socket, b: socket.socket):
        try:
            while True:
                data = a.recv(1 << 20)
                if not data:
                    break
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            a.close()

    def cut(self, seconds: float):
        self.closed_until = time.monotonic() + seconds
        with self.lock:
            pairs, self.pairs = self.pairs, []
        for pair in pairs:
            for s in pair:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        # A thread blocked in accept() keeps the socket listening past a
        # close(); a shutdown() wakes it.
        try:
            self.server.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.server.close()
        self.cut(0)


def link_up(port: int) -> bool:
    text = info(port)
    return field(text, "master_link_status") == "up" and field(text, "master_sync_in_progress") == "0"


def settle(primary: int, replica: int) -> str:
    """Once writes stopped: the replica's DBSIZE and offset as the primary's."""
    wait_for("the replica to catch up",
             lambda: field(info(primary), "master_repl_offset") == field(info(replica), "slave_repl_offset"), timeout=60)
    a, b = call(primary, "DBSIZE"), call(replica, "DBSIZE")
    return "the same" if a == b else f"DIFFERENT: {a} / {b}"


def measure(start_replica, primary_pid: int, replica_pid, primary: int, replica: int, n: int, writes: bool) -> Dict:
    before = pss(primary_pid)
    load_proc = writer(primary, n) if writes else None
    if load_proc:
        time.sleep(1)  # under way
    sampler = Sampler(primary_pid, replica_pid)
    sampler.start()
    t0 = time.monotonic()
    start_replica()
    wait_for("the replica's link up", lambda: link_up(replica), timeout=1800, every=EVERY)
    took = time.monotonic() - t0
    if load_proc:
        load_proc.kill()
        load_proc.wait()
    time.sleep(0.5)
    sampler.done.set()
    sampler.join()
    check = settle(primary, replica)
    after = pss(replica_pid())
    return {"took": took, "before": before, "peak": sampler.peak_primary, "replica_peak": sampler.peak_replica,
            "replica_after": after, "check": check}


CUT = 1.0


def syncs(port: int) -> Tuple[int, int]:
    text = info(port, "stats")
    return int(field(text, "sync_full")), int(field(text, "sync_partial_ok"))


def measure_cut(proxy: Proxy, primary: int, replica: int, n: int, writes: bool) -> Dict:
    wait_for("the replica's link up", lambda: link_up(replica), timeout=600, every=EVERY)
    load_proc = writer(primary, n) if writes else None
    time.sleep(1)
    before, offset = syncs(primary), int(field(info(primary), "master_repl_offset"))
    proxy.cut(CUT)
    reopen = time.monotonic() + CUT
    wait_for("the replica's link to drop", lambda: not link_up(replica), timeout=10, every=0.01)
    time.sleep(max(0.0, reopen - time.monotonic()))
    during = int(field(info(primary), "master_repl_offset")) - offset
    wait_for("the replica's link up again", lambda: link_up(replica), timeout=1800, every=EVERY)
    back = time.monotonic() - reopen
    if load_proc:
        load_proc.kill()
        load_proc.wait()
    after = syncs(primary)
    how = {(1, 0): "full", (0, 1): "partial"}.get((after[0] - before[0], after[1] - before[1]), f"{before} -> {after}")
    return {"back": back, "during": during, "how": how, "check": settle(primary, replica)}


def ours(args, n: int, writes: bool) -> Dict:
    base = args.base_port
    primary, replica = base + 1, base + 2
    cluster = Cluster(args.bin, base, [], ["--auto", "--replicas", "1"],
                      cpus={base: STUART_CPU, primary: PRIMARY_CPU, replica: REPLICA_CPU})
    try:
        cluster.start_node(primary)
        wait_for("the primary to own every slot",
                 lambda: b"\ncluster_slots_assigned:16384\r" in call(primary, "CLUSTER", "INFO"), timeout=30)
        load(primary, n)
        started = threading.Thread(target=cluster.start_node, args=(replica,), daemon=True)
        return measure(started.start, cluster.nodes[primary].pid, lambda: cluster.nodes[replica].pid if replica in cluster.nodes else 0,
                       primary, replica, n, writes)
    finally:
        cluster.stop(keep=False)


def ours_cut(args, n: int, writes: bool) -> Dict:
    base = args.base_port
    primary, replica = base + 1, base + 2
    proxy = Proxy(base + 20, primary)
    cluster = Cluster(args.bin, base, [], ["--auto", "--replicas", "1"],
                      cpus={base: STUART_CPU, primary: PRIMARY_CPU, replica: REPLICA_CPU})
    try:
        cluster.start_node(primary, ["--announce", f"{HOST}:{base + 20}"])
        wait_for("the primary to own every slot",
                 lambda: b"\ncluster_slots_assigned:16384\r" in call(primary, "CLUSTER", "INFO"), timeout=30)
        load(primary, n)
        cluster.start_node(replica)
        return measure_cut(proxy, primary, replica, n, writes)
    finally:
        cluster.stop(keep=False)
        proxy.close()


class Servers:
    """A redis-server-like primary and replica, killed at the end."""

    def __init__(self, server: str):
        self.server = server
        self.tmp = tempfile.mkdtemp(prefix="sync_bench.")
        self.procs: List[subprocess.Popen] = []

    def start(self, port: int, cpu: int) -> subprocess.Popen:
        d = os.path.join(self.tmp, str(port))
        os.mkdir(d)
        p = subprocess.Popen(["taskset", "-c", str(cpu), self.server, "--port", str(port), "--dir", d, "--save", "",
                              "--appendonly", "no", "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0",
                              "--loglevel", "warning"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        wait_for(f"{self.server} {port}", lambda: call(port, "PING") == b"PONG")
        return p

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for p in self.procs:
            p.kill()
            p.wait()
        shutil.rmtree(self.tmp, ignore_errors=True)


def redis(args, server: str, n: int, writes: bool) -> Dict:
    primary, replica = args.base_port + 11, args.base_port + 12
    with Servers(server) as s:
        p = s.start(primary, PRIMARY_CPU)
        load(primary, n)
        r = s.start(replica, REPLICA_CPU)
        return measure(lambda: call(replica, "REPLICAOF", HOST, str(primary)), p.pid, lambda: r.pid, primary, replica, n, writes)


def redis_cut(args, server: str, n: int, writes: bool) -> Dict:
    primary, replica = args.base_port + 11, args.base_port + 12
    proxy = Proxy(args.base_port + 20, primary)
    try:
        with Servers(server) as s:
            s.start(primary, PRIMARY_CPU)
            load(primary, n)
            s.start(replica, REPLICA_CPU)
            call(replica, "REPLICAOF", HOST, str(args.base_port + 20))
            return measure_cut(proxy, primary, replica, n, writes)
    finally:
        proxy.close()


def version(server: str) -> str:
    out = subprocess.run([server, "--version"], capture_output=True, text=True, check=True).stdout
    m = re.search(r"v=(\S+)", out)
    return m.group(1) if m else out.strip()


def mb(b: int) -> str:
    return f"{b / 1e6:,.0f}"


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="the directory with release dbnode and stuart")
    ap.add_argument("--keys", default="1000000,10000000")
    ap.add_argument("--cut-keys", type=int, default=1000000)
    ap.add_argument("--base-port", type=int, default=7950)
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
    setups = [("ours", ours, ours_cut)] + [
        (name, lambda a, n, w, s=server: redis(a, s, n, w), lambda a, n, w, s=server: redis_cut(a, s, n, w))
        for name, server in servers]
    lines = ["| server | keys | writes during the sync | sync (s) | primary before (MB) | primary peak (MB) | peak / before | "
             "replica peak (MB) | replica after (MB) | replica's keys |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    print("\n".join(lines), flush=True)
    for n in [int(k) for k in args.keys.split(",")]:
        for name, run, _ in setups:
            for writes in (False, True):
                r = run(args, n, writes)
                line = (f"| {name} | {n:,} | {'SET -c 50' if writes else 'none'} | {r['took']:.2f} | {mb(r['before'])} | "
                        f"{mb(r['peak'])} | {r['peak'] / r['before']:.2f} | {mb(r['replica_peak'])} | {mb(r['replica_after'])} | "
                        f"{r['check']} |")
                lines.append(line)
                print(line, flush=True)
    cut = ["", f"A {CUT:.0f} s cut, {args.cut_keys:,} keys:", "",
           "| server | writes | written during the cut (MB) | link up again after the cut (s) | resync | replica's keys |",
           "|---|---|---|---|---|---|"]
    print("\n".join(cut), flush=True)
    for name, _, run in setups:
        for writes in (False, True):
            r = run(args, args.cut_keys, writes)
            line = (f"| {name} | {'SET -c 50' if writes else 'none'} | {r['during'] / 1e6:,.1f} | {r['back']:.2f} | "
                    f"{r['how']} | {r['check']} |")
            cut.append(line)
            print(line, flush=True)
    lines += cut
    if args.out:
        with open(args.out, "w") as f:
            f.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
