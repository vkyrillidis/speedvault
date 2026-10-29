#!/usr/bin/env python3
"""How long a primary's slots go unserved when it dies or leaves (phase 8
milestones 5 and 10; run by failover_bench.sh).

    python3 failover_bench.py --bin DIR [--timeouts 2,5] [--runs 3] [--base-port 7700]
                              [--also NAME=SERVER ...] [--out FILE]

Three primaries with a replica each: ours (stuart --auto --replicas 1
--fail-after T, six nodes joining one by one), Redis 7.0.15's and each
--also NAME=SERVER's, a redis-server-like binary (Redis 8.10.2, Valkey
9.1.2) (redis-cli --cluster create --cluster-replicas 1, the redis-cli
next to the server if there is one, cluster-node-timeout T,
cluster-replica-validity-factor 0: WSL2's wall clock steps make a
replica's data look too old otherwise). A redis-py RedisCluster INCRs keys
of a primary's slots (not the first node's) in a loop, counting the INCRs
acknowledged per key.

kill      the primary is SIGKILLed. Measured, on the monotonic clock: from
          the kill to the replica's promotion (CLUSTER NODES, at stuart for
          ours, at the first node for theirs, says master with the slots),
          and to the first INCR the client gets through again (it
          refreshes its map on each error). Ours waits the dead primary's
          lease (3 s) after --fail-after: a primary only cut off has
          stopped writing by then; Redis has no such wait (a cut-off master
          writes on until it learns, from gossip).
graceful  (timeout 5 s) the primary leaves on purpose: ours gets SIGTERM
          (its writes pause, its replica catches up and takes its slots);
          theirs, CLUSTER FAILOVER at the replica (the master pauses
          writes, the replica catches up and takes over). Measured: from
          the SIGTERM / CLUSTER FAILOVER to the promotion, and the longest
          gap between two acknowledged INCRs (what the client sees).

Both: the writes lost, the INCRs acknowledged that the keys' values don't
hold once the replica serves them. Times are the median of --runs, the
writes lost each run's. Prints Markdown tables (and writes them to --out).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
from migrate_check import Cluster, wait_for  # noqa: E402
from redis.cluster import RedisCluster  # noqa: E402
from redis.crc import key_slot  # noqa: E402
from resp import Conn  # noqa: E402

HOST = "127.0.0.1"
REDIS_SERVER = os.environ.get("REDIS_SERVER", "redis-server")
GRACEFUL_TIMEOUT = 5


def call(port: int, *args):
    with Conn(HOST, port, timeout=5) as c:
        return c.call(*args).value


def nodes(port: int) -> List[List[str]]:
    return [l.split() for l in call(port, "CLUSTER", "NODES").decode().splitlines()]


def port_of(f: List[str]) -> int:
    return int(f[1].split("@")[0].rsplit(":", 1)[1])


def slots_in(f: List[str]) -> set:
    out = set()
    for r in f[8:]:
        a, _, b = r.partition("-")
        out.update(range(int(a), int(b or a) + 1))
    return out


def victim_and_heir(port: int, not_port: int):
    """A primary with slots (not `not_port`), its replica and its slots,
    as `port` sees them."""
    view = nodes(port)
    primary = next(f for f in view if "master" in f[2] and f[8:] and port_of(f) != not_port)
    replica = next(f for f in view if f[3] == primary[0])
    return port_of(primary), port_of(replica), slots_in(primary)


class Writer(threading.Thread):
    """INCRs of keys in `slots` until stopped, counting those acknowledged;
    notes when one first gets through again after `since` is set and one
    failed, and the longest gap between two acknowledged after `since`."""

    def __init__(self, seed: int, slots: set):
        super().__init__(daemon=True)
        self.keys = [k for k in (f"fb:{i}" for i in range(100000)) if key_slot(k.encode()) in slots][:200]
        self.seed = seed
        self.acked = {k: 0 for k in self.keys}
        self.since: Optional[float] = None
        self.failed = False
        self.back: Optional[float] = None
        self.last: Optional[float] = None
        self.gap = 0.0
        self.stop = threading.Event()

    def run(self):
        rc = RedisCluster(host=HOST, port=self.seed, socket_timeout=1, socket_connect_timeout=1)
        i = 0
        while not self.stop.is_set():
            i += 1
            key = self.keys[i % len(self.keys)]
            try:
                rc.incr(key)
                self.acked[key] += 1
                now = time.monotonic()
                if self.since is not None:
                    self.gap = max(self.gap, now - max(self.last or self.since, self.since))
                self.last = now
                if self.failed and self.back is None:
                    self.back = now - self.since
            except Exception:  # noqa: BLE001 - the node is gone: refresh and retry
                self.failed = self.since is not None
                try:
                    rc.nodes_manager.initialize()
                except Exception:  # noqa: BLE001
                    time.sleep(0.05)

    def lost(self) -> int:
        rc = RedisCluster(host=HOST, port=self.seed, socket_timeout=5)
        try:
            return sum(max(0, n - int(rc.get(k) or 0)) for k, n in self.acked.items())
        finally:
            rc.close()


def measure(seed: int, slots: set, act: Callable[[], None], promoted: Callable[[], bool], kill: bool) -> Dict[str, float]:
    w = Writer(seed, slots)
    w.start()
    time.sleep(2)
    t0 = time.monotonic()
    w.since = t0
    act()
    wait_for("the promotion", promoted, 60, 0.01)
    took = time.monotonic() - t0
    if kill:
        wait_for("the client to write again", lambda: w.back is not None, 60, 0.01)
    else:
        wait_for("the client to write after the promotion", lambda: (w.last or 0) > t0 + took, 60, 0.01)
    time.sleep(0.5)
    w.stop.set()
    w.join(10)
    return {"promoted": took, "served": w.back or 0.0, "gap": w.gap, "lost": w.lost()}


def ours(args, t: int, kill: bool) -> Dict[str, float]:
    base = args.base_port
    cluster = Cluster(args.bin, base, [], ["--auto", "--replicas", "1", "--fail-after", str(t)])
    try:
        for i in range(6):
            cluster.start_node(base + 1 + i)
        wait_for("three primaries with replicas", lambda: sum("replica-of=" in l for l in cluster.stuart_nodes()) == 3
                 and not cluster.migrations() and all(" up " in l for l in cluster.stuart_nodes()), 120, 0.1)
        cluster.wait_settled()
        wait_for("the replicas synced", lambda: sum(synced(base + 1 + i) for i in range(6)) == 3, 60, 0.1)
        victim, heir, slots = victim_and_heir(base + 1, base + 1)

        def promoted():
            return any(port_of(f) == heir and "master" in f[2] and slots_in(f) == slots for f in nodes(base))
        act = (lambda: cluster.kill_node(victim)) if kill else (lambda: cluster.term_node(victim))
        return measure(base, slots, act, promoted, kill)
    finally:
        cluster.stop(keep=False)


def synced(port: int) -> bool:
    text = call(port, "INFO", "replication").decode()
    return "master_link_status:up" in text and "master_sync_in_progress:0" in text


def cli_for(server: str) -> str:
    """The redis-cli next to `server`, else the one on the PATH."""
    path = shutil.which(server) or server
    sibling = os.path.join(os.path.dirname(path), "redis-cli")
    return sibling if os.path.exists(sibling) else "redis-cli"


def redis(args, server: str, t: int, kill: bool) -> Dict[str, float]:
    base = args.base_port + 20
    ports = [base + i for i in range(6)]
    tmp = tempfile.mkdtemp(prefix="failover_bench.")
    procs: Dict[int, subprocess.Popen] = {}
    try:
        for p in ports:
            d = os.path.join(tmp, str(p))
            os.mkdir(d)
            procs[p] = subprocess.Popen([server, "--port", str(p), "--dir", d, "--cluster-enabled", "yes",
                                         "--cluster-config-file", "nodes.conf", "--cluster-node-timeout", str(t * 1000),
                                         "--cluster-replica-validity-factor", "0", "--cluster-require-full-coverage", "no",
                                         "--save", "", "--appendonly", "no", "--loglevel", "warning"],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for p in ports:
            wait_for(f"{server} {p}", lambda p=p: call(p, "PING") == b"PONG")
        subprocess.run([cli_for(server), "--cluster", "create", *[f"{HOST}:{p}" for p in ports], "--cluster-replicas", "1",
                        "--cluster-yes"], check=True, stdout=subprocess.DEVNULL)
        wait_for("the cluster ok, replicas known", lambda: all(b"cluster_state:ok" in call(p, "CLUSTER", "INFO")
                                                               and sum("slave" in f[2] for f in nodes(p)) == 3 for p in ports), 60, 0.1)
        # Their diskless sync waits 5 s for more replicas: synced first.
        wait_for("the replicas synced", lambda: sum(synced(p) for p in ports) == 3, 60, 0.1)
        victim, heir, slots = victim_and_heir(ports[0], ports[0])
        seed = ports[0]

        def promoted():
            return any(port_of(f) == heir and "master" in f[2] and slots_in(f) == slots for f in nodes(seed))

        def act():
            if kill:
                procs[victim].send_signal(signal.SIGKILL)
                procs[victim].wait()
            else:
                call(heir, "CLUSTER", "FAILOVER")
        return measure(seed, slots, act, promoted, kill)
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()
        shutil.rmtree(tmp, ignore_errors=True)


def version(server: str) -> str:
    out = subprocess.run([server, "--version"], capture_output=True, text=True, check=True).stdout
    m = re.search(r"v=(\S+)", out)
    return m.group(1) if m else out.strip()


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="the directory with release dbnode and stuart")
    ap.add_argument("--timeouts", default="2,5", help="--fail-after / cluster-node-timeout, seconds")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=7700)
    ap.add_argument("--also", action="append", default=[], metavar="NAME=SERVER",
                    help="also a redis-server-like binary")
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    setups: List[Tuple[str, Callable[[int, bool], Dict[str, float]]]] = [("ours", lambda t, k: ours(args, t, k))]
    servers = [(f"Redis {version(REDIS_SERVER)}", REDIS_SERVER)]
    for a in args.also:
        name, sep, path = a.partition("=")
        if not sep:
            ap.error(f"--also takes NAME=SERVER, not {a}")
        servers.append((name, path))
    setups += [(name, lambda t, k, s=server: redis(args, s, t, k)) for name, server in servers]

    lines = ["Killed (SIGKILL):", "",
             "| Server | timeout (s) | promoted after (s) | client writes again after (s) | writes lost |", "|---|---|---|---|---|"]
    print("\n".join(lines), flush=True)
    for t in (int(x) for x in args.timeouts.split(",")):
        for name, f in setups:
            rs = [f(t, True) for _ in range(args.runs)]
            p, s = statistics.median(r["promoted"] for r in rs), statistics.median(r["served"] for r in rs)
            lines.append(f"| {name} | {t} | {p:.1f} | {s:.1f} | {', '.join(str(r['lost']) for r in rs)} |")
            print(lines[-1], flush=True)
    graceful = ["", f"Leaving on purpose (ours: SIGTERM; theirs: CLUSTER FAILOVER at the replica), timeout {GRACEFUL_TIMEOUT} s:", "",
                "| Server | promoted after (s) | client's longest wait (s) | writes lost |", "|---|---|---|---|"]
    print("\n".join(graceful), flush=True)
    for name, f in setups:
        rs = [f(GRACEFUL_TIMEOUT, False) for _ in range(args.runs)]
        p, g = statistics.median(r["promoted"] for r in rs), statistics.median(r["gap"] for r in rs)
        graceful.append(f"| {name} | {p:.2f} | {g:.3f} | {', '.join(str(r['lost']) for r in rs)} |")
        print(graceful[-1], flush=True)
    text = "\n".join(lines + graceful) + "\n"
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
