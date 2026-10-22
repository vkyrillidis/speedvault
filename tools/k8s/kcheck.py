#!/usr/bin/env python3
"""The checker of kind_check.sh, run in a pod in the Kubernetes cluster
(the image of checker/Dockerfile): migrate_check.py's writers and its check,
against the manifests' cluster instead of processes it starts.

    kcheck.py run --nodes N [--keys 2000] [--filler 20000] [--lettuce MODES]
    kcheck.py lossy POD          (kubectl exec, beside a running `run`)
    kcheck.py fill RATIO         (kubectl exec, beside a running `run`)
    kcheck.py even N             (kubectl exec)
    kcheck.py load < JSON        (kubectl exec -i)
    kcheck.py stop N             (kubectl exec, beside a running `run`)

`run` waits until stuart's N nodes hold even shares of the slots, empties
them (FLUSHALL), writes the filler keys, starts the writers (two redis-py RedisCluster threads, a
redis-cli -c process, a redis-py thread on the hot counters, and with --lettuce a Lettuce client
for each of LettuceLoad's modes given (comma separated); all seeded
with the speedvault-seed Service), prints `writing`, and reports progress
every 10 s. Meanwhile kind_check.sh scales, restarts or deletes pods.

`lossy POD` notes that pod's slots now (before kind_check.sh kills it
without letting it drain): their keys may be lost, and INCRs may fail.

`fill RATIO` writes m:0, m:1, ... (16 KiB values) until the nodes' used
memory averages RATIO of their maxmemory (none past 0.98): the load of the
memory autoscaling scenario. `run`'s check reads them back too.

`even N` waits until N nodes hold even shares and nothing moves, and says
how long that took.

`load` summarizes memtier_benchmark's --json-out-file: its throughput and
p99 latency over every 10 s, and in all.

`stop N` tells `run` to finish: it waits until N nodes hold even shares and
nothing moves, stops the writers and reads every node directly, as
migrate_check.py does: no key at two nodes or at a node whose table doesn't
give it its slot, every filler key there with its value, each counter at
least its acknowledged INCRs and at most those plus the ones in doubt (only
bounded above in the lossy slots), and no INCR failed unless some slots are
lossy. It prints OK or the problems, and exits 0 or 1.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Dict, List, Set

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import migrate_check as mc  # noqa: E402

SEED = os.environ.get("SEED", "speedvault-seed")
STUART = os.environ.get("STUART", "stuart")
PORT = 7000
CTL = os.environ.get("CTL", "/tmp/kcheck")


class K8sCluster(mc.Cluster):
    """stuart and the nodes as the manifests run them: a node is its
    announced host (its pod's address, port 7000), not a port."""

    def __init__(self):
        self.base = 7600

    def stuart(self):
        return mc.redis.Redis(host=STUART, port=7600, socket_timeout=10)

    def node(self, host: str):
        return mc.redis.Redis(host=host, port=PORT, socket_timeout=30)

    def node_ports(self) -> List[str]:
        """The nodes stuart knows that aren't leaving, by host."""
        return sorted(l.split()[1].rsplit(":", 1)[0] for l in self.stuart_nodes()
                      if not l.startswith("unassigned") and " leaving" not in l)

    def owners(self) -> Dict[int, str]:
        views = []
        for host in self.node_ports():
            view = {}
            for start, end, primary, *_ in self.node(host).execute_command("CLUSTER", "SLOTS"):
                for s in range(start, end + 1):
                    view[s] = mc.text(primary[0])
            views.append(view)
        if any(v != views[0] for v in views):
            raise mc.Fail("the nodes' CLUSTER SLOTS differ once settled")
        return views[0]


def say(msg: str):
    print(msg, flush=True)


def ctl(name: str) -> str:
    return os.path.join(CTL, name)


def lossy_slots() -> Set[int]:
    try:
        with open(ctl("lossy")) as f:
            return {int(s) for s in f.read().split()}
    except FileNotFoundError:
        return set()


def mem_keys() -> int:
    try:
        with open(ctl("mem")) as f:
            return int(f.read())
    except FileNotFoundError:
        return 0


def run(args) -> int:
    os.makedirs(CTL, exist_ok=True)
    cluster = K8sCluster()
    t0 = time.monotonic()
    mc.wait_even(cluster, args.nodes, 600)
    say(f"{args.nodes} nodes even after {time.monotonic() - t0:.0f} s: {', '.join(cluster.node_ports())}")
    # The counters start at 0: the keys of an earlier run go.
    for host in cluster.node_ports():
        cluster.node(host).flushall()
    if args.filler:
        cluster.fill(args.filler)
    mc.HOST = SEED
    lettuce = argparse.Namespace(lettuce=open("/opt/lettuce/classpath").read().strip(), lettuce_modes=args.lettuce.split(","),
                                 lettuce_port=PORT) if args.lettuce else argparse.Namespace(lettuce_modes=[])
    writers = mc.make_writers(argparse.Namespace(keys=args.keys, **vars(lettuce)), PORT)
    for j, w in enumerate(writers):
        w.start(j * args.keys // len(writers))
    say("writing")
    last = time.monotonic()
    while not os.path.exists(ctl("stop")):
        time.sleep(0.2)
        if time.monotonic() - last >= 10:
            last = time.monotonic()
            say("  " + "; ".join(f"{w.name} {w.ops} INCRs, {sum(w.doubt)} failed" for w in writers))
    with open(ctl("stop")) as f:
        n = int(f.read())
    problems: List[str] = []
    try:
        t0 = time.monotonic()
        mc.wait_even(cluster, n, 600)
        say(f"{n} nodes even {time.monotonic() - t0:.1f} s after stop: {', '.join(cluster.node_ports())}")
        time.sleep(1)
        for w in writers:
            w.stop()
        lossy = lossy_slots()
        problems += mc.report_writers(writers, may_fail=bool(lossy))
        problems += mc.check(cluster, writers, args.filler, lossy, mem_keys())
    except mc.Fail as e:
        problems.append(str(e))
    for p in problems:
        say(f"FAIL: {p}")
    say("OK" if not problems else "FAILED")
    return 1 if problems else 0


def lossy(pod: str) -> int:
    """Notes the slots the pod `pod` owns or is taking, from stuart."""
    cluster = K8sCluster()
    line = next((l for l in cluster.stuart_nodes() if re.search(rf" name={re.escape(pod)}( |$)", l)), None)
    if line is None:
        say(f"stuart knows no node {pod}")
        return 1
    slots = mc.slots_of(cluster, line.split()[0])
    with open(ctl("lossy"), "a") as f:
        f.write(" ".join(map(str, sorted(slots))) + "\n")
    say(f"{pod}: {len(slots)} slots noted as lossy")
    return 0


def ratios(cluster: K8sCluster) -> Dict[str, float]:
    """used_memory / maxmemory of each node."""
    out = {}
    for host in cluster.node_ports():
        info = cluster.node(host).info("memory")
        out[host] = info["used_memory"] / info["maxmemory"]
    return out


def fill(target: float) -> int:
    """m:<i> keys, in rounds of a few hundred (a percent or two of a node's
    maxmemory), straight to their owners, until the average is reached."""
    cluster = K8sCluster()
    n = 0
    while True:
        r = ratios(cluster)
        if sum(r.values()) / len(r) >= target:
            break
        if max(r.values()) >= 0.98:
            say(f"a node is at {max(r.values()):.3f} of its maxmemory before the average got to {target}")
            return 1
        owners = cluster.owners()
        by_host: Dict[str, List[int]] = {}
        for i in range(n, n + 100 * len(r)):
            by_host.setdefault(owners[mc.key_slot(f"m:{i}".encode())], []).append(i)
        for host, ids in by_host.items():
            p = cluster.node(host).pipeline(transaction=False)
            for i in ids:
                p.set(f"m:{i}", mc.mem_value(i))
            p.execute()
        n += 100 * len(r)
        # Written: `run` checks them, whatever happens next.
        with open(ctl("mem.tmp"), "w") as f:
            f.write(str(n))
        os.rename(ctl("mem.tmp"), ctl("mem"))
    say(f"{n} keys of 16 KiB: " + ", ".join(f"{h} {v:.3f}" for h, v in sorted(r.items())))
    return 0


def load_summary(stats: dict) -> int:
    """memtier_benchmark's JSON: ten seconds a line, then the totals."""
    totals = stats["ALL STATS"]["Totals"]
    series = {int(t): v for t, v in totals["Time-Serie"].items()}
    say("from    requests/s   p99 ms (worst second)")
    for start in range(0, max(series) + 1, 10):
        secs = [series[t] for t in range(start, start + 10) if t in series]
        ops = sum(s["Count"] for s in secs) / max(len(secs), 1)
        p99 = max((s.get("p99.00", 0.0) for s in secs), default=0.0)
        say(f"+{start:<4}s  {ops:>10.0f}   {p99:8.3f}")
    pct = totals.get("Percentile Latencies", {})
    say(f"in all: {totals['Ops/sec']:.0f} requests/s, p50 {pct.get('p50.00', 0):.3f} ms, "
        f"p99 {pct.get('p99.00', 0):.3f} ms, p99.9 {pct.get('p99.90', 0):.3f} ms")
    return 0


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--nodes", type=int, required=True, help="nodes up when it starts")
    r.add_argument("--keys", type=int, default=2000, help="counters")
    r.add_argument("--filler", type=int, default=20000, help="other keys, so moves take a while")
    r.add_argument("--lettuce", metavar="MODES", help="Lettuce writers too, in these modes (see LettuceLoad.java)")
    sub.add_parser("lossy").add_argument("pod")
    sub.add_parser("fill").add_argument("ratio", type=float)
    sub.add_parser("even").add_argument("nodes", type=int)
    sub.add_parser("load")
    sub.add_parser("stop").add_argument("nodes", type=int)
    args = ap.parse_args(argv)
    if args.cmd == "run":
        return run(args)
    if args.cmd == "lossy":
        return lossy(args.pod)
    if args.cmd == "fill":
        return fill(args.ratio)
    if args.cmd == "load":
        return load_summary(json.load(sys.stdin))
    if args.cmd == "even":
        t0 = time.monotonic()
        mc.wait_even(K8sCluster(), args.nodes, 600)
        say(f"{args.nodes} nodes even after {time.monotonic() - t0:.1f} s")
        return 0
    with open(ctl("stop.tmp"), "w") as f:
        f.write(str(args.nodes))
    os.rename(ctl("stop.tmp"), ctl("stop"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
