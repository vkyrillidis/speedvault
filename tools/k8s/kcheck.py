#!/usr/bin/env python3
"""The checker of kind_check.sh, run in a pod in the Kubernetes cluster
(the image of checker/Dockerfile): migrate_check.py's writers and its check,
against the manifests' cluster instead of processes it starts.

    kcheck.py run --nodes N [--keys 2000] [--filler 20000] [--lettuce MODES]
    kcheck.py lossy POD          (kubectl exec, beside a running `run`)
    kcheck.py lossy-zone ZONE    (kubectl exec, beside a running `run`)
    kcheck.py fill RATIO         (kubectl exec, beside a running `run`)
    kcheck.py even N             (kubectl exec)
    kcheck.py pairs              (kubectl exec)
    kcheck.py primary            (kubectl exec)
    kcheck.py failover ZONE      (kubectl exec)
    kcheck.py counts             (kubectl exec)
    kcheck.py load < JSON        (kubectl exec -i)
    kcheck.py stop N             (kubectl exec, beside a running `run`)

REPLICAS (the environment, 0 or 1) is stuart's --replicas. With 1, N
nodes are ceil(N / 2) primaries, each with a replica (one may lack one).

`run` waits until stuart's N nodes are laid out (the primaries hold even
shares of the slots, nothing moves, every replica synced with its
primary), empties them (FLUSHALL), writes the filler keys, starts the writers (two redis-py RedisCluster threads, a
redis-cli -c process, a redis-py thread on the hot counters, and with --lettuce a Lettuce client
for each of LettuceLoad's modes given (comma separated); with replicas,
redis-py writers sending each INCR, and SET/DEL, with a WAIT 1; all seeded
with the speedvault-seed Service), prints `writing`, and reports progress
every 10 s. Meanwhile kind_check.sh scales, restarts or deletes pods.

`lossy POD` notes that pod's slots now (before kind_check.sh kills it
without letting it drain): their keys may be lost, and INCRs may fail.
With replicas, only writes the replica hadn't got: no filler key may be
lost, nor an INCR or a SET/DEL that WAIT said the replica had. `lossy-zone
ZONE` notes the slots of every node in that zone (a Kubernetes node about
to be stopped), and as refusing those of each primary outside it whose
replica is in it: that primary answers -NOREPLICAS from when its lease
lapses until stuart marks the replica failed (docs/design/replicas.md),
so writes there may fail, with -NOREPLICAS only, and none may be lost.

`pairs` lists the primaries and their replicas with their zones, and
fails if a replica is in its primary's zone. `primary` prints the pod
name of a primary (the first by name). `failover ZONE` waits until no
node in ZONE serves a slot (its primaries' replicas promoted) and says
how long that took. `counts` prints stuart's counters: slots moved,
handovers, demotions.

`fill RATIO` writes m:0, m:1, ... (16 KiB values) until the nodes' used
memory averages RATIO of their maxmemory (none past 0.98): the load of the
memory autoscaling scenario. `run`'s check reads them back too.

`even N` waits until N nodes are laid out (see `run`), and says how long
that took.

`load` summarizes memtier_benchmark's --json-out-file: its throughput and
p99 latency over every 10 s, and in all.

`stop N` tells `run` to finish: it waits until N nodes are laid out,
stops the writers and reads every node directly, as
migrate_check.py does: no key at two nodes or at a node whose table doesn't
give it its slot, every filler key there with its value, each counter at
least its acknowledged INCRs and at most those plus the ones in doubt (only
bounded above in the lossy slots), no INCR failed unless some slots are
lossy, and no -ASK or -TRYAGAIN (slots move by snapshot). With replicas
the replicas are left out of that and compared with their primaries key
by key instead, once caught up, and the WAIT writers' keys checked (see
migrate_check.py). It prints OK or the problems, and exits 0 or 1.
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
REPLICAS = int(os.environ.get("REPLICAS", "0"))
# kind_check.sh --auth: every client logs in (the writers through migrate_check's).
mc.PASSWORD = os.environ.get("SPEEDVAULT_PASSWORD") or None


class K8sCluster(mc.Cluster):
    """stuart and the nodes as the manifests run them: a node is its
    announced host (its pod's address, port 7000), not a port."""

    def __init__(self):
        self.base = 7600

    def stuart(self):
        return mc.redis.Redis(password=mc.PASSWORD, host=STUART, port=7600, socket_timeout=10)

    def node(self, host: str):
        return mc.redis.Redis(password=mc.PASSWORD, host=host, port=PORT, socket_timeout=30)

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


def host_of(line: str) -> str:
    return line.split()[1].rsplit(":", 1)[0]


def field(line: str, name: str) -> str:
    m = re.search(rf" {name}=(\S+)", line)
    return m.group(1) if m else ""


def current(cluster: K8sCluster) -> Dict[str, str]:
    """STUART NODES by node id, the nodes leaving left out."""
    return {i: l for i, l in mc.stuart_lines(cluster).items() if " leaving" not in l}


def replica_hosts(cluster: K8sCluster) -> Dict[str, str]:
    """Replica host -> its primary's host."""
    lines = current(cluster)
    return {host_of(l): host_of(lines[p]) for l in lines.values() if (p := field(l, "replica-of")) in lines}


def not_laid_out(cluster: K8sCluster, n: int) -> str:
    """Why `n` nodes aren't laid out yet (see `run`); empty once they are."""
    lines = list(current(cluster).values())
    if len(lines) != n:
        return f"{len(lines)} nodes"
    primaries = [l for l in lines if "replica-of" not in l]
    want = -(-n // (1 + REPLICAS))
    if len(primaries) != want:
        return f"{len(primaries)} primaries, not {want}"
    counts = [int(field(l, "slots")) for l in primaries]
    if sum(counts) != mc.SLOTS or max(counts) - min(counts) > 1:
        return f"the primaries' slots: {counts}"
    if cluster.migrations():
        return "slots moving"
    for host in replica_hosts(cluster):
        info = cluster.node(host).info("replication")
        if info.get("master_link_status") != "up" or info.get("master_sync_in_progress"):
            return f"replica {host} not synced"
    return ""


def wait_laid_out(cluster: K8sCluster, n: int, timeout: float = 600):
    cluster.wait_settled(timeout)
    why = ["?"]

    def done():
        why[0] = not_laid_out(cluster, n)
        return not why[0]
    try:
        mc.wait_for(f"{n} nodes laid out", done, timeout, 0.2)
    except mc.Fail:
        raise mc.Fail(f"{n} nodes not laid out after {timeout:.0f} s: {why[0]}")
    cluster.wait_settled(timeout)


def ctl(name: str) -> str:
    return os.path.join(CTL, name)


def lossy_slots() -> Set[int]:
    try:
        with open(ctl("lossy")) as f:
            return {int(s) for s in f.read().split()}
    except FileNotFoundError:
        return set()


def refusing_slots() -> Set[int]:
    try:
        with open(ctl("refusing")) as f:
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
    wait_laid_out(cluster, args.nodes)
    say(f"{args.nodes} nodes laid out after {time.monotonic() - t0:.0f} s: {', '.join(cluster.node_ports())}")
    # The counters start at 0: the keys of an earlier run go (from the
    # primaries, and through their streams from the replicas).
    for host in cluster.node_ports():
        if host not in replica_hosts(cluster):
            cluster.node(host).flushall()
    if args.filler:
        cluster.fill(args.filler)
    mc.HOST = SEED
    lettuce = argparse.Namespace(lettuce=open("/opt/lettuce/classpath").read().strip(), lettuce_modes=args.lettuce.split(","),
                                 lettuce_port=PORT) if args.lettuce else argparse.Namespace(lettuce_modes=[])
    writers = mc.make_writers(argparse.Namespace(keys=args.keys, **vars(lettuce)), PORT)
    dels = None
    if REPLICAS:
        writers.append(mc.WaitWriter(writers[0].names, PORT))
        dels = mc.DelWriter(writers[0].names, PORT)
        writers.append(dels)
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
        wait_laid_out(cluster, n)
        say(f"{n} nodes laid out {time.monotonic() - t0:.1f} s after stop: {', '.join(cluster.node_ports())}")
        time.sleep(1)
        for w in writers:
            w.stop()
        lossy = lossy_slots()
        refusing = refusing_slots()
        replicas = replica_hosts(cluster)
        mc.wait_for("the replicas to catch up", lambda: mc.caught_up(cluster, replicas), 60, 0.1)
        # The WAIT writers send to the node they think owns the key: a
        # primary that handed its slots over answers -MOVED, one gone
        # drops the connection. Those fail (in doubt, refreshed), nothing
        # else.
        waits = [w for w in writers if isinstance(w, (mc.WaitWriter, mc.DelWriter))]
        problems += mc.report_writers([w for w in writers if w not in waits], may_fail=bool(lossy or refusing))
        problems += mc.report_writers([w for w in waits if w is not dels], may_fail=True)
        # Refused, without a node gone: -NOREPLICAS only.
        if refusing and not lossy:
            for w in writers:
                other = [e for e in w.errors if "NOREPLICAS" not in e and "redis_connection" not in e]
                if w not in waits and other:
                    problems.append(f"{w.name}: {len(other)} writes failed otherwise than refused: {other[:3]}")
        gone = r"MOVED|Moved|Connection|connection|Timeout|timed out|Broken pipe" + ("|NOREPLICAS" if refusing else "")
        for w in waits:
            other = [e for e in w.errors if not re.search(gone, e)]
            if other and not lossy:
                problems.append(f"{w.name}: {len(other)} writes failed otherwise than on a node gone: {other[:3]}")
        problems += mc.check(cluster, writers, args.filler, lossy, mem_keys(), replicas=set(replicas))
        if dels:
            problems += mc.check_deletes(cluster, dels, lossy)
        if replicas:
            problems += mc.compare_replicas(cluster, replicas)
        if REPLICAS and lossy:
            problems += filler_kept(args.filler)
        # Moves by snapshot send only -MOVED (counted at the nodes still up:
        # one that left or restarted took its counts along).
        asked = mc.redirect_counts(cluster, ("ask", "tryagain"))
        if asked:
            problems.append(f"{asked} -ASK/-TRYAGAIN with moves by snapshot")
        problems += [f"{w.name}: {w.client_bugs} failed on -ASK" for w in writers if w.client_bugs]
    except mc.Fail as e:
        problems.append(str(e))
    for p in problems:
        say(f"FAIL: {p}")
    say("OK" if not problems else "FAILED")
    return 1 if problems else 0


def filler_kept(n: int) -> List[str]:
    """With replicas, a node killed loses no filler key: written before,
    its replica had them."""
    rc = mc.RedisCluster(password=mc.PASSWORD, host=SEED, port=PORT)
    lost = wrong = 0
    for start in range(0, n, 2000):
        p = rc.pipeline()
        for i in range(start, min(n, start + 2000)):
            p.get(f"f:{i}")
        for i, v in zip(range(start, n), p.execute()):
            if v is None:
                lost += 1
            elif v != mc.filler_value(i):
                wrong += 1
    rc.close()
    say(f"  filler keys after the kill: {n - lost - wrong} of {n} kept")
    return [f"{lost} filler keys lost, {wrong} wrong, with a replica for each"] if lost or wrong else []


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


def lossy_zone(zone: str) -> int:
    """Notes the slots of every node in `zone`."""
    cluster = K8sCluster()
    nodes = [(i, l) for i, l in current(cluster).items() if field(l, "zone") == zone]
    slots: Set[int] = set()
    for i, _ in nodes:
        slots |= mc.slots_of(cluster, i)
    with open(ctl("lossy"), "a") as f:
        f.write(" ".join(map(str, sorted(slots))) + "\n")
    # The primaries elsewhere of the replicas here.
    ids = {i for i, _ in nodes}
    refusing: Set[int] = set()
    for _, l in nodes:
        if (p := field(l, "replica-of")) and p not in ids:
            refusing |= mc.slots_of(cluster, p)
    with open(ctl("refusing"), "a") as f:
        f.write(" ".join(map(str, sorted(refusing))) + "\n")
    primaries = [field(l, "name") for _, l in nodes if "replica-of" not in l]
    replicas = [field(l, "name") for _, l in nodes if "replica-of" in l]
    say(f"{zone}: primaries {', '.join(primaries) or 'none'}, replicas {', '.join(replicas) or 'none'}; "
        f"{len(slots)} slots noted as lossy, {len(refusing)} as refusing")
    return 0


def pairs() -> int:
    """The primaries and their replicas, with their zones: 1 if a replica
    is in its primary's zone."""
    lines = current(K8sCluster())
    bad = 0
    for i, l in sorted(lines.items(), key=lambda il: field(il[1], "name")):
        if "replica-of" in l:
            continue
        mine = [r for r in lines.values() if field(r, "replica-of") == i]
        shown = ", ".join(f"{field(r, 'name')} ({field(r, 'zone')})" for r in mine) or "no replica"
        say(f"{field(l, 'name')} ({field(l, 'zone')}, {field(l, 'slots')} slots): {shown}")
        bad += sum(field(r, "zone") == field(l, "zone") for r in mine)
    if bad:
        say(f"{bad} replicas in their primary's zone")
    return 1 if bad else 0


def primary() -> int:
    names = sorted(field(l, "name") for l in current(K8sCluster()).values()
                   if "replica-of" not in l and int(field(l, "slots") or 0))
    if not names:
        return 1
    say(names[0])
    return 0


def failover(zone: str) -> int:
    """Waits until no node in `zone` owns a slot, and every owner is up."""
    cluster = K8sCluster()
    t0 = time.monotonic()

    def served():
        lines = mc.stuart_lines(cluster).values()
        owners = [l for l in lines if "replica-of" not in l and int(field(l, "slots") or 0)]
        return sum(int(field(l, "slots")) for l in owners) == mc.SLOTS and \
            all(field(l, "zone") != zone and " up " in l for l in owners)
    mc.wait_for(f"the slots of {zone} served elsewhere", served, 120, 0.1)
    say(f"every slot served outside {zone} {time.monotonic() - t0:.1f} s after the call")
    return 0


def counts() -> int:
    """stuart's counters, from its /metrics."""
    import urllib.request
    with urllib.request.urlopen(f"http://{STUART}:9121/metrics", timeout=10) as r:
        body = r.read().decode()
    got = {}
    for name in ("slots_moved", "handovers", "demotions"):
        m = re.search(rf"^stuart_{name}_total (\S+)$", body, re.M)
        got[name] = int(float(m.group(1))) if m else -1
    say(" ".join(f"{k}={v}" for k, v in got.items()))
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
    sub.add_parser("lossy-zone").add_argument("zone")
    sub.add_parser("fill").add_argument("ratio", type=float)
    sub.add_parser("even").add_argument("nodes", type=int)
    sub.add_parser("pairs")
    sub.add_parser("primary")
    sub.add_parser("failover").add_argument("zone")
    sub.add_parser("counts")
    sub.add_parser("load")
    sub.add_parser("stop").add_argument("nodes", type=int)
    args = ap.parse_args(argv)
    if args.cmd == "run":
        return run(args)
    if args.cmd == "lossy":
        return lossy(args.pod)
    if args.cmd == "lossy-zone":
        return lossy_zone(args.zone)
    if args.cmd == "pairs":
        return pairs()
    if args.cmd == "primary":
        return primary()
    if args.cmd == "failover":
        return failover(args.zone)
    if args.cmd == "counts":
        return counts()
    if args.cmd == "fill":
        return fill(args.ratio)
    if args.cmd == "load":
        return load_summary(json.load(sys.stdin))
    if args.cmd == "even":
        t0 = time.monotonic()
        wait_laid_out(K8sCluster(), args.nodes)
        say(f"{args.nodes} nodes laid out after {time.monotonic() - t0:.1f} s")
        return 0
    with open(ctl("stop.tmp"), "w") as f:
        f.write(str(args.nodes))
    os.rename(ctl("stop.tmp"), ctl("stop"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
