#!/usr/bin/env python3
"""A primary leaving on SIGTERM, measured (phase 8 milestone 6 steps 4-5;
run by handover_bench.sh).

    python3 handover_bench.py PART... --bin DIR [--base-port 7920] [--filler 1000000] [--runs 3] [--out FILE]

Each part starts stuart --auto --replicas 1 and six nodes (three primaries
with a replica each, as migrate_check's failover), --filler keys of 100 B
(f:<i>, straight to their nodes), and two redis-py RedisCluster clients
INCRing keys one at a time: one on keys of the leaving primary's slots
(its latencies show the pause a client sees: the writes that waited, and
the -MOVED and slot map refresh after), one on keys of random slots.

handover   a primary (not the first node) gets SIGTERM: its writes pause
           (CLUSTER PAUSEWRITES), its replica catches up and takes its
           slots. Measured: the pause at the node (its verbose log's
           "Writes resumed after X us"), the client's worst INCR on its
           slots, the time from the SIGTERM to the table giving the
           replica its slots (STUART NODES, polled every 5 ms) and to the
           node's exit (after --leave-linger, 10 s). The node is started
           again (it joins as its old replica's replica) and, once synced,
           the next primary gets SIGTERM: --runs handovers.
drain      the same node drained instead: its replica gets SIGTERM first
           (and exits), then it: its slots move by snapshot to the other
           two primaries. The time from its SIGTERM to the table leaving it
           no slot, the cutover pauses (the sources' logs), the client's
           worst INCR. A new cluster for each of --runs.
rolling    each node in turn gets SIGTERM and is started again under its
           name (--name) once it exited, the next once it's a synced
           replica again, as Kubernetes rolls a StatefulSet: the total
           time, the slots moved (stuart_slots_moved_total: 0), the
           handovers, the full and partial syncs the primaries served
           (INFO stats sync_full / sync_partial_ok), the clients' worst
           INCR and errors.
scale-down (step 5) 6 nodes to 4, as migrate_check scale-down: two
           replicas of different primaries get SIGTERM, the second once
           the first exited; one primary of the three is demoted (its
           slots move by snapshot, it becomes the replica of the one
           lacking one). The slots moved, the time from the second SIGTERM
           to the demotion's start, its drain, to every primary having a
           synced replica, the cutover pauses, the client's p99 before
           and during the drain, the full syncs. A new cluster for each
           of --runs.

Prints Markdown tables (and writes them to --out). Times are monotonic.
"""

from __future__ import annotations

import argparse
import glob
import os
import random
import re
import statistics
import sys
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compat"))
import migrate_check as mc  # noqa: E402
from migrate_check import Cluster, key_slot, text, wait_for  # noqa: E402
from redis.cluster import ClusterNode, RedisCluster  # noqa: E402

HOST = "127.0.0.1"


def percentile(values: List[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * p))]


class Writer:
    """A RedisCluster client INCRing keys of `slots` (any, if None) one at
    a time: each INCR's latency and when it ended, and the errors."""

    def __init__(self, port: int, slots: Optional[List[int]] = None):
        self.port = port
        self.slots = slots
        self.done: List[Tuple[float, float, int]] = []  # (end, latency) s, monotonic; the key's slot
        self.errors: List[str] = []
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def keys(self) -> List[str]:
        out, i = [], 0
        wanted = set(self.slots or ())
        while len(out) < 1000:
            k = f"h:{i}"
            i += 1
            if self.slots is None or key_slot(k.encode()) in wanted:
                out.append(k)
        return out

    def run(self):
        rc = RedisCluster(startup_nodes=[ClusterNode(HOST, self.port)], socket_timeout=10)
        keys = [(k, key_slot(k.encode())) for k in self.keys()]
        rnd = random.Random(1)
        while not self.stopping.is_set():
            k, slot = keys[rnd.randrange(len(keys))]
            t0 = time.monotonic()
            try:
                rc.incr(k)
            except Exception as e:  # noqa: BLE001 - counted
                self.errors.append(f"{type(e).__name__}: {e}")
                continue
            t1 = time.monotonic()
            self.done.append((t1, t1 - t0, slot))
        rc.close()

    def latencies(self, since: float, until: float = float("inf"), slots: Optional[Set[int]] = None) -> List[float]:
        """The latencies (ms) of the INCRs that ended in [since, until], on
        keys of `slots` (any, if None)."""
        return [l * 1000 for t, l, s in self.done if since <= t <= until and (slots is None or s in slots)]

    def worst(self, since: float, until: float = float("inf"), slots: Optional[Set[int]] = None) -> float:
        """The worst latency (ms) of the INCRs that ended in [since, until]."""
        return max(self.latencies(since, until, slots), default=0.0)

    def stop(self):
        self.stopping.set()
        self.thread.join(30)


def start(args) -> Tuple[Cluster, List[int]]:
    """The six nodes, verbose (the pauses come from their logs), named, and
    stuart serving --metrics; filled."""
    a = argparse.Namespace(bin=args.bin, base_port=args.base_port, filler=args.filler, keep=False,
                           migrate_args=["--loglevel", "verbose"])
    return mc.setup_replicas(a, ["--metrics", f"{HOST}:{mc.metrics_port(args.base_port)}"], named=True)


def counters(args) -> Tuple[float, float]:
    m = mc.scrape(mc.metrics_port(args.base_port))
    return mc.one(m, "stuart_slots_moved_total"), mc.one(m, "stuart_handovers_total")


def node_id(c: Cluster, port: int) -> str:
    return text(c.node(port).execute_command("CLUSTER", "MYID"))


def slots(c: Cluster, nid: str) -> int:
    return mc.slot_count(c, nid)


def log_pauses(path: str, since: int, handover: bool) -> List[Tuple[float, str]]:
    """(ms, how) of the pauses logged in `path` past byte `since`: of every
    write (handover) or of slots moving (cutovers)."""
    pat = r"\bWrites resumed after (\d+) us \((\w+)\)" if handover else r"Writes to \d+ slots moving to \S+ resumed after (\d+) us \((\w+)\)"
    out = []
    with open(path, "rb") as f:
        f.seek(since)
        for line in f.read().decode(errors="replace").splitlines():
            m = re.search(pat, line)
            if m:
                out.append((int(m.group(1)) / 1000, m.group(2)))
    return out


def log_sizes(c: Cluster) -> Dict[str, int]:
    return {f: os.path.getsize(f) for f in glob.glob(os.path.join(c.dir, "*.log"))}


def sigterm(c: Cluster, port: int, nid: str) -> Tuple[float, float]:
    """SIGTERM to `port`: seconds until stuart's table leaves it no slot,
    and until it exits (status 0)."""
    t0 = time.monotonic()
    c.term_node(port)
    wait_for("its slots to go", lambda: slots(c, nid) == 0 if any(l.startswith(nid + " ") for l in c.stuart_nodes()) else True, 300, 0.005)
    off = time.monotonic() - t0
    status = c.wait_exit(port, 300)
    if status:
        raise mc.Fail(f"node {port} exited with status {status}")
    return off, time.monotonic() - t0


class Syncs:
    """The full and partial syncs the nodes served (INFO stats), kept for
    the nodes gone: read() before a node goes counts it."""

    def __init__(self, c: Cluster):
        self.c = c
        self.seen: Dict[int, Tuple[int, int]] = {}  # pid -> (sync_full, sync_partial_ok)

    def read(self) -> Tuple[int, int]:
        for port, proc in self.c.nodes.items():
            try:
                st = self.c.node(port).info("stats")
                self.seen[proc.pid] = (int(st["sync_full"]), int(st["sync_partial_ok"]))
            except Exception:  # noqa: BLE001 - gone
                pass
        return tuple(sum(v[i] for v in self.seen.values()) for i in (0, 1))


def ms(values: List[float]) -> str:
    """p50 / max."""
    return f"{statistics.median(values):.1f} / {max(values):.1f}" if values else "-"


def part_handover(args) -> List[str]:
    c, ports = start(args)
    rows = []
    try:
        for run in range(args.runs):
            # A primary not SIGTERMed yet if any; not the first node, as in
            # migrate_check.
            prim = mc.primaries(c)
            done = [r[0] for r in rows]
            victim = min((p for p in prim if p != ports[0]), key=lambda p: (p in done, p))
            nid = prim[victim]
            mine = mc.slots_of(c, nid)
            here, anywhere = Writer(args.base_port, sorted(mine)), Writer(args.base_port)
            time.sleep(2)
            log = os.path.join(c.dir, f"node-{victim}.log")
            mark = os.path.getsize(log)
            t0 = time.monotonic()
            off, exited = sigterm(c, victim, nid)
            time.sleep(1)
            here.stop()
            anywhere.stop()
            node = log_pauses(log, mark, True)
            table = [p for p, how in node if how == "table"]
            rows.append((victim, table[-1] if table else None, len(node) - len(table), here.worst(t0), anywhere.worst(t0),
                         off * 1000, exited, len(here.errors) + len(anywhere.errors)))
            print(f"  handover {run + 1}: node {victim}: paused {rows[-1][1]} ms ({rows[-1][2]} missed), client worst "
                  f"{rows[-1][3]:.1f} ms, slots off after {off * 1000:.0f} ms, exit after {exited:.1f} s", flush=True)
            c.start_node(victim, mc.pod_name(victim))
            wait_for("it to be a synced replica again", lambda: mc.synced(c, victim), 120, 0.05)
    finally:
        c.stop(False)
    out = ["### handover", "",
           f"{args.runs} handovers, {args.filler} keys of 100 B. Pause at the node: CLUSTER PAUSEWRITES to the table that "
           "hands the slots over (the node's log). Client: the worst INCR, one at a time, on keys of the leaving primary's "
           "slots / of any slot, from the SIGTERM on. Slots off: SIGTERM to the table giving them to the replica (STUART "
           "NODES, polled every 5 ms); exit: after --leave-linger (10 s).", "",
           "| run | pause at the node ms | missed pauses | client worst ms (its slots / any) | slots off ms | exit s | client errors |",
           "|---|---|---|---|---|---|---|"]
    for i, (_, pause, missed, w1, w2, off, ex, err) in enumerate(rows):
        out.append(f"| {i + 1} | {pause if pause is None else f'{pause:.2f}'} | {missed} | {w1:.1f} / {w2:.1f} | {off:.0f} | {ex:.1f} | {err} |")
    pauses = [r[1] for r in rows if r[1] is not None]
    out += ["", f"p50 / max: pause at the node {ms(pauses)} ms; client worst on its slots {ms([r[3] for r in rows])} ms; "
                f"slots off {ms([r[5] for r in rows])} ms.", ""]
    return out


def part_drain(args) -> List[str]:
    rows = []
    for run in range(args.runs):
        c, ports = start(args)
        try:
            prim = mc.primaries(c)
            victim = next(p for p in sorted(prim) if p != ports[0])
            nid = prim[victim]
            replica = next(r for r, p in mc.replica_ports(c).items() if p == victim)
            sigterm(c, replica, node_id(c, replica))
            mine = mc.slots_of(c, nid)
            here, anywhere = Writer(args.base_port, sorted(mine)), Writer(args.base_port)
            time.sleep(2)
            marks = log_sizes(c)
            moved0, _ = counters(args)
            t0 = time.monotonic()
            off, exited = sigterm(c, victim, nid)
            time.sleep(1)
            here.stop()
            anywhere.stop()
            moved1, _ = counters(args)
            cut = [p for f, at in marks.items() for p, how in log_pauses(f, at, False) if how == "table"]
            rows.append((len(mine), moved1 - moved0, ms(cut), len(cut), here.worst(t0), anywhere.worst(t0), off * 1000, exited,
                         len(here.errors) + len(anywhere.errors)))
            print(f"  drain {run + 1}: node {victim}: {len(mine)} slots off after {off * 1000:.0f} ms, {len(cut)} cutovers "
                  f"({ms(cut)} ms), client worst {rows[-1][4]:.1f} ms", flush=True)
        finally:
            c.stop(False)
    out = ["### drain of the same node", "",
           f"{args.runs} drains (a new cluster each), {args.filler} keys of 100 B: the primary's replica leaves first, then "
           "the primary gets SIGTERM and its slots move by snapshot to the other two. Cutover pauses p50 / max from the "
           "sources' logs.", "",
           "| run | slots | slots moved | cutover pause ms | cutovers | client worst ms (its slots / any) | slots off ms | exit s | client errors |",
           "|---|---|---|---|---|---|---|---|---|"]
    for i, (n, moved, cut, cuts, w1, w2, off, ex, err) in enumerate(rows):
        out.append(f"| {i + 1} | {n} | {moved:.0f} | {cut} | {cuts} | {w1:.1f} / {w2:.1f} | {off:.0f} | {ex:.1f} | {err} |")
    out += ["", f"p50 / max: client worst on its slots {ms([r[4] for r in rows])} ms; slots off {ms([r[6] for r in rows])} ms.", ""]
    return out


def part_rolling(args) -> List[str]:
    c, ports = start(args)
    try:
        read_syncs = Syncs(c).read
        full0, partial0 = read_syncs()
        moved0, handed0 = counters(args)
        anywhere = Writer(args.base_port)
        time.sleep(2)
        steps = []
        t0 = time.monotonic()
        for port in ports:
            role = "primary" if port in mc.primaries(c) else "replica"
            read_syncs()
            t1 = time.monotonic()
            off, exited = sigterm(c, port, node_id(c, port))
            c.start_node(port, mc.pod_name(port))
            wait_for("it to be a synced replica again", lambda: mc.synced(c, port), 120, 0.05)
            steps.append((port, role, off * 1000, time.monotonic() - t1, anywhere.worst(t1)))
            print(f"  {role} {port}: slots off after {off * 1000:.0f} ms, back and synced after {steps[-1][3]:.1f} s, "
                  f"client worst {steps[-1][4]:.1f} ms", flush=True)
        total = time.monotonic() - t0
        time.sleep(1)
        anywhere.stop()
        full1, partial1 = read_syncs()
        moved1, handed1 = counters(args)
    finally:
        c.stop(False)
    out = ["### rolling restart", "",
           f"Six nodes, {args.filler} keys of 100 B; each SIGTERMed, started again under its name once it exited (after "
           "--leave-linger, 10 s), the next once it's a synced replica again. A client INCRs keys of any slot throughout.", "",
           "| node | role | slots off ms | back, synced s | client worst ms |", "|---|---|---|---|---|"]
    for port, role, off, back, worst in steps:
        out.append(f"| {port} | {role} | {off:.0f} | {back:.1f} | {worst:.1f} |")
    out += ["", f"Total {total:.1f} s; slots moved {moved1 - moved0:.0f}; handovers {handed1 - handed0:.0f}; "
                f"full syncs {full1 - full0}, partial syncs {partial1 - partial0}; client errors {len(anywhere.errors)}"
                + (f" ({anywhere.errors[:3]})" if anywhere.errors else "") + ".", ""]
    return out


def part_scale_down(args) -> List[str]:
    rows = []
    stuart_metrics = mc.metrics_port(args.base_port)
    for run in range(args.runs):
        c, ports = start(args)
        try:
            syncs = Syncs(c)
            full0, _ = syncs.read()
            moved0, _ = counters(args)
            demoted0 = mc.one(mc.scrape(stuart_metrics), "stuart_demotions_total")
            # As migrate_check scale-down: two replicas of different
            # primaries, the highest first, the next once the first exited.
            replicas = mc.replica_ports(c)
            victims = sorted((r for r, p in replicas.items() if r != ports[0] and p != ports[0]), reverse=True)[:2]
            anywhere = Writer(args.base_port)
            time.sleep(2)
            before = time.monotonic()
            c.term_node(victims[0])
            if c.wait_exit(victims[0], 300):
                raise mc.Fail(f"node {victims[0]} didn't exit with status 0")
            syncs.read()
            marks = log_sizes(c)
            t0 = time.monotonic()
            c.term_node(victims[1])
            nid = wait_for("a primary to be demoted",
                           lambda: next((i for i, l in mc.stuart_lines(c).items() if " demoting" in l), None), 60, 0.005)
            t_demote = time.monotonic()
            mine = mc.slots_of(c, nid)
            wait_for("its slots to go", lambda: slots(c, nid) == 0, 300, 0.005)
            t_empty = time.monotonic()
            wait_for("it to be a replica", lambda: " replica-of=" in mc.stuart_lines(c)[nid], 60, 0.005)
            t_replica = time.monotonic()
            port = mc.port_of(c, nid)

            def all_synced():
                reps = mc.replica_ports(c)
                prims = [l for l in mc.stuart_lines(c).values() if "replica-of" not in l]
                return len(prims) == 2 == len(set(reps.values())) and all(mc.synced(c, r) for r in reps)
            wait_for("every primary to have a synced replica", all_synced, 300, 0.01)
            t_synced = time.monotonic()
            if c.wait_exit(victims[1], 300):
                raise mc.Fail(f"node {victims[1]} didn't exit with status 0")
            time.sleep(1)
            anywhere.stop()
            full1, _ = syncs.read()
            moved1, _ = counters(args)
            demoted = mc.one(mc.scrape(stuart_metrics), "stuart_demotions_total") - demoted0
            cut = [p for f, at in marks.items() for p, how in log_pauses(f, at, False) if how == "table"]
            base = anywhere.latencies(before - 2, before)
            during, its = anywhere.latencies(t_demote, t_empty), anywhere.latencies(t_demote, t_empty, mine)
            rows.append(dict(port=port, slots=len(mine), moved=moved1 - moved0, demotions=demoted, start=t_demote - t0,
                             drain=t_empty - t_demote, replica=t_replica - t0, synced=t_synced - t0, first=t_synced - before, cut=cut,
                             base=percentile(base, 0.99), p99=percentile(during, 0.99), p99_its=percentile(its, 0.99),
                             worst=max(during), worst_its=max(its), full=full1 - full0, errors=len(anywhere.errors)))
            r = rows[-1]
            print(f"  scale-down {run + 1}: node {port} demoted ({r['slots']} slots) {r['start'] * 1000:.0f} ms after the "
                  f"SIGTERM, drained in {r['drain']:.2f} s, a replica after {r['replica']:.2f} s, every primary with a synced "
                  f"replica after {r['synced']:.2f} s; p99 {r['base']:.2f} -> {r['p99']:.2f} ms, worst {r['worst']:.1f} ms",
                  flush=True)
        finally:
            c.stop(False)
    out = ["### scale-down", "",
           f"{args.runs} scale-downs from 6 nodes to 4 (a new cluster each), {args.filler} keys of 100 B: two replicas of "
           "different primaries get SIGTERM, the second once the first exited (as migrate_check scale-down), leaving three "
           "primaries for four nodes: one is demoted, its slots move by snapshot to the other two and it becomes the "
           "replica of the one lacking one. Times from the second SIGTERM: to the demotion's start (STUART NODES "
           "`demoting`, polled every 5 ms), its drain (to the table leaving it no slot), to it being a replica, to every "
           "primary having a synced replica (link up, listed online by its primary), and that from the first SIGTERM (one primary has no replica from then on). A client INCRs keys of any slot one "
           "at a time: p99 in the 2 s before the first SIGTERM and during the drain (on any slot / on the demoted "
           "primary's), worst during the drain. Full syncs: those the nodes served from the first SIGTERM on.", "",
           "| run | slots | slots moved | demotions | start ms | drain s | a replica s | all synced s (from the first) | cutover pause ms | "
           "cutovers | p99 before ms | p99 drain ms (any / its slots) | worst drain ms (any / its slots) | full syncs | client errors |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for i, r in enumerate(rows):
        out.append(f"| {i + 1} | {r['slots']} | {r['moved']:.0f} | {r['demotions']:.0f} | {r['start'] * 1000:.0f} | "
                   f"{r['drain']:.2f} | {r['replica']:.2f} | {r['synced']:.2f} ({r['first']:.1f}) | {ms(r['cut'])} | {len(r['cut'])} | "
                   f"{r['base']:.2f} | {r['p99']:.2f} / {r['p99_its']:.2f} | {r['worst']:.1f} / {r['worst_its']:.1f} | "
                   f"{r['full']} | {r['errors']} |")
    out += ["", f"p50 / max: drain {ms([r['drain'] for r in rows])} s; all synced {ms([r['synced'] for r in rows])} s; "
                f"p99 during the drain {ms([r['p99'] for r in rows])} ms (before: {ms([r['base'] for r in rows])}).", ""]
    return out


PARTS = {"handover": part_handover, "drain": part_drain, "rolling": part_rolling, "scale-down": part_scale_down}


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parts", nargs="*", choices=[*PARTS, "all"], default=["all"])
    ap.add_argument("--bin", required=True)
    ap.add_argument("--base-port", type=int, default=7920)
    ap.add_argument("--filler", type=int, default=1000000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    mc.COLLECTION = 0
    parts = list(PARTS) if "all" in args.parts else args.parts
    out = ["# SIGTERM handover", ""]
    for p in parts:
        print(f"{p}:", flush=True)
        out += PARTS[p](args)
    report = "\n".join(out)
    print(report)
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="\n") as f:
            f.write(report + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
