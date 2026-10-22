#!/usr/bin/env python3
"""redis-benchmark against 3-node clusters, two ways (run by cluster_bench.sh).

    cluster_bench.py --cluster NAME=P1,P2,P3 [--cluster ...] [-n N] [-P "1 16 64"]
                     [--tests set,get,incr,mset] [--client-cpus 8,10,12] [-o FILE] [--raw DIR]

per node: one redis-benchmark per node, at the same time, each on its own
  CPU (--client-cpus) with -c 50, each using keys of its node's slots (a
  hash tag the node owns, as redis-benchmark --cluster tags its keys); the
  cluster's requests/s is the sum. One client thread tops out near 400k
  requests/s, so one process can't load three nodes.
--cluster: redis-benchmark --cluster --threads 3 -c 150 on all three
  client CPUs, the way cluster numbers are usually reported.

Both use -r 100000 (random keys) and -d 3. -n N per node at -P 1, 4N with
pipelining (more requests per second); --cluster gets 3 times that. The
first cluster is the reference of the ratios. As bench.sh does, when the
WSL2 wall clock steps (see clockguard.py) each run starts right after a
step and is rerun (up to 4 tries) if the clock moved during it.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import io
import os
import platform
import subprocess
import sys
from typing import Dict, List, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
STEP_TOLERANCE_MS = 5.0


def clock(*args: str) -> str:
    return subprocess.run([sys.executable, os.path.join(HERE, "clockguard.py"), *args],
                          capture_output=True, text=True, check=True).stdout.strip()


def cli(port: int, *args: str) -> str:
    return subprocess.run(["redis-cli", "-p", str(port), *args], capture_output=True, text=True, check=True).stdout.strip()


def tags(ports: List[int]) -> List[str]:
    """Per node, a hash tag `{nK}` whose slot the node owns."""
    owned = []
    for p in ports:
        lines = cli(p, "CLUSTER", "NODES").splitlines()
        mine = next(l for l in lines if "myself" in l).split()[8:]
        ranges = [tuple(map(int, r.split("-"))) if "-" in r else (int(r), int(r)) for r in mine]
        owned.append(ranges)
    found: List[str] = [""] * len(ports)
    k = 0
    while not all(found):
        tag = f"{{n{k}}}"
        slot = int(cli(ports[0], "CLUSTER", "KEYSLOT", tag))
        for i, ranges in enumerate(owned):
            if not found[i] and any(s <= slot <= e for s, e in ranges):
                found[i] = tag
        k += 1
    return found


def command(test: str, tag: str) -> List[str]:
    # The key names of redis-benchmark -t (counters apart from the
    # strings), with the node's tag, as --cluster makes them.
    key, counter = f"key:{tag}:__rand_int__", f"counter:{tag}:__rand_int__"
    return {
        "set": ["SET", key, "xxx"],
        "get": ["GET", key],
        "incr": ["INCR", counter],
        "mset": ["MSET"] + [key, "xxx"] * 10,
    }[test]


def parse_csv(text: str) -> Tuple[float, str]:
    # --cluster prints the masters it found before the CSV.
    text = text[text.find('"test"'):] if '"test"' in text else text
    rows = [r for r in csv.DictReader(io.StringIO(text)) if r.get("rps")]
    if not rows:
        raise RuntimeError(f"no result in {text!r}")
    return float(rows[0]["rps"]), rows[0].get("p50_latency_ms", "")


def guarded(stepping: bool, run):
    """Runs `run()` between clock steps; returns its result and the tries."""
    for attempt in range(1, 5 if stepping else 2):
        before = float(clock("wait", "12") if stepping else clock("offset"))
        result = run()
        moved = abs(float(clock("offset")) - before)
        if moved <= STEP_TOLERANCE_MS:
            return result, attempt, False
    return result, attempt, True


def per_node(ports: List[int], node_tags: List[str], test: str, pipe: int, n: int, cpus: List[str], raw: str, name: str):
    procs = []
    for port, tag, cpu in zip(ports, node_tags, cpus):
        args = ["taskset", "-c", cpu, "redis-benchmark", "-p", str(port), "-n", str(n), "-c", "50", "-P", str(pipe),
                "-r", "100000", "--csv"] + command(test, tag)
        procs.append(subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
    outs = [p.communicate(timeout=900) for p in procs]
    for i, (out, err) in enumerate(outs):
        with open(os.path.join(raw, f"{name}.node{i}.{test}.P{pipe}.csv"), "a") as f:
            f.write(out + err)
    results = [parse_csv(out) for out, _ in outs]
    return sum(r for r, _ in results), [r for r, _ in results], [p for _, p in results]


def cluster_mode(ports: List[int], test: str, pipe: int, n: int, cpus: List[str], raw: str, name: str):
    args = ["taskset", "-c", ",".join(cpus), "redis-benchmark", "--cluster", "-p", str(ports[0]), "--threads", "3",
            "-n", str(3 * n), "-c", "150", "-P", str(pipe), "-r", "100000", "-d", "3", "-t", test, "--csv"]
    out = subprocess.run(args, capture_output=True, text=True, timeout=900)
    with open(os.path.join(raw, f"{name}.cluster.{test}.P{pipe}.csv"), "a") as f:
        f.write(out.stdout + out.stderr)
    rps, p50 = parse_csv(out.stdout)
    return rps, p50


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cluster", action="append", required=True, help="NAME=PORT,PORT,PORT")
    ap.add_argument("-n", type=int, default=1_000_000)
    ap.add_argument("-P", default="1 16 64")
    ap.add_argument("--tests", default="set,get,incr,mset")
    ap.add_argument("--client-cpus", default="8,10,12")
    ap.add_argument("--node-cpus", default="2,4,6", help="for the report only")
    ap.add_argument("-o", help="also write the markdown here")
    ap.add_argument("--raw", default="/tmp/cluster_bench_raw")
    args = ap.parse_args(argv)
    clusters = [(c.split("=")[0], [int(p) for p in c.split("=")[1].split(",")]) for c in args.cluster]
    pipes = [int(p) for p in args.P.split()]
    tests = args.tests.split(",")
    cpus = args.client_cpus.split(",")
    os.makedirs(args.raw, exist_ok=True)
    stepping = int(clock("watch", "7")) > 0
    if stepping:
        print("warning: the wall clock steps; each run starts right after a step", file=sys.stderr)

    a: Dict[Tuple[str, str, int], Tuple[float, List[float], List[str], bool]] = {}
    b: Dict[Tuple[str, str, int], Tuple[float, str, bool]] = {}
    reruns = 0
    for name, ports in clusters:
        node_tags = tags(ports)
        for test in tests:
            for pipe in pipes:
                n = args.n if pipe == 1 else 4 * args.n
                (total, each, p50s), tries, moved = guarded(stepping, lambda: per_node(ports, node_tags, test, pipe, n, cpus, args.raw, name))
                reruns += tries - 1
                a[(name, test, pipe)] = (total, each, p50s, moved)
                (rps, p50), tries, moved_b = guarded(stepping, lambda: cluster_mode(ports, test, pipe, n, cpus, args.raw, name))
                reruns += tries - 1
                b[(name, test, pipe)] = (rps, p50, moved_b)
                print(f"{name:14} {test:5} -P {pipe:<3} per node: {total:>12,.0f} ({', '.join(f'{x:,.0f}' for x in each)})"
                      f"   --cluster: {rps:>12,.0f}", file=sys.stderr, flush=True)

    names = [n for n, _ in clusters]
    cpu = subprocess.run("lscpu | sed -n 's/^Model name: *//p'", shell=True, capture_output=True, text=True).stdout.strip()
    kernel = platform.release()
    wsl = " (WSL2)" if "microsoft" in kernel.lower() else ""
    bench = subprocess.run(["redis-benchmark", "--version"], capture_output=True, text=True).stdout.strip()
    lines = [f"# 3-node clusters: {', '.join(names)}", "",
             f"- Date: {datetime.date.today()}",
             f"- Machine: {cpu}{wsl}, {os.cpu_count()} CPUs, kernel {kernel}",
             f"- Nodes pinned to CPUs {args.node_cpus} (one core each); clients on CPUs {args.client_cpus}",
             f"- Client: {bench}, `-r 100000 -d 3`; -n {args.n:,} per node at -P 1, {4 * args.n:,} with pipelining",
             "- Per node: one redis-benchmark per node at once, `-c 50`, keys of that node's slots (a hash tag it "
             "owns); requests/s summed",
             "- `--cluster`: `redis-benchmark --cluster --threads 3 -c 150`, 3 times the requests",
             f"- Clock: {'the wall clock stepped; runs started after a step, ' + str(reruns) + ' rerun(s)' if stepping else 'steady'}"
             "; † = the clock moved in every try (not corrected)", ""]

    def table(title: str, value):
        lines.extend([f"## {title}", "", "Requests/s, ratio to " + names[0] + ":", "",
                      "| test | -P | " + " | ".join(names) + " |", "|---|---:|" + "---:|" * len(names)])
        for test in tests:
            for pipe in pipes:
                base = value(names[0], test, pipe)[0]
                cells = []
                for nm in names:
                    v, mark = value(nm, test, pipe)
                    cells.append(f"{v:,.0f}{' †' if mark else ''}" + (f" ({v / base:.2f}x)" if nm != names[0] else ""))
                lines.append(f"| {test.upper()} | {pipe} | " + " | ".join(cells) + " |")
        lines.append("")

    table("Per node (3 redis-benchmark processes, summed)", lambda nm, t, p: (a[(nm, t, p)][0], a[(nm, t, p)][3]))
    table("redis-benchmark --cluster --threads 3", lambda nm, t, p: (b[(nm, t, p)][0], b[(nm, t, p)][2]))
    lines.extend(["## Per node, each node's share", "", "| cluster | test | -P | node 1 | node 2 | node 3 | p50 ms |", "|---|---|---:|---:|---:|---:|---|"])
    for nm in names:
        for test in tests:
            for pipe in pipes:
                total, each, p50s, _ = a[(nm, test, pipe)]
                lines.append(f"| {nm} | {test.upper()} | {pipe} | " + " | ".join(f"{x:,.0f}" for x in each) + f" | {' / '.join(p50s)} |")
    text = "\n".join(lines) + "\n"
    print(text)
    if args.o:
        with open(args.o, "w") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
