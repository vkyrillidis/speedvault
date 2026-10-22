#!/usr/bin/env python3
"""The node's busy time and what serving metrics costs (phase 6
milestone 4; run by metrics_check.sh, which builds dbnode).

    python3 metrics_check.py --bin DIR [--port 7300] [-n 1000000]
        [--rounds 3] [--server-cpu 2] [--bench-cpus 4-7]

Busy time: a node with --busy-poll 0 and one with --busy-poll 20 each run
redis-benchmark's SET and GET (-P 1) with 50 clients and with one;
/metrics is read before and after. The busy time may never be more than
the process's CPU time. With --busy-poll 0 and 50 clients the thread
sleeps whenever it waits, rarely, so its busy time must match the CPU
time (within 10%; with one client it sleeps after every request, and
going to sleep and waking up takes CPU time that counts as waiting).
With --busy-poll 20 and one client the node spins while it waits for
the client: the CPU time is near the wall time (over 70%), the busy time
must stay below (under 80% of the CPU time). That is what an autoscaler
should go by. (Busy-polling also makes a request cheaper, without the
sleep and the wakeup: the busy time per request is lower than with
--busy-poll 0, as the table shows. Under WSL2 a request of a lone client
costs some 25 us: its reply's write wakes the client.)

Cost: SET and GET at -P 1 and -P 16 against a node without --metrics and
one with it, scraped every second, alternating, --rounds times each: the
medians must be within 5% (the busy count reads the clock once more per
loop turn at most).

Exits 1 if a check fails. Needs redis-benchmark; pins the node to
--server-cpu and redis-benchmark to --bench-cpus with taskset (if found).
"""

from __future__ import annotations

import argparse
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from typing import Dict, List

HOST = "127.0.0.1"


def scrape(port: int) -> Dict[str, float]:
    with urllib.request.urlopen(f"http://{HOST}:{port}/metrics", timeout=5) as r:
        body = r.read().decode()
    out = {}
    for line in body.splitlines():
        if line and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            out[name] = float(value)
    return out


class Node:
    def __init__(self, args, port: int, busy_poll: int, metrics: bool):
        self.port = port
        self.metrics = port + 1000 if metrics else None
        cmd = [f"{args.bin}/dbnode", f"{HOST}:{port}", "--busy-poll", str(busy_poll), "--loglevel", "warning"]
        if metrics:
            cmd += ["--metrics", f"{HOST}:{self.metrics}"]
        if args.taskset:
            cmd = ["taskset", "-c", args.server_cpu, *cmd]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL)
        deadline = time.time() + 10
        while subprocess.run(["redis-cli", "-p", str(port), "PING"], capture_output=True, text=True).stdout.strip() != "PONG":
            if time.time() > deadline:
                raise SystemExit(f"dbnode on {port} didn't start")
            time.sleep(0.05)

    def stop(self):
        self.proc.kill()
        self.proc.wait()


def benchmark(args, port: int, test: str, pipeline: int, clients: int = 50, n: int = 0) -> float:
    """redis-benchmark's requests/s."""
    cmd = ["redis-benchmark", "-h", HOST, "-p", str(port), "-t", test, "-n", str(n or args.n), "-c", str(clients),
           "-P", str(pipeline), "--csv"]
    if args.taskset:
        cmd = ["taskset", "-c", args.bench_cpus, *cmd]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    row = [l for l in out.splitlines() if l.startswith(f'"{test.upper()}"')][0]
    return float(re.findall(r'"([\d.]+)"', row)[0])


def busy_time(args) -> List[str]:
    problems: List[str] = []
    print("| busy-poll | load | requests | wall s | CPU s | busy s | busy / CPU | busy per request |")
    print("|---|---|---|---|---|---|---|---|")
    for busy_poll in (0, 20):
        for clients, n in ((50, args.n), (1, args.n // 10)):
            node = Node(args, args.port, busy_poll, metrics=True)
            try:
                before, t0 = scrape(node.metrics), time.monotonic()
                for test in ("set", "get"):
                    benchmark(args, node.port, test, 1, clients, n)
                after, wall = scrape(node.metrics), time.monotonic() - t0
            finally:
                node.stop()
            cpu = after["speedvault_cpu_seconds_total"] - before["speedvault_cpu_seconds_total"]
            busy = after["speedvault_busy_seconds_total"] - before["speedvault_busy_seconds_total"]
            requests = after["speedvault_commands_processed_total"] - before["speedvault_commands_processed_total"]
            print(f"| {busy_poll} us | {clients} clients | {requests:.0f} | {wall:.1f} | {cpu:.2f} | {busy:.2f} | "
                  f"{busy / cpu:.2f} | {busy / requests * 1e6:.2f} us |")
            what = f"--busy-poll {busy_poll}, {clients} clients"
            if busy > cpu * 1.02 + 0.02:
                problems.append(f"{what}: busy {busy:.2f} s is more than the CPU time, {cpu:.2f} s")
            if busy_poll == 0 and clients > 1 and busy < 0.9 * cpu:
                problems.append(f"{what}: busy {busy:.2f} s, CPU {cpu:.2f} s: more than 10% apart")
            # One client: the node mostly waits for it, spinning. The CPU
            # time says busy; the busy time must not.
            if busy_poll and clients == 1 and not (cpu > 0.7 * wall and busy < 0.8 * cpu):
                problems.append(f"{what}: CPU {cpu:.2f} s and busy {busy:.2f} s in {wall:.1f} s; want the CPU near the "
                                f"wall time and busy below it")
    return problems


class Scraper(threading.Thread):
    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.port, self.stopping, self.count = port, threading.Event(), 0

    def run(self):
        while not self.stopping.wait(1.0):
            scrape(self.port)
            self.count += 1


def cost(args) -> List[str]:
    problems: List[str] = []
    plain = Node(args, args.port, 20, metrics=False)
    scraped = Node(args, args.port + 1, 20, metrics=True)
    scraper = Scraper(scraped.metrics)
    scraper.start()
    try:
        print("\n| test | without --metrics | with, scraped every second | ratio |")
        print("|---|---|---|---|")
        for test in ("set", "get"):
            for pipeline in (1, 16):
                runs: Dict[str, List[float]] = {"plain": [], "scraped": []}
                for _ in range(args.rounds):
                    runs["plain"].append(benchmark(args, plain.port, test, pipeline))
                    runs["scraped"].append(benchmark(args, scraped.port, test, pipeline))
                a, b = statistics.median(runs["plain"]), statistics.median(runs["scraped"])
                print(f"| {test.upper()} -P {pipeline} | {a:,.0f} | {b:,.0f} | {b / a:.2f} |")
                if b < 0.95 * a:
                    problems.append(f"{test.upper()} -P {pipeline}: {b / a:.2f}x with metrics")
    finally:
        scraper.stopping.set()
        plain.stop()
        scraped.stop()
    print(f"\n{scraper.count} scrapes")
    return problems


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--port", type=int, default=7300)
    ap.add_argument("-n", type=int, default=1000000)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--server-cpu", default="2")
    ap.add_argument("--bench-cpus", default="4-7")
    ap.add_argument("--only", choices=["busy", "cost"])
    args = ap.parse_args(argv)
    args.taskset = shutil.which("taskset") is not None
    problems: List[str] = []
    if args.only != "cost":
        problems += busy_time(args)
    if args.only != "busy":
        problems += cost(args)
    for p in problems:
        print(f"FAIL: {p}")
    print("OK" if not problems else "FAILED")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
