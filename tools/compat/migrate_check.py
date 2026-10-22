#!/usr/bin/env python3
"""Slots moving under load (phase 5 milestone 6; run by migrate_check.sh,
which builds the binaries).

    python3 migrate_check.py SCENARIO... --bin DIR [--base-port 7880]
        [--keys 2000] [--filler 200000] [--lettuce CLASSPATH]

Each scenario starts stuart and three dbnodes (four for drain), CREATE,
fills the cluster with --filler keys (f:<i>, values of 100 bytes, written
straight to their nodes) so that moves take a few seconds, and starts the
writers: two redis-py RedisCluster threads, one redis-cli -c process fed
on stdin, and with --lettuce one LettuceLoad per refresh mode given
(lettuce/build.sh prints the class path). They INCR the counters k:0 ..
k:<keys-1> round and round, seeded with stuart (Lettuce with the first
node: it can't take stuart as a seed), and note for every
INCR whether it was acknowledged or failed (in doubt: applied or not).
Then the scenario moves slots:

  rebalance    a fourth node joins, STUART REBALANCE gives it 4096 slots
  drain        four nodes, STUART DRAIN empties the first
  stuart-kill  rebalance, stuart SIGKILLed mid-move and restarted from its
               state file
  source-kill  rebalance, a source SIGKILLed mid-move and restarted at the
               same address a second later (a new id: its slots are lost;
               the one it was sending goes to the target)
  target-kill  rebalance, the new node SIGKILLed mid-move, down for three
               seconds, restarted at the same address (the moves to it end
               with the sources)
  drain-target-kill  drain, one of the targets SIGKILLed mid-move, down for
               five seconds, restarted (the moves to it are dropped: those
               slots stay with the drained node); meanwhile the moves to the
               other targets must go on
  auto-grow    stuart --auto and one node, which gets every slot; three more
               join half a second apart, while slots move: nobody types a
               command, and the four end with 4096 slots each
  leave-while-join  stuart --auto and four nodes; the first leaves (STUART
               LEAVE) as a fifth joins: the first ends removed and empty
               (it still answers, with -MOVED), the other four even
  sigterm      stuart --auto and four nodes; the first gets SIGTERM: it
               leaves the cluster and exits by itself (status 0), the other
               three even
  sigterm-two  the same with SIGTERM to two nodes at once
  sigterm-target  stuart --auto and three nodes; a fourth joins, and gets
               SIGTERM while slots move to it
  full-target  stuart --auto and three nodes; a fourth joins with a
               --maxmemory too small for its share: it refuses keys past its
               limit (CLUSTER IMPORT -OOM) and is full at 90%, so the moves
               to it stop, the others keep their slots, writes to its keys
               get -OOM (the only failures allowed); its used_memory stays
               within its limit. Then its limit goes (CONFIG SET maxmemory
               0): the moves resume and the four end even
  metrics      rebalance with stuart and every node serving --metrics
               (on their port + 1000), scraped every 100 ms throughout: each
               scrape must parse with prometheus_client's parser, no counter
               may go down, and once settled the metrics must agree with
               STUART NODES, INFO and DBSIZE (slots, epochs, keys, the
               migrate counters: 4096 slots moved)
  redirects    for each client alone (redis-py, redis-cli, Lettuce in each
               mode given): the -MOVED/-ASK/-TRYAGAIN replies the nodes
               send it (INFO cluster) before, during and in 2 s windows
               after a rebalance; redis-py and Lettuce must get none once
               their slot map is refreshed

With --collection N (default 20000), each hot slot (below) also holds a
hash, a set and a list of N elements, so that they move in parts
(--migrate-chunk), and one more redis-py writer works on them throughout:
HINCRBY on four counter fields of each hash, SADD and RPUSH of members
numbered in the order sent. At the end every element must be there, in
order for the lists: the original ones, every acknowledged SADD and RPUSH
(one in doubt may be there or not, an RPUSH retried twice), each counter
field within its INCRs as above.

Once nothing moves and the writers stopped, every node is read directly
(GETKEYSINSLOT for every slot): no key may be at two nodes or at a node
whose table doesn't give it its slot; every filler key must be there with
its value; each counter must be at least its acknowledged INCRs and at
most those plus the ones in doubt (redis-py may have retried those a few
times: up to 16 each). Without a node killed, no INCR may fail and every
count must be exact. With one, keys of the slots it owned or was taking
may be lost (each counter's value is then only bounded above).

Exits 1 on any failure. Needs redis-cli and redis-py (pip, or
REDIS_PY_PATH: default ~/venvs/redis-py/site); metrics also needs
prometheus_client there.
"""

from __future__ import annotations

import argparse
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from typing import Dict, List, Optional, Set, Tuple


def redis_py():
    path = os.environ.get("REDIS_PY_PATH", os.path.expanduser("~/venvs/redis-py/site"))
    if os.path.isdir(path):
        sys.path.insert(0, path)
    import redis  # noqa: E402
    return redis


redis = redis_py()
from redis.cluster import RedisCluster  # noqa: E402
from redis.crc import key_slot  # noqa: E402

HOST = "127.0.0.1"
SLOTS = 16384
# How many times redis-py may run one INCR that it reports as failed.
RETRIES = 16


class Fail(Exception):
    pass


def wait_for(what: str, cond, timeout: float = 20.0, every: float = 0.05):
    deadline = time.time() + timeout
    while True:
        try:
            got = cond()
            if got:
                return got
        except Exception:  # noqa: BLE001 - not yet
            pass
        if time.time() > deadline:
            raise Fail(f"timed out waiting for {what}")
        time.sleep(every)


def text(reply) -> str:
    return reply.decode() if isinstance(reply, bytes) else str(reply or "")


class Cluster:
    """stuart on the base port and nodes on the next ones, as processes."""

    def __init__(self, bin_dir: str, base: int, migrate_args: List[str], stuart_args: Optional[List[str]] = None,
                 cpus: Optional[Dict[int, int]] = None):
        self.bin = bin_dir
        self.base = base
        self.migrate_args = migrate_args
        self.stuart_args = stuart_args or []
        self.cpus = cpus or {}  # port -> the CPU to pin it to (benchmarks)
        self.dir = tempfile.mkdtemp(prefix="migrate_check.")
        self.stuart_proc: Optional[subprocess.Popen] = None
        self.nodes: Dict[int, subprocess.Popen] = {}  # port -> process
        self.start_stuart()

    # Processes.
    def start_stuart(self):
        log = open(os.path.join(self.dir, "stuart.log"), "ab")
        self.stuart_proc = subprocess.Popen(
            [*self.pin(self.base), f"{self.bin}/stuart", f"{HOST}:{self.base}", "--state", os.path.join(self.dir, "state"),
             "--loglevel", "warning", *self.stuart_args],
            stderr=log, stdout=subprocess.DEVNULL)
        wait_for("stuart to listen", lambda: self.stuart().ping())

    def pin(self, port: int) -> List[str]:
        return ["taskset", "-c", str(self.cpus[port])] if port in self.cpus else []

    def kill_stuart(self):
        self.stuart_proc.send_signal(signal.SIGKILL)
        self.stuart_proc.wait()

    def start_node(self, port: int, extra: Optional[List[str]] = None):
        log = open(os.path.join(self.dir, f"node-{port}.log"), "ab")
        self.nodes[port] = subprocess.Popen(
            [*self.pin(port), f"{self.bin}/dbnode", f"{HOST}:{port}", "--cluster", "--stuart", f"{HOST}:{self.base}",
             "--loglevel", "warning", *self.migrate_args, *(extra or [])],
            stderr=log, stdout=subprocess.DEVNULL)
        node_id = wait_for(f"node {port} to listen", lambda: text(self.node(port).execute_command("CLUSTER", "MYID")))
        wait_for(f"node {port} to join", lambda: any(l.startswith(node_id + " ") and " up " in l for l in self.stuart_nodes()))
        return node_id

    def kill_node(self, port: int):
        self.nodes[port].send_signal(signal.SIGKILL)
        self.nodes[port].wait()

    def term_node(self, port: int):
        self.nodes[port].send_signal(signal.SIGTERM)

    def wait_exit(self, port: int, timeout: float = 120.0) -> int:
        """Waits until node `port` exits by itself; its status. It's no
        longer one of the nodes."""
        try:
            status = self.nodes[port].wait(timeout)
        except subprocess.TimeoutExpired:
            raise Fail(f"node {port} didn't exit within {timeout:.0f} s of SIGTERM")
        del self.nodes[port]
        return status

    def stop(self, keep: bool):
        for p in [self.stuart_proc, *self.nodes.values()]:
            if p and p.poll() is None:
                p.kill()
                p.wait()
        if keep:
            print(f"  logs kept in {self.dir}")
        else:
            shutil.rmtree(self.dir, ignore_errors=True)

    # Connections.
    def stuart(self):
        return redis.Redis(host=HOST, port=self.base, socket_timeout=5)

    def node(self, port: int):
        return redis.Redis(host=HOST, port=port, socket_timeout=10)

    def node_ports(self) -> List[int]:
        return sorted(self.nodes)

    # stuart.
    def stuart_nodes(self) -> List[str]:
        return text(self.stuart().execute_command("STUART", "NODES")).splitlines()

    def migrations(self) -> List[str]:
        return text(self.stuart().execute_command("STUART", "MIGRATIONS")).splitlines()

    def epoch(self) -> int:
        return int(text(self.stuart().execute_command("STUART", "TABLE")).splitlines()[0].split()[1])

    def create(self, n: int):
        wait_for(f"{n} nodes up", lambda: sum(" up " in l for l in self.stuart_nodes()) == n)
        if text(self.stuart().execute_command("STUART", "CREATE")) != "OK":
            raise Fail("STUART CREATE")
        self.wait_settled()

    def wait_settled(self, timeout: float = 120.0):
        """Nothing moves, every node is up and took stuart's table."""
        def settled():
            if self.migrations():
                return False
            epoch = self.epoch()
            lines = [l for l in self.stuart_nodes() if not l.startswith("unassigned")]
            return all(" up " in l and f" acked={epoch} " in l for l in lines)
        wait_for("the moves to end and every node to take the table", settled, timeout, 0.1)

    def owners(self) -> Dict[int, int]:
        """Slot -> port, from the first node's CLUSTER SLOTS (all agree
        once settled)."""
        views = []
        for port in self.node_ports():
            view = {}
            for start, end, primary, *_ in self.node(port).execute_command("CLUSTER", "SLOTS"):
                for s in range(start, end + 1):
                    view[s] = primary[1]
            views.append(view)
        if any(v != views[0] for v in views):
            raise Fail("the nodes' CLUSTER SLOTS differ once settled")
        return views[0]

    def current_epoch(self, port: int) -> int:
        info = text(self.node(port).execute_command("CLUSTER", "INFO"))
        return int(re.search(r"cluster_current_epoch:(\d+)", info).group(1))

    def info(self, port: int) -> Dict[str, int]:
        return {k: v for k, v in self.node(port).info("cluster").items() if isinstance(v, int)}

    def fill(self, n: int):
        """f:0 .. f:n-1 with 100-byte values, straight to their owners."""
        owners = self.owners()
        by_port: Dict[int, List[int]] = {}
        for i in range(n):
            by_port.setdefault(owners[key_slot(f"f:{i}".encode())], []).append(i)
        for port, ids in by_port.items():
            r = self.node(port)
            for chunk in range(0, len(ids), 2000):
                p = r.pipeline(transaction=False)
                for i in ids[chunk:chunk + 2000]:
                    p.set(f"f:{i}", filler_value(i))
                p.execute()
        # A big value in each hot slot: the batch carrying its hot counters
        # takes a millisecond or so instead of a tenth.
        for slot in HOT_SLOTS:
            self.node(owners[slot]).set(big_key(slot), BIG_VALUE)
        fill_collections(self, owners, COLLECTION)


# Slots that move in every scenario: the first node's lowest (DRAIN empties
# the first of four nodes, 0-4095) and its highest kept ones (REBALANCE of
# three nodes onto a fourth takes 4096-5460 from it). Four hot counters in
# each, INCRed in a tight loop, and a 512 KiB value that makes their batch
# slow, so that some are written while in flight.
HOT_SLOTS = [100, 1000, 2000, 3000, 4200, 4600, 5000, 5400]
HOT_PER_SLOT = 4
BIG_VALUE = b"B" * (512 * 1024)


def tag_for(slot: int) -> str:
    """A hash tag whose keys land in `slot`."""
    i = 0
    while key_slot(f"t{i}".encode()) != slot:
        i += 1
    return f"t{i}"


def big_key(slot: int) -> str:
    return f"{{{tag_for(slot)}}}big"


def counter_names(keys: int) -> List[str]:
    """k:0 .. k:<keys-1> (Lettuce writes those), then the hot counters."""
    hot = [f"{{{tag_for(s)}}}h{j}" for s in HOT_SLOTS for j in range(HOT_PER_SLOT)]
    return [f"k:{i}" for i in range(keys)] + hot


def filler_value(i: int) -> bytes:
    return f"{i}:".encode().ljust(100, b"v")


def mem_value(i: int) -> bytes:
    """The value of m:<i>, the keys that fill nodes' memory (kcheck.py fill)."""
    return f"{i}:".encode().ljust(16384, b"m")


# Writers. Each counts per counter: acknowledged INCRs, and failed ones.
class Writer:
    name = "?"

    def __init__(self, names: List[str], seed_port: int, indices: Optional[List[int]] = None):
        self.names = names
        # The counters it writes, round and round: the k:<i> by default.
        self.indices = indices if indices is not None else [i for i, n in enumerate(names) if n.startswith("k:")]
        self.seed_port = seed_port
        self.acked = [0] * len(names)
        self.doubt = [0] * len(names)
        self.errors: List[str] = []
        # Failures from a known client bug (see RedisPyWriter), in doubt too.
        self.client_bugs = 0
        self.ops = 0
        self.redirects = 0  # seen by the client, where it shows them
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        raise NotImplementedError

    def start(self, offset: int):
        self.offset = offset
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join(60)
        if self.thread.is_alive():
            raise Fail(f"{self.name} didn't stop")

    def failed(self, i: int, e: str):
        self.doubt[i] += 1
        if len(self.errors) < 1000:
            self.errors.append(f"{self.names[i]}: {e}")


class RedisPyWriter(Writer):
    """A RedisCluster. redis-py 8.1.0 fails a command that gets -ASK to a
    node not in its slot map (AttributeError: 'NoneType' object has no
    attribute 'redis_connection', before sending anything) until a -MOVED
    or a refresh teaches it the node: a node added while it runs, before
    it owns a slot. Redis Cluster does the same (a node added with
    redis-cli --cluster add-node, then a slot migrating to it). Counted
    apart: the bug, not a lost write."""
    name = "redis-py"

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(host=HOST, port=self.seed_port))
        j = self.offset
        while not self.stopping.is_set():
            j = (j + 1) % len(self.indices)
            i = self.indices[j]
            try:
                rc.incr(self.names[i])
                self.acked[i] += 1
            except Exception as e:  # noqa: BLE001 - in doubt
                if isinstance(e, AttributeError) and "redis_connection" in str(e):
                    self.client_bugs += 1
                self.failed(i, f"{type(e).__name__}: {e}")
            self.ops += 1
        rc.close()


class RedisCliWriter(Writer):
    """One `redis-cli -c` fed on stdin; it prints `-> Redirected to slot
    ...` for each redirect it follows, then the reply. A redis-cli that
    exits or hangs (a node killed) is started again."""
    name = "redis-cli"

    def spawn(self):
        return subprocess.Popen(["redis-cli", "-c", "-h", HOST, "-p", str(self.seed_port)],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)

    def run(self):
        proc = self.spawn()
        buf = b""
        j = self.offset
        while not self.stopping.is_set():
            j = (j + 1) % len(self.indices)
            i = self.indices[j]
            self.ops += 1
            try:
                proc.stdin.write(f"INCR {self.names[i]}\n".encode())
            except OSError:
                self.failed(i, "redis-cli exited")
                proc = self.spawn()
                buf = b""
                continue
            deadline = time.time() + 10
            reply = None
            while reply is None:
                while b"\n" not in buf:
                    left = deadline - time.time()
                    ready = select.select([proc.stdout], [], [], max(left, 0))[0] if left > 0 else []
                    chunk = os.read(proc.stdout.fileno(), 65536) if ready else b""
                    if not chunk:
                        break
                    buf += chunk
                if b"\n" not in buf:
                    reply = "redis-cli exited or hung"
                    break
                line, buf = buf.split(b"\n", 1)
                line = line.decode(errors="replace").strip()
                if line.startswith("-> Redirected"):
                    self.redirects += 1
                elif line:
                    reply = line
            if reply is not None and re.fullmatch(r"-?\d+", reply):
                self.acked[i] += 1
            else:
                self.failed(i, reply)
                if reply == "redis-cli exited or hung":
                    proc.kill()
                    proc.wait()
                    proc = self.spawn()
                    buf = b""
        proc.stdin.close()
        proc.wait(10)


class LettuceWriter(Writer):
    """LettuceLoad (lettuce/LettuceLoad.java): counts its own, reports
    them when its stdin closes. Seeded with the first node, not stuart:
    Lettuce wants a `myself` line in its seed's CLUSTER NODES ("Cannot
    determine own partition"), and stuart is no node."""

    def __init__(self, names: List[str], seed_port: int, classpath: str, mode: str):
        super().__init__(names, seed_port)
        self.classpath, self.mode = classpath, mode
        self.name = f"lettuce-{mode}"

    def start(self, offset: int):
        self.proc = subprocess.Popen(["java", "-cp", self.classpath, "LettuceLoad", HOST, str(self.seed_port), str(len(self.indices)), self.mode],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.stderr: List[str] = []
        threading.Thread(target=lambda: self.stderr.extend(l.decode(errors="replace") for l in self.proc.stderr), daemon=True).start()

    def stop(self):
        try:
            out, _ = self.proc.communicate(timeout=60)  # closes its stdin
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise Fail(f"{self.name} didn't stop")
        if self.proc.returncode != 0:
            raise Fail(f"{self.name} exited with {self.proc.returncode}: {''.join(self.stderr[-20:])}")
        for line in out.decode().splitlines():
            i, acked, doubt = map(int, line.split())
            self.acked[i], self.doubt[i] = acked, doubt
        self.ops = sum(self.acked) + sum(self.doubt)
        if sum(self.doubt):
            self.errors = [l.strip() for l in self.stderr if " failed: " in l][:5]


# Large collections in the hot slots (--collection).
COLLECTION = 0
COUNTER_FIELDS = 4


def collection_keys(slot: int) -> Tuple[str, str, str]:
    tag = tag_for(slot)
    return f"{{{tag}}}ch", f"{{{tag}}}cs", f"{{{tag}}}cl"


def fill_collections(cluster: "Cluster", owners: Dict[int, int], n: int):
    """A hash (f<i>: v<i>), a set (m<i>) and a list (e<i>, in order) of n
    elements in each hot slot, straight to its owner."""
    if not n:
        return
    for slot in HOT_SLOTS:
        h, st, l = collection_keys(slot)
        r = cluster.node(owners[slot])
        for start in range(0, n, 5000):
            ids = range(start, min(n, start + 5000))
            p = r.pipeline(transaction=False)
            p.hset(h, mapping={f"f{i}": f"v{i}" for i in ids})
            p.sadd(st, *[f"m{i}" for i in ids])
            p.rpush(l, *[f"e{i}" for i in ids])
            p.execute()


class CollectionWriter(Writer):
    """A RedisCluster on the hot slots' collections, round and round:
    HINCRBY of a counter field, SADD and RPUSH of w<seq>. Its counts are
    its own (the counters' are zero), checked by check_collections."""
    name = "redis-py (collections)"

    def __init__(self, names: List[str], seed_port: int):
        super().__init__(names, seed_port, [])
        # (slot, field) -> [acknowledged, in doubt]; seq -> True if
        # acknowledged, False in doubt, per (slot, kind).
        self.fields: Dict[Tuple[int, int], List[int]] = {}
        self.sent: Dict[Tuple[int, str], Dict[int, bool]] = {}

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(host=HOST, port=self.seed_port))
        seq = 0
        while not self.stopping.is_set():
            seq += 1
            slot = HOT_SLOTS[seq % len(HOT_SLOTS)]
            h, st, l = collection_keys(slot)
            field = seq % COUNTER_FIELDS
            ops = [("hincrby", lambda: rc.hincrby(h, f"c{field}", 1)),
                   ("sadd", lambda: rc.sadd(st, f"w{seq}")),
                   ("rpush", lambda: rc.rpush(l, f"w{seq}"))]
            for kind, op in ops:
                try:
                    op()
                    ok = True
                except Exception as e:  # noqa: BLE001 - in doubt
                    if isinstance(e, AttributeError) and "redis_connection" in str(e):
                        self.client_bugs += 1
                    ok = False
                    if len(self.errors) < 1000:
                        self.errors.append(f"{kind} {slot}: {type(e).__name__}: {e}")
                    self.collection_failures += 1
                if kind == "hincrby":
                    self.fields.setdefault((slot, field), [0, 0])[0 if ok else 1] += 1
                else:
                    self.sent.setdefault((slot, kind), {})[seq] = ok
                self.ops += 1
        rc.close()

    collection_failures = 0


def check_collections(cluster: "Cluster", w: CollectionWriter, lossy: Set[int]) -> List[str]:
    problems: List[str] = []
    owners = cluster.owners()
    n = COLLECTION
    for slot in HOT_SLOTS:
        if slot in lossy:
            continue
        h, st, l = collection_keys(slot)
        r = cluster.node(owners[slot])
        got_h = {k.decode(): v.decode() for k, v in r.hgetall(h).items()}
        missing = sum(got_h.get(f"f{i}") != f"v{i}" for i in range(n))
        if missing:
            problems.append(f"{h}: {missing} of its {n} fields missing or wrong")
        for f in range(COUNTER_FIELDS):
            acked, doubt = w.fields.get((slot, f), [0, 0])
            v = int(got_h.get(f"c{f}", 0))
            if not acked <= v <= acked + RETRIES * doubt:
                problems.append(f"{h} c{f} = {v}, {acked} HINCRBYs acknowledged, {doubt} in doubt")
        extra = set(got_h) - {f"f{i}" for i in range(n)} - {f"c{f}" for f in range(COUNTER_FIELDS)}
        if extra:
            problems.append(f"{h}: fields nobody wrote: {sorted(extra)[:5]}")

        got_s = {m.decode() for m in r.smembers(st)}
        missing = sum(f"m{i}" not in got_s for i in range(n))
        sent = w.sent.get((slot, "sadd"), {})
        lost = [q for q, ok in sent.items() if ok and f"w{q}" not in got_s]
        unknown = [m for m in got_s if not m.startswith("m") and not (m.startswith("w") and int(m[1:]) in sent)]
        if missing or lost or unknown:
            problems.append(f"{st}: {missing} original members missing, acknowledged SADDs lost {lost[:5]}, members nobody added {unknown[:5]}")

        got_l = [e.decode() for e in r.lrange(l, 0, -1)]
        if got_l[:n] != [f"e{i}" for i in range(n)]:
            problems.append(f"{l}: its first {n} elements aren't the original ones in order")
        pushed = w.sent.get((slot, "rpush"), {})
        rest = [int(e[1:]) for e in got_l[n:] if e.startswith("w")]
        if len(rest) != len(got_l) - n:
            problems.append(f"{l}: elements nobody pushed after the original ones")
        # In order; a retried push (in doubt) may repeat, nothing else.
        bad = [b for a, b in zip(rest, rest[1:]) if b < a or (b == a and pushed.get(b) is not False)]
        lost = [q for q, ok in pushed.items() if ok and q not in set(rest)]
        unknown = [q for q in rest if q not in pushed]
        if bad or lost or unknown:
            problems.append(f"{l}: out of order {bad[:5]}, acknowledged RPUSHes lost {lost[:5]}, pushes nobody sent {unknown[:5]}")
    acked = sum(a for a, _ in w.fields.values()) + sum(ok for d in w.sent.values() for ok in d.values())
    parts = sum(cluster.info(p).get("migrate_parts_sent", 0) for p in cluster.node_ports())
    aborted = sum(cluster.info(p).get("migrate_parts_aborted", 0) for p in cluster.node_ports())
    print(f"  checked {3 * len(HOT_SLOTS)} collections of {n}: {acked} writes to them acknowledged, {w.collection_failures} failed; "
          f"{parts} parts sent, {aborted} moves of a collection given up (by the nodes up now)")
    return problems


def make_writers(args, seed_port: int) -> List[Writer]:
    names = counter_names(args.keys)
    hot = [i for i, n in enumerate(names) if not n.startswith("k:")]
    writers: List[Writer] = [RedisPyWriter(names, seed_port), RedisPyWriter(names, seed_port), RedisCliWriter(names, seed_port),
                             RedisPyWriter(names, seed_port, hot)]
    writers[-1].name = "redis-py (hot keys)"
    if COLLECTION:
        writers.append(CollectionWriter(names, seed_port))
    # Lettuce wants a node as its seed (see LettuceWriter): the first one,
    # unless told another.
    for mode in args.lettuce_modes:
        writers.append(LettuceWriter(names, getattr(args, "lettuce_port", seed_port + 1), args.lettuce, mode))
    return writers


# The check.
def check(cluster: Cluster, writers: List[Writer], filler: int, lossy: Set[int], mem_keys: int = 0) -> List[str]:
    problems: List[str] = []
    owners = cluster.owners()
    where: Dict[bytes, List[int]] = {}
    values: Dict[Tuple[bytes, int], Optional[bytes]] = {}
    for port in cluster.node_ports():
        r = cluster.node(port)
        p = r.pipeline(transaction=False)
        for s in range(SLOTS):
            p.execute_command("CLUSTER", "COUNTKEYSINSLOT", s)
        counts = p.execute()
        p = r.pipeline(transaction=False)
        slots = [s for s in range(SLOTS) if counts[s]]
        for s in slots:
            p.execute_command("CLUSTER", "GETKEYSINSLOT", s, counts[s])
        keys = [k for ks in p.execute() for k in ks]
        for s in slots:
            if owners.get(s) != port:
                problems.append(f"node {port} holds {counts[s]} keys of slot {s}, which its table gives to {owners.get(s)}")
        p = r.pipeline(transaction=False)
        for k in keys:
            p.get(k)
        for k, v in zip(keys, p.execute(raise_on_error=False)):
            where.setdefault(k, []).append(port)
            # A node refuses GET for a slot it doesn't own (reported above).
            values[(k, port)] = None if isinstance(v, Exception) else v
    twice = [k for k, ports in where.items() if len(ports) > 1]
    for k in twice[:10]:
        problems.append(f"{k!r} is at {where[k]}")
    if len(twice) > 10:
        problems.append(f"... {len(twice)} keys at two nodes")

    lost_filler = wrong_filler = 0
    for i in range(filler):
        k = f"f:{i}".encode()
        if k not in where:
            if key_slot(k) not in lossy:
                lost_filler += 1
        elif values[(k, where[k][0])] != filler_value(i):
            wrong_filler += 1
    for i in range(mem_keys):
        k = f"m:{i}".encode()
        if k not in where:
            if key_slot(k) not in lossy:
                lost_filler += 1
        elif values[(k, where[k][0])] != mem_value(i):
            wrong_filler += 1
    for slot in HOT_SLOTS:
        k = big_key(slot).encode()
        if k not in where:
            if slot not in lossy:
                lost_filler += 1
        elif values[(k, where[k][0])] != BIG_VALUE:
            wrong_filler += 1
    if lost_filler:
        problems.append(f"{lost_filler} filler keys lost")
    if wrong_filler:
        problems.append(f"{wrong_filler} filler keys with a wrong value")

    names = writers[0].names
    wrong = []
    lost = 0
    for i, name in enumerate(names):
        k = name.encode()
        acked = sum(w.acked[i] for w in writers)
        doubt = sum(w.doubt[i] for w in writers)
        raw = values[(k, where[k][0])] if k in where else b"0"
        v = int(raw) if raw is not None else -1
        if key_slot(k) in lossy:
            if k not in where:
                lost += 1
            if v > acked + RETRIES * doubt:
                wrong.append(f"{name} = {v}: more than its {acked} INCRs acknowledged + {doubt} in doubt")
        elif not acked <= v <= acked + RETRIES * doubt:
            wrong.append(f"{name} = {v}, {acked} INCRs acknowledged, {doubt} in doubt")
    problems += wrong[:10]
    if len(wrong) > 10:
        problems.append(f"... {len(wrong)} counters wrong")
    lossy_keys = sum(key_slot(n.encode()) in lossy for n in names)
    for w in writers:
        if isinstance(w, CollectionWriter):
            problems += check_collections(cluster, w, lossy)
    print(f"  checked: {len(where)} keys on {len(cluster.node_ports())} nodes, {sum(map(len, where.values())) - len(where)} twice; "
          f"{len(names)} counters, {sum(sum(w.acked) for w in writers)} INCRs acknowledged, {sum(sum(w.doubt) for w in writers)} in doubt"
          + (f"; {len(lossy)} slots of the dead node: {lost} of its {lossy_keys} counters lost" if lossy else ""))
    return problems


def report_writers(writers: List[Writer], may_fail: bool) -> List[str]:
    problems = []
    for w in writers:
        failed = sum(w.doubt) + (w.collection_failures if isinstance(w, CollectionWriter) else 0) - w.client_bugs
        print(f"  {w.name}: {w.ops} INCRs, {failed} failed" + (f", {w.redirects} redirects followed" if w.redirects else "")
              + (f", {w.client_bugs} failed on -ASK to a node it didn't know (the redis-py bug)" if w.client_bugs else ""))
        for e in [e for e in w.errors if "redis_connection" not in e][:3]:
            print(f"    {e}")
        if failed and not may_fail:
            problems.append(f"{w.name}: {failed} INCRs failed")
        if w.ops == 0:
            problems.append(f"{w.name} wrote nothing")
    return problems


# Scenarios.
def setup_auto(args, n: int) -> Tuple[Cluster, List[int]]:
    """stuart --auto and `n` nodes joining one after the other, evened out."""
    cluster = Cluster(args.bin, args.base_port, args.migrate_args, ["--auto"])
    ports = [args.base_port + 1 + i for i in range(n)]
    try:
        for port in ports:
            cluster.start_node(port)
        wait_even(cluster, n)
        if args.filler:
            cluster.fill(args.filler)
    except BaseException:
        cluster.stop(args.keep)
        raise
    return cluster, ports


def slot_counts(cluster: Cluster) -> List[int]:
    return [int(m.group(1)) for l in cluster.stuart_nodes() if (m := re.search(r" slots=(\d+)", l))]


def wait_even(cluster: Cluster, n: int, timeout: float = 120.0):
    """Settled, with `n` nodes owning 16384 / n slots each, or one more."""
    def even():
        counts = slot_counts(cluster)
        return len(counts) == n and sum(counts) == SLOTS and max(counts) - min(counts) <= 1
    cluster.wait_settled(timeout)
    wait_for(f"{n} nodes to have their share of the slots", lambda: even() and not cluster.migrations(), timeout, 0.1)
    cluster.wait_settled(timeout)


def setup(args, n: int) -> Tuple[Cluster, List[int]]:
    cluster = Cluster(args.bin, args.base_port, args.migrate_args)
    ports = [args.base_port + 1 + i for i in range(n)]
    try:
        for port in ports:
            cluster.start_node(port)
        cluster.create(n)
        if args.filler:
            cluster.fill(args.filler)
    except BaseException:
        cluster.stop(args.keep)
        raise
    return cluster, ports


def moving(cluster: Cluster) -> List[Tuple[int, str, str]]:
    """Slots moving now: (slot, from id, to id)."""
    out = []
    for line in cluster.migrations():
        m = re.match(r"(\d+) (\S+) -> (\S+) ", line)
        if m:
            out.append((int(m.group(1)), m.group(2), m.group(3)))
    return out


def port_of(cluster: Cluster, node_id: str) -> int:
    for port in cluster.node_ports():
        if text(cluster.node(port).execute_command("CLUSTER", "MYID")) == node_id:
            return port
    raise Fail(f"no node {node_id}")


def slots_of(cluster: Cluster, node_id: str) -> Set[int]:
    """The slots stuart's table gives `node_id`, and those moving to it."""
    out = {slot for slot, _, to in moving(cluster) if to == node_id}
    for line in text(cluster.stuart().execute_command("STUART", "TABLE")).splitlines()[1:]:
        fields = line.split()
        if fields[0] == node_id:
            for f in fields[8:]:
                a, _, b = f.partition("-")
                out.update(range(int(a), int(b or a) + 1))
    return out


def slot_count(cluster: Cluster, node_id: str) -> int:
    """How many slots stuart's table gives `node_id` (STUART NODES)."""
    line = next(l for l in cluster.stuart_nodes() if l.startswith(node_id + " "))
    return int(re.search(r" slots=(\d+)", line).group(1))


def run_scenario(name: str, args) -> List[str]:
    drain = name.startswith("drain")
    n = 4 if drain else 3
    writers = make_writers(args, args.base_port)
    cluster, ports = setup(args, n)
    problems: List[str] = []
    lossy: Set[int] = set()
    keep = False
    try:
        for j, w in enumerate(writers):
            w.start(j * args.keys // len(writers))
        time.sleep(1)
        t0 = time.time()
        first = text(cluster.node(ports[0]).execute_command("CLUSTER", "MYID"))
        if drain:
            moves = cluster.stuart().execute_command("STUART", "DRAIN", first)
            # Mid-move: a quarter of the first node's slots gone.
            quarter = lambda: slot_count(cluster, first) <= 3072 and moving(cluster)  # noqa: E731
        else:
            new_port = args.base_port + 4
            new_id = cluster.start_node(new_port)
            moves = cluster.stuart().execute_command("STUART", "REBALANCE")
            # Mid-move: a quarter of the slots moved, more to go.
            quarter = lambda: slot_count(cluster, new_id) >= 1024 and moving(cluster)  # noqa: E731
        print(f"  {moves} slots to move")
        if name != "rebalance" and name != "drain":
            wait_for("a quarter of the slots to move", quarter, 30, 0.005)
        if name == "stuart-kill":
            moving_then = len(moving(cluster))
            cluster.kill_stuart()
            print(f"  stuart killed after {time.time() - t0:.1f} s with {moving_then} slots moving; down 1 s")
            time.sleep(1)
            cluster.start_stuart()
        elif name in ("source-kill", "target-kill", "drain-target-kill"):
            slot, src, dst = moving(cluster)[0]
            victim_id = src if name == "source-kill" else dst
            victim = port_of(cluster, victim_id)
            lossy = slots_of(cluster, victim_id)
            epochs = {p: cluster.current_epoch(p) for p in cluster.node_ports() if p != victim}
            left = slot_count(cluster, first)
            cluster.kill_node(victim)
            lossy |= slots_of(cluster, victim_id)
            down = {"source-kill": 1, "target-kill": 3}.get(name, 5)
            print(f"  node {victim} ({'source' if name == 'source-kill' else 'target'} of slot {slot}) killed after "
                  f"{time.time() - t0:.1f} s; down {down} s")
            time.sleep(down)
            later = {p: cluster.current_epoch(p) for p in epochs}
            print(f"  meanwhile the other nodes' epochs went {epochs} -> {later}")
            if drain:
                # The moves to the other targets go on meanwhile.
                now = slot_count(cluster, first)
                print(f"  and the drained node's slots went {left} -> {now}")
                if now >= left:
                    problems.append(f"the drain stalled while a target was down ({left} -> {now} slots)")
            cluster.start_node(victim)
        cluster.wait_settled()
        print(f"  moves done in {time.time() - t0:.1f} s")
        time.sleep(1)
        for w in writers:
            w.stop()
        problems += report_writers(writers, may_fail=bool(lossy))
        problems += check(cluster, writers, args.filler, lossy)
        if name == "drain":
            owners = cluster.owners()
            if any(p == ports[0] for p in owners.values()) or cluster.node(ports[0]).dbsize():
                problems.append(f"node {ports[0]} still has slots or keys after DRAIN")
        elif drain:
            # The moves queued to the replaced node were dropped.
            print(f"  the drained node keeps {slot_count(cluster, first)} slots (moves to the dead node dropped)")
        elif name != "target-kill":
            have = sum(1 for p in cluster.owners().values() if p == args.base_port + 4)
            if not lossy and have != 4096:
                problems.append(f"the new node has {have} slots, not 4096")
    except Fail as e:
        problems.append(str(e))
    finally:
        keep = bool(problems) and args.keep
        for w in writers:
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            w.stopping.set()
        cluster.stop(keep)
    return problems


def run_auto(name: str, args) -> List[str]:
    writers = make_writers(args, args.base_port)
    cluster, ports = setup_auto(args, {"auto-grow": 1, "sigterm-target": 3, "full-target": 3}.get(name, 4))
    problems: List[str] = []
    keep = False
    try:
        for j, w in enumerate(writers):
            w.start(j * args.keys // len(writers))
        time.sleep(1)
        t0 = time.time()
        if name == "auto-grow":
            for i in range(3):
                cluster.start_node(args.base_port + 2 + i)
                print(f"  node {args.base_port + 2 + i} joined after {time.time() - t0:.1f} s: {len(moving(cluster))} slots moving")
                time.sleep(0.5)
            wait_even(cluster, 4)
            print(f"  moves done in {time.time() - t0:.1f} s: {slot_counts(cluster)}")
        elif name == "full-target":
            problems += full_target(cluster, args, t0)
        elif name.startswith("sigterm"):
            if name == "sigterm-target":
                new_port = args.base_port + 4
                new_id = cluster.start_node(new_port)
                wait_for("a quarter of its slots to move to the new node",
                         lambda: slot_count(cluster, new_id) >= 1024 and moving(cluster), 30, 0.005)
                victims = [new_port]
            else:
                victims = ports[:2] if name == "sigterm-two" else ports[:1]
            held = {p: slot_count(cluster, text(cluster.node(p).execute_command("CLUSTER", "MYID"))) for p in victims}
            for p in victims:
                cluster.term_node(p)
            print(f"  SIGTERM to {', '.join(f'node {p} ({held[p]} slots)' for p in victims)} after {time.time() - t0:.1f} s")
            for p in victims:
                status = cluster.wait_exit(p)
                print(f"  node {p} exited with status {status} after {time.time() - t0:.1f} s")
                if status != 0:
                    problems.append(f"node {p} exited with status {status}")
            remaining = len(cluster.node_ports())
            wait_even(cluster, remaining)
            print(f"  moves done in {time.time() - t0:.1f} s: {slot_counts(cluster)}")
        else:
            first = text(cluster.node(ports[0]).execute_command("CLUSTER", "MYID"))
            held = cluster.stuart().execute_command("STUART", "LEAVE", first)
            cluster.start_node(args.base_port + 5)
            print(f"  node {ports[0]} leaves (it holds {held - 1} slots) as node {args.base_port + 5} joins")
            wait_for("the first node to leave", lambda: cluster.stuart().execute_command("STUART", "LEAVE", first) == 0, 120, 0.1)
            print(f"  node {ports[0]} left after {time.time() - t0:.1f} s")
            wait_even(cluster, 4)
            print(f"  moves done in {time.time() - t0:.1f} s: {slot_counts(cluster)}")
            # It holds nothing and redirects; then it goes (it would exit by
            # itself after a SIGTERM, milestone 2).
            r = cluster.node(ports[0])
            if r.dbsize():
                problems.append(f"node {ports[0]} left with {r.dbsize()} keys")
            try:
                r.get("k:0")
                problems.append(f"node {ports[0]} serves k:0 after leaving")
            except redis.exceptions.ResponseError as e:
                if type(e).__name__ != "MovedError" and not str(e).startswith("MOVED"):
                    problems.append(f"node {ports[0]} answers {e} after leaving")
            if any(l.startswith(first) for l in cluster.stuart_nodes()):
                problems.append(f"node {ports[0]} is still in STUART NODES")
            cluster.kill_node(ports[0])
            del cluster.nodes[ports[0]]
        time.sleep(1)
        for w in writers:
            w.stop()
        full = name == "full-target"
        problems += report_writers(writers, may_fail=full)
        if full:
            # Refused writes, and only those: -OOM from the full node.
            for w in writers:
                other = [e for e in w.errors if not re.search(r"OOM|OutOfMemoryError", e) and "redis_connection" not in e]
                if other:
                    problems.append(f"{w.name}: {len(other)} INCRs failed otherwise than with -OOM: {other[:3]}")
        problems += check(cluster, writers, args.filler, set())
    except Fail as e:
        problems.append(str(e))
    finally:
        keep = bool(problems) and args.keep
        for w in writers:
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            w.stopping.set()
        cluster.stop(keep)
    return problems


def memory(cluster: Cluster, port: int) -> Dict[str, int]:
    return {k: v for k, v in cluster.node(port).info("memory").items() if isinstance(v, int)}


def full_target(cluster: Cluster, args, t0: float) -> List[str]:
    """full-target, from the three nodes evened out: see the docs."""
    problems: List[str] = []
    first = cluster.node_ports()[0]
    m = memory(cluster, first)
    # Room for 40% of what a node holds now: a fourth node's share is 75%.
    limit = m["used_memory_startup"] + m["used_memory_dataset"] * 4 // 10
    port = args.base_port + 4
    new_id = cluster.start_node(port, ["--maxmemory", str(limit)])
    print(f"  node {port} joined with --maxmemory {limit} ({limit / 1e6:.1f} MB) after {time.time() - t0:.1f} s")
    try:
        wait_for("it to be full", lambda: any(l.startswith(new_id) and l.endswith(" full") for l in cluster.stuart_nodes()), 60, 0.05)
    except Fail as e:
        mem = memory(cluster, port)
        line = next((l for l in cluster.stuart_nodes() if l.startswith(new_id)), "?")
        raise Fail(f"{e}: used {mem['used_memory']} of {limit}, {cluster.info(port).get('migrate_imports_refused', 0)} "
                   f"batches refused, {len(moving(cluster))} slots moving; stuart: {line}")
    # Nothing more reaches it: its slots stop changing, the moves under
    # way to it stall (forward only), the others keep theirs.
    def still():
        before = slot_counts(cluster)
        time.sleep(3)
        return before == slot_counts(cluster) and before
    counts = wait_for("the moves to it to stall", still, 60, 0.1)
    stalled = [m for m in moving(cluster) if m[2] == new_id]
    mem = memory(cluster, port)
    refused = cluster.info(port).get("migrate_imports_refused", 0)
    print(f"  full after {time.time() - t0:.1f} s: slots {counts}, {len(stalled)} moving to it stalled, "
          f"{refused} batches refused, used {mem['used_memory'] / 1e6:.1f} of {limit / 1e6:.1f} MB "
          f"(peak {mem['used_memory_peak'] / 1e6:.1f})")
    # Past it by the buffers of the connections the batches come in on at
    # most (not refused: as in Redis, client buffers count but nothing
    # checks them).
    if mem["used_memory_peak"] > limit + 2 * 1024 * 1024:
        problems.append(f"node {port} went to {mem['used_memory_peak']} bytes, past its maxmemory {limit}")
    if not refused and not stalled:
        problems.append("nothing was refused or stalled: the limit wasn't reached")
    if counts[-1] >= SLOTS // 4:
        problems.append(f"the full node got its whole share ({counts[-1]} slots)")
    cluster.node(port).execute_command("CONFIG", "SET", "maxmemory", "0")
    print(f"  its limit lifted after {time.time() - t0:.1f} s")
    wait_even(cluster, 4)
    print(f"  moves done in {time.time() - t0:.1f} s: {slot_counts(cluster)}")
    return problems


def redirect_counts(cluster: Cluster) -> int:
    total = 0
    for port in cluster.node_ports():
        try:
            info = cluster.info(port)
        except Exception:  # noqa: BLE001
            continue
        total += sum(info.get(f"cluster_redirects_{k}", 0) for k in ("moved", "ask", "tryagain"))
    return total


def run_redirects(args) -> List[str]:
    clients = ["redis-py", "redis-cli"] + [f"lettuce-{m}" for m in args.lettuce_modes]
    problems: List[str] = []
    print(f"  {'client':<17}{'before':>8}{'during':>8}   after, per 2 s window")
    for client in clients:
        mine: List[str] = []
        names = counter_names(args.keys)
        if client == "redis-py":
            w: Writer = RedisPyWriter(names, args.base_port)
        elif client == "redis-cli":
            w = RedisCliWriter(names, args.base_port)
        else:
            w = LettuceWriter(names, args.base_port + 1, args.lettuce, client.split("-", 1)[1])
        cluster, _ = setup(args, 3)
        try:
            w.start(0)
            time.sleep(2)  # connected, map loaded
            c0 = redirect_counts(cluster)
            time.sleep(2)
            c1 = redirect_counts(cluster)
            cluster.start_node(args.base_port + 4)
            cluster.stuart().execute_command("STUART", "REBALANCE")
            cluster.wait_settled()
            c2 = redirect_counts(cluster)
            after = []
            for _ in range(args.windows):
                time.sleep(2)
                c = redirect_counts(cluster)
                after.append(c - c2)
                c2 = c
            w.stop()
            print(f"  {client:<17}{c1 - c0:>8}{c2 - c1 - sum(after):>8}   {' '.join(f'{a:>6}' for a in after)}")
            mine += report_writers([w], may_fail=False)
            mine += check(cluster, [w], args.filler, set())
            # redis-cli keeps no slot map: it's redirected whenever the
            # key's node isn't the last one it was sent to.
            if client != "redis-cli" and c1 - c0:
                mine.append(f"{client}: {c1 - c0} redirects before any move")
            if client in ("redis-py", "lettuce-default", "lettuce-fast") and any(after[-2:]):
                mine.append(f"{client}: still redirected {after} after the rebalance")
        except Fail as e:
            mine.append(f"{client}: {e}")
        finally:
            w.stopping.set()
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            cluster.stop(bool(mine) and args.keep)
        problems += mine
    return problems


# metrics.
Series = Tuple[str, Tuple[Tuple[str, str], ...]]


def scrape(port: int) -> Dict[Series, float]:
    """GET /metrics at `port`, parsed by prometheus_client: (name,
    labels) -> value. Raises on anything it can't parse."""
    from prometheus_client.parser import text_string_to_metric_families
    with urllib.request.urlopen(f"http://{HOST}:{port}/metrics", timeout=5) as r:
        if r.status != 200 or not r.headers["Content-Type"].startswith("text/plain; version=0.0.4"):
            raise Fail(f"/metrics at {port}: {r.status} {r.headers['Content-Type']}")
        body = r.read().decode()
    out: Dict[Series, float] = {}
    for family in text_string_to_metric_families(body):
        if family.type not in ("counter", "gauge"):
            raise Fail(f"/metrics at {port}: {family.name} is a {family.type}")
        for sample in family.samples:
            key = (sample.name, tuple(sorted(sample.labels.items())))
            if key in out:
                raise Fail(f"/metrics at {port}: {key} twice")
            out[key] = sample.value
    return out


def one(metrics: Dict[Series, float], name: str, **labels) -> float:
    key = (name, tuple(sorted(labels.items())))
    if key not in metrics:
        raise Fail(f"no {name}{labels or ''}")
    return metrics[key]


class Scraper(threading.Thread):
    """Scrapes the ports in `ports` (which may grow) every 100 ms, noting
    what doesn't parse and the counters that go down."""

    def __init__(self, ports: List[int]):
        super().__init__(daemon=True)
        self.ports = ports
        self.stopping = threading.Event()
        self.last: Dict[int, Dict[Series, float]] = {}
        self.scrapes: Dict[int, int] = {}
        self.problems: List[str] = []

    def run(self):
        while not self.stopping.wait(0.1):
            for port in list(self.ports):
                try:
                    now = scrape(port)
                except Exception as e:  # noqa: BLE001
                    self.problems.append(f"scraping {port}: {e}")
                    continue
                for key, v in now.items():
                    before = self.last.get(port, {}).get(key)
                    if key[0].endswith("_total") and before is not None and v < before:
                        self.problems.append(f"{port}: {key} went down: {before} -> {v}")
                self.last[port] = now
                self.scrapes[port] = self.scrapes.get(port, 0) + 1


def metrics_port(port: int) -> int:
    return port + 1000


def run_metrics(args) -> List[str]:
    writers = make_writers(args, args.base_port)
    cluster = Cluster(args.bin, args.base_port, args.migrate_args, ["--metrics", f"{HOST}:{metrics_port(args.base_port)}"])
    problems: List[str] = []
    scraper = Scraper([metrics_port(args.base_port)])
    try:
        ports = [args.base_port + 1 + i for i in range(4)]
        for port in ports[:3]:
            cluster.start_node(port, ["--metrics", f"{HOST}:{metrics_port(port)}"])
            scraper.ports.append(metrics_port(port))
        cluster.create(3)
        if args.filler:
            cluster.fill(args.filler)
        scraper.start()
        for j, w in enumerate(writers):
            w.start(j * args.keys // len(writers))
        time.sleep(1)
        t0 = time.time()
        cluster.start_node(ports[3], ["--metrics", f"{HOST}:{metrics_port(ports[3])}"])
        scraper.ports.append(metrics_port(ports[3]))
        moves = cluster.stuart().execute_command("STUART", "REBALANCE")
        print(f"  {moves} slots to move")
        cluster.wait_settled()
        print(f"  moves done in {time.time() - t0:.1f} s")
        time.sleep(1)
        for w in writers:
            w.stop()
        time.sleep(0.5)
        scraper.stopping.set()
        scraper.join()
        problems += scraper.problems[:10]
        print(f"  scrapes: {', '.join(f'{p}: {n}' for p, n in sorted(scraper.scrapes.items()))}")
        if any(scraper.scrapes.get(metrics_port(p), 0) < 5 for p in [args.base_port, *ports]):
            problems.append(f"too few scrapes: {scraper.scrapes}")

        # Settled: the metrics against what stuart and the nodes say.
        st = scrape(metrics_port(args.base_port))
        if one(st, "stuart_slots_moved_total") != 4096:
            problems.append(f"stuart_slots_moved_total {one(st, 'stuart_slots_moved_total')}, not 4096")
        for name, want in [("stuart_slots_moving", 0), ("stuart_slots_queued", 0), ("stuart_slots_unassigned", 0)]:
            if one(st, name) != want:
                problems.append(f"{name} {one(st, name)}, not {want}")
        if one(st, "stuart_nodes", state="up") != 4 or one(st, "stuart_table_epoch") != cluster.epoch():
            problems.append(f"stuart: up {one(st, 'stuart_nodes', state='up')}, epoch {one(st, 'stuart_table_epoch')} "
                            f"(want 4, {cluster.epoch()})")
        sent = imported = 0
        for port in ports:
            node_id = text(cluster.node(port).execute_command("CLUSTER", "MYID"))
            slots = slot_count(cluster, node_id)
            if one(st, "stuart_node_slots", node=node_id, addr=f"{HOST}:{port}") != slots:
                problems.append(f"stuart_node_slots of {port} isn't {slots}")
            m = scrape(metrics_port(port))
            info = cluster.info(port)
            labels = next(dict(l) for n, l in m if n == "speedvault_info")
            if labels.get("mode") != "cluster" or labels.get("node_id") != node_id:
                problems.append(f"node {port}: speedvault_info {labels}")
            checks = [
                ("speedvault_cluster_slots", {}, slots),
                ("speedvault_cluster_epoch", {}, cluster.current_epoch(port)),
                ("speedvault_keys", {}, cluster.node(port).dbsize()),
                ("speedvault_migrate_keys_sent_total", {}, info["migrate_keys_sent"]),
                ("speedvault_migrate_keys_imported_total", {}, info["migrate_keys_imported"]),
                ("speedvault_redirects_total", {"kind": "moved"}, info["cluster_redirects_moved"]),
            ]
            for name, want_labels, want in checks:
                got = one(m, name, **want_labels)
                if got != want:
                    problems.append(f"node {port}: {name}{want_labels or ''} {got}, not {want}")
            busy, cpu = one(m, "speedvault_busy_seconds_total"), one(m, "speedvault_cpu_seconds_total")
            if not 0 < busy <= cpu * 1.05 + 0.05:
                problems.append(f"node {port}: busy {busy} s, CPU {cpu} s")
            sent += info["migrate_keys_sent"]
            imported += info["migrate_keys_imported"]
            print(f"  node {port}: {slots} slots, {one(m, 'speedvault_keys'):.0f} keys, busy {busy:.2f} s of {cpu:.2f} s CPU, "
                  f"{one(m, 'speedvault_redirects_total', kind='moved'):.0f} MOVED, "
                  f"{one(m, 'speedvault_redirects_total', kind='ask'):.0f} ASK")
        if sent != imported or sent == 0:
            problems.append(f"keys sent {sent}, imported {imported}")
        problems += report_writers(writers, may_fail=False)
        problems += check(cluster, writers, args.filler, set())
    except Fail as e:
        problems.append(str(e))
    finally:
        scraper.stopping.set()
        for w in writers:
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            w.stopping.set()
        cluster.stop(bool(problems) and args.keep)
    return problems


SCENARIOS = ["rebalance", "drain", "stuart-kill", "source-kill", "target-kill", "drain-target-kill", "auto-grow",
             "leave-while-join", "sigterm", "sigterm-two", "sigterm-target", "full-target", "metrics", "redirects"]


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="+", choices=SCENARIOS + ["all"])
    ap.add_argument("--bin", required=True, help="directory holding dbnode and stuart")
    ap.add_argument("--base-port", type=int, default=7880)
    ap.add_argument("--keys", type=int, default=2000, help="counters")
    ap.add_argument("--filler", type=int, default=200000, help="other keys, so moves take a while")
    ap.add_argument("--collection", type=int, default=20000, help="elements of each large collection in the hot slots (0: none)")
    ap.add_argument("--lettuce", help="LettuceLoad's class path (lettuce/build.sh): adds Lettuce writers")
    ap.add_argument("--lettuce-modes", default="default,off,fast", help="Lettuce refresh modes to run (comma separated)")
    ap.add_argument("--windows", type=int, default=8, help="redirects: 2 s windows after the rebalance")
    ap.add_argument("--migrate-arg", action="append", default=[], dest="migrate_args",
                    help="passed to each dbnode (e.g. --migrate-arg=--migrate-batch --migrate-arg=10)")
    ap.add_argument("--keep", action="store_true", help="keep the logs of a failed scenario")
    args = ap.parse_args(argv)
    args.lettuce_modes = args.lettuce_modes.split(",") if args.lettuce else []
    global COLLECTION
    COLLECTION = args.collection
    scenarios = SCENARIOS if "all" in args.scenarios else args.scenarios
    failed = 0
    for name in scenarios:
        print(f"{name}:", flush=True)
        t = time.time()
        try:
            if name == "redirects":
                problems = run_redirects(args)
            elif name == "metrics":
                problems = run_metrics(args)
            elif name in ("auto-grow", "leave-while-join", "sigterm", "sigterm-two", "sigterm-target", "full-target"):
                problems = run_auto(name, args)
            else:
                problems = run_scenario(name, args)
        except Fail as e:
            problems = [str(e)]
        except Exception:  # noqa: BLE001 - a bug here: report, go on
            import traceback
            problems = [traceback.format_exc()]
        for p in problems:
            print(f"  FAIL: {p}")
        print(f"  {'ok' if not problems else 'FAILED'} ({time.time() - t:.0f} s)", flush=True)
        failed += bool(problems)
    print("OK" if not failed else f"FAILED: {failed} of {len(scenarios)} scenarios")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
