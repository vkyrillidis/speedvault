#!/usr/bin/env python3
"""Slots moving under load (phase 5 milestone 6; run by migrate_check.sh,
which builds the binaries).

    python3 migrate_check.py SCENARIO... --bin DIR [--base-port 7880]
        [--keys 2000] [--filler 200000] [--lettuce CLASSPATH]

stuart moves slots by snapshot (since phase 8; phase 5 moved them key by
key).

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
               limit (its slot syncs wait unread) and is full at 90%, so
               the moves to it stop, the others keep their slots, writes to its keys
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
               their slot map is refreshed. By snapshot, -MOVED only: no
               -ASK or -TRYAGAIN
  failover     (phase 8) stuart --auto --replicas 1 and six nodes: three
               primaries with a replica each. One more writer, redis-py,
               sends each INCR with a WAIT 1 on the same connection. A
               primary is SIGKILLed: its replica must be promoted
               (--fail-after 5 s, then the 3 s lease); then the dead node
               is started again at its address (a new id: it joins as the
               new primary's replica). Every INCR the WAIT said the replica
               had must be there; those only acknowledged may be lost on
               the dead primary's slots (counted). The time from the kill
               to the first write the new primary takes is printed
  failover-stop  the same, the primary SIGSTOPped instead, past its
               failover, then SIGCONT: INCRs sent to it while it was
               stopped must all be refused once it runs again (its lease
               ran out: no write taken by both), and it comes back as the
               new primary's replica (the same id)

  move-source-failover  (phase 8) the failover setup, then a fourth
               primary and its replica join: slots move to it from the
               three. A source is SIGKILLed mid-move: its moves are called
               off (the target drops what it copied, its replica too), its
               replica promoted, the moves done again from there; the dead
               node comes back as its replica. Writers as in failover, and
               one more SETting and DELeting keys with WAIT 1. Every write
               WAIT confirmed must be there (a confirmed DEL: the key gone,
               none brought back); then every replica is compared key by
               key with its primary: type, value, expiry, nothing else
  move-target-failover  the same, the new primary SIGKILLed mid-move: its
               replica (holding the copies unserved) promoted
  move-cutover-failover  the same, the source of a link SIGKILLed as soon
               as the target lists it landing (the target took the table
               giving it the slots; the source's last writes on the way)
  move-cutover-stop  the same, the source SIGSTOPped instead, and SIGCONT
               once its replica is promoted: back with its keys under its
               id, marked failed, it must trim nothing (its replica may
               still be applying its stream) and becomes the new primary's
               replica

  sigterm-primary  (phase 8) the failover setup, writers as in
               move-source-failover, stuart serving --metrics on its port
               + 1000. A primary gets SIGTERM: it hands its slots to its
               replica (its writes paused until the replica has them all)
               and exits by itself (status 0); no slot moves (stuart's
               stuart_slots_moved_total), one handover
               (stuart_handovers_total), no write lost, not even one only
               acknowledged
  sigterm-replica  the same, a replica gets SIGTERM: it leaves at once
               (exits within --leave-linger + 5 s), no slot moves
  sigterm-no-replica  the same, a replica gets SIGTERM, then its
               primary: without a replica, its slots are drained to the
               other two primaries, as in phase 6 (each of its slots moved)
  rolling      the same with each node named (--name), SIGTERMed in turn
               and started again under its name at its address once it
               exited, as Kubernetes rolls a StatefulSet; the next once it
               is a replica again, caught up. Each primary hands over, each
               node back joins as the replica of the primary lacking one:
               no slot moves, one full sync per node, no write lost
  scale-down   (phase 8) the rolling setup scaled down as a StatefulSet
               is, a node SIGTERMed once the last exited: 6 nodes to 4,
               two replicas of different primaries going (nodes joining one
               by one pair up in join order, so the highest two would be a
               pair; a rolling restart shuffles the roles): three primaries
               are one too many, so one is demoted: its slots move to the
               other two, and it becomes the replica of the one lacking one
               (one demotion, stuart_demotions_total); then 4 to 2, the
               highest two: one primary and its replica remain. Each step
               ends with every primary having a replica, synced; writers
               and checks as in sigterm-primary
  scale-down-up  6 to 4 the same way with slower moves (--moves-per-source
               16), then, once some of the slots of the primary demoted
               have moved, two nodes started on new ports (the second one
               gone still lingers on its own): the demotion is called off
               (0 demotions), the cluster ends with three primaries even
               and their replicas

With --collection N (default 20000), each hot slot (below) also holds a
hash, a set, a list and a sorted set of N elements, so that they move in
parts (--migrate-chunk), and one more redis-py writer works on them
throughout: HINCRBY on four counter fields of each hash and ZINCRBY on four
counter members of each sorted set, SADD, RPUSH and ZADD of members
numbered in the order sent. At the end every element must be there, in
order for the lists: the original ones (the sorted set's with their
scores, one of them -0 byte for byte), every acknowledged SADD, RPUSH and
ZADD (one in doubt may be there or not, an RPUSH retried twice), each
counter within its INCRs as above.

With replicas, the replicas are left out of that and checked against
their primaries instead: once caught up, the same number of keys.

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

import math
import argparse
import os
import re
import select
import shutil
import signal
import socket
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
# --password: stuart and the nodes require it; every client logs in.
PASSWORD: Optional[str] = None
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


def requirepass() -> List[str]:
    """The flag that gives stuart or a node the password, if any."""
    return ["--requirepass", PASSWORD] if PASSWORD else []


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
            [*self.pin(self.base), f"{self.bin}/speedvault-stuart", f"{HOST}:{self.base}", "--state", os.path.join(self.dir, "state"),
             "--loglevel", "warning", *self.stuart_args, *requirepass()],
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
            [*self.pin(port), f"{self.bin}/speedvault-server", f"{HOST}:{port}", "--cluster", "--stuart", f"{HOST}:{self.base}",
             "--loglevel", "warning", *self.migrate_args, *(extra or []), *requirepass()],
            stderr=log, stdout=subprocess.DEVNULL)
        node_id = wait_for(f"node {port} to listen", lambda: text(self.node(port).execute_command("CLUSTER", "MYID")))
        wait_for(f"node {port} to join", lambda: any(l.startswith(node_id + " ") and " up " in l for l in self.stuart_nodes()))
        return node_id

    def kill_node(self, port: int):
        self.nodes[port].send_signal(signal.SIGKILL)
        self.nodes[port].wait()

    def term_node(self, port: int):
        self.nodes[port].send_signal(signal.SIGTERM)

    def signal_node(self, port: int, sig: int):
        self.nodes[port].send_signal(sig)

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
        return redis.Redis(password=PASSWORD, host=HOST, port=self.base, socket_timeout=5)

    def node(self, port: int):
        return redis.Redis(password=PASSWORD, host=HOST, port=port, socket_timeout=10)

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

    def wait_ready(self):
        """Until it can write (a client started out of process takes a
        while: a JVM on a busy CI runner well over a second)."""

    def stop(self):
        self.stopping.set()
        self.thread.join(60)
        if self.thread.is_alive():
            raise Fail(f"{self.name} didn't stop")

    def failed(self, i: int, e: str):
        self.doubt[i] += 1
        if len(self.errors) < 1000:
            self.errors.append(f"{self.names[i]}: {e}")


def start_writers(writers: List[Writer], keys: int):
    """Each from its own offset into the counters; back once all can write,
    so that what a scenario does next happens under load."""
    for j, w in enumerate(writers):
        w.start(j * keys // len(writers))
    for w in writers:
        w.wait_ready()


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
        rc = wait_for("redis-py to connect", lambda: RedisCluster(password=PASSWORD, host=HOST, port=self.seed_port))
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


class WaitWriter(Writer):
    """redis-py, each INCR sent with a WAIT 1 on the connection to the
    key's node: `safe` counts the INCRs WAIT said the replica has, which
    a failover must keep. A redirect or a failed node (in doubt) makes it
    refresh its slot map."""
    name = "redis-py + WAIT"

    def __init__(self, names: List[str], seed_port: int):
        super().__init__(names, seed_port)
        self.safe = [0] * len(names)

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(password=PASSWORD, host=HOST, port=self.seed_port))
        j = self.offset
        while not self.stopping.is_set():
            j = (j + 1) % len(self.indices)
            i = self.indices[j]
            try:
                r = rc.get_redis_connection(rc.get_node_from_key(self.names[i]))
                p = r.pipeline(transaction=False)
                p.incr(self.names[i])
                p.execute_command("WAIT", 1, 2000)
                _, replicas = p.execute()
                self.acked[i] += 1
                if replicas >= 1:
                    self.safe[i] += 1
            except Exception as e:  # noqa: BLE001 - in doubt
                self.failed(i, f"{type(e).__name__}: {e}")
                try:
                    rc.nodes_manager.initialize()
                except Exception:  # noqa: BLE001 - the next try again
                    time.sleep(0.1)
            self.ops += 1
        rc.close()


class DelWriter(Writer):
    """redis-py on the keys d:0 .. d:<n-1>, each in turn SET to its round
    number and then DELeted, each with a WAIT 1 on the connection to the
    key's node, as WaitWriter does. For every key, its last write and how
    it went (WAIT confirmed, acknowledged or in doubt): at the end a key
    whose last write was a confirmed DEL must be gone (nothing deleted
    comes back, from a copy sent again or a replica's), one whose last was
    a confirmed SET must hold its value. Its counts are its own."""
    name = "redis-py + WAIT (SET/DEL)"
    KEYS = 500

    def __init__(self, names: List[str], seed_port: int):
        super().__init__(names, seed_port, [])
        # key -> (round, "set"|"del", "safe"|"acked"|"doubt")
        self.last: Dict[str, Tuple[int, str, str]] = {}
        self.del_failures = 0

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(password=PASSWORD, host=HOST, port=self.seed_port))
        n = 0
        while not self.stopping.is_set():
            n += 1
            key, rnd = f"d:{n % self.KEYS}", n // self.KEYS
            for kind in ("set", "del"):
                try:
                    r = rc.get_redis_connection(rc.get_node_from_key(key))
                    p = r.pipeline(transaction=False)
                    if kind == "set":
                        p.set(key, str(rnd))
                    else:
                        p.delete(key)
                    p.execute_command("WAIT", 1, 2000)
                    _, replicas = p.execute()
                    self.last[key] = (rnd, kind, "safe" if replicas >= 1 else "acked")
                except Exception as e:  # noqa: BLE001 - in doubt
                    self.last[key] = (rnd, kind, "doubt")
                    self.del_failures += 1
                    if len(self.errors) < 1000:
                        self.errors.append(f"{key} {kind}: {type(e).__name__}: {e}")
                    try:
                        rc.nodes_manager.initialize()
                    except Exception:  # noqa: BLE001 - the next try again
                        time.sleep(0.1)
                    break
                self.ops += 1
        rc.close()


def check_deletes(cluster: "Cluster", w: DelWriter, lossy: Set[int]) -> List[str]:
    """DelWriter's keys, each as its last write left it (see there)."""
    problems: List[str] = []
    owners = cluster.owners()
    back, wrong, checked = [], [], 0
    for key, (rnd, kind, how) in w.last.items():
        if how == "doubt" or (how == "acked" and key_slot(key.encode()) in lossy):
            continue
        checked += 1
        v = cluster.node(owners[key_slot(key.encode())]).get(key)
        if kind == "del" and v is not None:
            back.append(f"{key}={v.decode()}")
        elif kind == "set" and v != str(rnd).encode():
            wrong.append(f"{key}={v!r} (set to {rnd})")
    if back:
        problems.append(f"{len(back)} keys deleted came back: {back[:5]}")
    if wrong:
        problems.append(f"{len(wrong)} keys set aren't as set: {wrong[:5]}")
    print(f"  checked {checked} of {len(w.last)} SET/DEL keys as last written ({w.del_failures} writes in doubt)")
    return problems


def fingerprint(r, keys: List[bytes]) -> Dict[bytes, tuple]:
    """Each key's type, value and absolute expiry, read at node `r`."""
    p = r.pipeline(transaction=False)
    for k in keys:
        p.type(k)
        p.execute_command("PEXPIRETIME", k)
    replies = p.execute()
    types = {k: (replies[2 * i], replies[2 * i + 1]) for i, k in enumerate(keys)}
    p = r.pipeline(transaction=False)
    for k in keys:
        t = types[k][0]
        t = t.decode() if isinstance(t, bytes) else t
        if t == "string":
            p.get(k)
        elif t == "hash":
            p.hgetall(k)
        elif t == "set":
            p.smembers(k)
        elif t == "list":
            p.lrange(k, 0, -1)
        else:
            p.exists(k)
    values = p.execute()
    out = {}
    for k, v in zip(keys, values):
        if isinstance(v, dict):
            v = tuple(sorted(v.items()))
        elif isinstance(v, set):
            v = tuple(sorted(v))
        elif isinstance(v, list):
            v = tuple(v)
        out[k] = (types[k][0], types[k][1], v)
    return out


def compare_replicas(cluster: "Cluster", replicas: Dict[int, int]) -> List[str]:
    """Every replica holds exactly its primary's keys, key by key: type,
    value, expiry (read after READONLY), and nothing else."""
    problems: List[str] = []
    owners = cluster.owners()
    compared = 0
    for rport, pport in sorted(replicas.items()):
        prim, rep_ = cluster.node(pport), cluster.node(rport)
        rep_.execute_command("READONLY")
        slots = [s for s, o in owners.items() if o == pport]
        differ: List[str] = []
        for chunk in range(0, len(slots), 256):
            part = slots[chunk:chunk + 256]
            keys = {}
            for name, r in (("primary", prim), ("replica", rep_)):
                p = r.pipeline(transaction=False)
                for sl in part:
                    p.execute_command("CLUSTER", "GETKEYSINSLOT", sl, 1 << 30)
                keys[name] = sorted(k for ks in p.execute() for k in ks)
            if keys["primary"] != keys["replica"]:
                only_p = set(keys["primary"]) - set(keys["replica"])
                only_r = set(keys["replica"]) - set(keys["primary"])
                differ.append(f"{len(only_p)} keys only at the primary {sorted(only_p)[:3]}, "
                              f"{len(only_r)} only at the replica {sorted(only_r)[:3]}")
                continue
            a, b = fingerprint(prim, keys["primary"]), fingerprint(rep_, keys["replica"])
            bad = [k for k in a if a[k] != b[k]]
            if bad:
                differ.append(f"{len(bad)} keys differ, e.g. {bad[0]!r}: {str(a[bad[0]])[:80]} vs {str(b[bad[0]])[:80]}")
            compared += len(a)
        # Nothing of other slots either.
        if rep_.dbsize() != prim.dbsize():
            differ.append(f"DBSIZE {rep_.dbsize()} vs its primary's {prim.dbsize()}")
        problems += [f"replica {rport} of {pport}: {d}" for d in differ[:5]]
    print(f"  replicas compared key by key with their primaries: {compared} keys")
    return problems


class RedisCliWriter(Writer):
    """One `redis-cli -c` fed on stdin; it prints `-> Redirected to slot
    ...` for each redirect it follows, then the reply. A redis-cli that
    exits or hangs (a node killed) is started again."""
    name = "redis-cli"

    def spawn(self):
        env = dict(os.environ, **({"REDISCLI_AUTH": PASSWORD} if PASSWORD else {}))
        return subprocess.Popen(["redis-cli", "-c", "-h", HOST, "-p", str(self.seed_port)], env=env,
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
        self.ready = threading.Event()

        def read():
            for l in self.proc.stderr:
                line = l.decode(errors="replace")
                self.stderr.append(line)
                if line.startswith(f"lettuce {self.mode}: ready"):
                    self.ready.set()
        threading.Thread(target=read, daemon=True).start()

    def wait_ready(self):
        # Connected, its slot map loaded. If it exits instead, stop() says why.
        deadline = time.monotonic() + 60
        while not self.ready.wait(0.1) and self.proc.poll() is None:
            if time.monotonic() > deadline:
                raise Fail(f"{self.name} not connected after 60 s")

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
            self.errors = [l.strip() for l in self.stderr if " failed: " in l]


# dbnode's --leave-linger default: how long a node that left redirects
# before it exits.
LEAVE_LINGER = 10

# Large collections in the hot slots (--collection).
COLLECTION = 0
COUNTER_FIELDS = 4


def collection_keys(slot: int) -> Tuple[str, str, str, str]:
    tag = tag_for(slot)
    return f"{{{tag}}}ch", f"{{{tag}}}cs", f"{{{tag}}}cl", f"{{{tag}}}cz"


def fill_collections(cluster: "Cluster", owners: Dict[int, int], n: int):
    """A hash (f<i>: v<i>), a set (m<i>), a list (e<i>, in order) and a
    sorted set (m<i> scoring i, m7 scoring -0) of n elements in each hot
    slot, straight to its owner."""
    if not n:
        return
    for slot in HOT_SLOTS:
        h, st, l, z = collection_keys(slot)
        r = cluster.node(owners[slot])
        for start in range(0, n, 5000):
            ids = range(start, min(n, start + 5000))
            p = r.pipeline(transaction=False)
            p.hset(h, mapping={f"f{i}": f"v{i}" for i in ids})
            p.sadd(st, *[f"m{i}" for i in ids])
            p.rpush(l, *[f"e{i}" for i in ids])
            p.execute_command("ZADD", z, *[x for i in ids for x in (zscore_of(i), f"m{i}")])
            p.execute()


def zscore_of(i: int) -> str:
    """The original score of a sorted set's m<i>, as ZSCORE writes it."""
    return "-0" if i == 7 else str(i)


class CollectionWriter(Writer):
    """A RedisCluster on the hot slots' collections, round and round:
    HINCRBY of a counter field and ZINCRBY of a counter member, SADD, RPUSH
    and ZADD of w<seq>. Its counts are its own (the counters' are zero),
    checked by check_collections."""
    name = "redis-py (collections)"

    def __init__(self, names: List[str], seed_port: int):
        super().__init__(names, seed_port, [])
        # (slot, field, kind) -> [acknowledged, in doubt]; seq -> True if
        # acknowledged, False in doubt, per (slot, kind).
        self.fields: Dict[Tuple[int, int, str], List[int]] = {}
        self.sent: Dict[Tuple[int, str], Dict[int, bool]] = {}

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(password=PASSWORD, host=HOST, port=self.seed_port))
        seq = 0
        while not self.stopping.is_set():
            seq += 1
            slot = HOT_SLOTS[seq % len(HOT_SLOTS)]
            h, st, l, z = collection_keys(slot)
            field = seq % COUNTER_FIELDS
            ops = [("hincrby", lambda: rc.hincrby(h, f"c{field}", 1)),
                   ("zincrby", lambda: rc.zincrby(z, 1, f"c{field}")),
                   ("sadd", lambda: rc.sadd(st, f"w{seq}")),
                   ("rpush", lambda: rc.rpush(l, f"w{seq}")),
                   ("zadd", lambda: rc.zadd(z, {f"w{seq}": seq}))]
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
                if kind in ("hincrby", "zincrby"):
                    self.fields.setdefault((slot, field, kind), [0, 0])[0 if ok else 1] += 1
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
        h, st, l, z = collection_keys(slot)
        r = cluster.node(owners[slot])
        got_h = {k.decode(): v.decode() for k, v in r.hgetall(h).items()}
        missing = sum(got_h.get(f"f{i}") != f"v{i}" for i in range(n))
        if missing:
            problems.append(f"{h}: {missing} of its {n} fields missing or wrong")
        for f in range(COUNTER_FIELDS):
            acked, doubt = w.fields.get((slot, f, "hincrby"), [0, 0])
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

        # The sorted set and its scores (redis-py reads them as floats:
        # -0 by its sign).
        got_z = {m.decode(): sc for m, sc in r.zrange(z, 0, -1, withscores=True)}
        want = lambda i: -0.0 if i == 7 else float(i)  # noqa: E731
        wrong = [i for i in range(n) if f"m{i}" not in got_z or got_z[f"m{i}"] != want(i)
                 or math.copysign(1, got_z[f"m{i}"]) != math.copysign(1, want(i))]
        if wrong:
            problems.append(f"{z}: {len(wrong)} of its {n} original members missing or with another score, e.g. {wrong[:5]}")
        for f in range(COUNTER_FIELDS):
            acked, doubt = w.fields.get((slot, f, "zincrby"), [0, 0])
            v = float(got_z.get(f"c{f}", 0))
            if not acked <= v <= acked + RETRIES * doubt:
                problems.append(f"{z} c{f} = {v}, {acked} ZINCRBYs acknowledged, {doubt} in doubt")
        added = w.sent.get((slot, "zadd"), {})
        lost = [q for q, ok in added.items() if ok and got_z.get(f"w{q}") != q]
        unknown = [m for m in got_z if not m.startswith(("m", "c")) and not (m.startswith("w") and int(m[1:]) in added)]
        if lost or unknown:
            problems.append(f"{z}: acknowledged ZADDs lost {lost[:5]}, members nobody added {unknown[:5]}")
    acked = sum(a for a, _ in w.fields.values()) + sum(ok for d in w.sent.values() for ok in d.values())
    print(f"  checked {4 * len(HOT_SLOTS)} collections of {n}: {acked} writes to them acknowledged, {w.collection_failures} failed")
    return problems


TX_PAIRS = 64
# Errors that say EXEC ran nothing: the transaction was refused whole.
REFUSED = ("MOVED", "ASK", "TRYAGAIN", "CLUSTERDOWN", "EXECABORT", "CROSSSLOT", "NOREPLICAS", "LOADING",
           "Transaction discarded")


def tx_keys(i: int) -> Tuple[str, str]:
    return f"{{tx:{i}}}a", f"{{tx:{i}}}b"


class TxWriter(Writer):
    """Transactions through redis-py, each on a connection to the node its
    slot map gives the pair's slot: MULTI, INCR {tx:<i>}a, INCR {tx:<i>}b,
    EXEC. One refused (EXEC's -MOVED, -TRYAGAIN, -EXECABORT...: nothing
    ran) makes it refresh its map and counts for nothing; one whose reply
    was lost is in doubt. check_transactions: the two counters of a pair
    always equal (never half a transaction, even in the slots of a node
    killed), and within acknowledged and in doubt."""
    name = "redis-py (transactions)"

    def __init__(self, names: List[str], seed_port: int):
        super().__init__(names, seed_port, [])
        # Per pair: acknowledged, in doubt.
        self.pairs = [[0, 0] for _ in range(TX_PAIRS)]
        self.refused = 0

    def run(self):
        rc = wait_for("redis-py to connect", lambda: RedisCluster(password=PASSWORD, host=HOST, port=self.seed_port))
        i = self.offset
        while not self.stopping.is_set():
            i = (i + 1) % TX_PAIRS
            a, b = tx_keys(i)
            try:
                r = rc.get_redis_connection(rc.get_node_from_key(a))
                p = r.pipeline(transaction=True)
                p.incr(a)
                p.incr(b)
                p.execute()
                self.pairs[i][0] += 1
            except Exception as e:  # noqa: BLE001 - refused, or in doubt
                ex = redis.exceptions
                redirected = (ex.MovedError, ex.AskError, ex.TryAgainError, ex.ClusterDownError, ex.ExecAbortError)
                if isinstance(e, redirected) or (isinstance(e, ex.ResponseError) and str(e).startswith(REFUSED)):
                    self.refused += 1
                else:
                    self.pairs[i][1] += 1
                    self.tx_failures += 1
                    if len(self.errors) < 1000:
                        self.errors.append(f"{a} {b}: {type(e).__name__}: {e}")
                try:
                    rc.nodes_manager.initialize()
                except Exception:  # noqa: BLE001 - the next try again
                    time.sleep(0.1)
            self.ops += 1
        rc.close()

    tx_failures = 0


def check_transactions(cluster: "Cluster", w: TxWriter, lossy: Set[int]) -> List[str]:
    problems: List[str] = []
    owners = cluster.owners()
    for i, (acked, doubt) in enumerate(w.pairs):
        a, b = tx_keys(i)
        slot = key_slot(a.encode())
        if slot not in owners:
            continue  # the dead node's, not given to any yet
        va, vb = cluster.node(owners[slot]).mget(a, b)
        va, vb = int(va or 0), int(vb or 0)
        if va != vb:
            problems.append(f"{a} = {va} but {b} = {vb}: half a transaction")
        elif slot in lossy:
            if va > acked + RETRIES * doubt:
                problems.append(f"{a} = {va}: more than its {acked} transactions acknowledged + {doubt} in doubt")
        elif not acked <= va <= acked + RETRIES * doubt:
            problems.append(f"{a} = {va}, {acked} transactions acknowledged, {doubt} in doubt")
    print(f"  checked {TX_PAIRS} transaction pairs: {sum(a for a, _ in w.pairs)} transactions acknowledged, "
          f"{w.tx_failures} in doubt, {w.refused} refused and retried")
    return problems


def make_writers(args, seed_port: int) -> List[Writer]:
    names = counter_names(args.keys)
    hot = [i for i, n in enumerate(names) if not n.startswith("k:")]
    writers: List[Writer] = [RedisPyWriter(names, seed_port), RedisPyWriter(names, seed_port), RedisCliWriter(names, seed_port),
                             RedisPyWriter(names, seed_port, hot)]
    writers[-1].name = "redis-py (hot keys)"
    if COLLECTION:
        writers.append(CollectionWriter(names, seed_port))
    writers.append(TxWriter(names, seed_port))
    # Lettuce wants a node as its seed (see LettuceWriter): the first one,
    # unless told another.
    for mode in args.lettuce_modes:
        writers.append(LettuceWriter(names, getattr(args, "lettuce_port", None) or seed_port + 1, args.lettuce, mode))
    return writers


# The check.
def check(cluster: Cluster, writers: List[Writer], filler: int, lossy: Set[int], mem_keys: int = 0,
          replicas: Set[int] = frozenset()) -> List[str]:
    problems: List[str] = []
    owners = cluster.owners()
    where: Dict[bytes, List[int]] = {}
    values: Dict[Tuple[bytes, int], Optional[bytes]] = {}
    for port in [p for p in cluster.node_ports() if p not in replicas]:
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
    lost = lost_incrs = 0
    for i, name in enumerate(names):
        k = name.encode()
        acked = sum(w.acked[i] for w in writers)
        doubt = sum(w.doubt[i] for w in writers)
        safe = sum(w.safe[i] for w in writers if isinstance(w, WaitWriter))
        raw = values[(k, where[k][0])] if k in where else b"0"
        v = int(raw) if raw is not None else -1
        if key_slot(k) in lossy:
            if k not in where:
                lost += 1
            lost_incrs += max(0, acked - v)
            if v > acked + RETRIES * doubt:
                wrong.append(f"{name} = {v}: more than its {acked} INCRs acknowledged + {doubt} in doubt")
            if v < safe:
                wrong.append(f"{name} = {v}: fewer than the {safe} INCRs WAIT said the replica had")
        elif not acked <= v <= acked + RETRIES * doubt:
            wrong.append(f"{name} = {v}, {acked} INCRs acknowledged, {doubt} in doubt")
    problems += wrong[:10]
    if len(wrong) > 10:
        problems.append(f"... {len(wrong)} counters wrong")
    lossy_keys = sum(key_slot(n.encode()) in lossy for n in names)
    for w in writers:
        if isinstance(w, CollectionWriter):
            problems += check_collections(cluster, w, lossy)
        if isinstance(w, TxWriter):
            problems += check_transactions(cluster, w, lossy)
    print(f"  checked: {len(where)} keys on {len(cluster.node_ports())} nodes, {sum(map(len, where.values())) - len(where)} twice; "
          f"{len(names)} counters, {sum(sum(w.acked) for w in writers)} INCRs acknowledged, {sum(sum(w.doubt) for w in writers)} in doubt"
          + (f"; {len(lossy)} slots of the dead node: {lost} of its {lossy_keys} counters lost, "
             f"{lost_incrs} acknowledged INCRs" if lossy else ""))
    return problems


def report_writers(writers: List[Writer], may_fail: bool) -> List[str]:
    problems = []
    for w in writers:
        failed = sum(w.doubt) + (w.collection_failures if isinstance(w, CollectionWriter) else 0) - w.client_bugs
        failed += w.tx_failures if isinstance(w, TxWriter) else 0
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


def stuart_lines(cluster: Cluster) -> Dict[str, str]:
    """STUART NODES by node id."""
    return {l.split()[0]: l for l in cluster.stuart_nodes() if not l.startswith("unassigned")}


def replica_ports(cluster: Cluster) -> Dict[int, int]:
    """Replica port -> its primary's port, from STUART NODES."""
    lines = stuart_lines(cluster)
    port = {i: int(l.split()[1].rsplit(":", 1)[1]) for i, l in lines.items()}
    return {port[i]: port[m.group(1)] for i, l in lines.items() if (m := re.search(r" replica-of=(\S+)", l))}


def caught_up(cluster: Cluster, replicas: Dict[int, int]) -> bool:
    for r, p in replicas.items():
        try:
            if cluster.node(r).info("replication").get("slave_repl_offset") != cluster.node(p).info("replication")["master_repl_offset"]:
                return False
        except Exception:  # noqa: BLE001 - not up yet
            return False
    return True


def pod_name(port: int) -> List[str]:
    """`--name` for the node at `port`, as a Kubernetes pod's: the same
    when it restarts."""
    return ["--name", f"pod-{port}"]


def synced(cluster: Cluster, port: int) -> bool:
    """Whether node `port` is a replica that has its copy and follows its
    primary's stream (writes go on: its offset trails a little)."""
    primary = replica_ports(cluster).get(port)
    if primary is None:
        return False
    mine = cluster.node(port).info("replication")
    if mine.get("master_port") != primary or mine.get("master_link_status") != "up" or mine.get("master_sync_in_progress"):
        return False
    theirs = cluster.node(primary).info("replication")
    return any(isinstance(v, dict) and v.get("port") == port and v.get("state") == "online" for v in theirs.values())


def setup_replicas(args, stuart_args: Optional[List[str]] = None, named: bool = False) -> Tuple[Cluster, List[int]]:
    """stuart --auto --replicas 1 and six nodes joining one after the
    other: three primaries with the slots, evened out, each with a replica
    that has caught up. `named`: each node with a --name (pod_name)."""
    cluster = Cluster(args.bin, args.base_port, args.migrate_args, ["--auto", "--replicas", "1", *(stuart_args or [])])
    ports = [args.base_port + 1 + i for i in range(6)]
    try:
        for port in ports:
            cluster.start_node(port, pod_name(port) if named else None)

        def even():
            counts = [int(m.group(1)) for l in stuart_lines(cluster).values() if "replica-of" not in l and (m := re.search(r" slots=(\d+)", l))]
            return len(counts) == 3 and sum(counts) == SLOTS and max(counts) - min(counts) <= 1 and not cluster.migrations()
        cluster.wait_settled()
        wait_for("three primaries to have their share of the slots", even, 120, 0.1)
        cluster.wait_settled()
        if args.filler:
            cluster.fill(args.filler)
        wait_for("the replicas to catch up", lambda: len(replica_ports(cluster)) == 3 and caught_up(cluster, replica_ports(cluster)), 60, 0.1)
    except BaseException:
        cluster.stop(args.keep)
        raise
    return cluster, ports


def run_failover(name: str, args) -> List[str]:
    writers = make_writers(args, args.base_port)
    names = writers[0].names
    writers.append(WaitWriter(names, args.base_port))
    cluster, ports = setup_replicas(args)
    problems: List[str] = []
    keep = False
    try:
        replicas = replica_ports(cluster)
        # Not the first node: Lettuce's seed.
        victim = next(p for p in sorted(set(replicas.values())) if p != ports[0])
        heir = next(r for r, p in replicas.items() if p == victim)
        victim_id = text(cluster.node(victim).execute_command("CLUSTER", "MYID"))
        heir_id = text(cluster.node(heir).execute_command("CLUSTER", "MYID"))
        lossy = slots_of(cluster, victim_id)
        probe_key = next(f"{{probe{i}}}" for i in range(100000) if key_slot(f"{{probe{i}}}".encode()) in lossy)
        start_writers(writers, args.keys)
        time.sleep(2)
        probe = None
        t0 = time.monotonic()
        if name == "failover":
            cluster.kill_node(victim)
            print(f"  primary {victim} SIGKILLed; its replica {heir}")
        else:
            probe = socket.create_connection((HOST, victim), timeout=30)
            if PASSWORD:
                probe.sendall(b"*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n" % (len(PASSWORD), PASSWORD.encode()))
                assert probe.recv(5) == b"+OK\r\n"
            cluster.signal_node(victim, signal.SIGSTOP)
            print(f"  primary {victim} SIGSTOPped; its replica {heir}")
            time.sleep(0.5)
            # Sent while it's stopped: read once it runs again.
            probe.sendall(b"".join(b"*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n" % (len(probe_key), probe_key.encode()) for _ in range(100)))
        heir_node = cluster.node(heir)
        wait_for("the replica to be promoted", lambda: f" slots={len(lossy)} " in stuart_lines(cluster)[heir_id], 60, 0.02)
        promoted = time.monotonic() - t0

        def serves() -> bool:
            try:
                return heir_node.set(probe_key + ":served", "1") is True
            except Exception:  # noqa: BLE001 - not yet
                return False
        wait_for("the new primary to take a write", serves, 30, 0.01)
        served = time.monotonic() - t0
        print(f"  promoted after {promoted:.1f} s, its first write taken after {served:.1f} s")
        if name == "failover":
            time.sleep(1)
            cluster.start_node(victim)
            print("  the dead node started again at its address (a new id)")
        else:
            time.sleep(1)
            cluster.signal_node(victim, signal.SIGCONT)
            replies = []
            buf = b""
            while len(replies) < 100:
                chunk = probe.recv(65536)
                if not chunk:
                    break
                buf += chunk
                *done, buf = buf.split(b"\r\n")
                replies += done
            taken = [r for r in replies if r.startswith(b":")]
            print(f"  after SIGCONT, of the 100 INCRs sent while it was stopped: {len(taken)} taken, "
                  f"{sum(r.startswith(b'-NOREPLICAS') for r in replies)} -NOREPLICAS, {sum(r.startswith(b'-MOVED') for r in replies)} -MOVED")
            if taken or len(replies) < 100:
                problems.append(f"the stopped primary, back, took {len(taken)} of the INCRs sent while it was stopped ({len(replies)} replies)")
            probe.close()
        wait_for("the old node to be the new primary's replica", lambda: replica_ports(cluster).get(victim) == heir, 60, 0.1)
        cluster.wait_settled()
        time.sleep(1)
        for w in writers:
            w.stop()
        replicas = replica_ports(cluster)
        wait_for("the replicas to catch up", lambda: caught_up(cluster, replicas), 60, 0.1)
        for r, p in replicas.items():
            if cluster.node(r).dbsize() != cluster.node(p).dbsize():
                problems.append(f"replica {r} has {cluster.node(r).dbsize()} keys, its primary {p} {cluster.node(p).dbsize()}")
        problems += report_writers(writers, may_fail=True)
        problems += check(cluster, writers, args.filler, lossy, replicas=set(replicas))
        if cluster.node(heir).get(probe_key) is not None:
            problems.append(f"{probe_key}, only ever sent to the stopped primary, is at the new one")
    except Fail as e:
        problems.append(str(e))
    finally:
        keep = bool(problems) and args.keep
        for w in writers:
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            w.stopping.set()
        for p in cluster.nodes.values():
            if p.poll() is None:
                p.send_signal(signal.SIGCONT)
        cluster.stop(keep)
    return problems


def run_move_failover(name: str, args) -> List[str]:
    """move-source-failover, move-target-failover, move-cutover-failover,
    move-cutover-stop: see the docs."""
    writers = make_writers(args, args.base_port)
    names = writers[0].names
    writers.append(WaitWriter(names, args.base_port))
    dels = DelWriter(names, args.base_port)
    writers.append(dels)
    # Fewer slots at a time than stuart's default (4096 slots of 50k keys
    # move in 0.2 s at 128 per source, before the new primary's replica
    # has synced): the moves must still be under way to be interrupted.
    cluster, ports = setup_replicas(args, ["--moves-per-source", "16", "--max-moves", "64"])
    problems: List[str] = []
    try:
        start_writers(writers, args.keys)
        time.sleep(2)
        t0 = time.monotonic()
        # A fourth primary and its replica: slots move to it from the three.
        target, target_replica = args.base_port + 7, args.base_port + 8
        target_id = cluster.start_node(target)
        cluster.start_node(target_replica)
        wait_for("the new primary's replica", lambda: replica_ports(cluster).get(target_replica) == target, 30, 0.01)
        ids = {p: text(cluster.node(p).execute_command("CLUSTER", "MYID")) for p in cluster.node_ports()}
        port_by_id = {i: p for p, i in ids.items()}
        stop = name == "move-cutover-stop"
        if name in ("move-cutover-failover", "move-cutover-stop"):
            # The source of a link landing (the target took the table that
            # gives it the slots, the source's last writes on the way): it
            # dies then.
            watch = redis.Redis(password=PASSWORD, host=HOST, port=target)

            def landing():
                for line in text(watch.execute_command("CLUSTER", "SLOTSYNCS")).splitlines():
                    f = line.split()
                    if f[1] == "landing":
                        return f[0]
                return None
            victim_id = wait_for("a cutover", landing, 60, 0)
        else:
            def mid_move():
                m = [x for x in moving(cluster) if x[2] == target_id]
                return m if slot_count(cluster, target_id) >= 256 and m else None
            seen = wait_for("slots moving to the new primary", mid_move, 60, 0.005)
            victim_id = seen[0][1] if name == "move-source-failover" else target_id
        victim = port_by_id[victim_id]
        replicas = replica_ports(cluster)
        heir = next(r for r, p in replicas.items() if p == victim)
        lossy = slots_of(cluster, victim_id) | {s for s, f, t in moving(cluster) if victim_id in (f, t)}
        if stop:
            cluster.signal_node(victim, signal.SIGSTOP)
        else:
            cluster.kill_node(victim)
        lossy |= slots_of(cluster, victim_id)
        kind = {"move-source-failover": "a source", "move-target-failover": "the target",
                "move-cutover-failover": "the source of a link landing",
                "move-cutover-stop": "the source of a link landing"}[name]
        print(f"  {kind}, primary {victim}, {'SIGSTOPped' if stop else 'SIGKILLed'} after {time.monotonic() - t0:.1f} s, "
              f"{slot_count(cluster, target_id)} slots at the new primary; its replica {heir}")
        heir_id = ids[heir]
        wait_for("the replica to be promoted", lambda: "replica-of" not in stuart_lines(cluster)[heir_id], 60, 0.02)
        print(f"  promoted after {time.monotonic() - t0:.1f} s")
        time.sleep(1)
        if stop:
            # Back with its keys (the same id): failed, it trims nothing,
            # and becomes the new primary's replica.
            cluster.signal_node(victim, signal.SIGCONT)
        else:
            cluster.start_node(victim)
        wait_for("the old node to be the new primary's replica", lambda: replica_ports(cluster).get(victim) == heir, 60, 0.1)

        def even():
            counts = [int(m.group(1)) for l in stuart_lines(cluster).values()
                      if "replica-of" not in l and (m := re.search(r" slots=(\d+)", l))]
            return len(counts) == 4 and sum(counts) == SLOTS and max(counts) - min(counts) <= 1 and not cluster.migrations()
        cluster.wait_settled(120)
        wait_for("four primaries with their share of the slots", even, 120, 0.1)
        cluster.wait_settled(120)
        print(f"  moves done in {time.monotonic() - t0:.1f} s")
        time.sleep(1)
        for w in writers:
            w.stop()
        replicas = replica_ports(cluster)
        wait_for("the replicas to catch up", lambda: caught_up(cluster, replicas), 60, 0.1)
        # Slots given up are trimmed in the background: let them go.
        time.sleep(1)
        problems += report_writers([w for w in writers if w is not dels], may_fail=True)
        problems += check(cluster, writers, args.filler, lossy, replicas=set(replicas))
        problems += check_deletes(cluster, dels, lossy)
        problems += compare_replicas(cluster, replicas)
    except Fail as e:
        problems.append(str(e))
    finally:
        keep = bool(problems) and args.keep
        for w in writers:
            if isinstance(w, LettuceWriter) and w.proc.poll() is None:
                w.proc.kill()
            w.stopping.set()
        for proc in cluster.nodes.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGCONT)
        cluster.stop(keep)
    return problems


def primaries(cluster: Cluster) -> Dict[int, str]:
    """Port -> id of the primaries owning slots, from STUART NODES."""
    lines = stuart_lines(cluster)
    return {int(l.split()[1].rsplit(":", 1)[1]): i for i, l in lines.items()
            if "replica-of" not in l and (m := re.search(r" slots=(\d+)", l)) and int(m.group(1))}


class Syncs:
    """The full syncs the primaries served (INFO stats sync_full), also
    of the nodes gone: read before each one is stopped."""

    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self.seen: Dict[int, int] = {}  # pid -> its sync_full
        self.at_start = self.read()

    def read(self) -> int:
        for port, proc in self.cluster.nodes.items():
            try:
                self.seen[proc.pid] = int(self.cluster.node(port).info("stats")["sync_full"])
            except Exception:  # noqa: BLE001 - gone: its last reading stands
                pass
        return sum(self.seen.values())


def handover_writers(args) -> Tuple[List[Writer], DelWriter]:
    writers = make_writers(args, args.base_port)
    names = writers[0].names
    writers.append(WaitWriter(names, args.base_port))
    dels = DelWriter(names, args.base_port)
    writers.append(dels)
    return writers, dels


def run_handover(name: str, args) -> List[str]:
    """sigterm-primary, sigterm-replica, sigterm-no-replica, rolling: see
    the docs."""
    writers, dels = handover_writers(args)
    stuart_metrics = metrics_port(args.base_port)
    rolling = name == "rolling"
    cluster, ports = setup_replicas(args, ["--metrics", f"{HOST}:{stuart_metrics}"], named=rolling)
    problems: List[str] = []

    def stuart_counts() -> Tuple[float, float]:
        m = scrape(stuart_metrics)
        return one(m, "stuart_slots_moved_total"), one(m, "stuart_handovers_total")

    def stop(port: int, t0: float, what: str) -> float:
        """SIGTERM to `port`; waits for its exit (status 0); how long it
        took."""
        syncs.read()
        cluster.term_node(port)
        status = cluster.wait_exit(port)
        took = time.monotonic() - t0
        print(f"  {what} {port}: SIGTERM, exited with status {status} after {took:.1f} s")
        if status != 0:
            problems.append(f"node {port} exited with status {status}")
        return took

    # Five or six nodes: three primaries; four (one drained): two.
    n_primaries = 2 if name == "sigterm-no-replica" else 3

    def primaries_settled():
        counts = [int(m.group(1)) for l in stuart_lines(cluster).values()
                  if "replica-of" not in l and (m := re.search(r" slots=(\d+)", l))]
        return sum(counts) == SLOTS and len([c for c in counts if c]) == n_primaries and not cluster.migrations()

    try:
        syncs = Syncs(cluster)
        moved0, handed0 = stuart_counts()
        start_writers(writers, args.keys)
        time.sleep(2)
        replicas = replica_ports(cluster)
        # Not the first node (Lettuce's seed), except in a rolling restart.
        primary = next(p for p in sorted(set(replicas.values())) if p != ports[0])
        replica = next(r for r, p in replicas.items() if p == primary)
        expect_moved, expect_handed = 0, 0
        t0 = time.monotonic()
        if name == "sigterm-primary":
            held = slots_of(cluster, primaries(cluster)[primary])
            stop(primary, t0, "primary")
            expect_handed = 1
            heir = stuart_lines(cluster)[text(cluster.node(replica).execute_command("CLUSTER", "MYID"))]
            if "replica-of" in heir or f" slots={len(held)} " not in heir:
                problems.append(f"its replica {replica} doesn't own its {len(held)} slots: {heir}")
        elif name == "sigterm-replica":
            took = stop(replica, t0, "replica")
            if took > LEAVE_LINGER + 5:
                problems.append(f"the replica took {took:.1f} s to leave")
        elif name == "sigterm-no-replica":
            # Its replica leaves first: then it has none, and is drained.
            stop(replica, t0, "replica")
            expect_moved = len(slots_of(cluster, primaries(cluster)[primary]))
            stop(primary, time.monotonic(), "primary, without a replica,")
        else:
            # Each node in turn, as Kubernetes rolls a StatefulSet: SIGTERM,
            # restarted under its name once it exited, ready (a replica
            # caught up) before the next.
            for port in ports:
                role = "primary" if port in primaries(cluster) else "replica"
                expect_handed += role == "primary"
                t1 = time.monotonic()
                stop(port, t1, role)
                cluster.start_node(port, pod_name(port))
                wait_for(f"node {port} to be a replica again, synced", lambda: synced(cluster, port), 60, 0.05)
                print(f"    back as the replica of {replica_ports(cluster)[port]} after {time.monotonic() - t1:.1f} s")
        cluster.wait_settled()
        wait_for(f"{n_primaries} primaries with all the slots", primaries_settled, 120, 0.1)
        cluster.wait_settled()
        print(f"  done after {time.monotonic() - t0:.1f} s")
        time.sleep(1)
        for w in writers:
            w.stop()
        moved1, handed1 = stuart_counts()
        full = syncs.read() - syncs.at_start
        print(f"  {moved1 - moved0:.0f} slots moved, {handed1 - handed0:.0f} handovers, {full} full syncs served")
        if moved1 - moved0 != expect_moved:
            problems.append(f"{moved1 - moved0:.0f} slots moved, expected {expect_moved}")
        if handed1 - handed0 != expect_handed:
            problems.append(f"{handed1 - handed0:.0f} handovers, expected {expect_handed}")
        if rolling and full != len(ports):
            problems.append(f"{full} full syncs in a rolling restart of {len(ports)} nodes, expected one each")
        replicas = replica_ports(cluster)
        wait_for("the replicas to catch up", lambda: caught_up(cluster, replicas), 60, 0.1)
        time.sleep(1)
        # A node going away: a client that never refreshes its slot map
        # (Lettuce "off") still sends it what it has in flight then; those
        # fail, on the connection only (see run_auto). The WAIT writers,
        # sending to the node they think owns the key, get -MOVED from a
        # primary that handed its slots over, or lose its connection (in
        # doubt, refreshed).
        gone = [w for w in writers if (isinstance(w, LettuceWriter) and w.mode == "off") or isinstance(w, (WaitWriter, DelWriter))]
        problems += report_writers([w for w in writers if w not in gone and w is not dels], may_fail=False)
        problems += report_writers([w for w in gone if w is not dels], may_fail=True)
        for w in gone:
            other = [e for e in w.errors if not re.search(r"Connection reset|Connection refused|timed out|Connection closed|"
                                                          r"MOVED|MovedError|Connection|Broken pipe", e)]
            if other:
                problems.append(f"{w.name}: {len(other)} writes failed otherwise than on a node gone: {other[:3]}")
        problems += check(cluster, writers, args.filler, set(), replicas=set(replicas))
        problems += check_deletes(cluster, dels, set())
        problems += compare_replicas(cluster, replicas)
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


def run_scale(name: str, args) -> List[str]:
    """scale-down, scale-down-up: see the docs."""
    writers, dels = handover_writers(args)
    stuart_metrics = metrics_port(args.base_port)
    slow = ["--moves-per-source", "16", "--max-moves", "64"] if name == "scale-down-up" else []
    cluster, ports = setup_replicas(args, ["--metrics", f"{HOST}:{stuart_metrics}", *slow], named=True)
    problems: List[str] = []

    def demotions() -> float:
        return one(scrape(stuart_metrics), "stuart_demotions_total")

    def stop(port: int):
        """SIGTERM to `port`, as a scale-down: waits for its exit (status 0)."""
        role = "replica" if port in replica_ports(cluster) else "primary"
        t1 = time.monotonic()
        cluster.term_node(port)
        status = cluster.wait_exit(port)
        print(f"  {role} {port}: SIGTERM, exited with status {status} after {time.monotonic() - t1:.1f} s")
        if status != 0:
            problems.append(f"node {port} exited with status {status}")

    def shape(n_primaries: int):
        """n primaries holding every slot, each with a replica (as many as
        there are nodes for), nothing moving, nobody demoting, the
        replicas synced."""
        lines = stuart_lines(cluster)
        counts = [int(re.search(r" slots=(\d+)", l).group(1)) for l in lines.values() if "replica-of" not in l]
        replicas = replica_ports(cluster)
        return (len(counts) == n_primaries and sum(counts) == SLOTS and all(counts)
                and len(set(replicas.values())) == len(replicas) == len(lines) - n_primaries
                and not any(" demoting" in l for l in lines.values()) and not cluster.migrations()
                and all(synced(cluster, r) for r in replicas))

    def settle(n_primaries: int, t0: float):
        wait_for(f"{n_primaries} primaries, each with a replica", lambda: shape(n_primaries), 120, 0.1)
        cluster.wait_settled()
        replicas = replica_ports(cluster)
        print(f"  {n_primaries} primaries with a replica each after {time.monotonic() - t0:.1f} s: {replicas} (replica: primary)")

    try:
        moved0 = one(scrape(stuart_metrics), "stuart_slots_moved_total")
        start_writers(writers, args.keys)
        time.sleep(2)
        t0 = time.monotonic()
        # Nodes joining one by one pair up in join order (7881 and its
        # replica 7882...), so the highest two would be a pair: after a
        # rolling restart roles no longer follow the pods' order. Two
        # replicas of different primaries go, the highest first, the next
        # once the first exited, as a StatefulSet scales down.
        replicas = replica_ports(cluster)
        victims = sorted((r for r, p in replicas.items() if r != ports[0] and p != ports[0]), reverse=True)[:2]
        stop(victims[0])
        if name == "scale-down":
            stop(victims[1])
            settle(2, t0)
            if demotions() != 1:
                problems.append(f"{demotions():.0f} demotions to go from 6 nodes to 4, expected 1")
            # Then 4 to 2, the highest two: whatever their roles, one
            # primary and its replica remain.
            t0 = time.monotonic()
            for port in sorted(cluster.node_ports(), reverse=True)[:2]:
                stop(port)
            settle(1, t0)
        else:
            # Scaled up again while a primary is demoted: the demotion is
            # called off, the new nodes join as replicas. The second
            # replica left at once, but lingers on its port (--leave-linger)
            # longer than the demotion takes: the new nodes on new ports.
            cluster.term_node(victims[1])
            demoting = wait_for("a primary to be demoted",
                                lambda: next((i for i, l in stuart_lines(cluster).items() if " demoting" in l), None), 30, 0.005)
            held = slot_count(cluster, demoting)
            # Mid-move: some of its slots gone, more to go.
            wait_for("some of its slots to move", lambda: slot_count(cluster, demoting) < held and moving(cluster), 30, 0.005)
            print(f"  a primary demoted after {time.monotonic() - t0:.1f} s, {held - slot_count(cluster, demoting)} of its "
                  f"{held} slots moved; scaling up again")
            for port in (args.base_port + 7, args.base_port + 8):
                cluster.start_node(port, pod_name(port))
            if cluster.wait_exit(victims[1]) != 0:
                problems.append(f"node {victims[1]} didn't exit with status 0")
            settle(3, t0)
            if demotions() != 0:
                problems.append(f"{demotions():.0f} demotions, expected the one started to be called off")
        time.sleep(1)
        for w in writers:
            w.stop()
        moved = one(scrape(stuart_metrics), "stuart_slots_moved_total") - moved0
        print(f"  {moved:.0f} slots moved, {demotions():.0f} demotions")
        replicas = replica_ports(cluster)
        wait_for("the replicas to catch up", lambda: caught_up(cluster, replicas), 60, 0.1)
        time.sleep(1)
        # Nodes going away, as in run_handover.
        gone = [w for w in writers if (isinstance(w, LettuceWriter) and w.mode == "off") or isinstance(w, (WaitWriter, DelWriter))]
        problems += report_writers([w for w in writers if w not in gone and w is not dels], may_fail=False)
        problems += report_writers([w for w in gone if w is not dels], may_fail=True)
        for w in gone:
            other = [e for e in w.errors if not re.search(r"Connection reset|Connection refused|timed out|Connection closed|"
                                                          r"MOVED|MovedError|Connection|Broken pipe", e)]
            if other:
                problems.append(f"{w.name}: {len(other)} writes failed otherwise than on a node gone: {other[:3]}")
        problems += check(cluster, writers, args.filler, set(), replicas=set(replicas))
        problems += check_deletes(cluster, dels, set())
        problems += compare_replicas(cluster, replicas)
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
        start_writers(writers, args.keys)
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
        seen: List[Tuple[int, str, str]] = []
        if name != "rebalance" and name != "drain":
            # The moves seen then: by snapshot, none may be listed a moment
            # later (one table ended them, the next starts more).
            seen = wait_for("a quarter of the slots to move", quarter, 30, 0.005)
        if name == "stuart-kill":
            moving_then = len(moving(cluster))
            cluster.kill_stuart()
            print(f"  stuart killed after {time.time() - t0:.1f} s with {moving_then} slots moving; down 1 s")
            time.sleep(1)
            cluster.start_stuart()
        elif name in ("source-kill", "target-kill", "drain-target-kill"):
            slot, src, dst = seen[0]
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
        start_writers(writers, args.keys)
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
            # As it would after a SIGTERM: it redirects for --leave-linger
            # before it goes, so that clients refreshing their slot map now
            # and then (Lettuce's default) have stopped using it.
            time.sleep(LEAVE_LINGER)
            cluster.kill_node(ports[0])
            del cluster.nodes[ports[0]]
        time.sleep(1)
        for w in writers:
            w.stop()
        full = name == "full-target"
        # A node goes away: a client that never refreshes its slot map
        # (Lettuce "off", Lettuce 6's default) still sends it what it has
        # in flight then, and its next command for those slots, whatever the
        # linger. Those fail (in doubt), with a connection error only.
        gone = [w for w in writers if name.startswith("sigterm") or name == "leave-while-join"
                if isinstance(w, LettuceWriter) and w.mode == "off"]
        problems += report_writers([w for w in writers if w not in gone], may_fail=full)
        if gone:
            problems += report_writers(gone, may_fail=True)
            for w in gone:
                other = [e for e in w.errors if not re.search(r"Connection reset|Connection refused|timed out|Connection closed", e)]
                if other:
                    problems.append(f"{w.name}: {len(other)} INCRs failed otherwise than on a node gone: {other[:3]}")
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


def redirect_counts(cluster: Cluster, kinds=("moved", "ask", "tryagain")) -> int:
    total = 0
    for port in cluster.node_ports():
        try:
            info = cluster.info(port)
        except Exception:  # noqa: BLE001
            continue
        total += sum(info.get(f"cluster_redirects_{k}", 0) for k in kinds)
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
            w.wait_ready()
            time.sleep(2)  # map loaded
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
            # By snapshot a slot changes owner in one table: -MOVED only.
            asked = redirect_counts(cluster, ("ask", "tryagain"))
            if asked:
                mine.append(f"{client}: {asked} -ASK/-TRYAGAIN with moves by snapshot")
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


def run_queue_seconds(pid: int) -> float:
    """The time a process's threads spent runnable but not running
    (/proc/PID/task/*/schedstat, its second field); 0 where the kernel
    doesn't say."""
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return 0.0
    total = 0
    for tid in tids:
        try:
            with open(f"/proc/{pid}/task/{tid}/schedstat") as f:
                total += int(f.read().split()[1])
        except (OSError, IndexError, ValueError):
            pass  # a thread gone since, or no schedstat
    return total / 1e9


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
        start_writers(writers, args.keys)
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
            # Busy is wall time: a node descheduled mid-turn stays busy
            # without using CPU, so on a crowded host (a CI runner of two
            # cores) busy can pass CPU by the time it spent runnable, not
            # running. Read after the scrape, that bound only grows.
            queued = run_queue_seconds(cluster.nodes[port].pid)
            if not 0 < busy <= (cpu + queued) * 1.05 + 0.05:
                problems.append(f"node {port}: busy {busy} s, CPU {cpu} s, {queued:.2f} s waiting for a CPU")
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
             "leave-while-join", "sigterm", "sigterm-two", "sigterm-target", "full-target", "metrics", "redirects",
             "failover", "failover-stop", "move-source-failover", "move-target-failover", "move-cutover-failover",
             "move-cutover-stop", "sigterm-primary", "sigterm-replica", "sigterm-no-replica", "rolling", "scale-down",
             "scale-down-up"]


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
    ap.add_argument("--lettuce-port", type=int, dest="lettuce_port", help="the node Lettuce takes as its seed (default: the first)")
    ap.add_argument("--keep", action="store_true", help="keep the logs of a failed scenario")
    ap.add_argument("--password", help="stuart and the nodes require this password (--requirepass); every client logs in")
    args = ap.parse_args(argv)
    args.lettuce_modes = args.lettuce_modes.split(",") if args.lettuce else []
    global COLLECTION, PASSWORD
    COLLECTION = args.collection
    PASSWORD = args.password
    if PASSWORD and args.lettuce:
        ap.error("--password doesn't log Lettuce in")
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
            elif name in ("failover", "failover-stop"):
                problems = run_failover(name, args)
            elif name.startswith("move-"):
                problems = run_move_failover(name, args)
            elif name in ("sigterm-primary", "sigterm-replica", "sigterm-no-replica", "rolling"):
                problems = run_handover(name, args)
            elif name in ("scale-down", "scale-down-up"):
                problems = run_scale(name, args)
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
