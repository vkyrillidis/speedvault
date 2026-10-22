#!/usr/bin/env python3
"""Our cluster against a Redis Cluster with the same layout (run by
cluster_difftest.sh, which starts both).

    python3 cluster_difftest.py requests --redis 7851,7852,7853 --ours 7861,7862,7863
    python3 cluster_difftest.py clients  --redis 7851 --ours 7860 [--ours 7861]
    python3 cluster_difftest.py load     --ours 7860 --seconds 6
    python3 cluster_difftest.py migrating --redis 7851,7852,7853 --ours 7861,7862,7863

requests: the same requests to node i of each cluster (keys of every
  node's slots, alone, twice, with hash tags and mixed: MOVED, CROSSSLOT;
  arity errors; the CLUSTER commands; DBSIZE and COUNTKEYSINSLOT after the
  writes), RESP2 and RESP3. Our replies are compared byte for byte after
  replacing each of our nodes' address and id by those of the Redis node
  with the same slots; CLUSTER NODES with the live fields masked (as in
  cluster_compat.py); CLUSTER INFO up to cluster_size (the epochs differ).
clients: one redis-py RedisCluster script against each cluster (seeded as
  given); the results (and exceptions) must be the same.
load: writes and reads keys through RedisCluster for --seconds, then reads
  them all back; fails on any error or missing key. The shell script
  restarts stuart in the middle: clients must not notice.
migrating: a slot of the first node migrating to the second, Redis's put
  so with CLUSTER SETSLOT MIGRATING / IMPORTING, ours with a table pushed
  to the nodes directly (epoch above stuart's: run it last); requests with
  and without ASKING to the source, the target and the third node (ASK,
  TRYAGAIN, MOVED, CLUSTER NODES with the [slot->-id] markers), RESP2 and
  RESP3. Both clusters end with the slot stable again.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Dict, List, Tuple

from resp import Conn, cmd, parse
from cluster_compat import mask_nodes

HELLO_ID = re.compile(rb"\$2\r\nid\r\n:\d+\r\n")


def ports(text: str) -> List[int]:
    return [int(p) for p in text.split(",")]


def slots_view(port: int) -> Dict[int, Tuple[bytes, int, bytes]]:
    """First slot of each range -> (host, port, id), from CLUSTER SLOTS."""
    with Conn("127.0.0.1", port, timeout=5) as c:
        reply = c.call("CLUSTER", "SLOTS")
    view = {}
    for r in reply.value:
        start = r.value[0].value
        node = r.value[2].value
        view[start] = (node[0].value, node[1].value, node[2].value)
    return view


def mapping(redis: List[int], ours: List[int]) -> List[Tuple[bytes, bytes]]:
    """(ours, theirs) byte replacements: addresses, ports, ids."""
    theirs, mine = slots_view(redis[0]), slots_view(ours[0])
    if sorted(theirs) != sorted(mine):
        raise SystemExit(f"the layouts differ: {sorted(theirs)} vs {sorted(mine)}")
    pairs = []
    for start in mine:
        (h1, p1, i1), (h2, p2, i2) = mine[start], theirs[start]
        pairs += [(b"%s:%d" % (h1, p1), b"%s:%d" % (h2, p2)), (b":%d\r\n" % p1, b":%d\r\n" % p2), (i1, i2)]
    return pairs


def node_order(redis: List[int], ours: List[int]) -> List[Tuple[int, int]]:
    """Pairs (redis port, our port) of nodes with the same slots."""
    theirs, mine = slots_view(redis[0]), slots_view(ours[0])
    return [(theirs[s][1], mine[s][1]) for s in sorted(mine)]


def keys_per_node(port: int, n: int) -> List[bytes]:
    """Keys k0, k1, ... until each range has `n`, plus hash-tagged ones."""
    view = sorted(slots_view(port))
    found: Dict[int, List[bytes]] = {s: [] for s in view}
    with Conn("127.0.0.1", port, timeout=5) as c:
        i = 0
        while any(len(v) < n for v in found.values()):
            key = f"k{i}".encode()
            slot = c.call("CLUSTER", "KEYSLOT", key).value
            start = max(s for s in view if s <= slot)
            if len(found[start]) < n:
                found[start].append(key)
            i += 1
    return [k for v in found.values() for k in v]


def requests(keys: List[bytes]) -> List[Tuple]:
    probes: List[Tuple] = [("PING",), ("DBSIZE",), ("CLUSTER", "KEYSLOT", "foo"), ("CLUSTER", "SLOTS"),
                           ("CLUSTER", "SHARDS"), ("CLUSTER", "NODES"), ("CLUSTER", "INFO"), ("CLUSTER", "MYID"),
                           ("READONLY",), ("READWRITE",), ("SELECT", "1"), ("GET",), ("CLUSTER", "FOO")]
    for i, k in enumerate(keys):
        other = keys[(i + len(keys) // 3) % len(keys)]
        tag1, tag2 = b"{" + k + b"}:a", b"{" + k + b"}:b"
        probes += [("SET", k, f"v{i}"), ("GET", k), ("APPEND", k, "x"), ("STRLEN", k), ("INCR", k), ("TYPE", k),
                   ("EXPIRE", k, "1000"), ("TTL", k), ("PERSIST", k), ("SETNX", k, "y"), ("MSET", tag1, "1", tag2, "2"),
                   ("MGET", tag1, tag2, k), ("MGET", tag1, tag2), ("EXISTS", k, k), ("MGET", k, other), ("DEL", k, other),
                   ("MSET", k, "1", other, "2"), ("GETDEL", tag1), ("INCRBY", tag2, "5"), ("DEL", tag2)]
    # Hash tag edge cases: an empty tag, an unclosed one, '}' before '{'.
    for i, k in enumerate(keys[::4]):
        for edge in (b"{}" + k, b"{" + k, b"}{" + k, b"{}{" + k + b"}"):
            probes += [("SET", edge, str(i)), ("GET", edge), ("CLUSTER", "KEYSLOT", edge), ("DEL", edge)]
    # Hashes, sets, lists (phase 7): served here or redirected by their
    # key, the two-key commands CROSSSLOT across slots.
    for i, k in enumerate(keys):
        other = keys[(i + len(keys) // 3) % len(keys)]
        h, st, l, l2 = b"{" + k + b"}:h", b"{" + k + b"}:s", b"{" + k + b"}:l", b"{" + k + b"}:l2"
        probes += [("HSET", h, "b", "1", "a", "2"), ("HGETALL", h), ("HINCRBY", h, "n", "3"), ("HDEL", h, "a"),
                   ("SADD", st, "3", "1", "2"), ("SMEMBERS", st), ("SUNIONSTORE", st, st, st), ("SUNION", st, other),
                   ("SMOVE", st, other, "1"), ("RPUSH", l, "x", "y"), ("LMOVE", l, l2, "LEFT", "RIGHT"),
                   ("LRANGE", l2, "0", "-1"), ("RPOPLPUSH", l, other), ("HSCAN", h, "0"), ("SSCAN", st, "0"),
                   ("OBJECT", "ENCODING", h), ("TYPE", l2)]
    probes += [("SCAN", "0", "COUNT", "100000"), ("SCAN", "0", "MATCH", "*:h", "COUNT", "100000"),
               ("SCAN", "0", "TYPE", "list", "COUNT", "100000"), ("KEYS", "*"), ("KEYS", "*:s")]
    probes += [("CLUSTER", "COUNTKEYSINSLOT", "0"), ("DBSIZE",)]
    return probes


def compare_requests(redis: List[int], ours: List[int]) -> int:
    pairs = mapping(redis, ours)
    order = node_order(redis, ours)
    keys = keys_per_node(redis[0], 8)
    for port in redis + ours:
        with Conn("127.0.0.1", port, timeout=5) as c:
            c.call("FLUSHALL")

    def ours_as_theirs(raw: bytes) -> bytes:
        for mine, theirs in pairs:
            raw = raw.replace(mine, theirs)
        return raw

    # SHARDS and NODES list the nodes in an order of their own: Redis in its
    # hash table's (different on each node), ours in the table's. Their
    # format was compared with the order matched in cluster_compat.py; here
    # the elements are compared as sets.
    def norm(p: Tuple, raw: bytes) -> bytes:
        raw = HELLO_ID.sub(b"$2\r\nid\r\n:ID\r\n", raw)
        if p[:2] == ("CLUSTER", "NODES"):
            kind, text = mask_nodes(raw)
            return kind + "\n".join(sorted(text.split("\n"))).encode()
        if p[:2] == ("CLUSTER", "SHARDS"):
            reply, _ = parse(raw)
            return raw[:raw.index(b"\r\n")] + b"".join(sorted(e.encode() for e in reply.value))
        if p[:2] == ("CLUSTER", "INFO"):
            return b"\r\n".join(raw.split(b"\r\n")[1:8])  # cluster_state .. cluster_size
        # A node's keys in its dict's order: compared as sets.
        if p[0] in ("SCAN", "KEYS"):
            reply, _ = parse(raw)
            if reply.kind == "*" and p[0] == "SCAN":
                return reply.value[0].encode() + b"".join(sorted(e.encode() for e in reply.value[1].value))
            if reply.kind == "*":
                return b"".join(sorted(e.encode() for e in reply.value))
        return raw

    failures = total = 0
    for proto in (2, 3):
        for rport, oport in order:
            # Also the COUNTKEYSINSLOT of every key's slot, once written.
            slots = []
            with Conn("127.0.0.1", rport, timeout=5) as c:
                slots = [str(c.call("CLUSTER", "KEYSLOT", k).value) for k in keys]
            probes = requests(keys) + [("CLUSTER", "COUNTKEYSINSLOT", s) for s in slots] + [("HELLO", str(proto))]
            with Conn("127.0.0.1", rport, timeout=5) as r, Conn("127.0.0.1", oport, timeout=5) as o:
                if proto == 3:
                    r.call("HELLO", "3")
                    o.call("HELLO", "3")
                same = 0
                for p in probes:
                    r.send(cmd(*p))
                    o.send(cmd(*p))
                    rr, _ = r.read_reply()
                    oo, _ = o.read_reply()
                    a, b = norm(p, rr), norm(p, ours_as_theirs(oo))
                    total += 1
                    if a == b:
                        same += 1
                    else:
                        failures += 1
                        if failures <= 20:
                            print(f"RESP{proto} node {rport}/{oport}: {' '.join(map(str, p))}: DIFFERENT\n"
                                  f"  redis: {rr[:300]!r}\n  ours:  {oo[:300]!r}")
                print(f"requests: RESP{proto}, Redis node {rport} / our node {oport}: {same}/{len(probes)} the same")
    print(f"requests: {total - failures}/{total} the same")
    return failures


def table_args(port: int, epoch: int, migrating: List[Tuple[int, bytes, bytes]]) -> List[bytes]:
    """CLUSTER SETTABLE for our cluster's layout (from node `port`), with
    `migrating` as (slot, from id, to id)."""
    view = slots_view(port)
    with Conn("127.0.0.1", port, timeout=5) as c:
        reply = c.call("CLUSTER", "SLOTS")
    nodes = sorted({v for v in view.values()}, key=lambda v: v[1])
    args = [b"CLUSTER", b"SETTABLE", str(epoch).encode(), b"NODES", str(len(nodes)).encode()]
    for host, p, node_id in nodes:
        args += [node_id, host, str(p).encode()]
    ranges = [(r.value[0].value, r.value[1].value, r.value[2].value[2].value) for r in reply.value]
    args += [b"RANGES", str(len(ranges)).encode()]
    for start, end, node_id in ranges:
        args += [str(start).encode(), str(end).encode(), node_id]
    if migrating:
        args += [b"MIGRATING", str(len(migrating)).encode()]
        for slot, src, dst in migrating:
            args += [str(slot).encode(), src, dst]
    return args


def epoch_of(port: int) -> int:
    with Conn("127.0.0.1", port, timeout=5) as c:
        info = c.call("CLUSTER", "INFO").value
    info = info.decode() if isinstance(info, bytes) else str(info)
    return int(re.search(r"cluster_current_epoch:(\d+)", info).group(1))


def migrating_probes(t: bytes, other_a: bytes, key_b: bytes) -> List[Tuple[str, List[Tuple]]]:
    """(node, requests sent on one connection) while slot of `{t}` moves
    from node A to node B. p1, p2 exist at A; e expired at A (reclaimed);
    a* don't exist anywhere. A's requests leave it no keys of the slot. `other_a` is a key of another slot of A, `key_b` one
    of B's."""
    k = lambda name: b"{" + t + b"}" + name.encode()  # noqa: E731
    p1, p2, e, a1, a2, a3 = k("p1"), k("p2"), k("e"), k("a1"), k("a2"), k("a3")
    nodes = ("CLUSTER", "NODES")
    count = ("CLUSTER", "COUNTKEYSINSLOT", "SLOT")
    return [
        ("A", [("GET", p1), ("GET", a1), ("GET", e), ("TTL", e), ("MGET", p1, p2), ("MGET", p1, a1), ("MGET", a1, a2),
               ("MGET", a1, a1), ("MGET", p1, p1), ("EXISTS", p1, a1), ("EXISTS", a1), ("SET", a1, "x"), ("SET", p1, "new"),
               ("GET", p1), ("INCR", p1), ("APPEND", p2, "+"), ("MSET", p1, "1", a1, "2"), ("MSET", p1, "1", p2, "2"),
               ("DEL", p1, a1), ("ASKING",), ("GET", a1), ("ASKING",), ("GET", p1), ("GET", other_a), ("GET", key_b),
               ("MGET", p1, other_a), ("MGET", a1, other_a), ("GET", b"{" + t), count, nodes, ("CLUSTER", "SLOTS"),
               ("CLUSTER", "GETKEYSINSLOT", "SLOT", "0"), ("DEL", p2), ("GET", p2), ("GETDEL", p1), ("SETNX", p1, "z"), count]),
        ("B", [("GET", a1), ("SET", a1, "v"), ("ASKING",), ("GET", a1), ("ASKING",), ("SET", a1, "v"), ("ASKING",),
               ("GET", a1), ("GET", a1), ("ASKING",), ("MGET", a1, a2), ("ASKING",), ("MGET", a1, a1), ("ASKING",),
               ("MGET", a2, a3), ("ASKING",), ("MSET", a2, "1", a3, "2"), ("ASKING",), ("EXISTS", a1, a1),
               ("ASKING",), ("EXISTS", a1, a2), ("ASKING",), ("PING",), ("GET", a1), ("ASKING",), ("NOSUCH",), ("GET", a1),
               ("ASKING",), ("GET",), ("GET", a1), ("ASKING",), ("ASKING",), ("GET", a1), ("ASKING",), ("GET", key_b),
               ("ASKING",), ("GET", other_a), ("ASKING",), ("INCR", a2), ("ASKING",), ("GET", p1), ("ASKING",),
               ("DEL", a1), ("ASKING",), ("GET", a1), count, nodes, ("CLUSTER", "SLOTS"), ("READONLY",), ("ASKING",),
               ("READWRITE",), ("GET", a1)]),
        ("C", [("GET", a1), ("ASKING",), ("GET", a1), ("GET", p1), nodes]),
        # Pipelined: ASKING applies to the request right after it.
        ("B", [("ASKING",), ("SET", a3, "1"), ("GET", a3), ("ASKING",), ("GET", a3)]),
    ]


def compare_migrating(redis: List[int], ours: List[int]) -> int:
    """Slot s of node A migrating to node B, Redis's (CLUSTER SETSLOT s
    MIGRATING / IMPORTING) against ours (a table with s migrating)."""
    pairs = mapping(redis, ours)
    order = node_order(redis, ours)  # (redis, ours) for A, B, C, in slot order
    theirs, mine = slots_view(redis[0]), slots_view(ours[0])
    starts = sorted(theirs)
    rid = {theirs[s][1]: theirs[s][2] for s in starts}
    oid = {mine[s][1]: mine[s][2] for s in starts}
    (ra, oa), (rb, ob), (rc, oc) = order
    # A owns starts[0] .. starts[1] - 1, B from starts[1] up to starts[2] - 1.
    with Conn("127.0.0.1", ra, timeout=5) as c:
        def key(prefix: str, ok) -> bytes:
            return next(f"{prefix}{i}".encode() for i in range(100000) if ok(c.call("CLUSTER", "KEYSLOT", f"{prefix}{i}").value))
        tag = key("t", lambda s: s < starts[1])
        slot = c.call("CLUSTER", "KEYSLOT", tag).value
        other_a = key("o", lambda s: s < starts[1] and s != slot)
        key_b = key("b", lambda s: starts[1] <= s < starts[2])
    print(f"migrating: slot {slot} (tag {tag.decode()}) from {ra}/{oa} to {rb}/{ob}")

    def ours_as_theirs(raw: bytes) -> bytes:
        for mine, theirs in pairs:
            raw = raw.replace(mine, theirs)
        return raw

    def norm(p: Tuple, raw: bytes) -> bytes:
        if p[:2] == ("CLUSTER", "NODES"):
            kind, text = mask_nodes(raw)
            return kind + "\n".join(sorted(text.split("\n"))).encode()
        return raw

    epoch = epoch_of(oa)
    failures = total = 0
    for proto in (2, 3):
        # A fresh start: no keys, the slot stable, then the keys at A, then
        # the migration.
        for port in redis + ours:
            with Conn("127.0.0.1", port, timeout=5) as c:
                c.call("FLUSHALL")
        with Conn("127.0.0.1", ra, timeout=5) as c:
            c.call("CLUSTER", "SETSLOT", slot, "STABLE")
        with Conn("127.0.0.1", rb, timeout=5) as c:
            c.call("CLUSTER", "SETSLOT", slot, "STABLE")
        epoch += 1
        stable = table_args(oa, epoch, [])
        for port in ours:
            with Conn("127.0.0.1", port, timeout=5) as c:
                assert c.call(*stable).value == b"OK", port
        for port in (ra, oa):
            with Conn("127.0.0.1", port, timeout=5) as c:
                for name in (b"p1", b"p2"):
                    assert c.call("SET", b"{" + tag + b"}" + name, name).value == b"OK"
                assert c.call("SET", b"{" + tag + b"}e", "gone", "PX", "1").value == b"OK"
                assert c.call("SET", other_a, "o").value == b"OK"
        # The expired key reclaimed at both (active expiry, every 100 ms)
        # before the slot migrates: whether it is by the time of the
        # probes is a race otherwise. Kept but expired is in tests/cluster.rs.
        deadline = time.monotonic() + 3
        for port in (ra, oa):
            with Conn("127.0.0.1", port, timeout=5) as c:
                while c.call("CLUSTER", "COUNTKEYSINSLOT", slot).value != 2:
                    assert time.monotonic() < deadline, f"{port} keeps the expired key"
                    time.sleep(0.01)
        with Conn("127.0.0.1", rb, timeout=5) as c:
            assert c.call("CLUSTER", "SETSLOT", slot, "IMPORTING", rid[ra]).value == b"OK"
        with Conn("127.0.0.1", ra, timeout=5) as c:
            assert c.call("CLUSTER", "SETSLOT", slot, "MIGRATING", rid[rb]).value == b"OK"
        # Our source moves the slot's keys itself (Redis's waits for
        # MIGRATE): the table reaches it alone first, so that the target
        # refuses them (it doesn't import the slot yet) while A is probed.
        # A's probes leave it no keys of the slot; then the others get it.
        epoch += 1
        args = table_args(oa, epoch, [(slot, oid[oa], oid[ob])])
        with Conn("127.0.0.1", oa, timeout=5) as c:
            r = c.call(*args)
            assert r.value == b"OK", (oa, r)
        role = {"A": (ra, oa), "B": (rb, ob), "C": (rc, oc)}
        for name, probes in migrating_probes(tag, other_a, key_b):
            if name != "A" and args:
                for port in (ra, oa):
                    with Conn("127.0.0.1", port, timeout=5) as c:
                        assert c.call("CLUSTER", "COUNTKEYSINSLOT", slot).value == 0, f"{port} keeps keys of the slot"
                for port in ours:
                    if port != oa:
                        with Conn("127.0.0.1", port, timeout=5) as c:
                            r = c.call(*args)
                            assert r.value == b"OK", (port, r)
                args = None
            rport, oport = role[name]
            probes = [tuple(str(slot) if x == "SLOT" else x for x in p) for p in probes]
            with Conn("127.0.0.1", rport, timeout=5) as r, Conn("127.0.0.1", oport, timeout=5) as o:
                if proto == 3:
                    r.call("HELLO", "3")
                    o.call("HELLO", "3")
                same = 0
                pipelined = name == "B" and probes[0] == ("ASKING",) and len(probes) == 5
                if pipelined:
                    r.send(b"".join(cmd(*p) for p in probes))
                    o.send(b"".join(cmd(*p) for p in probes))
                for p in probes:
                    if not pipelined:
                        r.send(cmd(*p))
                        o.send(cmd(*p))
                    rr, _ = r.read_reply()
                    oo, _ = o.read_reply()
                    a, b = norm(p, rr), norm(p, ours_as_theirs(oo))
                    total += 1
                    if a == b:
                        same += 1
                    else:
                        failures += 1
                        if failures <= 30:
                            print(f"RESP{proto} node {name} {rport}/{oport}: {' '.join(map(str, p))}: DIFFERENT\n"
                                  f"  redis: {rr[:400]!r}\n  ours:  {oo[:400]!r}")
                print(f"migrating: RESP{proto}, node {name} ({rport}/{oport}): {same}/{len(probes)} the same")
    # Back to stable, both.
    for port in (ra, rb):
        with Conn("127.0.0.1", port, timeout=5) as c:
            c.call("CLUSTER", "SETSLOT", slot, "STABLE")
    args = table_args(oa, epoch + 1, [])
    for port in ours:
        with Conn("127.0.0.1", port, timeout=5) as c:
            c.call(*args)
    print(f"migrating: {total - failures}/{total} the same")
    return failures


def redis_py():
    path = os.environ.get("REDIS_PY_PATH", os.path.expanduser("~/venvs/redis-py/site"))
    if os.path.isdir(path):
        sys.path.insert(0, path)
    import redis  # noqa: E402
    from redis.cluster import RedisCluster  # noqa: E402
    return redis, RedisCluster


def client_script(rc) -> List[Tuple[str, str]]:
    """Operations through a RedisCluster, with their results or errors."""
    out = []

    def do(name, f):
        try:
            result = f()
        except Exception as e:  # noqa: BLE001 - the error is the result
            result = f"{type(e).__name__}: {e}"
        out.append((name, repr(result)))

    rc.flushall()
    keys = [f"user:{i}" for i in range(200)]
    for k in keys:
        do(f"set {k}", lambda k=k: rc.set(k, k.upper()))
    do("get all", lambda: [rc.get(k) for k in keys])
    do("incr", lambda: [rc.incr(f"n:{i}", i) for i in range(50)])
    do("mset_nonatomic", lambda: rc.mset_nonatomic({f"m:{i}": i for i in range(100)}))
    do("mget_nonatomic", lambda: rc.mget_nonatomic([f"m:{i}" for i in range(100)] + ["missing"]))
    do("mget one tag", lambda: rc.mget(["{t}a", "{t}b"]))
    do("mset one tag", lambda: rc.mset({"{t}a": 1, "{t}b": 2}))
    do("mget one tag after", lambda: rc.mget(["{t}a", "{t}b"]))
    do("mget across slots", lambda: rc.mget(["user:1", "user:2"]))
    do("delete across", lambda: rc.delete(*[f"user:{i}" for i in range(20)]))
    do("exists", lambda: rc.exists(*[f"user:{i}" for i in range(40)]))
    do("expire ttl", lambda: [rc.expire(f"user:{i}", 1000) for i in range(30, 40)] + [rc.ttl(f"user:{i}") for i in range(30, 40)])
    do("keyslot", lambda: [rc.keyslot(k) for k in keys[:20]])
    do("cluster_keyslot", lambda: rc.cluster_keyslot("foo"))
    do("dbsize", lambda: rc.dbsize())

    def pipe():
        p = rc.pipeline()
        for i in range(50):
            p.set(f"p:{i}", i)
            p.get(f"p:{i}")
        return p.execute()
    do("pipeline", pipe)
    do("getdel", lambda: [rc.getdel(f"p:{i}") for i in range(10)])
    do("type", lambda: [rc.type(k) for k in ["p:20", "nope"]])
    do("wrong type of int", lambda: rc.incr("user:100"))
    # Phase 7: the data types, and SCAN across the nodes.
    do("hset hgetall", lambda: [rc.hset(f"h:{i}", mapping={"b": i, "a": -i}) for i in range(20)]
       + [rc.hgetall(f"h:{i}") for i in range(20)])
    do("sadd smembers", lambda: [rc.sadd(f"s:{i}", 3, 1, 2) for i in range(20)]
       + [sorted(rc.smembers(f"s:{i}")) for i in range(20)])
    do("rpush lrange lpop", lambda: [rc.rpush(f"l:{i}", "a", "b", "c") for i in range(20)]
       + [rc.lrange(f"l:{i}", 0, -1) for i in range(20)] + [rc.lpop(f"l:{i}") for i in range(20)])
    do("lmove one tag", lambda: [rc.lmove("{q}a", "{q}b"), rc.rpush("{q}a", 1, 2), rc.lmove("{q}a", "{q}b"),
                                 rc.lrange("{q}b", 0, -1)])
    do("sunion across slots", lambda: rc.sunion("s:1", "s:2"))
    do("scan_iter", lambda: sorted(rc.scan_iter(count=1000)))
    do("scan_iter match", lambda: sorted(rc.scan_iter(match="h:*", count=1000)))
    do("scan_iter type", lambda: sorted(rc.scan_iter(_type="list", count=1000)))
    do("hscan_iter", lambda: sorted(rc.hscan_iter("h:3")))
    do("sscan_iter", lambda: sorted(rc.sscan_iter("s:3")))
    do("keys", lambda: sorted(rc.keys("s:*")))
    return out


def compare_clients(redis_port: int, our_ports: List[int]) -> int:
    redis, RedisCluster = redis_py()
    failures = 0
    for proto in (2, 3):
        theirs = RedisCluster(host="127.0.0.1", port=redis_port, protocol=proto)
        expected = client_script(theirs)
        theirs.close()
        for port in our_ports:
            ours = RedisCluster(host="127.0.0.1", port=port, protocol=proto)
            got = client_script(ours)
            nodes = len(ours.get_primaries())
            ours.close()
            diff = [(e, g) for e, g in zip(expected, got) if e != g]
            print(f"clients: redis-py {redis.__version__} RESP{proto}, seeded with {port} ({nodes} primaries): "
                  f"{len(expected) - len(diff)}/{len(expected)} results the same")
            for e, g in diff[:10]:
                print(f"  {e[0]}:\n    redis: {e[1][:300]}\n    ours:  {g[1][:300]}")
            failures += len(diff) + abs(len(expected) - len(got))
    return failures


def load(port: int, seconds: float) -> int:
    redis, RedisCluster = redis_py()
    rc = RedisCluster(host="127.0.0.1", port=port)
    rc.flushall()
    errors, written, i = [], 0, 0
    deadline = time.time() + seconds
    while time.time() < deadline:
        key = f"load:{i}"
        try:
            rc.set(key, i)
            written += 1
            if i % 10 == 0 and int(rc.get(f"load:{i // 2}")) != i // 2:
                errors.append(f"load:{i // 2} read back wrong")
        except Exception as e:  # noqa: BLE001
            errors.append(f"{key}: {type(e).__name__}: {e}")
        i += 1
    values = rc.mget_nonatomic([f"load:{j}" for j in range(written)])
    missing = [j for j, v in enumerate(values) if v is None or int(v) != j]
    print(f"load: {written} keys written in {seconds} s, {len(errors)} errors, {len(missing)} missing or wrong")
    for e in errors[:10]:
        print(f"  {e}")
    rc.close()
    return len(errors) + len(missing)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["requests", "clients", "load", "migrating"])
    ap.add_argument("--redis", help="Redis Cluster node port(s), comma separated")
    ap.add_argument("--ours", action="append", help="our node ports (requests) or a seed port (clients, load)")
    ap.add_argument("--seconds", type=float, default=6)
    args = ap.parse_args(argv)
    if args.mode == "requests":
        failures = compare_requests(ports(args.redis), ports(args.ours[0]))
    elif args.mode == "clients":
        failures = compare_clients(ports(args.redis)[0], [int(p) for p in args.ours])
    elif args.mode == "migrating":
        failures = compare_migrating(ports(args.redis), ports(args.ours[0]))
    else:
        failures = load(int(args.ours[0]), args.seconds)
    print("OK" if failures == 0 else f"FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
