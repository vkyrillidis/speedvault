"""Memory per element of hashes, sets, lists and sorted sets, ours against
Redis and Valkey (phase 7 milestone 7, sorted sets since phase 9): used_memory growth divided by the elements,
for 10000 small collections of 100 elements (within Redis's listpack
limits; sets of non-integers, which Redis 7.2+ also packs, and of
integers, an intset while small) and for one of 1M. Elements: 8-byte
strings (a hash: field and value of 8 bytes each; integers up to 7 digits;
a sorted set: a member of 9 bytes, `m` and 8 digits, scored by its number,
so added in order; `zset-random`: the same members with random integer
scores, added in a random order of scores).
Each server must be running on 127.0.0.1 at its port; it's flushed.

    python3 tools/bench/collections_memory.py [--kinds=zset,hash] NAME=PORT ...

Needs redis-py (pip, or ~/venvs/redis-py/site).
"""
import os, sys
sys.path.insert(0, os.path.expanduser("~/venvs/redis-py/site"))
import redis

SHAPES = [("small", 10000, 100), ("big", 1, 1000000)]
RNG = __import__("random").Random(1)


def fill(r, kind, keys, n):
    p = r.pipeline(transaction=False)
    queued = 0
    for k in range(keys):
        for start in range(0, n, 10000):
            items = [f"{i:08d}" for i in range(start, min(n, start + 10000))]
            key = f"c{k}"
            if kind == "hash":
                p.hset(key, mapping={i: i for i in items})
            elif kind == "set":
                p.sadd(key, *[f"m{i}" for i in items])  # not integers: no intset
            elif kind == "intset":
                p.sadd(key, *[str(int(i)) for i in items])
            elif kind == "zset":
                p.zadd(key, {f"m{i}": int(i) for i in items})
            elif kind == "zset-random":
                p.zadd(key, {f"m{i}": RNG.randrange(n) for i in items})
            else:
                p.rpush(key, *items)
            queued += len(items)
            if queued >= 50000:
                p.execute()
                queued = 0
    p.execute()


def flush(r):
    """FLUSHALL SYNC where the server knows it (Valkey frees asynchronously
    by default), then until nothing is pending to free."""
    import time
    try:
        r.execute_command("FLUSHALL", "SYNC")
    except redis.ResponseError:
        r.flushall()
    while r.info("memory").get("lazyfree_pending_objects", 0):
        time.sleep(0.05)
    time.sleep(0.2)


KINDS = ["hash", "set", "intset", "list", "zset", "zset-random"]
for a in sys.argv[1:]:
    if a.startswith("--kinds="):
        KINDS = a.split("=", 1)[1].split(",")
servers = [a.split("=") for a in sys.argv[1:] if not a.startswith("--")]
rows = {}
for name, port in servers:
    r = redis.Redis(port=int(port))
    for shape, keys, n in SHAPES:
        for kind in KINDS:
            flush(r)
            before = r.info("memory")["used_memory"]
            fill(r, kind, keys, n)
            after = r.info("memory")["used_memory"]
            enc = r.object("encoding", "c0").decode()
            rows.setdefault((shape, kind), {})[name] = ((after - before) / (keys * n), enc)
    r.flushall()

names = [n for n, _ in servers]
print("| collection | " + " | ".join(names) + " |")
print("|---|" + "---|" * len(names))
for (shape, kind), by in rows.items():
    keys, n = next((k, m) for s, k, m in SHAPES if s == shape)
    label = f"{kind}, {keys} x {n}"
    cells = [f"{by[nm][0]:.1f} ({by[nm][1]})" for nm in names]
    print(f"| {label} | " + " | ".join(cells) + " |")
