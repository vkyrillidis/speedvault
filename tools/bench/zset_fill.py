"""Fills a server for zset_bench.sh: `big`, a sorted set of N members
`m:%012d` (redis-benchmark's __rand_int__ form) with random integer scores
in [0, N), and `geo`, N points `p:%012d` spread over the whole earth.
The same members and points on every server (a fixed seed).

    python3 tools/bench/zset_fill.py PORT [N]

Needs redis-py (pip, or ~/venvs/redis-py/site).
"""
import os
import random
import sys

sys.path.insert(0, os.path.expanduser("~/venvs/redis-py/site"))
import redis  # noqa: E402

port = int(sys.argv[1])
n = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000
r = redis.Redis(port=port)
r.delete("big", "geo")
rng = random.Random(1)
BATCH = 1000
p = r.pipeline(transaction=False)
for start in range(0, n, BATCH):
    end = min(n, start + BATCH)
    p.zadd("big", {f"m:{i:012d}": rng.randrange(n) for i in range(start, end)})
    pts = []
    for i in range(start, end):
        pts += [rng.uniform(-180, 180), rng.uniform(-85, 85), f"p:{i:012d}"]
    p.geoadd("geo", pts)
    if start % 50_000 == 0:
        p.execute()
p.execute()
print(f"port {port}: big {r.zcard('big')} members ({r.object('encoding', 'big').decode()}), geo {r.zcard('geo')} points")
