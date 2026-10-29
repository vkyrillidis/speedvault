#!/bin/bash
for p in $(pgrep -f "miri|rustc"); do taskset -a -cp 8-15 $p >/dev/null 2>&1; done
pkill -f 'release/speedvault-server'; sleep 0.3
taskset -c 2 $HOME/target/speedvault/release/speedvault-server 127.0.0.1:7000 --loglevel warning >/dev/null 2>&1 &
taskset -c 2 $HOME/target/speedvault-before/release/speedvault-server 127.0.0.1:7001 --loglevel warning >/dev/null 2>&1 &
sleep 0.5
taskset -c 4-7 python3 - <<'PY'
import os, sys, time
sys.path.insert(0, os.path.expanduser("~/venvs/redis-py/site"))
import redis
N = 1 << 20  # the index (2^21 places) doubles at the next new field
for port, name in [(7001, "before"), (7000, "after")]:
    r = redis.Redis(port=port)
    out = []
    for run in range(7):
        r.delete("h")
        p = r.pipeline(transaction=False)
        for st in range(0, N, 10000):
            p.hset("h", mapping={f"{i:08d}": "x" * 8 for i in range(st, min(N, st + 10000))})
            p.execute()
        r.ping()
        normal = []
        for i in range(5):  # replace existing fields: no growth
            t = time.perf_counter(); r.hset("h", f"{i:08d}", "y" * 8); normal.append(time.perf_counter() - t)
        t = time.perf_counter(); r.hset("h", "new-field", "x"); grow = time.perf_counter() - t
        out.append((min(normal) * 1e3, grow * 1e3))
    g = sorted(x[1] for x in out); print(name, "median %.2f ms" % g[3], " ".join("%.1f" % x for x in g))
PY
pkill -f 'release/speedvault-server'
