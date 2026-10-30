#!/bin/bash
# Random HSET/HGET/SADD/SISMEMBER on a 1M-element hash and set: Redis
# 7.0.15 (6393), speedvault before step 4 (7001) and after (7000).
source ~/.cargo/env 2>/dev/null || true
REPO=$(git rev-parse --show-toplevel)
OLD=$HOME/step4-before
set -e
# rm -rf "$OLD" && mkdir -p "$OLD"
# (cd "$REPO" && git archive HEAD) | tar -x -C "$OLD"
# (cd "$OLD" && CARGO_TARGET_DIR=$HOME/target/speedvault-before cargo build -q --release -p dbnode)
(cd "$REPO" && CARGO_TARGET_DIR=$HOME/target/speedvault cargo build -q --release -p dbnode)
set +e
pkill -f 'release/speedvault-server' ; sleep 0.3
redis-cli -p 6393 ping >/dev/null 2>&1 || { redis-server --port 6393 --save '' --appendonly no --daemonize yes >/dev/null; sleep 0.5; }
taskset -c 2 $HOME/target/speedvault/release/speedvault-server 127.0.0.1:7000 --loglevel warning >/tmp/after.log 2>&1 &
taskset -c 2 $HOME/target/speedvault-before/release/speedvault-server 127.0.0.1:7001 --loglevel warning >/tmp/before.log 2>&1 &
taskset -cp 2 $(pgrep -f 'redis-server.*6393') >/dev/null
sleep 0.5
fill() {
  redis-cli -p $1 flushall >/dev/null
  python3 - "$1" <<'PY'
import os, sys
sys.path.insert(0, os.path.expanduser("~/venvs/redis-py/site"))
import redis
r = redis.Redis(port=int(sys.argv[1]))
p = r.pipeline(transaction=False)
for start in range(0, 1000000, 10000):
    items = [f"{i:012d}" for i in range(start, start + 10000)]
    p.hset("h", mapping={i: i for i in items})
    p.sadd("s", *items)
    p.execute()
PY
}
for port in 6393 7001 7000; do fill $port; done
run() { # port pipeline name cmd...
  local port=$1 P=$2 name=$3; shift 3
  taskset -c 4-7 redis-benchmark -p $port -n 2000000 -c 50 -P $P -r 1000000 -q "$@" | tr '\r' '\n' | grep -o '[0-9.]* requests per second' | head -1 | cut -d' ' -f1
}
med() { for i in 1 2 3; do run "$@"; done | sort -n | sed -n 2p; }
printf "| test | -P | Redis 7.0.15 | before | after |\n|---|---:|---:|---:|---:|\n"
for P in 1 16; do
  for t in "HGET:hget h __rand_int__" "HSET:hset h __rand_int__ __rand_int__" "SISMEMBER:sismember s __rand_int__" "SADD:sadd s __rand_int__"; do
    name=${t%%:*}; cmd=${t#*:}
    a=$(med 6393 $P $name $cmd); b=$(med 7001 $P $name $cmd); c=$(med 7000 $P $name $cmd)
    echo "| $name | $P | $a | $b | $c |"
  done
done
pkill -f 'release/speedvault-server'
