#!/usr/bin/env bash
# Our cluster against a Redis Cluster, side by side (Linux / WSL2, CI):
# a 3-node Redis Cluster (redis-cli --cluster create) and stuart with three
# dbnodes (joined one at a time, then STUART CREATE: the same layout). Then
#   1. cluster_difftest.py requests: the same requests to each node pair;
#   2. cluster_difftest.py clients: the same redis-py RedisCluster script
#      against both, ours seeded with stuart and with a node;
#   3. redis-cli -c: the same commands against both;
#   4. redis-benchmark --cluster against both;
#   5. cluster_difftest.py load against ours while stuart is restarted;
#   6. cluster_difftest.py migrating: a slot migrating, ASK and TRYAGAIN
#      (last: it pushes a table to our nodes past stuart's epoch).
# Fails on any difference; stops everything at the end.
#
#   tools/compat/cluster_difftest.sh [--base-port 7850]
#
# Redis gets the base port + 1..3, stuart + 10, our nodes + 11..13. Needs
# redis-server and redis-cli 7.0.15 (REDIS_SERVER, REDIS_CLI to choose
# others), redis-benchmark, and redis-py (pip, or REDIS_PY_PATH: a directory
# holding the package, default ~/venvs/redis-py/site). CARGO_TARGET_DIR
# defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7850
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    -h|--help) sed -n '2,19p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
REDIS_SERVER="${REDIS_SERVER:-redis-server}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"
"$REDIS_SERVER" --version
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p dbnode -p stuart
BIN="$CARGO_TARGET_DIR/release"
R=($((BASE + 1)) $((BASE + 2)) $((BASE + 3)))
S=$((BASE + 10))
O=($((BASE + 11)) $((BASE + 12)) $((BASE + 13)))
for p in "${R[@]}" "$S" "${O[@]}"; do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done

DIR="$(mktemp -d /tmp/cluster_difftest.XXXXXX)"
PIDS=(); STUART_PID=
cleanup() {
  for p in "${R[@]}"; do "$REDIS_CLI" -p "$p" shutdown nosave >/dev/null 2>&1 || true; done
  # SIGINT: a node that joined stuart leaves the cluster on SIGTERM first.
  for p in "${PIDS[@]}" $STUART_PID; do kill -INT "$p" 2>/dev/null || true; done
  rm -rf "$DIR"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
wait_for() {  # wait_for WHAT COMMAND...
  local what="$1"; shift
  for _ in $(seq 200); do "$@" && return; sleep 0.1; done
  fail "$what"
}
start_stuart() {
  "$BIN/stuart" "127.0.0.1:$S" --state "$DIR/state" --loglevel warning 2>>"$DIR/stuart.log" & STUART_PID=$!
  wait_for "stuart starts" pings "$S"
}
joined() { [ "$("$REDIS_CLI" -p "$S" stuart nodes | grep -c ' up ')" = "$1" ]; }
epoch_everywhere() { for p in "${O[@]}"; do "$REDIS_CLI" -p "$p" cluster info | grep -q "^cluster_current_epoch:$1"$'\r'; done; }
redis_ok() { "$REDIS_CLI" -p "${R[0]}" cluster info | grep -q "^cluster_state:ok"; }
pings() { "$REDIS_CLI" -p "$1" ping >/dev/null 2>&1; }

# The Redis Cluster.
for p in "${R[@]}"; do
  mkdir "$DIR/$p"
  (cd "$DIR/$p" && "$REDIS_SERVER" --port "$p" --cluster-enabled yes --cluster-config-file nodes.conf \
     --cluster-require-full-coverage no --save '' --appendonly no --daemonize yes --logfile "$DIR/$p/log")
done
for p in "${R[@]}"; do wait_for "redis $p starts" pings "$p"; done
"$REDIS_CLI" --cluster create "127.0.0.1:${R[0]}" "127.0.0.1:${R[1]}" "127.0.0.1:${R[2]}" \
  --cluster-replicas 0 --cluster-yes >/dev/null
wait_for "the Redis Cluster is ok" redis_ok

# Ours: one node at a time, so the join order is the port order.
start_stuart
for i in 0 1 2; do
  "$BIN/dbnode" "127.0.0.1:${O[$i]}" --cluster --stuart "127.0.0.1:$S" --loglevel warning 2>>"$DIR/node-${O[$i]}.log" & PIDS+=($!)
  wait_for "node ${O[$i]} joins" joined $((i + 1))
done
[ "$("$REDIS_CLI" -p "$S" stuart create)" = OK ] || fail "STUART CREATE"
wait_for "our nodes take the table" epoch_everywhere 1

# Both are up: wait for Redis's rejoin delay to pass on every node too.
for p in "${R[@]}"; do wait_for "redis $p ok" bash -c "\"$REDIS_CLI\" -p $p cluster info | grep -q '^cluster_state:ok'"; done

echo ">> requests"
python3 "$HERE/cluster_difftest.py" requests --redis "$(IFS=,; echo "${R[*]}")" --ours "$(IFS=,; echo "${O[*]}")"
echo ">> clients"
python3 "$HERE/cluster_difftest.py" clients --redis "${R[0]}" --ours "$S" --ours "${O[1]}"

echo ">> redis-cli -c"
same=0; n=0
for c in "SET foo 1" "GET foo" "INCR foo" "SET bar x" "APPEND bar y" "GET bar" "MSET {u}a 1 {u}b 2" "MGET {u}a {u}b" \
         "MGET foo bar" "DEL foo bar" "DEL foo" "EXISTS foo bar" "SET k1 v" "SET k2 v" "GET k1" "TTL k2" "CLUSTER KEYSLOT foo"; do
  a=$("$REDIS_CLI" -c -p "${R[0]}" $c 2>&1); b=$("$REDIS_CLI" -c -p "$S" $c 2>&1); n=$((n + 1))
  if [ "$a" = "$b" ]; then same=$((same + 1)); else echo "  $c: redis '$a', ours '$b'"; fi
done
echo "redis-cli -c: $same/$n the same"
[ "$same" = "$n" ] || fail "redis-cli -c"

echo ">> redis-benchmark --cluster"
for p in "${R[0]}" "${O[0]}"; do
  out=$(redis-benchmark --cluster -p "$p" -t set,get,incr -n 30000 -c 20 -q 2>&1) || { echo "$out"; fail "redis-benchmark --cluster -p $p"; }
  echo "$out" | grep -q "requests per second" || { echo "$out"; fail "redis-benchmark --cluster -p $p: no results"; }
  ! echo "$out" | grep -qi "error" || { echo "$out"; fail "redis-benchmark --cluster -p $p: errors"; }
  # Progress lines end in \r; the results are what follows the last one.
  echo "port $p: $(echo "$out" | tr '\r' '\n' | grep -o '^[A-Z]*: [0-9.]* requests per second' | tr '\n' ' ')"
done

echo ">> load while stuart restarts"
python3 "$HERE/cluster_difftest.py" load --ours "$S" --seconds 6 > "$DIR/load.txt" 2>&1 & LOAD=$!
sleep 2
kill "$STUART_PID"; wait "$STUART_PID" 2>/dev/null || true; STUART_PID=
sleep 1.5
start_stuart
wait "$LOAD" || { cat "$DIR/load.txt"; fail "load during the stuart restart"; }
cat "$DIR/load.txt"

echo ">> a slot migrating"
python3 "$HERE/cluster_difftest.py" migrating --redis "$(IFS=,; echo "${R[*]}")" --ours "$(IFS=,; echo "${O[*]}")"
echo "OK"
