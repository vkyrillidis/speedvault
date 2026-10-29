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
#      (it pushes a table to our nodes past stuart's epoch);
#   7. cluster_difftest.py replicas: a Redis Cluster of three primaries with
#      a replica each (--cluster-replicas 1) and ours (stuart --replicas 1,
#      six nodes), the same keys written to both and synced: the layout
#      replies at every node, reads (READONLY) and redirects at the replicas;
#   8. cluster_difftest.py failed: a replica SIGKILLed in each (Redis with
#      cluster-node-timeout 2000, ours marked failed by stuart after 5 s):
#      the layout replies at the five nodes left, a write at its primary;
#   9. cluster_difftest.py failover: then a primary SIGKILLed in each, Redis
#      failing over by itself (cluster-replica-validity-factor 0), ours
#      through stuart: the layout at the live nodes, a write at the new one.
# Fails on any difference; stops everything at the end.
#
#   tools/compat/cluster_difftest.sh [--base-port 7850]
#
# Redis gets the base port + 1..3, stuart + 10, our nodes + 11..13; with
# replicas, Redis + 21..26, stuart + 30, our nodes + 31..36. Needs
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
    -h|--help) sed -n '2,25p' "$0"; exit 0;;
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
R6=($(seq $((BASE + 21)) $((BASE + 26))))
S6=$((BASE + 30))
O6=($(seq $((BASE + 31)) $((BASE + 36))))
for p in "${R[@]}" "$S" "${O[@]}" "${R6[@]}" "$S6" "${O6[@]}"; do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done

DIR="$(mktemp -d /tmp/cluster_difftest.XXXXXX)"
PIDS=(); STUART_PID=; STUART6_PID=
cleanup() {
  for p in "${R[@]}" "${R6[@]}"; do "$REDIS_CLI" -p "$p" shutdown nosave >/dev/null 2>&1 || true; done
  # SIGINT: a node that joined stuart leaves the cluster on SIGTERM first.
  for p in "${PIDS[@]}" $STUART_PID $STUART6_PID; do kill -INT "$p" 2>/dev/null || true; done
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

echo ">> replicas"
for p in "${R6[@]}"; do
  mkdir "$DIR/$p"
  (cd "$DIR/$p" && "$REDIS_SERVER" --port "$p" --cluster-enabled yes --cluster-config-file nodes.conf \
     --cluster-require-full-coverage no --cluster-node-timeout 2000 --cluster-replica-validity-factor 0 --save '' --appendonly no \
     --daemonize yes --logfile "$DIR/$p/log")
done
for p in "${R6[@]}"; do wait_for "redis $p starts" pings "$p"; done
"$REDIS_CLI" --cluster create $(for p in "${R6[@]}"; do echo "127.0.0.1:$p"; done) --cluster-replicas 1 --cluster-yes >/dev/null
three_slaves() { [ "$("$REDIS_CLI" -p "$1" cluster nodes | grep -cE '[ ,]slave ')" = 3 ]; }
for p in "${R6[@]}"; do
  wait_for "redis $p ok" bash -c "\"$REDIS_CLI\" -p $p cluster info | grep -q '^cluster_state:ok'"
  wait_for "redis $p knows the replicas" three_slaves "$p"
done
"$BIN/stuart" "127.0.0.1:$S6" --state "$DIR/state6" --replicas 1 --loglevel warning 2>>"$DIR/stuart6.log" & STUART6_PID=$!
wait_for "stuart (replicas) starts" pings "$S6"
joined6() { [ "$("$REDIS_CLI" -p "$S6" stuart nodes | grep -c ' up ')" = "$1" ]; }
for i in 0 1 2 3 4 5; do
  "$BIN/dbnode" "127.0.0.1:${O6[$i]}" --cluster --stuart "127.0.0.1:$S6" --loglevel warning 2>>"$DIR/node-${O6[$i]}.log" & PIDS+=($!)
  wait_for "node ${O6[$i]} joins" joined6 $((i + 1))
done
[ "$("$REDIS_CLI" -p "$S6" stuart create)" = OK ] || fail "STUART CREATE (replicas)"
for p in "${O6[@]}"; do wait_for "node $p takes the table" three_slaves "$p"; done
python3 "$HERE/cluster_difftest.py" replicas --redis "$(IFS=,; echo "${R6[*]}")" --ours "$(IFS=,; echo "${O6[*]}")"

echo ">> a replica killed"
# The port of the replica of slot 0's primary, as node $1 sees it.
replica_of_slot_0() {
  local nodes primary
  nodes=$("$REDIS_CLI" -p "$1" cluster nodes)
  primary=$(echo "$nodes" | awk '$9 ~ /^0-/ {print $1}')
  echo "$nodes" | awk -v p="$primary" '$4 == p {split($2, a, "[:@]"); print a[2]}'
}
pid_of() { "$REDIS_CLI" -p "$1" info server | tr -d '\r' | sed -n 's/^process_id://p'; }
dead_r=$(replica_of_slot_0 "${R6[0]}"); dead_o=$(replica_of_slot_0 "${O6[0]}")
[ -n "$dead_r" ] && [ -n "$dead_o" ] || fail "no replica of slot 0's primary"
kill -9 "$(pid_of "$dead_r")" "$(pid_of "$dead_o")"
python3 "$HERE/cluster_difftest.py" failed --redis "$(IFS=,; echo "${R6[*]}")" --ours "$(IFS=,; echo "${O6[*]}")" --dead "$dead_r,$dead_o"

echo ">> a primary killed"
python3 "$HERE/cluster_difftest.py" failover --redis "$(IFS=,; echo "${R6[*]}")" --ours "$(IFS=,; echo "${O6[*]}")"
echo "OK"
