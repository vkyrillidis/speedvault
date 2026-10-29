#!/usr/bin/env bash
# stuart and three dbnodes, end to end with stock clients (Linux / WSL2):
# builds both binaries, starts stuart and three nodes with --stuart, runs
# STUART CREATE, then checks with redis-cli -c (seeded with a node, and
# with stuart itself), redis-cli --cluster check, a stuart restart in the
# middle, and a node restart (a new id at the same address: its slots are
# lost until STUART ASSIGN gives them to it). Fails on the first wrong
# answer; stops everything at the end.
#
#   tools/compat/stuart_cluster.sh [--base-port 7830]
#
# stuart listens on the base port, the nodes on the next three.
# CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7830
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    -h|--help) sed -n '2,13p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p dbnode -p stuart
BIN="$CARGO_TARGET_DIR/release"
S=$BASE; NODES=($((BASE + 1)) $((BASE + 2)) $((BASE + 3)))
for p in "$S" "${NODES[@]}"; do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done

DIR="$(mktemp -d /tmp/stuart_cluster.XXXXXX)"
PIDS=(); STUART_PID=
cleanup() {
  # SIGINT: a node that joined stuart leaves the cluster on SIGTERM first.
  for p in "${PIDS[@]}" $STUART_PID; do kill -INT "$p" 2>/dev/null || true; done
  rm -rf "$DIR"
}
trap cleanup EXIT
start_stuart() {
  "$BIN/speedvault-stuart" "127.0.0.1:$S" --state "$DIR/state" --loglevel warning 2>>"$DIR/stuart.log" & STUART_PID=$!
  for _ in $(seq 50); do redis-cli -p "$S" ping >/dev/null 2>&1 && return; sleep 0.1; done
  echo "stuart didn't start; see $DIR/stuart.log" >&2; exit 1
}
fail() { echo "FAIL: $*" >&2; exit 1; }
check() {  # check WHAT EXPECTED ACTUAL
  [ "$2" = "$3" ] || fail "$1: expected '$2', got '$3'"
  echo "ok   $1"
}
wait_for() {  # wait_for WHAT COMMAND...
  local what="$1"; shift
  for _ in $(seq 100); do "$@" && { echo "ok   $what"; return; }; sleep 0.1; done
  fail "$what"
}
epochs_are() { for p in "${NODES[@]}"; do redis-cli -p "$p" cluster info | grep -q "^cluster_current_epoch:$1"$'\r'; done; }
nodes_up() { [ "$(redis-cli -p "$S" stuart nodes | grep -c " up .*acked=$1 ")" = 3 ]; }

start_stuart
for p in "${NODES[@]}"; do
  "$BIN/speedvault-server" "127.0.0.1:$p" --cluster --stuart "127.0.0.1:$S" --loglevel warning 2>>"$DIR/node-$p.log" & PIDS+=($!)
done
wait_for "the 3 nodes joined and are up" nodes_up 0
check "STUART CREATE" OK "$(redis-cli -p "$S" stuart create)"
wait_for "every node took table epoch 1" epochs_are 1
wait_for "stuart knows" nodes_up 1

# redis-cli -c follows MOVED; seeded with stuart, which owns no keys.
for k in foo bar baz '{user42}:name' '{user42}:email' k1 k2 k3; do
  check "SET $k via stuart" OK "$(redis-cli -c -p "$S" set "$k" "v-$k")"
done
for k in foo bar baz '{user42}:name' '{user42}:email' k1 k2 k3; do
  check "GET $k via node ${NODES[1]}" "v-$k" "$(redis-cli -c -p "${NODES[1]}" get "$k")"
done
total=0
for p in "${NODES[@]}"; do total=$((total + $(redis-cli -p "$p" dbsize))); done
check "the 8 keys are spread over the nodes, once each" 8 "$total"
check "MGET with one hash tag" $'v-{user42}:name\nv-{user42}:email' "$(redis-cli -c -p "$S" mget '{user42}:name' '{user42}:email')"
check "MGET across slots" "CROSSSLOT Keys in request don't hash to the same slot" "$(redis-cli -c -p "${NODES[0]}" mget foo bar)"
check "CLUSTER SLOTS: stuart and a node agree" "$(redis-cli -p "${NODES[2]}" cluster slots)" "$(redis-cli -p "$S" cluster slots)"
redis-cli --cluster check "127.0.0.1:${NODES[0]}" >"$DIR/check.txt" 2>&1 || true
grep -q "All 16384 slots covered" "$DIR/check.txt" || { cat "$DIR/check.txt"; fail "redis-cli --cluster check"; }
grep -q "All nodes agree about slots configuration" "$DIR/check.txt" || { cat "$DIR/check.txt"; fail "redis-cli --cluster check"; }
echo "ok   redis-cli --cluster check"

# stuart restarts; the nodes don't notice.
kill "$STUART_PID"; wait "$STUART_PID" 2>/dev/null || true; STUART_PID=
check "GET foo with stuart down" v-foo "$(redis-cli -c -p "${NODES[0]}" get foo)"
start_stuart
check "the table survived" "epoch 1" "$(redis-cli -p "$S" stuart table | head -1)"
wait_for "the nodes are up again for stuart" nodes_up 1
check "GET foo via the restarted stuart" v-foo "$(redis-cli -c -p "$S" get foo)"

# The node owning foo dies and restarts at the same address: a new id, no
# keys. It replaces the old node; its slots are lost until STUART ASSIGN.
for i in 0 1 2; do
  [ "$(redis-cli -p "${NODES[$i]}" cluster countkeysinslot 12182)" = 1 ] && owner=$i
done
p=${NODES[$owner]}
old_id=$(redis-cli -p "$p" cluster myid)
range=$(redis-cli -p "$S" stuart table | awk -v id="$old_id" '$1 == id {print $NF}')   # e.g. 10923-16383
first=${range%-*}; last=${range#*-}
kill -KILL "${PIDS[$owner]}"; wait "${PIDS[$owner]}" 2>/dev/null || true
"$BIN/speedvault-server" "127.0.0.1:$p" --cluster --stuart "127.0.0.1:$S" --loglevel warning 2>>"$DIR/node-$p.log" & PIDS[$owner]=$!
new_id_joined() { new_id=$(redis-cli -p "$p" cluster myid 2>/dev/null) && [ -n "$new_id" ] && [ "$new_id" != "$old_id" ] &&
  redis-cli -p "$S" stuart nodes | grep -q "^$new_id "; }
wait_for "the restarted node joined with a new id" new_id_joined
lost=$(redis-cli -p "$S" stuart nodes | sed -n 's/^unassigned slots: //p')
check "its slots ($range) are lost" $((last - first + 1)) "$lost"
check "GET foo: its slot isn't served" "CLUSTERDOWN Hash slot not served" "$(redis-cli -c -p "$S" get foo)"
check "STUART ASSIGN $first $last" OK "$(redis-cli -p "$S" stuart assign "$first" "$last" "$new_id")"
wait_for "every node took the new table (epoch 3)" epochs_are 3
check "GET foo: served again, its key lost" "" "$(redis-cli -c -p "$S" get foo)"
check "SET foo on the new node" OK "$(redis-cli -c -p "$S" set foo again)"
check "GET bar from another node: kept" v-bar "$(redis-cli -c -p "$S" get bar)"
echo "OK"
