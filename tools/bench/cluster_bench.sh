#!/usr/bin/env bash
# Cluster benchmarks (phase 4 milestone 5, Linux / WSL2): 3-node clusters
# of Redis 7.0.15 and of speedvault (stuart + 3 dbnodes), and of the --also
# servers, each node on its own core; cluster_bench.py loads them two ways
# (one redis-benchmark per node, and redis-benchmark --cluster --threads
# 3). Then the cost of cluster mode: one dbnode standalone against one in
# cluster mode owning every slot, with bench.sh. Results go to
# tools/bench/results/<date>-<commit>/ (as full_run.sh's).
#
#   tools/bench/cluster_bench.sh [-n N] [--also NAME=SERVER_DIR ...] [--no-slot-check]
#
#   -n N              requests per node at -P 1 (4N with pipelining) [1000000]
#   --also NAME=DIR   another Redis-like server: DIR/bin/*-server and *-cli,
#                     e.g. --also redis-8.10.2=$HOME/redis-8 --also valkey-9.1.2=$HOME/valkey-9
#   --no-slot-check   skip standalone vs cluster mode
#
# CPUs (lscpu -e: 2n and 2n+1 are one core): nodes 2, 4, 6; clients 8, 10,
# 12; stuart 14. One cluster is measured at a time; the others idle.
# Results: cluster.md, slotcheck.md, servers.txt, raw/.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
N=1000000; ALSO=(); SLOT_CHECK=1
while [ $# -gt 0 ]; do
  case "$1" in
    -n) N="$2"; shift 2;;
    --also) ALSO+=("$2"); shift 2;;
    --no-slot-check) SLOT_CHECK=0; shift;;
    -h|--help) sed -n '2,20p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
[ "$(uname -s)" = Linux ] || { echo "run this on Linux (or WSL2)" >&2; exit 2; }
for c in cargo redis-server redis-cli redis-benchmark python3 ss taskset; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
redis-server --version | grep -q 'v=7\.0\.15' || echo "warning: the baseline is Redis 7.0.15, this is $(redis-server --version)" >&2

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
BIN="$CARGO_TARGET_DIR/release"
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT/raw/cluster"
DIR="$(mktemp -d /tmp/cluster_bench.XXXXXX)"
NODE_CPUS=(2 4 6)

CLIS=(); PORTS_ALL=(); PIDS=()
cleanup() {
  for i in "${!PORTS_ALL[@]}"; do "${CLIS[$i]}" -p "${PORTS_ALL[$i]}" shutdown nosave >/dev/null 2>&1 || true; done
  # SIGINT: a node that joined stuart leaves the cluster on SIGTERM first.
  for p in "${PIDS[@]}"; do kill -INT "$p" 2>/dev/null || true; done
  rm -rf "$DIR"
}
trap cleanup EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
wait_for() { local what="$1"; shift; for _ in $(seq 300); do "$@" && return; sleep 0.1; done; fail "$what"; }
port_free() { [ -z "$(ss -ltnH "sport = :$1")" ]; }
pings() { "$1" -p "$2" ping >/dev/null 2>&1; }
state_ok() { "$1" -p "$2" cluster info 2>/dev/null | grep -q '^cluster_state:ok'; }

# A Redis-like 3-node cluster on BASE+1..3: NAME SERVER CLI BASE.
redis_cluster() {
  local name=$1 server=$2 cli=$3 base=$4 ports=() i
  for i in 0 1 2; do
    local p=$((base + 1 + i)); ports+=("$p")
    port_free "$p" || fail "port $p is taken"
    mkdir -p "$DIR/$name/$p"
    (cd "$DIR/$name/$p" && taskset -c "${NODE_CPUS[$i]}" "$server" --port "$p" --cluster-enabled yes \
       --cluster-config-file nodes.conf --cluster-require-full-coverage no --save '' --appendonly no \
       --daemonize yes --logfile "$DIR/$name/$p/log")
    CLIS+=("$cli"); PORTS_ALL+=("$p")
  done
  for p in "${ports[@]}"; do wait_for "$name $p starts" pings "$cli" "$p"; done
  "$cli" --cluster create "127.0.0.1:${ports[0]}" "127.0.0.1:${ports[1]}" "127.0.0.1:${ports[2]}" \
    --cluster-replicas 0 --cluster-yes >/dev/null
  for p in "${ports[@]}"; do wait_for "$name $p is ok" state_ok "$cli" "$p"; done
  echo "$name: $("$server" --version)" >>"$OUT/servers.txt"
  CLUSTERS+=("$name=$(IFS=,; echo "${ports[*]}")")
}

: >"$OUT/servers.txt"
CLUSTERS=()
echo ">> starting the clusters" >&2
redis_cluster redis-7.0.15 redis-server redis-cli 7900

# Ours: stuart on 7910, nodes on 7911..7913, joined in port order.
S=7910; OURS=(7911 7912 7913)
for p in "$S" "${OURS[@]}"; do port_free "$p" || fail "port $p is taken"; done
taskset -c 14 "$BIN/stuart" "127.0.0.1:$S" --state "$DIR/stuart.state" --loglevel warning 2>>"$DIR/stuart.log" & PIDS+=($!)
wait_for "stuart starts" pings redis-cli "$S"
for i in 0 1 2; do
  taskset -c "${NODE_CPUS[$i]}" "$BIN/dbnode" "127.0.0.1:${OURS[$i]}" --cluster --stuart "127.0.0.1:$S" \
    --loglevel warning 2>>"$DIR/node-${OURS[$i]}.log" & PIDS+=($!)
  wait_for "node ${OURS[$i]} joins" bash -c "[ \$(redis-cli -p $S stuart nodes | grep -c ' up ') = $((i + 1)) ]"
done
[ "$(redis-cli -p "$S" stuart create)" = OK ] || fail "STUART CREATE"
for p in "${OURS[@]}"; do wait_for "node $p has the table" state_ok redis-cli "$p"; done
echo "speedvault: dbnode $(git -C "$ROOT" rev-parse --short HEAD), stuart" >>"$OUT/servers.txt"
CLUSTERS+=("speedvault=$(IFS=,; echo "${OURS[*]}")")

base=7920
for a in "${ALSO[@]}"; do
  name="${a%%=*}"; dir="${a#*=}"
  server=$(ls "$dir"/bin/*-server | head -1); cli=$(ls "$dir"/bin/*-cli | head -1)
  redis_cluster "$name" "$server" "$cli" "$base"
  base=$((base + 10))
done

echo ">> benchmarks: ${CLUSTERS[*]}" >&2
args=(); for c in "${CLUSTERS[@]}"; do args+=(--cluster "$c"); done
python3 "$HERE/cluster_bench.py" "${args[@]}" -n "$N" -o "$OUT/cluster.md" --raw "$OUT/raw/cluster" >/dev/null

if [ "$SLOT_CHECK" = 1 ]; then
  echo ">> the cost of cluster mode: standalone vs cluster mode, one node" >&2
  cleanup; CLIS=(); PORTS_ALL=(); PIDS=(); DIR="$(mktemp -d /tmp/cluster_bench.XXXXXX)"
  for p in 7941 7942; do port_free "$p" || fail "port $p is taken"; done
  "$BIN/dbnode" 127.0.0.1:7941 --loglevel warning 2>>"$DIR/standalone.log" & PIDS+=($!)
  "$BIN/dbnode" 127.0.0.1:7942 --cluster --loglevel warning 2>>"$DIR/cluster.log" & PIDS+=($!)
  wait_for "the nodes start" bash -c "redis-cli -p 7941 ping >/dev/null && redis-cli -p 7942 ping >/dev/null"
  id=$(redis-cli -p 7942 cluster myid)
  [ "$(redis-cli -p 7942 cluster settable 1 NODES 1 "$id" 127.0.0.1 7942 RANGES 1 0 16383 "$id")" = OK ] || fail "SETTABLE"
  BENCH_RAW_DIR="$OUT/raw/slotcheck" "$HERE/bench.sh" -n "$N" -P "1 16 64" -t set,get,incr,mset -o "$OUT/slotcheck.md" \
    --title "speedvault $(git -C "$ROOT" rev-parse --short HEAD): standalone vs cluster mode (owning every slot)" \
    standalone=127.0.0.1:7941 cluster-mode=127.0.0.1:7942 >/dev/null
fi
echo ">> done: $OUT" >&2
ls "$OUT" >&2
