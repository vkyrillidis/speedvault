#!/usr/bin/env bash
# The cluster crate against a real Redis Cluster (Linux / WSL2): starts four
# cluster-enabled redis-servers, makes a 3-node cluster of the first three
# with `redis-cli --cluster create`, builds the crate's cluster_compat
# example and the dbnode, and runs cluster_compat.py: KEYSLOT on random keys;
# CLUSTER SLOTS, SHARDS and NODES in RESP2 and RESP3 for several layouts,
# replicas among them;
# and our node in cluster mode in a Redis node's place, the same requests to
# both (MOVED, CROSSSLOT, CLUSTERDOWN, the CLUSTER commands). Stops the
# servers afterwards.
#
#   tools/compat/cluster_compat.sh [--ports 7801,7802,7803,7804] [--keys N] [--node-port 7811]
#
# The Redis nodes run with cluster-require-full-coverage no, the behaviour
# our node copies: an unassigned slot is "not served", the others carry on.
#
# REDIS_SERVER / REDIS_CLI pick other binaries (default: the ones on PATH,
# Redis 7.0.15, whose replies the crate copies). CARGO_TARGET_DIR defaults
# to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PORTS=7801,7802,7803,7804; KEYS=20000; NODE_PORT=7811
while [ $# -gt 0 ]; do
  case "$1" in
    --ports) PORTS="$2"; shift 2;;
    --keys) KEYS="$2"; shift 2;;
    --node-port) NODE_PORT="$2"; shift 2;;
    -h|--help) sed -n '2,19p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
REDIS_SERVER="${REDIS_SERVER:-redis-server}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"
"$REDIS_SERVER" --version

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p cluster --example cluster_compat
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p dbnode
BIN="$CARGO_TARGET_DIR/release/examples/cluster_compat"

IFS=, read -r -a P <<< "$PORTS"
[ "${#P[@]}" = 4 ] || { echo "--ports takes 4 ports" >&2; exit 2; }
for p in "${P[@]}" "$NODE_PORT"; do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done

DIR="$(mktemp -d /tmp/cluster_compat.XXXXXX)"
cleanup() {
  for p in "${P[@]}"; do "$REDIS_CLI" -p "$p" shutdown nosave >/dev/null 2>&1 || true; done
  rm -rf "$DIR"
}
trap cleanup EXIT
for p in "${P[@]}"; do
  mkdir "$DIR/$p"
  # No PING to replicas (they'd move the replication offsets off 0).
  (cd "$DIR/$p" && "$REDIS_SERVER" --port "$p" --cluster-enabled yes --cluster-config-file nodes.conf \
     --cluster-require-full-coverage no --repl-ping-replica-period 3600 \
     --save '' --appendonly no --daemonize yes --logfile "$DIR/$p/log")
done
for p in "${P[@]}"; do
  for _ in $(seq 50); do "$REDIS_CLI" -p "$p" ping >/dev/null 2>&1 && break; sleep 0.1; done
done
"$REDIS_CLI" --cluster create "127.0.0.1:${P[0]}" "127.0.0.1:${P[1]}" "127.0.0.1:${P[2]}" \
  --cluster-replicas 0 --cluster-yes >/dev/null

python3 "$HERE/cluster_compat.py" --bin "$BIN" --ports "$PORTS" --keys "$KEYS" \
  --dbnode "$CARGO_TARGET_DIR/release/dbnode" --node-port "$NODE_PORT"
