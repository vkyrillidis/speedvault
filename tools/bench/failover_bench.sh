#!/usr/bin/env bash
# How long a primary's slots go unserved when it dies or leaves (phase 8
# milestones 5 and 10, Linux / WSL2): builds dbnode and stuart and runs
# failover_bench.py, ours (stuart --fail-after T) against Redis 7.0.15 and
# each --also server (cluster-node-timeout T): a SIGKILL, and a graceful
# leave (ours SIGTERM, theirs CLUSTER FAILOVER).
# Results go to tools/bench/results/<date>-<commit>/failover.md.
#
#   tools/bench/failover_bench.sh [--timeouts 2,5] [--runs 3] [--base-port 7700]
#                                 [--also NAME=SERVER ...]
#   e.g. --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Needs redis-server and redis-cli 7.0.15 (REDIS_SERVER to choose another)
# and redis-py (pip, or REDIS_PY_PATH: default ~/venvs/redis-py/site).
# CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --timeouts|--runs|--base-port|--also) ARGS+=("$1" "$2"); shift 2;;
    -h|--help) sed -n '2,16p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
export PYTHONPATH="${REDIS_PY_PATH:-$HOME/venvs/redis-py/site}${PYTHONPATH:+:$PYTHONPATH}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/failover_bench.py" --bin "$CARGO_TARGET_DIR/release" "${ARGS[@]}" --out "$OUT/failover.md"
echo "results: $OUT/failover.md"
