#!/usr/bin/env bash
# A replica's sync, measured (phase 8 milestones 3 and 10, Linux / WSL2):
# builds dbnode and stuart and runs sync_bench.py: the time a replica takes
# to sync 1M and 10M keys, ours, Redis 7.0.15's and each --also server's,
# with and without writes meanwhile, and the primary's and the replica's
# memory during it; then how each comes back from a 1 s cut of its link.
# Results go to tools/bench/results/<date>-<commit>/sync.md.
#
#   tools/bench/sync_bench.sh [--base-port 7950] [--keys 1000000,10000000]
#                             [--cut-keys 1000000] [--also NAME=SERVER ...]
#   e.g. --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Needs redis-server 7.0.15 (REDIS_SERVER to choose another),
# redis-benchmark, taskset and redis-py (pip, or REDIS_PY_PATH: default
# ~/venvs/redis-py/site). CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7950
KEYS=1000000,10000000
CUT_KEYS=1000000
ALSO_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    --keys) KEYS="$2"; shift 2;;
    --cut-keys) CUT_KEYS="$2"; shift 2;;
    --also) ALSO_ARGS+=(--also "$2"); shift 2;;
    -h|--help) sed -n '2,16p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
for c in "${REDIS_SERVER:-redis-server}" redis-benchmark taskset python3 ss; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
for p in $(seq "$BASE" $((BASE + 2))) $((BASE + 11)) $((BASE + 12)) $((BASE + 20)); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
export PYTHONPATH="${REDIS_PY_PATH:-$HOME/venvs/redis-py/site}${PYTHONPATH:+:$PYTHONPATH}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/sync_bench.py" --bin "$CARGO_TARGET_DIR/release" --base-port "$BASE" --keys "$KEYS" \
  --cut-keys "$CUT_KEYS" "${ALSO_ARGS[@]}" --out "$OUT/sync.md"
echo "results: $OUT/sync.md"
