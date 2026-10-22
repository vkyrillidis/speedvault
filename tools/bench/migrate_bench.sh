#!/usr/bin/env bash
# Moving slots, measured (phase 5 milestone 6, Linux / WSL2): builds
# dbnode and stuart and runs migrate_bench.py: how fast keys move, what a
# move costs the clients of its nodes for each --migrate-batch /
# --migrate-pause, and REBALANCE for each --moves-per-source /
# --max-moves. Results go to tools/bench/results/<date>-<commit>/migrate.md.
#
#   tools/bench/migrate_bench.sh [--base-port 7900] [PART...]   # speed impact rebalance (default: all)
#
# Needs redis-benchmark, taskset and redis-py (pip, or REDIS_PY_PATH:
# default ~/venvs/redis-py/site). CARGO_TARGET_DIR defaults to
# ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7900
PARTS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    -h|--help) sed -n '2,12p' "$0"; exit 0;;
    -*) echo "unknown argument: $1" >&2; exit 2;;
    *) PARTS+=("$1"); shift;;
  esac
done
[ ${#PARTS[@]} -gt 0 ] || PARTS=(all)
for c in redis-benchmark taskset python3 ss; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
for p in $(seq "$BASE" $((BASE + 4))); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/migrate_bench.py" "${PARTS[@]}" --bin "$CARGO_TARGET_DIR/release" --base-port "$BASE" --out "$OUT/migrate.md"
echo "results: $OUT/migrate.md"
