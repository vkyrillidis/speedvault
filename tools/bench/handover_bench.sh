#!/usr/bin/env bash
# A primary leaving on SIGTERM, measured (phase 8 milestone 6 steps 4-5,
# Linux / WSL2): builds dbnode and stuart and runs handover_bench.py: the
# handover's write pause at the node and at a client, the time it takes
# against a drain of the same node, a rolling restart (slots moved, syncs),
# and a scale-down from 6 nodes to 4 (a primary demoted).
# Results go to tools/bench/results/<date>-<commit>/handover.md.
#
#   tools/bench/handover_bench.sh [--base-port 7920] [PART...] [-- handover_bench.py options]
#   # parts: handover drain rolling scale-down (default: all)
#
# Needs redis-py and prometheus_client (pip, or REDIS_PY_PATH: default
# ~/venvs/redis-py/site). CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7920
PARTS=()
EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    -h|--help) sed -n '2,13p' "$0"; exit 0;;
    --) shift; EXTRA=("$@"); break;;
    -*) echo "unknown argument: $1" >&2; exit 2;;
    *) PARTS+=("$1"); shift;;
  esac
done
[ ${#PARTS[@]} -gt 0 ] || PARTS=(all)
for p in $(seq "$BASE" $((BASE + 6))) $((BASE + 1000)); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
export PYTHONPATH="${REDIS_PY_PATH:-$HOME/venvs/redis-py/site}${PYTHONPATH:+:$PYTHONPATH}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/handover_bench.py" "${PARTS[@]}" --bin "$CARGO_TARGET_DIR/release" --base-port "$BASE" --out "$OUT/handover.md" "${EXTRA[@]}"
echo "results: $OUT/handover.md"
