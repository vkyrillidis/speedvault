#!/usr/bin/env bash
# Memory of a primary and its replica (phase 8 milestone 10, Linux / WSL2):
# builds dbnode and stuart and runs memory_bench.py: 1M strings and a mix
# of collections, ours, Redis 7.0.15's and each --also server's (standalone
# and in cluster mode), used_memory and RSS of the primary and the replica.
# Results go to tools/bench/results/<date>-<commit>/memory.md.
#
#   tools/bench/memory_bench.sh [--base-port 7940] [--also NAME=SERVER ...]
#   e.g. --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Needs redis-server 7.0.15 (REDIS_SERVER to choose another).
# CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7940
ALSO_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    --also) ALSO_ARGS+=(--also "$2"); shift 2;;
    -h|--help) sed -n '2,13p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
command -v "${REDIS_SERVER:-redis-server}" >/dev/null || { echo "redis-server not found" >&2; exit 2; }
for p in $(seq "$BASE" $((BASE + 2))) $((BASE + 11)) $((BASE + 12)) $((BASE + 10011)) $((BASE + 10012)); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/memory_bench.py" --bin "$CARGO_TARGET_DIR/release" --base-port "$BASE" "${ALSO_ARGS[@]}" --out "$OUT/memory.md"
echo "results: $OUT/memory.md"
