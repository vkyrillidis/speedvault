#!/usr/bin/env bash
# Stalls while the keyspace grows (Linux / WSL2): builds dbnode and runs
# stall_bench.py, 10M new keys into ours, Redis 7.0.15 and each --also
# server, one at a time; the round trip of every batch of SETs, so that a
# rehash of the whole keyspace at once shows. Results go to
# tools/bench/results/<date>-<commit>/stalls.md.
#
#   tools/bench/stall_bench.sh [--keys N] [--also NAME=SERVER ...]
#   e.g. --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#        --also old=$HOME/target/old/release/speedvault-server (another build of ours)
#
# Needs redis-server 7.0.15 (REDIS_SERVER to choose another).
# CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
ARGS=()
SERVERS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --keys) ARGS+=(--keys "$2"); shift 2;;
    --also) SERVERS+=("$2"); shift 2;;
    -h|--help) sed -n '2,14p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
REDIS="$(command -v "${REDIS_SERVER:-redis-server}")" || { echo "redis-server not found" >&2; exit 2; }
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode)
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
taskset -c 4 python3 "$HERE/stall_bench.py" "${ARGS[@]}" --out "$OUT/stalls.md" \
  ours="$CARGO_TARGET_DIR/release/speedvault-server" "redis-$("$REDIS" --version | sed 's/.*v=\([0-9.]*\).*/\1/')=$REDIS" "${SERVERS[@]}"
echo "results: $OUT/stalls.md"
