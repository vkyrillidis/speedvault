#!/usr/bin/env bash
# What a replica costs its primary (phase 8 milestones 4 and 10, Linux /
# WSL2): builds dbnode and stuart, and with --base COMMIT also that commit
# (a build from before the acknowledgements, in a git worktree), and runs
# ack_bench.py: redis-benchmark SET, GET, INCR, HSET at a primary alone and
# with a replica, ours, Redis 7.0.15's and each --also server's (standalone
# and in cluster mode). Results go
# to tools/bench/results/<date>-<commit>/acks.md.
#
#   tools/bench/ack_bench.sh [--base 23f3d1e] [--base-port 7990] [--runs 5]
#                            [--also NAME=SERVER ...]
#   e.g. --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Needs redis-server 7.0.15 (REDIS_SERVER to choose another),
# redis-benchmark, taskset and redis-py (pip, or REDIS_PY_PATH: default
# ~/venvs/redis-py/site). CARGO_TARGET_DIR defaults to
# ~/target/speedvault; the base builds in ~/target/speedvault-base.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE_PORT=7990
RUNS=5
BASE_COMMIT=
ALSO_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE_COMMIT="$2"; shift 2;;
    --base-port) BASE_PORT="$2"; shift 2;;
    --runs) RUNS="$2"; shift 2;;
    --also) ALSO_ARGS+=(--also "$2"); shift 2;;
    -h|--help) sed -n '2,18p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
for c in "${REDIS_SERVER:-redis-server}" redis-benchmark taskset python3 ss; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
for p in $(seq "$BASE_PORT" $((BASE_PORT + 2))) $((BASE_PORT + 11)) $((BASE_PORT + 12)) $((BASE_PORT + 10011)) $((BASE_PORT + 10012)); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
export PYTHONPATH="${REDIS_PY_PATH:-$HOME/venvs/redis-py/site}${PYTHONPATH:+:$PYTHONPATH}"
(cd "$ROOT" && cargo build --release --locked --quiet -p dbnode -p stuart)
BASE_ARGS=()
if [ -n "$BASE_COMMIT" ]; then
  WT="$(mktemp -d /tmp/ack_bench_base.XXXXXX)"
  trap 'git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1 || true' EXIT
  git -C "$ROOT" worktree add --detach --quiet "$WT" "$BASE_COMMIT"
  (cd "$WT" && CARGO_TARGET_DIR="$HOME/target/speedvault-base" cargo build --release --locked --quiet -p dbnode -p stuart)
  BASE_ARGS=(--base-bin "$HOME/target/speedvault-base/release")
fi
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT"
python3 "$HERE/ack_bench.py" --bin "$CARGO_TARGET_DIR/release" "${BASE_ARGS[@]}" --base-port "$BASE_PORT" --runs "$RUNS" "${ALSO_ARGS[@]}" --out "$OUT/acks.md"
echo "results: $OUT/acks.md"
