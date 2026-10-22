#!/usr/bin/env bash
# The whole benchmark session in one go (Linux): builds our node, starts
# it next to Redis, checks our node against Redis,
# runs the redis-benchmark suites and the crates' cargo benches, and writes
# everything to tools/bench/results/<date>-<commit>/.
#
#   tools/bench/full_run.sh [-n N] [--no-cargo-bench] [--types] [--also NAME=SERVER ...]
#
#   -n N              requests per redis-benchmark test [1000000]
#   --no-cargo-bench  skip `cargo bench -p fiber` and `-p reactor`
#   --types           also the data-type suite (lists, sets, hashes, LRANGE_*), -P 1 16
#   --also NAME=SERVER  also run the suite against another redis-server-like
#                     binary (repeatable), e.g.
#                     --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#                     --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Servers (all on 127.0.0.1; the script stops if a port is taken):
#   Redis      6393  redis-server --save '' --appendonly no, pinned to CPU 2
#   speedvault 7000  target/release/dbnode --loglevel warning, CPU 2
#   --also     6394, 6395, ...  SERVER --save '' --appendonly no, CPU 2
# They run one at a time under load, so they can share CPU 2. Redis (on
# PATH, 7.0.15) stays the reference: the compat check and the ratios.
#
# Results (tools/bench/results/<date>-<commit>/):
#   suite.md        Redis vs speedvault (and the --also servers):
#                   ping,set,get,incr,mset, -P 1, 16, 64
#   types.md        with --types: lpush,rpush,lpop,rpop,sadd,hset,spop,
#                   lrange_100..600, -P 1, 16
#   compat.json     tools/compat/difftest.py, our node against Redis
#   cargo-bench.txt cargo bench -p fiber and -p reactor
#   raw/            redis-benchmark CSVs and stderr
#
# CARGO_TARGET_DIR defaults to ~/target/speedvault (under WSL much faster
# than a target dir on a Windows drive).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
N=1000000; CARGO_BENCH=1; TYPES=0; ALSO=()
while [ $# -gt 0 ]; do
  case "$1" in
    -n) N="$2"; shift 2;;
    --no-cargo-bench) CARGO_BENCH=0; shift;;
    --types) TYPES=1; shift;;
    --also) ALSO+=("$2"); shift 2;;
    -h|--help) sed -n '2,38p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done

[ "$(uname -s)" = Linux ] || { echo "run this on Linux (or WSL2)" >&2; exit 2; }
for c in cargo redis-server redis-benchmark python3 ss taskset; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
redis-server --version | grep -q 'v=7\.0\.15' ||
  echo "warning: the baseline is Redis 7.0.15, this is $(redis-server --version)" >&2

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)"
mkdir -p "$OUT/raw"
LOGS="$OUT/raw/logs"; mkdir -p "$LOGS"

PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

port_free() { [ -z "$(ss -ltnH "sport = :$1")" ]; }
wait_port() {  # up to 10 s
  for _ in $(seq 50); do port_free "$1" || return 0; sleep 0.2; done
  echo "nothing listens on $1 after 10 s; see $LOGS" >&2; exit 1
}
# --also NAME=SERVER: ALSO_NAMES[i] on port 6394+i.
ALSO_NAMES=(); ALSO_BINS=(); ALSO_PORTS=()
for i in "${!ALSO[@]}"; do
  a="${ALSO[$i]}"
  [ "${a%%=*}" != "$a" ] || { echo "--also takes NAME=SERVER, not $a" >&2; exit 2; }
  ALSO_NAMES+=("${a%%=*}"); ALSO_BINS+=("${a#*=}"); ALSO_PORTS+=($((6394 + i)))
  [ -x "${a#*=}" ] || { echo "${a#*=}: not an executable" >&2; exit 2; }
done
for p in 6393 7000 "${ALSO_PORTS[@]}"; do
  port_free "$p" || { echo "port $p is taken: stop what's on it first" >&2; exit 1; }
done

echo ">> building" >&2
(cd "$ROOT" && cargo build --release --locked -p dbnode)

echo ">> starting servers" >&2
redis-server --port 6393 --save '' --appendonly no >"$LOGS/redis.log" 2>&1 & PIDS+=($!)
"$CARGO_TARGET_DIR/release/dbnode" 127.0.0.1:7000 --loglevel warning >"$LOGS/dbnode.log" 2>&1 & PIDS+=($!)
for i in "${!ALSO_NAMES[@]}"; do
  "${ALSO_BINS[$i]}" --port "${ALSO_PORTS[$i]}" --save '' --appendonly no >"$LOGS/${ALSO_NAMES[$i]}.log" 2>&1 & PIDS+=($!)
done
wait_port 6393; wait_port 7000
for p in "${ALSO_PORTS[@]}"; do wait_port "$p"; done
for i in "${!ALSO_NAMES[@]}"; do
  echo "${ALSO_NAMES[$i]}: $("${ALSO_BINS[$i]}" --version)" >>"$OUT/servers.txt"
done

echo ">> compatibility check" >&2
python3 "$ROOT/tools/compat/difftest.py" --ref 127.0.0.1:6393 --target 127.0.0.1:7000 --json "$OUT/compat.json" ||
  { echo "our node differs from Redis: not benchmarking (see $OUT/compat.json)" >&2; exit 1; }

echo ">> suite: Redis vs speedvault${ALSO_NAMES[*]:+ vs ${ALSO_NAMES[*]}}" >&2
EXTRA=(); for i in "${!ALSO_NAMES[@]}"; do EXTRA+=("${ALSO_NAMES[$i]}=127.0.0.1:${ALSO_PORTS[$i]}"); done
BENCH_RAW_DIR="$OUT/raw/suite" "$HERE/bench.sh" -n "$N" -P "1 16 64" -o "$OUT/suite.md" \
  --title "Redis 7.0.15 vs speedvault $(git -C "$ROOT" rev-parse --short HEAD)${ALSO_NAMES[*]:+ vs ${ALSO_NAMES[*]}}" \
  redis=127.0.0.1:6393 speedvault=127.0.0.1:7000 "${EXTRA[@]}" >/dev/null

if [ "$TYPES" = 1 ]; then
  echo ">> data types: Redis vs speedvault${ALSO_NAMES[*]:+ vs ${ALSO_NAMES[*]}}" >&2
  BENCH_RAW_DIR="$OUT/raw/types" "$HERE/bench.sh" -n "$N" -P "1 16" -o "$OUT/types.md" \
    -t lpush,rpush,lpop,rpop,sadd,hset,spop,lrange_100,lrange_300,lrange_500,lrange_600 \
    --title "Data types: Redis 7.0.15 vs speedvault $(git -C "$ROOT" rev-parse --short HEAD)${ALSO_NAMES[*]:+ vs ${ALSO_NAMES[*]}}" \
    redis=127.0.0.1:6393 speedvault=127.0.0.1:7000 "${EXTRA[@]}" >/dev/null
fi

cleanup; PIDS=()

if [ "$CARGO_BENCH" = 1 ]; then
  echo ">> cargo bench" >&2
  (cd "$ROOT" && { cargo bench -p fiber; cargo bench -p reactor; }) 2>&1 | tee "$OUT/cargo-bench.txt" >&2
fi

echo ">> done: $OUT" >&2
ls "$OUT" >&2
