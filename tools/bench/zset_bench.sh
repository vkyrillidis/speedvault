#!/usr/bin/env bash
# Sorted sets and geo (phase 9): our node against Redis 7.0.15 and the --also
# servers, each pinned to CPU 2 and loaded one at a time, as full_run.sh.
#
#   tools/bench/zset_bench.sh [-n N] [--members M] [--also NAME=SERVER ...]
#
#   -n N           requests per redis-benchmark test [1000000; a tenth for GEOSEARCH]
#   --members M    members of the large set and points of the geo set [1000000]
#   --also NAME=SERVER  another redis-server-like binary (repeatable), e.g.
#                  --also redis-8.10.2=$HOME/redis-8/bin/redis-server
#                  --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
#
# Three parts, all in tools/bench/results/<date>-<commit>-zset/:
#   small.md   redis-benchmark's own zadd, zpopmin (one small key), -P 1 16
#   large.md   on a set of M members (zset_fill.py): ZADD (updating),
#              ZSCORE, ZRANK, ZRANGEBYSCORE ... LIMIT 0 10, ZINCRBY of random
#              members, -P 1 16; on M points: GEOSEARCH FROMMEMBER BYRADIUS
#              50 km and BYBOX 100 100 km (COUNT 10 ASC), GEOPOS, GEODIST
#   memory.md  collections_memory.py, sorted sets among the collections
# Ports: Redis 6393, ours 7000, --also 6394, 6395, ...
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
N=1000000; M=1000000; ALSO=()
while [ $# -gt 0 ]; do
  case "$1" in
    -n) N="$2"; shift 2;;
    --members) M="$2"; shift 2;;
    --also) ALSO+=("$2"); shift 2;;
    -h|--help) sed -n '2,20p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
for c in cargo redis-server redis-benchmark python3 ss taskset; do
  command -v "$c" >/dev/null || { echo "$c not found" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
OUT="$HERE/results/$(date +%F)-$(git -C "$ROOT" rev-parse --short HEAD)-zset"
LOGS="$OUT/raw/logs"; mkdir -p "$LOGS"

PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
port_free() { [ -z "$(ss -ltnH "sport = :$1")" ]; }
wait_port() {
  for _ in $(seq 50); do port_free "$1" || return 0; sleep 0.2; done
  echo "nothing listens on $1 after 10 s; see $LOGS" >&2; exit 1
}
NAMES=(redis-7.0.15 speedvault); PORTS=(6393 7000); BINS=(redis-server "")
for i in "${!ALSO[@]}"; do
  a="${ALSO[$i]}"
  [ "${a%%=*}" != "$a" ] || { echo "--also takes NAME=SERVER, not $a" >&2; exit 2; }
  NAMES+=("${a%%=*}"); BINS+=("${a#*=}"); PORTS+=($((6394 + i)))
done
for p in "${PORTS[@]}"; do port_free "$p" || { echo "port $p is taken" >&2; exit 1; }; done

echo ">> building" >&2
(cd "$ROOT" && cargo build --release --locked -p dbnode)
echo ">> starting servers" >&2
: >"$OUT/servers.txt"
for i in "${!NAMES[@]}"; do
  if [ -z "${BINS[$i]}" ]; then
    "$CARGO_TARGET_DIR/release/speedvault-server" "127.0.0.1:${PORTS[$i]}" --loglevel warning >"$LOGS/${NAMES[$i]}.log" 2>&1 & PIDS+=($!)
    echo "${NAMES[$i]}: $(git -C "$ROOT" rev-parse --short HEAD)" >>"$OUT/servers.txt"
  else
    "${BINS[$i]}" --port "${PORTS[$i]}" --save '' --appendonly no >"$LOGS/${NAMES[$i]}.log" 2>&1 & PIDS+=($!)
    echo "${NAMES[$i]}: $("${BINS[$i]}" --version)" >>"$OUT/servers.txt"
  fi
done
for p in "${PORTS[@]}"; do wait_port "$p"; done
SERVERS=(); for i in "${!NAMES[@]}"; do SERVERS+=("${NAMES[$i]}=127.0.0.1:${PORTS[$i]}"); done
TAG="$(git -C "$ROOT" rev-parse --short HEAD)"

echo ">> small sorted set: zadd, zpopmin" >&2
BENCH_RAW_DIR="$OUT/raw/small" "$HERE/bench.sh" -n "$N" -P "1 16" -r 100000 -t zadd,zpopmin -o "$OUT/small.md" \
  --title "Sorted sets, redis-benchmark zadd and zpopmin: speedvault $TAG" "${SERVERS[@]}" >/dev/null

echo ">> filling: $M members, $M points" >&2
for p in "${PORTS[@]}"; do python3 "$HERE/zset_fill.py" "$p" "$M" | tee -a "$OUT/servers.txt" >&2; done

echo ">> large sorted set and geo" >&2
BENCH_RAW_DIR="$OUT/raw/large" "$HERE/bench.sh" -n "$N" -P "1 16" -r "$M" -t "" -o "$OUT/large.md" \
  --title "A sorted set of $M members: speedvault $TAG" \
  --command "ZSCORE big m:__rand_int__" --command "ZRANK big m:__rand_int__" \
  --command "ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10" --command "ZADD big __rand_int__ m:__rand_int__" \
  --command "ZINCRBY big 1 m:__rand_int__" --command "GEOPOS geo p:__rand_int__" \
  --command "GEODIST geo p:__rand_int__ p:__rand_int__ km" "${SERVERS[@]}" >/dev/null
BENCH_RAW_DIR="$OUT/raw/geosearch" "$HERE/bench.sh" -n "$((N / 10))" -P "1 16" -r "$M" -t "" -o "$OUT/geosearch.md" \
  --title "GEOSEARCH on $M points: speedvault $TAG" \
  --command "GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC" \
  --command "GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC" \
  --command "GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km" "${SERVERS[@]}" >/dev/null

echo ">> memory" >&2
MEM=(); for i in "${!NAMES[@]}"; do MEM+=("${NAMES[$i]}=${PORTS[$i]}"); done
python3 "$HERE/collections_memory.py" "${MEM[@]}" | tee "$OUT/memory.md" >&2

echo ">> done: $OUT" >&2
ls "$OUT" >&2
