#!/usr/bin/env bash
# Slots moving under load (Linux / WSL2, CI): builds dbnode and stuart and
# runs migrate_check.py's scenarios (default: all of them: rebalance, drain,
# stuart-kill, source-kill, target-kill, drain-target-kill, auto-grow,
# leave-while-join, sigterm, sigterm-two, sigterm-target, full-target, metrics,
# redirects, failover, failover-stop, move-source-failover, move-target-failover,
# move-cutover-failover, move-cutover-stop, sigterm-primary, sigterm-replica,
# sigterm-no-replica, rolling, scale-down, scale-down-up). With --lettuce, builds LettuceLoad
# (lettuce/build.sh: JDK 17+, downloads Lettuce from Maven Central) and
# adds Lettuce writers. Fails on any lost or doubled write. With --password,
# stuart and the nodes require it and every client logs in.
#
#   tools/compat/migrate_check.sh [--base-port 7880] [--lettuce] [--filler N] [--collection N] [--password PASSWORD] [--keep] [SCENARIO...]
#
# stuart listens on the base port, the nodes on the next eight (metrics:
# the same + 1000). Needs redis-cli and redis-py (pip, or REDIS_PY_PATH: a
# directory holding the package, default ~/venvs/redis-py/site), and for
# metrics, sigterm-primary, sigterm-replica, sigterm-no-replica, rolling,
# scale-down and scale-down-up prometheus_client there too. CARGO_TARGET_DIR defaults to
# ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BASE=7880
ARGS=()
SCENARIOS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base-port) BASE="$2"; shift 2;;
    --lettuce) ARGS+=(--lettuce "$(bash "$HERE/lettuce/build.sh")"); shift;;
    --filler|--keys|--collection|--lettuce-modes|--lettuce-port|--migrate-arg|--password) ARGS+=("$1" "$2"); shift 2;;
    --keep) ARGS+=("$1"); shift;;
    -h|--help) sed -n '2,19p' "$0"; exit 0;;
    -*) echo "unknown argument: $1" >&2; exit 2;;
    *) SCENARIOS+=("$1"); shift;;
  esac
done
[ ${#SCENARIOS[@]} -gt 0 ] || SCENARIOS=(all)
for p in $(seq "$BASE" $((BASE + 8))) $(seq $((BASE + 1000)) $((BASE + 1004))); do
  [ -z "$(ss -ltnH "sport = :$p")" ] || { echo "port $p is taken" >&2; exit 2; }
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p dbnode -p stuart
exec python3 "$HERE/migrate_check.py" "${SCENARIOS[@]}" --bin "$CARGO_TARGET_DIR/release" --base-port "$BASE" "${ARGS[@]}"
