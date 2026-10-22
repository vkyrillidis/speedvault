#!/usr/bin/env bash
# The node's busy time against its CPU time, and what --metrics costs
# under redis-benchmark (Linux / WSL2): builds dbnode and runs
# metrics_check.py (see there).
#
#   tools/bench/metrics_check.sh [--port 7300] [-n 1000000] [--rounds 3] [--only busy|cost]
#
# Uses the port given and the next one, and those + 1000 for /metrics.
# CARGO_TARGET_DIR defaults to ~/target/speedvault.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/target/speedvault}"
cargo build --release --quiet --manifest-path "$ROOT/Cargo.toml" -p dbnode
exec python3 "$HERE/metrics_check.py" --bin "$CARGO_TARGET_DIR/release" "$@"
