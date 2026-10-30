#!/usr/bin/env bash
# docker-compose.yml end to end: brings the cluster up (stuart and three
# nodes, --create-when 3), then from the `cli` container (Redis 7.0.15's
# redis-cli and redis-benchmark on the cluster's network) checks that the
# table was made and taken, that keys spread over the nodes and are found
# from any node, that redis-benchmark --cluster runs, that stuart keeps
# its table across a restart (its state is on a volume), that a node
# stopped (SIGTERM) leaves its keys to the others and that started again
# it gets slots back (stuart --auto). Tears it all down, quickly (every
# node leaving at once is a shutdown).
#
#   tools/compat/compose_smoke.sh
#
# Needs docker with the compose plugin. SPEEDVAULT_IMAGE: use that image
# instead of building one (CI).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
dc() { docker compose -f "$ROOT/docker-compose.yml" -p speedvault-smoke "$@"; }
cli() { dc run --rm -T cli redis-cli "$@" | tr -d '\r'; }
fail() { echo "FAIL: $*" >&2; dc logs --no-color --tail 30 >&2 || true; exit 1; }
check() { [ "$2" = "$3" ] || fail "$1: expected '$2', got '$3'"; echo "ok   $1"; }
wait_for() {  # wait_for WHAT COMMAND...
  local what="$1"; shift
  for _ in $(seq 60); do "$@" && { echo "ok   $what"; return; }; sleep 1; done
  fail "$what"
}
trap 'dc down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

if [ -n "${SPEEDVAULT_IMAGE:-}" ]; then dc up -d --no-build; else dc up -d --build; fi
created() { cli -h stuart -p 7600 STUART TABLE | head -1 | grep -qx 'epoch 1'; }
taken() { [ "$(cli -h stuart -p 7600 STUART NODES | grep -c ' up .*acked=1 ')" = 3 ]; }
wait_for "stuart made the table once all three joined" created
wait_for "the three nodes took it" taken

check "SET foo via node1" OK "$(cli -c -h node1 -p 7001 SET foo bar)"
check "GET foo via node3" bar "$(cli -c -h node3 -p 7003 GET foo)"
for i in $(seq 30); do cli -c -h node2 -p 7002 SET "key:$i" "$i" >/dev/null; done
total=0
for n in 1 2 3; do
  keys=$(cli -h "node$n" -p "700$n" DBSIZE); total=$((total + keys))
  [ "$keys" -gt 0 ] || fail "node$n holds no key"
done
check "31 keys, spread over the 3 nodes" 31 "$total"
out=$(dc run --rm -T cli redis-benchmark --cluster -h node1 -p 7001 -t set,get -n 20000 -q 2>&1 | tr '\r' '\n')
echo "$out" | grep -q "GET: .* requests per second" || { echo "$out"; fail "redis-benchmark --cluster"; }
echo "ok   redis-benchmark --cluster: $(echo "$out" | grep -o '^[A-Z]*: [0-9.]* requests per second' | tr '\n' ' ')"

dc restart stuart >/dev/null
wait_for "stuart restarted with its table" created
wait_for "and sees the nodes again" taken
check "GET foo after the restart" bar "$(cli -c -h node2 -p 7002 GET foo)"

keys() { local t=0; for n in "$@"; do t=$((t + $(cli -h "node$n" -p "700$n" DBSIZE))); done; echo "$t"; }
before=$(keys 1 2 3)  # redis-benchmark added some
start=$(date +%s)
dc stop node3 >/dev/null
[ $(($(date +%s) - start)) -lt 60 ] || fail "node3 took a minute to stop"
check "node3 stopped: it left, stuart has two nodes" 2 "$(cli -h stuart -p 7600 STUART NODES | grep -c ' up ')"
check "its keys went to node1 and node2" "$before" "$(keys 1 2)"
dc start node3 >/dev/null
even() { [ "$(cli -h stuart -p 7600 STUART NODES | grep -c ' up .*slots=546[12]')" = 3 ]; }
wait_for "node3 started again gets its share back" even
settled() { [ -z "$(cli -h stuart -p 7600 STUART MIGRATIONS)" ]; }
wait_for "no slot moving" settled
check "the same keys over the 3 nodes" "$before" "$(keys 1 2 3)"
check "GET foo at the end" bar "$(cli -c -h node3 -p 7003 GET foo)"
start=$(date +%s)
dc down -v --remove-orphans >/dev/null 2>&1
# 7 s: the nodes leave at once (a shutdown), then stuart, 1.5 s after
# they were told to stop. 14-18 s when stuart's keepers froze it looking
# up the names of the nodes gone.
[ $(($(date +%s) - start)) -lt 12 ] || fail "down took $(($(date +%s) - start)) s"
echo "ok   down in $(($(date +%s) - start)) s"
echo "OK"
