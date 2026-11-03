#!/usr/bin/env bash
# redis-benchmark suite against one or more servers, one markdown table out.
#
#   tools/bench/bench.sh [options] NAME=HOST:PORT[@CPUS] [NAME=HOST:PORT[@CPUS] ...]
#
# Options (defaults in brackets):
#   -n N            requests per test [1000000]
#   -c N            parallel connections [50]
#   -P "LIST"       pipeline depths, space separated ["1 16"]
#   -d N            value size in bytes [3, redis-benchmark's default]
#   -r N            random keys out of N (redis-benchmark -r) [none: one key]
#   -t LIST         tests [ping,set,get,incr,mset]; "" for none (--command only)
#   --command ARGS  also this command as a test of its own (repeatable; words
#                   split on spaces, __rand_int__ as redis-benchmark has it)
#   --threads N     redis-benchmark threads [1; more was slower on WSL2]
#   --server-cpus L pin the server's threads (taskset -a -p) to CPU list L [2]
#   --bench-cpus L  run redis-benchmark under taskset -c L [4-7]
#   --no-pin        don't pin anything
#   --timeout S     give up on one redis-benchmark run after S seconds [600]
#   -o FILE         also write the markdown to FILE
#   --title TEXT    heading of the markdown
#
# Several NAME=HOST:PORT run back to back with identical parameters; the
# output then has a comparison table (requests/s per server, ratio to the
# first one). Servers are pinned only when local: the pid comes from `ss -ltnp`.
# @CPUS pins that server to its own CPU list instead of --server-cpus.
# Raw CSVs are kept in $BENCH_RAW_DIR (default: a fresh mktemp dir, printed).
#
# Examples:
#   tools/bench/bench.sh redis-7.0.15=127.0.0.1:6379
#   tools/bench/bench.sh -n 200000 -P "1 16 64" redis=127.0.0.1:6379 speedvault=127.0.0.1:7000
#   tools/bench/bench.sh -t get,set -P 1 redis=127.0.0.1:6379 other=127.0.0.1:8080@2,8
set -u
N=1000000; C=50; PIPES="1 16"; D=3; TESTS="ping,set,get,incr,mset"; THREADS=1; R=
SERVER_CPUS=2; BENCH_CPUS=4-7; PIN=1; TMO=600; OUT=""; TITLE=""
SERVERS=(); COMMANDS=()
while [ $# -gt 0 ]; do
  case "$1" in
    -n) N="$2"; shift 2;;
    -c) C="$2"; shift 2;;
    -P) PIPES="$2"; shift 2;;
    -d) D="$2"; shift 2;;
    -r) R="$2"; shift 2;;
    -t) TESTS="$2"; shift 2;;
    --command) COMMANDS+=("$2"); shift 2;;
    --threads) THREADS="$2"; shift 2;;
    --server-cpus) SERVER_CPUS="$2"; shift 2;;
    --bench-cpus) BENCH_CPUS="$2"; shift 2;;
    --no-pin) PIN=0; shift;;
    --timeout) TMO="$2"; shift 2;;
    -o) OUT="$2"; shift 2;;
    --title) TITLE="$2"; shift 2;;
    -h|--help) sed -n '2,30p' "$0"; exit 0;;
    *=*) SERVERS+=("$1"); shift;;
    *) echo "unknown argument: $1 (servers are NAME=HOST:PORT)" >&2; exit 2;;
  esac
done
[ ${#SERVERS[@]} -gt 0 ] || { echo "no server given (NAME=HOST:PORT)" >&2; exit 2; }
command -v redis-benchmark >/dev/null || { echo "redis-benchmark not found" >&2; exit 2; }
if [ "$PIN" = 1 ] && ! command -v taskset >/dev/null; then echo "taskset not found: not pinning" >&2; PIN=0; fi

RAW="${BENCH_RAW_DIR:-$(mktemp -d -t bench.XXXXXX)}"
mkdir -p "$RAW"
echo "raw results in $RAW" >&2
BENCH_PREFIX=()
[ "$PIN" = 1 ] && BENCH_PREFIX=(taskset -c "$BENCH_CPUS")

# redis-benchmark measures with the wall clock; under WSL2 it can step forward
# and back by seconds every ~5.5 s (see clockguard.py). A test that holds both
# steps of a pair is usually fine (they mostly cancel out), one
# that starts or ends between them is not. So each test starts right after a
# pair and is rerun if the clock moved during it; if all tries moved, the last
# run is kept with requests/s corrected for the move.
clock() { python3 "$(dirname "${BASH_SOURCE[0]}")/clockguard.py" "$@"; }
STEP_TOLERANCE_MS=5; TRIES=1; STEPPING=0
if [ "$(clock watch 7)" -gt 0 ]; then
  STEPPING=1; TRIES=4
  echo "warning: the wall clock steps (unsynced host clock under WSL2? see clockguard.py);" \
    "starting each test right after a step, rerunning on a step" >&2
fi
TEST_LIST=()
if [ -n "$TESTS" ]; then
  IFS=, read -r -a TEST_LIST <<<"${TESTS//ping,/ping_inline,ping_mbulk,}"
  [ "${TEST_LIST[-1]}" = ping ] && TEST_LIST=("${TEST_LIST[@]:0:${#TEST_LIST[@]}-1}" ping_inline ping_mbulk)
fi
# A command runs as a test named "cmd:<its words>".
for c in "${COMMANDS[@]}"; do TEST_LIST+=("cmd:$c"); done

for s in "${SERVERS[@]}"; do
  name="${s%%=*}"; addr="${s#*=}"; cpus="$SERVER_CPUS"
  case "$addr" in *@*) cpus="${addr#*@}"; addr="${addr%@*}";; esac
  host="${addr%:*}"; port="${addr##*:}"
  pinned="no"
  if [ "$PIN" = 1 ] && { [ "$host" = 127.0.0.1 ] || [ "$host" = localhost ]; }; then
    pid=$(ss -ltnpH "sport = :$port" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)
    if [ -n "$pid" ] && taskset -a -p -c "$cpus" "$pid" >/dev/null 2>&1; then
      pinned="cpus $cpus (pid $pid)"
    else
      pinned="no (pid not found or not ours)"
    fi
  fi
  echo "$pinned" >"$RAW/$name.pinned"
  for p in $PIPES; do
    f="$RAW/$name.P$p.csv"; clk="$RAW/$name.P$p.clock"
    echo ">> $name ($addr) -P $p -n $N -c $C -d $D -t $TESTS, server pinned: $pinned" >&2
    : >"$f"; : >"$clk"; : >"$RAW/$name.P$p.err"; rc=0
    # One test per run, so that each can be checked for clock steps alone.
    for t in "${TEST_LIST[@]}"; do
      for try in $(seq "$TRIES"); do
        if [ "$STEPPING" = 1 ]; then before=$(clock wait 12); else before=$(clock offset); fi
        if [ "${t#cmd:}" != "$t" ]; then read -r -a what <<<"${t#cmd:}"; else what=(-t "$t"); fi
        timeout "$TMO" "${BENCH_PREFIX[@]}" redis-benchmark -h "$host" -p "$port" -n "$N" -c "$C" -P "$p" -d "$D" ${R:+-r "$R"} \
          --threads "$THREADS" --csv "${what[@]}" >"$RAW/try.csv" 2>>"$RAW/$name.P$p.err"
        trc=$?
        after=$(clock offset)
        step=$(python3 -c "print(round($after - $before))")
        [ "$trc" = 0 ] || break
        python3 -c "import sys; sys.exit(abs($after - $before) > $STEP_TOLERANCE_MS)" && break
        echo "   $t: the wall clock stepped ${step} ms during the run (try $try of $TRIES)" >&2
      done
      [ "$trc" = 0 ] || { rc=$trc; echo "   redis-benchmark -t $t exited with $trc (124 = timeout)" >&2; }
      [ -s "$f" ] && tail -n +2 "$RAW/try.csv" >>"$f" || cat "$RAW/try.csv" >>"$f"
      # test name as in the CSV, tries, wall-clock step during the kept run (ms)
      tail -n +2 "$RAW/try.csv" | while IFS=, read -r tn _; do printf '%s\t%s\t%s\n' "${tn//\"/}" "$try" "$step"; done >>"$clk"
    done
    rm -f "$RAW/try.csv"
    echo "$rc" >"$RAW/$name.P$p.rc"
    [ "$rc" = 0 ] || echo "   redis-benchmark failed; see $RAW/$name.P$p.err" >&2
    cat "$f" >&2
  done
done

python3 - "$RAW" "$N" "$C" "$D" "$TESTS" "$THREADS" "$PIN" "$BENCH_CPUS" "$TITLE" "$PIPES" "$STEPPING" "${SERVERS[@]}" <<'EOF' | tee ${OUT:+"$OUT"}
import csv, math, os, platform, subprocess, sys, datetime
raw, n, c, d, tests, threads, pin, bench_cpus, title, pipes, stepping, *servers = sys.argv[1:]
pipes = pipes.split()
def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()
    except Exception:
        return "?"
cpu = sh("lscpu | sed -n 's/^Model name: *//p'") or platform.processor()
kernel = platform.release()
wsl = " (WSL2)" if "microsoft" in kernel.lower() else ""
ver = sh("redis-benchmark --version")
rows, order = {}, []
retries = 0
def fmt(rps):
    return "invalid (clock)" if math.isnan(rps) else f"{rps:,.0f}"
names = []
for s in servers:
    name = s.split("=", 1)[0]
    names.append(name)
    for p in pipes:
        base = os.path.join(raw, f"{name}.P{p}")
        rc = open(base + ".rc").read().strip() if os.path.exists(base + ".rc") else "?"
        got = False
        if os.path.exists(base + ".csv"):
            with open(base + ".csv") as f:
                for r in csv.DictReader(f):
                    if not r.get("rps"):
                        continue
                    got = True
                    t = r["test"]
                    if (t, p) not in order:
                        order.append((t, p))
                    rows[(name, t, p)] = r
        # Per test: tries, and the wall-clock step left in the kept run (ms).
        # rps = n / wall time, and wall time = real time + step.
        if os.path.exists(base + ".clock"):
            for line in open(base + ".clock"):
                t, tries, step = line.rstrip("\n").split("\t")
                retries += int(tries) - 1
                r = rows.get((name, t, p))
                if r and abs(float(step)) > 2000:
                    # Started or ended between the two steps of a pair in
                    # every try: no way to tell what the test measured.
                    r["rps"] = "nan"
                    r["corrected"] = step
                elif r and abs(float(step)) > 5:
                    real_s = int(n) / float(r["rps"]) - float(step) / 1000
                    r["rps"] = str(int(n) / real_s)
                    r["corrected"] = step
        if not got or rc != "0":
            rows[(name, "_status", p)] = f"redis-benchmark exit {rc}" + (" (timeout)" if rc == "124" else "")
print(f"# {title or 'redis-benchmark: ' + ', '.join(names)}\n")
print(f"- Date: {datetime.date.today()}")
print(f"- Machine: {cpu}{wsl}, {os.cpu_count()} CPUs, kernel {kernel}")
print(f"- Client: {ver}, `-n {n} -c {c} -d {d} -t {tests} --threads {threads} --csv`, -P in {{{', '.join(pipes)}}}")
if pin == "1":
    pins = {nm: open(os.path.join(raw, nm + '.pinned')).read().strip() for nm in names if os.path.exists(os.path.join(raw, nm + '.pinned'))}
    print(f"- Pinning: redis-benchmark on CPUs {bench_cpus}; servers: " + "; ".join(f"{k}: {v}" for k, v in pins.items()))
else:
    print("- Pinning: none")
if stepping == "1":
    print(f"- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark"
          f" times with it. Each test started right after a step and was rerun if the clock had moved during it ({retries} rerun(s));"
          f" rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)")
print()
print("| server | test | -P | requests/s | p50 ms | p99 ms |")
print("|---|---|---:|---:|---:|---:|")
for name in names:
    for t, p in order:
        r = rows.get((name, t, p))
        if r:
            mark = " †" if r.get("corrected") else ""
            print(f"| {name} | {t} | {p} | {fmt(float(r['rps']))}{mark} | {r['p50_latency_ms']} | {r['p99_latency_ms']} |")
    for p in pipes:
        st = rows.get((name, "_status", p))
        if st:
            print(f"| {name} | (run failed) | {p} | {st} | | |")
if len(names) > 1:
    print()
    print("Requests/s, ratio to " + names[0] + ":\n")
    print("| test | -P | " + " | ".join(names) + " |")
    print("|---|---:|" + "---:|" * len(names))
    for t, p in order:
        base = rows.get((names[0], t, p))
        cells = []
        for nm in names:
            r = rows.get((nm, t, p))
            if not r:
                cells.append("-")
                continue
            v = float(r["rps"])
            ratio = v / float(base["rps"]) if base else math.nan
            cells.append(fmt(v) + (" †" if r.get("corrected") else "")
                         + (f" ({ratio:.2f}x)" if not math.isnan(ratio) and nm != names[0] else ""))
        print(f"| {t} | {p} | " + " | ".join(cells) + " |")
EOF
