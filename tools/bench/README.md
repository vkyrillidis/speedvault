# Benchmarks

`bench.sh` runs `redis-benchmark` against one or more servers and prints one
markdown table: requests/s, p50 and p99 latency per test and pipeline depth.

```sh
tools/bench/bench.sh redis-7.0.15=127.0.0.1:6393                       # defaults below
tools/bench/bench.sh -o results/x.md redis=127.0.0.1:6393 speedvault=127.0.0.1:7000   # back to back + ratio table
tools/bench/bench.sh -n 100000 -t get,set -P 1 other=127.0.0.1:6400
```

The whole session is one command:

```sh
tools/bench/full_run.sh              # or --no-cargo-bench, --types, -n N
```

It builds our node, starts Redis on 6393 and ours on 7000, and stops if our
node differs from Redis (`tools/compat/difftest.py`). Then it runs the full
suite (Redis against ours, `-P 1 16 64`) and `cargo bench -p fiber` and
`-p reactor`. Everything goes to `results/<date>-<commit>/`: `suite.md`,
`compat.json`, `cargo-bench.txt` and `raw/` (CSVs, server logs).

A server can have its own CPUs: `other=127.0.0.1:6400@2,8`.

Other servers join the suite with `--also NAME=SERVER` (repeatable), each
started like Redis (`--save '' --appendonly no`) on 6394, 6395, ...; their
`--version` goes to `servers.txt`. Redis 7.0.15 on `PATH` stays the
reference for the compat check and the ratios:

```sh
tools/bench/full_run.sh --also redis-8.10.2=$HOME/redis-8/bin/redis-server \
                        --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
```

Redis 8.10.2 and Valkey 9.1.2 are built from their release tags into their
own prefixes, next to the packaged 7.0.15 (`make -j12 && make
PREFIX=$HOME/redis-8 install`, the same for Valkey with `PREFIX=$HOME/valkey-9`),
with their default settings (one I/O thread).

Defaults:
- Tests: `-t ping,set,get,incr,mset -n 1000000 -c 50 -d 3 --threads 1 --csv`,
  each run once with `-P 1` and once with `-P 16`, one redis-benchmark run
  per test. `ping` gives both PING_INLINE and PING_MBULK.
- Options: `-n -c -P "1 16 64" -d -t --threads --timeout -o --title`; see
  the header of `bench.sh`.
- Raw CSVs are kept in `$BENCH_RAW_DIR`, or a temporary directory whose path
  is printed.

With several servers they run one after the other with the same parameters.
The output then adds a "requests/s, ratio to the first server" table.

## Data types

`full_run.sh --types` adds the data-type suite (`types.md`: `lpush, rpush,
lpop, rpop, sadd, hset, spop, lrange_100..600`, `-P 1 16`) for every server.
Memory per element of hashes, sets and lists, many small ones and one large,
against servers already running and empty:

```sh
python3 tools/bench/collections_memory.py redis-7.0.15=6393 speedvault=7000 valkey-9.1.2=6395
```

It fills each shape, reads `used_memory` before and after, and prints a
markdown table with what `OBJECT ENCODING` says. Results:
`results/2026-10-22-types/`.

## Clusters

```sh
tools/bench/cluster_bench.sh --also redis-8.10.2=$HOME/redis-8 --also valkey-9.1.2=$HOME/valkey-9
```

Starts 3-node clusters of Redis 7.0.15, of speedvault (stuart and three
dbnodes) and of each `--also` server (`DIR/bin/*-server`), every node on
its own core (CPUs 2, 4, 6), and loads each cluster in turn two ways with
`cluster_bench.py`:

- **Per node:** one redis-benchmark per node, all at once, each on its own
  core (CPUs 8, 10, 12) with `-c 50` and keys of its node's slots (a hash
  tag the node owns, as `--cluster` tags its keys); the cluster's
  requests/s is the sum. One redis-benchmark thread tops out near 400k
  requests/s, so one process can't load three nodes.
- **`redis-benchmark --cluster --threads 3 -c 150`**, as cluster numbers
  are usually reported.

Both use `-r 100000 -d 3`, `-n` per node at `-P 1` and 4 times that with
pipelining. Then one dbnode standalone against one in cluster mode owning
every slot, with `bench.sh`: the cost of the slot check. Results:
`results/<date>-<commit>/cluster.md`, `slotcheck.md`, `servers.txt`, `raw/`.

## Moving slots

```sh
tools/bench/migrate_bench.sh                 # or one part: speed, impact, rebalance
```

stuart and dbnodes from the release binaries, each pinned (stuart CPU 14,
nodes 2, 4, 6, 12, redis-benchmark 8 and 10), filled straight from Python
(keys with a hash tag per slot); `migrate_bench.py` measures:

- **speed:** `STUART MOVE` of 8 or 1024 slots holding 1M keys of 16 B,
  300k of 1 KiB or 30k of 16 KiB, for `--migrate-batch` 10, 100, 1000,
  with no clients: the time to the last table, keys/s and MB/s.
- **impact:** 4M keys of 16 B moving per `--migrate-batch` /
  `--migrate-pause` pair (`--impact 100/0,100/50,...`) while
  redis-benchmark (`-c 50`) SETs and GETs keys of a slot staying at the
  source and SETs at the target, in 0.3 s runs for 4 s or until the move
  ends: mean requests/s and the worst p99, against the same with no move.
- **rebalance:** `STUART REBALANCE` of 4M keys from three nodes onto a
  fourth per `--moves-per-source` / `--max-moves` pair (`--rebalance
  32/128,...`), with redis-benchmark on a slot the first node keeps.

Times are monotonic; redis-benchmark runs during which the wall clock
stepped (below) are dropped. Results: `results/<date>-<commit>/migrate.md`.

## Pinning

When `taskset` exists, `bench.sh` pins by default:
- `redis-benchmark` to CPUs `4-7` (`--bench-cpus`).
- The server's threads to CPU `2` (`--server-cpus`, applied with
  `taskset -a -p` to the pid listening on the port, found with `ss -ltnp`;
  local servers only).

`--no-pin` turns this off. On the 9800X3D under WSL2, CPUs 2n and 2n+1 are
SMT siblings of one core (`lscpu -e`), so CPU 2 and CPUs 4-7 are on different
cores. The pinning stays on the server process after the run. A server with
more than one busy thread needs more CPUs, e.g. `--server-cpus 2,8`.

`--threads` above 1 made redis-benchmark slower on WSL2 (at -P 16: 3.3M/s with
1 thread, 2.0M with 2, 1.1M with 4), so the default is 1. At `-P 16` the
single client thread may be the limit near 3-4M requests/s. Check that
before reading too much into differences at that level.

Run-to-run noise is about ±10% (two identical runs against the same Redis).

## Clock steps (WSL2)

redis-benchmark times with the wall clock. When the Windows clock isn't
synced (`w32tm /query /status` says "Local CMOS Clock"), Hyper-V sets the
WSL2 clock to the host's time every ~5.5 s and `systemd-timesyncd` steps it
back to NTP time ~50 ms later. On 2026-10-17 the host was 6.7 s ahead, so the
clock jumped +6.7 s and back every 5.5 s. A test that started or ended
between the two steps came out with a negative or a far too low requests/s
(`-155,497`, `143,082`), and requests in flight across a step got a max
latency of 3000.319 ms (redis-benchmark's histogram limit). Redis was hit as
often as our node.

`bench.sh` checks for this for 7 s at start (`clockguard.py watch`). If the
clock steps, it runs each test on its own, starts it right after a pair of
steps (`clockguard.py wait`), and reruns it (up to 4 tries) if wall minus
monotonic time moved during it. A pair often cancels out (a test that holds
it is kept), but not always: some pairs left a net −0.42 to −0.50 s. A row
that moved in every try is marked † and has its requests/s corrected by the measured move. The real fix is to sync
the host clock: `w32tm /resync /force` in an elevated prompt.

## Results

- `results/2026-10-17-bbf979c/` and `results/2026-10-17-7a98a6c/`: 3-node
  clusters (`cluster_bench.sh`), after and before the faster slot hash.
- `results/2026-10-17-ef94116/`: the same session with Redis 8.10.2 and
  Valkey 9.1.2 added (`--also`): faster than all three everywhere.
- `results/redis-7.0.15-baseline.md`: Redis 7.0.15, full suite, -P 1 and 16.
- `results/2026-10-22-types/` and `results/2026-10-22-compact/`: hashes,
  sets and lists against Redis 7.0.15, 8.10.2 and Valkey 9.1.2, memory per
  element, before and after the compact encodings.

Machine: AMD Ryzen 7 9800X3D (8 cores, 16 threads), WSL2, kernel
5.15.167.4-microsoft-standard-WSL2.

## Busy time and metrics

```sh
tools/bench/metrics_check.sh                 # or --only busy, --only cost
```

A dbnode with `--metrics`, pinned to CPU 2 (redis-benchmark on 4-7):
SET then GET at `-P 1` with 50 clients and with one, at `--busy-poll 0`
and 20, comparing the reactor's busy time (`speedvault_busy_seconds_total`)
with the process's CPU time; then SET and GET at `-P 1` and 16 against a
node without `--metrics` and one scraped every second, alternating. Fails
if the busy time ever exceeds the CPU time, if under load without
busy-polling they're more than 10% apart, if busy-polling a lone client
doesn't show near 100% CPU with busy time below it, or if metrics cost
more than 5%. See `metrics_check.py`. Results:
`results/2026-10-19-metrics/`.
