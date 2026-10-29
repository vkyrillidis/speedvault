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
tools/bench/migrate_bench.sh                 # or one part: speed, impact, rebalance, drain
tools/bench/migrate_bench.sh drain -- --drain 128/512 --drain-runs 5
```

stuart and dbnodes from the release binaries, each pinned (stuart CPU 14,
nodes 2, 4, 6, 12, redis-benchmark 8 and 10, a RedisCluster client 0),
filled straight from Python (keys with a hash tag per slot). Slots move by
snapshot (`results/2026-10-25-*` compare it with phase 5's moves key by
key, since removed); `migrate_bench.py` measures:

- **speed:** `STUART MOVE` of 8 or 1024 slots holding 1M keys of 16 B,
  300k of 1 KiB or 30k of 16 KiB with no clients, for `--migrate-batch`
  100 and 1000 (the copy's batch): the time to the last table, keys/s and
  MB/s, the target's memory.
- **impact:** 4M keys of 16 B moving per `--migrate-batch` /
  `--migrate-pause` pair (`--impact 100/50,...`: the copy's batch and its
  pause while clients are served) while
  redis-benchmark (`-c 50`) SETs and GETs keys of a slot staying at the
  source and SETs at the target, in 0.3 s runs for 4 s or until the move
  ends: mean requests/s and the worst p99, against the same with no move.
- **rebalance:** `STUART REBALANCE` of 4M keys from three nodes onto a
  fourth per `--moves-per-source` / `--max-moves` pair (`--rebalance
  32/128,...`), with redis-benchmark on a slot the first node keeps.
- **drain:** `STUART LEAVE` of one of four nodes holding 200k keys of
  100 B (as `migrate_check`'s sigterm) per `--moves-per-source` /
  `--max-moves` pair (`--drain 32/128,128/512`): what moving 4096 slots
  costs besides their keys.

In every part a `RedisCluster` client SETs keys of random slots during the
move (p99 and worst latency). The nodes log verbosely, and the
cutover pauses (from `CLUSTER PAUSESLOTS` to the table that ends them)
come from the sources' logs: p50, max and how many.

Times are monotonic; redis-benchmark runs during which the wall clock
stepped (below) are dropped. Results: `results/<date>-<commit>/migrate.md`.

## A primary leaving on SIGTERM

```sh
tools/bench/handover_bench.sh                # or one part: handover, drain, rolling, scale-down
tools/bench/handover_bench.sh handover -- --runs 10 --filler 200000
```

stuart `--auto --replicas 1` and six named nodes (three primaries with a
replica each), `--filler` keys of 100 B (default 1M), and RedisCluster
clients INCRing keys one at a time, on the leaving primary's slots and
on any slot (`handover_bench.py`):

- **handover:** a primary gets SIGTERM and hands its slots to its
  replica: the write pause at the node (its verbose log's `Writes resumed
  after X us`, from `CLUSTER PAUSEWRITES` to the handover table), the
  clients' worst INCR, the time to the table giving the replica the slots
  and to the exit; the node then started again, the next primary once
  it's a synced replica: `--runs` handovers.
- **drain:** the same node drained instead (its replica leaves first):
  the time until it holds no slot, the cutover pauses, the clients' worst
  INCR; a new cluster each run.
- **rolling:** each node SIGTERMed in turn and started again under its
  name: the slots moved (`stuart_slots_moved_total`), the handovers, the
  full and partial syncs the primaries served, the client's worst INCR
  and errors.
- **scale-down:** 6 nodes to 4, as `migrate_check scale-down`: two
  replicas of different primaries get SIGTERM, the second once the first
  exited, and one of the three primaries is demoted (its slots drained,
  then it's the replica of the primary lacking one): the slots moved,
  the demotions, the time from the second SIGTERM to the demotion's
  start, to the end of its drain, to every primary having a synced
  replica, the cutover pauses, the client's p99 before and during the
  drain (on any slot and on the demoted primary's), the full syncs; a
  new cluster each run.

Results: `results/<date>-<commit>/handover.md`.

## A replica's sync

```sh
tools/bench/sync_bench.sh                    # or --keys 1000000 --cut-keys 100000
tools/bench/sync_bench.sh --also redis-8.10.2=$HOME/redis-8/bin/redis-server \
                          --also valkey-9.1.2=$HOME/valkey-9/bin/valkey-server
```

A primary loaded with 1M and 10M keys (`key:%012d`, 16 B values), then a
replica attached: ours (stuart `--auto --replicas 1`, the second node
joining as the replica), Redis 7.0.15's and each `--also` server's
(`REPLICAOF`, diskless, with `repl-diskless-sync-delay 0`), each with no
clients and with redis-benchmark SETting random keys of those (`-c 50`)
meanwhile.
`sync_bench.py` measures the time from the replica's start (ours: the
process; Redis's: `REPLICAOF`) to its link up, and every 50 ms the Pss
(`/proc/PID/smaps_rollup`) of the primary, with Redis's forked child,
and of the replica: Pss counts a page shared after a fork once, so the
primary's peak shows what copy-on-write copied. Then the replica's
`DBSIZE` and offset are checked against the primary's. Pinned: primary
CPU 2, replica 4, stuart 14, redis-benchmark 8 and 10.

Then a cut (`--cut-keys`, default 1M): the replica reaches its primary
through a proxy in `sync_bench.py` (ours: the primary `--announce`s the
proxy's port, so stuart's connection goes through it too; theirs:
`REPLICAOF` the proxy), which a second after the link is up closes every
connection and turns new ones away for 1 s. Measured: the MB written
meanwhile, the time from the proxy's reopening to the link up, and
whether that was a partial resync or a full one (`INFO stats`
`sync_partial_ok`, `sync_full`); with no clients and with the SETs. Only
the cut goes through the proxy: the syncs above are direct. Results:
`results/<date>-<commit>/sync.md`.

## What a replica costs its primary

```sh
tools/bench/ack_bench.sh [--runs 5] [--base COMMIT] [--also NAME=SERVER ...]
```

redis-benchmark SET, GET, INCR and HSET (`-r 1000000`, 16 B values,
`-c 50`; one run per pipeline depth: `-P 1` with `-n 1000000`, `-P 16`
with `-n 4000000`) at a primary alone and with a replica: ours (stuart
`--auto`, `--replicas 1` for the replica), Redis 7.0.15 and each `--also`
server, standalone (`REPLICAOF`) and in cluster mode as ours runs (the
primary owns every slot, `CLUSTER REPLICATE`); with `--base`, ours built
from that commit too. Each figure is the median of `--runs`; a run during
which the clock stepped is run again. The replica's offset is checked
against the primary's after each set-up, and ours must have refused no
write (the lease). Pinned: primary CPU 2, replica 4, stuart 14,
redis-benchmark 8 and 10. The tables: requests/s, with a replica against
alone, ours against theirs. Run it on a quiet host: a busy one (a browser,
a game launcher) took up to a quarter off ours with `-P 16`, and less off
theirs. Results: `results/<date>-<commit>/acks.md`.

## Failover

```sh
tools/bench/failover_bench.sh [--timeouts 2,5] [--runs 3] [--also NAME=SERVER ...]
```

Three primaries with a replica each: ours (stuart `--auto --replicas 1
--fail-after T`), Redis 7.0.15's and each `--also` server's (`redis-cli
--cluster create --cluster-replicas 1`, the server's own `redis-cli` if
it has one, `cluster-node-timeout T`, `cluster-replica-validity-factor 0`:
WSL2's clock steps make a replica's data look too old otherwise), every
replica synced first. A redis-py RedisCluster INCRs keys of one primary's
slots, counting the INCRs acknowledged per key.

- **killed:** the primary is SIGKILLed: the time to the replica's
  promotion (`CLUSTER NODES` at stuart, or at the first node) and to the
  client's first INCR through again.
- **leaving** (timeout 5 s): ours gets SIGTERM, theirs `CLUSTER FAILOVER`
  at the replica: the time to the promotion and the client's longest gap
  between two acknowledged INCRs.

Both: the writes lost (INCRs acknowledged that the keys' values don't
hold), per run. Results: `results/<date>-<commit>/failover.md`.

## Memory with a replica

```sh
tools/bench/memory_bench.sh [--also NAME=SERVER ...]
```

Two datasets, each into a new primary with a replica, pipelined straight
to the primary: 1M strings (`key:%012d`, 16 B values) and collections
(10,000 each of hashes of 100 fields, sets of 100 members, lists of 100
elements, all 8-9 B; and 10 hashes and 10 sets of 100,000: 5M elements).
Ours, Redis 7.0.15 and each `--also` server, standalone and in cluster
mode. Once the replica has the primary's offset and keys: `used_memory`
and RSS (`VmRSS`) of both, and `used_memory`'s growth over the empty node
a key or an element. Results: `results/<date>-<commit>/memory.md`.

## Stalls while the keyspace grows

```sh
tools/bench/stall_bench.sh [--keys N] [--also NAME=SERVER ...]
```

10M new keys (`key:%012d`, 16 B values) from one connection, 100 `SET`s a
batch, into ours, Redis 7.0.15 and each `--also` server (a redis-server-like
binary, or another build of `dbnode`), one at a time, the server on CPU 2
and the client on CPU 4. Each batch's round trip is timed on the monotonic
clock: a keyspace rehashed all at once when it doubles shows as one slow
batch. The keys/s, the round trips' p50, p99.9 and max, how many took over
10 ms, and the three slowest with the keys there were. Results:
`results/<date>-<commit>/stalls.md`.

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

- `results/2026-10-27-d114045/` (throughput with a replica, standalone
  and cluster mode; sync and the 1 s cut), `results/2026-10-27-4103e02/`
  (failover) and `results/2026-10-27-ea5d41b/` (memory): phase 8
  milestone 10, against Redis 7.0.15, 8.10.2 and Valkey 9.1.2.
  `results/2026-10-26-dde1f70/acks.md`: the first throughput run,
  standalone theirs only.
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
