# Performance

`cargo bench -p fiber` on a Ryzen 9800X3D (WSL2):

| Operation                                  | Cost         |
|--------------------------------------------|--------------|
| Raw context switch (`arch::switch`)        | 6.7 ns       |
| `Fiber::resume` + `suspend`, per switch    | 14 ns        |
| New fiber, stack from `StackPool`          | 51 ns        |
| New fiber, freshly mmap'd stack            | 1.4 µs       |
| Reactor ping-pong over a socket pair       | 1.2 µs/round trip (~840k/s) |
| … with a deadline on every read            | 1.5 µs/round trip           |
| Reactor ping-pong over TCP loopback        | 2.6 µs/round trip (~385k/s) |
| `sleep(0)`: arm, one loop turn, fire       | 160 ns       |
| Timer with 10k fibers sleeping at once     | 190 ns       |

`redis-benchmark` against the dbnode on a Ryzen 9800X3D (WSL2, 2026-10-17,, `tools/bench/full_run.sh --also redis-8.10.2=... --also
valkey-9.1.2=...`, results in `tools/bench/results/2026-10-17-ef94116/`;
`-n 1000000 -c 50 -d 3`, one redis-benchmark 7.0.15 client thread on CPUs
4-7, each server pinned to CPU 2, Redis 8 and Valkey with their default
settings (one I/O thread); run-to-run noise about ±10%). Requests/s, and
the ratio to Redis 7.0.15:

| Test             | `-P 1`            | `-P 16`           | `-P 64`             |
|------------------|-------------------|-------------------|---------------------|
| PING_INLINE      | 395k (1.48×)      | 5.29M (1.84×)     | 13.0M (2.90×)       |
| PING_MBULK       | 399k (1.53×)      | 5.29M (1.31×)     | 13.2M (2.03×)       |
| SET              | 391k (1.38×)      | 5.29M (1.99×)     | 11.9M (2.95×)       |
| GET              | 398k (1.48×)      | 5.35M (1.75×)     | 12.3M (2.53×)       |
| INCR             | 397k (1.47×)      | 4.93M (1.60×)     | 7.94M (1.58×)       |
| MSET (10 keys)   | 406k (1.09×)      | 1.96M (2.24×)     | 2.54M (2.64×)       |

The same run, our node against the newer servers (ratio = ours / theirs):

| Test             | Redis 8.10.2 `-P 1` | `-P 16` | `-P 64` | Valkey 9.1.2 `-P 1` | `-P 16` | `-P 64` |
|------------------|------------|-------|-------|------------|-------|-------|
| PING_INLINE      | 295k (1.34×) | 1.72× | 2.53× | 285k (1.38×) | 1.83× | 2.90× |
| PING_MBULK       | 287k (1.39×) | 1.32× | 2.12× | 288k (1.38×) | 1.49× | 2.29× |
| SET              | 311k (1.26×) | 2.40× | 3.74× | 310k (1.26×) | 2.91× | 4.60× |
| GET              | 294k (1.36×) | 1.93× | 2.98× | 302k (1.32×) | 2.23× | 3.56× |
| INCR             | 308k (1.29×) | 1.90× | 1.87× | 299k (1.33×) | 2.13× | 2.30× |
| MSET (10 keys)   | 287k (1.42×) | 2.71× | 3.50× | 236k (1.72×) | 3.59× | 4.56× |

Without pipelining Redis 8.10.2 and Valkey 9.1.2 are 7–14% faster than
7.0.15 (MSET slower: 0.77× and 0.64×); with pipelining they are mostly
slower than 7.0.15 on one core (0.58–1.14×). So our lead without
pipelining is smaller against them, 1.26–1.39× (MSET 1.42× and 1.72×), and
larger with pipelining. The earlier run gave 1.42–1.55× Redis
7.0.15 without pipelining; this one 1.38–1.53× (MSET 1.06× then, 1.09×
now), within the noise.

Without pipelining, p50 latency is 0.063 ms (Redis 7.0.15: 0.087–0.095;
Redis 8 and Valkey: 0.087) and p99 0.20–0.23 ms (the others: 0.27–0.36, MSET
0.34–0.43).
At `-P 16` and above the single redis-benchmark thread may be the limit.

**Data types** (2026-10-22, `full_run.sh --types`, 200000
requests a test, same setup; results and the method in
`tools/bench/results/2026-10-22-types/`). Ratio to Redis 7.0.15:

| Test | ours `-P 1` | `-P 16` | Redis 8.10.2 `-P 1` | `-P 16` | Valkey 9.1.2 `-P 1` | `-P 16` |
|------|------|------|------|------|------|------|
| LPUSH | 1.55× | 1.35× | 1.27× | 0.91× | 1.27× | 0.84× |
| RPUSH | 1.44× | 1.06× | 1.16× | 0.88× | 1.17× | 0.79× |
| LPOP | 1.46× | 1.52× | 1.18× | 0.81× | 1.18× | 0.75× |
| RPOP | 1.53× | 1.60× | | 0.93× | 1.23× | 0.77× |
| SADD | 1.57× | 1.47× | 1.16× | 0.89× | 1.18× | 0.79× |
| HSET | 1.42× | 1.42× | 1.10× | 0.89× | 1.16× | 0.80× |
| SPOP | 1.46× | 1.65× | 1.02× | 0.91× | 1.03× | 0.88× |
| LRANGE_100 | 0.94× | 1.21× | 0.78× | 1.15× | 0.76× | 1.20× |
| LRANGE_300 | 1.05× | 1.39× | 0.92× | 1.13× | 0.84× | 1.05× |
| LRANGE_500 | 0.83× | 1.31× | 0.85× | 1.04× | 0.85× | 1.04× |
| LRANGE_600 | 0.88× | 1.44× | 0.86× | 1.21× | 0.85× | 1.01× |

(One RPOP cell was lost to a clock step.) `LRANGE_500/600` without
pipelining: all four servers between 38k and 51k requests/s, in an order
that changes from run to run (best of three: Redis 47.6k, ours 48.1k):
redis-benchmark's one thread reading 500-element replies is the limit; the
syscalls per request are the same as Redis's.

Memory per element (`used_memory`, 8-byte elements; a hash's field and
value 8 bytes each):

| Collection | Redis 7.0.15 | ours | Redis 8.10.2 | Valkey 9.1.2 |
|------------|------:|------:|------:|------:|
| hash, 10000 × 100 | 21 | 22.5 (was 112) | 21 | 21 |
| set, 10000 × 100 (not integers) | 65 | 13 (was 68) | 13 | 13 |
| set, 10000 × 100 (integers) | 2.9 | 6.1 | 2.7 | 2.6 |
| list, 10000 × 100 | 12 | 13 (was 47) | 11 | 11 |
| hash, 1 × 1M | 72 | 99 | 38 | 51 |
| set, 1 × 1M | 56 | 58 | 30 | 43 |
| list, 1 × 1M | 10 | 10 (was 41) | 10 | 10 |

Small collections and lists are packed as Redis packs them (see the phase
7 plan, compact encodings; `tools/bench/results/2026-10-22-compact/`), and
are now at Redis's size or near it; small sets of strings at Redis 8's.
What's left is large hashes and sets: a table entry and a heap allocation
per element, 1.3× Redis 7.0.15 and 2.6× Redis 8 for a hash. A hash of ~1M
fields also stalls ~3 ms when its table doubles (Redis rehashes
incrementally).

How we got there. The first run was 0.84–0.94× Redis without
pipelining. Per request our user time was equal or lower (MSET: half of
Redis's), but we spent more time in the kernel sleeping and waking.
redis-benchmark's one thread is the bottleneck there, and each time it had
to wake the server, or was woken for a single reply, it paid for it, more so
in a VM. Three changes:
- The replies of a turn leave together, like Redis's (kernel time per GET
  4.7 → 3.7 µs with the server saturated).
- A read after a short read waits for epoll instead of failing with EAGAIN
  (4–10% less CPU per request saturated).
- `--busy-poll 20` (µs) by default: the server polls before sleeping, so a
  client's next request rarely has to wake it. This is the big one (0.9× →
  1.4×), and it keeps the server's core busy under steady load;
  `--busy-poll 0` sleeps at once, like Redis.

`tools/bench` guards against a WSL2 clock problem that corrupted the first
run (the clock jumped by seconds every 5.5 s); see `tools/bench/README.md`.

**Memory** (phase 6 milestone 3; `used_memory` after writing the keys
into an empty node, standalone, Redis 7.0.15 the same way, 2026-10-19):

| Keys × value      | speedvault            | Redis 7.0.15          |
|-------------------|-----------------------|-----------------------|
| 1M × 10 bytes     | 184 MB (184 B a key)  | 88 MB (88 B a key)    |
| 1M × 100 bytes    | 264 MB (264 B a key)  | 184 MB (184 B a key)  |
| 100k × 1 KB       | 111 MB (1115 B a key) | 110 MB (1099 B a key) |
| 10k × 10 KB       | 101 MB (10144 B a key)| 103 MB (10317 B a key)|
| 500 × 1 MB        | 500 MB                | 524 MB                |

Keys are 12 bytes (`key:00000000`). The count is close to what the
process holds: with 1M small keys the RSS was 202 MB for 184 MB counted
(Redis: 111 MB for 88 MB). For small values we take about twice Redis's
memory (see Open items); from 1 KB up, the same or less.

Counting it costs nothing measurable: `tools/bench/bench.sh` against the
node before the count, with it, and with `--maxmemory 8gb`
(the `-OOM` check on every write), `-P 1`, 16 and 64: SET, GET, INCR and
MSET on one key 0.90–1.06× (within the run-to-run noise), SET of 100-byte
values on 1M random keys (an allocation each) 0.97–1.21×
(`tools/bench/results/2026-10-19-memory/`).

**Busy time and metrics** (phase 6 milestone 4;
`tools/bench/metrics_check.sh`, 2026-10-19,
`results/2026-10-19-metrics/`): a node's busy time (what
`speedvault_busy_seconds_total` counts) against its CPU time, under
redis-benchmark SET then GET, `-P 1`:

| `--busy-poll` | clients | wall  | CPU    | busy   | busy per request |
|---------------|---------|-------|--------|--------|------------------|
| 0             | 50      | 7.3 s | 4.64 s | 4.41 s | 2.20 µs          |
| 0             | 1       | 16.5 s| 7.18 s | 6.24 s | 31.2 µs          |
| 20 µs         | 50      | 5.2 s | 4.56 s | 3.48 s | 1.74 µs          |
| 20 µs         | 1       | 9.5 s | 8.21 s | 5.16 s | 25.8 µs          |

Without busy-polling the busy time is the CPU time, less what going to
sleep and waking takes (5% under load, 13% with one client, which makes
the thread sleep after every request). With it, the CPU time tracks the
wall time while the busy time doesn't: with one client the CPU says 86%
of a core and the busy time 54%, and a node with no clients spins
without counting. Busy-polling also makes a request cheaper (1.74 against
2.20 µs: no sleep and wakeup), as the throughput gain says. A lone
client's request costs some 25 µs under WSL2, mostly its reply's `write`
waking the client. Serving and scraping `/metrics` every second costs
nothing measurable: SET and GET at `-P 1` and `-P 16`, 0.98–1.01× a node
without `--metrics` (medians of three).

**3-node clusters** (`tools/bench/cluster_bench.sh`, 2026-10-17, two runs:
`results/2026-10-17-7a98a6c/` and, after the faster slot hash,
`results/2026-10-17-bbf979c/`): clusters of Redis 7.0.15, speedvault
(stuart and three dbnodes), Redis 8.10.2 and Valkey 9.1.2, each node pinned
to its own core, one redis-benchmark per node at once on three more cores
(`-c 50 -r 100000`, keys of the node's slots), requests/s summed. Ours, as
a ratio to each, over both runs:

| against        | `-P 1`      | `-P 16`     | `-P 64`     |
|----------------|-------------|-------------|-------------|
| Redis 7.0.15   | 1.08–1.69×  | 1.43–2.46×  | 1.69–2.33×  |
| Redis 8.10.2   | 1.02–1.47×  | 1.25–1.80×  | 1.40–2.10×  |
| Valkey 9.1.2   | 1.00–1.52×  | 1.39–2.33×  | 1.63–2.55×  |

The low ends at `-P 1` are rows where the other server's run was hit by a
clock step (marked † in the results, not corrected); without them our
cluster is 1.18× or more ahead everywhere. Each of our nodes does about
400k requests/s unpipelined, as a standalone node does: three nodes give
three times one (1.19–1.22M/s), the same scaling as Redis's. Pipelined
numbers moved by up to 15% between the two runs.

`redis-benchmark --cluster --threads 3` (the usual way to report cluster
numbers) agrees at `-P 1` (ours 1.0–1.3M/s for SET, GET and INCR, 0.8M/s
for MSET; 1.17–1.65× Redis 7.0.15), but
with pipelining its run times come out in coarse steps (7,989,348 and
11,976,048 requests/s recur for several servers), so it is a cross-check
only; one redis-benchmark per node is what the table above uses.

The cost of cluster mode (one node standalone against the same node in
cluster mode owning every slot, `bench.sh`): none to 6% at `-P 1` and `-P
16`; 4–10% at `-P 64`, where the server is the limit (~85 ns per request).
That is the slot check itself (skipping it brings cluster mode to 1.00×).
It was 25–30% at `-P 64` before `key_hash_slot` became 3.5× faster
(slicing-by-8 CRC16 and 8-byte scans for `{`; 4.9 ns for a
16-byte key, `cargo bench -p cluster`).

**Moving slots** (`tools/bench/migrate_bench.sh`, 2026-10-18, results in
`tools/bench/results/2026-10-18-276a495/migrate.md`; each process on its
own core). With no clients, keys move from one node to another at:

| values | `--migrate-batch 10` | `100` (default) | `1000` |
|--------|------|------|------|
| 16 B   | 501k keys/s | 530k keys/s | 789k keys/s |
| 1 KiB  | 221k (226 MB/s) | 357k (365 MB/s) | 468k (480 MB/s) |
| 16 KiB | 46k (760 MB/s) | 68k (1.1 GB/s) | 89k (1.5 GB/s) |

(8 slots of 1M, 300k and 30k keys; spread over 1024 slots the 16 B keys
move at 497k/s with batch 100, and 1024 empty slots take 0.43 s, stuart's
two tables per slot overlapping 32 at a time.)

While a source saturated by redis-benchmark (`-c 50`, unpipelined; 384k
SET/s alone) sends 4M keys of 16 B, the clients get:

| `--migrate-batch` / `--migrate-pause` | keys/s moved | source SET/s, p99 | target SET/s, p99 |
|---|---|---|---|
| no move | | 384k, 0.29 ms | 382k, 0.36 ms |
| 10 / 0 | 151k | 334k, 0.46 ms | 374k, 0.32 ms |
| 100 / 0 (the old default) | 398k | 234k, 0.61 ms | 350k, 0.42 ms |
| 1000 / 0 | 748k | 176k, 1.16 ms | 239k, 0.68 ms |
| **100 / 50 µs** (default) | 296k | 285k, 0.52 ms | 360k, 0.32 ms |
| 100 / 100 µs | 291k | 308k, 0.54 ms | 359k, 0.34 ms |
| 100 / 1000 µs | 85k | 347k, 0.38 ms | 377k, 0.29 ms |

Without a pause a move costs the clients about one request per three keys
moved, whatever the batch. A short pause after each batch (50–100 µs)
costs the move a quarter of its speed and gives the clients back half of
what it took; longer ones gain little more. The pause applies only while
clients are served (an idle node moves at full speed); before that, it
also needed a reactor that sleeps less than a millisecond (`epoll_pwait2`).
Bigger batches move faster but double p99. Commands on a key in flight
wait for its batch's round trip, not measured here (this load uses slots
that stay).

`STUART REBALANCE` of 4M keys from three nodes onto a fourth (1M keys and
4096 slots move), a client saturating the first node (382k SET/s before):

| `--moves-per-source` / `--max-moves` | time | keys/s | client SET/s, p99 |
|---|---|---|---|
| 1 / 64 | 13.98 s | 71k | 373k, 0.49 ms |
| 8 / 64 (the old default) | 3.32 s | 301k | 357k, 0.44 ms |
| 16 / 64 | 2.32 s | 431k | 346k, 0.44 ms |
| **32 / 128** (default) | 1.76 s | 567k | 336k, 0.43 ms |
| 64 / 256 | 1.66 s | 603k | 329k, 0.54 ms |

A node moves one slot at a time either way; more slots migrating keep its
next one ready while stuart ends the last (each slot takes two tables).
Past 32 per source little is gained.

`cargo bench -p reactor` on an Apple M1 Pro (macOS 27, kqueue):

| Operation                                  | Cost         |
|--------------------------------------------|--------------|
| Reactor ping-pong over a socket pair       | 2.9 µs/round trip (~340k/s) |
| … with a deadline on every read            | 3.2 µs/round trip           |
| Reactor ping-pong over TCP loopback        | 16 µs/round trip (~62k/s)   |
| `sleep(0)`: arm, one loop turn, fire       | 11.6 µs (the empty `kevent`, see open items) |
| Timer with 10k fibers sleeping at once     | 350 ns       |

**Autoscaling in kind** (`tools/k8s/kind_check.sh --autoscale`,
2026-10-20, results in `tools/bench/results/2026-10-20-autoscale/`): kind
v0.29.0 (Kubernetes 1.33.1) on Docker Desktop (16 CPUs, 32 GB), the kind
overlay's nodes (1 CPU limit, 256 MiB, `maxmemory` 192 MiB, `--busy-poll
0`), Prometheus scraping every 5 s, the test's HPA (`tools/k8s/autoscale`:
a 15 s window, two pods per 30 s, at most six, busy target 400m). A checker
writes throughout (redis-py, redis-cli, Lettuce 7.8.0 with its default
refresh); every scenario ended with no write lost or doubled and no client
error but redis-py's `-ASK` bug (counted apart):

| Scenario | From the load | The HPA asks | Pods ready | Slots even |
|----------|---------------|--------------|------------|------------|
| `hpa-memory`: 3 nodes filled to 95.5% of `maxmemory` (550 MB of 16 KiB values) | the fill done | 3 → 5 at +12 s | +15 s | +16 s; then no more pods in 60 s (ratio 0.575) |
| `hpa-cpu`: memtier_benchmark at a fixed rate per node, 5 nodes ~51% busy | the load started | 5 → 6 at +42 s (the busy rate is over a minute) | +42 s | +43 s |

During `hpa-cpu` the client's p99, second by second, stayed within
1.0–2.4 ms (1.6 ms over the run, p50 0.18 ms, p99.9 4.0 ms), the move to
the sixth node included; its throughput went from ~560k to ~615k requests
a second once the sixth node had its share (memtier's rate is per node).
By hand: 6 → 3 pods in 40 s (the HPA added none back), a rolling update of
three pods in 47 s, a deleted pod back in 25 s, Lettuce without a failure
in each. Before the nodes announced their pods' addresses (they announced
names), Lettuce lost 4–7 commands to timeouts in each rolling update: see
"Run on Kubernetes".
