# Moving slots after fixes A, B, C (phase 8 milestone 6, step 3 item 6)

`tools/bench/migrate_bench.sh` on the Ryzen 9800X3D (WSL2), 2026-10-25, at
0b36234, set up as in `../2026-10-25-0e7045b/migrate.md` (the findings
before the fixes). stuart's defaults were still `--moves-per-source 32
--max-moves 128` in this run; the rebalance and drain tables give each
pair. The impact pairs now apply to both paths: by snapshot
`--migrate-batch` is the copy's batch and `--migrate-pause` the pause
after each while clients were served.

The fixes (0b36234):

- **A.** stuart cuts over every link that caught up within 20 ms of the
  first pause, in the table that also ends the moves key by key and starts
  the next moves (`State::advance`): one state save (`fsync`) instead of
  two or more per link. Then (after this run) the defaults went to
  `--moves-per-source 128 --max-moves 512`, from the drain and rebalance
  tables below.
- **B.** a target whose source refuses `SLOTSYNC` (the source hasn't
  taken the table yet) asks again every 5 ms for a second, instead of
  failing the link for 500 ms.
- **C.** the slot copy goes in batches of `--migrate-batch` keys (100)
  instead of 1000, with `--migrate-pause` (50 µs) after each while
  clients were served.

Against before (0e7045b):

| | before | after |
|---|---|---|
| drain, 4096 slots, 50k keys, 32/128 | 3.4–5.1 s (key by key 1.6 s) | 1.73–1.76 s (key by key 1.61–1.62 s) |
| drain, 128/512 | 1.3–1.4 s (an experiment) | **0.52–0.53 s** (key by key 0.65–0.73 s) |
| REBALANCE of 1M keys, 8/64 | 10.46 s (key by key 3.34 s) | 3.95 s (3.55 s) |
| REBALANCE, 32/128 | 3.09 s (1.90 s) | **1.72 s** (1.91 s) |
| REBALANCE, 128/512 | | **1.15 s** (1.73 s) |
| source SET during a copy (no move: ~385k) | 96k / p99 2.56 ms, moving 802k keys/s | 224k / p99 1.09 ms, moving 524k keys/s (key by key 260k / 0.44 ms at 235k keys/s) |
| cutover pause p50 / max (target responsive) | 7–14 / 14–24 ms | 7–9 / 8–23 ms |

What is left:

- By snapshot, at the default batch and pause, a move costs a busy source
  a little more than key by key (224k against 260k requests/s, p99
  1.09 against 0.44 ms) while moving keys 2.2× as fast; `--migrate-pause
  100` gives 239k at the same speed. The source's p99 stays about 1 ms
  against 0.44 key by key; why isn't measured yet.
- Pauses of 78–219 ms remain, now and then, when the target stalls doubling
  its keyspace map (finding 5 of the earlier results; fix D, not done).
- The client errors key by key are redis-py's `-ASK` bug; none by snapshot.

## Tables

**Moving speed** (no load but one RedisCluster client): slots from one node to another, STUART MOVE to the last table; the client's SET p99 / max (ms); the target's used_memory grown (per key)

| path | values | keys | slots | time | keys/s | MB/s | pause p50 / max ms (cutovers) | client p99 / max | target memory |
|---|---|---|---|---|---|---|---|---|---|
| snapshot, batch 100 | none | 0 | 1024 | 0.46 s | | | 7.0 / 7.6 (32) | 0.31 / 5.9 | |
| key by key, batch 100 | none | 0 | 1024 | 0.42 s | | |  | 0.31 / 1.8 | |
| snapshot, batch 100 | 16 B | 1,000,000 | 8 | 0.73 s | 1364k | 22 | 15.0 / 15.0 (1) | 2.65 / 160.1 | 200 MB (200 B) |
| snapshot, batch 1000 | 16 B | 1,000,000 | 8 | 0.63 s | 1587k | 25 | 15.0 / 15.0 (1) | 3.11 / 358.7 | 200 MB (200 B) |
| key by key, batch 100 | 16 B | 1,000,000 | 8 | 2.37 s | 423k | 7 |  | 0.32 / 104.3 | 201 MB (201 B) |
| key by key, batch 1000 | 16 B | 1,000,000 | 8 | 1.25 s | 803k | 13 |  | 0.60 / 99.5 | 201 MB (201 B) |
| snapshot, batch 100 | 16 B | 1,000,000 | 1024 | 1.35 s | 743k | 12 | 7.4 / 8.1 (32) | 1.00 / 143.4 | 201 MB (201 B) |
| snapshot, batch 1000 | 16 B | 1,000,000 | 1024 | 1.15 s | 873k | 14 | 7.6 / 8.2 (32) | 1.89 / 138.8 | 201 MB (201 B) |
| key by key, batch 100 | 16 B | 1,000,000 | 1024 | 2.48 s | 403k | 6 |  | 0.35 / 154.2 | 201 MB (201 B) |
| key by key, batch 1000 | 16 B | 1,000,000 | 1024 | 1.16 s | 865k | 14 |  | 0.67 / 141.7 | 201 MB (201 B) |
| snapshot, batch 100 | 1024 B | 300,000 | 8 | 0.54 s | 553k | 567 | 8.2 / 8.2 (1) | 1.20 / 425.9 | 360 MB (1200 B) |
| snapshot, batch 1000 | 1024 B | 300,000 | 8 | 0.33 s | 896k | 918 | 18.2 / 18.2 (1) | 6.41 / 42.2 | 360 MB (1200 B) |
| key by key, batch 100 | 1024 B | 300,000 | 8 | 0.97 s | 310k | 317 |  | 0.37 / 34.5 | 360 MB (1201 B) |
| key by key, batch 1000 | 1024 B | 300,000 | 8 | 0.66 s | 456k | 467 |  | 1.15 / 41.7 | 360 MB (1200 B) |
| snapshot, batch 100 | 1024 B | 300,000 | 1024 | 0.78 s | 385k | 394 | 7.8 / 9.1 (32) | 1.17 / 35.9 | 360 MB (1200 B) |
| snapshot, batch 1000 | 1024 B | 300,000 | 1024 | 0.78 s | 386k | 395 | 7.9 / 8.4 (32) | 1.91 / 40.0 | 360 MB (1200 B) |
| key by key, batch 100 | 1024 B | 300,000 | 1024 | 1.00 s | 299k | 306 |  | 0.45 / 37.4 | 360 MB (1201 B) |
| key by key, batch 1000 | 1024 B | 300,000 | 1024 | 0.88 s | 340k | 348 |  | 0.59 / 45.1 | 360 MB (1201 B) |
| snapshot, batch 100 | 16384 B | 30,000 | 8 | 0.43 s | 70k | 1146 | 17.8 / 17.8 (1) | 0.79 / 289.0 | 497 MB (16575 B) |
| snapshot, batch 1000 | 16384 B | 30,000 | 8 | 0.22 s | 135k | 2218 | 8.4 / 8.4 (1) | 1.03 / 117.2 | 497 MB (16576 B) |
| key by key, batch 100 | 16384 B | 30,000 | 8 | 0.44 s | 68k | 1111 |  | 0.57 / 3.4 | 497 MB (16579 B) |
| key by key, batch 1000 | 16384 B | 30,000 | 8 | 0.44 s | 68k | 1114 |  | 0.63 / 3.1 | 497 MB (16579 B) |

**Clients during a move**: 4M keys of 16 B moving out of 1024 slots; redis-benchmark `-c 50` on keys of a slot staying at the source (SET and GET in turn) and of one at the target (SET), 0.3 s runs for 4 s or until the move ends: mean requests/s / worst p99 (ms); the RedisCluster client's SET p99 / max (ms) over the whole move

| path | moved keys/s | source SET | source GET | target SET | pause p50 / max ms (cutovers) | client p99 / max |
|---|---|---|---|---|---|---|
| no move | | 381k / 0.29 | 392k / 0.22 | 391k / 0.29 | | |
| snapshot, batch 100, pause 50 µs | 524k | 224k / 1.09 | 243k / 0.99 | 301k / 0.51 | 8.5 / 218.7 (32) | 1.27 / 640.0 |
| snapshot, batch 100, pause 100 µs | 525k | 239k / 1.07 | 257k / 1.14 | 308k / 0.53 | 8.4 / 21.5 (32) | 1.27 / 538.9 |
| snapshot, batch 1000, pause 100 µs | 845k | 144k / 1.39 | 132k / 1.36 | 196k / 1.29 | 7.8 / 17.1 (32) | 1.88 / 575.6 |
| snapshot, batch 1000, pause 1000 µs | 507k | 211k / 1.67 | 270k / 1.55 | 274k / 0.61 | 8.2 / 18.6 (32) | 1.50 / 660.7 |
| key by key, batch 100, pause 50 µs | 235k | 260k / 0.44 | 254k / 0.49 | 353k / 0.39 |  | 0.45 / 172.1 |
| key by key, batch 100, pause 100 µs | 224k | 257k / 0.47 | 260k / 0.50 | 363k / 0.34 |  | 0.44 / 153.6 |
| key by key, batch 1000, pause 100 µs | 645k | 197k / 0.77 | 177k / 0.83 | 260k / 0.61 |  | 0.84 / 358.4 |
| key by key, batch 1000, pause 1000 µs | 398k | 246k / 0.70 | 248k / 0.73 | 310k / 0.52 |  | 0.74 / 355.3 |

**REBALANCE** of 4M keys of 16 B from three nodes onto a fourth (1M keys and 4096 slots move); redis-benchmark `-c 50` SET on a slot the first node keeps: mean requests/s / worst p99 (ms); the RedisCluster client's SET p99 / max (ms); the new node's used_memory after

| path | `--moves-per-source` | `--max-moves` | time | keys/s | clients before | clients during | pause p50 / max ms (cutovers) | client p99 / max | target memory |
|---|---|---|---|---|---|---|---|---|---|
| snapshot | 8 | 64 | 3.95 s | 254k | 392k / 0.26 | 342k / 0.46 | 7.8 / 162.3 (513) | 0.53 / 156.6 | 202 MB |
| snapshot | 32 | 128 | 1.72 s | 580k | 383k / 0.29 | 316k / 0.49 | 8.0 / 18.8 (129) | 0.73 / 173.2 | 202 MB |
| snapshot | 64 | 256 | 1.34 s | 747k | 413k / 0.28 | 299k / 0.51 | 7.8 / 78.4 (66) | 0.91 / 153.9 | 202 MB |
| snapshot | 128 | 512 | 1.15 s | 870k | 416k / 0.26 | 311k / 0.50 | 9.0 / 23.0 (33) | 0.80 / 163.1 | 202 MB |
| key-by-key | 8 | 64 | 3.55 s | 282k | 391k / 0.26 | 359k / 0.32 |  | 0.38 / 166.6, 1 errors | 202 MB |
| key-by-key | 32 | 128 | 1.91 s | 523k | 392k / 0.26 | 317k / 0.43 |  | 0.55 / 157.6, 4 errors | 202 MB |
| key-by-key | 64 | 256 | 1.73 s | 579k | 389k / 0.30 | 305k / 0.41 |  | 0.55 / 155.4, 6 errors | 202 MB |
| key-by-key | 128 | 512 | 1.73 s | 578k | 388k / 0.26 | 312k / 0.41 |  | 0.57 / 166.0, 12 errors | 202 MB |

**Drain** (as migrate_check's sigterm): stuart `--auto`, four nodes, 200k keys of 100 B; STUART LEAVE of the first, until it holds nothing (4096 slots, 50k keys move); the RedisCluster client's SET p99 / max (ms)

| path | `--moves-per-source` | `--max-moves` | time | slots/s | pause p50 / max ms (cutovers) | client p99 / max |
|---|---|---|---|---|---|---|
| snapshot | 32 | 128 | 1.74 s | 2351 | 7.8 / 12.3 (130, 2 missed) | 0.72 / 8.3 |
| snapshot | 32 | 128 | 1.73 s | 2369 | 7.8 / 11.1 (130, 2 missed) | 0.71 / 8.0 |
| snapshot | 32 | 128 | 1.76 s | 2325 | 7.8 / 9.1 (130, 2 missed) | 0.71 / 8.9 |
| snapshot | 128 | 512 | 0.53 s | 7756 | 8.0 / 8.8 (34, 2 missed) | 0.79 / 8.0 |
| snapshot | 128 | 512 | 0.53 s | 7662 | 8.0 / 9.3 (34, 2 missed) | 0.87 / 9.6 |
| snapshot | 128 | 512 | 0.52 s | 7902 | 7.9 / 8.4 (34, 2 missed) | 0.90 / 9.1 |
| key-by-key | 32 | 128 | 1.62 s | 2532 |  | 0.69 / 8.1 |
| key-by-key | 32 | 128 | 1.62 s | 2536 |  | 0.70 / 6.8 |
| key-by-key | 32 | 128 | 1.61 s | 2546 |  | 0.68 / 8.8 |
| key-by-key | 128 | 512 | 0.68 s | 6052 |  | 0.74 / 6.9 |
| key-by-key | 128 | 512 | 0.65 s | 6256 |  | 0.72 / 7.4 |
| key-by-key | 128 | 512 | 0.73 s | 5633 |  | 0.75 / 8.0 |
