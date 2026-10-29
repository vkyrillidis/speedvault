# Compact-encoding step 4, 2026-10-23 (phase 7)

Large hashes and sets with one heap block an entry (`513bac8`, `store::block`)
against the commit before (`8e42c24`) and Redis 7.0.15, on WSL2 (Ryzen 9800X3D):
servers pinned to core 2, redis-benchmark to cores 4-7.

## Memory per element

`tools/bench/collections_memory.py`, 8-byte elements (a hash: field and
value of 8 bytes each):

| collection | before | after | Redis 7.0.15 | Redis 8.10.2 | Valkey 9.1.2 |
|---|---:|---:|---:|---:|---:|
| hash, 1 x 1000000 | 98.8 | 41.3 | 72.4 | 38.1 | 50.9 |
| set, 1 x 1000000 | 57.6 | 40.8 | 56.4 | 30.1 | 42.9 |
| intset, 1 x 1000000 | 57.6 | 40.8 | 48.4 | 22.1 | 34.9 |

(Redis and Valkey from `../2026-10-22-compact/`; small collections are
unchanged.) Just after the table doubled (`raw/doubling_memory.out`,
2^19 + 1 elements): 40 bytes beyond the payload for a hash's field and
value, 48 for a set's member, under the import estimate of 56.

## Throughput

`raw/speed.sh`: a hash and a set of 1M 12-digit elements, random
`HGET HSET SISMEMBER SADD` over them (`-r 1000000`, so `HSET` and `SADD`
mostly replace or find what's there), 2M requests, 50 clients, median of
three (`raw/speed.out`; requests/s):

| test | -P | Redis 7.0.15 | before | after | after / Redis | after / before |
|---|---:|---:|---:|---:|---:|---:|
| HGET | 1 | 347.6k | 382.1k | 381.8k | 1.10x | 1.00x |
| HSET | 1 | 329.7k | 369.5k | 394.2k | 1.20x | 1.07x |
| SISMEMBER | 1 | 312.8k | 390.2k | 391.5k | 1.25x | 1.00x |
| SADD | 1 | 325.5k | 405.9k | 416.1k | 1.28x | 1.03x |
| HGET | 16 | 1.75M | 2.03M | 2.40M | 1.37x | 1.18x |
| HSET | 16 | 1.16M | 1.71M | 2.16M | 1.85x | 1.26x |
| SISMEMBER | 16 | 1.75M | 2.18M | 2.54M | 1.45x | 1.17x |
| SADD | 16 | 1.87M | 2.31M | 2.24M | 1.20x | 0.97x |

SISMEMBER at `-P 16` is from a rerun of five
(`raw/sismember_p16_rerun.out`): the first run's "before" median, 313k,
came from disturbed runs.

## The doubling stall

`raw/stall.sh`: the one `HSET` that doubles a hash of 2^20 fields (the
entries and the index both grow), seven runs each (`raw/stall.out`):
median 5.9 ms before, 12.5 ms after. Rebuilding the index reads each
entry's hash from its block, a cache miss each, where before it read the
table's `Vec` in order. Kept (agreed 2026-10-23); incremental rehashing,
as Redis does, would remove the stall in either layout.
