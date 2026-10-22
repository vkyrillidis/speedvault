# Data types, 2026-10-22 (phase 7 milestone 7)

speedvault at `d1dddda` against Redis 7.0.15 (the reference), Redis 8.10.2
and Valkey 9.1.2, on the Ryzen under WSL2, each server pinned to CPU 2,
redis-benchmark on CPUs 4-7, 50 connections, 200000 requests a test.

- `types.md`: `lpush,rpush,lpop,rpop,sadd,hset,spop,lrange_100..600`,
  `-P 1` and `-P 16` (now `full_run.sh --types`).
- `lrange.md`: the LRANGE tests again, alone (several cells of the first
  run were invalid: the WSL clock stepped during them).
- Memory: `tools/bench/collections_memory.py` (below).

## Throughput

Requests/s as a ratio to Redis 7.0.15 (from `types.md`, LRANGE from
`lrange.md`):

| test | -P | speedvault | Redis 8.10.2 | Valkey 9.1.2 |
|---|---:|---:|---:|---:|
| LPUSH | 1 | 1.55x | 1.27x | 1.27x |
| RPUSH | 1 | 1.44x | 1.16x | 1.17x |
| LPOP | 1 | 1.46x | 1.18x | 1.18x |
| RPOP | 1 | 1.53x | | 1.23x |
| SADD | 1 | 1.57x | 1.16x | 1.18x |
| HSET | 1 | 1.42x | 1.10x | 1.16x |
| SPOP | 1 | 1.46x | 1.02x | 1.03x |
| LRANGE_100 | 1 | 0.94x | 0.78x | 0.76x |
| LRANGE_300 | 1 | 1.05x | 0.92x | 0.84x |
| LRANGE_500 | 1 | 0.83x | 0.85x | 0.85x |
| LRANGE_600 | 1 | 0.88x | 0.86x | 0.85x |
| LPUSH | 16 | 1.35x | 0.91x | 0.84x |
| RPUSH | 16 | 1.06x | 0.88x | 0.79x |
| LPOP | 16 | 1.52x | 0.81x | 0.75x |
| RPOP | 16 | 1.60x | 0.93x | 0.77x |
| SADD | 16 | 1.47x | 0.89x | 0.79x |
| HSET | 16 | 1.42x | 0.89x | 0.80x |
| SPOP | 16 | 1.65x | 0.91x | 0.88x |
| LRANGE_100 | 16 | 1.21x | 1.15x | 1.20x |
| LRANGE_300 | 16 | 1.39x | 1.13x | 1.05x |
| LRANGE_500 | 16 | 1.31x | 1.04x | 1.04x |
| LRANGE_600 | 16 | 1.44x | 1.21x | 1.01x |

LRANGE_500/600 without pipelining: all four servers land between 38k and
51k requests/s, and the order changes from run to run (best of three, pinned:
Redis 47.6k, ours 48.1k). There the limit is redis-benchmark's one thread
reading 500-element replies, not the server: the same number of syscalls a
request at both (`strace -c`: one read, one write), and with `-P 16`, where
the server is the limit, ours is 1.31-1.44x Redis.

## Memory per element

`used_memory` growth over the elements filled, 8-byte strings (a hash:
field and value of 8 bytes each; set members not integers, so no intset),
servers emptied with `FLUSHALL SYNC` in between (Valkey frees
asynchronously by default):

| collection | Redis 7.0.15 | speedvault | Redis 8.10.2 | Valkey 9.1.2 |
|---|---:|---:|---:|---:|
| hash, 10000 x 100 | 21.2 (listpack) | 112.0 | 20.9 (listpack) | 20.9 (listpack) |
| set, 10000 x 100 | 64.7 (hashtable) | 67.5 | 13.2 (listpack) | 13.2 (listpack) |
| list, 10000 x 100 | 11.9 (quicklist) | 46.8 | 10.7 (listpack) | 10.7 (listpack) |
| hash, 1 x 1000000 | 72.4 | 98.8 | 38.1 | 50.9 |
| set, 1 x 1000000 | 56.4 | 57.6 | 30.1 | 42.9 |
| list, 1 x 1000000 | 10.1 | 40.8 | 10.1 | 10.1 |

(Encodings in brackets are what `OBJECT ENCODING` says; ours reports what
Redis 7.0.15 would, its storage is the plain table or `VecDeque`.)

Where skipping the compact encodings shows: a small hash takes 5x Redis's
memory, a list 4x at any size (a box per element against Redis's packed
nodes). Sets are level with Redis 7.0.15, which has no listpack sets;
Redis 7.2+ packs small sets too. A large hash is 1.4x Redis 7.0.15 (2.6x
Redis 8). The compact encodings were left for later on purpose (phase 7
plan); these numbers say where they pay: lists and small hashes first.
