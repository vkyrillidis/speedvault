# Compact encodings, 2026-10-22 (phase 7)

speedvault with small hashes packed (`b0ce187`), lists in packed nodes
(`b9f465d`) and intsets and small sets packed (`d77494e`), against Redis
7.0.15, Redis 8.10.2 and Valkey 9.1.2; the same setup as
`../2026-10-22-types/` (which has the numbers before).

- `types.md`: the data-type suite, `-P 1` and `-P 16`, 200000 requests a
  test. The machine was slower this run for every server (Redis 7.0.15's
  `-P 16` pushes 2.0-2.3M against 2.4-2.8M in the run before); the ratios
  are what to compare.
- Memory: `tools/bench/collections_memory.py`, below.

## Memory per element

Bytes, 8-byte elements (a hash: field and value of 8 bytes each; integer
members up to 7 digits):

| collection | Redis 7.0.15 | speedvault | (before) | Redis 8.10.2 | Valkey 9.1.2 |
|---|---:|---:|---:|---:|---:|
| hash, 10000 x 100 | 21.2 | 22.5 | 112.0 | 20.9 | 20.9 |
| set, 10000 x 100 | 64.7 | 13.2 | 67.5 | 13.2 | 13.2 |
| intset, 10000 x 100 | 2.9 | 6.1 | | 2.7 | 2.6 |
| list, 10000 x 100 | 11.9 | 13.3 | 46.8 | 10.7 | 10.7 |
| hash, 1 x 1000000 | 72.4 | 98.8 | 98.8 | 38.1 | 50.9 |
| set, 1 x 1000000 | 56.4 | 57.6 | 57.6 | 30.1 | 42.9 |
| intset, 1 x 1000000 | 48.4 | 57.6 | | 22.1 | 34.9 |
| list, 1 x 1000000 | 10.1 | 10.2 | 40.8 | 10.1 | 10.1 |

Small collections are now at Redis's size or near it: a packed element
costs its bytes and its length twice (once each way), as a listpack
element costs its bytes, its encoding byte and its back-length. Intsets
keep their members' digits (Redis packs 16-bit integers: 2 bytes); small
sets of strings are packed as Redis 7.2+ does, while replies and `OBJECT
ENCODING` stay 7.0.15's. What's left is large hashes and sets, a table
entry and a heap allocation per element (Redis 8 rewrote its dict entries;
1.3-2.6x it).

## Throughput

Ratio to Redis 7.0.15 (`types.md`):

| test | -P 1 | (before) | -P 16 | (before) |
|---|---:|---:|---:|---:|
| LPUSH | 1.22x | 1.55x | 1.18x | 1.35x |
| RPUSH | 1.36x | 1.44x | 1.37x | 1.06x |
| LPOP | 1.35x | 1.46x | 1.28x | 1.52x |
| RPOP | 1.32x | 1.53x | 1.53x | 1.60x |
| SADD | 1.38x | 1.57x | 1.61x | 1.47x |
| HSET | 1.28x | 1.42x | 1.48x | 1.42x |
| SPOP | 1.31x | 1.46x | 1.51x | 1.65x |
| LRANGE_100 | 1.14x | 0.94x | 1.15x | 1.21x |
| LRANGE_300 | 1.07x | 1.05x | 1.43x | 1.39x |
| LRANGE_500 | 0.94x | 0.83x | 1.35x | 1.31x |
| LRANGE_600 | 0.94x | 0.88x | 1.35x | 1.44x |

Still ahead of Redis on every write and pop; the `-P 1` ratios are lower
than in the run before, as are Redis 8's and Valkey's against 7.0.15 in
the same run (1.01-1.05x against 1.02-1.27x), so part of it is the machine.
LPUSH pays what Redis pays too: the front node's bytes move to make room
(with the push written in place, no allocation, 1.25x Redis at `-P 16` and
1.30x at `-P 1` on a rerun). A node with room kept at its front would make
it O(1) at the cost of that room: not done.

A hash of 500 fields, random `HSET`/`HGET` (both walk a listpack):
1.10-1.12x Redis at `-P 1` and `-P 16`.
