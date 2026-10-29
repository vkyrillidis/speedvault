# Moving slots: by snapshot against key by key (phase 8 milestone 6, step 3 item 6)

`tools/bench/migrate_bench.sh` on the Ryzen 9800X3D (WSL2), 2026-10-25, at
0e7045b, both paths: by snapshot (stuart's default) and key by key (stuart
`--key-by-key`, phase 5's path; defaults `--migrate-batch 100`,
`--migrate-pause 50` µs). stuart `--moves-per-source 32 --max-moves 128`
unless a table says otherwise; its state file on WSL2's ext4 disk. Each
process pinned to its own CPU; requests/s the mean of 0.3 s
redis-benchmark runs, p99 the worst of them; runs during a WSL2 clock step
left out. By snapshot the nodes and stuart log verbosely: the pauses come
from the sources' logs (from `CLUSTER PAUSESLOTS` to the table that ends
the pause). "client" is a redis-py `RedisCluster` SETting keys of random
slots one at a time throughout each move.

## Findings

1. **Why the snapshot path takes longer (migrate_check sigterm 27 s
   against 21 s): stuart's state file fsync.** The copy, the stream, the
   pauses and the "owed" wait aren't the cause. A move by snapshot costs
   two tables per link (the cutover, then the table that starts the next
   link), and stuart saves the state file before each table
   (`State::save`: write, `fsync`, rename, `fsync` of the directory, about
   6.6 ms each on WSL2's ext4). A drain of 4096 slots holding 50k keys makes
   about 257 cutovers of at most 32 slots, so about 514 saves. It takes
   4.4 s (3.4–5.1) by snapshot against 1.5–1.7 s key by key, which ends
   about 45 slots per table. The same drain with the state file on tmpfs
   (`--state /dev/shm/...`) takes **1.0–1.2 s by snapshot and 1.0 s key
   by key**. One cycle, read from the logs: start table → copy (0–3 ms) →
   stuart sees `streaming` and pauses (+2 ms) → catch-up check, save, push
   (+6 ms) → the others take it (+2 ms) → save and push the next start
   table (+8 ms). In migrate_check the gap is the initial spreading of
   12288 slots over four nodes (7.7 s against 4.9 s) plus the drain (+3 s).
   Not the cause: PAUSESLOTS retries and `-BEHIND` (no pause missed in any
   drain), the owed wait of 658651e (a 2 ms tick at most), trimming (in
   the background, after the cutover), the copy (milliseconds here).
2. **A race costs 500 ms, 0–2 times a drain.** The target of a cutover
   that hasn't taken the cutover table yet is owed it (658651e), and an
   owed node gets the *current* table. When the mover has already made the
   next table, which starts moves and should reach their sources first,
   the target gets it before the source. Its `SLOTSYNC` is then refused
   (`The table doesn't move slot ...`), and the link starts again after
   `RETRY`, 500 ms.
3. **Speed: by snapshot is as fast or faster for many keys.** 1.3M
   keys/s of 16 B in 8 slots, against 364k key by key at batch 100 and
   719k at batch 1000; 674k against 256k and 394k for 1 KiB values in 8
   slots. The same for 1024 slots (32 links). A little slower for 16 KiB
   values (68k against 87k keys/s) and for slots without keys (0.60 s
   against 0.45 s).
4. **The copy isn't paced: clients of a busy source lose three quarters
   of their throughput while it runs.** Impact table: source SET 96k
   (p99 2.56 ms) against 384k with no move, and against 305k key by key
   with its default 50 µs pause; target SET 161k against 385k. The copy
   moves 802k keys/s, so the move is short (about 5 s), but the slot feed
   only yields between batches of 1000 keys or 1 MiB, with nothing like
   `--migrate-pause`. Key by key at batch 1000 and pause 100 µs moves 699k
   keys/s and leaves the source 226k.
5. **Cutover pauses: p50 7–14 ms, max 14–24 ms while the target is
   responsive**, with or without load. Once 80–176 ms, in the loaded
   rebalances with 32 or 64 slots per source. That one is the target, not
   the pause. A node's keyspace map rehashes in one go when it doubles: a
   lone dbnode adding 2M keys stalls 72 ms at 450k keys, 160 ms at 910k
   and 348 ms at 1.83M. The target, at about 1M keys, froze for 170 ms
   right after stuart pushed the cutover table, so the source took the
   table (ending the pause) 176 ms after the pause began. The same stalls
   give both paths' client max latencies (120–690 ms). PINGs straight to
   the nodes: target stalls up to 357 ms by snapshot and 121 ms key by
   key, sources at most 9 ms and 47 ms. An open item since phase 7
   (incremental rehashing). Unexplained: a 55.5 ms pause moving 1024 empty
   slots (no load).
6. **REBALANCE with small links is slow by snapshot.** At 8/64 it takes
   10.5 s against 3.3 s (the fsyncs again: 513 cutovers), at 32/128 3.1 s
   against 1.9 s, and at 64/256 the same (1.66 s and 1.68 s). The
   client's errors key by key are redis-py's `-ASK` bug (known since
   phase 5); none by snapshot.
7. **Target memory: the same on both paths** (200 B per 16 B key, 1200 B
   per 1 KiB, 16.6 KB per 16 KiB; 202–203 MB after a rebalance).

## Proposed fixes (none made here; A, B and C done in 0b36234: see `../2026-10-25-0b36234/migrate.md`)

- **A. Fewer saves per slot** (findings 1 and 6), any of:
  (a) cut over and start the next links in one table. This halves the
  saves; `first` then holds the cutover's targets and the new links'
  sources, and the race of finding 2 must be handled (see B).
  (b) cut over every link that is streaming and caught up in one table,
  not one per mover turn.
  (c) a larger default batch by snapshot: a drain with `--moves-per-source
  128 --max-moves 512` takes 1.3–1.4 s (against 4.4 s). A link costs the
  same for 32 or 128 slots, and a pause holds writes to more slots for the
  same ~8 ms.
  (c) is a default change; (a) and (b) are code in the mover and `State`.
  Not proposed: saving without `fsync` (a restarted stuart must know who
  owns a slot that was handed over).
- **B. The race** (finding 2), either of: the dbnode retries a `SLOTSYNC`
  the table refused after its 20 ms `TICK` instead of `RETRY` (500 ms); or
  stuart makes no new table while a node owes one (usually ≤2 ms). The
  first is a one-liner and covers other orders too.
- **C. Pace the slot copy** (finding 4): yield for `--migrate-pause`
  (50 µs) between batches while clients were served, as the key by key
  path does, maybe with smaller batches (100 keys). The replica full sync
  shares the feed loop (`repl::primary::serve`). Measure with the impact
  part.
- **D. Incremental rehashing of the keyspace map** (finding 5): the open
  item. It bounds the cutover pause too, since the target must be
  responsive to take the cutover table.

## Tables

**Moving speed** (no load but one RedisCluster client): slots from one node to another, STUART MOVE to the last table; the client's SET p99 / max (ms); the target's used_memory grown (per key)

| path | values | keys | slots | time | keys/s | MB/s | pause p50 / max ms (cutovers) | client p99 / max | target memory |
|---|---|---|---|---|---|---|---|---|---|
| snapshot | none | 0 | 1024 | 0.60 s | | | 12.4 / 55.5 (32) | 0.54 / 12.4 | |
| key by key, batch 100 | none | 0 | 1024 | 0.45 s | | |  | 0.47 / 2.1 | |
| snapshot | 16 B | 1,000,000 | 8 | 0.76 s | 1315k | 21 | 18.0 / 18.0 (1) | 6.68 / 254.6 | 200 MB (200 B) |
| key by key, batch 100 | 16 B | 1,000,000 | 8 | 2.75 s | 364k | 6 |  | 0.39 / 134.7 | 201 MB (201 B) |
| key by key, batch 1000 | 16 B | 1,000,000 | 8 | 1.39 s | 719k | 12 |  | 0.72 / 124.2 | 201 MB (201 B) |
| snapshot | 16 B | 1,000,000 | 1024 | 1.32 s | 760k | 12 | 13.1 / 14.1 (32) | 2.04 / 164.4 | 201 MB (201 B) |
| key by key, batch 100 | 16 B | 1,000,000 | 1024 | 2.57 s | 389k | 6 |  | 0.44 / 122.0 | 201 MB (201 B) |
| key by key, batch 1000 | 16 B | 1,000,000 | 1024 | 1.31 s | 765k | 12 |  | 0.79 / 125.4 | 201 MB (201 B) |
| snapshot | 1024 B | 300,000 | 8 | 0.45 s | 674k | 690 | 17.6 / 17.6 (1) | 0.93 / 393.3 | 360 MB (1200 B) |
| key by key, batch 100 | 1024 B | 300,000 | 8 | 1.17 s | 256k | 262 |  | 0.43 / 46.9 | 360 MB (1201 B) |
| key by key, batch 1000 | 1024 B | 300,000 | 8 | 0.76 s | 394k | 404 |  | 1.22 / 47.6 | 360 MB (1200 B) |
| snapshot | 1024 B | 300,000 | 1024 | 0.98 s | 305k | 312 | 12.7 / 13.7 (32) | 1.87 / 49.1 | 360 MB (1201 B) |
| key by key, batch 100 | 1024 B | 300,000 | 1024 | 1.00 s | 300k | 307 |  | 0.44 / 30.2 | 360 MB (1201 B) |
| key by key, batch 1000 | 1024 B | 300,000 | 1024 | 0.89 s | 339k | 347 |  | 0.73 / 43.9 | 360 MB (1200 B) |
| snapshot | 16384 B | 30,000 | 8 | 0.44 s | 68k | 1107 | 8.7 / 8.7 (1) | 0.83 / 311.2 | 497 MB (16575 B) |
| key by key, batch 100 | 16384 B | 30,000 | 8 | 0.35 s | 87k | 1418 |  | 0.59 / 1.9 | 497 MB (16578 B) |
| key by key, batch 1000 | 16384 B | 30,000 | 8 | 0.59 s | 51k | 840 |  | 1.19 / 40.1 | 497 MB (16579 B) |

**Clients during a move**: 4M keys of 16 B moving out of 1024 slots; redis-benchmark `-c 50` on keys of a slot staying at the source (SET and GET in turn) and of one at the target (SET), 0.3 s runs for 4 s or until the move ends: mean requests/s / worst p99 (ms); the RedisCluster client's SET p99 / max (ms) over the whole move

| path | moved keys/s | source SET | source GET | target SET | pause p50 / max ms (cutovers) | client p99 / max |
|---|---|---|---|---|---|---|
| no move | | 384k / 0.28 | 382k / 0.30 | 385k / 0.29 | | |
| snapshot | 802k | 96k / 2.56 | 99k / 2.53 | 161k / 1.61 | 13.8 / 23.9 (32) | 3.01 / 691.6 |
| key by key, batch 100, pause 50 µs | 288k | 305k / 0.41 | 311k / 0.38 | 432k / 0.24 |  | 0.36 / 145.0 |
| key by key, batch 1000, pause 100 µs | 699k | 226k / 0.81 | 166k / 0.83 | 286k / 0.59 |  | 0.83 / 338.8 |
| key by key, batch 1000, pause 1000 µs | 400k | 241k / 0.72 | 254k / 0.81 | 315k / 0.48 |  | 0.72 / 345.6 |

**REBALANCE** of 4M keys of 16 B from three nodes onto a fourth (1M keys and 4096 slots move); redis-benchmark `-c 50` SET on a slot the first node keeps: mean requests/s / worst p99 (ms); the RedisCluster client's SET p99 / max (ms); the new node's used_memory after

| path | `--moves-per-source` | `--max-moves` | time | keys/s | clients before | clients during | pause p50 / max ms (cutovers) | client p99 / max | target memory |
|---|---|---|---|---|---|---|---|---|---|
| snapshot | 8 | 64 | 10.46 s | 96k | 390k / 0.35 | 373k / 0.53 | 7.6 / 14.9 (513, 1 restarts) | 0.56 / 179.7 | 203 MB |
| snapshot | 32 | 128 | 3.09 s | 324k | 383k / 0.41 | 352k / 0.73 | 8.0 / 79.4 (129) | 1.21 / 176.9 | 202 MB |
| snapshot | 64 | 256 | 1.66 s | 604k | 413k / 0.39 | 346k / 1.19 | 7.6 / 168.0 (66) | 1.29 / 166.6 | 202 MB |
| key-by-key | 8 | 64 | 3.34 s | 300k | 414k / 0.38 | 386k / 0.31 |  | 0.33 / 145.4, 1 errors | 202 MB |
| key-by-key | 32 | 128 | 1.90 s | 528k | 379k / 0.38 | 317k / 0.46 |  | 0.53 / 161.1, 1 errors | 202 MB |
| key-by-key | 64 | 256 | 1.68 s | 595k | 399k / 0.26 | 318k / 0.53 |  | 0.55 / 155.2, 6 errors | 202 MB |

**Drain** (as migrate_check's sigterm): stuart `--auto`, four nodes, 200k keys of 100 B; STUART LEAVE of the first, until it holds nothing (4096 slots, 50k keys move); the RedisCluster client's SET p99 / max (ms)

| path | time | slots/s | pause p50 / max ms (cutovers) | client p99 / max |
|---|---|---|---|---|
| snapshot | 3.60 s | 1137 | 7.7 / 18.4 (185) | 0.73 / 14.4 |
| snapshot | 3.43 s | 1194 | 7.9 / 13.9 (179) | 0.73 / 13.5 |
| snapshot | 5.08 s | 806 | 7.5 / 23.6 (257) | 0.75 / 14.3 |
| key-by-key | 1.56 s | 2632 |  | 0.67 / 7.1 |
| key-by-key | 1.55 s | 2648 |  | 0.69 / 7.2 |
| key-by-key | 1.60 s | 2567 |  | 0.71 / 8.5 |

Experiments behind finding 1 (drain, stuart's "Moves done" time, three runs
each; `timeline.py` in the session's scratchpad, not kept): by snapshot
5.2 / 3.8 / 4.1 s; with `--moves-per-source 128 --max-moves 512` 1.4 /
1.4 / 1.3 s; with stuart's state on tmpfs 1.1 / 1.0 / 1.2 s; key by key
1.7 / 1.7 s, and on tmpfs 1.0 / 1.0 s.
