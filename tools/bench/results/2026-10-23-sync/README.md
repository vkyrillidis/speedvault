# A replica's sync, 2026-10-23 (phase 8 milestone 3)

`tools/bench/sync_bench.sh` at `2daa9a3` (the sync, `3a86256`; offsets
cluster-wide, `b727156`) against Redis 7.0.15, on WSL2 (Ryzen 9800X3D):
primary pinned to CPU 2, replica to 4, stuart to 14, redis-benchmark to 8
and 10. Keys `key:%012d` with 16 B values; writes during the sync:
redis-benchmark SET of random keys of those, `-c 50`, unpipelined. Memory
is Pss (the primary's with Redis's forked child). Ours runs in cluster
mode (its slot index counted), Redis standalone; ours takes more a key
for small keys in any case (README: "Memory per key").

| server | keys | writes during the sync | sync (s) | primary before (MB) | primary peak (MB) | peak / before | replica peak (MB) | replica after (MB) | replica's keys |
|---|---|---|---|---|---|---|---|---|---|
| ours | 1,000,000 | none | 0.77 | 225 | 225 | 1.00 | 254 | 225 | the same |
| ours | 1,000,000 | SET -c 50 | 0.81 | 225 | 230 | 1.02 | 241 | 225 | the same |
| Redis 7.0.15 | 1,000,000 | none | 0.58 | 137 | 139 | 1.01 | 131 | 131 | the same |
| Redis 7.0.15 | 1,000,000 | SET -c 50 | 0.89 | 137 | 256 | 1.86 | 132 | 132 | the same |
| ours | 10,000,000 | none | 8.56 | 1,997 | 1,997 | 1.00 | 2,189 | 1,985 | the same |
| ours | 10,000,000 | SET -c 50 | 10.13 | 2,000 | 2,039 | 1.02 | 2,177 | 1,985 | the same |
| Redis 7.0.15 | 10,000,000 | none | 8.04 | 1,356 | 1,357 | 1.00 | 1,290 | 1,290 | the same |
| Redis 7.0.15 | 10,000,000 | SET -c 50 | 12.56 | 1,355 | 2,480 | 1.83 | 1,291 | 1,291 | the same |

- **No doubling.** Under writes our primary grows 2% during the sync;
  Redis's fork copies 83–86% of its memory (1,355 to 2,480 MB at 10M
  keys).
- **Sync time.** Without writes, ours is 0.77 s for 1M keys and 8.6 s for
  10M, Redis's 0.58 s and 8.0 s; ours includes the replica's start and
  join. With writes, ours slows by 5–18%, Redis's by 53–56%: 10.1 s
  against 12.6 s at 10M keys.
- **The replica** peaks 7–13% above its final size while it syncs (not
  looked into yet) and ends at the primary's size.
