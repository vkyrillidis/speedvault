# Moving slots (phase 5 milestone 6)

`tools/bench/migrate_bench.sh` on the Ryzen 9800X3D (WSL2), 2026-10-18, at
276a495 (defaults: `--migrate-batch 100`, `--migrate-pause 50` µs while
clients are served, `--moves-per-source 32`, `--max-moves 128`). The
REBALANCE table is a second run, with the default pair in the list.
Each process pinned to its own CPU; requests/s as the mean of 0.3 s
redis-benchmark runs, p99 the worst of them; runs during a WSL2 clock step
left out.

**Moving speed** (no clients): slots from one node to another, STUART MOVE to the last table

| values | keys | slots | `--migrate-batch` | time | keys/s | MB/s |
|---|---|---|---|---|---|---|
| none | 0 | 1024 | 100 | 0.43 s | | |
| 16 B | 1,000,000 | 8 | 10 | 2.00 s | 501k | 8 |
| 16 B | 1,000,000 | 8 | 100 | 1.89 s | 530k | 8 |
| 16 B | 1,000,000 | 8 | 1000 | 1.27 s | 789k | 13 |
| 16 B | 1,000,000 | 1024 | 10 | 2.04 s | 490k | 8 |
| 16 B | 1,000,000 | 1024 | 100 | 2.01 s | 497k | 8 |
| 16 B | 1,000,000 | 1024 | 1000 | 1.18 s | 846k | 14 |
| 1024 B | 300,000 | 8 | 10 | 1.36 s | 221k | 226 |
| 1024 B | 300,000 | 8 | 100 | 0.84 s | 357k | 365 |
| 1024 B | 300,000 | 8 | 1000 | 0.64 s | 468k | 480 |
| 1024 B | 300,000 | 1024 | 10 | 0.96 s | 313k | 320 |
| 1024 B | 300,000 | 1024 | 100 | 0.87 s | 345k | 353 |
| 1024 B | 300,000 | 1024 | 1000 | 0.86 s | 350k | 358 |
| 16384 B | 30,000 | 8 | 10 | 0.65 s | 46k | 760 |
| 16384 B | 30,000 | 8 | 100 | 0.44 s | 68k | 1114 |
| 16384 B | 30,000 | 8 | 1000 | 0.34 s | 89k | 1453 |

**Clients during a move**: 4M keys of 16 B moving out of 1024 slots; redis-benchmark `-c 50` on keys of a slot staying at the source (SET and GET in turn) and of one at the target (SET), 0.3 s runs for 4 s or until the move ends; mean requests/s / worst p99 (ms)

| `--migrate-batch` | `--migrate-pause` | moved keys/s | source SET | source GET | target SET |
|---|---|---|---|---|---|
| no move | | | 384k / 0.29 | 383k / 0.23 | 382k / 0.36 |
| 10 | 0 µs | 151k | 334k / 0.46 | 322k / 0.48 | 374k / 0.32 |
| 100 | 0 µs | 398k | 234k / 0.61 | 226k / 0.54 | 350k / 0.42 |
| 1000 | 0 µs | 748k | 176k / 1.16 | 212k / 1.00 | 239k / 0.68 |
| 100 | 50 µs | 296k | 285k / 0.52 | 297k / 0.57 | 360k / 0.32 |
| 100 | 100 µs | 291k | 308k / 0.54 | 314k / 0.57 | 359k / 0.34 |
| 100 | 300 µs | 197k | 318k / 0.59 | 325k / 0.34 | 355k / 0.38 |
| 100 | 1000 µs | 85k | 347k / 0.38 | 349k / 0.36 | 377k / 0.29 |
| 1000 | 100 µs | 751k | 222k / 0.84 | 182k / 0.66 | 258k / 0.68 |
| 1000 | 300 µs | 665k | 205k / 0.73 | 235k / 0.80 | 276k / 0.49 |
| 1000 | 1000 µs | 430k | 264k / 0.73 | 268k / 0.73 | 307k / 0.49 |

**REBALANCE** of 4M keys of 16 B from three nodes onto a fourth (1M keys and 4096 slots move); redis-benchmark `-c 50` SET on a slot the first node keeps: mean requests/s / worst p99 (ms)

| `--moves-per-source` | `--max-moves` | time | keys/s | clients before | clients during |
|---|---|---|---|---|---|
| 1 | 64 | 13.98 s | 71k | 382k / 0.38 | 373k / 0.49 |
| 8 | 64 | 3.32 s | 301k | 376k / 0.46 | 357k / 0.44 |
| 16 | 64 | 2.32 s | 431k | 377k / 0.46 | 346k / 0.44 |
| 32 | 128 | 1.76 s | 567k | 393k / 0.26 | 336k / 0.43 |
| 32 | 256 | 1.89 s | 530k | 373k / 0.42 | 335k / 0.53 |
| 64 | 256 | 1.66 s | 603k | 377k / 0.34 | 329k / 0.54 |
