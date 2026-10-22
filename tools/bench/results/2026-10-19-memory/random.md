# random keys

- Date: 2026-10-19
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 2000000 -c 50 -d 100 -t set --threads 1 --csv`, -P in {1, 16, 64}
- Pinning: redis-benchmark on CPUs 4-7; servers: before: cpus 2 (pid 20396); counting: cpus 2 (pid 20397); maxmemory: cpus 2 (pid 20398)
- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark times with it. Each test started right after a step and was rerun if the clock had moved during it (2 rerun(s)); rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| before | SET | 1 | 370,920 | 0.063 | 0.255 |
| before | SET | 16 | 1,576,044 | 0.383 | 0.839 |
| before | SET | 64 | 2,515,723 | 1.159 | 1.751 |
| counting | SET | 1 | 360,101 | 0.063 | 0.311 |
| counting | SET | 16 | 1,910,220 | 0.375 | 0.623 |
| counting | SET | 64 | 2,580,645 | 1.127 | 1.735 |
| maxmemory | SET | 1 | 367,782 | 0.063 | 0.287 |
| maxmemory | SET | 16 | 1,901,141 | 0.383 | 0.607 |
| maxmemory | SET | 64 | 2,649,007 | 1.111 | 1.495 |

Requests/s, ratio to before:

| test | -P | before | counting | maxmemory |
|---|---:|---:|---:|---:|
| SET | 1 | 370,920 | 360,101 (0.97x) | 367,782 (0.99x) |
| SET | 16 | 1,576,044 | 1,910,220 (1.21x) | 1,901,141 (1.21x) |
| SET | 64 | 2,515,723 | 2,580,645 (1.03x) | 2,649,007 (1.05x) |
