# Data types: Redis 7.0.15 vs speedvault d1dddda vs Redis 8.10.2 vs Valkey 9.1.2

- Date: 2026-10-22
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 200000 -c 50 -d 3 -t lrange_100,lrange_300,lrange_500,lrange_600 --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis: cpus 2 (pid 335); speedvault: cpus 2 (pid 336); redis-8.10.2: cpus 2 (pid 338); valkey-9.1.2: cpus 2 (pid 340)
- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark times with it. Each test started right after a step and was rerun if the clock had moved during it (26 rerun(s)); rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis | LPUSH (needed to benchmark LRANGE) | 1 | 286,944 | 0.087 | 0.279 |
| redis | LRANGE_100 (first 100 elements) | 1 | 202,634 | 0.127 | 0.519 |
| redis | LRANGE_300 (first 300 elements) | 1 | 64,205 | 0.399 | 1.351 |
| redis | LRANGE_500 (first 500 elements) | 1 | 51,322 | 0.503 | 1.383 |
| redis | LRANGE_600 (first 600 elements) | 1 | 44,474 | 0.599 | 1.919 |
| redis | LPUSH (needed to benchmark LRANGE) | 16 | 2,020,202 | 0.311 | 0.967 |
| redis | LRANGE_100 (first 100 elements) | 16 | 324,149 | 1.831 | 3.591 |
| redis | LRANGE_300 (first 300 elements) | 16 | 85,397 | 5.591 | 9.975 |
| redis | LRANGE_500 (first 500 elements) | 16 | 50,063 | 8.951 | 14.215 |
| redis | LRANGE_600 (first 600 elements) | 16 | 40,282 | 11.359 | 16.255 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 1 | 328,947 | 0.071 | 0.463 |
| speedvault | LRANGE_100 (first 100 elements) | 1 | 190,658 | 0.111 | 0.623 |
| speedvault | LRANGE_300 (first 300 elements) | 1 | 67,431 | 0.351 | 0.999 |
| speedvault | LRANGE_500 (first 500 elements) | 1 | 42,409 | 0.575 | 1.271 |
| speedvault | LRANGE_600 (first 600 elements) | 1 | 38,979 | 0.631 | 1.463 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 16 | 3,125,000 | 0.199 | 0.919 |
| speedvault | LRANGE_100 (first 100 elements) | 16 | 393,701 | 1.023 | 2.047 |
| speedvault | LRANGE_300 (first 300 elements) | 16 | 118,483 | 2.159 | 4.375 |
| speedvault | LRANGE_500 (first 500 elements) | 16 | 65,789 | 2.199 | 5.599 |
| speedvault | LRANGE_600 (first 600 elements) | 16 | 58,207 | 2.199 | 5.383 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 1 | 278,552 | 0.095 | 0.335 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 1 | 157,978 | 0.159 | 0.583 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 1 | 58,910 | 0.431 | 1.231 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 1 | 43,697 | 0.591 | 1.527 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 1 | 38,052 | 0.679 | 1.615 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 16 | 1,666,667 | 0.375 | 1.159 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 16 | 373,134 | 1.103 | 2.527 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 16 | 96,339 | 3.975 | 8.351 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 16 | 51,962 | 7.687 | 11.695 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 16 | 48,864 | 7.303 | 13.647 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 1 | 241,838 | 0.095 | 0.631 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 1 | 154,202 | 0.159 | 0.783 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 1 | 54,010 | 0.463 | 1.263 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 1 | 43,678 | 0.583 | 1.447 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 1 | 37,707 | 0.671 | 1.575 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 16 | 1,526,718 | 0.415 | 1.199 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 16 | 387,597 | 1.087 | 2.775 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 16 | 90,090 | 4.767 | 9.367 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 16 | 52,151 | 8.143 | 13.303 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 16 | 40,675 | 11.431 | 16.359 |

Requests/s, ratio to redis:

| test | -P | redis | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| LPUSH (needed to benchmark LRANGE) | 1 | 286,944 | 328,947 (1.15x) | 278,552 (0.97x) | 241,838 (0.84x) |
| LRANGE_100 (first 100 elements) | 1 | 202,634 | 190,658 (0.94x) | 157,978 (0.78x) | 154,202 (0.76x) |
| LRANGE_300 (first 300 elements) | 1 | 64,205 | 67,431 (1.05x) | 58,910 (0.92x) | 54,010 (0.84x) |
| LRANGE_500 (first 500 elements) | 1 | 51,322 | 42,409 (0.83x) | 43,697 (0.85x) | 43,678 (0.85x) |
| LRANGE_600 (first 600 elements) | 1 | 44,474 | 38,979 (0.88x) | 38,052 (0.86x) | 37,707 (0.85x) |
| LPUSH (needed to benchmark LRANGE) | 16 | 2,020,202 | 3,125,000 (1.55x) | 1,666,667 (0.83x) | 1,526,718 (0.76x) |
| LRANGE_100 (first 100 elements) | 16 | 324,149 | 393,701 (1.21x) | 373,134 (1.15x) | 387,597 (1.20x) |
| LRANGE_300 (first 300 elements) | 16 | 85,397 | 118,483 (1.39x) | 96,339 (1.13x) | 90,090 (1.05x) |
| LRANGE_500 (first 500 elements) | 16 | 50,063 | 65,789 (1.31x) | 51,962 (1.04x) | 52,151 (1.04x) |
| LRANGE_600 (first 600 elements) | 16 | 40,282 | 58,207 (1.44x) | 48,864 (1.21x) | 40,675 (1.01x) |
