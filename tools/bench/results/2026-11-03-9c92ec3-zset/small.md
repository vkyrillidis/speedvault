# Sorted sets, redis-benchmark zadd and zpopmin: speedvault 9c92ec3

- Date: 2026-11-03
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t zadd,zpopmin --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis-7.0.15: cpus 2 (pid 498); speedvault: cpus 2 (pid 500); redis-8.10.2: cpus 2 (pid 507); valkey-9.1.2: cpus 2 (pid 509)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis-7.0.15 | ZADD | 1 | 330,578 | 0.103 | 0.487 |
| redis-7.0.15 | ZPOPMIN | 1 | 273,973 | 0.087 | 0.471 |
| redis-7.0.15 | ZADD | 16 | 938,967 | 0.815 | 1.391 |
| redis-7.0.15 | ZPOPMIN | 16 | 2,958,580 | 0.191 | 0.655 |
| speedvault | ZADD | 1 | 382,995 | 0.071 | 0.391 |
| speedvault | ZPOPMIN | 1 | 381,534 | 0.063 | 0.431 |
| speedvault | ZADD | 16 | 1,176,470 | 0.607 | 1.223 |
| speedvault | ZPOPMIN | 16 | 3,952,569 | 0.111 | 0.655 |
| redis-8.10.2 | ZADD | 1 | 357,782 | 0.087 | 0.423 |
| redis-8.10.2 | ZPOPMIN | 1 | 302,480 | 0.079 | 0.423 |
| redis-8.10.2 | ZADD | 16 | 1,002,004 | 0.767 | 1.175 |
| redis-8.10.2 | ZPOPMIN | 16 | 3,144,654 | 0.199 | 0.543 |
| valkey-9.1.2 | ZADD | 1 | 355,872 | 0.095 | 0.455 |
| valkey-9.1.2 | ZPOPMIN | 1 | 298,329 | 0.087 | 0.431 |
| valkey-9.1.2 | ZADD | 16 | 924,214 | 0.815 | 1.327 |
| valkey-9.1.2 | ZPOPMIN | 16 | 2,881,844 | 0.215 | 0.647 |

Requests/s, ratio to redis-7.0.15:

| test | -P | redis-7.0.15 | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| ZADD | 1 | 330,578 | 382,995 (1.16x) | 357,782 (1.08x) | 355,872 (1.08x) |
| ZPOPMIN | 1 | 273,973 | 381,534 (1.39x) | 302,480 (1.10x) | 298,329 (1.09x) |
| ZADD | 16 | 938,967 | 1,176,470 (1.25x) | 1,002,004 (1.07x) | 924,214 (0.98x) |
| ZPOPMIN | 16 | 2,958,580 | 3,952,569 (1.34x) | 3,144,654 (1.06x) | 2,881,844 (0.97x) |
