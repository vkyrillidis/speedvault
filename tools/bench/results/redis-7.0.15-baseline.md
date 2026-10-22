# Baseline: Redis 7.0.15 (redis-server --save '' --appendonly no)

- Date: 2026-10-13
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t ping,set,get,incr,mset --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis-7.0.15: cpus 2 (pid 42753)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis-7.0.15 | PING_INLINE | 1 | 279,408 | 0.087 | 0.383 |
| redis-7.0.15 | PING_MBULK | 1 | 274,574 | 0.095 | 0.311 |
| redis-7.0.15 | SET | 1 | 285,551 | 0.087 | 0.343 |
| redis-7.0.15 | GET | 1 | 287,936 | 0.087 | 0.279 |
| redis-7.0.15 | INCR | 1 | 282,646 | 0.087 | 0.351 |
| redis-7.0.15 | MSET (10 keys) | 1 | 386,997 | 0.079 | 0.351 |
| redis-7.0.15 | PING_INLINE | 16 | 3,225,806 | 0.207 | 0.511 |
| redis-7.0.15 | PING_MBULK | 16 | 4,329,004 | 0.143 | 0.463 |
| redis-7.0.15 | SET | 16 | 2,801,120 | 0.239 | 0.599 |
| redis-7.0.15 | GET | 16 | 3,225,806 | 0.207 | 0.495 |
| redis-7.0.15 | INCR | 16 | 3,205,128 | 0.207 | 0.479 |
| redis-7.0.15 | MSET (10 keys) | 16 | 956,938 | 0.799 | 1.231 |
