# fixed key

- Date: 2026-10-19
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t set,get,incr,mset --threads 1 --csv`, -P in {1, 16, 64}
- Pinning: redis-benchmark on CPUs 4-7; servers: before: cpus 2 (pid 20396); counting: cpus 2 (pid 20397); maxmemory: cpus 2 (pid 20398)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| before | SET | 1 | 359,066 | 0.063 | 0.343 |
| before | GET | 1 | 366,300 | 0.063 | 0.335 |
| before | INCR | 1 | invalid (clock) † | 0.063 | 0.247 |
| before | MSET (10 keys) | 1 | 379,507 | 0.063 | 0.271 |
| before | SET | 16 | 4,672,897 | 0.079 | 0.439 |
| before | GET | 16 | 4,901,961 | 0.079 | 0.487 |
| before | INCR | 16 | 5,025,126 | 0.087 | 0.263 |
| before | MSET (10 keys) | 16 | 1,968,504 | 0.359 | 0.759 |
| before | SET | 64 | 11,494,253 | 0.183 | 0.607 |
| before | GET | 64 | 11,627,907 | 0.127 | 0.639 |
| before | INCR | 64 | 7,462,686 | 0.351 | 0.847 |
| before | MSET (10 keys) | 64 | 2,577,320 | 1.127 | 1.879 |
| counting | SET | 1 | 371,333 | 0.063 | 0.279 |
| counting | GET | 1 | 378,788 | 0.063 | 0.231 |
| counting | INCR | 1 | 363,372 | 0.063 | 0.391 |
| counting | MSET (10 keys) | 1 | 369,004 | 0.063 | 0.327 |
| counting | SET | 16 | 4,901,961 | 0.079 | 0.319 |
| counting | GET | 16 | 4,975,124 | 0.079 | 0.239 |
| counting | INCR | 16 | 4,950,495 | 0.103 | 0.311 |
| counting | MSET (10 keys) | 16 | 1,915,709 | 0.375 | 0.775 |
| counting | SET | 64 | 10,989,011 | 0.207 | 0.487 |
| counting | GET | 64 | 12,345,679 | 0.127 | 0.447 |
| counting | INCR | 64 | 7,462,686 | 0.359 | 0.687 |
| counting | MSET (10 keys) | 64 | 2,493,766 | 1.183 | 1.791 |
| maxmemory | SET | 1 | 371,058 | 0.063 | 0.271 |
| maxmemory | GET | 1 | 367,107 | 0.063 | 0.295 |
| maxmemory | INCR | 1 | 371,471 | 0.063 | 0.255 |
| maxmemory | MSET (10 keys) | 1 | 370,920 | 0.063 | 0.319 |
| maxmemory | SET | 16 | 4,830,918 | 0.079 | 0.351 |
| maxmemory | GET | 16 | 4,807,692 | 0.079 | 0.463 |
| maxmemory | INCR | 16 | 4,716,981 | 0.095 | 0.519 |
| maxmemory | MSET (10 keys) | 16 | 1,858,736 | 0.375 | 0.815 |
| maxmemory | SET | 64 | 10,309,278 | 0.223 | 0.799 |
| maxmemory | GET | 64 | 12,048,193 | 0.127 | 0.407 |
| maxmemory | INCR | 64 | 7,407,407 | 0.367 | 0.735 |
| maxmemory | MSET (10 keys) | 64 | 2,475,248 | 1.183 | 1.903 |

Requests/s, ratio to before:

| test | -P | before | counting | maxmemory |
|---|---:|---:|---:|---:|
| SET | 1 | 359,066 | 371,333 (1.03x) | 371,058 (1.03x) |
| GET | 1 | 366,300 | 378,788 (1.03x) | 367,107 (1.00x) |
| INCR | 1 | invalid (clock) † | 363,372 | 371,471 |
| MSET (10 keys) | 1 | 379,507 | 369,004 (0.97x) | 370,920 (0.98x) |
| SET | 16 | 4,672,897 | 4,901,961 (1.05x) | 4,830,918 (1.03x) |
| GET | 16 | 4,901,961 | 4,975,124 (1.01x) | 4,807,692 (0.98x) |
| INCR | 16 | 5,025,126 | 4,950,495 (0.99x) | 4,716,981 (0.94x) |
| MSET (10 keys) | 16 | 1,968,504 | 1,915,709 (0.97x) | 1,858,736 (0.94x) |
| SET | 64 | 11,494,253 | 10,989,011 (0.96x) | 10,309,278 (0.90x) |
| GET | 64 | 11,627,907 | 12,345,679 (1.06x) | 12,048,193 (1.04x) |
| INCR | 64 | 7,462,686 | 7,462,686 (1.00x) | 7,407,407 (0.99x) |
| MSET (10 keys) | 64 | 2,577,320 | 2,493,766 (0.97x) | 2,475,248 (0.96x) |
