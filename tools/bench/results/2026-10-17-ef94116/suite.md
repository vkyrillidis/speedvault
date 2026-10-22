# Redis 7.0.15 vs speedvault ef94116 vs redis-8.10.2 valkey-9.1.2

- Date: 2026-10-17
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t ping,set,get,incr,mset --threads 1 --csv`, -P in {1, 16, 64}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis: cpus 2 (pid 108212); speedvault: cpus 2 (pid 108213); redis-8.10.2: cpus 2 (pid 108215); valkey-9.1.2: cpus 2 (pid 108216)
- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark times with it. Each test started right after a step and was rerun if the clock had moved during it (7 rerun(s)); rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis | PING_INLINE | 1 | 266,454 | 0.095 | 0.303 |
| redis | PING_MBULK | 1 | 260,078 | 0.095 | 0.311 |
| redis | SET | 1 | 284,172 | 0.087 | 0.279 |
| redis | GET | 1 | 269,034 | 0.095 | 0.311 |
| redis | INCR | 1 | 270,783 | 0.095 | 0.287 |
| redis | MSET (10 keys) | 1 | 371,195 | 0.095 | 0.343 |
| redis | PING_INLINE | 16 | 2,881,844 | 0.223 | 0.655 |
| redis | PING_MBULK | 16 | 4,048,583 | 0.143 | 0.455 |
| redis | SET | 16 | 2,652,520 | 0.255 | 0.559 |
| redis | GET | 16 | 3,048,780 | 0.215 | 0.567 |
| redis | INCR | 16 | 3,086,420 | 0.207 | 0.543 |
| redis | MSET (10 keys) | 16 | 874,891 | 0.863 | 1.567 |
| redis | PING_INLINE | 64 | 4,484,305 | 0.607 | 1.015 |
| redis | PING_MBULK | 64 | 6,493,506 | 0.423 | 0.783 |
| redis | SET | 64 | 4,032,258 | 0.695 | 1.407 |
| redis | GET | 64 | 4,878,049 | 0.567 | 1.127 |
| redis | INCR | 64 | 5,025,126 | 0.559 | 0.975 |
| redis | MSET (10 keys) | 64 | 962,464 | 2.607 | 3.703 |
| speedvault | PING_INLINE | 1 | 394,789 | 0.063 | 0.199 |
| speedvault | PING_MBULK | 1 | 398,724 | 0.063 | 0.223 |
| speedvault | SET | 1 | 390,778 | 0.063 | 0.231 |
| speedvault | GET | 1 | 398,406 | 0.063 | 0.215 |
| speedvault | INCR | 1 | 397,456 | 0.063 | 0.231 |
| speedvault | MSET (10 keys) | 1 | 405,844 | 0.063 | 0.199 |
| speedvault | PING_INLINE | 16 | 5,291,006 | 0.071 | 0.255 |
| speedvault | PING_MBULK | 16 | 5,291,006 | 0.071 | 0.183 |
| speedvault | SET | 16 | 5,291,006 | 0.071 | 0.295 |
| speedvault | GET | 16 | 5,347,594 | 0.071 | 0.271 |
| speedvault | INCR | 16 | 4,926,108 | 0.087 | 0.351 |
| speedvault | MSET (10 keys) | 16 | 1,960,784 | 0.351 | 0.727 |
| speedvault | PING_INLINE | 64 | 12,987,013 | 0.119 | 0.367 |
| speedvault | PING_MBULK | 64 | 13,157,895 | 0.119 | 0.367 |
| speedvault | SET | 64 | 11,904,762 | 0.183 | 0.495 |
| speedvault | GET | 64 | 12,345,679 | 0.127 | 0.743 |
| speedvault | INCR | 64 | 7,936,508 | 0.343 | 0.631 |
| speedvault | MSET (10 keys) | 64 | 2,538,071 | 1.103 | 1.871 |
| redis-8.10.2 | PING_INLINE | 1 | 295,247 | 0.087 | 0.295 |
| redis-8.10.2 | PING_MBULK | 1 | 286,779 | 0.087 | 0.279 |
| redis-8.10.2 | SET | 1 | 311,236 | 0.087 | 0.311 |
| redis-8.10.2 | GET | 1 | 293,513 | 0.087 | 0.335 |
| redis-8.10.2 | INCR | 1 | 308,356 | 0.087 | 0.279 |
| redis-8.10.2 | MSET (10 keys) | 1 | 286,779 | 0.143 | 0.431 |
| redis-8.10.2 | PING_INLINE | 16 | 3,067,484 | 0.207 | 0.575 |
| redis-8.10.2 | PING_MBULK | 16 | 4,016,064 | 0.151 | 0.599 |
| redis-8.10.2 | SET | 16 | 2,202,643 | 0.319 | 0.639 |
| redis-8.10.2 | GET | 16 | 2,770,083 | 0.239 | 0.503 |
| redis-8.10.2 | INCR | 16 | 2,590,674 | 0.263 | 0.503 |
| redis-8.10.2 | MSET (10 keys) | 16 | 724,638 | 1.055 | 1.887 |
| redis-8.10.2 | PING_INLINE | 64 | 5,128,206 | 0.559 | 0.791 |
| redis-8.10.2 | PING_MBULK | 64 | 6,211,180 | 0.431 | 0.799 |
| redis-8.10.2 | SET | 64 | 3,184,713 | 0.911 | 1.607 |
| redis-8.10.2 | GET | 64 | 4,149,378 | 0.639 | 1.415 |
| redis-8.10.2 | INCR | 64 | 4,237,288 | 0.679 | 1.095 |
| redis-8.10.2 | MSET (10 keys) | 64 | 725,689 | 3.583 | 6.439 |
| valkey-9.1.2 | PING_INLINE | 1 | 285,144 | 0.087 | 0.359 |
| valkey-9.1.2 | PING_MBULK | 1 | 288,018 | 0.087 | 0.271 |
| valkey-9.1.2 | SET | 1 | 310,366 | 0.087 | 0.327 |
| valkey-9.1.2 | GET | 1 | 301,750 | 0.087 | 0.327 |
| valkey-9.1.2 | INCR | 1 | 299,133 | 0.087 | 0.303 |
| valkey-9.1.2 | MSET (10 keys) | 1 | 235,905 | 0.183 | 0.423 |
| valkey-9.1.2 | PING_INLINE | 16 | 2,898,551 | 0.231 | 0.535 |
| valkey-9.1.2 | PING_MBULK | 16 | 3,558,719 | 0.175 | 0.479 |
| valkey-9.1.2 | SET | 16 | 1,818,182 | 0.391 | 0.815 |
| valkey-9.1.2 | GET | 16 | 2,398,082 | 0.287 | 0.679 |
| valkey-9.1.2 | INCR | 16 | 2,309,469 | 0.303 | 0.727 |
| valkey-9.1.2 | MSET (10 keys) | 16 | 546,747 | 1.383 | 2.343 |
| valkey-9.1.2 | PING_INLINE | 64 | 4,484,305 | 0.615 | 1.295 |
| valkey-9.1.2 | PING_MBULK | 64 | 5,747,126 | 0.479 | 0.879 |
| valkey-9.1.2 | SET | 64 | 2,590,674 | 1.159 | 1.599 |
| valkey-9.1.2 | GET | 64 | 3,472,222 | 0.823 | 1.687 |
| valkey-9.1.2 | INCR | 64 | 3,448,276 | 0.839 | 1.415 |
| valkey-9.1.2 | MSET (10 keys) | 64 | 557,103 | 4.671 | 7.527 |

Requests/s, ratio to redis:

| test | -P | redis | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| PING_INLINE | 1 | 266,454 | 394,789 (1.48x) | 295,247 (1.11x) | 285,144 (1.07x) |
| PING_MBULK | 1 | 260,078 | 398,724 (1.53x) | 286,779 (1.10x) | 288,018 (1.11x) |
| SET | 1 | 284,172 | 390,778 (1.38x) | 311,236 (1.10x) | 310,366 (1.09x) |
| GET | 1 | 269,034 | 398,406 (1.48x) | 293,513 (1.09x) | 301,750 (1.12x) |
| INCR | 1 | 270,783 | 397,456 (1.47x) | 308,356 (1.14x) | 299,133 (1.10x) |
| MSET (10 keys) | 1 | 371,195 | 405,844 (1.09x) | 286,779 (0.77x) | 235,905 (0.64x) |
| PING_INLINE | 16 | 2,881,844 | 5,291,006 (1.84x) | 3,067,484 (1.06x) | 2,898,551 (1.01x) |
| PING_MBULK | 16 | 4,048,583 | 5,291,006 (1.31x) | 4,016,064 (0.99x) | 3,558,719 (0.88x) |
| SET | 16 | 2,652,520 | 5,291,006 (1.99x) | 2,202,643 (0.83x) | 1,818,182 (0.69x) |
| GET | 16 | 3,048,780 | 5,347,594 (1.75x) | 2,770,083 (0.91x) | 2,398,082 (0.79x) |
| INCR | 16 | 3,086,420 | 4,926,108 (1.60x) | 2,590,674 (0.84x) | 2,309,469 (0.75x) |
| MSET (10 keys) | 16 | 874,891 | 1,960,784 (2.24x) | 724,638 (0.83x) | 546,747 (0.62x) |
| PING_INLINE | 64 | 4,484,305 | 12,987,013 (2.90x) | 5,128,206 (1.14x) | 4,484,305 (1.00x) |
| PING_MBULK | 64 | 6,493,506 | 13,157,895 (2.03x) | 6,211,180 (0.96x) | 5,747,126 (0.89x) |
| SET | 64 | 4,032,258 | 11,904,762 (2.95x) | 3,184,713 (0.79x) | 2,590,674 (0.64x) |
| GET | 64 | 4,878,049 | 12,345,679 (2.53x) | 4,149,378 (0.85x) | 3,472,222 (0.71x) |
| INCR | 64 | 5,025,126 | 7,936,508 (1.58x) | 4,237,288 (0.84x) | 3,448,276 (0.69x) |
| MSET (10 keys) | 64 | 962,464 | 2,538,071 (2.64x) | 725,689 (0.75x) | 557,103 (0.58x) |
