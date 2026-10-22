# Data types: Redis 7.0.15 vs speedvault d77494e vs Redis 8.10.2 vs Valkey 9.1.2

- Date: 2026-10-22
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 200000 -c 50 -d 3 -t lpush,rpush,lpop,rpop,sadd,hset,spop,lrange_100,lrange_300,lrange_500,lrange_600 --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis: cpus 2 (pid 368); speedvault: cpus 2 (pid 369); redis-8.10.2: cpus 2 (pid 370); valkey-9.1.2: cpus 2 (pid 371)
- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark times with it. Each test started right after a step and was rerun if the clock had moved during it (30 rerun(s)); rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis | LPUSH | 1 | 261,438 | 0.095 | 0.455 |
| redis | RPUSH | 1 | 247,525 | 0.095 | 0.575 |
| redis | LPOP | 1 | 247,219 | 0.095 | 0.599 |
| redis | RPOP | 1 | 253,165 | 0.095 | 0.583 |
| redis | SADD | 1 | 244,798 | 0.095 | 0.543 |
| redis | HSET | 1 | 236,686 | 0.095 | 0.599 |
| redis | SPOP | 1 | 243,309 | 0.103 | 0.551 |
| redis | LPUSH (needed to benchmark LRANGE) | 1 | 141,443 † | 0.103 | 0.623 |
| redis | LRANGE_100 (first 100 elements) | 1 | 179,856 | 0.143 | 0.575 |
| redis | LRANGE_300 (first 300 elements) | 1 | 71,149 | 0.351 | 1.167 |
| redis | LRANGE_500 (first 500 elements) | 1 | 45,641 | 0.551 | 1.735 |
| redis | LRANGE_600 (first 600 elements) | 1 | 40,056 † | 0.647 | 1.711 |
| redis | LPUSH | 16 | 2,040,816 | 0.327 | 0.679 |
| redis | RPUSH | 16 | 2,083,333 | 0.311 | 0.719 |
| redis | LPOP | 16 | 1,904,762 | 0.359 | 0.879 |
| redis | RPOP | 16 | 1,923,077 | 0.335 | 0.839 |
| redis | SADD | 16 | 2,298,851 | 0.271 | 0.615 |
| redis | HSET | 16 | 2,150,538 | 0.311 | 0.663 |
| redis | SPOP | 16 | 2,409,639 | 0.231 | 0.751 |
| redis | LPUSH (needed to benchmark LRANGE) | 16 | 2,325,581 | 0.287 | 0.807 |
| redis | LRANGE_100 (first 100 elements) | 16 | 327,869 | 1.895 | 3.543 |
| redis | LRANGE_300 (first 300 elements) | 16 | 80,645 | 5.991 | 10.535 |
| redis | LRANGE_500 (first 500 elements) | 16 | 47,015 | 9.831 | 15.167 |
| redis | LRANGE_600 (first 600 elements) | 16 | 40,519 | 11.279 | 16.087 |
| speedvault | LPUSH | 1 | 318,471 | 0.063 | 0.575 |
| speedvault | RPUSH | 1 | 337,268 | 0.071 | 0.399 |
| speedvault | LPOP | 1 | 334,448 | 0.071 | 0.535 |
| speedvault | RPOP | 1 | 334,448 | 0.071 | 0.375 |
| speedvault | SADD | 1 | 336,700 | 0.071 | 0.471 |
| speedvault | HSET | 1 | 303,490 | 0.071 | 0.583 |
| speedvault | SPOP | 1 | 318,979 | 0.071 | 0.567 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 1 | 174,520 † | 0.071 | 0.583 |
| speedvault | LRANGE_100 (first 100 elements) | 1 | 205,128 | 0.111 | 0.391 |
| speedvault | LRANGE_300 (first 300 elements) | 1 | 75,988 | 0.295 | 0.951 |
| speedvault | LRANGE_500 (first 500 elements) | 1 | 42,946 | 0.583 | 1.383 |
| speedvault | LRANGE_600 (first 600 elements) | 1 | 37,608 † | 0.655 | 1.527 |
| speedvault | LPUSH | 16 | 2,409,639 | 0.239 | 0.831 |
| speedvault | RPUSH | 16 | 2,857,143 | 0.199 | 0.839 |
| speedvault | LPOP | 16 | 2,439,024 | 0.247 | 0.823 |
| speedvault | RPOP | 16 | 2,941,176 | 0.215 | 0.415 |
| speedvault | SADD | 16 | 3,703,704 | 0.127 | 0.615 |
| speedvault | HSET | 16 | 3,174,603 | 0.167 | 0.735 |
| speedvault | SPOP | 16 | 3,636,364 | 0.095 | 0.647 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 16 | 2,469,136 | 0.247 | 0.727 |
| speedvault | LRANGE_100 (first 100 elements) | 16 | 375,940 | 0.999 | 2.143 |
| speedvault | LRANGE_300 (first 300 elements) | 16 | 115,009 | 2.327 | 4.895 |
| speedvault | LRANGE_500 (first 500 elements) | 16 | 63,654 | 2.407 | 6.047 |
| speedvault | LRANGE_600 (first 600 elements) | 16 | 54,585 | 2.663 | 6.415 |
| redis-8.10.2 | LPUSH | 1 | 271,003 | 0.087 | 0.535 |
| redis-8.10.2 | RPUSH | 1 | 253,807 | 0.095 | 0.591 |
| redis-8.10.2 | LPOP | 1 | 250,000 | 0.095 | 0.591 |
| redis-8.10.2 | RPOP | 1 | 246,609 | 0.095 | 0.623 |
| redis-8.10.2 | SADD | 1 | 251,572 | 0.095 | 0.535 |
| redis-8.10.2 | HSET | 1 | 247,831 | 0.095 | 0.623 |
| redis-8.10.2 | SPOP | 1 | 236,128 | 0.095 | 0.631 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 1 | 115,741 † | 0.087 | 0.431 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 1 | 158,228 | 0.159 | 0.599 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 1 | 53,248 | 0.463 | 1.271 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 1 | 41,314 † | 0.607 | 1.439 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 1 | 35,480 † | 0.703 | 1.591 |
| redis-8.10.2 | LPUSH | 16 | 1,724,138 | 0.359 | 1.175 |
| redis-8.10.2 | RPUSH | 16 | 1,801,802 | 0.343 | 1.167 |
| redis-8.10.2 | LPOP | 16 | 1,709,402 | 0.407 | 1.111 |
| redis-8.10.2 | RPOP | 16 | 1,538,462 | 0.407 | 1.231 |
| redis-8.10.2 | SADD | 16 | 2,173,913 | 0.295 | 0.759 |
| redis-8.10.2 | HSET | 16 | 1,652,893 | 0.375 | 1.199 |
| redis-8.10.2 | SPOP | 16 | 2,272,727 | 0.271 | 0.975 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 16 | 1,960,784 | 0.351 | 0.767 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 16 | 397,614 | 1.127 | 2.999 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 16 | 93,197 | 4.431 | 8.551 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 16 | 49,963 | 8.255 | 12.519 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 16 | 46,447 | 7.167 | 13.175 |
| valkey-9.1.2 | LPUSH | 1 | 300,752 | 0.087 | 0.327 |
| valkey-9.1.2 | RPUSH | 1 | 254,130 | 0.095 | 0.503 |
| valkey-9.1.2 | LPOP | 1 | 250,313 | 0.095 | 0.623 |
| valkey-9.1.2 | RPOP | 1 | 251,889 | 0.095 | 0.591 |
| valkey-9.1.2 | SADD | 1 | 240,674 | 0.095 | 0.567 |
| valkey-9.1.2 | HSET | 1 | 249,688 | 0.095 | 0.583 |
| valkey-9.1.2 | SPOP | 1 | 238,379 | 0.103 | 0.607 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 1 | 151,630 † | 0.095 | 0.623 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 1 | 151,630 | 0.159 | 0.655 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 1 | 65,531 | 0.391 | 1.215 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 1 | 44,140 | 0.599 | 1.735 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 1 | 37,857 † | 0.679 | 1.815 |
| valkey-9.1.2 | LPUSH | 16 | 1,709,402 | 0.415 | 0.687 |
| valkey-9.1.2 | RPUSH | 16 | 1,754,386 | 0.399 | 0.847 |
| valkey-9.1.2 | LPOP | 16 | 1,324,503 | 0.511 | 1.351 |
| valkey-9.1.2 | RPOP | 16 | 1,459,854 | 0.479 | 0.839 |
| valkey-9.1.2 | SADD | 16 | 1,980,198 | 0.343 | 0.823 |
| valkey-9.1.2 | HSET | 16 | 1,666,667 | 0.423 | 0.695 |
| valkey-9.1.2 | SPOP | 16 | 1,980,198 | 0.303 | 1.031 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 16 | 1,769,912 | 0.399 | 0.903 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 16 | 347,222 | 1.239 | 2.695 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 16 | 85,874 | 5.679 | 9.207 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 16 | 49,020 | 9.135 | 13.375 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 16 | 37,629 | 12.055 | 15.663 |

Requests/s, ratio to redis:

| test | -P | redis | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| LPUSH | 1 | 261,438 | 318,471 (1.22x) | 271,003 (1.04x) | 300,752 (1.15x) |
| RPUSH | 1 | 247,525 | 337,268 (1.36x) | 253,807 (1.03x) | 254,130 (1.03x) |
| LPOP | 1 | 247,219 | 334,448 (1.35x) | 250,000 (1.01x) | 250,313 (1.01x) |
| RPOP | 1 | 253,165 | 334,448 (1.32x) | 246,609 (0.97x) | 251,889 (0.99x) |
| SADD | 1 | 244,798 | 336,700 (1.38x) | 251,572 (1.03x) | 240,674 (0.98x) |
| HSET | 1 | 236,686 | 303,490 (1.28x) | 247,831 (1.05x) | 249,688 (1.05x) |
| SPOP | 1 | 243,309 | 318,979 (1.31x) | 236,128 (0.97x) | 238,379 (0.98x) |
| LPUSH (needed to benchmark LRANGE) | 1 | 141,443 † | 174,520 † (1.23x) | 115,741 † (0.82x) | 151,630 † (1.07x) |
| LRANGE_100 (first 100 elements) | 1 | 179,856 | 205,128 (1.14x) | 158,228 (0.88x) | 151,630 (0.84x) |
| LRANGE_300 (first 300 elements) | 1 | 71,149 | 75,988 (1.07x) | 53,248 (0.75x) | 65,531 (0.92x) |
| LRANGE_500 (first 500 elements) | 1 | 45,641 | 42,946 (0.94x) | 41,314 † (0.91x) | 44,140 (0.97x) |
| LRANGE_600 (first 600 elements) | 1 | 40,056 † | 37,608 † (0.94x) | 35,480 † (0.89x) | 37,857 † (0.95x) |
| LPUSH | 16 | 2,040,816 | 2,409,639 (1.18x) | 1,724,138 (0.84x) | 1,709,402 (0.84x) |
| RPUSH | 16 | 2,083,333 | 2,857,143 (1.37x) | 1,801,802 (0.86x) | 1,754,386 (0.84x) |
| LPOP | 16 | 1,904,762 | 2,439,024 (1.28x) | 1,709,402 (0.90x) | 1,324,503 (0.70x) |
| RPOP | 16 | 1,923,077 | 2,941,176 (1.53x) | 1,538,462 (0.80x) | 1,459,854 (0.76x) |
| SADD | 16 | 2,298,851 | 3,703,704 (1.61x) | 2,173,913 (0.95x) | 1,980,198 (0.86x) |
| HSET | 16 | 2,150,538 | 3,174,603 (1.48x) | 1,652,893 (0.77x) | 1,666,667 (0.78x) |
| SPOP | 16 | 2,409,639 | 3,636,364 (1.51x) | 2,272,727 (0.94x) | 1,980,198 (0.82x) |
| LPUSH (needed to benchmark LRANGE) | 16 | 2,325,581 | 2,469,136 (1.06x) | 1,960,784 (0.84x) | 1,769,912 (0.76x) |
| LRANGE_100 (first 100 elements) | 16 | 327,869 | 375,940 (1.15x) | 397,614 (1.21x) | 347,222 (1.06x) |
| LRANGE_300 (first 300 elements) | 16 | 80,645 | 115,009 (1.43x) | 93,197 (1.16x) | 85,874 (1.06x) |
| LRANGE_500 (first 500 elements) | 16 | 47,015 | 63,654 (1.35x) | 49,963 (1.06x) | 49,020 (1.04x) |
| LRANGE_600 (first 600 elements) | 16 | 40,519 | 54,585 (1.35x) | 46,447 (1.15x) | 37,629 (0.93x) |
