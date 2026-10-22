# Data types: Redis 7.0.15 vs speedvault d1dddda vs Redis 8.10.2 vs Valkey 9.1.2

- Date: 2026-10-22
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 200000 -c 50 -d 3 -t lpush,rpush,lpop,rpop,sadd,hset,spop,lrange_100,lrange_300,lrange_500,lrange_600 --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis: cpus 2 (pid 437); speedvault: cpus 2 (pid 438); redis-8.10.2: cpus 2 (pid 439); valkey-9.1.2: cpus 2 (pid 440)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis | LPUSH | 1 | 252,844 | 0.095 | 0.535 |
| redis | RPUSH | 1 | 272,109 | 0.087 | 0.535 |
| redis | LPOP | 1 | 279,330 | 0.087 | 0.535 |
| redis | RPOP | 1 | 268,817 | 0.087 | 0.527 |
| redis | SADD | 1 | 264,550 | 0.087 | 0.543 |
| redis | HSET | 1 | 280,899 | 0.087 | 0.487 |
| redis | SPOP | 1 | 280,505 | 0.087 | 0.327 |
| redis | LPUSH (needed to benchmark LRANGE) | 1 | invalid (clock) † | 0.087 | 0.295 |
| redis | LRANGE_100 (first 100 elements) | 1 | 194,932 | 0.127 | 0.575 |
| redis | LRANGE_300 (first 300 elements) | 1 | invalid (clock) † | 0.311 | 0.991 |
| redis | LRANGE_500 (first 500 elements) | 1 | 53,706 | 0.479 | 1.287 |
| redis | LRANGE_600 (first 600 elements) | 1 | 45,988 | 0.575 | 1.511 |
| redis | LPUSH | 16 | 2,597,402 | 0.255 | 0.543 |
| redis | RPUSH | 16 | 2,816,901 | 0.239 | 0.543 |
| redis | LPOP | 16 | 2,531,646 | 0.287 | 0.407 |
| redis | RPOP | 16 | 2,409,639 | 0.279 | 0.687 |
| redis | SADD | 16 | 3,174,603 | 0.215 | 0.423 |
| redis | HSET | 16 | 2,666,666 | 0.255 | 0.559 |
| redis | SPOP | 16 | 3,278,688 | 0.175 | 0.695 |
| redis | LPUSH (needed to benchmark LRANGE) | 16 | invalid (clock) † | 0.263 | 0.455 |
| redis | LRANGE_100 (first 100 elements) | 16 | 396,825 | 1.551 | 2.783 |
| redis | LRANGE_300 (first 300 elements) | 16 | 80,386 | 6.295 | 9.415 |
| redis | LRANGE_500 (first 500 elements) | 16 | 54,274 | 8.399 | 12.967 |
| redis | LRANGE_600 (first 600 elements) | 16 | invalid (clock) † | 11.559 | 14.879 |
| speedvault | LPUSH | 1 | 392,157 | 0.063 | 0.295 |
| speedvault | RPUSH | 1 | 392,927 | 0.063 | 0.327 |
| speedvault | LPOP | 1 | 407,332 | 0.063 | 0.239 |
| speedvault | RPOP | 1 | 412,371 | 0.063 | 0.231 |
| speedvault | SADD | 1 | 416,667 | 0.063 | 0.231 |
| speedvault | HSET | 1 | 400,000 | 0.063 | 0.247 |
| speedvault | SPOP | 1 | 408,163 | 0.063 | 0.231 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 1 | invalid (clock) † | 0.063 | 0.239 |
| speedvault | LRANGE_100 (first 100 elements) | 1 | 233,645 | 0.103 | 0.303 |
| speedvault | LRANGE_300 (first 300 elements) | 1 | 78,585 | 0.303 | 0.719 |
| speedvault | LRANGE_500 (first 500 elements) | 1 | 49,975 | 0.487 | 0.943 |
| speedvault | LRANGE_600 (first 600 elements) | 1 | invalid (clock) † | 0.575 | 1.127 |
| speedvault | LPUSH | 16 | 3,508,772 | 0.191 | 0.495 |
| speedvault | RPUSH | 16 | 2,985,074 | 0.191 | 0.623 |
| speedvault | LPOP | 16 | 3,846,154 | 0.167 | 0.431 |
| speedvault | RPOP | 16 | 3,846,154 | 0.167 | 0.527 |
| speedvault | SADD | 16 | 4,651,162 | 0.143 | 0.335 |
| speedvault | HSET | 16 | 3,773,585 | 0.167 | 0.455 |
| speedvault | SPOP | 16 | 5,405,406 | 0.071 | 0.319 |
| speedvault | LPUSH (needed to benchmark LRANGE) | 16 | 3,508,772 | 0.191 | 0.431 |
| speedvault | LRANGE_100 (first 100 elements) | 16 | 434,783 | 0.895 | 1.711 |
| speedvault | LRANGE_300 (first 300 elements) | 16 | 131,839 | 1.991 | 3.951 |
| speedvault | LRANGE_500 (first 500 elements) | 16 | 73,992 | 2.055 | 5.135 |
| speedvault | LRANGE_600 (first 600 elements) | 16 | 64,558 | 2.095 | 4.927 |
| redis-8.10.2 | LPUSH | 1 | 320,513 | 0.079 | 0.319 |
| redis-8.10.2 | RPUSH | 1 | 316,957 | 0.079 | 0.351 |
| redis-8.10.2 | LPOP | 1 | 328,947 | 0.079 | 0.311 |
| redis-8.10.2 | RPOP | 1 | invalid (clock) † | 0.079 | 0.343 |
| redis-8.10.2 | SADD | 1 | 307,692 | 0.079 | 0.359 |
| redis-8.10.2 | HSET | 1 | 309,598 | 0.079 | 0.391 |
| redis-8.10.2 | SPOP | 1 | 286,123 | 0.087 | 0.415 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 1 | invalid (clock) † | 0.087 | 0.351 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 1 | 160,772 | 0.159 | 0.559 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 1 | 64,329 | 0.399 | 0.943 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 1 | 46,404 | 0.551 | 1.151 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 1 | invalid (clock) † | 0.631 | 1.255 |
| redis-8.10.2 | LPUSH | 16 | 2,352,941 | 0.303 | 0.711 |
| redis-8.10.2 | RPUSH | 16 | 2,469,136 | 0.287 | 0.575 |
| redis-8.10.2 | LPOP | 16 | 2,061,856 | 0.343 | 0.807 |
| redis-8.10.2 | RPOP | 16 | 2,247,191 | 0.327 | 0.535 |
| redis-8.10.2 | SADD | 16 | 2,816,901 | 0.247 | 0.519 |
| redis-8.10.2 | HSET | 16 | 2,380,952 | 0.303 | 0.495 |
| redis-8.10.2 | SPOP | 16 | 2,985,074 | 0.223 | 0.511 |
| redis-8.10.2 | LPUSH (needed to benchmark LRANGE) | 16 | 2,127,660 † | 0.303 | 0.735 |
| redis-8.10.2 | LRANGE_100 (first 100 elements) | 16 | 442,478 | 0.935 | 2.143 |
| redis-8.10.2 | LRANGE_300 (first 300 elements) | 16 | 102,775 | 4.167 | 7.407 |
| redis-8.10.2 | LRANGE_500 (first 500 elements) | 16 | 55,005 † | 7.327 | 10.759 |
| redis-8.10.2 | LRANGE_600 (first 600 elements) | 16 | 48,876 | 7.927 | 12.007 |
| valkey-9.1.2 | LPUSH | 1 | 321,543 | 0.079 | 0.311 |
| valkey-9.1.2 | RPUSH | 1 | 318,979 | 0.079 | 0.335 |
| valkey-9.1.2 | LPOP | 1 | 328,947 | 0.079 | 0.343 |
| valkey-9.1.2 | RPOP | 1 | 329,489 | 0.079 | 0.327 |
| valkey-9.1.2 | SADD | 1 | 312,989 | 0.087 | 0.303 |
| valkey-9.1.2 | HSET | 1 | 325,733 | 0.079 | 0.295 |
| valkey-9.1.2 | SPOP | 1 | 288,184 | 0.087 | 0.439 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 1 | 301,659 † | 0.087 | 0.423 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 1 | 172,563 | 0.143 | 0.583 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 1 | 67,958 | 0.383 | 0.895 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 1 | 49,092 † | 0.527 | 1.191 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 1 | 42,088 † | 0.615 | 1.343 |
| valkey-9.1.2 | LPUSH | 16 | 2,173,913 | 0.335 | 0.455 |
| valkey-9.1.2 | RPUSH | 16 | 2,222,222 | 0.327 | 0.703 |
| valkey-9.1.2 | LPOP | 16 | 1,886,792 | 0.399 | 0.535 |
| valkey-9.1.2 | RPOP | 16 | 1,851,852 | 0.383 | 0.679 |
| valkey-9.1.2 | SADD | 16 | 2,500,000 | 0.279 | 0.591 |
| valkey-9.1.2 | HSET | 16 | 2,127,660 | 0.343 | 0.743 |
| valkey-9.1.2 | SPOP | 16 | 2,898,551 | 0.231 | 0.471 |
| valkey-9.1.2 | LPUSH (needed to benchmark LRANGE) | 16 | 2,127,660 | 0.335 | 0.687 |
| valkey-9.1.2 | LRANGE_100 (first 100 elements) | 16 | 451,467 | 0.943 | 2.551 |
| valkey-9.1.2 | LRANGE_300 (first 300 elements) | 16 | 90,662 | 5.319 | 8.687 |
| valkey-9.1.2 | LRANGE_500 (first 500 elements) | 16 | 54,705 | 8.295 | 12.559 |
| valkey-9.1.2 | LRANGE_600 (first 600 elements) | 16 | 40,201 | 11.215 | 14.239 |

Requests/s, ratio to redis:

| test | -P | redis | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| LPUSH | 1 | 252,844 | 392,157 (1.55x) | 320,513 (1.27x) | 321,543 (1.27x) |
| RPUSH | 1 | 272,109 | 392,927 (1.44x) | 316,957 (1.16x) | 318,979 (1.17x) |
| LPOP | 1 | 279,330 | 407,332 (1.46x) | 328,947 (1.18x) | 328,947 (1.18x) |
| RPOP | 1 | 268,817 | 412,371 (1.53x) | invalid (clock) † | 329,489 (1.23x) |
| SADD | 1 | 264,550 | 416,667 (1.57x) | 307,692 (1.16x) | 312,989 (1.18x) |
| HSET | 1 | 280,899 | 400,000 (1.42x) | 309,598 (1.10x) | 325,733 (1.16x) |
| SPOP | 1 | 280,505 | 408,163 (1.46x) | 286,123 (1.02x) | 288,184 (1.03x) |
| LPUSH (needed to benchmark LRANGE) | 1 | invalid (clock) † | invalid (clock) † | invalid (clock) † | 301,659 † |
| LRANGE_100 (first 100 elements) | 1 | 194,932 | 233,645 (1.20x) | 160,772 (0.82x) | 172,563 (0.89x) |
| LRANGE_300 (first 300 elements) | 1 | invalid (clock) † | 78,585 | 64,329 | 67,958 |
| LRANGE_500 (first 500 elements) | 1 | 53,706 | 49,975 (0.93x) | 46,404 (0.86x) | 49,092 † (0.91x) |
| LRANGE_600 (first 600 elements) | 1 | 45,988 | invalid (clock) † | invalid (clock) † | 42,088 † (0.92x) |
| LPUSH | 16 | 2,597,402 | 3,508,772 (1.35x) | 2,352,941 (0.91x) | 2,173,913 (0.84x) |
| RPUSH | 16 | 2,816,901 | 2,985,074 (1.06x) | 2,469,136 (0.88x) | 2,222,222 (0.79x) |
| LPOP | 16 | 2,531,646 | 3,846,154 (1.52x) | 2,061,856 (0.81x) | 1,886,792 (0.75x) |
| RPOP | 16 | 2,409,639 | 3,846,154 (1.60x) | 2,247,191 (0.93x) | 1,851,852 (0.77x) |
| SADD | 16 | 3,174,603 | 4,651,162 (1.47x) | 2,816,901 (0.89x) | 2,500,000 (0.79x) |
| HSET | 16 | 2,666,666 | 3,773,585 (1.42x) | 2,380,952 (0.89x) | 2,127,660 (0.80x) |
| SPOP | 16 | 3,278,688 | 5,405,406 (1.65x) | 2,985,074 (0.91x) | 2,898,551 (0.88x) |
| LPUSH (needed to benchmark LRANGE) | 16 | invalid (clock) † | 3,508,772 | 2,127,660 † | 2,127,660 |
| LRANGE_100 (first 100 elements) | 16 | 396,825 | 434,783 (1.10x) | 442,478 (1.12x) | 451,467 (1.14x) |
| LRANGE_300 (first 300 elements) | 16 | 80,386 | 131,839 (1.64x) | 102,775 (1.28x) | 90,662 (1.13x) |
| LRANGE_500 (first 500 elements) | 16 | 54,274 | 73,992 (1.36x) | 55,005 † (1.01x) | 54,705 (1.01x) |
| LRANGE_600 (first 600 elements) | 16 | invalid (clock) † | 64,558 | 48,876 | 40,201 |
