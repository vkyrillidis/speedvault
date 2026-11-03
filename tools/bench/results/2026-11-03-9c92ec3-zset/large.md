# A sorted set of 1000000 members: speedvault 9c92ec3

- Date: 2026-11-03
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t  --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis-7.0.15: cpus 2 (pid 498); speedvault: cpus 2 (pid 500); redis-8.10.2: cpus 2 (pid 507); valkey-9.1.2: cpus 2 (pid 509)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis-7.0.15 | ZSCORE big m:__rand_int__ | 1 | 366,166 | 0.071 | 0.375 |
| redis-7.0.15 | ZRANK big m:__rand_int__ | 1 | 269,251 | 0.151 | 0.503 |
| redis-7.0.15 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 1 | 179,340 | 0.247 | 0.607 |
| redis-7.0.15 | ZADD big __rand_int__ m:__rand_int__ | 1 | 186,428 | 0.231 | 0.575 |
| redis-7.0.15 | ZINCRBY big 1 m:__rand_int__ | 1 | 201,532 | 0.215 | 0.543 |
| redis-7.0.15 | GEOPOS geo p:__rand_int__ | 1 | 341,647 | 0.103 | 0.503 |
| redis-7.0.15 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 1 | 379,507 | 0.071 | 0.367 |
| redis-7.0.15 | ZSCORE big m:__rand_int__ | 16 | 1,335,114 | 0.559 | 0.919 |
| redis-7.0.15 | ZRANK big m:__rand_int__ | 16 | 727,273 | 1.063 | 1.559 |
| redis-7.0.15 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 16 | 427,168 | 1.759 | 2.759 |
| redis-7.0.15 | ZADD big __rand_int__ m:__rand_int__ | 16 | 447,227 | 1.743 | 2.359 |
| redis-7.0.15 | ZINCRBY big 1 m:__rand_int__ | 16 | 511,247 | 1.527 | 2.087 |
| redis-7.0.15 | GEOPOS geo p:__rand_int__ | 16 | 821,018 | 0.895 | 1.407 |
| redis-7.0.15 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 16 | 1,135,074 | 0.655 | 1.127 |
| speedvault | ZSCORE big m:__rand_int__ | 1 | 400,320 | 0.063 | 0.415 |
| speedvault | ZRANK big m:__rand_int__ | 1 | 389,257 | 0.063 | 0.335 |
| speedvault | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 1 | 270,124 | 0.151 | 0.511 |
| speedvault | ZADD big __rand_int__ m:__rand_int__ | 1 | 292,912 | 0.151 | 0.375 |
| speedvault | ZINCRBY big 1 m:__rand_int__ | 1 | 304,971 | 0.135 | 0.503 |
| speedvault | GEOPOS geo p:__rand_int__ | 1 | 380,952 | 0.063 | 0.415 |
| speedvault | GEODIST geo p:__rand_int__ p:__rand_int__ km | 1 | 394,322 | 0.063 | 0.319 |
| speedvault | ZSCORE big m:__rand_int__ | 16 | 1,988,072 | 0.359 | 0.671 |
| speedvault | ZRANK big m:__rand_int__ | 16 | 1,259,446 | 0.583 | 0.943 |
| speedvault | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 16 | 699,790 | 1.031 | 1.503 |
| speedvault | ZADD big __rand_int__ m:__rand_int__ | 16 | 807,754 | 0.935 | 1.367 |
| speedvault | ZINCRBY big 1 m:__rand_int__ | 16 | 872,600 | 0.863 | 1.263 |
| speedvault | GEOPOS geo p:__rand_int__ | 16 | 1,564,945 | 0.447 | 0.831 |
| speedvault | GEODIST geo p:__rand_int__ p:__rand_int__ km | 16 | 1,394,700 | 0.527 | 0.903 |
| redis-8.10.2 | ZSCORE big m:__rand_int__ | 1 | 344,353 | 0.071 | 0.431 |
| redis-8.10.2 | ZRANK big m:__rand_int__ | 1 | 291,800 | 0.135 | 0.519 |
| redis-8.10.2 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 1 | 204,290 | 0.207 | 0.503 |
| redis-8.10.2 | ZADD big __rand_int__ m:__rand_int__ | 1 | 202,799 | 0.215 | 0.519 |
| redis-8.10.2 | ZINCRBY big 1 m:__rand_int__ | 1 | 266,241 | 0.151 | 0.527 |
| redis-8.10.2 | GEOPOS geo p:__rand_int__ | 1 | 343,407 | 0.071 | 0.447 |
| redis-8.10.2 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 1 | 379,075 | 0.071 | 0.407 |
| redis-8.10.2 | ZSCORE big m:__rand_int__ | 16 | 1,801,802 | 0.399 | 0.743 |
| redis-8.10.2 | ZRANK big m:__rand_int__ | 16 | 936,330 | 0.815 | 1.295 |
| redis-8.10.2 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 16 | 576,037 | 1.279 | 2.111 |
| redis-8.10.2 | ZADD big __rand_int__ m:__rand_int__ | 16 | 554,324 | 1.407 | 2.079 |
| redis-8.10.2 | ZINCRBY big 1 m:__rand_int__ | 16 | 806,452 | 0.951 | 1.399 |
| redis-8.10.2 | GEOPOS geo p:__rand_int__ | 16 | 1,430,615 | 0.503 | 0.863 |
| redis-8.10.2 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 16 | 1,216,545 | 0.615 | 1.015 |
| valkey-9.1.2 | ZSCORE big m:__rand_int__ | 1 | 325,309 | 0.079 | 0.439 |
| valkey-9.1.2 | ZRANK big m:__rand_int__ | 1 | 307,031 | 0.127 | 0.471 |
| valkey-9.1.2 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 1 | 187,126 | 0.231 | 0.583 |
| valkey-9.1.2 | ZADD big __rand_int__ m:__rand_int__ | 1 | 199,481 | 0.215 | 0.543 |
| valkey-9.1.2 | ZINCRBY big 1 m:__rand_int__ | 1 | 261,986 | 0.151 | 0.535 |
| valkey-9.1.2 | GEOPOS geo p:__rand_int__ | 1 | 349,895 | 0.103 | 0.367 |
| valkey-9.1.2 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 1 | 379,795 | 0.071 | 0.423 |
| valkey-9.1.2 | ZSCORE big m:__rand_int__ | 16 | 1,709,402 | 0.431 | 0.775 |
| valkey-9.1.2 | ZRANK big m:__rand_int__ | 16 | 902,527 | 0.847 | 1.311 |
| valkey-9.1.2 | ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 16 | 484,496 | 1.559 | 2.287 |
| valkey-9.1.2 | ZADD big __rand_int__ m:__rand_int__ | 16 | 509,944 | 1.511 | 2.159 |
| valkey-9.1.2 | ZINCRBY big 1 m:__rand_int__ | 16 | 710,732 | 1.055 | 1.687 |
| valkey-9.1.2 | GEOPOS geo p:__rand_int__ | 16 | 778,210 | 0.935 | 1.671 |
| valkey-9.1.2 | GEODIST geo p:__rand_int__ p:__rand_int__ km | 16 | 1,069,519 | 0.695 | 1.207 |

Requests/s, ratio to redis-7.0.15:

| test | -P | redis-7.0.15 | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| ZSCORE big m:__rand_int__ | 1 | 366,166 | 400,320 (1.09x) | 344,353 (0.94x) | 325,309 (0.89x) |
| ZRANK big m:__rand_int__ | 1 | 269,251 | 389,257 (1.45x) | 291,800 (1.08x) | 307,031 (1.14x) |
| ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 1 | 179,340 | 270,124 (1.51x) | 204,290 (1.14x) | 187,126 (1.04x) |
| ZADD big __rand_int__ m:__rand_int__ | 1 | 186,428 | 292,912 (1.57x) | 202,799 (1.09x) | 199,481 (1.07x) |
| ZINCRBY big 1 m:__rand_int__ | 1 | 201,532 | 304,971 (1.51x) | 266,241 (1.32x) | 261,986 (1.30x) |
| GEOPOS geo p:__rand_int__ | 1 | 341,647 | 380,952 (1.12x) | 343,407 (1.01x) | 349,895 (1.02x) |
| GEODIST geo p:__rand_int__ p:__rand_int__ km | 1 | 379,507 | 394,322 (1.04x) | 379,075 (1.00x) | 379,795 (1.00x) |
| ZSCORE big m:__rand_int__ | 16 | 1,335,114 | 1,988,072 (1.49x) | 1,801,802 (1.35x) | 1,709,402 (1.28x) |
| ZRANK big m:__rand_int__ | 16 | 727,273 | 1,259,446 (1.73x) | 936,330 (1.29x) | 902,527 (1.24x) |
| ZRANGEBYSCORE big __rand_int__ +inf LIMIT 0 10 | 16 | 427,168 | 699,790 (1.64x) | 576,037 (1.35x) | 484,496 (1.13x) |
| ZADD big __rand_int__ m:__rand_int__ | 16 | 447,227 | 807,754 (1.81x) | 554,324 (1.24x) | 509,944 (1.14x) |
| ZINCRBY big 1 m:__rand_int__ | 16 | 511,247 | 872,600 (1.71x) | 806,452 (1.58x) | 710,732 (1.39x) |
| GEOPOS geo p:__rand_int__ | 16 | 821,018 | 1,564,945 (1.91x) | 1,430,615 (1.74x) | 778,210 (0.95x) |
| GEODIST geo p:__rand_int__ p:__rand_int__ km | 16 | 1,135,074 | 1,394,700 (1.23x) | 1,216,545 (1.07x) | 1,069,519 (0.94x) |
