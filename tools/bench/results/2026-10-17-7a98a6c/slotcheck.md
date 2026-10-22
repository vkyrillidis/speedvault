# speedvault 7a98a6c: standalone vs cluster mode (owning every slot)

- Date: 2026-10-17
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t set,get,incr,mset --threads 1 --csv`, -P in {1, 16, 64}
- Pinning: redis-benchmark on CPUs 4-7; servers: standalone: cpus 2 (pid 37299); cluster-mode: cpus 2 (pid 37300)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| standalone | SET | 1 | 391,389 | 0.063 | 0.271 |
| standalone | GET | 1 | 387,898 | 0.063 | 0.287 |
| standalone | INCR | 1 | 406,835 | 0.063 | 0.223 |
| standalone | MSET (10 keys) | 1 | 411,523 | 0.063 | 0.247 |
| standalone | SET | 16 | 5,376,344 | 0.071 | 0.183 |
| standalone | GET | 16 | 5,181,347 | 0.071 | 0.303 |
| standalone | INCR | 16 | 5,076,142 | 0.103 | 0.487 |
| standalone | MSET (10 keys) | 16 | 2,118,644 | 0.343 | 0.599 |
| standalone | SET | 64 | 12,500,000 | 0.183 | 0.463 |
| standalone | GET | 64 | 12,820,513 | 0.119 | 0.543 |
| standalone | INCR | 64 | 7,633,588 | 0.351 | 0.679 |
| standalone | MSET (10 keys) | 64 | 2,754,821 | 1.087 | 1.535 |
| cluster-mode | SET | 1 | 406,174 | 0.063 | 0.223 |
| cluster-mode | GET | 1 | 407,498 † | 0.063 | 0.223 |
| cluster-mode | INCR | 1 | 405,680 | 0.063 | 0.231 |
| cluster-mode | MSET (10 keys) | 1 | 409,165 | 0.063 | 0.263 |
| cluster-mode | SET | 16 | 5,263,158 | 0.079 | 0.335 |
| cluster-mode | GET | 16 | 5,347,594 | 0.071 | 0.303 |
| cluster-mode | INCR | 16 | 4,000,000 | 0.167 | 0.375 |
| cluster-mode | MSET (10 keys) | 16 | 1,893,939 | 0.383 | 0.719 |
| cluster-mode | SET | 64 | 8,403,361 | 0.311 | 0.599 |
| cluster-mode | GET | 64 | 9,433,962 | 0.287 | 0.439 |
| cluster-mode | INCR | 64 | 6,289,308 | 0.447 | 0.663 |
| cluster-mode | MSET (10 keys) | 64 | 2,450,980 | 1.223 | 1.655 |

Requests/s, ratio to standalone:

| test | -P | standalone | cluster-mode |
|---|---:|---:|---:|
| SET | 1 | 391,389 | 406,174 (1.04x) |
| GET | 1 | 387,898 | 407,498 † (1.05x) |
| INCR | 1 | 406,835 | 405,680 (1.00x) |
| MSET (10 keys) | 1 | 411,523 | 409,165 (0.99x) |
| SET | 16 | 5,376,344 | 5,263,158 (0.98x) |
| GET | 16 | 5,181,347 | 5,347,594 (1.03x) |
| INCR | 16 | 5,076,142 | 4,000,000 (0.79x) |
| MSET (10 keys) | 16 | 2,118,644 | 1,893,939 (0.89x) |
| SET | 64 | 12,500,000 | 8,403,361 (0.67x) |
| GET | 64 | 12,820,513 | 9,433,962 (0.74x) |
| INCR | 64 | 7,633,588 | 6,289,308 (0.82x) |
| MSET (10 keys) | 64 | 2,754,821 | 2,450,980 (0.89x) |
