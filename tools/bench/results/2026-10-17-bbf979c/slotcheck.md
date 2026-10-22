# speedvault bbf979c: standalone vs cluster mode (owning every slot)

- Date: 2026-10-17
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 1000000 -c 50 -d 3 -t set,get,incr,mset --threads 1 --csv`, -P in {1, 16, 64}
- Pinning: redis-benchmark on CPUs 4-7; servers: standalone: cpus 2 (pid 48706); cluster-mode: cpus 2 (pid 48707)
- Clock: the wall clock stepped by seconds every ~5.5 s (WSL2, host clock not synced), and redis-benchmark times with it. Each test started right after a step and was rerun if the clock had moved during it (0 rerun(s)); rows marked † moved in every try: by a whole pair (requests/s corrected), or by one step (invalid)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| standalone | SET | 1 | 399,521 | 0.063 | 0.231 |
| standalone | GET | 1 | 387,898 | 0.063 | 0.279 |
| standalone | INCR | 1 | 399,840 | 0.063 | 0.239 |
| standalone | MSET (10 keys) | 1 | 408,831 | 0.063 | 0.247 |
| standalone | SET | 16 | 4,761,905 | 0.079 | 0.327 |
| standalone | GET | 16 | 4,854,369 | 0.079 | 0.303 |
| standalone | INCR | 16 | 4,807,692 | 0.095 | 0.327 |
| standalone | MSET (10 keys) | 16 | 2,016,129 | 0.351 | 0.655 |
| standalone | SET | 64 | 10,416,667 | 0.199 | 0.607 |
| standalone | GET | 64 | 11,111,111 | 0.135 | 0.623 |
| standalone | INCR | 64 | 7,246,377 | 0.375 | 0.639 |
| standalone | MSET (10 keys) | 64 | 2,624,672 | 1.103 | 1.927 |
| cluster-mode | SET | 1 | 397,456 | 0.063 | 0.247 |
| cluster-mode | GET | 1 | 404,040 | 0.063 | 0.223 |
| cluster-mode | INCR | 1 | 400,160 | 0.063 | 0.263 |
| cluster-mode | MSET (10 keys) | 1 | 382,848 | 0.063 | 0.335 |
| cluster-mode | SET | 16 | 4,566,210 | 0.079 | 0.495 |
| cluster-mode | GET | 16 | 4,545,454 | 0.079 | 0.487 |
| cluster-mode | INCR | 16 | 4,566,210 | 0.119 | 0.335 |
| cluster-mode | MSET (10 keys) | 16 | 1,926,782 | 0.375 | 0.703 |
| cluster-mode | SET | 64 | 9,345,794 | 0.263 | 0.743 |
| cluster-mode | GET | 64 | 10,638,298 | 0.175 | 0.631 |
| cluster-mode | INCR | 64 | 6,802,721 | 0.399 | 0.767 |
| cluster-mode | MSET (10 keys) | 64 | 2,487,562 | 1.175 | 1.895 |

Requests/s, ratio to standalone:

| test | -P | standalone | cluster-mode |
|---|---:|---:|---:|
| SET | 1 | 399,521 | 397,456 (0.99x) |
| GET | 1 | 387,898 | 404,040 (1.04x) |
| INCR | 1 | 399,840 | 400,160 (1.00x) |
| MSET (10 keys) | 1 | 408,831 | 382,848 (0.94x) |
| SET | 16 | 4,761,905 | 4,566,210 (0.96x) |
| GET | 16 | 4,854,369 | 4,545,454 (0.94x) |
| INCR | 16 | 4,807,692 | 4,566,210 (0.95x) |
| MSET (10 keys) | 16 | 2,016,129 | 1,926,782 (0.96x) |
| SET | 64 | 10,416,667 | 9,345,794 (0.90x) |
| GET | 64 | 11,111,111 | 10,638,298 (0.96x) |
| INCR | 64 | 7,246,377 | 6,802,721 (0.94x) |
| MSET (10 keys) | 64 | 2,624,672 | 2,487,562 (0.95x) |
