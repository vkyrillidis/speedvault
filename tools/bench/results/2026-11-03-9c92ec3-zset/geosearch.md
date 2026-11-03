# GEOSEARCH on 1000000 points: speedvault 9c92ec3

- Date: 2026-11-03
- Machine: AMD Ryzen 7 9800X3D 8-Core Processor (WSL2), 16 CPUs, kernel 5.15.167.4-microsoft-standard-WSL2
- Client: redis-benchmark 7.0.15, `-n 100000 -c 50 -d 3 -t  --threads 1 --csv`, -P in {1, 16}
- Pinning: redis-benchmark on CPUs 4-7; servers: redis-7.0.15: cpus 2 (pid 498); speedvault: cpus 2 (pid 500); redis-8.10.2: cpus 2 (pid 507); valkey-9.1.2: cpus 2 (pid 509)

| server | test | -P | requests/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 1 | 54,054 | 0.871 | 1.663 |
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 1 | 57,937 | 0.839 | 1.327 |
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 1 | 3,975 | 11.791 | 24.399 |
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 16 | 67,476 | 11.543 | 17.775 |
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 16 | 73,692 | 10.471 | 15.535 |
| redis-7.0.15 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 16 | 3,889 | 158.975 | 239.231 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 1 | 65,445 | 0.687 | 1.407 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 1 | 73,260 | 0.623 | 1.175 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 1 | 6,154 | 7.575 | 16.367 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 16 | 93,458 | 8.311 | 11.063 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 16 | 108,814 | 7.095 | 9.599 |
| speedvault | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 16 | 6,229 | 95.551 | 143.103 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 1 | 61,463 | 0.767 | 1.439 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 1 | 67,295 | 0.711 | 1.199 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 1 | 4,444 | 10.599 | 22.591 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 16 | 77,640 | 9.927 | 14.559 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 16 | 85,616 | 9.031 | 12.231 |
| redis-8.10.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 16 | 4,455 | 135.551 | 221.567 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 1 | 58,858 | 0.799 | 1.455 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 1 | 62,228 | 0.759 | 1.335 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 1 | 4,235 | 11.063 | 23.343 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 16 | 76,687 | 10.071 | 16.015 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 16 | 81,169 | 9.487 | 13.231 |
| valkey-9.1.2 | GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 16 | 4,113 | 146.047 | 240.639 |

Requests/s, ratio to redis-7.0.15:

| test | -P | redis-7.0.15 | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---:|---:|---:|---:|---:|
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 1 | 54,054 | 65,445 (1.21x) | 61,463 (1.14x) | 58,858 (1.09x) |
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 1 | 57,937 | 73,260 (1.26x) | 67,295 (1.16x) | 62,228 (1.07x) |
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 1 | 3,975 | 6,154 (1.55x) | 4,444 (1.12x) | 4,235 (1.07x) |
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 50 km COUNT 10 ASC | 16 | 67,476 | 93,458 (1.39x) | 77,640 (1.15x) | 76,687 (1.14x) |
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYBOX 100 100 km COUNT 10 ASC | 16 | 73,692 | 108,814 (1.48x) | 85,616 (1.16x) | 81,169 (1.10x) |
| GEOSEARCH geo FROMMEMBER p:__rand_int__ BYRADIUS 200 km | 16 | 3,889 | 6,229 (1.60x) | 4,455 (1.15x) | 4,113 (1.06x) |
