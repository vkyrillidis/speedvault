Requests/s, median of 5 runs; -c 50, -r 1000000, 16 B values.

| -P 1 (req/s) | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours alone | 416,840 | 415,282 | 391,850 | 407,166 |
| ours with a replica | 281,294 | 389,864 | 274,725 | 262,743 |
| Redis 7.0.15 alone | 351,494 | 318,573 | 323,939 | 339,328 |
| Redis 7.0.15 with a replica | 255,558 | 329,815 | 267,738 | 260,824 |
| Redis 7.0.15 cluster alone | 364,830 | 349,773 | 364,830 | 337,496 |
| Redis 7.0.15 cluster with a replica | 240,038 | 356,761 | 242,189 | 257,400 |
| redis-8.10.2 alone | 363,769 | 347,826 | 343,879 | 352,983 |
| redis-8.10.2 with a replica | 258,131 | 350,508 | 262,467 | 257,003 |
| redis-8.10.2 cluster alone | 361,664 | 360,620 | 379,363 | 353,982 |
| redis-8.10.2 cluster with a replica | 246,427 | 367,242 | 250,188 | 253,229 |
| valkey-9.1.2 alone | 358,295 | 320,924 | 329,815 | 347,705 |
| valkey-9.1.2 with a replica | 240,154 | 322,581 | 257,136 | 240,790 |
| valkey-9.1.2 cluster alone | 369,549 | 356,125 | 357,782 | 363,769 |
| valkey-9.1.2 cluster with a replica | 230,044 | 357,143 | 247,586 | 238,663 |

| -P 1: with a replica / alone | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours | 0.67x | 0.94x | 0.70x | 0.65x |
| Redis 7.0.15 | 0.73x | 1.04x | 0.83x | 0.77x |
| Redis 7.0.15 cluster | 0.66x | 1.02x | 0.66x | 0.76x |
| redis-8.10.2 | 0.71x | 1.01x | 0.76x | 0.73x |
| redis-8.10.2 cluster | 0.68x | 1.02x | 0.66x | 0.72x |
| valkey-9.1.2 | 0.67x | 1.01x | 0.78x | 0.69x |
| valkey-9.1.2 cluster | 0.62x | 1.00x | 0.69x | 0.66x |

| -P 1: ours / them | SET | GET | INCR | HSET |
|---|---|---|---|---|
| Redis 7.0.15 alone | 1.19x | 1.30x | 1.21x | 1.20x |
| Redis 7.0.15 with a replica | 1.10x | 1.18x | 1.03x | 1.01x |
| Redis 7.0.15 cluster alone | 1.14x | 1.19x | 1.07x | 1.21x |
| Redis 7.0.15 cluster with a replica | 1.17x | 1.09x | 1.13x | 1.02x |
| redis-8.10.2 alone | 1.15x | 1.19x | 1.14x | 1.15x |
| redis-8.10.2 with a replica | 1.09x | 1.11x | 1.05x | 1.02x |
| redis-8.10.2 cluster alone | 1.15x | 1.15x | 1.03x | 1.15x |
| redis-8.10.2 cluster with a replica | 1.14x | 1.06x | 1.10x | 1.04x |
| valkey-9.1.2 alone | 1.16x | 1.29x | 1.19x | 1.17x |
| valkey-9.1.2 with a replica | 1.17x | 1.21x | 1.07x | 1.09x |
| valkey-9.1.2 cluster alone | 1.13x | 1.17x | 1.10x | 1.12x |
| valkey-9.1.2 cluster with a replica | 1.22x | 1.09x | 1.11x | 1.10x |

| -P 16 (req/s) | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours alone | 2,107,482 | 2,170,374 | 2,043,945 | 1,956,947 |
| ours with a replica | 1,636,661 | 2,200,220 | 1,641,362 | 1,429,082 |
| Redis 7.0.15 alone | 1,414,928 | 1,539,054 | 1,734,605 | 1,591,090 |
| Redis 7.0.15 with a replica | 1,069,805 | 1,537,279 | 1,290,739 | 1,157,407 |
| Redis 7.0.15 cluster alone | 1,226,618 | 1,306,336 | 1,374,098 | 1,494,768 |
| Redis 7.0.15 cluster with a replica | 930,016 | 1,294,080 | 1,007,557 | 1,112,966 |
| redis-8.10.2 alone | 1,839,926 | 2,312,139 | 2,256,063 | 1,733,102 |
| redis-8.10.2 with a replica | 1,454,546 | 2,285,714 | 1,724,881 | 1,268,231 |
| redis-8.10.2 cluster alone | 1,612,253 | 1,950,268 | 1,858,736 | 1,582,905 |
| redis-8.10.2 cluster with a replica | 1,310,616 | 1,930,502 | 1,480,385 | 1,208,094 |
| valkey-9.1.2 alone | 1,736,865 | 2,228,412 | 2,262,444 | 1,535,509 |
| valkey-9.1.2 with a replica | 1,274,697 | 2,163,332 | 1,574,803 | 1,078,167 |
| valkey-9.1.2 cluster alone | 1,412,928 | 1,755,926 | 1,680,672 | 1,409,940 |
| valkey-9.1.2 cluster with a replica | 1,113,586 | 1,767,565 | 1,311,906 | 1,022,495 |

| -P 16: with a replica / alone | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours | 0.78x | 1.01x | 0.80x | 0.73x |
| Redis 7.0.15 | 0.76x | 1.00x | 0.74x | 0.73x |
| Redis 7.0.15 cluster | 0.76x | 0.99x | 0.73x | 0.74x |
| redis-8.10.2 | 0.79x | 0.99x | 0.76x | 0.73x |
| redis-8.10.2 cluster | 0.81x | 0.99x | 0.80x | 0.76x |
| valkey-9.1.2 | 0.73x | 0.97x | 0.70x | 0.70x |
| valkey-9.1.2 cluster | 0.79x | 1.01x | 0.78x | 0.73x |

| -P 16: ours / them | SET | GET | INCR | HSET |
|---|---|---|---|---|
| Redis 7.0.15 alone | 1.49x | 1.41x | 1.18x | 1.23x |
| Redis 7.0.15 with a replica | 1.53x | 1.43x | 1.27x | 1.23x |
| Redis 7.0.15 cluster alone | 1.72x | 1.66x | 1.49x | 1.31x |
| Redis 7.0.15 cluster with a replica | 1.76x | 1.70x | 1.63x | 1.28x |
| redis-8.10.2 alone | 1.15x | 0.94x | 0.91x | 1.13x |
| redis-8.10.2 with a replica | 1.13x | 0.96x | 0.95x | 1.13x |
| redis-8.10.2 cluster alone | 1.31x | 1.11x | 1.10x | 1.24x |
| redis-8.10.2 cluster with a replica | 1.25x | 1.14x | 1.11x | 1.18x |
| valkey-9.1.2 alone | 1.21x | 0.97x | 0.90x | 1.27x |
| valkey-9.1.2 with a replica | 1.28x | 1.02x | 1.04x | 1.33x |
| valkey-9.1.2 cluster alone | 1.49x | 1.24x | 1.22x | 1.39x |
| valkey-9.1.2 cluster with a replica | 1.47x | 1.24x | 1.25x | 1.40x |
