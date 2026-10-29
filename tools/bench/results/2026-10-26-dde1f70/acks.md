Requests/s, median of 5 runs; -c 50, -r 1000000, 16 B values.

| -P 1 (req/s) | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours alone | 405,844 | 416,320 | 410,341 | 407,664 |
| ours with a replica | 317,561 | 415,628 | 317,058 | 293,945 |
| Redis 7.0.15 alone | 375,657 | 346,260 | 332,447 | 366,569 |
| Redis 7.0.15 with a replica | 264,760 | 353,482 | 282,486 | 277,778 |
| redis-8.10.2 alone | 387,147 | 368,596 | 379,939 | 380,228 |
| redis-8.10.2 with a replica | 263,158 | 375,940 | 274,348 | 271,003 |
| valkey-9.1.2 alone | 371,885 | 340,252 | 347,464 | 369,822 |
| valkey-9.1.2 with a replica | 257,400 | 342,936 | 271,370 | 256,805 |

| -P 1: with a replica / alone | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours | 0.78x | 1.00x | 0.77x | 0.72x |
| Redis 7.0.15 | 0.70x | 1.02x | 0.85x | 0.76x |
| redis-8.10.2 | 0.68x | 1.02x | 0.72x | 0.71x |
| valkey-9.1.2 | 0.69x | 1.01x | 0.78x | 0.69x |

| -P 1: ours / them | SET | GET | INCR | HSET |
|---|---|---|---|---|
| Redis 7.0.15 alone | 1.08x | 1.20x | 1.23x | 1.11x |
| Redis 7.0.15 with a replica | 1.20x | 1.18x | 1.12x | 1.06x |
| redis-8.10.2 alone | 1.05x | 1.13x | 1.08x | 1.07x |
| redis-8.10.2 with a replica | 1.21x | 1.11x | 1.16x | 1.08x |
| valkey-9.1.2 alone | 1.09x | 1.22x | 1.18x | 1.10x |
| valkey-9.1.2 with a replica | 1.23x | 1.21x | 1.17x | 1.14x |

| -P 16 (req/s) | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours alone | 2,224,694 | 2,203,857 | 2,155,172 | 2,137,894 |
| ours with a replica | 1,739,130 | 2,237,136 | 1,707,213 | 1,558,239 |
| Redis 7.0.15 alone | 1,499,250 | 1,614,205 | 1,814,059 | 1,698,514 |
| Redis 7.0.15 with a replica | 1,129,944 | 1,631,987 | 1,358,696 | 1,231,527 |
| redis-8.10.2 alone | 1,952,172 | 2,421,308 | 2,343,292 | 1,721,170 |
| redis-8.10.2 with a replica | 1,531,394 | 2,418,380 | 1,800,991 | 1,354,096 |
| valkey-9.1.2 alone | 1,857,010 | 2,340,550 | 2,351,558 | 1,639,344 |
| valkey-9.1.2 with a replica | 1,339,136 | 2,308,136 | 1,679,966 | 1,117,943 |

| -P 16: with a replica / alone | SET | GET | INCR | HSET |
|---|---|---|---|---|
| ours | 0.78x | 1.02x | 0.79x | 0.73x |
| Redis 7.0.15 | 0.75x | 1.01x | 0.75x | 0.73x |
| redis-8.10.2 | 0.78x | 1.00x | 0.77x | 0.79x |
| valkey-9.1.2 | 0.72x | 0.99x | 0.71x | 0.68x |

| -P 16: ours / them | SET | GET | INCR | HSET |
|---|---|---|---|---|
| Redis 7.0.15 alone | 1.48x | 1.37x | 1.19x | 1.26x |
| Redis 7.0.15 with a replica | 1.54x | 1.37x | 1.26x | 1.27x |
| redis-8.10.2 alone | 1.14x | 0.91x | 0.92x | 1.24x |
| redis-8.10.2 with a replica | 1.14x | 0.93x | 0.95x | 1.15x |
| valkey-9.1.2 alone | 1.20x | 0.94x | 0.92x | 1.30x |
| valkey-9.1.2 with a replica | 1.30x | 0.97x | 1.02x | 1.39x |
