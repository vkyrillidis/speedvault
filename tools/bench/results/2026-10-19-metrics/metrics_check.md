| busy-poll | load | requests | wall s | CPU s | busy s | busy / CPU | busy per request |
|---|---|---|---|---|---|---|---|
| 0 us | 50 clients | 2000004 | 7.3 | 4.64 | 4.41 | 0.95 | 2.20 us |
| 0 us | 1 clients | 200004 | 16.5 | 7.18 | 6.24 | 0.87 | 31.20 us |
| 20 us | 50 clients | 2000004 | 5.2 | 4.56 | 3.48 | 0.76 | 1.74 us |
| 20 us | 1 clients | 200004 | 9.5 | 8.21 | 5.16 | 0.63 | 25.80 us |

| test | without --metrics | with, scraped every second | ratio |
|---|---|---|---|
| SET -P 1 | 392,465 | 389,257 | 0.99 |
| SET -P 16 | 5,154,639 | 5,102,041 | 0.99 |
| GET -P 1 | 382,117 | 384,172 | 1.01 |
| GET -P 16 | 5,263,158 | 5,181,347 | 0.98 |

33 scrapes
OK
