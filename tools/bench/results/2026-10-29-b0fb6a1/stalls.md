stalls while filling 10,000,000 keys, 100 SETs a batch, one connection:

| server | keys/s | batch p50 (ms) | p99.9 (ms) | max (ms) | batches > 10 ms | slowest |
|---|---:|---:|---:|---:|---:|---|
| ours | 638,890 | 0.09 | 0.51 | 1.4 | 0 | 1 ms at 1.92M, 1 ms at 2.10M, 1 ms at 0.68M |
| redis-7.0.15 | 486,333 | 0.12 | 0.75 | 29.5 | 2 | 29 ms at 1.11M, 10 ms at 2.21M, 2 ms at 4.61M |
| old | 537,352 | 0.10 | 0.53 | 1416.5 | 10 | 1416 ms at 7.34M, 701 ms at 3.67M, 327 ms at 1.83M |
| redis-8.10.2 | 528,018 | 0.13 | 1.17 | 4.3 | 0 | 4 ms at 3.25M, 4 ms at 3.24M, 3 ms at 2.18M |
| valkey-9.1.2 | 531,759 | 0.12 | 0.63 | 36.4 | 3 | 36 ms at 1.02M, 17 ms at 2.11M, 13 ms at 3.27M |
