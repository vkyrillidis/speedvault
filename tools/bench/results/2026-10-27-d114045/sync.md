| server | keys | writes during the sync | sync (s) | primary before (MB) | primary peak (MB) | peak / before | replica peak (MB) | replica after (MB) | replica's keys |
|---|---|---|---|---|---|---|---|---|---|
| ours | 1,000,000 | none | 0.77 | 225 | 225 | 1.00 | 267 | 225 | the same |
| ours | 1,000,000 | SET -c 50 | 0.84 | 225 | 229 | 1.02 | 264 | 225 | the same |
| Redis 7.0.15 | 1,000,000 | none | 0.59 | 138 | 139 | 1.01 | 132 | 132 | the same |
| Redis 7.0.15 | 1,000,000 | SET -c 50 | 0.89 | 138 | 257 | 1.87 | 133 | 133 | the same |
| redis-8.10.2 | 1,000,000 | none | 0.42 | 86 | 87 | 1.02 | 79 | 79 | the same |
| redis-8.10.2 | 1,000,000 | SET -c 50 | 0.72 | 85 | 156 | 1.83 | 88 | 88 | the same |
| valkey-9.1.2 | 1,000,000 | none | 1.25 | 77 | 78 | 1.02 | 74 | 74 | the same |
| valkey-9.1.2 | 1,000,000 | SET -c 50 | 1.27 | 77 | 142 | 1.84 | 85 | 85 | the same |
| ours | 10,000,000 | none | 8.28 | 1,998 | 1,999 | 1.00 | 2,189 | 1,985 | the same |
| ours | 10,000,000 | SET -c 50 | 9.64 | 1,998 | 2,030 | 1.02 | 2,160 | 1,986 | the same |
| Redis 7.0.15 | 10,000,000 | none | 7.99 | 1,356 | 1,357 | 1.00 | 1,288 | 1,288 | the same |
| Redis 7.0.15 | 10,000,000 | SET -c 50 | 11.32 | 1,357 | 2,472 | 1.82 | 1,290 | 1,290 | the same |
| redis-8.10.2 | 10,000,000 | none | 6.57 | 836 | 838 | 1.00 | 745 | 745 | the same |
| redis-8.10.2 | 10,000,000 | SET -c 50 | 9.19 | 833 | 1,606 | 1.93 | 834 | 834 | the same |
| valkey-9.1.2 | 10,000,000 | none | 6.28 | 656 | 657 | 1.00 | 641 | 641 | the same |
| valkey-9.1.2 | 10,000,000 | SET -c 50 | 7.09 | 655 | 1,202 | 1.84 | 653 | 653 | the same |

A 1 s cut, 1,000,000 keys:

| server | writes | written during the cut (MB) | link up again after the cut (s) | resync | replica's keys |
|---|---|---|---|---|---|
| ours | none | 0.0 | 0.00 | partial | the same |
| ours | SET -c 50 | 24.6 | 1.76 | full | the same |
| Redis 7.0.15 | none | 0.0 | 0.41 | partial | the same |
| Redis 7.0.15 | SET -c 50 | 23.6 | 1.47 | full | the same |
| redis-8.10.2 | none | 0.0 | 0.44 | partial | the same |
| redis-8.10.2 | SET -c 50 | 21.9 | 1.21 | full | the same |
| valkey-9.1.2 | none | 0.0 | 0.80 | partial | the same |
| valkey-9.1.2 | SET -c 50 | 23.2 | 2.02 | full | the same |
