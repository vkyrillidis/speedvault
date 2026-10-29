strings: 1,000,000 keys

| server | used_memory (MB) | B per key | RSS (MB) | replica used_memory (MB) | replica RSS (MB) | used_memory / ours |
|---|---|---|---|---|---|---|
| ours | 209 | 207 | 228 | 209 | 227 | 1.00x |
| Redis 7.0.15 | 123 | 122 | 147 | 123 | 147 | 0.59x |
| Redis 7.0.15 cluster | 139 | 138 | 163 | 139 | 162 | 0.67x |
| redis-8.10.2 | 72 | 71 | 91 | 72 | 91 | 0.35x |
| redis-8.10.2 cluster | 81 | 78 | 94 | 81 | 95 | 0.39x |
| valkey-9.1.2 | 77 | 76 | 93 | 77 | 94 | 0.37x |
| valkey-9.1.2 cluster | 79 | 76 | 94 | 79 | 94 | 0.38x |

collections: 30,020 keys, 5,000,000 elements

| server | used_memory (MB) | B per element | RSS (MB) | replica used_memory (MB) | replica RSS (MB) | used_memory / ours |
|---|---|---|---|---|---|---|
| ours | 143 | 28 | 158 | 143 | 157 | 1.00x |
| Redis 7.0.15 | 238 | 47 | 263 | 238 | 263 | 1.67x |
| Redis 7.0.15 cluster | 240 | 48 | 264 | 240 | 264 | 1.68x |
| redis-8.10.2 | 129 | 26 | 143 | 129 | 143 | 0.90x |
| redis-8.10.2 cluster | 132 | 26 | 145 | 132 | 146 | 0.92x |
| valkey-9.1.2 | 123 | 24 | 138 | 123 | 138 | 0.87x |
| valkey-9.1.2 cluster | 127 | 25 | 140 | 127 | 140 | 0.89x |
