strings: 1,000,000 keys

| server | used_memory (MB) | B per key | RSS (MB) | replica used_memory (MB) | replica RSS (MB) | used_memory / ours |
|---|---|---|---|---|---|---|
| ours | 66 | 64 | 77 | 66 | 76 | 1.00x |
| Redis 7.0.15 | 123 | 122 | 148 | 123 | 149 | 1.87x |
| Redis 7.0.15 cluster | 139 | 138 | 163 | 139 | 164 | 2.12x |
| redis-8.10.2 | 72 | 71 | 89 | 72 | 91 | 1.10x |
| redis-8.10.2 cluster | 81 | 78 | 95 | 81 | 95 | 1.23x |
| valkey-9.1.2 | 77 | 76 | 93 | 77 | 94 | 1.17x |
| valkey-9.1.2 cluster | 79 | 76 | 94 | 79 | 94 | 1.20x |

collections: 30,020 keys, 5,000,000 elements

| server | used_memory (MB) | B per element | RSS (MB) | replica used_memory (MB) | replica RSS (MB) | used_memory / ours |
|---|---|---|---|---|---|---|
| ours | 139 | 27 | 154 | 139 | 153 | 1.00x |
| Redis 7.0.15 | 238 | 47 | 263 | 238 | 263 | 1.72x |
| Redis 7.0.15 cluster | 240 | 48 | 264 | 240 | 264 | 1.73x |
| redis-8.10.2 | 129 | 26 | 143 | 129 | 143 | 0.93x |
| redis-8.10.2 cluster | 132 | 26 | 145 | 132 | 148 | 0.95x |
| valkey-9.1.2 | 123 | 24 | 138 | 123 | 138 | 0.89x |
| valkey-9.1.2 cluster | 127 | 25 | 140 | 127 | 140 | 0.91x |
