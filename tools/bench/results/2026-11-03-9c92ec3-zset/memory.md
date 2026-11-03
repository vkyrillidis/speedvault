| collection | redis-7.0.15 | speedvault | redis-8.10.2 | valkey-9.1.2 |
|---|---|---|---|---|
| hash, 10000 x 100 | 21.2 (listpack) | 21.8 (listpack) | 20.9 (listpack) | 20.9 (listpack) |
| set, 10000 x 100 | 59.6 (hashtable) | 12.5 (listpack) | 13.2 (listpack) | 13.2 (listpack) |
| intset, 10000 x 100 | 2.9 (intset) | 5.5 (intset) | 2.7 (intset) | 2.6 (intset) |
| list, 10000 x 100 | 11.9 (quicklist) | 13.4 (listpack) | 10.8 (listpack) | 10.7 (listpack) |
| zset, 10000 x 100 | 16.1 (listpack) | 16.7 (listpack) | 15.8 (listpack) | 15.7 (listpack) |
| zset-random, 10000 x 100 | 16.1 (listpack) | 16.7 (listpack) | 15.8 (listpack) | 15.7 (listpack) |
| hash, 1 x 1000000 | 72.4 (hashtable) | 41.3 (hashtable) | 38.1 (hashtable) | 50.9 (hashtable) |
| set, 1 x 1000000 | 56.4 (hashtable) | 40.8 (hashtable) | 30.1 (hashtable) | 42.9 (hashtable) |
| intset, 1 x 1000000 | 48.4 (hashtable) | 40.8 (hashtable) | 22.1 (hashtable) | 34.9 (hashtable) |
| list, 1 x 1000000 | 10.1 (quicklist) | 10.2 (quicklist) | 10.1 (quicklist) | 10.1 (quicklist) |
| zset, 1 x 1000000 | 109.7 (skiplist) | 58.5 (skiplist) | 67.4 (skiplist) | 70.8 (skiplist) |
| zset-random, 1 x 1000000 | 109.8 (skiplist) | 65.5 (skiplist) | 67.4 (skiplist) | 70.8 (skiplist) |
