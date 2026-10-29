Killed (SIGKILL):

| Server | timeout (s) | promoted after (s) | client writes again after (s) | writes lost |
|---|---|---|---|---|
| ours | 2 | 5.9 | 6.0 | 0, 0, 0 |
| Redis 7.0.15 | 2 | 3.7 | 3.7 | 0, 0, 0 |
| redis-8.10.2 | 2 | 3.7 | 3.7 | 0, 0, 0 |
| valkey-9.1.2 | 2 | 3.7 | 3.7 | 0, 0, 0 |
| ours | 5 | 8.9 | 8.9 | 0, 0, 0 |
| Redis 7.0.15 | 5 | 8.5 | 8.5 | 0, 0, 0 |
| redis-8.10.2 | 5 | 8.0 | 8.0 | 0, 0, 0 |
| valkey-9.1.2 | 5 | 8.9 | 8.9 | 0, 0, 0 |

Leaving on purpose (ours: SIGTERM; theirs: CLUSTER FAILOVER at the replica), timeout 5 s:

| Server | promoted after (s) | client's longest wait (s) | writes lost |
|---|---|---|---|
| ours | 0.03 | 0.009 | 0, 0, 0 |
| Redis 7.0.15 | 0.05 | 0.048 | 0, 0, 0 |
| redis-8.10.2 | 0.06 | 0.056 | 0, 0, 0 |
| valkey-9.1.2 | 0.07 | 0.067 | 0, 0, 0 |
