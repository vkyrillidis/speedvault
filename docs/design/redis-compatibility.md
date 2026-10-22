# Redis compatibility

The node speaks the Redis protocol well enough that stock clients
(`redis-cli`, `redis-benchmark`, redis-py, Lettuce, ...) work against it
unchanged. Redis 7.0.15 is the reference: replies and error texts match it
byte for byte, which the differential tests check (`tools/compat`).

Decisions:

- **The protocol is Redis 7's:** RESP2 by default, RESP3 after `HELLO 3`,
  inline commands, case-insensitive command names, Redis's error messages.
  The parser is incremental: partial reads, pipelining, size limits and
  binary-safe values, with Redis's protocol errors (`resp` crate).
- **Copy Redis's behaviour, including the odd parts clients can see**
  (reply order of small hashes and sets, `OBJECT ENCODING`, which keys an
  `SCAN` returns), but not the accidents of its C code. The few deliberate
  differences are allow-listed in `tools/compat/allowlist.py`: only
  database 0 exists (`SELECT 0`), and a NUL byte in an inline command is an
  ordinary byte.
- **Unknown commands get an error reply, never a closed connection.**

Built in steps:

| # | Step | Content |
|---|------|---------|
| 1 | `resp` crate | Incremental request parser, reply encoder for RESP2 and RESP3: 600 random streams byte-identical to Redis; parsing a GET takes 12.5 ns |
| 2 | `store` crate | The keyspace: strings, integers (`INCR`...), expiry (lazy plus active), owned by one thread, no locks; a GET hit ~250 ns with 1M keys |
| 3 | `dbnode` binary | One fiber per connection, pipelined replies flushed in batches, the command table, idle timeouts (`--timeout`); a panicking connection is closed and logged without stopping the server |
| 4 | Compatibility | Differential tests: the same requests (split, pipelined, malformed) against Redis and the node, comparing replies byte for byte; redis-py 8.1.0 (RESP2 and RESP3), `redis-cli` (also `-3`) and `redis-benchmark` work against the node |
| 5 | Benchmarks | `redis-benchmark` against Redis and the node, with and without pipelining (see [performance](../performance.md)) |

What real clients send on connect (captured from Redis, see
`tools/compat/README.md`), and so what the node must get right:

- **redis-py 8** sends `HELLO 3` and fails to connect unless it gets a RESP3
  map back; then `CLIENT MAINT_NOTIFICATIONS ...` and `CLIENT SETINFO ...`
  (an error reply is fine); `CLIENT SETNAME` / `SELECT` when configured.
- **redis-benchmark** sends `CONFIG GET save` and `CONFIG GET appendonly`
  (answer like Redis to avoid a warning), then inline `PING`, `SET`, `GET`,
  `INCR`, and `MSET` with 10 pairs.
- **redis-cli** sends `HELLO 3` with `-3`, and `COMMAND DOCS` when
  interactive (an error reply makes it fall back).

Being faster than Redis comes in two steps: on one core, through cheaper I/O
(fibers, edge-triggered epoll, batched writes; see
[performance](../performance.md) for how the first run, 0.84–0.94× Redis
without pipelining, became 1.4–1.5×); on many cores, through more one-core
nodes in a cluster, which a single Redis can't do.
