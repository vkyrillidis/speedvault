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

**Passwords** (`AUTH`, after the Redis 8.10.2 milestone) copy Redis
8.10.2's `requirepass`: the order of the checks (unknown command and
arity before `-NOAUTH`; in `HELLO`, the version, then each option, a bad
name before the password, then the login), `AUTH`'s replies with and
without a password (`default` is the one user, case-sensitive; the
internal-connection user name gets Redis's errors), the 10-argument and
16 KiB limits on requests before the login, and that changing the
password keeps clients logged in. Deliberately different:

- A failed `AUTH` always gets `-WRONGPASS`. Redis 8.10.2 sends no reply at
  all to one while replies are pending (`addAuthErrReply` returns if
  `clientHasPendingReplies`): a client pipelining it gets one reply too few,
  and every later reply answers the wrong request.
- In cluster mode `CONFIG SET requirepass` is refused: the password is the
  cluster's (the nodes and stuart log in to each other with it), set when
  they start.
- No `ACL`, no users but `default`, no `RESET`; `INFO stats` has only
  `acl_access_denied_auth` of the ACL counters.
- Redis 8.10.2 never answers a bad bulk length whose line ends in a later
  read (an unauthenticated one too); we reply its error, as 7.0.15 does.

**Transactions** (`MULTI`/`EXEC`, after `AUTH`) copy Redis 8.10.2's: the
checks while queueing (an unknown command, an arity error, a refused
slot, `-OOM` flag the transaction, which `EXEC` then refuses with
`-EXECABORT`), `EXEC` checked as one command of all the queued keys
(`CROSSSLOT`, `MOVED`, `TRYAGAIN` discard it, `-OOM` and `-NOREPLICAS`
abort it with `-EXECABORT ... because of: ...`), errors while running as
replies in the array, `ASKING` holding until `EXEC`, `WAIT` not waiting.
`WATCH` breaks exactly where Redis's `signalModifiedKey` is called: a
write that did something, even the same value again (`SET`, `HSET`,
`INCRBY 0`, `APPEND ""`, `LSET`, `LTRIM`), not one that did nothing
(`SADD` of a member there, a `SET NX` that lost, `DEL` of a missing key,
`LREM` without a match, `PERSIST` without a TTL); a key expiring, unless
it had already expired when watched; `FLUSHALL` only for a key that was
there. The difftest corpus has each of our write commands both ways
against Redis. No `RESET` (not implemented), and Redis's queueing memory
shortcut (an aborted transaction keeps nothing more) shows only in
`CLIENT LIST`'s `multi`, as in Redis.

Being faster than Redis comes in two steps: on one core, through cheaper I/O
(fibers, edge-triggered epoll, batched writes; see
[performance](../performance.md) for how the first run, 0.84–0.94× Redis
without pipelining, became 1.4–1.5×); on many cores, through more one-core
nodes in a cluster, which a single Redis can't do.
