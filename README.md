# SpeedVault

A Redis-compatible key-value store, written in Rust, that **autoscales
itself on Kubernetes**: a HorizontalPodAutoscaler adds and removes nodes,
and their keys move between them while clients keep reading and writing.

- **Redis-compatible.** It speaks RESP2 and RESP3 and the Redis Cluster
  protocol, with Redis 8.10.2's replies and error texts byte for byte
  (checked by a differential test against a real Redis). Stock clients work
  unchanged: `redis-cli`, `redis-benchmark`, redis-py, Lettuce, and any
  Redis Cluster client. See [compatibility](#compatibility-with-redis) for
  what's there and what isn't yet.
- **Faster than Redis on one core**: against Redis 8.10.2, 1.3–1.4× on
  strings without pipelining and 1.3–2.7× at `-P 16`; 1.2–1.4× and 1.4–1.9×
  on hashes, sets and lists; ahead of Valkey 9.1.2 by as much or more
  ([performance](docs/performance.md)).
- **Replicas and failover**: a replica per primary, synced without a fork;
  a primary that dies is replaced by its replica within seconds, fenced so
  that the two never both take writes; `WAIT` for writes that must survive.
- **Live slot migration** with no write lost: the target copies the slots
  while the source keeps serving them, then takes them over after a short
  pause of their writes; clients see one `-MOVED`, never `-ASK`.
- **Small**: 64 bytes for a small string key (Redis 8: 71, Valkey 9: 76),
  and no stall when the keyspace grows.
- **stuart, the steward**: one small process that assigns slots, moves them
  when nodes join or leave, and keeps the cluster even. Never in the data
  path.
- **Made for Kubernetes**: one-core pods; on SIGTERM a primary hands its
  slots to its replica (a rolling update moves no key); memory and CPU
  metrics for the HPA; Kustomize manifests with a replica per primary.

Redis-compatible stores today need someone to reshard them, and stores that
rebalance themselves don't speak Redis. SpeedVault does both.

**A good fit** for a cache or session store, counters, queues on lists, or
any data set that has to grow and shrink with its load, where the data can
live in memory and survive as replicas rather than on disk. **Not a fit
(yet)** if you need sorted sets, pub/sub, Lua scripts or TLS, or the data
must outlive every copy of it in memory: there are no RDB or AOF files.

## Quick start

One node, as a Redis server on port 7000:

```sh
cargo build --release
target/release/speedvault-server 7000              # or: docker build -t speedvault . && docker run -p 7000:7000 speedvault
# or the released image: docker run -p 7000:7000 registry.gitlab.com/speedvault/speedvault:0.1.0
redis-cli -p 7000 SET greeting hello
redis-cli -p 7000 GET greeting
```

A cluster of three nodes and stuart with Docker Compose:

```sh
docker compose up -d --build
docker compose run --rm cli redis-cli -c -h node1 -p 7001 SET foo bar
docker compose run --rm cli redis-cli -h stuart -p 7600 STUART NODES
docker compose stop node3     # node3 leaves: its slots and keys move to the others
```

On Kubernetes:

```sh
kubectl create namespace db
kubectl -n db apply -k deploy/kubernetes/base          # set the image in an overlay of your own
kubectl -n db scale statefulset speedvault --replicas 5
```

See [running a cluster](docs/cluster.md) and [Kubernetes](docs/kubernetes.md)
(autoscaling with the HPA included).

With a password (a file, so that it stays out of `ps`; clients log in as
with Redis):

```sh
target/release/speedvault-server 7000 --requirepass-file /run/secrets/speedvault
REDISCLI_AUTH="$(cat /run/secrets/speedvault)" redis-cli -p 7000 PING
```

From code, use any Redis client: redis-py's `redis.Redis(port=7000)`, or
`redis.cluster.RedisCluster(host="node1", port=7001)` for a cluster.

## Compatibility with Redis

| Redis feature | SpeedVault |
|---------------|------------|
| RESP2, RESP3 (`HELLO`), inline commands, pipelining | Yes |
| Strings, integers, floats | Yes; not yet `GETRANGE SETRANGE GETEX GETSET SETEX PSETEX MSETNX` |
| Keys and expiry | Yes; not yet `RENAME UNLINK COPY TOUCH RANDOMKEY` |
| Hashes, sets, lists | Yes, packed as Redis packs them; not yet blocking pops (`BLPOP`...), hash field expiry (`HEXPIRE`...) |
| Sorted sets, streams, bitmaps, HyperLogLog, geo | Not yet |
| Transactions (`MULTI EXEC WATCH`) | Yes |
| Pub/sub, scripting (`EVAL`, functions) | Not yet |
| `requirepass` and `AUTH` | Yes; ACL users not yet |
| TLS | Not yet |
| Redis Cluster protocol (redirects, `CLUSTER SLOTS SHARDS NODES`) | Yes; the layout is stuart's: no `CLUSTER ADDSLOTS MEET FAILOVER`, `redis-cli --cluster` |
| Replicas, `WAIT`, reads from replicas | Yes, paired by stuart; no `REPLICAOF` |
| Persistence (RDB, AOF) | No, by design: replicas keep the data |
| Eviction | `noeviction` only (`--maxmemory`) |
| Databases | 0 only |
| Modules (JSON, search, vector sets) | No |

A differential test suite (`tools/compat`) sends the same requests to
SpeedVault and to Redis 8.10.2 and compares the replies byte for byte; it
also lists the few deliberate differences.

### Commands

- **Strings and integers:** `GET SET` (with `EX PX EXAT PXAT NX XX KEEPTTL
  GET IFEQ IFNE IFDEQ IFDNE`) `MGET MSET MSETEX SETNX GETDEL DELEX DIGEST
  APPEND STRLEN INCR DECR INCRBY DECRBY INCRBYFLOAT INCREX` (floats in
  Redis's 80-bit `long double`, digit for digit).
- **Keys:** `DEL EXISTS TYPE EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL
  EXPIRETIME PEXPIRETIME PERSIST SCAN KEYS DBSIZE FLUSHDB FLUSHALL`,
  `OBJECT ENCODING`.
- **Hashes:** `HSET HMSET HSETNX HGET HMGET HDEL HEXISTS HLEN HSTRLEN HKEYS
  HVALS HGETALL HINCRBY HINCRBYFLOAT HRANDFIELD HSCAN`.
- **Sets:** `SADD SREM SISMEMBER SMISMEMBER SCARD SMEMBERS SPOP SRANDMEMBER
  SMOVE SUNION SINTER SDIFF SUNIONSTORE SINTERSTORE SDIFFSTORE SINTERCARD
  SUNIONCARD SDIFFCARD SSCAN`.
- **Lists:** `LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LLEN LRANGE LINDEX LSET
  LINSERT LREM LTRIM LPOS LMOVE LMOVEM RPOPLPUSH LMPOP`.
- **Connection and server:** `PING ECHO HELLO AUTH SELECT 0 QUIT CLIENT
  COMMAND INFO`, `CONFIG GET` and `SET`.
- **Transactions:** `MULTI EXEC DISCARD WATCH UNWATCH`, as Redis's: the
  commands of a transaction run back to back, nothing else in between,
  and reach the replicas whole; `WATCH` sees every write that changes a
  key, its expiry, `FLUSHALL`. In a cluster, a transaction's keys are in
  one slot (hash tags), as in Redis Cluster.
- **A password:** `--requirepass-file PATH` (or `--requirepass`), Redis's
  `requirepass`: clients `AUTH` (or `HELLO 3 AUTH default ...`) first. A
  cluster has one: every node and stuart take it, log in to each other
  with it, and stuart's port requires it too; `/metrics` and the probes
  stay open. There are no ACL users.
- **Cluster:** `CLUSTER SLOTS SHARDS NODES INFO MYID MYSHARDID KEYSLOT COUNTKEYSINSLOT
  GETKEYSINSLOT`, `ASKING READONLY READWRITE`, with `-MOVED`, `-ASK` and
  `-TRYAGAIN` redirects as Redis Cluster sends them.
- **Replication:** `WAIT`, `ROLE`, `INFO replication`, reads from replicas
  after `READONLY`; replicas listed in `CLUSTER SLOTS SHARDS NODES`.
- **Memory:** `--maxmemory` with Redis's `noeviction` (`-OOM` on writes past
  it).

There is no persistence by design: replicas keep the data, and a pod
stopping hands over to its replica first.

## How it works

Each node is one thread: an event loop (epoll, or kqueue on macOS) runs a
stackful fiber per connection, which reads like blocking code and suspends
while it waits for I/O. Replies to pipelined requests go out in batches.
In cluster mode each node owns slots of the keyspace, as in Redis Cluster;
stuart pushes the slot table to every node. A replica, or a node taking
over slots, copies them from their owner as a snapshot by slots followed by
the writes made meanwhile, so nothing forks and nothing stops serving. See
[architecture](docs/architecture.md) and the [design notes](docs/design/).

| Crate | What it is |
|-------|------------|
| `fiber` | Stackful coroutines with a context switch in assembly (x86_64, aarch64) |
| `reactor` | The event loop, timers and non-blocking TCP for fibers |
| `resp` | Incremental RESP2/RESP3 parser and reply encoder |
| `store` | The keyspace: a table of one-block keys per slot; strings, hashes, sets, lists, expiry |
| `cluster` | Hash slots, the slot table, Redis Cluster's replies |
| `dbnode` | The node: `speedvault-server` |
| `stuart` | The cluster's steward: `speedvault-stuart` |

## Documentation

- [Running a cluster](docs/cluster.md): stuart, nodes joining and leaving, moving slots, clients.
- [Kubernetes](docs/kubernetes.md): the manifests, replicas, autoscaling, what happens on scale-down and on a lost machine.
- [Architecture](docs/architecture.md): each crate and the node's and stuart's options.
- [Performance](docs/performance.md): benchmarks against Redis 7.0.15, Redis 8.10.2 and Valkey 9.1.2, memory.
- [Design notes](docs/design/): the decisions behind each part.
- [Compatibility tools](tools/compat/README.md) and [benchmarks](tools/bench/README.md).

## Building and testing

Linux or macOS, with Rust 1.92 or later:

```sh
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
```

On Windows, build inside WSL2 (with `CARGO_TARGET_DIR` inside the Linux
file system: much faster than a target directory on a Windows drive). The
compatibility suites need a Redis 8.10.2 `redis-server`
(`tools/compat/build_redis.sh` builds one) and redis-py; see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Status

Working and tested, not yet used in production. Phases 1–8 are done (see
the [design notes](docs/design/)): replicas and failover included; stuart
itself isn't replicated yet (while it's down, writes go on but the layout
doesn't change). Issues and merge requests are welcome on
[GitLab](https://gitlab.com/speedvault/speedvault), the project's home;
[GitHub](https://github.com/vkyrillidis/speedvault) is a read-only mirror.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
