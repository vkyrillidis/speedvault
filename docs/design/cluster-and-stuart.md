# A cluster, and stuart (phase 4)

The feature that matters most is **live autoscaling**: nodes are added and
removed, and their keys move between them while traffic flows. Phases 4–6
build it; phase 4 lays the base.

Decisions (2026-10-17):

- **No proxy in the data path.** A router in front of the nodes adds a hop
  to every request, needs about as many cores as the nodes, and is a
  bottleneck and a single point of failure unless it's run as a fleet.
  Instead, as in Redis Cluster, **clients send each request straight to the
  node that owns its key**.
- **Speak the Redis Cluster protocol.** A key's slot is
  `CRC16(key) mod 16384`, hashing only the `{tag}` part when there is one,
  exactly as Redis does. Each node owns ranges of slots, and redirects a
  request for a slot it doesn't own with `-MOVED slot host:port`. Clients
  learn the layout with `CLUSTER SLOTS` / `SHARDS` / `NODES`, which every
  node answers. So every Redis Cluster client works unchanged: redis-py
  `RedisCluster`, Jedis, Lettuce, go-redis, ioredis, `redis-cli -c`,
  `redis-benchmark --cluster`. Our own protocol (our own key ranges, our
  own clients) was the alternative; it would have meant writing and
  maintaining a client per language, and no drop-in replacement for Redis.
- **stuart, the steward, is the brain.** A separate process, and the only
  one that changes the layout. Nodes join it (`--stuart host:port`); it
  assigns slots to them, pushes the slot table (with an epoch, a version
  number that only grows) to every node, and later moves slots between
  nodes (phase 5) and decides when to, from load (phase 6). It is never in
  the data path: if stuart is down, traffic carries on and only changes to
  the layout wait. It also answers `CLUSTER SLOTS` / `SHARDS` / `NODES`, so a
  client can be given stuart's address as its seed.
- **The nodes are the authority on what they own.** A node serves only the
  slots the latest table it was given says it owns, and ignores a table
  with an older epoch. A client or node with an outdated view of the others
  costs one more redirect, never a wrong answer.
- **Multi-key commands follow Redis Cluster:** all keys of `MGET MSET DEL
  EXISTS` must be in one slot, or the reply is `-CROSSSLOT Keys in request
  don't hash to the same slot`. Hash tags (`{user42}:name`,
  `{user42}:email`) put related keys in one slot. `DBSIZE` and `FLUSHDB`
  act on the node they're sent to, as in Redis Cluster.
- **Standalone stays the default.** Without `--cluster` the node is a
  single Redis-compatible server. Cluster mode is a flag, as
  `cluster-enabled` is in Redis.
- **Scale out, one core per node.** Growing while traffic flows needs
  more machines, not a bigger one, so the cluster and live migration come
  first. A node stays single-threaded: on Kubernetes a pod with one core is
  the unit a HorizontalPodAutoscaler adds and removes. A multi-core node
  (Dragonfly's approach) makes one machine faster but can't grow past it;
  it was optional and last, and was dropped on 2026-10-20 (on Kubernetes
  more one-core pods do the same).
- **Kubernetes decides how many nodes, stuart decides which slots each
  owns.** A new pod registers with stuart and gets slots moved to it; a pod
  being removed drains its slots on SIGTERM before it exits (phase 6).

How a cluster starts (phase 4):

1. Start stuart: `stuart [PORT] --state FILE`.
2. Start the nodes: `speedvault-server 7000 --cluster --announce 10.0.0.2:7000
   --stuart 10.0.0.1:7600` (`--announce` is the address clients reach the
   node at, as with Redis's `cluster-announce-ip`; it defaults to the
   listen address, so it's needed when the node listens on all
   interfaces). Each gets a random 40-character node id (Redis's format) at
   startup, joins stuart (retrying every second until stuart answers) and
   serves no slots yet (`-CLUSTERDOWN Hash slot not served`).
3. `redis-cli -p 7600 STUART CREATE` splits the 16384 slots evenly over the
   nodes that have joined and pushes the table. A node that joins later
   gets none until `STUART REBALANCE` (phase 5) moves some to it.
4. Clients connect to any node, or to stuart, and route by themselves.

stuart's state (the nodes, the table, its epoch; in phase 5 the migrations
in progress) is written to its `--state` file before it acts on it, so a
restarted stuart carries on where it stopped. stuart pings the nodes and
`STUART NODES` reports the ones that don't answer. A node that dies loses its
keys (there's no persistence or replication yet), and on restart it has a
new id and no slots; stuart reports its slots as lost. Replicas and
failover are phase 8.

Node commands in cluster mode: `CLUSTER MYID SLOTS SHARDS NODES INFO KEYSLOT
COUNTKEYSINSLOT`, `READONLY` / `READWRITE` (accepted; replicas hold no
data yet, phase 8),
`INFO cluster` (`cluster_enabled:1`), and `CLUSTER SETTABLE epoch ...`, the
command stuart pushes the table with (ours; Redis has none).

Milestones:

| # | Milestone | Content | State |
|---|-----------|---------|-------|
| 1 | `cluster` crate | `key_hash_slot` (CRC16 and hash tags, checked against Redis's `CLUSTER KEYSLOT` on random keys); the slot table: slot ranges to node id and address, the epoch, validation, lookup; the replies to `CLUSTER SLOTS SHARDS NODES` in RESP2 and RESP3, checked against a real Redis Cluster | done: KEYSLOT matches Redis 7.0.15 on 20,000 random keys (binary, braces); SLOTS and SHARDS byte-identical, NODES identical apart from bus port, ping/pong and epoch, RESP2 and RESP3, 3 layouts; 33 injected bugs all caught by the unit tests |
| 2 | Cluster mode in `dbnode` | `--cluster`, `--announce`; slot checks for every command from the key positions in the command table (`MOVED`, `CROSSSLOT`, `CLUSTERDOWN`); the `CLUSTER` commands, `CLUSTER SETTABLE` (epoch-checked); `INFO cluster`; standalone unchanged | done: our node in a Redis 7.0.15 node's place answers 650 requests (RESP2 and RESP3, 4 placements in 3 layouts) exactly as Redis does, node ids masked; `redis-cli -c` and `redis-cli --cluster check` work across 3 of our nodes; standalone difftest 284/287 pass, 3 allowed; 36 injected bugs all caught |
| 3 | `stuart` binary | RESP server on the reactor; nodes join it and are pinged; `STUART CREATE NODES TABLE`; pushes the table to every node (and again to one that missed it); answers `CLUSTER SLOTS SHARDS NODES`; the state file | done: 3 nodes join, `STUART CREATE`, every node takes the table; a 4th joining later and a 5th joining while stuart is down get in, a stuart restart carries on from its state file; a node that drops connections for 2 s gets the table after; `redis-cli -c` and redis-py 8.1 `RedisCluster` (RESP2 and RESP3) seeded with stuart alone work, `redis-cli --cluster check` agrees; 25 injected bugs all caught |
| 4 | Compatibility | A cluster difftest: the same requests against a 3-node Redis Cluster and our 3 nodes, through a cluster client and to each node directly (same replies, including `MOVED`, `CROSSSLOT` and `CLUSTERDOWN`, apart from node ids and addresses); redis-py `RedisCluster`, `redis-cli -c`, `redis-benchmark --cluster`; stuart restarted mid-run; the standalone difftest unchanged | done: beside a Redis 7.0.15 Cluster with the same layout, 3696/3696 direct replies the same (RESP2 and RESP3), the same redis-py 8.1 `RedisCluster` results seeded with stuart or a node (218/218 per run), `redis-cli -c` 17/17, `redis-benchmark --cluster` without errors, 35k writes with no error while stuart restarts; `STUART ASSIGN` recovers a restarted node's slots; the CI job `cluster-compat` runs it all (checked in the CI image); 14 injected bugs all caught |
| 5 | Benchmarks | `redis-benchmark --cluster` against a 3-node Redis Cluster and our 3 nodes, each node on its own core, `-P 1 16 64`; one node standalone against the same node in cluster mode (the cost of the slot check) | done (results `2026-10-17-7a98a6c`, `2026-10-17-bbf979c`): one redis-benchmark per node, summed: ours 1.18× or more ahead of Redis 7.0.15, Redis 8.10.2 and Valkey 9.1.2 at `-P 1` (rows hit by a clock step aside), 1.25–2.55× pipelined; `--cluster --threads 3` agrees at `-P 1`, coarse with pipelining. Cluster mode costs 4–10% at `-P 64` after a 3.5× faster `key_hash_slot` (was 25–30%), little or nothing below. See [performance](../performance.md) |
| 6 | Packaging | `stuart` options (`--state`, `--loglevel`), the node's `--cluster`, `--stuart` and `--announce`; the Docker image holds both binaries; a Docker Compose file with stuart and 3 nodes; CI runs the cluster compat test | done: stuart's `--create-when N`; one image with both binaries (116 MB, `--entrypoint speedvault-stuart`); `docker-compose.yml` with stuart and 3 nodes that forms a cluster by itself; `compose_smoke.sh` checks it, locally and in CI's image job (checked in the `docker:27` image); "Run a cluster" in this README; the cluster compat job in CI since milestone 4 |

Later phases, in outline:

- **5. Live migration.** See [live slot migration](slot-migration.md).
- **6. Kubernetes autoscaling.** A HorizontalPodAutoscaler changes the
  number of nodes in a StatefulSet; stuart moves the slots. The outline as
  first written; see [Kubernetes autoscaling](autoscaling.md).
  - A node registers with stuart at startup. On SIGTERM it doesn't exit:
    it asks stuart to drain it, its slots move to the others, then it
    exits. `terminationGracePeriodSeconds` is sized for a drain, and a
    PodDisruptionBudget keeps Kubernetes from removing several nodes at
    once. `STUART DRAIN node` drains one for a planned removal.
  - **Scale up automatically, on memory and CPU; scale down by hand.**
    Removing one of N nodes raises everyone's memory by N/(N-1), and memory
    rises and falls with TTLs, so automatic scale-down oscillates. The
    default HPA settings: scale up after a 60 s window, at most 2 nodes per
    5 minutes; `scaleDown.selectPolicy: Disabled`. Automatic scale-down can
    be turned on with a long window (an hour, one node per 30 minutes).
    Scaling down by hand below what memory needs is undone by the HPA,
    which is a safety net.
  - **Metrics for the HPA** (Prometheus, through the Prometheus adapter):
    the node's own busy CPU, not the container's (busy-polling makes an
    idle node look busy); the memory its keys use against `maxmemory`, not
    the container's (the allocator keeps memory after keys expire); ops/s.
  - `maxmemory` and what a node does when it's reached (reject writes, or
    evict like Redis's `maxmemory-policy`). The memory target (70%) leaves
    room to finish moving slots before a node fills up; the source keeps a
    slot's keys until that slot has moved.
  - Manifests or a Helm chart: stuart, the nodes' StatefulSet and Services,
    the HPA above.
  - Tested end to end: load scales 3 nodes to 6, a manual scale back to 3,
    with no write lost and no client error.
- **7. Replicas and failover.** Kubernetes restarts a crashed pod, but its
  keys are gone. A replica per node (a snapshot, then the stream of
  writes); stuart promotes the replica when its primary dies and pushes the
  new table; stuart itself replicated (Raft), so it isn't a single point
  of failure either.
- **8. Multi-core nodes (optional).** Several reactors per node, one per
  core, sharing nothing; multi-key commands across cores stay atomic. For
  deployments that want fewer, bigger nodes; the cluster scales without it.
