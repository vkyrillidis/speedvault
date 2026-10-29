# Run a cluster

**With Docker Compose** (`docker-compose.yml`): stuart and three nodes;
stuart splits the slots as soon as all three have joined
(`--create-when 3`), keeps them even (`--auto`), and keeps its state on a
volume.

```sh
docker compose up -d --build
docker compose run --rm cli redis-cli -c -h node1 -p 7001 SET foo bar
docker compose run --rm cli redis-cli -h stuart -p 7600 STUART NODES   # or redis-cli -p 7600 from the host
docker compose stop node3   # node3 leaves: its slots and keys move to node1 and node2, then it exits
docker compose start node3  # back (a new node): it gets its share again
docker compose down -v      # all nodes leave at once: a shutdown; -v also drops stuart's state
```

The nodes have a 5 minute grace period to leave (`stop_grace_period`),
and stop before stuart (`depends_on`), which they need for it.

The nodes announce their service names (`node1:7001`...): stuart and any
client on the Compose network reach them there (`cli` is Redis 7.0.15's
`redis-cli` and `redis-benchmark`). A client on the host would be
redirected to names it can't resolve; for that, give the nodes host
networking or `--announce` addresses the host can reach.
`tools/compat/compose_smoke.sh` brings the file up and checks it (in CI
too, on the image just built).

**On Kubernetes**: see [Kubernetes](kubernetes.md).

**By hand**, on one machine:

```sh
speedvault-stuart --state ./stuart.state --create-when 3 &
for p in 7001 7002 7003; do speedvault-server 127.0.0.1:$p --cluster --stuart 127.0.0.1:7600 & done
redis-cli -p 7600 STUART NODES      # three nodes, up, acked=1
redis-cli -c -p 7600 SET foo bar    # stuart redirects to the owner of foo's slot
```

**With a password**, every node and stuart take the same one; the nodes
log in to stuart and to each other with it, stuart to the nodes, and
clients to both:

```sh
echo 's3cret' > ./password                     # one line end at the end is dropped
speedvault-stuart --state ./stuart.state --create-when 3 --requirepass-file ./password &
for p in 7001 7002 7003; do speedvault-server 127.0.0.1:$p --cluster --stuart 127.0.0.1:7600 --requirepass-file ./password & done
REDISCLI_AUTH=s3cret redis-cli -c -p 7600 SET foo bar
```

A node with another password can't join (its log says why). In cluster
mode `CONFIG SET requirepass` is refused: to change the password,
restart every node and stuart with the new one (until they all have it,
the old and the new can't reach each other).

Without `--create-when`, run `redis-cli -p 7600 STUART CREATE` once the
nodes have joined. On several machines, each node gets `--announce` with
the address clients reach it at, and `--stuart` with stuart's address
(a name is resolved again on every try). Clients take any node, or stuart,
as their seed: `redis-cli -c`, redis-py `RedisCluster`, or any Redis
Cluster client; Lettuce takes nodes only (it wants a `myself` line in its
seed's `CLUSTER NODES`, and stuart is no node).

**When a node dies**, its keys are gone (no persistence or replicas until
phase 8). Restarted at the same address it has a new id; stuart replaces
the old node, and the slots that node owned become unassigned (`STUART
NODES`: `unassigned slots: N`; those keys get `-CLUSTERDOWN Hash slot not
served`). Give them back to the restarted node:

```sh
redis-cli -p 7600 STUART TABLE                     # the old node's range, e.g. 10923-16383
redis-cli -p 7600 STUART NODES                     # the new node's id
redis-cli -p 7600 STUART ASSIGN 10923 16383 <new id>
```

**Adding and removing nodes** (phase 5): start the new node with
`--stuart`, wait until `STUART NODES` has it `up`, then `redis-cli -p 7600
STUART REBALANCE`; the slots move while clients go on (`STUART
MIGRATIONS` shows them; empty when done). To remove a node, `STUART DRAIN
<id>`, wait until `STUART MIGRATIONS` is empty and the node has `slots=0`,
then stop it. Or `STUART LEAVE <id>`, which drains the node and removes
it from the table: stop it once `STUART LEAVE <id>` replies `0`. With
`stuart --auto` a node that joins gets its share by itself, and slots
left unassigned by a node that restarted go out again. Phase 6 does all
of it by itself on Kubernetes.

**Clients while slots move** (`tools/compat/migrate_check.sh`, see
`tools/compat/README.md`): a client that refreshes its slot map on
`-MOVED` stops being redirected as soon as the moves end. redis-py does.
Lettuce 7 does by default but at most once in 5 s (redirected for 5–8 s
after a move ends; 1–3 s with the settings below), so give it a shorter
limit and a periodic refresh:

```java
ClusterTopologyRefreshOptions refresh = ClusterTopologyRefreshOptions.builder()
        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(1))   // the triggers (-MOVED, -ASK...) are on by default
        .enablePeriodicRefresh(Duration.ofSeconds(10))
        .build();
client.setOptions(ClusterClientOptions.builder().topologyRefreshOptions(refresh).build());
```

With the adaptive triggers off (Lettuce 6's default) it follows every
`-MOVED` and never learns the new layout: a redirect per command for a
moved slot, for good. redis-py 8.1.0 fails a command sent `-ASK` to a node
it doesn't know yet (`AttributeError`, a redis-py bug that Redis Cluster
trips too), once or twice right after a node is added; retry it.

**Keep stuart's state file**: a stuart that lost it starts again at epoch
1, and the nodes refuse tables older than the one they have.
