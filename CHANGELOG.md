# Changelog

## 0.1.0 (unreleased)

The first public release.

- A Redis-compatible node (RESP2, RESP3, inline commands): strings and
  integers, hashes, sets, lists, expiry, `SCAN`, `--maxmemory` with
  `noeviction`; replies and `OBJECT ENCODING` checked byte for byte
  against Redis 8.10.2, `INCRBYFLOAT` and `HINCRBYFLOAT` digit for digit in
  Redis's 80-bit `long double`.
- Redis 8's new string, set and list commands: `MSETEX`, `DELEX`,
  `INCREX`, `DIGEST` (XXH3), `SET ... IFEQ/IFNE/IFDEQ/IFDNE`,
  `SUNIONCARD` and `SDIFFCARD` (HyperLogLog estimates, as Redis's),
  `LMOVEM`; `CLIENT SETINFO`, `CLIENT NO-TOUCH`.
- The keyspace: a table per slot of keys that are one heap block each
  (64 bytes a small string key); small hashes, sets and lists packed as
  Redis packs them, large ones one block per element.
- Cluster mode with the Redis Cluster protocol (slots, `-MOVED`, `-ASK`,
  `-TRYAGAIN`, `CLUSTER SLOTS/SHARDS/NODES/MYSHARDID`; shards by shard
  id, as Redis 7.2+ keeps them through a failover).
- Replicas (`--replicas N`, `--zone`): synced by a snapshot by slots then
  the writes, with no fork; a backlog for short drops; `WAIT`, `ROLE`,
  `INFO replication`, reads after `READONLY`; a 3 s write lease renewed by
  the replica's acknowledgements.
- Failover: stuart promotes the replica of a primary down for
  `--fail-after`, after the lease has fenced the old primary off; a
  primary stopping on SIGTERM hands its slots to its replica, moving no
  key.
- stuart, the steward: nodes join it; it assigns slots and roles, moves
  slots live (`STUART MOVE`, `REBALANCE`, `DRAIN`, `--auto`), and keeps its
  state on disk.
- Live slot migration by snapshot plus the writes made meanwhile: no key
  frozen, no write lost, one `-MOVED` per slot.
- Kubernetes: Prometheus metrics, Kustomize manifests with a replica per
  primary, autoscaling with the HorizontalPodAutoscaler by pairs of pods.
