# Changelog

## Unreleased

- Sorted sets: a fifth type, packed while Redis keeps a listpack (128
  members of 64 bytes), a counted B+ tree beside a member table past that;
  the single-key commands (`ZADD` with every option, `ZRANGE` by rank,
  score and member, `ZRANGESTORE`, `ZPOPMIN`, `ZRANDMEMBER`, `ZSCAN`...);
  scores read and written as Redis 8.10.2 does (its `fast_float` and
  `fpconv` ported), compared byte for byte; the multi-key ones (`ZUNION`
  `ZINTER` `ZDIFF` and their `STORE` forms with `WEIGHTS` and `AGGREGATE`,
  `ZINTERCARD`, `ZMPOP`), routed by all their keys in a cluster.
- `HSCAN ... NOVALUES`.

## 0.1.0 (2026-10-31)

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
- Transactions, Redis 8.10.2's: `MULTI`, `EXEC`, `DISCARD`, `WATCH`,
  `UNWATCH`; each command checked as queued, `EXEC` as one command of
  their keys (`-EXECABORT`, `-CROSSSLOT`, `-MOVED`), run with nothing in
  between, in the replication stream between `MULTI` and `EXEC` (a
  replica applies it whole); `WATCH` touched as Redis touches it
  (`signalModifiedKey`), expiry, `FLUSHALL` and a slot leaving included;
  `CLIENT LIST`'s `multi`, `watch` and flags `x`, `d`, `r`; `INFO`'s
  `watching_clients`, `total_watched_keys`.
- A password (`--requirepass-file`, `--requirepass`), Redis 8.10.2's
  `requirepass`: `AUTH`, `HELLO ... AUTH`, `-NOAUTH`, `CONFIG GET/SET
  requirepass`, `acl_access_denied_auth` (and
  `speedvault_auth_failures_total`). One for a cluster: the nodes and
  stuart log in to each other with it, and stuart's port requires it;
  `components/auth` reads it from a Kubernetes Secret.
- Kubernetes: Prometheus metrics, Kustomize manifests with a replica per
  primary, autoscaling with the HorizontalPodAutoscaler by pairs of pods.
