# Changelog

## 0.1.0 (unreleased)

The first public release.

- A Redis-compatible node (RESP2, RESP3, inline commands): strings and
  integers, hashes, sets, lists, expiry, `SCAN`, `--maxmemory` with
  `noeviction`; replies checked byte for byte against Redis 7.0.15.
- Small hashes, sets and lists packed as Redis packs them.
- Cluster mode with the Redis Cluster protocol (slots, `-MOVED`, `-ASK`,
  `-TRYAGAIN`, `CLUSTER SLOTS/SHARDS/NODES`).
- stuart, the steward: nodes join it; it assigns slots, moves them live
  (`STUART MOVE`, `REBALANCE`, `DRAIN`, `--auto`), and keeps its state on
  disk.
- Live slot migration without lost writes; large collections move in parts.
- Kubernetes: a node drains on SIGTERM, Prometheus metrics, Kustomize
  manifests, autoscaling with the HorizontalPodAutoscaler.
