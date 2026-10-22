# Design notes

SpeedVault was built in phases, each planned before it was built and
measured after. These notes are those plans, with the decisions taken and
what each step found; they explain why things are the way they are.

| Phase | What | Note |
|-------|------|------|
| 1 | `fiber`: stackful coroutines, the context switch | [architecture](../architecture.md#fiber) |
| 2 | `reactor`: an event loop per thread (epoll, kqueue), timers, TCP | [architecture](../architecture.md#reactor) |
| 3 | A Redis-compatible node | [redis-compatibility.md](redis-compatibility.md) |
| 4 | Cluster mode (the Redis Cluster protocol) and stuart, the steward | [cluster-and-stuart.md](cluster-and-stuart.md) |
| 5 | Live migration of slots between nodes | [slot-migration.md](slot-migration.md) |
| 6 | Kubernetes autoscaling (the HPA) | [autoscaling.md](autoscaling.md) |
| 7 | Hashes, sets, lists, `SCAN`; compact encodings | [data-types.md](data-types.md) |
| 8 | Replicas and failover | planned |

Multi-core nodes were considered and dropped: on Kubernetes, more one-core
pods scale out instead.
