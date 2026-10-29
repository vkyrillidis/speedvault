# SIGTERM handover

### scale-down

5 scale-downs from 6 nodes to 4 (a new cluster each), 1000000 keys of 100 B: two replicas of different primaries get SIGTERM, the second once the first exited (as migrate_check scale-down), leaving three primaries for four nodes: one is demoted, its slots move by snapshot to the other two and it becomes the replica of the one lacking one. Times from the second SIGTERM: to the demotion's start (STUART NODES `demoting`, polled every 5 ms), its drain (to the table leaving it no slot), to it being a replica, to every primary having a synced replica (link up, listed online by its primary), and that from the first SIGTERM (one primary has no replica from then on). A client INCRs keys of any slot one at a time: p99 in the 2 s before the first SIGTERM and during the drain (on any slot / on the demoted primary's), worst during the drain. Full syncs: those the nodes served from the first SIGTERM on.

| run | slots | slots moved | demotions | start ms | drain s | a replica s | all synced s (from the first) | cutover pause ms | cutovers | p99 before ms | p99 drain ms (any / its slots) | worst drain ms (any / its slots) | full syncs | client errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 5461 | 5461 | 1 | 243 | 0.95 | 1.25 | 1.67 (12.8) | 8.4 / 9.4 | 44 | 0.29 | 1.22 / 2.36 | 83.9 / 9.7 | 1 | 0 |
| 2 | 5461 | 5461 | 1 | 491 | 0.94 | 1.49 | 1.85 (12.9) | 8.5 / 17.1 | 44 | 0.28 | 0.92 / 2.46 | 74.5 / 74.5 | 1 | 0 |
| 3 | 5461 | 5461 | 1 | 409 | 0.96 | 1.42 | 1.78 (12.9) | 8.5 / 10.2 | 44 | 0.28 | 1.34 / 4.35 | 74.4 / 71.3 | 1 | 0 |
| 4 | 5461 | 5461 | 1 | 494 | 1.01 | 1.56 | 1.91 (13.0) | 8.0 / 78.2 | 64 | 0.26 | 0.98 / 2.60 | 72.4 / 41.1 | 1 | 0 |
| 5 | 5461 | 5461 | 1 | 480 | 0.92 | 1.46 | 1.78 (12.9) | 8.3 / 11.1 | 44 | 0.26 | 1.02 / 2.07 | 70.6 / 12.3 | 1 | 0 |

p50 / max: drain 1.0 / 1.0 s; all synced 1.8 / 1.9 s; p99 during the drain 1.0 / 1.3 ms (before: 0.3 / 0.3).


### Findings

- Going from 6 nodes to 4 (two replicas of different primaries gone)
  demotes one primary: 1 demotion and 5461 slots moved in each of the 5
  runs, no client error, 1 full sync (the demoted node's, from the
  primary it now follows).
- Every primary has a synced replica again 1.8 s after the second
  SIGTERM (p50, max 1.9): 0.24-0.49 s until the demotion starts (stuart
  plans again at most once a second while nothing moves, `REPLAN` in
  `mover.rs`, after the replica leaving was removed), 0.92-1.01 s to
  drain its slots (as the drain of the same node at 065658f, 0.92 s),
  60 ms more to be a replica (the node's log: the table emptying it, then
  the one making it a replica), and 0.3-0.4 s for its full sync of the
  other primary's 0.5M keys. Counted from the first SIGTERM it's 12.9 s,
  10 s of which are the first replica's `--leave-linger`, as the
  StatefulSet waits for its exit: one primary has no replica for that
  long.
- During the drain the client's p99 goes from 0.28 ms to 1.0 ms (p50,
  max 1.3) on any slot, 2.1-4.4 ms on the demoted primary's slots (44
  cutovers in about a second, pausing them 8 ms each, p50). The worst
  write is 71-84 ms, on any slot: the targets doubling their keyspace
  map (the open incremental rehash item; run 4's 78 ms cutover pause is
  the same stall).
