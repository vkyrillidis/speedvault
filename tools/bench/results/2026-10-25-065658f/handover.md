# SIGTERM handover

### handover

5 handovers, 1000000 keys of 100 B. Pause at the node: CLUSTER PAUSEWRITES to the table that hands the slots over (the node's log). Client: the worst INCR, one at a time, on keys of the leaving primary's slots / of any slot, from the SIGTERM on. Slots off: SIGTERM to the table giving them to the replica (STUART NODES, polled every 5 ms); exit: after --leave-linger (10 s).

| run | pause at the node ms | missed pauses | client worst ms (its slots / any) | slots off ms | exit s | client errors |
|---|---|---|---|---|---|---|
| 1 | 6.93 | 0 | 8.3 / 7.9 | 21 | 11.0 | 0 |
| 2 | 7.21 | 0 | 10.0 / 9.0 | 22 | 11.0 | 0 |
| 3 | 6.98 | 0 | 7.8 / 8.1 | 12 | 11.0 | 0 |
| 4 | 7.43 | 0 | 10.3 / 9.8 | 21 | 11.0 | 0 |
| 5 | 7.05 | 0 | 9.3 / 7.6 | 21 | 11.0 | 0 |

p50 / max: pause at the node 7.1 / 7.4 ms; client worst on its slots 9.3 / 10.3 ms; slots off 21.3 / 21.6 ms.

### drain of the same node

5 drains (a new cluster each), 1000000 keys of 100 B: the primary's replica leaves first, then the primary gets SIGTERM and its slots move by snapshot to the other two. Cutover pauses p50 / max from the sources' logs.

| run | slots | slots moved | cutover pause ms | cutovers | client worst ms (its slots / any) | slots off ms | exit s | client errors |
|---|---|---|---|---|---|---|---|---|
| 1 | 5461 | 5461 | 7.7 / 9.3 | 44 | 72.9 / 70.7 | 918 | 11.0 | 0 |
| 2 | 5461 | 5461 | 7.8 / 13.0 | 44 | 72.9 / 72.6 | 892 | 11.0 | 0 |
| 3 | 5461 | 5461 | 7.9 / 9.1 | 44 | 75.2 / 75.3 | 916 | 11.0 | 0 |
| 4 | 5461 | 5461 | 7.9 / 9.2 | 44 | 75.6 / 76.0 | 916 | 11.0 | 0 |
| 5 | 5461 | 5461 | 7.5 / 72.5 | 64 | 79.1 / 79.7 | 993 | 12.1 | 0 |

p50 / max: client worst on its slots 75.2 / 79.1 ms; slots off 915.7 / 993.5 ms.

### rolling restart

Six nodes, 1000000 keys of 100 B; each SIGTERMed, started again under its name once it exited (after --leave-linger, 10 s), the next once it's a synced replica again. A client INCRs keys of any slot throughout.

| node | role | slots off ms | back, synced s | client worst ms |
|---|---|---|---|---|
| 7921 | primary | 26 | 11.4 | 8.6 |
| 7922 | primary | 26 | 11.3 | 7.3 |
| 7923 | primary | 15 | 11.4 | 7.4 |
| 7924 | primary | 26 | 11.3 | 7.7 |
| 7925 | primary | 19 | 11.4 | 9.2 |
| 7926 | primary | 15 | 11.3 | 7.8 |

Total 68.1 s; slots moved 0; handovers 6; full syncs 6, partial syncs 0; client errors 0.

### Findings

- A handover pauses the leaving primary's writes for 7.1 ms (p50, max 7.4).
  The replica catches up in well under a millisecond. Most of the pause is
  stuart saving its state file with fsync before it pushes the table that
  hands the slots over (about 6.6 ms on WSL2's ext4, as measured on
  2026-10-25 at 0e7045b). Then come the two table pushes, to the replica
  first. A client sees 9.3 ms at worst (p50, max 10.3): the wait, then the
  -MOVED and a slot map refresh. No slot moves, and no client gets an error.
- Draining the same node (5461 slots, 333k keys) takes 0.92 s to get its
  slots off it, against 21 ms for a handover, and moves every one of its
  slots. Clients see 75 ms at worst, on any slot, not only the drained
  ones. That is the targets' keyspace map doubling as the keys arrive (the
  open incremental rehash item; drain 5's 72.5 ms cutover is the same
  stall), not the cutover pauses, which are 7.5-7.9 ms p50.
- A rolling restart of six nodes moves 0 slots and makes 6 handovers.
  Every node was a primary when its turn came: each node coming back
  becomes the replica of the next one in the order. That costs 6 full
  syncs (one per node coming back) and 0 partial syncs, with no client
  error. Each step takes 11.3-11.4 s, almost all of it --leave-linger
  (10 s).
