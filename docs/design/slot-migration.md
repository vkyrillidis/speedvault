# Live slot migration (phase 5)

Drafted 2026-10-17, agreed 2026-10-18 with the open questions below
settled as proposed.

Goal: stuart moves slots between nodes **while clients read and write
them**, with no write lost, no key served by two nodes at once, and no
error a stock Redis Cluster client can't handle. This is what phase 6 (the
HPA adding and removing nodes) stands on: adding a node means moving slots
to it, removing one means moving all of its slots away first.

How one slot moves from node A to node B (Redis Cluster's protocol, as
clients see it; stuart drives it instead of `redis-cli --cluster reshard`):

1. **stuart writes the migration to its state file**, then pushes a table
   with a new epoch that says *slot s: owner A, migrating to B*. The table
   carries migrations, so the one mechanism that already exists (epochs,
   `CLUSTER SETTABLE`, re-pushing to nodes that missed it) carries them
   too; B gets it before A.
2. **B imports**: for slot s it answers only requests sent after `ASKING`
   (the client's one-shot flag); anything else gets `-MOVED s A`, as Redis
   does. **A migrates**: it serves keys of s that it still has; a request
   for a key of s it doesn't have gets `-ASK s B` (the key moved, or is new
   and will be created at B); a multi-key request whose keys are partly
   there and partly not gets `-TRYAGAIN Multiple keys request during
   rehashing of slot`. Clients (redis-py, Jedis, go-redis, `redis-cli -c`)
   handle all three.
3. **A streams the slot's keys to B** over a node-to-node connection, in
   batches (a new internal command, `CLUSTER IMPORT s n key ttl value ...`,
   which B accepts only for a slot its table says it imports). A key leaves
   A only once B has acknowledged it. While a batch is on its way its keys
   are *in flight*: a command touching one waits for the acknowledgement
   (a round trip), then finds the key gone and is sent to B (`-ASK`). So
   the copy at B is the key's last state, and at any moment each key is
   served by exactly one node; A never blocks, other clients are served
   between batches. (The draft compared each key with what was sent and
   sent changed ones again instead; that loses a write when a key sent is
   deleted at A, created at B after `-ASK`, and then the stale copy or its
   delete arrives.) If the connection fails before the acknowledgement, B
   may or may not have the batch: its keys are *dirty*, A answers for them
   whether it still has them or not, and sends their state again (a delete
   for one deleted meanwhile) once it reconnects.
4. **When A has no key left in s**, stuart (polling `CLUSTER PENDING s`:
   A's keys of s and its dirty ones) writes "s belongs to B" to its state file and pushes a
   table where B owns s, to B first, then to A. From then on A answers
   `-MOVED s B`. A client that cached the old layout costs one more
   redirect, never a wrong answer.

Failures: if stuart dies mid-migration, it resumes from its state file
(re-pushes the table; A carries on streaming). If B dies (restarts with a
new id), stuart ends the migration with A as the owner; the keys already
moved are lost, as for any node that dies until phase 8. If A dies, the
slot goes to B with whatever arrived. The node with the newest table wins:
a node never serves a slot its table doesn't give it.

Milestones:

| # | Milestone | Content | State |
|---|-----------|---------|-------|
| 1 | Keys by slot | The store indexes its keys by slot (a list per slot, removal O(1), as the expiry index does): `CLUSTER GETKEYSINSLOT`, `COUNTKEYSINSLOT` in O(1); its cost on SET/DEL measured (and only in cluster mode if it's not negligible) | done: a `Vec` of keys per slot, the entry keeps its place in padding (an `Entry` stays 40 bytes; 16 bytes per key in the lists); `GETKEYSINSLOT` errors as Redis 7.0.15's; 12 injected bugs all caught. Cost, store bench (1M keys): SET of a new key +55 ns (212 → 266), DEL + SET +280 ns (the key moved into the hole is looked up), GET and overwrites unchanged; end to end at `-P 16` with 1M random keys, SET within 5%, DEL 25% slower. Not negligible for DEL, so cluster mode only, as Redis does |
| 2 | Migration states and redirects | Tables with migrations (`SETTABLE` with a `MIGRATING s A B` section); `ASKING` sets the one-shot flag; `-ASK`, `-TRYAGAIN` and `-MOVED` at A and B as above; `CLUSTER NODES` shows `[s->-B]` / `[s-<-A]` on the `myself` line as Redis does. Checked against a Redis Cluster put in the same state with `CLUSTER SETSLOT s MIGRATING/IMPORTING` | done: `SlotTable::with_migrations` (from the owner to another node, each slot once), `SETTABLE ... MIGRATING k slot from to ...`; the slot check is Redis 7.0.15's `getNodeByQuery`, expired keys counting as missing without being reclaimed (Redis's `LOOKUP_NOEXPIRE`); `cluster_difftest.py migrating`: 198/198 replies the same as Redis's in RESP2 and RESP3; 19 injected bugs all caught |
| 3 | Streaming keys | A streams a migrating slot's keys to B: batches (size and pause configurable), TTLs carried, values binary safe, copy-acknowledge-check-delete; a node restart or a dropped connection resumes | done: one fiber per node moves its migrating slots, lowest first: `CLUSTER IMPORT` batches (`--migrate-batch` keys, default 100, and at most 1 MiB of values; `--migrate-pause`), keys in flight make commands wait, dirty keys after a failed connection (see step 3); `CLUSTER PENDING`; `INFO cluster` counts keys, batches and bytes sent. Tests: 300 keys with TTLs, binary and 3 MiB values; 3 writers doing `INCR`/`DEL` through redirects while the slot moves in batches of 3, every count exact; a proxy that drops the connection after B applied a batch, a key deleted at A while dirty is deleted at B too; a target without the table refuses and the keys stay; another node at the target's address gets nothing. 17 injected bugs all caught. One slot of 865k keys moves in 1.3 s (batch 100) or 0.7 s (batch 1000) |
| 4 | stuart moves slots | `STUART MOVE start end id`: the migrations in the state file, one slot at a time per source node, the final table pushed target first; `STUART MIGRATIONS` shows progress; resumes after a stuart restart; the failures above | done: `STUART MOVE` queues the slots (in the state file, `queue` lines; the table's migrations as `migrate` lines); a mover fiber starts a queued slot when both nodes are up, its source moves no other and neither node is the other end of a move under way, and ends a move once both nodes took the table and the source's `CLUSTER PENDING` is 0, ending one and starting the next in one table; each such table goes to the targets first (`first` lines; the other keepers wait). A source that is replaced gives the slot to the target, a target replaced ends the move. Tests: 300 keys of 100 slots moved while a client `INCR`s them through redirects, every count exact; stuart killed mid-move and restarted from its file; fake nodes that acknowledge slowly show the target-first order, that a move ends only after the source took its table, and that no node is a source and a target at once. 15 injected bugs all caught. 4096 slots move in 41 s (10 ms a slot, one at a time) |
| 5 | Rebalancing | `STUART REBALANCE` (even the slots out over all joined nodes with as few moves as possible, e.g. after nodes joined) and `STUART DRAIN id` (move everything off a node, for its removal); limits on how many slots move at once | done: `REBALANCE` leaves every node `assigned / n` slots or one more (the extra ones with the nodes that have the most), each node giving its highest slots and each receiving runs; `DRAIN` moves only that node's slots, to the nodes with the fewest first; both refused while slots move. `--moves-per-source` (default 8) and `--max-moves` (default 64) limit the slots moving at once. Tests: a 4th node joins 3, `REBALANCE` gives it 4096 slots, then `DRAIN` empties the 1st node, while a client `INCR`s 2000 keys through redirects: every count exact, the drained node holds no key; fake nodes show the limits are kept and reached. 9 injected bugs all caught. 4096 slots move in 5.5 s with 8 per source (41 s one at a time), with 500k keys among them in 5.7 s |
| 6 | Under load | A checker that keeps writing while slots move (counters with `INCR`, checked at the end: no write lost, no key twice), through redis-py `RedisCluster` and `redis-cli -c`; stuart killed mid-migration; a node killed mid-migration. Redirects: count the `-ASK` and `-MOVED` replies each client gets during and after a rebalance, and check they drop to zero once its slot map is refreshed; Lettuce with and without its topology refresh (by default it follows `-MOVED` without updating its map), so the docs can say what clients must configure. Benchmarks: keys/s and MB/s moved, client throughput and p99 during a move. CI runs the checker | done: `tools/compat/migrate_check.sh`: 2000 counters `INCR`ed through two redis-py `RedisCluster` threads, `redis-cli -c` and Lettuce 7.8.0 (three refresh modes), and 32 hot ones in slots that always move, during `REBALANCE`, `DRAIN`, stuart SIGKILLed mid-move, a source or a target SIGKILLed and restarted at its address, a drain target down 3 s; then every node read directly: no key twice or misplaced, every count exact (with a node killed, only its slots lose keys). `INFO cluster` counts the `-MOVED`/`-ASK`/`-TRYAGAIN` sent: after a rebalance redis-py gets none at once, Lettuce 7 (refresh on by default, at most every 5 s) after 5–8 s, 1–3 s with a 1 s limit, never with its refresh off (Lettuce 6's default); the client docs say what to set. Found: redis-py 8.1.0 fails on `-ASK` to a node not in its map (Redis Cluster trips it too); Lettuce can't take stuart as a seed; while a target with moves under way is down, the other moves stall. Benchmarks (`tools/bench/migrate_bench.sh`): 0.5–0.8M keys/s of 16 B, up to 1.5 GB/s of 16 KiB values; with no pause a move cost a saturated source a third of its client throughput, so `--migrate-pause` became 50 µs while clients are served (half the cost; idle moves at full speed), which needed `epoll_pwait2` for sub-millisecond timers; `--moves-per-source 32 --max-moves 128` (REBALANCE of 1M keys 3.3 → 1.8 s). CI: `cluster-compat` runs the checker, `cluster-clients` with Lettuce. 15 injected bugs all caught (the checker alone: 5 of the 8 in the data path; the in-flight ones only once hot keys were added) |

Open questions:

- **Rolling back.** The plan only moves forward (or ends a failed move with
  the slot at whichever node is alive). A real rollback would stream keys
  back from B to A; worth it only if forward-only proves too coarse.
- **The slot index's memory:** a pointer and a slot number per key (about
  24 bytes). If milestone 1 measures it as noticeable, it exists only in
  cluster mode.
- **Transfer format.** Our own `CLUSTER IMPORT` (plain strings, our TTLs)
  instead of Redis's `MIGRATE`/`RESTORE` with the `DUMP` format; the
  latter would only matter for moving slots between our nodes and real
  Redis, which isn't a goal.
- **Throttling.** Batch size and pause per batch, so a move doesn't eat the
  node's core while it serves; what the defaults should be comes out of
  milestone 6's benchmarks. Settled there: batch 100, a 50 µs pause only
  while clients are served, 32 slots moving per source and 128 in all
  (see [performance](../performance.md)).
