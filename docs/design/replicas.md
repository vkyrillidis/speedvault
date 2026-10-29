# Replicas and failover (phase 8)

Decided 2026-10-23 (the open questions below are still open); the
milestones approved the same day, milestone 8 (stuart's Raft) deferred.

Goal: **a node that dies loses no acknowledged write that reached its
replica, and its slots are served again within seconds, with nobody typing
a command**; stuart is no longer a single point of failure; a rolling
update no longer moves every key out and back in. Today a node that dies
takes its keys with it (phase 4), Kubernetes brings it back empty, and
stuart, one process with a state file, holds up every change to the layout
while it's down.

Decisions:

- **stuart assigns the roles; the pods stay identical.** A node joins as
  now and stuart makes it a primary or a replica, keeping `--replicas N`
  (default 0 until milestone 7, then 1: until replicas fail over, a
  replica is a pod that serves nothing) per primary, a replica on another machine than its primary
  where it can (the node passes its Kubernetes node's name, `--zone`, from
  the Downward API). The table carries the replicas; a replica attaches to
  its primary with commands of our own (Redis's `REPLCONF`/`PSYNC` in
  spirit). A separate StatefulSet for replicas was the alternative: simpler
  for stuart, but two things for the HPA to scale. The cost: a new pod
  fills a missing replica before it becomes a primary, so capacity grows by
  pairs (the HPA's step is already 2). A pod joining when every primary
  has its replicas is a primary at once, with slots (agreed 2026-10-23),
  unprotected until the next pod joins as its replica, rather than a
  spare waiting for a pair.
- **The stream: no fork; a snapshot by slots, then the writes.** Redis
  forks and lets copy-on-write keep the snapshot; under a pod's memory
  limit a write-heavy fork can double the memory and get the pod killed.
  Instead the primary walks its slots in batches (phase 5's streaming,
  phase 7's parts for large collections) and forwards, in order, the
  writes to keys it has already sent. What is replicated is a write's
  **effect**, as Redis propagates it (`SPOP` as `SREM`, `EXPIRE` as
  `PEXPIREAT`, `INCRBYFLOAT` as `SET`), so the replica ends up with the
  same bytes. A replication id, an offset and a backlog (a ring buffer,
  1 MiB by default) let a replica that dropped off briefly carry on without
  a full sync.
- **Asynchronous, with `WAIT`** (agreed 2026-10-23). A write is
  acknowledged before the replica has it, as in Redis: a failover can lose
  the last writes. `WAIT numreplicas timeout` makes a write wait for its
  replicas when it must survive. Synchronous replication would add a round
  trip (~50–100 µs) to every write.
- **stuart decides failovers; the replica's acknowledgements fence the
  old primary** (agreed 2026-10-23). The nodes don't gossip or vote (phase
  4: stuart is the only one that changes the layout). stuart can't tell a
  dead primary from one it can't reach, so a primary accepts writes only
  while its replica has acknowledged within `L` (3 s): stuart, seeing the
  primary unreachable for 5 s, tells the replica to stop acknowledging,
  waits `L`, and promotes it with a new epoch; a primary that was only cut
  off has stopped writing by then (`-NOREPLICAS`, as Redis's
  `min-replicas-to-write`). Writes depend on the primary and its replica
  reaching each other, not on stuart, so traffic still carries on while
  stuart is down. A primary whose replica died writes alone once stuart
  says so. Leases granted by stuart were the alternative: every primary
  would stop writing whenever stuart couldn't reach it. A pod that rejoins
  under the same name is known dead: its replica is promoted at once.
  **Any acknowledgement renews the lease, a replica's during its copy
  too** (agreed 2026-10-23, before milestone 4): Redis's
  `min-replicas-to-write` counts a replica only once it has synced, so
  each sync from scratch would stop the primary's writes for the whole
  copy (8.6 s for 10M keys). The fence still holds: a replica that goes
  silent stops the writes after `L`, and stuart never promotes a replica
  that hasn't finished its copy (it asks the replica at failover).
  Before a newly paired replica's first acknowledgement the primary
  writes as before: nothing could be promoted yet. **Refined in milestone
  5: an acknowledgement renews the lease from when the primary sent the
  ping it answers** (`REPLPING`, every second and after each batch of a
  copy), not from when it arrived: one that waited in the socket while the
  primary was stopped renewed it, and a SIGSTOPped primary took writes
  alongside its promoted replica (found by `migrate_check failover-stop`).
  A copy keeps its pings from waiting long behind its batches (it queues
  no more than the link drains in 250 ms), so writes still go on through
  a copy that moves; one that doesn't move lets the lease lapse. **"Write alone" is a
  fail mark in the table** (agreed 2026-10-23): stuart marks a node down
  for 5 s failed (`FAILED` in `CLUSTER SETTABLE`), and clears the mark
  once it's back; a primary whose replica is marked writes alone. As in
  Redis Cluster, `NODES` shows `fail` and `SLOTS` leaves a failed replica
  out; the pairing stays, so a replica back soon carries on from the
  backlog. Milestone 5 marks a dead primary the same way. Stuart promotes
  a replica only once its primary has taken a table where the replica
  isn't marked, so a primary still writing alone can't write alongside it.
- **On SIGTERM a primary hands over instead of draining.** It pauses
  writes, waits for its replica to catch up, and stuart promotes the
  replica (Redis's `CLUSTER FAILOVER`): nothing lost, nothing moved. This
  replaces phase 6's drain on every SIGTERM, so a rolling update moves no
  keys. On a scale-down the planner reshapes what's left (primaries =
  pods / (1 + N)), moving slots off the primaries it gives up.
- **stuart replicated with our own Raft** (agreed 2026-10-23; deferred
  the same day, not in this phase): without it, while stuart is down
  nothing in the layout changes, failovers included, but writes carry on
  (the lease is between a primary and its replica). Three
  stuarts, the state file's lines becoming the Raft log and the file its
  snapshot; the leader alone pushes tables and decides; followers answer
  `CLUSTER SLOTS`/`NODES` and send `STUART` commands to the leader. Nodes
  are given all three (`--stuart a,b,c`). Written here on the reactor
  rather than with tikv's `raft-rs` (sans-IO, but large, and its storage
  and messages to adapt) or a Kubernetes Lease (active/passive, but RBAC,
  an API client, and nothing outside Kubernetes).
- **What a replica answers before its first copy: as Redis does**
  (agreed 2026-10-23, after milestone 2). Not `-MOVED` as a "not ready
  yet": clients take `-MOVED` as a change of the map and refresh it. What
  Redis 7.0.15 does, found in milestone 1: `CLUSTER SLOTS` leaves out a
  replica whose replication offset is 0 (`isReplicaAvailable`), `SHARDS`
  shows it `health loading`; past 0 (after its first sync, or the
  primary's first PING) it is listed whatever its lag; and after
  `READONLY` a replica answers reads for its primary's slots from what it
  has, its empty store before the first copy (`GET` gives nil). During a
  copy it answers `-LOADING Redis is loading the dataset in memory`, as
  Redis does while it loads the snapshot (agreed 2026-10-23): ours has no
  load apart from the transfer, and a half copy is never served.
- **Slot migration by snapshot plus change stream, first, replacing
  phase 5's key by key** (agreed 2026-10-24, before milestone 6; it was to
  come last). A failover in the middle of a phase 5 move can't be made
  safe cheaply: the source's replica may hold copies of keys the target
  has changed or deleted since (the source's deletes in the asynchronous
  window), so a promoted source sending them again brings old data back
  over new; milestone 5 called such a move off, which loses the keys
  already moved. Redis Cluster up to 7 leaves this to `redis-cli --cluster
  fix`; Valkey 8 replicates the slots' migration states, which keeps the
  routing but not the data right; Valkey 9 and Redis 8.4 replaced the
  model with atomic slot migration, which is what this is. Replica sync is
  that mechanism for every slot; for a few it becomes: the target pulls
  the slots from the source (a snapshot, then the writes to them), the
  source owning and serving them throughout, the target serving nothing
  of them; then a short pause of their writes (they wait), the target
  catches up, and the owner switches in one table. Two nodes never serve a
  slot; a failure or a failover of either side cancels the move (the
  target drops the slots), and stuart queues it again. Clients see one
  `-MOVED` per slot, never `-ASK` or `-TRYAGAIN`; no key is frozen,
  however large. Phase 5's path stays behind a flag until the new one
  passes `migrate_check` and `kind_check`, then goes (agreed 2026-10-24).
- **stuart drives the moves as before, out of the data path** (agreed
  2026-10-24). A table starts a move; the target pulls by itself; stuart
  asks each target how its moves stand (one call for all of them), and
  once one is close behind, pauses the slots' writes at the source (a
  pause that ends by itself), waits for the target to reach the source's
  offset, and saves and pushes the table that gives the target the slots.
  Cancelling is a table without the move. With stuart down, moves don't
  end, and every slot is still served by its source. **The numbers**
  (agreed 2026-10-25, against Redis 8.10's and Valkey 9.1's defaults):
  the pause only while the target has less than 1 MiB to acknowledge
  (Redis's `cluster-slot-migration-handoff-max-lag-bytes`; Valkey's
  `slot-migration-max-failover-repl-bytes` is 0), else `-BEHIND` and stuart
  asks again later, however long that takes (a target that can't keep up
  overflows the source's output buffer and copies again; a cancel would
  throw the copy away); the pause ends by itself after 2 s (Redis 10 s,
  but its source hands over by itself, ours waits for stuart); 1 s for the
  target to catch up, 5 missed pauses and the move is called off, as it is
  when its source or target is down or fails over. Moves by snapshot are
  the default; `--key-by-key` kept phase 5's until the checks of step 3
  passed, then went.
  **The table ending a move goes to the target first** (changed while
  building step 2, 2026-10-25; the plan had the source first): the
  source, taking it, stops serving the slots and ends the sync with
  `SLOTSDONE` after every write it took; the target holds requests to the
  slots (they wait) until that comes. So the two never serve a slot
  together, whichever order the table arrives in and even if the pause ran
  out, and a client the source redirects finds the target waiting rather
  than being sent back: with the source first, a test client was
  redirected back and forth 50 times before the target took the table
  (redis-py retries 3 times).
- **Writes wait during a SIGTERM handover** (agreed 2026-10-24), as Redis's
  `CLIENT PAUSE WRITE` makes them during `CLUSTER FAILOVER`: clients see
  latency, not errors.
- **What clients see.** `CLUSTER SLOTS`, `SHARDS` and `NODES` list the
  replicas as Redis does (after the primary; `role replica`; `slave
  <primary id>`). After `READONLY` a replica answers reads for its
  primary's slots (redis-py `read_from_replicas`, Lettuce `ReadFrom`),
  possibly stale; anything else gets `-MOVED` to the primary. `INFO
  replication`, `ROLE`, `WAIT`. During a failover, requests to the dead
  primary fail with connection errors until the client refreshes its map
  (redis-py at once, Lettuce through adaptive refresh): measured, as phase
  5 measured redirects.

Milestones:

| # | Milestone | Content | State |
|---|-----------|---------|-------|
| 1 | Roles and the table | stuart assigns primaries and replicas (`--replicas N`, `--zone`), in the state file; the table carries replicas; `CLUSTER SLOTS SHARDS NODES` with replicas, checked against a Redis 7.0.15 Cluster with replicas (`cluster_difftest`, RESP2 and RESP3); `READONLY`/`READWRITE` (a replica redirects writes, and reads without `READONLY`, with `-MOVED`); `ROLE`, `INFO replication`; no data yet | done: the table's replicas (`with_replicas`: no chains, a replica owns no slots and none migrates to it) and `CLUSTER SETTABLE ... REPLICAS`; the replies take the nodes' replication offsets, all 0 until milestone 3, so as in Redis no replica is in `SLOTS` and each is `loading` in `SHARDS`; a replica redirects every key, after `READONLY` too (milestone 3); `ROLE`, `INFO replication`, `HELLO`'s role as Redis's for a primary with no replica attached and a replica never connected. stuart: `--replicas N` (default 0), `CREATE` makes the first `ceil(k / (1 + N))` nodes primaries, a node joining later fills a primary with fewer than N, else is a primary; a replica to a primary on another zone first (`--zone`, `STUART JOIN ... ZONE`), then the fewest replicas, then join order; slots to primaries only; a replica whose primary goes moves to another or becomes a primary; roles and zones in the state file, `STUART NODES` `replica-of=` `zone=`. Checked byte for byte: captured 3+3 Redis replies (unit tests); `cluster_compat` with a replica per primary and two on one, at primaries and replicas, our node in a replica's place (47 requests); `cluster_difftest replicas`, 3+3 Redis (`--cluster-replicas 1`) against stuart `--replicas 1`: 792 of 792 the same. Redis's offsets kept at 0 for that (nothing written, `repl-ping-replica-period 3600`), checked. 35 injected bugs, all caught (the two on zones once a test case was added for each) |
| 2 | The stream | Every write's effect into a stream (`SPOP` as `SREM`, `EXPIRE` and the others as `PEXPIREAT`, `INCRBYFLOAT` and `HINCRBYFLOAT` as the value they set), keys expiring as `DEL`; the replication id, offset and backlog. Checked by applying the stream of random command sequences to a second store: the same keys, byte for byte | done: the store records an `Effect` where a write's isn't what it says as sent (`SPOP` as `SREM` of what it took; `EXPIRE` and the others, and `SET` with `EX`/`PX`/`EXAT`/`PXAT`, as the absolute time, `PEXPIREAT` or `SET ... PXAT`, a `DEL` once that time has come, nothing when refused or blocked; `INCRBYFLOAT` as `SET ... KEEPTTL` and `HINCRBYFLOAT` as `HSET` of the value written), and every key it reclaims as expired, by a lookup or the sweep, as `DEL`; any other write that didn't fail goes in as sent (Redis's `write` flag, checked against Redis 7.0.15's `COMMAND INFO`; a write that changed nothing without failing, `DEL` of a missing key, goes in too). RESP commands, as Redis's stream; `master_replid`, `master_repl_offset` and a 1 MiB ring backlog in `INFO replication` as Redis reports them. Recorded only once started (milestone 3 starts it when a replica attaches), so SET and GET are unchanged (`-P 1` and `-P 16`, within the noise). Checked: 16 random sequences of 10,000 commands (every type; expiries past and future with `NX XX GT LT`; values converting the encodings; `WRONGTYPE`; sweeps) at one node and its stream at another that never expires keys by itself: every 50 commands each key has the same value, absolute expiry and encoding, and the replica takes every command without an error; the backlog read from every offset it still holds. 17 injected bugs, 16 caught; the other was equivalent (a bound that only differs once the ring is full, when they agree). Imports and the source's deletes of a slot moving go into the stream in milestone 6 |
| 3 | Sync | A replica attaches: the snapshot by slots in batches, the writes to keys already sent forwarded in order, large collections in parts; a short drop resumes from the backlog, a long one syncs again; `READONLY` reads. Checked under random writes: primary and replica the same key by key; memory during a sync (no doubling); sync time for 1M and 10M keys | done: `REPLSYNC id replid offset` (ours, `PSYNC` in spirit) gets `+CONTINUE` and the backlog from there, or `+FULLRESYNC` and the keys slot by slot in batches of up to 1000 keys or 1 MiB of values (`REPLKEY key at kind first last payload`: the absolute expiry; a collection over `--migrate-chunk` elements in parts), meanwhile the writes to slots already sent, a write to the slot being sent only if all its keys were sent (else they go again, as they are then), and last `REPLSYNCED offset`. The replica flushes first, applies the stream at the time before every expiry, never reclaims a key itself (it hides an expired one until the primary's `DEL`), answers `-LOADING` during a copy and, after `READONLY`, reads of its primary's slots from what it has. `ROLE`, `INFO replication` and `INFO stats` (`sync_full`, `sync_partial_ok`, `sync_partial_err`) as Redis's; a `PING` into the stream every 10 s while replicas are attached; `EXPIRETIME`/`PEXPIRETIME` (Redis rounds `EXPIRETIME` to the nearest second, as `TTL`). Offsets cluster-wide: stuart's keepers read each node's `master_repl_offset` with its memory every second and push them all to their node (`CLUSTER SETOFFSETS`, ours, not versioned), so every node's `CLUSTER SLOTS`, and stuart's, lists the synced replicas a second or two behind, as Redis's gossip does. Checked: the copy step by step under random writes (12 seeds, half the writes during a copy aimed at the slot being sent; the two compared when the copy ends and again after) and four cases (a collection written, deleted or flushed while in parts; a write across a sent and an unsent key); end to end through a proxy (a sync under writes, carrying on, a full resync, expired keys held back, only its own replicas may sync); `cluster_difftest` against a Redis 7.0.15 Cluster with synced replicas: 1302/1302 replies the same, RESP2 and RESP3 (the layout at every node; writes, reads, and reads after `READONLY` at the replicas); `difftest` 332/335 (the 3 allowed). Injected bugs: 17 in the sync, 15 caught, 2 equivalent (the replica counting the copy, which `REPLSYNCED` overwrites; clearing a FLUSHALL's leftovers, which a write to the key clears anyway); 11 in the offsets, all caught. Found on the way: a replica kept what it staged of a collection deleted while it went in parts (now dropped at `REPLSYNCED`). Memory and time (`tools/bench/sync_bench.sh`, `results/2026-10-23-sync/`): no doubling, the primary 2% up during a sync under writes where Redis's fork copies 83–86%; 0.77 s for 1M keys of 16 B and 8.6 s for 10M (Redis 0.58 s and 8.0 s), with writes meanwhile 10.1 s at 10M (Redis 12.6 s) |
| 4 | Acknowledgements and `WAIT` | The replica acknowledges offsets; `WAIT`; the lease (`-NOREPLICAS` without an acknowledgement within `L`); stuart's "write alone" when a replica is gone | done: `REPLACK offset` (ours, Redis's `REPLCONF ACK`) from the replica once what it read is applied, at most every millisecond (the reads between bundled; at once at the copy's end) and anyway every second, `REPLACK` alone during a copy; the primary reads them on a fiber of its own per replica. `ROLE` and `INFO replication` show each replica's acknowledged offset and lag, `online` once past its copy. `WAIT numreplicas timeout` as Redis 7.0's (the client's last write, same errors, refused at a replica), woken by each acknowledgement. The lease: a primary whose table gives it replicas writes while one acknowledged within 3 s (or before any first did), else `-NOREPLICAS Not enough good replicas to write.`; any acknowledgement renews it, a copy's too; a flag set by a lease fiber at the newest acknowledgement's end and cleared by the next one, so a write reads no clock. Fail marks: stuart marks a replica down 5 s failed (`FAILED` in `CLUSTER SETTABLE`, `failed` in `STUART NODES`, the state file and `stuart_nodes{state="failed"}`) and clears the mark once it answers; its primary writes without it, the pairing stays; `NODES` `slave,fail ... disconnected`, `SLOTS` without it, `SHARDS` `health fail`, as Redis 7.0.15 with a dead replica (captured). Checked: `difftest` 334 (`WAIT` cases); `cluster_difftest` with a replica SIGKILLed in each cluster (Redis `cluster-node-timeout 2000`): 48/48 the same at the live nodes; a lease lapsing 2.9-3.4 s after the last acknowledgement, held by one of two replicas, kept through a 4.5 s copy, ignoring a marked replica. 35 injected bugs (acknowledgements, marks, bundling, the lease flag), 32 caught (three once tests were added); missed: `WAIT` counting a replica that hung up (equivalent: it did acknowledge), an acknowledgement not clearing the lease flag itself (equivalent: the lease fiber it wakes does, a fiber switch later), an idle replica's reads left short (CPU only). Cost (`tools/bench/ack_bench.sh`): first 7% of a primary's writes unpipelined and 10% with `-P 16`, almost all the lease's clock read and lookup per write, not the acknowledgements (bundling them changed nothing); with the flag 4% and 2% (267k and 1.61M SET/s with a replica, Redis 7.0.15 with one 254k and 1.08M) |
| 5 | Failover | stuart: a primary down for 5 s (configurable), the replica told to stop acknowledging, `L` waited, the replica with the highest offset promoted, epoch + 1; at once for a pod that rejoins under its name; the old primary back as a replica. `migrate_check` with replicas: a primary SIGKILLed under load (with `WAIT 1` after each write, nothing lost; without, only what wasn't acknowledged), a primary cut off by a proxy (no write taken by both); redis-py, Lettuce, `redis-cli -c`; time to serve again measured | done: stuart's failover fiber, for a primary down for `--fail-after` (default 5 s, also the replicas' fail marks): `CLUSTER FENCE 10` (ours) at its replicas, which stop acknowledging and say their offset and whether their copy is complete; of those up, complete, not marked failed and whose primary took the table that last cleared their mark (else it may be writing alone), the highest offset; the lease waited (3.2 s); promoted in a table where it owns the slots and takes the table first, the old primary a failed master without slots (as Redis shows one), back later as its successor's replica; moves to or from it called off (milestone 6 resumes them). A primary answering again during the wait calls it off. A pod joining under a primary's name or address gets `-TRYAGAIN` while its replica is promoted at once, then joins as that one's replica. `stuart_failovers_total`. The lease, reworked: from the `REPLPING` the replica answered (above), checked against the reactor's turn time (`reactor::turn_time()`, read once a turn: no clock read per write, and a fiber woken after a stall sees the time after it). Checked: `migrate_check failover` (a primary SIGKILLed under load: promoted after 8.3 s, every INCR `WAIT` confirmed kept, and measured none acknowledged lost) and `failover-stop` (SIGSTOPped past its failover: the 100 INCRs sent to it meanwhile all `-NOREPLICAS` on waking; back as the replica), redis-py, `redis-cli -c` and Lettuce in its three refresh modes; `cluster_difftest` with a primary SIGKILLed in each cluster, Redis failing over by itself: 40/40 the same at the live nodes; stuart: a primary cut off from stuart but not from its replica refuses writes before the promotion and after, failing over, called off, the rejoin, the candidates; dbnode: a stall with acknowledgements waiting in the socket, a fence, a slow copy. 25 injected bugs (the turn time, FENCE, the lease from pings, the copy's flow control, stuart's failover and state), 24 caught, 7 of them only once tests were added or fixed (the partition test first waited for the refusal and then for the promotion, so a primary never fenced passed it); missed: a new ping's answer waiting for the 1 ms bundle (latency only). Found on the way: bug injection under WSL2 can run stale builds (the wall clock steps back, so a mutant can look older than the last build); the injector now stamps future mtimes. The acknowledgements' cost with the lease reworked: 0.95x unpipelined, 1.01x with -P 16 (266k and 1.61M SET/s with a replica). Time to serve again (`tools/bench/failover_bench.sh`, a redis-py writer): with `--fail-after` 2 s 5.7 s, Redis 7.0.15 with `cluster-node-timeout 2000` 3.8 s; 5 s: 8.8 s and 7.9 s (ours adds the 3 s lease to the detection; a cut-off Redis master writes on until gossip tells it). Found: Redis 7.0.15 doesn't fail over under WSL2 without `cluster-replica-validity-factor 0` (the wall clock steps make its replica's data look too old); a sync from scratch stayed at 0.78 s for 1M keys (one batch at a time took 1.32 s) |
| 6 | Slot migration by snapshot, the handover, the planner | Rewritten 2026-10-24 (see the decisions above). 1. Slot sync: the target pulls slots from the source as a replica does (`REPLSYNC` for slots: their keys in batches, large ones in parts, then the writes to them, the copy's flow control), applies them unserved (`-MOVED` to the source) and into its own stream; cancelling drops them, in its stream too. 2. Cutover: stuart polls each target (`CLUSTER SLOTSYNC STATUS`, ours), pauses the slots' writes at the source once it's close behind (`CLUSTER PAUSESLOTS`, ours, ending by itself; the source says its offset), waits for the target to reach it, and pushes the table giving the target the slots (to the target first, see the decisions); the source deletes them in pieces; a failover of either side, a target that keeps missing the pause, or a failure before the table cancels the move, queued again. 3. Checks: every `migrate_check` scenario on the new path, a source and a target failing over mid-move (nothing `WAIT` confirmed lost, nothing deleted brought back), replicas equal to their primaries after a rebalance, redis-py, Lettuce and `redis-cli -c` seeing only `-MOVED`, `kind_check`; the cutover pause, keys/s, MB/s and client p99 during a rebalance against phase 5's; then phase 5's path removed. 4. SIGTERM: a primary with a complete replica pauses its writes (they wait), stuart waits for the replica's offset and promotes it without the lease's wait, the primary exits; a rolling update moves no keys; a replica just leaves; else drain. 5. The planner with roles: `ceil(pods / (1 + N))` primaries; a scale-down drains surplus primaries and makes them replicas of those lacking one. 6. Injected bugs, the README | done. Steps 1-2: the table's `SYNCING` moves; the target pulls them (`SLOTSYNC id slot...`, a feed of those slots: their keys, `SLOTSSYNCED`, then the writes to them), holds them unserved (`-MOVED` to the source; hidden from `DBSIZE`, `KEYS`, `SCAN`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT` as Redis 8.10 hides a slot it imports, at its replicas too; `INFO keyspace` counts them, as Redis's), into its own stream; `CLUSTER SLOTSYNCS` (a line per link: source, state, slots, keys, offset). The cutover: the feed's offset (bytes after the copy), `CLUSTER PAUSESLOTS ms target slot...` (writes and `FLUSHALL` wait, reads go on; `-BEHIND n`), `SLOTSDONE` from the source taking the table, the target's slots landing (requests wait) until then; a link failing before the cutover keeps its copy until the source takes a new one, one failing while landing serves what it copied; a `FLUSHALL` at the target copies again; the source trims the slots it gave up, 1000 keys at a time, as `DEL`s for its replicas. stuart: the moves by snapshot by default (`--key-by-key` then kept phase 5's), the state file's `sync` lines, the cutover and the cancels above, a failover of a source queuing its moves again. Checked: a cutover under an `INCR` writer following redirects (none lost), the waiting write redirected, the source and its replica trimmed, pauses ending by themselves, by 0 and by a table; through a proxy: a link cut mid-copy, `-BEHIND`, `FLUSHALL` at the target, a link cut while landing; stuart's ordering tests on both paths, a target down. Injected bugs (46, `p8/m6s2.py`): 33 caught; 2 equivalent (a called-off copy is trimmed anyway, a held slot is excluded by its move); missed then (step 3 wrote tests for them): `FLUSHALL` at a source not sent as `SLOTDROP` or not waiting for a pause, writes after `SLOTSDONE` forwarded, leftovers kept at a new link, pings counted in the target's offset, stuart cutting over before the source took the table, ignoring the offset (`SLOTSDONE` keeps the data right; the pause is then longer), calling a move off at the first missed pause, a removal turning a move key by key, trimming a slot moving key by key; a failed link dropping its copy at once was caught only by a flaky start. Found on the way: a primary failed over trimmed the slots now its replica's, and the replica, still on its stream, deleted them (`a_primary_rejoining_under_its_name_fails_over_at_once`, 1 run in 3): a node marked failed doesn't trim, nor are unassigned slots trimmed. Step 3: every `migrate_check` scenario by snapshot, and four new ones: a source, then a target, SIGKILLed mid-move, the source of a link landing SIGKILLed and SIGSTOPped (with a `SET`/`DEL` writer confirmed by `WAIT 1`: nothing confirmed lost, nothing deleted back; replicas compared key by key with their primaries); redis-py, `redis-cli -c` and Lettuce (default, off, fast) get only `-MOVED`; `kind_check` 9/9. Found and fixed: the copy ignored `maxmemory` (a full target now stops reading, `full` in `CLUSTER SLOTSYNCS`, `migrate_imports_refused`); a node that left advertised its last table through `--leave-linger` and Lettuce adopted it (since phase 6, both paths: stuart now pushes a node that left the tables that follow, listing it slotless, while it lingers); a cutover's source took the next table before its target took the cutover, sending clients back and forth (Lettuce failed a command after 6 `-MOVED`s: a table now waits for the nodes that owe the one before, 2 s at most). Tests for the missed bugs (`p8/m6s3_item4.py`: 10 of 12 caught, 2 equivalent). Measured against phase 5's moves (`tools/bench/results/2026-10-25-0e7045b`, then `-0b36234`): the snapshot path was 20-30% slower in `migrate_check` because stuart saved its state (`fsync`, ~6.6 ms on WSL2) for two tables per link of 32 slots (a drain of 4096 slots 4.4 s against 1.6 s; 1.0 s for both with the state on tmpfs); a target ahead of its source lost 500 ms to a refused `SLOTSYNC`; the copy wasn't paced, a busy source keeping a quarter of its throughput. Fixed: the links that caught up cut over together, in the table that starts the next moves; a refused `SLOTSYNC` asked again every 5 ms; the copy in `--migrate-batch` keys with `--migrate-pause` after each while clients were served (224k requests/s of 385k at the source while it copies 524k keys/s, phase 5 260k at 235k keys/s); `--moves-per-source 128 --max-moves 512` by default (a drain 0.53 s, phase 5 0.68 s; `REBALANCE` of 1M keys 1.15 s, 1.73 s). Cutover pauses p50 7-9 ms, max 8-23 ms; up to 220 ms when the target stalls doubling its keyspace map (one go: 160 ms at 0.9M keys, 348 ms at 1.8M; incremental rehashing is open). Target memory the same on both paths. Then phase 5's path removed (the migrate fiber, `CLUSTER IMPORT`, `IMPORTPART`, `PENDING`, keys in flight and dirty, `--key-by-key`; a phase 5 state file reads as moves by snapshot), keeping the `MIGRATING` state's replies (`-ASK`, `-TRYAGAIN`, `ASKING`, the `CLUSTER NODES` markers: `cluster_difftest migrating` 198/198 the same as Redis 7.0.15). Not rerun since the new defaults: `kind_check`. Open: the source's p99 while it copies (1.1 ms, phase 5 0.44). Step 4: `CLUSTER PAUSEWRITES ms` (ours, as Redis's `CLIENT PAUSE WRITE`: every write waits, reads go on, no active expiry; the reply is the node's offset; it ends by itself, 10 s at most, with 0, or with the table that leaves the node no slots). stuart: the `STUART LEAVE` of a primary with a replica up, not failed nor leaving, hands its slots over instead of draining them: the plans leave them alone (`handing` in the state file); the handover fiber pauses the primary for 10 s, polls its most advanced complete replica (`CLUSTER FENCE 0`) every 2 ms for up to 1 s, and gives it the slots in a table that reaches it first, without the lease's wait; a miss lifts the pause, and the next try comes 100 ms later; after 5 misses, with no replica left, or with one still copying after 60 s, the slots are drained as in phase 6; a primary down is the failover's; `stuart_handovers_total`. A replica's `LEAVE` no longer needs room on the others (it was refused, and the node retried until its drain timeout). Found and fixed: a promoted replica applied what its stream loop had read before it looked at the table (its old primary's trim `DEL`s: the keys written during the handover were lost); a primary trimmed the slots its replica took over while another replica still applied its stream (`Cluster::successors`); a replica that started leaving while a try waited on it was promoted (`State::hand_over` refuses it). Checked: `migrate_check` `sigterm-primary`, `sigterm-replica`, `sigterm-no-replica` and `rolling` (six named nodes SIGTERMed in turn and started again: each primary hands over, each node comes back as the replica of the primary lacking one), with Lettuce (default, off, fast), redis-py, `redis-cli -c` and the `WAIT` writers, 3 rounds of 3: no write lost, not even one only acknowledged. Measured (`tools/bench/results/2026-10-25-065658f`, 1M keys): the handover's pause at the node is 7.1 ms p50, 7.4 max (mostly stuart's state `fsync`); the worst write a client saw 9.3 / 10.3 ms; the slots are off the node 21 ms after the SIGTERM, against 0.92 s for a drain of the same node (5461 slots moved; clients' worst write 75 ms, from the targets doubling their keyspace map); a rolling restart of six: 0 slots moved, 6 handovers, 6 full syncs, 0 partial, no client error, 68 s (10 s of `--leave-linger` per node). Injected bugs (42, `p8/m6s4.py`): 28 caught; 3 equivalent (successors kept by a node that became a replica: cleared anyway with no replica following it; a drain of a handing node: its slots are hidden from the plan; a slotless primary's `LEAVE` handing over: it leaves as before); the 11 missed got tests (`p8/m6s4b.py`: 12 of 12 caught, with one for the fix above): the table ending the pause, and only that table, the loop's order (with a `FLUSHALL` that waited), a replica copying again mid-catch-up, the 100 ms before the next try, the 60 s wait for a replica copying, a replica leaving or the primary going down while it hands over. Step 5: with `--auto`, stuart plans the roles before the moves: with no node leaving and no primary failed, a primary beyond the `ceil(k / (1 + N))` the k nodes staying need (so another lacks a replica), when the others have room for its keys, is demoted (fewest replicas, then fewest slots, then the last joined; one at a time): it gets no slots and no new replica, the rebalance moves its slots away by snapshot, and once it took a table in which it holds nothing (or at once if it's down) it becomes the replica of a primary lacking one, its own replicas going to other primaries first; a join calls the demotion off, a failover or a SIGTERM of it ends it (`demoting` in the state file and `STUART NODES`, `stuart_nodes{state="demoting"}`, `stuart_demotions_total`). Found and fixed: `STUART CREATE` with `--replicas 2` and 9 nodes paired a replica with a node not placed yet, which then became a replica too (a panic); stuart counted replicas as room for a drained node's keys. Found: nodes joining one by one with `--auto` pair up in join order, so a StatefulSet scaling down (the highest pods first) removes whole pairs and demotes nobody; a demotion is needed once rolling restarts have shuffled the roles. Checked: `migrate_check` `scale-down` (6 nodes to 4, two replicas of different primaries going: one demotion; then to 2: one primary and its replica) and `scale-down-up` (scaled up again mid-drain: the demotion called off), with Lettuce (default, off, fast), redis-py, `redis-cli -c` and the `WAIT` writers, 3 rounds: no write lost; stuart tests with fake nodes (slow to take a table: not demoted before it took the one emptying it). Injected bugs (34, `p8/m6s5.py`): 20 caught; 1 equivalent (the emptied epoch kept while it still holds slots); 1 dead code, removed (the check that another primary lacks a replica: a surplus implies one); the 12 missed got tests (`p8/m6s5b.py`: all caught). Measured (`tools/bench/results/2026-10-26-5f992a6`, 1M keys, 6 nodes to 4, 5 runs): 1 demotion and 5461 slots moved each time, every primary has a synced replica 1.8 s after the second SIGTERM (p50, max 1.9: 0.24-0.49 s until stuart's next plan, 0.95 s of drain, 60 ms to be a replica, 0.35 s for its full sync), 12.9 s after the first (the first replica's 10 s of `--leave-linger`); the client's p99 0.28 ms before, 1.0 ms during the drain (2.1-4.4 ms on the demoted primary's slots), its worst write 71-84 ms (the targets doubling their keyspace map); 1 full sync, no client error. Step 6: the milestone's mutant lists run again at its end (`p8/session/regress.py`): those whose code steps 3-5 rewrote retargeted (`p8/m6s6c.py`: 9 of 9 caught), and the two caught before but no longer got tests (`-BEHIND` taken for a missed pause, a slot queued from a primary handing over); 10 more where the steps meet (`p8/m6s6.py`): 5 caught, the 5 missed (stuart's owed tables, a lingering node's tables, a demotion's room) got tests (`p8/m6s6b.py`: all caught), and the test of an owed table, which for a while had tested nothing, was made to. At the end the workspace tests, the 26 `migrate_check` scenarios with Lettuce (adaptive refresh default, off and fast), `cluster_difftest` against Redis 7.0.15 and `kind_check` (9 scenarios) pass. |
| 7 | Kubernetes | Manifests: `--zone` from the Downward API, the replica's anti-affinity, grace periods for a handover; `kind_check.sh`: a rolling update moving no keys, a primary force-deleted (its replica serves; only unacknowledged writes lost), a kind worker stopped, the HPA adding pairs | done. Manifests: stuart `--replicas 1`, six pods; `--zone $(NODE_NAME)` (`spec.nodeName`); a soft `topologySpreadConstraint` on the hostname in place of the preferred anti-affinity (stuart pairs by zone as pods join; a required rule would leave pods unscheduled on fewer machines); the grace period stays 660 s (a handover takes milliseconds, a primary without a usable replica still drains); the HPA 6 to 12 pods, busy cores at 400m (replicas only apply writes: about half the primaries' average, measured 324m with three primaries about 55% busy); `components/no-replicas` for the old layout. The plan's shorter `not-ready`/`unreachable` tolerations dropped: a StatefulSet never replaces a pod on a machine that stopped answering, whatever they are, until the node is deleted or marked out of service. Added: a replica is ready (`/ready`) only while synced with its primary, so a rolling update and the PodDisruptionBudget wait for each pod back to hold its copy. `kind_check.sh`: kind with three workers; the checker knows replicas (primaries even, replicas synced; `WAIT` writers on INCR and SET/DEL; every replica compared key by key with its primary; a pod killed loses no filler key); `form` (each replica on another worker), scale 6 → 8 → 6, `rollout` (0 slots moved, 3 handovers), `delete` of a primary (0 moved, 1 handover), `force` of a primary (its replica promoted, nothing `WAIT` confirmed lost), `worker-stop` (new: a worker without stuart and the checker `docker kill`ed: every slot served elsewhere 7.3 s later; tainted out of service once NotReady, at +53 s, its pods ready on the others at +66 s; nothing acknowledged lost; a `docker stop` would have SIGTERMed the pods, which then hand over); `--autoscale` with the HPA by pairs (`hpa-memory` 6 → 8, asked at +13 s, slots even at +16 s, then held at 69%; `hpa-cpu` 6 → 8 at +29 s, client p99 1.4 ms; `hpa-down` 8 → 6 by hand); `--no-replicas` runs the old counts. All pass: 10/10, 5/5 with `--autoscale`, 10/10 with `--no-replicas`. `teardown` now checks the pods gone within 60 s (17 s) and gives the namespace 5 minutes: with six pods Kubernetes 1.33 deleted its other objects 205 s after the pods had gone (16 s in all without replicas). Open: after a worker is lost, two pairs share a worker and stay so once it's back (no re-pairing; see the open questions); replica pods take 13–17 s to go at a namespace delete (not looked into); the CI `kind-check` job not run with four kind nodes |
| 8 | stuart's Raft (deferred 2026-10-23) | Leader election, log replication and snapshots on the reactor; three stuarts, `--stuart a,b,c` on the nodes; tested with a simulated network (drops, reordering, partitions, crashes and restarts) and injected bugs; in kind, the leader killed during a failover | |
| 9 | (Slot migration by snapshot) | Moved into milestone 6 on 2026-10-24 | |
| 10 | Benchmarks and docs | Throughput of a primary with a replica against one without and against Redis 7.0.15, 8 and Valkey 9 with replicas (`-P 1` and `-P 16`); sync time; failover time; memory; README and "Run on Kubernetes" | done (results in `tools/bench/results/2026-10-26-dde1f70`, `2026-10-27-d114045`, `-4103e02`, `-ea5d41b`; see Performance, "With replicas"). Every bench takes `--also NAME=SERVER` (Redis 8.10.2, Valkey 9.1.2; their defaults but diskless sync without its delay, and `cluster-replica-validity-factor 0` for failover under WSL2). `ack_bench`: SET, GET, INCR, HSET at a primary alone and with a replica, median of 5, theirs standalone and in cluster mode (which costs them 10–20% with `-P 16`); against theirs in cluster mode with a replica, ours 1.02–1.22× unpipelined, 1.11–1.76× with `-P 16`. `sync_bench`: 1M and 10M keys, no fork (the primary's peak 1.02× under writes, theirs 1.8–1.9×), 10M in 8.3 s idle (theirs 6.3–8.0 s); a 1 s cut through a proxy: without writes every server carries on from its backlog (ours at once, theirs after 0.4–0.8 s), with SETs ~23 MB pile up past the 1 MiB backlog and every server syncs again in full (ours 1.8 s, theirs 1.2–2.0 s). `failover_bench`: SIGKILLed, ours serves again after 5.9 s with `--fail-after 2` (theirs 3.7 s: ours waits the 3 s lease) and 8.9 s with 5 (theirs 8.0–8.9 s); leaving, ours (SIGTERM) promotes in 0.03 s with the client's longest wait 9 ms, theirs (`CLUSTER FAILOVER`) 0.05–0.07 s and 48–67 ms; no acknowledged write lost by any. `memory_bench` (new): a replica holds what its primary does; collections 28 B an element (Redis 8 and Valkey 9 24–26, Redis 7 47), strings 207 B a key (71–78; Redis 7 122–138). Found on the way: `failover_bench` killed Redis's primaries before their replicas had synced (their diskless sync waits 5 s), which looked like 12,500 writes lost; it now waits for synced replicas | 

Each milestone: tests against Redis 7.0.15 where it has the same command
(byte for byte), injected bugs, focused commits, the README per milestone.

Open questions:

- **More than one replica per primary.** `--replicas N` assigns any N
  (milestone 1 tests 2); with 2, which replica is promoted, and must the
  lease count one acknowledgement or a majority?
- **Zones once roles are given.** A replica goes to another zone than its
  primary when it joins, if a primary there lacks one; roles aren't
  reshuffled later to fix a pairing that had to share a zone. Seen in
  kind (milestone 7): a worker lost, its pods back on the two others
  pair up there, and stay so once it's back. Still open: a planner that
  swaps replicas between primaries.
- **Persistence.** None is planned: replicas are the durability, so
  losing a primary and its replica together, or the whole cluster, loses
  their keys. An append-only file or snapshots on a volume would be a
  later phase.
- **Defaults** for the failure timeout (5 s), the lease (3 s) and the
  backlog (1 MiB). Milestone 5 measured 8.8 s to serve again with 5 s
  (Redis 7.9 s with a 5 s node timeout) and 5.7 s with 2 s; the lease's
  3 s is what a cut-off primary needs to stop writing before the
  promotion. Milestone 10, with synced replicas: 5.9 s with 2 s (Redis
  7.0.15, 8.10.2 and Valkey 9.1.2 3.7 s), 8.9 s with 5 s (theirs
  8.0–8.9 s). Kept for now; a shorter `--fail-after` risks failing over a
  primary only slow to answer (a keeper's reply times out after 2 s).
- ~~Stale copies after a promoted source~~: settled 2026-10-24 by
  replacing phase 5's migration (see the decisions): nothing a source
  sends can overwrite what the target served, since the target serves
  nothing before the cutover.
- **Reads from replicas** may be stale by the replication lag, as in
  Redis. `INFO replication`'s offsets show the lag; refusing reads past a
  bound on it isn't planned.

