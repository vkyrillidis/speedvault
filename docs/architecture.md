# Architecture

The crates, from the bottom up.


# `fiber`

Stackful coroutines: `Fiber::resume()` from
outside, a free `suspend()` from inside.

```rust
use fiber::{suspend, Fiber, StackPool};

let mut pool = StackPool::new(64 * 1024, 1024);
let mut f = Fiber::new(pool.get()?, || {
    println!("step 1");
    suspend();
    println!("step 2");
});
f.resume().unwrap(); // prints "step 1"
f.resume().unwrap(); // prints "step 2"
pool.put(f.into_stack());
```

- `stack.rs`: mmap'd stacks with a guard page; an overflow dies with
  SIGSEGV instead of corrupting memory.
- `arch/`: the context switch in assembly, for x86_64 (System V) and aarch64
  (AAPCS64): the callee-saved registers pushed on the fiber's own stack, a
  suspended fiber kept as its stack pointer.
- `fiber.rs`: the safe API. Panics are caught per fiber and returned from
  `resume()`. Dropping a suspended fiber unwinds its stack.
- `pool.rs`: reuses stacks instead of mapping a new one per fiber.

**Read the "Rules for code running on a fiber" in the crate docs**
(`cargo doc --open`) before writing fiber code. Most importantly: `Drop` must
never suspend.

Only Unix (Linux, macOS) on x86_64 and aarch64 is supported. The crate
requires `panic = "unwind"`.

# `reactor`

A single-threaded event loop, one per thread. Fibers do I/O through `Registered<T>`: try the
operation, and on `WouldBlock` suspend until epoll reports the fd ready. To
the fiber it looks like blocking I/O.

```rust
let reactor = Reactor::new()?;
reactor.spawn(move || {
    let sock = Registered::new(sock).unwrap();
    let n = sock.read_with(|mut s| s.read(&mut buf)).unwrap(); // suspends
})?;
reactor.run()?; // until every fiber has finished
```

- Every fd is registered once, edge-triggered, for reading and writing:
  waiting costs no `epoll_ctl`.
- epoll carries a `Token` (slot index + generation), not a pointer: an event
  for an fd that was closed meanwhile can't reach freed memory.
- Dropping the reactor cancels the remaining fibers (their stacks unwind,
  their fds are deregistered).
- Each turn of the loop runs the fibers that were ready when it started, then
  asks epoll: a fiber calling `yield_now()` in a loop can't starve I/O.
- A `TcpStream` read that returns less than asked for has emptied the
  socket, so the next read waits for epoll first instead of failing with
  EAGAIN. Not after a hangup or error was reported: a short read may then
  have stopped at the end of the stream, which no new event will announce.
- `Reactor::set_busy_poll(dur)`: before sleeping in epoll_wait, poll with
  timeout 0 for up to `dur` (never past the next deadline). A client that
  answers quickly then needn't wake the thread. Off by default.
- `Reactor::busy_time()` (and `reactor::busy_time()` from a fiber): the
  time `run` spent not waiting, running fibers and dispatching events.
  Busy-polling counts as waiting, so this stays the work done while the
  thread's CPU time goes to 100%. Two clock reads per sleep: one before
  the wait (which the busy-poll and the timeout use), one after (which
  the timers use).

`TcpListener` and `TcpStream` wrap `Registered` for TCP: `accept`, `connect`
(non-blocking, the fiber suspends until the handshake is done) and `Read` /
`Write`, also on `&TcpStream` so one fiber can read while another writes.

```rust
let listener = TcpListener::bind("127.0.0.1:7000".parse().unwrap())?;
reactor.spawn(move || loop {
    let (mut conn, _) = listener.accept().unwrap(); // suspends
    spawn(move || { /* conn.read(..), conn.write_all(..) */ }).unwrap();
})?;
```

They take a `SocketAddr`, not a host name: resolving one would block the
thread.

Timers: `sleep(dur)` / `sleep_until(instant)` suspend a fiber;
`TcpStream::set_read_timeout` / `set_write_timeout` (and
`Registered::read_with_deadline` / `write_with_deadline` underneath) make a
wait fail with `ErrorKind::TimedOut`, the socket staying usable. Deadlines
live in a min-heap whose earliest entry becomes the wait's timeout:
`epoll_pwait2`'s and kevent's are nanoseconds (plus the kernel's timer
slack, 50 µs by default); on Linux before 5.11, or where seccomp refuses
`epoll_pwait2`, `epoll_wait`'s whole milliseconds, rounded up. A fiber woken by its I/O and its deadline in the
same turn runs once, and I/O wins.

Linux (epoll) and macOS (kqueue, with EV_CLEAR as the edge trigger; a hangup
arrives as two events, readable and writable, where epoll sends one).
Milestones: 1 epoll + Token ✅, 2 reactor core ✅, 3 TCP ✅, 4 timers ✅,
5 kqueue (macOS) ✅, 6 benchmarks.

# `cluster`

The Redis Cluster model, for the dbnode and stuart: which slot a key is in,
which node owns each slot, and the replies that tell clients.

```rust
use cluster::{key_hash_slot, reply, Node, NodeId, SlotRange, SlotTable};

assert_eq!(key_hash_slot(b"{user42}:name"), key_hash_slot(b"{user42}:email"));
let table = SlotTable::new(epoch, nodes, &[
    (SlotRange::new(0, 8191), Some(a)),
    (SlotRange::new(8192, 16383), None), // unassigned, explicitly
])?;
let owner = table.owner(key_hash_slot(key));  // Option<&Node>
reply::slots(&table, &[], &mut out);           // CLUSTER SLOTS, RESP2 or RESP3; offsets 0
```

- `slot.rs`: `key_hash_slot` is Redis's `keyHashSlot`: CRC16-XMODEM mod
  16384 over the bytes between the first `{` and the first `}` after it,
  or over the whole key when there's no such pair or the tag is empty
  (`{}a{b}` hashes all of it, `{{a}}` hashes `{a`). Table-driven CRC.
- `table.rs`: `NodeId` (40 lowercase hex), `Node` (id and the announced
  host and port), `SlotTable` (the epoch, the nodes in reply order, and a
  flat 16384-entry owner array). Building a table checks that every slot is
  given exactly once, owned or unassigned (`Overlap`, `Gap`, `BadRange`),
  that ranges name known nodes, and that node ids and addresses are unique.
  `runs()` yields the longest runs of slots with one owner, which the
  replies are built from, so adjacent ranges merge as in Redis. A table
  may also hold slots migrating (`with_migrations`: from the owner to
  another node, each slot once), which `NODES` shows on the node's own
  line, replicas (`with_replicas`, phase 8: of a node that is no
  replica, owning no slots, none migrating to them, in the order given),
  and nodes marked failed (`with_failed`, phase 8: stuart found them down).
- `reply.rs`: `CLUSTER SLOTS`, `SHARDS` and `NODES` as Redis 8.10.2 sends
  them, RESP2 and RESP3. `SHARDS` groups the nodes by shard id, as Redis
  7.2+ does: the table lists every node's shard (`with_shards`; by
  default a primary and its replicas), and stuart carries them from one
  table to the next (`with_shards_after`) so that a primary failed over
  stays in its shard, first, as in Redis. The nodes' replication offsets are passed in: a
  replica at 0 is left out of `SLOTS` and `loading` in `SHARDS`, as in
  Redis. A node marked failed is shown as Redis shows one in `FAIL`:
  `fail` in its `NODES` flags and its link `disconnected`, `health fail`
  in `SHARDS`, and a failed replica left out of `SLOTS`. What a table
  doesn't know is fixed: every other node connected, ping and pong times
  0, config epoch = the table's epoch, cluster bus port = the client port.

`tools/compat/cluster_compat.sh` checks the crate against a real 3-node
Redis Cluster: `CLUSTER KEYSLOT` on 20,000 random keys (binary, many with
braces), and the three replies byte for byte in both protocols for several
layouts, two with replicas (only the `NODES` fields above are masked).

# `dbnode`

The server. `dbnode [PORT | HOST:PORT] [--timeout SECONDS] [--busy-poll
USEC] [--loglevel LEVEL] [--maxmemory BYTES | N%] [--metrics PORT |
HOST:PORT] [--cluster [--announce HOST:PORT] [--stuart HOST:PORT [--name
NAME] [--drain-timeout SECONDS] [--leave-linger SECONDS]] [--migrate-batch
KEYS] [--migrate-pause USEC]]` listens on
`127.0.0.1:7000` by default (a bare port listens on all interfaces).

- `--timeout` closes connections that sent nothing for that long,
  silently, like Redis's `timeout` (default 0: never); `CONFIG GET timeout`
  reports it.
- `--busy-poll` (default 20 µs) polls for requests that long before the
  thread sleeps. Without it, a client sending its next request had to wake
  the server each time, which made unpipelined requests ~10% slower than
  Redis; with it they're 1.1–1.4× faster. The cost: under steady load the
  server's core stays busy. `--busy-poll 0` sleeps at once, like Redis.
- `--loglevel` is Redis's: `warning`, `notice` (default: start and stop) or
  `verbose` (also every connection, and why it closed). Lines go to stderr
  as `2026-10-16T20:54:16.013Z NOTICE ...`, UTC.
- SIGTERM and SIGINT exit at once (nothing is persisted), so `docker stop`
  doesn't wait. A node that joined stuart (`--stuart`) leaves the cluster
  on the first SIGTERM instead (phase 6): a thread sends `STUART LEAVE id`
  once a second while the node serves on, stuart moves its slots to the
  others (a primary with a replica that can take over hands them to it
  instead, phase 8: its writes wait a few milliseconds, and no key moves;
  a replica has none and just goes; see `STUART LEAVE`) and removes it,
  and once stuart answers `0` the node serves on
  for `--leave-linger` (default 10 s), redirecting every command to the
  slots' new owners, then exits (status 0); it logs the slots left every
  5 s, and `INFO cluster` says `cluster_leaving:1`. The linger is for
  clients that learn the new layout from a redirect only now and then:
  Lettuce 7 refreshes its slot map at most once in 5 s, and a node that
  exited as soon as its last slot went left it sending that slot's
  commands to a closed connection, where they timed out. Without
  persistence leaving is the only way a node stops without losing keys,
  and Kubernetes stops pods with SIGTERM (scale-down, eviction, rolling
  update). A second SIGTERM, or SIGINT, exits at once. If stuart can't be
  reached the node keeps trying for `--drain-timeout` (default 600 s),
  then exits (status 1; the keys still here are lost); a refused `LEAVE`
  exits at once (status 1). Once every node is leaving (a cluster scaled
  to zero, `docker compose down`, `kubectl delete -k`) stuart answers each
  `0` at once, and a node that still owns slots then exits without
  lingering. If stuart's name doesn't resolve or answer, the node tries
  the address stuart was last seen at (where it answered, or where its
  tables come from): `kubectl delete -k` deletes stuart's Service before
  the pods get their SIGTERM, while stuart serves on at its pod's
  address; before, the nodes then drained until `--drain-timeout`.
- `--maxmemory` (phase 6): past this much memory, the commands Redis
  flags `denyoom` (`SET`, `SETNX`, `MSET`, `APPEND`, `INCR`, `DECR`,
  `INCRBY`, `DECRBY`, `INCRBYFLOAT`; since phase 7 also `HSET`, `HMSET`,
  `HSETNX`, `HINCRBY`, `HINCRBYFLOAT`, `SADD`, the `STORE` set commands, `LPUSH`, `RPUSH`, `LPUSHX`,
  `RPUSHX`, `LSET`, `LINSERT`, `LMOVE`, `RPOPLPUSH`) get Redis's `-OOM command not allowed when used
  memory > 'maxmemory'.`, after the arity and cluster checks and without
  counting as processed; reads, `DEL`, `GETDEL`, `EXPIRE`, `FLUSHALL` still
  run, so room can be made. Nothing is evicted: Redis's `noeviction`, the
  only policy (as a store that scales out, the answer to a full node is
  another node). A size as Redis takes it (`2gb`, `500mb`, `1000000`), or
  `N%` of the container's memory limit (cgroup v2 `memory.max`, else v1
  `memory.limit_in_bytes`; refused at start without one): Kubernetes gives
  `--maxmemory 75%`, leaving a quarter for what isn't counted and for the
  buffers. Default 0: no limit, as in Redis. `CONFIG SET maxmemory` changes
  it, `CONFIG GET maxmemory` and `maxmemory-policy` report it; `INFO
  memory` has Redis's main fields (`used_memory`, `used_memory_rss`,
  `used_memory_peak` sampled every 100 ms, `used_memory_startup`,
  `used_memory_dataset`, `maxmemory`, `maxmemory_policy`,
  `mem_fragmentation_ratio`, each size also `_human`; and ours, `mem_map_growth`: the most the next new key may allocate at once, a table of keys growing). A write is also
  refused, below the limit, when the key it may add could make a table
  grow past it (a few kB: the keys sit in 16384 tables; the expiry index,
  one `Vec`, doubles by 8 bytes a key with a TTL); Redis likewise refuses
  to expand a dict past `maxmemory`. In
  cluster mode a node copying slots moving in waits, reading nothing more,
  while the copy would take it past its limit, and stuart moves nothing to
  a node at 90%.
- `--metrics PORT | HOST:PORT` (phase 6): serves HTTP there, for
  Prometheus and Kubernetes' probes (off by default; the manifests use
  9121, redis_exporter's port). `GET /metrics` is Prometheus's text format:
  `speedvault_busy_seconds_total` (the reactor's busy time: its rate is the
  share of its core the node needs, what the HPA scales on; the
  container's CPU can't say, busy-polling keeps it near 100%),
  `speedvault_cpu_seconds_total`, `speedvault_memory_used_bytes`,
  `_max_bytes`, `_used_ratio` (the HPA's other input), `_peak_bytes`,
  `_rss_bytes`, `speedvault_commands_processed_total`,
  `speedvault_commands_refused_oom_total`, `speedvault_clients`,
  `speedvault_connections_received_total`, `speedvault_auth_failures_total`, `speedvault_keys`,
  `speedvault_keys_expiring`, `speedvault_uptime_seconds`,
  `speedvault_info{version,mode,node_id}`, and in cluster mode
  `speedvault_cluster_slots`, `speedvault_cluster_slots_migrating{direction="out|in"}`,
  `speedvault_cluster_epoch`, `speedvault_cluster_leaving`,
  `speedvault_migrate_{keys_sent,keys_imported,imports_refused}_total`
  and `speedvault_redirects_total{kind="moved|ask|tryagain"}`. `GET
  /healthz` answers `200` while the reactor runs (liveness); `GET /ready`
  `200` when the node should get traffic (readiness): standalone always,
  in cluster mode once stuart took its join (without `--stuart`: once it
  has a table), and a replica only while synced with its primary, never
  while leaving (`503` and the reason).
- `--cluster` is cluster mode, Redis Cluster's protocol (see "Cluster
  mode" below). `--announce` is the address clients reach the node at, as
  Redis's `cluster-announce-ip` and `-port` (default: where it listens;
  required when it listens on all interfaces, since `0.0.0.0` reaches
  nobody). `--stuart HOST:PORT` joins stuart: a thread of its own sends
  `STUART JOIN id host port [NAME name] [ZONE zone]` every second until stuart
  answers `+OK` (resolving the name again each time, so a Kubernetes
  Service that doesn't resolve yet is fine); stuart then pushes the table.
  `--name` is a name that stays when the node restarts at another address
  (on Kubernetes, the pod's name, while it announces its pod's address):
  stuart takes a node joining with another's name for its replacement, as
  one at its address. `--zone` is the machine the node runs on (on
  Kubernetes its node's name): stuart puts a replica on another zone than
  its primary where it can (phase 8). Without `--cluster` the node is standalone, as
  before.
- `--migrate-batch` (default 100) and `--migrate-pause` (default 50 µs):
  how the copy of slots moving away is sent, see [cluster mode](#cluster-mode): batches
  of that many keys, the pause after each only if clients were served
  since the one before, so that a busy source keeps most of its
  throughput while an idle one copies at full speed (see [performance](performance.md)).
  `--migrate-chunk` (default 4096): the elements a batch holds at most; a
  larger hash, set or list goes in parts of that many.

```sh
cargo run --release -p dbnode
redis-cli -p 7000 SET k v
```

- `conn.rs`: one fiber per connection. The requests one `read` brought in
  run back to back and their replies leave in one `write` (flushed early
  past 64 KiB), after the other connections ready in the same turn have run
  (`yield_now`): the replies of a turn leave together, as Redis writes them
  just before it sleeps, so clients aren't woken once per reply. Partial requests wait in the buffer, which grows for large
  values and shrinks back afterwards. A protocol error is answered, then the
  connection closed, as Redis does.
- `command.rs`: the command table (with subcommands and key positions),
  Redis's arity rules and error texts, and the data commands. `execute`
  checks a request in Redis's order (the command, its arity, the password,
  the slot, memory, the lease), then runs it (`run`), or queues it after
  `MULTI`.
- `multi.rs`: transactions, as Redis's `multi.c`. A command refused while
  queued flags the transaction (`-EXECABORT` at `EXEC`); `EXEC` is checked
  as one command of every queued key (one slot, served here: Redis's
  `getNodeByQuery` takes it so), then runs the queue through `run`
  without suspending: the slot check's waits happen before, `WAIT`
  answers at once. The stream gets the transaction between `MULTI` and
  `EXEC` (only once it writes); a replica's fiber waits for its `EXEC`
  before applying any of it, all in one go, and a slot sync never gets the
  markers. `WATCH`: who watches which key is in `client.rs`; the store
  records the keys writes touch (its `Touch`es, where Redis calls
  `signalModifiedKey`) only while some key is watched, and `touched`
  drains them after each command, each expiry sweep and each piece of the
  stream a replica applies. `FLUSHALL`, a slot trimmed and a full resync
  touch the watched keys that were there.
- `memory.rs`: the memory count. A global allocator wrapping the system
  one keeps the bytes allocated and not freed (`malloc_usable_size` of
  each block on Linux, so allocator rounding counts), as Redis's zmalloc
  does for `used_memory`: keys, values, the map and its spare room, the
  indexes, client buffers. Nothing reports its own size, so nothing can
  forget to; the cost is a relaxed atomic add per allocation and free (the
  node's helper threads allocate too). Not counted: fiber stacks (`mmap`)
  and fragmentation, which the RSS shows. Also the `--maxmemory` sizes and
  the cgroup limit.
- `http.rs`: the HTTP responder behind `--metrics`, used by stuart too:
  a fiber per connection reads one request head (8 KiB at most, within
  5 s, so a silent client holds only its own fiber), answers `GET` or
  `HEAD` of `/metrics`, `/healthz`, `/ready` (`404`, `405` and `400`
  otherwise) and closes the connection, after reading what else the
  client sent for up to a second (closing with unread bytes would reset
  the connection, and the client could lose the reply). No HTTP library:
  a scrape is one request. Also the text format's writer, with label
  escaping. `metrics.rs`: the node's page and readiness.
- `admin.rs`: `HELLO`, `AUTH`, `CLIENT`, `COMMAND`, `INFO`, `CONFIG GET`
  and `SET` (`maxmemory` and `requirepass`). The node
  presents itself as Redis 8.10.2 (`HELLO`'s `version`, `INFO`'s
  `redis_version`), since clients pick features by it; `INFO` also has
  `speedvault_version`. Fields the node doesn't track
  are fixed: zero memory counters in `CLIENT LIST`, no flags, ACL categories
  or docs in `COMMAND`.
- `auth.rs`: the password (`--requirepass-file`, `--requirepass`), Redis's
  `requirepass`, the one password of its `default` user: before a client
  has logged in, `execute` refuses every command but `AUTH`, `HELLO` and
  `QUIT` with `-NOAUTH` (after the unknown-command and arity checks, as
  Redis), the connection refuses `REPLSYNC` and `SLOTSYNC` likewise, and
  the parser takes requests of 10 arguments of 16 KiB at most (Redis's
  limits for clients that haven't logged in). A client connected while
  there was no password counts as logged in; changing the password keeps
  clients logged in. The comparison takes the same time wherever the
  passwords differ. In a cluster the nodes and stuart share it: each link
  sends `AUTH default <password>` first, in the same write as its first
  request (to stuart, a replica's to its primary, a slot copy's to its
  source, stuart's to the nodes). stuart answers `AUTH` and `HELLO` with
  these functions too.
- `cluster_mode.rs`: cluster mode: the slot table, the slot check, and
  `CLUSTER`, `READONLY`, `READWRITE`, `ASKING`. `admin.rs` has `ROLE` and
  `INFO replication` (phase 8), which say what the table makes the node.
- `repl.rs` (phase 8): the replication stream, every write's effect as
  RESP commands (the store's `Effect`s, keys reclaimed as expired as
  `DEL`, else a write that didn't fail as sent), with its id, offset and
  backlog (a 1 MiB ring). Nothing is recorded until it starts (the first
  replica). `repl/primary.rs`: `REPLSYNC`, the copy in batches and the
  fiber sending a replica its feed, with a `REPLPING seq` (outside the
  stream) every second and after each batch of a copy; a copy queues its
  next batch only while what it queued unacknowledged is within what the
  link drains in 250 ms. `repl/replica.rs`: the replica's fiber, following
  its primary, the stream applied, and `REPLACK offset|- seq` (the offset
  applied, `-` during a copy, and the last ping) at once for a ping, at
  most every millisecond for the stream, and every second. The lease
  runs 3 s from the sending of the newest ping a replica in it answered.
- `migrate.rs`: the fiber that sends the keys of slots migrating away.
- `client.rs`: the registry of connections behind `CLIENT LIST`, and of
  the keys they watch.
- `log.rs`: the logger; `main.rs`: arguments and signals.
- `lib.rs`: the accept loop, a panic handler that logs and keeps serving,
  and active expiry every 100 ms, which also gives the expiry index's room
  back after many deletions (`store` docs) and samples the memory peak.

`dbnode/tests/server.rs` runs a server per test over real TCP and checks
the exact reply bytes; `dbnode/tests/cluster.rs` does the same in cluster
mode.

## Cluster mode

A node in cluster mode gets a random 40-hex id at startup and knows only
itself, owning no slots: every key gets `-CLUSTERDOWN Hash slot not
served`, and `CLUSTER NODES` lists the node alone, as a fresh Redis node
does. stuart (phase 4 milestone 3) gives it the whole layout:

```
CLUSTER SETTABLE <epoch> NODES <n> <id> <host> <port> ... RANGES <m> <start> <end> <id | -> ...
                 [MIGRATING <k> <slot> <from> <to> ...] [REPLICAS <r> <id> <primary> ...]
                 [FAILED <f> <id> ...]
```

The ranges must name every slot once (`-`: unassigned), and the nodes
must include this one. `SYNCING slot from to` moves a slot by snapshot
(below). `MIGRATING slot from to` is the state Redis's `CLUSTER SETSLOT
slot MIGRATING` on the one and `IMPORTING` on the other leave, with
Redis's replies (below); nothing moves such a slot's keys (phase 5 did;
stuart makes no such table since phase 8). A replica (phase 8) names its primary;
`FAILED` lists the nodes stuart found down for 5 s: a primary takes
writes without a replica marked failed (the lease ignores it), and clients
aren't sent to it. `CLUSTER FENCE seconds` (ours, stuart's failover) makes
a replica acknowledge nothing for that long (0 lifts it; a table making
it a primary ends it), so its primary's lease runs out; it replies
`[offset, 1 if its copy is complete]`. `CLUSTER PAUSEWRITES ms` (ours,
stuart's handover) makes a primary's writes wait for `ms` (at most 10 s; 0
ends it, and a new pause replaces the one before), or until a table leaves
it no slots, when they are redirected; reads go on, and the reply is its
replication offset. A table is taken only if its epoch is newer than
the current one; the same table again with the same epoch is `+OK` (stuart
may push twice), a different one with the same epoch or an older one is an
error, and so is a malformed one, which changes nothing.

Before a command with keys runs, after the arity check, its keys (from the
command table's key positions) are checked as Redis 7.0.15 checks them with
`cluster-require-full-coverage no`:

1. The first key's slot is unassigned: `-CLUSTERDOWN Hash slot not served`.
2. Another key is in another slot: `-CROSSSLOT Keys in request don't hash
   to the same slot` (the same key twice is fine; a `{tag}` keeps keys in
   one slot).
3. Another node owns the slot: `-MOVED <slot> <host>:<port>`.

While a slot is `MIGRATING`, the source (still its owner) serves a request whose
keys it all has, sends one with none of them to the target (`-ASK <slot>
<host>:<port>`), and refuses one with some (`-TRYAGAIN Multiple keys
request during rehashing of slot`). The target serves the slot only to a
request right after `ASKING` (which holds for one command, whatever it
is), refusing several keys it doesn't all have with `-TRYAGAIN`; without
`ASKING` it redirects to the owner (`-MOVED`). A key that has expired
counts as missing, and isn't reclaimed by the check. `CLUSTER NODES` shows
`[slot->-<to>]` and `[slot-<-<from>]` on the node's own line. Redis 7.0.15
in the same state answers the same (`cluster_difftest.py migrating`).

A slot moving by snapshot (`SYNCING`, phase 8) is served by its owner
alone until the cutover: the target copies it (`SLOTSYNC`, ours: the
slots' keys in batches of `--migrate-batch` keys and at most 1 MiB, a
larger collection in parts of `--migrate-chunk` elements, the source
pausing `--migrate-pause` after each batch if commands with keys were
served meanwhile; then the writes to them) and serves none of it
(`-MOVED` to the owner). stuart then pauses the slots' writes at the
owner (`CLUSTER PAUSESLOTS`, ours: they wait, reads go on) and gives the
target the slots once it has every write; there they wait until the
owner's last frame. `CLUSTER SLOTSYNCS` (ours) lists a node's copies. The
owner deletes the slots' keys in the background afterwards. `INFO
cluster` adds `migrate_keys_sent` and `migrate_keys_imported` (the keys
the slots held at their cutovers), `migrate_imports_refused` (times a copy
waited for room under `maxmemory`), and the redirects the node has sent
clients, `cluster_redirects_moved`, `cluster_redirects_ask` and
`cluster_redirects_tryagain` (Redis has no such counters;
`migrate_check.py redirects` reads them).

A refused command doesn't count in `total_commands_processed`. Commands
without keys (`PING`, `DBSIZE`, `FLUSHDB`, `INFO`...) act on the node they
are sent to. `SELECT` takes only 0. `CLUSTER MYID SLOTS SHARDS NODES INFO
KEYSLOT COUNTKEYSINSLOT GETKEYSINSLOT` answer from the table and the
store, which in cluster mode keeps its keys by slot (`COUNTKEYSINSLOT` is
O(1); a table per slot, see the `store` docs); `READONLY` and `READWRITE`
are accepted, `ASKING` as above.

A table may make the node a replica (phase 8): it owns no slots, so it
redirects every key to its primary with `-MOVED`, writes and reads, also
after `READONLY` until replicas copy their primaries (milestone 3). `ROLE`
is `slave host port connect -1` and `INFO replication` has
`master_link_status:down`, as for a Redis replica that never reached its
primary; a primary's is `master 0 []`, no replica attached, no backlog,
and `master_replid` is random at each start. `HELLO` says `role replica`. `HELLO` and `INFO` say `cluster` mode; `INFO
cluster` has `cluster_enabled:1`. Without `--cluster`, these commands
answer `-ERR This instance has cluster support disabled`, as Redis does.

`CLUSTER SETTABLE` takes a table from anyone who can connect, as Redis's
`CLUSTER ADDSLOTS` does: keep the nodes on a network only clients and
stuart can reach.

# `stuart`

The steward: `stuart [PORT | HOST:PORT] --state FILE [--auto] [--create-when N]
[--replicas N] [--fail-after SECONDS] [--moves-per-source N] [--max-moves N]
[--loglevel LEVEL] [--metrics PORT | HOST:PORT]` listens on
`127.0.0.1:7600` by default. It holds the layout of a cluster of dbnodes,
moves slots between them, and is never in the data path.

```sh
speedvault-stuart --state /var/lib/stuart/state &
speedvault-server 127.0.0.1:7001 --cluster --stuart 127.0.0.1:7600 &   # and 7002, 7003
redis-cli -p 7600 STUART NODES      # wait until they're up
redis-cli -p 7600 STUART CREATE     # 0-5460, 5461-10922, 10923-16383, in join order
redis-cli -c -p 7600 SET foo bar    # MOVED to the owner of slot 12182, then OK
```

- `STUART JOIN id host port [NAME name] [ZONE zone]` (from the nodes): stuart
  remembers the node (and its name and zone, in the state file) and starts a keeper
  for it. A known node at a new address moves; a new id at an address
  another node announced, or with the name another node joined with,
  replaces that node (it restarted, without its keys; on Kubernetes a pod
  comes back under its name at a new address), and the slots it owned
  become unassigned: lost, as
  `STUART NODES` reports (`STUART ASSIGN` gives them out again). After
  `CREATE` a joining node is added to the table without slots (`STUART
  REBALANCE` gives it some), or with `--replicas N` as a replica of a
  primary that has fewer than N.
- `--replicas N` (phase 8, default 0): `CREATE` makes the first
  `ceil(k / (1 + N))` of the k joined nodes primaries and splits the slots
  over them; each other node becomes the replica of a primary with fewer
  than N, one on another zone first (`ZONE`), then the one with the
  fewest, then the first joined. Only primaries get slots (`MOVE` and
  `ASSIGN` refuse a replica). A replica whose primary is removed goes to
  another primary that lacks one, or becomes a primary. Raising N later
  demotes primaries with `--auto` (see there); lowering it changes
  nobody's role, only what nodes joining become.
- `--fail-after SECONDS` (phase 8, default 5): a replica stuart can't reach
  this long is marked failed (its primary writes without it) until it
  answers; a primary is failed over to its replica. stuart fences the
  replicas (`CLUSTER FENCE`), waits the primary's 3 s lease, and promotes
  the most advanced one with a complete copy; the old primary is a
  failed master without slots until it answers, then its successor's
  replica. A pod that joins under a primary's name or address is a new
  process: its replica is promoted at once, and the pod joins as that
  one's replica. A primary answering during the wait isn't failed over.
- `STUART CREATE` splits the 16384 slots over the joined nodes in join
  order, as `redis-cli --cluster create` does (contiguous ranges, the same
  rounding), epoch 1. Once only: after that slots change hands with
  `MOVE`, `REBALANCE`, `DRAIN` and `ASSIGN`. With `--create-when N` stuart does it by itself when the
  N-th node joins (or at start, if the state file already holds N nodes),
  if there's no table yet: for Docker Compose and Kubernetes, where nobody
  types it.
- `STUART ASSIGN start end id` gives unassigned slots (a replaced node's,
  lost) to a node, in a new epoch: the way back after a node restarted,
  e.g. to the restarted node itself. Slots another node serves are refused,
  since their keys would stay behind: `STUART MOVE` moves those.
- `STUART MOVE start end id` (phase 5): moves the range's slots to node
  `id` with their keys, while clients use them; replies how many slots
  were queued (those `id` owns don't move). Every slot must be owned and
  not moving yet. stuart moves a slot by snapshot (phase 8; phase 5 moved
  it key by key), in two tables: one where the target copies it while the
  source serves it, then, once stuart has paused its writes at the source
  (`CLUSTER PAUSESLOTS`) and the target has them all, one where the target
  owns it (see "Phase 8 plan"). The first goes to the source before the
  other nodes, the second to the target, so clients aren't sent back and
  forth. At most `--moves-per-source` slots (default 128) move out of one
  node at once, and `--max-moves` (default 512) in all; a node is a source
  or a target at a time. The slots moving from one node to another share
  one copy and one cutover, and each table costs stuart an `fsync`: a
  drain of 4096 slots takes 0.5 s at 128 per source against 1.7 s at 32
  (`tools/bench/results/2026-10-25-0b36234`). No move starts to or from a
  node that is down, and a move whose source or target goes down is
  called off and queued again (the source still has the slots).
- `STUART REBALANCE`: plans the moves again (the queue is replaced) so
  that, once the moves under way and the queued ones end, the slots are
  even over the joined nodes that aren't leaving, moving as few as it can:
  each node ends with `assigned / n` slots or one more, the extra ones
  staying with the nodes that had the most, and leaving nodes with none.
  A node gives its highest slots, and receives runs; slots moving now go
  on as they are. Replies how many slots are queued.
- `STUART DRAIN id`: queues moves of all of a node's slots to the others
  (not leaving), the ones with the fewest first, moving nothing else;
  slots queued to it aren't any more. Replies how many were queued. A
  slot moving to it when it's drained stays there (`DRAIN` again moves
  it; `LEAVE` does that by itself). Both are accepted while slots move.
- `STUART LEAVE id` (phase 6): the node is leaving. Its slots are drained
  (as `DRAIN` does, and again for slots that were on their way to it),
  then, once it holds nothing and took a table in which it holds nothing
  (so that it redirects clients for its old slots), it is removed from
  the table and from the nodes. Replies what it still holds (slots owned,
  moving to it, queued to it) plus one while it is a node: `0` once it has
  left, and for a node stuart doesn't know. A node calls it on SIGTERM
  (see `dbnode`) until it gets `0`. Before `CREATE` it leaves at
  once. Once every node is leaving, the cluster is being shut down:
  there's nowhere to keep the keys, and each gets `0` at once (stuart logs
  it). `STUART NODES` marks a leaving node `leaving`. A `LEAVE` (or
  `DRAIN`) is refused when the nodes not leaving haven't room for the
  node's keys below 90% of their `maxmemory` (`-ERR Not enough memory for
  node <id>'s keys on the other nodes: they take N bytes, the others have
  M ...`, logged): a node stopping then exits at once, its keys lost,
  rather than drain until Kubernetes kills it. Nodes without a limit, or
  whose memory stuart hasn't read, have room for anything.
  A primary with a replica that can take over (phase 8: up, not marked
  failed nor leaving) isn't drained, and its keys need no room: stuart
  pauses its writes (`CLUSTER PAUSEWRITES`, 10 s at most), waits up to
  1 s for its most advanced replica with a complete copy to reach the
  offset the primary answered (`CLUSTER FENCE 0`), and gives that replica
  the slots in a table that reaches it first; the old primary redirects
  the writes that waited, and is removed once it took that table. A
  replica that missed is paused for again 100 ms later; after 5 misses,
  with no replica left that can take over, or with one still copying from
  scratch after 60 s, the slots are drained instead. No move starts from
  or to a primary handing over. A replica leaving just goes (its keys are
  copies).
- `STUART FORGET id`: the node is gone for good (its pod was removed
  without leaving, its machine died). It is removed as a replaced node is:
  its slots become unassigned (lost), but a slot migrating from it goes to
  its target, and moves to it end. Replies how many slots were lost.
  stuart never does this by itself: it can't tell a dead node from one it
  can't reach, and giving away the slots of a node that still serves them
  would put keys on two nodes.
- `--auto` (phase 6): stuart keeps the slots even by itself. The table is
  made when the first node joins (or as `--create-when` says), and
  whenever a node joins, leaves, is replaced or forgotten, and every
  second while nothing moves, stuart gives unassigned slots to the nodes
  with the fewest (their keys are gone: nothing moves) and plans the
  moves as `REBALANCE` does. Nobody types a `STUART` command: nodes join
  and get slots, nodes leaving are drained and removed. Without `--auto`
  nothing moves unless asked, as before (`LEAVE` still drains and
  removes). With it, `MOVE` works but the next plan may undo it. With
  `--replicas N` (phase 8) it plans the roles first: with no node leaving
  and no primary failed, a primary beyond the `ceil(k / (1 + N))` that
  the k nodes need (so another lacks a replica), if the others have room
  for its keys, is demoted: the one with the fewest replicas, then the
  fewest slots, then the last joined, one at a time. It gets no slots and
  no new replica (`MOVE` to it is refused), its slots move away by
  snapshot, and once it took a table in which it holds nothing (at once
  if it's down) it becomes the replica of a primary lacking one, its own
  replicas going to other primaries first. A join calls the demotion off;
  a failover or a `LEAVE` of it ends it.
- `STUART MIGRATIONS`: a line per slot moving, `slot from -> to for=Ns`,
  and `queued: N`; empty when nothing moves.
- Full nodes (phase 6): a node using 90% of its `maxmemory` or more is
  full, counting as used what its next new key may allocate at once
  (`INFO memory`'s `mem_map_growth`): past the
  limit with it the node refuses new keys, however far below it is. The plans (`REBALANCE`, `DRAIN`, `LEAVE`, `--auto`) give it
  nothing (it may still give), its share going to the other nodes, and a
  queued move to it doesn't start; while queued moves wait for full nodes
  stuart plans again every second. A move under way to a node that fills
  up waits: its copy stops reading past the limit, and moves only go
  forward, so the slot stays with its source, which serves it, until the
  node has room (keys deleted or expired, a higher limit). No slot moves to a node before stuart has read
  its memory. stuart logs when a node becomes full and when it has room
  again; the remaining 10% is room for writes while moves under way end.
- `STUART NODES`: a line per node, `id host:port up|down since=Ns acked=E
  slots=N [replica-of=ID] [name=NAME] [zone=ZONE] [used=B max=B] [full] [leaving] [demoting] [failed] [error="..."]` (`acked`: the
  newest table epoch the node took; `used` and `max` its memory; `failed`:
  the table marks it so, a replica stuart couldn't reach for 5 s, until
  it answers again; `demoting`: a primary being made a replica, see
  `--auto`),
  and `unassigned slots: N` when some are. `STUART TABLE`: `epoch N` and
  the table as `CLUSTER NODES` prints it, or nil before `CREATE`.
- `CLUSTER SLOTS SHARDS NODES INFO KEYSLOT` answer from the table (before
  `CREATE`: the joined nodes, owning nothing), so stuart can be a cluster
  client's seed (redis-py `RedisCluster(port=7600)`). A data command gets
  `-MOVED` to the owner of its first key, so `redis-cli -c -p 7600` works.
  `HELLO`, `PING`, `INFO`, `COMMAND`, `CLIENT SETNAME|SETINFO|ID` are
  answered as clients expect on connect; `CLUSTER MYID` is an error
  (stuart is no node).
- `--metrics PORT | HOST:PORT` (phase 6): `/metrics`, `/healthz` and
  `/ready` over HTTP, as the node serves them: `stuart_nodes{state="up|down|leaving|demoting|full|failed"}`,
  `stuart_table_epoch`, `stuart_slots_moving`, `stuart_slots_queued`,
  `stuart_slots_unassigned`, `stuart_slots_moved_total`, `stuart_failovers_total`,
  `stuart_handovers_total`, `stuart_demotions_total`, `stuart_info{version,auto}`, `stuart_uptime_seconds`, and per node
  (labels `node` and `addr`) `stuart_node_up`, `stuart_node_slots`,
  `stuart_node_leaving`, `stuart_node_memory_used_bytes` and
  `_max_bytes` (as its keeper last read them: a scrape asks the nodes
  nothing). `/ready` is `200` once stuart serves.
- SIGTERM (phase 6): stuart serves on for the nodes stopping with it,
  since deleting a Kubernetes namespace (or `kubectl delete -k`) stops
  every pod at once and a leaving node exits only at stuart's `0`: it
  exits once every node has been leaving for 1.5 s (each was told to
  stop), or 5 s after the signal if no node is leaving (stuart alone is
  stopping), or after 15 s at most (nodes still leaving ask the next
  stuart). A second SIGTERM, or SIGINT, exits at once.

Every change (a join, `CREATE`) is applied to a copy of the state, which
is written to the state file (a temporary file, fsynced, renamed over the
old one, the directory fsynced) before it replaces the state, before the
reply and before any push: a crash leaves the old or the new state. A
restarted stuart reads the file and pushes the table again; the nodes
serve on meanwhile. The file is text:

```
stuart-state 1
node <id> <host> <port>        (one per node, join order)
leaving <id>                   (nodes leaving)
table <epoch>                  (after CREATE)
range <start> <end> <id | ->   (every slot once)
migrate <slot> <from> <to>     (slots moving)
first <id>                     (who gets this table first)
queue <slot> <to>              (slots waiting to move, in order)
```

A keeper fiber per node connects to it (2 s timeout, `connect_timeout`),
checks that `CLUSTER MYID` is the node that joined (a restarted node at
the same address is another node), pushes the table with `CLUSTER
SETTABLE` until the node took it (and again after every reconnect: the
same table is `+OK`), and reads its `INFO memory replication` on connect
and every second, which also says it's up; then it gives the node every
node's replication offset as last read (`CLUSTER SETOFFSETS`, when they
changed), for `CLUSTER SLOTS` and `SHARDS`. A node that refuses a
table is logged and reported, and gets it again with the next table or
reconnect. Host names are looked up on a thread of their own (as the
nodes do: `dbnode::resolve`), so that a name that is slow to fail (a
removed container's or pod's) doesn't hold stuart up. A stuart that lost its state
file starts afresh with epoch 1, which nodes holding a newer table refuse:
keep the file.

- `state.rs`: the state, joins, `CREATE`, `ASSIGN`, `MOVE`, the file;
  `keeper.rs`: the keepers; `mover.rs`: starts and ends the moves;
  `conn.rs`: client connections and commands; `metrics.rs`: `/metrics`;
  `lib.rs`: `serve`, the accept loop, saving before acting; `main.rs`:
  arguments (logging, signals and the HTTP responder come from the dbnode
  crate).

When a node is replaced (a new id at its address) or forgotten, a slot
migrating from it goes to the target, with the keys that got there; a
slot migrating to it stays with its source (the keys sent are gone with
the node); moves queued to it are dropped.

`stuart/tests/stuart.rs` runs the stuart binary with dbnodes on threads:
a cluster forming, growing and surviving a stuart restart (with a node
joining while stuart is down); a node that drops stuart's connections for
two seconds still getting the table; a restarted node replacing the old
one; the errors. `tools/compat/stuart_cluster.sh` does it with the release
binaries and `redis-cli -c`.
