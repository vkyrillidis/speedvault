# Hashes, sets, lists, and `SCAN` (phase 7)

Decided 2026-10-21: hashes, sets and lists now; sorted sets, the blocking
list commands (`BLPOP` and the others, which need one client's push to wake
another) and pub/sub later; `SCAN`, `HSCAN`, `SSCAN` and `KEYS` now. Plain
Rust collections first, compact encodings (Redis's listpack) as a later
optimisation. Replies as close to Redis 7.0.15 as possible, order included,
with the storage chosen for speed.

**Reply order.** Redis 7.0.15 has three orders a client can see:

| Type | While small | Past the thresholds |
|------|-------------|---------------------|
| Hash | listpack: insertion order (≤ 512 fields by default, 128 with the shipped `redis.conf`, each field and value ≤ 64 bytes) | hashtable: the dict's order, randomly seeded per process |
| Set  | intset: ascending numbers (only integers, ≤ 512 members) | hashtable: the dict's order |
| List | always in list order | |

So a hash or set is an **insertion-ordered table**: the entries in a `Vec`,
and an index over it (open addressing over `u32` positions, each entry
keeping its hash; no key stored twice). Iterating walks the `Vec`, which is
faster than a hash map's buckets. While Redis would still have the compact
encoding, a deletion shifts the entries after it (O(n), n ≤ 512) so the
order stays Redis's exactly; once Redis would have converted (one way, as in
Redis) it's a `swap_remove`, O(1), and the order is as unspecified as
Redis's. A set of integers keeps a flag while it's intset-like and its
members are read out sorted. `OBJECT ENCODING` reports what Redis would
(`listpack`/`hashtable`, `intset`/`hashtable`, `quicklist`), from the same
flags. A list is a `VecDeque<Vec<u8>>`. As in Redis, a collection emptied
by a command deletes its key.

**What `SCAN` needs.** A cursor that guarantees what Redis's does: every key
present for the whole scan is returned at least once, possibly more. The
store's dense `Vec`s give that when walked from the end: removal is a
`swap_remove`, which only moves the last element (already returned, or not
yet reached) into a hole, and new keys land at the end (not returned, which
is allowed). The cursor is a position in that walk.
- The keyspace: in cluster mode every key already sits in its slot's
  list, so the cursor is (slot, position). Standalone nodes have no such
  list; they'd get one (a single list, the same code). Its cost was
  measured in phase 5: 16 bytes a key, `DEL` about 25% slower at `-P 16`.
  So standalone `SCAN` is behind that index, always on (to be measured
  again; `KEYS` needs no index). Measured in milestone 5: 18 bytes a key
  and `DEL` 18–28% slower at `-P 16`, so a standalone node builds the list
  at its first `SCAN` instead (17 ms for 950k keys) and pays nothing until
  then.
- `HSCAN`, `SSCAN`: while compact, everything in one reply with cursor 0,
  as Redis does for listpack and intset (so the order matches too); past
  that, the same walk over the table's `Vec`.
- Options: `MATCH` (Redis's glob: `* ? [abc] [^a-z] \x`), `COUNT`
  (default 10), `TYPE` for `SCAN`.

**Migration.** `CLUSTER IMPORT` sends `[key ttl value]` today: it gets a type
(`[key ttl type payload]`), and a collection larger than a threshold is cut
into chunks over several batches, so that a node, single threaded, never
spends long on one key. The first chunk creates the key, the last one sets
its TTL; at the target the key is hidden (and can't be written through
`ASKING`) until it's complete; at the source it stays frozen until the last
chunk is acknowledged, as keys in flight are now. The memory check of an
import gets a per-type estimate.

Milestones:

| # | Milestone | Content | State |
|---|-----------|---------|-------|
| 1 | Groundwork | `Value::Hash`, `Set`, `List`; the ordered table with its tests; the generic commands type aware (`TYPE`, `DEL`, `EXISTS`, the expire commands), string commands `WRONGTYPE` on collections, an emptied collection deletes its key; `OBJECT ENCODING` (and `REFCOUNT`, `IDLETIME`, `FREQ` as far as they make sense); a typed export instead of the string-only `peek` | done (typed export in): `Value::Hash/Set/List` (boxed, an `Entry` stays 40 bytes); `store::table`, the ordered table (no index up to 8 entries; tested against a `HashMap` through growth, both removals and shrinking); the encodings Redis would use (`collections.rs`); `OBJECT ENCODING` only (strings `int`/`embstr`/`raw` too; `REFCOUNT`, `IDLETIME`, `FREQ` left out: the store has no sharing or LRU clock). Found: Redis's built-in `hash-max-listpack-entries` is 512 (`redis.conf` says 128) |
| 2 | Hashes | `HSET HMSET HSETNX HGET HMGET HDEL HEXISTS HLEN HSTRLEN HKEYS HVALS HGETALL HINCRBY HINCRBYFLOAT HRANDFIELD` | done but `HINCRBYFLOAT` and `HRANDFIELD`: 9 difftest cases byte for byte, order included, RESP2 and RESP3. Found: `HSETNX` on an existing field converts nothing |
| 3 | Sets | `SADD SREM SISMEMBER SMISMEMBER SCARD SMEMBERS SPOP SRANDMEMBER SMOVE SINTER SUNION SDIFF SINTERSTORE SUNIONSTORE SDIFFSTORE SINTERCARD` (`CROSSSLOT` across slots in cluster mode) | done but `SINTERCARD` (its keys follow `numkeys`, which the command table's key positions can't express yet): 11 difftest cases, dict-ordered replies compared sorted, random picks by length; `SPOP`/`SRANDMEMBER` from a seeded xorshift64*. Found: `SINTER` checks every key's type, even after a missing key |
| 4 | Lists | `LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP` (with a count) `LLEN LRANGE LINDEX LSET LINSERT LREM LTRIM LPOS LMOVE RPOPLPUSH LMPOP` | done but `LMPOP` (`numkeys` as above): 8 difftest cases; `LMOVE` of a key onto itself rotates in place and keeps the TTL. Found: Redis 7.0.15 takes `LPOS ... RANK -9223372036854775808` as -1 |
| 5 | `SCAN` | `SCAN` (`MATCH COUNT TYPE`), `HSCAN`, `SSCAN`, `KEYS`; the key index for standalone nodes, its cost measured | done: cursors walk the dense lists from the end (the guarantee tested with deletions and insertions between calls, by slot and standalone); compact hashes and intsets whole with cursor 0 in their order, as Redis; `store::glob`, Redis 7.0's `stringmatchlen` ported quirks and all (a lone `*` is "everything", as `KEYS`/`SCAN` take it: Redis's matcher doesn't match `*` to an empty key); the cursor read as `strtoul` reads it. Standalone, the key list is built by the first `SCAN` (kept from the start it cost 18 bytes a key and `DEL` 18–28% at `-P 16`; built lazily, 17 ms for 950k keys, nothing before). difftest: 8 cases, 319 pass, 3 allowed; `cluster_difftest`: the types and `SCAN`/`KEYS` per node beside a Redis 7.0.15 Cluster (6174/6174), redis-py `RedisCluster` `scan_iter`/`hscan_iter`/`sscan_iter` (229/229); `redis-cli --scan`, `--bigkeys`; redis-py's iterators miss nothing while another client deletes and adds. 12 injected bugs all caught |
| 6 | Migration | Typed `CLUSTER IMPORT`, chunks, hidden until complete, per-type memory estimate; `migrate_check` with `HINCRBY`, `SADD`, `RPUSH` writers and large collections moving under load | done: `CLUSTER IMPORT slot n [key ttl type value]...`, collections as their elements in order, rebuilt at the target. A batch also stops at `--migrate-chunk` elements (default 4096; a string is one); a larger hash, set or list moves alone in parts, `CLUSTER IMPORTPART slot key type total offset payload ttl`, one a round trip with the pause between, frozen at the source until the last part is acknowledged (commands touching it wait, as for any key in flight), built outside the store at the target so nothing sees it until whole, then freed at the source a part at a time. The first part is refused if the whole, estimated from it, wouldn't fit; a part refused later (the target filled up) gives the move of that key up: it's writable again at once and sent from the start later (decided 2026-10-22). A dropped connection or a table no longer importing the slot drops what the target staged. Per-element memory measured: 75–110 bytes beyond the payload for a hash's field and value, 46–68 for a set's member, 29–40 for a list's element (the estimate takes the higher). Part cost (1M elements, 4096 a part): 0.07 ms to dump, 0.32 ms to append a hash part at the target, 0.08 ms a list part; 16384 a part takes 1.3 ms. Found: a hash of ~1M fields stalls ~3 ms when its table doubles, whatever the part size (any `HSET` that grows it does too; Redis rehashes incrementally). `migrate_check`: a hash, set and list in each hot slot (`--collection`, default 20000) with `HINCRBY`/`SADD`/`RPUSH` writers, every element checked; rebalance and drain pass with 200000 (588 parts). 14 injected bugs all caught. Later (decided 2026-10-22): reads from a frozen key; a slot snapshot plus change stream instead of freezing, as Valkey 9 and Dragonfly do, with replication (phase 8) |
| 7 | Benchmarks and docs | redis-benchmark's `LPUSH RPUSH LPOP RPOP SADD HSET SPOP LRANGE_100..600` against Redis 7.0.15, 8 and Valkey 9; memory per element against Redis (where skipping listpack shows); README | done (`tools/bench/full_run.sh --types`, `tools/bench/collections_memory.py`, results in `tools/bench/results/2026-10-22-types/`; see Performance): 1.42–1.57× Redis 7.0.15 without pipelining (Redis 8 and Valkey 9: 1.02–1.27×), 1.06–1.65× at `-P 16`; `LRANGE_500/600` without pipelining bound by redis-benchmark's client for all four servers. Memory: 5× Redis for small hashes, 4× for lists, level with 7.0.15 for sets: where the compact encodings would pay |

Each milestone: difftest cases against Redis 7.0.15 byte for byte (replies
whose order Redis leaves unspecified, `SPOP`, `SRANDMEMBER`, `HRANDFIELD`
and large `SMEMBERS`/`HGETALL`, compared sorted or by their properties),
unit tests, injected bugs, focused commits.

**Compact encodings** (decided 2026-10-22, after milestone 7 measured 5×
Redis's memory for small hashes and 4× for lists): the storage follows the
encoding the store already tracks, so replies don't change.

| # | Step | State |
|---|------|-------|
| 1 | Small hashes packed: while Redis would keep a listpack, fields and values back to back in one buffer (`store::packed`), found by a walk as in Redis | done: 112 → 22.5 bytes a field (Redis 21.2); 500-field hashes, random `HSET`/`HGET`, 1.10–1.12× Redis; 7 injected bugs caught |
| 2 | Lists as packed nodes of 8 KiB (Redis's quicklist with its default `list-max-listpack-size -2`); each element's length also after it, so a node reads backwards (`RPOP`) | done: 46.8 → 13.3 bytes an element in small lists (Redis 11.9), 40.8 → 10.2 in a 1M list (Redis 10.1); a random test against a `VecDeque` through 20+ nodes; 8 injected bugs caught |
| 3 | Intsets packed in numeric order (canonical strings, compared without parsing); small sets of strings (≤ 128 members of ≤ 64 bytes, Redis 7.2's limits) packed while still `hashtable` to 7.0.15 | done: small sets 67.5 → 13.2 bytes a member (Redis 7.0.15: 59.6, Redis 8: 13.2), intsets 6.1 (Redis's 16-bit integers: 2.9); 6 of 7 injected bugs caught, the 7th equivalent |
| 4 | Large hashes and sets: one allocation per entry, a smaller slot | not started: decided from the numbers (large hashes 98.8 bytes a field, Redis 7.0.15 72.4, Redis 8 38.1) |
| 5 | Measure and document | done (`tools/bench/results/2026-10-22-compact/`): every write and pop still ahead of Redis 7.0.15 (1.18–1.61×); LPUSH moves the front node's bytes, as Redis does (1.25× Redis at `-P 16`, RPUSH 1.48×) |
