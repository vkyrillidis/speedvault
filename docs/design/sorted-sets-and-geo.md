# Sorted sets and geo (phase 9)

Planned 2026-11-03. Redis's geo commands are sorted-set commands: a geo key
*is* a sorted set whose scores are 52-bit geohashes (`TYPE` says `zset`,
and clients use `ZREM`, `ZRANGE`, `ZCARD` on geo keys). So sorted sets come
first, geo is built on them. Reference: Redis 8.10.2, byte for byte, as for
the rest of the node.

## What Redis 8.10.2 does

**Encodings.** While a sorted set has at most `zset-max-listpack-entries`
(128) members of at most `zset-max-listpack-value` (64) bytes it's a
listpack of member, score pairs in order; past either limit it converts,
one way, to a skiplist plus a dict (`OBJECT ENCODING` `listpack` /
`skiplist`). The order a client sees is always the same, by score then
member bytes, so unlike hashes and sets the encoding changes no reply,
only `OBJECT ENCODING` and memory. `ZADD` and the `STORE` commands size the
new key up front (`zsetTypeCreate`, `zsetTypeMaybeConvert`): to be copied.

**Scores.** Doubles. Read by `string2d` (no leading space, every byte read,
NaN refused, `inf`/`-inf`/`+inf` taken) through `fast_float_strtod`: a port
of fast_float for up to 19 significant digits and a decimal exponent in
[-342, 308], glibc's `strtod` for the rest (hex floats among them). Only
`strtod` reports a range error, so `ZADD z 9e308 m` stores `inf` and
`ZADD z 1e400 m` is refused. A range bound (`zslParseRange`) is read up to
its first NUL byte, after white space, and an empty one is 0. Written by `d2string`:
`nan`, `inf`, `-inf`, `-0`, integers in the `long long` range as integers,
everything else by `fpconv_dtoa` (Grisu2: round-trips but isn't always the
shortest, and has its own exponent format), so `fpconv_dtoa` is ported
as it is, not replaced by Rust's `{}`. RESP3 sends a double (`,`).
`ZINCRBY` and `ZADD INCR` refuse a NaN result.

**Geo.**
- A point is longitude in [-180, 180] and latitude in [-85.05112878,
  85.05112878], each cut into 26 bits and interleaved (`geohash.c`), the
  52-bit result stored as the score.
- `GEOSEARCH` (`geohash_helper.c`): picks a geohash step from the radius
  or box (`geohashEstimateStepsByRadius`, made coarser near the poles), takes
  the centre cell and its 8 neighbours, drops neighbours the area can't
  reach, scans each cell as a score range, keeps the points inside the
  circle (haversine, earth radius 6372797.560856 m) or box. Without
  `ASC`/`DESC` the reply is in that scan order, so the port follows
  `geoGetPointsInRange` line by line.
- Distances are written with 4 decimals (`fixedpoint_d2string`), positions
  with `addReplyDouble` (the `d2string` above).
- `GEOHASH` strings use latitude [-90, 90], not the stored range: 11
  base32 characters, re-encoded from the decoded point.
- Maths through libm `sin cos asin sqrt`: Rust's `f64` methods call the
  platform's libm, glibc on Linux as Redis's build, so the same bits; a
  macOS build may differ in the last bit (noted, not chased).
- `GEORADIUS` and `GEORADIUSBYMEMBER` (deprecated, still used by older
  clients) have their `STORE`/`STOREDIST` key after a keyword: a key kind
  the command table can't express yet (`movable` only knows "a count, then
  keys").

## Decisions (taken 2026-11-03, the recommendations)

1. **Large sorted sets: counted B+ tree or skiplist.** Recommended: a B+
   tree ordered by (score, member) with element counts in the inner nodes,
   so `ZRANK` and `ZRANGE` by index are O(log n), beside the existing
   ordered table for member → score. Less per member than Redis's skiplist
   (a node there is its own allocation: member pointer, score, back
   pointer and 1.33 levels on average of 16 bytes each), and better cache
   behaviour, as a leaf holds many entries side by side; the same order either way. The alternative is the
   skiplist ported from `t_zset.c`: simplest to make match, more memory.
2. **Compact encoding from the start.** Recommended: yes. `OBJECT ENCODING`
   has to say `listpack` anyway, and phase 7 measured what skipping it costs
   (4–5× Redis's memory for small collections). `store::packed` already
   does the listpack-like buffer.
3. **Blocking pops (`BZPOPMIN BZPOPMAX BZMPOP`)** wait for the blocking
   machinery `BLPOP` needs; recommended: deferred with it.
4. **Deprecated `GEORADIUS*`:** recommended: all four, with a second
   `movable` kind (a key after a keyword, last one wins, as
   `georadiusGetKeys`) so cluster routing stays right.

## Milestones

| # | Milestone | Content | State |
|---|-----------|---------|-------|
| 1 | Groundwork | `Value::ZSet` (boxed, `Entry` stays its size); the packed small encoding and the large one (decision 1), tested against a `BTreeMap` model through conversion and removals; `string2d` and `d2string` with `fpconv_dtoa` ported, fuzzed against Redis 8 (`ZADD`/`ZSCORE` of random doubles, byte for byte); the generic commands type aware (`TYPE zset`, `OBJECT ENCODING`, `WRONGTYPE`, an emptied key deleted); the dump kind `zset` | done (2026-11-03): `store::double` (`string2d`, the range-bound reading, `d2string` with night-shift's `fpconv_dtoa` ported as it is), compared with Redis 8.10.2's own C (`fast_float_strtod.c`, `fpconv_dtoa.c`) on 2.3M generated inputs (decimal near fast_float's limits, hex floats, junk, NUL bytes, random doubles printed every way): every refusal, value and printed digit the same. Found: fast_float reports no range error, so `9e308` is a score (`inf`) and `1e400` isn't; `2^63` prints as `9223372036854776000`; a range bound of `""` is 0. `store::ztree`, the counted B+ tree (64 wide), tested against a sorted model through four levels and back to empty; `store::zset` (`listpack`: member and 8-byte score in turn; `skiplist`: the ordered table beside the tree, the tree's entries pointing at the table's blocks), against a `HashMap` model through the conversion; Miri on the pointer paths. A fifth value kind (the kind takes 3 bits of a key's flags); `TYPE`, `OBJECT ENCODING`, `WRONGTYPE`, `SCAN TYPE zset`, expiry, `DEL` checked in the store; dumps (scores as their 8 bytes, NaN refused), whole and in parts. Commands come in milestone 2, so nothing a client can reach yet. 30 injected bugs: 29 caught (one after a test was added: the sticky bit of a hex float past 16 digits), the 30th equivalent (a leading-space check the grammar made redundant, removed) |
| 2 | Single-key commands | `ZADD` (`NX XX GT LT CH INCR`) `ZINCRBY ZREM ZCARD ZSCORE ZMSCORE ZRANK ZREVRANK` (`WITHSCORE`) `ZCOUNT ZLEXCOUNT ZRANGE` (`BYSCORE BYLEX REV LIMIT WITHSCORES`) `ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZRANGESTORE ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX ZPOPMIN ZPOPMAX ZRANDMEMBER ZSCAN`; range syntax (`(`, `-inf`, `[`, `-`, `+`) and its errors as Redis; RESP2 and RESP3 | done (2026-11-03): `store::store::zset` (the writes), `ZSet`'s ranges (Redis's in-range pre-check, the first member from the front, the last from the back, a walk while packed so even a lex range over mixed scores answers as a listpack does), `dbnode::command::zset`; `HSCAN ... NOVALUES` on the way (the other scans refuse it as Redis does). difftest: 21 cases, 483 in all (478 pass, the 5 allowed as before); `cluster_difftest` with sorted sets and `ZRANGESTORE` across slots (12078/12078 the same); a random fuzz of every command against Redis 8.10.2 on RESP2 and RESP3, sets crossing the 128-member limit (lex ranges only over equal scores: past the limit Redis's answer then depends on its skiplist's random levels), 260,000 steps, no difference. Found: the packed encoding keeps an integral score as an integer, so `-0` is `0` there and `-0` in a large set; `ZSCAN` writes a large set's scores `%.17Lg` (`0.10000000000000001`), a packed one's as packed; `ZRANDMEMBER` with a count covering the set replies it last first (Redis walks a sorted set from its end there); `ZRANGE ... LIMIT 0 -1` is allowed by rank, -1 being the default; `ZPOPMIN key x` says "must be positive"; a restore must build large and pack at the end, as Redis's load does, or a large set's `-0` scores change. `redis-benchmark` (one run): `ZADD` 1.16× Redis 8.10.2 without pipelining, 1.27× at `-P 16`; `ZPOPMIN` 1.23×, 1.12×. 24 injected bugs, all caught (2 only after tests were added: an offset past the first member backwards, and the end search of a range falling in a gap) |
| 3 | Multi-key commands | `ZUNION ZINTER ZDIFF ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZINTERCARD ZMPOP` (`WEIGHTS AGGREGATE WITHSCORES`, sets taken as scores of 1 as Redis does); the `movable` kind extended to a count after a destination (`ZUNIONSTORE dest numkeys ...`); `CROSSSLOT` in cluster mode | done (2026-11-03): `Store::zcheck` (the keys' types, before the options, as Redis), `zcombine`, `zintercard`, `zstore_set`, `zmpop`; a command's `dest` (the destination first, then the keys after the count at `args[2]`, as Redis's key specs list them; none at all if the count isn't one; `COMMAND` shows `1 1 1`). As Redis: the inputs sorted smallest first (a stable sort, as glibc's qsort is for a few inputs) and summed in that order, which decides the last digit (`0.1 + 0.2 + 0.3`); a sorted set iterated from its end; a NaN sum or weighted score is 0; `MIN` and `MAX` keep the first of a tie (`0` or `-0`); `COUNT` adds each input's weight; a result built large, so a reply keeps `-0` and a stored one is packed if it fits. difftest: 8 cases, 491 in all (486 pass, 5 allowed); `cluster_difftest` with the `STORE` forms, `ZINTERCARD` and `ZMPOP` across slots (13806/13806); the fuzz with the multi-key commands, sets and missing keys among the inputs, random weights and aggregates, 240,000 steps, no difference. 18 injected bugs, all caught |
| 4 | Migration, replication, transactions | Sorted sets in `CLUSTER IMPORT` and `IMPORTPART` (chunks, hidden until whole), the per-element import estimate measured; replication of the new writes and parts; `WATCH` touches where Redis signals; `migrate_check` with a `ZADD`/`ZINCRBY` writer and sorted sets of 200000 moving under load | done (2026-11-03): no change was needed for sorted sets to move: since phase 8 a slot moves by the replicas' copy (`REPLKEY`, parts of `--migrate-chunk` elements), which knows a value only by its dump and its type, and waits for room by bytes, whatever the type (so there is no per-type estimate left to measure). Found and fixed: the stream's touched keys left a `STORE` command's destination out (milestone 3's key kind), so a `ZUNIONSTORE` into a key already sent, during a copy, never reached the copy; and the sorted set `STORE`s replaced an expired destination without reclaiming it as expired. Found, not a bug: a lex range over members of different scores answers differently in a packed copy than in its skiplist source, as at a Redis replica (which packs a set it loads), so the random replication tests use lex ranges only where they're defined. The replication random tests (stream, full copy, slot copy) with every sorted set write; the integration tests' writers and loaders with sorted sets (a large one in parts, `-0` among its scores); a deterministic test of a `STORE` into a key already sent; 27 `WATCH` cases, each sorted set write changing its key or not (`ZADD` of an equal score, `ZINCRBY 0`, an empty `ZRANGESTORE` onto a missing key leave it unmodified, as Redis); `cluster_difftest` all the same; `migrate_check` with a sorted set in each hot slot (`ZINCRBY` counters, `ZADD` of each written member, the original members with their scores, `-0` included): every scenario, and rebalance and drain with collections of 200000. 5 injected bugs: 4 caught (one after the deterministic test was added), the 5th equivalent for a replica (the `STORE` it receives replaces the destination anyway) |
| 5 | Geo | `store::geohash` (encode, decode, neighbours, step estimate, areas, distances) ported from `geohash.c`/`geohash_helper.c`; `GEOADD` (`NX XX CH`) `GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE` (`FROMMEMBER FROMLONLAT BYRADIUS BYBOX ASC DESC COUNT [ANY] WITHCOORD WITHDIST WITHHASH STOREDIST`), units `m km mi ft`, `GEORADIUS GEORADIUSBYMEMBER` and their `_RO` forms, the keyword key kind; difftest cases at the poles' limits, across the antimeridian, empty and whole-earth areas; a fuzz of random points and searches against Redis 8, unsorted order included | done (2026-11-03): `store::geohash` (`geohash.c`, `geohash_helper.c` and `geoGetPointsInRange`'s scan, line by line: the cells in Redis's order, each by score, a neighbour the same as the one before skipped, the centre's cell never), `ASC`/`DESC` as glibc 2.39's `qsort` (a stable top-down merge sort, checked: ties keep the scan order) or, for `COUNT` below what was found, Redis's own `pqsort` ported (not stable: ties land where Redis's do), distances with `fixedpoint_d2string`'s 4 decimals (`llrint`, so half to even; NaN or past `long long`, `-922337203685477.5808` as on x86), `dbnode::command::geo`. A second key kind for the command table (`store_from`): the key after the last `STORE`/`STOREDIST` from `args[5]` on, as `georadiusGetKeys` reads it (Redis's key specs are marked incomplete there, so its routing uses that procedure), for cluster routing, the replication stream's touched keys and `WATCH`. `GEORADIUS` and `GEORADIUSBYMEMBER` are writes, as in Redis, but one that stores nothing stays out of the replication stream, as there. Found: a score that was never a geohash is cast to 64 bits as x86 does (a negative one wraps, `-1` is (180, 85.05112878), past 2^64 or infinite is 0), checked on Redis 8.10.2 with `ZADD` then `GEOPOS`/`GEOHASH`; `%f` in the "invalid longitude,latitude pair" error writes every digit (`1e300` comes back 309 digits long), as Rust's `{:.6}` does; without the key, `GEORADIUSBYMEMBER` doesn't read its radius at all (only the options, for the reply's form); a stored result is built large then packed if it fits, so a `-0` score stays `-0` only in a set too large to pack (the fuzz found it: building it packed lost the sign when a long member came after); a point at longitude 180 or latitude 85.05112878 has an offset of 2^26, past the 52 bits, so no search ever finds it, and the cells don't wrap over the latitude limits (Redis 8.10.2 misses those points too: kept). Not copied: Redis resolves a command name with a NUL inside now and then (it hashes the whole name with a seed picked at each start but compares names as C strings). difftest: 16 geo cases and 13 `WATCH` cases, 547 in all (542 pass, the 5 allowed as before); `cluster_difftest` with the geo commands, `STORE` keys across slots and the last of two winning (16686/16686 the same); a random fuzz of every geo command against Redis 8.10.2 on RESP2 and RESP3 (points worldwide, clustered with repeats, at the limits; scores that were never geohashes; every search form, centre, size from 0 to whole-earth, unit, order, `COUNT`/`ANY`, store), 300,000 steps, no difference; the replication random tests with geo writes, a `GEORADIUS ... STORE` into a key already sent during a copy, a search that stores nothing kept out of the stream. 38 injected bugs: 37 caught (two after tests were added: a search that must find every point inside away from the limits, and a `-0` found before a long member), the 38th equivalent (the cell index's 32-bit wrap, hidden by the latitude clamp) |
| 6 | Benchmarks and docs | redis-benchmark `ZADD ZPOPMIN` against Redis 7.0.15, 8 and Valkey 9; `ZADD ZSCORE ZRANK ZRANGEBYSCORE` on 1M members, `GEOSEARCH` on 1M points; memory per member small and large against the three; README compatibility table and command list, CHANGELOG | done (2026-11-03): `tools/bench/zset_bench.sh` (`bench.sh --command` for commands of our own choosing; `zset_fill.py`), sorted sets in `collections_memory.py` and in `full_run.sh --types`; results in `tools/bench/results/2026-11-03-9c92ec3-zset/`, the numbers in `docs/performance.md`. Ours ahead of all three on every test: against Redis 8.10.2 1.04–1.44× without pipelining and 1.08–1.46× at `-P 16` (`ZRANK`, `ZADD` of a set of 1M, `GEOSEARCH` of 200 km gain most). Memory, first measured at 108 bytes a member of a 1M set (Redis 8.10.2: 67) and 23 of a small one (15.8): found and fixed, a full leaf split after its 65th entry, doubling its vector to 128 places that both halves kept (a full leaf now splits before it takes another entry, an inner node's vectors have room for their 65 children); splits in halves left the leaves half full for good when scores only grow (the last leaf, full, now keeps its entries and starts another, the same at the first; only at the ends, so no choice of scores makes a leaf per entry); the packed form wrote every score in 8 bytes (now an integral one in 1 to 7, as a listpack). Now 58.5 bytes a member of a 1M set in score order, 65.5 in a random order (Redis 8.10.2 67.4, Valkey 9.1.2 70.8, Redis 7.0.15 110), and 16.7 of a small one (15.8). Each change checked by the store tests (the leaves' fill and places in each order, and back down), the sorted set and geo difftest cases and both fuzzes |

Each milestone, as before: difftest cases against Redis 8.10.2 byte for
byte (`ZRANDMEMBER` compared by its properties), `cluster_difftest`
where keys cross slots, unit tests, injected bugs, focused commits.

**Risks.** `fpconv_dtoa` is the one place where "looks right" isn't enough:
Grisu2 output differs from the shortest form for some doubles, hence the
fuzz in milestone 1. The large sorted set's member index is the ordered
table, so a 1M-member key inherits its doubling stall (12.5 ms measured
for hashes; the fix, incremental rehashing, is still open).
