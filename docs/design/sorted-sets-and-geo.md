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

**Scores.** Doubles. Read by `string2d` (`strtod`, no leading space, every
byte read, NaN refused, `inf`/`-inf`/`+inf` taken). Written by `d2string`:
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
| 1 | Groundwork | `Value::ZSet` (boxed, `Entry` stays its size); the packed small encoding and the large one (decision 1), tested against a `BTreeMap` model through conversion and removals; `string2d` and `d2string` with `fpconv_dtoa` ported, fuzzed against Redis 8 (`ZADD`/`ZSCORE` of random doubles, byte for byte); the generic commands type aware (`TYPE zset`, `OBJECT ENCODING`, `WRONGTYPE`, an emptied key deleted); the dump kind `zset` | planned |
| 2 | Single-key commands | `ZADD` (`NX XX GT LT CH INCR`) `ZINCRBY ZREM ZCARD ZSCORE ZMSCORE ZRANK ZREVRANK` (`WITHSCORE`) `ZCOUNT ZLEXCOUNT ZRANGE` (`BYSCORE BYLEX REV LIMIT WITHSCORES`) `ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZREVRANGEBYLEX ZRANGESTORE ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX ZPOPMIN ZPOPMAX ZRANDMEMBER ZSCAN`; range syntax (`(`, `-inf`, `[`, `-`, `+`) and its errors as Redis; RESP2 and RESP3 | planned |
| 3 | Multi-key commands | `ZUNION ZINTER ZDIFF ZUNIONSTORE ZINTERSTORE ZDIFFSTORE ZINTERCARD ZMPOP` (`WEIGHTS AGGREGATE WITHSCORES`, sets taken as scores of 1 as Redis does); the `movable` kind extended to a count after a destination (`ZUNIONSTORE dest numkeys ...`); `CROSSSLOT` in cluster mode | planned |
| 4 | Migration, replication, transactions | Sorted sets in `CLUSTER IMPORT` and `IMPORTPART` (chunks, hidden until whole), the per-element import estimate measured; replication of the new writes and parts; `WATCH` touches where Redis signals; `migrate_check` with a `ZADD`/`ZINCRBY` writer and sorted sets of 200000 moving under load | planned |
| 5 | Geo | `store::geohash` (encode, decode, neighbours, step estimate, areas, distances) ported from `geohash.c`/`geohash_helper.c`; `GEOADD` (`NX XX CH`) `GEOPOS GEODIST GEOHASH GEOSEARCH GEOSEARCHSTORE` (`FROMMEMBER FROMLONLAT BYRADIUS BYBOX ASC DESC COUNT [ANY] WITHCOORD WITHDIST WITHHASH STOREDIST`), units `m km mi ft`, `GEORADIUS GEORADIUSBYMEMBER` and their `_RO` forms, the keyword key kind; difftest cases at the poles' limits, across the antimeridian, empty and whole-earth areas; a fuzz of random points and searches against Redis 8, unsorted order included | planned |
| 6 | Benchmarks and docs | redis-benchmark `ZADD ZPOPMIN` against Redis 7.0.15, 8 and Valkey 9; `ZADD ZSCORE ZRANK ZRANGEBYSCORE` on 1M members, `GEOSEARCH` on 1M points; memory per member small and large against the three; README compatibility table and command list, CHANGELOG | planned |

Each milestone, as before: difftest cases against Redis 8.10.2 byte for
byte (`ZRANDMEMBER` compared by its properties), `cluster_difftest`
where keys cross slots, unit tests, injected bugs, focused commits.

**Risks.** `fpconv_dtoa` is the one place where "looks right" isn't enough:
Grisu2 output differs from the shortest form for some doubles, hence the
fuzz in milestone 1. The large sorted set's member index is the ordered
table, so a 1M-member key inherits its doubling stall (12.5 ms measured
for hashes; the fix, incremental rehashing, is still open).
