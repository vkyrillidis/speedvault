# Compatibility tools

Everything here runs on Linux (WSL Ubuntu 24.04 on Windows) with the Python 3
standard library only.

| File | What it is |
|------|------------|
| `resp.py` | Small RESP2/RESP3 client: raw sends (optionally split into chunks with delays), exact reply framing for every RESP3 type, close and timeout detection. Also a CLI: `python3 resp.py 127.0.0.1:6379 --resp3 MGET a b` |
| `corpus.py` | The test cases (518), with their normalisers and expected-byte overrides |
| `difftest.py` | Differential tester: runs the corpus against a reference and a target and compares replies byte for byte |
| `allowlist.py` | Known, intended differences (case pattern → reason) |
| `resp_proxy.py` | Logging TCP proxy: prints every request and reply going through it, including the ones `MONITOR` doesn't show (rejected commands) |
| `build_redis.sh` | Builds the reference Redis, 8.10.2, from its release tag (the commit pinned) into `.redis`; CI uses it too |
| `client_startup_capture.sh` | Runs redis-cli, redis-benchmark and redis-py through the proxy and logs what they send. The output for Redis 7.0.15 is in `captures/redis-7.0.15-client-startup.log` |
| `cluster_compat.sh`, `cluster_compat.py` | The `cluster` crate against a real Redis Cluster: `CLUSTER KEYSLOT` on random keys, `CLUSTER SLOTS SHARDS NODES` byte for byte |
| `stuart_cluster.sh` | stuart and three dbnodes from the release binaries, end to end: join, `STUART CREATE`, `redis-cli -c` seeded with stuart and with a node, `redis-cli --cluster check`, a stuart restart, a node restart and `STUART ASSIGN`; with `--password`, all of it with one (and the refusals without it) |
| `compose_smoke.sh` | `docker-compose.yml` end to end: stuart and three nodes come up (`--create-when 3`), keys spread and are found from any node, `redis-benchmark --cluster`, a stuart restart keeps the table, a node stopped leaves its keys to the others and started again gets its share back, `down` is quick |
| `cluster_difftest.sh`, `cluster_difftest.py` | Our cluster beside a 3-node Redis Cluster with the same layout: the same requests to each node pair, the same redis-py `RedisCluster` script, `redis-cli -c`, `redis-benchmark --cluster`, writes while stuart restarts, a slot migrating (`ASK`, `TRYAGAIN`) |
| `migrate_check.sh`, `migrate_check.py` | Slots moving under load: counters `INCR`ed through redis-py, `redis-cli -c` and Lettuce during `REBALANCE` and `DRAIN`, with stuart or a node killed mid-move; no write lost or doubled. The redirects each client gets during and after a rebalance |
| `lettuce/` | `LettuceLoad.java` (a Lettuce 7.8.0 cluster client for `migrate_check.py`) and `build.sh` (fetches the jars from Maven Central, SHA-256 pinned, and compiles it with `javac`; JDK 17+) |

## Setup

```sh
# Redis 8.10.2 as the reference (any free port), built from its tag
tools/compat/build_redis.sh .redis && export PATH=$PWD/.redis/bin:$PATH
redis-server --port 6393 --save '' --appendonly no --daemonize yes
```

## difftest.py

```sh
cd tools/compat
# Main mode: Redis (reference) vs our node (target)
python3 difftest.py --ref 127.0.0.1:6393 --target 127.0.0.1:7000
# Selecting cases: substring, fnmatch pattern or =exact name; tags; skip the slow (sleeping) ones
python3 difftest.py --ref ... --target ... -k 'split_*' -k =hello_3 --tag large --skip-slow -v
python3 difftest.py --list                                   # all cases with their tags
python3 difftest.py --show --target 127.0.0.1:7000 -k incr   # just print what the target replies
# AUTH: both started with the password pw (redis-server --requirepass pw, speedvault-server --requirepass pw);
# runs only the cases tagged auth, which need it (the main mode skips them)
python3 difftest.py --ref 127.0.0.1:6394 --target 127.0.0.1:7001 --password pw
```

- Each case is a list of steps (`Send` with optional chunking and delays,
  `Read(n)`, `UntilClose`, `Silent(wait)`, `Sleep`) on one or more
  connections. The resulting transcript (reply bytes plus events like
  `<CLOSED>`, `<TIMEOUT>`, `<SILENT>`, `<STILL OPEN>`, `<CLOSED AT END>`,
  `<EXTRA BYTES ...>`) of the reference is compared with the target's.
- Before each case both servers get a `FLUSHALL` on a separate connection
  (not with `--no-flush`). The target therefore needs
  `FLUSHALL`. Without it, cases can see keys left over from earlier cases.
- Normalisers mask what can't be deterministic: TTL/PTTL become ranges
  (`:<98..100>`), HELLO's `server`/`version`/`id`, `CLIENT ID`, INFO/COMMAND
  bodies (only the type, or "contains `# Server` and `redis_version:`").
- `expected=[...]` replaces the reference with the bytes a reply must be.
  A `None` entry means "take this one from the reference".
- Allow-listing: add `"case_or_pattern": "reason"` to `ALLOW["rust"]` in
  `allowlist.py` (the default set), or pass `--allow 'select_1=only db 0'`.
  Such cases are reported `ALLOWED` and don't fail the run. An allow-listed
  case that passes is reported `STALE`. `--allowlist none` is strict.
- Exit status: 1 if any case is `FAIL`, else 0. `--json FILE` writes all
  transcripts.
- Slow cases (marked `slow`, about 2 s in total): EX 1 + sleep 1.1, PX 100,
  PEXPIRE 100.
### Corpus coverage

PING/ECHO, SET/GET with every option (NX, XX, GET, EX, PX, EXAT, PXAT, KEEPTTL)
and every invalid combination (NX+XX, EX 0, EX -1, EX abc, EX 1.5, EX without
a value, overflowing expiries, EX+PX, KEEPTTL+EX), case-insensitive names,
binary keys and values (`\0`, `\r\n`, all 256 bytes), empty values and keys,
TTL/PTTL/EXPIRE/PEXPIRE/EXPIREAT with NX/XX/GT/LT and their invalid
combinations, EXPIREAT in the past, PERSIST, DEL/EXISTS (duplicate keys), MSET/MGET,
SETNX, GETDEL, APPEND, STRLEN, TYPE, INCR/DECR/INCRBY/DECRBY (non-integers,
` 1`, `01`, `+1`, `-0`, `1e3`, 20 digits, overflow at both ends, keys with
spaces), DBSIZE, FLUSHDB/FLUSHALL with ASYNC/SYNC, SELECT 0/1/16/-1/abc, wrong
arity for every command, unknown commands (with the "with args beginning
with:" text, 128-character truncation, newlines replaced),
HELLO/HELLO 2/HELLO 3/HELLO 1/HELLO 4/HELLO abc/HELLO 3 SETNAME, RESP3 nulls
and maps, RESP3 per connection, CLIENT GETNAME/SETNAME/ID/INFO/LIST/SETINFO,
COMMAND/COUNT/DOCS/INFO, INFO/INFO keyspace, CONFIG GET save/appendonly,
inline commands (quotes, escapes, `\xHH`, empty lines, LF only, tabs,
unbalanced quotes, inline mixed with multibulk, 64 KiB+ inline), protocol
errors (bad and huge multibulk/bulk lengths, `*1\r\nX` waiting and then
failing, `$-1` as a request, too-big count strings, replies before the
error, then the close), `*0`/`*-1`, QUIT (plain, pipelined, with an
argument, only closing its own connection), pipelining (100 SETs,
100 mixed, errors in the middle, 1000 inline PINGs, 10000 INCRs, HELLO 3 and
SELECT in the middle), a SET split at every byte boundary (32 cases), a
pipeline and an inline command dripped one byte at a time, splits inside
CRLF and across requests, 64 KiB chunks, 1 MiB and 10 MiB values, a 100 KB
key, DEL with 10000 keys; since phase 7 hashes, sets, lists and `SCAN`;
since phase 9 sorted sets (every `ZADD` option and combination, scores
written and read past fast_float's limits, `-0` packed and not, ranges by
rank, score and member with `REV`/`LIMIT`, `(`, `[`, `+`, `-` and NUL bytes,
members of mixed scores while packed, the encodings at their limits,
`ZRANGESTORE` over other types and itself, RESP3 pairs, `ZUNION`/`ZINTER`/
`ZDIFF` and their `STORE` forms with sets among the inputs, weights of
`inf`, `0`, `-1`, every aggregate, sums that depend on their order, `MIN`
of `0` and `-0`, `ZINTERCARD`, `ZMPOP`), geo (Redis's own examples,
every option and error in its order, scores that were never geohashes,
the latitude limits, the antimeridian, dense grids at high latitudes,
zero and whole-earth areas, large sets in scan order, `COUNT` with ties,
stored results with `-0`, RESP3);
transactions (tag `tx`: `MULTI`/`EXEC`/`DISCARD` and their errors, the
checks while queueing, `-OOM` at either end, RESP3, `CLIENT INFO` and
`INFO` inside, and `WATCH` against each write command, once changing the
key and once not, against expiry and `FLUSHALL`).

### Redis quirks the target may choose not to copy (allow-list if so)

- `inline_binary_zero`: Redis finds the end of an inline command with
  `strchr`, which stops at a NUL byte, so `ECHO a\0b\r\n` never gets a reply.
- `proto_bulk_not_crlf_terminated`: Redis doesn't check the two bytes after a
  bulk (`$4\r\nPINGXY` runs PING).
- `select_1`, `pipeline_select_midway`: Redis has 16 databases. The README
  only plans `SELECT 0`.
- `client_setinfo`: 7.0 replies `unknown subcommand`. Redis 7.2+ replies
  `+OK`, and clients ignore the error either way.
- `info_*`, `command_*`: only the type and a few markers are compared.
- `auth_pipelined_wrong*`: Redis 8.10.2 sends no reply to a failed `AUTH`
  while replies are pending, desynchronising the client; we reply
  `-WRONGPASS`.
- `auth_limit_split`: Redis 8.10.2 never answers a bad bulk length whose
  line ends in a later read; we reply the error, as Redis 7.0.15 does.

### Validation results (2026-10-13)

- **Redis vs Redis** (`--ref :6393 --target :6393 --allowlist none
  --skip-expected`): **275/275 equal**, slow cases included, repeated three
  times. This shows the corpus and the masking are deterministic.
## What clients send at startup

Captured with `client_startup_capture.sh` (proxy in front of Redis 7.0.15);
full log in `captures/redis-7.0.15-client-startup.log`. "Needs" was checked
against a stub server that replies an error to everything but PING/GET/SET.

**redis-cli 7.0.15**
- One-shot (`redis-cli -p P PING`, `redis-cli SET k v`): sends just the
  command, no handshake.
- `redis-cli -3 ...`: `HELLO 3` first. If HELLO fails it prints
  `HELLO 3 failed: <error>` and goes on in RESP2.
- Interactive: `COMMAND DOCS` on connect (used for command hints; Redis
  replies 480 entries, 172 KB). If it fails, redis-cli falls back to its
  built-in hints and works normally. `quit` is handled client-side (it just
  closes, no `QUIT` sent).
- From the redis-cli source (not captured): `-n DB` sends `SELECT DB`,
  `-a`/`--user` send `AUTH`.

**redis-benchmark 7.0.15**
- One extra connection at startup sends `CONFIG GET save` and
  `CONFIG GET appendonly`, both in one write. Redis replies
  `*2 save ""` / `*2 appendonly no` (RESP2 arrays). If they fail (error
  reply or connection closed), it prints
  `WARNING: Could not fetch server CONFIG` and benchmarks anyway.
- Then per test, `-c` connections each send the plain commands:
  - `PING_INLINE` is the inline `PING\r\n`; `PING_MBULK` is `*1 PING`.
  - `SET key:__rand_int__ VXK`, `GET key:__rand_int__`,
    `INCR counter:__rand_int__`, `MSET` with 10 pairs. The key is literally
    `key:__rand_int__` unless `-r` is given; the value is `-d` bytes of `x`
    (`VXK` with the default 3).
  - With `-P N`: N commands per write.

**redis-py 8.1.0** (unpacked wheel, see below)
- `Redis()` defaults to RESP3. Sent on connect:
  1. `HELLO 3`: **must succeed with a RESP3 map**. Otherwise the connect
     raises `ResponseError` (checked with the stub).
  2. `CLIENT MAINT_NOTIFICATIONS ON moving-endpoint-type internal-fqdn`:
     error ignored (Redis 7.0 replies `unknown subcommand`).
  3. `CLIENT SETINFO LIB-NAME redis-py` and `CLIENT SETINFO LIB-VER 8.1.0`:
     errors ignored.
- `client_name="x"` adds `CLIENT SETNAME x` (must reply `+OK`).
- `db=1` adds `SELECT 1`.
- `protocol=2`: no HELLO, only the two `CLIENT SETINFO`; works against a
  server without HELLO.
- `pipeline(transaction=False)`: all commands in one write.

**Minimum for these clients to work:**
- `HELLO 3` returning the RESP3 map, including at least `proto` (redis-py
  8.1 by default).
- `CLIENT SETNAME`.
- `SELECT 0`.
- Errors (not a dropped connection) for `CLIENT SETINFO`,
  `CLIENT MAINT_NOTIFICATIONS`, `COMMAND DOCS` and `CONFIG GET` if they
  aren't implemented. `CONFIG GET save|appendonly` answered like Redis avoids
  redis-benchmark's warning.
- Inline `PING`.

redis-py without pip: python3.12-venv/ensurepip isn't installed (needs sudo),
so the wheel was downloaded from PyPI and unpacked:

```sh
mkdir -p ~/venvs/redis-py/site && cd ~/venvs/redis-py
curl -sSLO "$(curl -sS https://pypi.org/pypi/redis/json | python3 -c 'import json,sys; print([u["url"] for u in json.load(sys.stdin)["urls"] if u["filename"].endswith(".whl")][0])')"
cd site && python3 -m zipfile -e ../redis-*.whl .
PYTHONPATH=~/venvs/redis-py/site python3 -c 'import redis; print(redis.__version__)'
```

## cluster_compat.sh

```sh
tools/compat/cluster_compat.sh                  # [--ports 7801,7802,7803,7804] [--keys 20000]
REDIS_SERVER=~/redis-8/bin/redis-server REDIS_CLI=~/redis-8/bin/redis-cli tools/compat/cluster_compat.sh
```

Starts four cluster-enabled `redis-server`s (stopped again at the end),
makes a cluster of the first three with `redis-cli --cluster create
--cluster-replicas 0`, and builds the `cluster` crate's `cluster_compat`
example, which answers over stdin/stdout. Then `cluster_compat.py` checks:

- `CLUSTER KEYSLOT` against `cluster::key_hash_slot` for 20,000 keys: the
  tag edge cases (`{}`, `{`, `}{`, `{a}{b}`, `a{b`, `{{a}}`, `{}a{b}`),
  random binary keys, and keys made mostly of `{` and `}`.
- `CLUSTER SLOTS`, `SHARDS` and `NODES`, RESP2 and RESP3, for three
  layouts: what `--cluster create` made (asking two nodes); after `CLUSTER
  RESET HARD` and `ADDSLOTSRANGE`, an uneven one (alternating single slots,
  several ranges per node, unassigned slots, a fourth node without slots;
  asking a node with slots and the one without); and one with no slots
  assigned. Our table gets Redis's node ids, addresses and node order (the
  order of its hash table, which `SHARDS` and `NODES` follow) but its own
  copy of the slot ranges, not Redis's. `SLOTS` and `SHARDS` must be
  identical; `NODES` too, after masking the cluster bus port, the ping and
  pong times and the config epoch (`cluster/src/reply.rs` says why).
- Our node in cluster mode in a Redis node's place, for each layout
  (asking a node with slots, and in the uneven layout also the one
  without): `speedvault-server --cluster --announce` that node's address, given that
  node's view with `CLUSTER SETTABLE` (our id in place of its id, its
  `cluster_current_epoch` as the epoch). Then the same requests go to both,
  in RESP2 and RESP3: keys in its slots, another node's and unassigned
  ones, alone, twice, with hash tags and mixed (`MOVED`, `CROSSSLOT`,
  `CLUSTERDOWN`, in Redis's order), arity errors, `SELECT`, `HELLO`, `INFO
  cluster` and the `CLUSTER` commands. The replies must be identical with
  the node ids masked (and the `NODES` fields as above); of `CLUSTER INFO`,
  the lines from `cluster_state` to `cluster_current_epoch`.

The Redis nodes run with `cluster-require-full-coverage no`, the behaviour
our node copies: a slot no node owns is "not served" and the others carry
on (with `yes`, one unassigned slot stops the whole cluster: `CLUSTERDOWN
The cluster is down`). A Redis master that has just joined also reports
`cluster_state:fail` for a few seconds (its rejoin delay); the script waits
for that to pass.

The baseline is Redis 8.10.2, as for the rest of the compat tests. The
`cluster_disabled*` cases of `corpus.py` check the standalone side (the
cluster commands refused as Redis refuses them) in the main difftest.

## cluster_difftest.sh

```sh
tools/compat/cluster_difftest.sh                # [--base-port 7850]
```

Two clusters side by side: a 3-node Redis Cluster (`redis-cli --cluster
create`, `cluster-require-full-coverage no`) on the base port + 1..3, and
stuart (+10) with three dbnodes (+11..13), which join one at a time so
that `STUART CREATE` gives them the same slots as the Redis nodes in the
same order. Then:

1. `cluster_difftest.py requests`: the same requests to each node pair,
   RESP2 and RESP3 (3696 in all): keys in every node's slots, alone, twice,
   with hash tags (also `{}`, `{` and `}{` ones) and mixed with keys of
   other nodes (`MOVED`,
   `CROSSSLOT`), arity errors, `SELECT`, the `CLUSTER` commands, and
   `DBSIZE` and `COUNTKEYSINSLOT` once the keys are written. Our replies
   must be byte for byte Redis's once each of our nodes' address and id is
   replaced by those of the Redis node with the same slots. `CLUSTER
   SHARDS` and `NODES` are compared as sets of nodes (each server lists
   them in its own order; `cluster_compat.sh` compares the order-matched
   bytes), `NODES` with the live fields masked; `CLUSTER INFO` up to
   `cluster_size` (the epochs differ).
2. `cluster_difftest.py clients`: one redis-py `RedisCluster` script (200
   keys, counters, `mset_nonatomic`, `mget_nonatomic`, hash tags, a
   cross-slot `mget`, `delete` and `exists` across nodes, expiry, a
   pipeline, a wrong-type error) against Redis, and against ours seeded
   with stuart and with a node, RESP2 and RESP3: the same results and the
   same errors.
3. `redis-cli -c`: 17 commands against both (ours seeded with stuart).
4. `redis-benchmark --cluster -t set,get,incr` against both: no errors (it
   reads `CLUSTER NODES` itself).
5. `cluster_difftest.py load`: 6 s of writes through `RedisCluster` seeded
   with stuart while stuart is killed and restarted; every write must
   succeed and read back.
6. `cluster_difftest.py migrating`: a slot of the first node migrating to
   the second, Redis's put so with `CLUSTER SETSLOT slot MIGRATING` /
   `IMPORTING`, ours with a table holding the migration (pushed to the
   nodes directly with an epoch past stuart's, so it runs last). With two
   keys at the source and one expired there: 99 requests per protocol to
   the source, the target and the third node, with and without `ASKING`,
   alone and pipelined (`ASK`, `TRYAGAIN`, `MOVED`, `COUNTKEYSINSLOT`,
   `CLUSTER NODES` with `[slot->-id]` and `[slot-<-id]`), compared as in 1.

redis-py comes from `REDIS_PY_PATH` (default `~/venvs/redis-py/site`, see
above) or from pip.

## migrate_check.sh

```sh
tools/compat/migrate_check.sh                   # [--base-port 7880] [--lettuce] [--filler N] [--password PW] [--keep] [SCENARIO...]
```

With `--password`, stuart and the nodes require it and every writer logs
in (not Lettuce's).

Slots moving while clients write (phase 5 milestone 6). Each scenario
starts stuart on the base port and three dbnodes (four for `drain`; the
`--auto` ones as below) on the next ports, `STUART CREATE`, fills them with `--filler` keys (default
200,000, 100-byte values, so that moves take a few seconds), and starts
the writers: two redis-py `RedisCluster` threads and one `redis-cli -c`
process fed on stdin, seeded with stuart, a redis-py thread sending
transactions (`MULTI`, `INCR` of two keys of one slot, `EXEC`: the two
must always be equal, a transaction never half applied, even through a
failover), and with `--lettuce` a
`LettuceLoad` per refresh mode (`default`, `off`, `fast`; `MODE@MS` also
caps Lettuce's reconnect delay at MS ms, and `-Dlettuce.nodnscache` makes
it resolve names uncached), seeded with the first node. Each failure is
printed with the key's slot, the node Lettuce's map gives it and what that
node's name resolves to. They `INCR` 2000 counters round and round, and a third
redis-py thread `INCR`s 32 hot counters (4 in each of 8 slots that move
in every scenario, each slot also holding a 512 KiB value that makes its
batch slow) in a tight loop, so that some are written while their batch
is in flight; each `INCR` is noted as acknowledged or failed. Then:

| Scenario | What happens |
|---|---|
| `rebalance` | a fourth node joins, `STUART REBALANCE` gives it 4096 slots |
| `drain` | four nodes, `STUART DRAIN` empties the first |
| `stuart-kill` | `rebalance`, stuart SIGKILLed once a quarter of the slots moved, restarted from its state file a second later |
| `source-kill` | `rebalance`, a node sending a slot SIGKILLed then, restarted at its address a second later (a new id: its slots are lost, the slot it was sending goes to the target) |
| `target-kill` | `rebalance`, the new node SIGKILLed then, down three seconds, restarted (the moves to it end with their sources) |
| `drain-target-kill` | `drain`, one of the three targets SIGKILLed once a quarter moved, down five seconds, restarted (the moves to it are dropped: those slots stay with the drained node); the drain must go on meanwhile |
| `auto-grow` | `stuart --auto` and one node (it gets every slot); three more join half a second apart while slots move; no `STUART` command; the four end with 4096 slots each |
| `leave-while-join` | `stuart --auto` and four nodes; the first leaves (`STUART LEAVE`) as a fifth joins; it must end removed, empty, redirecting with `-MOVED`, and the other four even |
| `sigterm` | `stuart --auto` and four nodes; SIGTERM to the first: it must leave the cluster and exit by itself with status 0, the other three even |
| `sigterm-two` | the same, SIGTERM to two nodes at once |
| `sigterm-target` | `stuart --auto` and three nodes; a fourth joins and gets SIGTERM once a quarter of its slots arrived |
| `redirects` | each client alone, through a rebalance: the redirects the nodes send it (`INFO cluster`'s `cluster_redirects_*`) before, during, and in eight 2 s windows after |

Once nothing moves (every node took stuart's table) and the writers
stopped, every node is read directly (`COUNTKEYSINSLOT` and
`GETKEYSINSLOT` for every slot, then `GET`): no key may be at two nodes,
or at one its table doesn't give the key's slot; every filler key must be
there with its value; each counter must be at least its acknowledged
`INCR`s and at most those plus the failed ones (redis-py retries
internally, so up to 16 per failure). With no node killed no `INCR` may
fail, so every count is exact. With one killed, the slots it owned or was
taking may lose keys (bounded above only); the rest stay exact.

`drain-target-kill` used to show a limit (phase 5): while a target was
down the drain all but stopped (0–32 more slots in three seconds), the
source's 32 moves under way all being to the dead node and a table that
had to reach it first reaching no other node. Since phase 6 milestone 1
moves to a down target don't count against the limits, such a table
reaches the others once the target has been down 2 s, and the source
moves its other slots meanwhile: the drain goes on (2754 slots left when
the target died, 0 five seconds later). The moves to the dead target
itself wait until it's back or replaced.

Two client bugs it meets, neither ours:

- **redis-py 8.1.0** (the newest) fails a command that gets `-ASK` to a
  node not in its slot map: `AttributeError: 'NoneType' object has no
  attribute 'redis_connection'`, raised before anything is sent. A node
  added while the client runs isn't in its map until it owns a slot
  (`CLUSTER SLOTS` lists only those, in Redis too) or a `-MOVED` names
  it, so the first slots moving to a new node trip it, once or twice per
  client. A Redis 7.0.15 Cluster does the same (`redis-cli --cluster
  add-node`, then `CLUSTER SETSLOT ... MIGRATING/IMPORTING`). The checker
  counts these apart, not as failures.
- **Lettuce** wants its seed's `CLUSTER NODES` to have a `myself` line
  (`Cannot determine own partition`); stuart, which is no node, has none.
  Seed Lettuce with nodes.

The `redirects` table, from a run on the Ryzen (2026-10-18), redirects per
client before the rebalance, during it (about a second), and in each 2 s
window after it ended:

| Client | before | during | after |
|---|---|---|---|
| redis-py 8.1.0 | 0 | 77 | 0 0 0 0 0 0 0 0 |
| `redis-cli -c` | 5525 | 2656 | 7084 6670 6691 6595 6675 6645 6678 6777 |
| Lettuce 7.8.0, default refresh | 0 | 349 | 2329 2231 363 0 0 0 0 0 |
| Lettuce, adaptive refresh off | 0 | 551 | 2801 2769 2792 2517 2522 2518 2639 2571 |
| Lettuce, `fast` (1 s limit, periodic 10 s) | 0 | 310 | 433 0 0 0 0 0 0 0 |

redis-py updates its map on each `-MOVED`. `redis-cli -c` keeps no map: it
stays connected to the node it was last sent to, so most commands are
redirected, moving or not. Lettuce 7 refreshes its map on `-MOVED` and
`-ASK` by default, but at most once in 5 s: a refresh during the move
fetches a layout that is soon stale, and the next one waits, so it is
redirected for 5–8 s after the move ends; with `fast` 1–3 s. With the
adaptive triggers off (Lettuce 6's default) it follows every `-MOVED` and
never updates. Configure Lettuce with the adaptive triggers on (the
default since 7), a shorter `adaptiveRefreshTriggersTimeout` (1 s here)
and a periodic refresh. The scenario fails if redis-py or Lettuce
(`default`, `fast`) is still redirected in the last two windows.

`lettuce/build.sh` needs `curl`, `sha256sum` and a JDK 17 or newer; the
jars go to `$LETTUCE_DIR` (default `~/.cache/speedvault/lettuce-7.8.0`).
CI runs the checker without Lettuce in the `cluster-compat` job and with
it in `cluster-clients`.
