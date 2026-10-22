"""Test corpus for difftest.py.

A Case is a list of steps run on one or more connections:

    Send(data, chunk=None, delay=0, conn=0)   write bytes (optionally split)
    Read(n=1, conn=0)                         read n complete replies
    UntilClose(conn=0)                        read replies until the server closes
    Silent(wait=0.3, conn=0)                  expect no reply within `wait`
    Sleep(seconds)

Every reply (and events such as "<CLOSED>") goes into a transcript; the
reference's transcript is compared with the target's. Builders:

    seq(name, *commands)    one write per command, wait for each reply
    pipe(name, commands)    all commands in one write, then read them all
    raw(name, data, n)      raw bytes, n replies (n=CLOSE: until close)

Options on every builder: tags=..., norm=[normalisers], expected=[...]
(replaces the reference server: the bytes a reply must be), slow=True,
flush=False (don't FLUSHALL before the case), timeout=.

Normalisers are functions (index, raw_reply_bytes) -> bytes, applied to both
sides before comparing (mask non-deterministic content: ids, versions, TTL
milliseconds...).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Union

from resp import Reply, cmd, parse

# ---------------------------------------------------------------- steps


@dataclass
class Send:
    data: bytes
    chunk: Union[int, Sequence[int], None] = None
    delay: float = 0.0
    conn: int = 0


@dataclass
class Read:
    n: int = 1
    conn: int = 0


@dataclass
class UntilClose:
    conn: int = 0


@dataclass
class Silent:
    wait: float = 0.3
    conn: int = 0


@dataclass
class Sleep:
    seconds: float


Norm = Callable[[int, bytes], bytes]


@dataclass
class Case:
    name: str
    steps: list
    tags: set = field(default_factory=set)
    norm: List[Norm] = field(default_factory=list)
    expected: Optional[list] = None
    slow: bool = False
    flush: bool = True
    timeout: float = 3.0
    doc: str = ""


CLOSE = "close"
CASES: List[Case] = []


def _enc(c) -> bytes:
    if isinstance(c, bytes):
        return c
    if isinstance(c, str):
        return c.encode()
    return cmd(*c)


def _add(name, steps, **kw) -> Case:
    if any(c.name == name for c in CASES):
        raise ValueError(f"duplicate case name {name}")
    norm = kw.pop("norm", [])
    if callable(norm):
        norm = [norm]
    tags = set(kw.pop("tags", ()))
    case = Case(name, steps, tags=tags, norm=list(norm), **kw)
    CASES.append(case)
    return case


def seq(name, *commands, **kw) -> Case:
    steps = []
    for c in commands:
        steps += [Send(_enc(c)), Read(1)]
    return _add(name, steps, **kw)


def pipe(name, commands, **kw) -> Case:
    data = b"".join(_enc(c) for c in commands)
    return _add(name, [Send(data), Read(len(commands))], **kw)


def raw(name, data: Union[bytes, Sequence[bytes]], n: Union[int, str] = 1, delay=0.0, **kw) -> Case:
    if isinstance(data, (bytes, str)):
        steps = [Send(_enc(data))]
    else:
        steps = [Send(b"".join(data), chunk=_cuts(data), delay=delay)]
    steps.append(UntilClose() if n == CLOSE else Read(n))
    return _add(name, steps, **kw)


def _cuts(chunks: Sequence[bytes]) -> List[int]:
    out, pos = [], 0
    for c in chunks[:-1]:
        pos += len(c)
        out.append(pos)
    return out


def steps(name, *st, **kw) -> Case:
    return _add(name, list(st), **kw)


# ---------------------------------------------------------- normalisers


def _parse1(b: bytes) -> Reply:
    return parse(b)[0]


def at(idxs, fn):
    """Apply fn(raw) only to transcript entries with these indexes."""
    idxs = {idxs} if isinstance(idxs, int) else set(idxs)
    return lambda i, b: fn(b) if i in idxs else b


def int_range(lo, hi):
    """Integer reply within [lo, hi] -> ':<lo..hi>' (else left alone)."""

    def f(b):
        try:
            r = _parse1(b)
        except Exception:
            return b
        if r.kind == ":" and lo <= r.value <= hi:
            return b":<%d..%d>\r\n" % (lo, hi)
        return b

    return f


def kind_only(b: bytes) -> bytes:
    """Keep only the reply type ('$<masked>' etc.)."""
    return b[:1] + b"<masked>"


def bulk_contains(*needles: bytes):
    """Bulk/verbatim reply containing all needles -> type + '<contains ...>'."""

    def f(b):
        ok = all(n in b for n in needles)
        return b[:1] + (b"<contains %s>" % b",".join(needles) if ok else b"<MISSING one of %s>" % b",".join(needles))

    return f


def sorted_elems(b: bytes) -> bytes:
    """Array or set reply -> its elements sorted (for orders Redis leaves to its dict)."""
    try:
        r = _parse1(b)
    except Exception:
        return b
    if r.kind not in "*~" or r.value is None:
        return b
    return b"%s<sorted>" % r.kind.encode() + b"".join(sorted(v.encode() for v in r.value))


def length_only(b: bytes) -> bytes:
    """Array or set reply -> its type and length (random picks)."""
    try:
        r = _parse1(b)
    except Exception:
        return b
    if r.kind not in "*~" or r.value is None:
        return b
    return b"%s<%d elements>" % (r.kind.encode(), len(r.value))


def scan_sorted(pairs: bool = False):
    """SCAN-family reply [cursor, [elements]] -> the cursor and the elements
    sorted (field/value pairs kept together with pairs=True)."""

    def f(b):
        try:
            r = _parse1(b)
        except Exception:
            return b
        if r.kind != "*" or not r.value or len(r.value) != 2 or r.value[1].kind != "*":
            return b
        elems = [v.encode() for v in r.value[1].value]
        if pairs:
            elems = [a + b for a, b in zip(elems[0::2], elems[1::2])]
        return b"scan " + r.value[0].encode() + b"<sorted>" + b"".join(sorted(elems))

    return f


HELLO_MASK = {b"server", b"version", b"id"}


def mask_hello(b: bytes) -> bytes:
    """HELLO reply (RESP2 flat array or RESP3 map): mask server/version/id."""
    try:
        r = _parse1(b)
    except Exception:
        return b
    if r.kind == "%":
        pairs = r.value
    elif r.kind == "*" and r.value and len(r.value) % 2 == 0:
        pairs = list(zip(r.value[0::2], r.value[1::2]))
    else:
        return b
    out = [r.kind.encode() + b"%d" % (len(r.value)) + b"\r\n"]
    for k, v in pairs:
        out.append(k.encode())
        if k.value in HELLO_MASK:
            out.append(v.kind.encode() + b"<masked>\r\n")
        else:
            out.append(v.encode())
    return b"".join(out)


# ------------------------------------------------------------- the corpus

LONG = 9223372036854775807
MINLONG = -9223372036854775808
MiB = 1 << 20

# --- PING / ECHO --------------------------------------------------------
seq("ping", ("PING",))
seq("ping_arg", ("PING", "hello"))
seq("ping_empty_arg", ("PING", ""))
seq("ping_binary_arg", ("PING", b"a\x00b\r\nc"))
seq("ping_arity", ("PING", "a", "b"))
seq("ping_lowercase", ("ping",), ("Ping", "x"))
seq("echo", ("ECHO", "hello world"))
seq("echo_empty", ("ECHO", ""))
seq("echo_binary", ("ECHO", b"\x00\xff\r\n\r\n"))
seq("echo_arity", ("ECHO",), ("ECHO", "a", "b"))

# --- SET / GET ----------------------------------------------------------
seq("set_get", ("SET", "k", "v"), ("GET", "k"))
seq("get_missing", ("GET", "nokey"))
seq("set_overwrite", ("SET", "k", "a"), ("SET", "k", "bbbb"), ("GET", "k"))
seq("set_empty_value", ("SET", "k", ""), ("GET", "k"), ("STRLEN", "k"), ("EXISTS", "k"),
    doc="empty value must be $0, not nil")
seq("set_empty_key", ("SET", "", "v"), ("GET", ""), ("EXISTS", ""))
seq("set_binary_key_value", ("SET", b"k\x00\r\nk", b"v\x00\r\n\x00v"), ("GET", b"k\x00\r\nk"), ("GET", b"k"),
    ("STRLEN", b"k\x00\r\nk"))
seq("set_all_bytes", ("SET", "allbytes", bytes(range(256))), ("GET", "allbytes"))
seq("set_value_with_spaces", ("SET", "my key", "a b c"), ("GET", "my key"))
seq("set_lowercase", ("set", "k", "v"), ("get", "k"), ("GeT", "k"), ("sEt", "k", "w", "ex", "100", "nx"),
    ("SET", "k", "w", "xx", "get"))
seq("set_nx", ("SET", "k", "v", "NX"), ("SET", "k", "w", "NX"), ("GET", "k"))
seq("set_xx", ("SET", "k", "v", "XX"), ("GET", "k"), ("SET", "k", "v"), ("SET", "k", "w", "XX"), ("GET", "k"))
seq("set_get_option", ("SET", "k", "v", "GET"), ("SET", "k", "w", "GET"), ("GET", "k"))
seq("set_nx_get", ("SET", "k", "v", "NX", "GET"), ("SET", "k", "w", "NX", "GET"), ("GET", "k"))
seq("set_xx_get", ("SET", "k", "v", "XX", "GET"), ("SET", "k", "v"), ("SET", "k", "w", "XX", "GET"))
seq("set_nx_xx", ("SET", "k", "v", "NX", "XX"))
seq("set_ex_zero", ("SET", "k", "v", "EX", "0"))
seq("set_ex_negative", ("SET", "k", "v", "EX", "-1"))
seq("set_px_zero", ("SET", "k", "v", "PX", "0"))
seq("set_ex_nan", ("SET", "k", "v", "EX", "abc"))
seq("set_ex_float", ("SET", "k", "v", "EX", "1.5"))
seq("set_ex_missing_value", ("SET", "k", "v", "EX"))
seq("set_ex_overflow", ("SET", "k", "v", "EX", str(LONG)), ("SET", "k", "v", "PX", str(LONG)),
    ("SET", "k", "v", "EX", "99999999999999999999"))
seq("set_ex_px", ("SET", "k", "v", "EX", "10", "PX", "100"))
seq("set_ex_twice", ("SET", "k", "v", "EX", "10", "EX", "100"))
seq("set_keepttl_ex", ("SET", "k", "v", "KEEPTTL", "EX", "10"))
seq("set_exat_pxat", ("SET", "k", "v", "EXAT", "10", "PXAT", "100"))
seq("set_unknown_option", ("SET", "k", "v", "FOO"), ("SET", "k", "v", "NX", "bar"))
seq("set_arity", ("SET",), ("SET", "k"))
seq("get_arity", ("GET",), ("GET", "a", "b"))
seq("set_noarg_keyword_values", ("SET", "k", "NX"), ("GET", "k"), ("SET", "EX", "EX"), ("GET", "EX"))

# --- expiry ------------------------------------------------------------
NOW = int(time.time())
seq("set_ex_ttl", ("SET", "k", "v", "EX", "100"), ("TTL", "k"), ("PTTL", "k"), ("GET", "k"),
    norm=[at(1, int_range(98, 100)), at(2, int_range(98000, 100000))])
seq("set_px_ttl", ("SET", "k", "v", "PX", "100500"), ("TTL", "k"), ("PTTL", "k"),
    norm=[at(1, int_range(98, 101)), at(2, int_range(98000, 100500))])
seq("set_exat", ("SET", "k", "v", "EXAT", str(NOW + 1000)), ("TTL", "k"),
    norm=[at(1, int_range(900, 1000))])
seq("set_pxat", ("SET", "k", "v", "PXAT", str((NOW + 1000) * 1000)), ("TTL", "k"),
    norm=[at(1, int_range(900, 1000))])
seq("set_exat_past", ("SET", "k", "v", "EXAT", "1"), ("GET", "k"), ("EXISTS", "k"))
seq("set_keepttl", ("SET", "k", "v", "EX", "100"), ("SET", "k", "w", "KEEPTTL"), ("TTL", "k"), ("GET", "k"),
    norm=[at(2, int_range(98, 100))])
seq("set_clears_ttl", ("SET", "k", "v", "EX", "100"), ("SET", "k", "w"), ("TTL", "k"))
seq("ttl_missing_and_persistent", ("TTL", "nokey"), ("PTTL", "nokey"), ("SET", "k", "v"), ("TTL", "k"), ("PTTL", "k"))
seq("expire_basic", ("SET", "k", "v"), ("EXPIRE", "k", "100"), ("TTL", "k"), ("EXPIRE", "nokey", "100"),
    norm=[at(2, int_range(98, 100))])
seq("pexpire_basic", ("SET", "k", "v"), ("PEXPIRE", "k", "100000"), ("PTTL", "k"), ("TTL", "k"),
    norm=[at(2, int_range(98000, 100000)), at(3, int_range(98, 100))])
seq("expire_nx", ("SET", "k", "v"), ("EXPIRE", "k", "100", "NX"), ("EXPIRE", "k", "200", "NX"), ("TTL", "k"),
    norm=[at(3, int_range(98, 100))])
seq("expire_xx", ("SET", "k", "v"), ("EXPIRE", "k", "100", "XX"), ("TTL", "k"), ("EXPIRE", "k", "100"),
    ("EXPIRE", "k", "200", "XX"), ("TTL", "k"), norm=[at(5, int_range(198, 200))])
seq("expire_gt", ("SET", "k", "v"), ("EXPIRE", "k", "100", "GT"), ("TTL", "k"), ("EXPIRE", "k", "100"),
    ("EXPIRE", "k", "50", "GT"), ("EXPIRE", "k", "200", "GT"), ("TTL", "k"), norm=[at(6, int_range(198, 200))])
seq("expire_lt", ("SET", "k", "v"), ("EXPIRE", "k", "100", "LT"), ("TTL", "k"), ("EXPIRE", "k", "200", "LT"),
    ("EXPIRE", "k", "50", "LT"), ("TTL", "k"), norm=[at(2, int_range(98, 100)), at(5, int_range(48, 50))])
seq("expire_bad_option_combos", ("SET", "k", "v"), ("EXPIRE", "k", "100", "NX", "XX"),
    ("EXPIRE", "k", "100", "GT", "LT"), ("EXPIRE", "k", "100", "NX", "GT"), ("EXPIRE", "k", "100", "FOO"),
    ("EXPIRE", "k", "100", "nx"), ("TTL", "k"), norm=[at(6, int_range(98, 100))])
seq("expire_nan", ("SET", "k", "v"), ("EXPIRE", "k", "abc"), ("EXPIRE", "k", "1.5"), ("PEXPIRE", "k", ""))
seq("expire_zero_and_negative_delete", ("SET", "a", "v"), ("SET", "b", "v"), ("EXPIRE", "a", "0"),
    ("PEXPIRE", "b", "-100"), ("EXISTS", "a", "b"), ("GET", "a"))
seq("expire_overflow", ("SET", "k", "v"), ("EXPIRE", "k", str(LONG)), ("PEXPIRE", "k", str(LONG)),
    ("EXPIRE", "k", str(LONG // 1000)), ("TTL", "k"))
seq("expireat_past_deletes", ("SET", "k", "v"), ("EXPIREAT", "k", "1"), ("EXISTS", "k"), ("GET", "k"),
    ("EXPIREAT", "k", "1"))
seq("expireat_future", ("SET", "k", "v"), ("EXPIREAT", "k", str(NOW + 1000)), ("TTL", "k"),
    ("EXPIREAT", "k", str(NOW + 2000), "LT"), ("EXPIREAT", "k", str(NOW + 2000), "GT"), ("TTL", "k"),
    norm=[at(2, int_range(900, 1000)), at(5, int_range(1900, 2000))])
seq("persist", ("SET", "k", "v", "EX", "100"), ("PERSIST", "k"), ("TTL", "k"), ("PERSIST", "k"), ("PERSIST", "nokey"),
    ("GET", "k"))
seq("expire_arity", ("EXPIRE", "k"), ("PEXPIRE", "k"), ("EXPIREAT", "k"), ("TTL",), ("TTL", "a", "b"), ("PTTL",),
    ("PERSIST",), ("PERSIST", "a", "b"))
seq("expire_on_missing_with_options", ("EXPIRE", "nokey", "10", "NX"), ("EXPIRE", "nokey", "10", "XX"))
steps("slow_ex_1s_expires", Send(cmd("SET", "k", "v", "EX", "1")), Read(), Sleep(1.1), Send(cmd("GET", "k")), Read(),
      Send(cmd("EXISTS", "k")), Read(), Send(cmd("TTL", "k")), Read(), slow=True)
steps("slow_px_expires", Send(cmd("SET", "k", "v", "PX", "100")), Read(), Send(cmd("GET", "k")), Read(), Sleep(0.25),
      Send(cmd("GET", "k")), Read(), Send(cmd("DBSIZE")), Read(), slow=True)
steps("slow_expire_then_set_again", Send(cmd("SET", "k", "v")), Read(), Send(cmd("PEXPIRE", "k", "100")), Read(),
      Sleep(0.25), Send(cmd("SET", "k", "w", "NX")), Read(), Send(cmd("TTL", "k")), Read(), Send(cmd("GET", "k")), Read(),
      slow=True)

# --- DEL / EXISTS / MGET / MSET / SETNX / GETDEL / APPEND / STRLEN ------
seq("del", ("SET", "a", "1"), ("SET", "b", "2"), ("DEL", "a", "b", "c"), ("DEL", "a"), ("EXISTS", "a", "b"))
seq("del_duplicates", ("SET", "a", "1"), ("DEL", "a", "a"))
seq("del_arity", ("DEL",))
seq("exists", ("SET", "a", "1"), ("EXISTS", "a"), ("EXISTS", "a", "a", "b"), ("EXISTS", "nokey"), ("EXISTS",))
seq("mset_mget", ("MSET", "a", "1", "b", "2"), ("MGET", "a", "b", "c"), ("MGET", "a", "a"), ("MGET", "c"))
seq("mset_overwrite_clears_ttl", ("SET", "a", "x", "EX", "100"), ("MSET", "a", "1", "a", "2"), ("GET", "a"),
    ("TTL", "a"))
seq("mset_arity", ("MSET",), ("MSET", "a"), ("MSET", "a", "1", "b"), ("MGET",))
seq("mset_binary", ("MSET", b"\x00", b"\r\n", "e", ""), ("MGET", b"\x00", "e"))
seq("setnx", ("SETNX", "k", "v"), ("SETNX", "k", "w"), ("GET", "k"), ("SETNX", "k"), ("SETNX",))
seq("getdel", ("SET", "k", "v", "EX", "100"), ("GETDEL", "k"), ("GETDEL", "k"), ("EXISTS", "k"), ("GETDEL",),
    ("GETDEL", "a", "b"))
seq("append", ("APPEND", "k", "ab"), ("APPEND", "k", "cd"), ("APPEND", "k", ""), ("GET", "k"),
    ("APPEND", "e", ""), ("EXISTS", "e"), ("GET", "e"), ("APPEND", "k"))
seq("append_binary_and_int", ("SET", "n", "12"), ("INCR", "n"), ("APPEND", "n", b"\x00x"), ("GET", "n"))
seq("append_keeps_ttl", ("SET", "k", "v", "EX", "100"), ("APPEND", "k", "w"), ("TTL", "k"),
    norm=[at(2, int_range(98, 100))])
seq("strlen", ("SET", "k", "hello"), ("STRLEN", "k"), ("STRLEN", "nokey"), ("SET", "n", "123"), ("INCR", "n"),
    ("STRLEN", "n"), ("STRLEN",), ("STRLEN", "a", "b"))
seq("type", ("SET", "k", "v"), ("TYPE", "k"), ("TYPE", "nokey"), ("SET", "n", "1"), ("INCR", "n"), ("TYPE", "n"),
    ("TYPE",))
# What Redis would call a string: int, embstr up to 44 bytes, raw past that.
seq("object_encoding_strings", ("SET", "i", "12345"), ("OBJECT", "ENCODING", "i"),
    ("SET", "neg", "-9223372036854775808"), ("OBJECT", "ENCODING", "neg"),
    ("SET", "big", "9223372036854775808"), ("OBJECT", "ENCODING", "big"),
    ("SET", "z", "007"), ("OBJECT", "ENCODING", "z"), ("SET", "m0", "-0"), ("OBJECT", "ENCODING", "m0"),
    ("SET", "e", "x" * 44), ("OBJECT", "ENCODING", "e"), ("SET", "r", "x" * 45), ("OBJECT", "ENCODING", "r"),
    ("INCR", "n"), ("OBJECT", "ENCODING", "n"), ("OBJECT", "ENCODING", "nokey"), ("object", "encoding", "i"),
    ("OBJECT", "ENCODING"), ("OBJECT", "ENCODING", "i", "j"), ("OBJECT", "NOPE", "i"))

# --- hashes ------------------------------------------------------------
# Small hashes are listpacks in Redis: insertion order, kept on deletion.
seq("hash_basic", ("HSET", "h", "b", "1", "a", "2"), ("HSET", "h", "a", "3", "c", "4", "c", "5"),
    ("HGET", "h", "a"), ("HGET", "h", "nope"), ("HGET", "nokey", "a"), ("HGETALL", "h"), ("HKEYS", "h"),
    ("HVALS", "h"), ("HLEN", "h"), ("HLEN", "nokey"), ("TYPE", "h"), ("OBJECT", "ENCODING", "h"),
    ("HMGET", "h", "a", "x", "c"), ("HMGET", "nokey", "a", "b"), ("HEXISTS", "h", "a"), ("HEXISTS", "h", "x"),
    ("HEXISTS", "nokey", "a"), ("HSTRLEN", "h", "c"), ("HSTRLEN", "h", "x"), ("HSTRLEN", "nokey", "x"),
    ("HGETALL", "nokey"), ("HKEYS", "nokey"), ("HVALS", "nokey"))
seq("hash_basic_resp3", ("HELLO", "3"), ("HSET", "h", "b", "1", "a", "2"), ("HGETALL", "h"), ("HGETALL", "nokey"),
    ("HKEYS", "h"), ("HMGET", "h", "a", "x"), norm=[at(0, mask_hello)])
seq("hash_order_after_delete", ("HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4"), ("HDEL", "h", "b", "x", "b"),
    ("HSET", "h", "b", "5", "a", "6"), ("HGETALL", "h"), ("HDEL", "h", "a", "b", "c", "d"), ("EXISTS", "h"),
    ("HDEL", "h", "a"), ("HDEL", "nokey", "a"))
seq("hash_hmset_hsetnx", ("HMSET", "h", "a", "1", "b", "2"), ("HSETNX", "h", "a", "9"), ("HSETNX", "h", "c", "3"),
    ("HGETALL", "h"), ("HSETNX", "h", "a", "x" * 65), ("OBJECT", "ENCODING", "h"), ("HGET", "h", "a"),
    ("HMSET", "h", "a"), ("HMSET", "h", "a", "1", "b"), ("HSET", "h", "a", "1", "b"), ("HSET", "h", "a"),
    ("HSET", "h"), ("HSETNX", "h", "a"))
seq("hash_hincrby", ("HINCRBY", "h", "n", "5"), ("HINCRBY", "h", "n", "-7"), ("HGET", "h", "n"),
    ("HSET", "h", "s", "x", "m", "9223372036854775807", "z", "07"), ("HINCRBY", "h", "s", "1"),
    ("HINCRBY", "h", "m", "1"), ("HINCRBY", "h", "z", "1"), ("HINCRBY", "h", "n", "x"),
    ("HINCRBY", "h", "n", "1.5"), ("HINCRBY", "nokey", "f", "x"), ("EXISTS", "nokey"), ("HGETALL", "h"))
seq("hash_encoding", ("HSET", "h", "f", "x" * 64), ("OBJECT", "ENCODING", "h"), ("HSET", "h", "g", "x" * 65),
    ("OBJECT", "ENCODING", "h"), ("HSET", "k", "x" * 65, "v"), ("OBJECT", "ENCODING", "k"),
    ("HDEL", "h", "g"), ("OBJECT", "ENCODING", "h"))
seq("hash_512_fields", ("HSET", "h", *[s for i in range(512) for s in (f"f{i}", str(i))]),
    ("OBJECT", "ENCODING", "h"), ("HDEL", "h", "f5", "f77"), ("HKEYS", "h"), ("HSET", "h", "f5", "x", "f77", "y"),
    ("HSET", "h", "one-more", "z"), ("OBJECT", "ENCODING", "h"), ("HLEN", "h"))
seq("hash_wrongtype", ("SET", "s", "v"), ("HSET", "s", "a", "1"), ("HGET", "s", "a"), ("HGETALL", "s"),
    ("HMGET", "s", "a"), ("HDEL", "s", "a"), ("HLEN", "s"), ("HEXISTS", "s", "a"), ("HSTRLEN", "s", "a"),
    ("HKEYS", "s"), ("HVALS", "s"), ("HINCRBY", "s", "a", "1"), ("HSETNX", "s", "a", "1"), ("HMSET", "s", "a", "1"),
    ("HSET", "h", "a", "1"), ("GET", "h"), ("INCR", "h"), ("APPEND", "h", "x"), ("STRLEN", "h"), ("GETDEL", "h"),
    ("SET", "h", "v", "GET"), ("MGET", "h", "s"), ("SET", "h", "v"), ("TYPE", "h"))
# --- sets --------------------------------------------------------------
# Integer sets are intsets in Redis: numeric order. Other sets are dicts,
# their order random: compared sorted.
seq("set_intset", ("SADD", "s", "10", "-3", "7", "100", "7"), ("SMEMBERS", "s"), ("OBJECT", "ENCODING", "s"),
    ("SREM", "s", "-3", "x"), ("SMEMBERS", "s"), ("SCARD", "s"), ("SISMEMBER", "s", "7"), ("SISMEMBER", "s", "8"),
    ("SMISMEMBER", "s", "7", "8", "10"), ("SADD", "s", "07"), ("OBJECT", "ENCODING", "s"), ("TYPE", "s"))
seq("set_strings", ("SADD", "s", "b", "a", "c"), ("SMEMBERS", "s"), ("OBJECT", "ENCODING", "s"),
    ("SREM", "s", "a", "b", "c"), ("EXISTS", "s"), ("SREM", "s", "a"), ("SMEMBERS", "nokey"), ("SCARD", "nokey"),
    ("SISMEMBER", "nokey", "a"), ("SMISMEMBER", "nokey", "a", "b"), norm=[at(1, sorted_elems)])
seq("set_resp3", ("HELLO", "3"), ("SADD", "s", "3", "1", "2"), ("SMEMBERS", "s"), ("SMEMBERS", "nokey"),
    ("SUNION", "s", "nokey"), ("SINTER", "s", "nokey"), ("SPOP", "nokey", "2"), ("SPOP", "s", "0"),
    ("SRANDMEMBER", "s", "5"), ("SRANDMEMBER", "nokey", "5"), ("SPOP", "s", "5"), norm=[at(0, mask_hello)])
seq("set_intset_limit", ("SADD", "s", *[str(i) for i in range(512)]), ("OBJECT", "ENCODING", "s"),
    ("SADD", "s", "511"), ("OBJECT", "ENCODING", "s"), ("SADD", "s", "512"), ("OBJECT", "ENCODING", "s"))
seq("set_spop", ("SADD", "s", "a", "b", "c", "d"), ("SPOP", "s", "2"), ("SCARD", "s"), ("SPOP", "s"), ("SCARD", "s"),
    ("SPOP", "s", "5"), ("EXISTS", "s"), ("SPOP", "s"), ("SPOP", "s", "1"), ("SPOP", "s", "-1"), ("SPOP", "s", "x"),
    ("SPOP", "s", "1", "2"), ("SADD", "n", "3", "1", "2"), ("SPOP", "n", "3"),
    norm=[at([1, 5], length_only), at(3, kind_only)])
seq("set_srandmember", ("SADD", "s", "a", "b", "c", "d"), ("SRANDMEMBER", "s", "2"), ("SRANDMEMBER", "s", "-6"),
    ("SRANDMEMBER", "s", "0"), ("SRANDMEMBER", "s", "9"), ("SRANDMEMBER", "s"), ("SCARD", "s"),
    ("SRANDMEMBER", "nokey"), ("SRANDMEMBER", "nokey", "3"), ("SRANDMEMBER", "s", "x"),
    ("SRANDMEMBER", "s", "-9223372036854775808"), ("SRANDMEMBER", "s", "1", "2"),
    ("SADD", "n", "3", "1", "2"), ("SRANDMEMBER", "n", "3"), ("SRANDMEMBER", "n", "-2"),
    norm=[at([1, 2, 14], length_only), at(4, sorted_elems), at(5, kind_only)])
seq("set_smove", ("SADD", "a", "x", "y"), ("SMOVE", "a", "b", "x"), ("SMOVE", "a", "b", "x"), ("SMOVE", "a", "a", "y"),
    ("SMOVE", "a", "a", "z"), ("SMOVE", "a", "b", "y"), ("EXISTS", "a"), ("SMEMBERS", "b"), ("SET", "str", "v"),
    ("SMOVE", "nokey", "str", "x"), ("SMOVE", "b", "str", "x"), ("SMOVE", "str", "b", "x"), ("SMOVE", "b", "str", "q"),
    norm=[at(7, sorted_elems)])
seq("set_algebra", ("SADD", "a", "1", "2", "3"), ("SADD", "b", "3", "4"), ("SADD", "c", "x", "3"),
    ("SUNION", "b", "a", "nokey"), ("SINTER", "a", "b"), ("SDIFF", "a", "b"), ("SINTER", "a", "c"),
    ("SUNION", "a", "c"), ("SDIFF", "c", "a"), ("SDIFF", "nokey", "a"), ("SINTER", "a", "nokey"),
    ("SET", "str", "v"), ("SINTER", "a", "nokey", "str"), ("SINTER", "a", "str", "nokey"), ("SUNION", "a", "nokey", "str"),
    ("SDIFF", "nokey", "str"), ("SUNIONSTORE", "d", "a", "b"), ("SMEMBERS", "d"), ("OBJECT", "ENCODING", "d"),
    ("SINTERSTORE", "str", "a", "b"), ("TYPE", "str"), ("SDIFFSTORE", "str", "a", "nokey"), ("SMEMBERS", "str"),
    ("SINTERSTORE", "str", "a", "nokey"), ("EXISTS", "str"), ("SUNIONSTORE", "d", "c"), ("OBJECT", "ENCODING", "d"),
    ("SUNION",), ("SINTERSTORE", "d"), norm=[at([7, 8], sorted_elems)])
seq("set_store_keeps_no_ttl", ("SADD", "a", "1"), ("SADD", "d", "9"), ("EXPIRE", "d", "100"),
    ("SUNIONSTORE", "d", "a"), ("TTL", "d"), ("SMEMBERS", "d"))
seq("set_wrongtype", ("SET", "str", "v"), ("SADD", "str", "a"), ("SREM", "str", "a"), ("SMEMBERS", "str"),
    ("SCARD", "str"), ("SISMEMBER", "str", "a"), ("SMISMEMBER", "str", "a"), ("SPOP", "str"), ("SPOP", "str", "1"),
    ("SRANDMEMBER", "str"), ("SRANDMEMBER", "str", "1"), ("SADD", "s", "a"), ("GET", "s"), ("HGET", "s", "a"),
    ("HSET", "s", "a", "1"), ("SADD", "h", "x"), ("HSET", "h2", "a", "1"), ("SADD", "h2", "x"), ("TYPE", "s"))
# --- lists -------------------------------------------------------------
seq("list_push_pop", ("LPUSH", "l", "a", "b"), ("RPUSH", "l", "c", "d"), ("LRANGE", "l", "0", "-1"), ("LLEN", "l"),
    ("LPOP", "l"), ("RPOP", "l"), ("LPOP", "l", "1"), ("RPOP", "l", "5"), ("EXISTS", "l"), ("LPOP", "l"),
    ("LPOP", "l", "2"), ("RPOP", "l", "0"), ("RPUSH", "l", "x"), ("LPOP", "l", "0"), ("LPOP", "l", "-1"),
    ("LPOP", "l", "x"), ("LPOP", "l", "1", "2"), ("LPUSHX", "nokey", "a"), ("RPUSHX", "nokey", "a"),
    ("EXISTS", "nokey"), ("LPUSHX", "l", "y", "z"), ("RPUSHX", "l", "w"), ("LRANGE", "l", "0", "-1"),
    ("TYPE", "l"), ("OBJECT", "ENCODING", "l"), ("LLEN", "nokey"))
seq("list_push_pop_resp3", ("HELLO", "3"), ("RPUSH", "l", "a", "b"), ("LPOP", "nokey", "2"), ("LPOP", "nokey"),
    ("LPOP", "l", "1"), ("LRANGE", "nokey", "0", "-1"), ("LPOS", "l", "b", "COUNT", "0"), ("LPOS", "l", "q"),
    norm=[at(0, mask_hello)])
seq("list_ranges", ("RPUSH", "l", "0", "1", "2", "3", "4"), ("LRANGE", "l", "0", "-1"), ("LRANGE", "l", "-2", "100"),
    ("LRANGE", "l", "-100", "1"), ("LRANGE", "l", "3", "2"), ("LRANGE", "l", "5", "10"), ("LRANGE", "l", "0", "-6"),
    ("LRANGE", "l", "-9223372036854775808", "9223372036854775807"), ("LRANGE", "l", "x", "1"),
    ("LRANGE", "l", "0", "1.5"), ("LRANGE", "nokey", "0", "-1"), ("LINDEX", "l", "-1"), ("LINDEX", "l", "5"),
    ("LINDEX", "l", "-6"), ("LINDEX", "l", "-9223372036854775808"), ("LINDEX", "l", "x"), ("LINDEX", "nokey", "0"),
    ("LTRIM", "l", "1", "-2"), ("LRANGE", "l", "0", "-1"), ("LTRIM", "l", "x", "1"), ("LTRIM", "nokey", "0", "1"),
    ("LTRIM", "l", "5", "6"), ("EXISTS", "l"))
seq("list_lset_linsert_lrem", ("LSET", "nokey", "0", "x"), ("RPUSH", "l", "a", "b", "a", "c", "a"),
    ("LSET", "l", "-1", "z"), ("LSET", "l", "5", "z"), ("LSET", "l", "x", "z"), ("LINSERT", "l", "AFTER", "c", "d"),
    ("LINSERT", "l", "before", "a", "0"), ("LINSERT", "l", "BEFORE", "q", "d"), ("LINSERT", "nokey", "AFTER", "a", "b"),
    ("LINSERT", "l", "MIDDLE", "a", "b"), ("LRANGE", "l", "0", "-1"), ("LREM", "l", "-1", "a"), ("LREM", "l", "0", "a"),
    ("LREM", "l", "1", "q"), ("LREM", "l", "x", "a"), ("LREM", "nokey", "0", "a"), ("LRANGE", "l", "0", "-1"),
    ("LREM", "l", "0", "0"), ("LREM", "l", "0", "b"), ("LREM", "l", "2", "c"), ("LREM", "l", "0", "d"),
    ("LREM", "l", "0", "z"), ("EXISTS", "l"))
seq("list_lpos", ("RPUSH", "l", "a", "b", "a", "c", "a"), ("LPOS", "l", "a"), ("LPOS", "l", "a", "RANK", "2"),
    ("LPOS", "l", "a", "RANK", "-1", "COUNT", "0"), ("LPOS", "l", "a", "COUNT", "0", "MAXLEN", "2"),
    ("LPOS", "l", "a", "RANK", "-2", "COUNT", "2"), ("LPOS", "l", "a", "RANK", "4"), ("LPOS", "l", "a", "RANK", "4", "COUNT", "1"),
    ("LPOS", "l", "a", "RANK", "0"), ("LPOS", "l", "a", "RANK", "x"), ("LPOS", "l", "a", "RANK", "-9223372036854775808"),
    ("LPOS", "l", "a", "COUNT", "-1"), ("LPOS", "l", "a", "COUNT", "x"), ("LPOS", "l", "a", "MAXLEN", "-1"),
    ("LPOS", "l", "a", "MAXLEN", "x"), ("LPOS", "l", "a", "NOPE", "1"), ("LPOS", "l", "a", "RANK"),
    ("LPOS", "nokey", "a"), ("LPOS", "nokey", "a", "COUNT", "2"), ("LPOS", "l", "a", "rank", "-1", "maxlen", "2"),
    ("LPOS", "l", "q", "COUNT", "3"))
seq("list_lmove", ("RPUSH", "a", "1", "2"), ("EXPIRE", "a", "100"), ("LMOVE", "a", "a", "LEFT", "RIGHT"),
    ("LRANGE", "a", "0", "-1"), ("TTL", "a"), ("LMOVE", "a", "b", "RIGHT", "LEFT"), ("RPOPLPUSH", "a", "b"),
    ("EXISTS", "a"), ("LRANGE", "b", "0", "-1"), ("LMOVE", "b", "b", "left", "left"), ("LMOVE", "nokey", "b", "LEFT", "LEFT"),
    ("LMOVE", "b", "c", "UP", "LEFT"), ("SET", "str", "v"), ("LMOVE", "nokey", "str", "LEFT", "LEFT"),
    ("LMOVE", "b", "str", "LEFT", "LEFT"), ("LMOVE", "str", "b", "LEFT", "LEFT"), ("RPOPLPUSH", "b", "str"),
    ("LRANGE", "b", "0", "-1"), ("RPUSH", "one", "x"), ("EXPIRE", "one", "100"), ("RPOPLPUSH", "one", "one"),
    ("TTL", "one"))
seq("list_wrongtype", ("SET", "str", "v"), ("LPUSH", "str", "a"), ("RPUSH", "str", "a"), ("LPUSHX", "str", "a"),
    ("LPOP", "str"), ("RPOP", "str", "1"), ("LPOP", "str", "x"), ("LLEN", "str"), ("LRANGE", "str", "0", "-1"),
    ("LRANGE", "str", "x", "1"), ("LINDEX", "str", "0"), ("LSET", "str", "0", "a"), ("LINSERT", "str", "BEFORE", "a", "b"),
    ("LINSERT", "str", "NOPE", "a", "b"), ("LREM", "str", "0", "a"), ("LTRIM", "str", "0", "1"), ("LPOS", "str", "a"),
    ("LPOS", "str", "a", "RANK", "0"), ("RPUSH", "l", "a"), ("GET", "l"), ("SADD", "l", "a"), ("HSET", "l", "a", "b"),
    ("TYPE", "l"))
# --- SCAN, KEYS --------------------------------------------------------
# Small keyspaces come whole with a large COUNT; their order is the dict's:
# compared sorted. Compact hashes and intsets come whole, in their order.
seq("scan_keyspace", ("SET", "a1", "v"), ("SET", "a2", "v"), ("SET", "b", "v"), ("SET", "", "empty"),
    ("HSET", "h", "f", "v"), ("SADD", "s", "1"), ("RPUSH", "l", "x"),
    ("SCAN", "0", "COUNT", "1000"), ("SCAN", "0", "MATCH", "a*", "COUNT", "1000"),
    ("SCAN", "0", "MATCH", "*", "COUNT", "1000"), ("SCAN", "0", "TYPE", "hash", "COUNT", "1000"),
    ("SCAN", "0", "TYPE", "STRING", "COUNT", "1000"), ("SCAN", "0", "TYPE", "nope", "COUNT", "1000"),
    ("SCAN", "0", "match", "?", "type", "string", "count", "1000"), ("SCAN", "0", "COUNT", "5", "COUNT", "1000"),
    ("SCAN", "0", "MATCH", "zzz", "COUNT", "1000"), ("SCAN", "0"),
    norm=[at(range(7, 17), scan_sorted())])
seq("scan_errors", ("SET", "k", "v"), ("SCAN", "x"), ("SCAN", " 0"), ("SCAN", "0 "), ("SCAN", "+"),
    ("SCAN", "18446744073709551616"), ("SCAN", "0", "COUNT", "0"), ("SCAN", "0", "COUNT", "-1"),
    ("SCAN", "0", "COUNT", "x"), ("SCAN", "0", "COUNT"), ("SCAN", "0", "MATCH"), ("SCAN", "0", "TYPE"),
    ("SCAN", "0", "FOO", "bar"), ("SCAN",), ("SCAN", "+0", "COUNT", "100"), ("SCAN", "", "COUNT", "100"))
seq("scan_empty", ("SCAN", "0"), ("SCAN", "0", "MATCH", "*"), ("KEYS", "*"))
seq("keys", ("SET", "user:1", "v"), ("SET", "user:22", "v"), ("SET", "hello", "v"), ("SET", "hallo", "v"),
    ("SET", "h*llo", "v"), ("SET", "", "v"), ("SADD", "user:set", "x"),
    ("KEYS", "*"), ("KEYS", "user:?"), ("KEYS", "user:*"), ("KEYS", "h[ae]llo"), ("KEYS", "h[^e]llo"),
    ("KEYS", "h\\*llo"), ("KEYS", "h[a-b]llo"), ("KEYS", ""), ("KEYS", "nomatch*"), ("KEYS",), ("KEYS", "a", "b"),
    norm=[at(range(7, 15), sorted_elems)])
seq("hscan_compact", ("HSET", "h", "b", "1", "a", "2", "ab", "3"), ("HSCAN", "h", "0"), ("HSCAN", "h", "0", "COUNT", "1"),
    ("HSCAN", "h", "0", "MATCH", "a*"), ("HSCAN", "h", "0", "MATCH", "zz"), ("HSCAN", "nokey", "0"),
    ("HSCAN", "nokey", "0", "COUNT", "0"), ("HSCAN", "nokey", "x"), ("HSCAN", "h", "x"),
    ("HSCAN", "h", "0", "COUNT", "0"), ("HSCAN", "h", "0", "TYPE", "string"), ("HSCAN", "h", "0", "MATCH"),
    ("SET", "str", "v"), ("HSCAN", "str", "0"), ("HSCAN", "str", "x"), ("HSCAN", "h"))
seq("hscan_resp3", ("HELLO", "3"), ("HSET", "h", "b", "1", "a", "2"), ("HSCAN", "h", "0"), ("SADD", "s", "2", "1"),
    ("SSCAN", "s", "0"), ("SCAN", "0", "TYPE", "set"), norm=[at(0, mask_hello)])
seq("hscan_big", ("HSET", "h", *[s for i in range(300) for s in (f"f{i}", str(i))]), ("HSET", "h", "big", "x" * 65),
    ("OBJECT", "ENCODING", "h"), ("HSCAN", "h", "0", "COUNT", "1000"), ("HSCAN", "h", "0", "MATCH", "f1?", "COUNT", "1000"),
    norm=[at([3, 4], scan_sorted(pairs=True))])
seq("sscan", ("SADD", "i", "10", "-3", "7"), ("SSCAN", "i", "0"), ("SSCAN", "i", "0", "MATCH", "1*"),
    ("SADD", "t", *[f"m{i}" for i in range(100)]), ("SSCAN", "t", "0", "COUNT", "1000"),
    ("SSCAN", "t", "0", "MATCH", "m1?", "COUNT", "1000"), ("SSCAN", "nokey", "0"), ("SET", "str", "v"),
    ("SSCAN", "str", "0"), ("SSCAN", "i", "0", "TYPE", "set"), norm=[at([4, 5], scan_sorted())])
seq("hash_expiry", ("HSET", "h", "a", "1"), ("EXPIRE", "h", "100"), ("TTL", "h"), ("HSET", "h", "b", "2"),
    ("TTL", "h"), ("HDEL", "h", "a", "b"), ("TTL", "h"), ("HSET", "h", "a", "1"), ("TTL", "h"),
    ("PEXPIRE", "h", "-1"), ("EXISTS", "h"))

# --- integers ----------------------------------------------------------
seq("incr_basic", ("INCR", "n"), ("INCR", "n"), ("GET", "n"), ("DECR", "n"), ("DECR", "m"), ("GET", "m"))
seq("incrby_decrby", ("INCRBY", "n", "10"), ("INCRBY", "n", "-25"), ("DECRBY", "n", "5"), ("DECRBY", "n", "-100"),
    ("GET", "n"))
seq("incr_non_integer", ("SET", "s", "abc"), ("INCR", "s"), ("DECR", "s"), ("INCRBY", "s", "1"), ("GET", "s"))
seq("incr_weird_strings", *[c for v in ["", " 1", "1 ", "01", "+1", "-0", "0x10", "1e3", "1.0", "1.5", b"1\x00",
                                        "12345678901234567890", "-", "00"]
                            for c in (("SET", "s", v), ("INCR", "s"))])
seq("incr_int_edges", ("SET", "s", "-0"), ("SET", "z", "0"), ("INCR", "z"), ("SET", "m", "-1"), ("INCR", "m"),
    ("GET", "m"), ("SET", "big", str(LONG - 1)), ("INCR", "big"), ("GET", "big"))
seq("incr_overflow", ("SET", "n", str(LONG)), ("INCR", "n"), ("INCRBY", "n", "1"), ("GET", "n"),
    ("SET", "m", str(MINLONG)), ("DECR", "m"), ("DECRBY", "m", "1"), ("INCRBY", "m", "-1"), ("GET", "m"))
seq("incrby_huge", ("SET", "n", "1"), ("INCRBY", "n", str(LONG)), ("INCRBY", "n", "9223372036854775808"),
    ("INCRBY", "n", str(MINLONG)), ("GET", "n"), ("DECRBY", "z", str(MINLONG)), ("INCRBY", "z", str(MINLONG)),
    ("DECRBY", "z", "1"), ("GET", "z"))
seq("incrby_nan", ("INCRBY", "n", "abc"), ("INCRBY", "n", "1.5"), ("INCRBY", "n", ""), ("DECRBY", "n", " 1"),
    ("EXISTS", "n"))
seq("incr_key_with_spaces", ("INCR", "my key"), ("INCRBY", "my key", "41"), ("GET", "my key"),
    ("INCR", b"bin\x00\r\nkey"), ("GET", b"bin\x00\r\nkey"))
seq("incr_keeps_ttl", ("SET", "n", "1", "EX", "100"), ("INCR", "n"), ("TTL", "n"),
    norm=[at(2, int_range(98, 100))])
seq("incr_arity", ("INCR",), ("INCR", "a", "b"), ("DECR",), ("INCRBY", "a"), ("INCRBY", "a", "1", "2"),
    ("DECRBY", "a"))

# --- DBSIZE / FLUSHDB / FLUSHALL / SELECT ------------------------------
seq("dbsize", ("DBSIZE",), ("MSET", "a", "1", "b", "2", "c", "3"), ("DBSIZE",), ("DEL", "a"), ("DBSIZE",),
    ("DBSIZE", "x"))
seq("flushdb", ("MSET", "a", "1", "b", "2"), ("FLUSHDB",), ("DBSIZE",), ("SET", "a", "1"), ("FLUSHDB", "ASYNC"),
    ("FLUSHDB", "SYNC"), ("FLUSHDB", "foo"), ("FLUSHDB", "SYNC", "ASYNC"), ("DBSIZE",))
seq("flushall", ("MSET", "a", "1", "b", "2"), ("FLUSHALL",), ("DBSIZE",), ("FLUSHALL", "async"), ("FLUSHALL", "SYNC"),
    ("FLUSHALL", "x"), ("GET", "a"))
seq("select_0", ("SELECT", "0"), ("SET", "k", "v"), ("GET", "k"))
seq("select_1", ("SET", "k", "v"), ("SELECT", "1"), ("GET", "k"), ("SELECT", "0"), ("GET", "k"),
    doc="Redis has 16 databases; the Rust node may only have db 0 (allow-list if so)")
seq("select_errors", ("SELECT", "16"), ("SELECT", "-1"), ("SELECT", "abc"), ("SELECT",), ("SELECT", "0", "1"))

# --- HELLO / RESP3 -----------------------------------------------------
seq("hello_noargs", ("HELLO",), norm=[at(0, mask_hello)])
seq("hello_2", ("HELLO", "2"), ("GET", "nokey"), norm=[at(0, mask_hello)])
seq("hello_3", ("HELLO", "3"), norm=[at(0, mask_hello)])
seq("hello_bad", ("HELLO", "1"), ("HELLO", "4"), ("HELLO", "abc"), ("HELLO", "3", "FOO"), ("PING",))
seq("hello_3_setname", ("HELLO", "3", "SETNAME", "myconn"), ("CLIENT", "GETNAME"), norm=[at(0, mask_hello)])
seq("resp3_nulls", ("HELLO", "3"), ("GET", "nokey"), ("MGET", "a", "nokey"), ("SET", "k", "v", "GET"),
    ("CLIENT", "GETNAME"), ("GETDEL", "nokey"), ("SET", "k", "v", "NX"), norm=[at(0, mask_hello)])
seq("resp3_values", ("HELLO", "3"), ("SET", "k", "v"), ("GET", "k"), ("INCR", "n"), ("EXISTS", "k"), ("TTL", "k"),
    ("MGET", "k", "n"), ("PING",), ("ECHO", "x"), ("TYPE", "k"), ("DBSIZE",), norm=[at(0, mask_hello)])
seq("resp3_errors", ("HELLO", "3"), ("GET",), ("FOO",), ("INCR", "x"), ("SET", "x", "a"), ("INCR", "x"),
    norm=[at(0, mask_hello)])
seq("resp3_back_to_2", ("HELLO", "3"), ("HELLO", "2"), ("GET", "nokey"), ("MGET", "nokey"),
    norm=[at(0, mask_hello), at(1, mask_hello)])
seq("resp3_config_get", ("HELLO", "3"), ("CONFIG", "GET", "save"), ("CONFIG", "GET", "appendonly"),
    norm=[at(0, mask_hello)], tags={"client-startup"})
seq("resp3_info_kind", ("HELLO", "3"), ("INFO",), norm=[at(0, mask_hello), at(1, kind_only)])
steps("resp3_is_per_connection", Send(cmd("HELLO", "3"), conn=0), Read(conn=0), Send(cmd("GET", "nokey"), conn=1),
      Read(conn=1), Send(cmd("GET", "nokey"), conn=0), Read(conn=0), norm=[at(0, mask_hello)])

# --- CLIENT / COMMAND / INFO / CONFIG ------------------------------------
seq("client_name", ("CLIENT", "GETNAME"), ("CLIENT", "SETNAME", "foo"), ("CLIENT", "GETNAME"),
    ("client", "setname", ""), ("CLIENT", "GETNAME"))
seq("client_setname_invalid", ("CLIENT", "SETNAME", "a b"), ("CLIENT", "SETNAME", "a\nb"), ("CLIENT", "SETNAME"))
seq("client_id", ("CLIENT", "ID"), norm=[at(0, int_range(1, 1 << 62))])
seq("client_info_kind", ("CLIENT", "INFO"), norm=[at(0, bulk_contains(b"id=", b"addr=", b"cmd=client"))])
seq("client_list_kind", ("CLIENT", "LIST"), norm=[at(0, bulk_contains(b"id=", b"addr="))])
seq("client_setinfo", ("CLIENT", "SETINFO", "LIB-NAME", "redis-py"), ("CLIENT", "SETINFO", "LIB-VER", "5.0.0"),
    tags={"client-startup"}, doc="Redis 7.2+ replies +OK; 7.0 replies an error. redis-py ignores the error.")
seq("client_errors", ("CLIENT",), ("CLIENT", "FOO"))
seq("command_count", ("COMMAND", "COUNT"), norm=[at(0, int_range(1, 10000))])
seq("command_docs_kind", ("COMMAND", "DOCS"), norm=[at(0, kind_only)], tags={"client-startup"})
seq("command_docs_get_kind", ("COMMAND", "DOCS", "GET"), norm=[at(0, kind_only)])
seq("command_kind", ("COMMAND",), norm=[at(0, kind_only)])
seq("command_info_kind", ("COMMAND", "INFO", "GET"), norm=[at(0, kind_only)])
seq("command_errors", ("COMMAND", "FOO"))
seq("info_kind", ("INFO",), norm=[at(0, bulk_contains(b"# Server", b"redis_version:"))])
seq("info_server_kind", ("INFO", "server"), norm=[at(0, bulk_contains(b"# Server", b"redis_version:"))])
seq("info_keyspace", ("SET", "a", "1"), ("SET", "b", "2", "EX", "100"), ("INFO", "keyspace"),
    norm=[at(2, lambda b: b if b"avg_ttl=0" in b else b.split(b"avg_ttl=")[0] + b"avg_ttl=<masked>\r\n")])
seq("info_keyspace_empty", ("INFO", "keyspace"))
seq("info_cluster", ("INFO", "cluster"))

# --- Cluster commands on a standalone node ---------------------------------
seq("cluster_disabled", ("CLUSTER", "INFO"), ("CLUSTER", "KEYSLOT", "foo"), ("CLUSTER", "MYID"), ("CLUSTER", "SLOTS"),
    ("CLUSTER", "SHARDS"), ("CLUSTER", "NODES"), ("CLUSTER", "COUNTKEYSINSLOT", "1"), ("READONLY",), ("READWRITE",),
    ("ASKING",), doc="cluster-mode only: an error without --cluster, after the subcommand and arity checks")
seq("cluster_disabled_errors", ("CLUSTER",), ("CLUSTER", "FOO"), ("CLUSTER", "KEYSLOT"), ("CLUSTER", "MYID", "x"),
    ("READONLY", "x"))
seq("cluster_disabled_cross_slot", ("MSET", "foo", "1", "bar", "2"), ("MGET", "foo", "bar"), ("DEL", "foo", "bar"),
    doc="no slot check without --cluster")
seq("config_get_startup", ("CONFIG", "GET", "save"), ("CONFIG", "GET", "appendonly"), tags={"client-startup"},
    doc="redis-benchmark sends these on startup")
seq("config_get_misc", ("CONFIG", "GET", "nosuchparam"), ("CONFIG", "GET"), tags={"client-startup"})

# --- maxmemory (noeviction) ----------------------------------------------
# Each case ends with maxmemory 0, so the cases after it can write.
seq("config_set_maxmemory", ("CONFIG", "SET", "maxmemory", "1kb"), ("CONFIG", "GET", "maxmemory"),
    ("CONFIG", "SET", "maxmemory", "2G"), ("CONFIG", "GET", "maxmemory"), ("CONFIG", "SET", "maxmemory", "1x"),
    ("CONFIG", "SET", "maxmemory", "-1"), ("CONFIG", "SET", "maxmemory", "1.5mb"),
    ("CONFIG", "SET", "maxmemory", "0", "maxmemory", "5"), ("CONFIG", "SET", "maxmemory"),
    ("CONFIG", "SET", "maxmemory", "5", "x"), ("CONFIG", "SET", "nosuchparam", "1"),
    ("CONFIG", "SET", "maxmemory-policy", "noeviction"), ("CONFIG", "GET", "maxmemory-policy"),
    ("CONFIG", "SET", "maxmemory", "0"), ("CONFIG", "GET", "maxmemory"))
seq("oom_noeviction", ("SET", "k", "v"), ("SET", "n", "1"), ("CONFIG", "SET", "maxmemory", "1"),
    ("SET", "k", "w"), ("SETNX", "new", "v"), ("MSET", "a", "1", "b", "2"), ("APPEND", "k", "x"), ("INCR", "n"),
    ("DECR", "n"), ("INCRBY", "n", "5"), ("DECRBY", "n", "5"), ("SET", "k"), ("GET", "k"), ("MGET", "k", "n"),
    ("EXISTS", "k"), ("STRLEN", "k"), ("EXPIRE", "n", "100"), ("TTL", "n"), ("PERSIST", "n"), ("GETDEL", "n"),
    ("DEL", "k"), ("SET", "k", "v"), ("CONFIG", "SET", "maxmemory", "0"), ("SET", "k", "v"), ("GET", "k"),
    doc="past maxmemory the denyoom commands get -OOM, after the arity check; reads, DEL, EXPIRE still run")
pipe("oom_pipelined", [("CONFIG", "SET", "maxmemory", "1"), ("SET", "a", "1"), ("GET", "a"), ("INCR", "c"),
                       ("PING",), ("CONFIG", "SET", "maxmemory", "0"), ("INCR", "c")])

# --- unknown commands ----------------------------------------------------
seq("unknown_command", ("FOO",), ("FOO", "a", "b"), ("foo", "x y"))
seq("unknown_command_long_arg", ("FOO", "x" * 300, "y"))
seq("unknown_command_many_args", ("FOO",) + tuple(str(i) for i in range(30)))
seq("unknown_command_newlines", ("FOO", "a\r\nb"), (b"BA\r\nR",))
seq("empty_command_name", ("",))

# --- arity, every command -----------------------------------------------
for _c in [("GET",), ("SET", "k"), ("ECHO",), ("DEL",), ("EXISTS",), ("MGET",), ("MSET", "k"), ("SETNX", "k"),
           ("GETDEL",), ("APPEND", "k"), ("STRLEN",), ("INCR",), ("DECR",), ("INCRBY", "k"), ("DECRBY", "k"),
           ("EXPIRE", "k"), ("PEXPIRE", "k"), ("EXPIREAT", "k"), ("TTL",), ("PTTL",), ("PERSIST",), ("TYPE",),
           ("DBSIZE", "x"), ("SELECT",), ("CLIENT",), ("CONFIG",), ("GET", "a", "b"), ("STRLEN", "a", "b"),
           ("TYPE", "a", "b"), ("INCR", "a", "b"), ("SELECT", "0", "0")]:
    seq("arity_" + "_".join(a.lower() for a in _c), _c)

# --- inline commands ----------------------------------------------------
raw("inline_ping", b"PING\r\n")
raw("inline_lf_only", b"PING\n")
raw("inline_spaces", b"   PING   \r\n")
raw("inline_set_get", b"SET a b\r\nGET a\r\n", 2)
raw("inline_quoted", b'SET a "b c"\r\nGET a\r\n', 2)
raw("inline_single_quoted", b"SET a 'b c'\r\nGET a\r\nSET b 'it\\'s'\r\nGET b\r\n", 4)
raw("inline_escapes", b'SET a "x\\ty\\x41\\\\\\"\\n\\r\\b\\a"\r\nGET a\r\n', 2)
raw("inline_hex_escape_bad", b'SET a "\\xZZ"\r\nGET a\r\n', 2)
raw("inline_empty_quoted", b'SET a ""\r\nGET a\r\n', 2)
raw("inline_empty_lines", b"\r\n\r\n\n  \r\nPING\r\n", 1)
raw("inline_many_spaces", b"SET    a     b   \r\nGET a\r\n", 2)
raw("inline_tabs", b"SET\ta\tb\r\nGET a\r\n", 2)
raw("inline_unbalanced_double", b'SET a "b\r\nPING\r\n', CLOSE)
raw("inline_unbalanced_single", b"SET a 'b\r\nPING\r\n", CLOSE)
raw("inline_quote_not_followed_by_space", b'SET a "b"c\r\nPING\r\n', CLOSE)
raw("inline_unknown", b"FOO bar baz\r\n")
raw("inline_lowercase", b"set a 1\r\nincr a\r\nget a\r\n", 3)
raw("inline_mixed_with_multibulk", b"PING\r\n*1\r\n$4\r\nPING\r\nECHO hi\r\n" + cmd("GET", "nokey"), 4)
raw("inline_quit", b"QUIT\r\n", CLOSE)
raw("inline_too_big", b"a" * 70000, CLOSE)
steps("inline_binary_zero", Send(b"ECHO a\x00b\r\n"), Silent(0.5),
      doc="Redis finds the inline newline with strchr, which stops at the NUL: it never replies. "
          "A binary-safe parser replies $3 a\\0b instead (allow-list that)")

# --- protocol errors ------------------------------------------------------
raw("proto_bad_multibulk_len", b"*abc\r\nPING\r\n", CLOSE)
raw("proto_expected_dollar", b"*1\r\nX\r\nPING\r\n", CLOSE)
steps("proto_star1_X_waits", Send(b"*1\r\nX"), Silent(0.5), Send(b"\r\n"), UntilClose(),
      doc="'*1\\r\\nX' without a newline: Redis waits for more, then errors once the line is complete")
raw("proto_bad_bulk_len", b"*1\r\n$abc\r\nPING\r\n", CLOSE)
raw("proto_negative_bulk_len", b"*1\r\n$-1\r\n", CLOSE)
raw("proto_huge_bulk_len", b"*1\r\n$600000000\r\n", CLOSE)
raw("proto_bulk_len_overflow", b"*1\r\n$99999999999999999999\r\n", CLOSE)
raw("proto_huge_multibulk_len", b"*3000000000\r\n", CLOSE)
raw("proto_multibulk_overflow", b"*99999999999999999999\r\n", CLOSE)
raw("proto_star_in_bulk_position", b"*2\r\n$3\r\nGET\r\n*1\r\n", CLOSE)
raw("proto_too_big_mbulk_count", b"*" + b"1" * 70000, CLOSE)
raw("proto_too_big_bulk_count", b"*1\r\n$" + b"1" * 70000, CLOSE)
raw("proto_empty_multibulk", b"*0\r\nPING\r\n", 1)
raw("proto_negative_multibulk", b"*-1\r\nPING\r\n", 1)
raw("proto_bulk_not_crlf_terminated", b"*1\r\n$4\r\nPINGXY*1\r\n$4\r\nPING\r\n", 2,
    doc="Redis doesn't check the two bytes after a bulk; a stricter parser may error instead")
raw("proto_replies_before_error", b"PING\r\n" + cmd("SET", "k", "v") + b"*abc\r\nPING\r\n", CLOSE)
raw("proto_bulk_len_plus_sign", b"*1\r\n$+4\r\nPING\r\n", CLOSE)
raw("proto_multibulk_len_space", b"*1 \r\n$4\r\nPING\r\n", CLOSE)

# --- QUIT ---------------------------------------------------------------
raw("quit", cmd("QUIT"), CLOSE)
raw("quit_pipelined", cmd("PING") + cmd("QUIT") + cmd("PING"), CLOSE)
raw("quit_with_arg", cmd("QUIT", "x"), CLOSE)

# --- multiple connections -------------------------------------------------
steps("two_connections_share_keyspace", Send(cmd("SET", "k", "v"), conn=0), Read(conn=0),
      Send(cmd("GET", "k"), conn=1), Read(conn=1), Send(cmd("DEL", "k"), conn=1), Read(conn=1),
      Send(cmd("GET", "k"), conn=0), Read(conn=0))
steps("client_name_per_connection", Send(cmd("CLIENT", "SETNAME", "a"), conn=0), Read(conn=0),
      Send(cmd("CLIENT", "GETNAME"), conn=1), Read(conn=1))
steps("quit_only_closes_one", Send(cmd("QUIT"), conn=0), UntilClose(conn=0), Send(cmd("PING"), conn=1),
      Read(conn=1))

# --- pipelining -----------------------------------------------------------
pipe("pipeline_100_set", [("SET", f"k{i}", f"v{i}") for i in range(100)])
pipe("pipeline_100_mixed", [c for i in range(25) for c in
                            (("SET", f"k{i}", f"v{i}"), ("GET", f"k{i}"), ("INCR", f"n{i % 3}"), ("DEL", f"k{i}"))])
pipe("pipeline_with_errors", [("SET", "a", "1"), ("GET",), ("INCR", "a"), ("FOO", "x"), ("SET", "a", "b", "NX", "XX"),
                              ("INCR", "a"), ("GET", "a")])
raw("pipeline_1000_inline_ping", b"PING\r\n" * 1000, 1000)
pipe("pipeline_10000_incr", [("INCR", "n")] * 10000 + [("GET", "n")])
pipe("pipeline_hello3_midway", [("GET", "nokey"), ("HELLO", "3"), ("GET", "nokey"), ("HELLO", "2"), ("GET", "nokey")],
     norm=[at(1, mask_hello), at(3, mask_hello)])
pipe("pipeline_select_midway", [("SET", "k", "0"), ("SELECT", "1"), ("SET", "k", "1"), ("SELECT", "0"), ("GET", "k")])

# --- split requests -------------------------------------------------------
_SPLIT = cmd("SET", "key", "value")
for _i in range(1, len(_SPLIT)):
    raw(f"split_set_at_{_i:02d}", [_SPLIT[:_i], _SPLIT[_i:]], 1, delay=0.02, tags={"split"})
raw("split_get_after_split_set", [cmd("SET", "k", "v")[:7], cmd("SET", "k", "v")[7:] + cmd("GET", "k")[:5],
                                  cmd("GET", "k")[5:]], 2, delay=0.02, tags={"split"})
_two = cmd("SET", "a", "hello") + cmd("GET", "a") + b"PING\r\n" + cmd("APPEND", "a", "!") + cmd("GET", "a")
_add("split_every_byte_pipeline", [Send(_two, chunk=1, delay=0.002), Read(5)], tags={"split"})
_add("split_inline_every_byte", [Send(b'SET a "x y"\r\nGET a\r\n', chunk=1, delay=0.002), Read(2)], tags={"split"})
raw("split_inside_crlf", [b"*1\r", b"\n$4\r", b"\nPING\r", b"\n"], 1, delay=0.02, tags={"split"})
raw("split_multiple_requests_across_chunks", [cmd("SET", "a", "1") + cmd("INCR", "a")[:3],
                                              cmd("INCR", "a")[3:] + cmd("GET", "a")[:-1], cmd("GET", "a")[-1:]],
    3, delay=0.02, tags={"split"})
_big = cmd("SET", "big", b"x" * 200000)
_add("split_large_value_64k_chunks", [Send(_big, chunk=65536, delay=0.01), Read(), Send(cmd("STRLEN", "big")), Read()],
     tags={"split"})
raw("split_protocol_error_late", [b"*1\r\n$4\r\nPI", b"NG\r\n*x", b"yz\r\n"], CLOSE, delay=0.02, tags={"split"})

# --- large values ---------------------------------------------------------
seq("large_1mib", ("SET", "big", b"a" * MiB), ("STRLEN", "big"), ("GET", "big"), tags={"large"})
seq("large_10mib", ("SET", "big", bytes(range(256)) * (10 * MiB // 256)), ("STRLEN", "big"), ("GET", "big"),
    tags={"large"})
seq("large_append", ("SET", "big", b"a" * MiB), ("APPEND", "big", b"b" * MiB), ("STRLEN", "big"), tags={"large"})
seq("large_key", ("SET", b"K" * 100000, "v"), ("GET", b"K" * 100000), ("EXISTS", b"K" * 100000), tags={"large"})
seq("large_mset", ("MSET", "a", b"1" * MiB, "b", b"2" * MiB), ("MGET", "a", "b"), tags={"large"})
pipe("large_many_args", [("DEL",) + tuple(f"k{i}" for i in range(10000))], tags={"large"})

# --- GET/SET one request per write ------------------------------------------
seq("plain_set_get", ("SET", "pk1", "myvalue"), ("GET", "pk1"))
seq("plain_get_missing", ("GET", "never-set"))
seq("plain_set_overwrite", ("SET", "pk2", "first"), ("SET", "pk2", "second-longer"), ("GET", "pk2"),
    ("SET", "pk2", "x"), ("GET", "pk2"))
seq("plain_set_get_1000_bytes", ("SET", "pk3", "v" * 1000), ("GET", "pk3"))
seq("plain_key_value_spaces", ("SET", "a b", "c d e"), ("GET", "a b"))
seq("plain_many_keys", *[c for i in range(50) for c in (("SET", f"many{i}", f"value-{i}"), ("GET", f"many{i}"))])
seq("set_get_5000_bytes", ("SET", "k5000", "v" * 5000), ("GET", "k5000"))
seq("set_get_4000_bytes", ("SET", "k4000", "v" * 4000), ("GET", "k4000"))
seq("get_after_shorter_set", ("SET", "kk", "longvalue"), ("SET", "kk", "ab"), ("GET", "kk"))
seq("value_with_nul", ("SET", "nul", b"ab\x00cd"), ("GET", "nul"))
seq("value_with_crlf", ("SET", "crlf", b"ab\r\ncd"), ("GET", "crlf"))
raw("two_sets_one_write", cmd("SET", "p1", "a") + cmd("SET", "p2", "b"), 2)
raw("split_set", [cmd("SET", "ps", "abc")[:12], cmd("SET", "ps", "abc")[12:]], 1, delay=0.05)

def by_name():
    return {c.name: c for c in CASES}
