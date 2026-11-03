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


# `modules` too: Redis 8 lists its built-in ones (vectorset; the official
# images add JSON, search, time series and bloom), we have none.
HELLO_MASK = {b"server", b"version", b"id", b"modules"}


def mask_hello(b: bytes) -> bytes:
    """HELLO reply (RESP2 flat array or RESP3 map): mask server/version/id
    and the modules."""
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
seq("expiretime", ("SET", "k", "v"), ("EXPIREAT", "k", str(NOW + 1000)), ("EXPIRETIME", "k"), ("PEXPIRETIME", "k"),
    ("PEXPIREAT", "k", str(NOW * 1000 + 1999)), ("EXPIRETIME", "k"), ("PEXPIRETIME", "k"), ("SET", "p", "v"),
    ("EXPIRETIME", "p"), ("PEXPIRETIME", "p"), ("EXPIRETIME", "nokey"), ("PEXPIRETIME", "nokey"),
    ("HSET", "h", "f", "v"), ("EXPIREAT", "h", str(NOW + 1000)), ("EXPIRETIME", "h"), ("EXPIRETIME",),
    ("PEXPIRETIME", "a", "b"))
seq("wait_no_replicas", ("WAIT", "0", "0"), ("SET", "k", "v"), ("WAIT", "0", "0"), ("WAIT", "-1", "0"),
    ("WAIT", "1", "50"), ("WAIT", "2", "1"), ("GET", "k"))
seq("wait_errors", ("WAIT", "x", "0"), ("WAIT", "1", "x"), ("WAIT", "1", "1.5"), ("WAIT", "1", "-1"),
    ("WAIT", "99999999999999999999", "0"), ("WAIT", "1"), ("WAIT", "1", "2", "3"),
    ("WAIT",))
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
seq("hash_hincrbyfloat", ("HSET", "h", "a", "1"), ("HINCRBYFLOAT", "h", "a", "2.5"), ("HINCRBYFLOAT", "h", "b", "1e2"),
    ("HINCRBYFLOAT", "h", "c", "x"), ("HSET", "h", "s", "xx"), ("HINCRBYFLOAT", "h", "s", "1"),
    ("HINCRBYFLOAT", "h", "a", "inf"), ("HINCRBYFLOAT", "h", "a", "nan"), ("HINCRBYFLOAT", "h", "a", "0.1"),
    ("HGETALL", "h"), ("OBJECT", "ENCODING", "h"), ("HINCRBYFLOAT", "h", "a", "1e300"), ("OBJECT", "ENCODING", "h"),
    ("HSET", "h", "i", "inf"), ("HINCRBYFLOAT", "h", "i", "1"), ("HINCRBYFLOAT", "nokey", "f", "-inf"),
    ("EXISTS", "nokey"), ("HINCRBYFLOAT", "big", "f", "1e4932"), ("HINCRBYFLOAT", "big", "f", "1e4932"),
    ("SET", "str", "v"), ("HINCRBYFLOAT", "str", "a", "1"), ("HINCRBYFLOAT", "str", "a", "x"),
    ("HINCRBYFLOAT", "h", "a"), ("HINCRBYFLOAT", "h", "a", "1", "2"), ("HSET", "k", "x" * 65, "1"),
    ("HSET", "k2", "a", "1"), ("HINCRBYFLOAT", "k2", "x" * 65, "1"), ("OBJECT", "ENCODING", "k2"))
# A small hash's fields come whole in their order; random picks by length.
seq("hash_hrandfield", ("HSET", "h", "a", "1", "b", "2", "c", "3"), ("HRANDFIELD", "nokey"),
    ("HRANDFIELD", "nokey", "2"), ("HRANDFIELD", "h", "5"), ("HRANDFIELD", "h", "3", "withvalues"),
    ("HRANDFIELD", "h", "0"), ("HRANDFIELD", "h", "-5", "WITHVALUES"), ("HRANDFIELD", "h", "2"), ("HRANDFIELD", "h"),
    ("HRANDFIELD", "h", "x"), ("HRANDFIELD", "h", "1", "foo"), ("HRANDFIELD", "h", "1", "withvalues", "x"),
    ("HRANDFIELD", "h", "-9223372036854775808"), ("HRANDFIELD", "h", "4611686018427387904", "withvalues"),
    ("HRANDFIELD", "h", "-4611686018427387904", "withvalues"), ("HRANDFIELD", "h", "4611686018427387903", "withvalues"),
    ("HRANDFIELD", "nokey", "1", "foo"), ("HRANDFIELD", "nokey", "x"), ("SET", "str", "v"), ("HRANDFIELD", "str"),
    ("HRANDFIELD", "str", "1"), ("HRANDFIELD", "str", "0"), ("HRANDFIELD",), ("HRANDFIELD", "h", "-1"),
    norm=[at([6, 7], length_only), at([8, 23], kind_only)])
seq("hash_hrandfield_resp3", ("HELLO", "3"), ("HSET", "h", "a", "1", "b", "2", "c", "3"), ("HRANDFIELD", "nokey"),
    ("HRANDFIELD", "nokey", "1"), ("HRANDFIELD", "h", "5", "WITHVALUES"), ("HRANDFIELD", "h", "-2", "withvalues"),
    ("HRANDFIELD", "h", "1", "withvalues"), ("HRANDFIELD", "h", "-1"),
    norm=[at(0, mask_hello), at([5, 6, 7], length_only)])
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
seq("set_sintercard", ("SADD", "s1", "a", "b", "c", "d"), ("SADD", "s2", "b", "c", "d", "e"),
    ("SINTERCARD", "2", "s1", "s2"), ("SINTERCARD", "2", "s1", "s2", "LIMIT", "2"), ("SINTERCARD", "2", "s1", "s2", "limit", "0"),
    ("SINTERCARD", "1", "s1"), ("SINTERCARD", "2", "s1", "s1"), ("SINTERCARD", "0", "s1"), ("SINTERCARD", "x", "s1"),
    ("SINTERCARD", "-1", "s1"), ("SINTERCARD", "3", "s1", "s2"), ("SINTERCARD", "2", "s1", "s2", "LIMIT"),
    ("SINTERCARD", "2", "s1", "s2", "LIMIT", "-1"), ("SINTERCARD", "2", "s1", "s2", "LIMIT", "x"),
    ("SINTERCARD", "2", "s1", "s2", "FOO", "1"), ("SINTERCARD", "2", "s1", "s2", "LIMIT", "1", "LIMIT", "3"),
    ("SET", "str", "v"), ("SINTERCARD", "2", "nokey", "str"), ("SINTERCARD", "2", "str", "nokey"),
    ("SINTERCARD", "1", "nokey"), ("SINTERCARD", "2abc", "s1", "s2"), ("SINTERCARD", "1", "s1", "s2"),
    ("SINTERCARD", "9223372036854775808", "s1"), ("SADD", "i", "1", "2", "3"), ("SINTERCARD", "2", "i", "s1"),
    ("SINTERCARD", "1"), ("SINTERCARD",), ("sintercard", "1", "i", "limit", "2"))
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
seq("list_lmpop", ("RPUSH", "l1", "a", "b", "c", "d"), ("RPUSH", "l2", "x"), ("LMPOP", "2", "nokey", "l1", "LEFT"),
    ("LMPOP", "2", "nokey", "l1", "RIGHT", "COUNT", "2"), ("LMPOP", "2", "l1", "l2", "left", "count", "10"),
    ("EXISTS", "l1"), ("LMPOP", "1", "nokey", "LEFT"), ("LMPOP", "0", "l2", "LEFT"), ("LMPOP", "1", "l2"),
    ("LMPOP", "2", "l2", "LEFT"), ("LMPOP", "1", "l2", "UP"), ("LMPOP", "1", "l2", "LEFT", "COUNT", "0"),
    ("LMPOP", "1", "l2", "LEFT", "COUNT", "x"), ("LMPOP", "1", "l2", "LEFT", "COUNT", "1", "COUNT", "2"),
    ("LMPOP", "1", "l2", "LEFT", "FOO"), ("LMPOP", "x", "l2", "LEFT"), ("LMPOP", "1", "l2", "LEFT", "COUNT"),
    ("LMPOP", "9223372036854775807", "l2", "LEFT"), ("LMPOP", "1", "l2", "LEFT", "COUNT", "9223372036854775807"),
    ("SET", "str", "v"), ("RPUSH", "l3", "q"), ("LMPOP", "3", "nokey", "str", "l3", "LEFT"),
    ("LMPOP", "2", "l3", "str", "LEFT"), ("LMPOP", "1", "str", "RIGHT"), ("LMPOP",), ("LMPOP", "1", "a"))
seq("list_lmpop_resp3", ("HELLO", "3"), ("RPUSH", "l", "x", "y"), ("LMPOP", "1", "nokey", "LEFT"),
    ("LMPOP", "1", "l", "RIGHT", "COUNT", "5"), norm=[at(0, mask_hello)])
seq("list_wrongtype", ("SET", "str", "v"), ("LPUSH", "str", "a"), ("RPUSH", "str", "a"), ("LPUSHX", "str", "a"),
    ("LPOP", "str"), ("RPOP", "str", "1"), ("LPOP", "str", "x"), ("LLEN", "str"), ("LRANGE", "str", "0", "-1"),
    ("LRANGE", "str", "x", "1"), ("LINDEX", "str", "0"), ("LSET", "str", "0", "a"), ("LINSERT", "str", "BEFORE", "a", "b"),
    ("LINSERT", "str", "NOPE", "a", "b"), ("LREM", "str", "0", "a"), ("LTRIM", "str", "0", "1"), ("LPOS", "str", "a"),
    ("LPOS", "str", "a", "RANK", "0"), ("RPUSH", "l", "a"), ("GET", "l"), ("SADD", "l", "a"), ("HSET", "l", "a", "b"),
    ("TYPE", "l"))
# --- sorted sets -------------------------------------------------------
# Every order a sorted set replies in is Redis's own (score, then member
# bytes), so most cases compare as they are. A step written (fn, command)
# has its reply normalised by fn: random picks, and the order of a large
# set's ZSCAN (its dict's).


def zseq(name, *commands, **kw) -> Case:
    plain, norms = [], kw.pop("norm", [])
    for i, c in enumerate(commands):
        if isinstance(c, tuple) and c and callable(c[0]):
            norms = norms + [at(i, c[0])]
            c = c[1]
        plain.append(c)
    return seq(name, *plain, norm=norms, **kw)


BIG = [s for i in range(200) for s in (str(i / 10), f"m{i}")]
seq("zset_zadd_basic", ("ZADD", "z", "1", "a", "2", "b", "3", "c"), ("ZADD", "z", "1", "a"), ("ZADD", "z", "CH", "5", "a"),
    ("ZADD", "z", "NX", "9", "a", "10", "d"), ("ZADD", "z", "XX", "7", "b", "8", "e"), ("ZADD", "z", "xx", "ch", "7", "b", "9", "c"),
    ("ZRANGE", "z", "0", "-1", "WITHSCORES"), ("ZCARD", "z"), ("TYPE", "z"), ("OBJECT", "ENCODING", "z"),
    ("ZADD", "z", "ch", "nx", "1", "q", "2", "a"), ("ZADD", "z", "XX", "1", "nope"), ("EXISTS", "nope"),
    ("ZADD", "nokey", "XX", "1", "a"), ("EXISTS", "nokey"), ("ZADD", "z", "1", "a", "1", "a", "2", "a"),
    ("ZSCORE", "z", "a"), ("ZADD", "z", "1", ""), ("ZRANGE", "z", "0", "-1"))
seq("zset_zadd_errors", ("ZADD", "z"), ("ZADD", "z", "1"), ("ZADD", "z", "1", "a", "2"), ("ZADD", "z", "NX", "XX", "1", "a"),
    ("ZADD", "z", "GT", "LT", "1", "a"), ("ZADD", "z", "GT", "NX", "1", "a"), ("ZADD", "z", "LT", "NX", "1", "a"),
    ("ZADD", "z", "INCR", "1", "a", "2", "b"), ("ZADD", "z", "x", "a"), ("ZADD", "z", "1", "a", "x", "b"), ("EXISTS", "z"),
    ("ZADD", "z", "nan", "a"), ("ZADD", "z", "NX"), ("ZADD", "z", "nx", "xx"), ("ZADD", "z", "1", "a", "CH", "2"),
    ("ZADD", "z", "NX", "XX", "1"), ("ZADD", "z", "INCR", "NX", "XX", "1", "a"), ("ZADD", "z", " 1", "a"),
    ("ZADD", "z", "1 ", "a"), ("ZADD", "z", "1e400", "a"), ("ZADD", "z", "", "a"), ("ZADD", "z", b"1\x00", "a"),
    ("ZADD", "z", "--1", "a"), ("EXISTS", "z"), ("ZINCRBY", "z", "nx", "m"), ("ZINCRBY", "z", "x", "m"),
    ("ZINCRBY", "z", "1"), ("ZINCRBY", "z", "1", "m", "n"))
seq("zset_zadd_gt_lt", ("ZADD", "z", "5", "a"), ("ZADD", "z", "GT", "3", "a"), ("ZADD", "z", "GT", "CH", "7", "a"),
    ("ZADD", "z", "LT", "9", "a"), ("ZADD", "z", "LT", "CH", "1", "a"), ("ZADD", "z", "GT", "100", "new"),
    ("ZADD", "z", "XX", "GT", "50", "a"), ("ZADD", "z", "XX", "LT", "CH", "40", "a", "1", "new"),
    ("ZRANGE", "z", "0", "-1", "WITHSCORES"), ("ZADD", "z", "GT", "INCR", "-1", "a"), ("ZADD", "z", "LT", "INCR", "-1", "a"),
    ("ZADD", "z", "GT", "CH", "40", "a"), ("ZSCORE", "z", "a"))
seq("zset_zadd_incr", ("ZADD", "z", "INCR", "1.5", "m"), ("ZADD", "z", "INCR", "1.5", "m"), ("ZINCRBY", "z", "2", "m"),
    ("ZINCRBY", "z", "-1e3", "m"), ("ZINCRBY", "z", "abc", "m"), ("ZADD", "z", "INCR", "inf", "m"),
    ("ZADD", "z", "INCR", "-inf", "m"), ("ZSCORE", "z", "m"), ("ZINCRBY", "z", "-inf", "m"), ("ZADD", "z", "XX", "INCR", "1", "nope"),
    ("ZADD", "z", "NX", "INCR", "1", "m"), ("ZINCRBY", "nokey", "5", "m"), ("ZSCORE", "nokey", "m"),
    ("ZINCRBY", "z", "0.1", "f"), ("ZINCRBY", "z", "0.2", "f"), ("ZADD", "z", "CH", "INCR", "0", "f"),
    ("ZINCRBY", "z", "9e308", "big"), ("ZINCRBY", "z", "9e308", "big"))
seq("zset_scores_written", ("ZADD", "z", "0.1", "a", "1e-7", "b", "1.5e300", "c", "-0", "d", "inf", "e", "-inf", "f",
                            "4611686018427387904", "g", "9223372036854775808", "h", "1e21", "i", "123456789.123456789", "j",
                            "0x1p3", "k", "9e308", "l", "1e-340", "m", ".5", "n", "5.", "o", "+1", "p", "0.30000000000000004", "q",
                            "-1.25e-300", "r", "4611686018427387903", "s", "1e-310", "t", "100", "u", "-7", "v"),
    ("ZRANGE", "z", "0", "-1", "WITHSCORES"), ("ZSCORE", "z", "d"), ("ZSCORE", "z", "h"), ("ZMSCORE", "z", "a", "x", "q"),
    ("ZSCAN", "z", "0"), ("OBJECT", "ENCODING", "z"))
zseq("zset_scores_written_large", ("ZADD", "z", *BIG), ("ZADD", "z", "-0", "negzero", "0.1", "tenth", "1e21", "huge", "1e-7", "tiny"),
     ("OBJECT", "ENCODING", "z"), ("ZSCORE", "z", "negzero"), ("ZSCORE", "z", "tenth"),
     ("ZRANGEBYSCORE", "z", "-1", "0.3", "WITHSCORES"), ("ZRANGE", "z", "-3", "-1", "WITHSCORES"),
     (scan_sorted(pairs=True), ("ZSCAN", "z", "0", "COUNT", "1000")),
     (scan_sorted(pairs=True), ("ZSCAN", "z", "0", "MATCH", "m1?", "COUNT", "1000")))
seq("zset_negative_zero", ("ZADD", "z", "-0", "a"), ("ZSCORE", "z", "a"), ("ZADD", "z", "0", "a"), ("ZADD", "z", "CH", "-0", "b"),
    ("ZRANGE", "z", "0", "-1", "WITHSCORES"), ("ZADD", "z", "INCR", "-0", "a"), ("ZINCRBY", "y", "-0", "a"),
    ("ZSCORE", "y", "a"), ("ZADD", "w", "1", "x" * 65, "-0", "a"), ("ZSCORE", "w", "a"), ("ZADD", "w", "0", "a"),
    ("ZSCORE", "w", "a"), ("ZRANGE", "w", "0", "0", "WITHSCORES"))
seq("zset_zrange_rank", ("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"), ("ZRANGE", "z", "0", "-1"),
    ("ZRANGE", "z", "-2", "-1"), ("ZRANGE", "z", "1", "100"), ("ZRANGE", "z", "3", "1"), ("ZRANGE", "z", "5", "6"),
    ("ZRANGE", "z", "-100", "0"), ("ZRANGE", "z", "0", "-1", "REV"), ("ZRANGE", "z", "0", "1", "rev", "withscores"),
    ("ZREVRANGE", "z", "0", "1"), ("ZREVRANGE", "z", "0", "-1", "WITHSCORES"), ("ZRANGE", "z", "x", "1"),
    ("ZRANGE", "z", "0", "1.5"), ("ZRANGE", "z", "0", "1", "LIMIT", "0", "1"), ("ZRANGE", "z", "0", "-1", "LIMIT", "0", "-1"),
    ("ZRANGE", "z", "0", "1", "FOO"), ("ZRANGE", "z", "0", "1", "WITHSCORES", "WITHSCORES"), ("ZRANGE", "z", "0", "1", "REV", "REV"),
    ("ZREVRANGE", "z", "0", "1", "REV"), ("ZREVRANGE", "z", "0", "1", "LIMIT", "0", "1"), ("ZRANGE", "nokey", "0", "-1"),
    ("ZRANGE", "z", "-9223372036854775808", "9223372036854775807"), ("ZRANGE", "z", "0", "1", "LIMIT", "0"),
    ("ZRANGE", "z", "0", "1", "LIMIT", "x", "1"), ("ZRANGE", "z", "0"), ("ZREVRANGE", "z", "-1", "-1"),
    ("ZRANGE", "z", "0", "-1", "BYSCORE", "BYLEX"), ("ZRANGE", "z", "-1", "0", b"withscores\x00x"))
seq("zset_zrange_score", ("ZADD", "z", "1", "a", "2", "b", "2", "c", "3", "d", "5", "e"), ("ZRANGE", "z", "2", "3", "BYSCORE"),
    ("ZRANGE", "z", "(2", "3", "BYSCORE"), ("ZRANGE", "z", "2", "(3", "BYSCORE"), ("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "WITHSCORES"),
    ("ZRANGE", "z", "3", "2", "BYSCORE"), ("ZRANGE", "z", "(2", "(2", "BYSCORE"), ("ZRANGE", "z", "2", "2", "BYSCORE"),
    ("ZRANGE", "z", "5", "1", "BYSCORE", "REV"), ("ZRANGE", "z", "1", "5", "BYSCORE", "REV"),
    ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "1", "2"), ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "1", "-1"),
    ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "0", "0"), ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "-1", "2"),
    ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "5", "1"), ("ZRANGE", "z", "-inf", "inf", "BYSCORE", "LIMIT", "4", "10"),
    ("ZRANGE", "z", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "1", "2"), ("ZRANGE", "z", "3", "1", "BYSCORE", "REV", "LIMIT", "2", "5"),
    ("ZRANGE", "z", "(5", "2", "BYSCORE", "REV", "LIMIT", "3", "1"),
    # Backwards, an offset past the first member yet below the length: nothing.
    ("ZRANGE", "z", "2", "-inf", "BYSCORE", "REV", "LIMIT", "3", "1"), ("ZREVRANGEBYSCORE", "z", "(2", "-inf", "LIMIT", "1", "1"),
    ("ZRANGEBYSCORE", "z", "2", "3"),
    ("ZRANGEBYSCORE", "z", "2", "3", "WITHSCORES", "LIMIT", "0", "1"), ("ZREVRANGEBYSCORE", "z", "3", "2"),
    ("ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "1", "1", "WITHSCORES"), ("ZRANGEBYSCORE", "z", "x", "1"),
    ("ZRANGEBYSCORE", "z", "(", "("), ("ZRANGEBYSCORE", "z", "", "2"), ("ZRANGEBYSCORE", "z", " 1", "2"),
    ("ZRANGEBYSCORE", "z", b"1\x00x", "2"), ("ZRANGEBYSCORE", "z", "1e400", "inf"), ("ZRANGEBYSCORE", "z", "nan", "1"),
    ("ZRANGEBYSCORE", "z", "1", "2", "BYSCORE"), ("ZRANGEBYSCORE", "z", "1", "2", "REV"), ("ZRANGEBYSCORE", "z", "-1e400", "(2"),
    ("ZRANGEBYSCORE", "z", "0x1p1", "0X1.8P1"), ("ZRANGEBYSCORE", "z", "1 ", "2"), ("ZRANGEBYSCORE", "z", "\t1", "2"),
    ("ZCOUNT", "z", "2", "3"), ("ZCOUNT", "z", "(2", "3"), ("ZCOUNT", "z", "-inf", "+inf"), ("ZCOUNT", "z", "3", "2"),
    ("ZCOUNT", "z", "x", "1"), ("ZCOUNT", "nokey", "0", "1"), ("ZCOUNT", "z", "6", "7"), ("ZCOUNT", "z", "-7", "0"),
    ("ZRANGEBYSCORE", "nokey", "0", "1"), ("ZRANGE", "z", "1", "2", "BYSCORE", "LIMIT", "0", "1", "LIMIT", "1", "1"),
    ("ZRANGEBYSCORE", "z", "1", "5", "LIMIT", "0", "9223372036854775807"))
seq("zset_zrange_lex", ("ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"), ("ZRANGE", "z", "[b", "[d", "BYLEX"),
    ("ZRANGE", "z", "(b", "(d", "BYLEX"), ("ZRANGE", "z", "-", "+", "BYLEX"), ("ZRANGE", "z", "+", "-", "BYLEX", "REV"),
    ("ZRANGE", "z", "[d", "[b", "BYLEX", "REV"), ("ZRANGE", "z", "-", "[c", "BYLEX", "LIMIT", "1", "1"),
    ("ZRANGE", "z", "-", "+", "BYLEX", "WITHSCORES"), ("ZRANGEBYLEX", "z", "[a", "(c"), ("ZREVRANGEBYLEX", "z", "+", "-"),
    ("ZREVRANGEBYLEX", "z", "+", "-", "LIMIT", "1", "2"), ("ZRANGEBYLEX", "z", "a", "c"), ("ZRANGEBYLEX", "z", "+x", "+"),
    ("ZRANGEBYLEX", "z", "-", "-"), ("ZRANGEBYLEX", "z", "+", "+"), ("ZRANGEBYLEX", "z", "[", "+"), ("ZRANGEBYLEX", "z", "(", "+"),
    ("ZRANGEBYLEX", "z", "-", b"+\x00x"), ("ZRANGEBYLEX", "z", b"-\x00", "(c"), ("ZRANGEBYLEX", "z", "[c", "[c"),
    ("ZRANGEBYLEX", "z", "(c", "[c"), ("ZRANGEBYLEX", "z", "+", "-"), ("ZRANGEBYLEX", "z", "-", "+", "WITHSCORES"),
    ("ZLEXCOUNT", "z", "-", "+"), ("ZLEXCOUNT", "z", "[b", "[d"), ("ZLEXCOUNT", "z", "x", "y"), ("ZLEXCOUNT", "nokey", "-", "+"),
    ("ZLEXCOUNT", "z", "(e", "+"), ("ZRANGEBYLEX", "z", "", "+"), ("ZRANGEBYLEX", "nokey", "-", "+"),
    # Members of different scores while packed: Redis walks the listpack.
    ("ZADD", "m", "1", "c", "2", "a", "3", "b", "4", "d"), ("ZRANGEBYLEX", "m", "[a", "[b"), ("ZRANGEBYLEX", "m", "[b", "+"),
    ("ZLEXCOUNT", "m", "-", "[b"), ("ZLEXCOUNT", "m", "[a", "+"), ("ZREVRANGEBYLEX", "m", "[b", "[a"),
    ("ZREVRANGEBYLEX", "m", "+", "[b"), ("ZRANGE", "m", "[b", "+", "BYLEX", "LIMIT", "1", "5"), ("ZREMRANGEBYLEX", "m", "[b", "+"),
    ("ZRANGE", "m", "0", "-1"),
    # The pre-check: nothing in range when the last member is below the
    # minimum or the first above the maximum, whatever comes between.
    ("ZADD", "n", "1", "z", "2", "a"), ("ZRANGEBYLEX", "n", "[b", "+"), ("ZLEXCOUNT", "n", "[b", "+"),
    ("ZREVRANGEBYLEX", "n", "(y", "-"), ("ZREMRANGEBYLEX", "n", "[b", "+"), ("ZRANGE", "n", "0", "-1"))
seq("zset_rank", ("ZADD", "z", "1", "a", "2", "b", "3", "c"), ("ZRANK", "z", "a"), ("ZRANK", "z", "c"), ("ZREVRANK", "z", "a"),
    ("ZRANK", "z", "nope"), ("ZRANK", "nokey", "a"), ("ZRANK", "z", "b", "WITHSCORE"), ("ZREVRANK", "z", "b", "withscore"),
    ("ZRANK", "z", "nope", "WITHSCORE"), ("ZRANK", "nokey", "a", "WITHSCORE"), ("ZRANK", "z", "a", "FOO"),
    ("ZRANK", "z", "a", "WITHSCORE", "x"), ("ZRANK", "z"), ("ZREVRANK", "z", "a", "b", "c"), ("ZRANK", "z", "a", "WITHSCORES"),
    ("ZREVRANK", "z", "nope", "WITHSCORE"), ("ZREVRANK", "nokey", "a"))
seq("zset_zrem_zremrange", ("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f"), ("ZREM", "z", "a", "nope"),
    ("ZREM", "z", "nope"), ("ZREM", "nokey", "a"), ("ZREMRANGEBYRANK", "z", "0", "0"), ("ZRANGE", "z", "0", "-1"),
    ("ZREMRANGEBYRANK", "z", "-1", "-1"), ("ZREMRANGEBYRANK", "z", "5", "10"), ("ZREMRANGEBYRANK", "z", "x", "1"),
    ("ZREMRANGEBYRANK", "z", "2", "1"), ("ZREMRANGEBYSCORE", "z", "(3", "4"), ("ZREMRANGEBYSCORE", "z", "x", "1"),
    ("ZREMRANGEBYSCORE", "z", "9", "10"), ("ZRANGE", "z", "0", "-1"), ("ZREM", "z", "c", "e", "x"), ("EXISTS", "z"),
    ("ZADD", "l", "0", "a", "0", "b", "0", "c"), ("ZREMRANGEBYLEX", "l", "[a", "(c"), ("ZREMRANGEBYLEX", "l", "x", "y"),
    ("ZREMRANGEBYLEX", "l", "-", "+"), ("EXISTS", "l"), ("ZREMRANGEBYRANK", "nokey", "0", "1"),
    ("ZREMRANGEBYSCORE", "nokey", "x", "1"), ("ZREMRANGEBYLEX", "nokey", "-", "+"), ("ZADD", "q", "1", "a", "2", "b"),
    ("ZREMRANGEBYRANK", "q", "-100", "100"), ("EXISTS", "q"), ("ZREM", "q"))
seq("zset_pop", ("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"), ("ZPOPMIN", "z"), ("ZPOPMAX", "z"), ("ZPOPMIN", "z", "0"),
    ("ZPOPMIN", "z", "5"), ("EXISTS", "z"), ("ZPOPMIN", "nokey"), ("ZPOPMIN", "nokey", "0"), ("ZPOPMIN", "z", "-1"),
    ("ZPOPMIN", "z", "x"), ("ZPOPMIN", "z", "1", "2"), ("ZPOPMAX", "nokey", "2"), ("SET", "str", "v"), ("ZPOPMIN", "str", "0"),
    ("ZADD", "y", "1", "a", "2", "b"), ("ZPOPMAX", "y", "1"), ("ZPOPMAX", "y", "9223372036854775807"), ("ZPOPMIN", "y"))
seq("zset_zrangestore", ("ZADD", "src", "1", "a", "2", "b", "3", "c"), ("ZRANGESTORE", "dst", "src", "0", "-1"),
    ("ZRANGE", "dst", "0", "-1", "WITHSCORES"), ("ZRANGESTORE", "dst", "src", "2", "1", "BYSCORE", "REV", "LIMIT", "0", "1"),
    ("ZRANGE", "dst", "0", "-1", "WITHSCORES"), ("ZRANGESTORE", "dst", "src", "[a", "[b", "BYLEX"), ("ZRANGE", "dst", "0", "-1"),
    ("ZRANGESTORE", "dst", "src", "5", "6"), ("EXISTS", "dst"), ("ZRANGESTORE", "dst", "nokey", "0", "-1"),
    ("ZRANGESTORE", "dst", "src", "0", "-1", "WITHSCORES"), ("SET", "str", "v"), ("ZRANGESTORE", "str", "src", "0", "0"),
    ("TYPE", "str"), ("ZRANGESTORE", "dst", "str", "0", "-1"), ("ZRANGESTORE", "dst", "src", "0", "1"), ("EXPIRE", "dst", "100"),
    ("ZRANGESTORE", "dst", "src", "0", "0"), ("TTL", "dst"), ("ZRANGESTORE", "src", "src", "1", "1"), ("ZRANGE", "src", "0", "-1"),
    ("ZRANGESTORE", "dst", "src", "x", "1"), ("ZRANGESTORE", "dst", "src", "0"), ("SET", "s2", "v"),
    ("ZRANGESTORE", "s2", "nokey", "0", "1"), ("EXISTS", "s2"), ("ZRANGESTORE", "dst", "src", "0", "1", "LIMIT", "0", "1"))
seq("zset_encoding", ("ZADD", "z", "1", "x" * 64), ("OBJECT", "ENCODING", "z"), ("ZADD", "z", "1", "y" * 65),
    ("OBJECT", "ENCODING", "z"), ("ZADD", "a", *[s for i in range(128) for s in (str(i), f"m{i}")]), ("OBJECT", "ENCODING", "a"),
    ("ZADD", "a", "1", "m1"), ("OBJECT", "ENCODING", "a"), ("ZADD", "a", "1", "one-more"), ("OBJECT", "ENCODING", "a"),
    ("ZADD", "b", *[s for i in range(129) for s in ("1", "same")]), ("OBJECT", "ENCODING", "b"), ("ZCARD", "b"),
    ("ZADD", "c", "1", "x" * 65), ("OBJECT", "ENCODING", "c"), ("ZREMRANGEBYRANK", "a", "0", "125"), ("OBJECT", "ENCODING", "a"),
    ("ZRANGESTORE", "d", "a", "0", "-1"), ("OBJECT", "ENCODING", "d"), ("ZADD", "e", *BIG), ("ZRANGESTORE", "f", "e", "0", "128"),
    ("OBJECT", "ENCODING", "f"), ("ZRANGESTORE", "g", "e", "0", "127"), ("OBJECT", "ENCODING", "g"),
    ("ZRANGESTORE", "h", "e", "-inf", "+inf", "BYSCORE"), ("OBJECT", "ENCODING", "h"),
    ("ZRANGESTORE", "i", "e", "0", "5", "BYSCORE"), ("OBJECT", "ENCODING", "i"), ("ZADD", "j", "1", "a"),
    ("ZADD", "j", *[s for i in range(129) for s in ("1", "a")]), ("OBJECT", "ENCODING", "j"))
seq("zset_large", ("ZADD", "z", *BIG), ("ZCARD", "z"), ("ZRANGE", "z", "100", "105", "WITHSCORES"),
    ("ZRANGE", "z", "-3", "-1"), ("ZREVRANGE", "z", "0", "2", "WITHSCORES"), ("ZRANK", "z", "m150"), ("ZREVRANK", "z", "m150", "WITHSCORE"),
    ("ZRANGEBYSCORE", "z", "(5", "6", "WITHSCORES"), ("ZRANGEBYSCORE", "z", "5", "+inf", "LIMIT", "10", "3"),
    ("ZREVRANGEBYSCORE", "z", "(5", "-inf", "LIMIT", "10", "3"), ("ZCOUNT", "z", "1", "(2"), ("ZCOUNT", "z", "-inf", "+inf"),
    ("ZREM", "z", "m0", "m199", "nope"), ("ZPOPMIN", "z", "2"), ("ZPOPMAX", "z", "2"), ("ZREMRANGEBYRANK", "z", "10", "19"),
    ("ZREMRANGEBYSCORE", "z", "(10", "12"), ("ZCARD", "z"), ("ZRANGE", "z", "5", "15", "WITHSCORES"), ("ZSCORE", "z", "m55"),
    ("ZADD", "z", "XX", "CH", "-5", "m55", "100", "m56"), ("ZRANGE", "z", "0", "0", "WITHSCORES"), ("ZRANGE", "z", "-1", "-1", "WITHSCORES"),
    ("ZINCRBY", "z", "1e3", "m57"), ("ZRANGE", "z", "-2", "-1", "WITHSCORES"), ("ZADD", "lex", *[s for i in range(150) for s in ("0", f"k{i:03}")]),
    ("ZRANGEBYLEX", "lex", "[k010", "(k015"), ("ZREVRANGEBYLEX", "lex", "(k015", "[k010", "LIMIT", "1", "2"), ("ZLEXCOUNT", "lex", "[k100", "+"),
    ("ZREMRANGEBYLEX", "lex", "-", "(k140"), ("ZRANGE", "lex", "0", "-1"), ("OBJECT", "ENCODING", "lex"))
seq("zset_wrongtype", ("SET", "str", "v"), ("ZADD", "str", "1", "a"), ("ZADD", "str", "x", "a"), ("ZINCRBY", "str", "1", "a"),
    ("ZREM", "str", "a"), ("ZCARD", "str"), ("ZSCORE", "str", "a"), ("ZMSCORE", "str", "a"), ("ZRANK", "str", "a"),
    ("ZRANK", "str", "a", "BAD"), ("ZREVRANK", "str", "a"), ("ZRANGE", "str", "0", "-1"), ("ZRANGE", "str", "x", "y"),
    ("ZRANGEBYSCORE", "str", "1", "2"), ("ZRANGEBYLEX", "str", "-", "+"), ("ZCOUNT", "str", "x", "1"), ("ZCOUNT", "str", "0", "1"),
    ("ZLEXCOUNT", "str", "-", "+"), ("ZREMRANGEBYRANK", "str", "0", "1"), ("ZREMRANGEBYSCORE", "str", "0", "1"),
    ("ZREMRANGEBYLEX", "str", "-", "+"), ("ZPOPMIN", "str"), ("ZPOPMAX", "str", "2"), ("ZRANDMEMBER", "str"),
    ("ZRANDMEMBER", "str", "2"), ("ZSCAN", "str", "0"), ("ZSCAN", "str", "x"), ("ZRANGESTORE", "d", "str", "0", "1"),
    ("ZADD", "z", "1", "a"), ("GET", "z"), ("SADD", "z", "x"), ("HSET", "z", "a", "1"), ("LPUSH", "z", "x"), ("INCR", "z"),
    ("SMEMBERS", "z"), ("TYPE", "z"), ("ZADD", "z", "XX", "1", "a"), ("SCAN", "0", "TYPE", "zset"))
zseq("zset_zrandmember", ("ZADD", "z", "1", "a", "2", "b", "3", "c"), ("ZRANDMEMBER", "nokey"), ("ZRANDMEMBER", "nokey", "2"),
     ("ZRANDMEMBER", "z", "3"), ("ZRANDMEMBER", "z", "5", "WITHSCORES"), (length_only, ("ZRANDMEMBER", "z", "2")),
     (length_only, ("ZRANDMEMBER", "z", "-5")), (length_only, ("ZRANDMEMBER", "z", "-5", "WITHSCORES")), ("ZRANDMEMBER", "z", "0"),
     ("ZRANDMEMBER", "z", "x"), ("ZRANDMEMBER", "z", "-9223372036854775808"), ("ZRANDMEMBER", "z", "1", "2"),
     ("ZRANDMEMBER", "z", "4611686018427387904", "WITHSCORES"), ("ZRANDMEMBER", "z", "-4611686018427387904", "WITHSCORES"),
     ("ZRANDMEMBER", "z", "4611686018427387903", "WITHSCORES"), (kind_only, ("ZRANDMEMBER", "z")),
     ("ZRANDMEMBER", "z", "1", "WITHSCORE"), ("ZADD", "big", *BIG), (length_only, ("ZRANDMEMBER", "big", "5", "WITHSCORES")),
     ("ZRANDMEMBER", "big", "0"), (length_only, ("ZRANDMEMBER", "big", "-3")),
     ("ZRANDMEMBER", "big", "300"), ("ZRANDMEMBER", "nokey", "-2", "WITHSCORES"))
seq("zset_zscan", ("ZADD", "z", "1.5", "a", "2", "b", "0.1", "c", "-0", "d", "inf", "e"), ("ZSCAN", "z", "0"),
    ("ZSCAN", "z", "0", "MATCH", "a*"), ("ZSCAN", "z", "0", "COUNT", "1"), ("ZSCAN", "z", "0", "NOVALUES"), ("ZSCAN", "nokey", "0"),
    ("ZSCAN", "z", "x"), ("ZSCAN", "z", "0", "TYPE", "string"), ("ZSCAN", "z", "0", "MATCH"), ("ZSCAN", "z"),
    ("ZSCAN", "nokey", "0", "COUNT", "0"), ("ZSCAN", "z", "0", "COUNT", "0"), ("HSET", "h", "f", "v", "g", "w"),
    ("HSCAN", "h", "0", "NOVALUES"), ("HSCAN", "h", "0", "novalues", "MATCH", "g"), ("HSCAN", "h", "0", "MATCH", "novalues"),
    ("SSCAN", "h", "0", "NOVALUES"), ("SCAN", "0", "NOVALUES"), ("SADD", "s", "1"), ("SSCAN", "s", "0", "NOVALUES"))
seq("zset_resp3", ("HELLO", "3"), ("ZADD", "z", "1.5", "a", "2", "b", "inf", "c"), ("ZSCORE", "z", "a"), ("ZSCORE", "z", "c"),
    ("ZSCORE", "z", "nope"), ("ZMSCORE", "z", "a", "nope"), ("ZRANGE", "z", "0", "-1", "WITHSCORES"), ("ZRANGE", "z", "0", "-1"),
    ("ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES"), ("ZRANGE", "nokey", "0", "-1", "WITHSCORES"), ("ZRANK", "z", "b", "WITHSCORE"),
    ("ZRANK", "z", "nope", "WITHSCORE"), ("ZRANK", "z", "nope"), ("ZINCRBY", "z", "1", "a"), ("ZADD", "z", "XX", "INCR", "1", "nope"),
    ("ZPOPMIN", "z"), ("ZPOPMIN", "z", "1"), ("ZPOPMAX", "z", "5"), ("ZPOPMIN", "z", "2"), ("ZPOPMIN", "nokey", "2"),
    ("ZADD", "y", "1", "a", "2", "b"), ("ZRANDMEMBER", "y", "2", "WITHSCORES"), ("ZRANDMEMBER", "y", "5"), ("ZSCAN", "y", "0"),
    ("ZRANGE", "y", "0", "0", "BYSCORE", "WITHSCORES"), norm=[at(0, mask_hello)])
ZSRC = [("ZADD", "a", "1", "x", "2", "y", "3", "z"), ("ZADD", "b", "4", "y", "5", "w"), ("SADD", "s", "y", "q")]
seq("zset_zunion", *ZSRC, ("ZUNION", "2", "a", "b"), ("ZUNION", "2", "a", "b", "WITHSCORES"),
    ("ZUNION", "3", "a", "b", "s", "WITHSCORES"), ("ZUNION", "2", "a", "b", "WEIGHTS", "2", "3", "WITHSCORES"),
    ("ZUNION", "2", "a", "b", "AGGREGATE", "MIN", "WITHSCORES"), ("ZUNION", "2", "a", "b", "aggregate", "max", "withscores"),
    ("ZUNION", "3", "a", "b", "s", "AGGREGATE", "COUNT", "WITHSCORES"),
    ("ZUNION", "3", "a", "b", "s", "WEIGHTS", "1", "2", "0.5", "AGGREGATE", "COUNT", "WITHSCORES"),
    ("ZUNION", "1", "nokey"), ("ZUNION", "2", "a", "nokey", "WITHSCORES"), ("ZUNION", "2", "a", "a", "WITHSCORES"),
    ("ZADD", "i", "1", "m"), ("ZADD", "j", "-1", "m"), ("ZUNION", "2", "i", "j", "WEIGHTS", "inf", "inf", "WITHSCORES"),
    ("ZADD", "k", "inf", "m"), ("ZUNION", "1", "k", "WEIGHTS", "0", "WITHSCORES"), ("ZUNION", "2", "k", "k", "WEIGHTS", "1", "-1", "WITHSCORES"),
    # -0 in a reply (a result is built large), 0 once stored packed.
    ("ZADD", "zz", "0", "m"), ("ZUNION", "1", "zz", "WEIGHTS", "-1", "WITHSCORES"), ("ZUNIONSTORE", "d", "1", "zz", "WEIGHTS", "-1"),
    ("ZSCORE", "d", "m"),
    # MIN and MAX keep the first of 0 and -0: the inputs' order, smallest first.
    ("ZADD", "m1", "0", "x"), ("ZADD", "m2", "0", "x"), ("ZUNION", "2", "m1", "m2", "WEIGHTS", "-1", "1", "AGGREGATE", "MIN", "WITHSCORES"),
    ("ZUNION", "2", "m1", "m2", "WEIGHTS", "1", "-1", "AGGREGATE", "MIN", "WITHSCORES"),
    ("ZUNION", "2", "m2", "m1", "WEIGHTS", "-1", "1", "AGGREGATE", "MAX", "WITHSCORES"),
    # Sums depend on their order: 0.1 + 0.2 + 0.3, then with the sets sized
    # so that the smallest goes first.
    ("ZADD", "f1", "0.1", "x"), ("ZADD", "f2", "0.2", "x"), ("ZADD", "f3", "0.3", "x"), ("ZUNION", "3", "f1", "f2", "f3", "WITHSCORES"),
    ("ZUNION", "3", "f3", "f2", "f1", "WITHSCORES"), ("ZADD", "f1", "1", "p", "2", "q"), ("ZADD", "f2", "1", "p"),
    ("ZUNION", "3", "f1", "f2", "f3", "WITHSCORES"), ("ZINTER", "3", "f1", "f2", "f3", "WITHSCORES"))
seq("zset_zinter", *ZSRC, ("ZINTER", "2", "a", "b"), ("ZINTER", "2", "a", "b", "WITHSCORES"), ("ZINTER", "3", "a", "b", "s", "WITHSCORES"),
    ("ZINTER", "2", "a", "s", "WEIGHTS", "10", "100", "WITHSCORES"), ("ZINTER", "2", "a", "b", "AGGREGATE", "MAX", "WITHSCORES"),
    ("ZINTER", "2", "a", "b", "AGGREGATE", "COUNT", "WITHSCORES"), ("ZINTER", "2", "a", "nokey"), ("ZINTER", "1", "a", "WITHSCORES"),
    ("ZINTER", "2", "a", "a", "WITHSCORES"), ("ZINTER", "2", "a", "a", "WEIGHTS", "1", "-1", "AGGREGATE", "MIN", "WITHSCORES"),
    ("ZINTER", "1", "s", "WITHSCORES"), ("ZINTER", "2", "s", "s", "WEIGHTS", "inf", "-inf", "WITHSCORES"),
    ("ZADD", "k", "inf", "y"), ("ZINTER", "2", "k", "a", "WEIGHTS", "0", "1", "WITHSCORES"),
    ("ZINTER", "2", "k", "a", "WEIGHTS", "0", "1", "AGGREGATE", "MIN", "WITHSCORES"),
    ("ZINTER", "2", "a", "k", "WEIGHTS", "1", "0", "AGGREGATE", "MAX", "WITHSCORES"))
seq("zset_zdiff", *ZSRC, ("ZDIFF", "2", "a", "b"), ("ZDIFF", "2", "a", "b", "WITHSCORES"), ("ZDIFF", "1", "a", "WITHSCORES"),
    ("ZDIFF", "2", "a", "a"), ("ZDIFF", "2", "nokey", "a"), ("ZDIFF", "2", "a", "s", "WITHSCORES"), ("ZDIFF", "2", "a", "nokey", "WITHSCORES"),
    ("ZDIFF", "3", "s", "a", "nokey", "WITHSCORES"), ("ZDIFF", "2", "a", "b", "WEIGHTS", "1", "2"), ("ZDIFF", "2", "a", "b", "AGGREGATE", "SUM"),
    ("ZDIFF", "3", "a", "b", "a"), ("ZDIFF", "2", "b", "s", "WITHSCORES"))
seq("zset_combine_errors", *ZSRC, ("ZUNION", "0", "a"), ("ZUNION", "x", "a"), ("ZUNION", "-1", "a"), ("ZUNION", "3", "a", "b"),
    ("ZUNION", "2", "a", "b", "WEIGHTS", "1"), ("ZUNION", "2", "a", "b", "WEIGHTS", "1", "x"), ("ZUNION", "2", "a", "b", "WEIGHTS", "1", "nan"),
    ("ZUNION", "2", "a", "b", "WEIGHTS", "1", " 2"), ("ZUNION", "2", "a", "b", "AGGREGATE"), ("ZUNION", "2", "a", "b", "AGGREGATE", "AVG"),
    ("ZUNION", "2", "a", "b", "FOO"), ("SET", "str", "v"), ("ZUNION", "2", "a", "str"), ("ZUNION", "2", "a", "str", "WEIGHTS", "x"),
    ("ZUNION", "2", "str", "nokey"), ("ZUNION", "2", "nokey", "str"), ("ZUNIONSTORE", "d", "0", "a"), ("ZINTERSTORE", "d", "x", "a"),
    ("ZDIFFSTORE", "d", "-5", "a"), ("ZINTERSTORE", "d", "1", "a", "WITHSCORES"), ("ZDIFFSTORE", "d", "1", "a", "WEIGHTS", "1"),
    ("ZUNION", "2", "a", "b", "WEIGHTS", "1", "2", "WEIGHTS", "3", "4", "WITHSCORES"), ("ZUNION", "2", "a", "b", "withscores", "withscores"),
    ("ZUNION", "1", "a", "AGGREGATE", "sum", "aggregate", "max", "WITHSCORES"), ("ZINTER", "0", "a"), ("ZDIFF", "0", "a"),
    ("ZINTER", "2", "a", "b", "LIMIT", "1"), ("ZUNION", "2", "a", "b", "WEIGHTS", "1", "2", "3"), ("ZUNION", "1", "a", b"withscores\x00"),
    ("ZUNION", "9223372036854775807", "a"), ("ZUNIONSTORE", "d", "2", "a"), ("ZUNIONSTORE", "d", "1", "str"), ("TYPE", "d"))
seq("zset_combine_store", *ZSRC, ("ZUNIONSTORE", "d", "2", "a", "b"), ("ZRANGE", "d", "0", "-1", "WITHSCORES"), ("OBJECT", "ENCODING", "d"),
    ("ZINTERSTORE", "d", "2", "a", "b", "WEIGHTS", "2", "3"), ("ZRANGE", "d", "0", "-1", "WITHSCORES"), ("ZDIFFSTORE", "d", "2", "a", "b"),
    ("ZRANGE", "d", "0", "-1", "WITHSCORES"), ("ZUNIONSTORE", "d", "1", "nokey"), ("EXISTS", "d"), ("ZINTERSTORE", "d", "2", "a", "nokey"),
    ("SET", "str", "v"), ("ZUNIONSTORE", "str", "1", "a"), ("TYPE", "str"), ("EXPIRE", "str", "100"), ("ZINTERSTORE", "str", "1", "s"),
    ("TTL", "str"), ("ZRANGE", "str", "0", "-1", "WITHSCORES"), ("ZUNIONSTORE", "a", "2", "a", "b"), ("ZRANGE", "a", "0", "-1", "WITHSCORES"),
    ("ZADD", "big", *BIG), ("ZUNIONSTORE", "d", "1", "big"), ("OBJECT", "ENCODING", "d"), ("ZINTERSTORE", "d", "2", "big", "big"),
    ("OBJECT", "ENCODING", "d"), ("ZRANGE", "d", "-2", "-1", "WITHSCORES"), ("ZDIFFSTORE", "d", "2", "big", "a"), ("ZCARD", "d"),
    ("ZADD", "long", "1", "x" * 65), ("ZUNIONSTORE", "d", "2", "long", "b"), ("OBJECT", "ENCODING", "d"),
    ("ZINTERSTORE", "d", "2", "long", "b"), ("EXISTS", "d"), ("ZDIFFSTORE", "e", "2", "big", "big"), ("EXISTS", "e"),
    ("ZUNIONSTORE", "d", "2", "a", "s", "AGGREGATE", "COUNT"), ("ZRANGE", "d", "0", "-1", "WITHSCORES"))
seq("zset_zintercard", *ZSRC, ("ZINTERCARD", "2", "a", "b"), ("ZINTERCARD", "3", "a", "b", "s"), ("ZINTERCARD", "1", "a", "LIMIT", "2"),
    ("ZINTERCARD", "1", "a", "LIMIT", "0"), ("ZINTERCARD", "1", "a", "LIMIT", "-1"), ("ZINTERCARD", "1", "a", "LIMIT", "x"),
    ("ZINTERCARD", "1", "a", "LIMIT", "1", "LIMIT", "5"), ("ZINTERCARD", "2", "a", "nokey"), ("ZINTERCARD", "0", "a"),
    ("ZINTERCARD", "2", "a", "b", "WITHSCORES"), ("ZINTERCARD", "2", "a", "b", "LIMIT"), ("SET", "str", "v"),
    ("ZINTERCARD", "2", "a", "str"), ("ZINTERCARD", "2", "a", "a"), ("ZINTERCARD", "2", "s", "s", "LIMIT", "1"),
    ("ZINTERCARD", "3", "a", "b"), ("ZINTERCARD", "1", "a", "WEIGHTS", "1"), ("ZINTERCARD", "1", "nokey", "LIMIT", "x"))
seq("zset_zmpop", *ZSRC, ("ZMPOP", "2", "nokey", "a", "MIN"), ("ZMPOP", "2", "a", "b", "MAX", "COUNT", "2"), ("ZMPOP", "1", "a", "min", "count", "10"),
    ("EXISTS", "a"), ("ZMPOP", "1", "nokey", "MIN"), ("ZMPOP", "0", "b", "MIN"), ("ZMPOP", "1", "b"), ("ZMPOP", "1", "b", "UP"),
    ("ZMPOP", "1", "b", "MIN", "COUNT", "0"), ("ZMPOP", "1", "b", "MIN", "COUNT", "x"), ("ZMPOP", "1", "b", "MIN", "COUNT", "1", "COUNT", "2"),
    ("ZMPOP", "1", "b", "MIN", "COUNT"), ("ZMPOP", "1", "b", "MIN", "FOO"), ("SET", "str", "v"), ("ZMPOP", "2", "str", "b", "MIN"),
    ("ZMPOP", "2", "nokey", "str", "MIN"), ("ZMPOP", "x", "b", "MIN"), ("ZMPOP", "2", "b", "MAX"), ("ZMPOP", "1", "s", "MIN"),
    ("ZMPOP", "9223372036854775807", "b", "MIN"), ("ZMPOP", "1", "b", "MAX", "COUNT", "9223372036854775807"), ("EXISTS", "b"))
seq("zset_multikey_resp3", ("HELLO", "3"), *ZSRC, ("ZUNION", "2", "a", "b", "WITHSCORES"), ("ZUNION", "2", "a", "b"),
    ("ZINTER", "2", "a", "b", "WITHSCORES"), ("ZDIFF", "2", "a", "b", "WITHSCORES"), ("ZUNION", "1", "nokey", "WITHSCORES"),
    ("ZMPOP", "1", "a", "MIN", "COUNT", "2"), ("ZMPOP", "1", "nokey", "MIN"), ("ZINTERCARD", "2", "a", "b"), norm=[at(0, mask_hello)])
seq("zset_expiry_and_keys", ("ZADD", "z", "1", "a"), ("EXPIRE", "z", "100"), ("TTL", "z"), ("ZADD", "z", "2", "b"), ("TTL", "z"),
    ("ZREM", "z", "a", "b"), ("TTL", "z"), ("ZADD", "z", "1", "a"), ("TTL", "z"), ("PEXPIRE", "z", "-1"), ("EXISTS", "z"),
    ("ZADD", "z", "1", "a"), ("DEL", "z"), ("ZCARD", "z"), ("ZADD", "z", "1", "a"), ("ZPOPMIN", "z"), ("EXISTS", "z"))
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
# --- floats ------------------------------------------------------------
# Redis on x86_64 adds in the x87's 80-bit long double and writes %.17Lf.
seq("incrbyfloat_basic", ("SET", "f", "10.5"), ("INCRBYFLOAT", "f", "0.1"), ("INCRBYFLOAT", "f", "5.0e3"),
    ("INCRBYFLOAT", "f", "-5010.6"), ("INCRBYFLOAT", "n", "3"), ("GET", "n"), ("SET", "i", "3.0"),
    ("INCRBYFLOAT", "i", "1.0"), ("INCRBYFLOAT", "i", "1.5e-20"), ("INCRBYFLOAT", "i", "0x10"),
    ("INCRBYFLOAT", "i", "10000000000000000000000"), ("INCRBYFLOAT", "i", "1.123456789012345678901"),
    ("SET", "j", "1.7976931348623157e308"), ("INCRBYFLOAT", "j", "1.7976931348623157e308"),
    ("INCRBYFLOAT", "big", "1e4932"), ("SET", "t", "-1e-20"), ("INCRBYFLOAT", "t", "0"),
    ("SET", "t", "1.00000000000000000005"), ("INCRBYFLOAT", "t", "0"), ("SET", "t", b"1.5\x00junk"),
    ("INCRBYFLOAT", "t", "1"), ("SET", "k", "5", "EX", "100"), ("INCRBYFLOAT", "k", "1"), ("TTL", "k"),
    norm=[at(23, int_range(98, 100))])
seq("incrbyfloat_strings", *[c for v in ["1e-4940", "1e-4950", "1e-5000", "-1e-20", "1.2e4932", "infinity", "-inf",
                                         "0x1p3", "0x1.8p-1", ".5", "5.", "+5", "-0", "1e-17", "5e-18", "1.5e-17",
                                         "2.5e-17", "00012", "1_0", "", " ", "1 ", " 1", "1e", "1e+", "0x", "0xg",
                                         "nan", "0x1p", "1e99999999999999999999", "0e9999999999", "0x1p-16446",
                                         "0x1p-16445", "0x0.ffffffffffffffffp16384", "0x1p16384", "\t1", "1" * 5119,
                                         "0x1p-18", "0x3p-18", "0" * 5119 + "1", "0" * 5118 + "1"]
                             for c in (("SET", "x", "0"), ("INCRBYFLOAT", "x", v))])
seq("incrbyfloat_errors", ("SET", "g", "abc"), ("INCRBYFLOAT", "g", "1"), ("INCRBYFLOAT", "g", "x"),
    ("SET", "m", "1.18e4932"), ("INCRBYFLOAT", "m", "1e4931"), ("GET", "m"), ("SET", "s", "inf"),
    ("INCRBYFLOAT", "s", "1"), ("INCRBYFLOAT", "nokey", "x"), ("EXISTS", "nokey"), ("HSET", "h", "a", "1"),
    ("INCRBYFLOAT", "h", "x"), ("INCRBYFLOAT", "h", "1"), ("INCRBYFLOAT", "f"), ("INCRBYFLOAT", "f", "1", "2"),
    ("SET", "y", "1" * 5120), ("INCRBYFLOAT", "y", "1"),
    # Only the sticky bit puts this past halfway: ...615; -0.5 ties to even.
    ("SET", "e", "18446744073709551616"), ("INCRBYFLOAT", "e", "-0x1.0000000000000002p-1"),
    ("SET", "e", "18446744073709551616"), ("INCRBYFLOAT", "e", "-0.5"))

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

# --- Redis 8 (7.2 to 8.10): commands, options, encodings ------------------
seq("r8_embstr_by_key_length", ("SET", "k", "x" * 40), ("OBJECT", "ENCODING", "k"), ("SET", "k", "x" * 41), ("OBJECT", "ENCODING", "k"),
    ("SET", "k" * 20, "x" * 21), ("OBJECT", "ENCODING", "k" * 20), ("SET", "k" * 20, "x" * 22), ("OBJECT", "ENCODING", "k" * 20),
    ("SET", "k", "x" * 30, "EX", "100"), ("OBJECT", "ENCODING", "k"), ("SET", "k", "12"), ("APPEND", "k", "3"),
    ("OBJECT", "ENCODING", "k"), ("SET", "f", "1.5"), ("INCRBYFLOAT", "f", "1.5"), ("OBJECT", "ENCODING", "f"),
    ("SET", "f", "4"), ("OBJECT", "ENCODING", "f"))
seq("r8_list_encodings", ("RPUSH", "l", *["y" * 100 for _ in range(70)]), ("OBJECT", "ENCODING", "l"),
    ("RPUSH", "l", *["y" * 100 for _ in range(20)]), ("OBJECT", "ENCODING", "l"), ("LTRIM", "l", "0", "50"), ("OBJECT", "ENCODING", "l"),
    ("LTRIM", "l", "0", "30"), ("OBJECT", "ENCODING", "l"), ("RPUSH", "big", "z" * 9000), ("OBJECT", "ENCODING", "big"),
    ("RPOP", "big"), ("RPUSH", "m", "a"), ("LSET", "m", "5", "z" * 9000), ("OBJECT", "ENCODING", "m"),
    ("RPUSH", "n", "a"), ("LINSERT", "n", "BEFORE", "nope", "z" * 9000), ("OBJECT", "ENCODING", "n"))
seq("r8_set_encodings", ("SADD", "s", "1", "2", "x"), ("OBJECT", "ENCODING", "s"), ("SMEMBERS", "s"), ("SSCAN", "s", "0", "COUNT", "1"),
    ("SADD", "t", *[f"m{i}" for i in range(128)]), ("OBJECT", "ENCODING", "t"), ("SADD", "t", "m128"), ("OBJECT", "ENCODING", "t"),
    ("SADD", "u", *[f"m{i}" for i in range(129)]), ("OBJECT", "ENCODING", "u"), ("SADD", "v", "y" * 65), ("OBJECT", "ENCODING", "v"))
seq("r8_client_setinfo", ("CLIENT", "SETINFO", "lib-name", "redis-py"), ("CLIENT", "SETINFO", "LIB-VER", "5.0.1"),
    ("CLIENT", "SETINFO", "lib-name", "a b"), ("CLIENT", "SETINFO", "lib-x", "v"), ("CLIENT", "SETINFO", "lib-name"),
    ("CLIENT", "NO-TOUCH", "on"), ("CLIENT", "NO-TOUCH", "OFF"), ("CLIENT", "NO-TOUCH", "x"))
seq("r8_sunioncard_sdiffcard", ("SADD", "a", "1", "2", "3", "x"), ("SADD", "b", "3", "4", "y"), ("SET", "str", "v"),
    ("SUNIONCARD", "2", "a", "b"), ("SUNIONCARD", "3", "a", "b", "nokey", "LIMIT", "3"), ("SUNIONCARD", "2", "a", "b", "APPROX"),
    ("SUNIONCARD", "2", "a", "b", "approx", "LIMIT", "2"), ("SUNIONCARD", "2", "a", "str"), ("SUNIONCARD", "0", "a"),
    ("SUNIONCARD", "3", "a", "b"), ("SUNIONCARD", "2", "a", "b", "LIMIT", "-1"), ("SUNIONCARD", "2", "a", "b", "FOO"),
    ("SDIFFCARD", "2", "a", "b"), ("SDIFFCARD", "2", "a", "a"), ("SDIFFCARD", "2", "nokey", "a"), ("SDIFFCARD", "2", "a", "b", "LIMIT", "1"),
    ("SDIFFCARD", "2", "a", "b", "APPROX"))
seq("r8_sunioncard_approx_big", ("SADD", "p", *[f"e{i}" for i in range(6000)]), ("SADD", "q", *[f"e{i}" for i in range(3000, 9000)]),
    ("SADD", "r", *[str(i) for i in range(0, 2000, 3)]), ("SUNIONCARD", "3", "p", "q", "r", "APPROX"),
    ("SUNIONCARD", "2", "p", "q", "APPROX", "LIMIT", "5000"), ("SUNIONCARD", "2", "p", "q"), ("SDIFFCARD", "2", "p", "q"))
seq("r8_digest", *[c for i, n in enumerate([0, 1, 3, 4, 8, 9, 16, 17, 128, 129, 240, 241, 1024, 1025, 3000])
                   for c in (("SET", f"d{i}", bytes((j * 31 + n) % 256 for j in range(n))), ("DIGEST", f"d{i}"))],
    ("SET", "i", "12345"), ("DIGEST", "i"), ("DIGEST", "nokey"), ("RPUSH", "l", "a"), ("DIGEST", "l"))
seq("r8_set_if", ("SET", "k", "v1"), ("SET", "k", "v2", "IFEQ", "v1"), ("SET", "k", "v3", "IFEQ", "no"), ("GET", "k"),
    ("SET", "k", "v4", "IFNE", "v2"), ("SET", "k", "v5", "IFNE", "zz"), ("SET", "new", "x", "IFEQ", "v"), ("SET", "new", "x", "IFNE", "v"),
    ("DIGEST", "k"), ("SET", "k", "v6", "IFDEQ", "short"), ("SET", "k", "v6", "IFDNE", "0123456789ABCDEF", "GET"),
    ("SET", "k", "v7", "IFEQ", "v6", "NX"), ("SET", "k", "v7", "IFEQ", "a", "IFNE", "b"), ("SET", "k", "v7", "IFEQ"),
    ("RPUSH", "l", "a"), ("SET", "l", "x", "IFEQ", "a"), ("SET", "l", "x", "IFDEQ", "short"), ("SET", "nx2", "x", "IFDEQ", "short"))
seq("r8_delex", ("SET", "k", "abc"), ("DELEX", "k", "IFEQ", "no"), ("DELEX", "k", "IFNE", "abc"), ("DELEX", "k", "IFEQ", "abc"),
    ("EXISTS", "k"), ("SET", "k", "abc"), ("DELEX", "k", "IFDEQ", "short"), ("DELEX", "k", "IFDNE", "0000000000000000"),
    ("DELEX", "k", "FOO", "x"), ("DELEX", "nokey", "FOO", "x"), ("DELEX", "k", "IFEQ"), ("DELEX", "k"), ("RPUSH", "l", "a"),
    ("DELEX", "l", "IFEQ", "a"), ("DELEX", "l"))
seq("r8_msetex", ("MSETEX", "2", "a", "1", "b", "2"), ("MGET", "a", "b"), ("MSETEX", "2", "a", "1", "c", "3", "NX"), ("EXISTS", "c"),
    ("MSETEX", "2", "c", "1", "d", "3", "NX", "EX", "100"), ("TTL", "c"), ("MSETEX", "1", "zz", "1", "XX"),
    ("MSETEX", "1", "c", "z", "KEEPTTL"), ("TTL", "c"), ("MSETEX", "1", "c", "z"), ("TTL", "c"), ("MSETEX", "0", "a", "1"),
    ("MSETEX", "x", "a", "1"), ("MSETEX", "2", "a", "1"), ("MSETEX", "1", "a", "1", "GET"), ("MSETEX", "1", "a", "1", "EX", "0"),
    ("MSETEX", "1", "a", "1", "EX", "x"), ("MSETEX", "1", "a", "1", "NX", "XX"), ("MSETEX", "1", "a", "1", "EXAT", "1"), ("EXISTS", "a"),
    norm=[at(5, int_range(98, 100)), at(8, int_range(98, 100))])
seq("r8_increx", ("INCREX", "n"), ("INCREX", "n", "BYINT", "10"), ("INCREX", "n", "BYINT", "100", "UBOUND", "50"),
    ("INCREX", "n", "BYINT", "100", "UBOUND", "50", "SATURATE"), ("INCREX", "n", "BYINT", "-100", "LBOUND", "0", "SATURATE"),
    ("SET", "m", "9223372036854775800"), ("INCREX", "m", "BYINT", "100"), ("INCREX", "m", "BYINT", "100", "SATURATE"),
    ("SET", "m", "-9223372036854775808"), ("INCREX", "m", "BYINT", "1", "UBOUND", "5", "LBOUND", "5", "SATURATE"),
    ("SET", "m", "9223372036854775807"), ("INCREX", "m", "LBOUND", "-10", "UBOUND", "-5", "SATURATE"),
    ("INCREX", "x", "BYINT", "a"), ("INCREX", "x", "BYINT", "1", "BYFLOAT", "2"), ("INCREX", "x", "LBOUND", "5", "UBOUND", "1"),
    ("INCREX", "x", "UBOUND", "1.5"), ("INCREX", "x", "FOO"), ("INCREX", "x", "ENX"), ("INCREX", "x", "PERSIST", "ENX"),
    ("INCREX", "x", "EX", "0"), ("INCREX", "x", "LBOUND", "9", "UBOUND", "1", "EX", "x"), ("SET", "s", "abc"), ("INCREX", "s"),
    ("INCREX", "nokey", "BYINT", "5", "UBOUND", "3"), ("EXISTS", "nokey"), ("INCREX", "t", "EX", "100"), ("TTL", "t"),
    ("INCREX", "t", "EX", "200", "ENX"), ("TTL", "t"), ("INCREX", "t", "PERSIST"), ("TTL", "t"), ("INCREX", "t", "EXAT", "1"),
    ("EXISTS", "t"), norm=[at(26, int_range(98, 100)), at(28, int_range(98, 100))])
seq("r8_increx_float", ("INCREX", "f", "BYFLOAT", "1.5"), ("INCREX", "f", "BYFLOAT", "0.1"), ("OBJECT", "ENCODING", "f"),
    ("INCREX", "f", "BYFLOAT", "10", "UBOUND", "5"), ("INCREX", "f", "BYFLOAT", "10", "UBOUND", "5", "SATURATE"),
    ("INCREX", "f", "BYFLOAT", "-100", "LBOUND", "-2.5", "SATURATE"), ("INCREX", "f", "BYFLOAT", "inf"), ("INCREX", "f", "BYFLOAT", "x"),
    ("INCREX", "f", "BYFLOAT", "1", "UBOUND", "-inf"), ("SET", "g", "1e4932"), ("INCREX", "g", "BYFLOAT", "1e4932"),
    ("INCREX", "g", "BYFLOAT", "1e4932", "SATURATE"), ("STRLEN", "g"), ("SET", "g", "inf"), ("INCREX", "g", "BYFLOAT", "1"),
    ("SET", "g", "-1e4932"), ("INCREX", "g", "BYFLOAT", "-1e4932", "SATURATE", "UBOUND", "1e4932"),
    ("HELLO", "3"), ("INCREX", "f", "BYFLOAT", "0.25"), ("INCREX", "n3"), norm=[at(17, mask_hello)])
seq("r8_lmovem", ("RPUSH", "s", "a", "b", "c", "d", "e"), ("LMOVEM", "s", "d", "LEFT", "RIGHT"),
    ("LMOVEM", "s", "d", "LEFT", "RIGHT", "COUNT", "2", "BULK"), ("LMOVEM", "s", "d", "RIGHT", "LEFT", "COUNT", "5", "OBO"),
    ("LRANGE", "d", "0", "-1"), ("EXISTS", "s"), ("RPUSH", "s", "1", "2", "3", "4"), ("LMOVEM", "s", "d", "RIGHT", "LEFT", "COUNT", "3", "BULK"),
    ("LMOVEM", "d", "d", "LEFT", "RIGHT", "COUNT", "3", "OBO"), ("LRANGE", "d", "0", "-1"),
    ("LMOVEM", "d", "e", "LEFT", "LEFT", "EXACTLY", "100", "BULK"), ("LMOVEM", "d", "e", "LEFT", "LEFT", "EXACTLY", "3", "BULK"),
    ("LMOVEM", "nokey", "e", "LEFT", "LEFT"), ("LMOVEM", "e", "d", "UP", "LEFT"), ("LMOVEM", "e", "d", "LEFT", "LEFT", "COUNT", "0", "OBO"),
    ("LMOVEM", "e", "d", "LEFT", "LEFT", "COUNT", "1"), ("LMOVEM", "e", "d", "LEFT", "LEFT", "COUNT", "1", "ALL"),
    ("SET", "str", "v"), ("LMOVEM", "e", "str", "LEFT", "LEFT"), ("LMOVEM", "nokey", "str", "LEFT", "LEFT"))
for _c in [("MSETEX", "1", "k"), ("DELEX",), ("DIGEST",), ("DIGEST", "a", "b"), ("INCREX",), ("SUNIONCARD", "1"),
           ("SDIFFCARD", "1"), ("LMOVEM", "a", "b", "LEFT")]:
    seq("arity_" + "_".join(a.lower() for a in _c), _c)

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

# --- AUTH (requirepass) ----------------------------------------------------
# Tagged "auth": run only by `difftest.py --password pw`, against servers
# started with that password (Redis: --requirepass pw; ours: --requirepass
# pw); the other cases run without one. Each case starts unauthenticated
# (the FLUSHALL before it logs in on a connection of its own); a case that
# changes the password puts it back.
AUTH = {"auth"}


def any_hello(i: int, b: bytes) -> bytes:
    """mask_hello on whatever entry is a HELLO reply."""
    return mask_hello(b) if b.startswith((b"%7\r\n", b"*14\r\n")) else b


def on(conn: int, *commands) -> list:
    """Steps: each command on connection `conn`, its reply read."""
    out = []
    for c in commands:
        out += [Send(_enc(c), conn=conn), Read(conn=conn)]
    return out


# Without a password (not tagged: the main run): the default user is `nopass`.
seq("nopass_auth", ("AUTH", "x"), ("AUTH", "default", "x"), ("AUTH", "bob", "x"), ("AUTH", "DEFAULT", "x"),
    ("AUTH", "internal connection", "x"), ("AUTH", "a", "b", "c"), ("HELLO", "3", "AUTH", "default", "x"),
    ("HELLO", "2", "AUTH", "bob", "x"), ("HELLO", "2", "AUTH", "bob", "x", "SETNAME", "a b"),
    ("CONFIG", "GET", "requirepass"), norm=[any_hello])
raw("nopass_large_request", b"*12\r\n" + b"$5\r\nRPUSH\r\n" + b"$1\r\nl\r\n" + b"$20000\r\n" + b"x" * 20000 + b"\r\n" + b"$1\r\na\r\n" * 9, 1)
seq("auth_required", ("PING",), ("GET", "k"), ("SET", "k", "v"), ("CLIENT", "SETNAME", "x"), ("CLUSTER", "INFO"),
    ("COMMAND", "COUNT"), ("SELECT", "0"), ("INFO",), ("CONFIG", "GET", "requirepass"), ("FLUSHALL",), tags=AUTH)
seq("auth_checks_before", ("FOO", "x"), ("GET",), ("CLIENT", "BOGUS"), ("HELLO", "4"), ("HELLO", "x"), ("AUTH",),
    tags=AUTH, doc="unknown commands, arity and HELLO's version fail as without a password")
seq("auth_hello_before", ("HELLO",), ("HELLO", "3"), ("HELLO", "3", "SETNAME", "x"), ("CLIENT", "GETNAME"), tags=AUTH)
raw("auth_required_inline", b"PING\r\nGET k\r\n", 2, tags=AUTH)
steps("auth_quit_before", Send(cmd("QUIT")), UntilClose(), tags=AUTH)
seq("auth_forms", ("AUTH", "wrong"), ("AUTH", "default", "wrong"), ("AUTH", "bob", "pw"), ("AUTH", "PW"),
    ("AUTH", "DEFAULT", "pw"), ("AUTH", "pw "), ("AUTH", ""), ("AUTH", "a", "b", "c"), ("PING",), ("AUTH", "pw"), ("PING",),
    ("AUTH", "wrong"), ("PING",), ("AUTH", "default", "pw"), ("GET", "k"), tags=AUTH)
seq("auth_internal_connection", ("AUTH", "internal connection", "pw"), ("PING",), tags=AUTH)
seq("auth_hello", ("HELLO", "3", "AUTH", "default", "wrong"), ("HELLO", "3", "AUTH", "bob", "pw"),
    ("HELLO", "3", "AUTH", "default", "wrong", "SETNAME", "a b"), ("HELLO", "3", "AUTH", "default"),
    ("HELLO", "AUTH", "default", "pw"), ("PING",), ("HELLO", "3", "AUTH", "default", "pw", "SETNAME", "n1"),
    ("CLIENT", "GETNAME"), ("HELLO", "2", "AUTH", "default", "wrong"), ("PING",), ("HELLO", "2"),
    norm=[any_hello], tags=AUTH)
seq("auth_hello_rejects_name_first", ("HELLO", "2", "AUTH", "default", "pw", "SETNAME", "a\nb"), ("PING",), tags=AUTH)
pipe("auth_pipelined", [("AUTH", "pw"), ("SET", "k", "v"), ("GET", "k")], tags=AUTH)
pipe("auth_pipelined_noauth_first", [("GET", "k"), ("AUTH", "pw"), ("GET", "k")], tags=AUTH)
pipe("auth_pipelined_wrong", [("PING",), ("AUTH", "wrong"), ("PING",)], tags=AUTH)
pipe("auth_pipelined_wrong_after", [("AUTH", "pw"), ("AUTH", "wrong"), ("PING",)], tags=AUTH)
pipe("auth_pipelined_noauth_30", [("GET", "k")] * 30 + [("AUTH", "pw"), ("GET", "k")], tags=AUTH)
raw("auth_limit_args", cmd("PING") + b"*11\r\n", CLOSE, tags=AUTH)
raw("auth_limit_args_10", b"*10\r\n" + b"$1\r\na\r\n" * 10, 1, tags=AUTH)
raw("auth_limit_bulk", b"*2\r\n$4\r\nAUTH\r\n$16385\r\n", CLOSE, tags=AUTH)
raw("auth_limit_bulk_16384", b"*2\r\n$4\r\nAUTH\r\n$16384\r\n" + b"x" * 16384 + b"\r\n", 1, tags=AUTH)
raw("auth_limit_lifted", cmd("AUTH", "pw") + b"*11\r\n" + b"$1\r\na\r\n" * 11, 2, tags=AUTH)
raw("auth_limit_lifted_bulk", cmd("AUTH", "pw") + cmd("SET", "k", "x" * 20000) + cmd("STRLEN", "k"), 3, tags=AUTH)
raw("auth_limit_split", [b"*2\r\n$4\r\nAUTH\r\n$", b"16385\r\n"], CLOSE, delay=0.05, tags=AUTH)
raw("auth_limit_inline_unlimited", b"PING " + b"x " * 20 + b"\r\n", 1, tags=AUTH)
raw("auth_inline", b"AUTH pw\r\nPING\r\n", 2, tags=AUTH)
steps("auth_change_password",
      *on(0, ("AUTH", "pw"), ("CONFIG", "GET", "requirepass"), ("CONFIG", "SET", "requirepass", "new"), ("PING",)),
      *on(1, ("PING",), ("AUTH", "pw"), ("AUTH", "new"), ("PING",)),
      *on(0, ("CONFIG", "GET", "req*"), ("CONFIG", "SET", "requirepass", "")),
      *on(2, ("PING",), ("AUTH", "x"), ("AUTH", "default", "x"), ("AUTH", "bob", "x"), ("HELLO", "3", "AUTH", "default", "x"),
          ("HELLO", "3", "AUTH", "bob", "x")),
      *on(0, ("CONFIG", "GET", "requirepass"), ("CONFIG", "SET", "requirepass", "pw")),
      *on(2, ("PING",)),
      *on(3, ("PING",)),
      *on(0, ("CONFIG", "SET", "requirepass", "x", "maxmemory", "bogus"), ("CONFIG", "SET", "requirepass", "a", "requirepass", "b"),
          ("CONFIG", "GET", "requirepass")),
      norm=[any_hello], tags=AUTH,
      doc="clients authenticated stay so; one that connected without a password counts as authenticated")
steps("auth_wrongpass_counted", *on(0, ("AUTH", "wrong"), ("HELLO", "2", "AUTH", "default", "wrong"), ("AUTH", "x", "y", "z"),
                                    ("AUTH", "pw"), ("INFO", "stats")),
      norm=[at(4, lambda b: b"<" + b"".join(l for l in b.split(b"\r\n") if l.startswith(b"acl_access_denied_auth")) + b">")],
      tags=AUTH, doc="acl_access_denied_auth counts the wrong passwords, not the syntax errors")


# --- MULTI / EXEC / DISCARD / WATCH ------------------------------------------
TX = {"tx"}
seq("tx_basic", ("MULTI",), ("SET", "a", "1"), ("INCR", "a"), ("GET", "a"), ("HSET", "h", "f", "v"), ("EXEC",), ("GET", "a"),
    tags=TX)
seq("tx_empty", ("MULTI",), ("EXEC",), tags=TX)
seq("tx_errors_outside", ("EXEC",), ("DISCARD",), ("UNWATCH",), ("MULTI",), ("MULTI",), ("SET", "a", "1"), ("EXEC",),
    ("GET", "a"), tags=TX, doc="a nested MULTI is an error that doesn't abort the transaction")
seq("tx_discard", ("MULTI",), ("SET", "a", "1"), ("DISCARD",), ("GET", "a"), ("EXEC",), tags=TX)
seq("tx_queue_unknown", ("MULTI",), ("SET", "a", "1"), ("FOO", "x"), ("GET", "a"), ("EXEC",), ("GET", "a"), tags=TX)
seq("tx_queue_arity", ("MULTI",), ("SET", "a", "1"), ("GET",), ("EXEC",), ("GET", "a"), tags=TX)
seq("tx_queue_bad_subcommand", ("MULTI",), ("CLIENT", "BOGUS"), ("EXEC",), tags=TX)
seq("tx_runtime_errors", ("MULTI",), ("SET", "a", "x"), ("INCR", "a"), ("SET", "b", "2"), ("LPUSH", "a", "y"), ("EXEC",),
    ("MGET", "a", "b"), tags=TX, doc="errors while running don't stop the others")
seq("tx_watch_inside", ("MULTI",), ("WATCH", "a"), ("SET", "a", "1"), ("EXEC",), tags=TX)
seq("tx_syntax_errors_queue", ("MULTI",), ("SET", "a", "1", "NX", "XX"), ("SET", "a", "2"), ("EXEC",), tags=TX,
    doc="a syntax error found while running is a reply, not an abort")
seq("tx_conn_commands", ("MULTI",), ("CLIENT", "SETNAME", "tx"), ("CLIENT", "GETNAME"), ("SELECT", "0"), ("PING",),
    ("ECHO", "e"), ("DBSIZE",), ("WAIT", "0", "0"), ("AUTH", "x"), ("CONFIG", "GET", "maxmemory"), ("EXEC",), tags=TX)
seq("tx_wait", ("MULTI",), ("WAIT", "1", "0"), ("EXEC",), tags=TX, doc="no blocking in a transaction")
seq("tx_info_clients", ("WATCH", "a", "b"), ("INFO", "clients"), ("UNWATCH",), ("INFO", "clients"),
    norm=[lambda i, b: b"<" + b",".join(l for l in b.split(b"\r\n") if l.startswith((b"watching_clients", b"total_watched_keys"))) + b">"
          if b"# Clients" in b else b], tags=TX)
seq("tx_resp3", ("HELLO", "3"), ("MULTI",), ("HSET", "h", "f", "v"), ("HGETALL", "h"), ("SMEMBERS", "nos"), ("GET", "no"),
    ("INCRBYFLOAT", "f", "1.5"), ("EXEC",), norm=[at(0, mask_hello)], tags=TX)
pipe("tx_pipelined", [("MULTI",), ("SET", "a", "1"), ("INCR", "a"), ("EXEC",), ("GET", "a")], tags=TX)
pipe("tx_pipelined_abort", [("MULTI",), ("SET", "a", "1"), ("NOPE",), ("EXEC",), ("GET", "a")], tags=TX)
pipe("tx_many", [("MULTI",)] + [("RPUSH", "l", str(i)) for i in range(500)] + [("EXEC",), ("LLEN", "l")], tags=TX)
steps("tx_quit_inside", Send(cmd("MULTI")), Read(), Send(cmd("SET", "a", "1")), Read(), Send(cmd("QUIT")), UntilClose(),
      Send(cmd("GET", "a"), conn=1), Read(conn=1), tags=TX)
steps("tx_atomic_for_others", *on(0, ("MULTI",), ("SET", "a", "1")), *on(1, ("GET", "a")), *on(0, ("SET", "b", "2"), ("EXEC",)),
      *on(1, ("MGET", "a", "b")), tags=TX)
seq("tx_oom_at_queue", ("CONFIG", "SET", "maxmemory", "1"), ("MULTI",), ("SET", "a", "1"), ("GET", "a"), ("EXEC",),
    ("CONFIG", "SET", "maxmemory", "0"), ("GET", "a"), tags=TX)
seq("tx_oom_at_exec", ("MULTI",), ("SET", "a", "1"), ("CONFIG", "SET", "maxmemory", "1"), ("EXEC",),
    ("CONFIG", "SET", "maxmemory", "0"), ("GET", "a"), tags=TX, doc="the CONFIG SET queued: runs in EXEC")
steps("tx_oom_at_exec_other", *on(0, ("MULTI",), ("SET", "a", "1")), *on(1, ("CONFIG", "SET", "maxmemory", "1")),
      *on(0, ("EXEC",)), *on(1, ("CONFIG", "SET", "maxmemory", "0")), *on(0, ("GET", "a")), tags=TX)
steps("tx_oom_reads_pass", *on(1, ("SET", "a", "1"), ("CONFIG", "SET", "maxmemory", "1")), *on(0, ("MULTI",), ("GET", "a"),
      ("DEL", "a"), ("EXEC",)), *on(1, ("CONFIG", "SET", "maxmemory", "0")), tags=TX)

# WATCH: conn 1 runs `cmd` between conn 0's WATCH and EXEC; EXEC is nil if
# Redis took the key as modified (signalModifiedKey), else runs.
WATCHES = [
    # (name, setup, command on conn 1)
    ("set", [], ("SET", "k", "v")),
    ("set_same", [("SET", "k", "v")], ("SET", "k", "v")),
    ("set_nx_fails", [("SET", "k", "v")], ("SET", "k", "w", "NX")),
    ("set_xx_fails", [], ("SET", "k", "w", "XX")),
    ("set_ifeq_fails", [("SET", "k", "v")], ("SET", "k", "w", "IFEQ", "x")),
    ("set_get", [("SET", "k", "v")], ("SET", "k", "w", "GET")),
    ("setnx_fails", [("SET", "k", "v")], ("SETNX", "k", "w")),
    ("setnx", [], ("SETNX", "k", "w")),
    ("mset", [], ("MSET", "x", "1", "k", "2")),
    ("msetex_nx_fails", [("SET", "k", "v")], ("MSETEX", "2", "k", "1", "z", "2", "NX")),
    ("msetex", [], ("MSETEX", "1", "k", "1", "PX", "100000")),
    ("del", [("SET", "k", "v")], ("DEL", "k")),
    ("del_missing", [], ("DEL", "k")),
    ("del_other", [("SET", "x", "v")], ("DEL", "x")),
    ("delex_fails", [("SET", "k", "v")], ("DELEX", "k", "IFEQ", "x")),
    ("delex", [("SET", "k", "v")], ("DELEX", "k", "IFEQ", "v")),
    ("incr", [], ("INCR", "k")),
    ("incrby_0", [("SET", "k", "1")], ("INCRBY", "k", "0")),
    ("incr_fails", [("SET", "k", "x")], ("INCR", "k")),
    ("incrbyfloat", [], ("INCRBYFLOAT", "k", "1.5")),
    ("increx", [], ("INCREX", "k", "BYINT", "2")),
    ("append_empty", [("SET", "k", "v")], ("APPEND", "k", "")),
    ("append", [], ("APPEND", "k", "x")),
    ("getdel", [("SET", "k", "v")], ("GETDEL", "k")),
    ("getdel_missing", [], ("GETDEL", "k")),
    ("get", [("SET", "k", "v")], ("GET", "k")),
    ("expire", [("SET", "k", "v")], ("EXPIRE", "k", "100")),
    ("expire_missing", [], ("EXPIRE", "k", "100")),
    ("expire_nx_fails", [("SET", "k", "v", "EX", "100")], ("EXPIRE", "k", "200", "NX")),
    ("expire_past", [("SET", "k", "v")], ("PEXPIREAT", "k", "1")),
    ("persist", [("SET", "k", "v", "EX", "100")], ("PERSIST", "k")),
    ("persist_no_ttl", [("SET", "k", "v")], ("PERSIST", "k")),
    ("flushall", [("SET", "k", "v")], ("FLUSHALL",)),
    ("flushall_missing", [("SET", "x", "v")], ("FLUSHALL",)),
    ("flushdb", [("SET", "k", "v")], ("FLUSHDB",)),
    ("hset", [], ("HSET", "k", "f", "v")),
    ("hset_same", [("HSET", "k", "f", "v")], ("HSET", "k", "f", "v")),
    ("hsetnx_fails", [("HSET", "k", "f", "v")], ("HSETNX", "k", "f", "w")),
    ("hdel", [("HSET", "k", "f", "v")], ("HDEL", "k", "f")),
    ("hdel_missing", [("HSET", "k", "f", "v")], ("HDEL", "k", "g")),
    ("hincrby", [], ("HINCRBY", "k", "f", "1")),
    ("hincrbyfloat", [], ("HINCRBYFLOAT", "k", "f", "1.5")),
    ("hget", [("HSET", "k", "f", "v")], ("HGET", "k", "f")),
    ("sadd", [], ("SADD", "k", "a")),
    ("sadd_existing", [("SADD", "k", "a")], ("SADD", "k", "a")),
    ("srem", [("SADD", "k", "a", "b")], ("SREM", "k", "a")),
    ("srem_missing", [("SADD", "k", "a")], ("SREM", "k", "z")),
    ("spop", [("SADD", "k", "a")], ("SPOP", "k")),
    ("spop_missing", [], ("SPOP", "k")),
    ("smove_from", [("SADD", "k", "a")], ("SMOVE", "k", "d", "a")),
    ("smove_to", [("SADD", "s", "a")], ("SMOVE", "s", "k", "a")),
    ("smove_missing", [("SADD", "k", "a")], ("SMOVE", "k", "d", "z")),
    ("sinterstore_empty", [], ("SINTERSTORE", "k", "nosuch")),
    ("sinterstore_empty_existing", [("SET", "k", "v")], ("SINTERSTORE", "k", "nosuch")),
    ("sunionstore", [("SADD", "s", "a")], ("SUNIONSTORE", "k", "s")),
    ("lpush", [], ("LPUSH", "k", "a")),
    ("lpushx_missing", [], ("LPUSHX", "k", "a")),
    ("lpop", [("RPUSH", "k", "a")], ("LPOP", "k")),
    ("lpop_missing", [], ("LPOP", "k")),
    ("lset", [("RPUSH", "k", "a")], ("LSET", "k", "0", "a")),
    ("linsert_missing_pivot", [("RPUSH", "k", "a")], ("LINSERT", "k", "BEFORE", "z", "x")),
    ("lrem_none", [("RPUSH", "k", "a")], ("LREM", "k", "0", "z")),
    ("lrem", [("RPUSH", "k", "a")], ("LREM", "k", "0", "a")),
    ("ltrim_nothing", [("RPUSH", "k", "a", "b")], ("LTRIM", "k", "0", "-1")),
    ("ltrim", [("RPUSH", "k", "a", "b")], ("LTRIM", "k", "0", "0")),
    ("lmove_from", [("RPUSH", "k", "a")], ("LMOVE", "k", "d", "LEFT", "RIGHT")),
    ("lmove_to", [("RPUSH", "s", "a")], ("LMOVE", "s", "k", "LEFT", "RIGHT")),
    ("lmove_missing", [], ("LMOVE", "k", "d", "LEFT", "RIGHT")),
    ("lmpop", [("RPUSH", "k", "a")], ("LMPOP", "1", "k", "LEFT")),
    ("lmovem", [("RPUSH", "k", "a", "b")], ("LMOVEM", "k", "d", "LEFT", "RIGHT")),
    ("rpoplpush_same", [("RPUSH", "k", "a")], ("RPOPLPUSH", "k", "k")),
    # Sorted sets (phase 9): modified only when something changed.
    ("zadd", [], ("ZADD", "k", "1", "a")),
    ("zadd_same", [("ZADD", "k", "1", "a")], ("ZADD", "k", "1", "a")),
    ("zadd_negzero_same", [("ZADD", "k", "0", "a")], ("ZADD", "k", "-0", "a")),
    ("zadd_nx_fails", [("ZADD", "k", "1", "a")], ("ZADD", "k", "NX", "2", "a")),
    ("zadd_xx_missing", [], ("ZADD", "k", "XX", "1", "a")),
    ("zadd_gt_fails", [("ZADD", "k", "5", "a")], ("ZADD", "k", "GT", "1", "a")),
    ("zincrby", [("ZADD", "k", "1", "a")], ("ZINCRBY", "k", "1", "a")),
    ("zincrby_0", [("ZADD", "k", "1", "a")], ("ZINCRBY", "k", "0", "a")),
    ("zadd_incr_nan", [("ZADD", "k", "inf", "a")], ("ZADD", "k", "INCR", "-inf", "a")),
    ("zrem", [("ZADD", "k", "1", "a", "2", "b")], ("ZREM", "k", "a")),
    ("zrem_missing", [("ZADD", "k", "1", "a")], ("ZREM", "k", "z")),
    ("zrem_last", [("ZADD", "k", "1", "a")], ("ZREM", "k", "a")),
    ("zremrangebyscore_none", [("ZADD", "k", "1", "a")], ("ZREMRANGEBYSCORE", "k", "5", "6")),
    ("zremrangebyrank", [("ZADD", "k", "1", "a", "2", "b")], ("ZREMRANGEBYRANK", "k", "0", "0")),
    ("zremrangebylex_none", [("ZADD", "k", "0", "a")], ("ZREMRANGEBYLEX", "k", "[x", "+")),
    ("zpopmin", [("ZADD", "k", "1", "a", "2", "b")], ("ZPOPMIN", "k")),
    ("zpopmax_0", [("ZADD", "k", "1", "a")], ("ZPOPMAX", "k", "0")),
    ("zpopmin_missing", [], ("ZPOPMIN", "k")),
    ("zmpop", [("ZADD", "k", "1", "a")], ("ZMPOP", "1", "k", "MIN")),
    ("zrangestore", [("ZADD", "s", "1", "a")], ("ZRANGESTORE", "k", "s", "0", "-1")),
    ("zrangestore_empty_missing", [], ("ZRANGESTORE", "k", "nosuch", "0", "-1")),
    ("zrangestore_empty_existing", [("SET", "k", "v")], ("ZRANGESTORE", "k", "nosuch", "0", "-1")),
    ("zrangestore_from", [("ZADD", "k", "1", "a")], ("ZRANGESTORE", "d", "k", "0", "-1")),
    ("zunionstore", [("ZADD", "s", "1", "a")], ("ZUNIONSTORE", "k", "1", "s")),
    ("zinterstore_empty_missing", [], ("ZINTERSTORE", "k", "1", "nosuch")),
    ("zdiffstore_empty_existing", [("ZADD", "k", "1", "a")], ("ZDIFFSTORE", "k", "2", "k", "k")),
    ("zunion_read", [("ZADD", "k", "1", "a")], ("ZUNION", "1", "k")),
]
for name, setup, command in WATCHES:
    steps(f"tx_watch_{name}", *on(0, *setup, ("WATCH", "k")), *on(1, command), *on(0, ("MULTI",), ("PING",), ("EXEC",)),
          tags=TX)
steps("tx_watch_own_write", *on(0, ("WATCH", "k"), ("SET", "k", "v"), ("MULTI",), ("PING",), ("EXEC",)), tags=TX)
steps("tx_watch_inside_tx", *on(0, ("WATCH", "k"), ("MULTI",), ("SET", "k", "v"), ("EXEC",), ("MULTI",), ("PING",), ("EXEC",)),
      tags=TX, doc="the transaction's own writes don't count; EXEC unwatches")
steps("tx_watch_other_tx", *on(0, ("WATCH", "k")), *on(1, ("MULTI",), ("SET", "k", "v"), ("EXEC",)),
      *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX)
steps("tx_watch_several", *on(0, ("WATCH", "a", "b"), ("WATCH", "c")), *on(1, ("SET", "c", "1")),
      *on(0, ("MULTI",), ("PING",), ("EXEC",), ("MULTI",), ("PING",), ("EXEC",)), tags=TX)
steps("tx_unwatch", *on(0, ("WATCH", "k")), *on(1, ("SET", "k", "1")), *on(0, ("UNWATCH",), ("MULTI",), ("PING",), ("EXEC",)),
      tags=TX)
steps("tx_discard_unwatches", *on(0, ("WATCH", "k"), ("MULTI",), ("DISCARD",)), *on(1, ("SET", "k", "1")),
      *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX)
steps("tx_watch_after_dirty", *on(0, ("WATCH", "k")), *on(1, ("SET", "k", "1")), *on(0, ("WATCH", "j"), ("MULTI",), ("PING",), ("EXEC",)),
      tags=TX)
steps("tx_watch_resp3", *on(0, ("HELLO", "3"), ("WATCH", "k")), *on(1, ("SET", "k", "1")), *on(0, ("MULTI",), ("PING",), ("EXEC",)),
      norm=[at(0, mask_hello)], tags=TX)
steps("tx_watch_dirty_queue_error", *on(0, ("WATCH", "k")), *on(1, ("SET", "k", "1")), *on(0, ("MULTI",), ("NOPE",), ("EXEC",)),
      tags=TX, doc="a queueing error wins over the WATCH")
steps("tx_watch_expires", *on(0, ("SET", "k", "v", "PX", "100"), ("WATCH", "k")), Sleep(0.25),
      *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX, slow=True)
steps("tx_watch_expires_just_now",
      *on(0, ("MSETEX", "20000", *[x for i in range(20000) for x in (f"x{i}", "v")], "EX", "1000"),
          ("SET", "k", "v", "PX", "20"), ("WATCH", "k")), Sleep(0.03),
      *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX, slow=True,
      doc="expired, not reclaimed yet (the active expiry has 20000 other keys to go through first): EXEC finds it expired")
steps("tx_watch_expired_already", *on(0, ("SET", "k", "v", "PX", "20")), Sleep(0.1), *on(0, ("WATCH", "k"), ("MULTI",), ("PING",), ("EXEC",)),
      tags=TX, slow=True)
steps("tx_watch_expired_then_deleted", *on(0, ("SET", "k", "v", "PX", "20")), Sleep(0.1), *on(0, ("WATCH", "k")),
      *on(1, ("DEL", "k")), *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX, slow=True)
steps("tx_watch_expired_then_set", *on(0, ("SET", "k", "v", "PX", "20")), Sleep(0.1), *on(0, ("WATCH", "k")),
      *on(1, ("SET", "k", "w")), *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX, slow=True)
steps("tx_watch_expired_then_flushall", *on(0, ("SET", "k", "v", "PX", "20")), Sleep(0.1), *on(0, ("WATCH", "k")),
      *on(1, ("FLUSHALL",)), *on(0, ("MULTI",), ("PING",), ("EXEC",)), tags=TX, slow=True)
steps("tx_client_info", *on(0, ("WATCH", "a", "b"), ("CLIENT", "INFO"), ("MULTI",), ("SET", "a", "1"), ("CLIENT", "INFO"), ("EXEC",)),
      *on(1, ("WATCH", "x")), *on(0, ("SET", "x", "1")), *on(1, ("CLIENT", "INFO")),
      norm=[lambda i, b: b"<" + b" ".join(f for f in b.split() if f.startswith((b"flags=", b"multi=", b"watch=", b"cmd="))) + b">"
            if b"id=" in b else b], tags=TX)


def by_name():
    return {c.name: c for c in CASES}
