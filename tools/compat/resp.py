"""Small stdlib-only RESP2/RESP3 client for compatibility tests.

Sends raw bytes (optionally split into chunks with delays, to simulate
packets arriving in pieces) and reads complete replies with exact framing,
keeping the raw bytes of every reply so they can be compared byte for byte.

    from resp import Conn, cmd
    with Conn("127.0.0.1", 6379) as c:
        c.send(cmd("SET", "k", b"v\r\n"))
        raw, reply = c.read_reply()          # b"+OK\r\n", Reply('+', b'OK')
        c.send(b"PING\r\nPING\r\n", chunk=1, delay=0.001)
        replies = c.read_replies(2)
        rest, closed = c.read_until_close()  # replies until EOF / timeout

Reply objects: Reply(kind, value[, attrs]) where kind is the RESP type byte
as a str ('+', '-', ':', '$', '*', '_', ',', '#', '!', '=', '(', '%', '~',
'>', '|'). value: bytes for + - $ ! = ( , (as text) ; int for ':' ; bool for
'#' ; None for '_' and RESP2 nulls ($-1 / *-1) ; list of Reply for * ~ > ;
list of (Reply, Reply) for '%' and '|'.

Command line:  python3 resp.py HOST:PORT [--resp3] CMD [ARG ...]
"""

from __future__ import annotations

import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence, Tuple, Union

CRLF = b"\r\n"


class Incomplete(Exception):
    """The buffer doesn't hold a complete reply yet."""


class ProtocolError(Exception):
    """The server sent bytes that aren't valid RESP."""


class Closed(Exception):
    """The server closed the connection (EOF) before a complete reply."""

    def __init__(self, partial: bytes = b""):
        super().__init__("connection closed by server" + (f" ({len(partial)} bytes of an incomplete reply pending)" if partial else ""))
        self.partial = partial


class Timeout(Exception):
    """No complete reply before the deadline."""

    def __init__(self, partial: bytes = b""):
        super().__init__("timeout waiting for reply" + (f" ({len(partial)} bytes of an incomplete reply pending)" if partial else ""))
        self.partial = partial


@dataclass
class Reply:
    kind: str
    value: Any
    attrs: Optional[list] = field(default=None)

    def __repr__(self) -> str:
        if self.kind in "*~>":
            inner = ", ".join(repr(v) for v in self.value) if self.value is not None else "nil"
            return f"{self.kind}[{inner}]"
        if self.kind in "%|":
            inner = ", ".join(f"{k!r}: {v!r}" for k, v in self.value)
            return f"{self.kind}{{{inner}}}"
        return f"{self.kind}{self.value!r}"

    def encode(self) -> bytes:
        """Re-serialise (RESP2 null bulk/array come back as $-1 / *-1)."""
        k, v = self.kind, self.value
        pre = b""
        if self.attrs is not None:
            pre = Reply("|", self.attrs).encode()
        if k in "+-":
            return pre + k.encode() + v + CRLF
        if k == ":":
            return pre + b":%d\r\n" % v
        if k == ",":
            return pre + b"," + v + CRLF
        if k == "(":
            return pre + b"(" + v + CRLF
        if k == "#":
            return pre + (b"#t\r\n" if v else b"#f\r\n")
        if k == "_":
            return pre + b"_\r\n"
        if k in "$!=":
            if v is None:
                return pre + k.encode() + b"-1\r\n"
            return pre + k.encode() + str(len(v)).encode() + CRLF + v + CRLF
        if k in "*~>":
            if v is None:
                return pre + k.encode() + b"-1\r\n"
            return pre + k.encode() + str(len(v)).encode() + CRLF + b"".join(x.encode() for x in v)
        if k in "%|":
            return pre + k.encode() + str(len(v)).encode() + CRLF + b"".join(a.encode() + b.encode() for a, b in v)
        raise ValueError(f"unknown kind {k!r}")

    # convenience
    def to_py(self) -> Any:
        """Plain Python value: bytes/int/None/list/dict; errors become ErrorReply."""
        k, v = self.kind, self.value
        if k in "*~>":
            return None if v is None else [x.to_py() for x in v]
        if k == "%":
            return {_hashable(a.to_py()): b.to_py() for a, b in v}
        if k in "-!":
            return ErrorReply(v)
        if k == "=":
            return v[4:] if v is not None else None  # strip "txt:"
        return v


class ErrorReply(bytes):
    pass


def _hashable(x):
    return tuple(x) if isinstance(x, list) else x


def _line(buf: bytes, pos: int) -> Tuple[bytes, int]:
    end = buf.find(CRLF, pos)
    if end < 0:
        raise Incomplete
    return buf[pos:end], end + 2


def _int(b: bytes) -> int:
    try:
        return int(b)
    except ValueError:
        raise ProtocolError(f"bad integer {b!r}")


def parse(buf: bytes, pos: int = 0) -> Tuple[Reply, int]:
    """Parses one reply starting at pos. Returns (reply, end). Raises Incomplete."""
    if pos >= len(buf):
        raise Incomplete
    t = chr(buf[pos])
    line, p = _line(buf, pos + 1)
    if t in "+-":
        return Reply(t, line), p
    if t == ":":
        return Reply(t, _int(line)), p
    if t == ",":
        return Reply(t, line), p
    if t == "(":
        return Reply(t, line), p
    if t == "#":
        if line not in (b"t", b"f"):
            raise ProtocolError(f"bad boolean {line!r}")
        return Reply(t, line == b"t"), p
    if t == "_":
        if line:
            raise ProtocolError("null with payload")
        return Reply(t, None), p
    if t in "$!=":
        n = _int(line)
        if n < 0:
            return Reply(t, None), p
        if len(buf) < p + n + 2:
            raise Incomplete
        if buf[p + n : p + n + 2] != CRLF:
            raise ProtocolError(f"bulk of {n} bytes not followed by CRLF")
        return Reply(t, buf[p : p + n]), p + n + 2
    if t in "*~>":
        n = _int(line)
        if n < 0:
            return Reply(t, None), p
        items = []
        for _ in range(n):
            r, p = parse(buf, p)
            items.append(r)
        return Reply(t, items), p
    if t in "%|":
        n = _int(line)
        pairs = []
        for _ in range(n):
            a, p = parse(buf, p)
            b, p = parse(buf, p)
            pairs.append((a, b))
        if t == "|":
            # attribute: belongs to the following reply
            r, p = parse(buf, p)
            r.attrs = pairs
            return r, p
        return Reply(t, pairs), p
    raise ProtocolError(f"unknown reply type byte {buf[pos:pos+1]!r} at offset {pos}: {buf[pos:pos+40]!r}")


def _b(x: Union[str, bytes, int, float]) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, (int, float)):
        return str(x).encode()
    return x.encode()


def cmd(*args: Union[str, bytes, int]) -> bytes:
    """Encodes a command as a RESP multibulk request."""
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = _b(a)
        out.append(b"$%d\r\n" % len(a))
        out.append(a)
        out.append(CRLF)
    return b"".join(out)


def split_chunks(data: bytes, chunk: Union[int, Sequence[int], None]) -> List[bytes]:
    """chunk=int: fixed size pieces; a sequence: split at those offsets."""
    if chunk is None:
        return [data]
    if isinstance(chunk, int):
        return [data[i : i + chunk] for i in range(0, len(data), chunk)] or [b""]
    cuts = [0] + sorted(chunk) + [len(data)]
    return [data[a:b] for a, b in zip(cuts, cuts[1:]) if b > a]


class Conn:
    def __init__(self, host: str, port: int, timeout: float = 2.0, connect_timeout: float = 2.0):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = socket.create_connection((host, port), timeout=connect_timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        self.eof = False

    @classmethod
    def from_addr(cls, addr: str, **kw) -> "Conn":
        host, port = parse_addr(addr)
        return cls(host, port, **kw)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def send(self, data: bytes, chunk=None, delay: float = 0.0) -> None:
        """Sends data, optionally in chunks with `delay` seconds between them.

        A send to a connection the server already closed raises OSError
        (BrokenPipe / ConnectionReset); callers testing close behaviour
        should catch it.
        """
        pieces = split_chunks(data, chunk)
        for i, piece in enumerate(pieces):
            if i and delay:
                time.sleep(delay)
            self.sock.sendall(piece)

    def send_chunks(self, chunks: Sequence[bytes], delay: float = 0.0) -> None:
        for i, piece in enumerate(chunks):
            if i and delay:
                time.sleep(delay)
            self.sock.sendall(piece)

    def _fill(self, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise Timeout(self.buf)
        self.sock.settimeout(remaining)
        try:
            data = self.sock.recv(1 << 20)
        except socket.timeout:
            raise Timeout(self.buf)
        except ConnectionResetError:
            data = b""
        if not data:
            self.eof = True
            raise Closed(self.buf)
        self.buf += data

    def read_reply(self, timeout: Optional[float] = None) -> Tuple[bytes, Reply]:
        """Reads one complete reply: returns (raw bytes, parsed)."""
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            try:
                r, end = parse(self.buf)
                raw, self.buf = self.buf[:end], self.buf[end:]
                return raw, r
            except Incomplete:
                pass
            if self.eof:
                raise Closed(self.buf)
            self._fill(deadline)

    def read_replies(self, n: int, timeout: Optional[float] = None) -> List[Tuple[bytes, Reply]]:
        return [self.read_reply(timeout) for _ in range(n)]

    def read_until_close(self, timeout: Optional[float] = None) -> Tuple[List[Tuple[bytes, Reply]], bool]:
        """Reads replies until the server closes the connection.

        Returns (replies, closed): closed is False if the deadline passed
        with the connection still open. Leftover bytes of an incomplete
        reply stay in self.buf.
        """
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        out = []
        while True:
            try:
                out.append(self.read_reply(timeout=max(0.0, deadline - time.monotonic())))
            except Closed:
                return out, True
            except Timeout:
                return out, False

    def is_closed(self, wait: float = 0.2) -> bool:
        """True if the server closed the connection (EOF within `wait`)."""
        if self.eof:
            return True
        self.sock.settimeout(wait)
        try:
            data = self.sock.recv(1 << 16)
        except socket.timeout:
            return False
        except ConnectionResetError:
            data = b""
        if not data:
            self.eof = True
            return True
        self.buf += data
        return False

    def call(self, *args) -> Reply:
        self.send(cmd(*args))
        return self.read_reply()[1]


def parse_addr(addr: str) -> Tuple[str, int]:
    host, _, port = addr.rpartition(":")
    return (host or "127.0.0.1"), int(port)


def main(argv: List[str]) -> int:
    if len(argv) < 3:
        print(__doc__.split("Command line:")[1].strip())
        return 2
    addr, args = argv[1], argv[2:]
    resp3 = False
    if args and args[0] == "--resp3":
        resp3, args = True, args[1:]
    with Conn.from_addr(addr) as c:
        if resp3:
            c.call("HELLO", "3")
        c.send(cmd(*args))
        raw, r = c.read_reply()
        print(repr(raw))
        print(r)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
