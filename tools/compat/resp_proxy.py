#!/usr/bin/env python3
"""Logging TCP proxy: shows exactly what a client sends and what it gets back.

    python3 resp_proxy.py --listen 7393 --upstream 127.0.0.1:6393 [--log FILE]

then point the client at port 7393. Every request (multibulk or inline) and
every reply is printed, one line each, per connection. Unlike MONITOR it also
shows requests the server rejects (unknown commands / subcommands, arity
errors) and the replies. Stops on Ctrl-C (or --seconds N).
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading
import time

from resp import Incomplete, parse, parse_addr

lock = threading.Lock()
out = sys.stdout


def log(line: str) -> None:
    with lock:
        out.write(line + "\n")
        out.flush()


def parse_request(buf: bytes):
    """Returns (args, rest) or None if incomplete. Inline requests are split on spaces."""
    if not buf:
        return None
    if buf[:1] == b"*":
        end = buf.find(b"\r\n")
        if end < 0:
            return None
        try:
            n = int(buf[1:end])
        except ValueError:
            return [b"<malformed>", buf[:end]], b""
        pos, args = end + 2, []
        for _ in range(max(n, 0)):
            e = buf.find(b"\r\n", pos)
            if e < 0:
                return None
            if buf[pos:pos + 1] != b"$":
                return [b"<malformed>", buf[pos:e]], b""
            ln = int(buf[pos + 1:e])
            if len(buf) < e + 2 + ln + 2:
                return None
            args.append(buf[e + 2:e + 2 + ln])
            pos = e + 2 + ln + 2
        return args, buf[pos:]
    end = buf.find(b"\n")
    if end < 0:
        return None
    return [b"<inline>"] + buf[:end].rstrip(b"\r").split(), buf[end + 1:]


def fmt_args(args) -> str:
    parts = []
    for a in args:
        r = repr(a)[2:-1]
        parts.append(r if len(r) <= 80 else r[:60] + f"...({len(a)} bytes)")
    return " ".join(parts)


def pump(src, dst, cid, direction):
    buf = b""
    try:
        while True:
            data = src.recv(1 << 16)
            if not data:
                break
            dst.sendall(data)
            buf += data
            while True:
                if direction == "C>":
                    r = parse_request(buf)
                    if r is None:
                        break
                    args, buf = r
                    if args:
                        log(f"[{cid}] C> {fmt_args(args)}")
                else:
                    try:
                        _, end = parse(buf)
                    except Incomplete:
                        break
                    except Exception:  # noqa: BLE001
                        log(f"[{cid}] S> <unparseable> {buf[:80]!r}")
                        buf = b""
                        break
                    raw, buf = buf[:end], buf[end:]
                    r = repr(raw)
                    log(f"[{cid}] S> {r if len(r) < 200 else r[:180] + f'...({len(raw)} bytes)'}")
    except OSError:
        pass
    finally:
        log(f"[{cid}] {direction} closed")
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def main() -> int:
    global out
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--log")
    ap.add_argument("--seconds", type=float)
    a = ap.parse_args()
    if a.log:
        out = open(a.log, "a")
    up = parse_addr(a.upstream)
    ls = socket.socket()
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("127.0.0.1", a.listen))
    ls.listen(128)
    ls.settimeout(0.5)
    deadline = time.monotonic() + a.seconds if a.seconds else None
    cid = 0
    while deadline is None or time.monotonic() < deadline:
        try:
            c, _ = ls.accept()
        except socket.timeout:
            continue
        except KeyboardInterrupt:
            break
        cid += 1
        s = socket.create_connection(up)
        log(f"[{cid}] connected")
        threading.Thread(target=pump, args=(c, s, cid, "C>"), daemon=True).start()
        threading.Thread(target=pump, args=(s, c, cid, "S>"), daemon=True).start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
