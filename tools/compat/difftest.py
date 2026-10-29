#!/usr/bin/env python3
"""Differential tester: runs the corpus (corpus.py) against a reference and a
target server and compares what each one replies, byte for byte (after the
per-case normalisers).

  # Redis (reference) vs our node (target): the main mode
  python3 difftest.py --ref 127.0.0.1:6379 --target 127.0.0.1:7000

  # selection / debugging
  python3 difftest.py ... -k 'split_*' -k incr --tag large --skip-slow -v
  python3 difftest.py --list
  python3 difftest.py --show --target 127.0.0.1:7000 -k hello_3   # just print transcripts

  # AUTH: both servers started with the password "pw" (Redis --requirepass pw,
  # ours --requirepass pw): runs only the cases tagged "auth", which need it
  python3 difftest.py --ref 127.0.0.1:6379 --target 127.0.0.1:7000 --password pw

Cases with `expected=` are compared against those bytes instead of the
reference server.

Before each case both servers get a FLUSHALL on a separate connection
(after AUTH with --password).

Exit status: 0 if every case matched or is allow-listed (allowlist.py),
1 otherwise, 2 on usage errors.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
import time
from typing import Dict, List, Optional, Union

import corpus
from corpus import Case, Read, Send, Silent, Sleep, UntilClose
from resp import Closed, Conn, Timeout, cmd, parse_addr

Entry = Union[bytes, str]  # bytes = a reply; str = an event marker like "<CLOSED>"

STALL_TIMEOUT = 0.3
END_CHECK = 0.03


class Runner:
    def __init__(self, addr: str, timeout_scale: float = 1.0, password: Optional[str] = None):
        self.host, self.port = parse_addr(addr)
        self.addr = addr
        self.scale = timeout_scale
        self.password = password

    def flushall(self) -> Optional[str]:
        try:
            with Conn(self.host, self.port, timeout=5) as c:
                if self.password is not None:
                    c.send(cmd("AUTH", self.password))
                    raw, _ = c.read_reply()
                    if raw != b"+OK\r\n":
                        return f"AUTH replied {raw!r}"
                c.send(cmd("FLUSHALL"))
                raw, _ = c.read_reply()
                if raw != b"+OK\r\n":
                    return f"FLUSHALL replied {raw!r}"
        except Exception as e:  # noqa: BLE001
            return f"FLUSHALL failed: {type(e).__name__}: {e}"
        return None

    def run(self, case: Case) -> List[Entry]:
        out: List[Entry] = []
        conns: Dict[int, Conn] = {}
        closed: Dict[int, bool] = {}
        stalled: Dict[int, bool] = {}
        timeout = case.timeout * self.scale

        failed: Dict[int, bool] = {}

        def get(i: int) -> Optional[Conn]:
            if i in failed:
                return None
            if i not in conns:
                try:
                    conns[i] = Conn(self.host, self.port, timeout=timeout)
                    closed[i] = False
                    stalled[i] = False
                except OSError as e:
                    out.append(f"<CONNECT FAILED conn{i}: {e}>")
                    failed[i] = True
                    return None
            return conns[i]

        def tag(i: int, s: str) -> str:
            return s if i == 0 else s[:-1] + f" conn{i}>"

        def read_one(i: int, c: Conn, wait: Optional[float] = None) -> bool:
            t = wait if wait is not None else (STALL_TIMEOUT if stalled[i] else timeout)
            try:
                raw, _ = c.read_reply(timeout=t)
                out.append(raw)
                return True
            except Closed as e:
                out.append(tag(i, "<CLOSED>") if not e.partial else tag(i, f"<CLOSED, partial reply {e.partial[:60]!r}>"))
                closed[i] = True
            except Timeout as e:
                out.append(tag(i, "<TIMEOUT>") if not e.partial else tag(i, f"<TIMEOUT, partial reply {e.partial[:60]!r}>"))
                stalled[i] = True
                c.buf = b""
            except Exception as e:  # noqa: BLE001  (malformed reply)
                out.append(tag(i, f"<BAD REPLY {type(e).__name__}: {e}>"))
                closed[i] = True
            return False

        try:
            for st in case.steps:
                if isinstance(st, Sleep):
                    time.sleep(st.seconds)
                    continue
                c = get(st.conn)
                if c is None:
                    continue
                i = st.conn
                if isinstance(st, Send):
                    if closed[i]:
                        out.append(tag(i, "<SEND SKIPPED: closed>"))
                        continue
                    try:
                        c.send(st.data, chunk=st.chunk, delay=st.delay)
                    except OSError:
                        out.append(tag(i, "<SEND FAILED>"))
                        closed[i] = True
                elif isinstance(st, Read):
                    for _ in range(st.n):
                        if closed[i]:
                            break
                        if not read_one(i, c):
                            break
                elif isinstance(st, UntilClose):
                    if closed[i]:
                        continue
                    replies, was_closed = c.read_until_close(timeout=timeout)
                    out.extend(r for r, _ in replies)
                    if was_closed:
                        out.append(tag(i, "<CLOSED>") if not c.buf else tag(i, f"<CLOSED, partial reply {c.buf[:60]!r}>"))
                        closed[i] = True
                    else:
                        out.append(tag(i, "<STILL OPEN>"))
                elif isinstance(st, Silent):
                    if closed[i]:
                        continue
                    try:
                        raw, _ = c.read_reply(timeout=st.wait)
                        out.append(raw)
                    except Timeout:
                        out.append(tag(i, "<SILENT>"))
                    except Closed:
                        out.append(tag(i, "<CLOSED>"))
                        closed[i] = True
                else:
                    raise TypeError(f"unknown step {st!r}")
            # Anything unexpected after the last step? (extra replies, close after error)
            for i, c in conns.items():
                if closed[i] or stalled[i]:
                    continue
                if c.buf:
                    out.append(tag(i, f"<EXTRA BYTES {c.buf[:60]!r}>"))
                elif c.is_closed(END_CHECK):
                    out.append(tag(i, "<CLOSED AT END>"))
                elif c.buf:
                    out.append(tag(i, f"<EXTRA BYTES {c.buf[:60]!r}>"))
        finally:
            for c in conns.values():
                c.close()
        return out


def normalise(case: Case, entries: List[Entry]) -> List[Entry]:
    res = []
    for i, e in enumerate(entries):
        if isinstance(e, bytes):
            for f in case.norm:
                e = f(i, e)
        res.append(e)
    return res


def short(e: Optional[Entry], limit: int = 160) -> str:
    if e is None:
        return "(nothing)"
    if isinstance(e, str):
        return e
    r = repr(e)
    if len(r) > limit:
        r = r[: limit - 30] + f"...' ({len(e)} bytes)"
    return r


def select(cases: List[Case], args) -> List[Case]:
    out = []
    for c in cases:
        if args.skip_slow and c.slow:
            continue
        if args.skip_expected and c.expected is not None:
            continue
        if args.tag and not (set(args.tag) & c.tags):
            continue
        if args.k:
            ok = False
            for pat in args.k:
                if pat.startswith("="):
                    ok |= c.name == pat[1:]
                elif any(ch in pat for ch in "*?["):
                    ok |= fnmatch.fnmatchcase(c.name, pat)
                else:
                    ok |= pat in c.name
            if not ok:
                continue
        out.append(c)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ref", help="reference server host:port (Redis)")
    ap.add_argument("--target", help="server under test host:port")
    ap.add_argument("-k", action="append", help="select cases by name: substring, fnmatch pattern, or =exact; repeatable")
    ap.add_argument("--tag", action="append", help="select cases with this tag (split, large, client-startup)")
    ap.add_argument("--skip-slow", action="store_true", help="skip cases that sleep (expiry)")
    ap.add_argument("--skip-expected", action="store_true",
                    help="skip cases with expected= overrides (for Redis vs Redis determinism runs)")
    ap.add_argument("--allowlist", default="rust", help="allow-list set from allowlist.py (default rust; 'none' = strict)")
    ap.add_argument("--allow", action="append", default=[], metavar="PATTERN=REASON", help="extra allow-list entry")
    ap.add_argument("--no-flush", action="store_true", help="don't FLUSHALL before each case")
    ap.add_argument("--timeout-scale", type=float, default=1.0, help="multiply read timeouts")
    ap.add_argument("--show", action="store_true", help="print transcripts (with only --target: just run and print)")
    ap.add_argument("-v", "--verbose", action="store_true", help="print full transcripts of failing cases")
    ap.add_argument("--json", help="write results to this file")
    ap.add_argument("--list", action="store_true", help="list the selected cases and exit")
    ap.add_argument("--password", help="the servers' password: run only the cases tagged auth (which need it)")
    args = ap.parse_args(argv)

    # The auth cases need servers with a password; the others, without.
    cases = [c for c in select(corpus.CASES, args) if ("auth" in c.tags) == (args.password is not None)]
    if args.list:
        for c in cases:
            flags = ",".join(sorted(c.tags | ({"slow"} if c.slow else set())
                                    | ({"expected"} if c.expected else set())))
            print(f"{c.name:45s} {flags}")
        print(f"{len(cases)} cases")
        return 0
    if not args.target or not (args.ref or args.show):
        ap.error("--target and --ref are required (or --show --target)")

    import allowlist
    allow = dict(allowlist.ALLOW.get(args.allowlist, {}))
    if args.allowlist not in allowlist.ALLOW:
        ap.error(f"unknown allow-list set {args.allowlist!r}: {sorted(allowlist.ALLOW)}")
    for a in args.allow:
        pat, _, reason = a.partition("=")
        allow[pat] = reason or "(command line)"

    ref = Runner(args.ref, args.timeout_scale, args.password) if args.ref else None
    tgt = Runner(args.target, args.timeout_scale, args.password)
    flush = not args.no_flush
    print(f"difftest: {len(cases)} cases, ref={args.ref}, target={args.target}, flush={flush}")

    results = []
    counts = {"PASS": 0, "FAIL": 0, "ALLOWED": 0, "STALE-ALLOW": 0, "SHOWN": 0}
    flush_problems = {}
    t0 = time.monotonic()
    try:
        for case in cases:
            ref_t: List[Entry] = []
            need_ref = ref is not None and (case.expected is None or any(e is None for e in case.expected))
            if need_ref:
                if flush and case.flush:
                    p = ref.flushall()
                    if p:
                        flush_problems.setdefault("ref", p)
                ref_t = ref.run(case)
            if case.expected is not None:
                exp = []
                for i, e in enumerate(case.expected):
                    exp.append(ref_t[i] if e is None and i < len(ref_t) else e)
                ref_t = exp
            if flush and case.flush:
                p = tgt.flushall()
                if p:
                    flush_problems.setdefault("target", p)
            tgt_t = tgt.run(case)

            if ref is None and case.expected is None:
                counts["SHOWN"] += 1
                print(f"---- {case.name}")
                for e in normalise(case, tgt_t):
                    print("   ", short(e, 400))
                continue

            a, b = normalise(case, ref_t), normalise(case, tgt_t)
            reason = next((r for p, r in allow.items() if fnmatch.fnmatchcase(case.name, p)), None)
            if a == b:
                status = "STALE-ALLOW" if reason else "PASS"
            else:
                status = "ALLOWED" if reason else "FAIL"
            counts[status] += 1
            first = next((i for i in range(max(len(a), len(b)))
                          if i >= len(a) or i >= len(b) or a[i] != b[i]), None)
            results.append({"case": case.name, "status": status, "reason": reason, "first_diff": first,
                            "ref": [short(e, 400) for e in a], "target": [short(e, 400) for e in b],
                            "expected_override": case.expected is not None})
            if status == "PASS" and not args.show:
                continue
            label = {"FAIL": "FAIL   ", "ALLOWED": "ALLOWED", "STALE-ALLOW": "STALE  ", "PASS": "PASS   "}[status]
            src = "expected" if case.expected is not None else "ref"
            print(f"{label} {case.name}" + (f"  [{reason}]" if reason else "") + (f"  -- {case.doc}" if case.doc and status == "FAIL" else ""))
            if first is not None:
                print(f"        entry {first}: {src:8s}= {short(a[first] if first < len(a) else None)}")
                print(f"        entry {first}: target  = {short(b[first] if first < len(b) else None)}")
            if (args.verbose and status in ("FAIL", "ALLOWED")) or args.show:
                print(f"        {src} transcript:")
                for e in a:
                    print("          ", short(e, 300))
                print("        target transcript:")
                for e in b:
                    print("          ", short(e, 300))
    except KeyboardInterrupt:
        print("interrupted")

    dt = time.monotonic() - t0
    for side, p in flush_problems.items():
        print(f"WARNING: {side}: {p} (cases may see leftover keys)")
    summary = ", ".join(f"{v} {k.lower()}" for k, v in counts.items() if v)
    print(f"\n{summary}  ({dt:.1f}s)")
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"ref": args.ref, "target": args.target, "auth": args.password is not None, "counts": counts, "results": results}, f,
                      indent=1)
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
