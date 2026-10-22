#!/usr/bin/env bash
# Records what real clients send (and receive) on connect, through the
# logging proxy resp_proxy.py: redis-cli (one-shot, RESP3, interactive),
# redis-benchmark, redis-py.
#
#   tools/compat/client_startup_capture.sh [UPSTREAM host:port] [PROXY_PORT] [LOG]
#
# Defaults: upstream 127.0.0.1:6393 (Redis), proxy on 7393, log in $TMPDIR.
# Point UPSTREAM at the Rust node to see which client flows it breaks.
# redis-py is used if importable, or from $REDISPY_PATH (a directory holding
# the unpacked wheel; see README).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UP="${1:-127.0.0.1:6393}"
LP="${2:-7393}"
LOG="${3:-${TMPDIR:-/tmp}/client_startup_capture.log}"
REDISPY_PATH="${REDISPY_PATH:-$HOME/venvs/redis-py/site}"
: >"$LOG"
python3 "$HERE/resp_proxy.py" --listen "$LP" --upstream "$UP" --log "$LOG" &
PROXY=$!
trap 'kill $PROXY 2>/dev/null' EXIT
sleep 0.5

section() { sleep 0.3; echo; echo "==== $*" >>"$LOG"; }

section "redis-cli -p PORT PING"
redis-cli -p "$LP" PING >/dev/null
section "redis-cli -3 -p PORT PING"
redis-cli -3 -p "$LP" PING >/dev/null
section "redis-cli -p PORT SET k v / GET k (one-shot)"
redis-cli -p "$LP" SET k v >/dev/null
section "redis-cli interactive (driven through a pty): PING, SET a b, GET a, quit"
# `script` + a pipe doesn't work (linenoise loses the input), so drive a pty.
timeout 20 python3 - "$LP" <<'EOF'
import os, pty, sys, time, select, fcntl, termios, struct
pid, fd = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm"
    os.execvp("redis-cli", ["redis-cli", "-p", sys.argv[1]])
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 120, 0, 0))
def wait_prompt(t=5.0):
    buf, end = b"", time.time() + t
    while time.time() < end and not buf.rstrip().endswith(b">"):
        if select.select([fd], [], [], 0.1)[0]:
            try:
                d = os.read(fd, 65536)
            except OSError:
                break
            if b"\x1b[6n" in d:  # linenoise asks for the cursor position
                os.write(fd, b"\x1b[1;1R")
            buf += d
    return buf
wait_prompt()
for line in [b"PING", b"SET a b", b"GET a", b"quit"]:
    os.write(fd, line + b"\r")
    wait_prompt(2.0)
os.waitpid(pid, 0)
EOF
section "redis-benchmark -p PORT -n 3 -c 1 -t ping,set,get,incr,mset"
redis-benchmark -p "$LP" -n 3 -c 1 -t ping,set,get,incr,mset -q >/dev/null 2>&1
section "redis-benchmark -p PORT -n 32 -c 1 -P 16 -t get (pipelined)"
redis-benchmark -p "$LP" -n 32 -c 1 -P 16 -t get -q >/dev/null 2>&1
if PYTHONPATH="$REDISPY_PATH" python3 -c 'import redis' 2>/dev/null; then
  section "redis-py $(PYTHONPATH="$REDISPY_PATH" python3 -c 'import redis; print(redis.__version__)'): Redis(); ping; set; get; pipeline"
  PYTHONPATH="$REDISPY_PATH" python3 - "$LP" <<'EOF'
import sys, redis
r = redis.Redis(port=int(sys.argv[1]))
r.ping(); r.set("k", "v"); r.get("k")
p = r.pipeline(transaction=False); p.set("a", 1); p.incr("a"); p.get("a"); p.execute()
r.close()
EOF
  section "redis-py: Redis(protocol=3, client_name='x', db=0)"
  PYTHONPATH="$REDISPY_PATH" python3 - "$LP" <<'EOF'
import sys, redis
r = redis.Redis(port=int(sys.argv[1]), protocol=3, client_name="x")
r.ping(); r.get("k")
r.close()
EOF
  section "redis-py: Redis(protocol=2)"
  PYTHONPATH="$REDISPY_PATH" python3 - "$LP" <<'EOF'
import sys, redis
r = redis.Redis(port=int(sys.argv[1]), protocol=2)
r.ping()
r.close()
EOF
  section "redis-py: Redis(db=1)"
  PYTHONPATH="$REDISPY_PATH" python3 - "$LP" <<'EOF'
import sys, redis
r = redis.Redis(port=int(sys.argv[1]), db=1)
r.ping()
r.close()
EOF
else
  section "redis-py not available (set REDISPY_PATH)"
fi
sleep 0.5
cat "$LOG"
