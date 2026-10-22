#!/usr/bin/env python3
"""Wall-clock steps, for bench.sh.

redis-benchmark times everything with gettimeofday(). Under WSL2 with a
Windows clock that isn't synced, Hyper-V sets the guest clock to the host's
time every ~5.5 s and systemd-timesyncd steps it back ~50 ms later: a pair of
steps of several seconds, which mostly cancel out. A test that starts or ends
between the two gets a wrong requests/s (even negative); requests in flight
across a step get a latency clamped to 3000.319 ms. Between pairs the clock is steady.
(Fix the host: `w32tm /resync /force` in an elevated prompt.)

  clockguard.py offset         print wall - monotonic, in ms
  clockguard.py watch SECONDS  count the steps seen in SECONDS
  clockguard.py wait SECONDS   return once a pair of steps has passed (or a
                               lone step, PAIR_WAIT_S after it) and the clock
                               has been steady for SETTLE_S, or after SECONDS;
                               print the offset
"""
import sys
import time

STEP_MS = 5.0       # a change of wall - monotonic beyond this is a step
PAIR_WAIT_S = 1.0   # the second step of a pair came 50-100 ms after the first
SETTLE_S = 0.02     # quiet time after the pair before returning
POLL_S = 0.002


def offset_ms():
    return (time.time() - time.monotonic()) * 1000.0


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "offset":
        print(f"{offset_ms():.3f}")
    elif cmd in ("watch", "wait"):
        end = time.monotonic() + float(sys.argv[2])
        base = offset_ms()
        steps, last_step, first_sign = 0, None, 0
        while time.monotonic() < end:
            time.sleep(POLL_S)
            o = offset_ms()
            if abs(o - base) > STEP_MS:
                sign = 1 if o > base else -1
                if steps == 0 or first_sign == 0:
                    first_sign = sign
                elif sign != first_sign:
                    first_sign = 0  # the pair is complete
                steps, last_step, base = steps + 1, time.monotonic(), o
            elif cmd == "wait" and last_step:
                quiet = time.monotonic() - last_step
                if (first_sign == 0 and quiet > SETTLE_S) or quiet > PAIR_WAIT_S:
                    break
        print(steps if cmd == "watch" else f"{offset_ms():.3f}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
