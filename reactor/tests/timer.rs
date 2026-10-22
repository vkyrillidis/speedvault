//! Milestone 4: sleeping fibers and I/O with deadlines.

mod common;

use common::{panic_message, run, DropFlag};
use reactor::{busy_time, sleep, sleep_until, yield_now, Reactor, Registered, TcpListener, TcpStream};
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::time::{Duration, Instant};

const MS: Duration = Duration::from_millis(1);

#[test]
fn sleeps_end_in_deadline_order() {
    let reactor = Reactor::new().unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));
    let start = Instant::now();
    for ms in [30, 10, 20] {
        let log = log.clone();
        reactor
            .spawn(move || {
                sleep(ms * MS);
                assert!(start.elapsed() >= ms * MS, "woke up early");
                log.borrow_mut().push(ms);
            })
            .unwrap();
    }
    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), [10, 20, 30]);
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(reactor.timer_count(), 0);
}

#[test]
fn a_sleeping_fiber_is_not_reported_as_stuck() {
    // run() errors on fibers nothing can resume; a timer can.
    let reactor = Reactor::new().unwrap();
    reactor.spawn(|| sleep(20 * MS)).unwrap();
    let start = Instant::now();
    run(&reactor).unwrap();
    assert!(start.elapsed() >= 20 * MS);
}

#[test]
fn stuck_after_sleeping_is_still_reported() {
    // The sleep's wait must be fully accounted for once it is over, or the
    // reactor would think something can still wake the fiber and hang.
    let reactor = Reactor::new().unwrap();
    reactor
        .spawn(|| {
            sleep(MS);
            fiber::suspend(); // nothing will ever resume this
        })
        .unwrap();
    let err = run(&reactor).unwrap_err();
    assert!(err.to_string().contains("nothing can resume"), "{err}");
}

#[test]
fn sleep_zero_waits_for_the_next_turn() {
    let reactor = Reactor::new().unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));
    let l = log.clone();
    reactor
        .spawn(move || {
            l.borrow_mut().push("a1");
            sleep(Duration::ZERO);
            l.borrow_mut().push("a2");
        })
        .unwrap();
    let l = log.clone();
    reactor.spawn(move || l.borrow_mut().push("b1")).unwrap();
    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), ["a1", "b1", "a2"]);
}

#[test]
fn sleep_until_a_past_deadline_returns_on_the_next_turn() {
    let reactor = Reactor::new().unwrap();
    let past = Instant::now();
    reactor.spawn(move || sleep_until(past - 10 * MS)).unwrap();
    run(&reactor).unwrap();
}

#[test]
fn sleeping_fibers_do_not_block_io() {
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let got = Rc::new(Cell::new(None));
    reactor.spawn(|| sleep(Duration::from_millis(100))).unwrap();
    let g = got.clone();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            let mut buf = [0; 1];
            let start = Instant::now();
            a.read_with(|mut s| s.read(&mut buf)).unwrap();
            g.set(Some(start.elapsed()));
        })
        .unwrap();
    reactor
        .spawn(move || {
            let b = Registered::new(b).unwrap();
            b.write_with(|mut s| s.write(b"x")).unwrap();
        })
        .unwrap();
    run(&reactor).unwrap();
    assert!(got.get().unwrap() < Duration::from_millis(50), "the read waited for the sleeper");
}

#[test]
fn many_timers() {
    // 10k fibers × 10 sleeps of 0-9 ms each.
    const FIBERS: u32 = 10_000;
    let reactor = Reactor::new().unwrap();
    let early = Rc::new(Cell::new(0));
    let done = Rc::new(Cell::new(0));
    for i in 0..FIBERS {
        let (early, done) = (early.clone(), done.clone());
        reactor
            .spawn(move || {
                for j in 0..10 {
                    let deadline = Instant::now() + ((i + j) % 10) * MS;
                    sleep_until(deadline);
                    if Instant::now() < deadline {
                        early.set(early.get() + 1);
                    }
                }
                done.set(done.get() + 1);
            })
            .unwrap();
    }
    run(&reactor).unwrap();
    assert_eq!(done.get(), FIBERS);
    assert_eq!(early.get(), 0, "timers fired before their deadline");
}

#[test]
fn read_deadline_passes_and_the_socket_still_works() {
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let timed_out = Rc::new(Cell::new(false));

    let t = timed_out.clone();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            let mut buf = [0; 1];
            let start = Instant::now();
            let err = a.read_with_deadline(start + 30 * MS, |mut s| s.read(&mut buf)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            assert!(start.elapsed() >= 30 * MS);
            t.set(true);
            // Still registered and usable: the next read gets the data.
            a.read_with(|mut s| s.read(&mut buf)).unwrap();
            assert_eq!(&buf, b"x");
        })
        .unwrap();
    let t = timed_out.clone();
    reactor
        .spawn(move || {
            let b = Registered::new(b).unwrap();
            while !t.get() {
                sleep(5 * MS);
            }
            b.write_with(|mut s| s.write(b"x")).unwrap();
        })
        .unwrap();
    run(&reactor).unwrap();
    assert_eq!(reactor.timer_count(), 0);
}

#[test]
fn data_in_time_cancels_the_deadline() {
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            let mut buf = [0; 1];
            let deadline = Instant::now() + Duration::from_secs(3600);
            a.read_with_deadline(deadline, |mut s| s.read(&mut buf)).unwrap();
        })
        .unwrap();
    reactor
        .spawn(move || {
            let b = Registered::new(b).unwrap();
            sleep(10 * MS);
            b.write_with(|mut s| s.write(b"x")).unwrap();
        })
        .unwrap();
    let start = Instant::now();
    run(&reactor).unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    // run() returns once no fiber is left, timers or not: a timer the read
    // forgot to cancel would still be counted.
    assert_eq!(reactor.timer_count(), 0);
}

#[test]
fn io_and_deadline_in_the_same_turn_wake_the_fiber_once() {
    // The reader's deadline passes and its data arrives before the reactor
    // looks again: both would wake it. It must run once (and get the data,
    // since I/O is dispatched before timers). A second, stale wakeup would
    // cut its next wait short.
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let slept = Rc::new(Cell::new(Duration::ZERO));

    let s = slept.clone();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            let mut buf = [0; 1];
            let deadline = Instant::now() + 20 * MS;
            a.read_with_deadline(deadline, |mut s| s.read(&mut buf)).expect("I/O wins");
            let start = Instant::now();
            sleep(50 * MS);
            s.set(start.elapsed());
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut b = b;
            // Block the whole thread past the reader's deadline, then write:
            // the next epoll_wait reports the data, and the deadline is over.
            std::thread::sleep(40 * MS);
            b.write_all(b"x").unwrap();
        })
        .unwrap();
    run(&reactor).unwrap();
    assert!(slept.get() >= 50 * MS, "the sleep ended after {:?}: woken twice", slept.get());
}

#[test]
fn tcp_read_timeout() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let timed_out = Rc::new(Cell::new(false));

    let t = timed_out.clone();
    reactor
        .spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            while !t.get() {
                sleep(5 * MS);
            }
            conn.write_all(b"late").unwrap();
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            assert_eq!(conn.read_timeout(), None);
            conn.set_read_timeout(Some(30 * MS)).unwrap();
            assert_eq!(conn.read_timeout(), Some(30 * MS));
            let mut buf = [0; 4];
            let start = Instant::now();
            let err = conn.read(&mut buf).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            assert!(start.elapsed() >= 30 * MS);
            timed_out.set(true);
            conn.set_read_timeout(None).unwrap();
            conn.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"late");
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn tcp_write_timeout() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let done = Rc::new(Cell::new(false));

    let d = done.clone();
    reactor
        .spawn(move || {
            let (_conn, _) = listener.accept().unwrap();
            // Never reads; keeps the connection open until the writer is done.
            while !d.get() {
                sleep(5 * MS);
            }
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            conn.set_write_timeout(Some(30 * MS)).unwrap();
            let chunk = vec![0; 65536]; // not on the 64 KiB fiber stack
            let err = loop {
                if let Err(e) = conn.write(&chunk) {
                    break e;
                }
            };
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            done.set(true);
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn zero_timeout_is_rejected() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    reactor
        .spawn(move || {
            let conn = TcpStream::connect(addr).unwrap();
            let err = conn.set_read_timeout(Some(Duration::ZERO)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            let err = conn.set_write_timeout(Some(Duration::ZERO)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            drop(listener);
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn dropping_the_reactor_cancels_sleeping_fibers() {
    let reactor = Reactor::new().unwrap();
    let dropped = Rc::new(Cell::new(false));
    let d = dropped.clone();
    reactor
        .spawn(move || {
            let _guard = DropFlag(d);
            sleep(Duration::from_secs(3600));
            unreachable!("never resumed");
        })
        .unwrap();
    reactor
        .spawn(|| {
            yield_now(); // the sleeper sleeps now
            panic!("boom");
        })
        .unwrap();
    let result = panic::catch_unwind(AssertUnwindSafe(|| run(&reactor)));
    assert_eq!(panic_message(&*result.unwrap_err()), "boom");
    assert_eq!(reactor.timer_count(), 1);

    drop(reactor);
    assert!(dropped.get(), "the sleeping fiber's stack was unwound");
    let reactor = Reactor::new().unwrap();
    run(&reactor).unwrap();
}

#[test]
#[should_panic(expected = "outside of a fiber")]
fn sleeping_outside_a_reactor_fiber_panics() {
    let _reactor = Reactor::new().unwrap();
    sleep(MS);
}

#[test]
fn busy_polling_gives_way_to_the_next_deadline() {
    let reactor = Reactor::new().unwrap();
    reactor.set_busy_poll(Duration::from_secs(5));
    let start = Instant::now();
    reactor.spawn(|| sleep(20 * MS)).unwrap();
    run(&reactor).unwrap();
    assert!(start.elapsed() < Duration::from_secs(1), "the timer fired after {:?}", start.elapsed());
}

/// Spins for `d` without letting the reactor run.
fn spin(d: Duration) {
    let start = Instant::now();
    while start.elapsed() < d {
        std::hint::spin_loop();
    }
}

#[test]
fn busy_time_counts_work_not_waiting() {
    for busy_poll in [Duration::ZERO, Duration::from_millis(5)] {
        let reactor = Reactor::new().unwrap();
        reactor.set_busy_poll(busy_poll);
        let seen = Rc::new(Cell::new(Duration::ZERO));
        let s = seen.clone();
        reactor
            .spawn(move || {
                for _ in 0..3 {
                    spin(Duration::from_millis(20));
                    sleep(Duration::from_millis(40));
                }
                // Asked from a fiber: the work since the loop last woke counts.
                spin(Duration::from_millis(20));
                s.set(busy_time());
            })
            .unwrap();
        reactor.run().unwrap();
        let (seen, total) = (seen.get(), reactor.busy_time());
        // 80 ms of work, 120 ms of sleeping (busy-polled for the first 5 ms
        // of each sleep, which counts as waiting).
        assert!(seen >= Duration::from_millis(80) && seen < Duration::from_millis(110), "busy-poll {busy_poll:?}: {seen:?}");
        assert!(total >= seen && total < seen + Duration::from_millis(10), "{total:?} after {seen:?}");
        drop(reactor);
    }
}

/// A fiber yielding in a loop keeps the loop from ever waiting: deadlines
/// must still pass (the loop reads the clock for them).
#[test]
fn sleepers_wake_while_another_fiber_yields() {
    let reactor = Reactor::new().unwrap();
    let woke = Rc::new(Cell::new(false));
    let w = woke.clone();
    reactor
        .spawn(move || {
            sleep(10 * MS);
            w.set(true);
        })
        .unwrap();
    let w = woke;
    reactor
        .spawn(move || {
            let start = Instant::now();
            while !w.get() && start.elapsed() < Duration::from_secs(2) {
                yield_now();
            }
            assert!(w.get(), "the sleeper didn't wake while this fiber yielded");
        })
        .unwrap();
    reactor.run().unwrap();
}
