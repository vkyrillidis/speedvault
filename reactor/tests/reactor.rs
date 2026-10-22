//! Milestone 2: fibers waiting on real fds (pipes and Unix socket pairs, so no
//! network is needed).

mod common;

use common::{panic_message, run, DropFlag};
use reactor::{spawn, yield_now, Reactor, Registered};
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::time::{Duration, Instant};

type Log = Rc<RefCell<Vec<String>>>;

fn log() -> Log {
    Rc::new(RefCell::new(Vec::new()))
}

fn push(log: &Log, s: impl Into<String>) {
    log.borrow_mut().push(s.into());
}

/// Reads exactly `buf.len()` bytes, waiting as needed.
fn read_exact<T: std::os::fd::AsRawFd>(r: &Registered<T>, buf: &mut [u8]) -> io::Result<()>
where
    for<'a> &'a T: Read,
{
    let mut done = 0;
    while done < buf.len() {
        let n = r.read_with(|mut s| s.read(&mut buf[done..]))?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += n;
    }
    Ok(())
}

/// Writes all of `buf`, waiting as needed.
fn write_all<T: std::os::fd::AsRawFd>(w: &Registered<T>, buf: &[u8]) -> io::Result<()>
where
    for<'a> &'a T: Write,
{
    let mut done = 0;
    while done < buf.len() {
        done += w.write_with(|mut s| s.write(&buf[done..]))?;
    }
    Ok(())
}

#[test]
fn run_without_fibers_returns_at_once() {
    let reactor = Reactor::new().unwrap();
    run(&reactor).unwrap();
}

#[test]
fn runs_every_fiber_to_completion() {
    let reactor = Reactor::new().unwrap();
    let count = Rc::new(Cell::new(0));
    for _ in 0..10 {
        let c = count.clone();
        reactor.spawn(move || c.set(c.get() + 1)).unwrap();
    }
    assert_eq!(count.get(), 0, "fibers start only in run()");
    run(&reactor).unwrap();
    assert_eq!(count.get(), 10);
    assert_eq!(reactor.fiber_count(), 0);
}

#[test]
fn fibers_can_spawn_fibers() {
    let reactor = Reactor::new().unwrap();
    let log = log();
    let l = log.clone();
    reactor
        .spawn(move || {
            push(&l, "parent");
            let l2 = l.clone();
            spawn(move || push(&l2, "child")).unwrap();
            push(&l, "parent done");
        })
        .unwrap();
    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), ["parent", "parent done", "child"]);
}

#[test]
fn yield_now_interleaves_fibers() {
    let reactor = Reactor::new().unwrap();
    let log = log();
    for name in ["a", "b"] {
        let l = log.clone();
        reactor
            .spawn(move || {
                for i in 1..=3 {
                    push(&l, format!("{name}{i}"));
                    yield_now();
                }
            })
            .unwrap();
    }
    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), ["a1", "b1", "a2", "b2", "a3", "b3"]);
}

#[test]
fn yielding_in_a_loop_does_not_starve_io() {
    // The spinner waits for the reader by yielding. The reader only wakes up
    // through epoll, so the loop must poll epoll between turns even though a
    // fiber is always ready.
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let got = Rc::new(Cell::new(false));

    let g = got.clone();
    reactor
        .spawn(move || {
            let r = Registered::new(r).unwrap();
            read_exact(&r, &mut [0; 1]).unwrap();
            g.set(true);
        })
        .unwrap();
    reactor
        .spawn(move || {
            let w = Registered::new(w).unwrap();
            write_all(&w, b"x").unwrap();
        })
        .unwrap();
    reactor
        .spawn(move || {
            while !got.get() {
                yield_now();
            }
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn reader_waits_until_writer_writes() {
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let log = log();

    let l = log.clone();
    reactor
        .spawn(move || {
            let r = Registered::new(r).unwrap();
            push(&l, "reader waits");
            let mut buf = [0; 5];
            read_exact(&r, &mut buf).unwrap();
            push(&l, format!("reader got {}", std::str::from_utf8(&buf).unwrap()));
        })
        .unwrap();
    let l = log.clone();
    reactor
        .spawn(move || {
            let w = Registered::new(w).unwrap();
            yield_now(); // make sure the reader really waits first
            push(&l, "writer writes");
            write_all(&w, b"hello").unwrap();
        })
        .unwrap();

    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), ["reader waits", "writer writes", "reader got hello"]);
    assert_eq!(reactor.registration_count(), 0, "dropping Registered deregisters");
}

#[test]
fn ping_pong_between_two_fibers() {
    const ROUNDS: u32 = 1000;
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let total = Rc::new(Cell::new(0u32));

    let t = total.clone();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            for i in 0..ROUNDS {
                write_all(&a, &i.to_le_bytes()).unwrap();
                let mut buf = [0; 4];
                read_exact(&a, &mut buf).unwrap();
                assert_eq!(u32::from_le_bytes(buf), i + 1);
                t.set(t.get() + 1);
            }
        })
        .unwrap();
    reactor
        .spawn(move || {
            let b = Registered::new(b).unwrap();
            for _ in 0..ROUNDS {
                let mut buf = [0; 4];
                read_exact(&b, &mut buf).unwrap();
                write_all(&b, &(u32::from_le_bytes(buf) + 1).to_le_bytes()).unwrap();
            }
        })
        .unwrap();

    run(&reactor).unwrap();
    assert_eq!(total.get(), ROUNDS);
}

#[test]
fn large_transfer_makes_the_writer_wait() {
    // Far more than a pipe buffer (64 KiB), so the writer must wait for the
    // reader to drain it, again and again.
    const LEN: usize = 4 << 20;
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let writer_waits = Rc::new(Cell::new(0));
    let received = Rc::new(RefCell::new(Vec::new()));

    let waits = writer_waits.clone();
    reactor
        .spawn(move || {
            let w = Registered::new(w).unwrap();
            let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
            let mut done = 0;
            while done < LEN {
                done += w
                    .write_with(|mut s| {
                        let r = s.write(&data[done..]);
                        if matches!(&r, Err(e) if e.kind() == io::ErrorKind::WouldBlock) {
                            waits.set(waits.get() + 1);
                        }
                        r
                    })
                    .unwrap();
            }
            // Dropping `w` closes the pipe: the reader sees EOF.
        })
        .unwrap();
    let out = received.clone();
    reactor
        .spawn(move || {
            let r = Registered::new(r).unwrap();
            let mut buf = vec![0; 16 * 1024];
            loop {
                let n = r.read_with(|mut s| s.read(&mut buf)).unwrap();
                if n == 0 {
                    break;
                }
                out.borrow_mut().extend_from_slice(&buf[..n]);
            }
        })
        .unwrap();

    run(&reactor).unwrap();
    let received = received.borrow();
    assert_eq!(received.len(), LEN);
    assert!(received.iter().enumerate().all(|(i, &b)| b == (i % 251) as u8), "data corrupted");
    assert!(writer_waits.get() > 0, "the writer never had to wait; the test proves nothing");
}

#[test]
fn closed_writer_gives_the_reader_eof() {
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let got_eof = Rc::new(Cell::new(false));
    let eof = got_eof.clone();
    reactor
        .spawn(move || {
            let r = Registered::new(r).unwrap();
            let mut buf = [0; 8];
            eof.set(r.read_with(|mut s| s.read(&mut buf)).unwrap() == 0);
        })
        .unwrap();
    reactor
        .spawn(move || {
            yield_now();
            drop(w);
        })
        .unwrap();
    run(&reactor).unwrap();
    assert!(got_eof.get());
}

#[test]
fn readiness_reported_while_not_waiting_is_not_lost() {
    // The reader waits on `r2`. Meanwhile epoll reports `r1` readable, with no
    // fiber waiting for it. Edge triggered: epoll won't report it again, so
    // the reactor must remember it, or the later wait on `r1` hangs.
    let reactor = Reactor::new().unwrap();
    let (r1, mut w1) = io::pipe().unwrap();
    let (r2, mut w2) = io::pipe().unwrap();
    w1.write_all(b"x").unwrap();
    let waited = Rc::new(Cell::new(Duration::MAX));
    let got = Rc::new(Cell::new(0u8));

    let (wt, g) = (waited.clone(), got.clone());
    reactor
        .spawn(move || {
            let r1 = Registered::new(r1).unwrap();
            let r2 = Registered::new(r2).unwrap();
            let mut buf = [0; 1];
            r2.read_with(|mut s| s.read(&mut buf)).unwrap(); // r1's report arrives meanwhile
            let start = Instant::now();
            r1.wait_readable(); // must return at once: the report is pending
            wt.set(start.elapsed());
            r1.read_with(|mut s| s.read(&mut buf)).unwrap();
            g.set(buf[0]);
        })
        .unwrap();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        w2.write_all(b"y").unwrap();
        // If the report was lost, this second write is what wakes the reader
        // (instead of the test hanging). Normally the reader is long done
        // and the pipe closed.
        std::thread::sleep(Duration::from_millis(400));
        let _ = w1.write_all(b"z");
    });
    run(&reactor).unwrap();
    writer.join().unwrap();
    assert_eq!(got.get(), b'x');
    assert!(waited.get() < Duration::from_millis(200), "waited {:?}: readiness was lost", waited.get());
}

#[test]
fn woken_by_another_thread() {
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let got = Rc::new(Cell::new(0u8));
    let g = got.clone();
    reactor
        .spawn(move || {
            let a = Registered::new(a).unwrap();
            let mut buf = [0; 1];
            a.read_with(|mut s| s.read(&mut buf)).unwrap();
            g.set(buf[0]);
        })
        .unwrap();
    let start = Instant::now();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        (&b).write_all(&[42]).unwrap();
    });
    // Nothing to run: blocks in epoll_wait until the other thread writes.
    run(&reactor).unwrap();
    writer.join().unwrap();
    assert_eq!(got.get(), 42);
    assert!(start.elapsed() >= Duration::from_millis(50));
}

#[test]
fn many_fibers_wait_at_once() {
    const N: usize = 200;
    let reactor = Reactor::new().unwrap();
    let done = Rc::new(Cell::new(0));
    let mut writers = Vec::new();
    for i in 0..N {
        let (a, b) = UnixStream::pair().unwrap();
        writers.push(b);
        let d = done.clone();
        reactor
            .spawn(move || {
                let a = Registered::new(a).unwrap();
                let mut buf = [0; 8];
                read_exact(&a, &mut buf).unwrap();
                assert_eq!(u64::from_le_bytes(buf), i as u64);
                d.set(d.get() + 1);
            })
            .unwrap();
    }
    reactor
        .spawn(move || {
            yield_now(); // all readers wait now
            // Answer in reverse order, to show each fiber gets its own wakeup.
            for (i, b) in writers.into_iter().enumerate().rev() {
                let b = Registered::new(b).unwrap();
                write_all(&b, &(i as u64).to_le_bytes()).unwrap();
            }
        })
        .unwrap();
    run(&reactor).unwrap();
    assert_eq!(done.get(), N);
    assert_eq!(reactor.fiber_count(), 0);
    assert_eq!(reactor.registration_count(), 0);
}

#[test]
fn reader_and_writer_fibers_share_one_socket() {
    // One fiber reads from `a` while another writes to it; the peer echoes.
    const MSGS: u32 = 100;
    let reactor = Reactor::new().unwrap();
    let (a, b) = UnixStream::pair().unwrap();
    let a = Rc::new(Registered::new(a).unwrap());
    let received = Rc::new(Cell::new(0));

    let reader = a.clone();
    let r = received.clone();
    reactor
        .spawn(move || {
            for i in 0..MSGS {
                let mut buf = [0; 4];
                read_exact(&reader, &mut buf).unwrap();
                assert_eq!(u32::from_le_bytes(buf), i);
                r.set(r.get() + 1);
            }
        })
        .unwrap();
    let writer = a.clone();
    reactor
        .spawn(move || {
            for i in 0..MSGS {
                write_all(&writer, &i.to_le_bytes()).unwrap();
                yield_now();
            }
        })
        .unwrap();
    reactor
        .spawn(move || {
            let b = Registered::new(b).unwrap();
            let mut buf = [0; 4];
            for _ in 0..MSGS {
                read_exact(&b, &mut buf).unwrap();
                write_all(&b, &buf).unwrap();
            }
        })
        .unwrap();
    drop(a);
    run(&reactor).unwrap();
    assert_eq!(received.get(), MSGS);
}

#[test]
fn panic_leaves_run_and_drop_cancels_the_waiting_fibers() {
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let dropped = Rc::new(Cell::new(false));

    let d = dropped.clone();
    reactor
        .spawn(move || {
            let _guard = DropFlag(d); // lives on the fiber's stack
            let r = Registered::new(r).unwrap();
            let mut buf = [0; 1];
            let _ = r.read_with(|mut s| s.read(&mut buf)); // nobody ever writes
            unreachable!("never resumed");
        })
        .unwrap();
    reactor
        .spawn(|| {
            yield_now(); // the reader waits now
            panic!("boom");
        })
        .unwrap();

    let result = panic::catch_unwind(AssertUnwindSafe(|| run(&reactor)));
    assert_eq!(panic_message(&*result.unwrap_err()), "boom");
    assert_eq!(reactor.fiber_count(), 1, "the waiting reader is still there");
    assert_eq!(reactor.registration_count(), 1);
    assert!(!dropped.get());

    drop(reactor);
    assert!(dropped.get(), "the waiting fiber's stack was unwound");
    drop(w);

    // The thread's reactor is gone: a new one can be created.
    let reactor = Reactor::new().unwrap();
    run(&reactor).unwrap();
}

#[test]
fn panic_handler_keeps_the_loop_running() {
    let reactor = Reactor::new().unwrap();
    let (r, w) = io::pipe().unwrap();
    let caught = Rc::new(RefCell::new(Vec::new()));
    let c = caught.clone();
    reactor.set_panic_handler(move |payload| c.borrow_mut().push(panic_message(&*payload)));

    let dropped = Rc::new(Cell::new(false));
    let d = dropped.clone();
    reactor
        .spawn(move || {
            let _guard = DropFlag(d);
            panic!("boom");
        })
        .unwrap();
    let got = Rc::new(Cell::new(false));
    let g = got.clone();
    reactor
        .spawn(move || {
            // Waits across the other fiber's panic.
            let r = Registered::new(r).unwrap();
            read_exact(&r, &mut [0; 1]).unwrap();
            g.set(true);
        })
        .unwrap();
    reactor
        .spawn(move || {
            yield_now();
            let w = Registered::new(w).unwrap();
            write_all(&w, b"x").unwrap();
            panic!("second");
        })
        .unwrap();

    run(&reactor).unwrap();
    assert_eq!(*caught.borrow(), ["boom", "second"], "the handler stays installed");
    assert!(dropped.get(), "the panicking fiber's stack was unwound");
    assert!(got.get(), "the other fibers carried on");
    assert_eq!(reactor.fiber_count(), 0);
}

#[test]
fn panic_handler_may_spawn() {
    let reactor = Reactor::new().unwrap();
    let respawned = Rc::new(Cell::new(false));
    let r = respawned.clone();
    reactor.set_panic_handler(move |_| {
        let r = r.clone();
        spawn(move || r.set(true)).unwrap();
    });
    reactor.spawn(|| panic!("boom")).unwrap();
    run(&reactor).unwrap();
    assert!(respawned.get());
}

#[test]
fn dropping_reactor_drops_fibers_that_never_ran() {
    let dropped = Rc::new(Cell::new(false));
    let reactor = Reactor::new().unwrap();
    let flag = DropFlag(dropped.clone());
    reactor.spawn(move || drop(flag)).unwrap();
    drop(reactor);
    assert!(dropped.get());
}

#[test]
fn suspended_without_waiting_for_io_is_an_error() {
    let reactor = Reactor::new().unwrap();
    let (r, mut w) = io::pipe().unwrap();
    reactor
        .spawn(move || {
            // An I/O wait first, so the count of waiting fibers goes up and,
            // correctly, down again.
            let r = Registered::new(r).unwrap();
            let mut buf = [0; 1];
            r.read_with(|mut s| s.read(&mut buf)).unwrap();
            fiber::suspend(); // nothing will ever resume this
        })
        .unwrap();
    reactor
        .spawn(move || {
            yield_now();
            w.write_all(b"x").unwrap();
        })
        .unwrap();
    let err = run(&reactor).unwrap_err();
    assert!(err.to_string().contains("1 fiber(s) suspended"), "{err}");
}

#[test]
fn two_fibers_reading_one_fd_panics() {
    let reactor = Reactor::new().unwrap();
    let (r, _w) = io::pipe().unwrap();
    let r = Rc::new(Registered::new(r).unwrap());
    for _ in 0..2 {
        let r = r.clone();
        reactor
            .spawn(move || {
                let mut buf = [0; 1];
                let _ = r.read_with(|mut s| s.read(&mut buf));
            })
            .unwrap();
    }
    drop(r);
    let result = panic::catch_unwind(AssertUnwindSafe(|| run(&reactor)));
    assert!(panic_message(&*result.unwrap_err()).contains("two fibers wait for the same fd"));
}

#[test]
fn waiting_outside_a_reactor_fiber_panics() {
    let _reactor = Reactor::new().unwrap();
    let (r, _w) = io::pipe().unwrap();
    let r = Registered::new(r).unwrap();
    let result = panic::catch_unwind(AssertUnwindSafe(|| r.wait_readable()));
    assert!(panic_message(&*result.unwrap_err()).contains("outside of a fiber"));
}

#[test]
#[should_panic(expected = "already has a reactor")]
fn one_reactor_per_thread() {
    let _a = Reactor::new().unwrap();
    let _b = Reactor::new().unwrap();
}

#[test]
#[should_panic(expected = "no reactor on this thread")]
fn registering_without_a_reactor_panics() {
    let (r, _w) = io::pipe().unwrap();
    let _ = Registered::new(r);
}

#[test]
fn registered_makes_the_fd_non_blocking() {
    use std::os::fd::AsRawFd;
    let _reactor = Reactor::new().unwrap();
    let (a, _b) = UnixStream::pair().unwrap();
    let a = Registered::new(a).unwrap();
    // Checked via the flag rather than by reading: a blocking read would hang.
    let flags = unsafe { libc::fcntl(a.get_ref().as_raw_fd(), libc::F_GETFL) };
    assert!(flags != -1 && flags & libc::O_NONBLOCK != 0);
}
