//! Milestone 3: TCP over loopback.

mod common;

use common::{panic_message, run, DropFlag};
use reactor::{spawn, yield_now, Reactor, TcpListener, TcpStream};
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::time::Duration;

fn localhost() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Echoes everything it reads until the peer shuts down writing.
fn echo(conn: TcpStream) {
    let mut buf = [0; 4096];
    loop {
        let n = (&conn).read(&mut buf).unwrap();
        if n == 0 {
            return;
        }
        (&conn).write_all(&buf[..n]).unwrap();
    }
}

/// Distinct, non-repeating-looking bytes, so a misplaced chunk is noticed.
fn pattern(seed: u32, len: usize) -> Vec<u8> {
    (0..len as u32).map(|i| (i.wrapping_mul(31).wrapping_add(seed * 7) % 251) as u8).collect()
}

#[test]
fn echo_server_with_many_clients() {
    echo_server(Duration::ZERO);
}

#[test]
fn echo_server_busy_polling() {
    echo_server(Duration::from_micros(200));
}

fn echo_server(busy_poll: Duration) {
    const CLIENTS: u32 = 100;
    let reactor = Reactor::new().unwrap();
    reactor.set_busy_poll(busy_poll);
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();

    reactor
        .spawn(move || {
            for _ in 0..CLIENTS {
                let (conn, _) = listener.accept().unwrap();
                spawn(move || echo(conn)).unwrap();
            }
        })
        .unwrap();
    let ok = Rc::new(Cell::new(0));
    for i in 0..CLIENTS {
        let ok = ok.clone();
        reactor
            .spawn(move || {
                let mut conn = TcpStream::connect(addr).unwrap();
                // Bigger than one read of the echo loop, smaller than the
                // socket buffers (the client writes everything before it reads).
                let sent = pattern(i, 20_000);
                conn.write_all(&sent).unwrap();
                conn.shutdown(Shutdown::Write).unwrap();
                let mut got = Vec::new();
                conn.read_to_end(&mut got).unwrap();
                assert!(got == sent, "client {i} got its data back mangled");
                ok.set(ok.get() + 1);
            })
            .unwrap();
    }
    run(&reactor).unwrap();
    assert_eq!(ok.get(), CLIENTS);
    assert_eq!(reactor.registration_count(), 0);
}

#[test]
fn accept_reports_the_clients_address() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Rc::new(RefCell::new(None));
    let client = Rc::new(RefCell::new(None));

    let s = seen.clone();
    reactor
        .spawn(move || {
            let (conn, peer) = listener.accept().unwrap();
            assert_eq!(conn.peer_addr().unwrap(), peer);
            assert_eq!(conn.local_addr().unwrap(), addr);
            *s.borrow_mut() = Some(peer);
        })
        .unwrap();
    let c = client.clone();
    reactor
        .spawn(move || {
            let conn = TcpStream::connect(addr).unwrap();
            assert_eq!(conn.peer_addr().unwrap(), addr);
            *c.borrow_mut() = Some(conn.local_addr().unwrap());
        })
        .unwrap();
    run(&reactor).unwrap();
    assert!(seen.borrow().is_some());
    assert_eq!(*seen.borrow(), *client.borrow());
}

#[test]
fn connect_waits_for_the_listener_to_accept() {
    // The connection is established by the kernel before accept() runs; the
    // client must still see a working socket, and data it sent early must be
    // there when the server finally accepts.
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();

    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            conn.write_all(b"early").unwrap();
        })
        .unwrap();
    reactor
        .spawn(move || {
            for _ in 0..10 {
                yield_now();
            }
            let (mut conn, _) = listener.accept().unwrap();
            let mut got = Vec::new();
            conn.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"early");
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn connect_timeout_connects() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    reactor
        .spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            echo(conn);
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect_timeout(addr, Duration::from_secs(5)).unwrap();
            conn.write_all(b"ping").unwrap();
            let mut buf = [0; 4];
            conn.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"ping");
        })
        .unwrap();
    run(&reactor).unwrap();
}

/// A listener whose accept queue is full drops new SYNs (Linux), so the
/// connect neither succeeds nor fails until the timeout.
#[cfg(target_os = "linux")]
#[test]
fn connect_timeout_gives_up() {
    use std::os::fd::FromRawFd;
    let listener = unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        assert!(fd >= 0);
        let mut sin: libc::sockaddr_in = std::mem::zeroed();
        sin.sin_family = libc::AF_INET as libc::sa_family_t;
        sin.sin_addr.s_addr = u32::from_ne_bytes([127, 0, 0, 1]);
        let len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        assert_eq!(libc::bind(fd, &sin as *const _ as *const libc::sockaddr, len), 0);
        assert_eq!(libc::listen(fd, 0), 0);
        std::net::TcpListener::from_raw_fd(fd)
    };
    let addr = listener.local_addr().unwrap();
    // Fill the queue: nobody accepts.
    let mut held = Vec::new();
    while let Ok(s) = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
        held.push(s);
        assert!(held.len() < 16, "the accept queue never filled");
    }

    let reactor = Reactor::new().unwrap();
    let result = Rc::new(RefCell::new(None));
    let r = result.clone();
    reactor
        .spawn(move || {
            let start = std::time::Instant::now();
            let outcome = TcpStream::connect_timeout(addr, Duration::from_millis(300)).map(drop);
            *r.borrow_mut() = Some((outcome, start.elapsed()));
        })
        .unwrap();
    run(&reactor).unwrap();
    let (outcome, elapsed) = result.borrow_mut().take().unwrap();
    assert_eq!(outcome.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(2), "{elapsed:?}");
    assert_eq!(reactor.registration_count(), 0, "the socket was closed");
    drop((held, listener));
}

#[test]
fn connect_to_a_closed_port_is_refused() {
    let reactor = Reactor::new().unwrap();
    // Find a port nobody listens on: bind one, then close it.
    let addr = std::net::TcpListener::bind(localhost()).unwrap().local_addr().unwrap();
    let result = Rc::new(RefCell::new(None));

    let r = result.clone();
    reactor
        .spawn(move || {
            *r.borrow_mut() = Some(TcpStream::connect(addr).map(drop));
        })
        .unwrap();
    run(&reactor).unwrap();
    let err = result.borrow_mut().take().unwrap().unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err}");
    assert_eq!(reactor.registration_count(), 0, "the failed socket was closed");
}

#[test]
fn ipv6_loopback() {
    let Ok(probe) = std::net::TcpListener::bind("[::1]:0") else {
        eprintln!("no IPv6 loopback here, skipping");
        return;
    };
    drop(probe);
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind("[::1]:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    reactor
        .spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            echo(conn);
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            conn.write_all(b"v6").unwrap();
            conn.shutdown(Shutdown::Write).unwrap();
            let mut got = Vec::new();
            conn.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"v6");
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn peer_closing_gives_eof_then_write_errors() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();

    reactor
        .spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            drop(conn); // close without reading or writing anything
        })
        .unwrap();
    let error = Rc::new(RefCell::new(None));
    let e = error.clone();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            let mut buf = [0; 16];
            assert_eq!(conn.read(&mut buf).unwrap(), 0, "EOF once the server closed");
            // The first write usually still succeeds (the peer answers it with
            // a reset); keep writing until the error shows up.
            loop {
                if let Err(err) = conn.write(&[0; 1024]) {
                    *e.borrow_mut() = Some(err.kind());
                    return;
                }
                yield_now();
            }
        })
        .unwrap();
    run(&reactor).unwrap();
    let kind = error.borrow().unwrap();
    assert!(
        matches!(kind, io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset),
        "unexpected error {kind:?}"
    );
}

#[test]
fn large_write_waits_for_the_reader() {
    // 16 MiB is far more than the socket buffers hold: the writer must
    // suspend before the reader has even started.
    const LEN: usize = 16 << 20;
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));

    let l = log.clone();
    reactor
        .spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(&pattern(1, LEN)).unwrap();
            l.borrow_mut().push("writer done");
        })
        .unwrap();
    let l = log.clone();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            l.borrow_mut().push("reader starts");
            let mut got = Vec::new();
            conn.read_to_end(&mut got).unwrap();
            assert!(got == pattern(1, LEN), "data mangled");
            l.borrow_mut().push("reader done");
        })
        .unwrap();
    run(&reactor).unwrap();
    assert_eq!(*log.borrow(), ["reader starts", "writer done", "reader done"]);
}

#[test]
fn read_wakes_up_while_the_send_buffer_is_full() {
    // On Linux epoll also reports "writable" when data arrives, as long as
    // the send buffer has room. With it full, only "readable" is reported: a
    // read that waited for the wrong direction would never wake up.
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    let filled = Rc::new(Cell::new(false));
    let got_reply = Rc::new(Cell::new(false));

    let (f, g) = (filled.clone(), got_reply.clone());
    reactor
        .spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            while !f.get() {
                yield_now();
            }
            conn.write_all(b"x").unwrap();
            while !g.get() {
                yield_now();
            }
            io::copy(&mut conn, &mut io::sink()).unwrap(); // drain, until EOF
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            // Fill the send buffer (and the server's receive buffer) without
            // suspending: the std socket is non-blocking.
            let chunk = vec![0; 65536]; // not on the 64 KiB fiber stack
            loop {
                match conn.get_ref().write(&chunk) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("{e}"),
                }
            }
            filled.set(true);
            let mut buf = [0; 1];
            conn.read_exact(&mut buf).unwrap();
            got_reply.set(true);
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn reader_and_writer_fibers_share_one_connection() {
    const MESSAGES: u32 = 1000;
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();

    reactor
        .spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            echo(conn);
        })
        .unwrap();
    reactor
        .spawn(move || {
            let conn = Rc::new(TcpStream::connect(addr).unwrap());
            let writer = conn.clone();
            spawn(move || {
                for i in 0..MESSAGES {
                    (&*writer).write_all(&i.to_le_bytes()).unwrap();
                }
                writer.shutdown(Shutdown::Write).unwrap();
            })
            .unwrap();
            // Meanwhile this fiber reads the echoes.
            for i in 0..MESSAGES {
                let mut buf = [0; 4];
                (&*conn).read_exact(&mut buf).unwrap();
                assert_eq!(u32::from_le_bytes(buf), i);
            }
            assert_eq!((&*conn).read(&mut [0; 1]).unwrap(), 0);
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn dropping_the_reactor_cancels_a_pending_accept() {
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    let dropped = Rc::new(Cell::new(false));

    let d = dropped.clone();
    reactor
        .spawn(move || {
            let _guard = DropFlag(d);
            let _ = listener.accept(); // nobody ever connects
            unreachable!("never resumed");
        })
        .unwrap();
    reactor
        .spawn(|| {
            yield_now(); // the acceptor waits now
            panic!("boom");
        })
        .unwrap();
    let result = panic::catch_unwind(AssertUnwindSafe(|| run(&reactor)));
    assert_eq!(panic_message(&*result.unwrap_err()), "boom");
    assert_eq!(reactor.registration_count(), 1);

    drop(reactor);
    assert!(dropped.get(), "the accepting fiber's stack was unwound");
    // The listener was closed with it: the port can be bound again (a
    // listening socket would make this fail with EADDRINUSE).
    std::net::TcpListener::bind(addr).expect("listener still open");
}

#[test]
fn from_std_registers_existing_sockets() {
    let reactor = Reactor::new().unwrap();
    let std_listener = std::net::TcpListener::bind(localhost()).unwrap();
    let addr = std_listener.local_addr().unwrap();
    // Connected outside the reactor, before it ever runs.
    let std_client = std::net::TcpStream::connect(addr).unwrap();

    reactor
        .spawn(move || {
            let listener = TcpListener::from_std(std_listener).unwrap();
            let (conn, _) = listener.accept().unwrap();
            echo(conn);
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::from_std(std_client).unwrap();
            conn.set_nodelay(true).unwrap();
            conn.write_all(b"adopted").unwrap();
            conn.shutdown(Shutdown::Write).unwrap();
            let mut got = Vec::new();
            conn.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"adopted");
        })
        .unwrap();
    run(&reactor).unwrap();
}

#[test]
fn end_of_stream_right_after_data() {
    // The data and the end of the stream arrive together while the reader
    // waits: one event. The first read is short, it stops at the end of the
    // stream, and no later event will report that end again, so the second
    // read must not wait for one.
    let reactor = Reactor::new().unwrap();
    let listener = TcpListener::bind(localhost()).unwrap();
    let addr = listener.local_addr().unwrap();
    let reading = Rc::new(Cell::new(false));
    let reads = Rc::new(RefCell::new(Vec::new()));
    let (r, log) = (reading.clone(), reads.clone());
    reactor
        .spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut buf = [0; 1024];
            r.set(true);
            loop {
                let n = conn.read(&mut buf).unwrap(); // the first one waits
                log.borrow_mut().push(n);
                if n == 0 {
                    break;
                }
            }
        })
        .unwrap();
    reactor
        .spawn(move || {
            let mut conn = TcpStream::connect(addr).unwrap();
            while !reading.get() {
                yield_now();
            }
            yield_now(); // the reader is suspended in its first read now
            conn.write_all(b"hello").unwrap();
            conn.shutdown(Shutdown::Write).unwrap();
        })
        .unwrap();
    run(&reactor).unwrap();
    assert_eq!(*reads.borrow(), [5, 0]);
}
