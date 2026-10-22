//! Cost of one wakeup through the reactor. Run with `cargo bench -p reactor`.
//!
//! Two fibers bounce a byte over a Unix socket pair, then over a loopback TCP
//! connection. Each round trip is two writes, two reads, two waits that
//! suspend, and the epoll_waits (kevents on macOS) that wake the fibers again. A third run adds
//! a deadline to every read, as a read timeout does: a timer armed and
//! cancelled per wait.

fn main() {
    bench::main();
}

mod bench {
    use reactor::{Reactor, Registered, TcpListener, TcpStream};
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    const ROUND_TRIPS: u32 = 200_000;

    pub fn main() {
        println!("{ROUND_TRIPS} round trips, best of 5\n");
        report("ping-pong over a socket pair", |n| ping_pong(n, false));
        report("  with a deadline per read", |n| ping_pong(n, true));
        report("ping-pong over TCP loopback", tcp_ping_pong);
    }

    fn report(name: &str, bench: fn(u32) -> f64) {
        let best = (0..5).map(|_| bench(ROUND_TRIPS)).fold(f64::INFINITY, f64::min);
        let per_trip = best / f64::from(ROUND_TRIPS);
        println!("{name:30} {per_trip:7.0} ns/round trip");
        println!("{:30} {:7.0} round trips/s", "", 1e9 / per_trip);
    }

    /// Returns the elapsed nanoseconds.
    fn ping_pong(n: u32, deadlines: bool) -> f64 {
        let reactor = Reactor::new().unwrap();
        let (a, b) = UnixStream::pair().unwrap();
        reactor
            .spawn(move || {
                let a = Registered::new(a).unwrap();
                let mut buf = [0u8; 1];
                for _ in 0..n {
                    a.write_with(|mut s| s.write(&buf)).unwrap();
                    read(&a, &mut buf, deadlines);
                }
            })
            .unwrap();
        reactor
            .spawn(move || {
                let b = Registered::new(b).unwrap();
                let mut buf = [0u8; 1];
                for _ in 0..n {
                    read(&b, &mut buf, deadlines);
                    b.write_with(|mut s| s.write(&buf)).unwrap();
                }
            })
            .unwrap();
        let start = Instant::now();
        reactor.run().unwrap();
        start.elapsed().as_nanos() as f64
    }

    fn read(s: &Registered<UnixStream>, buf: &mut [u8], deadline: bool) {
        if deadline {
            let deadline = Instant::now() + Duration::from_secs(3600);
            s.read_with_deadline(deadline, |mut s| s.read(buf)).unwrap();
        } else {
            s.read_with(|mut s| s.read(buf)).unwrap();
        }
    }

    /// Like `ping_pong`, over TCP with Nagle off. Connection setup is not
    /// timed.
    fn tcp_ping_pong(n: u32) -> f64 {
        let reactor = Reactor::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let start = std::rc::Rc::new(std::cell::Cell::new(None));
        let s = start.clone();
        reactor
            .spawn(move || {
                let mut a = TcpStream::connect(addr).unwrap();
                a.set_nodelay(true).unwrap();
                let mut buf = [0u8; 1];
                s.set(Some(Instant::now()));
                for _ in 0..n {
                    a.write_all(&buf).unwrap();
                    a.read_exact(&mut buf).unwrap();
                }
            })
            .unwrap();
        reactor
            .spawn(move || {
                let (mut b, _) = listener.accept().unwrap();
                b.set_nodelay(true).unwrap();
                let mut buf = [0u8; 1];
                for _ in 0..n {
                    b.read_exact(&mut buf).unwrap();
                    b.write_all(&buf).unwrap();
                }
            })
            .unwrap();
        reactor.run().unwrap();
        start.get().unwrap().elapsed().as_nanos() as f64
    }
}
