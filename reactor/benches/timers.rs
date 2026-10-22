//! Cost of timers. Run with `cargo bench -p reactor`.
//!
//! - One fiber sleeping 0 in a loop: arm a timer, a turn of the loop (with
//!   its epoll_wait or kevent), fire it.
//! - Many fibers sleeping at once: what each timer costs when the heap is
//!   large.

fn main() {
    bench::main();
}

mod bench {
    use reactor::{sleep, sleep_until, Reactor};
    use std::time::{Duration, Instant};

    pub fn main() {
        println!("best of 5\n");
        let n = 200_000;
        let best = best_of_5(|| sleep_zero(n));
        println!("sleep(0) in a loop            {:7.0} ns/sleep", best / f64::from(n));

        let (fibers, sleeps) = (10_000, 20);
        let best = best_of_5(|| many_sleepers(fibers, sleeps));
        println!(
            "{fibers} fibers sleeping at once {:7.0} ns/timer (armed, ordered, fired, fiber resumed)",
            best / f64::from(fibers * sleeps)
        );
    }

    fn best_of_5(mut bench: impl FnMut() -> f64) -> f64 {
        (0..5).map(|_| bench()).fold(f64::INFINITY, f64::min)
    }

    /// Returns the elapsed nanoseconds.
    fn sleep_zero(n: u32) -> f64 {
        let reactor = Reactor::new().unwrap();
        reactor
            .spawn(move || {
                for _ in 0..n {
                    sleep(Duration::ZERO);
                }
            })
            .unwrap();
        let start = Instant::now();
        reactor.run().unwrap();
        start.elapsed().as_nanos() as f64
    }

    /// Every fiber sleeps until the same deadlines, all already due, so the
    /// time measured is the reactor's work and not the waiting: each round
    /// all `fibers` timers are in the heap at once.
    fn many_sleepers(fibers: u32, sleeps: u32) -> f64 {
        let reactor = Reactor::new().unwrap();
        let base = Instant::now();
        for i in 0..fibers {
            reactor
                .spawn(move || {
                    for j in 0..sleeps {
                        // Distinct deadlines spread over the past, so the
                        // heap has real ordering work to do.
                        let offset = Duration::from_nanos(u64::from((i * 7919 + j * 104_729) % 1_000_000));
                        sleep_until(base - Duration::from_millis(1) + offset / 1000);
                    }
                })
                .unwrap();
        }
        let start = Instant::now();
        reactor.run().unwrap();
        start.elapsed().as_nanos() as f64
    }
}
