//! `Notify`: a fiber woken by another, a notify kept for the next wait,
//! deadlines, and a waiter cancelled while it waits.

use reactor::{sleep, spawn, yield_now, Notify, Reactor};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

type Log = Rc<RefCell<Vec<String>>>;

fn push(log: &Log, s: &str) {
    log.borrow_mut().push(s.to_string());
}

#[test]
fn a_waiter_is_woken_by_another_fiber() {
    let reactor = Reactor::new().unwrap();
    let (n, log): (Rc<Notify>, Log) = (Rc::default(), Rc::default());
    let (n2, log2) = (n.clone(), log.clone());
    reactor
        .spawn(move || {
            push(&log2, "wait");
            n2.wait();
            push(&log2, "woken");
            n2.wait();
            push(&log2, "woken again");
        })
        .unwrap();
    let log3 = log.clone();
    reactor
        .spawn(move || {
            for i in 0..3 {
                yield_now();
                push(&log3, &format!("turn {i}"));
            }
            n.notify();
            // Two notifies before the next wait count as one.
            yield_now();
            n.notify();
            n.notify();
            push(&log3, "notified");
        })
        .unwrap();
    reactor.run().unwrap();
    assert_eq!(*log.borrow(), ["wait", "turn 0", "turn 1", "turn 2", "woken", "notified", "woken again"]);
}

#[test]
fn a_notify_before_the_wait_is_kept() {
    let reactor = Reactor::new().unwrap();
    let n = Rc::new(Notify::new());
    let n2 = n.clone();
    let done = Rc::new(RefCell::new(false));
    let d = done.clone();
    n.notify();
    reactor
        .spawn(move || {
            n2.wait();
            *d.borrow_mut() = true;
            // Taken: the next wait waits (here until its deadline).
            assert!(!n2.wait_until(Some(Instant::now() + Duration::from_millis(5))));
        })
        .unwrap();
    reactor.run().unwrap();
    assert!(*done.borrow());
}

#[test]
fn deadlines() {
    let reactor = Reactor::new().unwrap();
    let n = Rc::new(Notify::new());
    let (a, b) = (n.clone(), n.clone());
    let got = Rc::new(RefCell::new(Vec::new()));
    let g = got.clone();
    reactor
        .spawn(move || {
            let start = Instant::now();
            let woke = a.wait_until(Some(start + Duration::from_millis(20)));
            g.borrow_mut().push((woke, start.elapsed() >= Duration::from_millis(20)));
            // Notified before the deadline: true, early.
            let start = Instant::now();
            let woke = a.wait_until(Some(start + Duration::from_secs(10)));
            g.borrow_mut().push((woke, start.elapsed() < Duration::from_secs(1)));
        })
        .unwrap();
    reactor
        .spawn(move || {
            sleep(Duration::from_millis(40));
            b.notify();
        })
        .unwrap();
    reactor.run().unwrap();
    assert_eq!(*got.borrow(), [(false, true), (true, true)]);
}

#[test]
fn a_timed_out_waiter_is_not_woken_later() {
    // The notify after the deadline is kept for the next wait, not sent to
    // the fiber whose wait ended.
    let reactor = Reactor::new().unwrap();
    let n = Rc::new(Notify::new());
    let (a, b) = (n.clone(), n.clone());
    let log: Log = Rc::default();
    let l = log.clone();
    reactor
        .spawn(move || {
            assert!(!a.wait_until(Some(Instant::now() + Duration::from_millis(1))));
            push(&l, "timed out");
            sleep(Duration::from_millis(20));
            push(&l, "slept");
            a.wait();
            push(&l, "took the kept notify");
        })
        .unwrap();
    let l = log.clone();
    reactor
        .spawn(move || {
            sleep(Duration::from_millis(10));
            b.notify();
            push(&l, "notified");
        })
        .unwrap();
    reactor.run().unwrap();
    assert_eq!(*log.borrow(), ["timed out", "notified", "slept", "took the kept notify"]);
}

#[test]
fn a_cancelled_waiter_leaves_the_notify_free() {
    // A fiber waiting forever is cancelled when the reactor is dropped; the
    // Notify (outliving it) can be waited on again by another reactor's
    // fiber.
    let n = Rc::new(Notify::new());
    {
        let reactor = Reactor::new().unwrap();
        let a = n.clone();
        reactor.spawn(move || a.wait()).unwrap();
        reactor
            .spawn(move || {
                yield_now();
                // `run` ends only once every fiber has: a panic (no
                // handler) leaves it with the other still waiting.
                panic!("stop");
            })
            .unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reactor.run()));
    }
    let reactor = Reactor::new().unwrap();
    let a = n.clone();
    reactor
        .spawn(move || {
            spawn({
                let b = a.clone();
                move || b.notify()
            })
            .unwrap();
            a.wait();
        })
        .unwrap();
    reactor.run().unwrap();
}

#[test]
fn a_woken_waiter_leaves_the_next_one_in_place() {
    // A is woken, and before it runs B starts waiting: when A goes on, B
    // must still be the one the next notify wakes.
    let reactor = Reactor::new().unwrap();
    let n = Rc::new(Notify::new());
    let log: Log = Rc::default();
    let (a, l) = (n.clone(), log.clone());
    reactor
        .spawn(move || {
            a.wait();
            push(&l, "a woken");
        })
        .unwrap();
    let (b, l) = (n.clone(), log.clone());
    reactor
        .spawn(move || {
            yield_now();
            // A is waiting: wake it, then wait here before it runs.
            b.notify();
            b.wait();
            push(&l, "b woken");
        })
        .unwrap();
    let (c, l) = (n.clone(), log.clone());
    reactor
        .spawn(move || {
            for _ in 0..3 {
                yield_now();
            }
            push(&l, "notifying b");
            c.notify();
        })
        .unwrap();
    reactor.run().unwrap();
    assert_eq!(*log.borrow(), ["a woken", "notifying b", "b woken"]);
}

#[test]
#[should_panic(expected = "two fibers wait on one Notify")]
fn one_waiter_at_a_time() {
    let reactor = Reactor::new().unwrap();
    let n = Rc::new(Notify::new());
    for _ in 0..2 {
        let a = n.clone();
        reactor.spawn(move || a.wait()).unwrap();
    }
    reactor.run().unwrap();
}
