//! The contract both pollers (epoll, kqueue) keep, tested against whichever
//! one this platform uses.

use crate::poller::{Event, Events, Poller};
use crate::Token;
use std::io::{self, PipeReader, PipeWriter, Write};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

fn pipe() -> (PipeReader, PipeWriter) {
    io::pipe().unwrap()
}

fn poll(poller: &Poller) -> Vec<Event> {
    let mut events = Events::with_capacity(16);
    poller.wait(&mut events, Some(Duration::ZERO)).unwrap();
    events.iter().collect()
}

#[test]
fn empty_pipe_is_not_readable() {
    let poller = Poller::new().unwrap();
    let (r, _w) = pipe();
    poller.add(r.as_raw_fd(), Token::new(1, 0)).unwrap();
    assert!(poll(&poller).is_empty());
}

#[test]
fn written_pipe_is_readable_with_its_token() {
    let poller = Poller::new().unwrap();
    let (r, mut w) = pipe();
    poller.add(r.as_raw_fd(), Token::new(7, 3)).unwrap();
    w.write_all(b"hi").unwrap();
    let events = poll(&poller);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].token, Token::new(7, 3));
    assert!(events[0].is_readable());
}

#[test]
fn is_edge_triggered() {
    let poller = Poller::new().unwrap();
    let (r, mut w) = pipe();
    poller.add(r.as_raw_fd(), Token::new(1, 0)).unwrap();
    w.write_all(b"x").unwrap();
    assert_eq!(poll(&poller).len(), 1);
    // Nothing read, but no new data either: no second report.
    assert!(poll(&poller).is_empty());
    // New data is a new edge.
    w.write_all(b"y").unwrap();
    assert_eq!(poll(&poller).len(), 1);
}

#[test]
fn write_end_is_writable_right_away() {
    let poller = Poller::new().unwrap();
    let (_r, w) = pipe();
    poller.add(w.as_raw_fd(), Token::new(2, 0)).unwrap();
    let events = poll(&poller);
    assert_eq!(events.len(), 1);
    assert!(events[0].is_writable());
    assert!(!events[0].is_readable());
}

#[test]
fn closed_writer_wakes_the_reader() {
    let poller = Poller::new().unwrap();
    let (r, w) = pipe();
    poller.add(r.as_raw_fd(), Token::new(1, 0)).unwrap();
    drop(w);
    // The hangup may be reported as writable too: epoll sets EPOLLHUP,
    // kqueue sends EV_EOF on both filters.
    let events = poll(&poller);
    assert!(events.iter().all(|e| e.token == Token::new(1, 0)));
    assert!(events.iter().any(Event::is_readable), "a reader must wake up to see EOF");
}

#[test]
fn deleted_fd_reports_nothing() {
    let poller = Poller::new().unwrap();
    let (r, mut w) = pipe();
    poller.add(r.as_raw_fd(), Token::new(1, 0)).unwrap();
    poller.delete(r.as_raw_fd()).unwrap();
    w.write_all(b"x").unwrap();
    assert!(poll(&poller).is_empty());
}

#[test]
fn adding_a_bad_fd_fails() {
    let poller = Poller::new().unwrap();
    let err = poller.add(-1, Token::new(1, 0)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn wait_honours_the_timeout() {
    let poller = Poller::new().unwrap();
    let mut events = Events::with_capacity(1);
    let start = Instant::now();
    poller.wait(&mut events, Some(Duration::from_millis(20))).unwrap();
    assert!(events.is_empty());
    assert!(start.elapsed() >= Duration::from_millis(20));
}
