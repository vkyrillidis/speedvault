//! Helpers shared by the integration tests.

#![allow(dead_code)] // each test binary uses a different subset

use reactor::Reactor;
use std::any::Any;
use std::cell::Cell;
use std::io;
use std::rc::Rc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

/// `reactor.run()`, but if it hangs (a lost wakeup leaves a fiber waiting
/// forever) the test binary aborts with a message after 10 s, instead of
/// blocking until some outer timeout.
pub fn run(reactor: &Reactor) -> io::Result<()> {
    let (done, finished) = mpsc::channel::<()>();
    let name = std::thread::current().name().unwrap_or("?").to_string();
    std::thread::spawn(move || {
        // Returns early with Disconnected when `done` is dropped: run() is over.
        if finished.recv_timeout(Duration::from_secs(10)) == Err(RecvTimeoutError::Timeout) {
            eprintln!("{name}: Reactor::run hung for 10 s, a fiber was never woken up");
            std::process::abort();
        }
    });
    let result = reactor.run();
    drop(done);
    result
}

/// Sets a flag when dropped.
pub struct DropFlag(pub Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}
