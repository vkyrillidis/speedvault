//! Waking a fiber from another: [`Notify`].

use crate::reactor::current;
use crate::Token;
use std::cell::Cell;
use std::time::Instant;

/// One fiber waits ([`wait`](Notify::wait)) until another calls
/// [`notify`](Notify::notify): e.g. a fiber that sends what others queue,
/// woken when they queue something. A `notify` with nobody waiting is kept
/// for the next wait, which then returns at once; several of them count as
/// one. One waiter at a time, on the reactor of the thread.
///
/// ```
/// use reactor::{Notify, Reactor};
/// use std::rc::Rc;
///
/// let reactor = Reactor::new().unwrap();
/// let n = Rc::new(Notify::new());
/// let woken = n.clone();
/// reactor.spawn(move || woken.wait()).unwrap();
/// reactor.spawn(move || n.notify()).unwrap();
/// reactor.run().unwrap();
/// ```
#[derive(Debug, Default)]
pub struct Notify {
    waiter: Cell<Option<Token>>,
    pending: Cell<bool>,
}

impl Notify {
    pub fn new() -> Notify {
        Notify::default()
    }

    /// Wakes the fiber waiting, or else the next one to wait.
    pub fn notify(&self) {
        match self.waiter.take() {
            Some(task) => {
                current().wake_task(task);
            }
            None => self.pending.set(true),
        }
    }

    /// Suspends the fiber until notified (at once if a notify is pending).
    ///
    /// # Panics
    /// If another fiber waits already, or not called from a fiber.
    pub fn wait(&self) {
        self.wait_until(None);
    }

    /// [`wait`](Notify::wait), or until `deadline` passes: false then.
    pub fn wait_until(&self, deadline: Option<Instant>) -> bool {
        if self.pending.replace(false) {
            return true;
        }
        let core = current();
        let task = core.running();
        assert!(self.waiter.get().is_none(), "two fibers wait on one Notify");
        self.waiter.set(Some(task));
        // Gone however the wait ends: notified (taken by `notify`), timed
        // out, or the fiber cancelled while it waits.
        let _guard = Clear(&self.waiter, task);
        core.park(deadline)
    }
}

struct Clear<'a>(&'a Cell<Option<Token>>, Token);

impl Drop for Clear<'_> {
    fn drop(&mut self) {
        if self.0.get() == Some(self.1) {
            self.0.set(None);
        }
    }
}
