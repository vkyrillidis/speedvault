//! The event loop: runs fibers until they wait for I/O or a deadline, waits
//! on epoll (kqueue on macOS), and resumes the fibers whose I/O became ready or whose deadline
//! passed.
//!
//! ```text
//!   ┌─▶ run the fibers in `ready` (as of now) until each waits or finishes
//!   │     (a waiting fiber is recorded in its I/O slot: reader/writer,
//!   │      and with its deadline in `timers`)
//!   │   epoll_wait (timeout 0 if fibers are ready again, else until the
//!   │               earliest deadline; with busy-polling, first with
//!   │               timeout 0 for up to the busy-poll time)
//!   │   for each event: slot = io[token]  (stale generation: ignored)
//!   │       wake the slot's waiting fiber
//!   └── wake the fibers whose deadline passed
//! ```
//!
//! "Wake" moves a fiber to `ready` unless it was woken already: a fiber whose
//! I/O became ready in the same turn its deadline passed runs once.
//!
//! All state lives in [`Core`], shared through `Rc` by the [`Reactor`]
//! handle, the thread-local current reactor and every [`Registered`]. Only one
//! fiber runs at a time and no `RefCell` borrow is ever held across a fiber
//! switch, so the borrows never conflict.
//!
//! [`Registered`]: crate::Registered

use crate::poller::{Event, Events, Poller};
use crate::slab::Slab;
use crate::timer::Timers;
use crate::Token;
use fiber::{Fiber, StackPool, State, DEFAULT_STACK_SIZE};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::panic;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// Unused stacks kept for reuse. 1024 × 64 KiB = 64 MiB of address space at
/// most; physical memory only for the pages fibers actually touched.
const MAX_FREE_STACKS: usize = 1024;

/// Events fetched per epoll_wait; more stay pending for the next call.
const EVENTS_PER_WAIT: usize = 1024;

thread_local! {
    /// The reactor of this thread, set while a `Reactor` exists.
    static CURRENT: RefCell<Option<Rc<Core>>> = const { RefCell::new(None) };
}

/// The reactor of the calling thread.
///
/// # Panics
/// If the thread has no reactor.
pub(crate) fn current() -> Rc<Core> {
    CURRENT.with(|c| c.borrow().clone()).expect("no reactor on this thread")
}

pub(crate) struct Core {
    pub(crate) poller: Poller,
    pub(crate) io: RefCell<Slab<IoSlot>>,
    /// Every fiber not finished yet.
    tasks: RefCell<Slab<Task>>,
    /// Fibers to resume, in order.
    ready: RefCell<VecDeque<Token>>,
    pool: RefCell<StackPool>,
    /// The task running right now.
    running_task: Cell<Option<Token>>,
    timers: RefCell<Timers>,
    /// Fibers suspended waiting for I/O or a deadline.
    waiting: Cell<usize>,
    in_run: Cell<bool>,
    /// Called with the payload of a fiber that panicked. `None`: the panic
    /// continues out of `run`.
    panic_handler: RefCell<Option<PanicHandler>>,
    /// How long to poll without blocking before epoll_wait may sleep.
    busy_poll: Cell<Duration>,
    /// Time spent in `run` not waiting for I/O: up to the last time the
    /// loop went to wait (busy-polling counts as waiting), and when it last
    /// stopped waiting.
    busy: Cell<Duration>,
    woke: Cell<Instant>,
}

/// What a panic carries, as returned by [`std::panic::catch_unwind`].
pub type PanicPayload = Box<dyn Any + Send>;

type PanicHandler = Box<dyn FnMut(PanicPayload)>;

struct Task {
    /// `None` while the fiber runs (it is taken out to be resumed).
    fiber: Option<Fiber>,
    /// Suspended in [`Core::wait`] and not woken yet. The first wakeup (I/O
    /// or deadline) clears it; later ones for the same wait are ignored.
    waiting: bool,
    /// The last wait ended because its deadline passed.
    timed_out: bool,
}

/// Per registered fd: who waits for it and readiness nobody waited for yet.
#[derive(Default)]
pub(crate) struct IoSlot {
    pub(crate) reader: Option<Token>,
    pub(crate) writer: Option<Token>,
    /// epoll reported readable while no fiber was waiting to read.
    pub(crate) read_ready: bool,
    /// epoll reported writable while no fiber was waiting to write.
    pub(crate) write_ready: bool,
    /// The last read emptied the socket, and epoll reported nothing since:
    /// the next read waits for readiness instead of trying first (it would
    /// fail with EAGAIN). Edge-triggered epoll reports any data that arrives
    /// after that read.
    pub(crate) read_drained: bool,
    /// epoll reported a hangup or error. A short read may then have stopped
    /// at the end of the stream, which no new event will report: reads are
    /// always tried first.
    pub(crate) read_closed: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Direction {
    Read,
    Write,
}

/// A single-threaded event loop running fibers. At most one per thread.
///
/// Create it, [`spawn`](Reactor::spawn) fibers, then [`run`](Reactor::run)
/// until all of them have finished. Inside the fibers, the free functions
/// [`spawn`] and [`yield_now`] and the [`Registered`](crate::Registered) I/O
/// wrapper use the thread's reactor.
///
/// Dropping the reactor cancels every unfinished fiber: each one's stack is
/// unwound, so destructors run and registrations are removed.
///
/// ```
/// use reactor::{Reactor, Registered};
/// use std::io::{Read, Write};
/// use std::os::unix::net::UnixStream;
///
/// let reactor = Reactor::new().unwrap();
/// let (a, b) = UnixStream::pair().unwrap();
/// reactor.spawn(move || {
///     let a = Registered::new(a).unwrap();
///     let mut buf = [0; 5];
///     // Suspends this fiber until the other one has written.
///     let n = a.read_with(|mut s| s.read(&mut buf)).unwrap();
///     assert_eq!(&buf[..n], b"hello");
/// }).unwrap();
/// reactor.spawn(move || {
///     let b = Registered::new(b).unwrap();
///     b.write_with(|mut s| s.write(b"hello")).unwrap();
/// }).unwrap();
/// reactor.run().unwrap();
/// ```
pub struct Reactor {
    core: Rc<Core>,
}

impl Reactor {
    /// Creates the reactor of the calling thread.
    ///
    /// # Panics
    /// If the thread already has one.
    pub fn new() -> io::Result<Reactor> {
        let core = Rc::new(Core {
            poller: Poller::new()?,
            io: RefCell::new(Slab::new()),
            tasks: RefCell::new(Slab::new()),
            ready: RefCell::new(VecDeque::new()),
            pool: RefCell::new(StackPool::new(DEFAULT_STACK_SIZE, MAX_FREE_STACKS)),
            running_task: Cell::new(None),
            timers: RefCell::new(Timers::new()),
            waiting: Cell::new(0),
            in_run: Cell::new(false),
            panic_handler: RefCell::new(None),
            busy_poll: Cell::new(Duration::ZERO),
            busy: Cell::new(Duration::ZERO),
            woke: Cell::new(Instant::now()),
        });
        CURRENT.with(|c| {
            let mut c = c.borrow_mut();
            assert!(c.is_none(), "this thread already has a reactor");
            *c = Some(core.clone());
        });
        Ok(Reactor { core })
    }

    /// Adds a fiber running `f`. It starts on the next turn of the loop.
    pub fn spawn<F: FnOnce() + 'static>(&self, f: F) -> io::Result<()> {
        self.core.spawn(f)
    }

    /// Runs fibers and dispatches I/O events until every fiber has finished.
    ///
    /// Returns an error if epoll fails, or if fibers are left that nothing can
    /// ever resume: suspended with [`fiber::suspend`] directly instead of
    /// waiting for I/O or sleeping.
    ///
    /// # Panics
    /// If a fiber panics and no [panic handler](Reactor::set_panic_handler)
    /// is set, the panic continues out of `run` (the other fibers stay
    /// suspended; dropping the reactor cancels them). If called from inside a
    /// fiber.
    pub fn run(&self) -> io::Result<()> {
        let core = &*self.core;
        assert!(!core.in_run.replace(true), "Reactor::run is not reentrant");
        // On every way out, panics included: not running any more, and the
        // time since the loop last woke was busy.
        struct Exit<'a>(&'a Core);
        impl Drop for Exit<'_> {
            fn drop(&mut self) {
                let core = self.0;
                core.busy.set(core.busy.get() + core.woke.get().elapsed());
                core.in_run.set(false);
            }
        }

        let mut events = Events::with_capacity(EVENTS_PER_WAIT);
        core.woke.set(Instant::now());
        let _exit = Exit(core);
        loop {
            // Only the fibers that are ready now. One that yields or is
            // spawned meanwhile runs on the next turn, after epoll was asked:
            // a fiber calling yield_now() in a loop must not starve I/O.
            let turn = core.ready.borrow().len();
            for _ in 0..turn {
                let Some(task) = core.pop_ready() else { break };
                core.run_task(task);
            }
            let tasks = core.tasks.borrow().len();
            if tasks == 0 {
                return Ok(());
            }
            let more_ready = !core.ready.borrow().is_empty();
            if !more_ready && core.waiting.get() == 0 {
                return Err(io::Error::other(format!(
                    "{tasks} fiber(s) suspended without waiting for I/O or a timer, nothing can resume them"
                )));
            }
            // Don't block while fibers are ready, nor past the next deadline.
            let waited = !more_ready;
            if more_ready {
                core.poller.wait(&mut events, Some(Duration::ZERO))?;
            } else {
                let deadline = core.timers.borrow_mut().next_deadline();
                core.wait_for_io(&mut events, deadline)?;
            }
            for event in events.iter() {
                core.dispatch(event);
            }
            // After the I/O: a fiber whose I/O is ready gets it, even if its
            // deadline passed meanwhile. Reads the clock only if there are
            // timers, and not again right after a wait.
            if !core.timers.borrow().is_empty() {
                core.expire_timers(if waited { core.woke.get() } else { Instant::now() });
            }
        }
    }

    /// The time [`run`](Reactor::run) has spent not waiting for I/O or a
    /// deadline: running fibers and dispatching events. Busy-polling counts
    /// as waiting, so with it on this stays what the thread would use
    /// without it, while its CPU time goes to 100%. See [`busy_time`].
    pub fn busy_time(&self) -> Duration {
        self.core.busy_time()
    }

    /// Number of fibers that have not finished.
    pub fn fiber_count(&self) -> usize {
        self.core.tasks.borrow().len()
    }

    /// Number of fds currently registered.
    pub fn registration_count(&self) -> usize {
        self.core.io.borrow().len()
    }

    /// Number of deadlines fibers currently wait for.
    pub fn timer_count(&self) -> usize {
        self.core.timers.borrow().len()
    }

    /// Keeps the loop running when a fiber panics: `handler` gets the panic
    /// payload instead, and the other fibers carry on. The panicking fiber's
    /// stack has been unwound by then, so its destructors ran (its sockets
    /// are closed).
    ///
    /// A server sets this so that one crashed connection doesn't take the
    /// others down. `handler` may spawn fibers but must not panic.
    pub fn set_panic_handler(&self, handler: impl FnMut(PanicPayload) + 'static) {
        *self.core.panic_handler.borrow_mut() = Some(Box::new(handler));
    }

    /// Before the loop sleeps in epoll_wait, it polls without blocking for
    /// up to `duration` (never past the next deadline). Zero, the default,
    /// turns this off.
    ///
    /// A server whose clients answer quickly gets their next requests
    /// without sleeping, and they don't have to wake it up: a wakeup costs
    /// both sides microseconds, more in a VM. Under steady load the thread
    /// then keeps its CPU busy; when idle it still sleeps, after `duration`.
    pub fn set_busy_poll(&self, duration: Duration) {
        self.core.busy_poll.set(duration);
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        let core = &*self.core;
        // Cancel the fibers outside of any borrow: unwinding runs their
        // destructors, which deregister fds and may even spawn. Repeat until
        // nothing is left.
        loop {
            let fibers = core.tasks.borrow_mut().drain();
            if fibers.is_empty() {
                break;
            }
            drop(fibers);
        }
        core.ready.borrow_mut().clear();
        CURRENT.with(|c| c.borrow_mut().take());
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reactor")
            .field("fibers", &self.fiber_count())
            .field("registrations", &self.registration_count())
            .finish()
    }
}

impl Core {
    fn spawn<F: FnOnce() + 'static>(&self, f: F) -> io::Result<()> {
        let stack = self.pool.borrow_mut().get()?;
        let fiber = Fiber::new(stack, f);
        let task = self.tasks.borrow_mut().insert(Task { fiber: Some(fiber), waiting: false, timed_out: false });
        self.ready.borrow_mut().push_back(task);
        Ok(())
    }

    // A method, so the borrow ends before the fiber runs. In
    // `while let Some(t) = self.ready.borrow_mut().pop_front()` it would last
    // for the whole loop body.
    fn pop_ready(&self) -> Option<Token> {
        self.ready.borrow_mut().pop_front()
    }

    fn run_task(&self, task: Token) {
        let Some(mut fiber) = self.tasks.borrow_mut().get_mut(task).and_then(|t| t.fiber.take()) else {
            return; // finished or cancelled meanwhile
        };
        self.running_task.set(Some(task));
        let result = fiber.resume();
        self.running_task.set(None);

        if fiber.state() == State::Finished {
            self.tasks.borrow_mut().remove(task);
            let stack = fiber.into_stack();
            self.pool.borrow_mut().put(stack);
        } else {
            self.tasks.borrow_mut().get_mut(task).expect("running fiber vanished").fiber = Some(fiber);
        }
        if let Err(payload) = result {
            // Taken out while it runs, so it can use the reactor (spawn).
            let handler = self.panic_handler.borrow_mut().take();
            match handler {
                Some(mut handler) => {
                    handler(payload);
                    self.panic_handler.borrow_mut().get_or_insert(handler);
                }
                None => panic::resume_unwind(payload),
            }
        }
    }

    fn busy_time(&self) -> Duration {
        let busy = self.busy.get();
        // Inside `run` (a fiber asking), the time since it last woke counts.
        if self.in_run.get() {
            busy + self.woke.get().elapsed()
        } else {
            busy
        }
    }

    /// Fetches I/O events, waiting until `deadline` at most (`None`: as
    /// long as it takes). Busy-polls first if that is on. The time since
    /// the loop last woke counts as busy; this reads the clock once before
    /// the wait and once after (the busy-poll and the deadline use those).
    fn wait_for_io(&self, events: &mut Events, deadline: Option<Instant>) -> io::Result<()> {
        let mut now = Instant::now();
        self.busy.set(self.busy.get() + now.saturating_duration_since(self.woke.get()));
        let busy_poll = self.busy_poll.get();
        if !busy_poll.is_zero() {
            let until = deadline.map_or(now + busy_poll, |d| d.min(now + busy_poll));
            loop {
                self.poller.wait(events, Some(Duration::ZERO))?;
                now = Instant::now();
                if !events.is_empty() {
                    self.woke.set(now);
                    return Ok(());
                }
                if now >= until {
                    break;
                }
                std::hint::spin_loop();
            }
        }
        let timeout = deadline.map(|d| d.saturating_duration_since(now));
        let result = self.poller.wait(events, timeout);
        self.woke.set(Instant::now());
        result
    }

    fn dispatch(&self, event: Event) {
        let mut io = self.io.borrow_mut();
        let Some(slot) = io.get_mut(event.token) else {
            return; // stale: the fd was deregistered after epoll reported it
        };
        // No waiter: remember it. (A waiter is always still waiting: one
        // woken by its deadline has cleared itself out before epoll is asked
        // again.)
        slot.read_closed |= event.is_read_closed();
        if event.is_readable() && !slot.reader.take().is_some_and(|task| self.wake(task, false)) {
            slot.read_ready = true;
        }
        if event.is_writable() && !slot.writer.take().is_some_and(|task| self.wake(task, false)) {
            slot.write_ready = true;
        }
    }

    fn expire_timers(&self, now: Instant) {
        self.timers.borrow_mut().expire(now, |task| {
            self.wake(task, true);
        });
    }

    /// Moves `task` to `ready` if it is waiting and nothing woke it yet.
    /// Returns whether it did.
    fn wake(&self, task: Token, timed_out: bool) -> bool {
        let mut tasks = self.tasks.borrow_mut();
        let Some(t) = tasks.get_mut(task).filter(|t| t.waiting) else {
            return false;
        };
        t.waiting = false;
        t.timed_out = timed_out;
        self.ready.borrow_mut().push_back(task);
        true
    }

    /// Suspends the running fiber until the fd of `io` is ready for its
    /// direction or `deadline` passes, whichever comes first. Returns `false`
    /// if the deadline passed.
    ///
    /// Returns at once if epoll already reported the readiness and nobody
    /// consumed it yet. A deadline in the past still suspends the fiber until
    /// the next turn of the loop.
    pub(crate) fn wait(&self, io: Option<(Token, Direction)>, deadline: Option<Instant>) -> bool {
        assert!(io.is_some() || deadline.is_some(), "a wait nothing can end");
        let task = self
            .running_task
            .get()
            .expect("waiting for I/O or a timer outside of a fiber spawned on the reactor");
        if let Some((slot, dir)) = io {
            let mut io = self.io.borrow_mut();
            let s = io.get_mut(slot).expect("registration of another reactor");
            let (ready, waiter) = match dir {
                Direction::Read => (&mut s.read_ready, &mut s.reader),
                Direction::Write => (&mut s.write_ready, &mut s.writer),
            };
            if std::mem::take(ready) {
                return true;
            }
            assert!(waiter.is_none(), "two fibers wait for the same fd ({dir:?})");
            *waiter = Some(task);
        }
        let timer = deadline.map(|d| self.timers.borrow_mut().add(d, task));
        {
            let mut tasks = self.tasks.borrow_mut();
            let t = tasks.get_mut(task).expect("running fiber vanished");
            t.waiting = true;
            t.timed_out = false;
        }
        self.waiting.set(self.waiting.get() + 1);
        // Cleans up when the wait ends, also when the fiber is cancelled and
        // unwinds out of suspend().
        let _guard = WaitGuard { core: self, task, io, timer };
        fiber::suspend();
        let timed_out = self.tasks.borrow_mut().get_mut(task).is_some_and(|t| t.timed_out);
        !timed_out
    }

    fn yield_now(&self) {
        let task = self.running_task.get().expect("yield_now() outside of a fiber spawned on the reactor");
        self.ready.borrow_mut().push_back(task);
        fiber::suspend();
    }
}

struct WaitGuard<'a> {
    core: &'a Core,
    task: Token,
    io: Option<(Token, Direction)>,
    timer: Option<Token>,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let core = self.core;
        core.waiting.set(core.waiting.get() - 1);
        // Whatever did not end the wait is still armed: the I/O slot when the
        // deadline passed (or the fiber is cancelled), the timer when the I/O
        // became ready.
        if let Some((slot, dir)) = self.io {
            if let Some(s) = core.io.borrow_mut().get_mut(slot) {
                let waiter = match dir {
                    Direction::Read => &mut s.reader,
                    Direction::Write => &mut s.writer,
                };
                if *waiter == Some(self.task) {
                    *waiter = None;
                }
            }
        }
        if let Some(timer) = self.timer {
            core.timers.borrow_mut().cancel(timer);
        }
    }
}

/// Adds a fiber to the thread's reactor; it starts on the next turn of the
/// loop. Callable from inside fibers.
///
/// # Panics
/// If the thread has no reactor.
pub fn spawn<F: FnOnce() + 'static>(f: F) -> io::Result<()> {
    current().spawn(f)
}

/// Lets the other ready fibers run and the reactor check for I/O, then
/// continues on the next turn of the loop.
///
/// # Panics
/// If not called from a fiber spawned on the reactor.
pub fn yield_now() {
    current().yield_now()
}

/// The time the thread's reactor has spent not waiting: see
/// [`Reactor::busy_time`]. Callable from inside fibers.
///
/// # Panics
/// If the thread has no reactor.
pub fn busy_time() -> Duration {
    current().busy_time()
}

/// Suspends the fiber for at least `duration`. Other fibers run meanwhile.
///
/// `sleep(Duration::ZERO)` still waits for the next turn of the loop, like
/// [`yield_now`]. On Linux the resolution is a millisecond (epoll's timeout).
///
/// # Panics
/// If not called from a fiber spawned on the reactor.
pub fn sleep(duration: Duration) {
    match Instant::now().checked_add(duration) {
        Some(deadline) => sleep_until(deadline),
        None => panic!("sleep({duration:?}) is too long"),
    }
}

/// Suspends the fiber until `deadline`. See [`sleep`].
pub fn sleep_until(deadline: Instant) {
    current().wait(None, Some(deadline));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_event_is_ignored() {
        let reactor = Reactor::new().unwrap();
        let core = &reactor.core;
        let old = core.io.borrow_mut().insert(IoSlot::default());
        core.io.borrow_mut().remove(old);
        let new = core.io.borrow_mut().insert(IoSlot::default());
        assert_eq!(new.index(), old.index());

        // An event still carrying the old token must not touch the new slot.
        core.dispatch(Event::for_test(old, true, true));
        let mut io = core.io.borrow_mut();
        let slot = io.get_mut(new).unwrap();
        assert!(!slot.read_ready && !slot.write_ready);
    }

    #[test]
    fn finished_fibers_give_their_stacks_back() {
        // Each fiber spawns the next one and finishes: at most two stacks are
        // ever in use, and they are reused for all 20k fibers.
        fn chain(left: u32, count: Rc<Cell<u32>>) {
            count.set(count.get() + 1);
            if left > 0 {
                spawn(move || chain(left - 1, count)).unwrap();
            }
        }
        let reactor = Reactor::new().unwrap();
        let count = Rc::new(Cell::new(0));
        let c = count.clone();
        reactor.spawn(move || chain(20_000 - 1, c)).unwrap();
        reactor.run().unwrap();
        assert_eq!(count.get(), 20_000);
        assert_eq!(reactor.core.pool.borrow().free_count(), 2);
    }

    #[test]
    fn event_without_waiter_is_remembered() {
        let reactor = Reactor::new().unwrap();
        let core = &reactor.core;
        let slot = core.io.borrow_mut().insert(IoSlot::default());
        core.dispatch(Event::for_test(slot, true, false));
        let mut io = core.io.borrow_mut();
        let s = io.get_mut(slot).unwrap();
        assert!(s.read_ready && !s.write_ready);
        assert!(core.ready.borrow().is_empty());
    }
}
