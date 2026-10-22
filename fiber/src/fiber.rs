//! The safe fiber API: `Fiber::new`, `resume` from outside, `suspend` from
//! inside.
//!
//! ```text
//!            resume()            suspend()
//!   Ready ───────────▶ Running ───────────▶ Suspended
//!                        ▲ │                    │
//!                        │ └────── resume() ◀───┘
//!                        │ closure returns or panics
//!                        ▼
//!                     Finished
//! ```

use crate::arch;
use crate::Stack;
use std::any::Any;
use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::{self, AssertUnwindSafe};
use std::ptr::{self, NonNull};

// A panic must never unwind across a context switch (the frames above a fiber's
// entry belong to no one), so every fiber catches panics at its entry. That
// needs unwinding, and so does cancelling a suspended fiber on drop.
#[cfg(not(panic = "unwind"))]
compile_error!("fiber requires panic = \"unwind\"");

/// Usable stack size we use for connection fibers by default.
pub const DEFAULT_STACK_SIZE: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Created, the closure has not started yet.
    Ready,
    /// Executing: between `resume()` and the next `suspend()`.
    Running,
    /// Paused in `suspend()`, waiting for `resume()`.
    Suspended,
    /// The closure returned or panicked. Cannot be resumed again.
    Finished,
}

/// A stackful coroutine running a closure on its own [`Stack`].
///
/// A fiber runs on the thread that resumes it, only while it is resumed, and
/// pauses itself with [`suspend`]. It is `!Send`: the stack holds references
/// to thread-local state and must never be resumed from another thread.
///
/// Dropping a suspended fiber unwinds its stack (`suspend()` panics with an
/// internal payload, which the fiber's entry catches), so the destructors of
/// everything alive on it run, just as if the closure had returned. Those
/// destructors must not suspend (see the crate docs).
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<fiber::Fiber>();
/// ```
pub struct Fiber {
    // Not a Box: the running fiber holds a pointer to `Inner`, and moving a Box
    // would assert unique access to it.
    inner: NonNull<Inner>,
    _not_send: PhantomData<*mut ()>,
}

/// The part of a fiber shared between the `Fiber` handle (the resumer's side)
/// and the code running on the fiber's stack. Heap allocated, so its address
/// stays fixed while the `Fiber` handle moves. Only one side runs at a time,
/// and all mutable fields are `Cell`s, so no `&mut` is ever needed.
struct Inner {
    stack: Stack,
    /// The fiber's saved stack pointer while it is not running.
    sp: Cell<*mut u8>,
    /// The resumer's saved stack pointer while the fiber runs.
    caller_sp: Cell<*mut u8>,
    state: Cell<State>,
    /// The closure, until the fiber starts it.
    func: Cell<Option<Box<dyn FnOnce()>>>,
    /// Panic payload, handed from the fiber's entry to `resume()`.
    panic: Cell<Option<Box<dyn Any + Send>>>,
    /// Set by `Drop` to make the fiber unwind out of `suspend()`.
    cancelled: Cell<bool>,
}

/// Payload `suspend()` unwinds with when a suspended fiber is dropped.
struct Cancelled;

thread_local! {
    /// The fiber currently running on this thread, null outside any fiber.
    static CURRENT: Cell<*const Inner> = const { Cell::new(ptr::null()) };
}

impl Fiber {
    /// Creates a fiber that will run `f` on `stack`, starting at the first
    /// `resume()`.
    pub fn new<F: FnOnce() + 'static>(stack: Stack, f: F) -> Fiber {
        let inner = NonNull::from(Box::leak(Box::new(Inner {
            stack,
            sp: Cell::new(ptr::null_mut()),
            caller_sp: Cell::new(ptr::null_mut()),
            state: Cell::new(State::Ready),
            func: Cell::new(Some(Box::new(f))),
            panic: Cell::new(None),
            cancelled: Cell::new(false),
        })));
        let i = unsafe { inner.as_ref() };
        i.sp.set(unsafe { arch::init_stack(i.stack.top(), fiber_entry, inner.as_ptr().cast()) });
        Fiber { inner, _not_send: PhantomData }
    }

    /// Creates a fiber with a freshly allocated stack of
    /// [`DEFAULT_STACK_SIZE`] usable bytes.
    pub fn with_default_stack<F: FnOnce() + 'static>(f: F) -> std::io::Result<Fiber> {
        Ok(Fiber::new(Stack::new(DEFAULT_STACK_SIZE)?, f))
    }

    pub fn state(&self) -> State {
        self.inner().state.get()
    }

    /// Runs the fiber until it calls [`suspend`] or finishes.
    ///
    /// Returns `Err(payload)` if the closure panicked; the fiber is then
    /// `Finished` and the panic does not propagate any further, so a bug in one
    /// connection's fiber cannot take down the whole server.
    ///
    /// # Panics
    /// If the fiber is `Finished`.
    pub fn resume(&mut self) -> std::thread::Result<()> {
        let inner = self.inner();
        match inner.state.get() {
            State::Ready | State::Suspended => {}
            // Unreachable without unsafe code: `&mut self` is held while it runs.
            State::Running => panic!("resume() on a running fiber"),
            State::Finished => panic!("resume() on a finished fiber"),
        }
        inner.state.set(State::Running);
        let outer = CURRENT.replace(inner);
        // Returns once the fiber suspends or finishes.
        unsafe { arch::switch(inner.caller_sp.as_ptr(), inner.sp.get()) };
        CURRENT.set(outer);
        match inner.panic.take() {
            Some(payload) => Err(payload),
            None => Ok(()),
        }
    }

    /// Unwinds the fiber if it is suspended, then gives back its stack for
    /// reuse by another fiber (a stack pool).
    pub fn into_stack(self) -> Stack {
        let mut this = ManuallyDrop::new(self);
        this.cancel();
        unsafe { Box::from_raw(this.inner.as_ptr()) }.stack
    }

    /// Makes a suspended fiber unwind its stack and finish.
    fn cancel(&mut self) {
        if self.state() == State::Suspended {
            self.inner().cancelled.set(true);
            // A panic while unwinding is not ours to report: the fiber is being
            // thrown away, like a thread nobody joins.
            let _ = self.resume();
            debug_assert_eq!(self.state(), State::Finished);
        }
    }

    fn inner(&self) -> &Inner {
        unsafe { self.inner.as_ref() }
    }
}

impl Drop for Fiber {
    fn drop(&mut self) {
        self.cancel();
        unsafe { drop(Box::from_raw(self.inner.as_ptr())) };
    }
}

impl std::fmt::Debug for Fiber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fiber").field("state", &self.state()).finish_non_exhaustive()
    }
}

/// Pauses the current fiber and returns to whoever resumed it. Returns when
/// the fiber is resumed again.
///
/// # Panics
/// If called outside a fiber. If the fiber is being dropped, it unwinds out of
/// here instead of returning (don't swallow that panic with `catch_unwind`).
/// So destructors of values on a fiber's stack must not suspend (e.g. wait
/// for I/O): during that unwind it would be a panic inside a panic, which
/// aborts the process.
pub fn suspend() {
    let current = CURRENT.get();
    assert!(!current.is_null(), "suspend() called outside a fiber");
    let inner = unsafe { &*current };
    // Also checked before switching, in case the fiber caught the
    // cancellation unwind and tries to suspend again.
    unwind_if_cancelled(inner);
    inner.state.set(State::Suspended);
    unsafe { arch::switch(inner.sp.as_ptr(), inner.caller_sp.get()) };
    unwind_if_cancelled(inner);
}

/// Whether the calling code runs inside a fiber.
pub fn in_fiber() -> bool {
    !CURRENT.get().is_null()
}

fn unwind_if_cancelled(inner: &Inner) {
    if inner.cancelled.get() {
        // resume_unwind, unlike panic!, does not run the panic hook, so
        // nothing is printed.
        panic::resume_unwind(Box::new(Cancelled));
    }
}

/// The first function a fiber runs, called by `arch::init_stack`'s trampoline.
///
/// It is `extern "C"`, so a panic escaping it would abort the process instead
/// of unwinding into the trampoline. catch_unwind makes sure none does.
extern "C" fn fiber_entry(arg: *mut u8) -> ! {
    let inner = unsafe { &*(arg as *const Inner) };
    let f = inner.func.take().expect("fiber started twice");
    if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(f)) {
        if !payload.is::<Cancelled>() {
            inner.panic.set(Some(payload));
        }
    }
    // Nothing on this stack needs dropping anymore: `f` was consumed and the
    // payload moved. Switch away for good; the stack is freed or reused later.
    inner.state.set(State::Finished);
    unsafe { arch::switch(inner.sp.as_ptr(), inner.caller_sp.get()) };
    // Nobody resumes a finished fiber (`resume` checks), so this is a bug.
    std::process::abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    fn fiber<F: FnOnce() + 'static>(f: F) -> Fiber {
        Fiber::with_default_stack(f).unwrap()
    }

    /// Sets a flag when dropped.
    struct DropFlag(Rc<Cell<bool>>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[test]
    fn runs_to_completion() {
        let ran = Rc::new(Cell::new(false));
        let r = ran.clone();
        let mut f = fiber(move || r.set(true));
        assert_eq!(f.state(), State::Ready);
        assert!(!ran.get(), "fiber must not start before resume()");
        f.resume().unwrap();
        assert!(ran.get());
        assert_eq!(f.state(), State::Finished);
    }

    #[test]
    fn suspend_and_resume_interleave() {
        let log = Rc::new(std::cell::RefCell::new(Vec::new()));
        let l = log.clone();
        let mut f = fiber(move || {
            for i in 0..3 {
                l.borrow_mut().push(format!("fiber {i}"));
                suspend();
            }
        });
        for i in 0..3 {
            f.resume().unwrap();
            assert_eq!(f.state(), State::Suspended);
            log.borrow_mut().push(format!("main {i}"));
        }
        f.resume().unwrap();
        assert_eq!(f.state(), State::Finished);
        assert_eq!(
            *log.borrow(),
            ["fiber 0", "main 0", "fiber 1", "main 1", "fiber 2", "main 2"]
        );
    }

    #[test]
    fn in_fiber_is_true_only_inside_a_fiber() {
        let seen = Rc::new(Cell::new(false));
        let s = seen.clone();
        assert!(!in_fiber());
        let mut f = fiber(move || s.set(in_fiber()));
        f.resume().unwrap();
        assert!(seen.get());
        assert!(!in_fiber());
    }

    #[test]
    fn locals_survive_suspension() {
        let out = Rc::new(Cell::new(0u64));
        let o = out.clone();
        let mut f = fiber(move || {
            // Values on the fiber's own stack, across switches.
            let mut acc = [0u64; 64];
            for (i, slot) in acc.iter_mut().enumerate() {
                *slot = i as u64;
                suspend();
            }
            o.set(acc.iter().sum());
        });
        while f.state() != State::Finished {
            f.resume().unwrap();
        }
        assert_eq!(out.get(), (0..64).sum());
    }

    #[test]
    fn panic_is_returned_from_resume() {
        let mut f = fiber(|| {
            suspend();
            panic!("boom");
        });
        f.resume().unwrap();
        let payload = f.resume().unwrap_err();
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
        assert_eq!(f.state(), State::Finished);
        // The thread is fine and not considered panicking.
        assert!(!std::thread::panicking());
    }

    #[test]
    #[should_panic(expected = "resume() on a finished fiber")]
    fn resume_after_finish_panics() {
        let mut f = fiber(|| {});
        f.resume().unwrap();
        let _ = f.resume();
    }

    #[test]
    #[should_panic(expected = "suspend() called outside a fiber")]
    fn suspend_outside_fiber_panics() {
        suspend();
    }

    #[test]
    fn dropping_ready_fiber_drops_closure_without_running_it() {
        let dropped = Rc::new(Cell::new(false));
        let ran = Rc::new(Cell::new(false));
        let (d, r) = (DropFlag(dropped.clone()), ran.clone());
        let f = fiber(move || {
            let _d = d;
            r.set(true);
        });
        drop(f);
        assert!(dropped.get());
        assert!(!ran.get());
    }

    #[test]
    fn dropping_suspended_fiber_unwinds_its_stack() {
        let dropped = Rc::new(Cell::new(false));
        let after_suspend = Rc::new(Cell::new(false));
        let (d, a) = (dropped.clone(), after_suspend.clone());
        let mut f = fiber(move || {
            let _guard = DropFlag(d); // lives on the fiber's stack
            suspend();
            a.set(true);
        });
        f.resume().unwrap();
        assert!(!dropped.get());
        drop(f);
        assert!(dropped.get(), "destructors on the fiber stack must run");
        assert!(!after_suspend.get(), "code after suspend() must not run");
    }

    #[test]
    fn swallowed_cancellation_cannot_suspend_again() {
        let reached_end = Rc::new(Cell::new(false));
        let r = reached_end.clone();
        let mut f = fiber(move || {
            let _ = panic::catch_unwind(suspend); // swallows the cancel
            let _ = panic::catch_unwind(suspend); // must not switch away
            r.set(true);
        });
        f.resume().unwrap();
        drop(f);
        assert!(reached_end.get());
    }

    #[test]
    fn dropping_suspended_fiber_while_thread_panics() {
        let dropped = Rc::new(Cell::new(false));
        let d = dropped.clone();
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let mut f = fiber(move || {
                let _guard = DropFlag(d);
                suspend();
            });
            f.resume().unwrap();
            panic!("outer"); // `f` is dropped during this unwind
        }));
        assert!(result.is_err());
        assert!(dropped.get());
    }

    #[test]
    fn into_stack_reuses_stack() {
        let mut f = fiber(suspend);
        f.resume().unwrap();
        let mut stack = f.into_stack(); // cancels the suspended fiber
        let top = stack.top();
        let hits = Rc::new(Cell::new(0));
        for _ in 0..3 {
            let h = hits.clone();
            let mut f = Fiber::new(stack, move || {
                h.set(h.get() + 1);
                suspend();
            });
            f.resume().unwrap();
            stack = f.into_stack();
            assert_eq!(stack.top(), top, "same mapping handed back");
        }
        assert_eq!(hits.get(), 3);
    }

    #[test]
    fn nested_fibers() {
        let log = Rc::new(std::cell::RefCell::new(Vec::new()));
        let l = log.clone();
        let mut outer = fiber(move || {
            let l2 = l.clone();
            let mut inner = fiber(move || {
                l2.borrow_mut().push("inner 1");
                suspend(); // back to `outer`, not to main
                l2.borrow_mut().push("inner 2");
            });
            inner.resume().unwrap();
            l.borrow_mut().push("outer 1");
            suspend(); // back to main
            inner.resume().unwrap();
            l.borrow_mut().push("outer 2");
        });
        outer.resume().unwrap();
        log.borrow_mut().push("main");
        outer.resume().unwrap();
        assert_eq!(*log.borrow(), ["inner 1", "outer 1", "main", "inner 2", "outer 2"]);
    }

    #[test]
    fn many_fibers_round_robin() {
        let total = Rc::new(Cell::new(0u32));
        let mut fibers: Vec<Fiber> = (0..100)
            .map(|_| {
                let t = total.clone();
                fiber(move || {
                    for _ in 0..10 {
                        t.set(t.get() + 1);
                        suspend();
                    }
                })
            })
            .collect();
        while fibers.iter().any(|f| f.state() != State::Finished) {
            for f in fibers.iter_mut().filter(|f| f.state() != State::Finished) {
                f.resume().unwrap();
            }
        }
        assert_eq!(total.get(), 1000);
    }
}
