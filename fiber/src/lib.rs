//! Stackful fibers: coroutines with their own stacks, switched in user space.
//!
//! ```
//! use fiber::{suspend, Fiber, State};
//!
//! let mut f = Fiber::with_default_stack(|| {
//!     println!("step 1");
//!     suspend();
//!     println!("step 2");
//! })
//! .unwrap();
//!
//! f.resume().unwrap(); // prints "step 1"
//! assert_eq!(f.state(), State::Suspended);
//! f.resume().unwrap(); // prints "step 2"
//! assert_eq!(f.state(), State::Finished);
//! ```
//!
//! # Rules for code running on a fiber
//!
//! - **`Drop` must never suspend.** Dropping a suspended [`Fiber`] unwinds its
//!   stack to run the destructors of everything on it. A destructor that calls
//!   [`suspend`] during that unwind (e.g. to wait until a socket is writable)
//!   is a panic inside a panic, which aborts the process. Cleanup in `Drop`
//!   must not wait: closing a socket is fine, flushing it and waiting is not.
//! - **Don't swallow panics around `suspend()`.** A `catch_unwind` around code
//!   that suspends may catch the cancellation unwind. The fiber then can't
//!   suspend anymore; it unwinds again on the next `suspend()`.
//! - **A panic ends only its own fiber.** [`Fiber::resume`] returns it as
//!   `Err`; the caller decides whether to log it or to crash. Shared state the
//!   fiber was modifying may be left half-updated.
//! - **Build with `panic = "unwind"`** (the default). The crate doesn't compile
//!   with `panic = "abort"`: cancelling a dropped fiber and catching a
//!   fiber's panic both rely on unwinding.

pub mod arch;
mod fiber;
pub mod pool;
pub mod stack;

pub use fiber::{in_fiber, suspend, Fiber, State, DEFAULT_STACK_SIZE};
pub use pool::StackPool;
pub use stack::Stack;
