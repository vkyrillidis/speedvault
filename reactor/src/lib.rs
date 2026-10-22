//! Single-threaded event loop running fibers that wait for I/O.
//!
//! One reactor per thread. Every fd is registered with epoll once, edge
//! triggered, for both reading and writing; waiting costs no syscall. epoll
//! hands back a [`Token`] (slot index + generation) instead of a pointer, so
//! events for an fd that was deregistered meanwhile are recognised and dropped.
//!
//! Fibers can also wait for a deadline: [`sleep`], [`Registered::read_with_deadline`],
//! [`TcpStream::set_read_timeout`]. The deadlines live in a min-heap; the loop
//! hands the earliest one to `epoll_pwait2` as its timeout (to `epoll_wait`,
//! in whole milliseconds, on kernels older than 5.11).
//!
//! See [`Reactor`] for an example. Runs on Linux (epoll) and macOS (kqueue,
//! with EV_CLEAR for the edge-triggered behaviour).

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("reactor: unsupported OS (only Linux with epoll and macOS with kqueue)");

#[cfg(target_os = "linux")]
mod epoll;
#[cfg(target_os = "linux")]
use epoll as poller;
#[cfg(target_os = "macos")]
mod kqueue;
#[cfg(target_os = "macos")]
use kqueue as poller;
#[cfg(test)]
mod poller_tests;

mod reactor;
mod registered;
mod slab;
mod tcp;
mod timer;
pub mod token;

pub use reactor::{busy_time, sleep, sleep_until, spawn, yield_now, PanicPayload, Reactor};
pub use registered::Registered;
pub use tcp::{TcpListener, TcpStream};
pub use token::Token;
