//! Thin wrapper over Linux epoll.
//!
//! Sockets are registered once, edge triggered, for reading and writing at the
//! same time. epoll then reports each change of readiness exactly once, and the
//! registration never has to be modified until the socket is closed. In return
//! the reader/writer must keep going until the call fails with `EAGAIN`, or it
//! won't be told again.

use crate::Token;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Whether `epoll_pwait2` (Linux 5.11) works here: its timeout is a
/// timespec, `epoll_wait`'s whole milliseconds, which turns a 50 µs timer
/// into a 1 ms sleep. Cleared the first time the kernel (ENOSYS) or a
/// seccomp filter (EPERM, older Docker profiles) refuses it.
static PWAIT2: AtomicBool = AtomicBool::new(true);

/// Events every registration is made for. EPOLLRDHUP: the peer closed (or
/// shut down) its writing side.
const INTEREST: u32 = (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET) as u32;

const READABLE: u32 = (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32;
const WRITABLE: u32 = (libc::EPOLLOUT | libc::EPOLLHUP | libc::EPOLLERR) as u32;

pub struct Poller {
    fd: OwnedFd,
}

impl Poller {
    pub fn new() -> io::Result<Poller> {
        let fd = cvt(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
        Ok(Poller { fd: unsafe { OwnedFd::from_raw_fd(fd) } })
    }

    /// Registers `fd`; events for it will carry `token`. `fd` must be
    /// non-blocking.
    pub fn add(&self, fd: RawFd, token: Token) -> io::Result<()> {
        let mut event = libc::epoll_event { events: INTEREST, u64: token.to_u64() };
        cvt(unsafe { libc::epoll_ctl(self.fd.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut event) })?;
        Ok(())
    }

    /// Unregisters `fd`. Closing an fd also removes it from epoll (once no
    /// other fd refers to the same socket), but an explicit delete is exact.
    pub fn delete(&self, fd: RawFd) -> io::Result<()> {
        // A null event pointer is fine since Linux 2.6.9.
        cvt(unsafe { libc::epoll_ctl(self.fd.as_raw_fd(), libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) })?;
        Ok(())
    }

    /// Waits until at least one registered fd is ready or `timeout` passes
    /// (`None`: no timeout), and fills `events` with what happened. A signal
    /// interrupting the wait (EINTR) returns with no events.
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        events.len = 0;
        let n = self.pwait2(events, timeout).unwrap_or_else(|| unsafe {
            libc::epoll_wait(
                self.fd.as_raw_fd(),
                events.buf.as_mut_ptr(),
                events.buf.capacity() as libc::c_int,
                timeout_ms(timeout),
            )
        });
        match cvt(n) {
            Ok(n) => events.len = n as usize,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

impl Poller {
    /// `epoll_pwait2` with no signal mask, or `None` if it isn't available.
    fn pwait2(&self, events: &mut Events, timeout: Option<Duration>) -> Option<libc::c_int> {
        if !PWAIT2.load(Ordering::Relaxed) {
            return None;
        }
        let ts = timeout.map(timespec);
        let n = unsafe {
            libc::syscall(
                libc::SYS_epoll_pwait2,
                self.fd.as_raw_fd(),
                events.buf.as_mut_ptr(),
                events.buf.capacity() as libc::c_int,
                ts.as_ref().map_or(std::ptr::null(), |t| t as *const libc::timespec),
                std::ptr::null::<libc::sigset_t>(),
                0usize,
            )
        } as libc::c_int;
        if n == -1 && matches!(io::Error::last_os_error().raw_os_error(), Some(libc::ENOSYS | libc::EPERM)) {
            PWAIT2.store(false, Ordering::Relaxed);
            return None;
        }
        Some(n)
    }
}

fn timespec(t: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: t.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
        tv_nsec: t.subsec_nanos() as libc::c_long,
    }
}

/// For `epoll_wait`: rounds up, so that a timer doesn't wake up early and
/// spin until it is due.
fn timeout_ms(timeout: Option<Duration>) -> libc::c_int {
    match timeout {
        None => -1,
        Some(t) => {
            let ms = t.as_nanos().div_ceil(1_000_000);
            ms.min(libc::c_int::MAX as u128) as libc::c_int
        }
    }
}

fn cvt(ret: libc::c_int) -> io::Result<libc::c_int> {
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

/// Buffer for the events of one `wait`.
pub struct Events {
    buf: Vec<libc::epoll_event>,
    len: usize,
}

impl Events {
    /// At most `capacity` events are returned per `wait`; the rest stay
    /// pending in the kernel for the next one.
    pub fn with_capacity(capacity: usize) -> Events {
        assert!(capacity > 0, "epoll_wait needs room for at least one event");
        Events { buf: Vec::with_capacity(capacity), len: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        // epoll_wait wrote the first `len` entries.
        let filled = unsafe { std::slice::from_raw_parts(self.buf.as_ptr(), self.len) };
        filled.iter().map(|e| {
            // Copy the fields out: on x86_64 epoll_event is packed, so they
            // can't be borrowed.
            let (bits, data) = (e.events, e.u64);
            Event { token: Token::from_u64(data), bits }
        })
    }
}

/// What epoll reported for one registration.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub token: Token,
    bits: u32,
}

impl Event {
    #[cfg(test)]
    pub(crate) fn for_test(token: Token, readable: bool, writable: bool) -> Event {
        let mut bits = 0;
        if readable {
            bits |= libc::EPOLLIN as u32;
        }
        if writable {
            bits |= libc::EPOLLOUT as u32;
        }
        Event { token, bits }
    }

    /// A read won't block: data, end of stream or an error is waiting. Errors
    /// and hangups count as readable so that a waiting reader wakes up and
    /// gets them from `read`.
    pub fn is_readable(&self) -> bool {
        self.bits & READABLE != 0
    }

    /// A write won't block (or will fail with the pending error).
    pub fn is_writable(&self) -> bool {
        self.bits & WRITABLE != 0
    }

    /// The peer closed its side, or an error is pending: reads won't block
    /// from now on, and no later event will say so again.
    pub fn is_read_closed(&self) -> bool {
        self.bits & (libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32 != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_rounds_up_to_whole_milliseconds() {
        assert_eq!(timeout_ms(None), -1);
        assert_eq!(timeout_ms(Some(Duration::ZERO)), 0);
        assert_eq!(timeout_ms(Some(Duration::from_nanos(1))), 1);
        assert_eq!(timeout_ms(Some(Duration::from_micros(1500))), 2);
        assert_eq!(timeout_ms(Some(Duration::from_secs(u64::MAX))), libc::c_int::MAX);
    }

    /// With `epoll_pwait2`, a timeout under a millisecond isn't rounded up
    /// to one (the kernel's timer slack, 50 µs by default, still applies).
    #[test]
    fn short_timeouts_are_kept() {
        let poller = Poller::new().unwrap();
        let mut events = Events::with_capacity(4);
        let mut waits: Vec<Duration> = (0..21)
            .map(|_| {
                let t = std::time::Instant::now();
                poller.wait(&mut events, Some(Duration::from_micros(100))).unwrap();
                t.elapsed()
            })
            .collect();
        if !PWAIT2.load(Ordering::Relaxed) {
            assert!(kernel() < (5, 11), "epoll_pwait2 is off on Linux {:?}", kernel());
            return; // an older kernel: whole milliseconds
        }
        waits.sort();
        assert!(waits[0] >= Duration::from_micros(100), "{waits:?}");
        assert!(waits[10] < Duration::from_micros(600), "the median wait: {waits:?}");
    }

    /// The running kernel's version, as (major, minor).
    fn kernel() -> (u32, u32) {
        let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::uname(&mut uts) }, 0);
        let release = unsafe { std::ffi::CStr::from_ptr(uts.release.as_ptr()) }.to_string_lossy().into_owned();
        let mut parts = release.split(|c: char| !c.is_ascii_digit()).map(|p| p.parse().unwrap_or(0));
        (parts.next().unwrap_or(0), parts.next().unwrap_or(0))
    }
}
