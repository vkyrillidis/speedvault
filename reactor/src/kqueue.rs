//! Thin wrapper over kqueue (macOS), with the same interface as the epoll
//! wrapper.
//!
//! Every fd gets two filters, EVFILT_READ and EVFILT_WRITE, both with
//! EV_CLEAR: like epoll's EPOLLET, a change of readiness is reported once, and
//! the registration never has to be modified until the fd is closed. The
//! reader/writer must keep going until the call fails with `EAGAIN`, or it
//! won't be told again.
//!
//! Unlike epoll, kqueue reports each filter as its own event: one `Event` is
//! either readable or writable, never both.

use crate::Token;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::time::Duration;

pub struct Poller {
    fd: OwnedFd,
}

impl Poller {
    pub fn new() -> io::Result<Poller> {
        let fd = cvt(unsafe { libc::kqueue() })?;
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // kqueue has no CLOEXEC flag of its own.
        cvt(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) })?;
        Ok(Poller { fd })
    }

    /// Registers `fd`; events for it will carry `token`. `fd` must be
    /// non-blocking.
    pub fn add(&self, fd: RawFd, token: Token) -> io::Result<()> {
        let flags = libc::EV_ADD | libc::EV_CLEAR | libc::EV_RECEIPT;
        let changes = [change(fd, libc::EVFILT_READ, flags, token), change(fd, libc::EVFILT_WRITE, flags, token)];
        if let Err(e) = self.apply(&changes) {
            // One of the two may have been added.
            let _ = self.delete(fd);
            return Err(e);
        }
        Ok(())
    }

    /// Unregisters `fd`. Closing an fd also removes its filters, but an
    /// explicit delete is exact.
    pub fn delete(&self, fd: RawFd) -> io::Result<()> {
        let flags = libc::EV_DELETE | libc::EV_RECEIPT;
        let changes = [
            change(fd, libc::EVFILT_READ, flags, Token::new(0, 0)),
            change(fd, libc::EVFILT_WRITE, flags, Token::new(0, 0)),
        ];
        self.apply(&changes)
    }

    /// Applies `changes` and returns the first error. With EV_RECEIPT every
    /// change comes back as an EV_ERROR entry (`data` 0 on success), so all
    /// of them are attempted and none is mistaken for a pending event.
    fn apply(&self, changes: &[libc::kevent; 2]) -> io::Result<()> {
        let mut receipts: [libc::kevent; 2] = unsafe { std::mem::zeroed() };
        let n = cvt(unsafe {
            libc::kevent(
                self.fd.as_raw_fd(),
                changes.as_ptr(),
                changes.len() as libc::c_int,
                receipts.as_mut_ptr(),
                receipts.len() as libc::c_int,
                ptr::null(),
            )
        })?;
        for r in &receipts[..n as usize] {
            // ENOENT: a delete of a filter that wasn't there.
            if r.flags & libc::EV_ERROR != 0 && r.data != 0 && r.data != libc::ENOENT as isize {
                return Err(io::Error::from_raw_os_error(r.data as i32));
            }
        }
        Ok(())
    }

    /// Waits until at least one registered fd is ready or `timeout` passes
    /// (`None`: no timeout), and fills `events` with what happened. A signal
    /// interrupting the wait (EINTR) returns with no events.
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        events.len = 0;
        let ts = timeout.map(timespec);
        let n = unsafe {
            libc::kevent(
                self.fd.as_raw_fd(),
                ptr::null(),
                0,
                events.buf.as_mut_ptr(),
                events.buf.capacity() as libc::c_int,
                ts.as_ref().map_or(ptr::null(), |t| t as *const _),
            )
        };
        match cvt(n) {
            Ok(n) => events.len = n as usize,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

fn change(fd: RawFd, filter: i16, flags: u16, token: Token) -> libc::kevent {
    libc::kevent {
        ident: fd as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: token.to_u64() as usize as *mut libc::c_void,
    }
}

/// kevent takes nanoseconds, so no rounding is needed. Durations too long for
/// a `time_t` are capped (centuries).
fn timespec(t: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: t.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
        tv_nsec: t.subsec_nanos() as libc::c_long,
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
    buf: Vec<libc::kevent>,
    len: usize,
}

impl Events {
    /// At most `capacity` events are returned per `wait`; the rest stay
    /// pending in the kernel for the next one.
    pub fn with_capacity(capacity: usize) -> Events {
        assert!(capacity > 0, "kevent needs room for at least one event");
        Events { buf: Vec::with_capacity(capacity), len: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        // kevent wrote the first `len` entries.
        let filled = unsafe { std::slice::from_raw_parts(self.buf.as_ptr(), self.len) };
        filled.iter().map(|e| Event {
            token: Token::from_u64(e.udata as usize as u64),
            // EOF and errors arrive on the filter they concern (EV_EOF set),
            // so a waiting reader or writer wakes up and gets them from the
            // call itself.
            readable: e.filter == libc::EVFILT_READ,
            writable: e.filter == libc::EVFILT_WRITE,
            read_closed: e.filter == libc::EVFILT_READ && e.flags & (libc::EV_EOF | libc::EV_ERROR) != 0,
        })
    }
}

/// What kqueue reported for one registration.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub token: Token,
    readable: bool,
    writable: bool,
    read_closed: bool,
}

impl Event {
    #[cfg(test)]
    pub(crate) fn for_test(token: Token, readable: bool, writable: bool) -> Event {
        Event { token, readable, writable, read_closed: false }
    }

    /// A read won't block: data, end of stream or an error is waiting.
    pub fn is_readable(&self) -> bool {
        self.readable
    }

    /// A write won't block (or will fail with the pending error).
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// The peer closed its side, or an error is pending: reads won't block
    /// from now on, and no later event will say so again.
    pub fn is_read_closed(&self) -> bool {
        self.read_closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timespec_keeps_nanoseconds_and_caps_huge_durations() {
        let t = timespec(Duration::from_nanos(1_500_000_001));
        assert_eq!((t.tv_sec, t.tv_nsec), (1, 500_000_001));
        assert_eq!(timespec(Duration::from_secs(u64::MAX)).tv_sec, libc::time_t::MAX);
    }

    #[test]
    fn token_survives_udata() {
        let token = Token::new(u32::MAX, u32::MAX);
        let event = change(0, libc::EVFILT_READ, 0, token);
        assert_eq!(Token::from_u64(event.udata as usize as u64), token);
    }
}
