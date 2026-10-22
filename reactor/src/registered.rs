//! An I/O object registered with the thread's reactor.

use crate::reactor::{current, Core, Direction, IoSlot};
use crate::Token;
use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::time::Instant;

/// Owns an I/O object (anything with an fd: `OwnedFd`, `TcpStream`, ...),
/// switched to non-blocking mode and registered with the thread's reactor
/// for its whole lifetime.
///
/// Fibers do I/O with [`read_with`](Self::read_with) and
/// [`write_with`](Self::write_with): try the operation, and if it would block,
/// suspend until epoll reports the fd ready, then try again. To the fiber it
/// looks like blocking I/O.
///
/// One fiber at a time may wait to read and one to write (the same fiber or
/// two different ones).
///
/// Dropping it deregisters the fd first, then drops (closes) the object. It
/// is `!Send`: it belongs to its thread's reactor.
pub struct Registered<T: AsRawFd> {
    core: Rc<Core>,
    token: Token,
    io: T,
    _not_send: PhantomData<*const ()>,
}

impl<T: AsRawFd> Registered<T> {
    /// Makes `io`'s fd non-blocking and registers it with the thread's
    /// reactor.
    ///
    /// # Panics
    /// If the thread has no reactor.
    pub fn new(io: T) -> io::Result<Registered<T>> {
        let core = current();
        let fd = io.as_raw_fd();
        set_nonblocking(fd)?;
        let token = core.io.borrow_mut().insert(IoSlot::default());
        if let Err(e) = core.poller.add(fd, token) {
            core.io.borrow_mut().remove(token);
            return Err(e);
        }
        Ok(Registered { core, token, io, _not_send: PhantomData })
    }

    pub fn get_ref(&self) -> &T {
        &self.io
    }

    /// Runs `op` until it doesn't fail with `WouldBlock`, waiting for the fd
    /// to become readable in between. `op` must be a non-blocking read-type
    /// operation on the fd (read, recv, accept).
    ///
    /// # Panics
    /// If called outside of a fiber spawned on the reactor, or while another
    /// fiber is waiting to read this fd.
    pub fn read_with<R>(&self, op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        self.retry(Direction::Read, None, op)
    }

    /// Like [`read_with`](Self::read_with), for writing (write, send,
    /// connect).
    pub fn write_with<R>(&self, op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        self.retry(Direction::Write, None, op)
    }

    /// Like [`read_with`](Self::read_with), but fails with
    /// [`ErrorKind::TimedOut`](io::ErrorKind::TimedOut) if `op` still would
    /// block when `deadline` passes.
    pub fn read_with_deadline<R>(&self, deadline: Instant, op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        self.retry(Direction::Read, Some(deadline), op)
    }

    /// Writing counterpart of [`read_with_deadline`](Self::read_with_deadline).
    pub fn write_with_deadline<R>(&self, deadline: Instant, op: impl FnMut(&T) -> io::Result<R>) -> io::Result<R> {
        self.retry(Direction::Write, Some(deadline), op)
    }

    pub(crate) fn retry<R>(
        &self,
        dir: Direction,
        deadline: Option<Instant>,
        mut op: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        if matches!(dir, Direction::Read) && self.take_read_drained() && !self.core.wait(Some((self.token, dir)), deadline) {
            return Err(io::ErrorKind::TimedOut.into());
        }
        loop {
            match op(&self.io) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if !self.core.wait(Some((self.token, dir)), deadline) {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                }
                result => return result,
            }
        }
    }

    /// Records that a read just emptied the fd (it returned less than asked
    /// for): the next read waits for epoll to report new data first.
    pub(crate) fn set_read_drained(&self) {
        if let Some(s) = self.core.io.borrow_mut().get_mut(self.token).filter(|s| !s.read_closed) {
            s.read_ready = false;
            s.read_drained = true;
        }
    }

    fn take_read_drained(&self) -> bool {
        self.core.io.borrow_mut().get_mut(self.token).is_some_and(|s| std::mem::take(&mut s.read_drained))
    }

    /// Suspends the fiber until the fd may be readable. May return without
    /// the fd being readable (when an older readiness report was pending), so
    /// always retry the operation and wait again on `WouldBlock`.
    pub fn wait_readable(&self) {
        self.core.wait(Some((self.token, Direction::Read)), None);
    }

    /// Writing counterpart of [`wait_readable`](Self::wait_readable).
    pub fn wait_writable(&self) {
        self.core.wait(Some((self.token, Direction::Write)), None);
    }
}

impl<T: AsRawFd> Drop for Registered<T> {
    fn drop(&mut self) {
        // Before `io` is dropped: once the fd is closed its number can be
        // reused, and the delete could hit someone else's registration.
        let _ = self.core.poller.delete(self.io.as_raw_fd());
        self.core.io.borrow_mut().remove(self.token);
    }
}

impl<T: AsRawFd + std::fmt::Debug> std::fmt::Debug for Registered<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registered").field("io", &self.io).field("token", &self.token).finish()
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::O_NONBLOCK == 0 && libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
