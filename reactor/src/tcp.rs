//! TCP sockets for fibers: accept, connect, read and write suspend the fiber
//! instead of blocking the thread.

use crate::reactor::Direction;
use crate::Registered;
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::mem;
use std::net::{self, Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::{Duration, Instant};

/// A TCP socket listening for connections, registered with the thread's
/// reactor.
///
/// ```
/// use reactor::{Reactor, TcpListener, TcpStream};
/// use std::io::{Read, Write};
///
/// let reactor = Reactor::new().unwrap();
/// let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
/// let addr = listener.local_addr().unwrap();
/// reactor.spawn(move || {
///     let (mut conn, _) = listener.accept().unwrap(); // suspends
///     let mut buf = [0; 5];
///     conn.read_exact(&mut buf).unwrap();
///     conn.write_all(&buf).unwrap();
/// }).unwrap();
/// reactor.spawn(move || {
///     let mut conn = TcpStream::connect(addr).unwrap(); // suspends
///     conn.write_all(b"hello").unwrap();
///     let mut buf = [0; 5];
///     conn.read_exact(&mut buf).unwrap();
///     assert_eq!(&buf, b"hello");
/// }).unwrap();
/// reactor.run().unwrap();
/// ```
#[derive(Debug)]
pub struct TcpListener {
    inner: Registered<net::TcpListener>,
}

impl TcpListener {
    /// Binds to `addr` and starts listening. Port 0 picks a free port; see
    /// [`local_addr`](Self::local_addr).
    ///
    /// Takes a `SocketAddr`, not a host name: resolving a name would block
    /// the whole thread.
    ///
    /// # Panics
    /// If the thread has no reactor.
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        // bind and listen never block.
        TcpListener::from_std(net::TcpListener::bind(addr)?)
    }

    /// Registers a listener created elsewhere (and makes it non-blocking).
    pub fn from_std(listener: net::TcpListener) -> io::Result<TcpListener> {
        Ok(TcpListener { inner: Registered::new(listener)? })
    }

    /// Waits for the next connection.
    ///
    /// # Panics
    /// If called outside of a fiber spawned on the reactor, or while another
    /// fiber is accepting on this listener.
    pub fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = self.inner.read_with(|l| l.accept())?;
        Ok((TcpStream::from_std(stream)?, addr))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().local_addr()
    }

    pub fn get_ref(&self) -> &net::TcpListener {
        self.inner.get_ref()
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.get_ref().as_raw_fd()
    }
}

/// A TCP connection registered with the thread's reactor.
///
/// Reading and writing suspend the fiber until the socket is ready. `Read`
/// and `Write` are also implemented for `&TcpStream`, so one fiber can read
/// while another writes (at most one reader and one writer at a time).
///
/// With [`set_read_timeout`](Self::set_read_timeout) /
/// [`set_write_timeout`](Self::set_write_timeout), a call that waits longer
/// fails with [`ErrorKind::TimedOut`](io::ErrorKind::TimedOut); the stream
/// stays usable.
#[derive(Debug)]
pub struct TcpStream {
    inner: Registered<net::TcpStream>,
    read_timeout: Cell<Option<Duration>>,
    write_timeout: Cell<Option<Duration>>,
}

impl TcpStream {
    /// Connects to `addr`, suspending the fiber until the connection is
    /// established or has failed.
    ///
    /// Takes a `SocketAddr`, not a host name: resolving a name would block
    /// the whole thread.
    ///
    /// # Panics
    /// If called outside of a fiber spawned on the reactor.
    pub fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        TcpStream::connect_until(addr, None)
    }

    /// Like [`connect`](Self::connect), but fails with
    /// [`ErrorKind::TimedOut`](io::ErrorKind::TimedOut) if the connection
    /// isn't established within `timeout` (a host that drops the SYN would
    /// otherwise take the kernel's minutes of retries).
    ///
    /// # Panics
    /// If called outside of a fiber spawned on the reactor.
    pub fn connect_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
        TcpStream::connect_until(addr, Some(Instant::now() + timeout))
    }

    fn connect_until(addr: SocketAddr, deadline: Option<Instant>) -> io::Result<TcpStream> {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = socket(domain)?;

        let (storage, len) = sockaddr(addr);
        let in_progress = match cvt(unsafe { libc::connect(fd.as_raw_fd(), storage.as_ptr(), len) }) {
            Ok(_) => false,
            // EINTR: the connection attempt goes on in the background.
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINPROGRESS | libc::EINTR)) => true,
            Err(e) => return Err(e),
        };
        let stream = TcpStream::from_std(net::TcpStream::from(fd))?;
        if in_progress {
            stream.finish_connect(deadline)?;
        }
        Ok(stream)
    }

    /// Waits until a non-blocking connect has completed (or `deadline`).
    fn finish_connect(&self, deadline: Option<Instant>) -> io::Result<()> {
        // A wait may return early (an older readiness report), so each time
        // check whether the connection is really done.
        self.inner.retry(Direction::Write, deadline, |socket| {
            if let Some(e) = socket.take_error()? {
                return Err(e);
            }
            match socket.peer_addr() {
                Ok(_) => Ok(()),
                Err(e) if e.raw_os_error() == Some(libc::ENOTCONN) => Err(io::ErrorKind::WouldBlock.into()),
                Err(e) => Err(e),
            }
        })
    }

    /// Registers a connected stream created elsewhere (and makes it
    /// non-blocking).
    pub fn from_std(stream: net::TcpStream) -> io::Result<TcpStream> {
        Ok(TcpStream {
            inner: Registered::new(stream)?,
            read_timeout: Cell::new(None),
            write_timeout: Cell::new(None),
        })
    }

    /// Limits how long one `read` may wait for data. `None`: forever (the
    /// default). Like std, a zero duration is an error.
    ///
    /// The limit is per call: `read_exact` makes several and may take longer.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.read_timeout.set(check_timeout(timeout)?);
        Ok(())
    }

    /// Limits how long one `write` may wait for room in the send buffer.
    /// See [`set_read_timeout`](Self::set_read_timeout).
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.write_timeout.set(check_timeout(timeout)?);
        Ok(())
    }

    pub fn read_timeout(&self) -> Option<Duration> {
        self.read_timeout.get()
    }

    pub fn write_timeout(&self) -> Option<Duration> {
        self.write_timeout.get()
    }

    fn read_op<R>(&self, op: impl FnMut(&net::TcpStream) -> io::Result<R>) -> io::Result<R> {
        self.inner.retry(Direction::Read, deadline(self.read_timeout.get()), op)
    }

    fn write_op<R>(&self, op: impl FnMut(&net::TcpStream) -> io::Result<R>) -> io::Result<R> {
        self.inner.retry(Direction::Write, deadline(self.write_timeout.get()), op)
    }

    /// Shuts down reading, writing or both. Never blocks.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.inner.get_ref().shutdown(how)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().local_addr()
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.get_ref().set_nodelay(nodelay)
    }

    pub fn get_ref(&self) -> &net::TcpStream {
        self.inner.get_ref()
    }
}

impl Read for &TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.read_op(|mut s| s.read(&mut *buf))?;
        // A short read took all there was: the next read would only get
        // EAGAIN, so it waits for epoll instead. (0 is the end of the stream.)
        if n > 0 && n < buf.len() {
            self.inner.set_read_drained();
        }
        Ok(n)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.read_op(|mut s| s.read_vectored(bufs))
    }
}

impl Write for &TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_op(|mut s| s.write(buf))
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.write_op(|mut s| s.write_vectored(bufs))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // nothing buffered
    }
}

impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (&*self).read_vectored(bufs)
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (&*self).write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.get_ref().as_raw_fd()
    }
}

/// A new non-blocking, close-on-exec TCP socket.
#[cfg(target_os = "linux")]
fn socket(domain: libc::c_int) -> io::Result<OwnedFd> {
    let fd = cvt(unsafe { libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A new non-blocking, close-on-exec TCP socket. macOS has no flags for
/// that in `socket`, so they are set afterwards. Like std's sockets it also
/// gets SO_NOSIGPIPE: writing to a closed connection fails with EPIPE
/// instead of raising SIGPIPE (macOS has no MSG_NOSIGNAL).
#[cfg(target_os = "macos")]
fn socket(domain: libc::c_int) -> io::Result<OwnedFd> {
    let fd = cvt(unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) })?;
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let raw = fd.as_raw_fd();
    cvt(unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) })?;
    cvt(unsafe { libc::fcntl(raw, libc::F_SETFL, libc::O_NONBLOCK) })?;
    let one: libc::c_int = 1;
    cvt(unsafe {
        libc::setsockopt(
            raw,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &one as *const _ as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    })?;
    Ok(fd)
}

/// Wraps a `sockaddr_storage` so `as_ptr` can hand out a `*const sockaddr`.
struct SockAddr(libc::sockaddr_storage);

impl SockAddr {
    fn as_ptr(&self) -> *const libc::sockaddr {
        &self.0 as *const _ as *const libc::sockaddr
    }
}

/// `addr` in the C layout `connect` expects.
fn sockaddr(addr: SocketAddr) -> (SockAddr, libc::socklen_t) {
    // All zeros is a valid sockaddr_storage, and sets the fields we don't
    // (sin_zero, flow info) to zero.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) },
                ..unsafe { mem::zeroed() }
            };
            unsafe { (&mut storage as *mut _ as *mut libc::sockaddr_in).write(sin) };
            mem::size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: a.flowinfo(),
                sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                sin6_scope_id: a.scope_id(),
                #[cfg(target_os = "macos")]
                sin6_len: 0, // the kernel fills it in from connect's length
            };
            unsafe { (&mut storage as *mut _ as *mut libc::sockaddr_in6).write(sin6) };
            mem::size_of::<libc::sockaddr_in6>()
        }
    };
    (SockAddr(storage), len as libc::socklen_t)
}

fn check_timeout(timeout: Option<Duration>) -> io::Result<Option<Duration>> {
    if timeout == Some(Duration::ZERO) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "cannot set a 0 duration timeout"));
    }
    Ok(timeout)
}

/// When a call with `timeout` must give up. `None` for no timeout, or one too
/// long to represent.
fn deadline(timeout: Option<Duration>) -> Option<Instant> {
    timeout.and_then(|t| Instant::now().checked_add(t))
}

fn cvt(ret: libc::c_int) -> io::Result<libc::c_int> {
    if ret == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sockaddr_v4_layout() {
        let (s, len) = sockaddr("1.2.3.4:5678".parse().unwrap());
        assert_eq!(len as usize, mem::size_of::<libc::sockaddr_in>());
        let sin = unsafe { &*(s.as_ptr() as *const libc::sockaddr_in) };
        assert_eq!(sin.sin_family, libc::AF_INET as libc::sa_family_t);
        assert_eq!(u16::from_be(sin.sin_port), 5678);
        assert_eq!(sin.sin_addr.s_addr.to_ne_bytes(), [1, 2, 3, 4]);
    }

    #[test]
    fn sockaddr_v6_layout() {
        let (s, len) = sockaddr("[::1]:80".parse().unwrap());
        assert_eq!(len as usize, mem::size_of::<libc::sockaddr_in6>());
        let sin6 = unsafe { &*(s.as_ptr() as *const libc::sockaddr_in6) };
        assert_eq!(sin6.sin6_family, libc::AF_INET6 as libc::sa_family_t);
        assert_eq!(u16::from_be(sin6.sin6_port), 80);
        assert_eq!(sin6.sin6_addr.s6_addr, std::net::Ipv6Addr::LOCALHOST.octets());
    }
}
