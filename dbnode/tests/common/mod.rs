//! Shared by the integration tests: a server per test on its own thread,
//! and a blocking client reading exact reply bytes.

#![allow(dead_code)] // each test file uses a different part

use dbnode::Config;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

/// A server on a free port. Its thread runs until the test binary exits.
pub fn start() -> SocketAddr {
    start_with(Config::default())
}

/// A server in cluster mode, announcing 127.0.0.1 and its port.
pub fn start_cluster() -> SocketAddr {
    start_joining(None)
}

/// A server in cluster mode that joins `stuart`.
pub fn start_joining(stuart: Option<String>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cluster = Some(dbnode::Announce { host: "127.0.0.1".into(), port: addr.port() });
    thread::spawn(move || dbnode::serve(listener, None, Config { cluster, stuart, ..Config::default() }).unwrap());
    addr
}

pub fn start_with(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || dbnode::serve(listener, None, config).unwrap());
    addr
}

pub struct Client(pub TcpStream);

impl Client {
    pub fn connect(addr: SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        Client(stream)
    }

    pub fn send(&mut self, bytes: &[u8]) {
        self.0.write_all(bytes).unwrap();
    }

    /// Sends one multibulk request.
    pub fn cmd(&mut self, args: &[&[u8]]) {
        self.send(&request(args));
    }

    /// Reads exactly as many bytes as `expected` has and compares them.
    pub fn expect(&mut self, expected: &[u8]) {
        let mut got = vec![0; expected.len()];
        self.0.read_exact(&mut got).unwrap_or_else(|e| panic!("waiting for {:?}: {e}", expected.escape_ascii().to_string()));
        assert_eq!(got.escape_ascii().to_string(), expected.escape_ascii().to_string());
    }

    /// `cmd` and `expect` in one.
    pub fn call(&mut self, args: &[&[u8]], expected: &[u8]) {
        self.cmd(args);
        self.expect(expected);
    }

    /// The server closes the connection without sending anything more.
    pub fn expect_closed(&mut self) {
        let mut rest = Vec::new();
        self.0.read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "unexpected bytes before the close: {:?}", rest.escape_ascii().to_string());
    }
}

pub fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub const OK: &[u8] = b"+OK\r\n";
pub const NIL: &[u8] = b"$-1\r\n";

impl Client {
    /// One line of reply, `\r\n` included.
    pub fn read_line(&mut self) -> Vec<u8> {
        let mut line = Vec::new();
        let mut byte = [0];
        while !line.ends_with(b"\r\n") {
            self.0.read_exact(&mut byte).unwrap();
            line.push(byte[0]);
        }
        line
    }

    /// Reads an integer reply and returns its digits.
    pub fn read_integer(&mut self) -> String {
        let line = self.read_line();
        assert_eq!(line[0], b':', "not an integer reply: {:?}", line.escape_ascii().to_string());
        String::from_utf8(line[1..line.len() - 2].to_vec()).unwrap()
    }

    /// `CLIENT ID`, as the digits.
    pub fn id(&mut self) -> String {
        self.cmd(&[b"CLIENT", b"ID"]);
        self.read_integer()
    }

    /// Reads one bulk or verbatim string reply and returns its text.
    pub fn read_text(&mut self) -> String {
        let header = self.read_line();
        assert!(header[0] == b'$' || header[0] == b'=', "not a string reply: {:?}", header.escape_ascii().to_string());
        let len: usize = std::str::from_utf8(&header[1..header.len() - 2]).unwrap().parse().unwrap();
        let mut body = vec![0; len + 2];
        self.0.read_exact(&mut body).unwrap();
        body.truncate(len);
        String::from_utf8(body).unwrap()
    }

    /// Reads one bulk string reply and returns its bytes.
    pub fn read_bulk(&mut self) -> Vec<u8> {
        let header = self.read_line();
        assert_eq!(header[0], b'$', "not a bulk reply: {:?}", header.escape_ascii().to_string());
        let len: usize = std::str::from_utf8(&header[1..header.len() - 2]).unwrap().parse().unwrap();
        let mut body = vec![0; len + 2];
        self.0.read_exact(&mut body).unwrap();
        body.truncate(len);
        body
    }
}
