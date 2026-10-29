//! The password (`--requirepass`, `--requirepass-file`): Redis's
//! `requirepass`, the one password of its `default` user. There are no
//! other users (no ACLs).
//!
//! With a password, a client must authenticate (`AUTH`, or `HELLO`'s
//! `AUTH` option) before any command but `AUTH`, `HELLO` and `QUIT`: the
//! others get `-NOAUTH`, after the unknown-command and arity checks, and
//! the parser takes only small requests meanwhile (see
//! `resp::Parser::set_unauthenticated`). A client connected while there was
//! no password counts as authenticated, and changing the password
//! (`CONFIG SET requirepass`, standalone only) keeps clients
//! authenticated: both as in Redis 8.10.2.
//!
//! In a cluster the nodes and stuart share the one password: each logs in
//! with it on its links to the others (`AUTH default password` first: a
//! peer without a password takes that too), and stuart's port requires it.
//! The HTTP probes and `/metrics` stay open.
//!
//! These functions are also stuart's: it answers `AUTH` and `HELLO` the same way.

use resp::Encoder;
use std::path::Path;

/// `-NOAUTH`, for a command refused before authentication.
pub const NOAUTH: &[u8] = b"NOAUTH Authentication required.";
pub const WRONGPASS: &[u8] = b"WRONGPASS invalid username-password pair or user is disabled.";
/// `HELLO` without its `AUTH` option, before authentication.
pub const HELLO_NOAUTH: &[u8] = b"NOAUTH HELLO must be called with the client already authenticated, otherwise the \
    HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol \
    version at the same time";

/// The commands a client may send before it has authenticated (Redis's
/// `no-auth` flag), by their lower-case name.
pub fn allowed_before_auth(command: &str) -> bool {
    matches!(command, "auth" | "hello" | "quit")
}

/// Whether `user` and `pass` log in where the password is `password`
/// (`None`: none, Redis's `nopass`, where any password does). The one user
/// is `default`, case-sensitive as Redis's user names are.
pub fn check(password: Option<&[u8]>, user: &[u8], pass: &[u8]) -> bool {
    user == b"default" && password.is_none_or(|p| same(p, pass))
}

/// What an `AUTH` did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Login {
    /// The client is authenticated.
    Ok,
    /// `-WRONGPASS`: what Redis counts as `acl_access_denied_auth`.
    Wrong,
    /// Another error: nothing was tried.
    Error,
}

/// `AUTH [username] password` (its arity, -2, already checked): replies to
/// it. A failed `AUTH` leaves an authenticated client so, as in Redis.
/// `cluster` is for the one other user name Redis knows, its cluster's
/// internal connections.
pub fn auth(args: &[&[u8]], password: Option<&[u8]>, cluster: bool, out: &mut Encoder) -> Login {
    let (user, pass) = match args {
        [_, _] if password.is_none() => {
            out.error(b"ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
            return Login::Error;
        }
        [_, pass] => (&b"default"[..], *pass),
        [_, b"internal connection", _] if cluster => {
            out.error(b"WRONGPASS invalid internal password");
            return Login::Error;
        }
        [_, b"internal connection", _] => {
            out.error(b"ERR Cannot authenticate as an internal connection on non-cluster instances");
            return Login::Error;
        }
        [_, user, pass] => (*user, *pass),
        _ => {
            out.error(b"ERR syntax error");
            return Login::Error;
        }
    };
    if check(password, user, pass) {
        out.ok();
        Login::Ok
    } else {
        out.error(WRONGPASS);
        Login::Wrong
    }
}

/// Compares the password with what a client sent in a time that doesn't
/// depend on where they differ (Redis compares hashes for that), only on
/// the password's length (a shorter one sent is compared as if padded
/// with zeros, and fails for its length).
fn same(password: &[u8], given: &[u8]) -> bool {
    let mut diff = u8::from(password.len() != given.len());
    for (i, &b) in password.iter().enumerate() {
        diff |= b ^ given.get(i).copied().unwrap_or(0);
    }
    std::hint::black_box(diff) == 0
}

/// A password, which `{:?}` doesn't print (a `Config` is `Debug`).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(pub Vec<u8>);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(..)")
    }
}

/// Appends `AUTH default password`, the login a node or stuart sends first
/// on its links to the others.
pub fn login(req: &mut Encoder, password: &[u8]) {
    req.array(3);
    req.bulk(b"AUTH");
    req.bulk(b"default");
    req.bulk(password);
}

/// The password a command line gives, from `--requirepass PASSWORD` or
/// `--requirepass-file PATH` (a Kubernetes Secret mounted as a file): the
/// file's contents without one line end at the end. `Err` if both are
/// given, if the file can't be read, or if the password is empty (Redis
/// takes an empty `requirepass` as none; on a command line that is more
/// likely a mistake, such as an empty Secret).
pub fn password(flag: Option<String>, file: Option<&Path>) -> Result<Option<Secret>, String> {
    let password = match (flag, file) {
        (Some(_), Some(_)) => return Err("--requirepass and --requirepass-file exclude each other".to_string()),
        (None, None) => return Ok(None),
        (Some(p), None) => p.into_bytes(),
        (None, Some(path)) => {
            let mut p = std::fs::read(path).map_err(|e| format!("can't read --requirepass-file {}: {e}", path.display()))?;
            if p.ends_with(b"\n") {
                p.pop();
                if p.ends_with(b"\r") {
                    p.pop();
                }
            }
            p
        }
    };
    if password.is_empty() {
        return Err("the password is empty".to_string());
    }
    Ok(Some(Secret(password)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use resp::Protocol;

    fn reply(args: &[&[u8]], password: Option<&[u8]>, cluster: bool) -> (Login, Vec<u8>) {
        let mut out = Encoder::new(Protocol::Resp2);
        let ok = auth(args, password, cluster, &mut out);
        (ok, out.as_bytes().to_vec())
    }

    #[test]
    fn auth_replies_as_redis() {
        let pw = Some(&b"secret"[..]);
        assert_eq!(reply(&[b"AUTH", b"secret"], pw, false), (Login::Ok, b"+OK\r\n".to_vec()));
        assert_eq!(reply(&[b"AUTH", b"default", b"secret"], pw, false), (Login::Ok, b"+OK\r\n".to_vec()));
        let wrong = [b"-".as_slice(), WRONGPASS, b"\r\n"].concat();
        for args in [&[&b"AUTH"[..], b"Secret"][..], &[b"AUTH", b"secre"], &[b"AUTH", b"secret!"], &[b"AUTH", b""], &[b"AUTH", b"DEFAULT", b"secret"], &[b"AUTH", b"bob", b"secret"]] {
            assert_eq!(reply(args, pw, false), (Login::Wrong, wrong.clone()), "{args:?}");
        }
        assert_eq!(reply(&[b"AUTH", b"a", b"b", b"c"], pw, false), (Login::Error, b"-ERR syntax error\r\n".to_vec()));
        assert_eq!(
            reply(&[b"AUTH", b"internal connection", b"secret"], pw, false).1,
            b"-ERR Cannot authenticate as an internal connection on non-cluster instances\r\n"
        );
        assert_eq!(reply(&[b"AUTH", b"internal connection", b"secret"], None, true).1, b"-WRONGPASS invalid internal password\r\n");
        // Without a password: `default` with any password, as `nopass`.
        assert_eq!(reply(&[b"AUTH", b"default", b"x"], None, false), (Login::Ok, b"+OK\r\n".to_vec()));
        assert_eq!(reply(&[b"AUTH", b"bob", b"x"], None, false), (Login::Wrong, wrong.clone()));
        assert!(reply(&[b"AUTH", b"x"], None, false).1.starts_with(b"-ERR AUTH <password> called without any password configured"));
    }

    #[test]
    fn passwords_compare_whole() {
        assert!(same(b"abc", b"abc"));
        for other in [&b"abd"[..], b"ab", b"abcd", b"", b"ABC", b"\0bc"] {
            assert!(!same(b"abc", other), "{other:?}");
        }
        // Shorter by a zero byte: the length tells.
        assert!(!same(b"a\0", b"a"));
    }

    #[test]
    fn password_sources() {
        let dir = std::env::temp_dir().join(format!("speedvault-auth-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, text: &[u8]| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path
        };
        assert_eq!(password(None, None), Ok(None));
        assert_eq!(password(Some("pw".into()), None), Ok(Some(Secret(b"pw".to_vec()))));
        assert_eq!(password(None, Some(&file("plain", b"s3cret"))), Ok(Some(Secret(b"s3cret".to_vec()))));
        assert_eq!(password(None, Some(&file("lf", b"s3cret\n"))), Ok(Some(Secret(b"s3cret".to_vec()))));
        assert_eq!(password(None, Some(&file("crlf", b"s3cret\r\n"))), Ok(Some(Secret(b"s3cret".to_vec()))));
        assert_eq!(password(None, Some(&file("two", b"s3cret\n\n"))), Ok(Some(Secret(b"s3cret\n".to_vec()))), "only one line end goes");
        assert!(password(None, Some(&file("empty", b"\n"))).is_err());
        assert!(password(Some(String::new()), None).is_err());
        assert!(password(None, Some(&dir.join("missing"))).unwrap_err().contains("can't read"));
        assert!(password(Some("pw".into()), Some(&file("both", b"x"))).is_err());
        assert_eq!(format!("{:?}", Secret(b"pw".to_vec())), "Secret(..)");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
