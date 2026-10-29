use std::fmt;

/// Why a command failed. `Display` gives Redis's exact message (checked
/// against Redis 7.0.15), so the dbnode replies `-{error}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The key holds a value of another type (a list where a string was
    /// expected, ...).
    WrongType,
    /// The value, or an integer argument, isn't an integer in `i64` range.
    NotInteger,
    /// `INCRBY` and friends would leave the `i64` range.
    Overflow,
    /// `DECRBY key -9223372036854775808`: negating the decrement overflows.
    DecrementOverflow,
    /// The expiry is not positive (`SET`) or out of range; carries the
    /// command name, lower case, as Redis prints it (`"set"`, `"pexpire"`).
    InvalidExpireTime(&'static str),
    /// `EXPIRE` with `NX` and one of `XX`, `GT`, `LT`.
    ExpireNxConflict,
    /// `EXPIRE` with both `GT` and `LT`.
    ExpireGtLtConflict,
    /// `APPEND` would make the string longer than 512 MiB.
    StringTooLong,
    /// `HINCRBY` of a field that isn't an integer.
    HashNotInteger,
    /// The value, or the increment, of `INCRBYFLOAT` or `HINCRBYFLOAT`
    /// isn't a float Redis reads.
    NotFloat,
    /// `HINCRBYFLOAT` of a field that isn't a float.
    HashNotFloat,
    /// `HINCRBYFLOAT` by an infinity.
    NanOrInfinity,
    /// `INCRBYFLOAT` and `HINCRBYFLOAT` would make an infinity.
    NotFinite,
    /// `LSET` of a missing key.
    NoSuchKey,
    /// `LSET` past the end of the list.
    IndexOutOfRange,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::WrongType => f.write_str("WRONGTYPE Operation against a key holding the wrong kind of value"),
            Error::NotInteger => f.write_str("ERR value is not an integer or out of range"),
            Error::Overflow => f.write_str("ERR increment or decrement would overflow"),
            Error::DecrementOverflow => f.write_str("ERR decrement would overflow"),
            Error::InvalidExpireTime(cmd) => write!(f, "ERR invalid expire time in '{cmd}' command"),
            Error::ExpireNxConflict => f.write_str("ERR NX and XX, GT or LT options at the same time are not compatible"),
            Error::ExpireGtLtConflict => f.write_str("ERR GT and LT options at the same time are not compatible"),
            Error::StringTooLong => f.write_str("ERR string exceeds maximum allowed size (proto-max-bulk-len)"),
            Error::HashNotInteger => f.write_str("ERR hash value is not an integer"),
            Error::NotFloat => f.write_str("ERR value is not a valid float"),
            Error::HashNotFloat => f.write_str("ERR hash value is not a float"),
            Error::NanOrInfinity => f.write_str("ERR value is NaN or Infinity"),
            Error::NotFinite => f.write_str("ERR increment would produce NaN or Infinity"),
            Error::NoSuchKey => f.write_str("ERR no such key"),
            Error::IndexOutOfRange => f.write_str("ERR index out of range"),
        }
    }
}

impl std::error::Error for Error {}
