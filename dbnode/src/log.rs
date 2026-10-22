//! Log lines on stderr: `2026-10-16T20:01:02.123Z NOTICE message`, UTC.
//!
//! The levels are Redis's `loglevel`s: `warning` (something went wrong),
//! `notice` (start, stop; the default) and `verbose` (every connection
//! opened and closed). Logging is process-wide, set once at startup.
//! Lines are written with one `write` each, so they don't interleave.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Warning = 0,
    Notice = 1,
    Verbose = 2,
}

impl Level {
    /// Redis's names: `warning`, `notice`, `verbose`.
    pub fn parse(name: &str) -> Option<Level> {
        match name {
            "warning" => Some(Level::Warning),
            "notice" => Some(Level::Notice),
            "verbose" => Some(Level::Verbose),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Level::Warning => "WARNING",
            Level::Notice => "NOTICE",
            Level::Verbose => "VERBOSE",
        }
    }
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Notice as u8);

/// Lines above `level` are dropped from now on.
pub fn set_level(level: Level) {
    LEVEL.store(level as u8, Ordering::Relaxed);
}

pub fn enabled(level: Level) -> bool {
    level as u8 <= LEVEL.load(Ordering::Relaxed)
}

/// Writes one line if `level` is enabled. Use the macros.
pub fn write(level: Level, args: fmt::Arguments<'_>) {
    if !enabled(level) {
        return;
    }
    let mut line = String::with_capacity(96);
    timestamp(SystemTime::now(), &mut line);
    let _ = fmt::write(&mut line, format_args!(" {} {args}\n", level.label()));
    // Nowhere to report a failed log write.
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => { $crate::log::write($crate::log::Level::Warning, format_args!($($arg)*)) };
}

#[macro_export]
macro_rules! notice {
    ($($arg:tt)*) => { $crate::log::write($crate::log::Level::Notice, format_args!($($arg)*)) };
}

/// Formats its arguments only when verbose logging is on: it runs per
/// connection.
#[macro_export]
macro_rules! verbose {
    ($($arg:tt)*) => {
        if $crate::log::enabled($crate::log::Level::Verbose) {
            $crate::log::write($crate::log::Level::Verbose, format_args!($($arg)*))
        }
    };
}

/// `2026-10-16T20:01:02.123Z`. Before 1970 only with a broken clock; that
/// prints as 1970.
fn timestamp(now: SystemTime, out: &mut String) {
    let since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let rem = secs % 86_400;
    let _ = fmt::write(
        out,
        format_args!(
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
            rem / 3600,
            rem / 60 % 60,
            rem % 60,
            since.subsec_millis()
        ),
    );
}

/// Days since 1970-01-01 to (year, month, day) in the proleptic Gregorian
/// calendar: Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11], March first
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64, millis: u64) -> String {
        let mut s = String::new();
        timestamp(UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis), &mut s);
        s
    }

    #[test]
    fn timestamps() {
        assert_eq!(at(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(at(951_782_400, 5), "2000-02-29T00:00:00.005Z"); // leap day of a /400 year
        assert_eq!(at(1_792_183_234, 390), "2026-10-16T20:40:34.390Z");
        assert_eq!(at(4_107_542_399, 999), "2100-02-28T23:59:59.999Z"); // 2100 isn't a leap year
        assert_eq!(at(4_107_542_400, 0), "2100-03-01T00:00:00.000Z");
    }

    #[test]
    fn levels() {
        assert_eq!(Level::parse("notice"), Some(Level::Notice));
        assert_eq!(Level::parse("debug"), None);
        assert!(Level::Warning < Level::Verbose);
    }
}
