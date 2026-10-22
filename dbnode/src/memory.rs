//! How much memory the node uses, and its limit (`maxmemory`).
//!
//! [`Counting`] wraps the system allocator and keeps the bytes allocated
//! and not freed yet, as Redis's `zmalloc` does for `used_memory`: the
//! keys, values and indexes, but also the hash table's spare room, the
//! client buffers and a migration batch on its way. Nothing has to report
//! its own size, so nothing can forget to. On Linux the count is what
//! glibc's `malloc_usable_size` says each block holds (a 20-byte key takes
//! 24), so it follows what the allocator really hands out; elsewhere the
//! size asked for. Not counted: the fiber stacks (`mmap`ed by the `fiber`
//! crate) and the allocator's fragmentation, which is why the process's
//! RSS is higher and why `--maxmemory 75%` leaves a quarter of the
//! container's limit.
//!
//! The binary installs it (`#[global_allocator]` in `main.rs`); a process
//! without it (the library in another program, unit tests) counts 0.
//!
//! The count is one relaxed atomic add per allocation and free: a node
//! runs on one thread, but `join`, `leave` and the signal thread allocate
//! too. Nothing else is paid for: the peak is sampled, not tracked per
//! allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

static USED: AtomicUsize = AtomicUsize::new(0);

/// The counting allocator: `#[global_allocator] static A: Counting = Counting;`.
pub struct Counting;

/// What a block of `layout` that `System` returned at `ptr` really holds.
#[cfg(target_os = "linux")]
unsafe fn size_of_block(ptr: *mut u8, _layout: Layout) -> usize {
    libc::malloc_usable_size(ptr.cast())
}

#[cfg(not(target_os = "linux"))]
unsafe fn size_of_block(_ptr: *mut u8, layout: Layout) -> usize {
    layout.size()
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            USED.fetch_add(size_of_block(ptr, layout), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            USED.fetch_add(size_of_block(ptr, layout), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        USED.fetch_sub(size_of_block(ptr, layout), Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old = size_of_block(ptr, layout);
        let new = System.realloc(ptr, layout, new_size);
        if !new.is_null() {
            let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
            let size = size_of_block(new, new_layout);
            if size >= old {
                USED.fetch_add(size - old, Ordering::Relaxed);
            } else {
                USED.fetch_sub(old - size, Ordering::Relaxed);
            }
        }
        new
    }
}

/// Bytes allocated and not freed (`used_memory`): 0 without [`Counting`].
pub fn used() -> usize {
    USED.load(Ordering::Relaxed)
}

/// The process's resident memory (`used_memory_rss`), from
/// `/proc/self/statm`; 0 where that can't be read.
pub fn rss() -> usize {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else { return 0 };
    let pages: usize = statm.split_whitespace().nth(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    pages * page_size()
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(4096)
}

/// A memory size as Redis's `CONFIG SET maxmemory` takes it (`memtoull`):
/// bytes, or a number with `k`/`m`/`g` (powers of 1000) or `kb`/`mb`/`gb`
/// (powers of 1024), any case. Nothing else: no sign, no fraction, no
/// space.
pub fn parse_size(text: &str) -> Option<u64> {
    let digits = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    if digits == 0 {
        return None;
    }
    let unit = match text[digits..].to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    text[..digits].parse::<u64>().ok()?.checked_mul(unit)
}

/// What `--maxmemory` and `CONFIG SET maxmemory` take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// A size ([`parse_size`]); 0: no limit.
    Bytes(u64),
    /// `N%`, 1 to 100: that share of the container's memory limit.
    Share(u8),
}

impl Limit {
    pub fn parse(text: &str) -> Option<Limit> {
        match text.strip_suffix('%') {
            Some(n) => n.parse::<u8>().ok().filter(|n| (1..=100).contains(n)).map(Limit::Share),
            None => parse_size(text).map(Limit::Bytes),
        }
    }

    /// The limit in bytes, a share of the cgroup's limit read now; an error
    /// says why there's none to take a share of.
    pub fn resolve(self) -> Result<u64, String> {
        match self {
            Limit::Bytes(n) => Ok(n),
            Limit::Share(pct) => match cgroup_limit() {
                Some(limit) => Ok(share(limit, pct)),
                None => Err(format!(
                    "--maxmemory {pct}% needs a memory limit on the container (cgroup memory.max), and there is none; give a size instead"
                )),
            },
        }
    }
}

/// `pct`% of `limit`, rounded down, without overflowing.
fn share(limit: u64, pct: u8) -> u64 {
    limit / 100 * u64::from(pct) + limit % 100 * u64::from(pct) / 100
}

/// The memory limit of the process's cgroup (a container's `--memory`, a
/// pod's `limits.memory`): v2's `memory.max`, else v1's
/// `memory.limit_in_bytes`. `None` without a limit.
pub fn cgroup_limit() -> Option<u64> {
    let read = |path: &str| std::fs::read_to_string(path).ok();
    if let Some(dir) = read("/proc/self/cgroup").as_deref().and_then(cgroup_v2_dir) {
        for path in [format!("/sys/fs/cgroup{dir}/memory.max"), "/sys/fs/cgroup/memory.max".to_string()] {
            if let Some(text) = read(&path) {
                return parse_cgroup_limit(&text);
            }
        }
    }
    read("/sys/fs/cgroup/memory/memory.limit_in_bytes").as_deref().and_then(parse_cgroup_limit)
}

/// The v2 cgroup's path in `/proc/self/cgroup` (its `0::/path` line).
fn cgroup_v2_dir(text: &str) -> Option<&str> {
    text.lines().find_map(|line| line.strip_prefix("0::")).map(|dir| dir.trim_end_matches('/'))
}

/// `max` (v2) or v1's "no limit" (a page-rounded `i64::MAX`, or anything
/// as large) are `None`.
fn parse_cgroup_limit(text: &str) -> Option<u64> {
    text.trim().parse::<u64>().ok().filter(|&n| n > 0 && n < 1 << 62)
}

/// Redis's `bytesToHuman`, for the `_human` fields of `INFO memory`:
/// `512B`, `1.50K`, `2.00M`, `1.86G`... in powers of 1024.
pub struct Human(pub u64);

impl fmt::Display for Human {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let n = self.0 as f64;
        const UNITS: [(f64, &str); 6] = [
            (1024.0, "K"),
            (1024.0 * 1024.0, "M"),
            (1024.0 * 1024.0 * 1024.0, "G"),
            (1024.0 * 1024.0 * 1024.0 * 1024.0, "T"),
            (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0, "P"),
            (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0, "E"),
        ];
        if self.0 < 1024 {
            return write!(f, "{}B", self.0);
        }
        let i = UNITS.iter().rposition(|(base, _)| n >= *base).unwrap_or(0);
        write!(f, "{:.2}{}", n / UNITS[i].0, UNITS[i].1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("100"), Some(100));
        assert_eq!(parse_size("100b"), Some(100));
        assert_eq!(parse_size("1k"), Some(1000));
        assert_eq!(parse_size("1KB"), Some(1024));
        assert_eq!(parse_size("2G"), Some(2_000_000_000));
        assert_eq!(parse_size("3mb"), Some(3 << 20));
        assert_eq!(parse_size("1gb"), Some(1 << 30));
        for bad in ["", "-1", "1.5mb", "1x", "mb", " 1", "1 mb", "1tb", "99999999999999999999"] {
            assert_eq!(parse_size(bad), None, "{bad}");
        }
        assert_eq!(parse_size("20000000000gb"), None);
    }

    #[test]
    fn limits() {
        assert_eq!(Limit::parse("75%"), Some(Limit::Share(75)));
        assert_eq!(Limit::parse("100%"), Some(Limit::Share(100)));
        assert_eq!(Limit::parse("2gb"), Some(Limit::Bytes(2 << 30)));
        for bad in ["0%", "101%", "%", "-5%", "7.5%", "x"] {
            assert_eq!(Limit::parse(bad), None, "{bad}");
        }
        assert_eq!(Limit::Bytes(5).resolve(), Ok(5));
        assert_eq!(share(1 << 30, 75), 805_306_368);
        assert_eq!(share(1 << 30, 100), 1 << 30);
        assert_eq!(share(199, 50), 99);
        assert_eq!(share(u64::MAX, 100), u64::MAX);
    }

    #[test]
    fn cgroups() {
        assert_eq!(cgroup_v2_dir("0::/\n"), Some(""));
        assert_eq!(cgroup_v2_dir("12:memory:/x\n0::/kubepods/pod1/abc\n"), Some("/kubepods/pod1/abc"));
        assert_eq!(cgroup_v2_dir("4:memory:/docker/abc\n"), None);
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("536870912\n"), Some(512 << 20));
        assert_eq!(parse_cgroup_limit("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_limit("0\n"), None);
    }

    #[test]
    fn human() {
        assert_eq!(Human(0).to_string(), "0B");
        assert_eq!(Human(1023).to_string(), "1023B");
        assert_eq!(Human(1024).to_string(), "1.00K");
        assert_eq!(Human(988472).to_string(), "965.30K");
        assert_eq!(Human(2_000_000_000).to_string(), "1.86G");
    }
}
