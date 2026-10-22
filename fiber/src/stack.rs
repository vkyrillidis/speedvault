//! Fiber stacks: an mmap'd region with a guard page at the low end.

use std::io;
use std::ptr::NonNull;

/// A fiber stack.
///
/// ```text
///  high addr ┌──────────────────┐ <- top(), initial stack pointer
///            │  usable stack    │    grows downwards
///            ├──────────────────┤
///            │  guard page      │    PROT_NONE, overflow -> SIGSEGV
///  low addr  └──────────────────┘ <- base
/// ```
pub struct Stack {
    base: NonNull<u8>,
    /// Total mapping size, guard page included.
    len: usize,
}

impl Stack {
    /// Allocates a stack with at least `size` usable bytes, rounded up to
    /// whole pages, plus one guard page.
    pub fn new(size: usize) -> io::Result<Stack> {
        let page = page_size();
        let usable = size.max(1).div_ceil(page) * page;
        let len = usable + page;

        // Reserve the whole region as inaccessible first...
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        // ...then make everything above the guard page read/write.
        // Pages are only backed by physical memory once touched.
        let usable_start = unsafe { (base as *mut u8).add(page) };
        if unsafe { libc::mprotect(usable_start.cast(), usable, libc::PROT_READ | libc::PROT_WRITE) } != 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::munmap(base, len) };
            return Err(err);
        }

        Ok(Stack { base: NonNull::new(base.cast()).unwrap(), len })
    }

    /// Highest address of the stack, where the stack pointer starts.
    /// Page aligned, so it satisfies the 16-byte ABI alignment.
    pub fn top(&self) -> *mut u8 {
        unsafe { self.base.as_ptr().add(self.len) }
    }

    /// Lowest usable address (just above the guard page).
    pub fn bottom(&self) -> *mut u8 {
        unsafe { self.base.as_ptr().add(page_size()) }
    }

    /// Usable size in bytes.
    pub fn size(&self) -> usize {
        self.len - page_size()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

pub fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_up_to_pages_and_is_aligned() {
        let page = page_size();
        let stack = Stack::new(1).unwrap();
        assert_eq!(stack.size(), page);
        assert_eq!(stack.top() as usize % 16, 0);
        assert_eq!(stack.top() as usize - stack.bottom() as usize, stack.size());
    }

    #[test]
    fn usable_region_is_writable() {
        let stack = Stack::new(64 * 1024).unwrap();
        unsafe {
            // first and last usable byte
            stack.bottom().write(0xAB);
            stack.top().sub(1).write(0xCD);
            assert_eq!(stack.bottom().read(), 0xAB);
            assert_eq!(stack.top().sub(1).read(), 0xCD);
        }
    }
}
