//! A fiber that overflows its stack must hit the guard page and die with
//! SIGSEGV, not silently corrupt the memory below its stack.
//!
//! The overflow kills the process, so the test re-runs this test binary as a
//! child (selected by an environment variable) and inspects how it died.

use fiber::stack::page_size;
use fiber::{Fiber, Stack, DEFAULT_STACK_SIZE};
use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

const CHILD_ENV: &str = "FIBER_GUARD_PAGE_CHILD";
const IN_GUARD: &str = "fault inside the guard page";
const OUTSIDE_GUARD: &str = "fault OUTSIDE the guard page";

/// macOS reports PROT_NONE accesses as SIGBUS, Linux as SIGSEGV.
const FAULT_SIGNALS: &[libc::c_int] = &[libc::SIGSEGV, libc::SIGBUS];

static GUARD_LO: AtomicUsize = AtomicUsize::new(0);
static GUARD_HI: AtomicUsize = AtomicUsize::new(0);

#[test]
fn stack_overflow_hits_guard_page() {
    if std::env::var_os(CHILD_ENV).is_some() {
        overflow_in_fiber();
    }

    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "stack_overflow_hits_guard_page", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        stderr.contains(IN_GUARD),
        "child did not fault in the guard page; status {:?}, stderr:\n{stderr}",
        out.status
    );
    let signal = out.status.signal();
    assert!(
        signal.is_some_and(|s| FAULT_SIGNALS.contains(&s)),
        "child must be killed by SIGSEGV/SIGBUS, got {:?}",
        out.status
    );
}

/// Child side: never returns.
fn overflow_in_fiber() -> ! {
    let stack = Stack::new(DEFAULT_STACK_SIZE).unwrap();
    let guard_hi = stack.bottom() as usize;
    GUARD_LO.store(guard_hi - page_size(), Relaxed);
    GUARD_HI.store(guard_hi, Relaxed);
    install_fault_handler();

    let mut f = Fiber::new(stack, || {
        std::hint::black_box(recurse(0));
    });
    let _ = f.resume();
    eprintln!("fiber returned instead of overflowing");
    std::process::exit(1);
}

#[inline(never)]
fn recurse(depth: u64) -> u64 {
    let buf = std::hint::black_box([depth as u8; 512]);
    if depth == u64::MAX {
        return 0; // never; silences the unconditional_recursion lint
    }
    // Not a tail call: buf is used after the recursive call returns.
    recurse(depth + 1) + buf[0] as u64
}

/// Reports whether the faulting address lies in the guard page, then restores
/// the default action and returns: the faulting instruction runs again and
/// the process is killed by the signal.
fn install_fault_handler() {
    unsafe {
        // The fiber's stack is exhausted, so the handler needs its own stack.
        let alt_size = 64 * 1024;
        let alt = libc::stack_t {
            ss_sp: Box::leak(vec![0u8; alt_size].into_boxed_slice()).as_mut_ptr().cast(),
            ss_flags: 0,
            ss_size: alt_size,
        };
        assert_eq!(libc::sigaltstack(&alt, std::ptr::null_mut()), 0);

        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_fault as *const () as usize;
        action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
        for &sig in FAULT_SIGNALS {
            assert_eq!(libc::sigaction(sig, &action, std::ptr::null_mut()), 0);
        }
    }
}

extern "C" fn on_fault(sig: libc::c_int, info: *mut libc::siginfo_t, _ctx: *mut libc::c_void) {
    #[cfg(target_os = "linux")]
    let addr = unsafe { (*info).si_addr() } as usize;
    #[cfg(not(target_os = "linux"))]
    let addr = unsafe { (*info).si_addr } as usize;

    let inside = (GUARD_LO.load(Relaxed)..GUARD_HI.load(Relaxed)).contains(&addr);
    let msg = if inside { IN_GUARD } else { OUTSIDE_GUARD };
    unsafe {
        // Only async-signal-safe calls in here: write(2), signal(2), _exit(2).
        libc::write(2, msg.as_ptr().cast(), msg.len());
        libc::write(2, b"\n".as_ptr().cast(), 1);
        if inside {
            libc::signal(sig, libc::SIG_DFL);
        } else {
            libc::_exit(3);
        }
    }
}
