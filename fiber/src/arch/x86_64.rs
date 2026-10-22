//! x86_64 (System V ABI) context switch.
//!
//! Same design as `aarch64.rs`: a suspended context is represented by a single
//! pointer, its stack pointer. Everything else it needs to resume
//! (callee-saved registers, return address) is on its own stack.
//!
//! Unlike aarch64, x86 has no link register: `call` pushes the return address
//! onto the stack and `ret` pops it. So the return address is not part of what
//! `switch` pushes, it is already on the stack when `switch` starts.
//!
//! Not saved: the MXCSR and x87 control words. SysV makes their control bits
//! callee-saved, but fibers never change floating point control state, so all
//! contexts share the same values.

use core::arch::naked_asm;

/// Bytes `switch` pushes: rbp, rbx, r12-r15 (6 regs), 8 bytes each.
/// The return address above them is pushed by the caller's `call`.
pub const FRAME_SIZE: usize = 6 * 8;

/// Entry point of a new context. Receives the `arg` given to `init_stack`
/// and must never return (it switches away for the last time instead).
pub type Entry = extern "C" fn(arg: *mut u8) -> !;

/// Suspends the current context and resumes another one.
///
/// Pushes the callee-saved registers onto the current stack, stores the
/// resulting stack pointer into `*from`, switches to the stack pointer `to`,
/// pops the registers saved there and returns into that context.
///
/// From the caller's point of view this is an ordinary C function call that
/// returns once someone switches back to `*from`. The caller-saved registers
/// (rax, rcx, rdx, rsi, rdi, r8-r11, all of xmm0-15) are assumed clobbered by
/// any call, so the compiler has already saved whatever it needs from them.
///
/// Frame layout (offsets from the saved stack pointer):
///
/// ```text
///   0: r15     16: r13     32: rbx     48: return address (pushed by `call`)
///   8: r14     24: r12     40: rbp
/// ```
///
/// # Safety
/// `from` must be valid for writes and `to` must be a stack pointer produced
/// by a previous `switch` or by `init_stack`, whose stack is still alive.
#[unsafe(naked)]
pub unsafe extern "C" fn switch(from: *mut *mut u8, to: *mut u8) {
    naked_asm!(
        // rdi = from, rsi = to
        // Save the current context on its own stack. The return address,
        // where this context resumes, is already there.
        "push rbp",
        "push rbx",
        "push r12",
        "push r13",
        "push r14",
        "push r15",
        // *from = rsp
        "mov [rdi], rsp",
        // Switch stacks: from here on we are in the other context.
        "mov rsp, rsi",
        // Restore the other context's registers from its stack.
        "pop r15",
        "pop r14",
        "pop r13",
        "pop r12",
        "pop rbx",
        "pop rbp",
        // Pop the other context's return address: where it called `switch`,
        // or `trampoline` for a context that never ran.
        "ret",
    )
}

/// Prepares a fresh stack so that the first `switch` to it calls
/// `entry(arg)`. Returns the stack pointer to pass to `switch` as `to`.
///
/// It writes a fake saved frame, as if the new context had called `switch`
/// itself, with the return address pointing to `trampoline` and `arg`/`entry`
/// stashed in the callee-saved rbx/r12 slots:
///
/// ```text
///   top - 8:  trampoline   <- "return address", popped by switch's `ret`
///   top - 16: rbp = 0      <- ends the frame-pointer chain for debuggers
///   top - 24: rbx = arg
///   top - 32: r12 = entry
///   top - 40: r13 = 0
///   top - 48: r14 = 0
///   top - 56: r15 = 0      <- returned stack pointer
/// ```
///
/// # Safety
/// `top` must be the 16-byte aligned top of a writable stack with room for
/// at least `FRAME_SIZE + 8` bytes, which outlives the context.
pub unsafe fn init_stack(top: *mut u8, entry: Entry, arg: *mut u8) -> *mut u8 {
    let frame = top.sub(FRAME_SIZE + 8) as *mut usize;
    frame.write_bytes(0, FRAME_SIZE / 8 + 1);
    frame.add(3).write(entry as usize); // r12
    frame.add(4).write(arg as usize); // rbx
    // frame[5] = rbp = 0
    frame.add(6).write(trampoline as *const () as usize); // return address
    frame.cast()
}

/// First code a new context runs, "returned into" by `switch`.
///
/// After switch's `ret` the stack pointer is exactly `top`, a multiple of 16,
/// which is what the ABI requires right before a `call`. `call` pushes 8 bytes,
/// so `entry` starts with `rsp % 16 == 8`, like any normally called function.
#[unsafe(naked)]
unsafe extern "C" fn trampoline() -> ! {
    naked_asm!(
        "mov rdi, rbx", // arg
        "call r12",     // entry(arg), never returns
        "ud2",          // trap if it does
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Stack;
    use core::arch::asm;
    use core::ptr::{addr_of_mut, null_mut};

    struct Contexts {
        main_sp: *mut u8,
        fiber_sp: *mut u8,
        counter: u32,
    }

    fn new_contexts() -> *mut Contexts {
        Box::into_raw(Box::new(Contexts { main_sp: null_mut(), fiber_sp: null_mut(), counter: 0 }))
    }

    extern "C" fn ping_entry(arg: *mut u8) -> ! {
        let c = arg as *mut Contexts;
        loop {
            unsafe {
                (*c).counter += 1;
                switch(addr_of_mut!((*c).fiber_sp), (*c).main_sp);
            }
        }
    }

    #[test]
    fn ping_pong() {
        let stack = Stack::new(64 * 1024).unwrap();
        let c = new_contexts();
        unsafe {
            (*c).fiber_sp = init_stack(stack.top(), ping_entry, c.cast());
            for i in 1..=3 {
                switch(addr_of_mut!((*c).main_sp), (*c).fiber_sp);
                assert_eq!((*c).counter, i);
            }
            drop(Box::from_raw(c));
        }
    }

    extern "C" fn alignment_entry(arg: *mut u8) -> ! {
        let c = arg as *mut Contexts;
        let rsp: usize;
        unsafe {
            // The compiler guarantees rsp is 16-aligned inside an asm! block
            // (one without `nostack`), assuming this function was entered with
            // rsp % 16 == 8. If trampoline got the alignment wrong, so is this.
            asm!("mov {}, rsp", out(reg) rsp);
            (*c).counter = (rsp % 16) as u32;
            switch(addr_of_mut!((*c).fiber_sp), (*c).main_sp);
        }
        unreachable!("never resumed");
    }

    /// `entry` must start with the same stack alignment as a normally called
    /// function, or SSE spills (`movaps`) to the stack can fault.
    #[test]
    fn entry_stack_is_aligned() {
        let stack = Stack::new(64 * 1024).unwrap();
        let c = new_contexts();
        unsafe {
            (*c).fiber_sp = init_stack(stack.top(), alignment_entry, c.cast());
            (*c).counter = u32::MAX;
            switch(addr_of_mut!((*c).main_sp), (*c).fiber_sp);
            assert_eq!((*c).counter, 0, "rsp misaligned in entry body");
            drop(Box::from_raw(c));
        }
    }

    /// Fiber side of `callee_saved_registers_survive`: fills every callee-saved
    /// register with garbage and switches back to main.
    extern "C" fn clobber_entry(arg: *mut u8) -> ! {
        let c = arg as *mut Contexts;
        unsafe {
            asm!(
                // rbx and rbp are reserved by LLVM: save/restore them ourselves.
                // Two pushes keep rsp 16-byte aligned for the call.
                "push rbx", "push rbp",
                "mov rbx, 0xbad", "mov rbp, 0xbad",
                "mov r12, 0xbad", "mov r13, 0xbad", "mov r14, 0xbad", "mov r15, 0xbad",
                "call {switch}",
                "pop rbp", "pop rbx",
                switch = sym switch,
                in("rdi") addr_of_mut!((*c).fiber_sp),
                in("rsi") (*c).main_sp,
                out("r12") _, out("r13") _, out("r14") _, out("r15") _,
                clobber_abi("C"),
            );
        }
        unreachable!("never resumed");
    }

    #[test]
    fn callee_saved_registers_survive() {
        let stack = Stack::new(64 * 1024).unwrap();
        let c = new_contexts();
        let mismatches: u64;
        unsafe {
            (*c).fiber_sp = init_stack(stack.top(), clobber_entry, c.cast());
            asm!(
                "push rbx", "push rbp",
                // Known values in every callee-saved register...
                "mov rbx, 3", "mov rbp, 5",
                "mov r12, 12", "mov r13, 13", "mov r14, 14", "mov r15, 15",
                // ...switch to the fiber, which overwrites them all and switches back...
                "call {switch}",
                // ...and count how many did not come back intact.
                "xor ecx, ecx",
                "cmp rbx, 3",  "setne al", "add cl, al",
                "cmp rbp, 5",  "setne al", "add cl, al",
                "cmp r12, 12", "setne al", "add cl, al",
                "cmp r13, 13", "setne al", "add cl, al",
                "cmp r14, 14", "setne al", "add cl, al",
                "cmp r15, 15", "setne al", "add cl, al",
                "pop rbp", "pop rbx",
                switch = sym switch,
                in("rdi") addr_of_mut!((*c).main_sp),
                in("rsi") (*c).fiber_sp,
                out("rcx") mismatches,
                out("r12") _, out("r13") _, out("r14") _, out("r15") _,
                clobber_abi("C"),
            );
            drop(Box::from_raw(c));
        }
        assert_eq!(mismatches, 0, "callee-saved registers were not restored");
    }
}
