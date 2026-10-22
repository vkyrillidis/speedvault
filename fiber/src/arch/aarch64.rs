//! aarch64 (AAPCS64) context switch.
//!
//! A suspended context is represented by a single pointer: its stack pointer.
//! Everything else it needs to resume (callee-saved registers, return
//! address) has been pushed onto its own stack by `switch`.

use core::arch::naked_asm;

/// Bytes `switch` pushes: x19-x30 (12 regs) + d8-d15 (8 regs), 8 bytes each.
pub const FRAME_SIZE: usize = 20 * 8;

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
/// (x0-x18, d0-d7, d16-d31) are assumed clobbered by any call, so the
/// compiler has already saved whatever it needs from them.
///
/// Frame layout (offsets from the saved stack pointer):
///
/// ```text
///   0: x19  x20     48: x25  x26     96: d8   d9     144: d14  d15
///  16: x21  x22     64: x27  x28    112: d10  d11
///  32: x23  x24     80: x29  x30    128: d12  d13
/// ```
///
/// # Safety
/// `from` must be valid for writes and `to` must be a stack pointer produced
/// by a previous `switch` or by `init_stack`, whose stack is still alive.
#[unsafe(naked)]
pub unsafe extern "C" fn switch(from: *mut *mut u8, to: *mut u8) {
    naked_asm!(
        // x0 = from, x1 = to
        // Save the current context on its own stack.
        "sub sp, sp, #160",
        "stp x19, x20, [sp, #0]",
        "stp x21, x22, [sp, #16]",
        "stp x23, x24, [sp, #32]",
        "stp x25, x26, [sp, #48]",
        "stp x27, x28, [sp, #64]",
        "stp x29, x30, [sp, #80]", // x30 (lr) = where this context resumes
        "stp d8,  d9,  [sp, #96]",
        "stp d10, d11, [sp, #112]",
        "stp d12, d13, [sp, #128]",
        "stp d14, d15, [sp, #144]",
        // *from = sp (sp can't be stored directly, go through x2)
        "mov x2, sp",
        "str x2, [x0]",
        // Switch stacks: from here on we are in the other context.
        "mov sp, x1",
        // Restore the other context's registers from its stack.
        "ldp x19, x20, [sp, #0]",
        "ldp x21, x22, [sp, #16]",
        "ldp x23, x24, [sp, #32]",
        "ldp x25, x26, [sp, #48]",
        "ldp x27, x28, [sp, #64]",
        "ldp x29, x30, [sp, #80]",
        "ldp d8,  d9,  [sp, #96]",
        "ldp d10, d11, [sp, #112]",
        "ldp d12, d13, [sp, #128]",
        "ldp d14, d15, [sp, #144]",
        "add sp, sp, #160",
        // Return to the restored x30: where the other context called `switch`,
        // or `trampoline` for a context that never ran.
        "ret",
    )
}

/// Prepares a fresh stack so that the first `switch` to it calls
/// `entry(arg)`. Returns the stack pointer to pass to `switch` as `to`.
///
/// It writes a fake saved frame, as if the new context had called `switch`
/// itself, with x30 pointing to `trampoline` and `entry`/`arg` stashed in the
/// callee-saved x20/x19 slots.
///
/// # Safety
/// `top` must be the 16-byte aligned top of a writable stack with room for
/// at least `FRAME_SIZE` bytes, which outlives the context.
pub unsafe fn init_stack(top: *mut u8, entry: Entry, arg: *mut u8) -> *mut u8 {
    let frame = top.sub(FRAME_SIZE) as *mut usize;
    frame.write_bytes(0, FRAME_SIZE / 8);
    frame.add(0).write(arg as usize); // x19
    frame.add(1).write(entry as usize); // x20
    // frame[10] = x29 = 0: ends the frame-pointer chain for debuggers/backtraces
    frame.add(11).write(trampoline as *const () as usize); // x30
    frame.cast()
}

/// First code a new context runs, "returned into" by `switch`.
/// After the pops, sp is back at `top`, so it is 16-byte aligned for the call.
#[unsafe(naked)]
unsafe extern "C" fn trampoline() -> ! {
    naked_asm!(
        "mov x0, x19", // arg
        "blr x20",     // entry(arg), never returns
        "brk #1",      // trap if it does
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

    /// Fiber side of `callee_saved_registers_survive`: fills every callee-saved
    /// register with garbage and switches back to main.
    extern "C" fn clobber_entry(arg: *mut u8) -> ! {
        let c = arg as *mut Contexts;
        unsafe {
            asm!(
                // x19 and x29 are reserved by LLVM: save/restore them ourselves.
                "stp x19, x29, [sp, #-16]!",
                "mov x19, #0xbad", "mov x20, #0xbad", "mov x21, #0xbad", "mov x22, #0xbad",
                "mov x23, #0xbad", "mov x24, #0xbad", "mov x25, #0xbad", "mov x26, #0xbad",
                "mov x27, #0xbad", "mov x28, #0xbad", "mov x29, #0xbad",
                "fmov d8, #31.0",  "fmov d9, #31.0",  "fmov d10, #31.0", "fmov d11, #31.0",
                "fmov d12, #31.0", "fmov d13, #31.0", "fmov d14, #31.0", "fmov d15, #31.0",
                "bl {switch}",
                "ldp x19, x29, [sp], #16",
                switch = sym switch,
                in("x0") addr_of_mut!((*c).fiber_sp),
                in("x1") (*c).main_sp,
                out("x20") _, out("x21") _, out("x22") _, out("x23") _, out("x24") _,
                out("x25") _, out("x26") _, out("x27") _, out("x28") _,
                out("v8") _, out("v9") _, out("v10") _, out("v11") _,
                out("v12") _, out("v13") _, out("v14") _, out("v15") _,
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
                "stp x19, x29, [sp, #-16]!",
                // Known values in every callee-saved register...
                "mov x19, #19", "mov x20, #20", "mov x21, #21", "mov x22, #22",
                "mov x23, #23", "mov x24, #24", "mov x25, #25", "mov x26, #26",
                "mov x27, #27", "mov x28, #28", "mov x29, #29",
                "fmov d8, #8.0",   "fmov d9, #9.0",   "fmov d10, #10.0", "fmov d11, #11.0",
                "fmov d12, #12.0", "fmov d13, #13.0", "fmov d14, #14.0", "fmov d15, #15.0",
                // ...switch to the fiber, which overwrites them all and switches back...
                "bl {switch}",
                // ...and count how many did not come back intact.
                "mov x9, #0",
                "cmp x19, #19", "cinc x9, x9, ne",
                "cmp x20, #20", "cinc x9, x9, ne",
                "cmp x21, #21", "cinc x9, x9, ne",
                "cmp x22, #22", "cinc x9, x9, ne",
                "cmp x23, #23", "cinc x9, x9, ne",
                "cmp x24, #24", "cinc x9, x9, ne",
                "cmp x25, #25", "cinc x9, x9, ne",
                "cmp x26, #26", "cinc x9, x9, ne",
                "cmp x27, #27", "cinc x9, x9, ne",
                "cmp x28, #28", "cinc x9, x9, ne",
                "cmp x29, #29", "cinc x9, x9, ne",
                "fmov d0, #8.0",  "fcmp d8, d0",  "cinc x9, x9, ne",
                "fmov d0, #9.0",  "fcmp d9, d0",  "cinc x9, x9, ne",
                "fmov d0, #10.0", "fcmp d10, d0", "cinc x9, x9, ne",
                "fmov d0, #11.0", "fcmp d11, d0", "cinc x9, x9, ne",
                "fmov d0, #12.0", "fcmp d12, d0", "cinc x9, x9, ne",
                "fmov d0, #13.0", "fcmp d13, d0", "cinc x9, x9, ne",
                "fmov d0, #14.0", "fcmp d14, d0", "cinc x9, x9, ne",
                "fmov d0, #15.0", "fcmp d15, d0", "cinc x9, x9, ne",
                "ldp x19, x29, [sp], #16",
                switch = sym switch,
                in("x0") addr_of_mut!((*c).main_sp),
                in("x1") (*c).fiber_sp,
                out("x9") mismatches,
                out("x20") _, out("x21") _, out("x22") _, out("x23") _, out("x24") _,
                out("x25") _, out("x26") _, out("x27") _, out("x28") _,
                out("v8") _, out("v9") _, out("v10") _, out("v11") _,
                out("v12") _, out("v13") _, out("v14") _, out("v15") _,
                clobber_abi("C"),
            );
            drop(Box::from_raw(c));
        }
        assert_eq!(mismatches, 0, "callee-saved registers were not restored");
    }
}
