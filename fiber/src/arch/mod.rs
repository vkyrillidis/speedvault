//! Architecture specific context switching.
//!
//! Each architecture provides the same interface: `FRAME_SIZE`, `Entry`,
//! `switch(from, to)` and `init_stack(top, entry, arg)`. Only the Unix calling
//! conventions are implemented (AAPCS64, System V x86_64); Windows uses a
//! different ABI with more callee-saved registers and stack bounds in the TIB.

#[cfg(all(target_arch = "aarch64", unix))]
mod aarch64;
#[cfg(all(target_arch = "aarch64", unix))]
pub use aarch64::*;

#[cfg(all(target_arch = "x86_64", unix))]
mod x86_64;
#[cfg(all(target_arch = "x86_64", unix))]
pub use x86_64::*;

#[cfg(not(all(unix, any(target_arch = "aarch64", target_arch = "x86_64"))))]
compile_error!("fiber: unsupported target (only aarch64 and x86_64 Unix so far)");
