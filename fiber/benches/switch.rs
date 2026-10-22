//! Context switch cost. Run with `cargo bench -p fiber`.
//!
//! A round trip is two switches: into the fiber and back out.

use fiber::{arch, suspend, Fiber, Stack, StackPool, DEFAULT_STACK_SIZE};
use std::hint::black_box;
use std::ptr::{addr_of_mut, null_mut};
use std::time::Instant;

const ROUND_TRIPS: u32 = 10_000_000;

fn main() {
    println!("{ROUND_TRIPS} round trips each, best of 5\n");
    report("arch::switch (raw)", 2, raw_switch);
    report("Fiber::resume + suspend", 2, fiber_resume_suspend);
    println!();
    report_per("Fiber::new + run, reused stack", 100_000, fiber_lifecycle_reused_stack);
    report_per("Fiber::new + run, StackPool", 100_000, fiber_lifecycle_pool);
    report_per("Fiber::new + run, new mmap'd stack", 100_000, fiber_lifecycle_new_stack);
}

fn report(name: &str, switches_per_trip: u32, f: fn(u32)) {
    let ns = best_of_5(|| f(ROUND_TRIPS)) / f64::from(ROUND_TRIPS * switches_per_trip);
    println!("{name:<38} {ns:6.2} ns/switch");
}

fn report_per(name: &str, n: u32, f: fn(u32)) {
    let ns = best_of_5(|| f(n)) / f64::from(n);
    println!("{name:<38} {ns:6.0} ns/fiber");
}

fn best_of_5(mut run: impl FnMut()) -> f64 {
    run(); // warm up
    (0..5)
        .map(|_| {
            let start = Instant::now();
            run();
            start.elapsed().as_nanos() as f64
        })
        .fold(f64::INFINITY, f64::min)
}

struct Contexts {
    main_sp: *mut u8,
    fiber_sp: *mut u8,
}

extern "C" fn raw_entry(arg: *mut u8) -> ! {
    let c = arg as *mut Contexts;
    loop {
        unsafe { arch::switch(addr_of_mut!((*c).fiber_sp), (*c).main_sp) };
    }
}

fn raw_switch(n: u32) {
    let stack = Stack::new(DEFAULT_STACK_SIZE).unwrap();
    let mut c = Contexts { main_sp: null_mut(), fiber_sp: null_mut() };
    let cp = &mut c as *mut Contexts;
    unsafe {
        (*cp).fiber_sp = arch::init_stack(stack.top(), raw_entry, cp.cast());
        for _ in 0..n {
            arch::switch(addr_of_mut!((*cp).main_sp), (*cp).fiber_sp);
        }
    }
}

fn fiber_resume_suspend(n: u32) {
    let mut f = Fiber::with_default_stack(|| loop {
        suspend();
    })
    .unwrap();
    for _ in 0..n {
        black_box(f.resume()).unwrap();
    }
}

fn fiber_lifecycle_reused_stack(n: u32) {
    let mut stack = Stack::new(DEFAULT_STACK_SIZE).unwrap();
    for i in 0..n {
        let mut f = Fiber::new(stack, move || {
            black_box(i);
        });
        f.resume().unwrap();
        stack = f.into_stack();
    }
}

fn fiber_lifecycle_pool(n: u32) {
    let mut pool = StackPool::new(DEFAULT_STACK_SIZE, 16);
    for i in 0..n {
        let mut f = Fiber::new(pool.get().unwrap(), move || {
            black_box(i);
        });
        f.resume().unwrap();
        pool.put(f.into_stack());
    }
}

fn fiber_lifecycle_new_stack(n: u32) {
    for i in 0..n {
        let mut f = Fiber::with_default_stack(move || {
            black_box(i);
        })
        .unwrap();
        f.resume().unwrap();
    }
}
