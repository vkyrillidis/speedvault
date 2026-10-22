//! Cost of the keyspace operations. Run with `cargo bench -p store`.
//!
//! Every operation runs against a store holding 1M keys (`key:0000000` ..),
//! 16-byte keys, 32-byte values, so the map is far bigger than the caches,
//! as on a real node. Keys are visited in a scrambled order so the hardware
//! prefetcher can't follow. Reported: ns per operation, best of 5. Each
//! runs twice: on a plain store, and on one listing its keys by slot
//! (cluster mode).

use std::hint::black_box;
use std::time::Instant;
use store::{SetOptions, Store};

const KEYS: u64 = 1_000_000;
const OPS: u64 = 1_000_000;

fn main() {
    println!("{KEYS} keys present, {OPS} operations per run, best of 5");
    println!("\nstandalone (no slot index)");
    keyspace(Store::new());
    println!("\ncluster mode (keys listed by slot)");
    keyspace(Store::with_slots(cluster::key_hash_slot));
}

fn keyspace(mut s: Store) {
    s.set_now(1_700_000_000_000);
    let value = [b'v'; 32];
    for i in 0..KEYS {
        s.set(&key(i), &value, SetOptions::default()).unwrap();
    }

    report("GET hit", || {
        let mut n = 0;
        for i in 0..OPS {
            n += s.get(&key(scramble(i))).unwrap().map_or(0, |v| v.len());
        }
        black_box(n);
    });
    report("GET miss", || {
        let mut n = 0;
        for i in 0..OPS {
            n += s.get(&key(KEYS + scramble(i))).unwrap().map_or(0, |v| v.len());
        }
        black_box(n);
    });
    report("SET overwrite", || {
        for i in 0..OPS {
            black_box(s.set(&key(scramble(i)), &value, SetOptions::default()).unwrap());
        }
    });
    // New keys: each run adds 1M and deletes them again (untimed), so the
    // map has the same size every run.
    let mut run = 0;
    let mut timed_new = || {
        run += 1;
        let base = KEYS * (run + 1);
        let start = Instant::now();
        for i in 0..OPS {
            black_box(s.set(&key(base + scramble(i)), &value, SetOptions::default()).unwrap());
        }
        let ns = start.elapsed().as_nanos() as f64;
        for i in 0..OPS {
            s.del(&key(base + i));
        }
        ns
    };
    print_best("SET new key", (0..5).map(|_| timed_new()).fold(f64::INFINITY, f64::min));
    for i in 0..KEYS {
        s.set(&key(i), b"12345", SetOptions::default()).unwrap();
    }
    report("INCR", || {
        for i in 0..OPS {
            black_box(s.incr_by(&key(scramble(i)), 1).unwrap());
        }
    });
    // DEL and SET of the same key, so the map keeps its size.
    report("DEL + SET new", || {
        for i in 0..OPS {
            let k = key(scramble(i));
            black_box(s.del(&k));
            black_box(s.set(&k, &value, SetOptions::default()).unwrap());
        }
    });
    report("key formatting only (overhead)", || {
        for i in 0..OPS {
            black_box(key(scramble(i)));
        }
    });
}

/// "key:" and 12 digits: 16 bytes.
fn key(i: u64) -> [u8; 16] {
    let mut k = *b"key:000000000000";
    let mut v = i;
    for d in k[4..].iter_mut().rev() {
        *d = b'0' + (v % 10) as u8;
        v /= 10;
    }
    k
}

/// A permutation of 0..KEYS (multiplying by an odd number modulo a power of
/// two would do too, but KEYS isn't one): 7919 is prime, coprime with 10^6.
fn scramble(i: u64) -> u64 {
    i * 7919 % KEYS
}

fn report(name: &str, mut bench: impl FnMut()) {
    let best = (0..5)
        .map(|_| {
            let start = Instant::now();
            bench();
            start.elapsed().as_nanos() as f64
        })
        .fold(f64::INFINITY, f64::min);
    print_best(name, best);
}

fn print_best(name: &str, ns: f64) {
    println!("{name:32} {:7.1} ns/op", ns / OPS as f64);
}
