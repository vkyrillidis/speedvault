//! Cost of the slot check's parts. Run with `cargo bench -p cluster`.
//!
//! `key_hash_slot` on 16-byte keys (`key:__rand_int__`, the key
//! redis-benchmark sends without -r, and `key:000000012345`), on a key
//! with a hash tag, and on 64-byte keys; `SlotTable::owner_index`.
//! Reported: ns per call, best of 5.

use cluster::{crc16, key_hash_slot, Node, NodeId, SlotRange, SlotTable};
use std::hint::black_box;
use std::time::Instant;

const CALLS: u64 = 10_000_000;

fn main() {
    println!("{CALLS} calls per run, best of 5\n");
    let keys16: Vec<Vec<u8>> = (0..1024).map(|i| format!("key:{:012}", i * 7919).into_bytes()).collect();
    let tagged: Vec<Vec<u8>> = (0..1024).map(|i| format!("{{user{i}}}:name").into_bytes()).collect();
    let keys64: Vec<Vec<u8>> = (0..1024).map(|i| format!("{:064}", i * 7919).into_bytes()).collect();

    report("crc16, 16 bytes", || run(&keys16, |k| u64::from(crc16(k))));
    report("key_hash_slot, 16 bytes", || run(&keys16, |k| u64::from(key_hash_slot(k))));
    report("key_hash_slot, `key:__rand_int__`", || {
        let k = black_box(b"key:__rand_int__");
        let mut sum = 0u64;
        for _ in 0..CALLS {
            sum = sum.wrapping_add(u64::from(key_hash_slot(black_box(k))));
        }
        sum
    });
    report("key_hash_slot, {tag}:name", || run(&tagged, |k| u64::from(key_hash_slot(k))));
    report("key_hash_slot, 64 bytes", || run(&keys64, |k| u64::from(key_hash_slot(k))));

    let ids: Vec<NodeId> = (1..=3u8).map(|n| NodeId::from_bytes(&[n; 20])).collect();
    let nodes: Vec<Node> = ids.iter().enumerate().map(|(i, id)| Node::new(*id, "10.0.0.1", 7000 + i as u16).unwrap()).collect();
    let table = SlotTable::new(
        1,
        nodes,
        &[(SlotRange::new(0, 5460), Some(ids[0])), (SlotRange::new(5461, 10922), Some(ids[1])), (SlotRange::new(10923, 16383), Some(ids[2]))],
    )
    .unwrap();
    report("owner_index", || {
        let mut sum = 0u64;
        for i in 0..CALLS {
            sum = sum.wrapping_add(table.owner_index(black_box((i % 16384) as u16)).unwrap_or(9) as u64);
        }
        sum
    });
}

fn run(keys: &[Vec<u8>], f: impl Fn(&[u8]) -> u64) -> u64 {
    let mut sum = 0u64;
    for i in 0..CALLS as usize {
        sum = sum.wrapping_add(f(black_box(&keys[i & 1023])));
    }
    sum
}

fn report(name: &str, mut f: impl FnMut() -> u64) {
    let best = (0..5)
        .map(|_| {
            let start = Instant::now();
            black_box(f());
            start.elapsed().as_nanos() as f64
        })
        .fold(f64::INFINITY, f64::min);
    println!("{name:40} {:6.2} ns", best / CALLS as f64);
}
