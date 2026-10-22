//! Leaving the cluster on SIGTERM (phase 6): with `--stuart`, the first
//! SIGTERM doesn't stop the node. A thread of its own sends `STUART LEAVE
//! id` once a second; stuart moves the node's slots to the others while it
//! serves on, then removes it, and answers `0`. The node then serves on a
//! little longer (`--leave-linger`), redirecting every command, so that
//! clients that refresh their slot map now and then learn where its slots
//! went, and exits. Without persistence that is the only way a node can
//! stop without losing keys (Kubernetes sends SIGTERM to scale down, evict
//! or update a pod).
//!
//! If stuart can't be reached, the node keeps trying until `--drain-timeout`
//! runs out, then exits with what it still holds (lost). If stuart's name
//! doesn't resolve, or doesn't answer, it tries the address stuart was last
//! seen at (see `join::Stuart::call`). If stuart refuses (the last node with slots: nowhere to
//! move them), it exits at once; so does a node that stuart lets go with
//! slots (every node is leaving: a shutdown).

use crate::{join, notice, warning};
use cluster::NodeId;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const RETRY: Duration = Duration::from_secs(1);
/// How often the progress is logged.
const REPORT: Duration = Duration::from_secs(5);

/// Starts leaving the cluster in the background; `leaving` says so in
/// `INFO cluster`; `owned` is the slots the node's table gives it.
pub(crate) fn spawn(
    stuart: Arc<join::Stuart>,
    id: NodeId,
    timeout: Duration,
    linger: Duration,
    leaving: Arc<AtomicBool>,
    owned: Arc<AtomicUsize>,
) -> io::Result<()> {
    leaving.store(true, Ordering::Relaxed);
    thread::Builder::new().name("leave".into()).spawn(move || leave(&stuart, &id, timeout, linger, &owned)).map(drop)
}

fn leave(stuart: &join::Stuart, id: &NodeId, timeout: Duration, linger: Duration, owned: &AtomicUsize) {
    let deadline = Instant::now() + timeout;
    let mut reported: Option<Instant> = None;
    let mut failures = 0u64;
    loop {
        match stuart.call(&[b"STUART", b"LEAVE", id.as_bytes()]) {
            Ok(reply) if reply == ":0" => {
                let slots = owned.load(Ordering::Relaxed);
                if slots > 0 {
                    notice!("Left the cluster: every node is leaving, stuart let this one go with its {slots} slots; exiting");
                } else if linger.is_zero() {
                    notice!("Left the cluster: stuart moved this node's slots away and removed it; exiting");
                } else {
                    notice!(
                        "Left the cluster: stuart moved this node's slots away and removed it; redirecting clients for {} s, then exiting",
                        linger.as_secs()
                    );
                    thread::sleep(linger);
                    notice!("Exiting");
                }
                std::process::exit(0);
            }
            Ok(reply) if reply.starts_with(':') => {
                failures = 0;
                if reported.is_none_or(|r| r.elapsed() >= REPORT) {
                    let left = reply[1..].parse::<u64>().unwrap_or(1).saturating_sub(1);
                    notice!("Leaving the cluster: {left} slots to move away");
                    reported = Some(Instant::now());
                }
            }
            // The last node with slots: they can't go anywhere.
            Ok(reply) if reply.contains("No other node") => {
                warning!("stuart won't let this node leave ({}); exiting with its keys", &reply[1..]);
                std::process::exit(1);
            }
            Ok(reply) => {
                if failures.is_multiple_of(60) {
                    warning!("STUART LEAVE: {reply:?}; retrying every second");
                }
                failures += 1;
            }
            Err(e) => {
                if failures.is_multiple_of(60) {
                    warning!("Can't reach stuart at {} to leave the cluster ({e}); retrying every second", stuart.addr);
                }
                failures += 1;
            }
        }
        if Instant::now() >= deadline {
            warning!("Still in the cluster after --drain-timeout ({} s); exiting, the keys still here are lost", timeout.as_secs());
            std::process::exit(1);
        }
        thread::sleep(RETRY);
    }
}
