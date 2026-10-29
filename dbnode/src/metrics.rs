//! The node's `/metrics` and `/ready` (`--metrics`, see [`http`]): what
//! `INFO` says, for Prometheus and a Kubernetes HorizontalPodAutoscaler,
//! plus the reactor's busy time.
//!
//! An autoscaler can't go by the container's CPU: busy-polling keeps it
//! near 100% under any load. `speedvault_busy_seconds_total` counts only
//! the time the reactor wasn't waiting, so its rate is the share of its
//! core the node needs.

use crate::http::{self, family, metric, sample, Kind, Site};
use crate::{memory, Node};
use std::sync::atomic::Ordering;

impl Site for Node {
    fn metrics(&self, out: &mut String) {
        let mode = if self.cluster.is_some() { "cluster" } else { "standalone" };
        let id = self.cluster.as_ref().map_or(String::new(), |c| c.myself().to_string());
        family(out, "speedvault_info", Kind::Gauge, "The node: its version, mode and cluster node id.");
        sample(out, "speedvault_info", &[("version", env!("CARGO_PKG_VERSION")), ("mode", mode), ("node_id", &id)], 1);
        metric(out, "speedvault_uptime_seconds", Kind::Gauge, "Seconds since the node started.", self.started.elapsed().as_secs());

        let busy = "Seconds the node's reactor spent working, not waiting for I/O (busy-polling counts as waiting): its rate is the share of a core the node needs.";
        metric(out, "speedvault_busy_seconds_total", Kind::Counter, busy, http::seconds(reactor::busy_time()));
        metric(out, "speedvault_cpu_seconds_total", Kind::Counter, "CPU seconds the process used, user and system (busy-polling included).", http::seconds(http::cpu_time()));

        let (used, max) = (self.used_memory() as u64, self.maxmemory.get());
        metric(out, "speedvault_memory_used_bytes", Kind::Gauge, "Memory allocated (INFO's used_memory).", used);
        metric(out, "speedvault_memory_max_bytes", Kind::Gauge, "maxmemory; 0: no limit.", max);
        let ratio = if max == 0 { 0.0 } else { used as f64 / max as f64 };
        metric(out, "speedvault_memory_used_ratio", Kind::Gauge, "used / maxmemory; 0 without a limit.", ratio);
        metric(out, "speedvault_memory_peak_bytes", Kind::Gauge, "The most memory allocated so far (sampled).", self.peak_memory.get());
        metric(out, "speedvault_memory_rss_bytes", Kind::Gauge, "The process's resident memory.", memory::rss());
        let growth = "The most the next new key may allocate at once, a table of keys growing. Past maxmemory with it, new keys are refused.";
        metric(out, "speedvault_memory_map_growth_bytes", Kind::Gauge, growth, self.store.borrow().growth());

        metric(out, "speedvault_commands_processed_total", Kind::Counter, "Commands run.", self.commands_processed.get());
        let oom = "Commands refused with -OOM: past maxmemory.";
        metric(out, "speedvault_commands_refused_oom_total", Kind::Counter, oom, self.commands_refused_oom.get());
        metric(out, "speedvault_connections_received_total", Kind::Counter, "Client connections accepted.", self.connections_received.get());
        let denied = "AUTH (or HELLO's AUTH option) refused for a wrong password (INFO's acl_access_denied_auth).";
        metric(out, "speedvault_auth_failures_total", Kind::Counter, denied, self.auth_failures.get());
        metric(out, "speedvault_clients", Kind::Gauge, "Clients connected.", self.clients.len());
        {
            let store = self.store.borrow();
            metric(out, "speedvault_keys", Kind::Gauge, "Keys held.", store.dbsize());
            metric(out, "speedvault_keys_expiring", Kind::Gauge, "Keys held with an expiry.", store.expiring_len());
        }

        let Some(c) = &self.cluster else { return };
        let (slots, migrating_out, migrating_in, epoch) = {
            let (table, me) = c.table();
            let slots: usize = table.ranges_by_node().get(me).map_or(0, |r| r.iter().map(|r| r.len()).sum());
            let out = table.migrations().iter().filter(|m| m.from == me).count();
            let into = table.migrations().iter().filter(|m| m.to == me).count();
            (slots, out, into, table.epoch())
        };
        metric(out, "speedvault_cluster_slots", Kind::Gauge, "Slots the node owns (migrating ones included).", slots);
        family(out, "speedvault_cluster_slots_migrating", Kind::Gauge, "Slots moving out of the node, and into it.");
        sample(out, "speedvault_cluster_slots_migrating", &[("direction", "out")], migrating_out);
        sample(out, "speedvault_cluster_slots_migrating", &[("direction", "in")], migrating_in);
        metric(out, "speedvault_cluster_epoch", Kind::Gauge, "The epoch of the node's slot table; 0: none yet.", epoch);
        metric(out, "speedvault_cluster_leaving", Kind::Gauge, "1 once the node is leaving the cluster (SIGTERM).", u8::from(c.leaving.load(Ordering::Relaxed)));
        let s = &c.stats;
        metric(out, "speedvault_migrate_keys_sent_total", Kind::Counter, "Keys of migrating slots the new owners took.", s.keys_sent.get());
        metric(out, "speedvault_migrate_keys_imported_total", Kind::Counter, "Keys taken from other nodes.", s.keys_imported.get());
        let refused = "Times a slot sync's copy here waited for room under maxmemory.";
        metric(out, "speedvault_migrate_imports_refused_total", Kind::Counter, refused, s.imports_refused.get());
        family(out, "speedvault_redirects_total", Kind::Counter, "Redirects sent to clients: MOVED, ASK and TRYAGAIN.");
        sample(out, "speedvault_redirects_total", &[("kind", "moved")], s.moved.get());
        sample(out, "speedvault_redirects_total", &[("kind", "ask")], s.ask.get());
        sample(out, "speedvault_redirects_total", &[("kind", "tryagain")], s.tryagain.get());
    }

    /// Standalone: always. In cluster mode: once stuart knows it (it
    /// joined), or without stuart once it has a table; never while leaving.
    /// A replica also needs its link to its primary up and synced (phase
    /// 8): until then it protects nothing, and a rolling update or an
    /// eviction (the StatefulSet, the PodDisruptionBudget) waits for it
    /// before stopping the next pod, maybe its primary.
    fn ready(&self) -> Result<(), String> {
        let Some(c) = &self.cluster else { return Ok(()) };
        if c.leaving.load(Ordering::Relaxed) {
            return Err("leaving the cluster".into());
        }
        if self.config.stuart.is_some() {
            if !c.joined.load(Ordering::Relaxed) {
                return Err("not joined to stuart yet".into());
            }
        } else if c.table().0.epoch() == 0 {
            return Err("no slot table yet".into());
        }
        let replica = {
            let (table, me) = c.table();
            table.primary_of(me).is_some()
        };
        if replica && self.repl.link.state() != crate::repl::replica::State::Online {
            return Err("a replica not synced with its primary".into());
        }
        Ok(())
    }
}
