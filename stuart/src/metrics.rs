//! stuart's `/metrics` and `/ready` (`--metrics`, see `dbnode::http`):
//! the cluster as stuart sees it. The nodes' memory is what the keepers
//! last read; nothing is asked for a scrape.

use crate::{Health, Stuart};
use cluster::SLOTS;
use dbnode::http::{family, metric, sample, Kind, Site};

impl Site for Stuart {
    fn metrics(&self, out: &mut String) {
        family(out, "stuart_info", Kind::Gauge, "stuart: its version, and whether it plans the moves itself (--auto).");
        sample(out, "stuart_info", &[("version", env!("CARGO_PKG_VERSION")), ("auto", if self.auto { "1" } else { "0" })], 1);
        metric(out, "stuart_uptime_seconds", Kind::Gauge, "Seconds since stuart started.", self.started.elapsed().as_secs());

        let state = self.state.borrow();
        let health = self.health.borrow();
        let table = state.table();
        let ranges = table.map(|t| t.ranges_by_node()).unwrap_or_default();
        let (mut up, mut down, mut full) = (0, 0, 0);
        for node in state.nodes() {
            let h = health.get(node.id());
            if h.is_some_and(|h| h.up) {
                up += 1;
            } else {
                down += 1;
            }
            full += usize::from(h.and_then(|h| h.memory).is_some_and(|m| m.full()));
        }
        let failed = table.map_or(0, |t| t.failed().len());
        family(out, "stuart_nodes", Kind::Gauge, "Joined nodes: up and down (not answering), and of those leaving, being made a replica (demoting), full (90% of maxmemory) and marked failed.");
        sample(out, "stuart_nodes", &[("state", "up")], up);
        sample(out, "stuart_nodes", &[("state", "down")], down);
        sample(out, "stuart_nodes", &[("state", "leaving")], state.leaving().len());
        sample(out, "stuart_nodes", &[("state", "demoting")], usize::from(state.demoting().is_some()));
        sample(out, "stuart_nodes", &[("state", "full")], full);
        sample(out, "stuart_nodes", &[("state", "failed")], failed);
        metric(out, "stuart_table_epoch", Kind::Gauge, "The slot table's epoch; 0: no table yet.", table.map_or(0, |t| t.epoch()));
        metric(out, "stuart_slots_moving", Kind::Gauge, "Slots moving between nodes.", table.map_or(0, |t| t.migrations().len()));
        metric(out, "stuart_slots_queued", Kind::Gauge, "Slots waiting to move.", state.queue().len());
        let unassigned = table.map_or(SLOTS, |t| SLOTS - t.assigned());
        metric(out, "stuart_slots_unassigned", Kind::Gauge, "Slots no node owns (no table yet: all).", unassigned);
        metric(out, "stuart_slots_moved_total", Kind::Counter, "Slots moved since stuart started.", self.slots_moved.get());
        metric(out, "stuart_failovers_total", Kind::Counter, "Primaries failed over to a replica since stuart started.", self.failovers.get());
        metric(out, "stuart_handovers_total", Kind::Counter, "Leaving primaries' slots handed to a replica since stuart started.", self.handovers.get());
        metric(out, "stuart_demotions_total", Kind::Counter, "Primaries made replicas, the nodes needing fewer primaries, since stuart started.", self.demotions.get());

        // Per node: the labels tell them apart in dashboards.
        let nodes: Vec<PerNode> = state
            .nodes()
            .iter()
            .map(|n| PerNode {
                id: n.id().to_string(),
                addr: format!("{}:{}", n.host(), n.port()),
                slots: table.and_then(|t| t.position(n.id())).and_then(|i| ranges.get(i)).map_or(0, |r| r.iter().map(|r| r.len()).sum()),
                leaving: state.leaving().contains(n.id()),
                health: health.get(n.id()),
            })
            .collect();
        let each = |out: &mut String, name: &str, help: &str, value: &dyn Fn(&PerNode) -> Option<u64>| {
            family(out, name, Kind::Gauge, help);
            for n in &nodes {
                if let Some(v) = value(n) {
                    sample(out, name, &[("node", &n.id), ("addr", &n.addr)], v);
                }
            }
        };
        each(out, "stuart_node_up", "1 if the node answers stuart.", &|n| Some(u64::from(n.health.is_some_and(|h| h.up))));
        each(out, "stuart_node_slots", "Slots the node owns.", &|n| Some(n.slots as u64));
        each(out, "stuart_node_leaving", "1 if the node is leaving the cluster.", &|n| Some(u64::from(n.leaving)));
        let used = "The node's used memory, as it last said (none before it said).";
        each(out, "stuart_node_memory_used_bytes", used, &|n| n.health.and_then(|h| h.memory).map(|m| m.used));
        let max = "The node's maxmemory, as it last said; 0: no limit.";
        each(out, "stuart_node_memory_max_bytes", max, &|n| n.health.and_then(|h| h.memory).map(|m| m.max));
    }

    /// stuart serves once it has read its state: always, here.
    fn ready(&self) -> Result<(), String> {
        Ok(())
    }
}

/// What the per-node metrics say about one node.
struct PerNode<'a> {
    id: String,
    addr: String,
    slots: usize,
    leaving: bool,
    health: Option<&'a Health>,
}
