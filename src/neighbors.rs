//! Direct-neighbor registry ("Discovery Engine").
//!
//! A node only ever talks to the addresses in its configured peer set — the
//! mechanism that simulates a real ad hoc radio network where no node can see
//! beyond its direct neighbors. Frames arriving from an address outside the
//! peer set are dropped at the transport boundary and counted.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::linkq::DeliveryEstimator;
use crate::types::NodeId;

/// One direct neighbor.
#[derive(Debug)]
pub struct Neighbor {
    pub addr: SocketAddr,
    /// The neighbor's `NodeId`, learned from the first frame it sends.
    pub node_id: Option<NodeId>,
    /// Last time *any* valid frame arrived from this neighbor.
    pub last_seen: Instant,
    /// Last time a self-OGM arrived (drives the PDR estimator).
    pub last_ogm: Option<Instant>,
    /// Inbound OGM delivery estimator for this neighbor.
    pub ogm_pdr: DeliveryEstimator,
    /// Simulated inbound loss (0.0 = none, 1.0 = drop everything). This is a
    /// simulation/testing knob, not a transport feature.
    pub sim_loss: f64,
    /// Number of frames received from this neighbor (post-loss).
    pub rx_frames: u64,
    /// Number of frames sent to this neighbor.
    pub tx_frames: u64,
}

impl Neighbor {
    fn new(addr: SocketAddr) -> Neighbor {
        Neighbor {
            addr,
            node_id: None,
            last_seen: Instant::now(),
            last_ogm: None,
            ogm_pdr: DeliveryEstimator::new(crate::types::PDR_WINDOW_SLOTS),
            sim_loss: 0.0,
            rx_frames: 0,
            tx_frames: 0,
        }
    }

    /// Inbound link quality in `[0,1]` (PDR of this neighbor's self-OGMs).
    pub fn link_quality(&self) -> f64 {
        self.ogm_pdr.ratio()
    }
}

/// Address-keyed table of direct neighbors.
#[derive(Debug, Default)]
pub struct NeighborTable {
    by_addr: HashMap<SocketAddr, Neighbor>,
}

impl NeighborTable {
    pub fn new() -> NeighborTable {
        NeighborTable::default()
    }

    /// Fetch or create the entry for `addr` (only call this for addresses that
    /// passed the peer whitelist, otherwise you'd admit strangers).
    pub fn entry(&mut self, addr: SocketAddr) -> &mut Neighbor {
        self.by_addr.entry(addr).or_insert_with(|| Neighbor::new(addr))
    }

    pub fn get(&self, addr: &SocketAddr) -> Option<&Neighbor> {
        self.by_addr.get(addr)
    }

    pub fn get_mut(&mut self, addr: &SocketAddr) -> Option<&mut Neighbor> {
        self.by_addr.get_mut(addr)
    }

    /// Remove neighbors that have been silent for `timeout`. Returns the
    /// departed (address, node id) pairs so the caller can also release
    /// per-node state (routes, sequence watermarks).
    pub fn flush_expired(
        &mut self,
        now: Instant,
        timeout: Duration,
    ) -> Vec<(SocketAddr, NodeId)> {
        let gone: Vec<(SocketAddr, NodeId)> = self
            .by_addr
            .iter()
            .filter(|(_, n)| now.duration_since(n.last_seen) > timeout)
            .map(|(a, n)| (*a, n.node_id.unwrap_or(NodeId::ZERO)))
            .collect();
        for (a, _) in &gone {
            self.by_addr.remove(a);
        }
        gone
    }

    pub fn iter(&self) -> impl Iterator<Item = &Neighbor> {
        self.by_addr.values()
    }

    pub fn len(&self) -> usize {
        self.by_addr.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_addr.is_empty()
    }

    /// Resolve the current address of a known neighbor by its `NodeId`.
    pub fn addr_of(&self, id: &NodeId) -> Option<SocketAddr> {
        self.by_addr.values().find(|n| n.node_id == Some(*id)).map(|n| n.addr)
    }

    /// Find a neighbor whose learned `NodeId` matches a hex prefix.
    pub fn resolve_prefix(&self, prefix: &str) -> Result<(NodeId, SocketAddr), String> {
        let known: Vec<NodeId> =
            self.by_addr.values().filter_map(|n| n.node_id).collect();
        let id = NodeId::resolve_prefix(prefix, &known)?;
        let addr = self
            .addr_of(&id)
            .ok_or_else(|| format!("neighbor {id} has no address"))?;
        Ok((id, addr))
    }

    /// Record that a valid frame arrived from `addr`; binds `node_id` on first
    /// sight. Returns the (possibly just-created) neighbor.
    pub fn note_frame(&mut self, addr: SocketAddr, from: NodeId) -> &mut Neighbor {
        let n = self.entry(addr);
        n.last_seen = Instant::now();
        n.rx_frames += 1;
        if n.node_id != Some(from) {
            // A neighbor changing identity is suspicious (and impossible once
            // frames are signed in the crypto phase); log-worthy, handled by
            // the engine which owns the returned data.
            n.node_id = Some(from);
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn learn_and_lookup() {
        let mut t = NeighborTable::new();
        let a1 = addr(2000);
        let id = NodeId([1u8; 32]);
        {
            let n = t.note_frame(a1, id);
            n.ogm_pdr.observe(0);
            n.ogm_pdr.observe(1);
        }
        assert_eq!(t.addr_of(&id), Some(a1));
        assert_eq!(t.get(&a1).unwrap().node_id, Some(id));
        assert!(t.resolve_prefix(&id.to_hex()[..8]).is_ok());
        assert!(t.resolve_prefix("ff").is_err());
        assert_eq!(t.resolve_prefix(&id.to_hex()[..4]), Err("prefix must be at least 6 hex characters".to_string()));
    }

    #[test]
    fn expiry_flushes_silent_neighbors() {
        let mut t = NeighborTable::new();
        let a1 = addr(2001);
        t.note_frame(a1, NodeId([2u8; 32]));
        assert_eq!(t.len(), 1);
        // Walk time forward by mutating last_seen directly.
        t.get_mut(&a1).unwrap().last_seen =
            Instant::now() - Duration::from_secs(99);
        let gone = t.flush_expired(Instant::now(), Duration::from_secs(10));
        assert_eq!(gone, vec![(a1, NodeId([2u8; 32]))]);
        assert!(t.is_empty());
    }
}
