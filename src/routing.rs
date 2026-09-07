//! Routing engine: distance-vector tables learned from OGM advertisements,
//! with BATMAN-style originator tracking and PDR-weighted link costs.
//!
//! # Model
//!
//! Every node periodically broadcasts a self-OGM carrying its own distance
//! vector (a set of `(destination, cost)` advertisements). A neighbor that
//! receives the OGM relaxes its own table with `candidate_cost = adv_cost +
//! link_cost(neighbor)`; the neighbor that offered the best candidate becomes
//! the next hop. Because every hop adds at least [`crate::types::LINK_BASE_COST`]
//! and lossy links add an extra penalty derived from measured packet delivery
//! ratio, the table converges on the highest-quality (not merely shortest)
//! path toward every destination — the "OGM decay & hop metric table" from
//! the architecture sketch.
//!
//! Forwarded OGMs (origins that are not direct neighbors) provide *tentative*
//! reachability with an estimated hop cost, which keeps destinations
//! reachable even when a node's advertisement list overflows
//! [`crate::types::MAX_ADVERTS`]; precise distance-vector costs always win
//! when both exist.
//!
//! # Self-healing
//!
//! Entries expire after [`RouteTable::new`]'s `timeout` if they are not
//! refreshed by an OGM. When a link or node dies, refreshes stop, entries are
//! pruned, and the next OGM round re-converges around the failure.
//!
//! Known MVP limitation (documented in `docs/PROTOCOL.md`): classic
//! count-to-infinity behavior applies during partitions *before* expiry
//! prunes stale entries; the mesh is small and expiry is fast, so this is
//! acceptable for the MVP and is addressed by sequence-numbered link-state or
//! BATMAN TQ (not hop-count) routing in a later phase.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::types::{NodeId, COST_INF, LINK_BASE_COST, LINK_PENALTY_MAX, MAX_ADVERTS};
use crate::wire::Advert;

/// Convert a measured inbound packet-delivery ratio (0..1) into an additive
/// per-hop cost: `LINK_BASE_COST` for a perfect link, up to
/// `LINK_BASE_COST + LINK_PENALTY_MAX` for a fully lossy one.
pub fn link_cost(lq: f64) -> u16 {
    let lq = lq.clamp(0.0, 1.0);
    let penalty = ((1.0 - lq) * f64::from(LINK_PENALTY_MAX)).round() as u16;
    LINK_BASE_COST.saturating_add(penalty)
}

/// A route toward one destination.
#[derive(Debug, Clone)]
pub struct Route {
    pub dest: NodeId,
    /// Total additive cost from this node to `dest`.
    pub cost: u16,
    /// Immediate next hop (always a direct neighbor).
    pub next_hop: NodeId,
    /// When this entry was last refreshed.
    pub last_update: Instant,
    /// Some(hops) when learned from a forwarded OGM rather than from the
    /// destination's own distance vector.
    pub via_ogm_hops: Option<u8>,
}

impl Route {
    pub fn is_fresh(&self, now: Instant, timeout: Duration) -> bool {
        now.duration_since(self.last_update) <= timeout
    }
}

#[derive(Debug, Default)]
pub struct RouteTable {
    routes: HashMap<NodeId, Route>,
    timeout: Duration,
}

impl RouteTable {
    pub fn new(timeout: Duration) -> RouteTable {
        RouteTable { routes: HashMap::new(), timeout }
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn get(&self, dest: &NodeId) -> Option<&Route> {
        self.routes.get(dest)
    }

    /// Best (lowest-cost) route to `dest`, if any, that is still fresh.
    pub fn best_fresh(&self, dest: &NodeId, now: Instant) -> Option<&Route> {
        self.routes
            .get(dest)
            .filter(|r| r.is_fresh(now, self.timeout))
    }

    /// Best fresh route to `dest` usable *now* for forwarding.
    pub fn next_hop(&self, dest: &NodeId, now: Instant) -> Option<NodeId> {
        self.best_fresh(dest, now).map(|r| r.next_hop)
    }

    /// Relax with an advertisement received from a *direct* neighbor `via`
    /// whose inbound link quality was `lq` (its own distance vector).
    pub fn relax_dv(
        &mut self,
        dest: NodeId,
        adv_cost: u16,
        via: NodeId,
        lq: f64,
        now: Instant,
    ) -> bool {
        if adv_cost >= COST_INF {
            return false;
        }
        let candidate = adv_cost.saturating_add(link_cost(lq));
        self.consider(dest, candidate, via, None, now)
    }

    /// Learn/refresh a *tentative* route from a forwarded OGM: origin `origin`
    /// is `hops` hops away through direct neighbor `via`.
    pub fn relax_ogm(
        &mut self,
        origin: NodeId,
        via: NodeId,
        hops: u8,
        lq: f64,
        now: Instant,
    ) -> bool {
        // First hop costs what this link costs; further hops are approximated
        // at the base per-hop cost (precise DV relaxations supersede this).
        let cost = link_cost(lq).saturating_add(u16::from(hops).saturating_mul(LINK_BASE_COST));
        self.consider(origin, cost, via, Some(hops), now)
    }

    /// Insert/replace a route only if it improves on the current best.
    fn consider(
        &mut self,
        dest: NodeId,
        cost: u16,
        next_hop: NodeId,
        via_ogm_hops: Option<u8>,
        now: Instant,
    ) -> bool {
        let better = match self.routes.get(&dest) {
            None => true,
            Some(cur) => {
                cost < cur.cost
                    || (cost == cur.cost && via_ogm_hops.is_none() && cur.via_ogm_hops.is_some())
            }
        };
        if better {
            self.routes.insert(
                dest,
                Route {
                    dest,
                    cost: cost.min(COST_INF - 1),
                    next_hop,
                    last_update: now,
                    via_ogm_hops,
                },
            );
        }
        better
    }

    /// Explicitly drop a route (used when the next hop disappears).
    pub fn remove(&mut self, dest: &NodeId) {
        self.routes.remove(dest);
    }

    /// Remove every entry not refreshed within `timeout`; returns how many.
    pub fn prune_expired(&mut self, now: Instant) -> usize {
        let expired: Vec<NodeId> = self
            .routes
            .iter()
            .filter(|(_, r)| now.duration_since(r.last_update) > self.timeout)
            .map(|(d, _)| *d)
            .collect();
        let n = expired.len();
        for d in expired {
            self.routes.remove(&d);
        }
        n
    }

    /// Drop routes whose next hop is no longer a known neighbor.
    pub fn prune_unreachable_next_hops(
        &mut self,
        known_neighbors: &dyn Fn(&NodeId) -> bool,
    ) -> usize {
        let doomed: Vec<NodeId> = self
            .routes
            .iter()
            .filter(|(_, r)| !known_neighbors(&r.next_hop))
            .map(|(d, _)| *d)
            .collect();
        let n = doomed.len();
        for d in doomed {
            self.routes.remove(&d);
        }
        n
    }

    /// Build the distance vector for our next self-OGM: all routes sorted by
    /// cost ascending, capped at `max`. The node's own id is *not* included
    /// (receivers learn it from the OGM header at zero cost).
    pub fn build_adverts(&self, max: usize) -> Vec<Advert> {
        let mut adverts: Vec<Advert> = self
            .routes
            .iter()
            .map(|(d, r)| Advert { dst: *d, cost: r.cost })
            .collect();
        adverts.sort_by_key(|a| a.cost);
        adverts.truncate(max.min(MAX_ADVERTS));
        adverts
    }

    /// Sorted view of the table (cheapest first), for diagnostics and tests.
    pub fn sorted(&self) -> Vec<&Route> {
        let mut v: Vec<&Route> = self.routes.values().collect();
        v.sort_by_key(|r| r.cost);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> NodeId {
        NodeId([b; 32])
    }
    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn link_cost_quality_aware() {
        assert_eq!(link_cost(1.0), LINK_BASE_COST);
        assert_eq!(link_cost(0.0), LINK_BASE_COST + LINK_PENALTY_MAX);
        assert_eq!(link_cost(0.5), LINK_BASE_COST + LINK_PENALTY_MAX / 2);
        assert_eq!(link_cost(1.5), LINK_BASE_COST); // clamped
        assert_eq!(link_cost(-1.0), LINK_BASE_COST + LINK_PENALTY_MAX);
    }

    #[test]
    fn dv_relaxation_finds_shortest_path() {
        let mut t = RouteTable::new(Duration::from_secs(60));
        let b = id(2);
        let c = id(3);
        let n = now();

        // b advertises: it can reach c at cost 10 (perfect link quality).
        assert!(t.relax_dv(c, 10, b, 1.0, n));
        let r = t.get(&c).unwrap();
        assert_eq!(r.cost, 20); // 10 + base hop to b
        assert_eq!(r.next_hop, b);
        assert_eq!(t.next_hop(&c, n), Some(b));
    }

    #[test]
    fn worse_path_does_not_replace_better() {
        let mut t = RouteTable::new(Duration::from_secs(60));
        let a = id(1);
        let b = id(2);
        let c = id(3);
        let n = now();
        t.relax_dv(c, 10, b, 1.0, n);
        // A worse advertisement via b (cost 100) must not replace it.
        t.relax_dv(c, 100, b, 1.0, n);
        assert_eq!(t.get(&c).unwrap().cost, 20);
        // A *better* one via a lossy link can still win if the total is lower.
        t.relax_dv(c, 0, a, 0.5, n); // cost = 0 + 55 = 55 > 20
        assert_eq!(t.get(&c).unwrap().next_hop, b);
        t.relax_dv(c, 0, a, 1.0, n); // cost = 0 + 10 = 10 < 20
        assert_eq!(t.get(&c).unwrap().next_hop, a);
    }

    #[test]
    fn ogm_tentative_route_and_dv_override() {
        let mut t = RouteTable::new(Duration::from_secs(60));
        let me = id(1);
        let nbr = id(2);
        let far = id(9);
        let n = now();

        // far is 2 hops away through nbr: cost = 10 (nbr link) + 2*10 = 30.
        t.relax_ogm(far, nbr, 2, 1.0, n);
        assert_eq!(t.get(&far).unwrap().cost, 30);
        assert_eq!(t.get(&far).unwrap().via_ogm_hops, Some(2));

        // nbr's DV then advertises far at cost 5 (it knows a shortcut):
        // candidate = 5 + 10 = 15 < 30, and source is DV -> wins.
        t.relax_dv(far, 5, nbr, 1.0, n);
        let r = t.get(&far).unwrap();
        assert_eq!(r.cost, 15);
        assert_eq!(r.via_ogm_hops, None);
        let _ = me;
    }

    #[test]
    fn expiry_prunes_stale_routes() {
        let mut t = RouteTable::new(Duration::from_millis(500));
        let b = id(2);
        let c = id(3);
        let n = now();
        t.relax_dv(c, 10, b, 1.0, n);
        assert_eq!(t.len(), 1);
        // Simulate time passing without refresh.
        let later = n + Duration::from_secs(10);
        assert!(t.best_fresh(&c, later).is_none());
        assert_eq!(t.prune_expired(later), 1);
        assert!(t.is_empty());
    }

    #[test]
    fn adverts_sorted_and_capped() {
        let mut t = RouteTable::new(Duration::from_secs(60));
        let n = now();
        let b = id(2);
        for i in 0..70u8 {
            let d = id(100 + i);
            t.relax_dv(d, u16::from(i) * 2, b, 1.0, n);
        }
        let adverts = t.build_adverts(10);
        assert_eq!(adverts.len(), 10);
        assert!(adverts.windows(2).all(|w| w[0].cost <= w[1].cost));
        assert_eq!(adverts[0].dst, id(100));
    }

    #[test]
    fn prune_when_next_hop_gone() {
        let mut t = RouteTable::new(Duration::from_secs(60));
        let b = id(2);
        let c = id(3);
        let n = now();
        t.relax_dv(c, 5, b, 1.0, n);
        assert_eq!(t.prune_unreachable_next_hops(&|nh| *nh != b), 1);
        assert!(t.is_empty());
    }
}
