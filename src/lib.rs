//! # Mycelium
//!
//! A resilient, self-healing mesh networking engine (MVP). Each node is an
//! independent process that:
//!
//! 1. holds a stable identity ([`identity`]),
//! 2. discovers and tracks only its *direct* neighbors — it never sees the
//!    whole topology ([`neighbors`]),
//! 3. exchanges Originator Messages with a compact postcard/serde wire format
//!    ([`wire`]) to measure per-link delivery ratio ([`linkq`]) and maintain
//!    a quality-weighted distance-vector routing table ([`routing`]),
//! 4. forwards opaque `Data` frames hop-by-hop toward their destination
//!    ([`engine`]) over a swappable UDP transport ([`io`]).
//!
//! The MVP deliberately keeps cryptography out of the hot path; identity and
//! frame-level crypto seams are marked "phase 2" in the code.

pub mod engine;
pub mod identity;
pub mod io;
pub mod linkq;
pub mod neighbors;
pub mod routing;
pub mod types;
pub mod wire;

pub use engine::{
    start, Command, Counters, Event, NeighborInfo, NodeConfig, NodeController, PeerConfig,
    RouteInfo, Snapshot,
};
pub use identity::Identity;
pub use types::NodeId;
