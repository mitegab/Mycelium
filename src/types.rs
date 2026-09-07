//! Core value types and protocol constants for the Mycelium MVP.
//!
//! Everything in this module is deliberately free of IO and protocol logic so
//! it can be unit-tested in isolation.

use std::fmt;
use std::time::Duration;

/// Length of a [`NodeId`] in bytes.
pub const NODE_ID_LEN: usize = 32;

/// Maximum size of a single wire datagram (bytes). Chosen to stay safely below
/// the typical UDP payload ceiling (~1500 MTU minus IP/UDP headers) so frames
/// are never fragmented on a plain Ethernet/IP underlay.
pub const MAX_DATAGRAM: usize = 1400;

/// Maximum number of route advertisements carried inside one OGM.
pub const MAX_ADVERTS: usize = 48;

/// Maximum application payload size carried inside one `Data` frame.
pub const MAX_PAYLOAD: usize = 1024;

/// Default time-to-live for application `Data` frames (in hops).
pub const DATA_TTL: u8 = 32;

/// Time-to-live for forwarded Originator Messages (in hops).
pub const OGM_TTL: u8 = 8;

/// Cost of an unreachable destination.
pub const COST_INF: u16 = u16::MAX;

/// Base cost contributed by every hop of a path (before link-quality penalty).
pub const LINK_BASE_COST: u16 = 10;

/// Maximum link-quality penalty added on top of [`LINK_BASE_COST`] for a
/// completely lossy link (measured packet-delivery ratio of 0.0).
pub const LINK_PENALTY_MAX: u16 = 90;

/// Number of OGM intervals kept in the sliding-window delivery estimator.
pub const PDR_WINDOW_SLOTS: u32 = 10;

// ---------------------------------------------------------------------------
// Timing defaults (per-node, overridable from the CLI)
// ---------------------------------------------------------------------------

/// How often a node broadcasts its own OGM.
pub const DEFAULT_OGM_INTERVAL: Duration = Duration::from_secs(1);

/// How often a node emits a discovery ping to each neighbor.
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(1);

/// How long without *any* frame from a neighbor before it is considered gone.
pub const DEFAULT_NEIGHBOR_TIMEOUT: Duration = Duration::from_secs(4);

/// How long without a fresh OGM/distance-vector refresh before a route entry
/// is expired (must be >= the OGM interval).
pub const DEFAULT_ROUTE_TIMEOUT: Duration = Duration::from_secs(4);

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// Stable node identifier.
///
/// **Crypto seam (phase 2):** this placeholder is an opaque 32-byte random
/// identifier. In the authenticated-identity phase it becomes the raw Ed25519
/// public key, and the node proves ownership of the matching secret key by
/// signing its OGM/hello traffic. The surrounding code only ever treats a
/// `NodeId` as an opaque handle, so swapping the backing key type is local to
/// `identity.rs`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

impl NodeId {
    /// The all-zero id (used as a "none"/placeholder value in tests).
    pub const ZERO: NodeId = NodeId([0u8; NODE_ID_LEN]);

    pub fn as_bytes(&self) -> &[u8; NODE_ID_LEN] {
        &self.0
    }

    /// Full hex encoding (64 chars).
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Compact hex used in human-oriented logs (first 12 chars).
    pub fn short(&self) -> String {
        self.to_hex()[..12].to_string()
    }

    /// Parse a full 64-char hex string.
    pub fn from_hex(s: &str) -> Result<NodeId, String> {
        let bytes = hex_decode(s).ok_or_else(|| format!("invalid hex: {s:?}"))?;
        if bytes.len() != NODE_ID_LEN {
            return Err(format!(
                "expected {NODE_ID_LEN} bytes, got {}",
                bytes.len()
            ));
        }
        let mut out = [0u8; NODE_ID_LEN];
        out.copy_from_slice(&bytes);
        Ok(NodeId(out))
    }

    /// Resolve an unambiguous hex prefix (>= 6 chars) against a list of
    /// candidates, mirroring how git disambiguates abbreviated hashes.
    pub fn resolve_prefix(prefix: &str, candidates: &[NodeId]) -> Result<NodeId, String> {
        if prefix.len() < 6 {
            return Err("prefix must be at least 6 hex characters".to_string());
        }
        let matches: Vec<NodeId> = candidates
            .iter()
            .copied()
            .filter(|id| id.to_hex().starts_with(prefix))
            .collect();
        match matches.len() {
            0 => Err(format!("no node matches prefix {prefix:?}")),
            1 => Ok(matches[0]),
            n => Err(format!("prefix {prefix:?} is ambiguous ({n} matches)")),
        }
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.short())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl serde::Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<NodeId, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = NodeId;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{NODE_ID_LEN} raw bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<NodeId, E> {
                if v.len() != NODE_ID_LEN {
                    return Err(E::custom(format!(
                        "expected {NODE_ID_LEN} bytes, got {}",
                        v.len()
                    )));
                }
                let mut out = [0u8; NODE_ID_LEN];
                out.copy_from_slice(v);
                Ok(NodeId(out))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<NodeId, A::Error> {
                let mut out = [0u8; NODE_ID_LEN];
                for slot in out.iter_mut() {
                    match seq.next_element::<u8>()? {
                        Some(b) => *slot = b,
                        None => {
                            return Err(serde::de::Error::custom(format!(
                                "expected {NODE_ID_LEN} bytes, got fewer"
                            )))
                        }
                    }
                }
                Ok(NodeId(out))
            }
        }
        d.deserialize_bytes(V)
    }
}

// ---------------------------------------------------------------------------
// Small hex helpers (no external deps for the MVP)
// ---------------------------------------------------------------------------

/// Encode bytes as lowercase hex.
pub fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

/// Decode hex into bytes (accepts mixed case). Returns `None` on any error.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// A tiny deterministic PRNG used only for simulation knobs (ingress loss) and
// ephemeral message ids. **Not** a cryptographic generator: the crypto phase
// replaces it with a CSPRNG.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seed from OS entropy when available, falling back to the clock.
    pub fn from_entropy() -> Rng {
        let mut seed = 0u64;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            use std::io::Read;
            let mut buf = [0u8; 8];
            if f.read_exact(&mut buf).is_ok() {
                seed = u64::from_le_bytes(buf);
            }
        }
        if seed == 0 {
            seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9e3779b97f4a7c15)
                ^ (std::process::id() as u64) << 32;
        }
        Rng::new(seed)
    }

    pub fn new(seed: u64) -> Rng {
        // SplitMix64 seeding to avoid poor low bits from simple seeds.
        let mut r = Rng(seed.wrapping_add(0x9e3779b97f4a7c15));
        r.next_u64();
        r.next_u64();
        r
    }

    /// SplitMix64-style output; not cryptographic.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Bernoulli trial with probability `p` in [0,1].
    pub fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let x = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        x < p
    }
}

// ---------------------------------------------------------------------------
// Small random helpers
// ---------------------------------------------------------------------------

/// Make a throwaway random node id (for tests and quick demos).
pub fn random_node_id() -> NodeId {
    let mut rng = Rng::from_entropy();
    let mut b = [0u8; 32];
    for chunk in b.chunks_mut(8) {
        chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    NodeId(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let data = [0x00u8, 0x01, 0xab, 0xcd, 0xff];
        let s = hex_encode(&data);
        assert_eq!(s, "0001abc dff".replace(' ', ""));
        assert_eq!(hex_decode(&s).unwrap(), data);
        assert!(hex_decode("abc").is_none());
        assert!(hex_decode("zz").is_none());
    }

    #[test]
    fn node_id_parse_and_format() {
        let a = NodeId::ZERO;
        let b = NodeId::from_hex(&a.to_hex()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.short().len(), 12);
        assert!(NodeId::from_hex("deadbeef").is_err());
        assert!(NodeId::from_hex(&"0".repeat(63)).is_err());
    }

    #[test]
    fn prefix_resolution() {
        let a = NodeId::from_hex(&"ab".repeat(32)).unwrap();
        let b = NodeId::from_hex(&"cd".repeat(32)).unwrap();
        // Shares its first 31 bytes (62 hex chars) with `a`.
        let c = NodeId::from_hex(&format!("{}ef", "ab".repeat(31))).unwrap();
        assert_eq!(NodeId::resolve_prefix(&a.to_hex(), &[a, b, c]).unwrap(), a);
        assert_eq!(NodeId::resolve_prefix(&b.to_hex()[..8], &[a, b, c]).unwrap(), b);
        // `a` and `c` are ambiguous until the prefix reaches byte 32.
        assert!(NodeId::resolve_prefix(&a.to_hex()[..40], &[a, c]).is_err());
        assert_eq!(NodeId::resolve_prefix(&a.to_hex(), &[a, c]).unwrap(), a);
        assert!(NodeId::resolve_prefix("zz", &[a, b]).is_err());
        assert!(NodeId::resolve_prefix(&a.to_hex()[..4], &[a]).is_err());
        assert!(NodeId::resolve_prefix(&a.to_hex()[..8], &[]).is_err());
    }

    #[test]
    fn rng_works() {
        let mut r = Rng::new(42);
        let a = r.next_u64();
        let mut r2 = Rng::new(42);
        assert_eq!(a, r2.next_u64());
        assert!(r.chance(1.0));
        assert!(!r.chance(0.0));
    }
}
