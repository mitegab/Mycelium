//! Wire format: frames serialized with postcard, plus datagram framing.
//!
//! Each datagram is:
//!
//! ```text
//! +--------+---------+--------------------------------------------+
//! | magic  | version | postcard-encoded Packet                     |
//! | 2 bytes| 1 byte  | varint length prefix + payload               |
//! +--------+---------+--------------------------------------------+
//! ```
//!
//! The `postcard` crate provides a compact binary representation with no
//! padding: a tagged enum costs one varint variant byte, integers are varints
//! and `NodeId` costs 33 bytes (1-byte length + 32 raw bytes). Keeping frames
//! small matters on low-bandwidth radio links; see `docs/PROTOCOL.md`.
//!
//! **Crypto seam (phase 2):** once the authenticated-identity phase lands,
//! every `Packet` is additionally signed by `from` and `Data` bodies are
//! encrypted end-to-end (Noise-style X25519 + ChaCha20-Poly1305) so relaying
//! nodes only ever see routing metadata in the clear.

use crate::types::{hex_encode, NodeId, MAX_ADVERTS, MAX_DATAGRAM, MAX_PAYLOAD};

/// Two-byte magic prefix identifying Mycelium datagrams.
pub const MAGIC: [u8; 2] = *b"MC";
/// Wire protocol version. Bump on incompatible frame changes.
pub const VERSION: u8 = 1;

/// One route advertisement carried inside an OGM's distance vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Advert {
    pub dst: NodeId,
    /// Additive path cost from the advertising node to `dst`.
    pub cost: u16,
}

/// The inner payload of a Mycelium packet (everything after the envelope).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Body {
    /// Discovery probe sent periodically to every neighbor ("are you there?").
    Ping,
    /// Reply to a `Ping`, used for one-hop liveness.
    Pong { nonce: u64 },

    /// Originator Message. `origin` is the node that originated the OGM;
    /// `hops` counts how many times it has been forwarded and `adverts`
    /// carries the origin node's current distance vector (always empty once
    /// the OGM is being forwarded on behalf of another originator, since
    /// distance vectors are only trusted from their owner).
    Ogm {
        origin: NodeId,
        seq: u32,
        /// Remaining rebroadcast budget (decremented on each forward).
        ttl: u8,
        /// Number of forward hops already taken (0 at the origin).
        hops: u8,
        adverts: Vec<Advert>,
    },

    /// Application payload, forwarded hop-by-hop toward `dst`.
    Data {
        /// Original sender (not the last hop).
        src: NodeId,
        dst: NodeId,
        /// Random id chosen by `src`; relays use `(dst, msg_id)` to detect and
        /// drop retransmissions/loops.
        msg_id: u64,
        ttl: u8,
        payload: Vec<u8>,
    },
}

/// Envelope shared by every frame: who sent it (the immediate sender, i.e. the
/// last hop) plus a per-sender nonce used to detect replays/duplicates.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Packet {
    pub from: NodeId,
    /// Monotonic per-sender counter. Receivers keep the highest seen `from`
    /// value per neighbor and ignore lower/equal values (replay guard seam).
    pub nonce: u64,
    pub body: Body,
}

/// Errors produced while decoding a datagram.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    TooSmall,
    BadMagic,
    UnsupportedVersion(u8),
    Postcard(String),
    TooLarge(usize),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::TooSmall => write!(f, "datagram smaller than header"),
            DecodeError::BadMagic => write!(f, "bad magic"),
            DecodeError::UnsupportedVersion(v) => write!(f, "unsupported version {v}"),
            DecodeError::Postcard(e) => write!(f, "postcard decode error: {e}"),
            DecodeError::TooLarge(n) => write!(f, "frame of {n} bytes exceeds budget"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl Packet {
    /// Encode a packet into a wire datagram (magic + version + postcard).
    pub fn to_datagram(&self) -> Result<Vec<u8>, String> {
        let payload = postcard::to_stdvec(self).map_err(|e| format!("encode: {e}"))?;
        if payload.len() > MAX_DATAGRAM - 3 {
            return Err(format!(
                "frame of {} bytes exceeds max datagram of {}",
                payload.len(),
                MAX_DATAGRAM
            ));
        }
        let mut out = Vec::with_capacity(payload.len() + 3);
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Decode a datagram produced by [`Packet::to_datagram`].
    pub fn from_datagram(dg: &[u8]) -> Result<Packet, DecodeError> {
        if dg.len() < 3 {
            return Err(DecodeError::TooSmall);
        }
        if dg[0] != MAGIC[0] || dg[1] != MAGIC[1] {
            return Err(DecodeError::BadMagic);
        }
        if dg[2] != VERSION {
            return Err(DecodeError::UnsupportedVersion(dg[2]));
        }
        if dg.len() > MAX_DATAGRAM {
            return Err(DecodeError::TooLarge(dg.len()));
        }
        postcard::from_bytes(&dg[3..])
            .map_err(|e| DecodeError::Postcard(e.to_string()))
    }
}

/// Sanity limits applied when *building* frames so `to_datagram` rarely fails.
pub fn sanitize_adverts(adverts: &mut Vec<Advert>) {
    adverts.truncate(MAX_ADVERTS);
}

pub fn sanitize_payload(payload: &mut Vec<u8>) {
    if payload.len() > MAX_PAYLOAD {
        payload.truncate(MAX_PAYLOAD);
    }
}

/// Human-readable one-line description of a body, used in logs.
pub fn body_summary(body: &Body) -> String {
    match body {
        Body::Ping => "ping".into(),
        Body::Pong { .. } => "pong".into(),
        Body::Ogm { origin, seq, ttl, hops, adverts } => format!(
            "ogm origin={} seq={seq} ttl={ttl} hops={hops} adverts={}",
            origin.short(),
            adverts.len()
        ),
        Body::Data { src, dst, msg_id, ttl, payload } => format!(
            "data src={} dst={} msg={msg_id} ttl={ttl} len={}",
            src.short(),
            dst.short(),
            payload.len()
        ),
    }
}

/// Pretty-print a payload for the app layer: UTF-8 text when printable,
/// otherwise hex.
pub fn payload_display(payload: &[u8]) -> String {
    if payload.iter().all(|&b| b.is_ascii_graphic() || b == b' ') {
        String::from_utf8_lossy(payload).into_owned()
    } else {
        hex_encode(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

    fn sample_packet() -> Packet {
        Packet {
            from: NodeId::from_hex(&"12".repeat(32)).unwrap(),
            nonce: 7,
            body: Body::Data {
                src: NodeId::from_hex(&"34".repeat(32)).unwrap(),
                dst: NodeId::from_hex(&"56".repeat(32)).unwrap(),
                msg_id: 0xdead_beef,
                ttl: 31,
                payload: b"hello mesh".to_vec(),
            },
        }
    }

    #[test]
    fn node_id_wire_size_is_compact() {
        let id = NodeId::ZERO;
        let enc = postcard::to_stdvec(&id).unwrap();
        // 1-byte length prefix + 32 raw bytes.
        assert_eq!(enc.len(), 33);
        let dec: NodeId = postcard::from_bytes(&enc).unwrap();
        assert_eq!(dec, id);
    }

    #[test]
    fn datagram_round_trip() {
        let p = sample_packet();
        let dg = p.to_datagram().unwrap();
        // 3 NodeIds @33 bytes + envelope; well under the MTU budget.
        assert!(dg.len() < 160, "frame should be small, got {}", dg.len());
        let back = Packet::from_datagram(&dg).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn decode_errors() {
        assert_eq!(Packet::from_datagram(&[]), Err(DecodeError::TooSmall));
        let bad_magic = b"XX\x01rest";
        assert_eq!(Packet::from_datagram(bad_magic), Err(DecodeError::BadMagic));
        let mut bad_ver = sample_packet().to_datagram().unwrap();
        bad_ver[2] = 99;
        assert!(matches!(
            Packet::from_datagram(&bad_ver),
            Err(DecodeError::UnsupportedVersion(99))
        ));
        let mut truncated = sample_packet().to_datagram().unwrap();
        truncated.truncate(truncated.len() - 2);
        assert!(Packet::from_datagram(&truncated).is_err());
        // Truncation must never panic.
    }

    #[test]
    fn oversized_payload_rejected() {
        let mut p = sample_packet();
        if let Body::Data { payload, .. } = &mut p.body {
            payload.resize(MAX_PAYLOAD + 5000, 0u8);
        }
        assert!(p.to_datagram().is_err());
    }

    #[test]
    fn postcard_encoding_is_compact() {
        // A whole OGM with 5 adverts should stay far below the MTU budget.
        let mut adverts = Vec::new();
        for i in 0..5u8 {
            adverts.push(Advert {
                dst: NodeId([i; 32]),
                cost: (10 + i as u16) * 3,
            });
        }
        let p = Packet {
            from: NodeId::ZERO,
            nonce: 1,
            body: Body::Ogm { origin: NodeId([1; 32]), seq: 1000, ttl: 8, hops: 0, adverts },
        };
        let dg = p.to_datagram().unwrap();
        assert!(dg.len() < 300, "ogm with 5 adverts = {} bytes", dg.len());
    }
}
