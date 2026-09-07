//! Node identity.
//!
//! **Crypto seam (phase 2).** In the authenticated-identity phase a node's
//! `NodeId` becomes the raw bytes of its Ed25519 public key and `secret` the
//! matching seed; nodes prove ownership of their identity by signing their
//! OGM/hello traffic (zero-trust: no IP, DNS or registry binds identity to
//! anything but the key). Until then this module stores an opaque random
//! `NodeId` plus placeholder secret material so identity is *stable across
//! restarts* without pretending to be secure. Every other module already
//! treats `NodeId` as an opaque handle, so the swap is local to this file.
//!
//! The secret placeholder must never be used for real security; it exists to
//! keep the identity lifecycle (generate / persist / reload / display) real.

use std::fs;
use std::io::{self, Write};
use std::path::Path;

use crate::types::{hex_decode, hex_encode, random_node_id, NodeId};

/// The identity material of one node.
#[derive(Debug, Clone, Copy)]
pub struct Identity {
    pub node_id: NodeId,
    /// Placeholder secret bytes (see module docs).
    pub secret: [u8; 32],
}

impl Identity {
    /// Generate a fresh identity (placeholder randomness; not cryptographic).
    pub fn generate() -> Identity {
        let node_id = random_node_id();
        let mut secret = [0u8; 32];
        let mut r = crate::types::Rng::from_entropy();
        for chunk in secret.chunks_mut(8) {
            chunk.copy_from_slice(&r.next_u64().to_le_bytes());
        }
        Identity { node_id, secret }
    }

    /// Persist to `path` as two lowercase-hex lines (`node_id`, `secret`).
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = fs::File::create(path)?;
        writeln!(f, "{}", hex_encode(self.node_id.as_bytes()))?;
        writeln!(f, "{}", hex_encode(&self.secret))?;
        f.sync_all()?;
        Ok(())
    }

    /// Load from `path`; `None` when the file does not exist or is malformed.
    pub fn load(path: &Path) -> Option<Identity> {
        let contents = fs::read_to_string(path).ok()?;
        let mut lines = contents.lines();
        let node_id = NodeId::from_hex(lines.next()?.trim()).ok()?;
        let secret_bytes = hex_decode(lines.next()?.trim())?;
        if secret_bytes.len() != 32 {
            return None;
        }
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&secret_bytes);
        Some(Identity { node_id, secret })
    }

    /// Load `path`, or generate + persist a fresh identity there.
    pub fn load_or_create(path: &Path) -> io::Result<Identity> {
        if let Some(id) = Identity::load(path) {
            return Ok(id);
        }
        let id = Identity::generate();
        id.save(path)?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("mycelium-id-test-{}", std::process::id()));
        let path = dir.join("identity.key");
        let a = Identity::generate();
        a.save(&path).unwrap();
        let b = Identity::load(&path).expect("load");
        assert_eq!(a.node_id, b.node_id);
        assert_eq!(a.secret, b.secret);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_or_create_persists() {
        let dir = std::env::temp_dir().join(format!("mycelium-id-test2-{}", std::process::id()));
        let path = dir.join("identity.key");
        let a = Identity::load_or_create(&path).unwrap();
        let b = Identity::load_or_create(&path).unwrap();
        assert_eq!(a.node_id, b.node_id);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_file_yields_none() {
        let dir = std::env::temp_dir().join(format!("mycelium-id-test3-{}", std::process::id()));
        let path = dir.join("identity.key");
        fs::create_dir_all(&dir).unwrap();
        fs::write(&path, "not-hex\nzz\n").unwrap();
        assert!(Identity::load(&path).is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
