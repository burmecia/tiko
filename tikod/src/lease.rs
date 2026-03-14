//! Project lease — distributed fence preventing two servers from running the
//! same project simultaneously.
//!
//! The lease lives at `{org}/metadata/{proj}/lease.json` in the standard bucket.
//! "Conditional PUT" is emulated as a read-check-write: read the current lease,
//! confirm it is absent or expired, then overwrite. This is correct for the
//! SimStore (single-writer) and will be replaced by S3's native `If-None-Match`
//! conditional write when the real S3 client is added.

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use store::sim_store::SimStore;

// ── Constants ────────────────────────────────────────────────────────────────

/// Default lease TTL in seconds (60 s).  Renewal fires every 15 s.
pub const LEASE_TTL_SECS: u64 = 60;

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Lease {
    pub server_id: String,
    pub acquired_at: u64,
    pub expires_at: u64,
    pub generation: u64,
}

#[derive(Debug)]
pub enum LeaseError {
    /// Another server holds a valid (non-expired) lease.
    Held { by: String },
    /// Store I/O error.
    Store(io::Error),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Held { by } => write!(f, "lease held by {by}"),
            LeaseError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl From<io::Error> for LeaseError {
    fn from(e: io::Error) -> Self {
        LeaseError::Store(e)
    }
}

// ── Key helpers ───────────────────────────────────────────────────────────────

/// `{org}/metadata/{proj}/lease.json`
pub fn lease_key(org: u64, project: u64) -> String {
    format!("{org}/metadata/{project}/lease.json")
}

/// `{org}/gc_lease.json`  (used by tikod GC to serialise cross-server GC runs)
pub fn gc_lease_key(org: u64) -> String {
    format!("{org}/gc_lease.json")
}

// ── Core operations ───────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Acquire the lease at `key` for `server_id`.
///
/// Succeeds when:
/// - No lease object exists at `key`, or
/// - The existing lease's `expires_at` is in the past.
///
/// Returns the new `Lease` on success, `LeaseError::Held` if another server
/// owns a valid lease.
pub fn acquire(
    sim: &SimStore,
    key: &str,
    server_id: &str,
    ttl_secs: u64,
) -> Result<Lease, LeaseError> {
    let now = now_secs();

    // Read-check: is there an existing, non-expired lease?
    if let Some(bytes) = sim.get_standard(key)? {
        if let Ok(existing) = serde_json::from_slice::<Lease>(&bytes) {
            if existing.expires_at > now {
                return Err(LeaseError::Held {
                    by: existing.server_id,
                });
            }
            // Expired — fall through and overwrite.
            let new_generation = existing.generation + 1;
            return write_lease(sim, key, server_id, now, ttl_secs, new_generation);
        }
        // Corrupt JSON — treat as absent and overwrite with generation 0.
    }

    write_lease(sim, key, server_id, now, ttl_secs, 0)
}

/// Renew the lease (unconditional PUT — we own it).
///
/// Updates `expires_at = now + ttl_secs`. Does not change `generation`.
pub fn renew(sim: &SimStore, key: &str, lease: &Lease, ttl_secs: u64) -> Result<Lease, LeaseError> {
    let now = now_secs();
    let renewed = Lease {
        expires_at: now + ttl_secs,
        acquired_at: lease.acquired_at,
        server_id: lease.server_id.clone(),
        generation: lease.generation,
    };
    let json = serde_json::to_vec(&renewed).map_err(|e| io::Error::other(e))?;
    sim.put_standard(key, &json)?;
    Ok(renewed)
}

/// Release the lease by deleting the object.
pub fn release(sim: &SimStore, key: &str) -> Result<(), LeaseError> {
    sim.delete_standard(key)?;
    Ok(())
}

// ── Internal ──────────────────────────────────────────────────────────────────

fn write_lease(
    sim: &SimStore,
    key: &str,
    server_id: &str,
    now: u64,
    ttl_secs: u64,
    generation: u64,
) -> Result<Lease, LeaseError> {
    let lease = Lease {
        server_id: server_id.to_owned(),
        acquired_at: now,
        expires_at: now + ttl_secs,
        generation,
    };
    let json = serde_json::to_vec(&lease).map_err(|e| io::Error::other(e))?;
    sim.put_standard(key, &json)?;
    Ok(lease)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_sim() -> (SimStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let sim = SimStore::new(dir.path());
        (sim, dir)
    }

    #[test]
    fn acquire_succeeds_when_no_lease_exists() {
        let (sim, _dir) = temp_sim();
        let key = lease_key(1, 42);
        let lease = acquire(&sim, &key, "server-a", LEASE_TTL_SECS).unwrap();
        assert_eq!(lease.server_id, "server-a");
        assert_eq!(lease.generation, 0);
        assert!(lease.expires_at > lease.acquired_at);
    }

    #[test]
    fn acquire_fails_when_valid_lease_held_by_another() {
        let (sim, _dir) = temp_sim();
        let key = lease_key(1, 42);
        acquire(&sim, &key, "server-a", LEASE_TTL_SECS).unwrap();

        let err = acquire(&sim, &key, "server-b", LEASE_TTL_SECS).unwrap_err();
        match err {
            LeaseError::Held { by } => assert_eq!(by, "server-a"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn acquire_succeeds_when_existing_lease_expired() {
        let (sim, _dir) = temp_sim();
        let key = lease_key(1, 42);

        // Write a lease that already expired (ttl = 0 → expires_at == acquired_at).
        acquire(&sim, &key, "server-a", 0).unwrap();

        // server-b should be able to take it over.
        let lease = acquire(&sim, &key, "server-b", LEASE_TTL_SECS).unwrap();
        assert_eq!(lease.server_id, "server-b");
        assert_eq!(lease.generation, 1);
    }

    #[test]
    fn renew_updates_expires_at() {
        let (sim, _dir) = temp_sim();
        let key = lease_key(1, 42);
        let original = acquire(&sim, &key, "server-a", LEASE_TTL_SECS).unwrap();

        let renewed = renew(&sim, &key, &original, LEASE_TTL_SECS * 2).unwrap();
        assert_eq!(renewed.server_id, "server-a");
        assert!(renewed.expires_at >= original.expires_at);
        assert_eq!(renewed.generation, original.generation);
    }

    #[test]
    fn release_deletes_the_lease() {
        let (sim, _dir) = temp_sim();
        let key = lease_key(1, 42);
        acquire(&sim, &key, "server-a", LEASE_TTL_SECS).unwrap();
        release(&sim, &key).unwrap();

        // After release, another server can acquire immediately.
        let lease = acquire(&sim, &key, "server-b", LEASE_TTL_SECS).unwrap();
        assert_eq!(lease.server_id, "server-b");
    }
}
