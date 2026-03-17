//! GC / retention enforcement — delta GC, base GC, WAL GC, chunk GC.
//!
//! Entry point: `enforce_retention_org(sim, org_id, server_id, max_checkpoints)`.
//! Acquires a per-org GC lease before doing any work so that only one server
//! runs GC for a given org at a time.
//!
//! Retention policy: keep the last `max_checkpoints` delta manifests per
//! project.  Cutoff derivation:
//!
//! ```text
//! sorted_delta_lsns = all delta LSNs, ascending
//! if len > max_checkpoints:
//!     cutoff_lsn = sorted_delta_lsns[len - max_checkpoints]
//! else:
//!     skip (nothing to GC)
//! ```
//!
//! Four GC phases (in order):
//!
//! | Phase      | What is deleted                                            |
//! |------------|------------------------------------------------------------|
//! | Delta      | `manifest.bin` + `pg_state.tar.zst` with LSN < cutoff     |
//! | Base       | All bases with `base_lsn < cutoff`, except the newest one  |
//! | WAL        | Segments whose end LSN < `cutoff_lsn`                      |
//! | Chunk      | Versioned chunks not referenced by any retained manifest;  |
//! |            | zero-branch objects are permanent                          |

use std::collections::HashSet;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use pgsys::Lsn;
use store::manifest::Manifest;
use store::org;
use store::project::{ProjectMeta, ProjectNamespace};
use store::sim_store::SimStore;

// ── Constants ─────────────────────────────────────────────────────────────────

/// GC lease TTL in seconds.
const GC_LEASE_TTL_SECS: i64 = 300;

/// Standard PostgreSQL WAL segment size (16 MiB).
const WAL_SEGMENT_SIZE: u64 = 0x100_0000;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Error {
    Store(io::Error),
    Serialize(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(e) => write!(f, "store error: {e}"),
            Error::Serialize(s) => write!(f, "serialize error: {s}"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Store(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── GC lease ──────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct GcLease {
    server_id: String,
    expires_at: i64,
}

fn gc_lease_key(org_id: u64) -> String {
    format!("{}/gc_lease.json", org_id)
}

/// Try to acquire the per-org GC lease for `server_id`.
/// Returns `true` on success, `false` if another server holds a valid lease.
fn try_acquire_gc_lease(sim: &SimStore, org_id: u64, server_id: &str) -> Result<bool> {
    let key = gc_lease_key(org_id);
    let now = now_secs();

    if let Some(bytes) = sim.get_standard(&key)? {
        if let Ok(existing) = serde_json::from_slice::<GcLease>(&bytes) {
            if existing.expires_at > now && existing.server_id != server_id {
                return Ok(false);
            }
        }
    }

    let lease = GcLease {
        server_id: server_id.to_owned(),
        expires_at: now + GC_LEASE_TTL_SECS,
    };
    let json = serde_json::to_vec(&lease).map_err(|e| Error::Serialize(e.to_string()))?;
    sim.put_standard(&key, &json)?;
    Ok(true)
}

fn release_gc_lease(sim: &SimStore, org_id: u64) {
    let _ = sim.delete_standard(&gc_lease_key(org_id));
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Run all GC phases for every project under `org_id`.
///
/// Skipped when another server holds the per-org GC lease.
///
/// Special cases handled before per-project GC:
/// - `org.json` with `deleted_at` set: physically remove all `{org}/` objects.
/// - `project.json` with `deleted_at` set: physically remove that branch's objects.
pub fn enforce_retention_org(
    sim: &SimStore,
    org_id: u64,
    server_id: &str,
    max_checkpoints: usize,
) -> Result<()> {
    if !try_acquire_gc_lease(sim, org_id, server_id)? {
        return Ok(());
    }
    let _guard = GcLeaseGuard { sim, org_id };

    // ── Org-level soft-delete ─────────────────────────────────────────────────
    if let Ok(org_meta) = org::OrgMeta::get(sim, org_id) {
        if org_meta.deleted_at.is_some() {
            return enforce_org_delete(sim, org_id);
        }
    }

    // ── Per-project GC ────────────────────────────────────────────────────────
    let prefix = format!("{}/metadata/", org_id);
    let meta_keys = sim.list_prefix_standard(&prefix)?;

    for key in &meta_keys {
        if !key.ends_with("/project.json") {
            continue;
        }
        let Some(bytes) = sim.get_standard(key)? else {
            continue;
        };
        let Ok(meta) = serde_json::from_slice::<ProjectMeta>(&bytes) else {
            continue;
        };
        if meta.deleted_at.is_some() {
            enforce_branch_delete(sim, &meta.ns)?;
        } else {
            enforce_retention_project(sim, &meta.ns, max_checkpoints)?;
        }
    }

    Ok(())
}

// ── Internal ──────────────────────────────────────────────────────────────────

/// Physically delete all objects under `{org}/` for a soft-deleted org.
fn enforce_org_delete(sim: &SimStore, org_id: u64) -> Result<()> {
    let prefix = format!("{}/", org_id);
    for key in sim.list_prefix_standard(&prefix)? {
        sim.delete_standard(&key)?;
    }
    for key in sim.list_prefix_express(&prefix)? {
        sim.delete_express(&key)?;
    }
    Ok(())
}

/// Physically remove all objects for a soft-deleted branch.
///
/// Delegates express + PITR + metadata removal to `store::project::delete_branch`,
/// then also removes the versioned chunk objects in `{org}/chunks/{branch_id}/`.
fn enforce_branch_delete(sim: &SimStore, ns: &ProjectNamespace) -> Result<()> {
    store::project::delete_branch(sim, ns).map_err(|e| Error::Other(e.to_string()))?;

    // Versioned chunk objects are the control-plane's responsibility.
    let chunks_prefix = format!("{}/chunks/{}/", ns.org_id, ns.branch_id);
    for key in sim.list_prefix_standard(&chunks_prefix)? {
        sim.delete_standard(&key)?;
    }
    Ok(())
}

/// Run the four retention-GC phases for a single live project.
fn enforce_retention_project(
    sim: &SimStore,
    ns: &ProjectNamespace,
    max_checkpoints: usize,
) -> Result<()> {
    let prefix = ns.delta_prefix();
    let keys = sim.list_prefix_standard(&prefix)?;

    let mut delta_lsns: Vec<Lsn> = keys
        .iter()
        .filter(|k| k.ends_with("/manifest.bin"))
        .filter_map(|k| {
            let rel = k.strip_prefix(&prefix)?;
            // rel = "{tl:08X}/{lsn_hex}/manifest.bin"
            let lsn_hex = rel.split('/').nth(1)?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .collect();
    delta_lsns.sort();

    if delta_lsns.len() <= max_checkpoints {
        return Ok(());
    }

    // Oldest LSN that must be retained; everything strictly below is eligible.
    let cutoff_lsn = delta_lsns[delta_lsns.len() - max_checkpoints];

    gc_delta(sim, ns, cutoff_lsn)?;
    gc_base(sim, ns, cutoff_lsn)?;
    gc_wal(sim, ns, cutoff_lsn)?;
    gc_chunks(sim, ns, cutoff_lsn)?;

    Ok(())
}

/// Delete delta manifests + pg_state for LSN < `cutoff_lsn`.
fn gc_delta(sim: &SimStore, ns: &ProjectNamespace, cutoff_lsn: Lsn) -> Result<()> {
    let prefix = ns.delta_prefix();
    for key in sim.list_prefix_standard(&prefix)? {
        let rel = key.strip_prefix(&prefix).unwrap_or(&key);
        // rel = "{tl:08X}/{lsn_hex}/{manifest.bin or pg_state.tar.zst}"
        let lsn_hex = rel.split('/').nth(1).unwrap_or("");
        if let Ok(lsn) = Lsn::from_hex(lsn_hex) {
            if lsn < cutoff_lsn {
                sim.delete_standard(&key)?;
            }
        }
    }
    Ok(())
}

/// Delete base manifests below cutoff, keeping the newest as recovery anchor.
///
/// Among all bases with `base_lsn < cutoff_lsn`: keep the one with the
/// highest LSN; delete the rest.  Bases with `base_lsn >= cutoff_lsn` are
/// never touched.
fn gc_base(sim: &SimStore, ns: &ProjectNamespace, cutoff_lsn: Lsn) -> Result<()> {
    let prefix = ns.base_prefix();
    let keys = sim.list_prefix_standard(&prefix)?;

    // Collect (lsn, key) for all base manifests strictly below cutoff.
    let mut below: Vec<(Lsn, String)> = keys
        .iter()
        .filter(|k| k.ends_with("/manifest.bin"))
        .filter_map(|k| {
            let rel = k.strip_prefix(&prefix)?;
            let lsn_hex = rel.split('/').nth(1)?;
            let lsn = Lsn::from_hex(lsn_hex).ok()?;
            (lsn < cutoff_lsn).then_some((lsn, k.clone()))
        })
        .collect();
    below.sort_by_key(|(lsn, _)| *lsn);

    // Delete all but the newest (last in sorted order).
    if below.len() > 1 {
        for (_, key) in &below[..below.len() - 1] {
            sim.delete_standard(key)?;
        }
    }
    Ok(())
}

/// Delete WAL segments whose end LSN < `cutoff_lsn`.
///
/// WAL segment names follow PG's `{TL:08X}{HI:08X}{LO_SEG:08X}` format (24 hex
/// chars).  `end_lsn = (HI << 32 | LO_SEG * WAL_SEGMENT_SIZE) + WAL_SEGMENT_SIZE`.
fn gc_wal(sim: &SimStore, ns: &ProjectNamespace, cutoff_lsn: Lsn) -> Result<()> {
    let wal_prefix = format!("{}/pitr/{}/wal/", ns.org_id, ns.project_id);
    for key in sim.list_prefix_standard(&wal_prefix)? {
        // Key = "{wal_prefix}{tl:08X}/{segment}"
        let rel = key.strip_prefix(&wal_prefix).unwrap_or("");
        let segment = rel.split('/').nth(1).unwrap_or("");
        if let Some(end_lsn) = parse_wal_end_lsn(segment) {
            if end_lsn < cutoff_lsn {
                sim.delete_standard(&key)?;
            }
        }
    }
    Ok(())
}

/// Delete versioned chunk objects not referenced by any retained manifest.
///
/// Zero-branch (`branch_id = 0`) objects are permanent and are never deleted.
fn gc_chunks(sim: &SimStore, ns: &ProjectNamespace, cutoff_lsn: Lsn) -> Result<()> {
    let retained = collect_retained_chunk_keys(sim, ns, cutoff_lsn)?;
    let chunks_prefix = format!("{}/chunks/", ns.org_id);

    for key in sim.list_prefix_standard(&chunks_prefix)? {
        // Skip zero-branch objects (permanent).
        let rel = key.strip_prefix(&chunks_prefix).unwrap_or("");
        let branch_id_str = rel.split('/').next().unwrap_or("0");
        if branch_id_str == "0" {
            continue;
        }
        if !retained.contains(key.as_str()) {
            sim.delete_standard(&key)?;
        }
    }
    Ok(())
}

/// Build the set of versioned chunk S3 keys referenced by all retained manifests.
///
/// Retained manifests:
/// - All delta manifests with `lsn >= cutoff_lsn`
/// - The newest base with `base_lsn <= cutoff_lsn` (recovery anchor)
/// - Any bases with `base_lsn > cutoff_lsn` (created by post-cutoff PITR)
fn collect_retained_chunk_keys(
    sim: &SimStore,
    ns: &ProjectNamespace,
    cutoff_lsn: Lsn,
) -> Result<HashSet<String>> {
    let work_dir = tempfile::TempDir::new()?;
    let mut retained = HashSet::new();
    let mut idx = 0usize;

    // ── Retained delta manifests ──────────────────────────────────────────────
    let delta_prefix = ns.delta_prefix();
    for key in sim.list_prefix_standard(&delta_prefix)? {
        if !key.ends_with("/manifest.bin") {
            continue;
        }
        let rel = key.strip_prefix(&delta_prefix).unwrap_or(&key);
        let lsn_hex = rel.split('/').nth(1).unwrap_or("");
        let Ok(lsn) = Lsn::from_hex(lsn_hex) else {
            continue;
        };
        if lsn < cutoff_lsn {
            continue;
        }
        let Some(bytes) = sim.get_standard(&key)? else {
            continue;
        };
        idx += 1;
        let path = work_dir.path().join(format!("{idx}.tikm"));
        if let Ok(m) = Manifest::from_bytes(&bytes, &path) {
            if let Ok(entries) = m.entries() {
                for (tag, cref) in entries {
                    retained.insert(ns.chunk_versioned_key(
                        &tag,
                        cref.branch_id,
                        cref.timeline_id,
                        cref.lsn,
                    ));
                }
            }
        }
    }

    // ── Retained base manifests ───────────────────────────────────────────────
    let base_prefix = ns.base_prefix();
    let base_keys = sim.list_prefix_standard(&base_prefix)?;

    // Anchor: the newest base with base_lsn <= cutoff_lsn.
    let anchor: Option<String> = base_keys
        .iter()
        .filter(|k| k.ends_with("/manifest.bin"))
        .filter_map(|k| {
            let rel = k.strip_prefix(&base_prefix)?;
            let lsn_hex = rel.split('/').nth(1)?;
            let lsn = Lsn::from_hex(lsn_hex).ok()?;
            (lsn <= cutoff_lsn).then_some((lsn, k.clone()))
        })
        .max_by_key(|(lsn, _)| *lsn)
        .map(|(_, k)| k);

    for key in &base_keys {
        if !key.ends_with("/manifest.bin") {
            continue;
        }
        // Keep: the anchor base OR any base above cutoff.
        let is_anchor = anchor.as_deref() == Some(key.as_str());
        let is_above_cutoff = {
            let rel = key.strip_prefix(&base_prefix).unwrap_or("");
            let lsn_hex = rel.split('/').nth(1).unwrap_or("");
            Lsn::from_hex(lsn_hex).map_or(false, |lsn| lsn > cutoff_lsn)
        };
        if !is_anchor && !is_above_cutoff {
            continue;
        }
        let Some(bytes) = sim.get_standard(key)? else {
            continue;
        };
        idx += 1;
        let path = work_dir.path().join(format!("{idx}.tikm"));
        if let Ok(m) = Manifest::from_bytes(&bytes, &path) {
            if let Ok(entries) = m.entries() {
                for (tag, cref) in entries {
                    retained.insert(ns.chunk_versioned_key(
                        &tag,
                        cref.branch_id,
                        cref.timeline_id,
                        cref.lsn,
                    ));
                }
            }
        }
    }

    Ok(retained)
}

/// Parse the end LSN of a WAL segment from its 24-char hex filename.
///
/// Format: `{TL:08X}{HI:08X}{LO_SEG:08X}`
/// `end_lsn = (HI << 32 | LO_SEG * WAL_SEGMENT_SIZE) + WAL_SEGMENT_SIZE`
fn parse_wal_end_lsn(segment: &str) -> Option<Lsn> {
    if segment.len() != 24 || !segment.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let hi = u64::from_str_radix(&segment[8..16], 16).ok()?;
    let lo_seg = u64::from_str_radix(&segment[16..24], 16).ok()?;
    let start = (hi << 32) | lo_seg.checked_mul(WAL_SEGMENT_SIZE)?;
    Some(Lsn::new(start.checked_add(WAL_SEGMENT_SIZE)?))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// RAII guard that releases the GC lease on drop.
struct GcLeaseGuard<'a> {
    sim: &'a SimStore,
    org_id: u64,
}

impl Drop for GcLeaseGuard<'_> {
    fn drop(&mut self) {
        release_gc_lease(self.sim, self.org_id);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use store::ChunkTag;
    use store::manifest::ChunkRef;
    use store::project::ensure_root_project_meta;
    use tempfile::TempDir;

    fn temp_sim() -> (SimStore, TempDir) {
        let dir = TempDir::new().unwrap();
        (SimStore::new(dir.path()), dir)
    }

    fn root_ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 10, 1)
    }

    // Write a minimal delta manifest and return its bytes.
    fn write_delta(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lsn: Lsn) {
        write_delta_with_chunks(sim, ns, tl, lsn, &[]);
    }

    fn write_delta_with_chunks(
        sim: &SimStore,
        ns: &ProjectNamespace,
        tl: u32,
        lsn: Lsn,
        chunks: &[(ChunkTag, ChunkRef)],
    ) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("m.tikm");
        let m = Manifest::new(lsn, 0, chunks.to_vec(), HashMap::new(), &path).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(&ns.delta_manifest_key(tl, lsn), &bytes)
            .unwrap();
    }

    fn write_base(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lsn: Lsn) {
        write_base_with_chunks(sim, ns, tl, lsn, &[]);
    }

    fn write_base_with_chunks(
        sim: &SimStore,
        ns: &ProjectNamespace,
        tl: u32,
        lsn: Lsn,
        chunks: &[(ChunkTag, ChunkRef)],
    ) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("m.tikm");
        let m = Manifest::new(lsn, 0, chunks.to_vec(), HashMap::new(), &path).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(&ns.base_manifest_key(tl, lsn), &bytes)
            .unwrap();
    }

    fn tag(chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 0,
            db_oid: 0,
            rel_number: 0,
            fork_number: 0,
            chunk_id,
        }
    }

    fn cref(branch_id: u64, tl: u32, lsn: u64) -> ChunkRef {
        ChunkRef {
            branch_id,
            timeline_id: tl,
            lsn: Lsn::new(lsn),
        }
    }

    // ── delta GC ──────────────────────────────────────────────────────────────

    #[test]
    fn delta_gc_deletes_below_cutoff_keeps_max_checkpoints() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // Write 6 deltas at LSNs 1000..6000. max_checkpoints=4 → cutoff=3000.
        for lsn_val in [0x1000u64, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000] {
            write_delta(&sim, &ns, 1, Lsn::new(lsn_val));
        }

        enforce_retention_project(&sim, &ns, 4).unwrap();

        // Deltas at 0x1000 and 0x2000 are below cutoff (0x3000) → deleted.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(0x1000)))
                .unwrap()
                .is_none()
        );
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(0x2000)))
                .unwrap()
                .is_none()
        );
        // Deltas at 0x3000..0x6000 retained (4 checkpoints).
        for lsn_val in [0x3000u64, 0x4000, 0x5000, 0x6000] {
            assert!(
                sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(lsn_val)))
                    .unwrap()
                    .is_some(),
                "LSN {lsn_val:#x} must be retained"
            );
        }
    }

    #[test]
    fn delta_gc_skipped_when_within_max_checkpoints() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        write_delta(&sim, &ns, 1, Lsn::new(0x1000));
        write_delta(&sim, &ns, 1, Lsn::new(0x2000));

        enforce_retention_project(&sim, &ns, 10).unwrap();

        // Both deltas must still be present (len=2 ≤ max_checkpoints=10).
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(0x1000)))
                .unwrap()
                .is_some()
        );
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(0x2000)))
                .unwrap()
                .is_some()
        );
    }

    // ── base GC ───────────────────────────────────────────────────────────────

    #[test]
    fn base_gc_keeps_newest_base_below_cutoff_as_anchor() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // 6 deltas → cutoff = 3000.
        for lsn_val in [0x1000u64, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000] {
            write_delta(&sim, &ns, 1, Lsn::new(lsn_val));
        }
        // Bases: 0x1000 and 0x2000 are below cutoff; 0x2000 is the newest.
        write_base(&sim, &ns, 1, Lsn::new(0x1000));
        write_base(&sim, &ns, 1, Lsn::new(0x2000));

        enforce_retention_project(&sim, &ns, 4).unwrap();

        // Oldest base (0x1000) deleted; newest anchor (0x2000) kept.
        assert!(
            sim.get_standard(&ns.base_manifest_key(1, Lsn::new(0x1000)))
                .unwrap()
                .is_none(),
            "oldest base must be deleted"
        );
        assert!(
            sim.get_standard(&ns.base_manifest_key(1, Lsn::new(0x2000)))
                .unwrap()
                .is_some(),
            "newest anchor base must be retained"
        );
    }

    // ── WAL GC ────────────────────────────────────────────────────────────────

    /// Write a WAL segment key whose start LSN is `lo_seg * WAL_SEGMENT_SIZE`.
    fn write_wal(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lo_seg: u64) {
        let segment = format!("{tl:08X}{:08X}{lo_seg:08X}", 0u64);
        let key = ns.wal_key(tl, &segment);
        sim.put_standard(&key, b"wal_data").unwrap();
    }

    #[test]
    fn wal_gc_deletes_segments_with_end_lsn_below_cutoff() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // cutoff_lsn = 0x3000000 (segment lo_seg=3 starts at 0x3000000).
        // 6 deltas → cutoff = Lsn::new(0x3000000) when using those exact values.
        // Use max_checkpoints=4, 6 deltas at 0x1000000,0x2000000,...0x6000000.
        for i in 1u64..=6 {
            write_delta(&sim, &ns, 1, Lsn::new(i * WAL_SEGMENT_SIZE));
        }
        // WAL segments: lo_seg 1 (end=0x2000000), 2 (end=0x3000000), 3 (end=0x4000000).
        write_wal(&sim, &ns, 1, 1); // end = 0x2000000 < cutoff=0x3000000 → DELETE
        write_wal(&sim, &ns, 1, 2); // end = 0x3000000, NOT < cutoff         → KEEP
        write_wal(&sim, &ns, 1, 3); // end = 0x4000000 > cutoff              → KEEP

        enforce_retention_project(&sim, &ns, 4).unwrap();

        // Segment lo_seg=1 must be deleted, lo_seg=2 and 3 retained.
        let seg1 = format!("{:08X}{:08X}{:08X}", 1u32, 0u64, 1u64);
        let seg2 = format!("{:08X}{:08X}{:08X}", 1u32, 0u64, 2u64);
        let seg3 = format!("{:08X}{:08X}{:08X}", 1u32, 0u64, 3u64);
        assert!(
            sim.get_standard(&ns.wal_key(1, &seg1)).unwrap().is_none(),
            "seg1 must be deleted"
        );
        assert!(
            sim.get_standard(&ns.wal_key(1, &seg2)).unwrap().is_some(),
            "seg2 must be kept"
        );
        assert!(
            sim.get_standard(&ns.wal_key(1, &seg3)).unwrap().is_some(),
            "seg3 must be kept"
        );
    }

    // ── chunk GC ──────────────────────────────────────────────────────────────

    #[test]
    fn chunk_gc_removes_unreferenced_chunks_keeps_referenced() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // 6 deltas → cutoff=0x3000. Delta at 0x3000 references chunk A.
        let chunk_a = (tag(0), cref(ns.branch_id, 1, 0x3000));
        for lsn_val in [0x1000u64, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000] {
            if lsn_val == 0x3000 {
                write_delta_with_chunks(&sim, &ns, 1, Lsn::new(lsn_val), &[chunk_a]);
            } else {
                write_delta(&sim, &ns, 1, Lsn::new(lsn_val));
            }
        }

        let key_a = ns.chunk_versioned_key(
            &chunk_a.0,
            chunk_a.1.branch_id,
            chunk_a.1.timeline_id,
            chunk_a.1.lsn,
        );
        let orphan_key = ns.chunk_versioned_key(&tag(99), ns.branch_id, 1, Lsn::new(0x1000));

        sim.put_standard(&key_a, b"chunk_a").unwrap();
        sim.put_standard(&orphan_key, b"orphan").unwrap();

        enforce_retention_project(&sim, &ns, 4).unwrap();

        assert!(
            sim.get_standard(&key_a).unwrap().is_some(),
            "referenced chunk must be kept"
        );
        assert!(
            sim.get_standard(&orphan_key).unwrap().is_none(),
            "orphan chunk must be deleted"
        );
    }

    // ── GC lease ──────────────────────────────────────────────────────────────

    #[test]
    fn gc_skipped_when_another_server_holds_lease() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // Write 6 deltas so GC would normally run.
        ensure_root_project_meta(&sim, &ns).unwrap();
        for lsn_val in [0x1000u64, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000] {
            write_delta(&sim, &ns, 1, Lsn::new(lsn_val));
        }

        // Another server acquires the lease.
        assert!(try_acquire_gc_lease(&sim, ns.org_id, "other-server").unwrap());

        // Our server's GC must be a no-op.
        enforce_retention_org(&sim, ns.org_id, "our-server", 4).unwrap();

        // All 6 deltas still present — GC was skipped.
        for lsn_val in [0x1000u64, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000] {
            assert!(
                sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(lsn_val)))
                    .unwrap()
                    .is_some(),
                "delta at {lsn_val:#x} must be untouched when GC is skipped"
            );
        }
    }

    // ── org + branch soft-delete ──────────────────────────────────────────────

    #[test]
    fn org_delete_removes_all_org_objects() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // Create org + project metadata.
        store::org::OrgMeta::create(&sim, ns.org_id).unwrap();
        write_delta(&sim, &ns, 1, Lsn::new(0x1000));
        sim.put_express(&format!("{}/{}/hot", ns.org_id, ns.project_id), b"x")
            .unwrap();

        // Soft-delete the org.
        store::org::OrgMeta::delete(&sim, ns.org_id, false).unwrap();

        // GC should physically remove everything.
        enforce_retention_org(&sim, ns.org_id, "server-1", 500).unwrap();

        // Nothing should remain under {org}/.
        let std_keys = sim
            .list_prefix_standard(&format!("{}/", ns.org_id))
            .unwrap();
        let exp_keys = sim.list_prefix_express(&format!("{}/", ns.org_id)).unwrap();
        assert!(
            std_keys.is_empty(),
            "all standard objects must be removed for deleted org"
        );
        assert!(
            exp_keys.is_empty(),
            "all express objects must be removed for deleted org"
        );
    }

    #[test]
    fn branch_delete_removes_branch_objects() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // Set up a live org + soft-deleted branch.
        store::org::OrgMeta::create(&sim, ns.org_id).unwrap();
        store::project::ensure_root_project_meta(&sim, &ns).unwrap();
        write_delta(&sim, &ns, 1, Lsn::new(0x1000));
        // Write a versioned chunk for this branch.
        let chunk_key = ns.chunk_versioned_key(&tag(0), ns.branch_id, 1, Lsn::new(0x1000));
        sim.put_standard(&chunk_key, b"chunk").unwrap();

        // Soft-delete the branch via lifecycle::delete_branch.
        crate::lifecycle::delete_branch(&sim, &ns).unwrap();

        // GC should physically remove branch objects.
        enforce_retention_org(&sim, ns.org_id, "server-1", 500).unwrap();

        // PITR data (delta manifest) and chunk must be gone.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, Lsn::new(0x1000)))
                .unwrap()
                .is_none(),
            "PITR data must be removed for soft-deleted branch"
        );
        assert!(
            sim.get_standard(&chunk_key).unwrap().is_none(),
            "chunk objects must be removed for soft-deleted branch"
        );
    }
}
