//! PITR background task — periodic base manifest materialization.
//!
//! Runs on Tokio. Merges accumulated delta manifests into the latest base
//! manifest at a configurable interval (default 1 hour). Non-fatal: if
//! materialization fails, the error is logged to stderr and the task
//! continues; deltas remain the source of truth.
//!
//! GC (retention enforcement) is the control plane's responsibility and is
//! intentionally absent from this task.

use std::sync::Arc;

use pgsys::Lsn;

use crate::manifest::Manifest;
use crate::project::{ProjectCtx, ProjectNamespace};
use crate::sim_store::SimStore;

// ── Error type ────────────────────────────────────────────────────────────────

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// ── PitrConfig ────────────────────────────────────────────────────────────────

/// Configuration for the PITR background task.
pub struct PitrConfig {
    /// How often to materialize a new base manifest.
    /// Read from `TIKO_PITR_INTERVAL_SECS` (default: 3600 seconds).
    pub materialization_interval: std::time::Duration,
}

impl PitrConfig {
    /// Build config from environment.  Falls back to 3600s if the variable
    /// is absent or cannot be parsed.
    pub fn from_env() -> Self {
        let secs = std::env::var("TIKO_PITR_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3600);
        PitrConfig {
            materialization_interval: std::time::Duration::from_secs(secs),
        }
    }
}

// ── Background task ───────────────────────────────────────────────────────────

/// Tokio task: materialize a new base manifest periodically.
///
/// Runs until the process exits.  Errors are non-fatal — logged to stderr and
/// skipped.  A failed materialization only means the next recovery will replay
/// more deltas; correctness is never compromised.
pub async fn pitr_background_task(sim: Arc<SimStore>, ns: ProjectNamespace, config: PitrConfig) {
    let mut interval = tokio::time::interval(config.materialization_interval);
    loop {
        interval.tick().await;
        if let Err(e) = materialize_base(&sim, &ns) {
            // pg_log_warning must not be called from a Tokio thread (requires
            // PG process-local state).  Use stderr instead.
            eprintln!("s3worker: pitr_background_task: materialize_base failed: {e}");
        }
    }
}

// ── Core materialization ──────────────────────────────────────────────────────

/// Merge all delta manifests newer than the latest base into a new base and
/// upload it to the standard store.
///
/// Returns `Ok(())` immediately if there are no new deltas (idempotent).
/// Does NOT delete delta manifests — cleanup is enforce_retention_org's
/// responsibility.
pub fn materialize_base(sim: &SimStore, ns: &ProjectNamespace) -> Result<()> {
    // Step 1: find latest base manifest LSN.
    let base_prefix = ns.base_prefix();
    let base_keys = sim.list_prefix_standard(&base_prefix)?;

    let mut base_lsns: Vec<Lsn> = base_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&base_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .collect();
    base_lsns.sort();

    // Download the base manifest to a deterministic temp path.
    let base_local_path =
        std::env::temp_dir().join(format!("tiko_pitr_base_{}.tikm", ns.project_id));

    let (base, base_lsn) = if let Some(&l) = base_lsns.last() {
        let manifest_key = ns.base_manifest_key(l);
        let bytes = sim
            .get_standard(&manifest_key)?
            .ok_or_else(|| format!("base manifest not found: {manifest_key}"))?;
        (Manifest::from_bytes(&bytes, &base_local_path)?, l)
    } else {
        // No base yet — bootstrap from empty and merge ALL deltas.
        // Lsn::INVALID == 0, so every real delta LSN passes the `lsn > base_lsn` filter.
        (Manifest::empty(&base_local_path)?, Lsn::INVALID)
    };

    // Step 2: collect delta LSNs strictly newer than the current base.
    let delta_prefix = ns.delta_prefix();
    let delta_keys = sim.list_prefix_standard(&delta_prefix)?;

    let mut delta_lsns: Vec<Lsn> = delta_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&delta_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .filter(|&lsn| lsn > base_lsn)
        .collect();
    delta_lsns.sort();
    delta_lsns.dedup();

    // Step 3: nothing new to merge.
    if delta_lsns.is_empty() {
        return Ok(());
    }

    // Step 4: download each delta manifest.
    let mut deltas = Vec::with_capacity(delta_lsns.len());
    for &delta_lsn in &delta_lsns {
        let key = ns.delta_manifest_key(delta_lsn);
        let delta_bytes = sim
            .get_standard(&key)?
            .ok_or_else(|| format!("delta manifest not found: {key}"))?;
        let delta_path = std::env::temp_dir().join(format!(
            "tiko_pitr_delta_{}_{}.tikm",
            ns.project_id,
            delta_lsn.to_hex()
        ));
        deltas.push(Manifest::from_bytes(&delta_bytes, &delta_path)?);
    }

    // Apply all deltas to the base (in-place two-pointer merge).
    base.apply_deltas(&deltas)?;

    // Step 5: upload the merged base at the new LSN (the last delta's LSN).
    // This is a single atomic write; the new base is valid after this PUT.
    let new_lsn = *delta_lsns.last().unwrap(); // non-empty: checked above
    sim.put_standard(&ns.base_manifest_key(new_lsn), &base.to_bytes()?)?;

    // Step 6: refresh the global manifest in-place.
    // Must run after the S3 PUT so the on-disk index always reflects a committed
    // base.  Skipped if ProjectCtx was never initialised (env vars absent).
    //
    // We pass [base, deltas...] rather than just [deltas] so that the result is
    // always correct regardless of whether ctx.base_manifest is empty (e.g. a
    // fresh process that called bootstrap() instead of load(), which happens
    // during initdb --single because project.json is not yet written).  Passing
    // the loaded SimStore base first is idempotent when ctx already has the
    // correct history: ties at equal LSN keep the in-memory (self) entry.
    if let Some(ctx) = ProjectCtx::try_get() {
        let mut all_updates = vec![base];
        all_updates.extend(deltas);
        ctx.base_manifest.apply_deltas(&all_updates)?;
    }

    // Deltas are intentionally preserved — GC runs on the control plane only.
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ChunkTag;
    use crate::manifest::ChunkRef;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn setup() -> (TempDir, SimStore) {
        let dir = TempDir::new().unwrap();
        let store = SimStore::new(dir.path());
        (dir, store)
    }

    fn ns_with_project(project_id: u64) -> ProjectNamespace {
        ProjectNamespace::new(1001, project_id, 1)
    }

    fn tag(rel: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: rel,
            fork_number: 0,
            chunk_id: 0,
        }
    }

    fn cref(branch_id: u64, lsn: Lsn) -> ChunkRef {
        ChunkRef { branch_id, lsn }
    }

    fn store_manifest(
        sim: &SimStore,
        key: &str,
        lsn: Lsn,
        chunks: Vec<(ChunkTag, ChunkRef)>,
        tmp: &std::path::Path,
    ) {
        let m = Manifest::new(lsn, 0, chunks, HashMap::new(), tmp).unwrap();
        sim.put_standard(key, &m.to_bytes().unwrap()).unwrap();
    }

    // ── Seed sim with 10 delta manifests + a base at delta 3 ─────────────────

    #[test]
    fn materialize_merges_new_deltas_onto_base() {
        let (dir, sim) = setup();
        let ns = ns_with_project(20_001);

        // LSNs for deltas 1–10.
        let lsns: Vec<Lsn> = (1u64..=10).map(|i| Lsn::new(i * 0x100)).collect();

        // Base: covers tags 1–3 at their respective delta LSNs.
        let base_chunks: Vec<(ChunkTag, ChunkRef)> = (1u32..=3)
            .map(|i| (tag(i), cref(1, lsns[i as usize - 1])))
            .collect();
        store_manifest(
            &sim,
            &ns.base_manifest_key(lsns[2]),
            lsns[2],
            base_chunks,
            &dir.path().join("base.tikm"),
        );

        // All 10 delta manifests.  Each delta updates tag(i) and also tag(0).
        for i in 1u32..=10 {
            let delta_lsn = lsns[i as usize - 1];
            let chunks = vec![(tag(i), cref(1, delta_lsn)), (tag(0), cref(1, delta_lsn))];
            store_manifest(
                &sim,
                &ns.delta_manifest_key(delta_lsn),
                delta_lsn,
                chunks,
                &dir.path().join(format!("d{i}.tikm")),
            );
        }

        materialize_base(&sim, &ns).unwrap();

        // New base must exist at lsns[9] (delta 10).
        let bytes = sim
            .get_standard(&ns.base_manifest_key(lsns[9]))
            .unwrap()
            .expect("new base manifest must exist after materialization");
        let tmp = dir.path().join("verify.tikm");
        let merged = Manifest::from_bytes(&bytes, &tmp).unwrap();

        // tag(7) was only in delta 7 → must be present.
        assert_eq!(merged.lookup(&tag(7)).unwrap(), Some(cref(1, lsns[6])));
        // tag(0) updated by every delta → must reflect delta 10 (highest LSN).
        assert_eq!(merged.lookup(&tag(0)).unwrap(), Some(cref(1, lsns[9])));

        // All 10 delta files must still be present (no GC in this task).
        for i in 1u32..=10 {
            assert!(
                sim.get_standard(&ns.delta_manifest_key(lsns[i as usize - 1]))
                    .unwrap()
                    .is_some(),
                "delta {i} must not be deleted by materialize_base"
            );
        }
    }

    // ── After materialization, lookup returns correct ChunkRef ────────────────

    #[test]
    fn lookup_correct_chunk_ref_after_materialization() {
        let (dir, sim) = setup();
        let ns = ns_with_project(20_002);

        let base_lsn = Lsn::new(0x100);
        let d4_lsn = Lsn::new(0x400);
        let d7_lsn = Lsn::new(0x700);

        // Base at LSN 0x300 covering tags 1–3.
        store_manifest(
            &sim,
            &ns.base_manifest_key(Lsn::new(0x300)),
            Lsn::new(0x300),
            vec![
                (tag(1), cref(1, base_lsn)),
                (tag(2), cref(1, base_lsn)),
                (tag(3), cref(1, base_lsn)),
            ],
            &dir.path().join("base.tikm"),
        );

        // Delta at 0x400: updates tag(2).
        store_manifest(
            &sim,
            &ns.delta_manifest_key(d4_lsn),
            d4_lsn,
            vec![(tag(2), cref(1, d4_lsn))],
            &dir.path().join("d4.tikm"),
        );

        // Delta at 0x700: adds tag(7).
        store_manifest(
            &sim,
            &ns.delta_manifest_key(d7_lsn),
            d7_lsn,
            vec![(tag(7), cref(1, d7_lsn))],
            &dir.path().join("d7.tikm"),
        );

        materialize_base(&sim, &ns).unwrap();

        let bytes = sim
            .get_standard(&ns.base_manifest_key(d7_lsn))
            .unwrap()
            .expect("new base at d7_lsn must exist");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();

        assert_eq!(m.lookup(&tag(1)).unwrap(), Some(cref(1, base_lsn)));
        assert_eq!(m.lookup(&tag(2)).unwrap(), Some(cref(1, d4_lsn)));
        assert_eq!(m.lookup(&tag(7)).unwrap(), Some(cref(1, d7_lsn)));
    }

    // ── Idempotent: running twice produces same result ────────────────────────

    #[test]
    fn materialize_idempotent() {
        let (dir, sim) = setup();
        let ns = ns_with_project(20_003);

        let base_lsn = Lsn::new(0x100);
        let delta_lsn = Lsn::new(0x200);

        store_manifest(
            &sim,
            &ns.base_manifest_key(base_lsn),
            base_lsn,
            vec![(tag(1), cref(1, base_lsn))],
            &dir.path().join("base.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(delta_lsn),
            delta_lsn,
            vec![(tag(1), cref(1, delta_lsn))],
            &dir.path().join("d1.tikm"),
        );

        // First materialization.
        materialize_base(&sim, &ns).unwrap();

        // Second materialization: no new deltas after the newly written base.
        materialize_base(&sim, &ns).unwrap();

        // Exactly 2 base manifests: original + the materialized one.
        let keys = sim.list_prefix_standard(&ns.base_prefix()).unwrap();
        assert_eq!(keys.len(), 2, "second run must not write another base");

        // Result unchanged from first run.
        let bytes = sim
            .get_standard(&ns.base_manifest_key(delta_lsn))
            .unwrap()
            .expect("materialized base must exist");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();
        assert_eq!(m.lookup(&tag(1)).unwrap(), Some(cref(1, delta_lsn)));
    }

    // ── No new deltas: no new base written ───────────────────────────────────

    #[test]
    fn materialize_no_new_deltas_is_noop() {
        let (dir, sim) = setup();
        let ns = ns_with_project(20_004);

        let lsn = Lsn::new(0x100);
        store_manifest(
            &sim,
            &ns.base_manifest_key(lsn),
            lsn,
            vec![(tag(1), cref(1, lsn))],
            &dir.path().join("base.tikm"),
        );

        // No deltas written.
        materialize_base(&sim, &ns).unwrap();

        let keys = sim.list_prefix_standard(&ns.base_prefix()).unwrap();
        assert_eq!(
            keys.len(),
            1,
            "no new base should be written when no deltas"
        );
    }

    // ── No base at all, no deltas: still returns Ok ──────────────────────────

    #[test]
    fn materialize_no_base_returns_ok() {
        let (_dir, sim) = setup();
        let ns = ns_with_project(20_005);
        // No base, no deltas.
        materialize_base(&sim, &ns).unwrap();
        // No base manifest should be written either.
        let keys = sim.list_prefix_standard(&ns.base_prefix()).unwrap();
        assert!(
            keys.is_empty(),
            "no base should be written when no deltas exist"
        );
    }

    // ── No base + deltas: first base is bootstrapped from scratch ─────────────

    #[test]
    fn materialize_no_base_with_deltas_creates_first_base() {
        let (dir, sim) = setup();
        let ns = ns_with_project(20_006);

        let d1_lsn = Lsn::new(0x100);
        let d2_lsn = Lsn::new(0x200);

        // Two delta manifests — no base exists yet.
        store_manifest(
            &sim,
            &ns.delta_manifest_key(d1_lsn),
            d1_lsn,
            vec![(tag(1), cref(1, d1_lsn))],
            &dir.path().join("d1.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(d2_lsn),
            d2_lsn,
            vec![(tag(2), cref(1, d2_lsn))],
            &dir.path().join("d2.tikm"),
        );

        materialize_base(&sim, &ns).unwrap();

        // Base must be created at the highest delta LSN.
        let bytes = sim
            .get_standard(&ns.base_manifest_key(d2_lsn))
            .unwrap()
            .expect("initial base manifest must exist after materialization");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();

        // Both deltas must be present.
        assert_eq!(m.lookup(&tag(1)).unwrap(), Some(cref(1, d1_lsn)));
        assert_eq!(m.lookup(&tag(2)).unwrap(), Some(cref(1, d2_lsn)));
    }
}
