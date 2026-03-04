//! Project context — runtime identity and base manifest for the running project.
//!
//! `ProjectCtx` is populated once at s3worker startup and held in
//! `PROJECT_CTX` for the duration of the process lifetime.
//! The `base_manifest` field is file-backed (TIKM format) and supports
//! concurrent lookups via binary search + direct `pread` — no in-memory
//! page cache beyond what the OS provides.

use std::io;
use std::path::Path;
use std::sync::OnceLock;

use pgsys::Lsn;
use serde::{Deserialize, Serialize};

use crate::cache::ChunkTag;
use crate::manifest::{ChunkRef, Manifest};
use crate::sim_store::SimStore;

/// Global project context for the running s3worker process.
static PROJECT_CTX: OnceLock<ProjectCtx> = OnceLock::new();

// ── Error type ────────────────────────────────────────────────────────────────

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

// ── ProjectNamespace ──────────────────────────────────────────────────────────

/// Stateless S3 key formatter for a specific project/branch identity.
///
/// All key methods produce strings relative to the bucket root (no leading `/`).
/// Express-bucket keys are scoped to `{org}/{proj}/`.
/// Standard-bucket chunk keys are org-scoped with `{branch_id}` (not `project_id`).
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct ProjectNamespace {
    pub org_id: u64,
    pub project_id: u64,
    /// Org-scoped branch identifier used for standard-bucket chunk paths.
    pub branch_id: u64,
}

impl ProjectNamespace {
    pub fn new(org_id: u64, project_id: u64, branch_id: u64) -> Self {
        ProjectNamespace {
            org_id,
            project_id,
            branch_id,
        }
    }

    // ── Express-bucket keys ───────────────────────────────────────────────

    /// `{org}/{proj}/chunks/{chunk_path}/latest`
    pub fn chunk_latest_key(&self, tag: &ChunkTag) -> String {
        format!(
            "{}/{}/chunks/{}/latest",
            self.org_id,
            self.project_id,
            tag.to_path()
        )
    }

    /// `{org}/{proj}/chunks/{chunk_path}/.staging_{lsn_hex}`
    pub fn chunk_staging_key(&self, tag: &ChunkTag, lsn: Lsn) -> String {
        format!(
            "{}/{}/chunks/{}/.staging_{}",
            self.org_id,
            self.project_id,
            tag.to_path(),
            lsn.to_hex()
        )
    }

    // ── Standard-bucket chunk keys (org-level, keyed by branch_id) ───────

    /// `{org}/chunks/{branch_id}/{chunk_path}/{lsn_hex}`
    pub fn chunk_versioned_key(&self, tag: &ChunkTag, lsn: Lsn) -> String {
        format!(
            "{}/chunks/{}/{}/{}",
            self.org_id,
            self.branch_id,
            tag.to_path(),
            lsn.to_hex()
        )
    }

    // ── Standard-bucket PITR manifest keys (per-project) ─────────────────

    /// `{org}/pitr/{proj}/deltas/{lsn_hex}/manifest.bin`
    pub fn delta_manifest_key(&self, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/deltas/{}/manifest.bin",
            self.org_id,
            self.project_id,
            lsn.to_hex()
        )
    }

    /// `{org}/pitr/{proj}/bases/{lsn_hex}/manifest.bin`
    pub fn base_manifest_key(&self, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/bases/{}/manifest.bin",
            self.org_id,
            self.project_id,
            lsn.to_hex()
        )
    }

    /// `{org}/pitr/{proj}/deltas/{lsn_hex}/pg_state.tar.zst`
    pub fn pg_state_key(&self, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/deltas/{}/pg_state.tar.zst",
            self.org_id,
            self.project_id,
            lsn.to_hex()
        )
    }

    // ── Standard-bucket WAL and metadata keys ────────────────────────────

    /// `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}`
    pub fn wal_key(&self, timeline: u32, segment: &str) -> String {
        format!(
            "{}/pitr/{}/wal/{:08X}/{}",
            self.org_id, self.project_id, timeline, segment
        )
    }

    /// `{org}/metadata/{proj}/project.json`
    pub fn project_meta_key(&self) -> String {
        format!("{}/metadata/{}/project.json", self.org_id, self.project_id)
    }

    // ── List prefixes (for scanning) ──────────────────────────────────────

    /// `{org}/pitr/{proj}/deltas/`
    pub fn delta_prefix(&self) -> String {
        format!("{}/pitr/{}/deltas/", self.org_id, self.project_id)
    }

    /// `{org}/pitr/{proj}/bases/`
    pub fn base_prefix(&self) -> String {
        format!("{}/pitr/{}/bases/", self.org_id, self.project_id)
    }
}

// ── ProjectMeta ───────────────────────────────────────────────────────────────

/// Mirrors `metadata/{project_id}/project.json`.
///
/// The three identity fields (`org_id`, `project_id`, `branch_id`) live inside
/// the embedded `ProjectNamespace`; `#[serde(flatten)]` keeps the JSON
/// representation flat so the on-disk format is unchanged.
#[derive(Serialize, Deserialize, Clone)]
pub struct ProjectMeta {
    #[serde(flatten)]
    pub ns: ProjectNamespace,
    pub parent_project_id: Option<u64>,
    pub parent_branch_id: Option<u64>,
    pub branch_checkpoint_lsn: Option<Lsn>,
    pub branch_timeline_id: Option<u32>,
    pub created_at: i64,
    pub status: String,
}

// ── ProjectCtx ────────────────────────────────────────────────────────────────

/// Runtime identity and level-2 chunk fallback manifest for the running project.
pub struct ProjectCtx {
    pub meta: ProjectMeta,
    /// File-backed sorted manifest for the level-2 chunk read fallback.
    /// Binary search via direct pread — no in-memory page cache.
    pub base_manifest: Manifest,
}

impl ProjectCtx {
    /// Return a reference to the global project context.
    ///
    /// # Panics
    /// Panics if `ProjectCtx::init` has not been called.
    pub fn get() -> &'static Self {
        PROJECT_CTX
            .get()
            .expect("ProjectCtx::get() called before ProjectCtx::init()")
    }

    /// Populate the global project context. Silently ignored if already set.
    pub fn init(ctx: ProjectCtx) {
        let _ = PROJECT_CTX.set(ctx);
    }

    /// Load project.json from the sim store, download the latest base manifest,
    /// write the local TIKM file under `{data_dir}/tiko/base_manifest.bin`,
    /// and return a `ProjectCtx`.
    ///
    /// `ns` is the bootstrap key constructed from env vars before
    /// `project.json` is fetched. The loaded `ProjectMeta` must agree on all
    /// three identity fields or `load` returns an error.
    ///
    /// # Root project with no bases
    /// Constructs a zero-entry manifest — `base_manifest_lookup` returns
    /// `Ok(None)` for any key. This is correct: a fresh DB has no chunks yet.
    ///
    /// # Branch project with no bases
    /// Returns an error. A branch always has an initial base manifest written
    /// by `create_branch` (Module 7).
    pub fn load(sim: &SimStore, ns: &ProjectNamespace, data_dir: &Path) -> Result<Self> {
        // Step 1: fetch project.json
        let meta_key = ns.project_meta_key();
        let meta_bytes = sim
            .get_standard(&meta_key)?
            .ok_or_else(|| format!("project.json not found at key: {meta_key}"))?;
        let meta: ProjectMeta = serde_json::from_slice(&meta_bytes)?;

        // Step 2: validate identity
        if meta.ns != *ns {
            return Err(format!(
                "namespace mismatch: expected ({}/{}/{}), loaded ({}/{}/{})",
                ns.org_id,
                ns.project_id,
                ns.branch_id,
                meta.ns.org_id,
                meta.ns.project_id,
                meta.ns.branch_id,
            )
            .into());
        }

        // Step 3: list base manifests and find the latest LSN
        let base_prefix = ns.base_prefix();
        let keys = sim.list_prefix_standard(&base_prefix)?;

        // Keys look like "{org}/pitr/{proj}/bases/{lsn_hex}/manifest.bin".
        // After stripping the prefix we get "{lsn_hex}/manifest.bin".
        let mut base_lsns: Vec<Lsn> = keys
            .iter()
            .filter_map(|key| {
                let rest = key.strip_prefix(&base_prefix)?;
                let lsn_hex = rest.split('/').next()?;
                Lsn::from_hex(lsn_hex).ok()
            })
            .collect();
        base_lsns.sort();

        let local_path = Manifest::local_manifest_path(data_dir);

        // Step 4: download or construct the base manifest
        let base_manifest = if let Some(&latest_lsn) = base_lsns.last() {
            let manifest_key = ns.base_manifest_key(latest_lsn);
            let bytes = sim
                .get_standard(&manifest_key)?
                .ok_or_else(|| format!("manifest.bin not found at key: {manifest_key}"))?;
            Manifest::from_bytes(&bytes, &local_path)?
        } else if meta.parent_project_id.is_none() {
            // Root project with no bases yet — zero-entry manifest is valid.
            Manifest::new_sorted(Lsn::INVALID, 0, vec![], &local_path)?
        } else {
            return Err("branch project has no base manifests".into());
        };

        Ok(ProjectCtx {
            meta,
            base_manifest,
        })
    }

    /// Return the project namespace.
    pub fn ns(&self) -> &ProjectNamespace {
        &self.meta.ns
    }

    /// Return `true` if this project is a branch (has a parent project).
    pub fn is_branch(&self) -> bool {
        self.meta.parent_project_id.is_some()
    }

    /// Level-2 chunk lookup: binary search into the on-disk Manifest.
    pub fn base_manifest_lookup(&self, key: &ChunkTag) -> io::Result<Option<ChunkRef>> {
        self.base_manifest.lookup(key)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ChunkTag;
    use crate::manifest::ChunkRef;
    use tempfile::TempDir;

    fn setup() -> (TempDir, SimStore) {
        let dir = TempDir::new().unwrap();
        let store = SimStore::new(dir.path());
        (dir, store)
    }

    fn root_ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 1)
    }

    fn branch_ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2002, 2)
    }

    fn make_root_meta(ns: &ProjectNamespace) -> ProjectMeta {
        ProjectMeta {
            ns: ns.clone(),
            parent_project_id: None,
            parent_branch_id: None,
            branch_checkpoint_lsn: None,
            branch_timeline_id: None,
            created_at: 1_000_000,
            status: "active".to_string(),
        }
    }

    fn make_branch_meta(
        ns: &ProjectNamespace,
        parent_project_id: u64,
        parent_branch_id: u64,
        lsn: Lsn,
    ) -> ProjectMeta {
        ProjectMeta {
            ns: ns.clone(),
            parent_project_id: Some(parent_project_id),
            parent_branch_id: Some(parent_branch_id),
            branch_checkpoint_lsn: Some(lsn),
            branch_timeline_id: Some(1),
            created_at: 1_000_000,
            status: "active".to_string(),
        }
    }

    fn store_meta(sim: &SimStore, meta: &ProjectMeta) {
        let key = meta.ns.project_meta_key();
        let bytes = serde_json::to_vec(meta).unwrap();
        sim.put_standard(&key, &bytes).unwrap();
    }

    fn make_manifest_bytes(
        lsn: Lsn,
        chunks: Vec<(ChunkTag, ChunkRef)>,
        tmp_path: &Path,
    ) -> Vec<u8> {
        let m = Manifest::new_sorted(lsn, 0, chunks, tmp_path).unwrap();
        m.to_bytes().unwrap()
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

    // ── Root project, no base manifests ──────────────────────────────────

    #[test]
    fn root_project_no_bases_loads_empty_manifest() {
        let (dir, sim) = setup();
        let ns = root_ns();
        store_meta(&sim, &make_root_meta(&ns));

        let ctx = ProjectCtx::load(&sim, &ns, dir.path()).unwrap();

        assert!(!ctx.is_branch());
        assert_eq!(ctx.base_manifest_lookup(&tag(42)).unwrap(), None);
    }

    // ── Root project explicitly confirms empty bases returns Ok ───────────

    #[test]
    fn root_project_empty_bases_returns_ok_with_empty_manifest() {
        let (dir, sim) = setup();
        let ns = root_ns();
        store_meta(&sim, &make_root_meta(&ns));

        let ctx = ProjectCtx::load(&sim, &ns, dir.path()).unwrap();

        // Any key must miss on a zero-entry manifest
        let absent = ChunkTag {
            spc_oid: 999,
            db_oid: 999,
            rel_number: 999,
            fork_number: 0,
            chunk_id: 0,
        };
        assert_eq!(ctx.base_manifest_lookup(&absent).unwrap(), None);
    }

    // ── Branch project with a synthetic base manifest ─────────────────────

    #[test]
    fn branch_project_with_base_manifest_lookup_hit_and_miss() {
        let (dir, sim) = setup();
        let parent_ns = root_ns();
        let child_ns = branch_ns();
        let branch_lsn = Lsn::new(0x100);

        let meta = make_branch_meta(
            &child_ns,
            parent_ns.project_id,
            parent_ns.branch_id,
            branch_lsn,
        );
        store_meta(&sim, &meta);

        // Build and store a synthetic base manifest with one entry
        let known_tag = tag(42);
        let known_ref = ChunkRef {
            branch_id: 1,
            lsn: branch_lsn,
        };
        let tmp = dir.path().join("tmp_manifest.tikm");
        let bytes = make_manifest_bytes(branch_lsn, vec![(known_tag, known_ref)], &tmp);
        sim.put_standard(&child_ns.base_manifest_key(branch_lsn), &bytes)
            .unwrap();

        let ctx = ProjectCtx::load(&sim, &child_ns, dir.path()).unwrap();

        assert!(ctx.is_branch());
        assert_eq!(
            ctx.base_manifest_lookup(&known_tag).unwrap(),
            Some(known_ref)
        );
        assert_eq!(ctx.base_manifest_lookup(&tag(999)).unwrap(), None);
    }

    // ── Branch project, no base manifests → error ─────────────────────────

    #[test]
    fn branch_project_no_base_manifests_returns_error() {
        let (dir, sim) = setup();
        let ns = branch_ns();
        let meta = make_branch_meta(&ns, 2001, 1, Lsn::new(0x100));
        store_meta(&sim, &meta);

        // No base manifests written to sim
        let result = ProjectCtx::load(&sim, &ns, dir.path());
        assert!(result.is_err(), "branch with no bases must return error");
    }

    // ── Namespace mismatch → error ────────────────────────────────────────

    #[test]
    fn mismatched_namespace_returns_error() {
        let (dir, sim) = setup();
        let ns_stored = root_ns();
        let ns_query = ProjectNamespace::new(1001, 9999, 1); // different project_id

        store_meta(&sim, &make_root_meta(&ns_stored));

        // Query with a different namespace than what's in project.json
        let result = ProjectCtx::load(&sim, &ns_query, dir.path());
        assert!(result.is_err(), "namespace mismatch must return error");
    }

    // ── Missing project.json → error ──────────────────────────────────────

    #[test]
    fn missing_project_json_returns_error() {
        let (dir, sim) = setup();
        let ns = root_ns();
        // Do NOT store project.json

        let result = ProjectCtx::load(&sim, &ns, dir.path());
        assert!(result.is_err(), "missing project.json must return error");
    }

    // ── Latest base manifest is selected among multiple ───────────────────

    #[test]
    fn latest_base_manifest_wins_among_multiple() {
        let (dir, sim) = setup();
        let ns = root_ns();
        store_meta(&sim, &make_root_meta(&ns));

        let lsn_old = Lsn::new(0x100);
        let lsn_new = Lsn::new(0x200);

        let known_tag = tag(1);
        let old_ref = ChunkRef {
            branch_id: 1,
            lsn: lsn_old,
        };
        let new_ref = ChunkRef {
            branch_id: 1,
            lsn: lsn_new,
        };

        let tmp_old = dir.path().join("tmp_old.tikm");
        let bytes_old = make_manifest_bytes(lsn_old, vec![(known_tag, old_ref)], &tmp_old);
        let tmp_new = dir.path().join("tmp_new.tikm");
        let bytes_new = make_manifest_bytes(lsn_new, vec![(known_tag, new_ref)], &tmp_new);

        sim.put_standard(&ns.base_manifest_key(lsn_old), &bytes_old)
            .unwrap();
        sim.put_standard(&ns.base_manifest_key(lsn_new), &bytes_new)
            .unwrap();

        let ctx = ProjectCtx::load(&sim, &ns, dir.path()).unwrap();

        // Should use the newer manifest
        assert_eq!(ctx.base_manifest_lookup(&known_tag).unwrap(), Some(new_ref));
    }
}
