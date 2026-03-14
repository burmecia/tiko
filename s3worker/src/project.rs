//! Project context — runtime identity and base manifest for the running project.
//!
//! `ProjectCtx` is populated once at s3worker startup and held in
//! `PROJECT_CTX` for the duration of the process lifetime.
//! The `base_manifest` field is file-backed (TIKM format) and supports
//! concurrent lookups via binary search + direct `pread` — no in-memory
//! page cache beyond what the OS provides.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use pgsys::Lsn;
use serde::{Deserialize, Serialize};

use crate::cache::{ChunkTag, RelFork};
use crate::manifest::{ChunkRef, Manifest};
use crate::sim_store::SimStore;

// Environment variable names for project identity (org_id, project_id, branch_id).
pub const ENV_ORG_ID: &str = "TIKO_ORG_ID";
pub const ENV_PROJECT_ID: &str = "TIKO_PROJECT_ID";
pub const ENV_BRANCH_ID: &str = "TIKO_BRANCH_ID";

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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

    /// `{org}/{proj}/chunks/{chunk_path}/{tl:08X}/latest`
    pub fn chunk_latest_key(&self, tag: &ChunkTag, timeline: u32) -> String {
        format!(
            "{}/{}/chunks/{}/{:08X}/latest",
            self.org_id,
            self.project_id,
            tag.to_path(),
            timeline,
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

    /// `{org}/chunks/{branch_id}/{chunk_path}/{timeline:08X}/{lsn_hex}`
    ///
    /// `branch_id` is the writer's branch — use `self.branch_id` for writes,
    /// or `chunk_ref.branch_id` when resolving a manifest entry from a parent.
    pub fn chunk_versioned_key(
        &self,
        tag: &ChunkTag,
        branch_id: u64,
        timeline: u32,
        lsn: Lsn,
    ) -> String {
        format!(
            "{}/chunks/{}/{}/{:08X}/{}",
            self.org_id,
            branch_id,
            tag.to_path(),
            timeline,
            lsn.to_hex()
        )
    }

    // ── Standard-bucket PITR manifest keys (per-project) ─────────────────

    /// `{org}/pitr/{proj}/deltas/{timeline:08X}/{lsn_hex}/manifest.bin`
    pub fn delta_manifest_key(&self, timeline: u32, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/deltas/{:08X}/{}/manifest.bin",
            self.org_id,
            self.project_id,
            timeline,
            lsn.to_hex()
        )
    }

    /// `{org}/pitr/{proj}/bases/{timeline:08X}/{lsn_hex}/manifest.bin`
    pub fn base_manifest_key(&self, timeline: u32, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/bases/{:08X}/{}/manifest.bin",
            self.org_id,
            self.project_id,
            timeline,
            lsn.to_hex()
        )
    }

    /// `{org}/pitr/{proj}/deltas/{timeline:08X}/{lsn_hex}/pg_state.tar.zst`
    pub fn pg_state_key(&self, timeline: u32, lsn: Lsn) -> String {
        format!(
            "{}/pitr/{}/deltas/{:08X}/{}/pg_state.tar.zst",
            self.org_id,
            self.project_id,
            timeline,
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

    /// `{org}/metadata/org.json`
    pub fn org_meta_key(&self) -> String {
        format!("{}/metadata/org.json", self.org_id)
    }

    /// `{org}/metadata/` — prefix for listing all project metadata under an org.
    pub fn org_metadata_prefix(&self) -> String {
        format!("{}/metadata/", self.org_id)
    }

    // ── List prefixes (for scanning) ──────────────────────────────────────

    /// `{org}/pitr/{proj}/deltas/` — all timelines (used by GC for cross-timeline scanning).
    pub fn delta_prefix(&self) -> String {
        format!("{}/pitr/{}/deltas/", self.org_id, self.project_id)
    }

    /// `{org}/pitr/{proj}/bases/` — all timelines (used by GC for cross-timeline scanning).
    pub fn base_prefix(&self) -> String {
        format!("{}/pitr/{}/bases/", self.org_id, self.project_id)
    }

    /// `{org}/pitr/{proj}/deltas/{timeline:08X}/` — scoped to one timeline.
    pub fn delta_prefix_for_timeline(&self, timeline: u32) -> String {
        format!(
            "{}/pitr/{}/deltas/{:08X}/",
            self.org_id, self.project_id, timeline
        )
    }

    /// `{org}/pitr/{proj}/bases/{timeline:08X}/` — scoped to one timeline.
    pub fn base_prefix_for_timeline(&self, timeline: u32) -> String {
        format!(
            "{}/pitr/{}/bases/{:08X}/",
            self.org_id, self.project_id, timeline
        )
    }

    // ── Relation-level express keys ───────────────────────────────────────

    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/nblocks`
    ///
    /// Key for the live block count of a relation fork in the express bucket.
    /// Value: 4-byte little-endian `u32`.
    pub fn rel_nblocks_key(&self, rf: RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/nblocks",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
    }

    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/`
    ///
    /// Prefix covering all express keys for a relation fork (chunks + nblocks).
    pub fn rel_chunks_prefix(&self, rf: RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
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
    /// The active timeline ID after the most recent recovery or initial start.
    /// Starts at 1 for new projects. Updated by tikod in `post_recovery_cleanup`
    /// after each PITR recovery. Used to scope delta/base/chunk S3 keys to
    /// the current timeline, preventing key collisions across PITR recoveries.
    #[serde(default = "default_timeline_id")]
    pub current_timeline_id: u32,
    pub created_at: i64,
    pub status: String,
    /// Unix timestamp (seconds) set by tikod `delete_branch`; absent on live projects.
    /// GC uses this to identify objects eligible for physical deletion.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub deleted_at: Option<i64>,
}

fn default_timeline_id() -> u32 {
    1
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
    /// Populate the global project context. Silently ignored if already set.
    pub fn init(ctx: ProjectCtx) {
        let _ = PROJECT_CTX.set(ctx);
    }

    /// Return a reference to the global project context.
    ///
    /// # Panics
    /// Panics if `ProjectCtx::init` has not been called.
    pub fn get() -> &'static Self {
        PROJECT_CTX
            .get()
            .expect("ProjectCtx::get() called before ProjectCtx::init()")
    }

    /// Return a reference to the global project context, or `None` if not yet
    /// initialised. Used by the S3 read fallback path to avoid a panic when
    /// `ProjectCtx::init` was skipped (e.g. env vars not set).
    pub fn try_get() -> Option<&'static Self> {
        PROJECT_CTX.get()
    }

    /// Current timeline ID for this project.
    /// Used to scope delta/base/chunk S3 keys to the active timeline.
    pub fn current_timeline_id(&self) -> u32 {
        self.meta.current_timeline_id
    }

    /// Initialize the global `ProjectCtx` from environment variables.
    ///
    /// Reads `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`. Panics if
    /// any are absent or not valid u64s.
    ///
    /// If `project.json` exists in SimStore, loads the full `ProjectCtx`
    /// (namespace + base manifest). Otherwise falls back to `bootstrap`,
    /// which creates a namespace-only ctx with an empty manifest — correct
    /// for initdb where no project data exists yet.
    ///
    /// Silently ignored if `ProjectCtx` is already initialized (OnceLock).
    /// `SimStore::init()` must be called before this.
    pub fn init_from_env(data_dir: &Path) {
        fn read_u64(name: &str) -> u64 {
            std::env::var(name)
                .unwrap_or_else(|_| panic!("Environment variable {name} must be set"))
                .parse()
                .unwrap_or_else(|_| panic!("Environment variable {name} must be a valid u64"))
        }
        let org_id = read_u64(ENV_ORG_ID);
        let project_id = read_u64(ENV_PROJECT_ID);
        let branch_id = read_u64(ENV_BRANCH_ID);
        let sim = SimStore::get();
        let ns = ProjectNamespace::new(org_id, project_id, branch_id);
        // During initdb, project.json has not been written yet — fall back to
        // a namespace-only ctx with an empty manifest so writes are routed
        // correctly. The manifest is irrelevant during initdb (no reads).
        let ctx = ProjectCtx::load(&ns, data_dir, sim)
            .unwrap_or_else(|_| ProjectCtx::bootstrap(&ns, data_dir));
        ProjectCtx::init(ctx);
    }

    /// Construct a minimal `ProjectCtx` when `project.json` does not exist yet
    /// (e.g. during `initdb`).
    ///
    /// Sets the namespace so writes are routed to the correct SimStore paths.
    /// Uses an empty zero-entry manifest — level-2 reads are never needed
    /// before the first checkpoint writes a base manifest.
    fn bootstrap(ns: &ProjectNamespace, data_dir: &Path) -> Self {
        let meta = ProjectMeta {
            ns: ns.clone(),
            parent_project_id: None,
            parent_branch_id: None,
            branch_checkpoint_lsn: None,
            branch_timeline_id: None,
            current_timeline_id: 1,
            created_at: 0,
            status: "active".to_string(),
            deleted_at: None,
        };
        let local_path = Manifest::local_manifest_path(data_dir);
        let base_manifest = Manifest::empty(&local_path).expect("failed to create empty manifest");
        ProjectCtx {
            meta,
            base_manifest,
        }
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
    /// Constructs a zero-entry manifest (same as root). `is_branch()` still
    /// returns `true` so callers can skip root-only operations (e.g. base
    /// compaction after initdb). The initial base is written by the
    /// restore-from-parent process after initdb completes.
    pub fn load(ns: &ProjectNamespace, data_dir: &Path, sim: &SimStore) -> Result<Self> {
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

        // Step 3: list base manifests for the current timeline and find the latest LSN.
        // Keys look like "{org}/pitr/{proj}/bases/{tl:08X}/{lsn_hex}/manifest.bin".
        // After stripping the timeline-scoped prefix we get "{lsn_hex}/manifest.bin".
        let timeline = meta.current_timeline_id;
        let base_prefix = ns.base_prefix_for_timeline(timeline);
        let keys = sim.list_prefix_standard(&base_prefix)?;

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
            let manifest_key = ns.base_manifest_key(timeline, latest_lsn);
            let bytes = sim
                .get_standard(&manifest_key)?
                .ok_or_else(|| format!("manifest.bin not found at key: {manifest_key}"))?;
            Manifest::from_bytes(&bytes, &local_path)?
        } else {
            // No bases yet — either root project (before initdb checkpoint) or
            // branch project (before restore-from-parent). Zero-entry manifest
            // is valid; is_branch() still returns true for branches, letting
            // callers skip root-only operations such as base compaction.
            Manifest::empty(&local_path)?
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

// ── Module 7: Branch Creation ─────────────────────────────────────────────────

/// Build the initial base manifest for a new branch.
///
/// Finds the latest base manifest for `parent_ns` with `base_lsn ≤ branch_lsn`,
/// applies all parent deltas in `(base_lsn, branch_lsn]`, and writes the merged
/// result to `out_path`. Parent `branch_id` values inside each `ChunkRef` are
/// preserved — no re-keying.
///
/// Returns an error if no base manifest exists with `lsn ≤ branch_lsn`.
pub fn build_initial_manifest(
    sim: &SimStore,
    parent_ns: &ProjectNamespace,
    parent_timeline: u32,
    branch_lsn: Lsn,
    out_path: &Path,
) -> Result<Manifest> {
    // Step 1: list base manifests on the parent's timeline; find the latest
    // with lsn ≤ branch_lsn.
    // Keys look like "{org}/pitr/{proj}/bases/{tl:08X}/{lsn_hex}/manifest.bin".
    let base_prefix = parent_ns.base_prefix_for_timeline(parent_timeline);
    let base_keys = sim.list_prefix_standard(&base_prefix)?;

    let mut base_lsns: Vec<Lsn> = base_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&base_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .filter(|&lsn| lsn <= branch_lsn)
        .collect();
    base_lsns.sort();

    let chosen_base_lsn = base_lsns
        .last()
        .copied()
        .ok_or_else(|| format!("no base manifest with lsn ≤ {}", branch_lsn.to_hex()))?;

    // Step 2: download the chosen base manifest.
    let base_manifest_key = parent_ns.base_manifest_key(parent_timeline, chosen_base_lsn);
    let bytes = sim
        .get_standard(&base_manifest_key)?
        .ok_or_else(|| format!("base manifest not found: {base_manifest_key}"))?;
    let base = Manifest::from_bytes(&bytes, out_path)?;

    // Step 3: list deltas on the parent's timeline in (chosen_base_lsn, branch_lsn].
    let delta_prefix = parent_ns.delta_prefix_for_timeline(parent_timeline);
    let delta_keys = sim.list_prefix_standard(&delta_prefix)?;

    let mut delta_lsns: Vec<Lsn> = delta_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&delta_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .filter(|&lsn| lsn > chosen_base_lsn && lsn <= branch_lsn)
        .collect();
    delta_lsns.sort();
    delta_lsns.dedup();

    let mut deltas: Vec<Manifest> = Vec::with_capacity(delta_lsns.len());
    for &delta_lsn in &delta_lsns {
        let key = parent_ns.delta_manifest_key(parent_timeline, delta_lsn);
        let bytes = sim
            .get_standard(&key)?
            .ok_or_else(|| format!("delta manifest not found: {key}"))?;
        // Place each delta TIKM file alongside the output base manifest.
        let delta_path = out_path.with_file_name(format!("delta_{}.tikm", delta_lsn.to_hex()));
        deltas.push(Manifest::from_bytes(&bytes, &delta_path)?);
    }

    // Step 4: merge deltas into the base.
    base.apply_deltas(&deltas)?;

    Ok(base)
}

/// Write `project.json` for a root project to SimStore if it does not already exist.
///
/// Called once after the initdb shutdown checkpoint so that subsequent
/// `init_from_env` calls use `load()` (which fetches the real base manifest
/// from SimStore) instead of falling back to `bootstrap()` (which overwrites
/// `base_manifest.bin` with an empty file on every process start).
///
/// Idempotent: if `project.json` already exists it is left unchanged.
/// Only call for root projects (`parent_project_id: None`); branches write
/// their `project.json` via `create_branch`.
pub fn ensure_root_project_meta(sim: &SimStore, ns: &ProjectNamespace) -> Result<()> {
    let key = ns.project_meta_key();
    if sim.get_standard(&key)?.is_some() {
        return Ok(());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = ProjectMeta {
        ns: ns.clone(),
        parent_project_id: None,
        parent_branch_id: None,
        branch_checkpoint_lsn: None,
        branch_timeline_id: None,
        current_timeline_id: 1,
        created_at: now,
        status: "active".to_string(),
        deleted_at: None,
    };
    sim.put_standard(&key, &serde_json::to_vec(&meta)?)?;
    Ok(())
}

/// Create a child branch at `branch_lsn` forked from `parent_ns`.
///
/// Writes exactly two objects to the standard bucket:
/// - `child_ns.project_meta_key()` — serialised `ProjectMeta`
/// - `child_ns.base_manifest_key(1, branch_lsn)` — the merged base manifest (child starts on tl=1)
///
/// The branch is valid once both writes succeed.
pub fn create_branch(
    sim: &SimStore,
    parent_ns: &ProjectNamespace,
    parent_timeline: u32,
    child_ns: &ProjectNamespace,
    branch_lsn: Lsn,
) -> Result<()> {
    // Child branch always starts on timeline 1.
    const CHILD_TIMELINE: u32 = 1;

    // Build the initial manifest to a unique temp path.
    let local_path: PathBuf = std::env::temp_dir().join(format!(
        "tiko_branch_{}_{}.tikm",
        child_ns.branch_id,
        branch_lsn.to_hex()
    ));
    let initial_manifest =
        build_initial_manifest(sim, parent_ns, parent_timeline, branch_lsn, &local_path)?;

    // Construct child project metadata.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = ProjectMeta {
        ns: child_ns.clone(),
        parent_project_id: Some(parent_ns.project_id),
        parent_branch_id: Some(parent_ns.branch_id),
        branch_checkpoint_lsn: Some(branch_lsn),
        branch_timeline_id: Some(parent_timeline),
        current_timeline_id: CHILD_TIMELINE,
        created_at: now,
        status: "active".to_string(),
        deleted_at: None,
    };

    // Write project.json.
    sim.put_standard(&child_ns.project_meta_key(), &serde_json::to_vec(&meta)?)?;

    // Write manifest.bin — single atomic PUT; branch is valid after this.
    sim.put_standard(
        &child_ns.base_manifest_key(CHILD_TIMELINE, branch_lsn),
        &initial_manifest.to_bytes()?,
    )?;

    Ok(())
}

/// Delete all sim objects for `branch_ns`.
///
/// Removes:
/// - Express-bucket hot data: `{org}/{proj}/`
/// - Standard-bucket PITR data (manifests + WAL): `{org}/pitr/{proj}/`
/// - Standard-bucket metadata: `{org}/metadata/{proj}/`
///
/// Standard-bucket chunk objects (`{org}/chunks/{branch_id}/`) are intentionally
/// left in place and will be collected by the next GC run.
pub fn delete_branch(sim: &SimStore, branch_ns: &ProjectNamespace) -> Result<()> {
    // 1. Remove express-bucket hot data.
    let express_prefix = format!("{}/{}/", branch_ns.org_id, branch_ns.project_id);
    for key in sim.list_prefix_express(&express_prefix)? {
        sim.delete_express(&key)?;
    }

    // 2. Remove standard-bucket PITR data (manifests + WAL).
    let pitr_prefix = format!("{}/pitr/{}/", branch_ns.org_id, branch_ns.project_id);
    for key in sim.list_prefix_standard(&pitr_prefix)? {
        sim.delete_standard(&key)?;
    }

    // 3. Remove standard-bucket metadata.
    let meta_prefix = format!("{}/metadata/{}/", branch_ns.org_id, branch_ns.project_id);
    for key in sim.list_prefix_standard(&meta_prefix)? {
        sim.delete_standard(&key)?;
    }

    // 4. Chunk GC is the control plane's responsibility.
    eprintln!(
        "tiko: standard-bucket {}/chunks/{}/ will be collected by next GC run",
        branch_ns.org_id, branch_ns.branch_id
    );

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
            current_timeline_id: 1,
            created_at: 1_000_000,
            status: "active".to_string(),
            deleted_at: None,
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
            current_timeline_id: 1,
            created_at: 1_000_000,
            status: "active".to_string(),
            deleted_at: None,
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
        let m = Manifest::new(lsn, 0, chunks, HashMap::new(), tmp_path).unwrap();
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

        let ctx = ProjectCtx::load(&ns, dir.path(), &sim).unwrap();

        assert!(!ctx.is_branch());
        assert_eq!(ctx.base_manifest_lookup(&tag(42)).unwrap(), None);
    }

    // ── Root project explicitly confirms empty bases returns Ok ───────────

    #[test]
    fn root_project_empty_bases_returns_ok_with_empty_manifest() {
        let (dir, sim) = setup();
        let ns = root_ns();
        store_meta(&sim, &make_root_meta(&ns));

        let ctx = ProjectCtx::load(&ns, dir.path(), &sim).unwrap();

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
            timeline_id: 1,
            lsn: branch_lsn,
        };
        let tmp = dir.path().join("tmp_manifest.tikm");
        let bytes = make_manifest_bytes(branch_lsn, vec![(known_tag, known_ref)], &tmp);
        sim.put_standard(&child_ns.base_manifest_key(1, branch_lsn), &bytes)
            .unwrap();

        let ctx = ProjectCtx::load(&child_ns, dir.path(), &sim).unwrap();

        assert!(ctx.is_branch());
        assert_eq!(
            ctx.base_manifest_lookup(&known_tag).unwrap(),
            Some(known_ref)
        );
        assert_eq!(ctx.base_manifest_lookup(&tag(999)).unwrap(), None);
    }

    // ── Branch project, no base manifests → succeeds with empty manifest ────

    #[test]
    fn branch_project_no_base_manifests_returns_empty_manifest() {
        let (dir, sim) = setup();
        let ns = branch_ns();
        let meta = make_branch_meta(&ns, 2001, 1, Lsn::new(0x100));
        store_meta(&sim, &meta);

        // No base manifests written to sim — load() must still succeed.
        let ctx = ProjectCtx::load(&ns, dir.path(), &sim).unwrap();
        assert!(ctx.is_branch(), "is_branch() must be true");
        // Empty manifest returns None for any lookup.
        assert_eq!(ctx.base_manifest_lookup(&tag(1)).unwrap(), None);
    }

    // ── Namespace mismatch → error ────────────────────────────────────────

    #[test]
    fn mismatched_namespace_returns_error() {
        let (dir, sim) = setup();
        let ns_stored = root_ns();
        let ns_query = ProjectNamespace::new(1001, 9999, 1); // different project_id

        store_meta(&sim, &make_root_meta(&ns_stored));

        // Query with a different namespace than what's in project.json
        let result = ProjectCtx::load(&ns_query, dir.path(), &sim);
        assert!(result.is_err(), "namespace mismatch must return error");
    }

    // ── Missing project.json → error ──────────────────────────────────────

    #[test]
    fn missing_project_json_returns_error() {
        let (dir, sim) = setup();
        let ns = root_ns();
        // Do NOT store project.json

        let result = ProjectCtx::load(&ns, dir.path(), &sim);
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
            timeline_id: 1,
            lsn: lsn_old,
        };
        let new_ref = ChunkRef {
            branch_id: 1,
            timeline_id: 1,
            lsn: lsn_new,
        };

        let tmp_old = dir.path().join("tmp_old.tikm");
        let bytes_old = make_manifest_bytes(lsn_old, vec![(known_tag, old_ref)], &tmp_old);
        let tmp_new = dir.path().join("tmp_new.tikm");
        let bytes_new = make_manifest_bytes(lsn_new, vec![(known_tag, new_ref)], &tmp_new);

        sim.put_standard(&ns.base_manifest_key(1, lsn_old), &bytes_old)
            .unwrap();
        sim.put_standard(&ns.base_manifest_key(1, lsn_new), &bytes_new)
            .unwrap();

        let ctx = ProjectCtx::load(&ns, dir.path(), &sim).unwrap();

        // Should use the newer manifest
        assert_eq!(ctx.base_manifest_lookup(&known_tag).unwrap(), Some(new_ref));
    }

    // ── Branch with no base manifests succeeds and is_branch() == true ────────

    #[test]
    fn load_branch_no_base_returns_ok_and_is_branch_true() {
        let (dir, sim) = setup();
        let ns = branch_ns();

        // Pre-write project.json with parent_project_id set (branch project).
        let meta = make_branch_meta(&ns, 2001, 1, Lsn::new(0x100));
        store_meta(&sim, &meta);

        // No base manifests exist yet (initdb has run but restore has not).
        let ctx = ProjectCtx::load(&ns, dir.path(), &sim).unwrap();

        assert!(ctx.is_branch(), "is_branch() must return true");
        // Base manifest is empty — no entries.
        assert_eq!(
            ctx.base_manifest_lookup(&ChunkTag {
                spc_oid: 1,
                db_oid: 1,
                rel_number: 1,
                fork_number: 0,
                chunk_id: 0,
            })
            .unwrap(),
            None,
            "empty manifest must return None for any lookup"
        );
    }
}

// ── Module 7 tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod module7_tests {
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

    fn ns_a() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 1)
    }

    fn ns_b() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2002, 2)
    }

    fn ns_c() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2003, 3)
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
        ChunkRef {
            branch_id,
            timeline_id: 1,
            lsn,
        }
    }

    fn store_manifest_bytes(
        sim: &SimStore,
        key: &str,
        lsn: Lsn,
        chunks: Vec<(ChunkTag, ChunkRef)>,
        tmp_path: &Path,
    ) {
        let m = Manifest::new(lsn, 0, chunks, HashMap::new(), tmp_path).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(key, &bytes).unwrap();
    }

    // ── build_initial_manifest from base + 3 deltas ───────────────────────

    #[test]
    fn build_initial_manifest_merges_base_and_deltas() {
        let (dir, sim) = setup();
        let ns = ns_a();

        let base_lsn = Lsn::new(0x100);
        let d1_lsn = Lsn::new(0x200);
        let d2_lsn = Lsn::new(0x300);
        let d3_lsn = Lsn::new(0x400); // branch_lsn

        // Base: rel=1 at branch_id=1
        store_manifest_bytes(
            &sim,
            &ns.base_manifest_key(1, base_lsn),
            base_lsn,
            vec![(tag(1), cref(1, base_lsn))],
            &dir.path().join("b.tikm"),
        );
        // Delta 1: adds rel=2
        store_manifest_bytes(
            &sim,
            &ns.delta_manifest_key(1, d1_lsn),
            d1_lsn,
            vec![(tag(2), cref(1, d1_lsn))],
            &dir.path().join("d1.tikm"),
        );
        // Delta 2: updates rel=1, adds rel=3
        store_manifest_bytes(
            &sim,
            &ns.delta_manifest_key(1, d2_lsn),
            d2_lsn,
            vec![(tag(1), cref(1, d2_lsn)), (tag(3), cref(1, d2_lsn))],
            &dir.path().join("d2.tikm"),
        );
        // Delta 3: updates rel=2
        store_manifest_bytes(
            &sim,
            &ns.delta_manifest_key(1, d3_lsn),
            d3_lsn,
            vec![(tag(2), cref(1, d3_lsn))],
            &dir.path().join("d3.tikm"),
        );

        let out = dir.path().join("out.tikm");
        let m = build_initial_manifest(&sim, &ns, 1, d3_lsn, &out).unwrap();

        assert_eq!(m.lookup(&tag(1)).unwrap(), Some(cref(1, d2_lsn))); // updated by d2
        assert_eq!(m.lookup(&tag(2)).unwrap(), Some(cref(1, d3_lsn))); // updated by d3
        assert_eq!(m.lookup(&tag(3)).unwrap(), Some(cref(1, d2_lsn))); // added by d2
        assert_eq!(m.lookup(&tag(99)).unwrap(), None);
    }

    // ── Deltas beyond branch_lsn are excluded ─────────────────────────────

    #[test]
    fn build_initial_manifest_excludes_deltas_beyond_branch_lsn() {
        let (dir, sim) = setup();
        let ns = ns_a();

        let base_lsn = Lsn::new(0x100);
        let d1_lsn = Lsn::new(0x200);
        let d2_lsn = Lsn::new(0x300); // beyond branch point
        let branch_lsn = Lsn::new(0x200);

        store_manifest_bytes(
            &sim,
            &ns.base_manifest_key(1, base_lsn),
            base_lsn,
            vec![(tag(1), cref(1, base_lsn))],
            &dir.path().join("b.tikm"),
        );
        store_manifest_bytes(
            &sim,
            &ns.delta_manifest_key(1, d1_lsn),
            d1_lsn,
            vec![(tag(1), cref(1, d1_lsn))],
            &dir.path().join("d1.tikm"),
        );
        // Delta 2 is beyond branch_lsn — must NOT be applied
        store_manifest_bytes(
            &sim,
            &ns.delta_manifest_key(1, d2_lsn),
            d2_lsn,
            vec![(tag(1), cref(1, d2_lsn)), (tag(2), cref(1, d2_lsn))],
            &dir.path().join("d2.tikm"),
        );

        let out = dir.path().join("out.tikm");
        let m = build_initial_manifest(&sim, &ns, 1, branch_lsn, &out).unwrap();

        // rel=1 should reflect d1, not d2
        assert_eq!(m.lookup(&tag(1)).unwrap(), Some(cref(1, d1_lsn)));
        // rel=2 (only in d2) must not appear
        assert_eq!(m.lookup(&tag(2)).unwrap(), None);
    }

    // ── Cascaded branch: C from B from A ─────────────────────────────────

    #[test]
    fn cascaded_branch_preserves_original_branch_ids() {
        let (dir, sim) = setup();
        let a_ns = ns_a();
        let b_ns = ns_b();
        let _c_ns = ns_c();

        let a_lsn = Lsn::new(0x100);
        let b_lsn = Lsn::new(0x200);

        // A's base: rel=10 with A's branch_id
        store_manifest_bytes(
            &sim,
            &a_ns.base_manifest_key(1, a_lsn),
            a_lsn,
            vec![(tag(10), cref(a_ns.branch_id, a_lsn))],
            &dir.path().join("a_base.tikm"),
        );

        // Build B's initial manifest from A at a_lsn and store it as B's base
        let tmp_b_out = dir.path().join("b_init.tikm");
        let b_initial = build_initial_manifest(&sim, &a_ns, 1, a_lsn, &tmp_b_out).unwrap();
        sim.put_standard(
            &b_ns.base_manifest_key(1, a_lsn),
            &b_initial.to_bytes().unwrap(),
        )
        .unwrap();

        // B adds a delta: rel=20 with B's branch_id
        store_manifest_bytes(
            &sim,
            &b_ns.delta_manifest_key(1, b_lsn),
            b_lsn,
            vec![(tag(20), cref(b_ns.branch_id, b_lsn))],
            &dir.path().join("b_d1.tikm"),
        );

        // Build C's manifest from B at b_lsn
        let tmp_c_out = dir.path().join("c_out.tikm");
        let c = build_initial_manifest(&sim, &b_ns, 1, b_lsn, &tmp_c_out).unwrap();

        // rel=10 was only on A → branch_id = A's (1)
        assert_eq!(
            c.lookup(&tag(10)).unwrap(),
            Some(cref(a_ns.branch_id, a_lsn))
        );
        // rel=20 was added on B → branch_id = B's (2)
        assert_eq!(
            c.lookup(&tag(20)).unwrap(),
            Some(cref(b_ns.branch_id, b_lsn))
        );
    }

    // ── create_branch writes project.json + manifest.bin ─────────────────

    #[test]
    fn create_branch_writes_exactly_two_standard_files() {
        let (dir, sim) = setup();
        let parent_ns = ns_a();
        let child_ns = ns_b();
        let branch_lsn = Lsn::new(0x100);

        // Set up parent base
        store_manifest_bytes(
            &sim,
            &parent_ns.base_manifest_key(1, branch_lsn),
            branch_lsn,
            vec![(tag(1), cref(parent_ns.branch_id, branch_lsn))],
            &dir.path().join("parent_base.tikm"),
        );

        create_branch(&sim, &parent_ns, 1, &child_ns, branch_lsn).unwrap();

        // project.json must exist and deserialise correctly
        let meta_bytes = sim
            .get_standard(&child_ns.project_meta_key())
            .unwrap()
            .unwrap();
        let meta: ProjectMeta = serde_json::from_slice(&meta_bytes).unwrap();
        assert_eq!(meta.ns, child_ns);
        assert_eq!(meta.parent_project_id, Some(parent_ns.project_id));
        assert_eq!(meta.parent_branch_id, Some(parent_ns.branch_id));
        assert_eq!(meta.branch_checkpoint_lsn, Some(branch_lsn));

        // manifest.bin must exist and round-trip correctly
        let manifest_bytes = sim
            .get_standard(&child_ns.base_manifest_key(1, branch_lsn))
            .unwrap()
            .unwrap();
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&manifest_bytes, &tmp).unwrap();
        assert_eq!(
            m.lookup(&tag(1)).unwrap(),
            Some(cref(parent_ns.branch_id, branch_lsn))
        );

        // Exactly 1 base manifest and 1 project.json in standard for the child
        let base_keys = sim.list_prefix_standard(&child_ns.base_prefix()).unwrap();
        assert_eq!(base_keys.len(), 1);
        let meta_keys = sim
            .list_prefix_standard(&format!(
                "{}/metadata/{}/",
                child_ns.org_id, child_ns.project_id
            ))
            .unwrap();
        assert_eq!(meta_keys.len(), 1);
    }

    // ── delete_branch removes express + pitr/metadata; leaves chunks ──────

    #[test]
    fn delete_branch_removes_express_and_pitr_not_chunks() {
        let (dir, sim) = setup();
        let ns = ns_b();
        let branch_lsn = Lsn::new(0x100);

        // Express: a chunk latest
        sim.put_express(&ns.chunk_latest_key(&tag(1), 1), b"hot-data")
            .unwrap();

        // Standard PITR: a base manifest
        store_manifest_bytes(
            &sim,
            &ns.base_manifest_key(1, branch_lsn),
            branch_lsn,
            vec![(tag(1), cref(ns.branch_id, branch_lsn))],
            &dir.path().join("del_base.tikm"),
        );

        // Standard metadata: project.json
        let meta = ProjectMeta {
            ns: ns.clone(),
            parent_project_id: None,
            parent_branch_id: None,
            branch_checkpoint_lsn: None,
            branch_timeline_id: None,
            current_timeline_id: 1,
            created_at: 0,
            status: "active".to_string(),
            deleted_at: None,
        };
        sim.put_standard(&ns.project_meta_key(), &serde_json::to_vec(&meta).unwrap())
            .unwrap();

        // Standard chunks: versioned object (must NOT be deleted)
        let chunk_key = ns.chunk_versioned_key(&tag(1), ns.branch_id, 1, branch_lsn);
        sim.put_standard(&chunk_key, b"chunk-data").unwrap();

        delete_branch(&sim, &ns).unwrap();

        // Express data removed
        assert_eq!(
            sim.get_express(&ns.chunk_latest_key(&tag(1), 1)).unwrap(),
            None
        );
        // PITR data removed
        assert!(
            sim.list_prefix_standard(&ns.base_prefix())
                .unwrap()
                .is_empty()
        );
        // Metadata removed
        assert_eq!(sim.get_standard(&ns.project_meta_key()).unwrap(), None);
        // Chunks untouched
        assert_eq!(
            sim.get_standard(&chunk_key).unwrap(),
            Some(b"chunk-data".to_vec())
        );
    }

    // ── no base with lsn ≤ branch_lsn → error ────────────────────────────

    #[test]
    fn build_initial_manifest_no_base_lte_branch_lsn_returns_error() {
        let (dir, sim) = setup();
        let ns = ns_a();

        // Only base at 0x500, branch_lsn is 0x100
        store_manifest_bytes(
            &sim,
            &ns.base_manifest_key(1, Lsn::new(0x500)),
            Lsn::new(0x500),
            vec![(tag(1), cref(1, Lsn::new(0x500)))],
            &dir.path().join("future.tikm"),
        );

        let out = dir.path().join("out.tikm");
        let result = build_initial_manifest(&sim, &ns, 1, Lsn::new(0x100), &out);
        assert!(result.is_err(), "no base ≤ branch_lsn must return error");
    }
}
