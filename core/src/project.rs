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
use pgsys::common::is_under_postmaster;
use serde::{Deserialize, Serialize};

use crate::{
    chunk::{ChunkTag, RelFork},
    env,
    io::store::Store,
    manifest::{ChunkRef, Manifest},
};

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

    /// Construct a `ProjectNamespace` from environment variables.
    ///
    /// Expects `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID` to be set and
    /// parseable as u64s. Panics if any are missing or invalid.
    pub fn new_from_env() -> Self {
        let org_id = env::read_u64(env::ENV_ORG_ID);
        let project_id = env::read_u64(env::ENV_PROJECT_ID);

        ProjectNamespace::new(org_id, project_id, 0)
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

    /// Prefix for all 256 KiB chunk objects belonging to one in-flight segment.
    ///
    /// `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}.chunks/`
    ///
    /// The `.chunks` suffix distinguishes the chunk directory from the sealed
    /// segment object (`{segment}`) stored at the same parent prefix.
    pub fn wal_chunk_prefix(&self, timeline: u32, segment: &str) -> String {
        format!(
            "{}/pitr/{}/wal/{:08X}/{}.chunks/",
            self.org_id, self.project_id, timeline, segment
        )
    }

    /// Key for one 256 KiB streaming chunk within an in-flight WAL segment.
    ///
    /// `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}.chunks/{byte_offset:016X}`
    pub fn wal_chunk_key(&self, timeline: u32, segment: &str, byte_offset: usize) -> String {
        format!(
            "{}/pitr/{}/wal/{:08X}/{}.chunks/{:016X}",
            self.org_id, self.project_id, timeline, segment, byte_offset
        )
    }

    /// `{org}/metadata/{proj}/project.json`
    pub fn project_meta_key(&self) -> String {
        format!("{}/metadata/{}/project.json", self.org_id, self.project_id)
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
    pub fn rel_nblocks_key(&self, rf: &RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/nblocks",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
    }

    pub fn rel_meta_key(&self, rf: &RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/meta.json",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
    }

    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/`
    ///
    /// Prefix covering all express keys for a relation fork (chunks + nblocks).
    pub fn rel_chunks_prefix(&self, rf: &RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
    }

    /// `{org}/{proj}/chunks/` — prefix covering all chunk-related express keys
    /// for this project (chunk latest, nblocks, and deletion markers).
    pub fn all_chunks_express_prefix(&self) -> String {
        format!("{}/{}/chunks/", self.org_id, self.project_id)
    }

    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/.deleted`
    ///
    /// Deletion marker written by `delete_file` so that the checkpoint can
    /// record this fork in `deleted_forks` of the delta manifest even after
    /// the fork_meta entry has been evicted mid-interval.
    pub fn fork_deleted_marker_key(&self, rf: &RelFork) -> String {
        format!(
            "{}/{}/chunks/{}/{}/{}.{}/.deleted",
            self.org_id, self.project_id, rf.spc_oid, rf.db_oid, rf.rel_number, rf.fork_number
        )
    }

    /// Parse a `ChunkTag` from an express `latest` key:
    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/{chunk_id}/{tl:08X}/latest`
    ///
    /// Returns `None` for non-chunk keys (nblocks, deletion markers, etc.).
    pub fn parse_chunk_tag_from_express_key(&self, key: &str) -> Option<ChunkTag> {
        let prefix = self.all_chunks_express_prefix();
        let rest = key.strip_prefix(&prefix)?;
        // rest = "{spc}/{db}/{rel}.{fork}/{chunk_id}/{tl_hex}/latest"
        let mut parts = rest.splitn(7, '/');
        let spc_oid: u32 = parts.next()?.parse().ok()?;
        let db_oid: u32 = parts.next()?.parse().ok()?;
        let relfork_str = parts.next()?;
        let chunk_id: u32 = parts.next()?.parse().ok()?;
        let _tl_hex = parts.next()?;
        let suffix = parts.next()?;
        if suffix != "latest" {
            return None;
        }
        let dot = relfork_str.rfind('.')?;
        let rel_number: u32 = relfork_str[..dot].parse().ok()?;
        let fork_number: i32 = relfork_str[dot + 1..].parse().ok()?;
        Some(ChunkTag {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            chunk_id,
        })
    }

    /// Parse a `RelFork` from an express deletion marker key:
    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/.deleted`
    pub fn parse_relfork_from_deleted_marker(&self, key: &str) -> Option<RelFork> {
        let prefix = self.all_chunks_express_prefix();
        let rest = key.strip_prefix(&prefix)?;
        let rest = rest.strip_suffix("/.deleted")?;
        // rest = "{spc}/{db}/{rel}.{fork}"
        let mut parts = rest.splitn(4, '/');
        let spc_oid: u32 = parts.next()?.parse().ok()?;
        let db_oid: u32 = parts.next()?.parse().ok()?;
        let relfork_str = parts.next()?;
        let dot = relfork_str.rfind('.')?;
        let rel_number: u32 = relfork_str[..dot].parse().ok()?;
        let fork_number: i32 = relfork_str[dot + 1..].parse().ok()?;
        Some(RelFork {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
        })
    }

    /// Parse a `RelFork` from an express nblocks key:
    /// `{org}/{proj}/chunks/{spc}/{db}/{rel}.{fork}/nblocks`
    pub fn parse_relfork_from_nblocks_key(&self, key: &str) -> Option<RelFork> {
        let prefix = self.all_chunks_express_prefix();
        let rest = key.strip_prefix(&prefix)?;
        let rest = rest.strip_suffix("/nblocks")?;
        // rest = "{spc}/{db}/{rel}.{fork}"
        let mut parts = rest.splitn(4, '/');
        let spc_oid: u32 = parts.next()?.parse().ok()?;
        let db_oid: u32 = parts.next()?.parse().ok()?;
        let relfork_str = parts.next()?;
        let dot = relfork_str.rfind('.')?;
        let rel_number: u32 = relfork_str[..dot].parse().ok()?;
        let fork_number: i32 = relfork_str[dot + 1..].parse().ok()?;
        Some(RelFork {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
        })
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

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl ProjectMeta {
    /// Construct metadata for a root (non-branch) project.
    pub fn new_root(ns: &ProjectNamespace) -> Self {
        Self {
            ns: ns.clone(),
            parent_project_id: None,
            parent_branch_id: None,
            branch_checkpoint_lsn: None,
            branch_timeline_id: None,
            current_timeline_id: 1,
            created_at: now_secs(),
            status: "active".to_string(),
            deleted_at: None,
        }
    }

    /// Construct metadata for a child branch forked from `parent_ns`.
    pub fn new_branch(
        child_ns: &ProjectNamespace,
        parent_ns: &ProjectNamespace,
        parent_timeline: u32,
        branch_lsn: Lsn,
    ) -> Self {
        Self {
            ns: child_ns.clone(),
            parent_project_id: Some(parent_ns.project_id),
            parent_branch_id: Some(parent_ns.branch_id),
            branch_checkpoint_lsn: Some(branch_lsn),
            branch_timeline_id: Some(parent_timeline),
            current_timeline_id: 1,
            created_at: now_secs(),
            status: "active".to_string(),
            deleted_at: None,
        }
    }

    /// Write `project.json` for a root project to S3Sim.
    pub fn create_root(sim: &Store, ns: &ProjectNamespace) -> Result<()> {
        let meta = Self::new_root(ns);
        sim.put_standard(&ns.project_meta_key(), &serde_json::to_vec(&meta)?)?;
        Ok(())
    }

    /// Write `project.json` for a root project to S3Sim if it does not already exist.
    ///
    /// Called once after the initdb shutdown checkpoint so that subsequent
    /// `init_from_env` calls use `load()` (which fetches the real base manifest
    /// from S3Sim) instead of falling back to `bootstrap()` (which overwrites
    /// `base_manifest.bin` with an empty file on every process start).
    ///
    /// Idempotent: if `project.json` already exists it is left unchanged.
    /// Only call for root projects (`parent_project_id: None`); branches write
    /// their `project.json` via `create_branch`.
    pub fn ensure_root(sim: &Store, ns: &ProjectNamespace) -> Result<()> {
        let key = ns.project_meta_key();
        if sim.get_standard(&key).is_ok() {
            return Ok(());
        }
        Self::create_root(sim, ns)
    }

    pub fn load(sim: &Store, ns: &ProjectNamespace) -> Result<Self> {
        let key = ns.project_meta_key();
        let bytes = sim.get_standard(&key)?;
        let meta: Self = serde_json::from_slice(&bytes)?;
        Ok(meta)
    }
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
    /// During normal run (`is_under_postmaster() == true`), reads
    /// `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID` and panics if any
    /// are absent or not valid u64s.
    ///
    /// During initdb/single-user startup (`is_under_postmaster() == false`),
    /// uses `project_id=0` and `branch_id=0` regardless of env vars. For
    /// `org_id`, uses `TIKO_ORG_ID` when present (must parse as u64), else `0`.
    ///
    /// If `project.json` exists in S3Sim, loads the full `ProjectCtx`
    /// (namespace + base manifest). Otherwise falls back to `bootstrap`,
    /// which creates a namespace-only ctx with an empty manifest — correct
    /// for initdb where no project data exists yet.
    ///
    /// Silently ignored if `ProjectCtx` is already initialized (OnceLock).
    /// `S3Sim::init()` must be called before this.
    pub fn init_from_env(root_dir: &Path) {
        let ns = if is_under_postmaster() {
            ProjectNamespace::new_from_env()
        } else {
            let org_id = std::env::var(env::ENV_ORG_ID)
                .ok()
                .map(|v| {
                    v.parse::<u64>().unwrap_or_else(|_| {
                        panic!(
                            "Environment variable {} must be a valid u64",
                            env::ENV_ORG_ID
                        )
                    })
                })
                .unwrap_or(0);
            ProjectNamespace::new(org_id, 0, 0)
        };

        let sim = Store::get();

        // During initdb, project.json has not been written yet — fall back to
        // a namespace-only ctx with an empty manifest so writes are routed
        // correctly. The manifest is irrelevant during initdb (no reads).
        let ctx = ProjectCtx::load(&ns, root_dir, sim)
            .unwrap_or_else(|_| ProjectCtx::bootstrap(&ns, root_dir));
        ProjectCtx::init(ctx);
    }

    /// Construct a minimal `ProjectCtx` when `project.json` does not exist yet
    /// (e.g. during `initdb`).
    ///
    /// Sets the namespace so writes are routed to the correct S3Sim paths.
    /// Uses an empty zero-entry manifest — level-2 reads are never needed
    /// before the first checkpoint writes a base manifest.
    fn bootstrap(ns: &ProjectNamespace, root_dir: &Path) -> Self {
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
        let local_path = Manifest::local_manifest_path(root_dir);
        let base_manifest = Manifest::empty(&local_path).expect("failed to create empty manifest");
        ProjectCtx {
            meta,
            base_manifest,
        }
    }

    /// Load project.json from the sim store, download the latest base manifest,
    /// write the local TIKM file under `{root_dir}/tiko/base_manifest.bin`,
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
    pub fn load(ns: &ProjectNamespace, root_dir: &Path, sim: &Store) -> Result<Self> {
        // Step 1: fetch project.json
        let meta_key = ns.project_meta_key();
        let meta_bytes = sim.get_standard(&meta_key)?;
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

        let local_path = Manifest::local_manifest_path(root_dir);

        // Step 4: download or construct the base manifest
        let base_manifest = if let Some(&latest_lsn) = base_lsns.last() {
            let manifest_key = ns.base_manifest_key(timeline, latest_lsn);
            let bytes = sim.get_standard(&manifest_key)?;
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
    pub fn base_manifest_lookup(&self, _key: &ChunkTag) -> io::Result<Option<ChunkRef>> {
        //self.base_manifest.lookup(key)
        Ok(None)
    }

    /// Level-3 nblocks lookup: returns the block count recorded in the base
    /// manifest for this relation fork, or `None` if not present.
    pub fn base_manifest_lookup_nblocks(&self, rf: &RelFork) -> Option<u32> {
        self.base_manifest.lookup_nblocks(rf)
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
    sim: &Store,
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
    let bytes = sim.get_standard(&base_manifest_key)?;
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
        let bytes = sim.get_standard(&key)?;
        // Place each delta TIKM file alongside the output base manifest.
        let delta_path = out_path.with_file_name(format!("delta_{}.tikm", delta_lsn.to_hex()));
        deltas.push(Manifest::from_bytes(&bytes, &delta_path)?);
    }

    // Step 4: merge deltas into the base.
    base.apply_deltas(&deltas)?;

    Ok(base)
}

/// Create a child branch at `branch_lsn` forked from `parent_ns`.
///
/// Writes exactly two objects to the standard bucket:
/// - `child_ns.project_meta_key()` — serialised `ProjectMeta`
/// - `child_ns.base_manifest_key(1, branch_lsn)` — the merged base manifest (child starts on tl=1)
///
/// The branch is valid once both writes succeed.
pub fn create_branch(
    sim: &Store,
    parent_ns: &ProjectNamespace,
    parent_timeline: u32,
    child_ns: &ProjectNamespace,
    branch_lsn: Lsn,
) -> Result<ProjectMeta> {
    // Construct child project metadata.
    let meta = ProjectMeta::new_branch(child_ns, parent_ns, parent_timeline, branch_lsn);

    // Write project.json.
    sim.put_standard(&child_ns.project_meta_key(), &serde_json::to_vec(&meta)?)?;

    Ok(meta)
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
pub fn delete_branch(sim: &Store, branch_ns: &ProjectNamespace) -> Result<()> {
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
