//! Branch & project lifecycle — create / soft-delete branches, list projects.
//!
//! Thin wrappers over `store::project` primitives, adding the surrounding
//! steps that tikod owns (soft-delete bookkeeping, `list_projects` scan).
//!
//! PGDATA preparation for a new branch (skeleton extract, pg_state restore,
//! recovery_manifest.bin) is handled by `orchestrate::start` — lifecycle.rs
//! only manages the S3 metadata side.

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use pgsys::Lsn;
use store::project::{ProjectMeta, ProjectNamespace, create_branch as s3_create_branch};
use store::sim_store::SimStore;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Error {
    Store(io::Error),
    NotFound,
    Serialize(String),
    /// Wrapped error from s3worker project primitives.
    Project(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(e) => write!(f, "store error: {e}"),
            Error::NotFound => write!(f, "project not found"),
            Error::Serialize(s) => write!(f, "serialization error: {s}"),
            Error::Project(s) => write!(f, "project error: {s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Store(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Operations ────────────────────────────────────────────────────────────────

/// Create a branch forked from `parent_ns` at `branch_lsn` on `parent_timeline`.
///
/// Writes to the standard bucket:
/// - `child_ns.project_meta_key()` — child `ProjectMeta` with parent fields set
/// - `child_ns.base_manifest_key(1, branch_lsn)` — merged manifest at branch point
///
/// The PGDATA-side preparation (skeleton extract, pg_state restore,
/// recovery_manifest.bin) is separate and handled by `orchestrate::start`.
pub fn create_branch(
    sim: &SimStore,
    parent_ns: &ProjectNamespace,
    parent_timeline: u32,
    child_ns: &ProjectNamespace,
    branch_lsn: Lsn,
) -> Result<ProjectMeta> {
    let child_meta = s3_create_branch(sim, parent_ns, parent_timeline, child_ns, branch_lsn)
        .map_err(|e| Error::Project(e.to_string()))?;
    Ok(child_meta)
}

/// Soft-delete a branch: set `deleted_at` in `project.json`.
///
/// Physical removal of S3 objects is deferred to the next GC run which
/// detects `deleted_at` and calls the appropriate cleanup.
/// The caller is responsible for stopping PostgreSQL and releasing the lease
/// before calling this function.
pub fn delete_branch(sim: &SimStore, ns: &ProjectNamespace) -> Result<()> {
    let key = ns.project_meta_key();
    let bytes = sim.get_standard(&key)?.ok_or(Error::NotFound)?;
    let mut meta: ProjectMeta =
        serde_json::from_slice(&bytes).map_err(|e| Error::Serialize(e.to_string()))?;

    meta.deleted_at = Some(now_secs());
    meta.status = "deleted".to_owned();

    let json = serde_json::to_vec(&meta).map_err(|e| Error::Serialize(e.to_string()))?;
    sim.put_standard(&key, &json)?;
    Ok(())
}

/// List all live (non-deleted) projects / branches for an org.
///
/// Scans `{org}/metadata/` in the standard bucket, loads every
/// `*/project.json` found, and returns those without `deleted_at` set.
pub fn list_projects(sim: &SimStore, org_id: u64) -> Result<Vec<ProjectMeta>> {
    let prefix = format!("{}/metadata/", org_id);
    let keys = sim.list_prefix_standard(&prefix)?;

    let mut result = Vec::new();
    for key in keys {
        if !key.ends_with("/project.json") {
            continue;
        }
        let Some(bytes) = sim.get_standard(&key)? else {
            continue;
        };
        let Ok(meta) = serde_json::from_slice::<ProjectMeta>(&bytes) else {
            continue; // Skip malformed entries.
        };
        if meta.deleted_at.is_none() {
            result.push(meta);
        }
    }
    Ok(result)
}

/// Load the `ProjectMeta` for `ns` from the standard bucket.
pub fn get_project(sim: &SimStore, ns: &ProjectNamespace) -> Result<ProjectMeta> {
    let key = ns.project_meta_key();
    let bytes = sim.get_standard(&key)?.ok_or(Error::NotFound)?;
    serde_json::from_slice(&bytes).map_err(|e| Error::Serialize(e.to_string()))
}

// ── Internal ──────────────────────────────────────────────────────────────────

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use store::project::ProjectMeta;
    use tempfile::TempDir;

    fn temp_sim() -> (SimStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        (sim, dir)
    }

    fn root_ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 10, 1)
    }

    fn child_ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 20, 2)
    }

    /// Write a base manifest stub at `lsn` for `ns` on `tl`.
    fn write_base_stub(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lsn: Lsn) {
        use std::collections::HashMap;
        use store::manifest::Manifest;
        let tmp =
            std::env::temp_dir().join(format!("test_base_{}_{}.tikm", ns.branch_id, lsn.to_hex()));
        let m = Manifest::new(lsn, 0, vec![], HashMap::new(), vec![], &tmp).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(&ns.base_manifest_key(tl, lsn), &bytes)
            .unwrap();
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn create_branch_writes_child_project_json_with_parent_fields() {
        let (sim, _dir) = temp_sim();
        let parent = root_ns();
        let child = child_ns();

        // Set up parent state: project.json + a base manifest at branch_lsn.
        ProjectMeta::ensure_root(&sim, &parent).unwrap();
        let branch_lsn = Lsn::new(0x3000);
        write_base_stub(&sim, &parent, 1, branch_lsn);

        create_branch(&sim, &parent, 1, &child, branch_lsn).unwrap();

        let meta = get_project(&sim, &child).unwrap();
        assert_eq!(meta.ns.org_id, child.org_id);
        assert_eq!(meta.ns.project_id, child.project_id);
        assert_eq!(meta.parent_project_id, Some(parent.project_id));
        assert_eq!(meta.parent_branch_id, Some(parent.branch_id));
        assert_eq!(meta.branch_checkpoint_lsn, Some(branch_lsn));
        assert!(meta.deleted_at.is_none());
    }

    #[test]
    fn delete_branch_sets_deleted_at_without_removing_objects() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        delete_branch(&sim, &ns).unwrap();

        // project.json still exists — soft delete only.
        let meta = get_project(&sim, &ns).unwrap();
        assert!(meta.deleted_at.is_some());
        assert_eq!(meta.status, "deleted");
    }

    #[test]
    fn delete_branch_returns_not_found_for_missing_project() {
        let (sim, _dir) = temp_sim();
        let err = delete_branch(&sim, &root_ns()).unwrap_err();
        assert!(matches!(err, Error::NotFound));
    }

    #[test]
    fn list_projects_returns_live_projects_only() {
        let (sim, _dir) = temp_sim();
        let ns_a = ProjectNamespace::new(5, 10, 1);
        let ns_b = ProjectNamespace::new(5, 20, 1);
        let ns_c = ProjectNamespace::new(5, 30, 1);

        ProjectMeta::ensure_root(&sim, &ns_a).unwrap();
        ProjectMeta::ensure_root(&sim, &ns_b).unwrap();
        ProjectMeta::ensure_root(&sim, &ns_c).unwrap();

        // Delete project B.
        delete_branch(&sim, &ns_b).unwrap();

        let projects = list_projects(&sim, 5).unwrap();
        let ids: Vec<u64> = projects.iter().map(|m| m.ns.project_id).collect();
        assert!(ids.contains(&10), "project A should be listed");
        assert!(!ids.contains(&20), "deleted project B should be excluded");
        assert!(ids.contains(&30), "project C should be listed");
    }

    #[test]
    fn list_projects_returns_empty_for_org_with_no_projects() {
        let (sim, _dir) = temp_sim();
        let projects = list_projects(&sim, 999).unwrap();
        assert!(projects.is_empty());
    }
}
