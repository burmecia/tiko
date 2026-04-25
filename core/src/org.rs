//! Org lifecycle — create / soft-delete orgs.
//!
//! An org is the top-level namespace; all store keys are rooted at `{org}/`.
//! Each org has a metadata object at `{org}/metadata/org.json`.
//! Creating an org also writes the root `project.json` atomically.

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::io::store::Store;
use crate::project::{ProjectMeta, ProjectNamespace};

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrgMeta {
    pub org_id: u64,
    pub created_at: i64,
    /// Set by `delete_org`; absent on live orgs. GC uses this to schedule
    /// physical removal of all objects under `{org}/`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub deleted_at: Option<i64>,
}

#[derive(Debug)]
pub enum Error {
    Store(crate::Error),
    AlreadyExists,
    NotFound,
    Serialize(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(e) => write!(f, "store error: {e}"),
            Error::AlreadyExists => write!(f, "org already exists"),
            Error::NotFound => write!(f, "org not found"),
            Error::Serialize(s) => write!(f, "serialization error: {s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Store(e.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── OrgMeta methods ───────────────────────────────────────────────────────────

impl OrgMeta {
    /// `{org}/metadata/org.json`
    pub fn meta_key(&self) -> String {
        format!("{}/metadata/org.json", self.org_id)
    }

    /// `{org}/metadata/` — prefix for listing all project metadata under an org.
    pub fn metadata_prefix(&self) -> String {
        format!("{}/metadata/", self.org_id)
    }

    pub fn ensure_org_meta(sim: &Store, org_id: u64) -> Result<()> {
        let key = format!("{}/metadata/org.json", org_id);
        if sim.get_standard(&key).map_err(Error::Store).is_err() {
            // No org.json exists — create root org and project.
            Self::create(sim, org_id)?;
        }
        Ok(())
    }

    /// Create an org and its root project atomically.
    ///
    /// The root project always uses `project_id = 0` and `branch_id = 0`.
    /// Both `org.json` and `project.json` are written in a single logical step —
    /// an org without a root project is an invalid state.
    pub fn create(sim: &Store, org_id: u64) -> Result<OrgMeta> {
        let ns = ProjectNamespace::new(org_id, 0, 0);

        let meta = OrgMeta {
            org_id,
            created_at: now_secs(),
            deleted_at: None,
        };
        let json = serde_json::to_vec(&meta).map_err(|e| Error::Serialize(e.to_string()))?;
        sim.put_standard(&meta.meta_key(), &json)
            .map_err(Error::Store)?;

        // Write root project.json (no parent fields — this is the origin project).
        ProjectMeta::create_root(sim, &ns).map_err(|e| {
            Error::Store(crate::Error::Io(io::Error::new(
                io::ErrorKind::Other,
                e.to_string(),
            )))
        })?;

        Ok(meta)
    }

    /// Read `org.json` without modifying it.
    pub fn get(sim: &Store, org_id: u64) -> Result<OrgMeta> {
        let key = format!("{}/metadata/org.json", org_id);
        let bytes = sim.get_standard(&key).map_err(Error::Store)?;
        serde_json::from_slice(&bytes).map_err(|e| Error::Serialize(e.to_string()))
    }

    /// Soft-delete an org: set `deleted_at` in `org.json`.
    ///
    /// Physical removal of all `{org}/` objects is deferred to the GC run.
    /// With `force = false`, returns `AlreadyExists` (i.e. already deleted) if
    /// `deleted_at` is already set.
    pub fn delete(sim: &Store, org_id: u64, force: bool) -> Result<OrgMeta> {
        let key = format!("{}/metadata/org.json", org_id);
        let bytes = sim.get_standard(&key).map_err(Error::Store)?;
        let mut meta: OrgMeta =
            serde_json::from_slice(&bytes).map_err(|e| Error::Serialize(e.to_string()))?;

        if meta.deleted_at.is_some() && !force {
            return Err(Error::AlreadyExists);
        }

        meta.deleted_at = Some(now_secs());
        let json = serde_json::to_vec(&meta).map_err(|e| Error::Serialize(e.to_string()))?;
        sim.put_standard(&key, &json).map_err(Error::Store)?;
        Ok(meta)
    }
}

// ── Internal ──────────────────────────────────────────────────────────────────

#[inline]
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
