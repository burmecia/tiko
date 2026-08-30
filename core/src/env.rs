use std::env;
use std::path::PathBuf;

/// Shared (remote) storage root — the storage tree all databases in a
/// deployment share. S3Sim local filesystem now; real S3-compatible storage
/// later. Parent and branch databases set this identically so a branch can
/// read the parent's chunks (copy-on-write) via `ChunkRef.db_id`.
pub const ENV_TIKO_STORAGE_ROOT: &str = "TIKO_STORAGE_ROOT";

/// Per-database local path for non-shareable cache/state files
/// (`base_manifest.tikm`, `draft.spill`, `chunk_cache`). Each database
/// (parent or branch) uses its own value so these don't collide.
pub const ENV_TIKO_LOCAL_PATH: &str = "TIKO_LOCAL_PATH";

/// Environment variable names for db identity (org_id, db_id).
pub const ENV_ORG_ID: &str = "TIKO_ORG_ID";
pub const ENV_DB_ID: &str = "TIKO_DB_ID";

/// Environment variable for how often the compactor worker should materialize a new base manifest, in seconds (default: 3600).
pub const ENV_COMPACT_INTERVAL_SECS: &str = "TIKO_COMPACT_INTERVAL_SECS";

pub fn read_u64(name: &str) -> u64 {
    env::var(name)
        .unwrap_or_else(|_| panic!("Environment variable {name} must be set"))
        .parse()
        .unwrap_or_else(|_| panic!("Environment variable {name} must be a valid u64"))
}

pub fn read_u64_or(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

// Default base for tiko paths when the relevant env var is unset:
// `$PGDATA/tiko`.
fn data_dir_tiko() -> PathBuf {
    pgsys::common::data_dir_path().join("tiko")
}

/// Root of the SHARED (remote) storage tree. All Tiko object-store data
/// (chunks, manifests, WAL, backups) lives under `{storage_root}/s3sim/`,
/// namespaced by `{org}/{db}`. Currently a local-filesystem simulation
/// (`S3Sim`); will back a real S3-compatible bucket in production. Parent and
/// branch databases share this path so a branch can read the parent's chunks
/// (copy-on-write) via `ChunkRef.db_id`.
///
/// Set by `TIKO_STORAGE_ROOT`; defaults to `$PGDATA/tiko`.
pub fn storage_root_path() -> PathBuf {
    if let Ok(p) = env::var(ENV_TIKO_STORAGE_ROOT) {
        PathBuf::from(p)
    } else {
        data_dir_tiko()
    }
}

/// Per-database LOCAL path for cache/state files that must NOT be shared:
/// `base_manifest.tikm` (live manifest cache), `draft.spill` (draft buffer
/// overflow), `chunk_cache` (chunk cache backing). Each database (parent or
/// branch) uses its own `TIKO_LOCAL_PATH` so these don't collide.
///
/// Set by `TIKO_LOCAL_PATH`; defaults to `$PGDATA/tiko`.
pub fn local_path() -> PathBuf {
    if let Ok(p) = env::var(ENV_TIKO_LOCAL_PATH) {
        PathBuf::from(p)
    } else {
        data_dir_tiko()
    }
}
