pub mod chunk;
mod db;
pub mod env;
pub mod error;
pub mod io;
pub mod manifest;
pub mod ops;
pub mod org;
pub mod pgcontrol;
pub mod pitr;
pub mod relfork;

pub use chunk::{BLOCKS_PER_CHUNK, CHUNK_TAG_SIZE, ChunkTag, RelFork};
pub use db::DbNamespace;
pub use error::{Error, Result};
pub use io::storage;
pub use io::storage::{s3, s3_sim};
pub use io::store::Store;
pub use io::{cache, io_control};

use std::path::PathBuf;

/// Default base for tiko paths when the relevant env var is unset:
/// `$PGDATA/tiko`.
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
    if let Ok(p) = std::env::var(env::ENV_TIKO_STORAGE_ROOT) {
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
    if let Ok(p) = std::env::var(env::ENV_TIKO_LOCAL_PATH) {
        PathBuf::from(p)
    } else {
        data_dir_tiko()
    }
}
