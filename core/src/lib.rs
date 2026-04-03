pub mod chunk;
pub mod io;
pub mod manifest;
pub mod org;
pub mod project;
pub mod recovery;
pub mod store;

pub use chunk::{BLOCKS_PER_CHUNK, CHUNK_TAG_SIZE, ChunkLogEntry, ChunkTag, RelFork};
pub use io::{cache, fork_nblocks, io_control};
pub use store::Store;
pub use store::{backend, ops, s3, s3_sim};

use std::path::PathBuf;

/// Environment variable for the Tiko root path (overrides default of `$PGDATA/tiko`).
pub const ENV_TIKO_ROOT_PATH: &str = "TIKO_ROOT_PATH";

// Environment variable names for project identity (org_id, project_id, branch_id).
pub const ENV_ORG_ID: &str = "TIKO_ORG_ID";
pub const ENV_PROJECT_ID: &str = "TIKO_PROJECT_ID";
pub const ENV_BRANCH_ID: &str = "TIKO_BRANCH_ID";

/// Environment variable for how often the PITR worker should materialize a new base manifest, in seconds (default: 3600).
pub const ENV_PITR_INTERVAL_SECS: &str = "TIKO_PITR_INTERVAL_SECS";

/// Get the root path for Tiko data, either from `TIKO_ROOT_PATH` or defaulting to `$PGDATA/tiko`.
pub fn tiko_root_path() -> PathBuf {
    if let Ok(p) = std::env::var(ENV_TIKO_ROOT_PATH) {
        PathBuf::from(p)
    } else {
        pgsys::common::data_dir_path().join("tiko")
    }
}
