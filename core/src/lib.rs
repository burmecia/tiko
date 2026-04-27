pub mod chunk;
mod db;
pub mod env;
pub mod error;
pub mod io;
pub mod manifest;
pub mod ops;
pub mod org;
pub mod project;
pub mod recovery;
pub mod relfork;

pub use chunk::{BLOCKS_PER_CHUNK, CHUNK_TAG_SIZE, ChunkTag, RelFork};
pub use error::{Error, Result};
pub use io::store;
pub use io::store::Store;
pub use io::store::{backend, s3, s3_sim};
pub use io::{cache, checkpoint_history, io_control};

use std::path::PathBuf;

/// Get the root path for Tiko data, either from `TIKO_ROOT_PATH` or defaulting to `$PGDATA/tiko`.
pub fn tiko_root_path() -> PathBuf {
    if let Ok(p) = std::env::var(env::ENV_TIKO_ROOT_PATH) {
        PathBuf::from(p)
    } else {
        pgsys::common::data_dir_path().join("tiko")
    }
}
