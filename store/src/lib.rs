pub mod chunk;
pub mod manifest;
pub mod org;
pub mod project;
pub mod recovery;
pub mod sim_store;

pub use chunk::{BLOCKS_PER_CHUNK, CHUNK_TAG_SIZE, ChunkTag, RelFork};

use std::path::PathBuf;

pub fn tiko_root_path() -> PathBuf {
    if let Ok(p) = std::env::var("TIKO_ROOT_PATH") {
        PathBuf::from(p)
    } else {
        pgsys::common::data_dir_path().join("tiko")
    }
}
