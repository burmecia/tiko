pub mod cache;
pub mod chunk;
mod db;
pub mod env;
pub mod error;
pub mod io;
pub mod manifest;
pub mod relfork;
pub mod storage;
pub mod store;
pub mod timeline;
pub mod utils;

pub use chunk::{BLOCKS_PER_CHUNK, CHUNK_TAG_SIZE, ChunkTag};
pub use db::DbNamespace;
pub use env::{local_path, storage_root_path};
pub use error::{Error, Result};
pub use io::io_control;
pub use relfork::RelFork;
pub use storage::{s3, s3_sim};
pub use store::Store;

// Test-binary stand-in for the postmaster-provided elog wrapper (same role
// as cli's `pg_stubs.rs`): lock/watchdog code reachable from unit tests
// references it, and without a definition the test binary fails to link.
#[cfg(test)]
#[unsafe(no_mangle)]
extern "C" fn rust_pg_log(_elevel: std::ffi::c_int, _message: *const std::ffi::c_char) {}
