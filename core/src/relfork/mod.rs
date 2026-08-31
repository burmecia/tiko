//! Relation-fork module: identifier types plus store-backed block operations.

pub mod ops;
mod types;

pub(crate) use types::ChunkTagIterItem;
pub use types::{REL_FORK_SIZE, RelFork, RelForkMeta};
