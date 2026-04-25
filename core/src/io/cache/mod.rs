//! Local cache layer for S3-backed block storage.
//!
//! This module keeps the two cache subsystems separate:
//! - `ChunkCache`: write-back cache of 256 KB relation chunks
//! - `RelForkMetaCache`: write-back cache of per-fork `nblocks` and deletion state
//!
//! `CacheControl` remains the shared-memory entry point embedded in `IoControl`
//! and forwards the public operations to the appropriate subsystem.

mod chunk;
mod control;
mod meta;
pub(super) mod rwlock;

pub use chunk::CHUNK_NUM_SLOTS;
pub(crate) use chunk::{CHUNK_NUM_BUCKETS, ChunkCache, ChunkSlot};
pub use control::CacheControl;
pub(crate) use meta::{META_NUM_BUCKETS, META_NUM_SLOTS, MetaCache, MetaSlot};

pub(super) const CHAIN_NIL: u32 = u32::MAX;
pub(super) const MAX_USAGE_COUNT: u8 = 5;
