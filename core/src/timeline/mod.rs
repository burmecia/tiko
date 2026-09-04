//! Timeline subsystem types.
//!
//! Types in this module:
//! - [`TimelineState`] / [`ActiveCheckpoint`]: consolidated shmem state for
//!   the segment-based design. Lives inside `IoControl`. The chunk-presence
//!   Bloom filter lives in [`crate::utils::bloom`].
//! - [`CompactionRequest`] / [`PendingCompaction`]: basebackup→worker
//!   compaction coordination slot inside [`TimelineState`].
//! - [`TimelineSegment`] / [`CheckpointSummary`]: durable per-checkpoint
//!   summary stored on disk and in S3.

pub mod compaction;
pub mod draft;
pub mod segment;
pub mod state;

pub use compaction::*;
pub use segment::*;
pub use state::*;
