//! Timeline subsystem types.
//!
//! Types in this module:
//! - [`TimelineState`] / [`ActiveCheckpoint`] / [`ChunkBloom`]: consolidated
//!   shmem state for the segment-based design. Lives inside `IoControl`.
//! - [`TimelineSegment`] / [`CheckpointSummary`]: durable per-checkpoint
//!   summary stored on disk and in S3.

pub mod segment;
pub mod state;

pub use segment::*;
pub use state::*;
