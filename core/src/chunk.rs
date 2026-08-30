//! Fundamental chunk and relation-fork types shared across the storage layer.

use pgsys::Lsn;
use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};

use serde::{Deserialize, Serialize};

use crate::relfork::RelFork;

/// Number of blocks per chunk (32 blocks = 256 KB).
pub const BLOCKS_PER_CHUNK: u32 = 32;

/// Chunk size in bytes (32 × 8 KB = 256 KB).
pub const CHUNK_SIZE: usize = BLOCKS_PER_CHUNK as usize * BLCKSZ;

// FNV-1a 32-bit hash parameters for ChunkTag hashing.
pub(crate) const FNV_OFFSET: u32 = 2166136261;
pub(crate) const FNV_PRIME: u32 = 16777619;

/// Fold bytes into an existing FNV-1a state.
pub(crate) fn fnv1a_step(mut h: u32, bytes: &[u8]) -> u32 {
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

// ── ChunkTag ──

/// Identifies a 256 KB chunk (32 contiguous blocks) within a relation fork.
#[repr(C)]
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ChunkTag {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
    pub chunk_id: u32, // = blkno / BLOCKS_PER_CHUNK
}

/// Wire size of a serialised `ChunkTag` (5 × u32 LE).
pub const CHUNK_TAG_SIZE: usize = 20;

const _: () = assert!(std::mem::size_of::<ChunkTag>() == CHUNK_TAG_SIZE);

impl ChunkTag {
    /// Construct a ChunkTag from a [`RelFork`] and a block number.
    pub fn from_block(rf: &RelFork, blkno: BlockNumber) -> Self {
        ChunkTag {
            spc_oid: rf.spc_oid,
            db_oid: rf.db_oid,
            rel_number: rf.rel_number,
            fork_number: rf.fork_number,
            chunk_id: blkno / BLOCKS_PER_CHUNK,
        }
    }

    pub fn start_block(&self) -> BlockNumber {
        self.chunk_id * BLOCKS_PER_CHUNK
    }

    pub fn end_block(&self) -> BlockNumber {
        self.end_block_exclusive() - 1
    }

    pub fn end_block_exclusive(&self) -> BlockNumber {
        (self.chunk_id + 1) * BLOCKS_PER_CHUNK
    }

    /// FNV-1a hash for fast hash table probing.
    pub fn hash(&self) -> u32 {
        let mut h = FNV_OFFSET;
        h = fnv1a_step(h, &self.spc_oid.to_le_bytes());
        h = fnv1a_step(h, &self.db_oid.to_le_bytes());
        h = fnv1a_step(h, &self.rel_number.to_le_bytes());
        h = fnv1a_step(h, &self.fork_number.to_le_bytes());
        fnv1a_step(h, &self.chunk_id.to_le_bytes())
    }

    /// Format this chunk tag as a storage path segment:
    /// `{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}`.
    pub fn to_path(&self) -> String {
        let rf = RelFork::from(self);
        format!("{rf}/{}", self.chunk_id)
    }

    /// Encode into the 20-byte TIKM on-disk representation (all fields LE).
    pub fn encode(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..4].copy_from_slice(&self.spc_oid.to_le_bytes());
        buf[4..8].copy_from_slice(&self.db_oid.to_le_bytes());
        buf[8..12].copy_from_slice(&self.rel_number.to_le_bytes());
        buf[12..16].copy_from_slice(&self.fork_number.to_le_bytes());
        buf[16..20].copy_from_slice(&self.chunk_id.to_le_bytes());
        buf
    }

    /// Decode from the 20-byte TIKM on-disk representation.
    pub fn decode(buf: &[u8; 20]) -> Self {
        ChunkTag {
            spc_oid: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            db_oid: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            rel_number: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            fork_number: i32::from_le_bytes(buf[12..16].try_into().unwrap()),
            chunk_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        }
    }
}

// ── ChunkRef ──

/// Reference to a specific version of a chunk stored in S3.
///
/// Note: no `#[repr(C)]` and no `size_of` assert here — `ChunkRef` is never
/// cast to raw bytes. Its in-memory size is 24 bytes (4-byte alignment padding
/// between `timeline_id: u32` and `lsn: u64`), while the wire encoding is 20
/// bytes. The wire size is enforced by `encode() -> [u8; 20]`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) struct ChunkRef {
    /// Branch-scoped id: selects `{org}/chunks/{db_id}/` in the standard bucket.
    pub db_id: u64,
    /// Timeline on which this chunk version was written.
    /// Together with `db_id` and `lsn`, uniquely identifies the S3 object:
    /// `{org}/chunks/{db_id}/{tag}/{timeline_id:08X}/{lsn_hex}`.
    pub timeline_id: u32,
    /// Checkpoint LSN at which this chunk version was sealed.
    pub lsn: Lsn,
}

impl ChunkRef {
    pub(crate) fn encode(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..8].copy_from_slice(&self.db_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.timeline_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.lsn.as_u64().to_le_bytes());
        buf
    }

    pub(crate) fn decode(buf: &[u8; 20]) -> Self {
        ChunkRef {
            db_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            timeline_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            lsn: Lsn::new(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
        }
    }
}

/// Wire size of a serialised `ChunkRef` (u64 + u32 + u64 LE, no padding).
pub(crate) const CHUNK_REF_SIZE: usize = 20;
// In-memory size is 24 (4-byte padding after timeline_id:u32 before lsn:u64); wire
// encoding is 20 (explicit encode/decode, no padding). Catches accidental layout changes.
const _: () = assert!(std::mem::size_of::<ChunkRef>() == 24);
