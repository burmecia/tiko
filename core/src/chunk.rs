//! Fundamental chunk and relation-fork types shared across the storage layer.

use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};

use serde::{Deserialize, Serialize};

pub use crate::relfork::{REL_FORK_SIZE, RelFork};

/// Number of blocks per chunk (32 blocks = 256 KB).
pub const BLOCKS_PER_CHUNK: u32 = 32;

/// Chunk size in bytes (32 × 8 KB = 256 KB).
pub const CHUNK_SIZE: usize = BLOCKS_PER_CHUNK as usize * BLCKSZ;

// FNV-1a 32-bit hash parameters for ChunkTag hashing.
const FNV_OFFSET: u32 = 2166136261;
const FNV_PRIME: u32 = 16777619;

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

    /// Return the [`RelFork`] this chunk belongs to.
    // pub fn relfork(&self) -> RelFork {
    //     RelFork::from(*self)
    // }

    pub fn start_block(&self) -> BlockNumber {
        self.chunk_id * BLOCKS_PER_CHUNK
    }

    pub fn end_block(&self) -> BlockNumber {
        (self.chunk_id + 1) * BLOCKS_PER_CHUNK - 1
    }

    pub fn end_block_exclusive(&self) -> BlockNumber {
        (self.chunk_id + 1) * BLOCKS_PER_CHUNK
    }

    /// FNV-1a hash for fast hash table probing.
    pub fn hash(&self) -> u32 {
        let mut h = FNV_OFFSET;
        for &byte in &self.spc_oid.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &self.db_oid.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &self.rel_number.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &self.fork_number.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &self.chunk_id.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        h
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

/// Per-chunk context yielded by [`ChunkTagIter`].
///
/// All byte offsets are relative to the flat caller-supplied buffer that spans
/// the full `[start_block, start_block+nblocks)` request.
#[derive(Debug)]
pub(crate) struct ChunkTagIterItem {
    /// The chunk being processed.
    pub tag: ChunkTag,
    /// True when all `BLOCKS_PER_CHUNK` blocks of the chunk are covered.
    pub is_full_chunk: bool,
    /// First block's offset within the chunk (0..BLOCKS_PER_CHUNK).
    pub block_offset: BlockNumber,
    /// Byte offset of this chunk's slice in the caller's buffer.
    pub buf_offset: usize,
    /// One-past-the-end byte offset of this chunk's slice in the caller's buffer.
    pub buf_end: usize,
}

/// Iterator over a contiguous block range, yielding a [`ChunkTagIterItem`] for
/// every chunk touched, with all per-chunk offsets pre-computed.
pub(crate) struct ChunkTagIter {
    current: ChunkTag,
    end_id: u32,
    /// Next block number to process (advances chunk by chunk).
    blkno: BlockNumber,
    start_block: BlockNumber,
    end_block: BlockNumber,
}

impl Iterator for ChunkTagIter {
    type Item = ChunkTagIterItem;

    fn next(&mut self) -> Option<ChunkTagIterItem> {
        if self.current.chunk_id > self.end_id {
            return None;
        }
        let tag = self.current;
        let nblks = tag.end_block().min(self.end_block) - self.blkno + 1;
        let block_offset = self.blkno - tag.start_block();
        let buf_offset = (self.blkno - self.start_block) as usize * BLCKSZ;
        let buf_end = buf_offset + nblks as usize * BLCKSZ;
        let is_full_chunk = nblks == BLOCKS_PER_CHUNK;
        self.blkno += nblks;
        self.current.chunk_id += 1;
        Some(ChunkTagIterItem {
            tag,
            is_full_chunk,
            block_offset,
            buf_offset,
            buf_end,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.end_id + 1).saturating_sub(self.current.chunk_id) as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ChunkTagIter {}

impl ChunkTag {
    /// Returns an iterator over all chunks touched by `[start_block, end_block]`
    /// (inclusive), yielding a [`ChunkTagIterItem`] with per-chunk offsets.
    ///
    /// `self` must be `ChunkTag::from_block(rf, start_block)`;
    /// `end` must be `ChunkTag::from_block(rf, end_block)`.
    ///
    /// # Panics
    /// Panics in debug builds if `end.chunk_id < self.chunk_id`.
    pub(crate) fn range(
        self,
        end: ChunkTag,
        start_block: BlockNumber,
        end_block: BlockNumber,
    ) -> ChunkTagIter {
        debug_assert!(
            end.chunk_id >= self.chunk_id,
            "end chunk must be >= start chunk"
        );
        ChunkTagIter {
            current: self,
            end_id: end.chunk_id,
            blkno: start_block,
            start_block,
            end_block,
        }
    }
}
