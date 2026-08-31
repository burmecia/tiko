//! Relation-fork identifier: the (spc, db, rel, fork) key for a relation fork.

use crate::chunk::{BLOCKS_PER_CHUNK, ChunkTag, FNV_OFFSET, fnv1a_step};
use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};
use pgsys::smgr::SMgrRelationData;
use serde::{Deserialize, Serialize};

/// Wire size of a serialised `RelFork` (4 × 4-byte LE fields).
pub const REL_FORK_SIZE: usize = 16;

/// Identifies a specific fork of a relation — the (spc, db, rel, fork) key
/// that appears throughout the storage layer. A [`ChunkTag`](crate::chunk::ChunkTag)
/// is a `RelFork` plus a `chunk_id`.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct RelFork {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
}

impl RelFork {
    pub fn new(
        spc_oid: Oid,
        db_oid: Oid,
        rel_number: RelFileNumber,
        fork_number: ForkNumber,
    ) -> Self {
        RelFork {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
        }
    }

    // FFI boundary: `reln` points to PG process-local smgr state.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn from_rel(reln: *mut SMgrRelationData, fork_number: ForkNumber) -> Self {
        let loc = unsafe { &(*reln).smgr_rlocator.locator };
        RelFork {
            spc_oid: loc.spc_oid,
            db_oid: loc.db_oid,
            rel_number: loc.rel_number,
            fork_number,
        }
    }

    /// Encode into the 16-byte on-disk representation (all fields LE).
    pub fn encode(&self) -> [u8; REL_FORK_SIZE] {
        let mut buf = [0u8; REL_FORK_SIZE];
        buf[0..4].copy_from_slice(&self.spc_oid.to_le_bytes());
        buf[4..8].copy_from_slice(&self.db_oid.to_le_bytes());
        buf[8..12].copy_from_slice(&self.rel_number.to_le_bytes());
        buf[12..16].copy_from_slice(&self.fork_number.to_le_bytes());
        buf
    }

    /// Decode from the 16-byte on-disk representation.
    pub fn decode(buf: &[u8; REL_FORK_SIZE]) -> Self {
        RelFork {
            spc_oid: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            db_oid: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            rel_number: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            fork_number: i32::from_le_bytes(buf[12..16].try_into().unwrap()),
        }
    }

    /// FNV-1a hash over the four `RelFork` fields.
    pub(crate) fn hash(&self) -> u32 {
        let mut h = FNV_OFFSET;
        h = fnv1a_step(h, &self.spc_oid.to_le_bytes());
        h = fnv1a_step(h, &self.db_oid.to_le_bytes());
        h = fnv1a_step(h, &self.rel_number.to_le_bytes());
        fnv1a_step(h, &self.fork_number.to_le_bytes())
    }

    /// Iterate over every chunk touched by `[start_block, start_block+nblocks)`,
    /// yielding a [`ChunkTagIterItem`] with all per-chunk offsets pre-computed.
    pub(crate) fn chunk_block_range(
        &self,
        start_block: BlockNumber,
        nblocks: BlockNumber,
    ) -> ChunkTagIter {
        let end_block = start_block + nblocks - 1;
        ChunkTag::range(
            ChunkTag::from_block(self, start_block),
            ChunkTag::from_block(self, end_block),
            start_block,
            end_block,
        )
    }
}

impl std::fmt::Display for RelFork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}.{}",
            self.spc_oid, self.db_oid, self.rel_number, self.fork_number
        )
    }
}

impl From<&ChunkTag> for RelFork {
    fn from(tag: &ChunkTag) -> Self {
        RelFork {
            spc_oid: tag.spc_oid,
            db_oid: tag.db_oid,
            rel_number: tag.rel_number,
            fork_number: tag.fork_number,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct RelForkMeta {
    pub nblocks: u32,
    pub deleted: bool,
}

impl RelForkMeta {
    pub fn new(nblocks: u32, deleted: bool) -> Self {
        RelForkMeta { nblocks, deleted }
    }

    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("failed to serialize RelForkMeta")
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
