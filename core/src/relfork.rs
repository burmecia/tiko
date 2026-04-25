//! Relation-fork identifier: the (spc, db, rel, fork) key for a relation fork.

use crate::chunk::{ChunkTag, ChunkTagIter};
use pgsys::common::{BlockNumber, ForkNumber, Oid, RelFileNumber};
use pgsys::smgr::SMgrRelationData;
use serde::{Deserialize, Serialize};

// FNV-1a 32-bit hash parameters.
const FNV_OFFSET: u32 = 2166136261;
const FNV_PRIME: u32 = 16777619;

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
        h
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

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct RelForkMeta {
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
