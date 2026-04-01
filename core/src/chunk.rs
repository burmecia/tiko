//! Fundamental chunk and relation-fork types shared across the storage layer.

use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};
use serde::{Deserialize, Serialize};

/// Number of blocks per chunk (32 blocks = 256 KB).
pub const BLOCKS_PER_CHUNK: u32 = 32;

/// Chunk size in bytes (32 × 8 KB = 256 KB).
pub const CHUNK_SIZE: usize = BLOCKS_PER_CHUNK as usize * BLCKSZ;

// ── ChunkTag ──

/// Identifies a 256 KB chunk (32 contiguous blocks) within a relation fork.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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
    pub fn from_block(rf: RelFork, blkno: BlockNumber) -> Self {
        ChunkTag {
            spc_oid: rf.spc_oid,
            db_oid: rf.db_oid,
            rel_number: rf.rel_number,
            fork_number: rf.fork_number,
            chunk_id: blkno / BLOCKS_PER_CHUNK,
        }
    }

    /// Return the [`RelFork`] this chunk belongs to.
    pub fn rel_fork(&self) -> RelFork {
        RelFork::from(*self)
    }

    /// FNV-1a hash for fast hash table probing.
    pub fn hash(&self) -> u32 {
        const FNV_OFFSET: u32 = 2166136261;
        const FNV_PRIME: u32 = 16777619;

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
        format!(
            "{}/{}/{}.{}/{}",
            self.spc_oid, self.db_oid, self.rel_number, self.fork_number, self.chunk_id
        )
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

// ── ChunkLogEntry ──

/// A single entry in the unified `cache_log` file.
///
/// Three variants cover all state changes the checkpoint needs to track:
/// dirty chunk data (via sidecar file), nblocks updates, and fork deletions.
///
/// Wire layout:
/// ```text
/// ChunkDirty  [0x01][tag:20][seq:8 LE]  = 29 bytes
/// NblocksSet  [0x02][rf:16][n:4 LE]     = 21 bytes
/// ForkDeleted [0x03][rf:16]             = 17 bytes
/// ```
///
/// Parsing: read the 1-byte discriminant, then the fixed-size tail.
/// An incomplete trailing record (any byte count short) is silently dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkLogEntry {
    /// A 256 KB cache chunk was flushed to the express bucket.
    /// The compressed chunk data lives in `dirty_chunks/{tag}-{seq}`.
    ChunkDirty { tag: ChunkTag, seq: u64 },
    /// A relation fork's block count was set.
    NblocksSet { rf: RelFork, n: u32 },
    /// A relation fork was deleted.
    ForkDeleted { rf: RelFork },
}

impl ChunkLogEntry {
    /// Encode to bytes: 29, 21, or 17 bytes depending on variant.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ChunkLogEntry::ChunkDirty { tag, seq } => {
                let mut buf = Vec::with_capacity(29);
                buf.push(0x01);
                buf.extend_from_slice(&tag.encode());
                buf.extend_from_slice(&seq.to_le_bytes());
                buf
            }
            ChunkLogEntry::NblocksSet { rf, n } => {
                let mut buf = Vec::with_capacity(21);
                buf.push(0x02);
                buf.extend_from_slice(&rf.encode());
                buf.extend_from_slice(&n.to_le_bytes());
                buf
            }
            ChunkLogEntry::ForkDeleted { rf } => {
                let mut buf = Vec::with_capacity(17);
                buf.push(0x03);
                buf.extend_from_slice(&rf.encode());
                buf
            }
        }
    }

    /// Decode one entry starting at `buf[pos]`.
    ///
    /// Returns `(entry, bytes_consumed)` on success, or `None` if there are
    /// not enough bytes for the indicated variant (incomplete record).
    pub fn decode(buf: &[u8], pos: usize) -> Option<(ChunkLogEntry, usize)> {
        let discriminant = *buf.get(pos)?;
        match discriminant {
            0x01 => {
                // ChunkDirty: 1 + 20 + 8 = 29 bytes
                if pos + 29 > buf.len() {
                    return None;
                }
                let tag_buf: &[u8; 20] = buf[pos + 1..pos + 21].try_into().unwrap();
                let tag = ChunkTag::decode(tag_buf);
                let seq = u64::from_le_bytes(buf[pos + 21..pos + 29].try_into().unwrap());
                Some((ChunkLogEntry::ChunkDirty { tag, seq }, 29))
            }
            0x02 => {
                // NblocksSet: 1 + 16 + 4 = 21 bytes
                if pos + 21 > buf.len() {
                    return None;
                }
                let rf_buf: &[u8; REL_FORK_SIZE] = buf[pos + 1..pos + 17].try_into().unwrap();
                let rf = RelFork::decode(rf_buf);
                let n = u32::from_le_bytes(buf[pos + 17..pos + 21].try_into().unwrap());
                Some((ChunkLogEntry::NblocksSet { rf, n }, 21))
            }
            0x03 => {
                // ForkDeleted: 1 + 16 = 17 bytes
                if pos + 17 > buf.len() {
                    return None;
                }
                let rf_buf: &[u8; REL_FORK_SIZE] = buf[pos + 1..pos + 17].try_into().unwrap();
                let rf = RelFork::decode(rf_buf);
                Some((ChunkLogEntry::ForkDeleted { rf }, 17))
            }
            _ => None, // Unknown discriminant — treat as incomplete/corrupt.
        }
    }
}

// ── RelFork ──

/// Identifies a specific fork of a relation — the (spc, db, rel, fork) key
/// that appears throughout the storage layer. A [`ChunkTag`] is a `RelFork`
/// plus a `chunk_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelFork {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
}

impl From<ChunkTag> for RelFork {
    fn from(tag: ChunkTag) -> Self {
        RelFork {
            spc_oid: tag.spc_oid,
            db_oid: tag.db_oid,
            rel_number: tag.rel_number,
            fork_number: tag.fork_number,
        }
    }
}

/// Wire size of a serialised `RelFork` (4 × 4-byte LE fields).
pub const REL_FORK_SIZE: usize = 16;

impl RelFork {
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
}
