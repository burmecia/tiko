use std::sync::atomic::AtomicU32;

use super::{ChunkCache, ChunkSlot, MetaCache, MetaSlot};
use crate::{
    chunk::{CHUNK_SIZE, ChunkTag, RelFork},
    error::Result,
    io::cache::rwlock::AtomicRWLock,
};
use pgsys::common::BlockNumber;

#[repr(C)]
pub struct CacheControl {
    chunk_cache: ChunkCache,
    meta_cache: MetaCache,
}

impl CacheControl {
    pub(crate) fn init(
        &mut self,
        chunk_slots: *mut ChunkSlot,
        chunk_buckets: *mut AtomicU32,
        chunk_bucket_locks: *mut AtomicRWLock,
        chunk_io_locks: *mut AtomicRWLock,
        meta_slots: *mut MetaSlot,
        meta_buckets: *mut AtomicU32,
        meta_locks: *mut AtomicRWLock,
        meta_io_locks: *mut AtomicRWLock,
    ) {
        self.chunk_cache.init(
            chunk_slots,
            chunk_buckets,
            chunk_bucket_locks,
            chunk_io_locks,
        );
        self.meta_cache
            .init(meta_slots, meta_buckets, meta_locks, meta_io_locks);
    }

    // --------- new interface ------------

    pub(crate) fn get_nblocks(&self, rf: &RelFork) -> Result<BlockNumber> {
        self.meta_cache.get_nblocks(rf)
    }

    pub(crate) fn put_nblocks(&self, rf: &RelFork, nblocks: BlockNumber) -> Result<()> {
        self.meta_cache.put_nblocks(rf, nblocks)
    }

    pub(crate) fn get_deleted(&self, rf: &RelFork) -> Result<bool> {
        self.meta_cache.get_deleted(rf)
    }

    pub(crate) fn create_relfork(&self, rf: &RelFork) -> Result<()> {
        self.meta_cache.create_relfork(rf)
    }

    pub fn truncate_relfork(&self, rf: &RelFork, first_block: BlockNumber) -> Result<()> {
        // Meta first: narrows visible nblocks before the chunk-cache cleanup
        // so concurrent readers cannot race against the chunk unlinks.
        // put_nblocks preserves the deleted flag and errors if the relfork
        // is deleted or missing.
        self.meta_cache.put_nblocks(rf, first_block)?;
        self.chunk_cache.truncate_relfork(rf, first_block);
        Ok(())
    }

    pub(crate) fn delete_relfork(&self, rf: &RelFork) -> Result<()> {
        self.meta_cache.put_deleted(rf, true)?;
        self.chunk_cache.truncate_relfork(rf, 0);
        Ok(())
    }

    // ----- new chunk interface -----

    pub(crate) fn get_chunk(&self, tag: &ChunkTag, dst: &mut [u8]) -> Result<()> {
        debug_assert_eq!(dst.len(), CHUNK_SIZE);
        self.chunk_cache.get_chunk(tag, dst)
    }

    /// Partial-chunk atomic RMW — see `ChunkCache::patch_chunk`.
    pub(crate) fn patch_chunk(&self, tag: &ChunkTag, block_offset: u32, data: &[u8]) -> Result<()> {
        self.chunk_cache.patch_chunk(tag, block_offset, data)
    }

    // ----- new flush interface -----

    pub fn flush_dirty(&self) -> Result<(u32, u32)> {
        let flushed_chunks = self.chunk_cache.flush_dirty_chunks(None)?;
        let flushed_metas = self.meta_cache.flush_dirty_metas(None)?;
        Ok((flushed_chunks, flushed_metas))
    }

    pub fn flush_dirty_for_relfork(&self, relfork: &RelFork) -> Result<(u32, u32)> {
        let flushed_chunks = self.chunk_cache.flush_dirty_chunks(Some(relfork))?;
        let flushed_metas = self.meta_cache.flush_dirty_metas(Some(relfork))?;
        Ok((flushed_chunks, flushed_metas))
    }
}
