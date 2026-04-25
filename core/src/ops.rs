//! Store-backed block-level read/write operations.
//!
//! Two-layer storage: **shared-memory chunk cache → S3Sim (express bucket)**.
//! The local backing-file layer (`{DataDir}/tiko/`) has been removed.
//!
//! # Public surface
//!
//! | Function | Purpose |
//! |---|---|
//! | `exists` | Check whether a relation fork exists (nblocks key present) |
//! | `create` | Create a relation fork (write nblocks=0) |
//! | `nblocks` | Block count: max(S3Sim nblocks, cache max) |
//! | `read_blocks` | Read blocks: cache hit or S3Sim fetch |
//! | `write_blocks` | Write blocks: cache (or initdb S3Sim RMW) |
//! | `truncate_fork` | Truncate: invalidate cache + trim S3Sim + update nblocks |
//! | `delete_fork` | Delete: invalidate cache + remove all S3Sim chunks |
//! | `prefetch_blocks` | Prefetch: populate cache from S3Sim |

use crate::{
    chunk::{CHUNK_SIZE, ChunkTag, ChunkTagIterItem, RelFork},
    error::{Error, Result},
    io::store::Store,
    io_control::IoControl,
};
use pgsys::common::{BLCKSZ, BlockNumber};

/// Check whether a relation fork exists.
///
/// # Returns
/// - `Ok(true)` if the fork exists (nblocks key present and not deleted)
/// - `Ok(false)` if the fork does not exist (nblocks key missing or marked as deleted)
/// - `Err(errno)` any failure
pub fn exists(rf: &RelFork) -> Result<bool> {
    let result = if IoControl::cache_is_available() {
        IoControl::get_cache().get_deleted(rf)
    } else {
        Store::try_get()?.get_deleted(rf)
    };

    match result {
        Ok(deleted) => Ok(!deleted),
        Err(err) if err.is_not_found() => Ok(false),
        Err(err) => Err(err),
    }
}

/// Create a relation fork.
///
/// # Returns
/// - `Ok(true)` if a new fork was created
/// - `Ok(false)` if the fork already existed
/// - `Err(errno)` any failure
pub fn create(rf: &RelFork) -> Result<bool> {
    let result = if IoControl::cache_is_available() {
        IoControl::get_cache().create_relfork(rf)
    } else {
        Store::try_get()?.create_relfork(rf)
    };

    match result {
        Ok(()) => Ok(true),
        Err(err) if err.is_already_exists() => Ok(false),
        Err(err) => Err(err),
    }
}

pub fn get_nblocks(rf: &RelFork) -> Result<BlockNumber> {
    if IoControl::cache_is_available() {
        IoControl::get_cache().get_nblocks(rf)
    } else {
        Store::try_get()?.get_nblocks(rf)
    }
}

/// Cache-aware truncate. Invalidates cache blocks at or beyond `nblocks`,
/// deletes excess chunks from S3Sim, then updates the nblocks key.
///
/// Order matters: invalidating first prevents a dirty block in the truncated
/// range from being flushed by `flush_dirty_chunk` after the S3Sim chunks
/// are removed.
///
/// Falls back to only updating the nblocks key when the cache is unavailable.
pub fn truncate_relfork(rf: &RelFork, nblocks: BlockNumber) -> Result<()> {
    if IoControl::cache_is_available() {
        IoControl::get_cache().truncate_relfork(rf, nblocks)
    } else {
        Store::try_get()?.put_nblocks(rf, nblocks)
    }
}

/// Cache-aware delete. Invalidates ALL cache blocks for the relation fork,
/// then removes all express objects (chunks + nblocks key) from S3Sim.
///
/// Order matters: invalidating first prevents dirty blocks from being flushed
/// by `flush_dirty_chunk` after the S3Sim objects are gone — which would
/// silently recreate them. It also prevents stale cache hits if the same
/// `rel_number` is later reused.
///
/// Falls back to only removing S3Sim objects when the cache is unavailable.
pub fn delete_fork(rf: &RelFork) -> Result<()> {
    if IoControl::cache_is_available() {
        IoControl::get_cache().delete_relfork(rf)?;
    } else {
        Store::try_get()?.delete_relfork(rf)?;
    }

    Ok(())
}

fn get_chunk_merge(
    chunk_item: &ChunkTagIterItem,
    dst: &mut [u8],
    get_chunk: impl Fn(&ChunkTag, &mut [u8]) -> Result<()>,
) -> Result<()> {
    if chunk_item.is_full_chunk {
        // Full-chunk read can be read directly into caller's buffer.
        get_chunk(&chunk_item.tag, dst)?;
    } else {
        // For partial-chunk reads, we need to read the full chunk into a temp
        // buffer and copy out the requested portion.
        let temp_buf = &mut vec![0u8; CHUNK_SIZE];
        get_chunk(&chunk_item.tag, temp_buf)?;

        let offset = chunk_item.block_offset as usize * BLCKSZ;
        dst.copy_from_slice(&temp_buf[offset..offset + dst.len()]);
    }
    Ok(())
}

/// Read `nblocks` blocks starting at `block_number` into the caller's buffer.
///
/// Returns the number of blocks actually read, which may be less than
/// `nblocks` if the request extends past `rf_nblocks`. Returns
/// `Err::unexpected_eof` if `block_number > rf_nblocks`.
///
/// Concurrency: `rf_nblocks` is sampled once on entry and then used to clip
/// the request. This is safe because callers are expected to hold at least
/// a shared relation lock on `rf` (PG `AccessShareLock`), which serialises
/// against truncate's `AccessExclusiveLock` and prevents `rf_nblocks` from
/// shrinking during the read.
///
/// On `Err`, the contents of `buffer_ptr` are unspecified — earlier loop
/// iterations may have already written to it before a later chunk fetch
/// failed. Callers must treat the buffer as poisoned on error.
pub fn read_blocks(
    rf: &RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *mut u8,
) -> Result<BlockNumber> {
    let rf_nblocks = get_nblocks(rf)?;

    if block_number == rf_nblocks {
        return Ok(0);
    } else if block_number > rf_nblocks {
        return Err(Error::unexpected_eof("read block beyond end of file"));
    }

    let nblocks_to_read = nblocks.min(rf_nblocks - block_number);
    if nblocks_to_read == 0 {
        return Ok(0);
    }

    let dst =
        unsafe { std::slice::from_raw_parts_mut(buffer_ptr, nblocks_to_read as usize * BLCKSZ) };

    for item in rf.chunk_block_range(block_number, nblocks_to_read) {
        let dst_buf = &mut dst[item.buf_offset..item.buf_end];

        if IoControl::cache_is_available() {
            let cache = IoControl::get_cache();
            get_chunk_merge(&item, dst_buf, |tag, buf| cache.get_chunk(tag, buf))?;
        } else {
            let store = Store::get();
            get_chunk_merge(&item, dst_buf, |tag, buf| store.get_chunk(tag, buf))?;
        }
    }

    Ok(nblocks_to_read)
}

pub fn write_blocks(
    rf: &RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *const u8,
) -> Result<BlockNumber> {
    // Get current nblocks for the relfork.
    let rf_nblocks = if IoControl::cache_is_available() {
        IoControl::get_cache().get_nblocks(&rf)?
    } else {
        Store::get().get_nblocks(&rf)?
    };

    if nblocks == 0 {
        return Ok(0);
    }

    let src_data = unsafe { std::slice::from_raw_parts(buffer_ptr, nblocks as usize * BLCKSZ) };

    for item in rf.chunk_block_range(block_number, nblocks) {
        let chunk_data = &src_data[item.buf_offset..item.buf_end];

        if IoControl::cache_is_available() {
            // patch_chunk handles both full-chunk overwrites and partial-chunk
            // atomic RMWs under io_lock.
            IoControl::get_cache().patch_chunk(&item.tag, item.block_offset, chunk_data)?;
        } else {
            // Cache-unavailable path (initdb): single-threaded, so no io_lock
            // is needed. Do the read-merge-write inline.
            Store::get().patch_chunk(&item.tag, item.block_offset, chunk_data)?;
        }
    }

    // Update RelFork nblocks if this write extends the EOF.
    let new_nblocks = block_number + nblocks;
    if new_nblocks > rf_nblocks {
        if IoControl::cache_is_available() {
            IoControl::get_cache().put_nblocks(&rf, new_nblocks)?;
        } else {
            Store::get().put_nblocks(&rf, new_nblocks)?;
        }
    }

    Ok(nblocks)
}

pub fn prefetch_blocks(
    rf: &RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
) -> Result<BlockNumber> {
    if IoControl::cache_is_available() {
        let mut dummy_buf = vec![0u8; nblocks as usize * BLCKSZ];
        read_blocks(rf, block_number, nblocks, dummy_buf.as_mut_ptr())
    } else {
        Ok(0)
    }
}

pub fn flush_dirty_for_relfork(rf: &RelFork) -> Result<()> {
    if IoControl::cache_is_available() {
        IoControl::get_cache().flush_dirty_for_relfork(rf)?;
    }
    Ok(())
}
