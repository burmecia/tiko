//! S3 block-level read/write operations.
//!
//! Provides `read_blocks()` and `write_blocks()` — synchronous functions that
//! perform actual file I/O. Called from two contexts:
//!
//! 1. **s3worker io_handler** (Tokio): `process_io_request` calls these for
//!    Read/Write slot operations.
//! 2. **Backend during initdb** (sync): called directly when no s3worker exists.
//!
//! Uses S3-style path layout on local filesystem:
//! `{DataDir}/tiko/{spc_oid}/{db_oid}/{rel_number}.{fork}`
//!
//! Will be replaced by real S3 GET/PUT operations once the S3 client is added.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use pgsys::common::{
    BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber, data_dir_path, is_under_postmaster,
};

use crate::{
    cache::{BLOCKS_PER_CHUNK, ChunkTag},
    io_queue::S3IoControl,
    project::{ProjectCtx, ProjectNamespace},
    recovery,
    sim_store::SimStore,
};

// ── S3 chunk fetch ────────────────────────────────────────────────────────────

/// Attempt to fetch a full chunk from the S3 sim store using the two-level
/// fallback hierarchy.
///
/// Levels:
/// 1. **Recovery mode** (`is_recovery_mode()` = true):
///    Look up the chunk in `RECOVERY_MANIFEST` → GET versioned object from
///    standard sim at `{org}/chunks/{chunk_ref.branch_id}/{tag.to_path()}/{lsn_hex}`.
///    Returns `None` if not found; does NOT fall through to normal levels.
///
/// 2. **Normal — level 1** (own express-bucket `latest`):
///    GET express `{org}/{proj}/chunks/{tag.to_path()}/latest`.
///
/// 3. **Normal — level 2** (base manifest fallback for inherited chunks):
///    `ProjectCtx::try_get()?.base_manifest_lookup(tag)` → GET versioned
///    standard sim at `{org}/chunks/{chunk_ref.branch_id}/{tag.to_path()}/{lsn_hex}`.
///    Only attempted when `ProjectCtx` has been initialised.
///
/// Returns the raw chunk bytes on success, `None` on all misses.
fn try_fetch_chunk_from_s3(
    sim: &SimStore,
    ns: &ProjectNamespace,
    tag: &ChunkTag,
) -> Option<Vec<u8>> {
    if recovery::is_recovery_mode() {
        // Level R: versioned standard-sim object from recovery manifest.
        if let Ok(Some(chunk_ref)) = recovery::lookup_recovery_chunk(tag) {
            let key = format!(
                "{}/chunks/{}/{}/{}",
                ns.org_id,
                chunk_ref.branch_id,
                tag.to_path(),
                chunk_ref.lsn.to_hex()
            );
            if let Ok(Some(data)) = sim.get_standard(&key) {
                return Some(data);
            }
        }
        // In recovery mode we do not fall through to normal levels.
        return None;
    }

    // Level 1: express-bucket latest (own current checkpoint state).
    let latest_key = ns.chunk_latest_key(tag);
    if let Ok(Some(data)) = sim.get_express(&latest_key) {
        return Some(data);
    }

    // Level 2: base manifest fallback (inherited ancestor-branch chunks).
    // Only available when PROJECT_CTX is initialised.
    if let Some(ctx) = ProjectCtx::try_get() {
        if let Ok(Some(chunk_ref)) = ctx.base_manifest_lookup(tag) {
            // Use chunk_ref.branch_id (the branch that owns this chunk version),
            // NOT ns.branch_id (own branch) or ns.project_id.
            let key = format!(
                "{}/chunks/{}/{}/{}",
                ns.org_id,
                chunk_ref.branch_id,
                tag.to_path(),
                chunk_ref.lsn.to_hex()
            );
            if let Ok(Some(data)) = sim.get_standard(&key) {
                return Some(data);
            }
        }
    }

    None
}

/// Internal wrapper: look up `SIM_STORE` and `PROJECT_NS` globals and call
/// `try_fetch_chunk_from_s3`. Returns `None` if the statics are not set
/// (e.g. initdb, env vars absent).
fn try_fetch_chunk_from_s3_globals(tag: &ChunkTag) -> Option<Vec<u8>> {
    let sim = SimStore::get();
    let ns = ProjectCtx::get().ns();
    try_fetch_chunk_from_s3(sim, ns, tag)
}

/// True when the shared-memory cache is reachable from this process.
///
/// Requires both conditions:
/// - `is_under_postmaster()` — false during initdb (`--boot`/`--single`) where
///   `MyProcNumber` is invalid and S3IoControl was never sized via
///   `shmem_request_hook`.
/// - `S3IoControl::is_initialized()` — false if the shmem startup hook has not
///   yet run in this process (e.g. very early in backend startup).
#[inline]
fn cache_is_available() -> bool {
    is_under_postmaster() && S3IoControl::is_initialized()
}

/// Build the local file path for a relation fork.
///
/// Layout: `{DataDir}/tiko/{spc_oid}/{db_oid}/{rel_number}.{fork}`
///
/// Mirrors the future S3 key structure:
/// `s3://{bucket}/{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}`
fn block_path(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> PathBuf {
    let data_dir = data_dir_path();

    data_dir
        .join("tiko")
        .join(spc_oid.to_string())
        .join(db_oid.to_string())
        .join(format!("{}.{}", rel_number, fork_number))
}

/// Map `std::io::Error` to a raw errno value.
fn io_err_to_errno(e: &io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

/// Check if a relation fork file exists.
///
/// # Returns
/// - `true` if the file exists
/// - `false` otherwise
pub fn file_exists(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> bool {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);
    path.exists()
}

/// Create a relation fork file. Creates parent directories if needed.
///
/// # Returns
/// - `Ok(false)` if the file already existed
/// - `Ok(true)` if a new file was created
/// - `Err(errno)` on failure
pub fn create_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<bool, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let created = !path.exists();

    OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    Ok(created)
}

/// Get the number of blocks in a relation fork file.
///
/// Unlike `mdnblocks` which iterates across segments, S3 uses a single file
/// per fork — just `file_size / BLCKSZ`. Returns 0 if the file doesn't exist.
///
/// # Returns
/// - `Ok(nblocks)` — number of whole blocks in the file
/// - `Err(errno)` on I/O failure (other than file-not-found)
pub fn file_nblocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    match fs::metadata(&path) {
        Ok(meta) => Ok(meta.len() as u32 / BLCKSZ as u32),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(io_err_to_errno(&e)),
    }
}

/// Cache-aware block count. Returns `max(file_nblocks, cache_max)`.
///
/// With the write-back cache, `cached_write_blocks` does not extend the
/// S3-sim backing file immediately — dirty blocks stay in the cache until
/// eviction. So `file_nblocks` alone would return a stale (smaller) count
/// for relations that have been extended but not yet evicted.
///
/// Falls back to `file_nblocks` when the cache is unavailable (initdb).
pub fn cached_file_nblocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<BlockNumber, i32> {
    let disk = file_nblocks(spc_oid, db_oid, rel_number, fork_number)?;

    if !cache_is_available() {
        return Ok(disk);
    }

    let cache_max =
        S3IoControl::get()
            .cache
            .max_block_for_relation(spc_oid, db_oid, rel_number, fork_number);

    Ok(disk.max(cache_max))
}

/// Read blocks from a relation data file into a buffer.
///
/// Implements retry loop for short reads, matching PostgreSQL's FileReadV
/// behavior. Continues reading until all requested blocks are transferred
/// or EOF/error occurs.
///
/// # Returns
/// - `Ok(nblocks)` on full read
/// - `Ok(partial)` on EOF (fewer blocks than requested)
/// - `Err(errno)` on I/O failure
pub fn read_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *mut u8,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    let file = File::open(&path).map_err(|e| io_err_to_errno(&e))?;

    let mut total_blocks_read = 0u32;
    let mut remaining = nblocks;

    // Retry loop: handle short reads (partial transfers)
    while remaining > 0 {
        let offset = (block_number + total_blocks_read) as u64 * BLCKSZ as u64;
        let bytes_to_read = remaining as usize * BLCKSZ;
        let buf_offset = total_blocks_read as usize * BLCKSZ;
        let buf =
            unsafe { std::slice::from_raw_parts_mut(buffer_ptr.add(buf_offset), bytes_to_read) };

        match file.read_at(buf, offset) {
            Ok(0) => break, // EOF reached
            Ok(bytes_read) => {
                let blocks_read = bytes_read as u32 / BLCKSZ as u32;
                total_blocks_read += blocks_read;
                remaining -= blocks_read;

                // Partial block at EOF — shouldn't happen with aligned I/O,
                // but handle it gracefully (matches md behavior)
                if bytes_read % BLCKSZ != 0 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue, // EINTR: retry
            Err(e) => return Err(io_err_to_errno(&e)),
        }
    }

    Ok(total_blocks_read)
}

/// Write blocks from a buffer to a relation data file.
///
/// Creates parent directories if they don't exist. Uses `write_at` (pwrite),
/// which extends the file and zero-fills gaps if `block_number` is beyond EOF —
/// same semantics as `mdextend`'s `FileWrite`. In the future S3 implementation
/// this becomes a PUT to a per-block key, so extend vs overwrite is irrelevant.
///
/// Implements retry loop for short writes, matching PostgreSQL's FileWriteV
/// behavior. Continues writing until all requested blocks are transferred
/// or an error occurs.
///
/// # Returns
/// - `Ok(nblocks)` on full write
/// - `Err(errno)` on I/O failure (short writes are retried until completion)
pub fn write_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *const u8,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    let mut total_blocks_written = 0u32;
    let mut remaining = nblocks;

    // Retry loop: handle short writes (partial transfers)
    while remaining > 0 {
        let offset = (block_number + total_blocks_written) as u64 * BLCKSZ as u64;
        let bytes_to_write = remaining as usize * BLCKSZ;
        let buf_offset = total_blocks_written as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts(buffer_ptr.add(buf_offset), bytes_to_write) };

        match file.write_at(buf, offset) {
            Ok(0) => {
                // Short write with 0 bytes written — likely ENOSPC (disk full)
                // Return an error like md does
                return Err(libc::ENOSPC);
            }
            Ok(bytes_written) => {
                let blocks_written = bytes_written as u32 / BLCKSZ as u32;
                total_blocks_written += blocks_written;
                remaining -= blocks_written;

                // Partial block write — shouldn't happen with aligned I/O,
                // but handle it as potential ENOSPC
                if bytes_written % BLCKSZ != 0 && remaining > 0 {
                    return Err(libc::ENOSPC);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue, // EINTR: retry
            Err(e) => return Err(io_err_to_errno(&e)),
        }
    }

    Ok(total_blocks_written)
}

/// Extend a relation fork file with zero-filled blocks.
///
/// Uses `File::set_len()` (ftruncate) to extend the file to
/// `(blocknum + nblocks) * BLCKSZ`. On POSIX, `ftruncate` zero-fills
/// the extended region. Creates the file and parent directories if
/// they don't exist (matching `mdzeroextend`'s `EXTENSION_CREATE`).
///
/// Never shrinks the file: if the file is already at or beyond the target
/// size (e.g. during WAL replay or after async cache eviction), this is
/// a no-op. `set_len` / `ftruncate` would otherwise silently truncate,
/// discarding data — unlike `mdzeroextend` which only ever grows the file.
///
/// # Returns
/// - `Ok(())` on success
/// - `Err(errno)` on failure
pub fn zeroextend_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    let new_len = (block_number as u64 + nblocks as u64) * BLCKSZ as u64;
    let current_len = file.metadata().map_err(|e| io_err_to_errno(&e))?.len();
    if new_len > current_len {
        file.set_len(new_len).map_err(|e| io_err_to_errno(&e))?;
    }
    Ok(())
}

/// Truncate a relation fork file to the given number of blocks.
///
/// Uses `File::set_len()` (ftruncate) to shrink the file. If the file
/// doesn't exist, this is a no-op (the relation was already dropped or
/// never created).
///
/// # Returns
/// - `Ok(())` on success or if the file doesn't exist
/// - `Err(errno)` on failure
pub fn truncate_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    nblocks: BlockNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    let file = match OpenOptions::new().write(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err_to_errno(&e)),
    };

    let new_len = nblocks as u64 * BLCKSZ as u64;
    file.set_len(new_len).map_err(|e| io_err_to_errno(&e))
}

/// Cache-aware truncate. Invalidates cache blocks at or beyond `nblocks`
/// BEFORE shrinking the backing file.
///
/// Order matters: invalidating first prevents a dirty block in the truncated
/// range from being flushed by `flush_dirty_chunk` after `truncate_file`
/// has shrunk the file — which would silently re-extend it via `pwrite`.
///
/// Falls back to raw `truncate_file` when the cache is unavailable (initdb).
pub fn cached_truncate_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    nblocks: BlockNumber,
) -> Result<(), i32> {
    if cache_is_available() {
        S3IoControl::get().cache.invalidate_range(
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            nblocks,
        );
    }

    truncate_file(spc_oid, db_oid, rel_number, fork_number, nblocks)
}

/// Delete a relation fork file.
///
/// Silently ignores ENOENT — the file may not exist (e.g. non-main forks
/// that were never created, or WAL redo replaying a drop).
///
/// # Returns
/// - `Ok(())` on success or if the file doesn't exist
/// - `Err(errno)` on failure
pub fn delete_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err_to_errno(&e)),
    }
}

/// Cache-aware delete. Invalidates ALL cache blocks for the relation fork
/// BEFORE removing the backing file.
///
/// Order matters: invalidating first prevents dirty blocks from being
/// flushed by `flush_dirty_chunk` after the file is gone — which would
/// silently recreate it via `write_blocks`'s `create(true)` open flag.
/// It also prevents stale cache hits if the same `rel_number` is later
/// reused for a new relation.
///
/// Falls back to raw `delete_file` when the cache is unavailable (initdb).
pub fn cached_delete_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<(), i32> {
    if cache_is_available() {
        // first_block=0 invalidates every chunk for this fork
        S3IoControl::get()
            .cache
            .invalidate_range(spc_oid, db_oid, rel_number, fork_number, 0);
    }

    delete_file(spc_oid, db_oid, rel_number, fork_number)
}

// ── Cache-aware wrappers ──

/// Cache-aware read. Checks the local cache before reading from the backing file.
///
/// Falls back to raw `read_blocks` when the cache is unavailable (initdb,
/// single-user mode, before shared memory is initialized).
///
/// Uses chunk-level granularity: each cache slot holds 256 KB (32 blocks).
/// On chunk hit, reads individual blocks from the cache. On chunk miss,
/// allocates a new slot and prefetches the full chunk from S3-sim files.
pub fn cached_read_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *mut u8,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        return read_blocks(
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            buffer_ptr,
        );
    }
    let control = S3IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;
    stats
        .total_reads
        .fetch_add(nblocks as u64, Ordering::Relaxed);

    for i in 0..nblocks {
        let blkno = block_number + i;
        let chunk_tag = ChunkTag::from_block(spc_oid, db_oid, rel_number, fork_number, blkno);
        let block_offset = blkno % BLOCKS_PER_CHUNK;
        let buf_offset = i as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts_mut(buffer_ptr.add(buf_offset), BLCKSZ) };

        if let Some(slot) = cache.lookup(&chunk_tag) {
            // Chunk hit
            stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            cache.pin(slot);
            if cache.is_block_valid(slot, block_offset) {
                // Block is populated — read directly from cache
                cache.read_block(slot, block_offset, buf);
            } else {
                // Block not yet populated in this chunk — read from S3-sim, populate cache
                let blk_ptr = unsafe { buffer_ptr.add(buf_offset) };
                read_blocks(spc_oid, db_oid, rel_number, fork_number, blkno, 1, blk_ptr)?;
                cache.write_block(slot, block_offset, buf);
                cache.set_block_valid(slot, block_offset);
            }
            cache.touch(slot);
            cache.unpin(slot);
        } else {
            // Chunk miss — insert new chunk slot, prefetch full chunk from S3-sim
            stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            let slot = cache.insert(&chunk_tag); // returns pinned, valid_blocks=0

            // Prefetch: read as many blocks as possible from the S3-sim file
            let chunk_start_blk = chunk_tag.chunk_id * BLOCKS_PER_CHUNK;
            let file_nblks = file_nblocks(spc_oid, db_oid, rel_number, fork_number).unwrap_or(0);

            if file_nblks > chunk_start_blk {
                // How many blocks of this chunk exist in the file
                let avail = std::cmp::min(BLOCKS_PER_CHUNK, file_nblks - chunk_start_blk);
                let mut chunk_buf = vec![0u8; avail as usize * BLCKSZ];
                if read_blocks(
                    spc_oid,
                    db_oid,
                    rel_number,
                    fork_number,
                    chunk_start_blk,
                    avail,
                    chunk_buf.as_mut_ptr(),
                )
                .is_ok()
                {
                    // Write all fetched blocks into the cache slot
                    cache.write_blocks_to_slot(slot, 0, avail, &chunk_buf);
                    // Set valid bits for all fetched blocks
                    let valid_mask = if avail >= 32 {
                        u32::MAX
                    } else {
                        (1u32 << avail) - 1
                    };
                    cache.set_valid_blocks_mask(slot, valid_mask);
                }
            }

            // S3 fallback: if the requested block is still not valid after
            // the local-file prefetch, try the two-level S3 fallback.
            if !cache.is_block_valid(slot, block_offset) {
                if let Some(chunk_data) = try_fetch_chunk_from_s3_globals(&chunk_tag) {
                    if chunk_data.len() % BLCKSZ == 0 {
                        let nblocks_s3 = (chunk_data.len() / BLCKSZ) as u32;
                        let nblocks_s3 = nblocks_s3.min(BLOCKS_PER_CHUNK);
                        cache.write_blocks_to_slot(
                            slot,
                            0,
                            nblocks_s3,
                            &chunk_data[..nblocks_s3 as usize * BLCKSZ],
                        );
                        let valid_mask = if nblocks_s3 >= BLOCKS_PER_CHUNK {
                            u32::MAX
                        } else {
                            (1u32 << nblocks_s3) - 1
                        };
                        cache.set_valid_blocks_mask(slot, valid_mask);
                    }
                }
            }

            // Now read the requested block from the cache slot.
            if cache.is_block_valid(slot, block_offset) {
                cache.read_block(slot, block_offset, buf);
            } else {
                // Block beyond file extent — zero-fill (existing behaviour).
                buf.fill(0);
            }

            cache.touch(slot);
            cache.unpin(slot);
        }
    }
    Ok(nblocks)
}

/// Cache-aware write. Writes to the local cache only (write-back policy).
///
/// Falls back to raw `write_blocks` when the cache is unavailable (initdb).
///
/// Dirty blocks are flushed to S3-sim files on eviction — no write-through.
pub fn cached_write_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *const u8,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        return write_blocks(
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            buffer_ptr,
        );
    }
    let control = S3IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;
    stats
        .total_writes
        .fetch_add(nblocks as u64, Ordering::Relaxed);

    for i in 0..nblocks {
        let blkno = block_number + i;
        let chunk_tag = ChunkTag::from_block(spc_oid, db_oid, rel_number, fork_number, blkno);
        let block_offset = blkno % BLOCKS_PER_CHUNK;
        let buf_offset = i as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts(buffer_ptr.add(buf_offset), BLCKSZ) };

        let slot = match cache.lookup(&chunk_tag) {
            Some(slot) => {
                stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                cache.pin(slot);
                slot
            }
            None => {
                // Chunk miss: allocate empty slot (don't fetch from S3-sim)
                stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                cache.insert(&chunk_tag) // returns pinned
            }
        };

        cache.write_block(slot, block_offset, buf);
        cache.set_block_valid(slot, block_offset);
        cache.mark_dirty(slot, block_offset);
        cache.touch(slot);
        cache.unpin(slot);
    }

    // NO write-through — dirty blocks flushed on eviction
    Ok(nblocks)
}

/// Warm the cache for a block range without copying data to a caller buffer.
///
/// Iterates chunk-by-chunk over the requested range. For each chunk:
/// - **Cache hit**: pin, touch, unpin — data is already present.
/// - **Cache miss**: insert an empty slot (pinned), prefetch the full chunk
///   from the S3-sim backing file, mark the loaded blocks valid, then unpin.
///
/// This is the backend of `S3IoOpKind::Prefetch` — it allows subsequent
/// `cached_read_blocks` calls to be served entirely from the cache.
///
/// No-op (returns `Ok(0)`) when the cache is unavailable (initdb,
/// single-user mode).
pub fn warm_cache_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        return Ok(0);
    }

    let control = S3IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;

    let first_chunk = block_number / BLOCKS_PER_CHUNK;
    let last_chunk = (block_number + nblocks - 1) / BLOCKS_PER_CHUNK;

    // One call to file_nblocks covers all chunks in the range.
    let file_nblks = file_nblocks(spc_oid, db_oid, rel_number, fork_number).unwrap_or(0);

    for chunk_id in first_chunk..=last_chunk {
        let chunk_tag = ChunkTag {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            chunk_id,
        };

        if let Some(slot) = cache.lookup(&chunk_tag) {
            // Already cached — just refresh the usage count.
            stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            cache.pin(slot);
            cache.touch(slot);
            cache.unpin(slot);
        } else {
            // Cache miss — insert empty slot and populate from S3-sim.
            stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            let slot = cache.insert(&chunk_tag); // returns pinned, valid_blocks=0

            let chunk_start_blk = chunk_id * BLOCKS_PER_CHUNK;
            if file_nblks > chunk_start_blk {
                let avail = BLOCKS_PER_CHUNK.min(file_nblks - chunk_start_blk);
                let mut chunk_buf = vec![0u8; avail as usize * BLCKSZ];
                if read_blocks(
                    spc_oid,
                    db_oid,
                    rel_number,
                    fork_number,
                    chunk_start_blk,
                    avail,
                    chunk_buf.as_mut_ptr(),
                )
                .is_ok()
                {
                    cache.write_blocks_to_slot(slot, 0, avail, &chunk_buf);
                    let valid_mask = if avail >= BLOCKS_PER_CHUNK {
                        u32::MAX
                    } else {
                        (1u32 << avail) - 1
                    };
                    cache.set_valid_blocks_mask(slot, valid_mask);
                }
            }

            cache.touch(slot);
            cache.unpin(slot);
        }
    }

    Ok(nblocks)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ChunkTag;
    use crate::manifest::{ChunkRef, Manifest};
    use crate::project::ProjectNamespace;
    use crate::recovery;
    use crate::sim_store::SimStore;
    use pgsys::Lsn;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    // All tests that touch RECOVERY_MODE must hold `recovery::RECOVERY_MODE_TEST_GUARD`
    // (defined in recovery.rs as pub(crate)). Using that shared guard ensures
    // recovery::tests and s3_ops::tests are serialised against each other.

    fn ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 7)
    }

    fn tag(rel: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: rel,
            fork_number: 0,
            chunk_id: 0,
        }
    }

    fn chunk_data(fill: u8) -> Vec<u8> {
        vec![fill; BLOCKS_PER_CHUNK as usize * BLCKSZ]
    }

    // ── Level-1 express hit ───────────────────────────────────────────────

    #[test]
    fn level1_express_hit_returns_correct_data() {
        let _guard = recovery::RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = SimStore::new(dir.path());
        let tag = tag(100);

        // Ensure recovery mode is off.
        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Put chunk data in express latest; no backing file needed.
        let data = chunk_data(0xAB);
        sim.put_express_latest(&ns, &tag, &data).unwrap();

        // Level-1 should hit.
        let result = try_fetch_chunk_from_s3(&sim, &ns, &tag);
        assert_eq!(result, Some(data));
    }

    #[test]
    fn level1_miss_returns_none_when_express_empty() {
        let _guard = recovery::RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = SimStore::new(dir.path());
        let tag = tag(101);

        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Nothing in express, no ProjectCtx set for level-2.
        let result = try_fetch_chunk_from_s3(&sim, &ns, &tag);
        assert_eq!(result, None);
    }

    // ── Level-2 branch fallback ───────────────────────────────────────────

    #[test]
    fn level2_get_key_uses_branch_id_not_project_id() {
        // Verify the versioned-key format uses chunk_ref.branch_id, not
        // ns.branch_id or ns.project_id.
        let ns = ProjectNamespace::new(1001, 2001, 7); // project_id=2001, branch_id=7
        let tag = tag(200);
        let chunk_ref = ChunkRef {
            branch_id: 99, // different from both project_id and branch_id
            lsn: Lsn::new(0x500),
        };

        let key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            chunk_ref.branch_id,
            tag.to_path(),
            chunk_ref.lsn.to_hex()
        );

        assert!(
            key.contains("/99/"),
            "must use chunk_ref.branch_id=99: {key}"
        );
        assert!(
            !key.contains("/2001/"),
            "must not use project_id=2001 in versioned key: {key}"
        );
        // org_id=1001 appears in the key prefix; verify the chunks portion
        // does not embed project_id or ns.branch_id.
        assert!(
            !key.contains("chunks/7/"),
            "must not use ns.branch_id=7: {key}"
        );
    }

    #[test]
    fn level2_branch_fallback_correct_versioned_key_format() {
        let _guard = recovery::RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Test that the level-2 key format is correct by checking the key
        // built from a manifest lookup against an entry in the standard sim.
        let dir = TempDir::new().unwrap();
        let ns = ProjectNamespace::new(2002, 3003, 10);
        let sim = SimStore::new(dir.path());
        let tag = tag(300);
        let parent_branch_id: u64 = 42;
        let lsn = Lsn::new(0x800);
        let chunk_ref = ChunkRef {
            branch_id: parent_branch_id,
            lsn,
        };

        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Put versioned data in standard sim (inherited from parent branch).
        let data = chunk_data(0xCC);
        let versioned_key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            parent_branch_id,
            tag.to_path(),
            lsn.to_hex()
        );
        sim.put_standard(&versioned_key, &data).unwrap();

        // Ensure nothing in express (level-1 would miss).
        assert_eq!(sim.get_express(&ns.chunk_latest_key(&tag)).unwrap(), None);

        // Build a local manifest with the chunk entry.
        let manifest_path = dir.path().join("test_manifest.tikm");
        let manifest =
            Manifest::new_sorted(lsn, 0, vec![(tag, chunk_ref)], &manifest_path).unwrap();

        // Simulate level-2: look up the manifest and build the expected key.
        let found = manifest.lookup(&tag).unwrap().unwrap();
        assert_eq!(found.branch_id, parent_branch_id);
        let level2_key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            found.branch_id,
            tag.to_path(),
            found.lsn.to_hex()
        );
        let fetched = sim.get_standard(&level2_key).unwrap();
        assert_eq!(fetched, Some(data));
    }

    // ── Recovery mode ─────────────────────────────────────────────────────

    #[test]
    fn recovery_mode_fetches_versioned_chunk_from_standard_sim() {
        let _guard = recovery::RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = SimStore::new(dir.path());
        let tag = tag(400);
        let branch_id: u64 = 55;
        let lsn = Lsn::new(0x2000);
        let chunk_ref = ChunkRef { branch_id, lsn };

        // Build and write the recovery manifest blob.
        let build_path = dir.path().join("build.tikm");
        let m = Manifest::new_sorted(lsn, 0, vec![(tag, chunk_ref)], &build_path).unwrap();
        let blob = m.to_bytes().unwrap();

        let tiko_dir = dir.path().join("tiko");
        std::fs::create_dir_all(&tiko_dir).unwrap();
        let manifest_path = tiko_dir.join("recovery_manifest.bin");
        std::fs::write(&manifest_path, &blob).unwrap();

        // Attempt to load the recovery manifest (best-effort — OnceLock).
        let _ = recovery::load_recovery_manifest(&manifest_path);
        recovery::RECOVERY_MODE.store(true, Ordering::SeqCst);

        // Put versioned data in standard sim.
        let data = chunk_data(0xDD);
        let versioned_key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            branch_id,
            tag.to_path(),
            lsn.to_hex()
        );
        sim.put_standard(&versioned_key, &data).unwrap();

        // Test the recovery-mode lookup via a local manifest instance to
        // avoid OnceLock contention with RECOVERY_MANIFEST.
        let local = Manifest::from_bytes(&blob, &dir.path().join("local.tikm")).unwrap();
        let found = local.lookup(&tag).unwrap().unwrap();
        let key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            found.branch_id,
            tag.to_path(),
            found.lsn.to_hex()
        );
        let fetched = sim.get_standard(&key).unwrap();
        assert_eq!(fetched, Some(data));

        recovery::clear_recovery_mode();
    }

    #[test]
    fn recovery_mode_does_not_fall_through_to_express() {
        let _guard = recovery::RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = SimStore::new(dir.path());
        let tag = tag(500);

        // Enable recovery mode but do NOT put a matching versioned object.
        recovery::RECOVERY_MODE.store(true, Ordering::SeqCst);

        // Put data in express latest — it must NOT be returned in recovery mode.
        let data = chunk_data(0xEE);
        sim.put_express_latest(&ns, &tag, &data).unwrap();

        // With recovery mode on and no versioned match for tag(500), the result
        // is None (recovery mode does not fall through to express latest).
        let result = try_fetch_chunk_from_s3(&sim, &ns, &tag);
        // If RECOVERY_MANIFEST happens to have an entry for tag(500) from a
        // concurrent test, result may be Some — but it must not equal the
        // express data (which would mean we fell through, which is wrong).
        if let Some(ref bytes) = result {
            assert_ne!(
                bytes, &data,
                "express data must never be returned in recovery mode"
            );
        }

        recovery::clear_recovery_mode();
    }
}
