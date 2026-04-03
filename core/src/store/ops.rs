//! Store-backed block-level read/write operations.
//!
//! Two-layer storage: **shared-memory chunk cache → S3Sim (express bucket)**.
//! The local backing-file layer (`{DataDir}/tiko/`) has been removed.
//!
//! # Public surface
//!
//! | Function | Purpose |
//! |---|---|
//! | `store_exists` | Check whether a relation fork exists (nblocks key present) |
//! | `store_create` | Create a relation fork (write nblocks=0) |
//! | `cached_zeroextend` | Extend a relation fork (update nblocks if larger) |
//! | `cached_file_nblocks` | Block count: max(S3Sim nblocks, cache max) |
//! | `cached_read_blocks` | Read blocks: cache hit or S3Sim fetch |
//! | `cached_write_blocks` | Write blocks: cache (or initdb S3Sim RMW) |
//! | `cached_truncate_file` | Truncate: invalidate cache + trim S3Sim + update nblocks |
//! | `cached_delete_file` | Delete: invalidate cache + remove all S3Sim chunks |
//! | `warm_cache_blocks` | Prefetch: populate cache from S3Sim |

use std::io;
use std::sync::atomic::Ordering;

use crate::chunk::{BLOCKS_PER_CHUNK, CHUNK_SIZE, ChunkLogEntry, ChunkTag, RelFork};
use crate::{cache::CacheControl, io_control::IoControl};
use crate::{
    project::{ProjectCtx, ProjectNamespace},
    recovery,
    store::Store,
};
use pgsys::common::{BLCKSZ, BlockNumber, is_under_postmaster};

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
fn try_fetch_chunk_from_s3_with(
    sim: &Store,
    ns: &ProjectNamespace,
    tag: &ChunkTag,
) -> Option<Vec<u8>> {
    if recovery::is_recovery_mode() {
        // Level R: versioned standard-sim object from recovery manifest.
        if let Ok(Some(chunk_ref)) = recovery::lookup_recovery_chunk(tag) {
            let key = ns.chunk_versioned_key(
                tag,
                chunk_ref.branch_id,
                chunk_ref.timeline_id,
                chunk_ref.lsn,
            );
            if let Ok(Some(data)) = sim.get_standard(&key) {
                return Some(data);
            }
        }
        // In recovery mode we do not fall through to normal levels.
        return None;
    }

    // Level 1: express-bucket latest (own current checkpoint state).
    let tl = ProjectCtx::try_get()
        .map(|c| c.current_timeline_id())
        .unwrap_or(1);
    let latest_key = ns.chunk_latest_key(tag, tl);
    if let Ok(Some(data)) = sim.get_express(&latest_key) {
        return Some(data);
    }

    // Level 2: base manifest fallback (inherited ancestor-branch chunks).
    // Only available when PROJECT_CTX is initialised.
    if let Some(ctx) = ProjectCtx::try_get() {
        if let Ok(Some(chunk_ref)) = ctx.base_manifest_lookup(tag) {
            // Use chunk_ref.branch_id and chunk_ref.timeline_id to locate the
            // exact versioned S3 object (may belong to a different branch/timeline).
            let key = ns.chunk_versioned_key(
                tag,
                chunk_ref.branch_id,
                chunk_ref.timeline_id,
                chunk_ref.lsn,
            );
            if let Ok(Some(data)) = sim.get_standard(&key) {
                return Some(data);
            }
        }
    }

    None
}

fn try_fetch_chunk_from_s3(tag: &ChunkTag) -> Option<Vec<u8>> {
    let sim = Store::get();
    let ns = ProjectCtx::get().ns();
    try_fetch_chunk_from_s3_with(sim, ns, tag)
}

/// Fetch chunk data from S3Sim and populate the cache slot.
/// Returns `true` if data was found and written, `false` if S3Sim had no data.
fn populate_cache_slot_from_s3(cache: &CacheControl, slot: u32, chunk_tag: &ChunkTag) -> bool {
    if let Some(chunk_data) = try_fetch_chunk_from_s3(chunk_tag) {
        if chunk_data.len() % BLCKSZ == 0 {
            let nblocks_s3 = ((chunk_data.len() / BLCKSZ) as u32).min(BLOCKS_PER_CHUNK);
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
            return true;
        }
    }
    false
}

/// True when the shared-memory cache is reachable from this process.
///
/// Requires both conditions:
/// - `is_under_postmaster()` — false during initdb (`--boot`/`--single`) where
///   `MyProcNumber` is invalid and IoControl was never sized via
///   `shmem_request_hook`.
/// - `IoControl::is_initialized()` — false if the shmem startup hook has not
///   yet run in this process (e.g. very early in backend startup).
#[inline]
fn cache_is_available() -> bool {
    is_under_postmaster() && IoControl::is_initialized()
}

/// Map `std::io::Error` to a raw errno value.
fn io_err_to_errno(e: &io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

// ── S3Sim-backed relation metadata ─────────────────────────────────────────

/// Read the live block count for a relation fork from the express nblocks key.
///
/// Returns `Some(n)` when the key exists (including `Some(0)` for a relation
/// truncated to zero), and `None` when the key is absent. Callers that need a
/// plain integer should fall back to the base manifest or 0 themselves.
fn store_get_nblocks(sim: &Store, ns: &ProjectNamespace, rf: RelFork) -> Option<BlockNumber> {
    let key = ns.rel_nblocks_key(rf);
    match sim.get_express(&key) {
        Ok(Some(bytes)) if bytes.len() >= 4 => {
            Some(u32::from_le_bytes(bytes[0..4].try_into().unwrap()))
        }
        Ok(None) => None,
        _ => Some(0),
    }
}

/// Write the live block count for a relation fork.
///
/// **Normal path** (IoControl available): write-back to the shared-memory
/// NblocksTable only.  Express is written at checkpoint time by
/// `flush_all_dirty_nblocks`.
///
/// **Initdb / single-user path** (IoControl not available): write directly to
/// express and append a `NblocksSet` entry to the cache log so that the
/// shutdown checkpoint can build the initial manifest.
fn set_nblocks(sim: &Store, ns: &ProjectNamespace, rf: RelFork, n: BlockNumber) -> io::Result<()> {
    if IoControl::is_initialized() {
        IoControl::get().nblocks.set(rf, n);
    } else {
        let key = ns.rel_nblocks_key(rf);
        sim.put_express(&key, &n.to_le_bytes())?;
        CacheControl::append_to_cache_log(&ChunkLogEntry::NblocksSet { rf, n });
    }
    Ok(())
}

/// Check whether a relation fork exists.
///
/// Level 0: NblocksTable (write-back shmem) — covers relations created or
///   extended since the last checkpoint, whose express key hasn't been written yet.
/// Level 1: express nblocks key — the durable record written at checkpoint.
/// Level 2: base manifest — covers inherited relations on a fresh branch.
pub fn store_exists(rf: RelFork) -> bool {
    // Level 0: NblocksTable (write-back).
    if IoControl::is_initialized() && IoControl::get().nblocks.get(rf).is_some() {
        return true;
    }
    let sim = Store::get();
    let ctx = ProjectCtx::get();
    let key = ctx.ns().rel_nblocks_key(rf);
    if matches!(sim.get_express(&key), Ok(Some(_))) {
        return true;
    }
    ctx.base_manifest_lookup_nblocks(rf).is_some()
}

/// Create a relation fork. Writes nblocks=0 to the express nblocks key.
///
/// # Returns
/// - `Ok(false)` if the fork already existed
/// - `Ok(true)` if a new fork was created
/// - `Err(errno)` on I/O failure
pub fn store_create(rf: RelFork) -> Result<bool, i32> {
    // NblocksTable (write-back): relation may exist only in shared memory,
    // with no express key yet (express is written only at checkpoint).
    if IoControl::is_initialized() && IoControl::get().nblocks.get(rf).is_some() {
        return Ok(false);
    }
    let sim = Store::get();
    let ctx = ProjectCtx::get();
    let ns = ctx.ns();
    let key = ns.rel_nblocks_key(rf);
    match sim.get_express(&key) {
        Ok(Some(_)) => return Ok(false), // already exists in express
        _ => {}
    }
    // Defensive: don't overwrite an inherited relation whose nblocks lives
    // only in the base manifest (express key absent on a fresh branch).
    if ctx.base_manifest_lookup_nblocks(rf).is_some() {
        return Ok(false);
    }
    set_nblocks(sim, ns, rf, 0).map_err(|e| io_err_to_errno(&e))?;
    Ok(true)
}

/// Extend a relation fork's block count. Only updates the nblocks metadata key
/// if `blkno + nblocks` exceeds the current value.
///
/// Actual chunk data for extended-but-never-written blocks is implicitly zero:
/// cache returns zeros for non-existent blocks, and S3Sim returns None.
pub fn cached_zeroextend(rf: RelFork, blkno: BlockNumber, nblocks: BlockNumber) -> Result<(), i32> {
    let new_nblocks = blkno + nblocks;
    // Use cached_file_nblocks for the three-level read (NblocksTable → express →
    // base manifest) so we never undercount when NblocksTable has a value not yet
    // flushed to express.
    let current = cached_file_nblocks(rf)?;
    if new_nblocks > current {
        let sim = Store::get();
        let ctx = ProjectCtx::get();
        set_nblocks(sim, ctx.ns(), rf, new_nblocks).map_err(|e| io_err_to_errno(&e))?;
    }
    Ok(())
}

/// Delete all express chunk objects for a relation fork (chunk latest + staging keys).
/// Does NOT delete the nblocks key — the caller handles that.
fn store_delete_all_chunks(sim: &Store, ns: &ProjectNamespace, rf: RelFork) -> io::Result<()> {
    let prefix = ns.rel_chunks_prefix(rf);
    for key in sim.list_prefix_express(&prefix)? {
        sim.delete_express(&key)?;
    }
    Ok(())
}

/// Parse the numeric chunk_id from an express key given a relation prefix.
///
/// Keys under the prefix follow these patterns:
/// - `{prefix}{chunk_id}/latest`
/// - `{prefix}{chunk_id}/.staging_{lsn}`
/// - `{prefix}nblocks`  ← no numeric chunk_id; returns `None`
fn parse_chunk_id_from_key(key: &str, prefix: &str) -> Option<u32> {
    let rest = key.strip_prefix(prefix)?;
    let chunk_id_str = rest.split('/').next()?;
    chunk_id_str.parse().ok()
}

// ── Cache-aware wrappers ──────────────────────────────────────────────────────

/// Cache-aware block count. Three-level read:
///
/// 1. **NblocksTable** (shared-memory write-back cache) — O(1), no I/O.
///    Updated by every extend, truncate, and create via `set_nblocks`.
/// 2. **Express nblocks key** — cold miss: relation not yet seen this server
///    lifetime.  Populates the NblocksTable (clean, not dirty) for future
///    calls.
/// 3. **Base manifest `fork_nblocks`** — consulted only when the express key
///    is *absent* (key not found, not value 0). Covers fresh branches that
///    inherit relation sizes from a parent manifest before any write has
///    created the express key in the child's namespace.
///
/// Falls back to levels 2 + 3 alone when IoControl is unavailable (initdb).
pub fn cached_file_nblocks(rf: RelFork) -> Result<BlockNumber, i32> {
    // Level 1: NblocksTable (shared memory) — fastest path.
    if IoControl::is_initialized() {
        if let Some(n) = IoControl::get().nblocks.get(rf) {
            return Ok(n);
        }
    }

    // Level 2 + 3: cold miss — read from express / base manifest.
    let n = if let (Some(sim), Some(ctx)) = (Store::try_get(), ProjectCtx::try_get()) {
        match store_get_nblocks(sim, ctx.ns(), rf) {
            Some(n) => {
                // Populate NblocksTable (clean) so next call is O(1).
                if IoControl::is_initialized() {
                    IoControl::get().nblocks.set_clean(rf, n);
                }
                n
            }
            // Express key absent: fall back to base manifest snapshot.
            None => ctx.base_manifest_lookup_nblocks(rf).unwrap_or(0),
        }
    } else {
        0
    };

    Ok(n)
}

/// Cache-aware truncate. Invalidates cache blocks at or beyond `nblocks`,
/// deletes excess chunks from S3Sim, then updates the nblocks key.
///
/// Order matters: invalidating first prevents a dirty block in the truncated
/// range from being flushed by `flush_dirty_chunk` after the S3Sim chunks
/// are removed.
///
/// Falls back to only updating the nblocks key when the cache is unavailable.
pub fn cached_truncate_file(rf: RelFork, nblocks: BlockNumber) -> Result<(), i32> {
    if cache_is_available() {
        IoControl::get().cache.invalidate_range(rf, nblocks);
    }

    let sim = Store::get();
    let ns = ProjectCtx::get().ns();

    // Delete express chunk keys for chunks beyond the new nblocks boundary.
    let first_excess_chunk = (nblocks + BLOCKS_PER_CHUNK - 1) / BLOCKS_PER_CHUNK;
    let prefix = ns.rel_chunks_prefix(rf);
    match sim.list_prefix_express(&prefix) {
        Ok(keys) => {
            for key in &keys {
                if let Some(chunk_id) = parse_chunk_id_from_key(&key, &prefix) {
                    if chunk_id >= first_excess_chunk {
                        let _ = sim.delete_express(key);
                    }
                }
            }
        }
        Err(_) => {} // relation may not have any chunks yet; no-op
    }

    // Update the nblocks key.
    set_nblocks(sim, ns, rf, nblocks).map_err(|e| io_err_to_errno(&e))
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
pub fn cached_delete_file(rf: RelFork) -> Result<(), i32> {
    if cache_is_available() {
        // first_block=0 invalidates every chunk for this fork
        IoControl::get().cache.invalidate_range(rf, 0);
    }
    if IoControl::is_initialized() {
        // Remove nblocks entry so stale values are not served after drop.
        IoControl::get().nblocks.remove(rf);
    }

    let sim = Store::get();
    let ns = ProjectCtx::get().ns();

    // Remove all chunk express objects (includes chunk latest/staging keys).
    store_delete_all_chunks(sim, ns, rf).map_err(|e| io_err_to_errno(&e))?;

    // Remove the nblocks key.
    let nblocks_key = ns.rel_nblocks_key(rf);
    sim.delete_express(&nblocks_key)
        .map_err(|e| io_err_to_errno(&e))?;

    // Append a ForkDeleted entry to the cache log so the next checkpoint
    // records this fork in `deleted_forks` of the delta manifest.
    CacheControl::append_to_cache_log(&ChunkLogEntry::ForkDeleted { rf });

    Ok(())
}

/// Cache-aware read. Checks the local cache before fetching from S3Sim.
///
/// Falls back to direct S3Sim reads when the cache is unavailable (initdb,
/// single-user mode, before shared memory is initialized).
///
/// Uses chunk-level granularity: each cache slot holds 256 KB (32 blocks).
/// On chunk hit with a valid block, reads directly from cache.
/// On chunk hit with an invalid block, or chunk miss, fetches the full chunk
/// from S3Sim and populates the cache.
pub fn cached_read_blocks(
    rf: RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *mut u8,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        // initdb / single-user: read directly from S3Sim express.
        for i in 0..nblocks {
            let blkno = block_number + i;
            let chunk_tag = ChunkTag::from_block(rf, blkno);
            let block_offset = (blkno % BLOCKS_PER_CHUNK) as usize;
            let buf_offset = i as usize * BLCKSZ;
            let buf = unsafe { std::slice::from_raw_parts_mut(buffer_ptr.add(buf_offset), BLCKSZ) };

            if let Some(chunk_data) = try_fetch_chunk_from_s3(&chunk_tag) {
                let start = block_offset * BLCKSZ;
                let end = start + BLCKSZ;
                if end <= chunk_data.len() {
                    buf.copy_from_slice(&chunk_data[start..end]);
                } else {
                    buf.fill(0);
                }
            } else {
                buf.fill(0);
            }
        }
        return Ok(nblocks);
    }

    let control = IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;
    stats
        .total_reads
        .fetch_add(nblocks as u64, Ordering::Relaxed);

    for i in 0..nblocks {
        let blkno = block_number + i;
        let chunk_tag = ChunkTag::from_block(rf, blkno);
        let block_offset = blkno % BLOCKS_PER_CHUNK;
        let buf_offset = i as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts_mut(buffer_ptr.add(buf_offset), BLCKSZ) };

        if let Some(slot) = cache.lookup(&chunk_tag) {
            // Chunk hit
            stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            cache.pin(slot);
            if cache.is_block_valid(slot, block_offset) {
                // Block is populated — read directly from cache.
                cache.read_block(slot, block_offset, buf);
            } else {
                // Block not in cache — fetch whole chunk from S3Sim and
                // populate only the invalid slots (preserve dirty/valid blocks).
                if let Some(chunk_data) = try_fetch_chunk_from_s3(&chunk_tag) {
                    if chunk_data.len() % BLCKSZ == 0 {
                        let nblocks_s3 = ((chunk_data.len() / BLCKSZ) as u32).min(BLOCKS_PER_CHUNK);
                        for bit in 0..nblocks_s3 {
                            if !cache.is_block_valid(slot, bit) {
                                let start = bit as usize * BLCKSZ;
                                cache.write_block(slot, bit, &chunk_data[start..start + BLCKSZ]);
                                cache.set_block_valid(slot, bit);
                            }
                        }
                    }
                }
                if cache.is_block_valid(slot, block_offset) {
                    cache.read_block(slot, block_offset, buf);
                } else {
                    // Block beyond file extent — zero-fill.
                    buf.fill(0);
                }
            }
            cache.touch(slot);
            cache.unpin(slot);
        } else {
            // Chunk miss — insert new slot, fetch full chunk from S3Sim.
            stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            // insert() returns a pinned slot; may be an existing slot if a concurrent
            // thread inserted the same tag between our lookup miss and now.
            let slot = cache.insert(&chunk_tag);

            // Newly allocated slot — fetch from S3Sim and populate.
            populate_cache_slot_from_s3(cache, slot, &chunk_tag);
            // If S3Sim had no data, slot stays with valid_blocks=0; caller
            // handles via is_block_valid check below (zero-fill for new blocks).

            if cache.is_block_valid(slot, block_offset) {
                cache.read_block(slot, block_offset, buf);
            } else {
                // Block beyond file extent — zero-fill.
                buf.fill(0);
            }

            cache.touch(slot);
            cache.unpin(slot);
        }
    }
    Ok(nblocks)
}

/// Cache-aware write. On a cache miss, pre-populates the slot from S3Sim
/// (read-modify-write) so that `flush_dirty_chunk` never emits stale bytes
/// from the previous slot occupant for blocks the caller didn't write.
///
/// Falls back to chunk-level read-modify-write on S3Sim when the cache is
/// unavailable (initdb). Namespace must be initialized before initdb writes.
///
/// Dirty blocks are flushed to S3Sim express on eviction — no write-through.
pub fn cached_write_blocks(
    rf: RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *const u8,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        // initdb: chunk-level read-modify-write on S3Sim express.
        let sim = Store::get();
        let ctx = ProjectCtx::get();
        let ns = ctx.ns();
        let timeline = ctx.current_timeline_id();
        for i in 0..nblocks {
            let blkno = block_number + i;
            let chunk_tag = ChunkTag::from_block(rf, blkno);
            let block_offset = (blkno % BLOCKS_PER_CHUNK) as usize;
            let buf_offset = i as usize * BLCKSZ;
            let buf = unsafe { std::slice::from_raw_parts(buffer_ptr.add(buf_offset), BLCKSZ) };

            // Read existing chunk from express (or use zeros for a new chunk).
            let latest_key = ns.chunk_latest_key(&chunk_tag, timeline);
            let mut chunk_data = match sim.get_express(&latest_key) {
                Ok(Some(data)) if data.len() == CHUNK_SIZE => data,
                _ => vec![0u8; CHUNK_SIZE],
            };

            // Apply the block update.
            let start = block_offset * BLCKSZ;
            chunk_data[start..start + BLCKSZ].copy_from_slice(buf);

            // Write back the whole chunk.
            sim.put_express(&latest_key, &chunk_data)
                .map_err(|e| io_err_to_errno(&e))?;

            // Log the chunk so the shutdown checkpoint can archive it and
            // build the initial delta/base manifests — mirrors what flush_dirty_chunk
            // does on the normal shmem-cache path. Sidecar written before log entry.
            let seq = CacheControl::next_sidecar_seq();
            CacheControl::write_sidecar(&chunk_tag, seq, &chunk_data);
            CacheControl::append_to_cache_log(&ChunkLogEntry::ChunkDirty {
                tag: chunk_tag,
                seq,
            });
        }

        // Update nblocks if we extended the relation.
        let new_nblocks = block_number + nblocks;
        let current = match store_get_nblocks(sim, ns, rf) {
            Some(n) => n,
            None => ctx.base_manifest_lookup_nblocks(rf).unwrap_or(0),
        };
        if new_nblocks > current {
            set_nblocks(sim, ns, rf, new_nblocks).map_err(|e| io_err_to_errno(&e))?;
        }

        return Ok(nblocks);
    }

    let control = IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;
    stats
        .total_writes
        .fetch_add(nblocks as u64, Ordering::Relaxed);

    for i in 0..nblocks {
        let blkno = block_number + i;
        let chunk_tag = ChunkTag::from_block(rf, blkno);
        let block_offset = blkno % BLOCKS_PER_CHUNK;
        let buf_offset = i as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts(buffer_ptr.add(buf_offset), BLCKSZ) };

        let slot = match cache.lookup(&chunk_tag) {
            Some(slot) => {
                // Chunk hit — pin and write directly to cache.
                stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                cache.pin(slot);
                slot
            }
            None => {
                // Chunk miss — allocate a fresh slot (evicts an existing entry if full).
                stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                let slot = cache.insert(&chunk_tag);

                // Newly allocated slot: pre-populate from S3Sim before writing.
                //
                // `insert()` resets `valid_blocks` but does NOT zero the cache
                // file region — it retains raw bytes from the previous occupant.
                // Without this step, `flush_dirty_chunk` reads the full 256 KB
                // slot on eviction and emits stale bytes for every block the
                // caller never explicitly wrote — silently corrupting those blocks.
                if !populate_cache_slot_from_s3(cache, slot, &chunk_tag) {
                    // New chunk (no data in S3Sim yet) — zero the slot so
                    // flush_dirty_chunk never emits stale bytes from the prior
                    // occupant for blocks the caller doesn't write.
                    cache.write_blocks_to_slot(slot, 0, BLOCKS_PER_CHUNK, &vec![0u8; CHUNK_SIZE]);
                }

                slot
            }
        };

        cache.write_block(slot, block_offset, buf);
        cache.set_block_valid(slot, block_offset);
        cache.mark_dirty(slot, block_offset);
        cache.touch(slot);
        cache.unpin(slot);
    }

    // Update nblocks if this write extends the relation (e.g. tiko_extend).
    // Uses cached_file_nblocks for the three-level read so we never undercount
    // when NblocksTable has a value not yet flushed to express.
    let new_nblocks = block_number + nblocks;
    let current = cached_file_nblocks(rf)?;
    if new_nblocks > current {
        let sim = Store::get();
        let ctx = ProjectCtx::get();
        set_nblocks(sim, ctx.ns(), rf, new_nblocks).map_err(|e| io_err_to_errno(&e))?;
    }

    Ok(nblocks)
}

/// Warm the cache for a block range without copying data to a caller buffer.
///
/// Iterates chunk-by-chunk over the requested range. For each chunk:
/// - **Cache hit**: pin, touch, unpin — data is already present.
/// - **Cache miss**: insert an empty slot (pinned), fetch the full chunk from
///   S3Sim (express latest first, then standard bucket via base manifest
///   for inherited ancestor-branch chunks), mark loaded blocks valid, then unpin.
///
/// This is the backend of `S3IoOpKind::Prefetch` — it allows subsequent
/// `cached_read_blocks` calls to be served entirely from the cache.
///
/// No-op (returns `Ok(0)`) when the cache is unavailable (initdb,
/// single-user mode).
pub fn warm_cache_blocks(
    rf: RelFork,
    block_number: BlockNumber,
    nblocks: BlockNumber,
) -> Result<BlockNumber, i32> {
    if !cache_is_available() {
        return Ok(0);
    }

    let control = IoControl::get();
    let cache = &control.cache;
    let stats = &control.stats;

    let first_chunk = block_number / BLOCKS_PER_CHUNK;
    let last_chunk = (block_number + nblocks - 1) / BLOCKS_PER_CHUNK;

    for chunk_id in first_chunk..=last_chunk {
        let chunk_tag = ChunkTag {
            spc_oid: rf.spc_oid,
            db_oid: rf.db_oid,
            rel_number: rf.rel_number,
            fork_number: rf.fork_number,
            chunk_id,
        };

        if let Some(slot) = cache.lookup(&chunk_tag) {
            // Already cached — just refresh the usage count.
            stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            cache.pin(slot);
            cache.touch(slot);
            cache.unpin(slot);
        } else {
            // Cache miss — insert empty slot and populate from S3Sim.
            stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            let slot = cache.insert(&chunk_tag);

            populate_cache_slot_from_s3(cache, slot, &chunk_tag);

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
    use crate::chunk::ChunkTag;
    use crate::manifest::{ChunkRef, Manifest};
    use crate::project::ProjectNamespace;
    use crate::recovery;
    use crate::store::Store;
    use pgsys::Lsn;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    /// Serialises tests in this module that read or write `RECOVERY_MODE`.
    static RECOVERY_MODE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = Store::new_sim(dir.path());
        let tag = tag(100);

        // Ensure recovery mode is off.
        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Put chunk data in express latest; no backing file needed.
        let data = chunk_data(0xAB);
        sim.put_express_latest(&ns, &tag, 1, &data).unwrap();

        // Level-1 should hit.
        let result = try_fetch_chunk_from_s3_with(&sim, &ns, &tag);
        assert_eq!(result, Some(data));
    }

    #[test]
    fn level1_miss_returns_none_when_express_empty() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = Store::new_sim(dir.path());
        let tag = tag(101);

        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Nothing in express, no ProjectCtx set for level-2.
        let result = try_fetch_chunk_from_s3_with(&sim, &ns, &tag);
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
            timeline_id: 1,
            lsn: Lsn::new(0x500),
        };

        let key = ns.chunk_versioned_key(
            &tag,
            chunk_ref.branch_id,
            chunk_ref.timeline_id,
            chunk_ref.lsn,
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
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Test that the level-2 key format is correct by checking the key
        // built from a manifest lookup against an entry in the standard sim.
        let dir = TempDir::new().unwrap();
        let ns = ProjectNamespace::new(2002, 3003, 10);
        let sim = Store::new_sim(dir.path());
        let tag = tag(300);
        let parent_branch_id: u64 = 42;
        let lsn = Lsn::new(0x800);
        let chunk_ref = ChunkRef {
            branch_id: parent_branch_id,
            timeline_id: 1,
            lsn,
        };

        recovery::RECOVERY_MODE.store(false, Ordering::SeqCst);

        // Put versioned data in standard sim (inherited from parent branch).
        let data = chunk_data(0xCC);
        let versioned_key =
            ns.chunk_versioned_key(&tag, chunk_ref.branch_id, chunk_ref.timeline_id, lsn);
        sim.put_standard(&versioned_key, &data).unwrap();

        // Ensure nothing in express (level-1 would miss).
        assert_eq!(
            sim.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            None
        );

        // Build a local manifest with the chunk entry.
        let manifest_path = dir.path().join("test_manifest.tikm");
        let manifest = Manifest::new(
            lsn,
            0,
            vec![(tag, chunk_ref)],
            HashMap::new(),
            vec![],
            &manifest_path,
        )
        .unwrap();

        // Simulate level-2: look up the manifest and build the expected key.
        let found = manifest.lookup(&tag).unwrap().unwrap();
        assert_eq!(found.branch_id, parent_branch_id);
        let level2_key =
            ns.chunk_versioned_key(&tag, found.branch_id, found.timeline_id, found.lsn);
        let fetched = sim.get_standard(&level2_key).unwrap();
        assert_eq!(fetched, Some(data));
    }

    // ── Recovery mode ─────────────────────────────────────────────────────

    #[test]
    fn recovery_mode_fetches_versioned_chunk_from_standard_sim() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = Store::new_sim(dir.path());
        let tag = tag(400);
        let branch_id: u64 = 55;
        let lsn = Lsn::new(0x2000);
        let chunk_ref = ChunkRef {
            branch_id,
            timeline_id: 1,
            lsn,
        };

        // Build and write the recovery manifest blob.
        let build_path = dir.path().join("build.tikm");
        let m = Manifest::new(
            lsn,
            0,
            vec![(tag, chunk_ref)],
            HashMap::new(),
            vec![],
            &build_path,
        )
        .unwrap();
        let blob = m.to_bytes().unwrap();

        let tiko_dir = dir.path();
        std::fs::create_dir_all(&tiko_dir).unwrap();
        let manifest_path = tiko_dir.join("recovery_manifest.bin");
        std::fs::write(&manifest_path, &blob).unwrap();

        // Attempt to load the recovery manifest (best-effort — OnceLock).
        let _ = recovery::load_recovery_manifest(&manifest_path);
        recovery::RECOVERY_MODE.store(true, Ordering::SeqCst);

        // Put versioned data in standard sim.
        let data = chunk_data(0xDD);
        let versioned_key = ns.chunk_versioned_key(&tag, branch_id, 1, lsn);
        sim.put_standard(&versioned_key, &data).unwrap();

        // Test the recovery-mode lookup via a local manifest instance to
        // avoid OnceLock contention with RECOVERY_MANIFEST.
        let local = Manifest::from_bytes(&blob, &dir.path().join("local.tikm")).unwrap();
        let found = local.lookup(&tag).unwrap().unwrap();
        let key = ns.chunk_versioned_key(&tag, found.branch_id, found.timeline_id, found.lsn);
        let fetched = sim.get_standard(&key).unwrap();
        assert_eq!(fetched, Some(data));

        recovery::clear_recovery_mode();
    }

    #[test]
    fn recovery_mode_does_not_fall_through_to_express() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let ns = ns();
        let sim = Store::new_sim(dir.path());
        let tag = tag(500);

        // Enable recovery mode but do NOT put a matching versioned object.
        recovery::RECOVERY_MODE.store(true, Ordering::SeqCst);

        // Put data in express latest — it must NOT be returned in recovery mode.
        let data = chunk_data(0xEE);
        sim.put_express_latest(&ns, &tag, 1, &data).unwrap();

        // With recovery mode on and no versioned match for tag(500), the result
        // is None (recovery mode does not fall through to express latest).
        let result = try_fetch_chunk_from_s3_with(&sim, &ns, &tag);
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
