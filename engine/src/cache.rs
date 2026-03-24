//! Local cache layer for S3-backed block storage.
//!
//! Sits below PostgreSQL's shared buffers in the I/O stack:
//!
//! ```text
//! PostgreSQL shared buffers  (hot pages, managed by PG buffer manager)
//!          |
//!     smgr interface  (s3_readv / s3_writev)
//!          |
//!    +-----------+
//!    | Local Cache |  <-- this module (write-back, chunk-level)
//!    +-----------+
//!          |
//!    S3-sim files    (source of truth, future: real S3)
//! ```
//!
//! # Layout
//!
//! - **Cache file**: single pre-allocated file at `{DataDir}/tiko/cache`,
//!   divided into fixed 256 KB chunk slots. Slot N lives at byte offset
//!   `N * CHUNK_SIZE`. Each chunk holds 32 contiguous 8 KB blocks.
//! - **Metadata arrays**: slot metadata, hash table, and partition locks live
//!   in PG shared memory as trailing arrays after `IoControl`.
//! - **CacheControl**: embedded in `IoControl` in PG shared memory,
//!   holding `num_slots` and `clock_hand`.
//!
//! # Write-Back Policy
//!
//! Writes go to cache only — no write-through to backing files. Dirty chunks
//! are flushed to S3-sim files on eviction. Per-block tracking via
//! `valid_blocks`/`dirty_blocks` u32 bitmasks.
//!
//! # Concurrency
//!
//! - Hash table partitions use `AtomicRWLock` (spin-based, in PG shared memory).
//!   PG LWLocks cannot be used because Tokio threads in worker also access the
//!   hash table (via `cached_read_blocks`/`cached_write_blocks` in `io_handler`),
//!   and LWLocks require per-process state (`MyProc`) that isn't thread-safe.
//! - Lookups hold a shared (read) lock. Insertions/evictions hold exclusive (write).
//! - `pin_count` is atomically incremented/decremented — a pinned slot
//!   (`pin_count > 0`) is skipped during eviction.
//! - `usage_count` is atomically bumped on access (saturating at 5).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU32, Ordering};

use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};
use pgsys::logging::pg_log_debug1;

use store::{
    chunk::{BLOCKS_PER_CHUNK, CHUNK_SIZE, ChunkTag, NBLOCKS_RECORD_SIZE, NblocksRecord, RelFork},
    tiko_root_path,
};

// ── Constants ──

/// Number of 256 KB cache chunk slots. 1024 slots = 256 MB cache.
pub const CACHE_NUM_SLOTS: u32 = 1024;

/// Hash table size: 2× the number of slots for low collision rates.
pub const CACHE_NUM_HASH_ENTRIES: u32 = CACHE_NUM_SLOTS * 2;

/// Number of partitions for the hash table's lock array.
pub const CACHE_NUM_PARTITIONS: u32 = 128;

/// Maximum usage count (same as PG's BM_MAX_USAGE_COUNT).
const MAX_USAGE_COUNT: u8 = 5;

/// Process-local cache file handle. Each process opens its own fd to the
/// same file — file descriptors are per-process, so this cannot live in
/// PG shared memory. Initialized lazily on first access.
static CACHE_FILE: OnceLock<File> = OnceLock::new();

// ── CacheSlotMeta ──

/// Per-slot metadata in PG shared memory.
///
/// Each slot represents a 256 KB chunk (32 blocks). Per-block state is
/// tracked via bitmasks: `valid_blocks` (which blocks are populated) and
/// `dirty_blocks` (which blocks have been modified and need flush on eviction).
#[repr(C)]
pub struct CacheSlotMeta {
    pub tag: ChunkTag,           // 20 bytes
    pub valid_blocks: AtomicU32, // bitmask: which of 32 blocks are populated
    pub dirty_blocks: AtomicU32, // bitmask: which of 32 blocks are modified
    pub usage_count: AtomicU8,
    pub _pad: [u8; 3], // padding to align to 32 bytes for better cache efficiency
    pub pin_count: AtomicU32,
}

impl CacheSlotMeta {
    fn init(&mut self) {
        self.tag = ChunkTag {
            spc_oid: 0,
            db_oid: 0,
            rel_number: 0,
            fork_number: 0,
            chunk_id: 0,
        };
        self.valid_blocks.store(0, Ordering::Relaxed);
        self.dirty_blocks.store(0, Ordering::Relaxed);
        self.usage_count.store(0, Ordering::Relaxed);
        self._pad = [0; 3];
        self.pin_count.store(0, Ordering::Relaxed);
    }
}

// ── CacheHashEntry ──

/// Hash entry status.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum HashStatus {
    /// Slot was never occupied or has been fully reclaimed. Terminates probes.
    Empty = 0,
    Occupied = 1,
    /// Slot was deleted (tombstone). Probes continue past it; inserts may reuse it.
    Deleted = 2,
}

/// One entry in the open-addressing hash table (PG shared memory).
#[repr(C)]
pub struct CacheHashEntry {
    pub tag: ChunkTag,
    pub slot_index: u32, // index into cache slot array
    pub status: AtomicU8,
}

impl CacheHashEntry {
    fn init(&mut self) {
        self.tag = ChunkTag {
            spc_oid: 0,
            db_oid: 0,
            rel_number: 0,
            fork_number: 0,
            chunk_id: 0,
        };
        self.slot_index = 0;
        self.status
            .store(HashStatus::Empty as u8, Ordering::Relaxed);
    }
}

// ── AtomicRWLock ──

/// Spin-based atomic reader-writer lock for hash table partitions.
/// Lives in PG shared memory. Used instead of PG LWLocks because Tokio
/// threads also access the hash table and LWLocks require per-process state.
///
/// State: 0 = unlocked, -1 = exclusive (write), >0 = shared reader count.
#[repr(C)]
pub struct AtomicRWLock {
    state: AtomicI32,
}

impl AtomicRWLock {
    /// Construct a new unlocked `AtomicRWLock` (for tests and stack allocation).
    pub fn new_unlocked() -> Self {
        AtomicRWLock {
            state: AtomicI32::new(0),
        }
    }

    pub(crate) fn init(&self) {
        self.state.store(0, Ordering::Relaxed);
    }

    pub(crate) fn read_lock(&self) {
        loop {
            let state = self.state.load(Ordering::Relaxed);
            if state >= 0 {
                if self
                    .state
                    .compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
            }
            std::hint::spin_loop();
        }
    }

    pub(crate) fn read_unlock(&self) {
        self.state.fetch_sub(1, Ordering::Release);
    }

    pub(crate) fn write_lock(&self) {
        loop {
            if self
                .state
                .compare_exchange_weak(0, -1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            std::hint::spin_loop();
        }
    }

    pub(crate) fn write_unlock(&self) {
        self.state.store(0, Ordering::Release);
    }
}

// ── CacheControl ──

/// Main cache control structure. Embedded in `IoControl` in PG shared memory.
///
/// The variable-size arrays (slot metadata, hash table, partition locks) follow
/// `IoControl` as trailing arrays in the same shared memory allocation.
/// Pointers to these arrays are stored here — valid in all processes because PG
/// shared memory is mapped at the same virtual address (inherited via fork).
#[repr(C)]
pub struct CacheControl {
    pub num_slots: u32,
    pub num_hash_entries: u32,
    pub num_partitions: u32,
    pub entries_per_partition: u32,
    pub clock_hand: AtomicU32,
    slot_meta_base: *const CacheSlotMeta,
    hash_entries_base: *const CacheHashEntry,
    locks: *const AtomicRWLock,
}

// Safety: CacheControl lives in PG shared memory. The raw pointers point into
// the same shared memory region, mapped at identical virtual addresses in all
// processes (inherited via fork). Tokio threads in worker also access these.
unsafe impl Send for CacheControl {}
unsafe impl Sync for CacheControl {}

impl CacheControl {
    /// Initialize CacheControl fields and array pointers. Called once when
    /// shared memory is first created (from `IoControl::init`).
    pub fn init(
        &mut self,
        slots: *mut CacheSlotMeta,
        hash: *mut CacheHashEntry,
        locks: *mut AtomicRWLock,
    ) {
        self.num_slots = CACHE_NUM_SLOTS;
        self.num_hash_entries = CACHE_NUM_HASH_ENTRIES;
        self.num_partitions = CACHE_NUM_PARTITIONS;
        self.entries_per_partition = CACHE_NUM_HASH_ENTRIES / CACHE_NUM_PARTITIONS;
        self.clock_hand.store(0, Ordering::Relaxed);
        self.slot_meta_base = slots;
        self.hash_entries_base = hash;
        self.locks = locks;

        // Initialize all metadata arrays
        for i in 0..CACHE_NUM_SLOTS as usize {
            unsafe { (*slots.add(i)).init() };
        }
        for i in 0..CACHE_NUM_HASH_ENTRIES as usize {
            unsafe { (*hash.add(i)).init() };
        }
        for i in 0..CACHE_NUM_PARTITIONS as usize {
            unsafe { (*locks.add(i)).init() };
        }

        // Ensure cache data file exists and is pre-allocated
        let _ = Self::cache_file();
    }

    // ── Cache data file ──

    fn cache_file_path() -> PathBuf {
        store::tiko_root_path().join("cache")
    }

    pub fn cache_file() -> &'static File {
        CACHE_FILE.get_or_init(|| {
            let path = Self::cache_file_path();
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .expect("failed to open cache file");

            // Pre-allocate to full size: 1024 chunks × 256 KB = 256 MB
            let expected_size = CACHE_NUM_SLOTS as u64 * CHUNK_SIZE as u64;
            if let Ok(meta) = file.metadata() {
                if meta.len() < expected_size {
                    file.set_len(expected_size)
                        .expect("failed to pre-allocate cache file");
                }
            }
            file
        })
    }

    // ── Shared memory array accessors ──

    pub fn slot_meta(&self, index: u32) -> &CacheSlotMeta {
        assert!(index < self.num_slots, "slot index {} out of range", index);
        unsafe { &*self.slot_meta_base.add(index as usize) }
    }

    fn hash_entry(&self, index: u32) -> &CacheHashEntry {
        assert!(
            index < self.num_hash_entries,
            "hash entry index {} out of range",
            index
        );
        unsafe { &*self.hash_entries_base.add(index as usize) }
    }

    fn partition_lock(&self, partition: u32) -> &AtomicRWLock {
        assert!(
            partition < self.num_partitions,
            "partition {} out of range",
            partition
        );
        unsafe { &*self.locks.add(partition as usize) }
    }

    fn partition_for_hash_index(&self, hash_index: u32) -> u32 {
        hash_index / self.entries_per_partition
    }

    /// Byte offset of a chunk slot within the cache file.
    fn slot_offset_in_file(slot_index: u32) -> u64 {
        slot_index as u64 * CHUNK_SIZE as u64
    }

    /// Byte offset of a specific block within a chunk slot in the cache file.
    fn block_offset_in_file(slot_index: u32, block_offset: u32) -> u64 {
        Self::slot_offset_in_file(slot_index) + block_offset as u64 * BLCKSZ as u64
    }

    // ── Core operations ──

    /// Look up a chunk in the hash table.
    /// Returns the cache slot index if found, or `None` on miss.
    pub fn lookup(&self, tag: &ChunkTag) -> Option<u32> {
        let num_hash = self.num_hash_entries;
        let hash = tag.hash();
        let start = hash % num_hash;
        let partition = self.partition_for_hash_index(start);
        let lock = self.partition_lock(partition);

        lock.read_lock();
        let result = self.probe_hash_table(tag, start);
        lock.read_unlock();

        result
    }

    fn probe_hash_table(&self, tag: &ChunkTag, start: u32) -> Option<u32> {
        let num_hash = self.num_hash_entries;
        let mut idx = start;
        for _ in 0..num_hash {
            let entry = self.hash_entry(idx);
            let status = entry.status.load(Ordering::Acquire);

            match status {
                s if s == HashStatus::Empty as u8 => return None, // chain ends
                s if s == HashStatus::Deleted as u8 => {}         // tombstone: keep probing
                s if s == HashStatus::Occupied as u8 => {
                    if entry.tag == *tag {
                        return Some(entry.slot_index);
                    }
                }
                _ => {}
            }

            idx = (idx + 1) % num_hash;
        }
        None
    }

    /// Pin a cache slot (increment pin_count).
    pub fn pin(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        meta.pin_count.fetch_add(1, Ordering::Acquire);
    }

    /// Unpin a cache slot (decrement pin_count).
    pub fn unpin(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        let prev = meta.pin_count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "unpin on slot {} with pin_count 0", slot_index);
    }

    /// Bump the usage count (saturating at MAX_USAGE_COUNT).
    pub fn touch(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        let current = meta.usage_count.load(Ordering::Relaxed);
        if current < MAX_USAGE_COUNT {
            meta.usage_count.store(current + 1, Ordering::Relaxed);
        }
    }

    /// Check if a specific block within a chunk slot is valid (populated).
    pub fn is_block_valid(&self, slot_index: u32, block_offset: u32) -> bool {
        debug_assert!(block_offset < BLOCKS_PER_CHUNK);
        let meta = self.slot_meta(slot_index);
        let valid = meta.valid_blocks.load(Ordering::Acquire);
        valid & (1 << block_offset) != 0
    }

    /// Mark a specific block within a chunk slot as valid.
    pub fn set_block_valid(&self, slot_index: u32, block_offset: u32) {
        debug_assert!(block_offset < BLOCKS_PER_CHUNK);
        let meta = self.slot_meta(slot_index);
        meta.valid_blocks
            .fetch_or(1 << block_offset, Ordering::Release);
    }

    /// Mark a specific block within a chunk slot as dirty.
    pub fn mark_dirty(&self, slot_index: u32, block_offset: u32) {
        debug_assert!(block_offset < BLOCKS_PER_CHUNK);
        let meta = self.slot_meta(slot_index);
        meta.dirty_blocks
            .fetch_or(1 << block_offset, Ordering::Release);
    }

    /// Set multiple valid bits at once (used when populating a full chunk from S3-sim).
    pub fn set_valid_blocks_mask(&self, slot_index: u32, mask: u32) {
        let meta = self.slot_meta(slot_index);
        meta.valid_blocks.fetch_or(mask, Ordering::Release);
    }

    /// Insert a chunk into the cache. Returns the slot index, **pinned**.
    ///
    /// Evicts an existing slot if necessary (flushing dirty blocks to S3-sim).
    ///
    /// Returns a **pinned** slot for `tag`.  The slot is either:
    /// - **Newly allocated** (`is_populated()` returns `false`): eviction ran
    ///   normally; the caller must populate the slot before using it.
    /// - **An existing slot** (`is_populated()` may return `true`): a concurrent
    ///   thread inserted the same tag between the caller's `lookup_and_pin` miss
    ///   and this call.  The unnecessarily evicted slot is released back to the
    ///   pool; the caller should skip population and use existing content.
    pub fn insert(&self, tag: &ChunkTag) -> u32 {
        let slot_index = self.evict();
        let meta = self.slot_meta(slot_index);

        // Write new tag. Safe: we hold exclusive access via pin_count CAS in evict().
        unsafe {
            let meta_ptr = meta as *const CacheSlotMeta as *mut CacheSlotMeta;
            (*meta_ptr).tag = *tag;
        }
        meta.usage_count.store(1, Ordering::Relaxed);
        // Start with no valid or dirty blocks — caller populates as needed
        meta.valid_blocks.store(0, Ordering::Relaxed);
        meta.dirty_blocks.store(0, Ordering::Relaxed);

        // Insert into hash table — but first check for concurrent insertion.
        //
        // Multiple threads can simultaneously miss on the same chunk and all call
        // insert().  Without this check each thread would allocate a distinct slot,
        // producing duplicate hash entries.  On eviction the flushes would race and
        // one would overwrite the other's dirty blocks — silent data corruption.
        //
        // Under the write lock we re-probe the hash table.  If another thread
        // already inserted this tag, we pin its slot, release the one we evicted
        // (valid_blocks=0, no hash entry — reclaimed quickly by the next evict()),
        // and return the existing slot.
        let num_hash = self.num_hash_entries;
        let hash = tag.hash();
        let start = hash % num_hash;
        let partition = self.partition_for_hash_index(start);
        let lock = self.partition_lock(partition);

        lock.write_lock();

        let mut idx = start;
        let mut first_deleted: Option<u32> = None;
        for _ in 0..num_hash {
            let entry = self.hash_entry(idx);
            let status = entry.status.load(Ordering::Acquire);

            if status == HashStatus::Empty as u8 {
                // Use the first tombstone slot if we passed one; else use this empty slot.
                let target = first_deleted.unwrap_or(idx);
                let target_entry = self.hash_entry(target);
                unsafe {
                    let entry_ptr = target_entry as *const CacheHashEntry as *mut CacheHashEntry;
                    (*entry_ptr).tag = *tag;
                    (*entry_ptr).slot_index = slot_index;
                }
                target_entry
                    .status
                    .store(HashStatus::Occupied as u8, Ordering::Release);
                break;
            } else if status == HashStatus::Deleted as u8 && first_deleted.is_none() {
                first_deleted = Some(idx);
            }
            idx = (idx + 1) % num_hash;
        }
        // If no Empty was found but we have a tombstone slot, use it (table is fully
        // occupied + tombstoned with no Empty sentinel — rare but possible).
        if let Some(target) = first_deleted {
            let target_entry = self.hash_entry(target);
            if target_entry.status.load(Ordering::Acquire) == HashStatus::Deleted as u8 {
                unsafe {
                    let entry_ptr = target_entry as *const CacheHashEntry as *mut CacheHashEntry;
                    (*entry_ptr).tag = *tag;
                    (*entry_ptr).slot_index = slot_index;
                }
                target_entry
                    .status
                    .store(HashStatus::Occupied as u8, Ordering::Release);
            }
        }

        lock.write_unlock();

        slot_index
    }

    /// Clock-sweep eviction. Returns evicted slot index, **pinned** (pin_count = 1).
    ///
    /// If the evicted slot has dirty blocks, they are flushed to S3-sim files
    /// before the slot is returned.
    fn evict(&self) -> u32 {
        for _ in 0..(self.num_slots * MAX_USAGE_COUNT as u32) {
            let slot_index = self.clock_hand.fetch_add(1, Ordering::Relaxed) % self.num_slots;
            let meta = self.slot_meta(slot_index);

            // CAS pin_count 0 → 1
            if meta
                .pin_count
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            // Now we have exclusive access to this slot for eviction. Check if it's empty or can be evicted.

            let valid = meta.valid_blocks.load(Ordering::Acquire);

            // Empty slot — take it immediately
            if valid == 0 {
                return slot_index;
            }

            let usage = meta.usage_count.load(Ordering::Relaxed);
            if usage > 0 {
                meta.usage_count.store(usage - 1, Ordering::Relaxed);
                meta.pin_count.fetch_sub(1, Ordering::Release);
                continue;
            }

            // Evict: usage_count == 0, has valid blocks, we hold pin.
            // Flush dirty blocks to S3-sim before eviction.
            let dirty = meta.dirty_blocks.load(Ordering::Acquire);
            if dirty != 0 {
                self.flush_dirty_chunk(slot_index);
                crate::io_queue::IoControl::get()
                    .stats
                    .dirty_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            crate::io_queue::IoControl::get()
                .stats
                .evictions
                .fetch_add(1, Ordering::Relaxed);

            self.reset_slot(slot_index);

            return slot_index;
        }

        panic!("cache eviction failed: no evictable slot found after full sweep");
    }

    /// Reset a cache slot: remove it from the hash table and clear its metadata.
    ///
    /// Caller must hold the slot pin to ensure exclusive access.
    fn reset_slot(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        let tag = meta.tag;

        // 1. Remove from hash table
        let num_hash = self.num_hash_entries;
        let hash = tag.hash();
        let start = hash % num_hash;
        let partition = self.partition_for_hash_index(start);
        let lock = self.partition_lock(partition);

        lock.write_lock();

        let mut idx = start;
        for _ in 0..num_hash {
            let entry = self.hash_entry(idx);
            let status = entry.status.load(Ordering::Acquire);

            match status {
                s if s == HashStatus::Empty as u8 => break,
                s if s == HashStatus::Deleted as u8 => {} // keep probing through tombstones
                s if s == HashStatus::Occupied as u8 => {
                    if entry.tag == tag {
                        // Leave a tombstone so probes through this slot continue.
                        entry
                            .status
                            .store(HashStatus::Deleted as u8, Ordering::Release);
                        break;
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % num_hash;
        }

        lock.write_unlock();

        // 2. Clear metadata
        meta.valid_blocks.store(0, Ordering::Release);
        meta.dirty_blocks.store(0, Ordering::Relaxed);
        meta.usage_count.store(0, Ordering::Relaxed);
    }

    // ── Cache file I/O ──

    /// Read a single 8 KB block from a chunk slot in the cache file.
    pub fn read_block(&self, slot_index: u32, block_offset: u32, buf: &mut [u8]) {
        debug_assert_eq!(buf.len(), BLCKSZ, "read_block: buf must be BLCKSZ");
        debug_assert!(block_offset < BLOCKS_PER_CHUNK);
        let offset = Self::block_offset_in_file(slot_index, block_offset);
        let file = Self::cache_file();
        file.read_at(buf, offset)
            .expect("cache read_block: pread failed");
    }

    /// Write a single 8 KB block into a chunk slot in the cache file.
    pub fn write_block(&self, slot_index: u32, block_offset: u32, buf: &[u8]) {
        debug_assert_eq!(buf.len(), BLCKSZ, "write_block: buf must be BLCKSZ");
        debug_assert!(block_offset < BLOCKS_PER_CHUNK);
        let offset = Self::block_offset_in_file(slot_index, block_offset);
        let file = Self::cache_file();
        file.write_at(buf, offset)
            .expect("cache write_block: pwrite failed");
    }

    /// Read multiple contiguous blocks from a chunk slot into a buffer.
    /// `start_offset` is the block offset within the chunk (0..31).
    /// `nblocks` is how many blocks to read.
    pub fn read_blocks_from_slot(
        &self,
        slot_index: u32,
        start_offset: u32,
        nblocks: u32,
        buf: &mut [u8],
    ) {
        debug_assert!(start_offset + nblocks <= BLOCKS_PER_CHUNK);
        debug_assert_eq!(buf.len(), nblocks as usize * BLCKSZ);
        let offset = Self::block_offset_in_file(slot_index, start_offset);
        let file = Self::cache_file();
        file.read_at(buf, offset)
            .expect("cache read_blocks_from_slot: pread failed");
    }

    /// Write multiple contiguous blocks into a chunk slot from a buffer.
    pub fn write_blocks_to_slot(
        &self,
        slot_index: u32,
        start_offset: u32,
        nblocks: u32,
        buf: &[u8],
    ) {
        debug_assert!(start_offset + nblocks <= BLOCKS_PER_CHUNK);
        debug_assert_eq!(buf.len(), nblocks as usize * BLCKSZ);
        let offset = Self::block_offset_in_file(slot_index, start_offset);
        let file = Self::cache_file();
        file.write_at(buf, offset)
            .expect("cache write_blocks_to_slot: pwrite failed");
    }

    /// Flush dirty blocks from a cache slot to SimStore express.
    ///
    /// 1. PUTs the full 256 KB chunk to the express-bucket `latest` object via
    ///    `SimStore::put_express_latest` — a plain PUT, no staging, no
    ///    standard-bucket copy (those happen only at checkpoint time).
    /// 2. On a successful PUT, appends one `ChunkTag` (20 bytes) to the
    ///    eviction log with `O_APPEND` — atomic on Linux/macOS for writes this
    ///    small. No log entry is written if the PUT failed, so there are never
    ///    phantom entries for chunks that didn't reach express storage.
    ///
    /// Both steps are guarded: they are skipped when `SimStore` or `ProjectCtx`
    /// are not yet initialised (e.g. during initdb, single-user mode, or very
    /// early in backend startup before env vars are read).
    ///
    /// No deadlock risk: at this point we hold pin_count=1 but no partition
    /// locks.
    pub fn flush_dirty_chunk(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        let tag = meta.tag; // copy — avoids holding a borrow across I/O

        // Express PUT + eviction log append.
        // Guard: only run when SimStore and ProjectCtx are initialised.
        if let (Some(sim), Some(ctx)) = (
            store::sim_store::SimStore::try_get(),
            store::project::ProjectCtx::try_get(),
        ) {
            let mut chunk_data = vec![0u8; CHUNK_SIZE];
            self.read_blocks_from_slot(slot_index, 0, BLOCKS_PER_CHUNK, &mut chunk_data);
            if sim
                .put_express_latest(ctx.ns(), &tag, ctx.current_timeline_id(), &chunk_data)
                .is_ok()
            {
                // Clear dirty only after a successful PUT — if the PUT failed,
                // the slot stays dirty so the next checkpoint retries.
                meta.dirty_blocks.store(0, Ordering::Release);
                let log = Self::open_chunk_log();
                let _ = (&log).write_all(&tag.encode());
            }
        }
    }

    /// Scan all cache slots to find the highest block number for a relation fork.
    ///
    /// Returns 0 if no blocks for this relation are found in the cache.
    /// The return value is an exclusive upper bound (i.e. nblocks, not max block index).
    ///
    /// Pinned slots (mid-I/O) are skipped without spinning — their contribution is
    /// either already reflected on disk or will be seen in a future call. The caller
    /// (`cached_file_nblocks`) always takes `max(disk_nblocks, cache_max)`, so
    /// skipping a pinned slot is safe for correctness.
    pub fn max_block_for_relation(&self, rf: RelFork) -> BlockNumber {
        let mut nblocks: BlockNumber = 0;

        for i in 0..self.num_slots {
            let meta = self.slot_meta(i);

            // Fast pre-filter: skip empty slots without paying for a CAS.
            let preflight = meta.valid_blocks.load(Ordering::Acquire);
            if preflight == 0 {
                continue;
            }

            // Pin the slot to prevent concurrent eviction/re-insert between the
            // valid_blocks check above and the tag read below. Without this pin a
            // concurrent evict() + insert() could replace the tag while we still
            // hold a stale non-zero valid_blocks observation (TOCTOU), and the
            // 20-byte ChunkTag struct read would be non-atomic.
            // Skip without spinning if already pinned — mid-I/O is best-effort.
            if meta
                .pin_count
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            // Re-load valid_blocks now that we hold the pin — eviction is blocked.
            let valid = meta.valid_blocks.load(Ordering::Acquire);
            if valid != 0 {
                let tag = &meta.tag;
                if tag.rel_fork() == rf {
                    // ilog2: position of the highest set bit (panics on 0, safe here).
                    let highest_bit = valid.ilog2();
                    let chunk_high = tag.chunk_id * BLOCKS_PER_CHUNK + highest_bit;
                    nblocks = std::cmp::max(nblocks, chunk_high + 1);
                }
            }

            meta.pin_count.fetch_sub(1, Ordering::Release);
        }

        nblocks
    }

    /// Flush all dirty chunks to backing files.
    ///
    /// Iterates every slot and flushes any with `dirty_blocks != 0`. Spins on
    /// pinned slots so no dirty block escapes (safe because pins are held only
    /// for the duration of a single cache I/O and released promptly).
    ///
    /// Called from:
    /// - `s3_checkpoint_flush()` — end of every checkpoint, after buffer pool flush
    /// - `s3_shutdown()` — smgr shutdown hook (process exit)
    pub fn flush_all_dirty_chunks(&self) {
        let mut flushed_count = 0;

        for i in 0..self.num_slots {
            let meta = self.slot_meta(i);

            // Fast pre-filter: skip clean slots without paying for a CAS.
            if meta.dirty_blocks.load(Ordering::Acquire) == 0 {
                continue;
            }

            // Spin until we can pin the slot exclusively.
            loop {
                if meta
                    .pin_count
                    .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
                std::hint::spin_loop();
            }

            // Re-check under pin — another flusher may have beaten us here.
            if meta.dirty_blocks.load(Ordering::Acquire) != 0 {
                self.flush_dirty_chunk(i);
                flushed_count += 1;
            }

            meta.pin_count.fetch_sub(1, Ordering::Release);
        }

        pg_log_debug1(&format!(
            "tiko: flush_all_dirty_chunks: flushed {} chunk(s)",
            flushed_count
        ));
    }

    // ── Chunk log (renamed from eviction log) ─────────────────────────────
    //
    // Records every ChunkTag flushed to express (both mid-interval evictions
    // and the end-of-checkpoint flush).  At checkpoint time the file is
    // atomically snapshotted to `chunk_log.ckpt` and consumed.

    /// Path of the chunk log file: `{tiko_root}/chunk_log`.
    pub fn chunk_log_path(tiko_root: &Path) -> PathBuf {
        tiko_root.join("chunk_log")
    }

    /// Path of the chunk log checkpoint file: `{tiko_root}/chunk_log.ckpt`.
    pub fn chunk_log_checkpoint_path(tiko_root: &Path) -> PathBuf {
        tiko_root.join("chunk_log.ckpt")
    }

    /// Append a single `ChunkTag` to the process-local chunk log.
    ///
    /// Used by the initdb write path (`cached_write_blocks` without IoControl)
    /// to ensure the shutdown checkpoint can discover and archive all chunks
    /// written during initdb, identical to what `flush_dirty_chunk` does on the
    /// normal (shmem-cache) path.
    pub fn append_to_chunk_log(tag: &ChunkTag) {
        let log = Self::open_chunk_log();
        let _ = (&log).write_all(&tag.encode());
    }

    /// Open (or create) the chunk log for appending.
    ///
    /// Each call opens a fresh `File` with `O_APPEND`. This is intentional:
    /// the checkpoint flush renames `chunk_log` → `chunk_log.ckpt` to take an
    /// atomic snapshot; subsequent evictions must write to a fresh inode.
    fn open_chunk_log() -> File {
        let path = Self::chunk_log_path(&tiko_root_path());
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .expect("failed to open chunk log")
    }

    /// Read all complete `ChunkTag` records from a chunk log file.
    ///
    /// Records are densely packed 20-byte entries. Any incomplete trailing
    /// record (caused by a crash mid-write) is silently skipped. Returns an
    /// empty `Vec` if the file does not exist.
    pub fn read_chunk_log(path: &Path) -> Vec<ChunkTag> {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
            Err(_) => return Vec::new(),
        };
        let n_complete = data.len() / 20;
        let mut records = Vec::with_capacity(n_complete);
        for i in 0..n_complete {
            let buf: &[u8; 20] = data[i * 20..(i + 1) * 20].try_into().unwrap();
            records.push(ChunkTag::decode(buf));
        }
        records
    }

    // ── nblocks log ───────────────────────────────────────────────────────
    //
    // Records NblocksRecord entries (RelFork + nblocks value, 20 bytes each)
    // whenever a relation's block count is set via `set_nblocks` on the initdb
    // path, or drained from the NblocksTable at checkpoint time.  At checkpoint
    // time the file is snapshotted to `nblocks_log.ckpt` and consumed via
    // last-write-wins dedup to populate `rel_nblocks` in the delta manifest.

    /// Path of the nblocks log file: `{tiko_root}/nblocks_log`.
    pub fn nblocks_log_path(tiko_root: &Path) -> PathBuf {
        tiko_root.join("nblocks_log")
    }

    /// Path of the nblocks log checkpoint file: `{tiko_root}/nblocks_log.ckpt`.
    pub fn nblocks_log_checkpoint_path(tiko_root: &Path) -> PathBuf {
        tiko_root.join("nblocks_log.ckpt")
    }

    /// Open (or create) the nblocks log for appending.
    ///
    /// Same rationale as `open_chunk_log`: open fresh each time so that the
    /// checkpoint rename-to-.ckpt snapshot does not leave us pointing at the
    /// old inode.
    fn open_nblocks_log() -> File {
        let path = Self::nblocks_log_path(&tiko_root_path());
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .expect("failed to open nblocks log")
    }

    /// Append a `NblocksRecord` (RelFork + nblocks value) to the nblocks log.
    ///
    /// Called from `set_nblocks` (initdb path) or `flush_all_dirty_nblocks`
    /// (checkpoint path) whenever a relation's block count is committed to
    /// express.  The checkpoint reads these records (last-write-wins per
    /// RelFork) to build `rel_nblocks` in the delta manifest.
    pub fn append_to_nblocks_log(rf: &RelFork, nblocks: u32) {
        let rec = NblocksRecord { rf: *rf, nblocks };
        let log = Self::open_nblocks_log();
        let _ = (&log).write_all(&rec.encode());
    }

    /// Read all complete `NblocksRecord` entries from an nblocks log file.
    ///
    /// Records are densely packed 20-byte entries.  Any incomplete trailing
    /// record is silently skipped.  Returns an empty `Vec` if the file does
    /// not exist.
    pub fn read_nblocks_log(path: &Path) -> Vec<NblocksRecord> {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
            Err(_) => return Vec::new(),
        };
        let n = data.len() / NBLOCKS_RECORD_SIZE;
        (0..n)
            .map(|i| {
                let start = i * NBLOCKS_RECORD_SIZE;
                let buf: &[u8; NBLOCKS_RECORD_SIZE] =
                    data[start..start + NBLOCKS_RECORD_SIZE].try_into().unwrap();
                NblocksRecord::decode(buf)
            })
            .collect()
    }

    /// Flush all dirty chunks belonging to a specific relation fork.
    ///
    /// Called from `s3_immedsync()` when PostgreSQL requests an immediate
    /// sync for a relation (e.g. `smgrdosyncall` during explicit buffer flush).
    pub fn flush_dirty_chunks_for_relation(
        &self,
        spc_oid: Oid,
        db_oid: Oid,
        rel_number: RelFileNumber,
        fork_number: ForkNumber,
    ) {
        for i in 0..self.num_slots {
            let meta = self.slot_meta(i);

            // Fast pre-filter: skip empty or clean slots.
            if meta.valid_blocks.load(Ordering::Acquire) == 0
                || meta.dirty_blocks.load(Ordering::Acquire) == 0
            {
                continue;
            }

            // Spin to pin — same rationale as flush_all_dirty_chunks.
            loop {
                if meta
                    .pin_count
                    .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
                std::hint::spin_loop();
            }

            // Re-check tag and dirty under pin.
            let tag = &meta.tag;
            if tag.spc_oid == spc_oid
                && tag.db_oid == db_oid
                && tag.rel_number == rel_number
                && tag.fork_number == fork_number
                && meta.dirty_blocks.load(Ordering::Acquire) != 0
            {
                self.flush_dirty_chunk(i);
            }

            meta.pin_count.fetch_sub(1, Ordering::Release);
        }
    }

    /// Invalidate cache slots for a relation fork starting from `first_block`.
    ///
    /// Used by truncate and unlink to ensure the cache doesn't return stale data
    /// or "ghost" blocks beyond the new EOF.
    pub fn invalidate_range(&self, rf: RelFork, first_block: BlockNumber) {
        for i in 0..self.num_slots {
            let meta = self.slot_meta(i);

            // Preflight: skip empty slots without a CAS.
            if meta.valid_blocks.load(Ordering::Relaxed) == 0 {
                continue;
            }

            // Spin until we acquire exclusive access (pin_count CAS 0 → 1).
            // Pinners hold the pin briefly (cache I/O only, no sleeping), so
            // an unbounded spin is safe and avoids skipping slots that need
            // invalidation — skipping would leave stale blocks in the cache.
            loop {
                if meta
                    .pin_count
                    .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
                std::hint::spin_loop();
            }

            // Re-check valid_blocks now that we hold the pin — eviction is blocked.
            let valid = meta.valid_blocks.load(Ordering::Acquire);
            let tag = &meta.tag;

            if valid != 0 && tag.rel_fork() == rf {
                let chunk_start = tag.chunk_id * BLOCKS_PER_CHUNK;
                let chunk_end = chunk_start + BLOCKS_PER_CHUNK;

                if chunk_start >= first_block {
                    // Whole chunk is truncated — remove from hash table and reset
                    self.reset_slot(i);
                } else if first_block < chunk_end {
                    // Partial chunk truncation
                    let offset = first_block - chunk_start;
                    let mask = !((!0u32) << offset); // bits 0..offset-1 remain valid
                    meta.valid_blocks.fetch_and(mask, Ordering::Release);
                    meta.dirty_blocks.fetch_and(mask, Ordering::Release);
                }
            }

            meta.pin_count.fetch_sub(1, Ordering::Release);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::Lsn;
    use std::collections::HashSet;
    use std::sync::Arc;
    use store::project::ProjectNamespace;
    use store::sim_store::SimStore;
    use tempfile::TempDir;

    fn make_tag(i: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: i,
            db_oid: i,
            rel_number: i,
            fork_number: 0,
            chunk_id: i,
        }
    }

    // ── read_chunk_log ─────────────────────────────────────────────────

    #[test]
    fn read_chunk_log_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let records = CacheControl::read_chunk_log(&dir.path().join("no_such_log"));
        assert!(records.is_empty());
    }

    #[test]
    fn read_chunk_log_skips_partial_trailing_record() {
        let dir = TempDir::new().unwrap();
        let path = CacheControl::chunk_log_path(dir.path());

        // Write 2 complete records (40 bytes) + 10 bytes of a partial third record.
        let tag0 = make_tag(0);
        let tag1 = make_tag(1);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.write_all(&tag0.encode()).unwrap();
        file.write_all(&tag1.encode()).unwrap();
        file.write_all(&[0xAB; 10]).unwrap(); // partial record
        drop(file);

        let records = CacheControl::read_chunk_log(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], tag0);
        assert_eq!(records[1], tag1);
    }

    #[test]
    fn read_chunk_log_empty_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = CacheControl::chunk_log_path(dir.path());
        std::fs::write(&path, b"").unwrap();
        let records = CacheControl::read_chunk_log(&path);
        assert!(records.is_empty());
    }

    #[test]
    fn read_chunk_log_exact_n_records() {
        let dir = TempDir::new().unwrap();
        let path = CacheControl::chunk_log_path(dir.path());
        let n = 8usize;
        let tags: Vec<ChunkTag> = (0..n as u32).map(make_tag).collect();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        for tag in &tags {
            file.write_all(&tag.encode()).unwrap();
        }
        drop(file);

        let records = CacheControl::read_chunk_log(&path);
        assert_eq!(records, tags);
    }

    // ── Concurrent O_APPEND writes ────────────────────────────────────────

    #[test]
    fn concurrent_log_appends_produce_n_records_without_corruption() {
        let dir = TempDir::new().unwrap();
        let path = Arc::new(CacheControl::chunk_log_path(dir.path()));
        let n = 16usize;
        let tags: Vec<ChunkTag> = (0..n as u32).map(make_tag).collect();

        let handles: Vec<_> = tags
            .iter()
            .map(|tag| {
                let tag = *tag;
                let p = Arc::clone(&path);
                std::thread::spawn(move || {
                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(&*p)
                        .unwrap();
                    (&file).write_all(&tag.encode()).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let records = CacheControl::read_chunk_log(&path);
        assert_eq!(
            records.len(),
            n,
            "expected {n} records, got {}",
            records.len()
        );

        // Every record must be one of the original tags (no corruption).
        let tag_set: HashSet<ChunkTag> = tags.into_iter().collect();
        for rec in &records {
            assert!(tag_set.contains(rec), "unexpected record: {rec:?}");
        }
    }

    // ── Express PUT uses put_express_latest (no staging, no standard) ─────

    #[test]
    fn express_put_creates_only_latest_no_staging_no_standard() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ProjectNamespace::new(1001, 2001, 7);
        let tag = make_tag(42);
        let data = vec![0u8; CHUNK_SIZE];

        sim.put_express_latest(&ns, &tag, 1, &data).unwrap();

        // Express latest must exist.
        assert!(
            sim.get_express(&ns.chunk_latest_key(&tag, 1))
                .unwrap()
                .is_some()
        );

        // No staging file must exist.
        let staging_key = ns.chunk_staging_key(&tag, Lsn::INVALID);
        assert!(sim.get_express(&staging_key).unwrap().is_none());

        // No standard-bucket versioned object must exist.
        let versioned_key = format!(
            "{}/chunks/{}/{}/{}",
            ns.org_id,
            ns.branch_id,
            tag.to_path(),
            Lsn::INVALID.to_hex()
        );
        assert!(sim.get_standard(&versioned_key).unwrap().is_none());
    }
}
