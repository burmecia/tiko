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
//! - **Cache file**: single pre-allocated file at `{DataDir}/pico/cache`,
//!   divided into fixed 256 KB chunk slots. Slot N lives at byte offset
//!   `N * CHUNK_SIZE`. Each chunk holds 32 contiguous 8 KB blocks.
//! - **Metadata arrays**: slot metadata, hash table, and partition locks live
//!   in PG shared memory as trailing arrays after `S3IoControl`.
//! - **CacheControl**: embedded in `S3IoControl` in PG shared memory,
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
//!   PG LWLocks cannot be used because Tokio threads in s3worker also access the
//!   hash table (via `cached_read_blocks`/`cached_write_blocks` in `io_handler`),
//!   and LWLocks require per-process state (`MyProc`) that isn't thread-safe.
//! - Lookups hold a shared (read) lock. Insertions/evictions hold exclusive (write).
//! - `pin_count` is atomically incremented/decremented — a pinned slot
//!   (`pin_count > 0`) is skipped during eviction.
//! - `usage_count` is atomically bumped on access (saturating at 5).

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU32, Ordering};

use pgsys::common::{BLCKSZ, BlockNumber, DataDir, ForkNumber, Oid, RelFileNumber};

// ── Constants ──

/// Number of blocks per chunk (32 blocks = 256 KB).
pub const BLOCKS_PER_CHUNK: u32 = 32;

/// Chunk size in bytes (32 × 8 KB = 256 KB).
pub const CHUNK_SIZE: usize = BLOCKS_PER_CHUNK as usize * BLCKSZ;

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

// ── ChunkTag ──

/// Identifies a 256 KB chunk (32 contiguous blocks) within a relation fork.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ChunkTag {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
    pub chunk_id: u32, // = blkno / BLOCKS_PER_CHUNK
}

impl ChunkTag {
    /// Construct a ChunkTag from a block number.
    pub fn from_block(
        spc_oid: Oid,
        db_oid: Oid,
        rel_number: RelFileNumber,
        fork_number: ForkNumber,
        blkno: BlockNumber,
    ) -> Self {
        ChunkTag {
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            chunk_id: blkno / BLOCKS_PER_CHUNK,
        }
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
}

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
    Empty = 0,
    Occupied = 1,
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
    fn init(&self) {
        self.state.store(0, Ordering::Relaxed);
    }

    fn read_lock(&self) {
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

    fn read_unlock(&self) {
        self.state.fetch_sub(1, Ordering::Release);
    }

    fn write_lock(&self) {
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

    fn write_unlock(&self) {
        self.state.store(0, Ordering::Release);
    }
}

// ── CacheControl ──

/// Main cache control structure. Embedded in `S3IoControl` in PG shared memory.
///
/// The variable-size arrays (slot metadata, hash table, partition locks) follow
/// `S3IoControl` as trailing arrays in the same shared memory allocation.
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
// processes (inherited via fork). Tokio threads in s3worker also access these.
unsafe impl Send for CacheControl {}
unsafe impl Sync for CacheControl {}

impl CacheControl {
    /// Initialize CacheControl fields and array pointers. Called once when
    /// shared memory is first created (from `S3IoControl::init`).
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
        let data_dir = unsafe { std::ffi::CStr::from_ptr(DataDir).to_str().unwrap_or("") };
        PathBuf::from(data_dir).join("pico").join("cache")
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
                s if s == HashStatus::Empty as u8 => return None,
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
    /// The returned slot has valid_blocks=0 and dirty_blocks=0.
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

        // Insert into hash table
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

            if status == HashStatus::Empty as u8 {
                unsafe {
                    let entry_ptr = entry as *const CacheHashEntry as *mut CacheHashEntry;
                    (*entry_ptr).tag = *tag;
                    (*entry_ptr).slot_index = slot_index;
                }
                entry
                    .status
                    .store(HashStatus::Occupied as u8, Ordering::Release);
                break;
            }
            idx = (idx + 1) % num_hash;
        }

        lock.write_unlock();

        slot_index
    }

    /// Clock-sweep eviction. Returns evicted slot index, **pinned** (pin_count = 1).
    ///
    /// If the evicted slot has dirty blocks, they are flushed to S3-sim files
    /// before the slot is returned.
    fn evict(&self) -> u32 {
        for _ in 0..(self.num_slots * 2) {
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
                meta.pin_count.store(0, Ordering::Release);
                continue;
            }

            // Evict: usage_count == 0, has valid blocks, we hold pin.
            // Flush dirty blocks to S3-sim before eviction.
            let dirty = meta.dirty_blocks.load(Ordering::Acquire);
            if dirty != 0 {
                self.flush_dirty_chunk(slot_index);
                crate::io_queue::S3IoControl::get()
                    .stats
                    .dirty_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            crate::io_queue::S3IoControl::get()
                .stats
                .evictions
                .fetch_add(1, Ordering::Relaxed);

            self.remove_from_hash_table(&meta.tag);

            meta.valid_blocks.store(0, Ordering::Release);
            meta.dirty_blocks.store(0, Ordering::Relaxed);
            meta.usage_count.store(0, Ordering::Relaxed);

            return slot_index;
        }

        panic!("cache eviction failed: no evictable slot found after full sweep");
    }

    fn remove_from_hash_table(&self, tag: &ChunkTag) {
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
                s if s == HashStatus::Occupied as u8 => {
                    if entry.tag == *tag {
                        entry
                            .status
                            .store(HashStatus::Empty as u8, Ordering::Release);
                        break;
                    }
                }
                _ => {}
            }
            idx = (idx + 1) % num_hash;
        }

        lock.write_unlock();
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

    /// Flush dirty blocks from a cache slot to S3-sim files.
    ///
    /// Reads each dirty block from the cache file and writes it to the
    /// backing relation file via `s3_ops::write_blocks`. Called during
    /// eviction when `dirty_blocks != 0`.
    ///
    /// No deadlock risk: at this point we hold pin_count=1 but no partition
    /// locks. `write_blocks()` does simple pwrite — no cache/lock interaction.
    pub fn flush_dirty_chunk(&self, slot_index: u32) {
        let meta = self.slot_meta(slot_index);
        let dirty = meta.dirty_blocks.load(Ordering::Relaxed);
        let tag = &meta.tag;

        for bit in 0..BLOCKS_PER_CHUNK {
            if dirty & (1 << bit) != 0 {
                let mut buf = [0u8; BLCKSZ];
                self.read_block(slot_index, bit, &mut buf);
                let blkno = tag.chunk_id * BLOCKS_PER_CHUNK + bit;
                let _ = crate::s3_ops::write_blocks(
                    tag.spc_oid,
                    tag.db_oid,
                    tag.rel_number,
                    tag.fork_number,
                    blkno,
                    1,
                    buf.as_ptr(),
                );
            }
        }
        meta.dirty_blocks.store(0, Ordering::Release);
    }
}
