//! Per-backend slot pool + MPSC submit queue for I/O requests
//!
//! This module replaces the earlier 8-queue ring buffer design with a simpler architecture:
//!
//! # Architecture
//!
//! - **Per-backend slot pools**: Each backend owns a small pool of I/O slots (4 slots).
//!   Claiming is a local bit-scan — zero contention, no CAS races.
//! - **MPSC submit queue**: Backends push `(backend_id, slot_idx)` entries via `fetch_add`.
//!   worker pops entries and dispatches to Tokio. Strict ordering, no advisory hints.
//! - **SetLatch completion**: Tokio workers call `SetLatch` directly on the backend's latch
//!   after marking a slot Completed. No harvest step, no main-thread scan.
//!
//! # Slot State Machine
//!
//! `Free → Filling → Submitted → InProgress → Completed → Free`
//!
//! | Transition | Who | Mechanism |
//! |---|---|---|
//! | Free → Filling | Backend | Claim from own pool (bit clear) |
//! | Filling → Submitted | Backend | `slot.publish()` (Release store) |
//! | Submitted → InProgress | Tiko worker | `slot.try_start_processing()` (CAS) |
//! | InProgress → Completed | Tokio | `slot.mark_completed()` + `SetLatch` |
//! | Completed → Free | Backend | `slot.release()` + `pool.release()` |
//!
//! # Memory Ordering
//!
//! - `publish()`: Release — ensures request fields visible before Submitted
//! - `try_start_processing()`: Acquire on success — sees request data
//! - `mark_completed()`: Release — ensures result fields visible before Completed
//! - `current_state()`: Acquire — sees result data after Completed
//! - `SubmitQueue.head.fetch_add`: Relaxed — ordering provided by entry store (Release)
//! - `SubmitQueue.entries[].store`: Release — synchronizes with consumer's Acquire load
//!
//! # Shared Memory Layout
//!
//! ```text
//! IoControl (fixed size)
//! ├── num_backend_pools, worker_pid, worker_latch
//! ├── submit_queue (SubmitQueue)
//! ├── stats (S3IoStats)
//! ├── cache (CacheControl)
//! └── nblocks (NblocksControl)
//! BackendSlotPool[0]  ← immediately after IoControl (aligned)
//! BackendSlotPool[1]
//! ...
//! BackendSlotPool[MaxBackends-1]
//! CacheSlotMeta[0..1024]      ← cache chunk slot metadata (~36 KB)
//! CacheHashEntry[0..2048]     ← cache hash table (~52 KB)
//! AtomicRWLock[0..128]        ← cache partition locks (512 bytes)
//! NblocksEntry[0..4096]       ← nblocks hash table (~96 KB)
//! AtomicRWLock[0..64]         ← nblocks partition locks (256 bytes)
//! ```

use pgsys::{
    common::{BlockNumber, ForkNumber, Oid, RelFileNumber},
    latch::{Latch, SetLatch},
    logging::*,
    lwlock::*,
    shmem::{ShmemInitStruct, rust_get_addin_shmem_init_lock},
};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU32, AtomicU64, Ordering};
use tokio::sync::mpsc::error::TrySendError;

use crate::cache::{
    AtomicRWLock, CACHE_NUM_HASH_ENTRIES, CACHE_NUM_PARTITIONS, CACHE_NUM_SLOTS, CacheControl,
    CacheHashEntry, CacheSlotMeta,
};
use crate::nblocks_table::{
    NBLOCKS_NUM_ENTRIES, NBLOCKS_NUM_PARTITIONS, NblocksControl, NblocksEntry,
};

// ── Constants ──

pub const SLOTS_PER_BACKEND: usize = 4;
pub const SUBMIT_QUEUE_SIZE: usize = 1024; // power of 2

// ── Shared memory pointer ──

struct IoControlPtr(*mut IoControl);
unsafe impl Send for IoControlPtr {}
unsafe impl Sync for IoControlPtr {}

static IO_CONTROL: OnceLock<IoControlPtr> = OnceLock::new();

// ── Slot state machine ──

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Free = 0,
    Filling = 1,
    Submitted = 2,
    InProgress = 3,
    Completed = 4,
}

impl From<u8> for SlotState {
    fn from(val: u8) -> Self {
        match val {
            0 => SlotState::Free,
            1 => SlotState::Filling,
            2 => SlotState::Submitted,
            3 => SlotState::InProgress,
            4 => SlotState::Completed,
            _ => SlotState::Free,
        }
    }
}

// ── I/O operation types ──

/// S3 I/O operation kinds.
///
/// Used in two contexts:
/// - **AIO path** (`s3_io_perform`): `Read` and `Write` are submitted through
///   the shared-memory pipeline to Tiko worker. Buffers are always in shared memory
///   (`BufferBlocks`), so cross-process pointer access is safe.
/// - **Prefetch** (`s3_prefetch`): Submitted through the pipeline to warm the
///   local cache from S3. No `buffer_ptr` needed (Tiko worker manages its own buffers).
///
/// All other sync smgr functions (`s3_readv`, `s3_writev`, `s3_extend`, etc.)
/// call `store_ops` directly in the backend process — they do **not** use the
/// pipeline, because their buffers may be in backend-local memory (e.g.
/// `PageSetChecksumCopy` palloc'd pages, `LocalBufferBlockPointers`, stack-local
/// `PGIOAlignedBlock`) which Tiko worker cannot access.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOpKind {
    Invalid = 0,
    Read = 1,        // AIO pipeline + direct store_ops
    Write = 2,       // AIO pipeline + direct store_ops
    Exists = 3,      // direct store_ops only
    Create = 4,      // direct store_ops only
    Fsync = 5,       // direct store_ops only
    Nblocks = 6,     // direct store_ops only
    Prefetch = 7,    // pipeline only (cache warming, no buffer_ptr)
    Truncate = 8,    // direct store_ops only
    Unlink = 9,      // direct store_ops only
    ZeroExtend = 10, // direct store_ops only
}

/// Work request sent from worker main thread to Tokio workers.
///
/// Identifies a slot by its backend pool and slot index.
/// `backend_id` is a ProcNumber (u32), `slot_index` is 0..SLOTS_PER_BACKEND-1,
/// and `generation` guards against stale completions after backend slot recycle.
#[derive(Debug, Clone)]
pub struct IoWorkRequest {
    pub backend_id: u32,
    pub slot_index: u8,
    pub generation: u32,
}

// ── IoSlot ──

/// A single I/O request slot in shared memory.
///
/// Size: 64 bytes. No ConditionVariable — completion uses SetLatch via `owner_latch`.
/// Generation counter prevents stale Tokio completions from corrupting recycled slots
/// (e.g. when a backend dies with InProgress slots and a new backend reuses the ProcNumber).
#[repr(C, align(64))]
pub struct IoSlot {
    // ── Slot lifecycle ──
    pub state: AtomicU8,
    pub op: IoOpKind,

    // ── Request identity ──
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
    pub block_number: BlockNumber,
    pub nblocks: BlockNumber,

    // ── Ownership ──
    /// Backend's MyProcNumber (for debugging/validation)
    pub owner_proc: AtomicI32,

    /// Monotonically increasing generation counter. Bumped by `BackendSlotPool::attach()`
    /// when a new backend attaches. Tokio checks this at completion time — if the
    /// generation has changed, the result is silently discarded (the original backend died).
    pub generation: AtomicU32,

    /// Backend's MyLatch as u64. Tokio calls SetLatch(owner_latch) directly.
    pub owner_latch: AtomicU64,

    // ── Data transfer ──
    /// Pointer into shared_buffers where the block data lives.
    pub buffer_ptr: AtomicU64,

    // ── Result ──
    pub result_status: AtomicU32,
    pub result_nblocks: AtomicU32,
    // No _reserved needed: generation + alignment padding fill the 64 bytes exactly.
}

const _: () = assert!(
    std::mem::size_of::<IoSlot>() == 64,
    "IoSlot must be exactly 64 bytes"
);

impl IoSlot {
    fn init(&mut self) {
        self.state.store(SlotState::Free as u8, Ordering::Relaxed);
        self.op = IoOpKind::Invalid;
        self.owner_proc.store(-1, Ordering::Relaxed);
        self.generation.store(0, Ordering::Relaxed);
        self.owner_latch.store(0, Ordering::Relaxed);
        self.buffer_ptr.store(0, Ordering::Relaxed);
        self.result_status.store(0, Ordering::Relaxed);
        self.result_nblocks.store(0, Ordering::Relaxed);
    }

    pub fn current_state(&self) -> SlotState {
        SlotState::from(self.state.load(Ordering::Acquire))
    }

    /// Publish the request (Filling → Submitted).
    /// Release fence ensures all request fields are visible before state change.
    pub fn publish(&self) {
        self.state
            .store(SlotState::Submitted as u8, Ordering::Release);
    }

    /// Try to start processing (Submitted → InProgress).
    /// Called by Tiko worker after popping from the submit queue.
    pub fn try_start_processing(&self) -> bool {
        self.state
            .compare_exchange(
                SlotState::Submitted as u8,
                SlotState::InProgress as u8,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Mark completed (InProgress → Completed).
    /// Called by Tokio worker after writing result fields.
    /// Caller must then call SetLatch(owner_latch) to wake the backend.
    pub fn mark_completed(&self) {
        self.state
            .store(SlotState::Completed as u8, Ordering::Release);
    }

    /// Release slot (Completed → Free).
    /// Called by the backend after reading the result.
    pub fn release(&self) {
        self.state.store(SlotState::Free as u8, Ordering::Release);
    }

    /// Validate slot data before dispatching to Tokio.
    pub fn validate(&self) -> Result<(), u32> {
        if self.op == IoOpKind::Invalid {
            return Err(libc::EINVAL as u32);
        }

        // Only Read and Write operations require a buffer
        let needs_buffer = matches!(self.op, IoOpKind::Read | IoOpKind::Write);
        if needs_buffer {
            let buffer_ptr = self.buffer_ptr.load(Ordering::Acquire);
            if buffer_ptr == 0 {
                return Err(libc::EFAULT as u32);
            }
        }

        // Only operations that transfer blocks need nblocks validation
        let needs_nblocks = matches!(
            self.op,
            IoOpKind::Read | IoOpKind::Write | IoOpKind::ZeroExtend | IoOpKind::Prefetch
        );
        if needs_nblocks && (self.nblocks == 0 || self.nblocks > 1024) {
            return Err(libc::EINVAL as u32);
        }

        Ok(())
    }

    /// Fail slot with an error and wake the backend via SetLatch.
    pub fn fail_with_error(&self, error_code: u32) {
        self.result_status.store(error_code, Ordering::Release);
        self.mark_completed();
        // Wake the backend directly
        let latch = self.owner_latch.load(Ordering::Acquire) as *mut Latch;
        if !latch.is_null() {
            unsafe {
                SetLatch(latch);
            }
        }
    }
}

// ── BackendSlotPool ──

/// Per-backend pool of I/O slots. Allocated in shared memory, one per backend.
///
/// Only the owning backend writes to `free_mask` — zero contention on claiming.
/// Attached lazily when `s3_init()` calls `attach()` for that backend.
#[repr(C)]
pub struct BackendSlotPool {
    pub slots: [IoSlot; SLOTS_PER_BACKEND],
    /// Bitmask of free slots. Bit N set = slot N is free.
    pub free_mask: AtomicU8,
    /// 1 if a backend has attached to this pool via s3_init()
    pub attached: AtomicU8,
}

impl BackendSlotPool {
    fn init(&mut self) {
        self.free_mask.store(0, Ordering::Relaxed);
        self.attached.store(0, Ordering::Relaxed);
        for slot in &mut self.slots {
            slot.init();
        }
    }

    /// Attach a backend to this pool. Called from s3_init() each time a backend starts.
    ///
    /// Clears all slots to Free state and bumps their generation counter.
    /// This handles ProcNumber recycling: if a previous backend crashed with
    /// slots in InProgress state, Tokio will see the generation mismatch at
    /// completion time and silently discard the stale result instead of
    /// writing to recycled memory or calling SetLatch on a stale pointer.
    pub fn attach(&self) {
        for slot in &self.slots {
            slot.state.store(SlotState::Free as u8, Ordering::Relaxed);
            // Bump generation so any in-flight Tokio work for the old backend
            // will detect the mismatch and discard results.
            slot.generation.fetch_add(1, Ordering::Relaxed);
            slot.owner_proc.store(-1, Ordering::Relaxed);
            slot.owner_latch.store(0, Ordering::Relaxed);
            slot.buffer_ptr.store(0, Ordering::Relaxed);
            slot.result_status.store(0, Ordering::Relaxed);
            slot.result_nblocks.store(0, Ordering::Relaxed);
        }
        let all_free = (1u8 << SLOTS_PER_BACKEND) - 1; // 0x0F for 4 slots
        self.free_mask.store(all_free, Ordering::Relaxed);
        self.attached.store(1, Ordering::Release);
    }

    /// Claim a free slot from this pool. Returns slot index (0..3) or None.
    ///
    /// Single-writer (only the owning backend calls this), so no CAS loop needed.
    pub fn try_claim(&self) -> Option<usize> {
        loop {
            let mask = self.free_mask.load(Ordering::Relaxed);
            if mask == 0 {
                return None; // All slots in-flight
            }
            let idx = mask.trailing_zeros() as usize;
            let clear_bit = !(1u8 << idx);
            // Atomic to be safe (even though single-writer), Acquire to sync with release
            let prev = self.free_mask.fetch_and(clear_bit, Ordering::Acquire);
            if prev & (1u8 << idx) != 0 {
                // Successfully claimed
                return Some(idx);
            }
            // Bit was already cleared (shouldn't happen with single writer, but defensive)
        }
    }

    /// Release a slot back to the free pool.
    ///
    /// Transitions the slot state (Completed → Free) and sets the free bit.
    /// Called by the backend after reading the completed result.
    pub fn release(&self, slot_idx: usize) {
        debug_assert!(slot_idx < SLOTS_PER_BACKEND);
        self.slots[slot_idx].release();
        self.free_mask.fetch_or(1u8 << slot_idx, Ordering::Release);
    }

    /// Get a reference to a slot by index.
    pub fn slot(&self, idx: usize) -> &IoSlot {
        &self.slots[idx]
    }
}

// ── SubmitQueue ──

/// MPSC ring buffer for I/O submission. Backends push, Tiko worker pops.
///
/// Entries are packed as `(backend_id: u16, slot_idx: u16)` into a u32.
/// Zero is used as a sentinel (entry not yet written by producer).
#[repr(C, align(128))]
pub struct SubmitQueue {
    /// Producer head — backends do fetch_add(1) to claim next write position
    head: AtomicU32,
    _pad_head: [u8; 60],

    /// Consumer tail — only Tiko worker reads/writes
    tail: AtomicU32,
    _pad_tail: [u8; 60],

    entries: [AtomicU32; SUBMIT_QUEUE_SIZE],
}

/// Number of bits needed for slot_idx (log2(SLOTS_PER_BACKEND))
const SLOT_IDX_BITS: u32 = SLOTS_PER_BACKEND.ilog2();
const SLOT_IDX_MASK: u32 = (1 << SLOT_IDX_BITS) - 1; // 0x3 for 4 slots

// Compile-time check: max backend_id fits in remaining bits
// PG's MAX_BACKENDS is 0x3FFFF (262143), needs 18 bits. We have 30.
const _: () = assert!(
    SLOT_IDX_BITS + 18 <= 32,
    "packed entry must fit backend_id + slot_idx in 32 bits"
);

impl SubmitQueue {
    fn init(&self) {
        self.head.store(0, Ordering::Relaxed);
        self.tail.store(0, Ordering::Relaxed);
        for entry in &self.entries {
            entry.store(0, Ordering::Relaxed);
        }
    }

    /// Pack a (backend_id, slot_idx) pair into a u32 submit entry.
    ///
    /// Layout: `[backend_id (30 bits) | slot_idx (2 bits)] + 1`
    /// The +1 avoids zero, which is our sentinel for "not yet written".
    fn pack(backend_id: u32, slot_idx: u8) -> u32 {
        debug_assert!((slot_idx as u32) < SLOTS_PER_BACKEND as u32);
        ((backend_id << SLOT_IDX_BITS) | slot_idx as u32).wrapping_add(1)
    }

    /// Unpack a u32 entry back to (backend_id, slot_idx).
    fn unpack(val: u32) -> (u32, u8) {
        let raw = val.wrapping_sub(1);
        (raw >> SLOT_IDX_BITS, (raw & SLOT_IDX_MASK) as u8)
    }

    /// Push a submission entry. Called by backends after slot.publish().
    ///
    /// Uses fetch_add for strict MPSC ordering. The entry store uses Release
    /// to synchronize with the consumer's Acquire load.
    ///
    /// Returns false if the queue is full (backpressure).
    pub fn push(&self, backend_id: u32, slot_idx: u8) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= SUBMIT_QUEUE_SIZE as u32 {
            return false; // Queue full
        }

        let pos = self.head.fetch_add(1, Ordering::Relaxed);
        let idx = (pos as usize) % SUBMIT_QUEUE_SIZE;
        let packed = Self::pack(backend_id, slot_idx);
        self.entries[idx].store(packed, Ordering::Release);
        true
    }
}

// Compile-time assertions for cache line separation
const _: () = assert!(
    std::mem::offset_of!(SubmitQueue, head) == 0,
    "head must be at offset 0"
);
const _: () = assert!(
    std::mem::offset_of!(SubmitQueue, tail) == 64,
    "tail must be at offset 64 for cache line separation"
);
const _: () = assert!(
    std::mem::offset_of!(SubmitQueue, entries) == 128,
    "entries must be at offset 128"
);

// ── S3IoStats ──

#[repr(C)]
pub struct S3IoStats {
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub evictions: AtomicU64,
    pub dirty_evictions: AtomicU64,
    pub s3_gets: AtomicU64,
    pub s3_puts: AtomicU64,
    pub queue_full_waits: AtomicU64,
}

impl S3IoStats {
    pub fn init(&self) {
        self.total_reads.store(0, Ordering::Relaxed);
        self.total_writes.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.dirty_evictions.store(0, Ordering::Relaxed);
        self.s3_gets.store(0, Ordering::Relaxed);
        self.s3_puts.store(0, Ordering::Relaxed);
        self.queue_full_waits.store(0, Ordering::Relaxed);
    }

    /// Log a summary of cache performance stats.
    pub fn log_summary(&self) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total_lookups = hits + misses;
        let hit_rate = if total_lookups > 0 {
            hits as f64 / total_lookups as f64 * 100.0
        } else {
            0.0
        };

        pgsys::logging::pg_log_debug1(&format!(
            "tiko cache stats: reads={} writes={} hits={} misses={} hit_rate={:.1}% evictions={} dirty_evictions={}",
            self.total_reads.load(Ordering::Relaxed),
            self.total_writes.load(Ordering::Relaxed),
            hits,
            misses,
            hit_rate,
            self.evictions.load(Ordering::Relaxed),
            self.dirty_evictions.load(Ordering::Relaxed),
        ));
    }
}

// ── IoControl ──

/// Main control structure for I/O queues. Lives in PostgreSQL shared memory.
///
/// Backend slot pools follow immediately after this struct in shared memory,
/// accessed via `backend_pool()` pointer arithmetic.
#[repr(C)]
pub struct IoControl {
    /// Number of backend pools (= MaxBackends at init time)
    pub num_backend_pools: u32,

    /// Tiko worker's PID for liveness checks
    pub worker_pid: AtomicU32,

    /// Tiko worker's latch pointer. Backends call SetLatch to wake Tiko worker.
    pub worker_latch: AtomicU64,

    /// MPSC submission queue
    pub submit_queue: SubmitQueue,

    /// Local cache control (slot count, clock hand for eviction)
    pub cache: CacheControl,

    /// I/O statistics
    pub stats: S3IoStats,

    /// Write-back nblocks hash table (relation fork → live block count)
    pub nblocks: NblocksControl,

    /// Global monotonic counter for cache dirty chunk sidecar file uniqueness.
    /// Each call to `CacheControl::next_sidecar_seq()` does fetch_add(1, Relaxed).
    /// Uniqueness (not ordering) is the only requirement.
    pub sidecar_seq: AtomicU64,
}

impl IoControl {
    fn init(&mut self, max_backends: usize) {
        self.num_backend_pools = max_backends as u32;
        self.worker_pid.store(0, Ordering::Relaxed);
        self.worker_latch.store(0, Ordering::Relaxed);
        self.submit_queue.init();

        // Initialize all backend pools
        let pools_base = unsafe {
            (self as *mut Self as *mut u8).add(Self::backend_pools_offset()) as *mut BackendSlotPool
        };
        for i in 0..max_backends {
            unsafe { &mut *pools_base.add(i) }.init();
        }

        // Initialize cache control + metadata arrays in shared memory
        unsafe {
            let base = self as *mut Self as *mut u8;
            let slots = base.add(Self::slot_meta_offset(max_backends)) as *mut CacheSlotMeta;
            let locks = base.add(Self::cache_locks_offset(max_backends)) as *mut AtomicRWLock;
            let hash = base.add(Self::hash_entries_offset(max_backends)) as *mut CacheHashEntry;
            self.cache.init(slots, hash, locks);
        }

        self.stats.init();

        // Initialize nblocks table + its arrays in shared memory
        unsafe {
            let base = self as *mut Self as *mut u8;
            let entries = base.add(Self::nblocks_entries_offset(max_backends)) as *mut NblocksEntry;
            let nlocks = base.add(Self::nblocks_locks_offset(max_backends)) as *mut AtomicRWLock;
            self.nblocks.init(entries, nlocks);
        }

        self.sidecar_seq.store(0, Ordering::Relaxed);
    }

    /// Byte offset from the start of IoControl to the first BackendSlotPool.
    /// Accounts for alignment requirements of BackendSlotPool.
    fn backend_pools_offset() -> usize {
        let base = std::mem::size_of::<Self>();
        let align = std::mem::align_of::<BackendSlotPool>();
        (base + align - 1) & !(align - 1)
    }

    /// Byte offset to the slot metadata array (after backend pools).
    fn slot_meta_offset(max_backends: usize) -> usize {
        let after_pools =
            Self::backend_pools_offset() + max_backends * std::mem::size_of::<BackendSlotPool>();
        let align = std::mem::align_of::<CacheSlotMeta>();
        (after_pools + align - 1) & !(align - 1)
    }

    /// Byte offset to the hash entries array (after slot metadata).
    fn hash_entries_offset(max_backends: usize) -> usize {
        let after_slots = Self::slot_meta_offset(max_backends)
            + CACHE_NUM_SLOTS as usize * std::mem::size_of::<CacheSlotMeta>();
        let align = std::mem::align_of::<CacheHashEntry>();
        (after_slots + align - 1) & !(align - 1)
    }

    /// Byte offset to the partition locks array (after hash entries).
    fn cache_locks_offset(max_backends: usize) -> usize {
        let after_hash = Self::hash_entries_offset(max_backends)
            + CACHE_NUM_HASH_ENTRIES as usize * std::mem::size_of::<CacheHashEntry>();
        let align = std::mem::align_of::<AtomicRWLock>();
        (after_hash + align - 1) & !(align - 1)
    }

    /// Byte offset to the nblocks entries array (after cache partition locks).
    fn nblocks_entries_offset(max_backends: usize) -> usize {
        let after_cache_locks = Self::cache_locks_offset(max_backends)
            + CACHE_NUM_PARTITIONS as usize * std::mem::size_of::<AtomicRWLock>();
        let align = std::mem::align_of::<NblocksEntry>();
        (after_cache_locks + align - 1) & !(align - 1)
    }

    /// Byte offset to the nblocks partition locks array (after nblocks entries).
    fn nblocks_locks_offset(max_backends: usize) -> usize {
        let after_entries = Self::nblocks_entries_offset(max_backends)
            + NBLOCKS_NUM_ENTRIES as usize * std::mem::size_of::<NblocksEntry>();
        let align = std::mem::align_of::<AtomicRWLock>();
        (after_entries + align - 1) & !(align - 1)
    }

    /// Total shared memory size for the control structure + backend pools + all arrays.
    pub fn shmem_size(max_backends: usize) -> usize {
        Self::nblocks_locks_offset(max_backends)
            + NBLOCKS_NUM_PARTITIONS as usize * std::mem::size_of::<AtomicRWLock>()
    }

    /// Get the backend slot pool for a given proc number.
    pub fn backend_pool(&self, proc_number: i32) -> &BackendSlotPool {
        assert!(
            (proc_number as u32) < self.num_backend_pools,
            "proc_number {} out of range (max {})",
            proc_number,
            self.num_backend_pools
        );
        unsafe {
            let base = (self as *const Self as *const u8).add(Self::backend_pools_offset())
                as *const BackendSlotPool;
            &*base.add(proc_number as usize)
        }
    }

    /// Initialize or attach to the shared memory control structure.
    pub fn init_or_attach(max_backends: usize) -> &'static mut Self {
        unsafe {
            let lock = rust_get_addin_shmem_init_lock();
            acquire_lwlock_exclusive(lock);

            let mut found: bool = false;
            let control = ShmemInitStruct(
                c"TikoIoControl".as_ptr() as _,
                Self::shmem_size(max_backends),
                &mut found,
            ) as *mut IoControl;

            if !found {
                (*control).init(max_backends);
            }

            release_lwlock(lock);

            IO_CONTROL.get_or_init(|| IoControlPtr(control));

            &mut *control
        }
    }

    pub fn get() -> &'static Self {
        IO_CONTROL
            .get()
            .map(|wrapper| unsafe { &*wrapper.0 })
            .expect("IoControl::get() called before init_or_attach()")
    }

    /// Check if shared memory has been initialized (i.e. init_or_attach has been called).
    pub fn is_initialized() -> bool {
        IO_CONTROL.get().is_some()
    }

    /// Check if Tiko worker is alive by sending signal 0 to its PID.
    /// Returns false if PID is 0 (not started/shut down) or process doesn't exist.
    pub fn is_worker_alive(&self) -> bool {
        let pid = self.worker_pid.load(Ordering::Acquire) as i32;
        if pid == 0 {
            return false;
        }
        // kill(pid, 0) checks existence without sending a signal
        unsafe { libc::kill(pid, 0) == 0 }
    }

    /// Poll the submit queue and dispatch requests to Tokio workers.
    ///
    /// Pops entries from the MPSC queue, looks up the corresponding slot,
    /// transitions Submitted → InProgress, validates, and dispatches.
    ///
    /// Returns the number of requests dispatched, or Err(()) on fatal error.
    pub fn poll_submit_queue<F>(&self, mut dispatch: F) -> Result<u64, ()>
    where
        F: FnMut(IoWorkRequest) -> Result<(), TrySendError<IoWorkRequest>>,
    {
        let mut dispatched_count = 0u64;
        let head = self.submit_queue.head.load(Ordering::Acquire);
        let mut tail = self.submit_queue.tail.load(Ordering::Relaxed);

        while tail != head {
            let idx = (tail as usize) % SUBMIT_QUEUE_SIZE;
            let entry = &self.submit_queue.entries[idx];
            let packed = entry.load(Ordering::Acquire);

            if packed == 0 {
                // Producer claimed position but hasn't written entry yet — stop here
                break;
            }

            let (backend_id, slot_idx) = SubmitQueue::unpack(packed);
            let pool = self.backend_pool(backend_id as i32);
            let slot = pool.slot(slot_idx as usize);

            // Transition Submitted → InProgress
            if !slot.try_start_processing() {
                pg_log_warning(&format!(
                    "tiko: slot {}/{} not in Submitted state (state={:?}), skipping",
                    backend_id,
                    slot_idx,
                    slot.current_state()
                ));
                // Clear entry and advance tail — skip this invalid entry
                entry.store(0, Ordering::Relaxed);
                tail = tail.wrapping_add(1);
                continue;
            }

            // Validate slot data
            if let Err(error_code) = slot.validate() {
                pg_log_warning(&format!(
                    "tiko: invalid slot data at backend={} slot={} (error={})",
                    backend_id, slot_idx, error_code
                ));
                slot.fail_with_error(error_code);
                entry.store(0, Ordering::Relaxed);
                tail = tail.wrapping_add(1);
                continue;
            }

            // Dispatch to Tokio — snapshot the generation so the completion path
            // can detect if the slot was recycled by a new backend.
            let request = IoWorkRequest {
                backend_id,
                slot_index: slot_idx,
                generation: slot.generation.load(Ordering::Relaxed),
            };

            match dispatch(request) {
                Ok(()) => {
                    dispatched_count += 1;
                    pg_log_debug3(&format!(
                        "tiko: dispatched backend={} slot={} op={:?} blk={} nblk={}",
                        backend_id, slot_idx, slot.op, slot.block_number, slot.nblocks
                    ));
                    // Clear entry and advance tail on success
                    entry.store(0, Ordering::Relaxed);
                    tail = tail.wrapping_add(1);
                }
                Err(TrySendError::Full(_)) => {
                    // Channel full — slot is InProgress but we can't dispatch yet.
                    // Revert to Submitted, leave entry in place for next poll.
                    slot.state
                        .store(SlotState::Submitted as u8, Ordering::Release);
                    pg_log_debug1(&format!(
                        "tiko: dispatcher full, reverted backend={} slot={}",
                        backend_id, slot_idx
                    ));
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    // Fatal — fail the slot so the backend doesn't hang forever
                    pg_log_warning(&format!(
                        "tiko: dispatcher disconnected, failing backend={} slot={}",
                        backend_id, slot_idx
                    ));
                    slot.fail_with_error(libc::EIO as u32);
                    entry.store(0, Ordering::Relaxed);
                    tail = tail.wrapping_add(1);
                    self.submit_queue.tail.store(tail, Ordering::Release);
                    return Err(());
                }
            }
        }

        // Update tail
        self.submit_queue.tail.store(tail, Ordering::Release);

        Ok(dispatched_count)
    }
}
