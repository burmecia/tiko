//! Per-backend slot pool + MPSC submit queue for I/O requests
//!
//! This module replaces the earlier 8-queue ring buffer design with a simpler architecture:
//!
//! # Architecture
//!
//! - **Per-backend slot pools**: Each backend owns a small pool of I/O slots (4 slots).
//!   Claiming is a local bit-scan — zero contention, no CAS races.
//! - **MPSC submit queue**: Backends push `(backend_id, slot_idx)` entries through
//!   a bounded per-cell-sequence ring (Vyukov); the worker pops and dispatches to
//!   Tokio. Strict ordering, no advisory hints.
//! - **SetLatch completion**: Tokio workers call `SetLatch` directly on the backend's latch
//!   after marking a slot Completed. No harvest step, no main-thread scan.
//!
//! # Slot State Machine
//!
//! `Free → Filling → Submitted → InProgress → Completing → Completed → Free`
//!
//! | Transition | Who | Mechanism |
//! |---|---|---|
//! | Free → Filling | Backend | Claim from own pool (bit clear) |
//! | Filling → Submitted | Backend | `slot.publish()` (gen-preserving Release store) |
//! | Submitted → InProgress | Tiko worker | `slot.try_start_processing()` (CAS, returns gen) |
//! | InProgress → Completing | Tokio | `slot.try_start_completing(gen)` (CAS on packed word) |
//! | Completing → Completed | Tokio | `slot.finish_completing(gen)` (CAS on packed word) |
//! | Completed → Free | Backend | `slot.release()` + `pool.release()` |
//!
//! # Stale-completion guard
//!
//! State and a per-slot generation counter live in one atomic word
//! (`state_gen`: 29 bits generation, 3 bits state). `attach()` bumps the
//! generation and resets the state with a single store; every Tokio-side
//! transition CASes the whole word, so generation check and state transition
//! are one atomic step and a stale completer can never match a recycled slot
//! (even if the new request reached InProgress again — the classic ABA case).
//!
//! # Memory Ordering
//!
//! - `publish()`: Release — ensures request fields visible before Submitted
//! - `try_start_processing()`: Acquire on success — sees request data, and
//!   captures the generation atomically with the transition
//! - `try_start_completing()`: AcqRel on success — pins the slot; only the
//!   pin winner may write result fields
//! - `finish_completing()`: Release — ensures result fields visible before Completed
//! - `current_state()`: Acquire — sees result data after Completed
//! - `SubmitQueue.head` CAS: Relaxed — confers cell ownership only
//! - `SubmitQueue.seqs[]`: producer's Release store of `pos + 1` publishes its
//!   entry store; consumer's Acquire load of it reads the entry. Consumer's
//!   Release store of `pos + SUBMIT_QUEUE_SIZE` frees the cell for the next lap.
//!
//! # Shared Memory Layout
//!
//! ```text
//! IoControl (fixed size)
//! ├── num_backend_pools, worker_pid, worker_latch
//! ├── submit_queue (SubmitQueue)
//! ├── stats (IoStats)
//! ├── cache (CacheControl)
//! └── timeline (TimelineState)        ← active window + base/head/redo + live-interval draft buffer
//! BackendSlotPool[0]  ← immediately after IoControl (aligned)
//! BackendSlotPool[1]
//! ...
//! BackendSlotPool[MaxBackends-1]
//! ChunkSlot[0..1024]          ← cache chunk slot metadata (~36 KB)
//! AtomicU32[0..2048]          ← cache bucket heads (~8 KB)
//! AtomicRWLock[0..2048]       ← cache bucket locks (~16 KB, one per bucket)
//! AtomicRWLock[0..1024]       ← per-slot I/O locks (~8 KB, one per chunk slot)
//! MetaSlot[0..1024]           ← fork metadata table (~28 KB)
//! AtomicU32[0..2048]          ← fork meta bucket heads (~8 KB)
//! AtomicRWLock[0..2048]       ← fork meta bucket locks (~16 KB)
//! AtomicRWLock[0..1024]       ← per-slot I/O locks (~8 KB, one per meta slot)
//! ```

use std::mem::{align_of, size_of};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU32, AtomicU64, Ordering};
use tokio::sync::mpsc::error::TrySendError;

use super::stats::IoStats;
use crate::cache::{
    CHUNK_NUM_BUCKETS, CHUNK_NUM_SLOTS, CacheControl, ChunkSlot, META_NUM_BUCKETS, META_NUM_SLOTS,
    MetaSlot,
};
use crate::error::{Error, Result};
use crate::timeline::TimelineState;
use crate::utils::rw_lock::AtomicRWLock;
use pgsys::{
    common::{BlockNumber, ForkNumber, Oid, RelFileNumber, is_under_postmaster},
    latch::{Latch, SetLatch},
    logging::*,
    lwlock::*,
    shmem::{ShmemInitStruct, rust_get_addin_shmem_init_lock},
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
    Completing = 5,
}

impl From<u8> for SlotState {
    fn from(val: u8) -> Self {
        match val {
            0 => SlotState::Free,
            1 => SlotState::Filling,
            2 => SlotState::Submitted,
            3 => SlotState::InProgress,
            4 => SlotState::Completed,
            5 => SlotState::Completing,
            _ => SlotState::Free,
        }
    }
}

/// `state_gen` packs a 29-bit generation counter and the 3-bit slot state into
/// one atomic word so "check generation" + "transition state" is a single CAS.
const SLOT_STATE_BITS: u32 = 3;
const SLOT_STATE_MASK: u32 = (1 << SLOT_STATE_BITS) - 1;
const SLOT_GENERATION_MASK: u32 = u32::MAX >> SLOT_STATE_BITS;

fn pack_state_gen(generation: u32, state: SlotState) -> u32 {
    ((generation & SLOT_GENERATION_MASK) << SLOT_STATE_BITS) | state as u32
}

fn unpack_generation(packed: u32) -> u32 {
    (packed >> SLOT_STATE_BITS) & SLOT_GENERATION_MASK
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
/// call `relfork::ops` directly in the backend process — they do **not** use the
/// pipeline, because their buffers may be in backend-local memory (e.g.
/// `PageSetChecksumCopy` palloc'd pages, `LocalBufferBlockPointers`, stack-local
/// `PGIOAlignedBlock`) which Tiko worker cannot access.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOpKind {
    Invalid = 0,
    Read = 1,     // AIO pipeline only
    Write = 2,    // AIO pipeline only
    Prefetch = 3, // pipeline only (cache warming, no buffer_ptr)
}

/// Work request sent from worker main thread to Tokio workers.
///
/// A full snapshot of the slot, taken by the dispatcher as it transitions
/// Submitted → InProgress: the Tokio task performs I/O from this snapshot only
/// and never re-reads slot fields, so a recycled slot can't redirect in-flight
/// I/O. `generation` is captured atomically with that transition and guards
/// the completion path against stale completions after backend slot recycle.
#[derive(Debug, Clone)]
pub struct IoWorkRequest {
    pub backend_id: u32,
    pub slot_index: u8,
    pub generation: u32,
    pub op: IoOpKind,
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
    pub fork_number: ForkNumber,
    pub block_number: BlockNumber,
    pub nblocks: BlockNumber,
    pub buffer_ptr: u64,
}

impl IoWorkRequest {
    /// Validate the request snapshot before dispatching to Tokio.
    pub fn validate(&self) -> std::result::Result<(), i32> {
        if self.op == IoOpKind::Invalid {
            return Err(libc::EINVAL);
        }

        // Only Read and Write operations require a buffer
        let needs_buffer = matches!(self.op, IoOpKind::Read | IoOpKind::Write);
        if needs_buffer && self.buffer_ptr == 0 {
            return Err(libc::EFAULT);
        }

        // Only operations that transfer blocks need nblocks validation
        let needs_nblocks = matches!(
            self.op,
            IoOpKind::Read | IoOpKind::Write | IoOpKind::Prefetch
        );
        if needs_nblocks && (self.nblocks == 0 || self.nblocks > 1024) {
            return Err(libc::EINVAL);
        }

        Ok(())
    }
}

impl From<TrySendError<IoWorkRequest>> for Error {
    fn from(e: TrySendError<IoWorkRequest>) -> Self {
        Error::TrySendError(e)
    }
}

// ── IoSlot ──

/// A single I/O request slot in shared memory.
///
/// Size: 64 bytes. No ConditionVariable — completion uses SetLatch via `owner_latch`.
/// `state_gen` packs the slot state and a per-slot generation counter into one
/// atomic word, preventing stale Tokio completions from corrupting recycled slots
/// (e.g. when a backend dies with InProgress slots and a new backend reuses the
/// ProcNumber): every worker-side transition CASes generation and state together.
#[repr(C, align(64))]
pub struct IoSlot {
    // ── Slot lifecycle ──
    /// Packed `(generation << 3) | state`. `BackendSlotPool::attach()` bumps the
    /// generation and resets the state with a single store; Tokio-side transitions
    /// CAS the whole word, so a stale completion can never match a recycled slot.
    pub state_gen: AtomicU32,
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

    /// Backend's MyLatch as u64. Tokio calls SetLatch(owner_latch) directly.
    pub owner_latch: AtomicU64,

    // ── Data transfer ──
    /// Pointer into shared_buffers where the block data lives.
    pub buffer_ptr: AtomicU64,

    // ── Result ──
    /// Written only by the completer holding the (generation, Completing) pin.
    pub result_status: AtomicI32,
    pub result_nblocks: AtomicU32,
    // No _reserved needed: padding after `op` fills the 64 bytes exactly.
}

const _: () = assert!(size_of::<IoSlot>() == 64, "IoSlot must be exactly 64 bytes");

impl IoSlot {
    fn init(&mut self) {
        self.state_gen
            .store(pack_state_gen(0, SlotState::Free), Ordering::Relaxed);
        self.op = IoOpKind::Invalid;
        self.owner_proc.store(-1, Ordering::Relaxed);
        self.owner_latch.store(0, Ordering::Relaxed);
        self.buffer_ptr.store(0, Ordering::Relaxed);
        self.result_status.store(0, Ordering::Relaxed);
        self.result_nblocks.store(0, Ordering::Relaxed);
    }

    pub fn current_state(&self) -> SlotState {
        SlotState::from((self.state_gen.load(Ordering::Acquire) & SLOT_STATE_MASK) as u8)
    }

    pub fn generation(&self) -> u32 {
        unpack_generation(self.state_gen.load(Ordering::Acquire))
    }

    /// True iff the slot is currently in `state` at `generation`. Used by Tokio
    /// tasks to revalidate before touching buffers: a task scheduled late (slot
    /// already recycled) must not perform I/O from its stale snapshot.
    pub fn is_state(&self, generation: u32, state: SlotState) -> bool {
        self.state_gen.load(Ordering::Acquire) == pack_state_gen(generation, state)
    }

    /// Publish the request (Filling → Submitted), preserving the generation.
    /// Release fence ensures all request fields are visible before state change.
    /// Called exclusively by the owning backend.
    pub fn publish(&self) {
        let generation = self.generation();
        self.state_gen.store(
            pack_state_gen(generation, SlotState::Submitted),
            Ordering::Release,
        );
    }

    /// Try to start processing (Submitted → InProgress).
    /// Called by Tiko worker after popping from the submit queue (sole caller).
    /// Returns the generation captured atomically with the transition.
    pub fn try_start_processing(&self) -> Option<u32> {
        let mut cur = self.state_gen.load(Ordering::Acquire);
        loop {
            if SlotState::from((cur & SLOT_STATE_MASK) as u8) != SlotState::Submitted {
                return None;
            }
            let generation = unpack_generation(cur);
            match self.state_gen.compare_exchange_weak(
                cur,
                pack_state_gen(generation, SlotState::InProgress),
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(generation),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Pin the slot for completion (InProgress → Completing).
    ///
    /// CAS on the packed word: generation check and state transition are one
    /// atomic step, so a stale completer (slot recycled by `attach()` and
    /// redispatched for a new backend) can never match. Only the pin winner
    /// may write the result fields.
    pub fn try_start_completing(&self, generation: u32) -> bool {
        self.state_gen
            .compare_exchange(
                pack_state_gen(generation, SlotState::InProgress),
                pack_state_gen(generation, SlotState::Completing),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Finish completion (Completing → Completed). Release publishes the result
    /// fields written while pinned. Returns false if the slot was recycled
    /// while pinned — the caller must discard the result and skip `SetLatch`.
    pub fn finish_completing(&self, generation: u32) -> bool {
        self.state_gen
            .compare_exchange(
                pack_state_gen(generation, SlotState::Completing),
                pack_state_gen(generation, SlotState::Completed),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Revert InProgress → Submitted (dispatch channel full; the queue entry
    /// stays in place for the next poll). CAS, not a store: a slot recycled
    /// meanwhile must not be resurrected at a stale generation.
    pub fn revert_to_submitted(&self, generation: u32) -> bool {
        self.state_gen
            .compare_exchange(
                pack_state_gen(generation, SlotState::InProgress),
                pack_state_gen(generation, SlotState::Submitted),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Release slot (Completed → Free), preserving the generation.
    /// Called by the backend after reading the result.
    pub fn release(&self) {
        let generation = self.generation();
        self.state_gen.store(
            pack_state_gen(generation, SlotState::Free),
            Ordering::Release,
        );
    }

    /// Fail slot with an error and wake the backend via SetLatch.
    /// Returns false if the slot was no longer (generation, InProgress)
    /// (recycled — result discarded).
    pub fn fail_with_error(&self, generation: u32, error_code: i32) -> bool {
        self.result_status.store(error_code, Ordering::Relaxed);
        let completed = self
            .state_gen
            .compare_exchange(
                pack_state_gen(generation, SlotState::InProgress),
                pack_state_gen(generation, SlotState::Completed),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok();
        if completed {
            // Wake the backend directly
            let latch = self.owner_latch.load(Ordering::Acquire) as *mut Latch;
            if !latch.is_null() {
                unsafe {
                    SetLatch(latch);
                }
            }
        }
        completed
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
const _: () = assert!(
    std::mem::offset_of!(SubmitQueue, seqs) == 128 + SUBMIT_QUEUE_SIZE * size_of::<AtomicU32>(),
    "seqs must immediately follow entries"
);

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
    /// Bumps each slot's generation and resets it to Free with a single store to
    /// the packed word. This handles ProcNumber recycling: if a previous backend
    /// crashed with slots in InProgress state, in-flight Tokio work for the old
    /// backend can no longer match any CAS on this slot, so stale results are
    /// silently discarded instead of corrupting the new backend's requests.
    pub fn attach(&self) {
        for slot in &self.slots {
            let generation = slot.generation().wrapping_add(1) & SLOT_GENERATION_MASK;
            slot.state_gen.store(
                pack_state_gen(generation, SlotState::Free),
                Ordering::Relaxed,
            );
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

/// Bounded MPSC ring buffer for I/O submission (Vyukov per-cell-sequence design).
///
/// Backends push, Tiko worker pops. Each cell carries a sequence phase: the
/// cell for ticket `pos` is writable when `seqs[idx] == pos`, published
/// (readable) when `seqs[idx] == pos + 1`, and freed for the next lap when
/// the consumer stores `pos + SUBMIT_QUEUE_SIZE`. A producer validates the
/// cell *before* claiming its ticket via CAS on `head`, so a ticket beyond
/// capacity is never issued and an entry can never be written over an
/// unconsumed one (the race the earlier check-then-`fetch_add` version had).
///
/// Entries are packed as `[backend_id (30 bits) | slot_idx (2 bits)]`.
#[repr(C, align(128))]
pub struct SubmitQueue {
    /// Producer head — next ticket to hand out; CAS-claimed after cell validation
    head: AtomicU32,
    _pad_head: [u8; 60],

    /// Consumer tail — next ticket to consume. Only the Tiko worker touches it.
    tail: AtomicU32,
    _pad_tail: [u8; 60],

    /// Packed entry per cell. Valid only while published (`seqs[idx] == pos + 1`);
    /// never cleared — the sequence counter gates all reads.
    entries: [AtomicU32; SUBMIT_QUEUE_SIZE],

    /// Per-cell phase counter. Init: `seqs[i] = i`.
    seqs: [AtomicU32; SUBMIT_QUEUE_SIZE],
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
        for (i, seq) in self.seqs.iter().enumerate() {
            seq.store(i as u32, Ordering::Relaxed);
        }
    }

    /// Pack a (backend_id, slot_idx) pair into a u32 submit entry.
    ///
    /// Layout: `[backend_id (30 bits) | slot_idx (2 bits)]`
    fn pack(backend_id: u32, slot_idx: u8) -> u32 {
        debug_assert!((slot_idx as u32) < SLOTS_PER_BACKEND as u32);
        (backend_id << SLOT_IDX_BITS) | slot_idx as u32
    }

    /// Unpack a u32 entry back to (backend_id, slot_idx).
    fn unpack(val: u32) -> (u32, u8) {
        (val >> SLOT_IDX_BITS, (val & SLOT_IDX_MASK) as u8)
    }

    /// Push a submission entry. Called by backends after slot.publish().
    ///
    /// Vyukov bounded-queue producer: the target cell's sequence is validated
    /// *before* the ticket is claimed via CAS on `head`, so no ticket beyond
    /// capacity ever exists and no entry can overwrite an unconsumed one.
    /// The entry store is published by the Release store of `seqs[idx] = pos + 1`.
    ///
    /// Returns false if the queue is full (backpressure).
    pub fn push(&self, backend_id: u32, slot_idx: u8) -> bool {
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let idx = (pos as usize) % SUBMIT_QUEUE_SIZE;
            let seq = self.seqs[idx].load(Ordering::Acquire);
            let dif = seq.wrapping_sub(pos) as i32;
            if dif == 0 {
                // Cell confirmed writable for this ticket — claim it now
                match self.head.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.entries[idx]
                            .store(Self::pack(backend_id, slot_idx), Ordering::Release);
                        self.seqs[idx].store(pos.wrapping_add(1), Ordering::Release);
                        return true;
                    }
                    Err(actual) => pos = actual,
                }
            } else if dif < 0 {
                return false; // full: cell still holds an unconsumed entry
            } else {
                // Another producer raced ahead — re-evaluate at the new ticket
                pos = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// Read the entry published for ticket `tail`, if any.
    ///
    /// Single consumer. Returns `(pos, packed)` without freeing the cell — the
    /// caller processes the entry and then calls `free_cell(pos)`, or leaves
    /// the cell published (e.g. on backpressure) and retries the same entry
    /// on the next poll. The producer's Release store of `seqs[idx] = pos + 1`
    /// publishes its entry store.
    fn pop(&self) -> Option<(u32, u32)> {
        let pos = self.tail.load(Ordering::Relaxed);
        let idx = (pos as usize) % SUBMIT_QUEUE_SIZE;
        if self.seqs[idx].load(Ordering::Acquire) != pos.wrapping_add(1) {
            return None; // empty for this ticket, or producer hasn't published yet
        }
        Some((pos, self.entries[idx].load(Ordering::Relaxed)))
    }

    /// Free a consumed cell for its next lap and advance `tail`.
    ///
    /// Single consumer. The Release store of the sequence makes the cell
    /// writable by the next lap's producer; `tail` advances under Relaxed
    /// (only the consumer reads it for order, producers only use it for the
    /// fullness check).
    fn free_cell(&self, pos: u32) {
        let idx = (pos as usize) % SUBMIT_QUEUE_SIZE;
        self.tail.store(pos.wrapping_add(1), Ordering::Relaxed);
        self.seqs[idx].store(
            pos.wrapping_add(SUBMIT_QUEUE_SIZE as u32),
            Ordering::Release,
        );
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
    pub(crate) cache: CacheControl,

    /// I/O statistics
    pub stats: IoStats,

    /// Consolidated timeline state: active window + base/head/redo
    /// checkpoints + generation counter + live-interval [`DraftBuffer`],
    /// all under a shared/exclusive RWLock. See [`TimelineState`] for the
    /// fencing model between record/drain and `head_ckpt` advances.
    pub timeline: TimelineState,
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

        // Initialize cache control + all trailing arrays in shared memory
        unsafe {
            let base = self as *mut Self as *mut u8;
            let chunk_slots = base.add(Self::chunk_slots_offset(max_backends)) as *mut ChunkSlot;
            let chunk_buckets =
                base.add(Self::chunk_buckets_offset(max_backends)) as *mut AtomicU32;
            let chunk_bucket_locks =
                base.add(Self::chunk_bucket_locks_offset(max_backends)) as *mut AtomicRWLock;
            let chunk_io_locks =
                base.add(Self::chunk_io_locks_offset(max_backends)) as *mut AtomicRWLock;
            let meta_slots = base.add(Self::meta_slots_offset(max_backends)) as *mut MetaSlot;
            let meta_buckets = base.add(Self::meta_buckets_offset(max_backends)) as *mut AtomicU32;
            let meta_locks = base.add(Self::meta_locks_offset(max_backends)) as *mut AtomicRWLock;
            let meta_io_locks =
                base.add(Self::meta_io_locks_offset(max_backends)) as *mut AtomicRWLock;
            self.cache.init(
                chunk_slots,
                chunk_buckets,
                chunk_bucket_locks,
                chunk_io_locks,
                meta_slots,
                meta_buckets,
                meta_locks,
                meta_io_locks,
            );
        }

        self.stats.init();
        self.timeline.init();
    }

    /// Byte offset from the start of IoControl to the first BackendSlotPool.
    /// Accounts for alignment requirements of BackendSlotPool.
    fn backend_pools_offset() -> usize {
        let base = size_of::<Self>();
        let align = align_of::<BackendSlotPool>();
        (base + align - 1) & !(align - 1)
    }

    /// Byte offset to the slot metadata array (after backend pools).
    fn chunk_slots_offset(max_backends: usize) -> usize {
        let after_pools =
            Self::backend_pools_offset() + max_backends * size_of::<BackendSlotPool>();
        let align = align_of::<ChunkSlot>();
        (after_pools + align - 1) & !(align - 1)
    }

    /// Byte offset to the hash entries array (after slot metadata).
    fn chunk_buckets_offset(max_backends: usize) -> usize {
        let after_slots = Self::chunk_slots_offset(max_backends)
            + CHUNK_NUM_SLOTS as usize * size_of::<ChunkSlot>();
        let align = align_of::<AtomicU32>();
        (after_slots + align - 1) & !(align - 1)
    }

    /// Byte offset to the partition locks array (after bucket heads).
    fn chunk_bucket_locks_offset(max_backends: usize) -> usize {
        let after_hash = Self::chunk_buckets_offset(max_backends)
            + CHUNK_NUM_BUCKETS as usize * size_of::<AtomicU32>();
        let align = align_of::<AtomicRWLock>();
        (after_hash + align - 1) & !(align - 1)
    }

    /// Byte offset to the per-slot I/O locks array (after cache bucket locks).
    fn chunk_io_locks_offset(max_backends: usize) -> usize {
        let after_cache_locks = Self::chunk_bucket_locks_offset(max_backends)
            + CHUNK_NUM_BUCKETS as usize * size_of::<AtomicRWLock>();
        let align = align_of::<AtomicRWLock>();
        (after_cache_locks + align - 1) & !(align - 1)
    }

    /// Byte offset to the fork metadata entry pool (after per-slot I/O locks).
    fn meta_slots_offset(max_backends: usize) -> usize {
        let after_chunk_io_locks = Self::chunk_io_locks_offset(max_backends)
            + CHUNK_NUM_SLOTS as usize * size_of::<AtomicRWLock>();
        let align = align_of::<MetaSlot>();
        (after_chunk_io_locks + align - 1) & !(align - 1)
    }

    /// Byte offset to the fork metadata bucket heads array (after entry pool).
    fn meta_buckets_offset(max_backends: usize) -> usize {
        let after_entries =
            Self::meta_slots_offset(max_backends) + META_NUM_SLOTS as usize * size_of::<MetaSlot>();
        let align = align_of::<AtomicU32>();
        (after_entries + align - 1) & !(align - 1)
    }

    /// Byte offset to the fork metadata per-bucket locks array (after bucket heads).
    fn meta_locks_offset(max_backends: usize) -> usize {
        let after_buckets = Self::meta_buckets_offset(max_backends)
            + META_NUM_BUCKETS as usize * size_of::<AtomicU32>();
        let align = align_of::<AtomicRWLock>();
        (after_buckets + align - 1) & !(align - 1)
    }

    /// Byte offset to the fork metadata per-slot I/O locks array (after bucket locks).
    fn meta_io_locks_offset(max_backends: usize) -> usize {
        let after_locks = Self::meta_locks_offset(max_backends)
            + META_NUM_BUCKETS as usize * size_of::<AtomicRWLock>();
        let align = align_of::<AtomicRWLock>();
        (after_locks + align - 1) & !(align - 1)
    }

    /// Total shared memory size for the control structure + backend pools + all arrays.
    pub fn shmem_size(max_backends: usize) -> usize {
        // The last region is META per-slot I/O locks (one per meta slot).
        Self::meta_io_locks_offset(max_backends)
            + META_NUM_SLOTS as usize * size_of::<AtomicRWLock>()
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

    pub fn try_get() -> Option<&'static Self> {
        IO_CONTROL.get().map(|wrapper| unsafe { &*wrapper.0 })
    }

    pub fn get() -> &'static Self {
        Self::try_get().expect("IoControl::get() called before init_or_attach()")
    }

    pub fn get_cache() -> &'static CacheControl {
        &Self::get().cache
    }

    /// Check if shared memory has been initialized (i.e. init_or_attach has been called).
    pub fn is_initialized() -> bool {
        IO_CONTROL.get().is_some()
    }

    /// True when the shared-memory cache is reachable from this process.
    ///
    /// Requires both conditions:
    /// - `is_under_postmaster()` — false during initdb (`--boot`/`--single`) where
    ///   `MyProcNumber` is invalid and IoControl was never sized via
    ///   `shmem_request_hook`.
    /// - `IoControl::is_initialized()` — false if the shmem startup hook has not
    ///   yet run in this process (e.g. very early in backend startup).
    pub fn cache_is_available() -> bool {
        is_under_postmaster() && IoControl::is_initialized()
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
    pub fn poll_submit_queue<F>(&self, mut dispatch: F) -> Result<u64>
    where
        F: FnMut(IoWorkRequest) -> Result<()>,
    {
        let mut dispatched_count = 0u64;

        while let Some((pos, packed)) = self.submit_queue.pop() {
            let (backend_id, slot_idx) = SubmitQueue::unpack(packed);
            let pool = self.backend_pool(backend_id as i32);
            let slot = pool.slot(slot_idx as usize);

            // Transition Submitted → InProgress, capturing the generation
            // atomically with the state change.
            let Some(generation) = slot.try_start_processing() else {
                pg_log_warning(format!(
                    "tiko: slot {}/{} not in Submitted state (state={:?}), skipping",
                    backend_id,
                    slot_idx,
                    slot.current_state()
                ));
                // Free cell and advance tail — skip this invalid entry
                self.submit_queue.free_cell(pos);
                continue;
            };

            // Snapshot the request fields — the Tokio task performs I/O from
            // this snapshot only, never re-reading a possibly recycled slot.
            let request = IoWorkRequest {
                backend_id,
                slot_index: slot_idx,
                generation,
                op: slot.op,
                spc_oid: slot.spc_oid,
                db_oid: slot.db_oid,
                rel_number: slot.rel_number,
                fork_number: slot.fork_number,
                block_number: slot.block_number,
                nblocks: slot.nblocks,
                buffer_ptr: slot.buffer_ptr.load(Ordering::Acquire),
            };

            // Validate request data
            if let Err(error_code) = request.validate() {
                pg_log_warning(format!(
                    "tiko: invalid slot data at backend={} slot={} (error={})",
                    backend_id, slot_idx, error_code
                ));
                slot.fail_with_error(generation, error_code);
                self.submit_queue.free_cell(pos);
                continue;
            }

            match dispatch(request) {
                Ok(()) => {
                    dispatched_count += 1;
                    pg_log_debug3(format!(
                        "tiko: dispatched backend={} slot={} op={:?} blk={} nblk={}",
                        backend_id, slot_idx, slot.op, slot.block_number, slot.nblocks
                    ));
                    self.submit_queue.free_cell(pos);
                }
                Err(Error::TrySendError(TrySendError::Full(_))) => {
                    // Channel full — slot is InProgress but we can't dispatch yet.
                    // Revert to Submitted; the cell stays published (not freed) so
                    // the next poll re-reads this entry.
                    slot.revert_to_submitted(generation);
                    pg_log_debug1(format!(
                        "tiko: dispatcher full, reverted backend={} slot={}",
                        backend_id, slot_idx
                    ));
                    break;
                }
                Err(err) => {
                    // Fatal — fail the slot so the backend doesn't hang forever
                    pg_log_warning(format!(
                        "tiko: dispatcher failed, failing backend={} slot={}",
                        backend_id, slot_idx
                    ));
                    slot.fail_with_error(generation, libc::EIO);
                    self.submit_queue.free_cell(pos);
                    return Err(err);
                }
            }
        }

        Ok(dispatched_count)
    }
}

#[cfg(test)]
mod submit_queue_tests {
    use super::*;

    fn q() -> Box<SubmitQueue> {
        let q = Box::new(SubmitQueue {
            head: AtomicU32::new(0),
            _pad_head: [0; 60],
            tail: AtomicU32::new(0),
            _pad_tail: [0; 60],
            entries: std::array::from_fn(|_| AtomicU32::new(0)),
            seqs: std::array::from_fn(|i| AtomicU32::new(i as u32)),
        });
        q.init();
        q
    }

    #[test]
    fn fifo_full_and_wraparound() {
        let q = q();

        for i in 0..SUBMIT_QUEUE_SIZE as u32 {
            assert!(q.push(i, (i & 3) as u8));
        }
        assert!(!q.push(0, 0), "queue must report full");

        for i in 0..SUBMIT_QUEUE_SIZE as u32 {
            let (pos, packed) = q.pop().unwrap();
            assert_eq!(pos, i);
            assert_eq!(SubmitQueue::unpack(packed), (i, (i & 3) as u8));
            q.free_cell(pos);
        }
        assert!(q.pop().is_none());

        for _ in 0..3 {
            for i in 0..SUBMIT_QUEUE_SIZE as u32 {
                assert!(q.push(i, (i & 3) as u8));
            }
            assert!(!q.push(0, 0));
            for i in 0..SUBMIT_QUEUE_SIZE as u32 {
                let (pos, packed) = q.pop().unwrap();
                assert_eq!(SubmitQueue::unpack(packed), (i, (i & 3) as u8));
                q.free_cell(pos);
            }
        }
    }

    #[test]
    fn interleaved_push_pop_reuses_cells() {
        let q = q();
        let laps = 4u32;
        for lap in 0..laps {
            for i in 0..SUBMIT_QUEUE_SIZE as u32 {
                let expected = lap * SUBMIT_QUEUE_SIZE as u32 + i;
                assert!(q.push(i, (lap & 3) as u8));
                let (pos, packed) = q.pop().unwrap();
                assert_eq!(pos, expected, "FIFO ticket order across laps");
                assert_eq!(SubmitQueue::unpack(packed), (i, (lap & 3) as u8));
                q.free_cell(pos);
            }
        }
        assert!(q.pop().is_none());
        assert_eq!(
            q.head.load(Ordering::Relaxed),
            laps * SUBMIT_QUEUE_SIZE as u32
        );
        assert_eq!(
            q.tail.load(Ordering::Relaxed),
            laps * SUBMIT_QUEUE_SIZE as u32
        );
    }

    #[test]
    fn concurrent_producers_no_lost_entries() {
        let q = q();
        const PRODUCERS: u32 = 8;
        const PER_PRODUCER: u32 = 2000;
        const TOTAL: usize = (PRODUCERS * PER_PRODUCER) as usize;

        let counts: Vec<AtomicU32> = (0..PRODUCERS).map(|_| AtomicU32::new(0)).collect();

        std::thread::scope(|s| {
            for b in 0..PRODUCERS {
                let q = &q;
                s.spawn(move || {
                    for i in 0..PER_PRODUCER {
                        while !q.push(b, (i & 3) as u8) {
                            std::hint::spin_loop();
                        }
                    }
                });
            }

            let consumer = {
                let q = &q;
                let counts = &counts;
                s.spawn(move || {
                    let mut drained = 0usize;
                    while drained < TOTAL {
                        if let Some((pos, packed)) = q.pop() {
                            let (b, _) = SubmitQueue::unpack(packed);
                            counts[b as usize].fetch_add(1, Ordering::Relaxed);
                            q.free_cell(pos);
                            drained += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
            };
            consumer.join().unwrap();
        });

        for (b, c) in counts.iter().enumerate() {
            assert_eq!(
                c.load(Ordering::Relaxed),
                PER_PRODUCER,
                "backend {b} lost or duplicated entries"
            );
        }
        assert_eq!(q.head.load(Ordering::Relaxed), TOTAL as u32);
        assert_eq!(q.tail.load(Ordering::Relaxed), TOTAL as u32);
    }
}
