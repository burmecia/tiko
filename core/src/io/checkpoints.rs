//! Versioned checkpoint history for express-bucket read path.
//!
//! The express bucket is versioned: each checkpoint writes to a new folder
//! `{org}/{db}/chunks/{tl}/{lsn}`. The read path must scan from newest to
//! oldest checkpoint to find the latest version of any chunk or meta object.
//!
//! # Shared memory layout
//!
//! `CkptHistory` lives inside `IoControl` in PostgreSQL shared memory,
//! so all backends and s3worker see the same list without copying on every read.
//!
//! # Concurrency
//!
//! - **Single writer**: only s3worker calls `push()`, at each checkpoint commit.
//! - **Multiple readers**: any backend calls `snapshot()` to copy the current
//!   list into its process-local `LocalHistoryCache`.
//! - **Locking**: `AtomicRWLock` (shared-memory-safe spin RW lock). Readers hold
//!   the lock only for the duration of the memcpy (~4 KB). The lock is never held
//!   across any S3 I/O.
//!
//! # Per-backend caching
//!
//! `LocalHistoryCache` stores a private copy of the list keyed by a `generation`
//! counter. On each read the backend loads `generation` with `Acquire` ordering;
//! if it matches the local copy the copy is reused without acquiring any lock.
//! A refresh (lock + memcpy) only happens after a checkpoint, i.e. roughly once
//! every 5 minutes.
//!
//! # Ring-buffer layout
//!
//! `versions` is a power-of-2 ring. `head` is the index where the **next**
//! entry will be written. After a push, the newest entry sits at
//! `(head - 1) & MASK`. Iteration goes backward from `head - 1` for `count`
//! steps, wrapping with `& MASK`, giving newest-first order with O(1) push.
//!
//! # Buffer-full policy
//!
//! When `count == MAX_CHECKPOINT_VERSIONS` the oldest slot is overwritten
//! automatically (head advances past it). s3worker should trigger a **rebase**
//! at `REBASE_THRESHOLD` entries to incorporate deltas into the base manifest
//! before the buffer fills, keeping eviction as a safety valve only.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::rwlock::AtomicRWLock;
use pgsys::lsn::Lsn;

// ── Constants ────────────────────────────────────────────────────────────────

/// Maximum number of checkpoint versions kept in the ring. Must be a power of 2.
/// 256 × 16 bytes = 4 KB in shared memory; same in each backend's local cache.
const MAX_CHECKPOINT_VERSIONS: usize = 256;
const _: () = assert!(
    MAX_CHECKPOINT_VERSIONS.is_power_of_two(),
    "MAX_CHECKPOINT_VERSIONS must be a power of two"
);

/// Trigger a base-manifest rebase when the history reaches this many entries
/// (80% of capacity). Keeps the eviction path as a safety valve only.
//const REBASE_THRESHOLD: usize = MAX_CHECKPOINT_VERSIONS * 4 / 5; // 204

// ── CkptVersion ────────────────────────────────────────────────────────

/// One entry in the checkpoint version list.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CkptVersion {
    pub timeline_id: u32,
    _pad: [u8; 4],
    pub lsn: Lsn,
}

const _: () = assert!(std::mem::size_of::<CkptVersion>() == 16);

impl CkptVersion {
    fn new(timeline_id: u32, lsn: Lsn) -> Self {
        Self {
            timeline_id,
            _pad: [0; 4],
            lsn,
        }
    }
}

impl Default for CkptVersion {
    fn default() -> Self {
        Self {
            timeline_id: 1,
            _pad: [0; 4],
            lsn: Lsn::default(),
        }
    }
}

impl std::fmt::Display for CkptVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tl {} @ {}", self.timeline_id, self.lsn)
    }
}

// ── CkptHistory ────────────────────────────────────────────────────────

/// Versioned checkpoint list in shared memory, stored as a ring buffer.
///
/// `head` points to the slot where the **next** entry will be written.
/// The newest entry is always at `(head - 1) % MAX_CHECKPOINT_VERSIONS`.
/// Iterating `count` steps backward from `head - 1` (wrapping) gives
/// newest-first order. Push is O(1): no shifting required.
///
/// `generation` is bumped on every `push()`. Backends compare their cached
/// generation against this value to skip lock acquisition when nothing changed.
#[repr(C)]
pub(crate) struct CkptHistory {
    lock: AtomicRWLock,
    /// Monotonically increasing. Exposed solely for the lock-free staleness
    /// check in `LocalHistoryCache`.
    pub generation: AtomicU64,
    /// Index of the next write slot (0..MAX_CHECKPOINT_VERSIONS).
    head: AtomicU32,
    /// Number of valid entries (0..=MAX_CHECKPOINT_VERSIONS).
    count: AtomicU32,
    pub versions: [CkptVersion; MAX_CHECKPOINT_VERSIONS],
}

impl CkptHistory {
    /// Initialise all fields to zero / empty.
    pub fn init(&self) {
        self.lock.init();
        self.generation.store(0, Ordering::Relaxed);
        self.head.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }

    /// Append a new checkpoint version. O(1) — no shifting.
    ///
    /// Called by s3worker at each checkpoint commit. When the ring is full the
    /// oldest entry is overwritten automatically. Callers should trigger a
    /// rebase at `REBASE_THRESHOLD` to avoid reaching this path.
    pub fn push(&self, timeline_id: u32, lsn: Lsn) {
        let _guard = self.lock.write();

        let head = self.head.load(Ordering::Relaxed) as usize;
        debug_assert!(head < MAX_CHECKPOINT_VERSIONS);
        // SAFETY: head is always in 0..MAX_CHECKPOINT_VERSIONS.
        unsafe {
            let ptr = self.versions.as_ptr() as *mut CkptVersion;
            ptr.add(head).write(CkptVersion::new(timeline_id, lsn));
        }

        let new_head = (head + 1) % MAX_CHECKPOINT_VERSIONS;
        self.head.store(new_head as u32, Ordering::Relaxed);

        let _ = self
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if (c as usize) < MAX_CHECKPOINT_VERSIONS {
                    Some(c + 1)
                } else {
                    None // count stays at MAX; oldest slot silently overwritten.
                }
            });

        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Return a snapshot of the current list, **newest-first**.
    ///
    /// Acquires the read lock only for the duration of the copy.
    pub fn snapshot(&self) -> CkptHistSnapshot {
        let _guard = self.lock.read();

        let head = self.head.load(Ordering::Relaxed) as usize;
        let count = self.count.load(Ordering::Relaxed) as usize;
        let generation = self.generation.load(Ordering::Relaxed);

        let mut versions = Vec::with_capacity(count);
        for i in 0..count {
            // Walk backward from head: newest entry is at (head - 1) % MAX.
            let slot = (head + MAX_CHECKPOINT_VERSIONS - 1 - i) % MAX_CHECKPOINT_VERSIONS;
            versions.push(self.versions[slot]);
        }
        CkptHistSnapshot {
            generation,
            versions,
        }
    }

    // Returns true when `REBASE_THRESHOLD` has been reached.
    // pub fn needs_rebase(&self) -> bool {
    //     self.count.load(Ordering::Relaxed) as usize >= REBASE_THRESHOLD
    // }
}

// ── Local Checkpoint History Cache ────────────────────────────────────────────────────────

/// Per-backend (process-local) snapshot of the checkpoint version list.
///
/// Refreshed lazily: a single `AtomicU64` load detects staleness; the full
/// lock + copy only runs after a checkpoint (~every 5 min).
#[derive(Debug, Default)]
pub(crate) struct CkptHistSnapshot {
    /// Generation at which `versions` was captured.
    generation: u64,
    /// Private copy of the version list, newest-first.
    pub versions: Vec<CkptVersion>,
}

impl CkptHistSnapshot {
    /// Return an up-to-date slice of checkpoint versions, refreshing from
    /// shared memory if the generation has advanced.
    pub fn get_or_refresh<'a>(&'a mut self, shared_ckpt_hist: &CkptHistory) -> &'a [CkptVersion] {
        let shared_gen = shared_ckpt_hist.generation.load(Ordering::Acquire);
        if self.generation != shared_gen {
            *self = shared_ckpt_hist.snapshot();
        }
        &self.versions
    }
}
