//! Basebackup→worker compaction coordination through shared memory.
//!
//! The tikoworker compactor is the sole routine compaction executor. A
//! basebackup checkpointer needing a base manifest at its checkpoint LSN
//! publishes a request and waits on its latch; the worker's compactor task
//! runs `run_compaction_through` and completes the request. No mutual
//! exclusion is needed between the two: only the worker ever compacts.
//! Escape hatches (worker dead / request timed out) fall back to running
//! compaction in the checkpointer, which is safe because `run_compaction*`
//! re-checks `base_ckpt` under the write lock and discards raced runs.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use pgsys::{lsn::Lsn, timeline_id::TimelineId};

use super::segment::Checkpoint;

/// Compaction completed successfully.
pub const COMPACTION_STATUS_OK: u32 = 0;
/// The worker-side compaction run returned an error.
pub const COMPACTION_STATUS_ERROR: u32 = 1;

/// A pending compaction request observed by the worker.
#[derive(Clone, Copy)]
pub struct PendingCompaction {
    /// Generation to pass back to [`CompactionRequest::complete`].
    pub generation: u64,
    /// Checkpoint to compact through (inclusive).
    pub target: Checkpoint,
    /// Requester's latch (pointer value) to SetLatch on completion.
    pub requester_latch: u64,
}

/// Single-slot basebackup→worker compaction request in shared memory.
///
/// The tikoworker compactor is the sole routine compactor; a basebackup
/// checkpointer that needs a base manifest at its checkpoint LSN publishes a
/// request here and waits on its own latch instead of running compaction
/// itself. Publication uses generation counters: the requester writes the
/// payload with `Relaxed` stores and publishes with a `Release` bump of
/// `request_gen`; the worker pairs an `Acquire` load of `request_gen` with
/// `Relaxed` payload reads and completes with `status` (`Relaxed`) followed
/// by a `Release` store to `done_gen`.
///
/// Invariant: at most one request is outstanding. Guaranteed structurally —
/// `CreateCheckPoint` is serialised by PG's CheckpointLock and the requester
/// waits for completion before returning.
#[repr(C)]
pub struct CompactionRequest {
    /// Bumped by the requester to publish a request (payload written first).
    request_gen: AtomicU64,
    /// Set to the completed generation by the worker (payload `status` first).
    done_gen: AtomicU64,
    /// Compact through this checkpoint (inclusive).
    target_timeline: AtomicU32,
    target_lsn: AtomicU64,
    /// Requester's latch as a pointer value; the worker SetLatches it on
    /// completion. A spurious set on a recycled latch is harmless.
    requester_latch: AtomicU64,
    /// `COMPACTION_STATUS_*`; valid once `done_gen` reaches the request gen.
    status: AtomicU32,
}

impl CompactionRequest {
    pub(crate) fn init(&self) {
        self.request_gen.store(0, Ordering::Relaxed);
        self.done_gen.store(0, Ordering::Relaxed);
        self.target_timeline.store(0, Ordering::Relaxed);
        self.target_lsn.store(0, Ordering::Relaxed);
        self.requester_latch.store(0, Ordering::Relaxed);
        self.status.store(0, Ordering::Relaxed);
    }

    /// Publish a request to compact through `target`. Returns the generation
    /// to poll with [`Self::result`]. Caller must then SetLatch the worker
    /// latch to wake the compactor promptly.
    ///
    /// At most one request may be outstanding (see [`CompactionRequest`]).
    pub fn publish(&self, target: Checkpoint, requester_latch: u64) -> u64 {
        self.target_timeline
            .store(target.timeline_id.as_u32(), Ordering::Relaxed);
        self.target_lsn
            .store(target.lsn.as_u64(), Ordering::Relaxed);
        self.requester_latch
            .store(requester_latch, Ordering::Relaxed);
        self.status.store(0, Ordering::Relaxed);
        // Release publishes the payload above.
        self.request_gen.fetch_add(1, Ordering::Release) + 1
    }

    /// Worker side: observe the pending request, if any. Double-reads
    /// `request_gen` so a torn read can only delay the request to the next
    /// poll, never mix fields from two generations.
    pub fn pending(&self) -> Option<PendingCompaction> {
        let generation = self.request_gen.load(Ordering::Acquire);
        if generation == self.done_gen.load(Ordering::Acquire) {
            return None;
        }
        let pending = PendingCompaction {
            generation,
            target: Checkpoint::new(
                TimelineId::new(self.target_timeline.load(Ordering::Relaxed)),
                Lsn::new(self.target_lsn.load(Ordering::Relaxed)),
            ),
            requester_latch: self.requester_latch.load(Ordering::Relaxed),
        };
        if self.request_gen.load(Ordering::Acquire) != generation {
            return None;
        }
        Some(pending)
    }

    /// Worker side: mark `generation` complete. The caller SetLatches
    /// `requester_latch` afterwards to wake the waiting requester.
    pub fn complete(&self, generation: u64, status: u32) {
        self.status.store(status, Ordering::Relaxed);
        // Release publishes `status`.
        self.done_gen.store(generation, Ordering::Release);
    }

    /// Requester side: the completion status once `generation` has been
    /// serviced.
    pub fn result(&self, generation: u64) -> Option<u32> {
        if self.done_gen.load(Ordering::Acquire) == generation {
            Some(self.status.load(Ordering::Relaxed))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_request() -> CompactionRequest {
        CompactionRequest {
            request_gen: AtomicU64::new(0),
            done_gen: AtomicU64::new(0),
            target_timeline: AtomicU32::new(0),
            target_lsn: AtomicU64::new(0),
            requester_latch: AtomicU64::new(0),
            status: AtomicU32::new(0),
        }
    }

    fn ckpt(lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(1), Lsn::new(lsn))
    }

    #[test]
    fn compaction_request_round_trip() {
        let req = new_request();
        assert!(req.pending().is_none());
        assert!(req.result(1).is_none());

        let generation = req.publish(ckpt(500), 0xDEAD);
        assert_eq!(generation, 1);

        let pending = req.pending().expect("request pending");
        assert_eq!(pending.generation, 1);
        assert_eq!(pending.target, ckpt(500));
        assert_eq!(pending.requester_latch, 0xDEAD);
        // Still pending until completed.
        assert!(req.result(1).is_none());

        req.complete(1, COMPACTION_STATUS_OK);
        assert!(req.pending().is_none());
        assert_eq!(req.result(1), Some(COMPACTION_STATUS_OK));
    }

    #[test]
    fn compaction_request_generations_are_monotonic() {
        let req = new_request();
        let g1 = req.publish(ckpt(100), 1);
        req.complete(g1, COMPACTION_STATUS_OK);
        let g2 = req.publish(ckpt(200), 2);
        assert_eq!(g2, g1 + 1);
        // The previous generation stays complete; the new one is pending.
        assert_eq!(req.result(g1), Some(COMPACTION_STATUS_OK));
        assert!(req.result(g2).is_none());

        req.complete(g2, COMPACTION_STATUS_ERROR);
        assert_eq!(req.result(g2), Some(COMPACTION_STATUS_ERROR));
    }
}
