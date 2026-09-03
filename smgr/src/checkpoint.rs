//! Checkpoint flush — the S3/PITR half of PostgreSQL's checkpoint.
//!
//! Called from `CreateCheckPoint()` in `xlog.c` after `CheckPointBuffers()`.
//! The checkpointer is a plain PG process — no Tokio runtime. All I/O is
//! synchronous (`std::fs` + `S3Sim` which is also `std::fs`).
//!
//! # Algorithm
//!
//! 0. **Guard**: returns early if `Store` is not yet initialised.
//!
//! 1. **Segment commit** (`run_commit_protocol`): flush dirty chunks +
//!    relfork meta to the express bucket, write-lock fence, set `redo_ckpt`,
//!    drain the backend `DraftBuffer`, append a `CheckpointSummary` to the
//!    timeline segment file, push the active window, advance `head_ckpt`,
//!    and persist `DbMeta`.
//!
//! 2. **Basebackups** (`CHECKPOINT_CAUSE_BASEBACKUP`): materialise a base
//!    manifest at the checkpoint LSN so `tiko_pitr` can pair the (small)
//!    `pg_basebackup` tarball with the chunk-ref map at the same LSN. The
//!    checkpointer delegates to the tikoworker compactor — the sole routine
//!    compaction executor — via a shmem request slot and waits on its own
//!    latch; it runs compaction locally only as an escape hatch (worker dead,
//!    request timed out, or worker-side error), which is race-safe because
//!    `run_compaction*` re-checks `base_ckpt` under the timeline write lock.
//!
//! 3. **Shutdown**: fold accumulated segments into the base manifest inline.
//!    The Tiko bgworker is killed in `PM_STOP_BACKENDS` before the shutdown
//!    checkpoint runs, so there is no in-process compactor to race with.
//!
//! # Crash safety
//!
//! The checkpoint is naturally idempotent: re-running `run_commit_protocol`
//! reproduces the same segment because the draft drain + express scan are
//! consistent. The base manifest PUT is atomic.

use core::{
    io_control::IoControl,
    store::Store,
    timeline::{COMPACTION_STATUS_OK, Checkpoint},
};
use pgsys::{
    Lsn,
    latch::{
        Latch, MyLatch, ResetLatch, SetLatch, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT,
        WaitLatch,
    },
    logging::*,
    timeline_id::TimelineId,
};
use std::sync::atomic::Ordering;

const CHECKPOINT_CAUSE_BASEBACKUP: i32 = 0x0200;

/// Called from Postgres `CreateCheckPoint()`.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_perform_checkpoint(
    timeline_id: u32,
    checkpoint_lsn: u64,
    redo_lsn: u64,
    flags: i32,
    is_shutdown: bool,
) {
    let ckpt = Checkpoint::new(TimelineId::new(timeline_id), Lsn::new(checkpoint_lsn));
    let redo_ckpt = Checkpoint::new(TimelineId::new(timeline_id), Lsn::new(redo_lsn));
    let is_basebackup = (flags & CHECKPOINT_CAUSE_BASEBACKUP) != 0;

    pg_log_info(format!(
        "tiko: tiko_perform_checkpoint: checkpoint {ckpt}, redo {redo_ckpt}, is_basebackup {is_basebackup}, is_shutdown {is_shutdown}"
    ));

    let store = match Store::try_get() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Commit the interval's dirty state into a timeline segment.
    if let Err(e) = store.run_commit_protocol(&ckpt, &redo_ckpt) {
        pg_log_error(&format!(
            "tiko: tiko_perform_checkpoint: run_commit_protocol failed at {ckpt}, redo {redo_ckpt}: {e}"
        ));
    }

    // Basebackup checkpoint: form a base manifest at the checkpoint LSN, with
    // the background compactor paused + drained so we don't race it. The base
    // manifest pairs with the pg_basebackup tarball uploaded by
    // `tiko_pitr backup` to anchor PITR at this LSN.
    if is_basebackup {
        run_basebackup_compaction(store, ckpt);
    }

    // Shutdown checkpoint: fold accumulated segments into the base manifest
    // inline. The Tiko bgworker (which normally runs compaction) is killed
    // in `PM_STOP_BACKENDS` before the checkpointer reaches
    // `PM_WAIT_XLOG_SHUTDOWN`, so there is no in-process compactor to race
    // with. Cross-process compactors are handled by the existing
    // `CompactionResult::Raced` detection inside `run_compaction`. Failure
    // is non-fatal — shutdown still completes; the next startup picks up
    // the extra segments via the normal hydrate path.
    if is_shutdown {
        if let Err(e) = store.run_compaction() {
            pg_log_warning(format!(
                "tiko: tiko_perform_checkpoint: shutdown compaction failed: {e}"
            ));
        }
    }
}

/// Materialise a base manifest at the just-committed checkpoint LSN by
/// delegating to the tikoworker compactor — publish a `compaction_request`
/// in shmem, wake the worker, and wait on our own latch.
///
/// Escape hatches all fall back to running `run_compaction_through` locally:
/// `IoControl` unavailable (very early startup), worker dead, request timed
/// out, or worker-side error. Local runs are race-safe: compaction re-checks
/// `base_ckpt` under the timeline write lock and discards duplicate runs.
fn run_basebackup_compaction(store: &Store, commit_ckpt: Checkpoint) {
    if let Some(io_control) = IoControl::try_get()
        && io_control.is_worker_alive()
    {
        let generation = io_control
            .timeline
            .request_compaction(commit_ckpt, unsafe { MyLatch } as u64);
        let worker_latch = io_control.worker_latch.load(Ordering::Acquire) as *mut Latch;
        if !worker_latch.is_null() {
            unsafe { SetLatch(worker_latch) };
        }

        // Wait for completion: latch wake or a 1s poll tick.
        const TIMEOUT_SECS: u32 = 300;
        let mut waited_secs = 0u32;
        let outcome = loop {
            unsafe { ResetLatch(MyLatch) };
            if let Some(status) = io_control.timeline.compaction_result(generation) {
                break Some(status);
            }
            if !io_control.is_worker_alive() {
                pg_log_warning(
                    "tiko: worker died while servicing compaction request; running locally",
                );
                break None;
            }
            waited_secs += 1;
            if waited_secs >= TIMEOUT_SECS {
                pg_log_warning(format!(
                    "tiko: timed out waiting for basebackup compaction at {commit_ckpt}; running locally"
                ));
                break None;
            }
            unsafe {
                WaitLatch(
                    MyLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    1000,
                    crate::WAIT_EVENT_TIKO_COMPACTION,
                );
            }
        };

        match outcome {
            Some(COMPACTION_STATUS_OK) => return,
            Some(status) => pg_log_warning(format!(
                "tiko: worker-side basebackup compaction failed (status {status}); running locally"
            )),
            None => {}
        }
    }

    if let Err(e) = store.run_compaction_through(commit_ckpt) {
        pg_log_warning(format!(
            "tiko: tiko_perform_checkpoint: basebackup compaction failed: {e}"
        ));
    }
}
