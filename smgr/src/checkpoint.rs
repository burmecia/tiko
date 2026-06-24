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
//!    drain the backend `DraftBuffer`, append a `SegmentCheckpoint` to the
//!    timeline segment file, push the active window, advance `head_ckpt`,
//!    and persist `DbMeta`. (pg_state is no longer captured here: PITR bases
//!    now come from `pg_basebackup` tarballs uploaded by `tiko_pitr backup`,
//!    and segments carry an empty `pg_state` trailer.)
//!
//! 2. **Basebackups** (`CHECKPOINT_CAUSE_BASEBACKUP`): materialise a base
//!    manifest at the checkpoint LSN so `tiko_pitr` can pair the (small)
//!    `pg_basebackup` tarball with the chunk-ref map at the same LSN. To
//!    avoid racing the background compactor, the checkpointer pauses it,
//!    drains any in-flight run, then runs `run_compaction` itself.
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
    io::store::Store,
    io::timeline::Checkpoint,
    io_control::IoControl,
};
use pgsys::{Lsn, logging::*, timeline_id::TimelineId};

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

    // Commit the interval's dirty state into a timeline segment. pg_state is
    // no longer captured: PITR bases come from pg_basebackup tarballs.
    if let Err(e) = store.run_commit_protocol(&ckpt, &redo_ckpt, &[]) {
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

/// Run `run_compaction` to materialise a base manifest at the just-committed
/// checkpoint LSN, coordinating with the background compactor via the shmem
/// pause flag + in-progress counter so the two can't run in parallel.
///
/// Sequence: `pause_compaction` → `drain_compaction` (wait for any in-flight
/// background run) → `run_compaction` → `resume_compaction`. If `IoControl`
/// is unavailable (very early startup), coordination is skipped and
/// `run_compaction` is called directly (it self-skips in that case).
/// Run `run_compaction_through(commit_ckpt)` to materialise a base manifest AT
/// the basebackup checkpoint LSN, coordinating with the background compactor
/// via the shmem pause flag + in-progress counter so the two can't run in
/// parallel.
///
/// Sequence: `pause_compaction` → `drain_compaction` (wait for any in-flight
/// background run) → `run_compaction_through` → `resume_compaction`. If
/// `IoControl` is unavailable (very early startup), coordination is skipped
/// and the compaction is called directly (it self-skips in that case).
fn run_basebackup_compaction(store: &Store, commit_ckpt: Checkpoint) {
    if let Some(io_control) = IoControl::try_get() {
        io_control.timeline.pause_compaction();
        // Ensure the flag is always cleared, even if the compaction errors.
        let result = (|| {
            io_control.timeline.drain_compaction();
            store.run_compaction_through(commit_ckpt)
        })();
        io_control.timeline.resume_compaction();
        if let Err(e) = result {
            pg_log_warning(format!(
                "tiko: tiko_perform_checkpoint: basebackup compaction failed: {e}"
            ));
        }
    } else if let Err(e) = store.run_compaction_through(commit_ckpt) {
        pg_log_warning(format!(
            "tiko: tiko_perform_checkpoint: basebackup compaction failed: {e}"
        ));
    }
}
