//! Compactor — periodic base manifest materialization.
//!
//! Runs on Tokio. Reads on-storage timeline segments above the current
//! `base_ckpt`, merges their chunk references and relfork metadata into the
//! base manifest, advances `base_ckpt`, and deletes segment files whose
//! entire LSN range is now covered. Non-fatal: a failed compaction is logged
//! and the task keeps running; segments + the previous base manifest remain
//! the source of truth.
//!
//! GC (retention enforcement / orphan chunk cleanup) is the control plane's
//! responsibility and remains out of scope here.

use core::{
    env,
    io::store::{CompactionResult, Store},
    io_control::IoControl,
};
use pgsys::common::recovery_in_progress;
use std::time::Duration;

use crate::log_relay::{relay_debug1, relay_debug2, relay_info};

// ── Background task ───────────────────────────────────────────────────────────

/// RAII guard that bumps `compaction_in_progress` on creation and decrements
/// on drop, so a panic inside `run_compaction` can never strand the counter
/// (a stranded counter would deadlock a subsequent basebackup
/// `TimelineState::drain_compaction`).
struct CompactionGuard {
    io_control: &'static IoControl,
}

impl CompactionGuard {
    fn new(io_control: &'static IoControl) -> Self {
        io_control.timeline.begin_compaction();
        Self { io_control }
    }
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        self.io_control.timeline.end_compaction();
    }
}

/// Tokio task: advance `base_ckpt` periodically.
///
/// Runs until the process exits.  Errors are non-fatal — logged and skipped.
/// A failed compaction only means more segments remain in front of the base
/// until the next iteration; correctness is never compromised.
pub async fn compactor_task(store: &'static Store) {
    let interval_secs = Duration::from_secs(env::read_u64_or(env::ENV_COMPACT_INTERVAL_SECS, 60));
    let mut interval = tokio::time::interval(interval_secs);

    relay_info(format!(
        "tiko: compactor started (interval={}s, in_recovery={})",
        interval_secs.as_secs(),
        recovery_in_progress(),
    ));

    loop {
        interval.tick().await;

        // While the cluster is in archive/crash recovery the base manifest is
        // the PITR anchor — the compactor must not touch state. (It would be a
        // no-op anyway: head_ckpt stays at default until the end-of-recovery
        // checkpoint and the pre-recovery segments are deleted, so
        // `run_compaction` would return `NoNewSegments`. Skip explicitly for
        // clarity and defense-in-depth.) Resumes automatically once recovery
        // finishes (promote).
        if recovery_in_progress() {
            relay_debug1("tiko: compactor: cluster in recovery — skipping tick");
            continue;
        }

        // A `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint runs compaction itself to
        // form a base manifest at the basebackup LSN; skip this tick so we
        // don't race it. The checkpointer unpauses after its own compaction.
        if let Some(io_control) = IoControl::try_get() {
            if io_control.timeline.is_compaction_paused() {
                relay_debug2(
                    "tiko: compactor: paused for basebackup checkpoint — skipping tick",
                );
                continue;
            }
        }

        // Wrap the synchronous `run_compaction` in the in-progress guard so a
        // pausing checkpointer's `drain_compaction` observes our work.
        let result = match IoControl::try_get() {
            Some(io_control) => {
                let _guard = CompactionGuard::new(io_control);
                store.run_compaction()
            }
            None => store.run_compaction(),
        };

        match result {
            Ok(CompactionResult::Applied {
                base_ckpt,
                new_base_ckpt,
                count,
            }) => {
                relay_info(format!(
                    "tiko: compactor: merged {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt}",
                ));
            }
            Ok(CompactionResult::NoNewSegments) => {
                relay_debug2("tiko: compactor: no new segments above base — skipping");
            }
            Ok(CompactionResult::Raced) => {
                relay_debug1("tiko: compactor: raced with another compactor; will retry next tick");
            }
            Ok(CompactionResult::Skipped) => {
                relay_debug1(
                    "tiko: compactor: IoControl unavailable — skipping (initdb/single-user)",
                );
            }
            Err(e) => {
                relay_debug1(format!("tiko: compactor: run_compaction failed: {e}"));
            }
        }
    }
}
