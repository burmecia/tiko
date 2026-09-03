//! Compactor — periodic base manifest materialization.
//!
//! Runs on Tokio. Reads on-storage timeline segments above the current
//! `base_ckpt`, merges their chunk references and relfork metadata into the
//! base manifest, advances `base_ckpt`, and deletes segment files whose
//! entire LSN range is now covered. Non-fatal: a failed compaction is logged
//! and the task keeps running; segments + the previous base manifest remain
//! the source of truth.
//!
//! This task is the sole routine compaction executor. Besides the periodic
//! tick it also serves basebackup compaction requests published by the
//! checkpointer into `TimelineState::compaction_request` (relayed here by
//! the worker main loop), so the checkpointer never runs compaction itself
//! while the worker is alive. No mutual exclusion is needed between the two.
//!
//! GC (retention enforcement / orphan chunk cleanup) is the control plane's
//! responsibility and remains out of scope here.

use core::{
    env,
    io_control::IoControl,
    store::{CompactionResult, Store},
    timeline::{COMPACTION_STATUS_ERROR, COMPACTION_STATUS_OK, Checkpoint},
};
use pgsys::common::recovery_in_progress;
use pgsys::latch::{Latch, SetLatch};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::log_relay::{relay_debug1, relay_debug2, relay_info};

/// A basebackup compaction request relayed from the worker main loop.
pub(crate) struct CompactionRequestMsg {
    pub generation: u64,
    pub target: Checkpoint,
    pub requester_latch: u64,
}

// ── Background task ───────────────────────────────────────────────────────────

/// Tokio task: advance `base_ckpt` periodically + serve basebackup requests.
///
/// Runs until the process exits.  Errors are non-fatal — logged and skipped.
/// A failed compaction only means more segments remain in front of the base
/// until the next iteration; correctness is never compromised.
pub async fn compactor_task(
    store: &'static Store,
    mut req_rx: mpsc::Receiver<CompactionRequestMsg>,
) {
    let interval_secs = Duration::from_secs(env::read_u64_or(env::ENV_COMPACT_INTERVAL_SECS, 60));
    let mut interval = tokio::time::interval(interval_secs);

    relay_info(format!(
        "tiko: compactor started (interval={}s, in_recovery={})",
        interval_secs.as_secs(),
        recovery_in_progress(),
    ));

    loop {
        tokio::select! {
            _ = interval.tick() => {
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

                log_result("tick", store.run_compaction());
            }
            Some(req) = req_rx.recv() => {
                serve_compaction_request(store, req);
            }
        }
    }
}

/// Execute a basebackup compaction request and wake the requester. The
/// completion is published before the latch is set; the requester also
/// re-polls on a timeout, so a missed wake only costs one poll interval.
fn serve_compaction_request(store: &'static Store, req: CompactionRequestMsg) {
    relay_debug1(format!(
        "tiko: compactor: serving basebackup request gen={} target={}",
        req.generation, req.target,
    ));

    let status = match store.run_compaction_through(req.target) {
        Ok(result) => {
            log_result("basebackup", Ok(result));
            COMPACTION_STATUS_OK
        }
        Err(e) => {
            relay_debug1(format!(
                "tiko: compactor: basebackup compaction through {} failed: {e}",
                req.target,
            ));
            COMPACTION_STATUS_ERROR
        }
    };

    if let Some(io_control) = IoControl::try_get() {
        io_control
            .timeline
            .complete_compaction(req.generation, status);
    }
    if req.requester_latch != 0 {
        // SAFETY: the requester published its own live latch; a SetLatch on
        // a recycled latch is a harmless spurious wakeup.
        unsafe { SetLatch(req.requester_latch as *mut Latch) };
    }
}

fn log_result(context: &str, result: core::Result<CompactionResult>) {
    match result {
        Ok(CompactionResult::Applied {
            base_ckpt,
            new_base_ckpt,
            count,
        }) => {
            relay_info(format!(
                "tiko: compactor({context}): merged {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt}",
            ));
        }
        Ok(CompactionResult::NoNewSegments) => {
            relay_debug2(format!(
                "tiko: compactor({context}): no new segments above base — skipping"
            ));
        }
        Ok(CompactionResult::Raced) => {
            relay_debug1(format!(
                "tiko: compactor({context}): raced with another compactor — discarded"
            ));
        }
        Ok(CompactionResult::Skipped) => {
            relay_debug1(format!(
                "tiko: compactor({context}): IoControl unavailable — skipping (initdb/single-user)"
            ));
        }
        Err(e) => {
            relay_debug1(format!(
                "tiko: compactor({context}): run_compaction failed: {e}"
            ));
        }
    }
}
