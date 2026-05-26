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
};
use std::time::Duration;

use crate::log_relay::{relay_debug1, relay_info};

// ── Background task ───────────────────────────────────────────────────────────

/// Tokio task: advance `base_ckpt` periodically.
///
/// Runs until the process exits.  Errors are non-fatal — logged and skipped.
/// A failed compaction only means more segments remain in front of the base
/// until the next iteration; correctness is never compromised.
pub async fn compactor_task(store: &'static Store) {
    let interval_secs = Duration::from_secs(env::read_u64_or(env::ENV_COMPACT_INTERVAL_SECS, 60));
    let mut interval = tokio::time::interval(interval_secs);

    relay_info(format!(
        "tiko: compactor started (interval={}s)",
        interval_secs.as_secs(),
    ));

    loop {
        interval.tick().await;

        match store.run_compaction() {
            Ok(CompactionResult::Applied {
                new_base_ckpt,
                count,
            }) => {
                relay_info(format!(
                    "tiko: compactor: merged {count} segment checkpoint(s); base_ckpt → {new_base_ckpt}",
                ));
            }
            Ok(CompactionResult::NoNewSegments) => {
                relay_debug1("tiko: compactor: no new segments above base — skipping");
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
