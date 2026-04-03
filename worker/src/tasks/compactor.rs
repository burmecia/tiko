//! Compactor — periodic base manifest materialization.
//!
//! Runs on Tokio. Merges accumulated delta manifests into the latest base
//! manifest at a configurable interval (default 1 hour). Non-fatal: if
//! materialization fails, the error is logged to stderr and the task
//! continues; deltas remain the source of truth.
//!
//! GC (retention enforcement) is the control plane's responsibility and is
//! intentionally absent from this task.

use core::{
    ENV_PITR_INTERVAL_SECS,
    manifest::{MaterializeResult, materialize_base},
    project::{ProjectCtx, ProjectNamespace},
    sim_store::SimStore,
};

// ── CompactorConfig ──────────────────────────────────────────────────────────

/// Configuration for the compactor background task.
pub struct CompactorConfig {
    /// How often to materialize a new base manifest.
    /// Read from `TIKO_PITR_INTERVAL_SECS` (default: 3600 seconds).
    pub materialization_interval: std::time::Duration,
}

impl CompactorConfig {
    /// Build config from environment.  Falls back to 3600s if the variable
    /// is absent or cannot be parsed.
    pub fn from_env() -> Self {
        let secs = std::env::var(ENV_PITR_INTERVAL_SECS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3600);
        CompactorConfig {
            materialization_interval: std::time::Duration::from_secs(secs),
        }
    }
}

// ── Background task ───────────────────────────────────────────────────────────

/// Tokio task: materialize a new base manifest periodically.
///
/// Runs until the process exits.  Errors are non-fatal — logged to stderr and
/// skipped.  A failed materialization only means the next recovery will replay
/// more deltas; correctness is never compromised.
pub async fn compactor_task(sim: &'static SimStore, ns: ProjectNamespace, config: CompactorConfig) {
    tracing::info!(
        "tiko: compactor: started (project={}, interval={}s)",
        ns.project_id,
        config.materialization_interval.as_secs(),
    );
    let mut interval = tokio::time::interval(config.materialization_interval);
    loop {
        interval.tick().await;
        // Read current timeline from ProjectCtx each iteration so that PITR
        // recovery (which bumps current_timeline_id) is picked up without
        // restarting the background task.
        let timeline = ProjectCtx::try_get()
            .map(|ctx| ctx.current_timeline_id())
            .unwrap_or(1);
        match materialize_base(&sim, &ns, timeline) {
            Ok(MaterializeResult::NoNewDeltas { base_lsn }) => {
                tracing::info!(
                    "tiko: pitr: no new deltas since base {base_lsn} — skipping (project={})",
                    ns.project_id
                );
            }
            Ok(MaterializeResult::Materialized {
                prev_base_lsn,
                new_lsn,
                delta_count,
            }) => {
                tracing::info!(
                    "tiko: pitr: materialized new base at lsn={} \
                     ({delta_count} delta(s) merged, prev_base={prev_base_lsn}, project={})",
                    new_lsn.to_hex(),
                    ns.project_id,
                );
            }
            Err(e) => {
                tracing::warn!(
                    "tiko: pitr: materialize_base failed (project={}): {e}",
                    ns.project_id
                );
            }
        }
    }
}
