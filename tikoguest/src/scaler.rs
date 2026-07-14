//! Scaler loop: periodic PG metrics report + idle evaluation.
//!
//! Every `interval` (default 30s), the loop:
//! 1. Collects PG metrics via [`PgMetrics`].
//! 2. Sends `POST /vms/{vm_id}/reports` with the metrics to tikod. The
//!    response includes the current `pause_epoch` — this combines the status
//!    report and epoch check into a single HTTP round-trip.
//! 3. Compares the returned epoch with a local copy. If they differ, the VM
//!    was paused (and possibly snapshotted + restored) since the last tick —
//!    reset `idle_ticks`.
//! 4. Evaluates the [`ScalerPolicy`]: is the VM idle enough to pause?
//! 5. If eligible: sends `POST /vms/{vm_id}/pause-request` (send-and-forget).
//!    tikod dedups via `pause_requested` (idempotent 202 for duplicates).
//!
//! tikod decides when to snapshot: on receiving a pause-request, it pauses
//! the VM immediately and starts a 2-min warm window. If no client arrives
//! during the window → snapshot (cold freeze). If a client arrives →
//! resume. The guest's job is purely to signal "I'm idle."
//!
//! # Pause epoch detection
//!
//! `idle_ticks` is in-memory, so it survives a pause/restore *stale*. tikod
//! bumps `pause_epoch` each time it pauses the VM. Because the bump happens
//! at pause time (before the guest is frozen), the guest's local copy is
//! always stale on the first tick after resume/restore. The mismatch is
//! detected and `idle_ticks` is reset, preventing a premature re-request.

use std::time::Duration;

use serde::Serialize;
use tracing::{debug, info, warn};

use crate::http::{HttpClient, HttpError};
use crate::pgmetrics::{Metrics, PgMetrics};

/// Policy for idle evaluation. Defaults: 0 connections, 0 active backends,
/// 0 long-running tx, 4 idle ticks (= 2 min at 30s interval).
#[derive(Clone, Debug)]
pub struct ScalerPolicy {
    /// Consecutive idle ticks before requesting a pause.
    pub idle_threshold_ticks: u64,
    /// Max connections to be considered idle.
    pub max_connections: i64,
    /// Max active backends to be considered idle.
    pub max_active_backends: i64,
    /// Max long-running transactions to be considered idle.
    pub max_long_running_tx: i64,
}

impl Default for ScalerPolicy {
    fn default() -> Self {
        Self {
            idle_threshold_ticks: 4, // 4 * 30s = 2 min
            max_connections: 5,
            max_active_backends: 5,
            max_long_running_tx: 5,
        }
    }
}

/// Request body sent to tikod for a pause-request.
#[derive(Serialize)]
struct PauseRequest {
    reason: String,
    metrics: Metrics,
}

/// Run the scaler loop. Blocks forever (until the process is killed).
///
/// This unified loop replaces the former separate observer and scaler loops.
/// It pushes metrics to tikod (receiving the pause epoch back) and evaluates
/// idle policy in a single pass per tick.
pub async fn scaler_loop(
    pg: PgMetrics,
    vm_id: String,
    tikod: HttpClient,
    interval: Duration,
    policy: ScalerPolicy,
) {
    let reports_path = format!("/vms/{vm_id}/reports");
    let pause_path = format!("/vms/{vm_id}/pause-request");
    info!(
        vm_id = %vm_id,
        interval_secs = interval.as_secs(),
        threshold_ticks = policy.idle_threshold_ticks,
        "scaler loop started"
    );

    let mut idle_ticks: u64 = 0;
    // Last pause epoch seen from tikod. In-memory, so stale after resume/
    // restore — which is exactly why the mismatch is detected. Starts at 0
    // (the value `register` assigns); a fresh-boot VM also has epoch 0, so
    // there is no false reset on first run.
    let mut last_seen_epoch: u64 = 0;

    loop {
        tokio::time::sleep(interval).await;

        let metrics = pg.collect().await;

        // Combined status report + epoch check. The response body contains
        // the current pause epoch — a mismatch means the VM was paused (and
        // possibly restored) since our last tick.
        let metrics_body = serde_json::to_value(&metrics).unwrap_or_default();
        match tikod
            .send_json("POST", &reports_path, Some(&metrics_body))
            .await
        {
            Ok(resp) if resp.status == 200 => {
                if let Some(epoch) = parse_pause_epoch(&resp.body)
                    && epoch != last_seen_epoch
                {
                    info!(
                        vm_id = %vm_id,
                        epoch,
                        prev = last_seen_epoch,
                        "pause/restore detected — resetting scaler state"
                    );
                    idle_ticks = 0;
                    last_seen_epoch = epoch;
                }
            }
            Ok(resp) => {
                debug!(
                    vm_id = %vm_id,
                    status = resp.status,
                    "reports returned non-200 — skipping epoch check this tick"
                );
            }
            Err(e) => {
                debug!(
                    vm_id = %vm_id,
                    error = %e,
                    "failed to push report — skipping epoch check this tick"
                );
            }
        }

        if !metrics.available {
            warn!("PG unavailable — skipping idle evaluation");
            continue;
        }

        let is_idle = metrics.connections <= policy.max_connections
            && metrics.active_backends <= policy.max_active_backends
            && metrics.long_running_tx <= policy.max_long_running_tx;

        debug!(
            "pg metrics: {:?}, idle_ticks: {}, is_idle: {}",
            metrics, idle_ticks, is_idle
        );

        if is_idle {
            idle_ticks += 1;
            debug!(
                idle_ticks,
                threshold = policy.idle_threshold_ticks,
                "VM is idle"
            );

            if idle_ticks >= policy.idle_threshold_ticks {
                let body = PauseRequest {
                    reason: "idle".into(),
                    metrics: metrics.clone(),
                };
                let json = serde_json::to_value(&body).unwrap_or_default();

                match send_with_retry(&tikod, &pause_path, &json, 3).await {
                    Ok(()) => {
                        debug!(vm_id = %vm_id, "pause request sent (tikod dedups)");
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "pause request failed after retries — will retry next tick"
                        );
                    }
                }
            }
        } else {
            if idle_ticks > 0 {
                debug!(idle_ticks, "VM became active — resetting scaler state");
            }
            idle_ticks = 0;
        }
    }
}

/// Send the request with up to `max_attempts` retries. Backoff: 1s, 2s, 4s.
/// Returns `Ok(())` on any 2xx response.
async fn send_with_retry(
    tikod: &HttpClient,
    path: &str,
    body: &serde_json::Value,
    max_attempts: u32,
) -> Result<(), HttpError> {
    let backoffs = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(4),
    ];
    for attempt in 0..max_attempts {
        match tikod.send_json("POST", path, Some(body)).await {
            Ok(resp) if (200..300).contains(&resp.status) => {
                return Ok(());
            }
            Ok(resp) => {
                warn!(
                    attempt = attempt + 1,
                    status = resp.status,
                    body = %String::from_utf8_lossy(&resp.body),
                    "tikod rejected pause request"
                );
            }
            Err(e) => {
                warn!(attempt = attempt + 1, error = %e, "pause request transport error");
            }
        }
        if attempt + 1 < max_attempts {
            let backoff = backoffs[attempt as usize];
            tokio::time::sleep(backoff).await;
        }
    }
    Err(HttpError::Transport(format!(
        "pause request failed after {max_attempts} attempts"
    )))
}

/// Parse the `pause_epoch` field from a `POST /reports` response body
/// (`{"pause_epoch":N}`). Returns `None` on any parse failure — the caller
/// treats None as "no epoch info" and skips the reset this tick.
fn parse_pause_epoch(body: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("pause_epoch")?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pause_epoch_extracts_value() {
        let body = br#"{"pause_epoch":7}"#;
        assert_eq!(parse_pause_epoch(body), Some(7));
    }

    #[test]
    fn parse_pause_epoch_zero() {
        let body = br#"{"pause_epoch":0}"#;
        assert_eq!(parse_pause_epoch(body), Some(0));
    }

    #[test]
    fn parse_pause_epoch_missing_field() {
        let body = br#"{"foo":"bar"}"#;
        assert_eq!(parse_pause_epoch(body), None);
    }

    #[test]
    fn parse_pause_epoch_malformed_json() {
        assert_eq!(parse_pause_epoch(b"not json"), None);
    }

    #[test]
    fn parse_pause_epoch_non_numeric() {
        let body = br#"{"pause_epoch":"oops"}"#;
        assert_eq!(parse_pause_epoch(body), None);
    }
}
