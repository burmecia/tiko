//! Scaler loop: evaluate snapshot eligibility and signal tikod.
//!
//! Every `interval` (default 30s), the loop:
//! 1. Collects PG metrics via [`PgMetrics`].
//! 2. Evaluates the [`ScalerPolicy`]: is the VM idle enough to snapshot?
//! 3. If eligible and not already requested: sends
//!    `POST /vms/{vm_id}/snapshot-request` to tikod, retrying on failure
//!    (3 attempts, 1s→2s→4s backoff).
//! 4. On 2xx ack: sets `requested = true` — stops sending until PG becomes
//!    active again (connections > 0), which resets the state.
//!
//! tikod acks `202` **before** starting `scale_to_zero`, so the agent reads the
//! ack before the VM is frozen. If tikod fails to scale, that's tikod's
//! responsibility — the agent's job is to signal.

use std::time::Duration;

use serde::Serialize;
use tracing::{debug, info, warn};

use crate::http::{HttpClient, HttpError};
use crate::pgmetrics::{Metrics, PgMetrics};

/// Policy for snapshot eligibility. Defaults: 0 connections, 0 active backends,
/// 0 long-running tx, 10 idle ticks (= 5 min at 30s interval).
#[derive(Clone, Debug)]
pub struct ScalerPolicy {
    /// Consecutive idle ticks before requesting a snapshot.
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
            idle_threshold_ticks: 10, // 10 * 30s = 5 min
            max_connections: 0,
            max_active_backends: 0,
            max_long_running_tx: 0,
        }
    }
}

/// Request body sent to tikod.
#[derive(Serialize)]
struct SnapshotRequest {
    reason: String,
    metrics: Metrics,
}

/// Run the scaler loop. Blocks forever (until the process is killed).
pub async fn scaler_loop(
    pg: PgMetrics,
    vm_id: String,
    tikod: HttpClient,
    interval: Duration,
    policy: ScalerPolicy,
) {
    let path = format!("/vms/{vm_id}/snapshot-request");
    info!(
        vm_id = %vm_id,
        interval_secs = interval.as_secs(),
        threshold_ticks = policy.idle_threshold_ticks,
        "scaler loop started"
    );

    let mut idle_ticks: u64 = 0;
    let mut requested = false;

    loop {
        tokio::time::sleep(interval).await;

        let metrics = pg.collect().await;

        if !metrics.available {
            debug!("PG unavailable — skipping scaler tick");
            continue;
        }

        let is_idle = metrics.connections <= policy.max_connections
            && metrics.active_backends <= policy.max_active_backends
            && metrics.long_running_tx <= policy.max_long_running_tx;

        if is_idle {
            idle_ticks += 1;
            debug!(idle_ticks, threshold = policy.idle_threshold_ticks, requested, "VM is idle");

            if idle_ticks >= policy.idle_threshold_ticks && !requested {
                let body = SnapshotRequest {
                    reason: "idle".into(),
                    metrics,
                };
                let json = serde_json::to_value(&body).unwrap_or_default();

                match send_with_retry(&tikod, &path, &json, 3).await {
                    Ok(()) => {
                        info!(vm_id = %vm_id, "snapshot request acknowledged by tikod");
                        requested = true;
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "snapshot request failed after retries — will retry next tick"
                        );
                    }
                }
            }
        } else {
            if idle_ticks > 0 || requested {
                debug!(idle_ticks, requested, "VM became active — resetting scaler state");
            }
            idle_ticks = 0;
            requested = false;
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
                    "tikod rejected snapshot request"
                );
            }
            Err(e) => {
                warn!(attempt = attempt + 1, error = %e, "snapshot request transport error");
            }
        }
        if attempt + 1 < max_attempts {
            let backoff = backoffs[attempt as usize];
            tokio::time::sleep(backoff).await;
        }
    }
    Err(HttpError::Transport(format!(
        "snapshot request failed after {max_attempts} attempts"
    )))
}
