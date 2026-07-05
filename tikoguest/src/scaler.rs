//! Scaler loop: evaluate snapshot eligibility and signal tikod.
//!
//! Every `interval` (default 30s), the loop:
//! 1. Collects PG metrics via [`PgMetrics`].
//! 2. Evaluates the [`ScalerPolicy`]: is the VM idle enough to snapshot?
//! 3. If eligible and not already requested: sends
//!    `POST /vms/{vm_id}/snapshot-request` to tikod, retrying on failure
//!    (3 attempts, 1s→2s→4s backoff).
//! 4. On 2xx ack: sets `requested = true` — stops sending until PG becomes
//!    active again (connections > threshold), which resets the state.
//!
//! tikod acks `202` **before** starting `scale_to_zero`, so the agent reads the
//! ack before the VM is frozen. If tikod fails to scale, that's tikod's
//! responsibility — the agent's job is to signal.
//!
//! # Restore detection
//!
//! Because `requested`/`idle_ticks` are in-memory, they survive a
//! snapshot/restore *stale*: the snapshot is taken after `requested = true`
//! is set, so on restore the flag is stuck and the VM would never re-request.
//! Each tick, the loop polls tikod's restore epoch
//! (`GET /vms/{vm_id}/restore-epoch`). tikod bumps the epoch on every
//! successful cold restore (`Node::wake`); a mismatch means "I was restored"
//! and the scaler resets `requested`/`idle_ticks`. The `last_seen_epoch` is
//! itself in-memory, so it is stale after restore — which is exactly why the
//! mismatch is detected.

use std::time::Duration;

use serde::Serialize;
use tracing::{debug, info, warn};

use crate::http::{HttpClient, HttpError};
use crate::pgmetrics::{Metrics, PgMetrics};

/// Policy for snapshot eligibility. Defaults: 0 connections, 0 active backends,
/// 0 long-running tx, 4 idle ticks (= 2 min at 30s interval).
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
            idle_threshold_ticks: 4, // 4 * 30s = 2 min
            max_connections: 5,
            max_active_backends: 5,
            max_long_running_tx: 5,
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
    let epoch_path = format!("/vms/{vm_id}/restore-epoch");
    info!(
        vm_id = %vm_id,
        interval_secs = interval.as_secs(),
        threshold_ticks = policy.idle_threshold_ticks,
        "scaler loop started"
    );

    let mut idle_ticks: u64 = 0;
    let mut requested = false;
    // Last restore epoch seen from tikod. Because this is in-memory, it
    // survives a snapshot/restore *stale* — so on the first tick after
    // restore it mismatches tikod's bumped epoch and we reset. Starts at 0
    // (the value `register` assigns); a fresh-boot VM also has epoch 0, so
    // there is no false reset on first run.
    let mut last_seen_epoch: u64 = 0;

    loop {
        tokio::time::sleep(interval).await;

        // Restore detection: if tikod bumped the epoch since our last tick,
        // we were restored from a snapshot. Reset stale scaler state so we
        // can request scale-to-zero again in the new lifecycle. A failed
        // query is non-fatal — we just retry next tick.
        if let Ok(resp) = tikod.send_json("GET", &epoch_path, None).await {
            if resp.status == 200 {
                if let Some(epoch) = parse_epoch(&resp.body)
                    && epoch != last_seen_epoch
                {
                    info!(
                        vm_id = %vm_id,
                        epoch,
                        prev = last_seen_epoch,
                        "restore detected — resetting scaler state"
                    );
                    requested = false;
                    idle_ticks = 0;
                    last_seen_epoch = epoch;
                }
            } else if resp.status != 404 {
                debug!(
                    vm_id = %vm_id,
                    status = resp.status,
                    "restore-epoch query returned non-200"
                );
            }
        }

        let metrics = pg.collect().await;

        if !metrics.available {
            warn!("PG unavailable — skipping scaler tick");
            continue;
        }

        let is_idle = metrics.connections <= policy.max_connections
            && metrics.active_backends <= policy.max_active_backends
            && metrics.long_running_tx <= policy.max_long_running_tx;

        debug!("pg metrics: {:?}, idle_ticks: {}, is_idle: {}", metrics, idle_ticks, is_idle);

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

/// Parse the `epoch` field from a `GET /restore-epoch` response body
/// (`{"vm_id":"...","epoch":N}`). Returns `None` on any parse failure — the
/// caller treats None as "no epoch info" and skips the reset this tick.
fn parse_epoch(body: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("epoch")?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_epoch_extracts_value() {
        let body = br#"{"vm_id":"vm-1","epoch":7}"#;
        assert_eq!(parse_epoch(body), Some(7));
    }

    #[test]
    fn parse_epoch_zero() {
        let body = br#"{"vm_id":"vm-1","epoch":0}"#;
        assert_eq!(parse_epoch(body), Some(0));
    }

    #[test]
    fn parse_epoch_missing_field() {
        let body = br#"{"vm_id":"vm-1"}"#;
        assert_eq!(parse_epoch(body), None);
    }

    #[test]
    fn parse_epoch_malformed_json() {
        assert_eq!(parse_epoch(b"not json"), None);
    }

    #[test]
    fn parse_epoch_non_numeric() {
        let body = br#"{"epoch":"oops"}"#;
        assert_eq!(parse_epoch(body), None);
    }
}
