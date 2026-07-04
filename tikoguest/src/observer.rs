//! Observer loop: periodic Postgres metrics push to tikod.
//!
//! Every `interval` (default 30s), the loop:
//! 1. Collects a snapshot of PG metrics via [`PgMetrics`].
//! 2. POSTs the serialized [`Metrics`](crate::pgmetrics::Metrics) to tikod at
//!    `POST /vms/{vm_id}/reports`.
//!
//! Errors (PG down, tikod unreachable, non-2xx) are logged and retried next
//! tick — the loop never exits on its own. It runs for the lifetime of the
//! agent process (= the lifetime of the VM). When the VM is paused for
//! scale-to-zero, the loop is killed along with the process.

use std::time::Duration;

use tracing::{debug, info, warn};

use crate::http::{HttpClient, HttpError};
use crate::pgmetrics::PgMetrics;

/// Run the observer loop. Blocks forever (until the process is killed).
pub async fn observer_loop(
    pg: PgMetrics,
    vm_id: String,
    tikod: HttpClient,
    interval: Duration,
) {
    let path = format!("/vms/{vm_id}/reports");
    info!(
        vm_id = %vm_id,
        interval_secs = interval.as_secs(),
        "observer loop started"
    );

    loop {
        tokio::time::sleep(interval).await;

        let metrics = pg.collect().await;
        debug!(
            available = metrics.available,
            connections = metrics.connections,
            active = metrics.active_backends,
            "collected metrics"
        );

        let body = serde_json::to_value(&metrics).unwrap_or_default();
        match tikod.send_json("POST", &path, Some(&body)).await {
            Ok(resp) if (200..300).contains(&resp.status) => {
                debug!(status = resp.status, "report pushed to tikod");
            }
            Ok(resp) => {
                warn!(
                    status = resp.status,
                    body = %String::from_utf8_lossy(&resp.body),
                    "tikod rejected report — will retry next tick"
                );
            }
            Err(HttpError::Transport(e)) => {
                warn!(error = %e, "failed to push report to tikod — will retry next tick");
            }
        }
    }
}
