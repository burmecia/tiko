//! Periodic base-backup loop for PITR.
//!
//! Every `interval`, the loop spawns `tiko_pitr backup` — the CLI tool that
//! runs `pg_basebackup` against the live instance and uploads the (small) base
//! backup + base manifest to Tiko storage (see `cli/src/bin/tiko_pitr.rs`).
//! Regular base backups bound WAL replay during recovery, enabling PITR.
//!
//! The loop is best-effort: a failed backup is logged and retried next cycle.
//! It never crashes the agent. It starts with a sleep so PG has time to come
//! up on boot before the first `pg_basebackup` attempt.
//!
//! `tiko_pitr` is invoked via its `/usr/local/bin/tiko_pitr` wrapper, which
//! sources `tiko_env.sh` (identity, storage roots, PGDATA) before exec'ing the
//! real binary — so the agent doesn't need to replicate that env setup here.

use std::path::PathBuf;
use std::time::Duration;

use tokio::process::Command;
use tracing::{info, warn};

/// Run the periodic base-backup loop. Blocks forever (until the process is
/// killed). Each iteration sleeps `interval` first, then spawns
/// `tiko_pitr backup`.
pub async fn backup_loop(tiko_pitr: PathBuf, interval: Duration) {
    info!(
        bin = %tiko_pitr.display(),
        interval_secs = interval.as_secs(),
        "base-backup loop started"
    );

    loop {
        tokio::time::sleep(interval).await;

        let result = Command::new(&tiko_pitr)
            .arg("backup")
            .output()
            .await;

        match result {
            Ok(output) if output.status.success() => {
                info!("base backup completed");
            }
            Ok(output) => {
                let stderr = oneline(&output.stderr);
                warn!(
                    code = output.status.code().unwrap_or(-1),
                    error = %stderr,
                    "base backup failed — will retry next cycle"
                );
            }
            Err(e) => {
                warn!(error = %e, "failed to spawn tiko_pitr — will retry next cycle");
            }
        }
    }
}

/// Collapse captured bytes to a single trimmed line (newlines/extra whitespace
/// → single spaces) so the failure fits on one log line instead of spilling.
fn oneline(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let collapsed: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed
}
