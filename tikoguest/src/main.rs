//! `tikoguest` — guest-side agent for tikod.
//!
//! Runs inside each Tiko VM as the `postgres` user. Serves:
//! - **Inbound HTTP API** (tikod → agent): `pg_ctl` lifecycle, config R/W.
//! - **Scaler loop** (agent → tikod): periodic PG metrics push to
//!   `POST /vms/{id}/reports` (with pause-epoch check) and pause-request
//!   signals to `POST /vms/{id}/pause-request` when idle.
//!
//! ```text
//! tikod ──HTTP──→ guest:9000 ──→ tikoguest ──→ pg_ctl / postgresql.tiko.conf
//!                                       └──→ Postgres (PGDATA=/var/lib/postgresql/tt)
//!
//! tikoguest ──HTTP──→ tikod /vms/{id}/reports        (scaler: metrics + epoch check)
//! tikoguest ──HTTP──→ tikod /vms/{id}/pause-request  (scaler: idle signal)
//! ```
//!
//! The scaler loop starts only when `TIKO_VM_ID` and `TIKOD_ADDR` are
//! available (from `tiko.env` or the process env). Without them the agent
//! still serves its HTTP API — useful for testing and fresh VMs.
//!
//! Every path is overridable, so the agent is fully testable outside a VM:
//! point `--pg-ctl` at a fake script and `--data-dir` at a temp dir.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use tikoguest::env;
use tikoguest::http::HttpClient;
use tikoguest::pgmetrics::{PgMetrics, PgMetricsConfig};
use tikoguest::pgops::PgCtl;
use tikoguest::scaler::{self, ScalerPolicy};
use tikoguest::server::PgServer;

/// Guest-side agent for tikod.
#[derive(Parser, Debug)]
#[command(name = "tikoguest", version, about)]
struct Args {
    /// Address to listen on. `0.0.0.0` so tikod (on the host) can reach it via
    /// the guest IP.
    #[arg(long, default_value = "0.0.0.0:9000", env = "TIKOGUEST_LISTEN")]
    listen: String,

    /// PGDATA directory (default matches the Tiko guest layout).
    #[arg(long, default_value = "/var/lib/postgresql/tt", env = "PGDATA")]
    data_dir: PathBuf,

    /// `pg_ctl` executable. Defaults to PATH lookup; override for testing.
    #[arg(long, default_value = "pg_ctl", env = "PG_CTL_BIN")]
    pg_ctl: PathBuf,

    /// `initdb` executable. Defaults to PATH lookup; override for testing.
    #[arg(long, default_value = "initdb", env = "INITDB_BIN")]
    initdb: PathBuf,

    /// Log file passed to `pg_ctl -l` for start/restart.
    #[arg(long, default_value = "/var/lib/postgresql/log.log", env = "TIKOGUEST_LOG")]
    log_path: PathBuf,

    /// Override config file (`include_if_exists` target in `postgresql.conf`).
    /// Defaults to `postgresql.tiko.conf` inside the data dir.
    #[arg(long, env = "TIKOGUEST_CONFIG_FILE")]
    config_file: Option<PathBuf>,

    /// Per-VM Tiko identity file (org/db/project + storage roots), written by
    /// `start_vm.sh` and sourced by `tiko_env.sh`. Defaults to
    /// `<data_dir_parent>/tiko.env`. The resolved vars are passed to postgres.
    #[arg(long, env = "TIKO_ENV_FILE")]
    tiko_env: Option<PathBuf>,

    /// Scaler loop interval (seconds). 0 disables the scaler. The loop pushes
    /// metrics to tikod, checks the pause epoch, and evaluates idle policy in
    /// one pass.
    #[arg(long, default_value_t = 30, env = "TIKOGUEST_OBSERVE_INTERVAL")]
    observe_interval: u64,

    /// Consecutive idle ticks before requesting a pause (default 4 = 2 min
    /// at 30s interval).
    #[arg(long, default_value_t = 4, env = "TIKOGUEST_SCALE_THRESHOLD_TICKS")]
    scale_threshold_ticks: u64,

    /// libpq connection string for metrics collection. Defaults to the unix
    /// socket as the `postgres` user, database `tt`.
    #[arg(long, env = "TIKOGUEST_PG_CONN")]
    pg_conn: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let listen_addr: SocketAddr = args.listen.parse()?;

    let config_file = args
        .config_file
        .unwrap_or_else(|| args.data_dir.join("postgresql.tiko.conf"));

    let mut ctl_builder = PgCtl::new(args.pg_ctl, args.data_dir, args.log_path, config_file)
        .with_initdb(args.initdb);
    if let Some(tiko_env_path) = args.tiko_env {
        ctl_builder = ctl_builder.with_tiko_env_path(tiko_env_path);
    }
    let ctl = ctl_builder;

    tracing::info!(
        listen = %listen_addr,
        data_dir = %ctl.data_dir.display(),
        config_file = %ctl.config_file.display(),
        tiko_db_id = ?ctl.tiko_env().get("TIKO_DB_ID"),
        tiko_org_id = ?ctl.tiko_env().get("TIKO_ORG_ID"),
        "starting tikoguest agent"
    );

    // ── Background tasks ───────────────────────────────────────────────────
    //
    // The scaler loop pushes PG metrics to tikod (receiving the pause epoch
    // back) and evaluates idle policy in a single pass per tick. When idle
    // for enough ticks, it sends a pause-request. It starts only when the
    // agent knows its own VM ID and tikod's address — from tiko.env (written
    // by start_vm.sh) or the process env. Without them the agent still
    // serves its HTTP API (useful for testing and fresh VMs).

    let tiko_env = ctl.tiko_env();
    let vm_id = env::lookup_optional(tiko_env, "TIKO_VM_ID");
    let tikod_addr = env::lookup_optional(tiko_env, "TIKOD_ADDR");

    match (vm_id, tikod_addr) {
        (Some(vm_id), Some(addr)) => {
            let Ok(tikod_addr) = addr.parse::<SocketAddr>() else {
                tracing::warn!(addr = %addr, "TIKOD_ADDR is not a valid SocketAddr — background tasks disabled");
                return start_server(listen_addr, ctl).await;
            };

            if args.observe_interval > 0 {
                let pg_config = match &args.pg_conn {
                    Some(conn) => PgMetricsConfig {
                        connection_string: conn.clone(),
                        db_name: "tt".into(),
                    },
                    None => PgMetricsConfig::default(),
                };
                let pg_metrics = PgMetrics::new(pg_config);
                let tikod_client = HttpClient::new(tikod_addr);
                let policy = ScalerPolicy {
                    idle_threshold_ticks: args.scale_threshold_ticks,
                    ..ScalerPolicy::default()
                };
                tracing::info!(
                    vm_id = %vm_id,
                    tikod = %tikod_addr,
                    interval_secs = args.observe_interval,
                    threshold_ticks = policy.idle_threshold_ticks,
                    "starting scaler loop"
                );
                tokio::spawn(scaler::scaler_loop(
                    pg_metrics,
                    vm_id,
                    tikod_client,
                    Duration::from_secs(args.observe_interval),
                    policy,
                ));
            }
        }
        _ => {
            tracing::info!(
                "background tasks disabled — TIKO_VM_ID or TIKOD_ADDR not set \
                 (agent still serves HTTP API)"
            );
        }
    }

    start_server(listen_addr, ctl).await
}

async fn start_server(listen_addr: SocketAddr, ctl: PgCtl) -> Result<(), Box<dyn std::error::Error>> {
    let server = Arc::new(PgServer::new(ctl));
    server.run(listen_addr).await?;
    Ok(())
}
