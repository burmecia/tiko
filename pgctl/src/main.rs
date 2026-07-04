//! `pgctl` — guest-side Postgres control agent for tikod.
//!
//! Runs inside each Tiko VM as the `postgres` user. Exposes `pg_ctl` lifecycle
//! operations (start/stop/restart/reload) and `postgresql.tiko.conf` reads/writes
//! over a small HTTP/1.1 API on the guest network. tikod calls this agent over
//! the VM's guest IP to control the database.
//!
//! ```text
//! tikod ──HTTP──→ guest:9000 ──→ pgctl ──→ pg_ctl / postgresql.tiko.conf
//!                                       └──→ Postgres (PGDATA=/var/lib/postgresql/tt)
//! ```
//!
//! Every path is overridable, so the agent is fully testable outside a VM:
//! point `--pg-ctl` at a fake script and `--data-dir` at a temp dir.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use pgctl::pgops::PgCtl;
use pgctl::server::PgServer;

/// Guest-side Postgres control agent.
#[derive(Parser, Debug)]
#[command(name = "pgctl", version, about)]
struct Args {
    /// Address to listen on. `0.0.0.0` so tikod (on the host) can reach it via
    /// the guest IP.
    #[arg(long, default_value = "0.0.0.0:9000", env = "PGCTL_LISTEN")]
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
    #[arg(long, default_value = "/var/lib/postgresql/log.log", env = "PGCTL_LOG")]
    log_path: PathBuf,

    /// Override config file (`include_if_exists` target in `postgresql.conf`).
    /// Defaults to `postgresql.tiko.conf` inside the data dir.
    #[arg(long, env = "PGCTL_CONFIG_FILE")]
    config_file: Option<PathBuf>,

    /// Per-VM Tiko identity file (org/db/project + storage roots), written by
    /// `start_vm.sh` and sourced by `tiko_env.sh`. Defaults to
    /// `<data_dir_parent>/tiko.env`. The resolved vars are passed to postgres.
    #[arg(long, env = "TIKO_ENV_FILE")]
    tiko_env: Option<PathBuf>,
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
        "starting pgctl agent"
    );

    let server = Arc::new(PgServer::new(ctl));
    server.run(listen_addr).await?;
    Ok(())
}
