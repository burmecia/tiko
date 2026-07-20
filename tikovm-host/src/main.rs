//! `tikovm-hostd` — the host daemon binary.
//!
//! Loads config, opens the state store, reconciles the registry from disk
//! (crash recovery), and serves the control API + scheduler. A `--mock` flag
//! swaps in a [`tikovm_host::vmm::mock::MockVmm`] so the full API can be
//! exercised without KVM/Firecracker.

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use tikovm_host::api::ApiServer;
use tikovm_host::config::HostConfig;
use tikovm_host::control::Control;
use tikovm_host::metrics;
use tikovm_host::node::Node;
use tikovm_host::proxy::Proxy;
use tikovm_host::scheduler::Scheduler;
use tikovm_host::store::{SqliteStore, reconcile};
use tikovm_host::vmm::{Vmm, default_vmm, mock::MockVmm};

#[derive(Debug, Parser)]
#[command(name = "tikovm-hostd", version, about = "tikovm host daemon")]
struct Args {
    /// Data directory (snapshots, overlays, state DB).
    #[arg(long, default_value = "/tmp/tikovm")]
    data_dir: std::path::PathBuf,

    /// Control API listen address.
    #[arg(long, default_value = "0.0.0.0:9000")]
    api_listen: String,

    /// Path to a config file (TOML). CLI flags override file values.
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Use the in-memory MockVmm backend instead of the platform default
    /// (Firecracker on Linux, stub elsewhere). Lets you drive the full API
    /// without KVM.
    #[arg(long, default_value_t = false)]
    mock: bool,

    /// TCP proxy listen address (data plane). Enables wake-on-connect routing.
    #[arg(long)]
    proxy_listen: Option<String>,

    /// Default target VM when a request carries no routing header.
    #[arg(long, requires = "proxy_listen")]
    proxy_default_vm: Option<String>,

    /// Default workload port inside the VM (when the manifest has no [expose]).
    #[arg(long, default_value_t = 8080, requires = "proxy_listen")]
    proxy_default_port: u16,

    /// remote_slow backing: "s3files_image" (default) or "ublk".
    #[arg(long)]
    remote_slow_backing: Option<String>,

    /// tikoblkd control socket (ublk backing).
    #[arg(long)]
    ublk_sock: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();
    metrics::init();

    let args = Args::parse();
    let mut cfg = HostConfig::load(args.config.as_deref(), &args.data_dir, &args.api_listen)?;
    if let Some(b) = &args.remote_slow_backing {
        cfg.storage.remote_slow_backing = b.clone();
    }
    if let Some(s) = &args.ublk_sock {
        cfg.storage.ublk_sock = s.clone();
    }
    // Validate early so a typo fails at boot, not at first provision.
    cfg.storage.backing()?;

    std::fs::create_dir_all(&cfg.data_dir)?;

    // --- durable store + crash recovery ---
    let store = Arc::new(SqliteStore::open(&cfg.state_db_path())?);
    let control = Arc::new(Control::new());
    let recovered = reconcile(&control, &*store)?;
    tracing::info!(recovered, "registry reconciled from store");

    // --- lifecycle node (+ write-through persistence) ---
    let vmm: Arc<dyn Vmm> = if args.mock {
        tracing::warn!("running with MockVmm (no real VMs)");
        Arc::new(MockVmm::new(cfg.snapshots_dir()))
    } else {
        default_vmm(cfg.snapshots_dir(), &cfg.storage)
    };
    let node = Arc::new(Node::new(vmm, control).with_store(store));

    // --- scheduler (scheduled-job triggers) ---
    let sched = Arc::new(Scheduler::new(node.clone()));
    tokio::spawn(async move { sched.run().await });

    // --- TCP proxy (HTTP header routing + wake-on-connect), if configured ---
    if let Some(proxy_listen) = args.proxy_listen.clone() {
        let proxy = Proxy::new(
            node.clone(),
            proxy_listen.parse().expect("proxy_listen addr"),
            args.proxy_default_vm.clone(),
            args.proxy_default_port,
        );
        tokio::spawn(async move {
            let _ = proxy.run().await;
        });
    }

    // --- control API (blocks) ---
    let api = Arc::new(ApiServer::new(node));
    tracing::info!(%cfg.api_listen, "tikovm-hostd ready");
    api.serve(&cfg.api_listen).await?;
    Ok(())
}
