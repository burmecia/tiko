//! `tikod` entry point — the Tiko compute control plane.
//!
//! ```text
//! Usage: tikod [OPTIONS]
//!
//! Options:
//!   --data-dir <PATH>      Directory for snapshots and runtime artifacts
//!   --listen <ADDR>        Address for the PG proxy to listen on (default: 127.0.0.1:5432)
//!   --api-listen <ADDR>    Address for the HTTP control API (default: 127.0.0.1:9000)
//!   --agent-port <PORT>    Guest pgctl agent port for /vms/{id}/db/* (default: 9000)
//!   --idle-timeout <SECS>  Auto-pause after N seconds of inactivity (default: 300)
//!   --backend <NAME>       Force a VMM backend: auto|vz|firecracker (default: auto)
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use tikod::api::ApiServer;
use tikod::config::TikodConfig;
use tikod::control::{Control, IdlePolicy};
use tikod::node::Node;
use tikod::proxy::{Proxy, ProxyConfig};
use tikod::vmm::{default_vmm, Vmm};

/// Tiko compute control plane.
#[derive(Parser, Debug)]
#[command(name = "tikod", version, about)]
struct Args {
    /// Directory for snapshots and runtime artifacts.
    #[arg(long, default_value = "/tmp/tikod")]
    data_dir: String,

    /// Address for the PG proxy to listen on.
    #[arg(long, default_value = "127.0.0.1:5432")]
    listen: String,

    /// Address for the HTTP control API (VM lifecycle).
    #[arg(long, default_value = "127.0.0.1:9000")]
    api_listen: String,

    /// Port the in-guest `pgctl` agent listens on (used for /vms/{id}/db/*).
    #[arg(long, default_value_t = 9000)]
    agent_port: u16,

    /// Auto-pause after N seconds of inactivity.
    #[arg(long, default_value_t = 300)]
    idle_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    // Build configuration.
    let listen_addr: SocketAddr = args.listen.parse()?;
    let api_listen_addr: SocketAddr = args.api_listen.parse()?;
    let data_dir = PathBuf::from(&args.data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let config = TikodConfig {
        data_dir: data_dir.clone(),
        proxy: ProxyConfig {
            listen_addr,
            ..Default::default()
        },
        idle_policy: IdlePolicy {
            idle_timeout_secs: args.idle_timeout,
        },
    };

    tracing::info!(
        data_dir = %config.data_dir.display(),
        listen = %config.proxy.listen_addr,
        api_listen = %api_listen_addr,
        idle_timeout = config.idle_policy.idle_timeout_secs,
        platform = std::env::consts::OS,
        "starting tikod"
    );

    // Create the VMM backend (platform-specific).
    let snapshot_dir = data_dir.join("snapshots");
    let vmm: Arc<dyn Vmm> = Arc::from(default_vmm(snapshot_dir));

    // Create node (lifecycle manager).
    let node = Arc::new(Node::new(vmm, data_dir.join("snapshots")));

    // Create control plane.
    let control = Arc::new(Control::new(config.idle_policy.clone()));

    // Start the HTTP control API in a background task.
    let api_server = Arc::new(
        ApiServer::new(node.clone(), control.clone())
            .with_agent_port(args.agent_port),
    );
    tokio::spawn(async move {
        if let Err(e) = api_server.run(api_listen_addr).await {
            tracing::error!(error = %e, "API server exited");
        }
    });

    // Create and run proxy.
    let proxy = Proxy::new(node.clone(), control.clone(), config.proxy.clone());

    // Spawn the idle-checker background task.
    let control_bg = control.clone();
    let node_bg = node.clone();
    tokio::spawn(async move {
        idle_checker(control_bg, node_bg).await;
    });

    // Run the proxy (blocks until interrupted).
    proxy.run().await?;

    Ok(())
}

/// Background task that periodically checks for idle VMs and scales them
/// to zero.
async fn idle_checker(control: Arc<Control>, node: Arc<Node>) {
    let check_interval = std::time::Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;

        let idle = control.idle_vms();
        if idle.is_empty() {
            continue;
        }

        for vm_id in idle {
            tracing::info!(vm_id = %vm_id, "auto-pausing idle VM");
            match node.scale_to_zero(&vm_id).await {
                Ok(snapshot) => {
                    control.set_snapshot(&vm_id, snapshot.state_path.to_string_lossy().into_owned());
                }
                Err(e) => {
                    tracing::warn!(vm_id = %vm_id, error = %e, "failed to auto-pause");
                }
            }
        }
    }
}
