//! `tikod` entry point — the Tiko compute control plane.
//!
//! ```text
//! Usage: tikod [OPTIONS]
//!
//! Options:
//!   --data-dir <PATH>      Directory for snapshots and runtime artifacts
//!   --listen <ADDR>        Address for the PG proxy to listen on (default: 127.0.0.1:5432)
//!   --http-listen <ADDR>   Address for the PostgREST HTTP proxy (default: 0.0.0.0:3000)
//!   --api-listen <ADDR>    Address for the HTTP control API (default: 127.0.0.1:9000)
//!   --agent-port <PORT>    Guest tikoguest agent port for /vms/{id}/db/* (default: 9000)
//!   --assets-dir <PATH>    Kernel/rootfs/initramfs for preset VmConfig (default: tikod/assets)
//!   --backend <NAME>       Force a VMM backend: auto|vz|firecracker (default: auto)
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use tikod::api::ApiServer;
use tikod::config::TikodConfig;
use tikod::control::Control;
use tikod::http_proxy::HttpProxy;
use tikod::node::Node;
use tikod::proxy::{Proxy, ProxyConfig};
use tikod::vmm::{Vmm, default_vmm};

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

    /// Address for the PostgREST HTTP reverse proxy (fans out by
    /// `X-Tiko-Endpoint` header to each VM's in-guest PostgREST).
    #[arg(long, default_value = "0.0.0.0:3000")]
    http_listen: String,

    /// Address for the HTTP control API (VM lifecycle).
    #[arg(long, default_value = "0.0.0.0:9000")]
    api_listen: String,

    /// Port the in-guest `tikoguest` agent listens on (used for /vms/{id}/db/*).
    #[arg(long, default_value_t = 9000)]
    agent_port: u16,

    /// Directory containing kernel/rootfs/initramfs assets for the preset
    /// VmConfig (used by `PUT /vms` and `POST /vms/provision`).
    #[arg(long, default_value = "tikod/assets")]
    assets_dir: String,
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
    let http_listen_addr: SocketAddr = args.http_listen.parse()?;
    let api_listen_addr: SocketAddr = args.api_listen.parse()?;
    let data_dir = PathBuf::from(&args.data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let config = TikodConfig {
        data_dir: data_dir.clone(),
        proxy: ProxyConfig {
            listen_addr,
            ..Default::default()
        },
    };

    // HTTP proxy reuses the PG proxy's wake/dev settings with its own listen
    // address (default 0.0.0.0:3000).
    let http_config = ProxyConfig {
        listen_addr: http_listen_addr,
        ..Default::default()
    };

    tracing::info!(
        data_dir = %config.data_dir.display(),
        listen = %config.proxy.listen_addr,
        http_listen = %http_config.listen_addr,
        api_listen = %api_listen_addr,
        platform = std::env::consts::OS,
        "starting tikod"
    );

    // Create the VMM backend (platform-specific).
    let snapshot_dir = data_dir.join("snapshots");
    let vmm: Arc<dyn Vmm> = Arc::from(default_vmm(snapshot_dir));

    // Create node (lifecycle manager).
    let node = Arc::new(Node::new(vmm, data_dir.join("snapshots")));

    // Create control plane.
    let control = Arc::new(Control::new());

    // Start the HTTP control API in a background task.
    let api_server = Arc::new(
        ApiServer::new(node.clone(), control.clone())
            .with_agent_port(args.agent_port)
            .with_assets_dir(&args.assets_dir),
    );
    tokio::spawn(async move {
        if let Err(e) = api_server.run(api_listen_addr).await {
            tracing::error!(error = %e, "API server exited");
        }
    });

    // Create and run the proxies.
    let pg_proxy = Proxy::new(node.clone(), control.clone(), config.proxy.clone());
    let http_proxy = HttpProxy::new(node.clone(), control.clone(), http_config);

    // Run both proxies until Ctrl+C / SIGINT.
    tokio::select! {
        result = pg_proxy.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "PG proxy exited");
            }
        }
        result = http_proxy.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP proxy exited");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received Ctrl+C, shutting down");
        }
    }

    Ok(())
}
