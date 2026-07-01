//! PostgreSQL wire-protocol proxy with wake-on-connect.
//!
//! Accepts client connections on a local port and forwards them to the
//! target VM's PostgreSQL backend. If the VM is paused (scaled to zero),
//! the proxy triggers a resume and holds the client socket until the VM
//! is ready, then transparently forwards.
//!
//! v1 is a transparent TCP proxy (no auth, no PG protocol parsing).
//! Connection holding works because PostgreSQL's `libpq` default
//! `connect_timeout=0` (wait indefinitely) — the proxy can stall the
//! startup handshake for seconds while the VM resumes.
//!
//! ```text
//! Client ──TCP──→ Proxy (listen :5432)
//!                   │
//!                   ├─ VM running?  ──yes──→ forward bytes (tokio::io::copy)
//!                   │
//!                   └─ VM paused?   ──resume──→ wait ──→ forward bytes
//!                                          (hold client socket)
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::control::Control;
use crate::node::Node;
use crate::vmm::VmId;

/// Default PostgreSQL port.
const PG_PORT: u16 = 5432;

/// Connection forward target.
#[derive(Debug, Clone)]
pub enum ForwardTarget {
    /// Forward to a specific VM by ID (looked up in the registry).
    Vm(VmId),
    /// Forward to a fixed address (e.g. for testing without a VM).
    Direct(SocketAddr),
}

/// Configuration for the proxy.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address to listen on for client connections.
    pub listen_addr: SocketAddr,
    /// Default target when no VM routing is specified.
    pub default_target: ForwardTarget,
    /// Maximum time to wait for a VM to resume before giving up (seconds).
    pub resume_timeout_secs: u64,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:5432".parse().unwrap(),
            default_target: ForwardTarget::Direct("127.0.0.1:15432".parse().unwrap()),
            resume_timeout_secs: 30,
        }
    }
}

/// The PostgreSQL proxy server.
pub struct Proxy {
    node: Arc<Node>,
    control: Arc<Control>,
    config: ProxyConfig,
}

impl Proxy {
    pub fn new(node: Arc<Node>, control: Arc<Control>, config: ProxyConfig) -> Self {
        Self {
            node,
            control,
            config,
        }
    }

    /// Run the proxy server. Accepts connections in a loop.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "proxy listening for PG connections");

        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    let node = self.node.clone();
                    let control = self.control.clone();
                    let config = self.config.clone();

                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_connection(client_stream, client_addr, &node, &control, &config)
                                .await
                        {
                            error!(client = %client_addr, error = %e, "connection handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "accept failed");
                }
            }
        }
    }
}

/// Handle a single client connection: determine target, wake VM if needed,
/// forward bytes bidirectionally.
async fn handle_connection(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    node: &Node,
    control: &Control,
    config: &ProxyConfig,
) -> io::Result<()> {
    debug!(client = %client_addr, "new connection");

    // Determine the forward target.
    let backend_addr = resolve_target(&config.default_target, node, control).await?;

    // Connect to the backend (VM's PG port).
    debug!(client = %client_addr, backend = %backend_addr, "connecting to backend");
    let backend_stream = TcpStream::connect(backend_addr).await?;
    backend_stream.set_nodelay(true)?;

    // Forward bytes in both directions until either side closes.
    let (mut client_read, mut client_write) = client_stream.into_split();
    let (mut backend_read, mut backend_write) = backend_stream.into_split();

    let client_to_backend = async {
        io::copy(&mut client_read, &mut backend_write).await?;
        backend_write.shutdown().await
    };

    let backend_to_client = async {
        io::copy(&mut backend_read, &mut client_write).await?;
        client_write.shutdown().await
    };

    debug!(client = %client_addr, "proxying established");
    let _: ((), ()) = tokio::try_join!(client_to_backend, backend_to_client)?;

    debug!(client = %client_addr, "connection closed");
    Ok(())
}

/// Resolve a [`ForwardTarget`] to a socket address. For VM targets, this
/// triggers wake-on-connect: if the VM is paused, resume it first.
async fn resolve_target(
    target: &ForwardTarget,
    node: &Node,
    control: &Control,
) -> io::Result<SocketAddr> {
    match target {
        ForwardTarget::Direct(addr) => Ok(*addr),
        ForwardTarget::Vm(vm_id) => {
            // Wake-on-connect: ensure the VM is running.
            match node.state(vm_id).await {
                Ok(crate::vmm::VmState::Running) => {
                    debug!(vm_id = %vm_id, "VM already running");
                }
                Ok(crate::vmm::VmState::Paused) => {
                    info!(vm_id = %vm_id, "VM paused — triggering resume (wake-on-connect)");
                    node.ensure_running(vm_id).await.map_err(|e| {
                        io::Error::other(format!("failed to resume VM {vm_id}: {e}"))
                    })?;
                    info!(vm_id = %vm_id, "VM resumed");
                }
                Ok(state) => {
                    return Err(io::Error::other(format!(
                        "VM {vm_id} is in state {state}, cannot forward"
                    )));
                }
                Err(e) => {
                    return Err(io::Error::other(format!(
                        "cannot query VM {vm_id} state: {e}"
                    )));
                }
            }

            // Look up the guest IP.
            let guest_ip = node.guest_ip(vm_id).await.map_err(|e| {
                io::Error::other(format!("cannot get guest IP for {vm_id}: {e}"))
            })?;

            let ip = guest_ip.unwrap_or_else(|| {
                warn!(vm_id = %vm_id, "no guest IP discovered, using localhost");
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            });

            // Notify control plane of activity.
            control.on_connect(vm_id);

            Ok(SocketAddr::new(ip, PG_PORT))
        }
    }
}
