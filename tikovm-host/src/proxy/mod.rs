//! TCP proxy with wake-on-connect (design §11). The data-plane forwarder that
//! routes external traffic to a VM's workload port, waking the VM
//! (restore-on-demand) on the first byte if it is suspended.
//!
//! This is the host-side complement to the guest's idle evaluator: the guest
//! asks the host to suspend when idle; the proxy wakes the VM on the next
//! inbound connection. Together they form the scale-to-zero loop.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use tikovm_protocol::vm::VmId;

use crate::node::Node;

pub struct TcpProxy {
    node: Arc<Node>,
    listen: SocketAddr,
    target_vm: VmId,
    target_port: u16,
}

impl TcpProxy {
    pub fn new(node: Arc<Node>, listen: SocketAddr, target_vm: VmId, target_port: u16) -> Self {
        Self { node, listen, target_vm, target_port }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.listen).await?;
        info!(addr = %self.listen, target_vm = %self.target_vm, target_port = self.target_port, "TCP proxy listening");
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "proxy accept failed");
                    continue;
                }
            };
            let node = self.node.clone();
            let vm = self.target_vm.clone();
            let port = self.target_port;
            tokio::spawn(async move {
                if let Err(e) = handle(client, node, vm, port).await {
                    warn!(error = %e, %peer, "proxy connection ended");
                }
            });
        }
    }
}

async fn handle(
    mut client: TcpStream,
    node: Arc<Node>,
    vm_id: VmId,
    target_port: u16,
) -> Result<(), String> {
    // Wake the VM if it's suspended (scale-to-zero wake-on-connect).
    if let Err(e) = node.ensure_running(&vm_id).await {
        return Err(format!("wake {vm_id} failed: {e}"));
    }
    let guest_ip = node
        .control()
        .get(&vm_id)
        .and_then(|rec| rec.read().ok().and_then(|g| g.guest_ip))
        .ok_or_else(|| format!("no guest ip for {vm_id}"))?;

    // Connect to the in-VM workload. Retry briefly — the guest may still be
    // resuming from snapshot when the connection arrives.
    let mut backend = retry_connect((guest_ip, target_port), 40, 100).await?;

    // Splice bidirectionally until either side closes.
    let (mut cr, mut cw) = client.split();
    let (mut br, mut bw) = backend.split();
    let (c2b, b2c) = tokio::join!(
        async { tokio::io::copy(&mut cr, &mut bw).await },
        async { tokio::io::copy(&mut br, &mut cw).await },
    );
    let _ = (c2b, b2c);
    Ok(())
}

async fn retry_connect(addr: (std::net::IpAddr, u16), attempts: u32, interval_ms: u64) -> Result<TcpStream, String> {
    let mut last = String::from("no attempt");
    for _ in 0..attempts {
        match TcpStream::connect(addr).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            }
        }
    }
    Err(format!("connect {addr:?}: {last}"))
}
