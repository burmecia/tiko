//! Data-plane proxy with wake-on-connect (design §11).
//!
//! Routes external traffic to a VM's workload port, waking the VM
//! (restore-on-demand) on the first connection if it is suspended. This is the
//! host-side complement to the guest's idle evaluator: the guest asks the host
//! to suspend when idle; the proxy wakes the VM on the next inbound connection.
//!
//! Two routing modes:
//! - **HTTP header routing** (default): peeks the HTTP request head and selects
//!   the target VM by the `X-Tiko-Endpoint: vm-N` header (generalizes the
//!   `tiko.endpoint` trick), falling back to a configured default VM when the
//!   header is absent. The workload port comes from the VM's manifest
//!   `[expose].http_port` (else the default port).
//! - **Fixed TCP target**: a single configured VM+port (for non-HTTP / simple
//!   demos), selected when no HTTP head is detected.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use tikovm_protocol::vm::VmId;

use crate::node::Node;

/// HTTP header used to select the target VM.
pub const ENDPOINT_HEADER: &str = "x-tiko-endpoint:";

pub struct Proxy {
    node: Arc<Node>,
    listen: SocketAddr,
    default_vm: Option<VmId>,
    default_port: u16,
}

impl Proxy {
    /// `default_vm` is the fallback when a request carries no routing header.
    pub fn new(node: Arc<Node>, listen: SocketAddr, default_vm: Option<VmId>, default_port: u16) -> Self {
        Self { node, listen, default_vm, default_port }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.listen).await?;
        info!(
            addr = %self.listen,
            default_vm = ?self.default_vm,
            default_port = self.default_port,
            "proxy listening (HTTP header routing + wake-on-connect)"
        );
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "proxy accept failed");
                    continue;
                }
            };
            let node = self.node.clone();
            let default_vm = self.default_vm.clone();
            let default_port = self.default_port;
            tokio::spawn(async move {
                if let Err(e) = handle(client, node, default_vm, default_port).await {
                    warn!(error = %e, %peer, "proxy connection ended");
                }
            });
        }
    }
}

async fn handle(
    mut client: TcpStream,
    node: Arc<Node>,
    default_vm: Option<VmId>,
    default_port: u16,
) -> Result<(), String> {
    // Peek the HTTP request head to extract the routing header.
    let head = read_http_head(&mut client).await?;
    crate::metrics::record_proxy_connection();

    // Resolve the target VM + workload port.
    let (vm_id, port) = resolve_target(&head, &node, default_vm.clone(), default_port)?;

    // Wake the VM if it's suspended (scale-to-zero wake-on-connect).
    if let Err(e) = node.ensure_running(&vm_id).await {
        return Err(format!("wake {vm_id} failed: {e}"));
    }
    let guest_ip = node
        .control()
        .get(&vm_id)
        .and_then(|rec| rec.read().ok().and_then(|g| g.guest_ip))
        .ok_or_else(|| format!("no guest ip for {vm_id}"))?;

    // Connect to the in-VM workload. Retry briefly while the guest resumes.
    let mut backend = retry_connect((guest_ip, port), 40, 100).await?;
    backend.write_all(&head).await.map_err(|e| e.to_string())?;
    backend.flush().await.map_err(|e| e.to_string())?;

    // Splice the rest bidirectionally until either side closes.
    let (mut cr, mut cw) = client.split();
    let (mut br, mut bw) = backend.split();
    let (c2b, b2c) = tokio::join!(
        async { tokio::io::copy(&mut cr, &mut bw).await },
        async { tokio::io::copy(&mut br, &mut cw).await },
    );
    let _ = (c2b, b2c);
    Ok(())
}

/// Read the HTTP request head (up to and including the blank line) plus any
/// trailing bytes that arrived in the same read. Stops at `\r\n\r\n` or a
/// non-HTTP first byte (returns what was read for the fixed-target fallback).
async fn read_http_head(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 2048];
    loop {
        let n = stream.read(&mut tmp).await.map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 65536 {
            return Err("HTTP request head too large".into());
        }
        // If the first byte isn't an uppercase ASCII letter (a method), treat as
        // non-HTTP and fall through to the default target with what we read.
        if !buf.is_empty() && !(buf[0] as char).is_ascii_uppercase() {
            break;
        }
    }
    Ok(buf)
}

/// Pick the target VM + port from the request head, else the configured default.
fn resolve_target(
    head: &[u8],
    node: &Node,
    default_vm: Option<VmId>,
    default_port: u16,
) -> Result<(VmId, u16), String> {
    // Look for `X-Tiko-Endpoint: <vm-id>`.
    let header_vm = extract_header(head, ENDPOINT_HEADER);
    let candidate = header_vm.or_else(|| default_vm.clone());
    let vm_id = candidate.ok_or_else(|| "no routing header and no default VM".to_string())?;
    if node.control().get(&vm_id).is_none() {
        return Err(format!("unknown endpoint VM: {vm_id}"));
    }
    // Workload port from the VM's manifest [expose], else the default.
    let port = node
        .control()
        .get(&vm_id)
        .and_then(|rec| {
            rec.read().ok().and_then(|g| {
                g.spec.manifest.as_ref().and_then(|m| m.expose.as_ref()).map(|e| e.http_port)
            })
        })
        .unwrap_or(default_port);
    Ok((vm_id, port))
}

/// Case-insensitive search for a `Name: value` header; returns the trimmed value.
fn extract_header(head: &[u8], name_lower: &str) -> Option<VmId> {
    let text = String::from_utf8_lossy(head);
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix(name_lower) {
            return Some(rest.trim().to_string());
        }
    }
    None
}

async fn retry_connect(addr: (IpAddr, u16), attempts: u32, interval_ms: u64) -> Result<TcpStream, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_endpoint_header_case_insensitive() {
        let head = b"GET /hello HTTP/1.1\r\nHost: x\r\nX-Tiko-Endpoint: vm-7\r\n\r\n";
        assert_eq!(extract_header(head, ENDPOINT_HEADER).as_deref(), Some("vm-7"));
    }

    #[test]
    fn returns_none_when_no_header() {
        let head = b"GET /hello HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(extract_header(head, ENDPOINT_HEADER).is_none());
    }
}
