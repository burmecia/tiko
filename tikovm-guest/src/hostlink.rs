//! Host communication over virtio-vsock (design §7).
//!
//! [`VsockHostLink`] implements [`crate::idle::HostComm`] by connecting
//! (AF_VSOCK) to the host (CID 2) on [`HOST_CTRL_PORT`] and exchanging framed
//! JSON RPCs:
//! - `GetNetworkStats` — the host returns VM-scoped traffic stats (it sees the
//!   proxy's forwarded connections; authoritative, no guest port config).
//! - `Suspend` — the guest signals scale-to-zero.
//!
//! The host derives the target VM from the per-VM AF_UNIX socket the connection
//! arrives on, so the guest carries no vm_id. The `vsock` crate is synchronous;
//! each RPC runs on `spawn_blocking` (the control channel is low-frequency).

use std::io::{Read, Write};
use std::sync::Arc;

use tikovm_protocol::codec;
use tikovm_protocol::rpc::{GuestToHost, HostReply, NetworkStats, HOST_CID, HOST_CTRL_PORT};

use crate::idle::HostComm;

pub struct VsockHostLink;

impl VsockHostLink {
    pub fn new() -> Self {
        Self
    }

    pub fn into_host_comm(self) -> Arc<dyn HostComm> {
        Arc::new(self)
    }
}

impl Default for VsockHostLink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl HostComm for VsockHostLink {
    fn vm_id(&self) -> &str {
        "vsock"
    }

    async fn network_stats(&self) -> NetworkStats {
        let stats = tokio::task::spawn_blocking(|| -> Option<NetworkStats> {
            match vsock_rpc(&GuestToHost::GetNetworkStats) {
                Ok(HostReply::Stats(s)) => Some(s),
                Ok(other) => {
                    tracing::warn!(?other, "unexpected reply to GetNetworkStats");
                    None
                }
                Err(e) => {
                    tracing::debug!(error = %e, "vsock GetNetworkStats failed (treating as busy)");
                    None
                }
            }
        })
        .await
        .ok()
        .flatten();
        // On failure return a non-idle stats (last_data_age_secs:0) so we never
        // suspend a VM just because the control channel had a hiccup.
        stats.unwrap_or(NetworkStats { established_conns: 1, last_data_age_secs: 0, bytes_in: 0, bytes_out: 0 })
    }

    async fn request_suspend(&self) {
        let _ = tokio::task::spawn_blocking(|| vsock_rpc(&GuestToHost::Suspend)).await;
    }

    async fn report_health(&self, healthy: bool) {
        let _ = tokio::task::spawn_blocking(move || vsock_rpc(&GuestToHost::HealthReport { healthy: Some(healthy) })).await;
    }
}

/// One request/response round-trip over a fresh vsock connection to the host.
fn vsock_rpc(req: &GuestToHost) -> std::io::Result<HostReply> {
    let addr = vsock::VsockAddr::new(HOST_CID, HOST_CTRL_PORT);
    let mut stream = vsock::VsockStream::connect(&addr)?;
    let payload = serde_json::to_vec(req).map_err(std::io::Error::other)?;
    write_frame(&mut stream, &payload)?;
    let reply_payload = read_frame(&mut stream)?;
    let reply: HostReply = serde_json::from_slice(&reply_payload).map_err(std::io::Error::other)?;
    Ok(reply)
}

fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let frame = codec::encode_frame_bytes(payload);
    w.write_all(&frame)?;
    w.flush()
}

fn read_frame<R: Read>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
