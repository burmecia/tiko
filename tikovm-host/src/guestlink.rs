//! Per-VM vsock control server (host side, design §7).
//!
//! For each VM, binds the AF_UNIX socket at `{vsock_uds}_HOST_CTRL_PORT`.
//! Firecracker forwards the guest's AF_VSOCK connection to (CID 2,
//! HOST_CTRL_PORT) to this socket, so the host derives the target VM from
//! *which socket* the connection arrived on — the messages carry no vm_id.
//!
//! Serves the guest's idle-evaluator needs:
//! - `GetNetworkStats` — VM-scoped traffic, computed from the host's
//!   `/proc/net/tcp` (the proxy's forwarded connections to the guest appear
//!   here; this is the authoritative source the design calls for).
//! - `Suspend` / `Shutdown` — the guest signaling scale-to-zero / completion.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tracing::{info, warn};

use tikovm_protocol::codec;
use tikovm_protocol::rpc::{GuestToHost, HostReply, NetworkStats, HOST_CTRL_PORT};
use tikovm_protocol::vm::VmId;

use crate::node::Node;

pub struct GuestLink {
    node: Arc<Node>,
    vm_id: VmId,
    uds: PathBuf,
}

impl GuestLink {
    /// `uds_base` is the VM's vsock UDS path (from `Vmm::vsock_uds_path`).
    pub fn new(node: Arc<Node>, vm_id: VmId, uds_base: PathBuf) -> Self {
        let uds = PathBuf::from(format!("{}_{}", uds_base.display(), HOST_CTRL_PORT));
        Self { node, vm_id, uds }
    }

    /// Bind and serve until the process exits. Idempotent on the socket file.
    pub async fn run(self) -> std::io::Result<()> {
        let _ = std::fs::remove_file(&self.uds);
        let listener = UnixListener::bind(&self.uds)?;
        info!(vm_id = %self.vm_id, uds = %self.uds.display(), "guestlink listening");
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "guestlink accept failed");
                    continue;
                }
            };
            let node = self.node.clone();
            let vm = self.vm_id.clone();
            tokio::spawn(async move {
                if let Err(e) = handle(&mut stream, &node, &vm).await {
                    warn!(error = %e, "guestlink conn ended");
                }
            });
        }
    }
}

async fn handle(
    stream: &mut tokio::net::UnixStream,
    node: &Node,
    vm_id: &VmId,
) -> Result<(), String> {
    let payload = read_frame(stream).await?;
    let msg: GuestToHost = serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
    let reply = match msg {
        GuestToHost::GetNetworkStats => HostReply::Stats(vm_network_stats(node, vm_id)),
        GuestToHost::Suspend => match node.freeze(vm_id).await {
            Ok(_) => HostReply::Suspended {
                pause_epoch: node.bump_pause_epoch(vm_id).unwrap_or(0),
            },
            Err(e) => HostReply::Error { message: e.to_string() },
        },
        GuestToHost::Shutdown => match node.destroy(vm_id).await {
            Ok(_) => HostReply::Ok,
            Err(e) => HostReply::Error { message: e.to_string() },
        },
        GuestToHost::Ready { workload, .. } => {
            info!(%vm_id, %workload, "guest ready");
            HostReply::Ok
        }
        GuestToHost::HealthReport { healthy } => {
            info!(%vm_id, ?healthy, "guest health report");
            HostReply::Ok
        }
    };
    let out = serde_json::to_vec(&reply).map_err(|e| e.to_string())?;
    write_frame(stream, &out).await
}

/// VM-scoped network stats from the host's view: count established connections
/// in `/proc/net/tcp` whose *remote* address is the guest IP (these are the
/// proxy's forwarded connections — the authoritative traffic signal).
fn vm_network_stats(node: &Node, vm_id: &VmId) -> NetworkStats {
    let ip = node
        .control()
        .get(vm_id)
        .and_then(|rec| rec.read().ok().and_then(|g| g.guest_ip));
    let conns = ip.map(count_conns_to_ip).unwrap_or(0);
    if conns == 0 {
        NetworkStats { established_conns: 0, last_data_age_secs: 999, bytes_in: 0, bytes_out: 0 }
    } else {
        NetworkStats { established_conns: conns, last_data_age_secs: 0, bytes_in: 0, bytes_out: 0 }
    }
}

/// Count ESTABLISHED rows in `/proc/net/tcp` whose remote IPv4 == `ip`.
pub fn count_conns_to_ip(ip: IpAddr) -> u64 {
    let IpAddr::V4(v4) = ip else { return 0 };
    let want = ip_to_le_hex(v4);
    let text = std::fs::read_to_string("/proc/net/tcp").unwrap_or_default();
    text.lines()
        .skip(1)
        .filter(|l| {
            let mut f = l.split_whitespace();
            f.next();
            f.next();
            let rem = match f.next() {
                Some(r) => r,
                None => return false,
            };
            let st = f.next().unwrap_or("");
            st == "01" && rem.get(..8).map(|r| r.eq_ignore_ascii_case(&want)).unwrap_or(false)
        })
        .count() as u64
}

fn ip_to_le_hex(ip: Ipv4Addr) -> String {
    let o = ip.octets();
    format!("{:02X}{:02X}{:02X}{:02X}", o[3], o[2], o[1], o[0])
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Vec<u8>, String> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.map_err(|e| e.to_string())?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
    Ok(buf)
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, payload: &[u8]) -> Result<(), String> {
    let frame = codec::encode_frame_bytes(payload);
    w.write_all(&frame).await.map_err(|e| e.to_string())?;
    w.flush().await.map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_le_hex_matches_proc_net_format() {
        // 172.16.1.2 -> little-endian hex "020110AC"
        assert_eq!(ip_to_le_hex(Ipv4Addr::new(172, 16, 1, 2)), "020110AC");
        assert_eq!(ip_to_le_hex(Ipv4Addr::new(10, 0, 0, 1)), "0100000A");
    }

    #[test]
    fn count_conns_parses_proc_net() {
        // rem_address 020110AC:1F90 = 172.16.1.2:8080, ESTABLISHED
        let proc = "  sl  local_address rem_address   st\n\
                    0: 01000016:C8A4 020110AC:1F90 01\n\
                    1: 0100000A:2328 673A9F0A:B4E2 06\n";
        let text = proc.to_string();
        let want = ip_to_le_hex(Ipv4Addr::new(172, 16, 1, 2));
        let n = text
            .lines()
            .skip(1)
            .filter(|l| {
                let mut f = l.split_whitespace();
                f.next();
                f.next();
                let rem = f.next().unwrap_or("");
                let st = f.next().unwrap_or("");
                st == "01" && rem.get(..8).map(|r| r.eq_ignore_ascii_case(&want)).unwrap_or(false)
            })
            .count();
        assert_eq!(n, 1);
    }
}
