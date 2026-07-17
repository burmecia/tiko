//! Host communication over the TAP network (HTTP) + `/proc/net` self-observation.
//!
//! This is the HTTP-over-TAP implementation of [`crate::idle::HostComm`]: the
//! guest counts established connections to its workload port by reading
//! `/proc/net/tcp` (the idle signal) and POSTs a suspend-request to the host's
//! control API at the TAP gateway when sustained-idle. It matches the proven
//! `tikoguest` scaler pattern.
//!
//! (The design's vsock control channel — §7 — is the future hardening; this
//! HTTP path delivers the same scale-to-zero behavior today and degrades to it
//! even once vsock lands.)

use std::sync::Arc;
use std::time::Duration;

use tikovm_protocol::rpc::NetworkStats;

use crate::idle::HostComm;

/// Default host control API port (the gateway is the host).
const HOST_API_PORT: u16 = 9000;

pub struct HttpHostLink {
    vm_id: String,
    workload_port: u16,
    host_api: String, // e.g. "http://172.16.1.1:9000"
    own_ip: String,   // e.g. "172.16.1.2"
}

impl HttpHostLink {
    /// Discover own eth0 IPv4 + default gateway and build the link.
    pub async fn discover(vm_id: String, workload_port: u16) -> Result<Self, String> {
        let own_ip = own_eth0_ipv4().await.ok_or_else(|| "no eth0 IPv4".to_string())?;
        let gw = default_gateway().await.ok_or_else(|| "no default gateway".to_string())?;
        Ok(Self {
            vm_id,
            workload_port,
            host_api: format!("http://{gw}:{HOST_API_PORT}"),
            own_ip,
        })
    }

    pub fn vm_id(&self) -> &str {
        &self.vm_id
    }
}

#[async_trait::async_trait]
impl HostComm for HttpHostLink {
    fn vm_id(&self) -> &str {
        &self.vm_id
    }

    /// Idle signal: count established connections to the workload port. Zero
    /// connections => idle (we report a large `last_data_age_secs` so
    /// [`NetworkStats::is_idle`] is true).
    async fn network_stats(&self) -> NetworkStats {
        let conns = count_established_to_port(self.workload_port);
        if conns == 0 {
            NetworkStats { established_conns: 0, last_data_age_secs: 999, bytes_in: 0, bytes_out: 0 }
        } else {
            NetworkStats { established_conns: conns as u64, last_data_age_secs: 0, bytes_in: 0, bytes_out: 0 }
        }
    }

    async fn request_suspend(&self) {
        let url = format!("{}/vms/by-ip/{}/suspend-request", self.host_api, self.own_ip);
        // Fire-and-forget; best-effort with a short timeout.
        let _ = http_post(&url, &format!("{{\"vm_id\":\"{}\"}}", self.vm_id)).await;
    }

    async fn report_health(&self, _healthy: bool) {}
}

/// Count TCP connections in ESTABLISHED state whose LOCAL port == `port`, by
/// parsing `/proc/net/tcp`. Pure over the file contents (testable).
pub fn count_established_to_port(port: u16) -> u32 {
    let text = match std::fs::read_to_string("/proc/net/tcp") {
        Ok(t) => t,
        Err(_) => return 0,
    };
    count_established_in(&text, port)
}

/// Pure parser: count ESTABLISHED rows in a `/proc/net/tcp`-format blob whose
/// local port == `port`.
pub fn count_established_in(proc_net_tcp: &str, port: u16) -> u32 {
    let want_port = format!("{:04X}", port);
    let mut n = 0;
    for line in proc_net_tcp.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let _sl = fields.next();
        let Some(local) = fields.next() else { continue };
        let _rem = fields.next();
        let Some(st) = fields.next() else { continue };
        // local is "HEXIP:HEXPORT"; state "01" == TCP_ESTABLISHED.
        let local_port = local.rsplit(':').next().unwrap_or("");
        if st == "01" && local_port.eq_ignore_ascii_case(&want_port) {
            n += 1;
        }
    }
    n
}

async fn own_eth0_ipv4() -> Option<String> {
    let out = tokio::process::Command::new("ip")
        .arg("-4").arg("-o").arg("addr").arg("show").arg("eth0")
        .output().await.ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    // line: "2: eth0    inet 172.16.1.2/24 ..."
    s.lines()
        .find_map(|l| l.split("inet ").nth(1))
        .and_then(|rest| rest.split('/').next().map(|s| s.trim().to_string()))
}

async fn default_gateway() -> Option<String> {
    let out = tokio::process::Command::new("ip")
        .arg("route").arg("show").arg("default")
        .output().await.ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    // "default via 172.16.1.1 dev eth0 ..."
    s.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        if it.next() == Some("default") && it.next() == Some("via") {
            it.next().map(|s| s.to_string())
        } else {
            None
        }
    })
}

/// Minimal blocking-ish HTTP/1.1 POST (fire-and-forget). `url` is
/// `http://host:port/path`.
async fn http_post(url: &str, body: &str) -> Result<(), String> {
    let rest = url.strip_prefix("http://").ok_or("bad url")?;
    let (host_port, path) = rest.split_once('/').unwrap_or((rest, "/"));
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().unwrap_or(80)),
        None => (host_port, 80),
    };
    let req = format!(
        "POST /{path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\n\
         Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    let mut stream = tokio::net::TcpStream::connect((host, port)).await.map_err(|e| e.to_string())?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.write_all(req.as_bytes())).await;
    let mut buf = [0u8; 128];
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    Ok(())
}

// Keep an Arc<HttpHostLink> handy for the type that idle::IdleEvaluator expects.
impl HttpHostLink {
    pub fn into_host_comm(self) -> Arc<dyn HostComm> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_established_to_port() {
        let proc = "  sl  local_address rem_address   st\n\
                    0: 01000016:1F90 01000001:C8A4 01\n\
                    1: 0100000A:2328 673A9F0A:B4E2 01\n\
                    2: 01000016:1F90 02000001:D1E2 06\n";
        // port 0x1F90 = 8080; two rows but only one ESTABLISHED (st=01).
        assert_eq!(count_established_in(proc, 8080), 1);
        assert_eq!(count_established_in(proc, 9000), 1);
        assert_eq!(count_established_in(proc, 22), 0);
    }
}
