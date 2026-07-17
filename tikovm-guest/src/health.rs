//! Health monitor (design section 4.3). Runs the manifest's `[health]` probe
//! periodically and reports the result to the host over the vsock control
//! channel (`HostComm::report_health`).
//!
//! Probe kinds: `http` (GET, 2xx/3xx = healthy), `tcp` (connect succeeds),
//! `exec` (exit 0), `none` (no-op).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::Notify;
use tracing::warn;

use tikovm_protocol::manifest::HealthProbe;

use crate::idle::HostComm;

pub struct HealthMonitor {
    probe: HealthProbe,
    host: Arc<dyn HostComm>,
}

impl HealthMonitor {
    pub fn new(probe: HealthProbe, host: Arc<dyn HostComm>) -> Self {
        Self { probe, host }
    }

    /// Run the probe loop until cancelled.
    pub async fn run(self: Arc<Self>, cancel: Arc<Notify>) {
        let interval = Duration::from_secs(self.probe.interval_secs().max(1));
        loop {
            tokio::select! {
                _ = cancel.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            let healthy = check(&self.probe).await;
            self.host.report_health(healthy).await;
        }
    }
}

/// Evaluate the probe once. `None` => healthy (no probe = don't fail health).
async fn check(probe: &HealthProbe) -> bool {
    match probe {
        HealthProbe::None => true,
        HealthProbe::Tcp { port, .. } => TcpStream::connect(("127.0.0.1", *port)).await.is_ok(),
        HealthProbe::Exec { cmd, .. } => match Command::new("sh").arg("-c").arg(cmd).status().await {
            Ok(s) => s.success(),
            Err(e) => {
                warn!(error = %e, "health exec probe failed");
                false
            }
        },
        HealthProbe::Http { path, port, .. } => http_healthy(*port, path).await,
    }
}

/// Minimal HTTP/1.1 GET; healthy on a 2xx/3xx status line.
async fn http_healthy(port: u16, path: &str) -> bool {
    let mut stream = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(s) => s,
        Err(_) => return false,
    };
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    if tokio::io::AsyncWriteExt::write_all(&mut stream, req.as_bytes()).await.is_err() {
        return false;
    }
    let mut buf = [0u8; 64];
    let n = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(_) => return false,
    };
    // "HTTP/1.1 200 ..." -> parse the status code.
    let line = String::from_utf8_lossy(&buf[..n]);
    let code = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok());
    matches!(code, Some(c) if (200..400).contains(&c))
}
