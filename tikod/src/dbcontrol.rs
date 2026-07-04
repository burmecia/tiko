//! Client for the in-guest `pgctl` agent.
//!
//! [`DbControl`] speaks HTTP/1.1 to the `pgctl` process running inside each VM
//! (default `:9000`), giving tikod Postgres lifecycle control (start/stop/
//! restart/reload) plus `postgresql.tiko.conf` read/write — all over the VM's
//! guest IP. Raw HTTP/1.1, consistent with [`crate::api::ApiClient`] and the
//! Firecracker backend client (no external HTTP library).
//!
//! ```text
//! tikod ApiServer ──HTTP──→ guest_ip:9000 ──→ pgctl ──→ pg_ctl
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::vmm::VmmError;

/// Default port the `pgctl` agent listens on inside each guest.
pub const DEFAULT_AGENT_PORT: u16 = 9000;

/// Errors from a DB control operation. Covers VM resolution, transport, and
/// agent-side failures so the HTTP layer can map each to the right status.
#[derive(Debug, thiserror::Error)]
pub enum DbControlError {
    /// VM lookup failed (unknown id, bad state) — forwarded from the Vmm layer.
    #[error(transparent)]
    Vm(#[from] VmmError),
    /// Couldn't reach the agent or read its response (network/parse) → 502.
    #[error("agent transport error: {0}")]
    Transport(String),
    /// The agent returned a non-2xx response; `kind`/`message` are forwarded
    /// verbatim so the original cause (e.g. `not_initialized`) survives.
    #[error("agent responded {status}: {message}")]
    Agent {
        status: u16,
        kind: String,
        message: String,
    },
}

pub type DbResult<T> = Result<T, DbControlError>;

/// How to stop Postgres. Mirrors `pg_ctl -m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StopMode {
    Smart,
    #[default]
    Fast,
    Immediate,
}

/// `GET /health` from the agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgHealth {
    pub status: String,
    pub initialized: bool,
    pub running: bool,
}

/// `GET /pg/status` from the agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgStatus {
    pub initialized: bool,
    pub running: bool,
    pub ready: bool,
    pub pid: Option<i32>,
    pub version: Option<String>,
    pub data_dir: String,
    pub config_file: String,
}

/// HTTP client for the in-guest `pgctl` agent.
#[derive(Clone)]
pub struct DbControl {
    agent: SocketAddr,
}

impl DbControl {
    pub fn new(agent: SocketAddr) -> Self {
        Self { agent }
    }

    /// Build a client for a guest IP at the given agent port.
    pub fn for_guest(guest_ip: std::net::IpAddr, port: u16) -> Self {
        Self::new(SocketAddr::new(guest_ip, port))
    }

    pub fn agent_addr(&self) -> SocketAddr {
        self.agent
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// `GET /health`.
    pub async fn health(&self) -> DbResult<PgHealth> {
        let v = self.get_json("/health").await?;
        decode(v)
    }

    /// `GET /pg/status`.
    pub async fn status(&self) -> DbResult<PgStatus> {
        let v = self.get_json("/pg/status").await?;
        decode(v)
    }

    /// `POST /pg/start`.
    pub async fn start(&self) -> DbResult<()> {
        self.post_empty("/pg/start").await
    }

    /// `POST /pg/stop` with an optional mode (default fast).
    pub async fn stop(&self, mode: StopMode) -> DbResult<()> {
        let body = serde_json::json!({"mode": mode});
        self.send("POST", "/pg/stop", Some(&body)).await?;
        Ok(())
    }

    /// `POST /pg/restart`.
    pub async fn restart(&self) -> DbResult<()> {
        self.post_empty("/pg/restart").await
    }

    /// `POST /pg/reload`.
    pub async fn reload(&self) -> DbResult<()> {
        self.post_empty("/pg/reload").await
    }

    /// `POST /pg/init` — run `initdb` (wipe if `force`). The agent refuses when
    /// the cluster already exists (without `force`) or while postgres is running.
    pub async fn init(&self, force: bool) -> DbResult<()> {
        let body = serde_json::json!({"force": force});
        self.send("POST", "/pg/init", Some(&body)).await?;
        Ok(())
    }

    // ── Config ─────────────────────────────────────────────────────────────

    /// `GET /pg/config` → the parsed `postgresql.tiko.conf` settings.
    pub async fn get_config(&self) -> DbResult<BTreeMap<String, String>> {
        let v = self.get_json("/pg/config").await?;
        let settings = v
            .get("settings")
            .ok_or_else(|| DbControlError::Transport("missing settings in response".into()))?
            .clone();
        serde_json::from_value(settings).map_err(|e| {
            DbControlError::Transport(format!("failed to decode settings: {e}"))
        })
    }

    /// `PUT /pg/config` — merge `settings` into the override file and reload.
    pub async fn set_config(&self, settings: &BTreeMap<String, String>) -> DbResult<()> {
        let body = serde_json::json!({"settings": settings});
        self.send("PUT", "/pg/config", Some(&body)).await?;
        Ok(())
    }

    // ── Transport ──────────────────────────────────────────────────────────

    async fn get_json(&self, path: &str) -> DbResult<serde_json::Value> {
        self.send("GET", path, None).await
    }

    async fn post_empty(&self, path: &str) -> DbResult<()> {
        self.send("POST", path, None).await?;
        Ok(())
    }

    /// Core HTTP/1.1 request. Returns the parsed JSON body for 2xx, or an
    /// [`DbControlError::Agent`] carrying the agent's `kind`/`message`.
    async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> DbResult<serde_json::Value> {
        let body_bytes = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {len}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            host = self.agent,
            len = body_bytes.len(),
            body = body_bytes,
        );

        let mut stream = TcpStream::connect(self.agent).await.map_err(|e| {
            DbControlError::Transport(format!("connect to agent: {e}"))
        })?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| DbControlError::Transport(format!("write to agent: {e}")))?;

        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| DbControlError::Transport(format!("read agent response: {e}")))?;

        let text = String::from_utf8_lossy(&buf);
        let (status, body_str) = split_response(&text)?;

        if (200..300).contains(&status) {
            if body_str.is_empty() {
                return Ok(serde_json::Value::Null);
            }
            return serde_json::from_str(body_str).map_err(|e| {
                DbControlError::Transport(format!("JSON parse error: {e}"))
            });
        }

        // Forward the agent's structured error verbatim.
        let (kind, message) = decode_error_fields(body_str);
        Err(DbControlError::Agent {
            status,
            kind,
            message,
        })
    }
}

/// Split an HTTP response into `(status, body_str)`.
fn split_response(text: &str) -> DbResult<(u16, &str)> {
    let header_end = text
        .find("\r\n\r\n")
        .ok_or_else(|| DbControlError::Transport("malformed HTTP response".into()))?;
    let header_str = &text[..header_end];
    let body_str = &text[header_end + 4..];
    let status = header_str
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| DbControlError::Transport("malformed HTTP status line".into()))?;
    Ok((status, body_str))
}

/// Extract `(kind, message)` from an agent error body
/// `{"error":{"kind":...,"message":...}}`, falling back to the raw body.
fn decode_error_fields(body_str: &str) -> (String, String) {
    let fallback = (String::from("agent_error"), body_str.to_string());
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body_str) else {
        return fallback;
    };
    let Some(err) = v.get("error") else {
        return fallback;
    };
    let kind = err
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("agent_error")
        .to_string();
    let message = err
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or(body_str)
        .to_string();
    (kind, message)
}

fn decode<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> DbResult<T> {
    serde_json::from_value(v).map_err(|e| DbControlError::Transport(format!("decode error: {e}")))
}
