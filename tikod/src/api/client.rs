//! HTTP client for the [`crate::api`] control API.
//!
//! Mirrors the [`Vmm`](crate::vmm::Vmm) trait surface so that callers can
//! drive the full VM lifecycle over HTTP without depending on a concrete VMM
//! backend. Used by `boot_test` for end-to-end testing and by external
//! orchestration tools.
//!
//! Like the Firecracker backend client, this uses raw HTTP/1.1 — no external
//! HTTP library. Error responses are mapped back into [`VmmError`] variants so
//! that existing `matches!`-based assertions keep working unchanged.

use std::net::{IpAddr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::vmm::{Snapshot, VmConfig, VmId, VmState, VmmError, VmmResult};

/// HTTP client for the tikod control API.
#[derive(Clone)]
pub struct ApiClient {
    addr: SocketAddr,
}

impl ApiClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// The address this client connects to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    // ── Vmm-trait surface ──────────────────────────────────────────────────

    /// `PUT /vms` — create and register a new VM (does not start it).
    pub async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId> {
        let body = serde_json::to_value(&config).map_err(io)?;
        let resp = self.request("PUT", "/vms", Some(&body)).await?;
        resp.get("vm_id")
            .and_then(|v| v.as_str().map(String::from))
            .ok_or_else(|| VmmError::Backend("response missing vm_id".into()))
    }

    /// `PUT /vms/{id}/start` — start a created or paused VM.
    pub async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.request("PUT", &path(vm_id, "start"), None).await?;
        Ok(())
    }

    /// `PUT /vms/{id}/pause` — freeze a running VM.
    pub async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.request("PUT", &path(vm_id, "pause"), None).await?;
        Ok(())
    }

    /// `PUT /vms/{id}/resume` — resume a paused VM.
    pub async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.request("PUT", &path(vm_id, "resume"), None).await?;
        Ok(())
    }

    /// `PUT /vms/{id}/snapshot` — snapshot a paused VM.
    pub async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        let resp = self.request("PUT", &path(vm_id, "snapshot"), None).await?;
        serde_json::from_value(resp).map_err(|e| {
            VmmError::Backend(format!("failed to decode snapshot response: {e}"))
        })
    }

    /// `PUT /vms/{id}/restore` — restore a VM from the snapshot stored in the
    /// tikod registry (set by a prior scale-to-zero). The snapshot descriptor
    /// lives server-side, so the client only supplies the vm_id (in the path).
    pub async fn restore_vm(&self, vm_id: &VmId) -> VmmResult<VmId> {
        let resp = self.request("PUT", &path(vm_id, "restore"), None).await?;
        resp.get("vm_id")
            .and_then(|v| v.as_str().map(String::from))
            .ok_or_else(|| VmmError::Backend("response missing vm_id".into()))
    }

    /// `DELETE /vms/{id}` — destroy a VM and release its resources.
    pub async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.request("DELETE", &format!("/vms/{vm_id}"), None).await?;
        Ok(())
    }

    /// `GET /vms/{id}` — query VM lifecycle state.
    pub async fn vm_state(&self, vm_id: &VmId) -> VmmResult<VmState> {
        let resp = self.request("GET", &format!("/vms/{vm_id}"), None).await?;
        let state_val = resp
            .get("state")
            .ok_or_else(|| VmmError::Backend("response missing state".into()))?
            .clone();
        serde_json::from_value(state_val)
            .map_err(|e| VmmError::Backend(format!("failed to decode state: {e}")))
    }

    /// `GET /vms/{id}/ip` — guest IP address, if available.
    pub async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        let resp = self.request("GET", &path(vm_id, "ip"), None).await?;
        match resp.get("ip") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| VmmError::Backend(format!("failed to decode ip: {e}"))),
        }
    }

    // ── Node-level helpers ─────────────────────────────────────────────────

    /// `POST /vms/provision` — create + start.
    pub async fn provision(&self, config: VmConfig) -> VmmResult<VmId> {
        let body = serde_json::to_value(&config).map_err(io)?;
        let resp = self.request("POST", "/vms/provision", Some(&body)).await?;
        resp.get("vm_id")
            .and_then(|v| v.as_str().map(String::from))
            .ok_or_else(|| VmmError::Backend("response missing vm_id".into()))
    }

    /// `PUT /vms/{id}/scale-to-zero` — pause → snapshot → destroy.
    pub async fn scale_to_zero(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        let resp = self
            .request("PUT", &path(vm_id, "scale-to-zero"), None)
            .await?;
        serde_json::from_value(resp).map_err(|e| {
            VmmError::Backend(format!("failed to decode snapshot response: {e}"))
        })
    }

    /// `PUT /vms/{id}/scale-from-zero` — restore from the registry-stored
    /// snapshot, then resume. Snapshot is resolved server-side from the vm_id
    /// (in the path), so the client supplies no body.
    pub async fn scale_from_zero(&self, vm_id: &VmId) -> VmmResult<VmId> {
        let resp = self
            .request("PUT", &path(vm_id, "scale-from-zero"), None)
            .await?;
        resp.get("vm_id")
            .and_then(|v| v.as_str().map(String::from))
            .ok_or_else(|| VmmError::Backend("response missing vm_id".into()))
    }

    // ── Core transport ─────────────────────────────────────────────────────

    /// Send an HTTP request and return the parsed JSON body for 2xx responses,
    /// or a reconstructed [`VmmError`] for error responses.
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<serde_json::Value> {
        let body_bytes = body
            .map(|b| b.to_string())
            .unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {len}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            host = self.addr,
            len = body_bytes.len(),
            body = body_bytes,
        );

        let mut stream =
            TcpStream::connect(self.addr)
                .await
                .map_err(|e| VmmError::Backend(format!("connect to API: {e}")))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| VmmError::Backend(format!("write to API: {e}")))?;

        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| VmmError::Backend(format!("read API response: {e}")))?;

        let text = String::from_utf8_lossy(&buf);
        let (status, body_str) = split_response(&text)?;

        if (200..300).contains(&status) {
            if body_str.is_empty() {
                return Ok(serde_json::Value::Null);
            }
            return serde_json::from_str(body_str).map_err(|e| {
                VmmError::Backend(format!("JSON parse error: {e}"))
            });
        }

        Err(decode_error(status, body_str))
    }
}

/// Build `/vms/{vm_id}/{action}`.
fn path(vm_id: &VmId, action: &str) -> String {
    format!("/vms/{vm_id}/{action}")
}

/// Split an HTTP response into `(status_code, body_str)`.
fn split_response(text: &str) -> VmmResult<(u16, &str)> {
    let header_end = text
        .find("\r\n\r\n")
        .ok_or_else(|| VmmError::Backend("malformed HTTP response (no header end)".into()))?;
    let header_str = &text[..header_end];
    let body_str = &text[header_end + 4..];

    let status_code = header_str
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| VmmError::Backend("malformed HTTP status line".into()))?;

    Ok((status_code, body_str))
}

/// Reconstruct a [`VmmError`] from an error response body. The structured
/// `kind` field drives variant selection; `vm_id` / `current` / `expected` are
/// used to fully reconstruct `VmNotFound`, `SnapshotNotFound`, and `InvalidState`.
fn decode_error(status: u16, body_str: &str) -> VmmError {
    let value: serde_json::Value = match serde_json::from_str(body_str) {
        Ok(v) => v,
        Err(_) => {
            return VmmError::Backend(format!("HTTP {status}: {body_str}"));
        }
    };

    let err_obj = match value.get("error") {
        Some(e) => e,
        None => return VmmError::Backend(format!("HTTP {status}: {body_str}")),
    };

    let kind = err_obj
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let message = err_obj
        .get("message")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| format!("HTTP {status}"));

    let str_field = |key: &str| {
        err_obj
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    match kind {
        "vm_not_found" => VmmError::VmNotFound(str_field("vm_id").unwrap_or_default()),
        "snapshot_not_found" => {
            VmmError::SnapshotNotFound(str_field("vm_id").unwrap_or_default())
        }
        "invalid_state" => {
            let vm_id = str_field("vm_id").unwrap_or_default();
            let current = err_obj
                .get("current")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or(VmState::Stopped);
            let expected = err_obj
                .get("expected")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or(VmState::Stopped);
            VmmError::InvalidState {
                vm_id,
                current,
                expected,
            }
        }
        "invalid_config" | "bad_request" => VmmError::InvalidConfig(message),
        _ => VmmError::Backend(message),
    }
}

fn io(e: serde_json::Error) -> VmmError {
    VmmError::Backend(format!("serialization error: {e}"))
}
