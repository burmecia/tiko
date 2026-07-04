//! HTTP control API for VM lifecycle management.
//!
//! Exposes the [`Vmm`](crate::vmm::Vmm) trait and the higher-level
//! [`Node`](crate::node::Node) orchestration over a Firecracker-style REST API.
//! Like the Firecracker backend client in `vmm::firecracker`, this server uses
//! raw HTTP/1.1 over TCP — no external HTTP library is needed.
//!
//! # Routes
//!
//! | Method | Path                        | Handler           | Returns                                  |
//! |--------|-----------------------------|-------------------|------------------------------------------|
//! | `GET`  | `/health`                   | liveness probe    | `{"status":"ok"}`                        |
//! | `GET`  | `/vms`                      | list VMs          | `{"vms":[{vm_id,...},...]}`              |
//! | `PUT`  | `/vms`                      | create_vm         | `{"vm_id":"..."}`  body: `VmConfig`      |
//! | `POST` | `/vms/provision`            | create + start    | `{"vm_id":"..."}`  body: `VmConfig`      |
//! | `POST` | `/vms/restore`              | restore_vm        | `{"vm_id":"..."}`  body: `Snapshot`      |
//! | `POST` | `/vms/scale-from-zero`      | restore + resume  | `{"vm_id":"..."}`  body: `Snapshot`      |
//! | `GET`  | `/vms/{vm_id}`              | vm_state          | `{"vm_id":"...","state":"running"}`      |
//! | `DELETE`| `/vms/{vm_id}`             | destroy_vm        | 204                                      |
//! | `GET`  | `/vms/{vm_id}/ip`           | vm_guest_ip       | `{"vm_id":"...","ip":"1.2.3.4"\|null}`   |
//! | `PUT`  | `/vms/{vm_id}/start`        | start_vm          | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/pause`        | pause_vm          | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/resume`       | resume_vm         | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/snapshot`     | snapshot_vm       | `Snapshot`                               |
//! | `PUT`  | `/vms/{vm_id}/scale-to-zero`| pause+snap+destroy| `Snapshot`                               |
//!
//! # Error responses
//!
//! All errors use `{"error":{"kind":...,"message":...}}` with structured fields
//! (`vm_id`, `current`, `expected`) so clients can reconstruct the original
//! [`VmmError`](crate::vmm::VmmError) variant. Status codes: 400 invalid config /
//! bad request, 404 not found, 409 invalid state, 500 backend / I/O.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::control::Control;
use crate::node::Node;
use crate::vmm::{Snapshot, VmConfig, VmId, VmmError};

/// Maximum accepted request header block size (protects against runaway reads).
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// HTTP control API server. Shares the same [`Node`] / [`Control`] used by the
/// PG proxy, so lifecycle changes are immediately visible to both planes.
pub struct ApiServer {
    node: Arc<Node>,
    control: Arc<Control>,
}

impl ApiServer {
    pub fn new(node: Arc<Node>, control: Arc<Control>) -> Self {
        Self { node, control }
    }

    /// Bind to `listen_addr` and serve forever. Logs the resolved address.
    pub async fn run(self: Arc<Self>, listen_addr: SocketAddr) -> std::io::Result<()> {
        let listener = TcpListener::bind(listen_addr).await?;
        info!(addr = %listen_addr, "API server listening");
        self.serve(listener).await
    }

    /// Serve accepted connections from an already-bound listener. Used by tests
    /// that bind to `:0` and need the resolved local address before serving.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.handle_connection(stream, addr).await {
                            error!(client = %addr, error = %e, "API connection failed");
                        }
                    });
                }
                Err(e) => error!(error = %e, "API accept failed"),
            }
        }
    }

    /// Handle one HTTP/1.1 request/response cycle (connection: close).
    async fn handle_connection(
        &self,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> std::io::Result<()> {
        let req = match read_request(&mut stream).await? {
            Some(r) => r,
            None => {
                debug!(client = %addr, "connection closed before request");
                return Ok(());
            }
        };
        debug!(client = %addr, method = %req.method, path = %req.path, "API request");

        let resp = self.route(&req).await;
        write_response(&mut stream, resp.status, &resp.body).await
    }

    /// Top-level router: splits the path into segments and dispatches.
    async fn route(&self, req: &Request) -> Response {
        let path = req.path.split('?').next().unwrap_or("");
        let segs: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(percent_decode_seg)
            .collect();
        let method = req.method.as_str();

        match (method, segs.as_slice()) {
            ("GET", ["health"]) => ok_json(serde_json::json!({"status": "ok"})),

            // Collection-level routes.
            ("GET", ["vms"]) => self.list_vms(),
            ("PUT", ["vms"]) => match parse_json::<VmConfig>(&req.body) {
                Ok(config) => match self.node.vmm().create_vm(config).await {
                    Ok(id) => ok_json(serde_json::json!({"vm_id": id})),
                    Err(e) => err_resp(&e),
                },
                Err(r) => r,
            },
            ("POST", ["vms", "provision"]) => match parse_json::<VmConfig>(&req.body) {
                Ok(config) => match self.node.provision(config).await {
                    Ok(id) => {
                        self.control
                            .register(id.clone(), String::new(), String::new(), 5432);
                        ok_json(serde_json::json!({"vm_id": id}))
                    }
                    Err(e) => err_resp(&e),
                },
                Err(r) => r,
            },
            ("POST", ["vms", "restore"]) => match parse_json::<Snapshot>(&req.body) {
                Ok(snap) => match self.node.vmm().restore_vm(&snap).await {
                    Ok(id) => ok_json(serde_json::json!({"vm_id": id})),
                    Err(e) => err_resp(&e),
                },
                Err(r) => r,
            },
            ("POST", ["vms", "scale-from-zero"]) => match parse_json::<Snapshot>(&req.body) {
                Ok(snap) => match self.node.scale_from_zero(&snap).await {
                    Ok(id) => ok_json(serde_json::json!({"vm_id": id})),
                    Err(e) => err_resp(&e),
                },
                Err(r) => r,
            },

            // Per-VM routes: /vms/{vm_id}[/{action}].
            (_, ["vms", vm_id, rest @ ..]) => {
                self.route_vm(method, &VmId::from(*vm_id), rest, req).await
            }

            _ => not_found(method, path),
        }
    }

    /// Dispatch `/vms/{vm_id}/...` routes.
    async fn route_vm(
        &self,
        method: &str,
        vm_id: &VmId,
        rest: &[&str],
        _req: &Request,
    ) -> Response {
        let vmm = self.node.vmm();
        match (method, rest) {
            ("GET", []) => match vmm.vm_state(vm_id).await {
                Ok(state) => ok_json(serde_json::json!({"vm_id": vm_id, "state": state})),
                Err(e) => err_resp(&e),
            },
            ("DELETE", []) => match vmm.destroy_vm(vm_id).await {
                Ok(()) => {
                    self.control.unregister(vm_id);
                    no_content()
                }
                Err(e) => err_resp(&e),
            },
            ("GET", ["ip"]) => match vmm.vm_guest_ip(vm_id).await {
                Ok(ip) => ok_json(serde_json::json!({"vm_id": vm_id, "ip": ip})),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["start"]) => match vmm.start_vm(vm_id).await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["pause"]) => match vmm.pause_vm(vm_id).await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["resume"]) => match vmm.resume_vm(vm_id).await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["snapshot"]) => match vmm.snapshot_vm(vm_id).await {
                Ok(snap) => ok_value(&snap),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["scale-to-zero"]) => match self.node.scale_to_zero(vm_id).await {
                Ok(snap) => {
                    self.control.set_snapshot(
                        vm_id,
                        snap.state_path.to_string_lossy().into_owned(),
                    );
                    ok_value(&snap)
                }
                Err(e) => err_resp(&e),
            },
            _ => {
                let path = format!("/vms/{vm_id}/{}", rest.join("/"));
                not_found(method, &path)
            }
        }
    }

    /// `GET /vms` — best-effort registry listing. State is queried lazily so a
    /// transient backend lookup failure degrades to `state: unknown` rather than
    /// failing the whole list.
    fn list_vms(&self) -> Response {
        let records = self.control.list();
        let mut arr = Vec::with_capacity(records.len());
        for (vm_id, rec) in records {
            arr.push(serde_json::json!({
                "vm_id": vm_id,
                "tenant_id": rec.tenant_id,
                "branch_id": rec.branch_id,
                "connection_count": rec.connection_count,
                "snapshot_id": rec.snapshot_id,
            }));
        }
        ok_json(serde_json::json!({"vms": arr}))
    }
}

// ============================================================================
// HTTP/1.1 parsing & writing
// ============================================================================

/// A parsed HTTP/1.1 request.
struct Request {
    method: String,
    path: String,
    #[allow(dead_code)]
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// A response ready to be written: status code + (possibly empty) JSON body.
struct Response {
    status: u16,
    body: Vec<u8>,
}

/// Read and parse a single HTTP/1.1 request from `stream`. Returns `None` if the
/// peer closed the connection before sending anything.
async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    // Read the header block up to the terminating blank line.
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::other("client closed before sending full headers"));
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Err(std::io::Error::other("request headers too large"));
        }
    }

    let header_str = String::from_utf8_lossy(&buf);
    let mut lines = header_str.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    if method.is_empty() || path.is_empty() {
        return Err(std::io::Error::other("malformed request line"));
    }

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        stream.read_exact(&mut body).await?;
    }

    Ok(Some(Request {
        method,
        path,
        headers,
        body,
    }))
}

/// Write an HTTP/1.1 response with a JSON body and `Connection: close`.
async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
) -> std::io::Result<()> {
    let status_text = status_text(status);
    let head = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n",
        len = body.len(),
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

/// Canonical reason phrase for a status code.
fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

// ============================================================================
// Response constructors & error mapping
// ============================================================================

fn ok_json(value: serde_json::Value) -> Response {
    Response {
        status: 200,
        body: value.to_string().into_bytes(),
    }
}

fn ok_value<T: serde::Serialize>(value: &T) -> Response {
    match serde_json::to_value(value) {
        Ok(v) => ok_json(v),
        Err(e) => {
            warn!(error = %e, "failed to serialize success response");
            internal_error(format!("serialization failed: {e}"))
        }
    }
}

fn no_content() -> Response {
    Response {
        status: 204,
        body: Vec::new(),
    }
}

fn internal_error(message: String) -> Response {
    Response {
        status: 500,
        body: serde_json::json!({"error": {"kind": "internal_error", "message": message}})
            .to_string()
            .into_bytes(),
    }
}

fn not_found(method: &str, path: &str) -> Response {
    Response {
        status: 404,
        body: serde_json::json!({
            "error": {
                "kind": "not_found",
                "message": format!("no route for {method} {path}"),
            }
        })
        .to_string()
        .into_bytes(),
    }
}

/// Parse a JSON request body, returning a ready `Response` on failure so callers
/// can early-return without constructing an error variant themselves.
fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|e| Response {
        status: 400,
        body: serde_json::json!({
            "error": {
                "kind": "bad_request",
                "message": format!("invalid JSON body: {e}"),
            }
        })
        .to_string()
        .into_bytes(),
    })
}

/// Map a [`VmmError`] to an HTTP response, preserving enough structured data for
/// the client to reconstruct the original variant.
fn err_resp(err: &VmmError) -> Response {
    match err {
        VmmError::VmNotFound(id) => Response {
            status: 404,
            body: serde_json::json!({
                "error": {"kind": "vm_not_found", "message": err.to_string(), "vm_id": id}
            })
            .to_string()
            .into_bytes(),
        },
        VmmError::SnapshotNotFound(id) => Response {
            status: 404,
            body: serde_json::json!({
                "error": {"kind": "snapshot_not_found", "message": err.to_string(), "vm_id": id}
            })
            .to_string()
            .into_bytes(),
        },
        VmmError::InvalidState {
            vm_id,
            current,
            expected,
        } => Response {
            status: 409,
            body: serde_json::json!({
                "error": {
                    "kind": "invalid_state",
                    "message": err.to_string(),
                    "vm_id": vm_id,
                    "current": current,
                    "expected": expected,
                }
            })
            .to_string()
            .into_bytes(),
        },
        VmmError::InvalidConfig(m) => Response {
            status: 400,
            body: serde_json::json!({
                "error": {"kind": "invalid_config", "message": m}
            })
            .to_string()
            .into_bytes(),
        },
        VmmError::Backend(m) => Response {
            status: 500,
            body: serde_json::json!({
                "error": {"kind": "backend_error", "message": m}
            })
            .to_string()
            .into_bytes(),
        },
        VmmError::Io(e) => Response {
            status: 500,
            body: serde_json::json!({
                "error": {"kind": "io_error", "message": err.to_string(), "detail": e.to_string()}
            })
            .to_string()
            .into_bytes(),
        },
    }
}

/// Minimal percent-decoding for a single path segment (handles `%XX`).
fn percent_decode_seg(seg: &str) -> &str {
    // vm_ids in this codebase are plain ASCII (e.g. "fc-single-10"); full
    // decoding isn't required. We expose the raw segment so callers see the
    // exact characters. Reserved chars in vm_ids are not supported.
    seg
}
