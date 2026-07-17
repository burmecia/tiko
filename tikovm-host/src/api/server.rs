//! HTTP/1.1 control API server + pure dispatch function.

use std::sync::Arc;

use serde::Serialize;
use tikovm_protocol::error::ErrorEnvelope;
use tikovm_protocol::vm::{VmSpec, VmState};

use crate::node::Node;
use crate::vmm::{DriveConfig, VmConfig};

/// A simple JSON HTTP response.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: &'static str,
}

impl Response {
    pub fn json<T: Serialize>(status: u16, val: &T) -> Self {
        Self { status, body: serde_json::to_vec(val).unwrap_or_else(|_| b"null".to_vec()), content_type: "application/json" }
    }

    pub fn ok_empty() -> Self {
        Self { status: 204, body: Vec::new(), content_type: "application/json" }
    }

    pub fn error(status: u16, kind: &str, message: impl Into<String>) -> Self {
        Self::json(status, &ErrorEnvelope::new(kind, message.into()))
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::error(404, "not_found", message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::error(400, "bad_request", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::error(409, "conflict", message)
    }
}

/// The control API server. Holds an Arc<Node> and serves the routes in design
/// §10 over a minimal HTTP/1.1 connection per request.
pub struct ApiServer {
    node: Arc<Node>,
}

impl ApiServer {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    /// Dispatch a parsed request. Pure (no I/O) apart from driving the Node.
    pub async fn handle(&self, method: &str, path: &str, body: &[u8]) -> Response {
        dispatch(method, path, body, &self.node, 0).await
    }

    /// Run the HTTP/1.1 server until cancelled.
    pub async fn serve(self: Arc<Self>, addr: &str) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%addr, "control API listening");
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            };
            let svc = self.clone();
            tokio::spawn(async move {
                if let Err(e) = svc.handle_conn(stream).await {
                    tracing::debug!(%peer, error = %e, "connection closed");
                }
            });
        }
    }

    async fn handle_conn(&self, mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        let req = String::from_utf8_lossy(&buf[..n]);
        let (method, path, body_start) = match parse_request_line(&req) {
            Some(v) => v,
            None => {
                write_response(&mut stream, 400, b"bad request").await?;
                return Ok(());
            }
        };
        // For simplicity we trust Content-Length when present; otherwise use the
        // remainder of the first read. Sufficient for JSON control calls.
        let body = extract_body(&req, body_start, &buf[..n]);
        let resp = self.handle(&method, &path, &body).await;
        write_response_with(&mut stream, resp).await?;
        Ok(())
    }
}

/// Pure dispatch (testable with any Node, incl. MockVmm-backed).
pub async fn dispatch(method: &str, path: &str, body: &[u8], node: &Node, _cid: u32) -> Response {
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match segs.as_slice() {
        ["health"] => Response::json(200, &serde_json::json!({"status": "ok"})),
        ["vms"] if method == "GET" => Response::json(200, &node.control().list()),
        ["vms"] if method == "PUT" => {
            let spec = match serde_json::from_slice::<VmSpec>(body) {
                Ok(s) => s,
                Err(e) => return Response::bad_request(format!("invalid VmSpec: {e}")),
            };
            provision(node, spec).await
        }
        ["vms", "provision"] if method == "POST" => {
            let spec = match serde_json::from_slice::<VmSpec>(body) {
                Ok(s) => s,
                Err(e) => return Response::bad_request(format!("invalid VmSpec: {e}")),
            };
            provision(node, spec).await
        }
        ["vms", id] if method == "GET" => vm_view(node, id),
        ["vms", id] if method == "DELETE" => {
            match node.destroy(&id.to_string()).await {
                Ok(_) => Response::ok_empty(),
                Err(e) => err_from(e),
            }
        }
        // Guest signals: the agent asks the host to suspend/shutdown.
        // (Must precede the generic `[id, op]` lifecycle arm.)
        ["vms", id, "suspend-request"] if method == "POST" => {
            match node.freeze(&id.to_string()).await {
                Ok(_) => {
                    let epoch = node.bump_pause_epoch(&id.to_string()).unwrap_or(0);
                    Response::json(202, &serde_json::json!({"pause_epoch": epoch}))
                }
                Err(e) => err_from(e),
            }
        }
        // Guest-driven signal: the agent POSTs its own IP (it doesn't know its
        // vm_id); the host resolves guest_ip -> vm and freezes.
        ["vms", "by-ip", ip, "suspend-request"] if method == "POST" => {
            let ip: std::net::IpAddr = match ip.parse() {
                Ok(v) => v,
                Err(_) => return Response::bad_request("invalid ip"),
            };
            let (vm_id, _rec) = match node.control().find_by_guest_ip(ip) {
                Some(x) => x,
                None => return Response::not_found(format!("no vm with guest ip {ip}")),
            };
            match node.freeze(&vm_id).await {
                Ok(_) => {
                    let epoch = node.bump_pause_epoch(&vm_id).unwrap_or(0);
                    Response::json(202, &serde_json::json!({"vm_id": vm_id, "pause_epoch": epoch}))
                }
                Err(e) => err_from(e),
            }
        }
        ["vms", id, "shutdown-request"] if method == "POST" => {
            match node.destroy(&id.to_string()).await {
                Ok(_) => Response::ok_empty(),
                Err(e) => err_from(e),
            }
        }
        ["vms", id, op] if method == "POST" => lifecycle(node, id, op).await,
        ["vms", id, "ip"] if method == "GET" => {
            match node.vmm().vm_guest_ip(&id.to_string()).await {
                Ok(ip) => Response::json(200, &serde_json::json!({"vm_id": id, "guest_ip": ip})),
                Err(e) => err_from(e),
            }
        }
        // Generic guest proxy passthrough (tunneled over vsock in production).
        ["vms", id, "guest", rest @ ..] => {
            // TODO: forward to the guest agent over vsock. For now 502.
            Response::error(502, "not_implemented", format!("guest proxy not yet wired for {id}: {}", rest.join("/")))
        }
        _ => Response::not_found(format!("no route for {method} {path}")),
    }
}

async fn provision(node: &Node, spec: VmSpec) -> Response {
    let vm_id = spec.vm_id.clone();
    let config = vm_config_from_spec(&spec);
    match node.create(config, spec).await {
        Ok(_) => {
            // provision = create + start
            if let Err(e) = node.start(&vm_id).await {
                return err_from(e);
            }
            Response::json(201, &serde_json::json!({"vm_id": vm_id, "state": "started"}))
        }
        Err(e) => err_from(e),
    }
}

async fn lifecycle(node: &Node, id: &str, op: &str) -> Response {
    let vm_id = id.to_string();
    let res = match op {
        "start" => node.start(&vm_id).await,
        "pause" => node.pause(&vm_id).await,
        "resume" => node.resume(&vm_id).await,
        "suspend" => node.suspend(&vm_id).await,
        "restore" | "wake" => node.restore(&vm_id).await,
        "freeze" => node.freeze(&vm_id).await,
        "ensure-running" => node.ensure_running(&vm_id).await,
        _ => return Response::bad_request(format!("unknown lifecycle op: {op}")),
    };
    match res {
        Ok(_) => vm_view(node, id),
        Err(e) => err_from(e),
    }
}

fn vm_view(node: &Node, id: &str) -> Response {
    match node.control().get(&id.to_string()) {
        Some(rec) => {
            let g = rec.read().unwrap();
            Response::json(200, &g.to_info(&id.to_string()))
        }
        None => Response::not_found(format!("vm {id} not found")),
    }
}

fn err_from(e: crate::vmm::VmmError) -> Response {
    use crate::vmm::VmmError::*;
    match e {
        VmNotFound(_) => Response::not_found(e.to_string()),
        InvalidState { .. } => Response::conflict(e.to_string()),
        InvalidConfig(_) => Response::bad_request(e.to_string()),
        SnapshotNotFound(_) => Response::error(409, "no_snapshot", e.to_string()),
        _ => Response::error(500, "internal", e.to_string()),
    }
}

/// Derive a low-level [`VmConfig`] from a provision [`VmSpec`]. (Networking/vsock
/// CID allocation and volume→drive expansion are elaborated when those modules
/// land; for now volumes become drives and `cid` is caller-allocated.)
pub fn vm_config_from_spec(spec: &VmSpec) -> VmConfig {
    let drives = spec
        .manifest
        .as_ref()
        .map(|m| {
            m.volumes
                .iter()
                .enumerate()
                .map(|(i, v)| DriveConfig {
                    drive_id: v.name.clone(),
                    path: format!("/tmp/tikovm/vol-{}-{i}", v.name).into(),
                    read_only: v.read_only,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    VmConfig {
        vm_id: spec.vm_id.clone(),
        kernel_path: spec.kernel.kernel_path.clone(),
        kernel_cmdline: spec.kernel.kernel_cmdline.clone(),
        rootfs_path: spec.rootfs.path.clone(),
        memory_mb: spec.resources.memory_mb,
        vcpus: spec.resources.vcpus,
        drives,
        initrd_path: spec.kernel.initrd_path.clone(),
        // vsock (control channel) is enabled once the guest agent that uses it
        // is wired; restoring a vsock device needs a fresh UDS path per restore.
        guest_cid: None,
    }
}

// ---- minimal HTTP/1.1 helpers -------------------------------------------

fn parse_request_line(req: &str) -> Option<(String, String, usize)> {
    let line = req.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    // Header section ends at "\r\n\r\n"
    let body_start = req.find("\r\n\r\n").map(|i| i + 4)?;
    Some((method, path, body_start))
}

fn extract_body(req: &str, body_start: usize, raw: &[u8]) -> Vec<u8> {
    // Honor Content-Length when present.
    let len = req
        .lines()
        .take_while(|l| !l.is_empty())
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k.eq_ignore_ascii_case("content-length")).then(|| v.trim().parse::<usize>().ok())?
        });
    match len {
        Some(len) => {
            let start = body_start.min(raw.len());
            let end = (start + len).min(raw.len());
            raw[start..end].to_vec()
        }
        None => raw.get(body_start..).map(|s| s.to_vec()).unwrap_or_default(),
    }
}

async fn write_response(stream: &mut tokio::net::TcpStream, status: u16, body: &[u8]) -> std::io::Result<()> {
    let resp = Response { status, body: body.to_vec(), content_type: "application/json" };
    write_response_with(stream, resp).await
}

async fn write_response_with(stream: &mut tokio::net::TcpStream, resp: Response) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let reason = match resp.status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        resp.status,
        resp.content_type,
        resp.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await?;
    Ok(())
}

// silence unused import warnings for VmState when not referenced on all paths
#[allow(unused)]
fn _state_use(_s: VmState) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Control;
    use crate::vmm::mock::MockVmm;
    use tikovm_protocol::manifest::WorkloadManifest;

    fn node() -> Arc<Node> {
        Arc::new(Node::new(
            Arc::new(MockVmm::new(std::path::PathBuf::from("/tmp/tikovm-snaps"))),
            Arc::new(Control::new()),
        ))
    }

    fn spec_json(id: &str) -> Vec<u8> {
        let s = VmSpec {
            vm_id: id.into(),
            rootfs: tikovm_protocol::vm::RootfsRef { path: "/r".into(), read_only_base: true },
            resources: tikovm_protocol::vm::ResourceConfig::default(),
            kernel: tikovm_protocol::vm::KernelSpec {
                kernel_path: "/k".into(),
                kernel_cmdline: "console=ttyS0".into(),
                initrd_path: None,
            },
            network: tikovm_protocol::vm::NetworkSpec::default(),
            routing: vec![],
            env: Default::default(),
            manifest: Some(WorkloadManifest::empty("echo")),
            schedule: None,
        };
        serde_json::to_vec(&s).unwrap()
    }

    #[tokio::test]
    async fn health() {
        let n = node();
        let r = dispatch("GET", "/health", &[], &n, 3).await;
        assert_eq!(r.status, 200);
    }

    #[tokio::test]
    async fn provision_then_lifecycle() {
        let n = node();
        // provision
        let r = dispatch("POST", "/vms/provision", &spec_json("vm-a"), &n, 3).await;
        assert_eq!(r.status, 201);
        // GET shows started
        let r = dispatch("GET", "/vms/vm-a", &[], &n, 3).await;
        assert_eq!(r.status, 200);
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["state"], "started");
        // pause
        let r = dispatch("POST", "/vms/vm-a/pause", &[], &n, 3).await;
        assert_eq!(r.status, 200);
        // suspend (Paused -> Suspended)
        let r = dispatch("POST", "/vms/vm-a/suspend", &[], &n, 3).await;
        assert_eq!(r.status, 200);
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["state"], "suspended");
        // restore (Suspended -> Started)
        let r = dispatch("POST", "/vms/vm-a/restore", &[], &n, 3).await;
        assert_eq!(r.status, 200);
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["state"], "started");
    }

    #[tokio::test]
    async fn illegal_transition_returns_conflict() {
        let n = node();
        dispatch("POST", "/vms/provision", &spec_json("vm-b"), &n, 3).await;
        // suspend requires Paused; from Started it should be 409
        let r = dispatch("POST", "/vms/vm-b/suspend", &[], &n, 3).await;
        assert_eq!(r.status, 409);
    }

    #[tokio::test]
    async fn suspend_request_freezes_and_bumps_epoch() {
        let n = node();
        dispatch("POST", "/vms/provision", &spec_json("vm-c"), &n, 3).await;
        let r = dispatch("POST", "/vms/vm-c/suspend-request", &[], &n, 3).await;
        assert_eq!(r.status, 202);
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["pause_epoch"], 1);
        // VM should now be suspended
        let r = dispatch("GET", "/vms/vm-c", &[], &n, 3).await;
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["state"], "suspended");
    }
}
