//! HTTP reverse proxy: external `:3000` → per-VM PostgREST (`guest:3000`).
//!
//! Mirrors the PG wire proxy (`crate::proxy`) but for HTTP. A single edge port
//! fans out to the right VM's in-guest PostgREST based on the
//! `X-Tiko-Endpoint: vm-N` request header (the HTTP analog of the PG proxy's
//! `options='-c tiko.endpoint=vm-N'`). Each request wakes the target VM if it is
//! paused or cold-frozen (snapshot-restored), reusing the shared
//! [`resolve_guest`] helper.
//!
//! HTTP requests are short-lived, so — unlike the PG proxy — this does **not**
//! run a wake-on-stale / warm-pause / cancel splice; wake-on-connect (inside
//! [`resolve_guest`]) is enough. It does call `on_connect`/`on_disconnect`
//! (inside [`resolve_guest`] / on completion) so the idle counters stay
//! accurate.
//!
//! ```text
//! Client ──HTTP──→ HttpProxy (:3000)
//!                   │  read request line + headers
//!                   │  extract X-Tiko-Endpoint: vm-N
//!                   │  resolve_guest (wake if paused/frozen) → guest_ip
//!                   │  connect guest_ip:3000, forward request, splice response
//!                   ▼
//!                VM PostgREST (127.0.0.1:3000 in guest)
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::control::Control;
use crate::node::Node;
use crate::proxy::{ForwardTarget, ProxyConfig, ResolveError, resolve_guest};
use crate::vmm::VmId;

/// In-guest PostgREST port (matches `server-port` in postgrest.conf).
const PGRST_PORT: u16 = 3000;

/// Request header carrying the target VM id, mirroring the PG proxy's
/// `tiko.endpoint=vm-N` startup option.
const ENDPOINT_HEADER: &str = "x-tiko-endpoint";

/// Maximum accepted request header block size (protects against runaway reads).
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// HTTP reverse proxy for PostgREST.
pub struct HttpProxy {
    node: Arc<Node>,
    control: Arc<Control>,
    /// Configuration. `listen_addr` is the HTTP edge port (default
    /// `0.0.0.0:3000`); the wake/dev fields are shared with the PG proxy.
    config: ProxyConfig,
}

impl HttpProxy {
    pub fn new(node: Arc<Node>, control: Arc<Control>, config: ProxyConfig) -> Self {
        Self {
            node,
            control,
            config,
        }
    }

    /// Run the proxy server. Accepts connections in a loop.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(
            addr = %self.config.listen_addr,
            "http proxy listening for PostgREST connections"
        );

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let node = self.node.clone();
                    let control = self.control.clone();
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_connection(stream, addr, &node, &control, &config).await
                        {
                            debug!(client = %addr, error = %e, "http proxy connection failed");
                        }
                    });
                }
                Err(e) => error!(error = %e, "http proxy accept failed"),
            }
        }
    }
}

/// Handle one client connection: read the request, route + wake, forward,
/// splice the response.
async fn handle_connection(
    mut client_stream: TcpStream,
    client_addr: SocketAddr,
    node: &Node,
    control: &Control,
    config: &ProxyConfig,
) -> io::Result<()> {
    debug!(client = %client_addr, "new http connection");

    let req = match read_request(&mut client_stream).await? {
        Some(r) => r,
        None => {
            debug!(client = %client_addr, "client closed before sending request");
            return Ok(());
        }
    };

    // Route: X-Tiko-Endpoint header, else reject (or dev fallback).
    let (vm_id, backend_addr) = match resolve_route(&req, node, control, config).await {
        Ok((vm_id, addr)) => (vm_id, addr),
        Err(resp) => {
            write_response(&mut client_stream, resp.status, &resp.body).await?;
            return Ok(());
        }
    };

    // Connect to the in-guest PostgREST.
    let mut backend = match TcpStream::connect(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            let resp = http_err(
                502,
                "backend_unreachable",
                &format!("cannot connect to PostgREST at {backend_addr}: {e}"),
            );
            write_response(&mut client_stream, resp.status, &resp.body).await?;
            if let Some(id) = vm_id {
                control.on_disconnect(&id);
            }
            return Ok(());
        }
    };
    backend.set_nodelay(true)?;

    // Forward the request (hop-by-hop headers stripped, Connection: close so
    // PostgREST closes after the response and the splice terminates on EOF).
    let forwarded = rebuild_request(&req);
    if let Err(e) = backend.write_all(&forwarded).await {
        let resp = http_err(
            502,
            "backend_write_failed",
            &format!("failed to forward request to PostgREST: {e}"),
        );
        write_response(&mut client_stream, resp.status, &resp.body).await?;
        if let Some(id) = vm_id {
            control.on_disconnect(&id);
        }
        return Ok(());
    }

    // Splice the response back verbatim until the backend closes.
    if let Err(e) = io::copy(&mut backend, &mut client_stream).await {
        debug!(client = %client_addr, error = %e, "response splice ended");
    }
    let _ = client_stream.shutdown().await;

    // Tear-down: keep the connection counter accurate (mirrors the PG proxy).
    if let Some(id) = vm_id {
        control.on_disconnect(&id);
    }
    debug!(client = %client_addr, "http connection closed");
    Ok(())
}

/// Resolve the request to a backend address, applying wake-on-connect for
/// VM-routed targets. Returns the optional VmId (for connect accounting) and
/// the backend socket address, or an HTTP error response to send instead.
async fn resolve_route(
    req: &Request,
    node: &Node,
    control: &Control,
    config: &ProxyConfig,
) -> Result<(Option<VmId>, SocketAddr), HttpResp> {
    match req.header(ENDPOINT_HEADER) {
        Some(value) => {
            let vm_id: VmId = value.trim().to_string();
            let ip = resolve_guest(&vm_id, node, control, config.resume_timeout_secs)
                .await
                .map_err(|e| resolve_error_response(&vm_id, e))?;
            Ok((Some(vm_id), SocketAddr::new(ip, PGRST_PORT)))
        }
        None => {
            if config.dev_allow_missing_endpoint {
                match &config.default_target {
                    ForwardTarget::Direct(addr) => Ok((None, *addr)),
                    ForwardTarget::Vm(id) => {
                        let ip = resolve_guest(id, node, control, config.resume_timeout_secs)
                            .await
                            .map_err(|e| resolve_error_response(id, e))?;
                        Ok((Some(id.clone()), SocketAddr::new(ip, PGRST_PORT)))
                    }
                }
            } else {
                Err(http_err(
                    400,
                    "missing_endpoint",
                    &format!(
                        "request is missing the {ENDPOINT_HEADER} header (expected: {ENDPOINT_HEADER}: vm-N)"
                    ),
                ))
            }
        }
    }
}

/// Map a [`ResolveError`] to an HTTP response with an appropriate status.
fn resolve_error_response(vm_id: &VmId, e: ResolveError) -> HttpResp {
    match e {
        ResolveError::UnknownVm(id) => {
            http_err(404, "unknown_vm", &format!("VM {id} is not registered"))
        }
        ResolveError::Wake(err) => http_err(
            503,
            "wake_failed",
            &format!("failed to wake VM {vm_id}: {err}"),
        ),
        ResolveError::WakeTimeout(id, secs) => http_err(
            504,
            "wake_timeout",
            &format!("VM {id} did not resume within {secs}s"),
        ),
        ResolveError::GuestIp(id, err) => http_err(
            502,
            "guest_ip_failed",
            &format!("cannot get guest IP for VM {id}: {err}"),
        ),
    }
}

// ── HTTP parsing (minimal, original-case headers) ───────────────────────────

/// A parsed HTTP/1. request for forwarding. Header keys keep their original
/// casing (forwarded faithfully); lookups are case-insensitive.
struct Request {
    method: String,
    path: String,
    /// (original-case key, value), in received order.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    /// Case-insensitive header lookup.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Read one HTTP/1.1 request (header block + Content-Length body). Returns
/// `None` if the peer closed before sending anything.
async fn read_request(stream: &mut TcpStream) -> io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::other(
                "client closed before sending full headers",
            ));
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > MAX_HEADER_BYTES {
            return Err(io::Error::other("request headers too large"));
        }
    }

    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    if method.is_empty() || path.is_empty() {
        return Err(io::Error::other("malformed request line"));
    }

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    // Chunked request bodies are not yet supported (most PostgREST clients use
    // Content-Length or no body). Reject rather than silently truncate.
    if let Some(te) = header_lookup(&headers, "transfer-encoding") {
        if te.eq_ignore_ascii_case("chunked") {
            return Err(io::Error::other(
                "chunked request bodies are not supported (use Content-Length)",
            ));
        }
    }

    let content_length: usize = header_lookup(&headers, "content-length")
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

/// Case-insensitive lookup over an unsorted header list.
fn header_lookup<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Rebuild the request for forwarding: strip hop-by-hop headers, force
/// `Connection: close`, and re-emit `Content-Length` from the body we read.
fn rebuild_request(req: &Request) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + req.body.len());
    out.extend_from_slice(format!("{} {} HTTP/1.1\r\n", req.method, req.path).as_bytes());
    for (k, v) in &req.headers {
        if is_hop_by_hop(k) || k.eq_ignore_ascii_case("content-length") {
            continue;
        }
        out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }
    out.extend_from_slice(format!("Content-Length: {}\r\n", req.body.len()).as_bytes());
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out.extend_from_slice(&req.body);
    out
}

/// Hop-by-hop headers (RFC 7230 §6.1) that must not be forwarded.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    )
}

// ── Response helpers ────────────────────────────────────────────────────────

/// A status + JSON body ready to write back to the client.
struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

fn http_err(status: u16, kind: &str, message: &str) -> HttpResp {
    HttpResp {
        status,
        body: serde_json::json!({ "error": { "kind": kind, "message": message } })
            .to_string()
            .into_bytes(),
    }
}

/// Write an HTTP/1.1 response (JSON content type, `Connection: close`).
async fn write_response(stream: &mut TcpStream, status: u16, body: &[u8]) -> io::Result<()> {
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

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: &str, path: &str, headers: &[((&str, &str))], body: &[u8]) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn rebuild_strips_hop_by_hop_and_forces_close() {
        let r = req(
            "POST",
            "/articles",
            &[
                ("Host", "vm-42.tiko.app"),
                ("X-Tiko-Endpoint", "vm-42"),
                ("Connection", "keep-alive"),
                ("Keep-Alive", "timeout=5"),
                ("Content-Length", "ignored-on-input"),
            ],
            br#"{"title":"x"}"#,
        );
        let out = String::from_utf8(rebuild_request(&r)).unwrap();
        // Request line preserved.
        assert!(out.starts_with("POST /articles HTTP/1.1\r\n"));
        // Host + X-Tiko-Endpoint forwarded.
        assert!(out.contains("Host: vm-42.tiko.app\r\n"));
        assert!(out.contains("X-Tiko-Endpoint: vm-42\r\n"));
        // Hop-by-hop stripped.
        assert!(!out.contains("keep-alive"));
        assert!(!out.contains("Keep-Alive"));
        // Content-Length re-emitted from the actual body (13 bytes).
        assert!(out.contains("Content-Length: 13\r\n"));
        // Connection forced to close.
        assert!(out.contains("Connection: close\r\n"));
        // Body present.
        assert!(out.ends_with("{\"title\":\"x\"}"));
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let r = req("GET", "/", &[("X-TIKO-ENDPOINT", "vm-7")], &[]);
        assert_eq!(r.header(ENDPOINT_HEADER), Some("vm-7"));
    }

    #[test]
    fn resolve_error_maps_unknown_vm_to_404() {
        let resp = resolve_error_response(
            &"vm-9".to_string(),
            ResolveError::UnknownVm("vm-9".to_string()),
        );
        assert_eq!(resp.status, 404);
        assert!(String::from_utf8_lossy(&resp.body).contains("unknown_vm"));
    }

    #[test]
    fn resolve_error_maps_wake_timeout_to_504() {
        let resp = resolve_error_response(
            &"vm-9".to_string(),
            ResolveError::WakeTimeout("vm-9".to_string(), 30),
        );
        assert_eq!(resp.status, 504);
    }
}
