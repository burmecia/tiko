//! Shared HTTP/1.1 primitives for the tikoguest agent.
//!
//! Covers both directions:
//! - **Server side**: [`read_request`] / [`write_response`] / [`Response`]
//!   constructors — used by [`server`](crate::server) to handle inbound requests
//!   from tikod.
//! - **Client side**: [`HttpClient`] — used by the observer and scaler loops to
//!   push reports and snapshot-request signals to tikod.
//!
//! Raw HTTP/1.1 over TCP — no external HTTP library, consistent with tikod's
//! API server and [`GuestClient`](crate::guestcontrol).

use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum accepted request header block size (protects against runaway reads).
const MAX_HEADER_BYTES: usize = 64 * 1024;

// ── Server side ─────────────────────────────────────────────────────────────

/// A parsed inbound HTTP/1.1 request.
pub struct Request {
    pub method: String,
    pub path: String,
    #[allow(dead_code)]
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

/// An outbound HTTP/1.1 response (server side).
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Read one HTTP/1.1 request from `stream`. Returns `None` if the connection
/// was closed before any bytes were sent.
pub async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
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

/// Write an HTTP/1.1 response (JSON content type, `Connection: close`).
pub async fn write_response(
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

pub fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

// ── Response constructors ───────────────────────────────────────────────────

pub fn ok_json(value: serde_json::Value) -> Response {
    Response {
        status: 200,
        body: value.to_string().into_bytes(),
    }
}

pub fn no_content() -> Response {
    Response {
        status: 204,
        body: Vec::new(),
    }
}

pub fn bad_request(message: &str) -> Response {
    Response {
        status: 400,
        body: serde_json::json!({"error": {"kind": "bad_request", "message": message}})
            .to_string()
            .into_bytes(),
    }
}

pub fn not_found(method: &str, path: &str) -> Response {
    Response {
        status: 404,
        body: serde_json::json!({
            "error": {"kind": "not_found", "message": format!("no route for {method} {path}")}
        })
        .to_string()
        .into_bytes(),
    }
}

// ── Client side ─────────────────────────────────────────────────────────────

/// Errors from an outbound HTTP request.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// Couldn't connect, write, or read (network/parse).
    #[error("transport error: {0}")]
    Transport(String),
}

/// A received HTTP response (client side). The caller inspects `status` to
/// decide success/retry — non-2xx is a valid response, not an `HttpError`.
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Minimal HTTP/1.1 client for outbound pushes to tikod.
///
/// Used by the observer loop (`POST /vms/{id}/reports`) and the scaler loop
/// (`POST /vms/{id}/snapshot-request`). Each call opens a fresh connection
/// (`Connection: close`) — the push volume is low (one request per tick) so
/// keep-alive adds complexity without benefit.
pub struct HttpClient {
    target: SocketAddr,
}

impl HttpClient {
    pub fn new(target: SocketAddr) -> Self {
        Self { target }
    }

    /// Send a request with an optional JSON body. Returns the full response
    /// (status + body bytes) on any HTTP status, or [`HttpError::Transport`]
    /// on connect/write/read failure.
    pub async fn send_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<HttpResponse, HttpError> {
        let body_bytes = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {len}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            host = self.target,
            len = body_bytes.len(),
            body = body_bytes,
        );

        let mut stream = TcpStream::connect(self.target)
            .await
            .map_err(|e| HttpError::Transport(format!("connect: {e}")))?;

        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| HttpError::Transport(format!("write: {e}")))?;

        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .map_err(|e| HttpError::Transport(format!("read: {e}")))?;

        let text = String::from_utf8_lossy(&buf);
        let (status, body_offset) = parse_status_line(&text)?;
        let body = text[body_offset..].as_bytes().to_vec();

        Ok(HttpResponse { status, body })
    }
}

/// Parse the HTTP status line and return `(status_code, body_start_offset)`.
fn parse_status_line(text: &str) -> Result<(u16, usize), HttpError> {
    let header_end = text
        .find("\r\n\r\n")
        .ok_or_else(|| HttpError::Transport("malformed HTTP response".into()))?;
    let status = text[..header_end]
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| HttpError::Transport("malformed HTTP status line".into()))?;
    Ok((status, header_end + 4))
}
