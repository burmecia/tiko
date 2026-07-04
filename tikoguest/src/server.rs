//! HTTP control API surface for the guest agent.
//!
//! Raw HTTP/1.1 over TCP — same minimal-dependency style as tikod's API server.
//! Runs inside the guest, bound to `0.0.0.0:<port>` so tikod can reach it over
//! the guest IP.
//!
//! # Routes
//!
//! | Method | Path           | Handler      | Body / Returns                                          |
//! |--------|----------------|--------------|---------------------------------------------------------|
//! | `GET`  | `/health`      | liveness     | `{"status":"ok","initialized":bool,"running":bool}`     |
//! | `GET`  | `/pg/status`   | full status  | `{"initialized","running","ready","pid","version",...}` |
//! | `POST` | `/pg/init`     | initdb       | 204  body: `{"force":bool}` (409 if running/initialized) |
//! | `POST` | `/pg/start`    | pg_ctl start | 204                                                     |
//! | `POST` | `/pg/stop`     | pg_ctl stop  | 204  body: `{"mode":"fast\|smart\|immediate"}`          |
//! | `POST` | `/pg/restart`  | restart      | 204                                                     |
//! | `POST` | `/pg/reload`   | reload config| 204                                                     |
//! | `GET`  | `/pg/config`   | read config  | `{"settings":{name:value,...}}`                         |
//! | `PUT`  | `/pg/config`   | write config | 204  body: `{"settings":{name:value}}` (then reloads)   |
//!
//! Every spawned `pg_ctl` / `initdb` inherits the per-VM Tiko identity
//! (`TIKO_ORG_ID` / `TIKO_DB_ID` / `TIKO_PROJECT_ID` / `TIKO_STORAGE_ROOT` /
//! `TIKO_LOCAL_PATH`), loaded from `tiko.env` so the in-guest tikoworker
//! extension sees the correct org/db/project. See `pgops::load_tiko_env`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::pgops::{PgCtl, PgCtlError, PgCtlResult, StopMode};

/// Maximum accepted request header block size.
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// HTTP control server wrapping a [`PgCtl`].
pub struct PgServer {
    ctl: PgCtl,
}

impl PgServer {
    pub fn new(ctl: PgCtl) -> Self {
        Self { ctl }
    }

    pub async fn run(self: Arc<Self>, listen_addr: SocketAddr) -> std::io::Result<()> {
        let listener = TcpListener::bind(listen_addr).await?;
        info!(addr = %listen_addr, data_dir = %self.ctl.data_dir.display(), "tikoguest listening");
        self.serve(listener).await
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.handle_connection(stream, addr).await {
                            error!(client = %addr, error = %e, "tikoguest connection failed");
                        }
                    });
                }
                Err(e) => error!(error = %e, "tikoguest accept failed"),
            }
        }
    }

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
        debug!(client = %addr, method = %req.method, path = %req.path, "tikoguest request");
        let resp = self.route(&req).await;
        write_response(&mut stream, resp.status, &resp.body).await
    }

    async fn route(&self, req: &Request) -> Response {
        let path = req.path.split('?').next().unwrap_or("");
        let segs: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        let method = req.method.as_str();

        match (method, segs.as_slice()) {
            ("GET", ["health"]) => self.health().await,

            ("GET", ["pg", "status"]) => self.status().await,
            ("POST", ["pg", "start"]) => match self.ctl.start().await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("POST", ["pg", "stop"]) => {
                let mode = if req.body.is_empty() {
                    StopMode::default()
                } else {
                    match serde_json::from_slice::<StopBody>(&req.body) {
                        Ok(b) => b.mode.unwrap_or_default(),
                        Err(_) => {
                            return bad_request("invalid stop body; expected {\"mode\":...}");
                        }
                    }
                };
                match self.ctl.stop(mode).await {
                    Ok(()) => no_content(),
                    Err(e) => err_resp(&e),
                }
            }
            ("POST", ["pg", "restart"]) => match self.ctl.restart().await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("POST", ["pg", "reload"]) => match self.ctl.reload().await {
                Ok(()) => no_content(),
                Err(e) => err_resp(&e),
            },
            ("POST", ["pg", "init"]) => {
                // Body is optional: `{"force": true}` wipes an existing cluster.
                let force = if req.body.is_empty() {
                    false
                } else {
                    match serde_json::from_slice::<InitBody>(&req.body) {
                        Ok(b) => b.force.unwrap_or(false),
                        Err(_) => {
                            return bad_request("invalid init body; expected {\"force\":bool}");
                        }
                    }
                };
                match self.ctl.init(force).await {
                    Ok(()) => no_content(),
                    Err(e) => err_resp(&e),
                }
            }
            ("GET", ["pg", "config"]) => match self.ctl.read_config() {
                Ok(settings) => ok_json(serde_json::json!({"settings": settings})),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["pg", "config"]) => match self.parse_config_body(&req.body) {
                Ok(settings) => match self.ctl.write_config(&settings) {
                    Ok(()) => match self.ctl.reload().await {
                        Ok(()) => no_content(),
                        Err(e) => err_resp(&e),
                    },
                    Err(e) => err_resp(&e),
                },
                Err(r) => r,
            },

            _ => not_found(method, path),
        }
    }

    /// `GET /health` — cheap liveness + coarse PG state (no pg_ctl spawn for
    /// the running flag; we infer from postmaster.pid presence).
    async fn health(&self) -> Response {
        let initialized = self.ctl.is_initialized();
        let running = self.ctl.pid().is_some();
        ok_json(serde_json::json!({
            "status": "ok",
            "initialized": initialized,
            "running": running,
        }))
    }

    /// `GET /pg/status` — full status (spawns `pg_ctl status`).
    async fn status(&self) -> Response {
        let initialized = self.ctl.is_initialized();
        let running = match self.ctl.running().await {
            Ok(r) => r,
            Err(e) => return err_resp(&e),
        };
        let pid = self.ctl.pid();
        let version = self.ctl.version();
        ok_json(serde_json::json!({
            "initialized": initialized,
            "running": running,
            "ready": running,
            "pid": pid,
            "version": version,
            "data_dir": self.ctl.data_dir.to_string_lossy(),
            "config_file": self.ctl.config_file.to_string_lossy(),
        }))
    }

    fn parse_config_body(&self, body: &[u8]) -> Result<BTreeMap<String, String>, Response> {
        #[derive(serde::Deserialize)]
        struct ConfigBody {
            settings: BTreeMap<String, String>,
        }
        let parsed: ConfigBody = serde_json::from_slice(body).map_err(|e| Response {
            status: 400,
            body: serde_json::json!({
                "error": {"kind": "bad_request", "message": format!("invalid config body: {e}")}
            })
            .to_string()
            .into_bytes(),
        })?;
        Ok(parsed.settings)
    }
}

/// Body for `POST /pg/stop`.
#[derive(serde::Deserialize)]
struct StopBody {
    mode: Option<StopMode>,
}

/// Body for `POST /pg/init`.
#[derive(serde::Deserialize)]
struct InitBody {
    force: Option<bool>,
}

// ============================================================================
// HTTP/1.1 parsing & writing (same shape as tikod/src/api/server.rs)
// ============================================================================

struct Request {
    method: String,
    path: String,
    #[allow(dead_code)]
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct Response {
    status: u16,
    body: Vec<u8>,
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
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

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

// ── Response constructors ────────────────────────────────────────────────

fn ok_json(value: serde_json::Value) -> Response {
    Response {
        status: 200,
        body: value.to_string().into_bytes(),
    }
}

fn no_content() -> Response {
    Response {
        status: 204,
        body: Vec::new(),
    }
}

fn bad_request(message: &str) -> Response {
    Response {
        status: 400,
        body: serde_json::json!({"error": {"kind": "bad_request", "message": message}})
            .to_string()
            .into_bytes(),
    }
}

fn not_found(method: &str, path: &str) -> Response {
    Response {
        status: 404,
        body: serde_json::json!({
            "error": {"kind": "not_found", "message": format!("no route for {method} {path}")}
        })
        .to_string()
        .into_bytes(),
    }
}

fn err_resp(err: &PgCtlError) -> Response {
    let message = err.to_string();
    let (kind, status) = match err {
        PgCtlError::NotInitialized(_) => ("not_initialized", 409),
        PgCtlError::AlreadyInitialized(_) => ("already_initialized", 409),
        PgCtlError::StillRunning => ("still_running", 409),
        PgCtlError::ConfigParse(_) => ("config_error", 400),
        PgCtlError::CommandFailed { .. } => ("pg_ctl_error", 500),
        PgCtlError::InitdbFailed { .. } => ("initdb_error", 500),
        PgCtlError::Io(_) => ("io_error", 500),
    };
    Response {
        status,
        body: serde_json::json!({"error": {"kind": kind, "message": message}})
            .to_string()
            .into_bytes(),
    }
}

// Re-export the result alias so callers wiring the server can name it.
#[allow(dead_code)]
pub type ServerResult<T> = PgCtlResult<T>;
