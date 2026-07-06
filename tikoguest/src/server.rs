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
//! | `GET`  | `/services`    | service list | `{"services":[{name,status},...]}`                      |
//! | `*`    | `/services/{name}/*` | service dispatch | forwarded to registered [`Service`](crate::service::Service) |
//! | `GET`  | `/pg/status`   | full status  | `{"initialized","running","ready","pid","version",...}` |
//! | `POST` | `/pg/init`     | initdb       | 204  body: `{"force":bool}` (409 if running/initialized) |
//! | `POST` | `/pg/start`    | pg_ctl start | 204                                                     |
//! | `POST` | `/pg/stop`     | pg_ctl stop  | 204  body: `{"mode":"fast\|smart\|immediate"}`          |
//! | `POST` | `/pg/restart`  | restart      | 204                                                     |
//! | `POST` | `/pg/reload`   | reload config| 204                                                     |
//! | `GET`  | `/pg/config`   | read config  | `{"settings":{name:value,...}}`                         |
//! | `PUT`  | `/pg/config`   | write config | 204  body: `{"settings":{name:value}}` (then reloads)   |
//! | `GET`  | `/pitr/list`   | list backups | 200  `{"stdout":...,"stderr":...}`                     |
//! | `POST` | `/pitr/backup` | take backup  | 200  `{"stdout":...,"stderr":...}`                     |
//! | `POST` | `/pitr/recover`| recover PITR | 200  body: `{"time":"..."}` or `{"lsn":"..."}`          |
//!
//! Every spawned `pg_ctl` / `initdb` inherits the per-VM Tiko identity
//! (`TIKO_ORG_ID` / `TIKO_DB_ID` / `TIKO_PROJECT_ID` / `TIKO_STORAGE_ROOT` /
//! `TIKO_LOCAL_PATH`), loaded from `tiko.env` so the in-guest tikoworker
//! extension sees the correct org/db/project. See `env::load_tiko_env`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tracing::{debug, error, info};

use crate::http::{read_request, write_response, ok_json, no_content, bad_request, not_found, Request, Response};
use crate::pgops::{PgCtl, PgCtlError, PgCtlResult, StopMode};
use crate::service::ServiceRegistry;

/// HTTP control server wrapping a [`PgCtl`] + [`ServiceRegistry`].
pub struct PgServer {
    ctl: PgCtl,
    services: ServiceRegistry,
    /// Path to the `tiko_pitr` wrapper (`/usr/local/bin/tiko_pitr`).
    tiko_pitr: PathBuf,
}

impl PgServer {
    pub fn new(ctl: PgCtl, tiko_pitr: PathBuf) -> Self {
        Self {
            ctl,
            services: ServiceRegistry::new(),
            tiko_pitr,
        }
    }

    /// Register an in-VM service for `/services/{name}/*` dispatch.
    pub fn with_service(mut self, service: Arc<dyn crate::service::Service>) -> Self {
        self.services.register(service);
        self
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

            // Service registry: list registered services.
            ("GET", ["services"]) => self.list_services(),

            // Service dispatch: /services/{name}/* → registered service.
            (_, ["services", name, rest @ ..]) => {
                match self.services.route(name, method, rest, &req.body) {
                    Some(resp) => resp,
                    None => not_found(method, &format!("/services/{name}")),
                }
            }

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

            ("GET", ["pitr", "list"]) => self.pitr_list().await,
            ("POST", ["pitr", "backup"]) => self.pitr_backup().await,
            ("POST", ["pitr", "recover"]) => self.pitr_recover(&req.body).await,

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

    /// `GET /services` — list registered services and their status.
    fn list_services(&self) -> Response {
        let services: Vec<_> = self
            .services
            .list()
            .into_iter()
            .map(|(name, status)| serde_json::json!({"name": name, "status": status}))
            .collect();
        ok_json(serde_json::json!({"services": services}))
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

    /// `GET /pitr/list` — spawn `tiko_pitr list` and return its output.
    async fn pitr_list(&self) -> Response {
        let mut cmd = Command::new(&self.tiko_pitr);
        cmd.arg("list");
        run_pitr(cmd).await
    }

    /// `POST /pitr/backup` — spawn `tiko_pitr backup` and return its output.
    async fn pitr_backup(&self) -> Response {
        let mut cmd = Command::new(&self.tiko_pitr);
        cmd.arg("backup");
        run_pitr(cmd).await
    }

    /// `POST /pitr/recover` — spawn `tiko_pitr recover` with the target parsed
    /// from the JSON body (`time` or `lsn`, optional `timeline` /
    /// `recovery_timeout`). Other recover args use the wrapper's env defaults.
    async fn pitr_recover(&self, body: &[u8]) -> Response {
        #[derive(serde::Deserialize)]
        struct RecoverBody {
            time: Option<String>,
            lsn: Option<String>,
            timeline: Option<String>,
            recovery_timeout: Option<u64>,
        }

        let body: RecoverBody = if body.is_empty() {
            return bad_request("recover requires 'time' or 'lsn'");
        } else {
            match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return bad_request(&format!("invalid recover body: {e}")),
            }
        };

        let mut cmd = Command::new(&self.tiko_pitr);
        cmd.arg("recover");
        match (body.time.as_deref(), body.lsn.as_deref()) {
            (Some(t), None) => {
                cmd.arg("--time").arg(t);
            }
            (None, Some(l)) => {
                cmd.arg("--lsn").arg(l);
            }
            (Some(_), Some(_)) => {
                return bad_request("'time' and 'lsn' are mutually exclusive");
            }
            (None, None) => {
                return bad_request("recover requires 'time' or 'lsn'");
            }
        }
        if let Some(tl) = &body.timeline {
            cmd.arg("--timeline").arg(tl);
        }
        if let Some(timeout) = body.recovery_timeout {
            cmd.arg("--recovery-timeout").arg(timeout.to_string());
        }
        run_pitr(cmd).await
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

/// Run a `tiko_pitr` subcommand and format the result as an HTTP [`Response`].
///
/// - Exit 0 → `200 {"stdout":...,"stderr":...}`
/// - Exit non-0 → `500 {"error":{"kind":"pitr_error","exit_code":N,...}}`
/// - Spawn failure → `500 {"error":{"kind":"spawn_error","message":...}}`
async fn run_pitr(mut cmd: Command) -> Response {
    match cmd.output().await {
        Ok(out) if out.status.success() => ok_json(serde_json::json!({
            "stdout": String::from_utf8_lossy(&out.stdout),
            "stderr": String::from_utf8_lossy(&out.stderr),
        })),
        Ok(out) => Response {
            status: 500,
            body: serde_json::json!({
                "error": {
                    "kind": "pitr_error",
                    "exit_code": out.status.code(),
                    "stdout": String::from_utf8_lossy(&out.stdout),
                    "stderr": String::from_utf8_lossy(&out.stderr),
                }
            })
            .to_string()
            .into_bytes(),
        },
        Err(e) => Response {
            status: 500,
            body: serde_json::json!({
                "error": {"kind": "spawn_error", "message": e.to_string()}
            })
            .to_string()
            .into_bytes(),
        },
    }
}
