//! PostgREST in-guest service.
//!
//! One PostgREST per VM, co-located next to Postgres and spawned by tikoguest
//! — the same lifecycle model as PG via `pg_ctl` (no systemd unit). The host
//! `tikod` reaches it through the guest agent's `/services/postgrest/*` control
//! routes and proxies external HTTP to `127.0.0.1:3000` inside the guest.
//!
//! Lifecycle:
//! - [`PostgRest::start`] waits for Postgres to accept connections, provisions
//!   the PostgREST roles (`authenticator`/`anon`) + the `NOTIFY pgrst` event
//!   trigger via `psql` (idempotent), then spawns `postgrest <conf>`.
//! - [`PostgRest::stop`] terminates the child.
//! - [`PostgRest::reload`] sends SIGUSR2 (PostgREST rebuilds its schema cache).
//!
//! `handle_request` is synchronous per the [`Service`] trait; the async work is
//! driven to completion with `block_in_place` + `Handle::block_on` so callers
//! (e.g. tikod's `create_db`) get synchronous success/failure. This is safe on
//! tikoguest's multi-threaded runtime.
//!
//! Freeze/thaw needs no handling here: Firecracker snapshots capture full VM
//! memory, so a running PostgREST resumes with Postgres on restore.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::http::{Response, no_content, not_found, ok_json};
use crate::pgops::PgCtl;
use crate::service::{Service, ServiceStatus};

/// How long `start` waits for Postgres to accept connections before giving up.
const PG_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The application database PostgREST serves. This is the only user database on
/// a Tiko VM (`tt` is the PGDATA directory name, not a database). It MUST match
/// the `db-uri` in `postgrest.conf` so the setup SQL's event trigger lands in
/// the database PostgREST connects to.
const PG_DB: &str = "postgres";

/// A managed PostgREST instance.
pub struct PostgRest {
    /// Postgres control handle (readiness check + identity env + psql sibling).
    ctl: PgCtl,
    /// `postgrest` binary.
    postgrest: PathBuf,
    /// `postgrest.conf` (baked into the rootfs).
    config: PathBuf,
    /// `postgrest_setup.sql` (idempotent role/trigger provisioning).
    setup_sql: PathBuf,
    /// The spawned postgrest child, if alive.
    child: Mutex<Option<Child>>,
    /// PID of the spawned child (0 = none). Fast sync liveness probe for
    /// [`Service::status`], which can't await `Child::try_wait`.
    pid: AtomicI32,
}

impl PostgRest {
    /// Build a service around the given Postgres handle and PostgREST paths.
    /// Defaults point at the rootfs layout (`create_rootfs.sh`).
    pub fn new(ctl: PgCtl) -> Self {
        Self {
            ctl,
            postgrest: PathBuf::from("/usr/local/bin/postgrest"),
            config: PathBuf::from("/var/lib/postgresql/postgrest.conf"),
            setup_sql: PathBuf::from("/var/lib/postgresql/postgrest_setup.sql"),
            child: Mutex::new(None),
            pid: AtomicI32::new(0),
        }
    }

    /// Override the `postgrest` binary path (testing).
    #[allow(dead_code)]
    pub fn with_postgrest_bin(mut self, path: impl Into<PathBuf>) -> Self {
        self.postgrest = path.into();
        self
    }

    /// Override the config path (testing).
    #[allow(dead_code)]
    pub fn with_config(mut self, path: impl Into<PathBuf>) -> Self {
        self.config = path.into();
        self
    }

    /// Override the setup-SQL path (testing).
    #[allow(dead_code)]
    pub fn with_setup_sql(mut self, path: impl Into<PathBuf>) -> Self {
        self.setup_sql = path.into();
        self
    }

    // ── Operations ─────────────────────────────────────────────────────────

    /// Start PostgREST: wait for Postgres, provision roles, spawn the child.
    /// A no-op (Ok) if already running.
    async fn start(&self) -> Result<(), Response> {
        if self.is_alive() {
            debug!("postgrest already running — start is a no-op");
            return Ok(());
        }
        self.wait_pg_ready().await?;
        self.run_setup().await?;
        self.spawn().await
    }

    /// Stop PostgREST. A no-op (Ok) if not running.
    async fn stop(&self) -> Result<(), Response> {
        let mut guard = self.child.lock().await;
        if let Some(mut child) = guard.take() {
            let pid = child.id();
            // Best effort: ask postgrest to exit, then escalate to kill.
            if let Some(pid) = pid
                && let Err(e) = nix_kill(pid as i32, libc::SIGTERM)
            {
                warn!(error = %e, "SIGTERM to postgrest failed");
            }
            match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                Ok(Ok(status)) => info!(%status, "postgrest stopped"),
                Ok(Err(e)) => warn!(error = %e, "postgrest wait failed"),
                Err(_) => {
                    warn!("postgrest didn't exit on SIGTERM — killing");
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }
        }
        self.pid.store(0, Ordering::SeqCst);
        info!("postgrest stopped");
        Ok(())
    }

    /// Reload the schema cache (SIGUSR2).
    async fn reload(&self) -> Result<(), Response> {
        let pid = self.pid.load(Ordering::SeqCst);
        if pid == 0 {
            return Err(service_err("not_running", 409, "postgrest is not running"));
        }
        match nix_kill(pid, libc::SIGUSR2) {
            Ok(()) => {
                info!(pid, "postgrest reload (SIGUSR2) sent");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, pid, "failed to signal postgrest");
                Err(service_err("signal_error", 500, &format!("{e}")))
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Wait for Postgres to accept connections (bounded).
    async fn wait_pg_ready(&self) -> Result<(), Response> {
        let deadline = Instant::now() + PG_READY_TIMEOUT;
        loop {
            match self.ctl.ready().await {
                Ok(true) => return Ok(()),
                Ok(false) => debug!("waiting for postgres to accept connections"),
                Err(e) => debug!(error = %e, "postgres readiness check errored"),
            }
            if Instant::now() >= deadline {
                return Err(service_err(
                    "postgres_not_ready",
                    503,
                    "timed out waiting for postgres to accept connections",
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Run the idempotent role/trigger provisioning SQL via `psql`.
    async fn run_setup(&self) -> Result<(), Response> {
        let psql = sibling_binary(&self.ctl.pg_ctl, "psql");
        let output = Command::new(&psql)
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-U")
            .arg("postgres")
            .arg("-d")
            .arg(PG_DB)
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-q")
            .arg("-f")
            .arg(&self.setup_sql)
            .envs(&self.ctl.tiko_env)
            .output()
            .await;
        match output {
            Ok(o) if o.status.success() => {
                debug!("postgrest setup sql applied");
                Ok(())
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                error!(%stderr, code = ?o.status.code(), "postgrest setup sql failed");
                Err(service_err(
                    "setup_failed",
                    502,
                    &format!("postgrest setup sql failed: {stderr}"),
                ))
            }
            Err(e) => Err(service_err(
                "spawn_error",
                500,
                &format!("failed to spawn psql: {e}"),
            )),
        }
    }

    /// Spawn the postgrest process against the config file.
    async fn spawn(&self) -> Result<(), Response> {
        let child = Command::new(&self.postgrest)
            .arg(&self.config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(false)
            .envs(&self.ctl.tiko_env)
            .spawn()
            .map_err(|e| {
                error!(error = %e, bin = %self.postgrest.display(), "failed to spawn postgrest");
                service_err(
                    "spawn_error",
                    500,
                    &format!("failed to spawn postgrest: {e}"),
                )
            })?;
        if let Some(pid) = child.id() {
            self.pid.store(pid as i32, Ordering::SeqCst);
        }
        let mut guard = self.child.lock().await;
        *guard = Some(child);
        info!(config = %self.config.display(), "postgrest started");
        Ok(())
    }

    /// Sync liveness probe via `kill(pid, 0)`.
    fn is_alive(&self) -> bool {
        let pid = self.pid.load(Ordering::SeqCst);
        pid != 0 && nix_kill(pid, 0).is_ok()
    }

    /// JSON snapshot for `GET /services/postgrest`.
    fn status_json(&self) -> Response {
        let running = self.is_alive();
        let pid = self.pid.load(Ordering::SeqCst);
        ok_json(serde_json::json!({
            "status": if running { "running" } else { "stopped" },
            "pid": if pid != 0 { Some(pid) } else { None },
        }))
    }

    /// Drive the async request body to completion. See module docs.
    async fn handle_async(&self, method: &str, rest: &[&str]) -> Response {
        match (method, rest) {
            ("GET", []) => self.status_json(),
            ("POST", ["start"]) => match self.start().await {
                Ok(()) => no_content(),
                Err(r) => r,
            },
            ("POST", ["stop"]) => match self.stop().await {
                Ok(()) => no_content(),
                Err(r) => r,
            },
            ("POST", ["reload"]) => match self.reload().await {
                Ok(()) => no_content(),
                Err(r) => r,
            },
            ("GET", ["status"]) => self.status_json(),
            _ => not_found(method, &format!("/services/postgrest/{}", rest.join("/"))),
        }
    }
}

impl Service for PostgRest {
    fn name(&self) -> &str {
        "postgrest"
    }

    fn handle_request(&self, method: &str, rest: &[&str], _body: &[u8]) -> Response {
        // `Service::handle_request` is synchronous; the lifecycle ops are async
        // (pg_ctl readiness, psql, spawn). Drive them inline on the multi-
        // threaded runtime so callers get synchronous results.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.handle_async(method, rest))
        })
    }

    fn status(&self) -> ServiceStatus {
        if self.is_alive() {
            ServiceStatus::Running
        } else {
            ServiceStatus::Stopped
        }
    }
}

/// Send a signal to a process (libpq-free; `kill(2)` wrapper).
fn nix_kill(pid: i32, sig: i32) -> std::io::Result<()> {
    // Safety: `kill` is async-signal-safe and reads only the integer args.
    let rc = unsafe { libc::kill(pid, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// A structured-error [`Response`] for service operations.
fn service_err(kind: &str, status: u16, message: &str) -> Response {
    Response {
        status,
        body: serde_json::json!({ "error": { "kind": kind, "message": message } })
            .to_string()
            .into_bytes(),
    }
}

/// Derive a sibling binary (e.g. `psql`) of `pg_ctl`: same parent directory.
/// Falls back to `name` on `PATH` if `pg_ctl` has no parent.
fn sibling_binary(pg_ctl: &std::path::Path, name: &str) -> PathBuf {
    match pg_ctl.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(name),
        _ => PathBuf::from(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_stopped() {
        let svc = PostgRest::new(PgCtl::new(
            PathBuf::from("pg_ctl"),
            PathBuf::from("/tmp/tt"),
            PathBuf::from("/tmp/log.log"),
            PathBuf::from("/tmp/postgresql.tiko.conf"),
        ));
        assert!(matches!(svc.status(), ServiceStatus::Stopped));
        assert!(!svc.is_alive());
        let json = svc.status_json();
        assert_eq!(json.status, 200);
        assert!(String::from_utf8_lossy(&json.body).contains("stopped"));
    }

    #[test]
    fn reload_when_stopped_is_conflict() {
        // No child → reload must error 409 (not_running). Run in a multi-threaded
        // runtime because handle_request uses block_in_place.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let svc = PostgRest::new(PgCtl::new(
            PathBuf::from("pg_ctl"),
            PathBuf::from("/tmp/tt"),
            PathBuf::from("/tmp/log.log"),
            PathBuf::from("/tmp/postgresql.tiko.conf"),
        ));
        let resp = rt.block_on(async { svc.handle_async("POST", &["reload"]).await });
        assert_eq!(resp.status, 409);
        assert!(String::from_utf8_lossy(&resp.body).contains("not_running"));
    }

    #[test]
    fn unknown_route_is_404() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let svc = PostgRest::new(PgCtl::new(
            PathBuf::from("pg_ctl"),
            PathBuf::from("/tmp/tt"),
            PathBuf::from("/tmp/log.log"),
            PathBuf::from("/tmp/postgresql.tiko.conf"),
        ));
        let resp = rt.block_on(async { svc.handle_async("DELETE", &["purge"]).await });
        assert_eq!(resp.status, 404);
    }
}
