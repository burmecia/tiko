//! tikod DB control route round-trip tests.
//!
//! Exercises the full path `HTTP API → ApiServer → GuestClient → in-guest agent`
//! with a **mock Vmm** (so we control the guest IP) and a **fake agent** (so we
//! control Postgres control responses). No real VM or Postgres required.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use tikod::api::ApiServer;
use tikod::control::Control;
use tikod::node::Node;
use tikod::vmm::{Snapshot, VmConfig, VmId, VmInfo, VmState, Vmm, VmmError};

// ── Mock Vmm: lets the test dictate the guest IP per call ──────────────────

struct MockVmm {
    /// Guest IP returned by `vm_guest_ip`. `None` simulates "IP undiscovered".
    guest_ip: Option<IpAddr>,
    /// Known VM ids (anything else → VmNotFound).
    known: std::collections::HashSet<String>,
}

#[async_trait]
impl Vmm for MockVmm {
    async fn create_vm(&self, config: VmConfig) -> Result<VmId, VmmError> {
        Ok(config.vm_id)
    }
    async fn start_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn pause_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn resume_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn snapshot_vm(&self, _: &VmId) -> Result<Snapshot, VmmError> {
        Err(VmmError::Backend("not implemented in mock".into()))
    }
    async fn restore_vm(&self, snap: &Snapshot) -> Result<VmId, VmmError> {
        Ok(snap.vm_id.clone())
    }
    async fn destroy_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn vm_state(&self, id: &VmId) -> Result<VmState, VmmError> {
        if self.known.contains(id) {
            Ok(VmState::Running)
        } else {
            Err(VmmError::VmNotFound(id.clone()))
        }
    }
    async fn vm_guest_ip(&self, id: &VmId) -> Result<Option<IpAddr>, VmmError> {
        if !self.known.contains(id) {
            return Err(VmmError::VmNotFound(id.clone()));
        }
        Ok(self.guest_ip)
    }
    async fn list_vms(&self) -> Result<Vec<VmInfo>, VmmError> {
        Ok(self
            .known
            .iter()
            .map(|vm_id| VmInfo {
                vm_id: vm_id.clone(),
                state: VmState::Running,
                guest_ip: self.guest_ip,
            })
            .collect())
    }
}

// ── Fake agent: a canned HTTP responder for tikoguest routes ────────────────

/// A minimal HTTP/1.1 server that returns canned responses keyed by
/// `(method, path)`. Records the last config PUT body.
struct FakeAgent {
    listen: SocketAddr,
    config_put: Arc<std::sync::Mutex<Option<BTreeMap<String, String>>>>,
    /// Last request body received on any route (for body-forwarding asserts).
    last_body: Arc<std::sync::Mutex<Option<String>>>,
    /// Last body received on `POST /branch/restore` (captured separately so a
    /// later `/pg/start` with an empty body doesn't clobber it).
    last_restore_body: Arc<std::sync::Mutex<Option<String>>>,
}

impl FakeAgent {
    /// Start on an ephemeral port. Routes:
    /// - `GET /pg/status` → a running cluster
    /// - `GET /pg/config` → a fixed setting
    /// - `POST /pg/{start,stop,restart,reload}` / `PUT /pg/config` → 204
    /// - `GET /health` → ok
    /// - `/pitr/*` and `/branch/*` → canned CLI-style JSON (200)
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap();
        let config_put = Arc::new(std::sync::Mutex::new(None));
        let last_body = Arc::new(std::sync::Mutex::new(None));
        let last_restore_body = Arc::new(std::sync::Mutex::new(None));

        let config_put_task = config_put.clone();
        let last_body_task = last_body.clone();
        let last_restore_body_task = last_restore_body.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let cfg = config_put_task.clone();
                let lb = last_body_task.clone();
                let lrb = last_restore_body_task.clone();
                tokio::spawn(async move {
                    let _ = handle(&mut stream, cfg, lb, lrb).await;
                });
            }
        });

        Self {
            listen,
            config_put,
            last_body,
            last_restore_body,
        }
    }

    /// The last `PUT /pg/config` body the agent received.
    fn last_config_put(&self) -> Option<BTreeMap<String, String>> {
        self.config_put.lock().unwrap().clone()
    }

    /// The last request body the agent received on any route.
    fn last_body(&self) -> Option<String> {
        self.last_body.lock().unwrap().clone()
    }

    /// The last `POST /branch/restore` request body the agent received.
    fn last_restore_body(&self) -> Option<String> {
        self.last_restore_body.lock().unwrap().clone()
    }
}

async fn handle(
    stream: &mut TcpStream,
    config_put: Arc<std::sync::Mutex<Option<BTreeMap<String, String>>>>,
    last_body: Arc<std::sync::Mutex<Option<String>>>,
    last_restore_body: Arc<std::sync::Mutex<Option<String>>>,
) -> std::io::Result<()> {
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let req_line = text.lines().next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    // Record the request body (after the blank line) for forwarding asserts.
    let req_body = text
        .find("\r\n\r\n")
        .map(|i| text[i + 4..].trim().to_string())
        .filter(|s| !s.is_empty());
    *last_body.lock().unwrap() = req_body;

    // Each route returns an explicit (status, body). Action endpoints are 204;
    // reads are 200 JSON; unknown is 404.
    let (status, body): (u16, &str) = match (method, path) {
        ("GET", "/health") => (
            200,
            r#"{"status":"ok","initialized":true,"running":true,"storage_ready":true}"#,
        ),
        ("GET", "/pg/status") => (
            200,
            r#"{"initialized":true,"running":true,"ready":true,"pid":4242,"version":"17.0","data_dir":"/var/lib/postgresql/tt","config_file":"/var/lib/postgresql/tt/postgresql.tiko.conf"}"#,
        ),
        ("GET", "/pg/config") => (
            200,
            r#"{"settings":{"max_connections":"100","log_min_messages":"info"}}"#,
        ),
        ("POST", "/pg/start")
        | ("POST", "/pg/stop")
        | ("POST", "/pg/restart")
        | ("POST", "/pg/reload") => (204, ""),
        ("POST", "/pg/init") => (204, ""),
        // PostgREST service control (forwarded to the in-guest tikoguest agent).
        ("POST", "/services/postgrest/start")
        | ("POST", "/services/postgrest/stop")
        | ("POST", "/services/postgrest/reload") => (204, ""),
        ("GET", "/services/postgrest") | ("GET", "/services/postgrest/status") => (
            200,
            r#"{"status":"running","pid":1234}"#,
        ),
        ("PUT", "/pg/config") => {
            // Capture the request body (after the blank line).
            if let Some(body_start) = text.find("\r\n\r\n") {
                if let Ok(parsed) =
                    serde_json::from_str::<serde_json::Value>(&text[body_start + 4..])
                {
                    if let Some(settings) = parsed.get("settings") {
                        if let Ok(map) =
                            serde_json::from_value::<BTreeMap<String, String>>(settings.clone())
                        {
                            *config_put.lock().unwrap() = Some(map);
                        }
                    }
                }
            }
            (204, "")
        }
        // PITR CLI-passthrough routes (canned tiko_pitr JSON).
        ("GET", "/pitr/list") => (200, r#"{"backups":[],"window":null}"#),
        ("POST", "/pitr/backup") => (
            200,
            r#"{"status":"backed_up","timeline":"00000001","checkpoint_lsn":"0/3000000","bytes_compressed":42}"#,
        ),
        ("POST", "/pitr/recover") => (
            200,
            r#"{"status":"recovered","target_kind":"time","target_value":"2026-01-01T00:00:00Z","timeline":"00000001"}"#,
        ),
        ("POST", "/pitr/restart") => (200, r#"{"status":"started"}"#),
        // Branch CLI-passthrough routes (canned tiko_branch JSON).
        ("PUT", "/branch/backup") => (
            200,
            r#"{"status":"backed_up","pack":"/data/branch_packs/1.tar.zst","timeline":"00000001","checkpoint_lsn":"0/3000000"}"#,
        ),
        ("POST", "/branch/restore") => {
            // Capture the restore body separately (a later /pg/start with an
            // empty body would otherwise clobber last_body).
            if let Some(body_start) = text.find("\r\n\r\n") {
                let rb = text[body_start + 4..].trim();
                if !rb.is_empty() {
                    *last_restore_body.lock().unwrap() = Some(rb.to_string());
                }
            }
            (
                200,
                r#"{"status":"restored","db_id":2,"project_id":2,"parent_db_id":1,"timeline":"00000001","checkpoint_lsn":"0/3000000"}"#,
            )
        }
        ("POST", "/branch/restart") => {
            (200, r#"{"status":"started","db_id":2,"branch_port":5432}"#)
        }
        _ => (
            404,
            r#"{"error":{"kind":"not_found","message":"fake agent has no such route"}}"#,
        ),
    };

    let resp = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_text(status),
        body.len(),
        body,
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        _ => "Error",
    }
}

// ── HTTP client helper ─────────────────────────────────────────────────────

async fn api_request(
    api_addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, String) {
    let body_bytes = body.unwrap_or("").as_bytes();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len(),
    )
    .into_bytes();
    let payload = [req.as_slice(), body_bytes].concat();

    let fut = async {
        let mut s = TcpStream::connect(api_addr).await.unwrap();
        s.write_all(&payload).await.unwrap();
        let mut r = Vec::new();
        s.read_to_end(&mut r).await.unwrap();
        String::from_utf8_lossy(&r).into_owned()
    };
    let text = tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .unwrap();
    let end = text.find("\r\n\r\n").unwrap();
    let status = text[..end]
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    (status, text[end + 4..].to_string())
}

/// Bring up an ApiServer whose mock VMs resolve their guest IP to the fake
/// agent's address. Returns the API address plus the fake agent (for body
/// assertions).
async fn harness() -> (SocketAddr, FakeAgent) {
    let agent = FakeAgent::start().await;
    // Mock VMs report the loopback IP so GuestClient connects to the fake agent.
    let guest_ip = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let mut known = std::collections::HashSet::new();
    known.insert("vm-1".to_string());

    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm { guest_ip, known });
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-db-test")));
    let control = Arc::new(Control::new());
    let server = Arc::new(ApiServer::new(node, control).with_agent_port(agent.listen.port()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    (api_addr, agent)
}

#[tokio::test]
async fn db_status_proxies_to_agent() {
    let (api, _agent) = harness().await;
    let (status, body) = api_request(api, "GET", "/vms/vm-1/db/status", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""running":true"#), "{body}");
    assert!(body.contains(r#""version":"17.0""#), "{body}");
}

#[tokio::test]
async fn db_health_proxies_to_agent() {
    let (api, _agent) = harness().await;
    let (status, body) = api_request(api, "GET", "/vms/vm-1/db/health", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""status":"ok""#), "{body}");
}

#[tokio::test]
async fn db_start_stop_restart_reload_return_204() {
    let (api, _agent) = harness().await;
    for action in ["start", "stop", "restart", "reload"] {
        let (status, body) =
            api_request(api, "POST", &format!("/vms/vm-1/db/{action}"), None).await;
        assert_eq!(status, 204, "action {action}: {body}");
    }
}

#[tokio::test]
async fn db_stop_invalid_mode_is_400() {
    let (api, _agent) = harness().await;
    let (status, body) = api_request(
        api,
        "POST",
        "/vms/vm-1/db/stop",
        Some(r#"{"mode":"explode"}"#),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("bad_request"), "{body}");
}

#[tokio::test]
async fn db_init_forwards_to_agent() {
    let (api, _agent) = harness().await;

    // Default (no force) → 204.
    let (status, body) = api_request(api, "POST", "/vms/vm-1/db/init", None).await;
    assert_eq!(status, 204, "{body}");

    // Explicit force:true → 204.
    let (status, body) =
        api_request(api, "POST", "/vms/vm-1/db/init", Some(r#"{"force":true}"#)).await;
    assert_eq!(status, 204, "{body}");

    // Invalid body → 400.
    let (status, body) =
        api_request(api, "POST", "/vms/vm-1/db/init", Some(r#"{"force":"x"}"#)).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("bad_request"), "{body}");
}

#[tokio::test]
async fn db_config_get_proxies_settings() {
    let (api, _agent) = harness().await;
    let (status, body) = api_request(api, "GET", "/vms/vm-1/db/config", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""max_connections":"100""#), "{body}");
}

#[tokio::test]
async fn db_config_put_forwards_settings_to_agent() {
    let (api, agent) = harness().await;
    let (status, body) = api_request(
        api,
        "PUT",
        "/vms/vm-1/db/config",
        Some(r#"{"settings":{"work_mem":"8MB","max_connections":"50"}}"#),
    )
    .await;
    assert_eq!(status, 204, "{body}");

    // The fake agent recorded the forwarded PUT body.
    let put = agent
        .last_config_put()
        .expect("agent never received config PUT");
    assert_eq!(put.get("work_mem").unwrap(), "8MB");
    assert_eq!(put.get("max_connections").unwrap(), "50");
}

#[tokio::test]
async fn db_route_on_unknown_vm_is_404_vm_not_found() {
    let (api, _agent) = harness().await;
    let (status, body) = api_request(api, "GET", "/vms/ghost/db/status", None).await;
    assert_eq!(status, 404, "{body}");
    assert!(body.contains("vm_not_found"), "{body}");
}

#[tokio::test]
async fn db_route_with_no_guest_ip_is_502() {
    // Mock VMs exist but report no guest IP.
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: None,
        known: ["vm-1".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-db-test-noip"),
    ));
    let control = Arc::new(Control::new());
    let server = Arc::new(ApiServer::new(node, control));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (status, body) = api_request(api_addr, "GET", "/vms/vm-1/db/status", None).await;
    assert_eq!(status, 502, "{body}");
    assert!(body.contains("agent_unreachable"), "{body}");
}

#[tokio::test]
async fn db_route_unreachable_agent_is_502() {
    // Guest IP points at loopback but nothing listens on the agent port.
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        known: ["vm-1".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-db-test-dead"),
    ));
    let control = Arc::new(Control::new());
    // Use a port almost certainly nothing listens on.
    let server = Arc::new(ApiServer::new(node, control).with_agent_port(9));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (status, body) = api_request(api_addr, "GET", "/vms/vm-1/db/status", None).await;
    assert_eq!(status, 502, "{body}");
    assert!(body.contains("agent_unreachable"), "{body}");
}

/// Contract guard: every `/vms/{id}/db/*` route must be forwarded to the agent
/// (not 404 at the tikod layer). This is the central-control-point inventory —
/// adding an agent endpoint without a tikod route fails here.
#[tokio::test]
async fn all_db_routes_are_forwarded() {
    let (api, _agent) = harness().await;
    // (method, path, optional body) for each route in the documented table.
    let routes: &[(&str, &str, Option<&str>)] = &[
        ("GET", "/vms/vm-1/db/health", None),
        ("GET", "/vms/vm-1/db/status", None),
        ("POST", "/vms/vm-1/db/init", None),
        ("POST", "/vms/vm-1/db/start", None),
        ("POST", "/vms/vm-1/db/stop", None),
        ("POST", "/vms/vm-1/db/restart", None),
        ("POST", "/vms/vm-1/db/reload", None),
        ("GET", "/vms/vm-1/db/config", None),
        (
            "PUT",
            "/vms/vm-1/db/config",
            Some(r#"{"settings":{"work_mem":"4MB"}}"#),
        ),
    ];
    for (method, path, body) in routes {
        let (status, resp_body) = api_request(api, method, path, *body).await;
        // Every route forwards (2xx from the fake agent). A 404 here means the
        // tikod router has no arm for it — i.e. the central API is missing it.
        assert!(
            (200..300).contains(&status),
            "{method} {path} not forwarded: status {status}, body: {resp_body}"
        );
    }

    // An unknown db sub-route must still 404 (not silently fall through).
    let (status, body) = api_request(api, "GET", "/vms/vm-1/db/nonsense", None).await;
    assert_eq!(status, 404, "{body}");
}

/// Contract guard: every `/vms/{id}/pitr/*` and `/vms/{id}/branch/*` route is
/// forwarded to the agent (2xx), not 404 at the tikod layer. Adding a guest
/// `/pitr` or `/branch` endpoint without a tikod mirror route fails here.
#[tokio::test]
async fn pitr_and_branch_routes_are_forwarded() {
    let (api, _agent) = harness().await;
    let routes: &[(&str, &str, Option<&str>)] = &[
        ("GET", "/vms/vm-1/pitr/list", None),
        ("POST", "/vms/vm-1/pitr/backup", None),
        (
            "POST",
            "/vms/vm-1/pitr/recover",
            Some(r#"{"time":"2026-01-01 00:00:00"}"#),
        ),
        ("POST", "/vms/vm-1/pitr/restart", None),
        ("PUT", "/vms/vm-1/branch/backup", None),
        (
            "POST",
            "/vms/vm-1/branch/restore",
            Some(r#"{"pack":"/p","db_id":2,"parent_db_id":1}"#),
        ),
        ("POST", "/vms/vm-1/branch/restart", None),
    ];
    for (method, path, body) in routes {
        let (status, resp_body) = api_request(api, method, path, *body).await;
        assert!(
            (200..300).contains(&status),
            "{method} {path} not forwarded: status {status}, body: {resp_body}"
        );
    }

    // Unknown pitr/branch sub-routes must 404 at the tikod layer (not fall
    // through to the per-VM router, which would misinterpret them).
    let (status, body) = api_request(api, "GET", "/vms/vm-1/pitr/nonsense", None).await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = api_request(api, "POST", "/vms/vm-1/branch/nonsense", None).await;
    assert_eq!(status, 404, "{body}");
}

/// The mirror routes forward the request body unchanged to the agent (e.g. the
/// `/branch/restore` `{pack,db_id,parent_db_id,...}` payload).
#[tokio::test]
async fn branch_restore_forwards_request_body() {
    let (api, agent) = harness().await;
    let payload = r#"{"pack":"/data/branch_packs/1.tar.zst","db_id":2,"parent_db_id":1}"#;
    let (status, body) = api_request(api, "POST", "/vms/vm-1/branch/restore", Some(payload)).await;
    assert_eq!(status, 200, "{body}");

    // The agent observed the exact body tikod forwarded.
    let forwarded = agent.last_body().expect("agent never received a body");
    assert_eq!(forwarded, payload, "forwarded body mismatch");
}

/// The `/pitr/*` and `/branch/*` routes proxy the agent's response verbatim —
/// including rich error bodies (CLI `stderr`/`exit_code`) that the typed
/// `send()` path would strip down to `{kind,message}`. This is the whole
/// reason `forward_raw` exists.
#[tokio::test]
async fn agent_error_body_passes_through_verbatim() {
    // Custom agent that always returns a 500 with stderr/exit_code in the body.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let agent_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await;
                let body = r#"{"error":{"kind":"branch_error","exit_code":1,"stderr":"boom"}}"#;
                let resp = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body,
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        known: ["vm-1".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-branch-err-test"),
    ));
    let control = Arc::new(Control::new());
    let server = Arc::new(ApiServer::new(node, control).with_agent_port(agent_addr.port()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (status, body) = api_request(api_addr, "PUT", "/vms/vm-1/branch/backup", None).await;
    assert_eq!(status, 500, "{body}");
    // The rich fields survive the gateway intact (forward_raw, not send()).
    assert!(body.contains(r#""kind":"branch_error""#), "{body}");
    assert!(body.contains(r#""exit_code":1"#), "{body}");
    assert!(body.contains(r#""stderr":"boom""#), "{body}");
}

/// `GET /vms` is the authoritative swarm inventory: union of live VMs (from the
/// Vmm backend) and registered VMs (from the control registry, which includes
/// frozen VMs with no live process). Live state/guest_ip come from the
/// backend; registry-only entries surface with `state:null` + their snapshot.
#[tokio::test]
async fn get_vms_merges_live_and_registry() {
    // Backend knows one live VM.
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 2))),
        known: ["vm-live".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-list-test")));
    let control = Arc::new(Control::new());
    // Registry has the live VM (metadata) + a frozen VM (no live proc).
    control.register("vm-live".to_string(), "acme".into(), "main".into(), 5432);
    // A frozen VM: registered (so it has metadata) + a snapshot, but not
    // in the backend's live set.
    control.register("vm-paused".to_string(), "acme".into(), "feat".into(), 5432);
    control.set_snapshot(
        &"vm-paused".to_string(),
        Snapshot {
            vm_id: "vm-paused".into(),
            state_path: "/tmp/snap.mem".into(),
            mem_path: "/tmp/snap.mem".into(),
            config: VmConfig {
                vm_id: "vm-paused".into(),
                kernel_path: "/nonexistent/vmlinux".into(),
                kernel_cmdline: "console=ttyS0".into(),
                rootfs_path: "/nonexistent/rootfs.ext4".into(),
                memory_mb: 128,
                vcpus: 1,
                drives: vec![],
                initrd_path: None,
            },
        },
    );

    let server = Arc::new(ApiServer::new(node, control));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (status, body) = api_request(api_addr, "GET", "/vms", None).await;
    assert_eq!(status, 200, "{body}");

    // Live VM: state + guest_ip from the backend, registry metadata attached.
    assert!(body.contains(r#""vm_id":"vm-live""#), "{body}");
    assert!(body.contains(r#""state":"running""#), "{body}");
    assert!(body.contains(r#""guest_ip":"172.16.0.2""#), "{body}");
    assert!(body.contains(r#""tenant_id":"acme""#), "{body}");

    // Frozen VM: registry-only — state/guest_ip null, snapshot present.
    assert!(body.contains(r#""vm_id":"vm-paused""#), "{body}");
    assert!(body.contains(r#""snapshot_id":"/tmp/snap.mem""#), "{body}");
    // Find the vm-paused object and confirm state is null.
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let paused = v["vms"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["vm_id"] == "vm-paused")
        .expect("vm-paused missing");
    assert!(paused["state"].is_null(), "frozen state should be null");
    assert!(
        paused["guest_ip"].is_null(),
        "frozen guest_ip should be null"
    );
}

/// `POST /vms/{id}/reports` stores agent-pushed metrics in the control registry.
/// `GET /vms` then surfaces `last_report_secs_ago` + the raw metrics.
#[tokio::test]
async fn post_reports_stores_metrics() {
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        known: ["vm-1".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-report-test"),
    ));
    let control = Arc::new(Control::new());
    control.register("vm-1".into(), "acme".into(), "main".into(), 5432);

    let server = Arc::new(ApiServer::new(node, control));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    // POST a metrics report for a registered VM → 200 (with pause_epoch).
    let body = serde_json::json!({
        "available": true,
        "connections": 3,
        "active_backends": 1,
        "long_running_tx": 0,
        "db_size_bytes": 1048576,
        "cache_hit_ratio": 0.99,
        "wal_lsn": "0/3000000"
    });
    let (status, resp) = api_request(
        api_addr,
        "POST",
        "/vms/vm-1/reports",
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(status, 200, "{resp}");
    assert!(resp.contains(r#""pause_epoch":0"#), "{resp}");

    // GET /vms shows the report info.
    let (status, list_body) = api_request(api_addr, "GET", "/vms", None).await;
    assert_eq!(status, 200, "{list_body}");
    assert!(list_body.contains(r#""connections":3"#), "{list_body}");
    assert!(
        list_body.contains(r#""wal_lsn":"0/3000000""#),
        "{list_body}"
    );
    assert!(
        list_body.contains(r#""last_report_secs_ago""#),
        "{list_body}"
    );

    // POST to an unregistered VM → 404.
    let (status, _) = api_request(
        api_addr,
        "POST",
        "/vms/vm-ghost/reports",
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(status, 404);

    // POST with invalid JSON → 400.
    let (status, _) = api_request(api_addr, "POST", "/vms/vm-1/reports", Some("not json")).await;
    assert_eq!(status, 400);
}

/// `POST /vms/{id}/pause-request` acks 202 and validates input.
/// The warm-pause is spawned async — we verify the ack behavior, not the
/// full scale (that requires a real VMM backend). Idempotency is tested in
/// the control module's unit tests (the async task clears the guard too fast
/// to test it here with a mock VMM).
#[tokio::test]
async fn post_pause_request_acks_202() {
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        known: ["vm-1".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-snap-test")));
    let control = Arc::new(Control::new());
    control.register("vm-1".into(), "acme".into(), "main".into(), 5432);

    let server = Arc::new(ApiServer::new(node, control));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let body = serde_json::json!({"reason": "idle", "metrics": {"connections": 0}});

    // Request → 202 (accepted).
    let (status, resp) = api_request(
        api_addr,
        "POST",
        "/vms/vm-1/pause-request",
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(status, 202, "{resp}");
    assert!(resp.contains("accepted"), "{resp}");

    // Unregistered VM → 404.
    let (status, _) = api_request(
        api_addr,
        "POST",
        "/vms/vm-ghost/pause-request",
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(status, 404);

    // Invalid JSON → 400.
    let (status, _) = api_request(
        api_addr,
        "POST",
        "/vms/vm-1/pause-request",
        Some("not json"),
    )
    .await;
    assert_eq!(status, 400);
}

// ── Stateful mock: tracks created VMs so create_db can resolve guest IPs ────

/// A stateful `Vmm` mock: VMs added via `create_vm` become resolvable for
/// `vm_guest_ip` / `vm_state` / `list_vms`. Used by the `POST /dbs` test,
/// which provisions a brand-new VM and then talks to its guest agent.
struct StatefulMockVmm {
    guest_ip: IpAddr,
    known: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

#[async_trait]
impl Vmm for StatefulMockVmm {
    async fn create_vm(&self, config: VmConfig) -> Result<VmId, VmmError> {
        self.known.lock().unwrap().insert(config.vm_id.clone());
        Ok(config.vm_id)
    }
    async fn start_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn pause_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn resume_vm(&self, _: &VmId) -> Result<(), VmmError> {
        Ok(())
    }
    async fn snapshot_vm(&self, _: &VmId) -> Result<Snapshot, VmmError> {
        Err(VmmError::Backend("not implemented in mock".into()))
    }
    async fn restore_vm(&self, snap: &Snapshot) -> Result<VmId, VmmError> {
        Ok(snap.vm_id.clone())
    }
    async fn destroy_vm(&self, id: &VmId) -> Result<(), VmmError> {
        self.known.lock().unwrap().remove(id);
        Ok(())
    }
    async fn vm_state(&self, id: &VmId) -> Result<VmState, VmmError> {
        if self.known.lock().unwrap().contains(id) {
            Ok(VmState::Running)
        } else {
            Err(VmmError::VmNotFound(id.clone()))
        }
    }
    async fn vm_guest_ip(&self, id: &VmId) -> Result<Option<IpAddr>, VmmError> {
        if !self.known.lock().unwrap().contains(id) {
            return Err(VmmError::VmNotFound(id.clone()));
        }
        Ok(Some(self.guest_ip))
    }
    async fn list_vms(&self) -> Result<Vec<VmInfo>, VmmError> {
        let known = self.known.lock().unwrap().clone();
        Ok(known
            .iter()
            .map(|vm_id| VmInfo {
                vm_id: vm_id.clone(),
                state: VmState::Running,
                guest_ip: Some(self.guest_ip),
            })
            .collect())
    }
}

/// `POST /dbs` provisions a VM, restores the org bootstrap pack, and starts
/// Postgres. With an empty swarm, the auto-generated id is `vm-0` (db_id 0).
#[tokio::test]
async fn post_dbs_creates_and_starts_db() {
    let agent = FakeAgent::start().await;
    let guest_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let known = Arc::new(std::sync::Mutex::new(
        std::collections::HashSet::<String>::new(),
    ));
    let vmm: Arc<dyn Vmm> = Arc::new(StatefulMockVmm { guest_ip, known });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-create-db-test"),
    ));
    let control = Arc::new(Control::new());
    let server = Arc::new(
        ApiServer::new(node, control)
            .with_assets_dir(std::env::temp_dir().join("tikod-create-db-assets"))
            .with_agent_port(agent.listen.port()),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    // No body → auto vm_id + db_id, project_id defaults to db_id.
    let (status, body) = api_request(api_addr, "POST", "/dbs", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""vm_id":"vm-0""#), "{body}");
    assert!(body.contains(r#""db_id":0"#), "{body}");

    // The agent received /branch/restore with db_id (and project_id defaulting
    // to it). parent_db_id / pack were left absent so the agent applies its
    // defaults (parent 0, org bootstrap pack).
    let restore_body = agent
        .last_restore_body()
        .expect("agent never received /branch/restore");
    assert!(restore_body.contains(r#""db_id":0"#), "{restore_body}");
    assert!(restore_body.contains(r#""project_id":0"#), "{restore_body}");
    assert!(
        !restore_body.contains("parent_db_id"),
        "parent_db_id should be absent so the agent defaults it: {restore_body}"
    );
    assert!(
        !restore_body.contains("pack"),
        "pack should be absent so the agent defaults it: {restore_body}"
    );

    // A second call auto-increments to vm-1 / db_id 1.
    let (status, body) = api_request(api_addr, "POST", "/dbs", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""vm_id":"vm-1""#), "{body}");
    assert!(body.contains(r#""db_id":1"#), "{body}");

    // An explicit vm_id pins the id (and db_id/project_id follow it).
    let (status, body) = api_request(api_addr, "POST", "/dbs", Some(r#"{"vm_id":"vm-77"}"#)).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""vm_id":"vm-77""#), "{body}");
    assert!(body.contains(r#""db_id":77"#), "{body}");
    let restore_body = agent
        .last_restore_body()
        .expect("agent never received /branch/restore");
    assert!(restore_body.contains(r#""db_id":77"#), "{restore_body}");
    assert!(
        restore_body.contains(r#""project_id":77"#),
        "{restore_body}"
    );

    // Invalid body → 400.
    let (status, body) = api_request(api_addr, "POST", "/dbs", Some("not json")).await;
    assert_eq!(status, 400, "{body}");
}

/// `POST /dbs` waits for the agent's `storage_ready` flag (not just liveness)
/// before restoring: a freshly-booted guest's `/mnt/s3files` network mount may
/// still be coming up when the agent is already serving. Here a custom agent
/// reports `storage_ready:false` for the first few `/health` probes, then
/// `true`; tikod must keep polling until the mount is "ready" and only then
/// call `/branch/restore`.
#[tokio::test]
async fn post_dbs_waits_for_storage_ready() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    // `/health` flips to storage_ready=true after `READY_AFTER` probes.
    const READY_AFTER: u32 = 3;
    let health_calls = Arc::new(AtomicU32::new(0));
    let restore_called = Arc::new(AtomicBool::new(false));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let agent_addr = listener.local_addr().unwrap();
    let hc = health_calls.clone();
    let rc = restore_called.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let hc = hc.clone();
            let rc = rc.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await;
                let text = String::from_utf8_lossy(&buf);
                let req_line = text.lines().next().unwrap_or("");
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("");
                let path = parts.next().unwrap_or("");

                let (status, body): (u16, String) = match (method, path) {
                    ("GET", "/health") => {
                        let n = hc.fetch_add(1, Ordering::Relaxed);
                        let ready = n >= READY_AFTER;
                        (
                            200,
                            format!(
                                r#"{{"status":"ok","storage_ready":{ready}}}"#,
                                ready = ready
                            ),
                        )
                    }
                    ("POST", "/branch/restore") => {
                        rc.store(true, Ordering::Relaxed);
                        (200, r#"{"status":"restored"}"#.to_string())
                    }
                    ("POST", "/pg/start") => (204, String::new()),
                    ("POST", "/services/postgrest/start") => (204, String::new()),
                    _ => (404, r#"{"error":{"kind":"not_found"}}"#.to_string()),
                };
                let resp = format!(
                    "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status_text(status),
                    body.len(),
                    body,
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let guest_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let known = Arc::new(std::sync::Mutex::new(
        std::collections::HashSet::<String>::new(),
    ));
    let vmm: Arc<dyn Vmm> = Arc::new(StatefulMockVmm { guest_ip, known });
    let node = Arc::new(Node::new(
        vmm,
        std::env::temp_dir().join("tikod-storage-ready-test"),
    ));
    let control = Arc::new(Control::new());
    let server = Arc::new(
        ApiServer::new(node, control)
            .with_assets_dir(std::env::temp_dir().join("tikod-storage-ready-assets"))
            .with_agent_port(agent_addr.port()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (status, body) = api_request(api_addr, "POST", "/dbs", None).await;
    assert_eq!(status, 200, "{body}");

    // tikod polled /health more than once (it kept polling while storage was
    // reported not-ready) and only proceeded once ready flipped true.
    let probes = health_calls.load(Ordering::Relaxed);
    assert!(
        probes >= READY_AFTER,
        "expected tikod to keep polling until storage_ready (>= {READY_AFTER} probes), got {probes}"
    );
    assert!(
        restore_called.load(Ordering::Relaxed),
        "restore was never called"
    );
}
