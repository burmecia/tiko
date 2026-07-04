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
use tikod::control::{Control, IdlePolicy};
use tikod::node::Node;
use tikod::vmm::{Snapshot, VmConfig, VmId, VmInfo, Vmm, VmmError, VmState};

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
}

impl FakeAgent {
    /// Start on an ephemeral port. Routes:
    /// - `GET /pg/status` → a running cluster
    /// - `GET /pg/config` → a fixed setting
    /// - `POST /pg/{start,stop,restart,reload}` / `PUT /pg/config` → 204
    /// - `GET /health` → ok
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap();
        let config_put = Arc::new(std::sync::Mutex::new(None));

        let config_put_task = config_put.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let cfg = config_put_task.clone();
                tokio::spawn(async move {
                    let _ = handle(&mut stream, cfg).await;
                });
            }
        });

        Self { listen, config_put }
    }

    /// The last `PUT /pg/config` body the agent received.
    fn last_config_put(&self) -> Option<BTreeMap<String, String>> {
        self.config_put.lock().unwrap().clone()
    }
}

async fn handle(stream: &mut TcpStream, config_put: Arc<std::sync::Mutex<Option<BTreeMap<String, String>>>>) -> std::io::Result<()> {
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let req_line = text.lines().next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    // Each route returns an explicit (status, body). Action endpoints are 204;
    // reads are 200 JSON; unknown is 404.
    let (status, body): (u16, &str) = match (method, path) {
        ("GET", "/health") => (200, r#"{"status":"ok","initialized":true,"running":true}"#),
        ("GET", "/pg/status") => (
            200,
            r#"{"initialized":true,"running":true,"ready":true,"pid":4242,"version":"17.0","data_dir":"/var/lib/postgresql/tt","config_file":"/var/lib/postgresql/tt/postgresql.tiko.conf"}"#,
        ),
        ("GET", "/pg/config") => (
            200,
            r#"{"settings":{"max_connections":"100","log_min_messages":"info"}}"#,
        ),
        ("POST", "/pg/start") | ("POST", "/pg/stop") | ("POST", "/pg/restart") | ("POST", "/pg/reload") => {
            (204, "")
        }
        ("POST", "/pg/init") => (204, ""),
        ("PUT", "/pg/config") => {
            // Capture the request body (after the blank line).
            if let Some(body_start) = text.find("\r\n\r\n") {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text[body_start + 4..])
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

async fn api_request(api_addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
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
    let text = tokio::time::timeout(Duration::from_secs(5), fut).await.unwrap();
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
    let control = Arc::new(Control::new(IdlePolicy::default()));
    // Point the agent port at the fake agent.
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
        let (status, body) = api_request(api, "POST", &format!("/vms/vm-1/db/{action}"), None).await;
        assert_eq!(status, 204, "action {action}: {body}");
    }
}

#[tokio::test]
async fn db_stop_invalid_mode_is_400() {
    let (api, _agent) = harness().await;
    let (status, body) =
        api_request(api, "POST", "/vms/vm-1/db/stop", Some(r#"{"mode":"explode"}"#)).await;
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
    let put = agent.last_config_put().expect("agent never received config PUT");
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
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-db-test-noip")));
    let control = Arc::new(Control::new(IdlePolicy::default()));
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
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-db-test-dead")));
    let control = Arc::new(Control::new(IdlePolicy::default()));
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
        ("PUT", "/vms/vm-1/db/config", Some(r#"{"settings":{"work_mem":"4MB"}}"#)),
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

/// `GET /vms` is the authoritative swarm inventory: union of live VMs (from the
/// Vmm backend) and registered VMs (from the control registry, which includes
/// scaled-to-zero VMs with no live process). Live state/guest_ip come from the
/// backend; registry-only entries surface with `state:null` + their snapshot.
#[tokio::test]
async fn get_vms_merges_live_and_registry() {
    // Backend knows one live VM.
    let vmm: Arc<dyn Vmm> = Arc::new(MockVmm {
        guest_ip: Some(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 2))),
        known: ["vm-live".to_string()].into_iter().collect(),
    });
    let node = Arc::new(Node::new(vmm, std::env::temp_dir().join("tikod-list-test")));
    let control = Arc::new(Control::new(IdlePolicy::default()));
    // Registry has the live VM (metadata) + a scaled-to-zero VM (no live proc).
    control.register("vm-live".to_string(), "acme".into(), "main".into(), 5432);
    // A scaled-to-zero VM: registered (so it has metadata) + a snapshot, but not
    // in the backend's live set.
    control.register("vm-paused".to_string(), "acme".into(), "feat".into(), 5432);
    control.set_snapshot(&"vm-paused".to_string(), "/tmp/snap.mem".into());

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

    // Scaled-to-zero VM: registry-only — state/guest_ip null, snapshot present.
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
    assert!(paused["state"].is_null(), "scaled-to-zero state should be null");
    assert!(paused["guest_ip"].is_null(), "scaled-to-zero guest_ip should be null");
}
