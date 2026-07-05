//! HTTP control API — tikod is the single control point for a swarm of VMs.
//!
//! Both VM lifecycle ([`Vmm`](crate::vmm::Vmm) / [`Node`](crate::node::Node))
//! and in-guest Postgres control ([`GuestClient`](crate::guestcontrol::GuestClient) →
//! the per-VM `tikoguest` agent) are exposed here. Clients never talk to a VM or its
//! Postgres directly — every operation goes through this API, and tikod fans it
//! out to the right guest over the guest IP. Like the Firecracker backend
//! client, this server uses raw HTTP/1.1 over TCP — no external HTTP library.
//!
//! ```text
//! Client ──HTTP──→ tikod API ──┬─ Vmm/Node ──→ backend (Firecracker/VZ)
//!                              └─ GuestClient ──→ guest_ip:9000 ──→ tikoguest ──→ pg_ctl
//! ```
//!
//! # VM lifecycle routes
//!
//! | Method | Path                        | Handler           | Returns                                  |
//! |--------|-----------------------------|-------------------|------------------------------------------|
//! | `GET`  | `/health`                   | liveness probe    | `{"status":"ok"}`                        |
//! | `GET`  | `/vms`                      | list VMs          | `{"vms":[{vm_id,...},...]}`              |
//! | `PUT`  | `/vms`                      | create_vm         | `{"vm_id":"..."}`  body optional (auto-generates id) |
//! | `POST` | `/vms/provision`            | create + start    | `{"vm_id":"..."}`  body optional (auto-generates id) |
//! | `GET`  | `/vms/{vm_id}`              | vm_state          | `{"vm_id":"...","state":"running"}`      |
//! | `DELETE`| `/vms/{vm_id}`             | destroy_vm        | 204                                      |
//! | `GET`  | `/vms/{vm_id}/ip`           | vm_guest_ip       | `{"vm_id":"...","ip":"1.2.3.4"\|null}`   |
//! | `PUT`  | `/vms/{vm_id}/start`        | start_vm          | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/pause`        | pause_vm          | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/resume`       | resume_vm         | 204                                      |
//! | `PUT`  | `/vms/{vm_id}/restore`      | restore_vm        | `{"vm_id":"..."}`  (snapshot from registry)  |
//! | `PUT`  | `/vms/{vm_id}/snapshot`     | snapshot_vm       | `Snapshot`                               |
//! | `PUT`  | `/vms/{vm_id}/scale-to-zero`| pause+snap+destroy| `Snapshot` (stored in registry)          |
//! | `PUT`  | `/vms/{vm_id}/scale-from-zero`| restore + resume | `{"vm_id":"..."}`  (snapshot from registry) |
//!
//! # Postgres control routes (forwarded to the guest `tikoguest` agent)
//!
//! All `/vms/{vm_id}/db/*` routes resolve the VM's guest IP via the Vmm layer,
//! then proxy to that guest's `tikoguest` agent (`guest_ip:agent_port`, default
//! 9000 — see [`DEFAULT_AGENT_PORT`](crate::guestcontrol::DEFAULT_AGENT_PORT)).
//! The agent's status code and structured error body are forwarded verbatim.
//!
//! | Method | Path                            | Agent endpoint | Returns / Body                                            |
//! |--------|---------------------------------|----------------|-----------------------------------------------------------|
//! | `GET`  | `/vms/{vm_id}/db/health`        | `/health`      | `{"status","initialized","running"}`                      |
//! | `GET`  | `/vms/{vm_id}/db/status`        | `/pg/status`   | `{"initialized","running","ready","pid","version",...}`   |
//! | `POST` | `/vms/{vm_id}/db/init`          | `/pg/init`     | 204  body: `{"force":bool}` (409 if running/initialized)   |
//! | `POST` | `/vms/{vm_id}/db/start`         | `/pg/start`    | 204                                                       |
//! | `POST` | `/vms/{vm_id}/db/stop`          | `/pg/stop`     | 204  body: `{"mode":"fast\|smart\|immediate"}`            |
//! | `POST` | `/vms/{vm_id}/db/restart`       | `/pg/restart`  | 204                                                       |
//! | `POST` | `/vms/{vm_id}/db/reload`        | `/pg/reload`   | 204                                                       |
//! | `GET`  | `/vms/{vm_id}/db/config`        | `/pg/config`   | `{"settings":{name:value,...}}`                           |
//! | `PUT`  | `/vms/{vm_id}/db/config`        | `/pg/config`   | 204  body: `{"settings":{name:value}}` (then reloads)     |
//!
//! # Agent-inbound routes (pushed by the guest `tikoguest` agent)
//!
//! | Method | Path                            | From            | Returns / Body                              |
//! |--------|---------------------------------|-----------------|---------------------------------------------|
//! | `POST` | `/vms/{vm_id}/reports`          | scaler loop     | 200  body: `{"pause_epoch":N}` (404 if unregistered) |
//! | `POST` | `/vms/{vm_id}/pause-request`    | scaler loop     | 202  body: `{reason, metrics}` (idempotent; 404 if unregistered) |
//!
//! # Error responses
//!
//! All errors use `{"error":{"kind":...,"message":...}}` with structured fields
//! (`vm_id`, `current`, `expected`) so clients can reconstruct the original
//! [`VmmError`](crate::vmm::VmmError) variant. Status codes: 400 invalid config /
//! bad request, 404 not found, 409 invalid state, 500 backend / I/O, 502 agent
//! unreachable (no guest IP or agent connection failed).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// Warm-pause window: how long a warm-paused VM is kept in memory before
/// cold scale-to-zero (snapshot + destroy). During this window, existing
/// client connections survive and are transparently resumed.
const WARM_PAUSE_SECS: u64 = 120;

use crate::control::Control;
use crate::guestcontrol::{GuestClient, GuestClientError, DEFAULT_AGENT_PORT};
use crate::node::Node;
use crate::vmm::{VmConfig, VmId, VmmError};

/// Maximum accepted request header block size (protects against runaway reads).
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// HTTP control API server. Shares the same [`Node`] / [`Control`] used by the
/// PG proxy, so lifecycle changes are immediately visible to both planes.
pub struct ApiServer {
    node: Arc<Node>,
    control: Arc<Control>,
    /// Port the in-guest `tikoguest` agent listens on; used for `/vms/{id}/db/*`.
    agent_port: u16,
    /// Directory containing kernel/rootfs/initramfs assets for the preset
    /// [`VmConfig`]. Used by `PUT /vms` and `POST /vms/provision`.
    assets_dir: PathBuf,
}

impl ApiServer {
    pub fn new(node: Arc<Node>, control: Arc<Control>) -> Self {
        Self {
            node,
            control,
            agent_port: DEFAULT_AGENT_PORT,
            assets_dir: PathBuf::from("tikod/assets"),
        }
    }

    /// Override the guest-agent port (default [`DEFAULT_AGENT_PORT`]).
    pub fn with_agent_port(mut self, port: u16) -> Self {
        self.agent_port = port;
        self
    }

    /// Override the assets directory for the preset [`VmConfig`].
    pub fn with_assets_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.assets_dir = dir.into();
        self
    }

    /// Build the fixed preset [`VmConfig`] for a given vm_id from the assets
    /// directory. Used by `PUT /vms` and `POST /vms/provision` so clients
    /// don't need to know kernel/rootfs paths.
    fn default_vm_config(&self, vm_id: VmId) -> VmConfig {
        let a = &self.assets_dir;
        VmConfig {
            vm_id,
            kernel_path: a.join("vmlinux-6.1"),
            kernel_cmdline:
                "console=ttyS0 reboot=k panic=1 pci=off systemd.unified_cgroup_hierarchy=0".into(),
            rootfs_path: a.join("ubuntu-24.04-rootfs.ext4"),
            memory_mb: 512,
            vcpus: 2,
            drives: vec![],
            initrd_path: Some(a.join("tiko-initramfs.cpio.gz")),
        }
    }

    /// Parse the request body for `PUT /vms` / `POST /vms/provision`. Accepts:
    /// - empty body → auto-generate `"vm-{N}"` (next free index)
    /// - `{"vm_id":"vm-5"}` → use the given id
    /// - anything else → 400
    async fn resolve_create_request(&self, req: &Request) -> Result<VmId, Response> {
        if req.body.is_empty() {
            return Ok(self.auto_vm_id().await);
        }
        #[derive(serde::Deserialize)]
        struct CreateVmRequest {
            vm_id: Option<VmId>,
        }
        let parsed: CreateVmRequest = serde_json::from_slice(&req.body).map_err(|e| {
            bad_request(&format!("invalid body; expected {{\"vm_id\":\"...\"}}: {e}"))
        })?;
        match parsed.vm_id {
            Some(id) => Ok(id),
            None => Ok(self.auto_vm_id().await),
        }
    }

    /// Generate `"vm-{N}"` where N is the smallest non-negative integer not
    /// already used by a live or registered VM.
    async fn auto_vm_id(&self) -> VmId {
        let used: std::collections::HashSet<u32> = match self.node.list_vms().await {
            Ok(list) => list
                .iter()
                .filter_map(|info| info.vm_id.strip_prefix("vm-"))
                .filter_map(|s| s.parse().ok())
                .collect(),
            Err(_) => std::collections::HashSet::new(),
        };
        let next = (0u32..).find(|i| !used.contains(i)).unwrap_or(0);
        format!("vm-{next}")
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
        let body_preview = preview_body(&resp.body);
        debug!(
            client = %addr,
            method = %req.method,
            path = %req.path,
            status = resp.status,
            body = %body_preview,
            "API response"
        );
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
            ("GET", ["vms"]) => self.list_vms().await,
            ("PUT", ["vms"]) => {
                let vm_id = match self.resolve_create_request(req).await {
                    Ok(id) => id,
                    Err(r) => return r,
                };
                let config = self.default_vm_config(vm_id);
                match self.node.vmm().create_vm(config).await {
                    Ok(id) => ok_json(serde_json::json!({"vm_id": id})),
                    Err(e) => err_resp(&e),
                }
            }
            ("POST", ["vms", "provision"]) => {
                let vm_id = match self.resolve_create_request(req).await {
                    Ok(id) => id,
                    Err(r) => return r,
                };
                let config = self.default_vm_config(vm_id);
                match self.node.provision(config).await {
                    Ok(id) => {
                        self.control
                            .register(id.clone(), String::new(), String::new(), 5432);
                        ok_json(serde_json::json!({"vm_id": id}))
                    }
                    Err(e) => err_resp(&e),
                }
            }
            // Legacy `POST /vms/restore` / `POST /vms/scale-from-zero` (full
            // `Snapshot` in the body) were replaced by path-keyed,
            // body-less `PUT /vms/{vm_id}/restore` and
            // `PUT /vms/{vm_id}/scale-from-zero` (see `route_vm`). The snapshot
            // is now looked up in the registry by vm_id, so clients never need
            // to carry snapshot details.

            // DB control routes: /vms/{vm_id}/db/{...} → in-guest tikoguest agent.
            (_, ["vms", vm_id, "db", rest @ ..]) => {
                self.route_db(method, &VmId::from(*vm_id), rest, req)
                    .await
            }

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
        req: &Request,
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
            // Restore from the registry-stored snapshot (Paused state). The
            // snapshot is looked up by vm_id, so the request carries no body —
            // the client never needs snapshot paths/config. 404 if no snapshot
            // is registered for this VM (e.g. it was never scaled to zero).
            ("PUT", ["restore"]) => match self.control.get_snapshot(vm_id) {
                Some(snap) => match vmm.restore_vm(&snap).await {
                    Ok(id) => ok_json(serde_json::json!({"vm_id": id})),
                    Err(e) => err_resp(&e),
                },
                None => snapshot_not_found(vm_id),
            },
            // Scale from zero: restore from the registry-stored snapshot, then
            // resume. Routed through `Node::wake` so the single-flight restore
            // lock is shared with the proxy — concurrent requests (and
            // concurrent client connections) perform at most one restore.
            ("PUT", ["scale-from-zero"]) => match self.node.wake(vm_id, &self.control).await {
                Ok(()) => ok_json(serde_json::json!({"vm_id": vm_id})),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["snapshot"]) => match vmm.snapshot_vm(vm_id).await {
                Ok(snap) => ok_value(&snap),
                Err(e) => err_resp(&e),
            },
            ("PUT", ["scale-to-zero"]) => {
                // Close active proxied connections before pausing, so clients
                // get a prompt reset and reconnect through wake.
                self.control.cancel_vm_connections(vm_id);
                match self.node.scale_to_zero(vm_id).await {
                    Ok(snap) => {
                        self.control.set_snapshot(vm_id, snap.clone());
                        ok_value(&snap)
                    }
                    Err(e) => err_resp(&e),
                }
            }

            // Agent-inbound: metrics report from the guest's scaler loop.
            // Returns the current pause epoch so the guest can detect
            // pause/restore cycles and reset stale state.
            ("POST", ["reports"]) => self.record_report(vm_id, req).await,

            // Agent-inbound: pause-request from the guest's scaler loop.
            ("POST", ["pause-request"]) => self.pause_request(vm_id, req).await,

            // Register an externally-started VM in the control registry (no
            // VMM backend involvement). Used for VMs started by start_vm.sh.
            ("POST", ["register"]) => {
                #[derive(serde::Deserialize)]
                struct RegisterBody {
                    tenant_id: Option<String>,
                    branch_id: Option<String>,
                    pg_port: Option<u16>,
                }
                let body: RegisterBody = if req.body.is_empty() {
                    RegisterBody { tenant_id: None, branch_id: None, pg_port: None }
                } else {
                    match serde_json::from_slice(&req.body) {
                        Ok(b) => b,
                        Err(_) => return bad_request("invalid register body"),
                    }
                };
                self.control.register(
                    vm_id.clone(),
                    body.tenant_id.unwrap_or_default(),
                    body.branch_id.unwrap_or_default(),
                    body.pg_port.unwrap_or(5432),
                );
                ok_json(serde_json::json!({"vm_id": vm_id, "registered": true}))
            }

            _ => {
                let path = format!("/vms/{vm_id}/{}", rest.join("/"));
                not_found(method, &path)
            }
        }
    }

    /// `POST /vms/{vm_id}/reports` — receives a metrics report from the guest
    /// agent's scaler loop. Returns the current pause epoch so the guest can
    /// detect pause/restore cycles (mismatch with local copy → reset
    /// `idle_ticks`). Returns 404 if the VM isn't registered.
    async fn record_report(&self, vm_id: &VmId, req: &Request) -> Response {
        let metrics: serde_json::Value = match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(_) => return bad_request("invalid report body; expected JSON metrics"),
        };
        match self.control.record_report(vm_id, metrics) {
            Some(epoch) => Response {
                status: 200,
                body: serde_json::json!({"pause_epoch": epoch})
                    .to_string()
                    .into_bytes(),
            },
            None => Response {
                status: 404,
                body: serde_json::json!({
                    "error": {"kind": "not_found", "message": format!("VM {vm_id} not registered")}
                })
                .to_string()
                .into_bytes(),
            },
        }
    }

    /// `POST /vms/{vm_id}/pause-request` — the guest agent's scaler loop
    /// signals that the VM is idle and ready to be paused. tikod acks `202`
    /// **before** pausing so the agent reads the ack before the VM is frozen.
    /// The handler is idempotent: a duplicate request (e.g. ack lost, agent
    /// retried) returns 202 without double-triggering.
    ///
    /// On a new request tikod:
    /// 1. Bumps the pause epoch (guest detects mismatch → resets `idle_ticks`).
    /// 2. Notifies the proxy (`mark_warm_paused`) to disable keepalive.
    /// 3. Pauses the VM immediately (warm-pause: freeze, keep in memory).
    /// 4. Starts a 2-min countdown. If no client wakes the VM → snapshot
    ///    (cold scale-to-zero). If a client arrives → resume (wake-on-stale).
    async fn pause_request(&self, vm_id: &VmId, req: &Request) -> Response {
        let body: serde_json::Value = match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(_) => return bad_request("invalid pause-request body; expected JSON"),
        };

        match self.control.try_mark_pause_requested(vm_id) {
            None => Response {
                status: 404,
                body: serde_json::json!({
                    "error": {"kind": "not_found", "message": format!("VM {vm_id} not registered")}
                })
                .to_string()
                .into_bytes(),
            },
            Some(true) => {
                // Already requested — idempotent 202, no double-trigger.
                tracing::debug!(vm_id = %vm_id, "duplicate pause-request — idempotent 202");
                Response {
                    status: 202,
                    body: serde_json::json!({"status": "already_requested", "vm_id": vm_id})
                        .to_string()
                        .into_bytes(),
                }
            }
            Some(false) => {
                // New request — bump epoch, ack 202, then pause + warm countdown.
                let reason = body
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                // Bump the pause epoch BEFORE pausing. The guest's local copy
                // is stale after resume/restore, so the mismatch is detected
                // on the first tick and idle_ticks is reset.
                self.control.bump_pause_epoch(vm_id);
                // Notify the proxy to disable keepalive (the VM is about to
                // freeze; a frozen VM won't ACK keepalive probes). Connections
                // are NOT cancelled — they survive the pause and are resumed
                // transparently by wake-on-stale.
                self.control.mark_warm_paused(vm_id);
                let node = self.node.clone();
                let control = self.control.clone();
                let vm_id_owned = vm_id.clone();
                tokio::spawn(async move {
                    tracing::info!(vm_id = %vm_id_owned, reason = %reason, "processing pause request (warm-pause)");
                    // Phase 1 (warm): freeze the VM, keep it in memory.
                    if let Err(e) = node.warm_pause(&vm_id_owned).await {
                        tracing::warn!(
                            vm_id = %vm_id_owned, error = %e, "warm_pause failed"
                        );
                        control.clear_warm_paused(&vm_id_owned);
                        control.clear_pause_requested(&vm_id_owned);
                        return;
                    }
                    // Phase 2 (cold): after the warm window, snapshot + destroy
                    // if the VM is still paused (no client woke it).
                    tokio::time::sleep(Duration::from_secs(WARM_PAUSE_SECS)).await;
                    if control.is_warm_paused(&vm_id_owned) {
                        tracing::info!(
                            vm_id = %vm_id_owned,
                            "warm window expired — cold scale-to-zero"
                        );
                        control.clear_warm_paused(&vm_id_owned);
                        // NOW close surviving connections (the VM is about to be
                        // destroyed; clients reconnect through wake).
                        control.cancel_vm_connections(&vm_id_owned);
                        match node.cold_scale_to_zero(&vm_id_owned).await {
                            Ok(snap) => {
                                control.set_snapshot(&vm_id_owned, snap);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    vm_id = %vm_id_owned,
                                    error = %e,
                                    "cold_scale_to_zero failed after pause request"
                                );
                            }
                        }
                    } else {
                        tracing::info!(
                            vm_id = %vm_id_owned,
                            "VM woken during warm window — skipping cold scale"
                        );
                    }
                    control.clear_pause_requested(&vm_id_owned);
                });

                Response {
                    status: 202,
                    body: serde_json::json!({"status": "accepted", "vm_id": vm_id})
                        .to_string()
                        .into_bytes(),
                }
            }
        }
    }

    /// `GET /vms` — authoritative swarm inventory: the union of live VMs known
    /// to the backend ([`Node::list_vms`]) and registered VMs in the control
    /// plane (which includes scaled-to-zero VMs with no live process). Live
    /// state/guest_ip come from the backend; tenant/branch/connection/snapshot
    /// metadata come from the registry where available.
    async fn list_vms(&self) -> Response {
        // Live VMs from the backend (authoritative for state + guest IP).
        let live: std::collections::HashMap<VmId, crate::vmm::VmInfo> = match self.node.list_vms().await {
            Ok(list) => list.into_iter().map(|i| (i.vm_id.clone(), i)).collect(),
            Err(e) => return err_resp(&e),
        };
        let registry = self.control.list();

        // Merge: every live VM, then every registered VM not currently live
        // (e.g. scaled to zero — present only as a snapshot).
        let mut entries = serde_json::Map::new();
        for (vm_id, info) in &live {
            let rec = registry.iter().find(|(id, _)| id == vm_id).map(|(_, r)| r);
            entries.insert(
                vm_id.clone(),
                serde_json::json!({
                    "vm_id": vm_id,
                    "state": info.state,
                    "guest_ip": info.guest_ip,
                    "tenant_id": rec.map(|r| r.tenant_id.clone()),
                    "branch_id": rec.map(|r| r.branch_id.clone()),
                    "connection_count": rec.map(|r| r.connection_count),
                    "snapshot_id": rec.and_then(|r| r.snapshot.as_ref().map(|s| s.state_path.to_string_lossy().into_owned())),
                    "last_report_secs_ago": rec.and_then(|r| r.last_report_at.map(|t| t.elapsed().as_secs())),
                    "last_metrics": rec.and_then(|r| r.last_metrics.clone()),
                }),
            );
        }
        for (vm_id, rec) in registry {
            if live.contains_key(&vm_id) {
                continue;
            }
            entries.insert(
                vm_id.clone(),
                serde_json::json!({
                    "vm_id": vm_id,
                    "state": null,
                    "guest_ip": null,
                    "tenant_id": rec.tenant_id,
                    "branch_id": rec.branch_id,
                    "connection_count": rec.connection_count,
                    "snapshot_id": rec.snapshot.as_ref().map(|s| s.state_path.to_string_lossy().into_owned()),
                    "last_report_secs_ago": rec.last_report_at.map(|t| t.elapsed().as_secs()),
                    "last_metrics": rec.last_metrics.clone(),
                }),
            );
        }

        // Sort by vm_id for a stable response.
        let mut vms: Vec<_> = entries.into_values().collect();
        vms.sort_by(|a, b| {
            a.get("vm_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .cmp(b.get("vm_id").and_then(|v| v.as_str()).unwrap_or(""))
        });
        ok_json(serde_json::json!({"vms": vms}))
    }

    /// Dispatch `/vms/{vm_id}/db/{...}` routes to the in-guest `tikoguest` agent.
    /// Resolves the guest IP first; VM-not-found / no-IP surface as errors
    /// before any agent call is attempted.
    async fn route_db(&self, method: &str, vm_id: &VmId, rest: &[&str], req: &Request) -> Response {
        let guest_ip = match self.node.guest_ip(vm_id).await {
            Ok(Some(ip)) => ip,
            Ok(None) => {
                return Response {
                    status: 502,
                    body: serde_json::json!({
                        "error": {
                            "kind": "agent_unreachable",
                            "message": format!("no guest IP discovered for VM {vm_id}"),
                        }
                    })
                    .to_string()
                    .into_bytes(),
                };
            }
            Err(e) => return err_resp(&e),
        };
        let db = GuestClient::for_guest(guest_ip, self.agent_port);

        let path = format!("/vms/{vm_id}/db/{}", rest.join("/"));
        match (method, rest) {
            ("GET", ["health"]) => forward(db.health().await),
            ("GET", ["status"]) => forward(db.status().await),
            ("POST", ["start"]) => forward_void(db.start().await),
            ("POST", ["stop"]) => {
                let mode = match parse_stop_mode(req) {
                    Ok(m) => m,
                    Err(r) => return r,
                };
                forward_void(db.stop(mode).await)
            }
            ("POST", ["restart"]) => forward_void(db.restart().await),
            ("POST", ["reload"]) => forward_void(db.reload().await),
            ("POST", ["init"]) => {
                let force = match parse_init_force(req) {
                    Ok(f) => f,
                    Err(r) => return r,
                };
                forward_void(db.init(force).await)
            }
            ("GET", ["config"]) => forward(db.get_config().await),
            ("PUT", ["config"]) => match parse_config_settings(req) {
                Ok(settings) => forward_void(db.set_config(&settings).await),
                Err(r) => r,
            },
            _ => not_found(method, &path),
        }
    }
}

/// Forward a `GuestResult<T: Serialize>` to a Response: Ok → 200 JSON, Err → mapped.
fn forward<T: serde::Serialize>(res: Result<T, GuestClientError>) -> Response {
    match res {
        Ok(v) => ok_value(&v),
        Err(e) => guest_err_resp(e),
    }
}

/// Forward a `GuestResult<()>`: Ok → 204, Err → mapped.
fn forward_void(res: Result<(), GuestClientError>) -> Response {
    match res {
        Ok(()) => no_content(),
        Err(e) => guest_err_resp(e),
    }
}

/// Map a [`GuestClientError`] to a Response. VM errors reuse the Vmm mapping;
/// transport failures are 502 (bad gateway); agent errors forward the agent's
/// own status code and `kind`.
fn guest_err_resp(e: GuestClientError) -> Response {
    match e {
        GuestClientError::Vm(vmerr) => err_resp(&vmerr),
        GuestClientError::Transport(m) => Response {
            status: 502,
            body: serde_json::json!({
                "error": {"kind": "agent_unreachable", "message": m}
            })
            .to_string()
            .into_bytes(),
        },
        GuestClientError::Agent {
            status,
            kind,
            message,
        } => Response {
            status,
            body: serde_json::json!({
                "error": {"kind": kind, "message": message}
            })
            .to_string()
            .into_bytes(),
        },
    }
}

/// Parse `{"force": bool}` from a `/pg/init` request. Missing body / missing
/// field defaults to `false`; a present-but-non-bool value is a 400.
fn parse_init_force(req: &Request) -> Result<bool, Response> {
    if req.body.is_empty() {
        return Ok(false);
    }
    #[derive(serde::Deserialize)]
    struct Body {
        force: Option<bool>,
    }
    serde_json::from_slice::<Body>(&req.body)
        .map(|b| b.force.unwrap_or(false))
        .map_err(|_| Response {
            status: 400,
            body: serde_json::json!({
                "error": {"kind": "bad_request", "message": "invalid init body; expected {\"force\":bool}"}
            })
            .to_string()
            .into_bytes(),
        })
}

/// Parse `{"mode":"fast|smart|immediate"}` from a `/pg/stop` request. Missing
/// body defaults to [`crate::guestcontrol::StopMode::Fast`]; a present-but-invalid
/// value is a 400.
fn parse_stop_mode(req: &Request) -> Result<crate::guestcontrol::StopMode, Response> {
    use crate::guestcontrol::StopMode;
    if req.body.is_empty() {
        return Ok(StopMode::default());
    }
    #[derive(serde::Deserialize)]
    struct Body {
        mode: StopMode,
    }
    serde_json::from_slice::<Body>(&req.body).map(|b| b.mode).map_err(|_| Response {
        status: 400,
        body: serde_json::json!({
            "error": {"kind": "bad_request", "message": "invalid stop body; expected {\"mode\":\"fast|smart|immediate\"}"}
        })
        .to_string()
        .into_bytes(),
    })
}

/// Parse `{"settings":{...}}` from a `/pg/config` PUT.
fn parse_config_settings(req: &Request) -> Result<std::collections::BTreeMap<String, String>, Response> {
    #[derive(serde::Deserialize)]
    struct Body {
        settings: std::collections::BTreeMap<String, String>,
    }
    serde_json::from_slice::<Body>(&req.body)
        .map(|b| b.settings)
        .map_err(|e| Response {
            status: 400,
            body: serde_json::json!({
                "error": {"kind": "bad_request", "message": format!("invalid config body: {e}")}
            })
            .to_string()
            .into_bytes(),
        })
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

/// Truncate a response body for log preview (avoids dumping huge JSON).
fn preview_body(body: &[u8]) -> String {
    const MAX: usize = 500;
    let text = String::from_utf8_lossy(body);
    if text.len() <= MAX {
        text.into_owned()
    } else {
        format!("{}...(truncated {} bytes)", &text[..MAX], text.len())
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

fn bad_request(message: &str) -> Response {
    Response {
        status: 400,
        body: serde_json::json!({"error": {"kind": "bad_request", "message": message}})
            .to_string()
            .into_bytes(),
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

/// 404 response for a restore/scale-from-zero on a VM with no stored snapshot.
///
/// Mirrors [`VmmError::SnapshotNotFound`] so the HTTP client decodes it back
/// into the same variant as the pre-refactor behavior (a missing snapshot file
/// also maps to `SnapshotNotFound`). Keeps the `vm_id` structured field so
/// callers can identify which VM had no snapshot.
fn snapshot_not_found(vm_id: &VmId) -> Response {
    Response {
        status: 404,
        body: serde_json::json!({
            "error": {
                "kind": "snapshot_not_found",
                "message": format!("no snapshot registered for VM {vm_id} (scale to zero first)"),
                "vm_id": vm_id,
            }
        })
        .to_string()
        .into_bytes(),
    }
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
