//! Firecracker microVM backend (Linux/KVM only).
//!
//! Manages Firecracker microVM processes via the REST API (HTTP over Unix
//! socket). Each VM runs as a separate Firecracker process.
//!
//! ```text
//! tikod ──→ FcApiClient ──HTTP/Unix──→ Firecracker process ──KVM──→ Guest VM
//!               │
//!               ├── PUT /boot-source    (kernel + cmdline)
//!               ├── PUT /machine-config (vCPUs + memory)
//!               ├── PUT /drives/rootfs  (block device)
//!               ├── PUT /network-interfaces/eth0 (TAP device)
//!               ├── PUT /serial         (console output file)
//!               ├── PUT /actions        (InstanceStart)
//!               ├── PATCH /vm           (Pause / Resume)
//!               └── PUT /snapshot/*     (create / load)
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{info, debug, warn};

use super::{Snapshot, VmConfig, VmId, VmState, Vmm, VmmError, VmmResult};

// ============================================================================
// FcApiClient — minimal HTTP/1.1 client over Unix socket
// ============================================================================

/// HTTP client for the Firecracker REST API.
///
/// Firecracker serves a simple JSON API over a Unix domain socket.
/// We use raw HTTP/1.1 — no external HTTP library needed.
struct FcApiClient {
    sock_path: PathBuf,
}

impl FcApiClient {
    fn new(sock_path: impl AsRef<Path>) -> Self {
        Self {
            sock_path: sock_path.as_ref().to_path_buf(),
        }
    }

    /// Send a PUT request with a JSON body. Returns error on non-2xx.
    async fn put(&self, path: &str, body: &serde_json::Value) -> VmmResult<()> {
        self.request("PUT", path, Some(body)).await
    }

    /// Send a PATCH request with a JSON body.
    async fn patch(&self, path: &str, body: &serde_json::Value) -> VmmResult<()> {
        self.request("PATCH", path, Some(body)).await
    }

    /// Send a GET request and return the parsed JSON response.
    async fn get(&self, path: &str) -> VmmResult<serde_json::Value> {
        let body_str = self.request_raw("GET", path, None).await?;
        serde_json::from_str(&body_str)
            .map_err(|e| VmmError::Backend(format!("JSON parse error: {e}")))
    }

    /// Core HTTP request method.
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<()> {
        let _ = self.request_raw(method, path, body).await?;
        Ok(())
    }

    /// Core HTTP request returning the response body as a string.
    async fn request_raw(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<String> {
        let body_str = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {len}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            len = body_str.len(),
            body = body_str,
        );

        // Connect to the Unix socket.
        let mut stream = UnixStream::connect(&self.sock_path)
            .await
            .map_err(|e| VmmError::Backend(format!("connect to FC socket: {e}")))?;

        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| VmmError::Backend(format!("write to FC socket: {e}")))?;

        // Read response headers first (up to \r\n\r\n).
        let mut header_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = stream.read(&mut byte).await
                .map_err(|e| VmmError::Backend(format!("read header: {e}")))?;
            if n == 0 {
                break; // EOF
            }
            header_buf.push(byte[0]);
            // Check for end of headers.
            if header_buf.ends_with(b"\r\n\r\n") {
                break;
            }
            if header_buf.len() > 8192 {
                return Err(VmmError::Backend("response headers too large".into()));
            }
        }

        let header_str = String::from_utf8_lossy(&header_buf);
        let status_line = header_str.lines().next().unwrap_or("");
        let status_code = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);

        // Parse Content-Length for the body.
        let content_length: usize = header_str
            .lines()
            .find_map(|line| {
                let line = line.to_lowercase();
                if let Some(rest) = line.strip_prefix("content-length:") {
                    rest.trim().parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        // Read the body (if any).
        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 {
            stream.read_exact(&mut body_buf).await
                .map_err(|e| VmmError::Backend(format!("read body: {e}")))?;
        }

        let body_str = String::from_utf8_lossy(&body_buf).to_string();

        if status_code >= 200 && status_code < 300 {
            Ok(body_str)
        } else {
            let msg = serde_json::from_str::<serde_json::Value>(&body_str)
                .ok()
                .and_then(|v| {
                    v.get("fault_message")
                        .and_then(|f| f.as_str())
                        .map(String::from)
                })
                .unwrap_or_else(|| format!("HTTP {status_code}: {body_str}"));
            Err(VmmError::Backend(format!("FC API {method} {path}: {msg}")))
        }
    }
}

// ============================================================================
// Networking helpers
// ============================================================================

/// IP address counter for allocating per-VM subnets.
static IP_COUNTER: AtomicU64 = AtomicU64::new(2);

/// Allocate the next guest IP (10.0.N.2) and host gateway (10.0.N.1).
fn alloc_ip() -> (IpAddr, IpAddr, String) {
    let n = IP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let guest = format!("10.0.{n}.2");
    let gateway = format!("10.0.{n}.1");
    let subnet = format!("10.0.{n}.0/24");
    (
        guest.parse().expect("valid IP"),
        gateway.parse().expect("valid IP"),
        subnet,
    )
}

/// Create a TAP device and configure NAT.
fn create_tap(tap_name: &str, gateway_ip: &str, subnet: &str) -> VmmResult<()> {
    run_cmd("ip", &["tuntap", "add", tap_name, "mode", "tap"])?;
    run_cmd(
        "ip",
        &["addr", "add", &format!("{gateway_ip}/24"), "dev", tap_name],
    )?;
    run_cmd("ip", &["link", "set", tap_name, "up"])?;
    // NAT for outgoing traffic.
    run_cmd(
        "iptables",
        &["-t", "nat", "-A", "POSTROUTING", "-s", subnet, "-j", "MASQUERADE"],
    )?;
    Ok(())
}

/// Destroy a TAP device and remove NAT rules.
fn destroy_tap(tap_name: &str, subnet: &str) {
    let _ = run_cmd(
        "iptables",
        &["-t", "nat", "-D", "POSTROUTING", "-s", subnet, "-j", "MASQUERADE"],
    );
    let _ = run_cmd("ip", &["link", "set", tap_name, "down"]);
    let _ = run_cmd("ip", &["tuntap", "del", tap_name, "mode", "tap"]);
}

/// Run a system command via sudo, returning an error on non-zero exit.
fn run_cmd(program: &str, args: &[&str]) -> VmmResult<()> {
    let output = std::process::Command::new("sudo")
        .arg("-n")
        .arg(program)
        .args(args)
        .output()
        .map_err(|e| VmmError::Backend(format!("spawn {program}: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VmmError::Backend(format!(
            "{program} {} failed: {stderr}",
            args.join(" ")
        )));
    }
    Ok(())
}

// ============================================================================
// Per-VM state
// ============================================================================

/// Internal state for a Firecracker-managed VM.
struct FcVmEntry {
    /// Firecracker child process.
    child: Option<tokio::process::Child>,
    /// Path to the API Unix socket.
    api_sock: PathBuf,
    /// TAP device name.
    tap_name: String,
    /// NAT subnet (e.g., "10.0.2.0/24").
    subnet: String,
    /// Serial console log file path.
    serial_log: PathBuf,
    /// VM configuration.
    config: VmConfig,
    /// Guest IP address.
    guest_ip: IpAddr,
    /// Current lifecycle state.
    state: VmState,
}

impl Drop for FcVmEntry {
    fn drop(&mut self) {
        // Best-effort cleanup.
        destroy_tap(&self.tap_name, &self.subnet);
        let _ = std::fs::remove_file(&self.api_sock);
    }
}

// ============================================================================
// FirecrackerVmm
// ============================================================================

/// Firecracker VMM backend.
///
/// Manages Firecracker processes via the REST API (Unix socket).
/// Each VM runs as a separate Firecracker process.
pub struct FirecrackerVmm {
    snapshot_dir: PathBuf,
    runtime_dir: PathBuf,
    firecracker_bin: PathBuf,
    vms: StdMutex<HashMap<VmId, FcVmEntry>>,
    vm_counter: AtomicU64,
}

impl FirecrackerVmm {
    pub fn new(snapshot_dir: PathBuf) -> Self {
        let runtime_dir = snapshot_dir.join("runtime");
        std::fs::create_dir_all(&runtime_dir).ok();
        std::fs::create_dir_all(&snapshot_dir).ok();

        let fc_bin = std::env::var("FIRECRACKER_BIN").unwrap_or_else(|_| {
            "/home/ubuntu/tiko/firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug/firecracker"
                .into()
        });

        Self {
            snapshot_dir,
            runtime_dir,
            firecracker_bin: PathBuf::from(fc_bin),
            vms: StdMutex::new(HashMap::new()),
            vm_counter: AtomicU64::new(0),
        }
    }

    pub fn with_firecracker_bin(mut self, path: PathBuf) -> Self {
        self.firecracker_bin = path;
        self
    }

    /// Spawn a Firecracker process and wait for its API socket to be ready.
    fn spawn_firecracker(&self, vm_id: &str) -> VmmResult<(tokio::process::Child, PathBuf)> {
        let sock_path = self.runtime_dir.join(format!("{vm_id}.sock"));
        let _ = std::fs::remove_file(&sock_path); // clean stale socket

        info!(vm_id = %vm_id, sock = %sock_path.display(), "spawning Firecracker");

        let stderr_path = self.runtime_dir.join(format!("{vm_id}.stderr.log"));
        let stderr_file = std::fs::File::create(&stderr_path)
            .map_err(|e| VmmError::Backend(format!("create stderr file: {e}")))?;

        let child = tokio::process::Command::new(&self.firecracker_bin)
            .arg("--api-sock")
            .arg(&sock_path)
            .arg("--no-seccomp")
            .arg("--id")
            .arg(vm_id)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::from(stderr_file))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| VmmError::Backend(format!("spawn firecracker: {e}")))?;

        // Wait for the API socket to appear (up to 5s).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if sock_path.exists() {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(VmmError::Backend(
                    "Firecracker API socket did not appear within 5s".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        Ok((child, sock_path))
    }

    /// Configure a newly spawned Firecracker VM via REST API.
    async fn configure_vm(
        client: &FcApiClient,
        config: &VmConfig,
        tap_name: &str,
        serial_log: &Path,
    ) -> VmmResult<()> {
        // Boot source.
        let mut boot_args = config.kernel_cmdline.clone();
        // Add static IP config if not already in cmdline.
        if !boot_args.contains("10.0.") {
            boot_args = format!(
                "{boot_args} ip=10.0.{}.2::10.0.{}.1:255.255.255.0:tiko:eth0:off",
                ip_third_octet(config),
                ip_third_octet(config),
            );
        }

        client
            .put(
                "/boot-source",
                &json!({
                    "kernel_image_path": config.kernel_path.to_string_lossy(),
                    "boot_args": boot_args,
                }),
            )
            .await?;

        // Machine configuration.
        client
            .put(
                "/machine-config",
                &json!({
                    "vcpu_count": config.vcpus,
                    "mem_size_mib": config.memory_mb,
                }),
            )
            .await?;

        // Root filesystem drive.
        client
            .put(
                "/drives/rootfs",
                &json!({
                    "drive_id": "rootfs",
                    "path_on_host": config.rootfs_path.to_string_lossy(),
                    "is_root_device": true,
                    "is_read_only": false,
                }),
            )
            .await?;

        // Extra drives.
        for drive in &config.drives {
            client
                .put(
                    &format!("/drives/{}", drive.drive_id),
                    &json!({
                        "drive_id": drive.drive_id,
                        "path_on_host": drive.path.to_string_lossy(),
                        "is_root_device": false,
                        "is_read_only": drive.read_only,
                    }),
                )
                .await?;
        }

        // Network interface (TAP device).
        client
            .put(
                "/network-interfaces/eth0",
                &json!({
                    "iface_id": "eth0",
                    "host_dev_name": tap_name,
                }),
            )
            .await?;

        // Serial console output.
        client
            .put(
                "/serial",
                &json!({
                    "serial_out_path": serial_log.to_string_lossy(),
                }),
            )
            .await?;

        Ok(())
    }

    /// Snapshot paths for a given VM.
    fn snapshot_paths(&self, vm_id: &str) -> (PathBuf, PathBuf) {
        let base = self.snapshot_dir.join(format!("{vm_id}.snapshot"));
        let mem = self.snapshot_dir.join(format!("{vm_id}.mem"));
        (base, mem)
    }
}

/// Extract the third octet from the guest IP (for boot args).
fn ip_third_octet(_config: &VmConfig) -> u64 {
    // Default; actual value comes from alloc_ip and is set on the entry.
    IP_COUNTER.load(Ordering::SeqCst) - 1
}

#[async_trait]
impl Vmm for FirecrackerVmm {
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId> {
        let vm_id = config.vm_id.clone();

        // Validate.
        if !config.kernel_path.exists() {
            return Err(VmmError::InvalidConfig(format!(
                "kernel not found: {}",
                config.kernel_path.display()
            )));
        }
        if !config.rootfs_path.exists() {
            return Err(VmmError::InvalidConfig(format!(
                "rootfs not found: {}",
                config.rootfs_path.display()
            )));
        }

        // Allocate networking.
        let (guest_ip, gateway_ip, subnet) = alloc_ip();
        let vm_num = self.vm_counter.fetch_add(1, Ordering::SeqCst);
        let tap_name = format!("tiko-tap-{vm_num}");
        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));

        info!(vm_id = %vm_id, tap = %tap_name, guest_ip = %guest_ip, "creating Firecracker VM");

        // Create TAP device + NAT.
        debug!(vm_id = %vm_id, tap = %tap_name, "creating TAP device");
        create_tap(&tap_name, &gateway_ip.to_string(), &subnet)?;

        // Spawn Firecracker process.
        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(result) => result,
            Err(e) => {
                destroy_tap(&tap_name, &subnet);
                return Err(e);
            }
        };

        // Configure via REST API.
        debug!(vm_id = %vm_id, "configuring via REST API");
        let client = FcApiClient::new(&api_sock);
        // Update the IP counter reference for the boot args.
        IP_COUNTER.store(vm_num + 2, Ordering::SeqCst);

        // Build boot args with the correct IP.
        let mut full_config = config.clone();
        let n = vm_num + 2;
        full_config.kernel_cmdline = format!(
            "{} ip=10.0.{n}.2::10.0.{n}.1:255.255.255.0:tiko:eth0:off",
            full_config.kernel_cmdline
        );

        if let Err(e) = Self::configure_vm(&client, &full_config, &tap_name, &serial_log).await {
            warn!(vm_id = %vm_id, error = %e, "configure_vm failed");
            destroy_tap(&tap_name, &subnet);
            let _ = std::fs::remove_file(&api_sock);
            return Err(e);
        }
        debug!(vm_id = %vm_id, "VM configured");

        let mut vms = self.vms.lock().unwrap();
        vms.insert(
            vm_id.clone(),
            FcVmEntry {
                child: Some(child),
                api_sock,
                tap_name,
                subnet,
                serial_log,
                config,
                guest_ip,
                state: VmState::Stopped,
            },
        );

        info!(vm_id = %vm_id, "Firecracker VM created and configured");
        Ok(vm_id)
    }

    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = {
            let vms = self.vms.lock().unwrap();
            let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            entry.api_sock.clone()
        };

        let client = FcApiClient::new(&sock_path);
        client
            .put("/actions", &json!({"action_type": "InstanceStart"}))
            .await?;

        let mut vms = self.vms.lock().unwrap();
        if let Some(entry) = vms.get_mut(vm_id) {
            entry.state = VmState::Running;
        }
        info!(vm_id = %vm_id, "Firecracker VM started");
        Ok(())
    }

    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = {
            let vms = self.vms.lock().unwrap();
            let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            entry.api_sock.clone()
        };

        let client = FcApiClient::new(&sock_path);
        client
            .patch("/vm", &json!({"state": "Paused"}))
            .await?;

        let mut vms = self.vms.lock().unwrap();
        if let Some(entry) = vms.get_mut(vm_id) {
            entry.state = VmState::Paused;
        }
        info!(vm_id = %vm_id, "Firecracker VM paused");
        Ok(())
    }

    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = {
            let vms = self.vms.lock().unwrap();
            let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            entry.api_sock.clone()
        };

        let client = FcApiClient::new(&sock_path);
        client
            .patch("/vm", &json!({"state": "Resumed"}))
            .await?;

        let mut vms = self.vms.lock().unwrap();
        if let Some(entry) = vms.get_mut(vm_id) {
            entry.state = VmState::Running;
        }
        info!(vm_id = %vm_id, "Firecracker VM resumed");
        Ok(())
    }

    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        let (sock_path, config, snap_paths) = {
            let vms = self.vms.lock().unwrap();
            let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            if entry.state != VmState::Paused {
                return Err(VmmError::InvalidState {
                    vm_id: vm_id.clone(),
                    current: entry.state,
                    expected: VmState::Paused,
                });
            }
            (entry.api_sock.clone(), entry.config.clone(), self.snapshot_paths(vm_id))
        };

        let (snap_path, mem_path) = snap_paths;
        let client = FcApiClient::new(&sock_path);
        client
            .put(
                "/snapshot/create",
                &json!({
                    "snapshot_path": snap_path.to_string_lossy(),
                    "mem_file_path": mem_path.to_string_lossy(),
                    "snapshot_type": "Full",
                }),
            )
            .await?;

        info!(vm_id = %vm_id, snap = %snap_path.display(), "Firecracker snapshot created");

        Ok(Snapshot {
            vm_id: vm_id.clone(),
            state_path: snap_path,
            config,
        })
    }

    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId> {
        let vm_id = snapshot.vm_id.clone();

        if !snapshot.state_path.exists() {
            return Err(VmmError::SnapshotNotFound(vm_id));
        }

        // For restore, we need the mem file path derived from the snapshot path.
        let mem_path = snapshot.state_path.with_extension("mem");

        // Allocate new networking with same name.
        let (guest_ip, gateway_ip, subnet) = alloc_ip();
        let tap_name = format!("tiko-tap-0");

        debug!(vm_id = %vm_id, tap = %tap_name, gateway = %gateway_ip, "creating TAP for restore");
        create_tap(&tap_name, &gateway_ip.to_string(), &subnet)?;

        // Spawn a fresh Firecracker process.
        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(result) => result,
            Err(e) => {
                destroy_tap(&tap_name, &subnet);
                return Err(e);
            }
        };

        debug!(vm_id = %vm_id, snapshot = %snapshot.state_path.display(), "loading snapshot");
        let client = FcApiClient::new(&api_sock);
        client
            .put(
                "/snapshot/load",
                &json!({
                    "snapshot_path": snapshot.state_path.to_string_lossy(),
                    "mem_file_path": mem_path.to_string_lossy(),
                    "resume_vm": false,
                }),
            )
            .await?;
        debug!(vm_id = %vm_id, "snapshot loaded");

        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));
        let mut vms = self.vms.lock().unwrap();
        vms.insert(
            vm_id.clone(),
            FcVmEntry {
                child: Some(child),
                api_sock,
                tap_name,
                subnet,
                serial_log,
                config: snapshot.config.clone(),
                guest_ip,
                state: VmState::Paused,
            },
        );

        info!(vm_id = %vm_id, "Firecracker VM restored from snapshot");
        Ok(vm_id)
    }

    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut entry = {
            let mut vms = self.vms.lock().unwrap();
            vms.remove(vm_id)
                .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?
        };

        info!(vm_id = %vm_id, "destroying Firecracker VM");

        // Try graceful shutdown via CtrlAltDel.
        let client = FcApiClient::new(&entry.api_sock);
        let _ = client
            .put("/actions", &json!({"action_type": "SendCtrlAltDel"}))
            .await;

        // Wait up to 3s for graceful exit.
        if let Some(child) = entry.child.as_mut() {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                child.wait(),
            )
            .await;

            // Force kill if still alive.
            let _ = child.start_kill();
            let _ = child.wait().await;
        }

        // FcVmEntry::drop handles TAP + socket cleanup.
        entry.state = VmState::Stopped;
        drop(entry);

        info!(vm_id = %vm_id, "Firecracker VM destroyed");
        Ok(())
    }

    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<VmState> {
        let (sock_path, cached_state) = {
            let vms = self.vms.lock().unwrap();
            let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            (entry.api_sock.clone(), entry.state)
        };

        // Try to query Firecracker for the actual state.
        let client = FcApiClient::new(&sock_path);
        match client.get("/").await {
            Ok(info) => {
                let state = info
                    .get("state")
                    .and_then(|s| s.as_str())
                    .unwrap_or("Not started");
                let mapped = match state {
                    "Running" => VmState::Running,
                    "Paused" => VmState::Paused,
                    _ => VmState::Stopped,
                };
                Ok(mapped)
            }
            Err(_) => Ok(cached_state),
        }
    }

    async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        let vms = self.vms.lock().unwrap();
        let entry = vms.get(vm_id).ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
        Ok(Some(entry.guest_ip))
    }
}
