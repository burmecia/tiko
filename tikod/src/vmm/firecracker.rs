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

/// Maximum number of concurrently distinct VM networks. The vm_index (derived
/// from the vm_id's trailing integer) doubles as the tap-name suffix and the
/// 3rd octet of the `172.16.{N}.0/24` subnet, so it must fit in a byte and
/// stay within the script-compatible range [0, 250].
const MAX_VM_INDEX: u32 = 250;

/// Derive a deterministic vm_index from the trailing decimal digits of `vm_id`.
///
/// This mirrors the scripts' `VM_ID` model: tap name, subnet, and guest MAC
/// are all keyed off a single small integer. Because the same vm_id always
/// maps to the same index, `create_vm` and `restore_vm` attach the VM to the
/// same subnet the guest was originally configured for — which is required
/// because the guest's static network config (baked into the rootfs) is
/// snapshot/restored verbatim.
///
/// `vm_id` must therefore end in a unique integer in `[0, MAX_VM_INDEX]`.
fn vm_index_from_id(vm_id: &str) -> VmmResult<u8> {
    let digits: String = vm_id
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if digits.is_empty() {
        return Err(VmmError::InvalidConfig(format!(
            "vm_id '{vm_id}' must end in a unique integer in [0,{MAX_VM_INDEX}] \
             (used to derive tap name / subnet / guest MAC)"
        )));
    }
    let n: u32 = digits
        .parse()
        .map_err(|_| VmmError::InvalidConfig(format!("invalid vm_id index: {digits}")))?;
    if n > MAX_VM_INDEX {
        return Err(VmmError::InvalidConfig(format!(
            "vm_id index must be <= {MAX_VM_INDEX} (got {n})"
        )));
    }
    Ok(n as u8)
}

/// Per-VM host networking parameters, derived deterministically from vm_index.
struct VmNet {
    tap_name: String,
    guest_ip: IpAddr,
    gateway_ip: IpAddr,
    /// NAT subnet in CIDR notation, e.g. `172.16.5.0/24`.
    subnet: String,
    /// Guest MAC, e.g. `AA:FC:00:00:00:07`.
    guest_mac: String,
}

/// Derive tap / guest IP / gateway / subnet / MAC from a vm_index.
///
/// Layout (matches `tikod/scripts/start_vm.sh`):
///   tap{N}  172.16.{N}.2 (guest)  172.16.{N}.1 (host gw)  AA:FC:00:00:00:{N+2:02x}
fn derive_net(vm_index: u8) -> VmNet {
    let n = vm_index as u32;
    VmNet {
        tap_name: format!("tiko-tap-{n}"),
        guest_ip: format!("172.16.{n}.2").parse().expect("valid guest IP"),
        gateway_ip: format!("172.16.{n}.1").parse().expect("valid gateway IP"),
        subnet: format!("172.16.{n}.0/24"),
        guest_mac: format!("AA:FC:00:00:00:{:02x}", n + 2),
    }
}

/// Create a TAP device and configure NAT. Idempotent: if the tap or NAT rule
/// already exists (e.g. left over from a crashed run), it is reused rather
/// than causing a failure. Mirrors `start_vm.sh`'s `ip link show` /
/// `iptables -C` guards.
fn create_tap(tap_name: &str, gateway_ip: &str, subnet: &str) -> VmmResult<()> {
    let tap_exists = run_shell(&format!("ip link show {tap_name} >/dev/null 2>&1")).is_ok();
    if !tap_exists {
        run_cmd("ip", &["tuntap", "add", tap_name, "mode", "tap"])?;
    }
    // (Re)add the address; ignore "RTNETLINK answers: File exists".
    let _ = run_shell(&format!(
        "ip addr add {gateway_ip}/24 dev {tap_name} 2>/dev/null || true"
    ));
    run_cmd("ip", &["link", "set", tap_name, "up"])?;
    // NAT for outgoing traffic (add only if the rule isn't already present).
    let nat_present = run_shell(&format!(
        "iptables -t nat -C POSTROUTING -s {subnet} -j MASQUERADE 2>/dev/null"
    ))
    .is_ok();
    if !nat_present {
        run_cmd(
            "iptables",
            &["-t", "nat", "-A", "POSTROUTING", "-s", subnet, "-j", "MASQUERADE"],
        )?;
    }
    // Ensure the host forwards packets between the tap and the default route.
    // Idempotent.
    let _ = run_shell("sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1");
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

/// Run a shell snippet (passed to `sudo sh -c`) and return its combined
/// output as a String, failing on non-zero exit.
fn run_shell(snippet: &str) -> VmmResult<String> {
    let output = std::process::Command::new("sudo")
        .arg("-n")
        .arg("sh")
        .arg("-c")
        .arg(snippet)
        .output()
        .map_err(|e| VmmError::Backend(format!("spawn sh: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VmmError::Backend(format!("sh failed: {stderr}")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Ensure the current user can open `/dev/kvm`.
///
/// Firecracker is launched unprivileged (as the current user, unlike the
/// scripts which use `sudo`), so it inherits this process's KVM access. If
/// the user is not in the `kvm` group and there is no ACL, `create_vm` would
/// fail with "Permission denied" on the KVM object. We self-grant access
/// using the passwordless `sudo` the VMM already requires for tap/iptables:
/// prefer `setfacl` (least privilege); fall back to `chmod 666` if `setfacl`
/// is unavailable, with a warning that a udev rule is the persistent fix.
fn ensure_kvm_access() {
    const KVM: &str = "/dev/kvm";
    let can_open = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(KVM)
        .is_ok();
    if can_open {
        return;
    }

    let user = std::env::var("USER").unwrap_or_default();
    let setfacl_ok = if !user.is_empty() {
        run_shell(&format!("setfacl -m u:{user}:rw {KVM} 2>/dev/null")).is_ok()
            && std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(KVM)
                .is_ok()
    } else {
        false
    };
    if setfacl_ok {
        info!(user = %user, "granted KVM access via setfacl on {KVM}");
        return;
    }

    match run_shell(&format!("chmod 666 {KVM}")) {
        Ok(_) => warn!(
            "{KVM} was not accessible as the current user; fell back to `chmod 666` \
             (setfacl unavailable). Add a udev rule or the kvm group membership for \
             persistence across reboots."
        ),
        Err(e) => warn!(
            "could not grant KVM access on {KVM}: {e}. \
             create_vm will likely fail with a KVM permission error — ensure the \
             service user is in the `kvm` group."
        ),
    }
}

/// Make a per-VM rootfs copy at `dest` from the base image `src`.
///
/// ext4 is single-writer: two VMs booted against the same read-write image
/// would corrupt it. We therefore sparse-copy the base image once per VM
/// (matching `start_vm.sh`) and reuse the copy on restart for speed.
///
/// The copy is made as the **current user** (no sudo) so that Firecracker —
/// which `spawn_firecracker` also launches as the current user — can open the
/// backing file. (The scripts run FC as root via sudo and so can use a
/// root-owned copy; this VMM keeps FC unprivileged and aligns ownership with
/// that.)
fn copy_rootfs_per_vm(src: &Path, dest: &Path) -> VmmResult<()> {
    if dest.exists() {
        debug!(dest = %dest.display(), "per-VM rootfs copy already exists, reusing");
        return Ok(());
    }
    if !src.exists() {
        return Err(VmmError::InvalidConfig(format!(
            "base rootfs not found: {}",
            src.display()
        )));
    }
    info!(src = %src.display(), dest = %dest.display(), "copying base rootfs (sparse)");
    // Plain `cp --sparse=always` (no sudo): creates a current-user-owned copy.
    let output = std::process::Command::new("cp")
        .arg("--sparse=always")
        .arg(src)
        .arg(dest)
        .output()
        .map_err(|e| VmmError::Backend(format!("spawn cp: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VmmError::Backend(format!("sparse cp failed: {stderr}")));
    }
    Ok(())
}

/// Inject the per-VM static network config into the rootfs copy.
///
/// The base image ships `20-eth0.network` hard-wired for `172.16.0.2` (see
/// `create_rootfs.sh`). systemd-networkd uses that file and ignores the
/// kernel `ip=` cmdline, so for any vm_index != 0 we must overwrite it with
/// the VM's own `172.16.{N}.2/24` config — otherwise the guest comes up on
/// the wrong subnet and is unreachable. Mirrors `start_vm.sh` (network +
/// hostname injection via a temporary mount).
fn inject_guest_net(rootfs: &Path, net: &VmNet, vm_index: u8) -> VmmResult<()> {
    let mnt = run_shell("mktemp -d")?;
    let mnt = mnt.trim_end_matches('\n');
    // Ensure the mountpoint is cleaned up no matter how we exit.
    let cleanup = |mnt: &str| {
        let _ = run_shell(&format!("umount {mnt} 2>/dev/null; rmdir {mnt} 2>/dev/null"));
    };

    let guest_cidr = match net.guest_ip {
        IpAddr::V4(v4) => format!("{v4}/24"),
        _ => {
            cleanup(mnt);
            return Err(VmmError::Backend("guest IP must be IPv4".into()));
        }
    };
    let gateway = net.gateway_ip.to_string();

    let net_unit = format!(
        "[Match]\nName=eth0\n\n[Network]\nAddress={guest_cidr}\nGateway={gateway}\nDNS=1.1.1.1\n"
    );
    let hostname = format!("tiko-vm-{vm_index}");

    // Mount (sudo), write the network unit + hostname, unmount.
    let mount_ok = run_shell(&format!("mount {} {mnt}", shell_quote(rootfs))).is_ok();
    if !mount_ok {
        cleanup(mnt);
        return Err(VmmError::Backend(format!(
            "failed to mount rootfs copy {} for net injection",
            rootfs.display()
        )));
    }

    let write_err = (|| {
        run_shell(&format!(
            "tee {mnt}/etc/systemd/network/20-eth0.network >/dev/null <<'NETUNIT'\n{net_unit}NETUNIT"
        ))?;
        run_shell(&format!(
            "echo {hostname} > {mnt}/etc/hostname"
        ))?;
        Ok::<(), VmmError>(())
    })();
    cleanup(mnt);
    write_err?;
    debug!(rootfs = %rootfs.display(), guest = %net.guest_ip, "injected per-VM network config");
    Ok(())
}

/// Single-quote a path for safe inclusion in a shell command. We only ever
/// pass absolute paths we constructed ourselves, so this is belt-and-suspenders.
fn shell_quote(p: &Path) -> String {
    format!("'{}'", p.display())
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
}

impl FirecrackerVmm {
    pub fn new(snapshot_dir: PathBuf) -> Self {
        let runtime_dir = snapshot_dir.join("runtime");
        std::fs::create_dir_all(&runtime_dir).ok();
        std::fs::create_dir_all(&snapshot_dir).ok();

        ensure_kvm_access();

        let fc_bin = std::env::var("FIRECRACKER_BIN").unwrap_or_else(|_| {
            "/home/ubuntu/tiko/firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug/firecracker"
                .into()
        });

        Self {
            snapshot_dir,
            runtime_dir,
            firecracker_bin: PathBuf::from(fc_bin),
            vms: StdMutex::new(HashMap::new()),
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
    ///
    /// The JSON shape matches `tikod/scripts/vm_config-0.json` (the
    /// verified-working reference). The guest IP is NOT passed on the kernel
    /// cmdline — systemd-networkd reads the static config baked into the
    /// rootfs copy (see `inject_guest_net`), so `boot_args` is used verbatim.
    async fn configure_vm(
        client: &FcApiClient,
        config: &VmConfig,
        tap_name: &str,
        guest_mac: &str,
        serial_log: &Path,
    ) -> VmmResult<()> {
        // Boot source.
        client
            .put(
                "/boot-source",
                &json!({
                    "kernel_image_path": config.kernel_path.to_string_lossy(),
                    "boot_args": config.kernel_cmdline,
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
                    "smt": false,
                    "track_dirty_pages": false,
                    "huge_pages": "None",
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
                    "cache_type": "Unsafe",
                    "io_engine": "Sync",
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

        // Network interface (TAP device) with a deterministic guest MAC.
        client
            .put(
                "/network-interfaces/eth0",
                &json!({
                    "iface_id": "eth0",
                    "guest_mac": guest_mac,
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

        // Deterministic per-VM networking keyed off the vm_id's trailing index.
        let vm_index = vm_index_from_id(&vm_id)?;
        let net = derive_net(vm_index);
        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));

        info!(
            vm_id = %vm_id, index = vm_index, tap = %net.tap_name,
            guest_ip = %net.guest_ip, mac = %net.guest_mac,
            "creating Firecracker VM"
        );

        // Per-VM rootfs copy (ext4 is single-writer). The VM attaches this
        // copy, never the shared base image.
        let rootfs_copy = self.snapshot_dir.join(format!("rootfs-{vm_index}.ext4"));
        copy_rootfs_per_vm(&config.rootfs_path, &rootfs_copy)?;
        inject_guest_net(&rootfs_copy, &net, vm_index)?;

        // Create TAP device + NAT. On failure we leave the per-VM rootfs copy
        // in place for a fast retry (it is reused by the next create_vm).
        create_tap(&net.tap_name, &net.gateway_ip.to_string(), &net.subnet)?;

        // Spawn Firecracker process.
        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(result) => result,
            Err(e) => {
                destroy_tap(&net.tap_name, &net.subnet);
                return Err(e);
            }
        };

        // Configure via REST API. Build a config whose rootfs_path points at
        // the per-VM copy so Firecracker attaches the copy, not the base.
        let mut vm_config = config.clone();
        vm_config.rootfs_path = rootfs_copy;
        let client = FcApiClient::new(&api_sock);
        if let Err(e) =
            Self::configure_vm(&client, &vm_config, &net.tap_name, &net.guest_mac, &serial_log).await
        {
            warn!(vm_id = %vm_id, error = %e, "configure_vm failed");
            destroy_tap(&net.tap_name, &net.subnet);
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
                tap_name: net.tap_name,
                subnet: net.subnet,
                serial_log,
                config,
                guest_ip: net.guest_ip,
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
            mem_path,
            config,
        })
    }

    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId> {
        let vm_id = snapshot.vm_id.clone();

        if !snapshot.state_path.exists() {
            return Err(VmmError::SnapshotNotFound(vm_id));
        }
        if !snapshot.mem_path.exists() {
            return Err(VmmError::SnapshotNotFound(vm_id));
        }

        // Precondition: the original VM must be stopped first. Resuming while
        // the original still holds the RW rootfs-{N}.ext4 would corrupt it
        // (the snapshot references that same file). Mirrors resume_vm.sh.
        {
            let vms = self.vms.lock().unwrap();
            if vms.contains_key(&vm_id) {
                return Err(VmmError::InvalidState {
                    vm_id: vm_id.clone(),
                    current: vms[&vm_id].state,
                    expected: VmState::Stopped,
                });
            }
        }

        // Same deterministic networking the VM was created with: the guest's
        // static config (baked into the rootfs copy, carried in memory across
        // the snapshot) expects exactly this subnet.
        let vm_index = vm_index_from_id(&vm_id)?;
        let net = derive_net(vm_index);

        debug!(
            vm_id = %vm_id, tap = %net.tap_name, gateway = %net.gateway_ip,
            "creating TAP for restore"
        );
        create_tap(&net.tap_name, &net.gateway_ip.to_string(), &net.subnet)?;

        // Spawn a fresh Firecracker process (no config-file: the snapshot
        // already encodes the full device config).
        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(result) => result,
            Err(e) => {
                destroy_tap(&net.tap_name, &net.subnet);
                return Err(e);
            }
        };

        debug!(vm_id = %vm_id, snapshot = %snapshot.state_path.display(), "loading snapshot");
        let client = FcApiClient::new(&api_sock);
        // `mem_backend` is the current Firecracker API shape (the older
        // `mem_file_path` top-level field is rejected by recent builds).
        // `resume_vm: false` — restore returns Paused per the Vmm trait
        // contract; the caller resumes explicitly.
        if let Err(e) = client
            .put(
                "/snapshot/load",
                &json!({
                    "snapshot_path": snapshot.state_path.to_string_lossy(),
                    "mem_backend": {
                        "backend_type": "File",
                        "backend_path": snapshot.mem_path.to_string_lossy(),
                    },
                    "resume_vm": false,
                }),
            )
            .await
        {
            warn!(vm_id = %vm_id, error = %e, "snapshot load failed");
            destroy_tap(&net.tap_name, &net.subnet);
            return Err(e);
        }
        debug!(vm_id = %vm_id, "snapshot loaded");

        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));
        let mut vms = self.vms.lock().unwrap();
        vms.insert(
            vm_id.clone(),
            FcVmEntry {
                child: Some(child),
                api_sock,
                tap_name: net.tap_name,
                subnet: net.subnet,
                serial_log,
                config: snapshot.config.clone(),
                guest_ip: net.guest_ip,
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
