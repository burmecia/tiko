//! Firecracker microVM backend (Linux/KVM only).
//!
//! Drives Firecracker via its REST API (HTTP over Unix socket). Generalized
//! from `tikod/src/vmm/firecracker.rs`: drops the hardcoded Tiko/PG identity
//! seeding, adds the virtio-vsock device (control channel), and keeps the
//! two-drive overlay model + deterministic per-VM networking.
//!
//! ```text
//! hostd ──→ FcApiClient ──HTTP/Unix──→ Firecracker process ──KVM──→ Guest VM
//!            ├── /boot-source, /machine-config, /drives/*, /network-interfaces/eth0
//!            ├── /vsock (guest_cid + uds_path)
//!            ├── /serial, /actions, /vm (Pause/Resume), /snapshot/{create,load}
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{debug, info, warn};

use super::{BackendState, DriveConfig, Snapshot, VmConfig, VmId, Vmm, VmmError, VmmResult};

// ============================================================================
// FcApiClient — minimal HTTP/1.1 client over Unix socket
// ============================================================================

struct FcApiClient {
    sock_path: PathBuf,
}

impl FcApiClient {
    fn new(sock_path: impl AsRef<Path>) -> Self {
        Self {
            sock_path: sock_path.as_ref().to_path_buf(),
        }
    }

    async fn put(&self, path: &str, body: &serde_json::Value) -> VmmResult<()> {
        self.request("PUT", path, Some(body)).await
    }

    async fn patch(&self, path: &str, body: &serde_json::Value) -> VmmResult<()> {
        self.request("PATCH", path, Some(body)).await
    }

    async fn get(&self, path: &str) -> VmmResult<serde_json::Value> {
        let body_str = self.request_raw("GET", path, None).await?;
        serde_json::from_str(&body_str).map_err(|e| VmmError::Backend(format!("JSON parse: {e}")))
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<()> {
        let _ = self.request_raw(method, path, body).await?;
        Ok(())
    }

    async fn request_raw(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<String> {
        let body_str = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
            len = body_str.len(),
            body = body_str,
        );

        let mut stream = UnixStream::connect(&self.sock_path)
            .await
            .map_err(|e| VmmError::Backend(format!("connect FC socket: {e}")))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| VmmError::Backend(format!("write FC socket: {e}")))?;

        let mut header_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = stream
                .read(&mut byte)
                .await
                .map_err(|e| VmmError::Backend(format!("read header: {e}")))?;
            if n == 0 {
                break;
            }
            header_buf.push(byte[0]);
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

        let content_length: usize = header_str
            .lines()
            .find_map(|line| {
                let line = line.to_lowercase();
                line.strip_prefix("content-length:")
                    .and_then(|rest| rest.trim().parse().ok())
            })
            .unwrap_or(0);

        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 {
            stream
                .read_exact(&mut body_buf)
                .await
                .map_err(|e| VmmError::Backend(format!("read body: {e}")))?;
        }
        let body_str = String::from_utf8_lossy(&body_buf).to_string();

        if (200..300).contains(&status_code) {
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
// Networking helpers (deterministic per-VM from the vm_id's trailing integer)
// ============================================================================

const MAX_VM_INDEX: u32 = 250;

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
            "vm_id '{vm_id}' must end in a unique integer in [0,{MAX_VM_INDEX}] (derives tap/subnet/MAC)"
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

struct VmNet {
    tap_name: String,
    guest_ip: IpAddr,
    gateway_ip: IpAddr,
    subnet: String,
    guest_mac: String,
}

fn derive_net(vm_index: u8) -> VmNet {
    let n = vm_index as u32;
    VmNet {
        tap_name: format!("tikovm-tap-{n}"),
        guest_ip: format!("172.16.{n}.2").parse().expect("valid guest IP"),
        gateway_ip: format!("172.16.{n}.1").parse().expect("valid gateway IP"),
        subnet: format!("172.16.{n}.0/24"),
        guest_mac: format!("AA:FC:00:00:00:{:02x}", n + 2),
    }
}

fn default_iface() -> VmmResult<String> {
    let out = run_shell("ip route show default")?;
    parse_default_iface(&out)
        .ok_or_else(|| VmmError::Backend("no default route found for FORWARD rules".into()))
}

fn parse_default_iface(ip_route_output: &str) -> Option<String> {
    for line in ip_route_output.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("default") {
            continue;
        }
        let mut iter = trimmed.split_whitespace();
        while let Some(tok) = iter.next() {
            if tok == "dev"
                && let Some(iface) = iter.next()
                && !iface.is_empty()
            {
                return Some(iface.to_string());
            }
        }
    }
    None
}

fn create_tap(tap_name: &str, gateway_ip: &str, subnet: &str) -> VmmResult<()> {
    let tap_exists = run_shell(&format!("ip link show {tap_name} >/dev/null 2>&1")).is_ok();
    if !tap_exists {
        run_cmd("ip", &["tuntap", "add", tap_name, "mode", "tap"])?;
    }
    let _ = run_shell(&format!(
        "ip addr add {gateway_ip}/24 dev {tap_name} 2>/dev/null || true"
    ));
    run_cmd("ip", &["link", "set", tap_name, "up"])?;
    let nat_present = run_shell(&format!(
        "iptables -t nat -C POSTROUTING -s {subnet} -j MASQUERADE 2>/dev/null"
    ))
    .is_ok();
    if !nat_present {
        run_cmd(
            "iptables",
            &[
                "-t",
                "nat",
                "-A",
                "POSTROUTING",
                "-s",
                subnet,
                "-j",
                "MASQUERADE",
            ],
        )?;
    }
    add_forward_rules(tap_name);
    let _ = run_shell("sysctl -w net.ipv4.ip_forward=1 >/dev/null 2>&1");
    Ok(())
}

fn destroy_tap(tap_name: &str, subnet: &str) {
    remove_forward_rules(tap_name);
    let _ = run_cmd(
        "iptables",
        &[
            "-t",
            "nat",
            "-D",
            "POSTROUTING",
            "-s",
            subnet,
            "-j",
            "MASQUERADE",
        ],
    );
    let _ = run_cmd("ip", &["link", "set", tap_name, "down"]);
    let _ = run_cmd("ip", &["tuntap", "del", tap_name, "mode", "tap"]);
}

fn add_forward_rules(tap_name: &str) {
    let Ok(out_iface) = default_iface() else {
        warn!(
            tap = tap_name,
            "no default route; skipping FORWARD rules (guest egress may be blocked)"
        );
        return;
    };
    let rules = [
        format!(
            "iptables -C FORWARD -i {tap_name} -o {out_iface} -j ACCEPT 2>/dev/null || iptables -A FORWARD -i {tap_name} -o {out_iface} -j ACCEPT"
        ),
        format!(
            "iptables -C FORWARD -i {out_iface} -o {tap_name} -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || iptables -A FORWARD -i {out_iface} -o {tap_name} -m state --state RELATED,ESTABLISHED -j ACCEPT"
        ),
    ];
    for rule in rules {
        if let Err(e) = run_shell(&rule) {
            warn!(tap = tap_name, %out_iface, error = %e, "FORWARD ACCEPT rule failed (non-fatal)");
        }
    }
}

fn remove_forward_rules(tap_name: &str) {
    if let Ok(out_iface) = default_iface() {
        let _ = run_shell(&format!(
            "iptables -D FORWARD -i {tap_name} -o {out_iface} -j ACCEPT 2>/dev/null || true"
        ));
        let _ = run_shell(&format!(
            "iptables -D FORWARD -i {out_iface} -o {tap_name} -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || true"
        ));
    }
}

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

fn run_user(program: &str, args: &[&str]) -> VmmResult<()> {
    let output = std::process::Command::new(program)
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

fn ensure_kvm_access() {
    const KVM: &str = "/dev/kvm";
    if std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(KVM)
        .is_ok()
    {
        return;
    }
    let user = std::env::var("USER").unwrap_or_default();
    let setfacl_ok = !user.is_empty()
        && run_shell(&format!("setfacl -m u:{user}:rw {KVM} 2>/dev/null")).is_ok()
        && std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(KVM)
            .is_ok();
    if setfacl_ok {
        info!(%user, "granted KVM access via setfacl");
        return;
    }
    match run_shell(&format!("chmod 666 {KVM}")) {
        Ok(_) => {
            warn!("fell back to chmod 666 /dev/kvm (add a udev rule or kvm group for persistence)")
        }
        Err(e) => warn!(error = %e, "could not grant KVM access; create_vm will likely fail"),
    }
}

fn copy_rootfs_per_vm(src: &Path, dest: &Path) -> VmmResult<()> {
    if dest.exists() {
        return Ok(());
    }
    if !src.exists() {
        return Err(VmmError::InvalidConfig(format!(
            "base rootfs not found: {}",
            src.display()
        )));
    }
    info!(src = %src.display(), dest = %dest.display(), "copying base rootfs (sparse)");
    let output = std::process::Command::new("cp")
        .arg("--sparse=always")
        .arg(src)
        .arg(dest)
        .output()
        .map_err(|e| VmmError::Backend(format!("spawn cp: {e}")))?;
    if !output.status.success() {
        return Err(VmmError::Backend(format!(
            "sparse cp failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

fn inject_guest_net(rootfs: &Path, net: &VmNet, vm_index: u8) -> VmmResult<()> {
    let mnt = run_shell("mktemp -d")?;
    let mnt = mnt.trim_end_matches('\n');
    let cleanup = |mnt: &str| {
        let _ = run_shell(&format!(
            "umount {mnt} 2>/dev/null; rmdir {mnt} 2>/dev/null"
        ));
    };
    let guest_cidr = match net.guest_ip {
        IpAddr::V4(v4) => format!("{v4}/24"),
        _ => {
            cleanup(mnt);
            return Err(VmmError::Backend("guest IP must be IPv4".into()));
        }
    };
    let net_unit = format!(
        "[Match]\nName=eth0\n\n[Network]\nAddress={guest_cidr}\nGateway={}\nDNS=1.1.1.1\n",
        net.gateway_ip
    );
    let hostname = format!("tikovm-{vm_index}");
    if run_shell(&format!("mount {} {mnt}", shell_quote(rootfs))).is_err() {
        cleanup(mnt);
        return Err(VmmError::Backend(format!(
            "failed to mount rootfs {}",
            rootfs.display()
        )));
    }
    let write_err = (|| {
        run_shell(&format!(
            "tee {mnt}/etc/systemd/network/20-eth0.network >/dev/null <<'NETUNIT'\n{net_unit}NETUNIT"
        ))?;
        run_shell(&format!("echo {hostname} > {mnt}/etc/hostname"))?;
        Ok::<(), VmmError>(())
    })();
    cleanup(mnt);
    write_err?;
    Ok(())
}

fn shell_quote(p: &Path) -> String {
    format!("'{}'", p.display())
}

// ============================================================================
// local_fast volume images (ext4, labeled so the guest mounts by LABEL=<name>)
// ============================================================================

/// For each drive with a `size_mb`, ensure its backing ext4 image exists,
/// formatted with `-L <drive_id>` so the guest mounts by label. Placement
/// depends on tier:
/// - `LocalFast` -> `snapshot_dir/volumes/<vm>/<name>.ext4` (ephemeral).
/// - `RemoteSlow` -> `<source>/<vm>/<name>.ext4` on a host-mounted remote FS
///   (persists across destroy). `source` must be set.
///
/// Rewrites each drive's `path` to the real image path.
fn provision_drives(snapshot_dir: &Path, vm_id: &str, drives: &mut [DriveConfig]) -> VmmResult<()> {
    for d in drives.iter_mut() {
        let Some(size_mb) = d.size_mb else {
            continue; // caller-supplied path; nothing to provision.
        };
        use tikovm_protocol::volume::VolumeTier;
        let dir: PathBuf = match d.tier {
            VolumeTier::LocalFast => snapshot_dir.join("volumes").join(vm_id),
            VolumeTier::RemoteSlow => {
                let base = d.source.clone().unwrap_or_else(|| {
                    warn!(drive = %d.drive_id, "remote_slow volume has no source; using local fallback");
                    snapshot_dir.join("volumes-remote").to_string_lossy().into_owned()
                });
                PathBuf::from(base).join(vm_id)
            }
        };
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.ext4", d.drive_id));
        if !path.exists() {
            let path_str = path.display().to_string();
            let size_arg = format!("{size_mb}M");
            info!(drive = %d.drive_id, %path_str, size_mb, tier = ?d.tier, "creating volume image");
            run_user("truncate", &["-s", &size_arg, &path_str])?;
            run_user("mkfs.ext4", &["-q", "-L", &d.drive_id, &path_str])?;
        }
        d.path = path;
    }
    Ok(())
}

// ============================================================================
// Two-drive overlay model (base RO + per-VM RW overlayfs upper)
// ============================================================================

fn overlay_mode(config: &VmConfig) -> bool {
    config.initrd_path.is_some()
}

fn overlay_image_path(snapshot_dir: &Path, vm_index: u8) -> PathBuf {
    snapshot_dir.join(format!("overlay-{vm_index}.ext4"))
}

fn overlay_size_mb() -> u64 {
    std::env::var("OVERLAY_SIZE_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2048)
}

fn create_overlay_image(path: &Path, base: &Path, net: &VmNet, vm_index: u8) -> VmmResult<()> {
    if path.exists() {
        return Ok(());
    }
    let size_mb = overlay_size_mb();
    info!(path = %path.display(), size_mb, "creating per-VM overlay image (sparse)");
    let size_arg = format!("{size_mb}M");
    let path_str = path.display().to_string();
    run_user("truncate", &["-s", &size_arg, &path_str])?;
    run_user("mkfs.ext4", &["-q", &path_str])?;
    seed_overlay(path, base, net, vm_index)
}

/// Seed the overlay's upper/ with the per-VM network unit + hostname. Generic
/// (no Tiko/PG identity): control-layer env/manifest injection is a separate
/// concern handled at provisioning time.
fn seed_overlay(overlay: &Path, base: &Path, net: &VmNet, vm_index: u8) -> VmmResult<()> {
    let ov = run_shell("mktemp -d")?;
    let ov = ov.trim_end_matches('\n');
    let bro = run_shell("mktemp -d")?;
    let bro = bro.trim_end_matches('\n');
    let cleanup = || {
        let _ = run_shell(&format!(
            "umount {ov} 2>/dev/null; umount {bro} 2>/dev/null; rmdir {ov} {bro} 2>/dev/null"
        ));
    };

    if run_shell(&format!("mount {} {ov}", shell_quote(overlay))).is_err() {
        cleanup();
        return Err(VmmError::Backend(format!(
            "failed to mount overlay {}",
            overlay.display()
        )));
    }
    // Base is mounted RO only to mirror ownership/mode of the network dir.
    if run_shell(&format!("mount -o ro,loop {} {bro}", shell_quote(base))).is_err() {
        cleanup();
        return Err(VmmError::Backend(format!(
            "failed to mount base {} RO",
            base.display()
        )));
    }

    let (guest_cidr, gateway) = match net.guest_ip {
        IpAddr::V4(v4) => (format!("{v4}/24"), net.gateway_ip.to_string()),
        _ => {
            cleanup();
            return Err(VmmError::Backend("guest IP must be IPv4".into()));
        }
    };
    let net_unit = format!(
        "[Match]\nName=eth0\n\n[Network]\nAddress={guest_cidr}\nGateway={gateway}\nDNS=1.1.1.1\n"
    );
    let hostname = format!("tikovm-{vm_index}");

    let seed_err = (|| {
        for rel in ["etc", "etc/systemd", "etc/systemd/network"] {
            run_shell(&format!("mkdir -p {ov}/upper/{rel}"))?;
            let _ = run_shell(&format!(
                "chown --reference={bro}/{rel} {ov}/upper/{rel} 2>/dev/null; \
                 chmod --reference={bro}/{rel} {ov}/upper/{rel} 2>/dev/null; true"
            ));
        }
        run_shell("mkdir -p {ov}/work").map(|_| ()).unwrap_or(());
        run_shell(&format!("mkdir -p {ov}/work"))?;
        run_shell(&format!(
            "tee {ov}/upper/etc/systemd/network/20-eth0.network >/dev/null <<'NETUNIT'\n{net_unit}NETUNIT"
        ))?;
        run_shell(&format!("echo {hostname} > {ov}/upper/etc/hostname"))?;
        Ok::<(), VmmError>(())
    })();
    cleanup();
    seed_err?;
    debug!(path = %overlay.display(), guest = %net.guest_ip, "overlay seeded (network + hostname)");
    Ok(())
}

// ============================================================================
// Per-VM state + FirecrackerVmm
// ============================================================================

struct FcVmEntry {
    child: Option<tokio::process::Child>,
    api_sock: PathBuf,
    /// virtio-vsock host-side UDS. Removed on drop so a restore can rebind it
    /// (Firecracker fails to bind a UDS path that still exists on disk).
    vsock_uds: PathBuf,
    tap_name: String,
    subnet: String,
    #[allow(dead_code)]
    serial_log: PathBuf,
    config: VmConfig,
    guest_ip: IpAddr,
    state: BackendState,
}

impl Drop for FcVmEntry {
    fn drop(&mut self) {
        destroy_tap(&self.tap_name, &self.subnet);
        let _ = std::fs::remove_file(&self.api_sock);
        let _ = std::fs::remove_file(&self.vsock_uds);
    }
}

pub struct FirecrackerVmm {
    snapshot_dir: PathBuf,
    runtime_dir: PathBuf,
    firecracker_bin: PathBuf,
    vms: StdMutex<HashMap<VmId, FcVmEntry>>,
    /// Next virtio-vsock guest CID to allocate (CIDs must be >= 3; 2 is host).
    next_cid: AtomicU32,
}

impl FirecrackerVmm {
    pub fn new(snapshot_dir: PathBuf) -> Self {
        let runtime_dir = snapshot_dir.join("runtime");
        std::fs::create_dir_all(&runtime_dir).ok();
        std::fs::create_dir_all(&snapshot_dir).ok();
        ensure_kvm_access();
        let fc_bin = std::env::var("FIRECRACKER_BIN").unwrap_or_else(|_| "firecracker".into());
        Self {
            snapshot_dir,
            runtime_dir,
            firecracker_bin: PathBuf::from(fc_bin),
            vms: StdMutex::new(HashMap::new()),
            next_cid: AtomicU32::new(3),
        }
    }

    /// Allocate a unique guest vsock CID (>= 3).
    fn alloc_cid(&self) -> u32 {
        loop {
            let cid = self.next_cid.fetch_add(1, Ordering::Relaxed);
            if cid >= 3 {
                return cid;
            }
        }
    }

    fn vsock_uds(&self, vm_id: &str) -> PathBuf {
        self.runtime_dir.join(format!("{vm_id}.vsock.sock"))
    }

    fn spawn_firecracker(&self, vm_id: &str) -> VmmResult<(tokio::process::Child, PathBuf)> {
        let sock_path = self.runtime_dir.join(format!("{vm_id}.sock"));
        let _ = std::fs::remove_file(&sock_path);
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
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if sock_path.exists() {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(VmmError::Backend(
                    "Firecracker API socket did not appear within 10s".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        Ok((child, sock_path))
    }

    #[allow(clippy::too_many_arguments)]
    async fn configure_vm(
        &self,
        client: &FcApiClient,
        config: &VmConfig,
        vm_id: &str,
        tap_name: &str,
        guest_mac: &str,
        serial_log: &Path,
        overlay_path: Option<&Path>,
    ) -> VmmResult<()> {
        let mut boot_source = json!({
            "kernel_image_path": config.kernel_path.to_string_lossy(),
            "boot_args": config.kernel_cmdline,
        });
        if let Some(initrd) = &config.initrd_path {
            boot_source["initrd_path"] = json!(initrd.to_string_lossy());
        }
        client.put("/boot-source", &boot_source).await?;
        client
            .put(
                "/machine-config",
                &json!({ "vcpu_count": config.vcpus, "mem_size_mib": config.memory_mb, "smt": false, "track_dirty_pages": false }),
            )
            .await?;

        client
            .put(
                "/drives/rootfs",
                &json!({
                    "drive_id": "rootfs",
                    "path_on_host": config.rootfs_path.to_string_lossy(),
                    "is_root_device": true,
                    "is_read_only": overlay_path.is_some(),
                    "cache_type": "Unsafe",
                    "io_engine": "Sync",
                }),
            )
            .await?;

        if let Some(overlay) = overlay_path {
            client
                .put(
                    "/drives/overlay",
                    &json!({
                        "drive_id": "overlay",
                        "path_on_host": overlay.to_string_lossy(),
                        "is_root_device": false,
                        "is_read_only": false,
                        "cache_type": "Unsafe",
                        "io_engine": "Sync",
                    }),
                )
                .await?;
        }

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

        client
            .put(
                "/network-interfaces/eth0",
                &json!({ "iface_id": "eth0", "guest_mac": guest_mac, "host_dev_name": tap_name }),
            )
            .await?;

        // virtio-vsock control channel.
        if let Some(cid) = config.guest_cid {
            let uds = self.vsock_uds(vm_id);
            client
                .put(
                    "/vsock",
                    &json!({ "guest_cid": cid, "uds_path": uds.to_string_lossy() }),
                )
                .await?;
        }

        client
            .put(
                "/serial",
                &json!({ "serial_out_path": serial_log.to_string_lossy() }),
            )
            .await?;
        Ok(())
    }

    fn snapshot_paths(&self, vm_id: &str) -> (PathBuf, PathBuf) {
        (
            self.snapshot_dir.join(format!("{vm_id}.snapshot")),
            self.snapshot_dir.join(format!("{vm_id}.mem")),
        )
    }
}

#[async_trait]
impl Vmm for FirecrackerVmm {
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId> {
        // Assign a vsock CID if none given so it is captured in the snapshot and
        // reused on restore (the guest's vsock identity must stay stable).
        let mut config = config;
        if config.guest_cid.is_none() {
            config.guest_cid = Some(self.alloc_cid());
        }
        let vm_id = config.vm_id.clone();
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
        if let Some(initrd) = &config.initrd_path
            && !initrd.exists()
        {
            return Err(VmmError::InvalidConfig(format!(
                "initrd not found: {}",
                initrd.display()
            )));
        }

        let vm_index = vm_index_from_id(&vm_id)?;
        let net = derive_net(vm_index);
        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));
        let overlay = overlay_mode(&config);

        info!(vm_id = %vm_id, index = vm_index, tap = %net.tap_name, guest_ip = %net.guest_ip, overlay, "creating Firecracker VM");

        let (rootfs_for_attach, overlay_path): (PathBuf, Option<PathBuf>) = if overlay {
            let ov = overlay_image_path(&self.snapshot_dir, vm_index);
            create_overlay_image(&ov, &config.rootfs_path, &net, vm_index)?;
            (config.rootfs_path.clone(), Some(ov))
        } else {
            let rootfs_copy = self.snapshot_dir.join(format!("rootfs-{vm_index}.ext4"));
            copy_rootfs_per_vm(&config.rootfs_path, &rootfs_copy)?;
            inject_guest_net(&rootfs_copy, &net, vm_index)?;
            (rootfs_copy, None)
        };

        create_tap(&net.tap_name, &net.gateway_ip.to_string(), &net.subnet)?;

        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(r) => r,
            Err(e) => {
                destroy_tap(&net.tap_name, &net.subnet);
                return Err(e);
            }
        };

        let mut vm_config = config.clone();
        vm_config.rootfs_path = rootfs_for_attach;
        // Provision local_fast volume images (ext4, labeled by drive_id so the
        // guest can mount by LABEL=<name>). Reused across restarts.
        provision_drives(&self.snapshot_dir, &vm_id, &mut vm_config.drives)?;
        let client = FcApiClient::new(&api_sock);
        if let Err(e) = self
            .configure_vm(
                &client,
                &vm_config,
                &vm_id,
                &net.tap_name,
                &net.guest_mac,
                &serial_log,
                overlay_path.as_deref(),
            )
            .await
        {
            warn!(vm_id = %vm_id, error = %e, "configure_vm failed");
            destroy_tap(&net.tap_name, &net.subnet);
            let _ = std::fs::remove_file(&api_sock);
            return Err(e);
        }

        self.vms.lock().unwrap().insert(
            vm_id.clone(),
            FcVmEntry {
                child: Some(child),
                api_sock,
                vsock_uds: self.vsock_uds(&vm_id),
                tap_name: net.tap_name,
                subnet: net.subnet,
                serial_log,
                config,
                guest_ip: net.guest_ip,
                state: BackendState::Created,
            },
        );
        info!(vm_id = %vm_id, "Firecracker VM created and configured");
        Ok(vm_id)
    }

    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = self.lock_sock(vm_id)?;
        let client = FcApiClient::new(&sock_path);
        client
            .put("/actions", &json!({"action_type": "InstanceStart"}))
            .await?;
        self.set_state(vm_id, BackendState::Started);
        info!(vm_id = %vm_id, "Firecracker VM started");
        Ok(())
    }

    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = self.lock_sock(vm_id)?;
        let client = FcApiClient::new(&sock_path);
        client.patch("/vm", &json!({"state": "Paused"})).await?;
        self.set_state(vm_id, BackendState::Paused);
        info!(vm_id = %vm_id, "Firecracker VM paused");
        Ok(())
    }

    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let sock_path = self.lock_sock(vm_id)?;
        let client = FcApiClient::new(&sock_path);
        client.patch("/vm", &json!({"state": "Resumed"})).await?;
        self.set_state(vm_id, BackendState::Started);
        info!(vm_id = %vm_id, "Firecracker VM resumed");
        Ok(())
    }

    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        let (sock_path, config, snap_paths) = {
            let vms = self.vms.lock().unwrap();
            let entry = vms
                .get(vm_id)
                .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            if entry.state != BackendState::Paused {
                return Err(VmmError::InvalidState {
                    vm_id: vm_id.clone(),
                    current: "not paused",
                    required: "paused",
                });
            }
            (
                entry.api_sock.clone(),
                entry.config.clone(),
                self.snapshot_paths(vm_id),
            )
        };
        let (snap_path, mem_path) = snap_paths;
        let client = FcApiClient::new(&sock_path);
        client
            .put(
                "/snapshot/create",
                &json!({ "snapshot_path": snap_path.to_string_lossy(), "mem_file_path": mem_path.to_string_lossy(), "snapshot_type": "Full" }),
            )
            .await?;
        info!(vm_id = %vm_id, snap = %snap_path.display(), "snapshot created");
        Ok(Snapshot {
            vm_id: vm_id.clone(),
            state_path: snap_path,
            mem_path,
            config,
        })
    }

    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId> {
        let vm_id = snapshot.vm_id.clone();
        if !snapshot.state_path.exists() || !snapshot.mem_path.exists() {
            return Err(VmmError::SnapshotNotFound(vm_id));
        }
        {
            let vms = self.vms.lock().unwrap();
            if vms.contains_key(&vm_id) {
                return Err(VmmError::InvalidState {
                    vm_id: vm_id.clone(),
                    current: "present",
                    required: "destroyed",
                });
            }
        }
        let vm_index = vm_index_from_id(&vm_id)?;
        let net = derive_net(vm_index);
        // Remove any stale vsock UDS so the restored Firecracker can rebind it.
        let _ = std::fs::remove_file(self.vsock_uds(&vm_id));
        create_tap(&net.tap_name, &net.gateway_ip.to_string(), &net.subnet)?;

        let (child, api_sock) = match self.spawn_firecracker(&vm_id) {
            Ok(r) => r,
            Err(e) => {
                destroy_tap(&net.tap_name, &net.subnet);
                return Err(e);
            }
        };

        let client = FcApiClient::new(&api_sock);
        let mut load = json!({
            "snapshot_path": snapshot.state_path.to_string_lossy(),
            "mem_backend": { "backend_type": "File", "backend_path": snapshot.mem_path.to_string_lossy() },
            "resume_vm": false,
        });
        // The vsock UDS path collides on restore; override to a fresh path.
        if snapshot.config.guest_cid.is_some() {
            let uds = self.vsock_uds(&vm_id).to_string_lossy().to_string();
            load["vsock_override"] = json!({ "uds_path": uds });
        }
        if let Err(e) = client.put("/snapshot/load", &load).await {
            warn!(vm_id = %vm_id, error = %e, "snapshot load failed");
            destroy_tap(&net.tap_name, &net.subnet);
            return Err(e);
        }

        let serial_log = self.runtime_dir.join(format!("{vm_id}.console.log"));
        self.vms.lock().unwrap().insert(
            vm_id.clone(),
            FcVmEntry {
                child: Some(child),
                api_sock,
                vsock_uds: self.vsock_uds(&vm_id),
                tap_name: net.tap_name,
                subnet: net.subnet,
                serial_log,
                config: snapshot.config.clone(),
                guest_ip: net.guest_ip,
                state: BackendState::Paused,
            },
        );
        info!(vm_id = %vm_id, "Firecracker VM restored from snapshot");
        Ok(vm_id)
    }

    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut entry = {
            self.vms
                .lock()
                .unwrap()
                .remove(vm_id)
                .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?
        };
        info!(vm_id = %vm_id, "destroying Firecracker VM");
        let client = FcApiClient::new(&entry.api_sock);
        let _ = client
            .put("/actions", &json!({"action_type": "SendCtrlAltDel"}))
            .await;
        if let Some(child) = entry.child.as_mut() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), child.wait()).await;
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        entry.state = BackendState::Destroyed;
        drop(entry); // FcVmEntry::drop tears down TAP + socket
        info!(vm_id = %vm_id, "Firecracker VM destroyed");
        Ok(())
    }

    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<BackendState> {
        let (sock_path, cached) = {
            let vms = self.vms.lock().unwrap();
            let entry = vms
                .get(vm_id)
                .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
            (entry.api_sock.clone(), entry.state)
        };
        let client = FcApiClient::new(&sock_path);
        match client.get("/").await {
            Ok(info) => {
                let state = info.get("state").and_then(|s| s.as_str()).unwrap_or("");
                Ok(match state {
                    "Running" => BackendState::Started,
                    "Paused" => BackendState::Paused,
                    _ => cached,
                })
            }
            Err(_) => Ok(cached),
        }
    }

    async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        let vms = self.vms.lock().unwrap();
        let entry = vms
            .get(vm_id)
            .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))?;
        Ok(Some(entry.guest_ip))
    }

    async fn list_vms(&self) -> VmmResult<Vec<(VmId, BackendState)>> {
        Ok(self
            .vms
            .lock()
            .unwrap()
            .iter()
            .map(|(id, e)| (id.clone(), e.state))
            .collect())
    }

    async fn vsock_uds_path(&self, vm_id: &VmId) -> VmmResult<Option<PathBuf>> {
        let vms = self.vms.lock().unwrap();
        if vms.contains_key(vm_id) {
            Ok(Some(self.vsock_uds(vm_id)))
        } else {
            Ok(None)
        }
    }

    async fn cleanup_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        // Delete ephemeral local_fast volume images. remote_slow images live on
        // a host-mounted remote FS and persist across destroy by design.
        let dir = self.snapshot_dir.join("volumes").join(vm_id);
        if dir.exists()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            warn!(vm_id = %vm_id, error = %e, "could not remove local volume dir");
        }
        Ok(())
    }
}

impl FirecrackerVmm {
    fn lock_sock(&self, vm_id: &VmId) -> VmmResult<PathBuf> {
        let vms = self.vms.lock().unwrap();
        vms.get(vm_id)
            .map(|e| e.api_sock.clone())
            .ok_or_else(|| VmmError::VmNotFound(vm_id.clone()))
    }

    fn set_state(&self, vm_id: &VmId, state: BackendState) {
        if let Some(entry) = self.vms.lock().unwrap().get_mut(vm_id) {
            entry.state = state;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_default_iface_picks_dev_token() {
        assert_eq!(
            parse_default_iface(
                "default via 172.31.16.1 dev ens5 proto dhcp src 172.31.21.123 metric 100\n"
            )
            .as_deref(),
            Some("ens5")
        );
    }

    #[test]
    fn parse_default_iface_ignores_non_default() {
        assert!(parse_default_iface("10.0.0.0/8 via 10.0.0.1 dev eth0 metric 100\n").is_none());
        assert!(parse_default_iface("").is_none());
    }
}
