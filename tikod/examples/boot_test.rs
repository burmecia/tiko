//! Firecracker VMM lifecycle integration test — driven over the HTTP control API.
//!
//! Starts an in-process `ApiServer` (backed by `FirecrackerVmm`) on an ephemeral
//! port, then exercises every method of the `Vmm` trait via [`ApiClient`] over
//! real HTTP — the same surface an external orchestrator would use. This is an
//! end-to-end test of the API layer, not a direct VMM call test.
//!
//! ```text
//! Stage A–E  single-VM full lifecycle   create→start→pause→resume→freeze→restore→resume→destroy
//! Stage F    error paths                VmNotFound / InvalidState / SnapshotNotFound
//! Stage G    concurrency                3 VMs through the full lifecycle in parallel
//! Stage H    idempotency                start-when-running / pause-when-paused
//! ```
//!
//! Liveness signal is TCP port 22 (sshd). PostgreSQL / S3-Files are out of
//! scope for the Vmm layer — they live inside the guest and are started by
//! the baked-in guest scripts, not by the VMM.
//!
//! # Prerequisites
//!
//! These are provided by `tikod/scripts/` (not by this test):
//!   - `download_kernel.sh` → `assets/vmlinux-6.1`
//!   - `create_rootfs.sh`   → `assets/ubuntu-24.04-rootfs.ext4` (Ubuntu + PG
//!     + S3 Files mount, static `172.16.0.2` network unit baked in)
//!
//! Each `vm_id` ends in a unique integer in `[0, 250]`; that integer derives
//! the tap name, `172.16.{N}.0/24` subnet, and guest MAC (see
//! `firecracker::vm_index_from_id`). The indices used here never overlap:
//!   single=10  errors=11,12  idempotency=13  concurrency=20,21,22

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};

use tikod::api::{ApiClient, ApiServer};
use tikod::control::Control;
use tikod::node::Node;
use tikod::vmm::firecracker::FirecrackerVmm;
use tikod::vmm::{VmConfig, VmState, VmmError};

/// Kernel cmdline for the two-drive overlay model. No `root=` is needed: the
/// initramfs (`assets/tiko-initramfs.cpio.gz`) assembles an overlayfs root from
/// /dev/vda (RO base) + /dev/vdb (RW overlay) before handing off to systemd.
const BOOT_ARGS: &str = "console=ttyS0 reboot=k panic=1 pci=off systemd.unified_cgroup_hierarchy=0";

/// TCP connect timeout used for single liveness probes.
const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    // ── Asset precondition check ───────────────────────────────────────────
    let assets_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets");
    let kernel = assets_dir.join("vmlinux-6.1");
    let rootfs = assets_dir.join("ubuntu-24.04-rootfs.ext4");
    let initrd = assets_dir.join("tiko-initramfs.cpio.gz");
    for (name, path) in [
        ("kernel", &kernel),
        ("rootfs", &rootfs),
        ("initrd", &initrd),
    ] {
        if !path.exists() {
            eprintln!("{name} not found at {}", path.display());
            eprintln!(
                "Run: scripts/download_kernel.sh && scripts/create_rootfs.sh \
                 && scripts/build_initramfs.sh"
            );
            std::process::exit(1);
        }
    }

    let data_dir = std::env::var("BOOT_TEST_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/tikod-boot-test"));
    std::fs::create_dir_all(&data_dir)?;
    tracing::info!(data_dir = %data_dir.display(), "boot test data dir");

    // ── Stand up the HTTP control API in-process ───────────────────────────
    //
    // The test talks to tikod the same way an external orchestrator would:
    // raw HTTP/JSON over TCP. Binding to :0 yields an ephemeral port so the
    // test never collides with a concurrently running tikod.
    let vmm: Arc<FirecrackerVmm> = Arc::new(FirecrackerVmm::new(data_dir.clone()));
    let node = Arc::new(Node::new(vmm, data_dir.join("snapshots")));
    let control = Arc::new(Control::new());

    let api_listener = TcpListener::bind("127.0.0.1:0").await?;
    let api_addr = api_listener.local_addr()?;
    tracing::info!(api_addr = %api_addr, "starting in-process API server");

    let api_server = Arc::new(ApiServer::new(node, control));
    let server_handle = {
        let api_server = api_server.clone();
        tokio::spawn(async move {
            let _ = api_server.serve(api_listener).await;
        })
    };

    let client = ApiClient::new(api_addr);

    // ── Stage A–E: single-VM full lifecycle ────────────────────────────────
    stage("A-E: single-VM full lifecycle");
    full_lifecycle(&client, "fc-single-10", &kernel, &rootfs, &initrd).await?;

    // ── Stage F: error paths ───────────────────────────────────────────────
    stage("F: error paths");
    stage_errors(&client, &kernel, &rootfs, &initrd).await?;

    // ── Stage G: concurrency (3 VMs in parallel) ───────────────────────────
    stage("G: concurrency (3 VMs in parallel)");
    stage_concurrency(&client, &kernel, &rootfs, &initrd).await?;

    // ── Stage H: idempotency ───────────────────────────────────────────────
    stage("H: idempotency");
    stage_idempotency(&client, &kernel, &rootfs, &initrd).await?;

    server_handle.abort();
    tracing::info!("=== ALL STAGES PASSED ===");
    Ok(())
}

// ============================================================================
// Stage A–E: reusable full lifecycle
// ============================================================================

/// Run the complete lifecycle for one VM. Used by the single-VM stage and
/// fanned out by the concurrency stage.
///
/// create → start → (reachable) → pause → (unreachable) → resume →
/// (reachable) → pause → freeze → restore → resume →
/// (reachable) → destroy
async fn full_lifecycle(
    client: &ApiClient,
    vm_id: &str,
    kernel: &Path,
    rootfs: &Path,
    initrd: &Path,
) -> Result<(), String> {
    tracing::info!("--- lifecycle start: {vm_id} ---");
    let config = make_config(vm_id, kernel, rootfs, initrd);

    // A. create + start.
    let id = client
        .create_vm(config)
        .await
        .map_err(|e| format!("create_vm({vm_id}): {e}"))?;
    assert_state(client, &id, VmState::Stopped).await?;

    client
        .start_vm(&id)
        .await
        .map_err(|e| format!("start_vm({vm_id}): {e}"))?;
    assert_state(client, &id, VmState::Running).await?;

    // Reachability via vm_guest_ip + TCP 22 (sshd).
    let ip = client
        .vm_guest_ip(&id)
        .await
        .map_err(|e| format!("vm_guest_ip({vm_id}): {e}"))?
        .ok_or_else(|| format!("vm_guest_ip({vm_id}): no IP returned"))?;
    tracing::info!("{vm_id}: guest IP {ip}; waiting for sshd on :22...");
    await_port_open(ip, 22, Duration::from_secs(90)).await?;

    // B. pause → unreachable → resume → reachable.
    client
        .pause_vm(&id)
        .await
        .map_err(|e| format!("pause_vm({vm_id}): {e}"))?;
    assert_state(client, &id, VmState::Paused).await?;
    await_port_closed(ip, 22, Duration::from_secs(20)).await?;

    client
        .resume_vm(&id)
        .await
        .map_err(|e| format!("resume_vm({vm_id}): {e}"))?;
    assert_state(client, &id, VmState::Running).await?;
    await_port_open(ip, 22, Duration::from_secs(30)).await?;

    // C. freeze: pause → snapshot → destroy, and the snapshot is
    //    recorded in the tikod registry so a later restore can look it up by
    //    vm_id alone (no snapshot details cross the wire to the client).
    client
        .pause_vm(&id)
        .await
        .map_err(|e| format!("pause_vm before freeze ({vm_id}): {e}"))?;
    assert_state(client, &id, VmState::Paused).await?;
    let snap = client
        .freeze(&id)
        .await
        .map_err(|e| format!("freeze({vm_id}): {e}"))?;
    if !snap.state_path.exists() {
        return Err(format!(
            "snapshot state_path missing: {}",
            snap.state_path.display()
        ));
    }
    if !snap.mem_path.exists() {
        return Err(format!(
            "snapshot mem_path missing: {}",
            snap.mem_path.display()
        ));
    }
    let mem_bytes = snap.mem_path.metadata().map(|m| m.len()).unwrap_or(0);
    tracing::info!(
        "{vm_id}: frozen; snapshot files present (state={}, mem={mem_bytes} bytes)",
        snap.state_path.display()
    );
    assert_gone(client, &id).await?;

    // D. restore from the registry-stored snapshot (looked up by vm_id) →
    //    resume → reachable. Same deterministic IP (vm_id unchanged).
    let restored = client
        .restore_vm(&id)
        .await
        .map_err(|e| format!("restore_vm({vm_id}): {e}"))?;
    assert_state(client, &restored, VmState::Paused).await?;
    client
        .resume_vm(&restored)
        .await
        .map_err(|e| format!("resume_vm(restored {vm_id}): {e}"))?;
    assert_state(client, &restored, VmState::Running).await?;
    await_port_open(ip, 22, Duration::from_secs(30)).await?;

    // Cleanup.
    client
        .destroy_vm(&restored)
        .await
        .map_err(|e| format!("destroy_vm(restored {vm_id}): {e}"))?;
    assert_gone(client, &restored).await?;

    tracing::info!("--- lifecycle OK: {vm_id} ---");
    Ok(())
}

// ============================================================================
// Stage F: error paths
// ============================================================================

/// Verify the documented error variants are returned for misuse.
async fn stage_errors(
    client: &ApiClient,
    kernel: &Path,
    rootfs: &Path,
    initrd: &Path,
) -> Result<(), String> {
    // 1. Operations on an unknown vm_id → VmNotFound.
    let ghost = "ghost-99".to_string();
    expect_err(
        "vm_state(unknown)",
        client.vm_state(&ghost).await,
        |e| matches!(e, VmmError::VmNotFound(_)),
        "VmNotFound",
    );
    expect_err(
        "pause_vm(unknown)",
        client.pause_vm(&ghost).await,
        |e| matches!(e, VmmError::VmNotFound(_)),
        "VmNotFound",
    );
    expect_err(
        "destroy_vm(unknown)",
        client.destroy_vm(&ghost).await,
        |e| matches!(e, VmmError::VmNotFound(_)),
        "VmNotFound",
    );

    // 2. snapshot_vm on a Running VM (not Paused) → InvalidState.
    let id = client
        .create_vm(make_config("err-snap-11", kernel, rootfs, initrd))
        .await
        .map_err(|e| format!("create_vm(err-snap-11): {e}"))?;
    client
        .start_vm(&id)
        .await
        .map_err(|e| format!("start_vm(err-snap-11): {e}"))?;
    expect_err(
        "snapshot_vm(Running)",
        client.snapshot_vm(&id).await,
        |e| matches!(e, VmmError::InvalidState { .. }),
        "InvalidState",
    );
    client
        .destroy_vm(&id)
        .await
        .map_err(|e| format!("destroy_vm(err-snap-11): {e}"))?;

    // 3. restore_vm on a VM with no stored snapshot → SnapshotNotFound.
    //    ghost-99 was never frozen, so the registry has no snapshot
    //    for it (and indeed isn't registered at all) → SnapshotNotFound.
    expect_err(
        "restore_vm(no snapshot)",
        client.restore_vm(&"ghost-99".to_string()).await,
        |e| matches!(e, VmmError::SnapshotNotFound(_)),
        "SnapshotNotFound",
    );

    // 4. restore_vm while a new VM with the same id is live → InvalidState
    //    (the snapshot and the live VM would share one RW overlay). The only
    //    way to reach this through the API: freeze (records snapshot +
    //    destroys original), then create a fresh VM with the same id, then
    //    restore — the registry snapshot and the live VM now coexist.
    let id = client
        .create_vm(make_config("err-restore-12", kernel, rootfs, initrd))
        .await
        .map_err(|e| format!("create_vm(err-restore-12): {e}"))?;
    client
        .start_vm(&id)
        .await
        .map_err(|e| format!("start_vm(err-restore-12): {e}"))?;
    client
        .pause_vm(&id)
        .await
        .map_err(|e| format!("pause_vm(err-restore-12): {e}"))?;
    client
        .freeze(&id)
        .await
        .map_err(|e| format!("freeze(err-restore-12): {e}"))?;
    let id2 = client
        .create_vm(make_config("err-restore-12", kernel, rootfs, initrd))
        .await
        .map_err(|e| format!("recreate_vm(err-restore-12): {e}"))?;
    debug_assert_eq!(id, id2, "create_vm should reuse the requested vm_id");
    expect_err(
        "restore_vm(while original live)",
        client.restore_vm(&id).await,
        |e| matches!(e, VmmError::InvalidState { .. }),
        "InvalidState",
    );
    client
        .destroy_vm(&id)
        .await
        .map_err(|e| format!("destroy_vm(err-restore-12): {e}"))?;

    tracing::info!("--- error paths all returned expected variants ---");
    Ok(())
}

// ============================================================================
// Stage G: concurrency
// ============================================================================

/// Run `full_lifecycle` for 3 VMs concurrently via `tokio::spawn` (true
/// parallelism on the multi-thread runtime). The headline check that the
/// API server + VMM handle multiple simultaneous VM lifecycles with no
/// resource collisions (distinct taps, subnets, sockets, and per-VM rootfs
/// copies).
async fn stage_concurrency(
    client: &ApiClient,
    kernel: &Path,
    rootfs: &Path,
    initrd: &Path,
) -> Result<(), String> {
    const N: usize = 3;
    let vm_ids = ["fc-concur-20", "fc-concur-21", "fc-concur-22"];

    let mut handles = Vec::with_capacity(N);
    for vm_id in vm_ids {
        let client = client.clone();
        let kernel = kernel.to_path_buf();
        let rootfs = rootfs.to_path_buf();
        let initrd = initrd.to_path_buf();
        handles.push(tokio::spawn(async move {
            full_lifecycle(&client, vm_id, &kernel, &rootfs, &initrd).await
        }));
    }

    let mut errs = Vec::new();
    for (vm_id, h) in vm_ids.iter().zip(handles) {
        match h.await {
            Ok(Ok(())) => tracing::info!("concurrent lifecycle OK: {vm_id}"),
            Ok(Err(e)) => errs.push(format!("{vm_id}: {e}")),
            Err(join) => errs.push(format!("{vm_id}: join error: {join}")),
        }
    }

    if errs.is_empty() {
        tracing::info!("--- {N} concurrent VM lifecycles all succeeded ---");
        Ok(())
    } else {
        Err(format!(
            "concurrency failures:\n  - {}",
            errs.join("\n  - ")
        ))
    }
}

// ============================================================================
// Stage H: idempotency / re-entrancy
// ============================================================================

/// The trait says methods are "idempotent where reasonable and safe". Observe
/// (do not hard-fail) the behavior of double start / double pause — either an
/// Ok no-op or a well-formed error is acceptable; a panic/hang is not.
async fn stage_idempotency(
    client: &ApiClient,
    kernel: &Path,
    rootfs: &Path,
    initrd: &Path,
) -> Result<(), String> {
    let id = client
        .create_vm(make_config("fc-idem-13", kernel, rootfs, initrd))
        .await
        .map_err(|e| format!("create_vm(fc-idem-13): {e}"))?;

    client
        .start_vm(&id)
        .await
        .map_err(|e| format!("start_vm(fc-idem-13): {e}"))?;
    assert_state(client, &id, VmState::Running).await?;

    match client.start_vm(&id).await {
        Ok(()) => tracing::info!("start_vm(Running) → Ok (treated as idempotent)"),
        Err(e) => tracing::info!("start_vm(Running) → Err (acceptable): {e}"),
    }
    assert_state(client, &id, VmState::Running).await?;

    client
        .pause_vm(&id)
        .await
        .map_err(|e| format!("pause_vm(fc-idem-13): {e}"))?;
    assert_state(client, &id, VmState::Paused).await?;
    match client.pause_vm(&id).await {
        Ok(()) => tracing::info!("pause_vm(Paused) → Ok (treated as idempotent)"),
        Err(e) => tracing::info!("pause_vm(Paused) → Err (acceptable): {e}"),
    }
    assert_state(client, &id, VmState::Paused).await?;

    client
        .resume_vm(&id)
        .await
        .map_err(|e| format!("resume_vm(fc-idem-13): {e}"))?;
    client
        .destroy_vm(&id)
        .await
        .map_err(|e| format!("destroy_vm(fc-idem-13): {e}"))?;
    assert_gone(client, &id).await?;

    tracing::info!("--- idempotency observations collected ---");
    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn stage(name: &str) {
    tracing::info!("===================================================================");
    tracing::info!("=== Stage {name}");
    tracing::info!("===================================================================");
}

/// Build a `VmConfig` for `vm_id` with the script-aligned kernel cmdline.
/// Setting `initrd_path` selects the two-drive overlay model in the Firecracker
/// backend: the rootfs is attached read-only (shared base) and a per-VM overlay
/// image (overlay-<index>.ext4) is attached read-write.
fn make_config(vm_id: &str, kernel: &Path, rootfs: &Path, initrd: &Path) -> VmConfig {
    VmConfig {
        vm_id: vm_id.to_string(),
        kernel_path: kernel.to_path_buf(),
        kernel_cmdline: BOOT_ARGS.to_string(),
        rootfs_path: rootfs.to_path_buf(),
        memory_mb: 512,
        vcpus: 2,
        drives: vec![],
        initrd_path: Some(initrd.to_path_buf()),
    }
}

/// Poll `vm_state` until it matches `expected` (within a short window), or
/// return an error describing the mismatch. State transitions are recorded
/// synchronously after each successful Firecracker API call, so this usually
/// succeeds on the first probe.
async fn assert_state(client: &ApiClient, vm_id: &str, expected: VmState) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match client.vm_state(&vm_id.to_string()).await {
            Ok(s) if s == expected => return Ok(()),
            Ok(s) if Instant::now() >= deadline => {
                return Err(format!(
                    "assert_state({vm_id}): expected {expected}, got {s}"
                ));
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            Err(e) => return Err(format!("assert_state({vm_id}): vm_state failed: {e}")),
        }
    }
}

/// Assert that `vm_id` is no longer known to the VMM (post-destroy).
async fn assert_gone(client: &ApiClient, vm_id: &str) -> Result<(), String> {
    match client.vm_state(&vm_id.to_string()).await {
        Err(VmmError::VmNotFound(_)) => Ok(()),
        Ok(s) => Err(format!(
            "assert_gone({vm_id}): expected VmNotFound, still {s}"
        )),
        Err(e) => Err(format!(
            "assert_gone({vm_id}): expected VmNotFound, got {e}"
        )),
    }
}

/// Assert that a `VmmResult` is an error matching `pred`. Panics otherwise —
/// each call is a discrete invariant, so a panic localizes the failure.
fn expect_err<T>(
    name: &str,
    res: Result<T, VmmError>,
    pred: impl Fn(&VmmError) -> bool,
    expected_label: &str,
) {
    match &res {
        Err(e) if pred(e) => tracing::info!("{name}: got expected {expected_label} ({e})"),
        Err(e) => panic!("{name}: expected {expected_label}, got {e}"),
        Ok(_) => panic!("{name}: expected {expected_label}, but the call succeeded"),
    }
}

/// Attempt a fresh TCP connect to `ip:port` with a short timeout.
async fn tcp_open(ip: IpAddr, port: u16) -> bool {
    let addr = SocketAddr::new(ip, port);
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// Poll until a TCP connect to `ip:port` succeeds (guest is up).
async fn await_port_open(ip: IpAddr, port: u16, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if tcp_open(ip, port).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!(
        "port {port} on {ip} did not open within {}s",
        timeout.as_secs()
    ))
}

/// Poll until a TCP connect to `ip:port` consistently fails (guest frozen).
/// A paused VM cannot complete the TCP handshake, so this is a reliable
/// "is the VM actually paused" signal (unlike ping, which ARP cache can
/// satisfy transiently).
async fn await_port_closed(ip: IpAddr, port: u16, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !tcp_open(ip, port).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!(
        "port {port} on {ip} still accepting connections after {}s \
         (VM not frozen?)",
        timeout.as_secs()
    ))
}
