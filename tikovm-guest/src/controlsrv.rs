//! Host→guest command server (design §7) — the guest's AF_VSOCK listener.
//!
//! The host connects (via the vsock UDS + `CONNECT <GUEST_VSOCK_PORT>`) to send
//! lifecycle commands. The guest runs the manifest's suspend hooks here:
//! - `PreSuspend`  → run `[suspend].pre_suspend_cmd`  (clean quiesce before the
//!   host pauses — the VM is still running).
//! - `PostRestore` → run `[suspend].post_restore_cmd` (resume after restore).
//!
//! The `vsock` crate is synchronous, so the accept loop runs on a dedicated
//! thread and each connection on its own thread (the control channel is
//! low-frequency).

use std::io::{Read, Write};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tikovm_protocol::codec;
use tikovm_protocol::manifest::WorkloadManifest;
use tikovm_protocol::rpc::{GuestReply, HostToGuest, GUEST_VSOCK_PORT};

/// `VMADDR_CID_ANY` — bind to any CID (the guest accepts connections addressed
/// to its own CID).
const VMADDR_CID_ANY: u32 = u32::MAX;

/// Hook execution timeout (don't let a misbehaving hook block suspend).
const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// Run the command server on a background thread until the process exits.
pub fn spawn(manifest: Arc<WorkloadManifest>) {
    std::thread::Builder::new()
        .name("tikovm-ctrl".into())
        .spawn(move || run(manifest))
        .ok();
}

fn run(manifest: Arc<WorkloadManifest>) {
    let addr = vsock::VsockAddr::new(VMADDR_CID_ANY, GUEST_VSOCK_PORT);
    let listener = match vsock::VsockListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, "control server: vsock bind failed (host->guest hooks disabled)");
            return;
        }
    };
    tracing::info!(port = GUEST_VSOCK_PORT, "control server listening on vsock");
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let m = manifest.clone();
        std::thread::spawn(move || {
            if let Err(e) = handle(&mut stream, &m) {
                tracing::debug!(error = %e, "control server conn ended");
            }
        });
    }
}

fn handle(stream: &mut vsock::VsockStream, manifest: &WorkloadManifest) -> std::io::Result<()> {
    let payload = read_frame(stream)?;
    let cmd: HostToGuest = serde_json::from_slice(&payload).map_err(std::io::Error::other)?;
    let reply = match cmd {
        HostToGuest::PreSuspend => run_hook(&manifest.suspend.pre_suspend_cmd, "pre_suspend"),
        HostToGuest::PostRestore => run_hook(&manifest.suspend.post_restore_cmd, "post_restore"),
        HostToGuest::GetHealth => GuestReply::Health { healthy: true },
        HostToGuest::Start => GuestReply::Ok, // workload auto-starts via the supervisor
        HostToGuest::Stop { .. } => GuestReply::Ok, // TODO: wire to the supervisor's StopHandle
    };
    let out = serde_json::to_vec(&reply).map_err(std::io::Error::other)?;
    write_frame(stream, &out)
}

/// Run a manifest hook command (`sh -c <cmd>`), capped at [`HOOK_TIMEOUT`].
/// Returns `Ok` when there's no hook or it exited successfully; `Error` otherwise.
fn run_hook(cmd: &Option<String>, label: &str) -> GuestReply {
    let Some(cmd) = cmd else { return GuestReply::Ok };
    let mut child = match std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return GuestReply::Error { message: format!("spawn {label}: {e}") },
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return if status.success() {
                    GuestReply::Ok
                } else {
                    GuestReply::Error { message: format!("{label} hook exited {status}") }
                };
            }
            Ok(None) => {
                if start.elapsed() >= HOOK_TIMEOUT {
                    let _ = child.kill();
                    return GuestReply::Error { message: format!("{label} hook timed out") };
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return GuestReply::Error { message: format!("{label} hook wait: {e}") },
        }
    }
}

fn read_frame<R: Read>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let frame = codec::encode_frame_bytes(payload);
    w.write_all(&frame)?;
    w.flush()
}
