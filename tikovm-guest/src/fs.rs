//! Volume mounting (design section 9). Mounts the manifest's declared volumes
//! at boot, *before* the workload starts.
//!
//! Volumes are attached by the host as virtio-block devices; the guest mounts
//! them **by ext4 label** (the host formats each `local_fast` image with
//! `-L <volume_name>`), so the guest never needs to know the `/dev/vd*` letter
//! — label mounting is order-independent.

use std::process::Command;

use tikovm_protocol::manifest::WorkloadManifest;

/// Mount every declared volume at its `mount_path`. Best-effort: a volume whose
/// backing device the host didn't attach (or that is already mounted) is warned
/// about and skipped, so a missing optional volume never blocks the workload.
pub fn mount_volumes(manifest: &WorkloadManifest) {
    for v in &manifest.volumes {
        if v.mount_path.as_os_str().is_empty() {
            continue;
        }
        if let Err(e) = std::fs::create_dir_all(&v.mount_path) {
            tracing::warn!(volume = %v.name, error = %e, "mkdir for volume failed");
            continue;
        }
        // Already mounted (e.g. restored from snapshot where the mount persists)?
        if is_mounted(&v.mount_path) {
            tracing::debug!(volume = %v.name, path = %v.mount_path.display(), "volume already mounted");
            continue;
        }
        let mut opts = vec![];
        if v.read_only {
            opts.push("-r".to_string());
        }
        let mut cmd = Command::new("mount");
        for o in &opts {
            cmd.arg(o);
        }
        cmd.arg(format!("LABEL={}", v.name)).arg(&v.mount_path);
        match cmd.status() {
            Ok(s) if s.success() => {
                tracing::info!(volume = %v.name, path = %v.mount_path.display(), "mounted volume");
            }
            Ok(s) => {
                tracing::warn!(volume = %v.name, code = ?s.code(), "volume mount failed (is the host attaching it?)");
            }
            Err(e) => {
                tracing::warn!(volume = %v.name, error = %e, "could not run mount");
            }
        }
    }
}

/// True if `path` is listed in `/proc/mounts` (already mounted).
fn is_mounted(path: &std::path::Path) -> bool {
    let target = path.to_string_lossy();
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return false,
    };
    mounts
        .lines()
        .filter_map(|l| l.split_whitespace().nth(1))
        .any(|p| p == target.as_ref())
}
