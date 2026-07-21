//! Volume provisioning: the [`RemoteBacking`] trait for `remote_slow`
//! storage plus the [`VolumeProvisioner`] that owns `local_fast` image
//! creation (per-VM ephemeral, or persistent across destroy when the drive
//! carries a `persist_key`) and dispatches `remote_slow` to the configured
//! backing (design §9).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tikovm_protocol::volume::VolumeTier;
use tracing::info;

use super::run_user;
use crate::vmm::{DriveConfig, VmmError, VmmResult};

/// A `remote_slow` storage backing. Implementations own placement,
/// formatting, and attach-time lifecycle of remote volumes.
pub trait RemoteBacking: Send + Sync {
    /// Ensure the volume for `drive` exists and return the host path to
    /// attach as a virtio-block drive. Idempotent: re-provisioning an
    /// existing volume returns the same path WITHOUT recreating or
    /// reformatting it (this is what makes snapshot restore and
    /// re-provision-after-destroy safe).
    fn provision(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<PathBuf>;

    /// Terminal destroy of the VM: release attach-time state (e.g. detach
    /// the ublk device). The volume's DATA always persists —
    /// `remote_slow` survives destroy by design. NOT called on suspend:
    /// suspend keeps the device attached (see [`VolumeProvisioner`]).
    fn on_destroy(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<()>;
}

/// Owns volume provisioning for the VMM. `local_fast` images are created
/// inline: per-VM + ephemeral under the snapshot dir when the drive has no
/// `persist_key`, or in a shared local-fast store keyed by `persist_key`
/// (persistent across destroy) when it does. `remote_slow` drives are
/// delegated to the configured backing.
///
/// Suspend does NOT detach remote volumes: `destroy_vm` is shared between
/// suspend (snapshot+destroy) and terminal destroy, and detaching a ublk
/// device that just served a live guest risks a driver-level wedge
/// (in-flight I/O on kill). The device + lease persist across suspend;
/// restore's re-attach is then a cheap idempotent no-op. Detach happens
/// only on terminal destroy (`cleanup_vm`).
pub struct VolumeProvisioner {
    snapshot_dir: PathBuf,
    remote: Arc<dyn RemoteBacking>,
    /// Remote drives provisioned per VM (for the terminal-destroy hook;
    /// `cleanup_vm` does not receive the VmConfig).
    provisioned: Mutex<HashMap<String, Vec<DriveConfig>>>,
}

impl VolumeProvisioner {
    /// New provisioner: `local_fast` images under `snapshot_dir`, remote
    /// drives via `remote`.
    pub fn new(snapshot_dir: PathBuf, remote: Arc<dyn RemoteBacking>) -> Self {
        Self {
            snapshot_dir,
            remote,
            provisioned: Mutex::new(HashMap::new()),
        }
    }

    /// The configured remote backing (for restore re-attach).
    pub fn remote(&self) -> &Arc<dyn RemoteBacking> {
        &self.remote
    }

    /// For each drive with a `size_mb`, ensure its backing exists and is
    /// formatted so the guest can mount by `LABEL=<drive_id>`. Placement:
    /// - `LocalFast`, no `persist_key` -> `snapshot_dir/volumes/<vm>/<name>.ext4`
    ///   (per-VM, deleted on terminal destroy).
    /// - `LocalFast` with `persist_key` ->
    ///   `snapshot_dir/volumes/_persist/<key>/<name>.ext4` (shared store,
    ///   survives destroy; re-provisioning the same key reattaches the
    ///   existing image — the `!path.exists()` guard never reformats).
    /// - `RemoteSlow` -> the configured [`RemoteBacking`] (persists across
    ///   destroy).
    ///
    /// Rewrites each drive's `path` to the real device/image path.
    pub fn provision_drives(&self, vm_id: &str, drives: &mut [DriveConfig]) -> VmmResult<()> {
        for d in drives.iter_mut() {
            let Some(size_mb) = d.size_mb else {
                continue; // caller-supplied path; nothing to provision.
            };
            match d.tier {
                VolumeTier::LocalFast => {
                    let dir = self.local_fast_dir(vm_id, d)?;
                    std::fs::create_dir_all(&dir)?;
                    let path = dir.join(format!("{}.ext4", d.drive_id));
                    if !path.exists() {
                        let path_str = path.display().to_string();
                        let size_arg = format!("{size_mb}M");
                        info!(drive = %d.drive_id, %path_str, size_mb, "creating local_fast volume image");
                        run_user("truncate", &["-s", &size_arg, &path_str])?;
                        run_user("mkfs.ext4", &["-q", "-L", &d.drive_id, &path_str])?;
                    }
                    d.path = path;
                }
                VolumeTier::RemoteSlow => {
                    let path = self.remote.provision(vm_id, d)?;
                    d.path = path;
                    self.provisioned
                        .lock()
                        .unwrap()
                        .entry(vm_id.to_string())
                        .or_default()
                        .push(d.clone());
                }
            }
        }
        Ok(())
    }

    /// Directory for a `local_fast` image: keyed shared store when the drive
    /// carries a `persist_key` (persistent across destroy), else the per-VM
    /// dir (deleted by `cleanup_vm` on terminal destroy). The key becomes a
    /// directory name, so it is restricted to a safe charset.
    fn local_fast_dir(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<PathBuf> {
        let base = self.snapshot_dir.join("volumes");
        match &drive.persist_key {
            Some(key) => {
                if key.is_empty()
                    || key == ".."
                    || !key
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                {
                    return Err(VmmError::InvalidConfig(format!(
                        "invalid persist_key {key:?} for volume {}: expected [A-Za-z0-9._-]+",
                        drive.drive_id
                    )));
                }
                Ok(base.join("_persist").join(key))
            }
            None => Ok(base.join(vm_id)),
        }
    }

    /// Terminal-destroy hook (called from `cleanup_vm`, NOT from
    /// `destroy_vm` — suspend must not detach): forward to the remote
    /// backing for each remote_slow drive provisioned for this VM. Errors
    /// are logged, not fatal — teardown must proceed.
    ///
    /// `local_fast` needs no hook here: per-VM images are removed with the
    /// per-VM dir by the caller; keyed persistent images live outside it and
    /// are retained by design.
    pub fn on_destroy_volumes(&self, vm_id: &str) {
        let drives = self
            .provisioned
            .lock()
            .unwrap()
            .remove(vm_id)
            .unwrap_or_default();
        for d in &drives {
            if let Err(e) = self.remote.on_destroy(vm_id, d) {
                tracing::warn!(vm_id, drive = %d.drive_id, error = %e, "remote on_destroy failed (continuing)");
            }
        }
    }

    /// Restore path: re-attach every remote_slow drive and require the
    /// backing to hand back the SAME path recorded in the snapshot
    /// (Firecracker's snapshot references the old device path; tikoblk's
    /// registry reserves dev ids to make this hold).
    pub fn reattach_drives(&self, vm_id: &str, drives: &[DriveConfig]) -> VmmResult<()> {
        for d in drives {
            if d.tier != VolumeTier::RemoteSlow {
                continue;
            }
            let path = self.remote.provision(vm_id, d)?;
            if path != d.path {
                return Err(VmmError::Backend(format!(
                    "restore re-attach of {} gave {}, snapshot recorded {}",
                    d.drive_id,
                    path.display(),
                    d.path.display()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// Recording fake backing for provisioner bookkeeping tests.
    struct FakeRemote {
        calls: StdMutex<Vec<String>>,
    }

    impl RemoteBacking for FakeRemote {
        fn provision(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<PathBuf> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("provision:{vm_id}:{}", drive.drive_id));
            Ok(PathBuf::from(format!("/dev/fake-{}", drive.drive_id)))
        }
        fn on_destroy(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("destroy:{vm_id}:{}", drive.drive_id));
            Ok(())
        }
    }

    fn remote_drive(name: &str) -> DriveConfig {
        DriveConfig {
            drive_id: name.into(),
            path: PathBuf::new(),
            read_only: false,
            size_mb: Some(64),
            tier: VolumeTier::RemoteSlow,
            source: None,
            persist_key: None,
        }
    }

    fn local_drive(name: &str, persist_key: Option<&str>) -> DriveConfig {
        DriveConfig {
            drive_id: name.into(),
            path: PathBuf::new(),
            read_only: false,
            size_mb: Some(64),
            tier: VolumeTier::LocalFast,
            source: None,
            persist_key: persist_key.map(|s| s.to_string()),
        }
    }

    fn test_provisioner() -> VolumeProvisioner {
        VolumeProvisioner::new(
            PathBuf::from("/snap"),
            Arc::new(FakeRemote {
                calls: StdMutex::new(Vec::new()),
            }),
        )
    }

    #[test]
    fn local_fast_without_key_is_per_vm() {
        let prov = test_provisioner();
        let d = local_drive("data", None);
        let dir = prov.local_fast_dir("vm-1", &d).unwrap();
        assert_eq!(dir, PathBuf::from("/snap/volumes/vm-1"));
    }

    #[test]
    fn local_fast_with_key_lives_in_shared_store_across_vms() {
        let prov = test_provisioner();
        let d = local_drive("data", Some("tenant-42.pg"));
        // Two different VM generations with the same key map to the SAME dir —
        // this is what makes PGDATA survive destroy + re-provision.
        let dir_a = prov.local_fast_dir("vm-1", &d).unwrap();
        let dir_b = prov.local_fast_dir("vm-7", &d).unwrap();
        assert_eq!(dir_a, dir_b);
        assert_eq!(dir_a, PathBuf::from("/snap/volumes/_persist/tenant-42.pg"));
    }

    #[test]
    fn local_fast_rejects_unsafe_persist_keys() {
        let prov = test_provisioner();
        for bad in ["", "..", "a/b", "a b", "../x", "x\\y"] {
            let d = local_drive("data", Some(bad));
            assert!(
                prov.local_fast_dir("vm-1", &d).is_err(),
                "key {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn provisioner_tracks_remote_drives_for_terminal_destroy() {
        let remote = Arc::new(FakeRemote {
            calls: StdMutex::new(Vec::new()),
        });
        let prov = VolumeProvisioner::new(PathBuf::from("/snap"), remote.clone());
        let mut drives = vec![remote_drive("archive")];
        prov.provision_drives("vm-1", &mut drives).unwrap();
        assert_eq!(drives[0].path, PathBuf::from("/dev/fake-archive"));
        // Nothing destroyed yet.
        assert!(
            remote
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|c| c.starts_with("provision:"))
        );
        prov.on_destroy_volumes("vm-1");
        let calls = remote.calls.lock().unwrap().clone();
        assert!(calls.contains(&"destroy:vm-1:archive".to_string()));
        // Second destroy: no recorded drives -> no further calls.
        let n = remote.calls.lock().unwrap().len();
        prov.on_destroy_volumes("vm-1");
        assert_eq!(remote.calls.lock().unwrap().len(), n);
    }
}
