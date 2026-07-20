//! Volume provisioning: the [`RemoteBacking`] trait for `remote_slow`
//! storage plus the [`VolumeProvisioner`] that owns `local_fast` image
//! creation (unchanged semantics) and dispatches `remote_slow` to the
//! configured backing (design §9).

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
/// inline exactly as before (ephemeral, under the snapshot dir);
/// `remote_slow` drives are delegated to the configured backing.
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
    /// - `LocalFast` -> `snapshot_dir/volumes/<vm>/<name>.ext4` (ephemeral).
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
                    let dir = self.snapshot_dir.join("volumes").join(vm_id);
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

    /// Terminal-destroy hook (called from `cleanup_vm`, NOT from
    /// `destroy_vm` — suspend must not detach): forward to the remote
    /// backing for each remote_slow drive provisioned for this VM. Errors
    /// are logged, not fatal — teardown must proceed.
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
