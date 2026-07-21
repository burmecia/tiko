//! `remote_slow` backing: a sparse ext4 image on a host-mounted remote FS
//! (e.g. S3 Files over NFS), attached as virtio-block. This is the
//! pre-existing behavior, moved verbatim out of
//! `vmm/firecracker.rs::provision_drives`.

use std::path::PathBuf;

use tracing::{info, warn};

use super::run_user;
use super::volume::RemoteBacking;
use crate::vmm::{DriveConfig, VmmResult};

/// Image-on-remote-mount backing: `<source>/<vm_id>/<name>.ext4`.
pub struct S3FilesImage {
    snapshot_dir: PathBuf,
}

impl S3FilesImage {
    /// `snapshot_dir` is only used for the no-source local fallback
    /// (mirrors the legacy behavior).
    pub fn new(snapshot_dir: PathBuf) -> Self {
        Self { snapshot_dir }
    }

    /// Image path for a drive: `<source>/<vm_id>/<drive_id>.ext4`, or the
    /// local fallback when `source` is unset.
    pub fn image_path(&self, vm_id: &str, drive: &DriveConfig) -> PathBuf {
        let base = drive.source.clone().unwrap_or_else(|| {
            warn!(drive = %drive.drive_id, "remote_slow volume has no source; using local fallback");
            self.snapshot_dir
                .join("volumes-remote")
                .to_string_lossy()
                .into_owned()
        });
        PathBuf::from(base)
            .join(vm_id)
            .join(format!("{}.ext4", drive.drive_id))
    }
}

impl RemoteBacking for S3FilesImage {
    fn provision(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<PathBuf> {
        let Some(size_mb) = drive.size_mb else {
            return Err(crate::vmm::VmmError::InvalidConfig(format!(
                "remote_slow volume {} needs size_mb",
                drive.drive_id
            )));
        };
        let path = self.image_path(vm_id, drive);
        std::fs::create_dir_all(path.parent().expect("image parent"))?;
        if !path.exists() {
            let path_str = path.display().to_string();
            let size_arg = format!("{size_mb}M");
            info!(drive = %drive.drive_id, %path_str, size_mb, "creating remote_slow volume image");
            run_user("truncate", &["-s", &size_arg, &path_str])?;
            run_user("mkfs.ext4", &["-q", "-L", &drive.drive_id, &path_str])?;
        }
        Ok(path)
    }

    fn on_destroy(&self, _vm_id: &str, _drive: &DriveConfig) -> VmmResult<()> {
        // Nothing to release: the image persists on the remote mount.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikovm_protocol::volume::VolumeTier;

    fn drive(name: &str, source: Option<&str>) -> DriveConfig {
        DriveConfig {
            drive_id: name.into(),
            path: PathBuf::new(),
            read_only: false,
            size_mb: Some(64),
            tier: VolumeTier::RemoteSlow,
            source: source.map(|s| s.to_string()),
            persist_key: None,
        }
    }

    #[test]
    fn placement_matches_legacy() {
        let b = S3FilesImage::new(PathBuf::from("/snap"));
        assert_eq!(
            b.image_path("vm-1", &drive("archive", Some("/mnt/s3files/tikoblk"))),
            PathBuf::from("/mnt/s3files/tikoblk/vm-1/archive.ext4")
        );
        // No source -> local fallback (unchanged legacy behavior).
        assert_eq!(
            b.image_path("vm-1", &drive("archive", None)),
            PathBuf::from("/snap/volumes-remote/vm-1/archive.ext4")
        );
    }

    #[test]
    fn provision_creates_then_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("tikovm-s3img-{}", std::process::id()));
        let b = S3FilesImage::new(dir.clone());
        let d = drive("archive", Some(dir.to_str().unwrap()));
        let p1 = b.provision("vm-1", &d).unwrap();
        assert!(p1.ends_with("vm-1/archive.ext4"));
        let md1 = std::fs::metadata(&p1).unwrap();
        let p2 = b.provision("vm-1", &d).unwrap();
        let md2 = std::fs::metadata(&p2).unwrap();
        assert_eq!(p1, p2);
        assert_eq!(md1.len(), md2.len(), "re-provision must not recreate");
        assert_eq!(md1.len(), 64 << 20);
        std::fs::remove_dir_all(&dir).ok();
    }
}
