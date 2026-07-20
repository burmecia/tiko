//! Host storage provisioning (design §9): volume images and remote_slow
//! backings behind the [`volume::RemoteBacking`] trait.

pub mod s3files_image;
pub mod ublk;
pub mod volume;

pub use volume::{RemoteBacking, VolumeProvisioner};

/// Run a program as the current user (no sudo). Moved verbatim from
/// `vmm/firecracker.rs` where volume-image creation used it first.
pub(crate) fn run_user(program: &str, args: &[&str]) -> crate::vmm::VmmResult<()> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| crate::vmm::VmmError::Backend(format!("spawn {program}: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(crate::vmm::VmmError::Backend(format!(
            "{program} {} failed: {stderr}",
            args.join(" ")
        )));
    }
    Ok(())
}

/// Run a program via `sudo -n` (privileged: ublk block devices, the tikoblk
/// control socket while it is root-owned).
pub(crate) fn run_sudo(program: &str, args: &[&str]) -> crate::vmm::VmmResult<()> {
    let output = std::process::Command::new("sudo")
        .arg("-n")
        .arg(program)
        .args(args)
        .output()
        .map_err(|e| crate::vmm::VmmError::Backend(format!("spawn {program}: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(crate::vmm::VmmError::Backend(format!(
            "{program} {} failed: {stderr}",
            args.join(" ")
        )));
    }
    Ok(())
}
