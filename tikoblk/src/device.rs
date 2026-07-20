//! ublk device lifecycle on `libublk`.
//!
//! All devices are created with `UBLK_F_USER_RECOVERY`: when the daemon
//! dies (SIGTERM or otherwise), the kernel quiesces the device and a later
//! daemon reattaches via [`build_recover`]; in-flight I/O stalls and
//! resumes (or fails) transparently — the semantics verified in the spike
//! for both SIGTERM and SIGKILL. `USER_RECOVERY_REISSUE` is deliberately
//! not used (it can repeat an already-applied write — unsafe for
//! non-idempotent backends). `USER_RECOVERY_FAIL_IO` would be preferable
//! for a loop backend, but libublk 0.4.6 rejects it: `UblkCtrlBuilder`
//! validates `ctrl_flags` against `UBLK_DRV_F_ALL`, which predates the
//! FAIL_IO bit (upstream uapi `1 << 9`), so ADD_DEV fails with EINVAL.
//!
//! Node-name bridge: the known-good out-of-tree `ublk2_drv` names its nodes
//! `/dev/ublk2cN`/`/dev/ublk2bN`, while `libublk` hardcodes
//! `/dev/ublkcN`/`/dev/ublkbN`. [`ensure_node_links`] creates symlinks for
//! the libublk names (no-op on mainline-named modules). Links are made in
//! the *host* `/dev` through a directory fd captured before the daemon
//! unshares its mount namespace (see `main.rs`).

use std::os::unix::io::RawFd;
use std::sync::OnceLock;
use std::time::Duration;

use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::{UblkError, UblkFlags};

use crate::registry::VolumeMeta;
use crate::{Error, Result};

/// Queue count for every device (single queue is plenty for Phase 1).
pub const NR_QUEUES: u16 = 1;
/// Queue depth for every device.
pub const QUEUE_DEPTH: u16 = 32;
/// Maximum I/O buffer size (1 MiB) for every device.
pub const IO_BUF_BYTES: u32 = 1 << 20;
/// libublk target type name recorded in the exported per-device json.
pub const TARGET_NAME: &str = "tikoloop";

/// Kernel device state values (uapi `UBLK_S_DEV_*`).
const UBLK_S_DEV_LIVE: u16 = 1;
const UBLK_S_DEV_QUIESCED: u16 = 2;

/// Host `/dev` directory fd captured before the mount-namespace unshare.
/// Symlinks/unlinks through this fd affect the host's devtmpfs even though
/// the daemon runs in a private mount namespace.
static HOST_DEV_DIR: OnceLock<RawFd> = OnceLock::new();

/// Record an open fd for the host's `/dev` directory (call before unshare).
pub fn set_host_dev_dir(fd: RawFd) {
    let _ = HOST_DEV_DIR.set(fd);
}

/// What the kernel knows about one device id.
#[derive(Debug)]
pub struct DevProbe {
    /// Device is present; value is the kernel state (`UBLK_S_DEV_*`).
    pub state: u16,
    /// Raw info flags (`UBLK_F_*` the device was created with).
    pub flags: u64,
}

impl DevProbe {
    /// Device exists and is quiesced, waiting for a recovery daemon.
    pub fn is_quiesced(&self) -> bool {
        self.state == UBLK_S_DEV_QUIESCED
    }
    /// Device exists and is live (served by some daemon).
    pub fn is_live(&self) -> bool {
        self.state == UBLK_S_DEV_LIVE
    }
}

/// True for the libublk errors that mean "no such device". Depending on
/// the driver and command this surfaces as -ENODEV, -ENOENT, or (ublk2
/// GET_DEV_INFO on an unknown id) -EOPNOTSUPP.
pub fn is_device_gone(e: &UblkError) -> bool {
    matches!(
        e,
        UblkError::UringIOError(c) | UblkError::OtherError(c)
            if *c == -libc::ENOENT || *c == -libc::ENODEV || *c == -libc::EOPNOTSUPP
    )
}

/// Probe a device id: `Ok(Some(..))` if the kernel knows it, `Ok(None)` if
/// no such device exists.
pub fn probe(dev_id: u32) -> Result<Option<DevProbe>> {
    // Sysfs presence is the unambiguous existence check (ublk2 names its
    // class ublk2-char; mainline uses ublk-char).
    let present = [
        format!("/sys/class/ublk2-char/ublk2c{dev_id}"),
        format!("/sys/class/ublk-char/ublkc{dev_id}"),
    ]
    .iter()
    .any(|p| std::path::Path::new(p).exists());
    if !present {
        return Ok(None);
    }
    match UblkCtrl::new_simple(dev_id as i32) {
        Ok(ctrl) => {
            let info = ctrl.dev_info();
            Ok(Some(DevProbe {
                state: info.state,
                flags: info.flags,
            }))
        }
        Err(e) if is_device_gone(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Build a control handle for a NEW device (performs `ADD_DEV`).
///
/// The persistent dev flags are stashed in the high 32 bits of
/// `ctrl_target_flags` so the recovery path can rebuild identical flags,
/// exactly as verified in the spike.
pub fn build_add(meta: &VolumeMeta) -> Result<UblkCtrl> {
    // NOTE: no USER_RECOVERY_FAIL_IO — libublk 0.4.6's ctrl_flags mask
    // predates that bit and rejects it with EINVAL (see module docs).
    let ctrl_flags = libublk::sys::UBLK_F_USER_RECOVERY as u64;
    let dev_flags = UblkFlags::UBLK_DEV_F_ADD_DEV;
    let persistent = (dev_flags & !UblkFlags::UBLK_DEV_F_ADD_DEV).bits() as u64;
    let ctrl = UblkCtrlBuilder::default()
        .name(TARGET_NAME)
        .id(meta.dev_id as i32)
        .nr_queues(NR_QUEUES)
        .depth(QUEUE_DEPTH)
        .io_buf_bytes(IO_BUF_BYTES)
        .ctrl_flags(ctrl_flags)
        .ctrl_target_flags(persistent << 32)
        .dev_flags(dev_flags)
        .build()?;
    ensure_node_links(meta.dev_id);
    Ok(ctrl)
}

/// Build a control handle reattaching to a QUIESCED device
/// (`START_USER_RECOVERY` + `UBLK_DEV_F_RECOVER_DEV`), verified in the spike.
///
/// Returns `Err` if the device is missing or not quiesced — callers decide
/// whether to fall back to [`build_add`].
pub fn build_recover(dev_id: u32) -> Result<UblkCtrl> {
    let ctrl = UblkCtrl::new_simple(dev_id as i32)?;
    let info = ctrl.dev_info();
    if (info.flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64)) == 0 {
        return Err(Error::Ublk(format!(
            "device {dev_id} lacks USER_RECOVERY flag"
        )));
    }
    if info.state != UBLK_S_DEV_QUIESCED {
        return Err(Error::InvalidState(format!(
            "device {dev_id} isn't quiesced (state={})",
            info.state
        )));
    }
    ctrl.start_user_recover()?;

    // Dev flags stashed in the high 32 bits of ublksrv_flags at ADD time.
    let stored = (info.ublksrv_flags >> 32) as u32;
    let recovered =
        UblkFlags::from_bits_truncate(stored) | UblkFlags::UBLK_DEV_F_RECOVER_DEV;
    let ctrl = UblkCtrlBuilder::default()
        .name(TARGET_NAME)
        .id(info.dev_id as i32)
        .nr_queues(info.nr_hw_queues)
        .depth(info.queue_depth)
        .ctrl_flags(libublk::sys::UBLK_F_USER_RECOVERY.into())
        .dev_flags(recovered)
        .build()?;
    ensure_node_links(dev_id);
    Ok(ctrl)
}

/// Kill the daemon side of a device and delete it (`kill_dev` + `del_dev`),
/// then drop the node links. Tolerates an already-gone device.
pub fn delete_device(dev_id: u32) -> Result<()> {
    match UblkCtrl::new_simple(dev_id as i32) {
        Ok(ctrl) => {
            // kill_dev is safe even with a dead/wedged daemon; it also makes
            // a live serving daemon's run_target() return.
            ctrl.kill_dev()?;
            ctrl.del_dev()?;
        }
        Err(e) if is_device_gone(&e) => {}
        Err(e) => return Err(e.into()),
    }
    drop_node_links(dev_id);
    Ok(())
}

/// True if the block device has sysfs holders (mounts, dm, loop, ...) —
/// used to refuse a detach that would yank a live filesystem.
pub fn device_busy(dev_id: u32) -> bool {
    // ublk2 names disks ublk2bN; mainline names them ublkbN. Check both.
    for name in [format!("ublk2b{dev_id}"), format!("ublkb{dev_id}")] {
        let holders = format!("/sys/block/{name}/holders");
        if let Ok(mut it) = std::fs::read_dir(&holders)
            && it.next().is_some()
        {
            return true;
        }
    }
    false
}

/// Create the `/dev/ublkcN` -> `/dev/ublk2cN` (and `ublkb`) symlink bridge.
/// Only needed on hosts running the out-of-tree **ublk2** driver (its nodes
/// are ublk2-named while libublk hardcodes the mainline names). On mainline
/// the real nodes have the libublk names already; linking there would squat
/// on the devtmpfs name and break node creation (observed: dangling
/// /dev/ublkb1 -> /dev/ublk2b1 hiding the real mainline node).
pub fn ensure_node_links(dev_id: u32) {
    if !std::path::Path::new("/sys/class/ublk2-char").exists() {
        return; // mainline-named driver: no bridge needed
    }
    for (prefix, alt) in [("/dev/ublkc", "/dev/ublk2c"), ("/dev/ublkb", "/dev/ublk2b")] {
        let link = format!("{prefix}{dev_id}");
        let target = format!("{alt}{dev_id}");
        // Create only when `link` itself is absent (a dangling symlink is
        // fine: devtmpfs may not have created the ublk2 target node yet).
        if link_missing(&link) {
            make_symlink(&target, &link);
        }
    }
}

/// Remove the symlink bridge (only if the names are symlinks we made).
pub fn drop_node_links(dev_id: u32) {
    for prefix in ["/dev/ublkc", "/dev/ublkb"] {
        let link = format!("{prefix}{dev_id}");
        remove_symlink(&link);
    }
}

fn link_missing(path: &str) -> bool {
    if let Some(&dirfd) = HOST_DEV_DIR.get() {
        let name = path.trim_start_matches("/dev/");
        let c = std::ffi::CString::new(name).unwrap();
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::fstatat(dirfd, c.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) };
        rc != 0
    } else {
        std::fs::symlink_metadata(path).is_err()
    }
}

fn make_symlink(target: &str, link: &str) {
    if let Some(&dirfd) = HOST_DEV_DIR.get() {
        let t = std::ffi::CString::new(target).unwrap();
        let l = std::ffi::CString::new(link.trim_start_matches("/dev/")).unwrap();
        let rc = unsafe { libc::symlinkat(t.as_ptr(), dirfd, l.as_ptr()) };
        if rc != 0 {
            tracing::warn!(%link, error = %std::io::Error::last_os_error(), "symlinkat failed");
        }
    } else if let Err(e) = std::os::unix::fs::symlink(target, link) {
        tracing::warn!(%link, error = %e, "symlink failed");
    }
}

fn remove_symlink(link: &str) {
    if let Some(&dirfd) = HOST_DEV_DIR.get() {
        let name = link.trim_start_matches("/dev/");
        let c = std::ffi::CString::new(name).unwrap();
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::fstatat(dirfd, c.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) };
        let is_link = rc == 0 && (st.st_mode & libc::S_IFMT) == libc::S_IFLNK;
        if is_link {
            unsafe { libc::unlinkat(dirfd, c.as_ptr(), 0) };
        }
    } else if let Ok(md) = std::fs::symlink_metadata(link)
        && md.file_type().is_symlink()
    {
        let _ = std::fs::remove_file(link);
    }
}

/// Initialize a [`libublk::io::UblkDev`] target for `meta`.
///
/// On recovery the kernel already has the params; only the target json is
/// refreshed (the spike verified that re-setting params during recovery
/// breaks the reattach).
pub fn init_target(
    dev: &mut libublk::io::UblkDev,
    meta: &VolumeMeta,
    recovering: bool,
) -> std::result::Result<(), UblkError> {
    if !recovering {
        let info = dev.dev_info;
        dev.tgt.dev_size = meta.size_bytes;
        dev.tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::sys::ublk_param_basic {
                attrs: libublk::sys::UBLK_ATTR_VOLATILE_CACHE,
                logical_bs_shift: 9,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: info.max_io_buf_bytes >> 9,
                dev_sectors: meta.size_bytes >> 9,
                ..Default::default()
            },
            ..Default::default()
        };
    }
    let val = serde_json::json!({
        TARGET_NAME: {
            "vol_id": meta.vol_id,
            "backing_path": meta.backing_path,
        }
    });
    dev.set_target_json(val);
    Ok(())
}

/// How long the startup smoke test may take before we declare the driver
/// broken (a broken mainline NULL-derefs ADD_DEV and the call never
/// returns; see target/tmp/ublk-spike/NOTES.md).
const GUARD_TIMEOUT: Duration = Duration::from_secs(10);

/// Startup guard: prove the ublk control device actually works before
/// serving any volume. Performs a throwaway ADD_DEV + DEL_DEV (tiny
/// device, no backend, driver-allocated id) in a worker thread and
/// refuses with a clear error on failure or timeout. This stops the daemon
/// from adopting Ubuntu's broken 6.17.0-10xx mainline ublk_drv (every
/// ADD_DEV oopses there and then leaks ublk_ctl_mutex, poisoning all later
/// control ops until reboot).
pub fn smoke_test_control_device() -> Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("ublk-guard".into())
        .spawn(move || {
            let r = guard_inner();
            let _ = tx.send(r);
        })
        .map_err(Error::Io)?;
    guard_verdict(rx.recv_timeout(GUARD_TIMEOUT).ok())
}

/// Verdict of the guard, factored for testability.
fn guard_verdict(res: Option<Result<()>>) -> Result<()> {
    match res {
        Some(Ok(())) => Ok(()),
        Some(Err(e)) => Err(Error::Ublk(format!(
            "ublk control device smoke test failed: {e} — refusing to start"
        ))),
        None => Err(Error::Ublk(format!(
            "ublk control device smoke test timed out after {GUARD_TIMEOUT:?} — the driver is likely broken \
             (Ubuntu 6.17.0-10xx mainline NULL-derefs ADD_DEV); refusing to start"
        ))),
    }
}

fn guard_inner() -> Result<()> {
    let ctrl = UblkCtrlBuilder::default()
        .name("tikoblk-guard")
        .id(-1) // driver allocates
        .nr_queues(1)
        .depth(4)
        .io_buf_bytes(512 << 10)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()?;
    let id = ctrl.dev_info().dev_id;
    // A working driver answers ADD_DEV with a real id; against a non-ublk
    // fd libublk can "succeed" without the driver answering (id stays -1).
    if id == u32::MAX {
        return Err(Error::Ublk(
            "ADD_DEV did not allocate a device id (control device is not a working ublk)".into(),
        ));
    }
    // ... and the char device must actually appear in sysfs.
    let mainline = format!("/sys/class/ublk-char/ublkc{id}");
    let ublk2 = format!("/sys/class/ublk2-char/ublk2c{id}");
    if !std::path::Path::new(&mainline).exists() && !std::path::Path::new(&ublk2).exists() {
        return Err(Error::Ublk(format!(
            "device {id} did not appear in sysfs after ADD_DEV"
        )));
    }
    // The device was never started (no daemon); kill+del removes it cleanly.
    ctrl.kill_dev()?;
    ctrl.del_dev()?;
    drop_node_links(id);
    tracing::info!(dev_id = id, "ublk control device smoke test passed");
    Ok(())
}

/// Path of the block device node callers should use (the libublk name,
/// which [`ensure_node_links`] bridges to the real node).
pub fn bdev_path(dev_id: u32) -> String {
    format!("/dev/ublkb{dev_id}")
}

/// Relax the block node mode so unprivileged consumers (an unprivileged
/// tikovm-hostd / Firecracker) can open the drive. Dev-host default; a
/// production deployment should use a dedicated group/udev rule instead
/// (tracked as a Phase 5 hardening item).
pub fn relax_bdev_perms(dev_id: u32) {
    use std::os::unix::fs::PermissionsExt;
    let path = bdev_path(dev_id);
    if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)) {
        tracing::warn!(dev_id, error = %e, "could not chmod block device node");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "needs a live ublk control device; run explicitly as root"]
    fn probe_missing_device_codes() {
        let r = super::probe(4242);
        eprintln!("probe(4242) = {r:?}");
        let e = libublk::ctrl::UblkCtrl::new_simple(4242).err();
        eprintln!("new_simple(4242) err = {e:?}");
    }

    #[test]
    fn guard_verdict_mapping() {
        assert!(super::guard_verdict(Some(Ok(()))).is_ok());
        let e = super::guard_verdict(Some(Err(crate::Error::Ublk("boom".into()))))
            .unwrap_err();
        assert!(e.to_string().contains("boom"));
        let e = super::guard_verdict(None).unwrap_err();
        assert!(e.to_string().contains("timed out"));
        assert!(e.to_string().contains("refusing to start"));
    }
}
