//! tikoblkd — host block-storage daemon.
//!
//! Usage:
//!   tikoblkd [--ctrl PATH] [--data-dir PATH] [--sock PATH] [--foreground]
//!
//! Defaults: `--ctrl /dev/ublk2-control` if it exists else
//! `/dev/ublk-control`, `--data-dir /var/lib/tikoblk`,
//! `--sock /run/tikoblk/daemon.sock`.
//!
//! Startup sequence: unshare a private mount namespace and bind-mount the
//! given ublk control device over `/dev/ublk-control` (libublk hardcodes
//! that path; on hosts where mainline `ublk_drv` is broken the known-good
//! `ublk2_drv` provides `/dev/ublk2-control`), load the registry, run the
//! recovery sweep, then serve the control API. SIGTERM exits immediately:
//! ublk devices were created with USER_RECOVERY, so the kernel quiesces
//! them and the next daemon start reattaches transparently. Never SIGKILL
//! this daemon with I/O in flight — it can wedge in D-state until reboot.

use std::ffi::CString;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tikoblk::control;
use tikoblk::device;
use tikoblk::volume::VolumeManager;

const CTRL_PATH: &str = "/dev/ublk-control";

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

struct Args {
    ctrl: PathBuf,
    data_dir: PathBuf,
    sock: PathBuf,
    store_root: PathBuf,
    cache_mb: u64,
    gc_interval_secs: u64,
    gc_grace_secs: u64,
    #[allow(dead_code)]
    foreground: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: tikoblkd [--ctrl PATH] [--data-dir PATH] [--sock PATH]\n\
         \x20       [--store-root PATH] [--cache-mb N] [--gc-interval-secs N]\n\
         \x20       [--gc-grace-secs N] [--foreground]\n\
         defaults: --ctrl /dev/ublk2-control (if present, else /dev/ublk-control)\n\
         \x20         --data-dir /var/lib/tikoblk --sock /run/tikoblk/daemon.sock\n\
         \x20         --store-root /mnt/s3files/tikoblk --cache-mb 512\n\
         \x20         --gc-interval-secs 3600 (0 disables) --gc-grace-secs 600"
    );
    std::process::exit(2);
}

fn default_ctrl() -> PathBuf {
    if Path::new("/dev/ublk2-control").exists() {
        PathBuf::from("/dev/ublk2-control")
    } else {
        PathBuf::from(CTRL_PATH)
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        ctrl: default_ctrl(),
        data_dir: PathBuf::from("/var/lib/tikoblk"),
        sock: PathBuf::from("/run/tikoblk/daemon.sock"),
        store_root: PathBuf::from("/mnt/s3files/tikoblk"),
        cache_mb: 512,
        gc_interval_secs: 3600,
        gc_grace_secs: 600,
        foreground: false,
    };
    let mut it = std::env::args().skip(1);
    fn take_u64(it: &mut std::iter::Skip<std::env::Args>) -> u64 {
        it.next()
            .unwrap_or_else(|| usage())
            .parse()
            .unwrap_or_else(|_| usage())
    }
    while let Some(a) = it.next() {
        match a.as_str() {
            "--ctrl" => args.ctrl = PathBuf::from(it.next().unwrap_or_else(|| usage())),
            "--data-dir" => args.data_dir = PathBuf::from(it.next().unwrap_or_else(|| usage())),
            "--sock" => args.sock = PathBuf::from(it.next().unwrap_or_else(|| usage())),
            "--store-root" => {
                args.store_root = PathBuf::from(it.next().unwrap_or_else(|| usage()))
            }
            "--cache-mb" => args.cache_mb = take_u64(&mut it),
            "--gc-interval-secs" => args.gc_interval_secs = take_u64(&mut it),
            "--gc-grace-secs" => args.gc_grace_secs = take_u64(&mut it),
            "--foreground" => args.foreground = true,
            "-h" | "--help" => usage(),
            _ => usage(),
        }
    }
    args
}

/// Unshare a private mount namespace and make libublk's hardcoded
/// `/dev/ublk-control` refer to `ctrl`. Verified trick from the ublk
/// investigation: `unshare(CLONE_NEWNS)` + `mount --bind ctrl
/// /dev/ublk-control` requires zero libublk changes.
fn setup_mount_ns(ctrl: &Path) -> tikoblk::Result<()> {
    use std::io::Error as IoErr;

    let ctrl_md = std::fs::metadata(ctrl).map_err(|e| {
        tikoblk::Error::Ublk(format!(
            "ctrl device {}: {e} (is a ublk module loaded?)",
            ctrl.display()
        ))
    })?;
    let want_rdev = ctrl_md.rdev();

    // SAFETY: single-threaded at this point; standard mount-namespace setup.
    unsafe {
        if libc::unshare(libc::CLONE_NEWNS) != 0 {
            return Err(IoErr::last_os_error().into());
        }
        if libc::mount(
            std::ptr::null(),
            c"/".as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        ) != 0
        {
            return Err(IoErr::last_os_error().into());
        }
    }

    match std::fs::metadata(CTRL_PATH) {
        Ok(md) if md.rdev() == want_rdev => {
            tracing::debug!(ctrl = %ctrl.display(), "control node already correct");
        }
        Ok(_) => {
            // Exists but is the wrong device (e.g. broken mainline): bind the
            // good control node over it inside our private namespace.
            bind_mount(ctrl, Path::new(CTRL_PATH))?;
        }
        Err(_) => {
            // Missing entirely: create a node with the right rdev (only
            // visible inside this namespace).
            let c = c_path(CTRL_PATH);
            // SAFETY: creating a char device node in our private /dev.
            let rc = unsafe { libc::mknod(c.as_ptr(), libc::S_IFCHR | 0o600, want_rdev) };
            if rc != 0 {
                return Err(IoErr::last_os_error().into());
            }
        }
    }

    // Verify the effective control node is the one we were asked to use.
    let md = std::fs::metadata(CTRL_PATH)?;
    if md.rdev() != want_rdev {
        return Err(tikoblk::Error::Ublk(format!(
            "{CTRL_PATH} has rdev {:#x}, want {:#x} ({})",
            md.rdev(),
            want_rdev,
            ctrl.display()
        )));
    }
    Ok(())
}

fn bind_mount(src: &Path, target: &Path) -> tikoblk::Result<()> {
    let c_src = c_path(src);
    let c_tgt = c_path(target);
    // SAFETY: standard bind mount of two validated paths.
    let rc = unsafe {
        libc::mount(
            c_src.as_ptr(),
            c_tgt.as_ptr(),
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn c_path(p: impl AsRef<std::ffi::OsStr>) -> CString {
    CString::new(p.as_ref().as_encoded_bytes()).expect("path contains NUL")
}

fn install_signal_handlers() -> tikoblk::Result<()> {
    // SAFETY: registering a trivial async-signal-safe handler (sets an
    // atomic). No SA_RESTART so accept(2) returns EINTR on shutdown.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_signal as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        for sig in [libc::SIGTERM, libc::SIGINT] {
            if libc::sigaction(sig, &sa, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    Ok(())
}

fn run() -> tikoblk::Result<()> {
    let args = parse_args();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if unsafe { libc::geteuid() } != 0 {
        return Err(tikoblk::Error::Ublk("tikoblkd must run as root".into()));
    }

    // Capture the host /dev BEFORE unsharing: node-link symlinks must land
    // in the host namespace so other processes (mkfs, mount, Firecracker)
    // can use /dev/ublkbN.
    let dev_dir = std::fs::File::open("/dev")?;
    device::set_host_dev_dir(dev_dir.as_raw_fd());
    std::mem::forget(dev_dir); // keep the fd for the process lifetime

    setup_mount_ns(&args.ctrl)?;
    tracing::info!(ctrl = %args.ctrl.display(), "ublk control device ready");

    // Refuse to start on a broken driver (broken mainline oopses ADD_DEV and
    // poisons all later control ops until reboot).
    device::smoke_test_control_device()?;

    if let Some(parent) = args.sock.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(&args.sock); // stale socket from a prior run

    let mgr = Arc::new(VolumeManager::new(
        &args.data_dir,
        &args.store_root,
        &tikoblk::volume::ManagerOpts {
            cache_bytes: args.cache_mb << 20,
            gc_interval_secs: args.gc_interval_secs,
            gc_grace_secs: args.gc_grace_secs,
        },
    )?);
    mgr.recover_attached();

    install_signal_handlers()?;
    let listener = UnixListener::bind(&args.sock)?;
    // Host-local consumers (tikovm-hostd runs unprivileged) need access.
    // The API is unauthenticated by design (host-local UDS only).
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&args.sock, std::fs::Permissions::from_mode(0o666))?;
    tracing::info!(sock = %args.sock.display(), data_dir = %args.data_dir.display(),
        store_root = %args.store_root.display(), cache_mb = args.cache_mb, "tikoblkd ready");

    let shutdown = Arc::new(AtomicBool::new(false));
    // Bridge the process-wide static to the Arc the server watches (the
    // signal handler may run on any thread).
    {
        let shutdown = shutdown.clone();
        std::thread::spawn(move || {
            while !SHUTDOWN.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    control::serve(listener, mgr, shutdown);

    // Devices are NOT torn down here: process exit closes the cdev fds and
    // the kernel quiesces them (USER_RECOVERY); the next start reattaches.
    tracing::info!("shutdown: exiting, ublk devices quiesce for recovery");
    std::process::exit(0);
}

fn main() {
    if let Err(e) = run() {
        eprintln!("tikoblkd: {e}");
        std::process::exit(1);
    }
}
