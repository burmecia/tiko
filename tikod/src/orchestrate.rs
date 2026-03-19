//! Recovery orchestration — cold-start, stop, freeze/thaw, and PITR restore.
//!
//! The same prepare_recovery + PG crash-recovery + post_recovery_cleanup sequence
//! is reused for cold-start (§12), PITR restore (§3), and new branch provisioning (§2).

use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use pgsys::Lsn;
use store::manifest::Manifest;
use store::project::{ProjectMeta, ProjectNamespace};
use store::sim_store::SimStore;

use crate::compute::ComputeBackend;
use crate::lease::{self, LEASE_TTL_SECS};
use crate::pitr;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Error {
    /// No delta manifests found; cannot determine the latest checkpoint.
    NoCheckpoint,
    /// PG did not exit within the allotted recovery timeout.
    RecoveryTimeout,
    /// A shell command returned a non-zero exit or failed to spawn.
    Compute(String),
    /// SimStore / local I/O error.
    Store(io::Error),
    /// Lease acquisition or renewal error.
    Lease(lease::LeaseError),
    /// All other error conditions.
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoCheckpoint => write!(f, "no checkpoint found for project"),
            Error::RecoveryTimeout => write!(f, "timed out waiting for recovery shutdown"),
            Error::Compute(s) => write!(f, "compute error: {s}"),
            Error::Store(e) => write!(f, "store error: {e}"),
            Error::Lease(e) => write!(f, "lease error: {e}"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Store(e)
    }
}

impl From<lease::LeaseError> for Error {
    fn from(e: lease::LeaseError) -> Self {
        Error::Lease(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Public API ────────────────────────────────────────────────────────────────

/// Cold-start a project (§12 full sequence).
///
/// 1. Acquire lease
/// 2. Find latest `(timeline, lsn)` from delta manifests
/// 3. Extract pre-packaged PGDATA skeleton
/// 4. Prepare crash recovery: pg_state + recovery_manifest.bin + postgresql.conf
/// 5. Start PG in recovery mode; wait for it to shut down at `target_lsn`
/// 6. `post_recovery_cleanup`: upload merged manifest, clean recovery files
/// 7. Start PG in normal mode
/// 8. Mark project active
pub fn start(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    skeleton_path: &Path,
    server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    lease::acquire(
        sim,
        &lease::lease_key(ns.org_id, ns.project_id),
        server_id,
        LEASE_TTL_SECS,
    )?;
    run_cold_start(
        sim,
        ns,
        pgdata,
        tiko_root,
        skeleton_path,
        server_id,
        backend,
    )
}

/// Graceful stop: CHECKPOINT → pg_ctl stop → release lease → mark stopped.
pub fn stop(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    _server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    // Best-effort CHECKPOINT; ignore errors (PG may already be in a bad state).
    let _ = backend.execute(&format!("psql -h /tmp -d postgres -c CHECKPOINT"));

    backend
        .execute(&format!("pg_ctl stop -D {} -m fast", pgdata.display()))
        .map_err(Error::Compute)?;

    lease::release(sim, &lease::lease_key(ns.org_id, ns.project_id))?;
    update_project_status(sim, ns, "stopped", None)?;
    Ok(())
}

/// Suspend: CHECKPOINT → Firecracker pause + snapshot → mark frozen.
///
/// The lease is **not** released. The tikod daemon renews it via
/// `renew_held_leases` (every 15 s) while the VM is paused.
/// If `snapshot_path` is `None`, falls back to a graceful `stop`.
pub fn freeze(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    snapshot_path: Option<&Path>,
    server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    let Some(snap) = snapshot_path else {
        // No snapshot target — fall back to a clean stop.
        return stop(sim, ns, pgdata, server_id, backend);
    };

    let _ = backend.execute(&format!("psql -h /tmp -d postgres -c CHECKPOINT"));

    backend.freeze(snap).map_err(Error::Compute)?;

    // TODO: store snapshot_path and snapshot_taken_at in ProjectMeta once the
    // field is added to the model.
    update_project_status(sim, ns, "frozen", Some(server_id))?;
    Ok(())
}

/// Wake from snapshot (~50 ms) or fall back to cold-start if snapshot unavailable.
///
/// The fallback is transparent: callers receive the same `Ok(())` either way.
pub fn thaw(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    snapshot_path: Option<&Path>,
    skeleton_path: &Path,
    server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    lease::acquire(
        sim,
        &lease::lease_key(ns.org_id, ns.project_id),
        server_id,
        LEASE_TTL_SECS,
    )?;

    // Attempt fast path: resume from local snapshot.
    let snap_available = snapshot_path.is_some_and(|p| p.exists());
    if let Some(snap) = snapshot_path.filter(|_| snap_available) {
        match backend.thaw_from_snapshot(snap) {
            Ok(()) => {
                update_project_status(sim, ns, "active", Some(server_id))?;
                return Ok(());
            }
            Err(e) => {
                eprintln!("thaw_from_snapshot failed ({e}); falling back to cold-start");
                // Lease is already held — skip re-acquire in cold-start path.
            }
        }
    }

    // Slow path: cold-start (lease already acquired above).
    run_cold_start(
        sim,
        ns,
        pgdata,
        tiko_root,
        skeleton_path,
        server_id,
        backend,
    )
}

/// PITR restore: stop PG → `run_recovery` → mark active.
pub fn restore(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    target_tl: u32,
    target_lsn: Lsn,
    server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    if backend.is_running(&pgdata.to_string_lossy()) {
        let _ = backend.execute(&format!("psql -h /tmp -d postgres -c CHECKPOINT"));
        backend
            .execute(&format!("pg_ctl stop -D {} -m fast", pgdata.display()))
            .map_err(Error::Compute)?;
    }

    run_recovery(
        sim,
        ns,
        pgdata,
        tiko_root,
        target_tl,
        target_lsn,
        server_id,
        backend,
        Duration::from_secs(300),
    )
}

/// Core recovery sequence: prepare → crash-recovery pass → cleanup → normal start.
///
/// Called by both `run_cold_start` (after skeleton extract) and `restore`
/// (after stopping a running PG).  The caller supplies `target_tl`/`target_lsn`
/// and the recovery `timeout`.
pub fn run_recovery(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    target_tl: u32,
    target_lsn: Lsn,
    server_id: &str,
    backend: &dyn ComputeBackend,
    timeout: Duration,
) -> Result<()> {
    pitr::prepare_recovery(sim, ns, pgdata, tiko_root, target_tl, target_lsn)
        .map_err(|e| Error::Other(e.to_string()))?;

    backend
        .execute(&format!("pg_ctl start -D {}", pgdata.display()))
        .map_err(Error::Compute)?;

    wait_for_recovery_shutdown(pgdata, backend, timeout)?;

    let new_tl = read_timeline_from_pgcontroldata(pgdata)?;
    post_recovery_cleanup(sim, ns, pgdata, tiko_root, target_lsn, new_tl)?;

    backend
        .execute(&format!("pg_ctl start -D {}", pgdata.display()))
        .map_err(Error::Compute)?;

    update_project_status(sim, ns, "active", Some(server_id))?;
    Ok(())
}

/// Upload merged manifest as the new base for `new_tl`, update `current_timeline_id`
/// in `project.json`, and remove local recovery files.
///
/// `new_tl` is the timeline written by PG after the recovery pass completes
/// (read via `pg_controldata` by the caller).  Passing it explicitly makes
/// this function fully testable without a real PGDATA.
pub fn post_recovery_cleanup(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    target_lsn: Lsn,
    new_tl: u32,
) -> Result<()> {
    // ── Upload recovery_manifest.bin as the initial base for the new timeline ─
    let manifest_path = Manifest::local_manifest_path(tiko_root);
    if manifest_path.exists() {
        let manifest = Manifest::open(&manifest_path).map_err(Error::Store)?;
        let bytes = manifest.to_bytes().map_err(Error::Store)?;
        sim.put_standard(&ns.base_manifest_key(new_tl, target_lsn), &bytes)?;
    }

    // ── Update project.json: persist new timeline ─────────────────────────────
    set_project_timeline(sim, ns, new_tl)?;

    // ── Remove recovery files from PGDATA ─────────────────────────────────────
    let _ = fs::remove_file(&manifest_path);
    remove_recovery_conf_entries(&pgdata.join("postgresql.conf"))?;
    let _ = fs::remove_file(pgdata.join("recovery.signal"));

    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Cold-start sequence after lease has already been acquired.
fn run_cold_start(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    skeleton_path: &Path,
    server_id: &str,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    let (target_tl, target_lsn) = latest_checkpoint(sim, ns)?;

    extract_skeleton(pgdata, skeleton_path, backend)?;

    // TODO(pre-warm): background S3 GETs for catalog chunks (base/1/, base/4/,
    // global/) directly into the shared-memory chunk cache (s3worker/src/cache.rs).
    // Skipped here: tikod runs as a separate process and cannot write to s3worker's
    // shared-memory cache directly. This optimisation requires an IPC channel or
    // running the pre-warm inside the s3worker startup path.

    run_recovery(
        sim,
        ns,
        pgdata,
        tiko_root,
        target_tl,
        target_lsn,
        server_id,
        backend,
        Duration::from_secs(120),
    )
}

/// Find the latest `(timeline_id, lsn)` across all delta manifests.
///
/// Key format under `delta_prefix()`:
/// `{org}/pitr/{proj}/deltas/{tl:08X}/{lsn_hex}/manifest.bin`
/// After stripping the prefix: `{tl:08X}/{lsn_hex}/manifest.bin`
pub fn latest_checkpoint(sim: &SimStore, ns: &ProjectNamespace) -> Result<(u32, Lsn)> {
    let prefix = ns.delta_prefix();
    let keys = sim.list_prefix_standard(&prefix)?;

    let mut best: Option<(u32, Lsn)> = None;
    for key in &keys {
        let rel = key.strip_prefix(&prefix).unwrap_or(key.as_str());
        // rel = "{tl:08X}/{lsn_hex}/manifest.bin"
        let mut parts = rel.splitn(3, '/');
        let (Some(tl_hex), Some(lsn_hex)) = (parts.next(), parts.next()) else {
            continue;
        };
        let (Ok(tl), Ok(lsn)) = (u32::from_str_radix(tl_hex, 16), Lsn::from_hex(lsn_hex)) else {
            continue;
        };
        let better = best.map_or(true, |(_, prev)| lsn > prev);
        if better {
            best = Some((tl, lsn));
        }
    }

    best.ok_or(Error::NoCheckpoint)
}

/// Untar the PGDATA skeleton tarball into `pgdata`.
fn extract_skeleton(
    pgdata: &Path,
    skeleton_path: &Path,
    backend: &dyn ComputeBackend,
) -> Result<()> {
    fs::create_dir_all(pgdata)?;
    backend
        .execute(&format!(
            "tar -xf {} -C {}",
            skeleton_path.display(),
            pgdata.display()
        ))
        .map_err(Error::Compute)?;
    Ok(())
}

/// Append tikod recovery settings to `postgresql.conf`.
pub fn append_recovery_conf(conf_path: &Path, target_lsn: Lsn) -> Result<()> {
    let snippet = format!(
        "\n# tikod recovery settings — removed by post_recovery_cleanup\n\
         restore_command = 'tiko_restore %f %p'\n\
         recovery_target_lsn = '{}'\n\
         recovery_target_action = 'shutdown'\n",
        target_lsn.to_pg_string()
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}

/// Remove tikod-appended recovery lines from `postgresql.conf`.
pub fn remove_recovery_conf_entries(conf_path: &Path) -> Result<()> {
    let Ok(content) = fs::read_to_string(conf_path) else {
        return Ok(());
    };
    let filtered: String = content
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with("restore_command")
                && !t.starts_with("recovery_target_lsn")
                && !t.starts_with("recovery_target_action")
                && !t.starts_with("# tikod recovery settings")
        })
        .map(|l| format!("{l}\n"))
        .collect();
    fs::write(conf_path, filtered)?;
    Ok(())
}

/// Poll `pg_ctl status` until PG is no longer running or `timeout` elapses.
fn wait_for_recovery_shutdown(
    pgdata: &Path,
    backend: &dyn ComputeBackend,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if !backend.is_running(&pgdata.to_string_lossy()) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::RecoveryTimeout);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Parse the active `TimeLineID` from `pg_controldata -D <pgdata>` output.
///
/// PG writes the new TLI to pg_control when it promotes after PITR recovery.
fn read_timeline_from_pgcontroldata(pgdata: &Path) -> Result<u32> {
    let out = std::process::Command::new("pg_controldata")
        .arg("-D")
        .arg(pgdata)
        .output()
        .map_err(|e| Error::Other(format!("pg_controldata failed to spawn: {e}")))?;

    let text = String::from_utf8_lossy(&out.stdout);
    for line in text.lines() {
        // Both "Latest checkpoint's TimeLineID" and "Latest checkpoint TimeLineID"
        // appear in different PG versions; match the common suffix.
        if line.contains("TimeLineID") && !line.contains("Prior") {
            if let Some(val) = line.split(':').nth(1) {
                if let Ok(tl) = val.trim().parse::<u32>() {
                    return Ok(tl);
                }
            }
        }
    }
    Err(Error::Other(
        "could not parse TimeLineID from pg_controldata output".into(),
    ))
}

/// Update `current_timeline_id` in `project.json`.
fn set_project_timeline(sim: &SimStore, ns: &ProjectNamespace, new_tl: u32) -> Result<()> {
    let key = ns.project_meta_key();
    let bytes = sim
        .get_standard(&key)?
        .ok_or_else(|| Error::Other("project.json not found".into()))?;
    let mut meta: ProjectMeta =
        serde_json::from_slice(&bytes).map_err(|e| Error::Other(e.to_string()))?;
    meta.current_timeline_id = new_tl;
    let json = serde_json::to_vec(&meta).map_err(|e| Error::Other(e.to_string()))?;
    sim.put_standard(&key, &json)?;
    Ok(())
}

/// Update `status` (and optionally `current_server_id`) in `project.json`.
///
/// NOTE: `current_server_id` is not yet a field on `ProjectMeta`; it will be
/// added when the model is extended.  For now only `status` is persisted.
fn update_project_status(
    sim: &SimStore,
    ns: &ProjectNamespace,
    status: &str,
    _server_id: Option<&str>,
) -> Result<()> {
    let key = ns.project_meta_key();
    let bytes = sim
        .get_standard(&key)?
        .ok_or_else(|| Error::Other("project.json not found".into()))?;
    let mut meta: ProjectMeta =
        serde_json::from_slice(&bytes).map_err(|e| Error::Other(e.to_string()))?;
    meta.status = status.to_owned();
    let json = serde_json::to_vec(&meta).map_err(|e| Error::Other(e.to_string()))?;
    sim.put_standard(&key, &json)?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use store::manifest::Manifest;
    use store::project::ProjectMeta;
    use tempfile::TempDir;

    fn temp_sim() -> (SimStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        (sim, dir)
    }

    fn root_ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 1, 1)
    }

    // ── latest_checkpoint ─────────────────────────────────────────────────────

    #[test]
    fn latest_checkpoint_returns_max_lsn() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // Write three delta manifest stubs at different LSNs on timeline 1.
        for lsn_val in [0x1000u64, 0x3000, 0x2000] {
            let lsn = Lsn::new(lsn_val);
            let key = ns.delta_manifest_key(1, lsn);
            sim.put_standard(&key, b"stub").unwrap();
        }

        let (tl, lsn) = latest_checkpoint(&sim, &ns).unwrap();
        assert_eq!(tl, 1);
        assert_eq!(lsn, Lsn::new(0x3000));
    }

    #[test]
    fn latest_checkpoint_err_when_no_deltas() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        let err = latest_checkpoint(&sim, &ns).unwrap_err();
        assert!(matches!(err, Error::NoCheckpoint));
    }

    #[test]
    fn latest_checkpoint_picks_max_across_timelines() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        // TL 1 has the lower LSN, TL 2 has the higher one.
        sim.put_standard(&ns.delta_manifest_key(1, Lsn::new(0x1000)), b"stub")
            .unwrap();
        sim.put_standard(&ns.delta_manifest_key(2, Lsn::new(0x5000)), b"stub")
            .unwrap();

        let (tl, lsn) = latest_checkpoint(&sim, &ns).unwrap();
        assert_eq!(tl, 2);
        assert_eq!(lsn, Lsn::new(0x5000));
    }

    // ── append / remove recovery conf ─────────────────────────────────────────

    #[test]
    fn append_and_remove_recovery_conf_round_trip() {
        let dir = TempDir::new().unwrap();
        let conf = dir.path().join("postgresql.conf");
        fs::write(&conf, "# existing setting\nmax_connections = 100\n").unwrap();

        append_recovery_conf(&conf, Lsn::new(0x3000000)).unwrap();
        let after = fs::read_to_string(&conf).unwrap();
        assert!(after.contains("restore_command"));
        assert!(after.contains("recovery_target_lsn = '0/3000000'"));
        assert!(after.contains("recovery_target_action = 'shutdown'"));
        assert!(
            after.contains("max_connections = 100"),
            "existing lines preserved"
        );

        remove_recovery_conf_entries(&conf).unwrap();
        let cleaned = fs::read_to_string(&conf).unwrap();
        assert!(!cleaned.contains("restore_command"));
        assert!(!cleaned.contains("recovery_target_lsn"));
        assert!(!cleaned.contains("recovery_target_action"));
        assert!(
            cleaned.contains("max_connections = 100"),
            "other lines preserved"
        );
    }

    #[test]
    fn remove_recovery_conf_is_noop_on_missing_file() {
        let dir = TempDir::new().unwrap();
        // Should not return an error even if the file doesn't exist.
        remove_recovery_conf_entries(&dir.path().join("postgresql.conf")).unwrap();
    }

    // ── wait_for_recovery_shutdown ────────────────────────────────────────────

    struct MockBackend {
        running: bool,
    }

    impl ComputeBackend for MockBackend {
        fn execute(&self, _cmd: &str) -> std::result::Result<String, String> {
            Ok(String::new())
        }
        fn is_running(&self, _pgdata: &str) -> bool {
            self.running
        }
        fn freeze(&self, _snap: &Path) -> std::result::Result<(), String> {
            Err("unsupported".into())
        }
        fn thaw_from_snapshot(&self, _snap: &Path) -> std::result::Result<(), String> {
            Err("unsupported".into())
        }
    }

    #[test]
    fn wait_for_recovery_shutdown_ok_when_already_stopped() {
        let dir = TempDir::new().unwrap();
        let backend = MockBackend { running: false };
        wait_for_recovery_shutdown(dir.path(), &backend, Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wait_for_recovery_shutdown_times_out() {
        let dir = TempDir::new().unwrap();
        let backend = MockBackend { running: true };
        let err = wait_for_recovery_shutdown(dir.path(), &backend, Duration::from_millis(50))
            .unwrap_err();
        assert!(matches!(err, Error::RecoveryTimeout));
    }

    // ── update_project_status ─────────────────────────────────────────────────

    #[test]
    fn update_project_status_persists_status() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        update_project_status(&sim, &ns, "stopped", None).unwrap();

        let key = ns.project_meta_key();
        let bytes = sim.get_standard(&key).unwrap().unwrap();
        let meta: ProjectMeta = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta.status, "stopped");
    }

    // ── set_project_timeline ──────────────────────────────────────────────────

    #[test]
    fn set_project_timeline_updates_timeline_id() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        set_project_timeline(&sim, &ns, 3).unwrap();

        let key = ns.project_meta_key();
        let bytes = sim.get_standard(&key).unwrap().unwrap();
        let meta: ProjectMeta = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta.current_timeline_id, 3);
    }

    // ── post_recovery_cleanup ─────────────────────────────────────────────────

    fn write_local_manifest(tiko_root: &Path, lsn: Lsn) {
        let manifest_path = Manifest::local_manifest_path(tiko_root);
        Manifest::new(lsn, 0, vec![], HashMap::new(), &manifest_path).unwrap();
    }

    #[test]
    fn post_recovery_cleanup_uploads_manifest_as_new_base() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        let pgdata = TempDir::new().unwrap();
        let target_lsn = Lsn::new(0x3000);
        write_local_manifest(pgdata.path(), target_lsn);
        fs::write(pgdata.path().join("postgresql.conf"), "").unwrap();

        post_recovery_cleanup(&sim, &ns, pgdata.path(), pgdata.path(), target_lsn, 2).unwrap();

        // Manifest must be stored under bases/{new_tl}/{lsn}/manifest.bin.
        let key = ns.base_manifest_key(2, target_lsn);
        assert!(
            sim.get_standard(&key).unwrap().is_some(),
            "manifest must be uploaded under new timeline base key"
        );
    }

    #[test]
    fn post_recovery_cleanup_updates_current_timeline_id() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        let pgdata = TempDir::new().unwrap();
        fs::write(pgdata.path().join("postgresql.conf"), "").unwrap();

        post_recovery_cleanup(&sim, &ns, pgdata.path(), pgdata.path(), Lsn::new(0x3000), 5)
            .unwrap();

        let bytes = sim.get_standard(&ns.project_meta_key()).unwrap().unwrap();
        let meta: ProjectMeta = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta.current_timeline_id, 5);
    }

    #[test]
    fn post_recovery_cleanup_removes_recovery_files() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        ProjectMeta::ensure_root(&sim, &ns).unwrap();

        let pgdata = TempDir::new().unwrap();
        let pgdata_path = pgdata.path();
        let target_lsn = Lsn::new(0x3000);

        write_local_manifest(pgdata_path, target_lsn);
        fs::write(pgdata_path.join("recovery.signal"), b"").unwrap();
        let conf = pgdata_path.join("postgresql.conf");
        fs::write(&conf, "max_connections = 100\n").unwrap();
        append_recovery_conf(&conf, target_lsn).unwrap();

        post_recovery_cleanup(&sim, &ns, pgdata_path, pgdata_path, target_lsn, 1).unwrap();

        assert!(
            !pgdata_path.join("recovery.signal").exists(),
            "recovery.signal must be removed"
        );
        assert!(
            !Manifest::local_manifest_path(pgdata_path).exists(),
            "local manifest must be removed"
        );
        let conf_text = fs::read_to_string(&conf).unwrap();
        assert!(
            !conf_text.contains("recovery_target_lsn"),
            "recovery conf entries must be stripped"
        );
        assert!(
            conf_text.contains("max_connections"),
            "non-recovery conf lines must be preserved"
        );
    }
}
