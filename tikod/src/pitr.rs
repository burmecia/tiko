//! PITR preparation — list restore points, prepare PGDATA for crash recovery.
//!
//! `list_restore_points` scans all delta manifests and extracts the embedded
//! checkpoint timestamp from each one.
//!
//! `prepare_recovery` validates a target `(timeline_id, lsn)`, merges the
//! appropriate base and delta manifests into `recovery_manifest.bin`, extracts
//! `pg_state.tar.zst` into PGDATA, and writes recovery settings to
//! `postgresql.conf`.  It is called by `orchestrate::start` (cold-start path)
//! and `orchestrate::restore` (explicit PITR path).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use pgsys::Lsn;
use serde::{Deserialize, Serialize};
use store::manifest::Manifest;
use store::project::ProjectNamespace;
use store::sim_store::SimStore;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Error {
    Store(io::Error),
    /// The requested `(timeline_id, lsn)` has no delta manifest.
    TargetNotFound(String),
    /// No base manifest exists with `base_lsn <= target_lsn`.
    NoCheckpoint,
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(e) => write!(f, "store error: {e}"),
            Error::TargetNotFound(s) => write!(f, "target delta not found: {s}"),
            Error::NoCheckpoint => write!(f, "no base manifest covers target_lsn"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Store(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Public types ──────────────────────────────────────────────────────────────

/// A single recoverable state derived from one delta manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestorePoint {
    pub timeline_id: u32,
    pub lsn: Lsn,
    /// Unix timestamp (seconds) embedded in the manifest header.
    pub timestamp: i64,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// List all available restore points for `ns` across all timelines.
///
/// Scans `{org}/pitr/{proj}/deltas/` in the standard bucket.  Every
/// `*/manifest.bin` entry is a restore point; the manifest is fetched to
/// read its embedded checkpoint timestamp.
///
/// The returned list is sorted by `(timeline_id, lsn)`.
pub fn list_restore_points(sim: &SimStore, ns: &ProjectNamespace) -> Result<Vec<RestorePoint>> {
    let prefix = ns.delta_prefix();
    let keys = sim.list_prefix_standard(&prefix).map_err(Error::Store)?;

    let mut points = Vec::new();

    for key in &keys {
        if !key.ends_with("/manifest.bin") {
            continue;
        }
        let rel = key.strip_prefix(&prefix).unwrap_or(key.as_str());
        // rel = "{tl:08X}/{lsn_hex}/manifest.bin"
        let mut parts = rel.splitn(3, '/');
        let (Some(tl_hex), Some(lsn_hex)) = (parts.next(), parts.next()) else {
            continue;
        };
        let (Ok(tl), Ok(lsn)) = (u32::from_str_radix(tl_hex, 16), Lsn::from_hex(lsn_hex)) else {
            continue;
        };
        let Some(bytes) = sim.get_standard(key).map_err(Error::Store)? else {
            continue;
        };
        // Use a unique temp dir per manifest to avoid file-name collisions when
        // called from parallel tests (or concurrent requests).
        let tmp_dir = tempfile::TempDir::new().map_err(Error::Store)?;
        let tmp = tmp_dir.path().join("manifest.tikm");
        let ts = match Manifest::from_bytes(&bytes, &tmp) {
            Ok(m) => m.timestamp(),
            Err(_) => continue,
        };
        points.push(RestorePoint {
            timeline_id: tl,
            lsn,
            timestamp: ts,
        });
    }

    points.sort_by_key(|p| (p.timeline_id, p.lsn));
    Ok(points)
}

/// Prepare PGDATA for crash recovery to `(target_tl, target_lsn)`.
///
/// Steps:
/// 1. Validate `deltas/{target_tl}/{target_lsn}/manifest.bin` exists.
/// 2. Download and extract `pg_state.tar.zst` (pg_control, transaction logs,
///    pg_filenode.map) into PGDATA.
/// 3. Build `recovery_manifest.bin` — newest base with `base_lsn <= target_lsn`
///    merged with all deltas in `(base_lsn, target_lsn]`.
/// 4. Append recovery settings to `postgresql.conf`.
/// 5. Touch `recovery.signal`.
///
/// `tiko_root` is the Tiko local root directory (`$TIKO_LOCAL_ROOT`) where Tiko
/// files (manifest, temp archives) are written.  Pass a `TempDir` in tests.
pub fn prepare_recovery(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
    tiko_root: &Path,
    target_tl: u32,
    target_lsn: Lsn,
) -> Result<()> {
    // ── 0. Validate target ────────────────────────────────────────────────────
    let delta_key = ns.delta_manifest_key(target_tl, target_lsn);
    if sim
        .get_standard(&delta_key)
        .map_err(Error::Store)?
        .is_none()
    {
        return Err(Error::TargetNotFound(delta_key));
    }

    fs::create_dir_all(tiko_root)?;

    // ── 1. pg_state ───────────────────────────────────────────────────────────
    let pg_state_key = ns.pg_state_key(target_tl, target_lsn);
    let pg_state_bytes = sim
        .get_standard(&pg_state_key)
        .map_err(Error::Store)?
        .ok_or_else(|| Error::Other(format!("pg_state not found: {pg_state_key}")))?;

    let pg_state_tmp = tiko_root.join("pg_state.tar.zst");
    fs::write(&pg_state_tmp, &pg_state_bytes)?;

    let status = std::process::Command::new("tar")
        .args([
            "-xf",
            &pg_state_tmp.to_string_lossy(),
            "-C",
            &pgdata.to_string_lossy(),
        ])
        .status()
        .map_err(|e| Error::Other(format!("tar failed to spawn: {e}")))?;
    if !status.success() {
        return Err(Error::Other("pg_state tar extraction failed".into()));
    }
    let _ = fs::remove_file(&pg_state_tmp);

    // ── 2. recovery_manifest.bin ──────────────────────────────────────────────
    let manifest_path = Manifest::local_manifest_path(tiko_root);
    let base = load_base_manifest(sim, ns, target_tl, target_lsn, &manifest_path)?;
    apply_deltas_up_to(sim, ns, &base, tiko_root, target_tl, target_lsn)?;

    // ── 3. postgresql.conf ────────────────────────────────────────────────────
    write_recovery_conf(&pgdata.join("postgresql.conf"), "tiko_restore %f %p")?;

    // ── 4. recovery.signal ────────────────────────────────────────────────────
    fs::write(pgdata.join("recovery.signal"), b"")?;

    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Load the newest base manifest with `base_lsn <= target_lsn` on `target_tl`.
fn load_base_manifest(
    sim: &SimStore,
    ns: &ProjectNamespace,
    target_tl: u32,
    target_lsn: Lsn,
    manifest_path: &Path,
) -> Result<Manifest> {
    let prefix = ns.base_prefix_for_timeline(target_tl);
    let keys = sim.list_prefix_standard(&prefix).map_err(Error::Store)?;

    // After stripping prefix: "{lsn_hex}/manifest.bin"
    let best_key = keys
        .iter()
        .filter_map(|k| {
            let rel = k.strip_prefix(&prefix)?;
            let lsn_hex = rel.split('/').next()?;
            let lsn = Lsn::from_hex(lsn_hex).ok()?;
            (lsn <= target_lsn).then_some((lsn, k))
        })
        .max_by_key(|(lsn, _)| *lsn)
        .map(|(_, k)| k)
        .ok_or(Error::NoCheckpoint)?;

    let bytes = sim
        .get_standard(best_key)
        .map_err(Error::Store)?
        .ok_or_else(|| Error::Other(format!("base manifest not found: {best_key}")))?;

    Manifest::from_bytes(&bytes, manifest_path).map_err(Error::Store)
}

/// Apply all delta manifests with `base_lsn < lsn <= target_lsn` on `target_tl`.
///
/// Temp delta files are written under `work_dir` and removed after merging.
fn apply_deltas_up_to(
    sim: &SimStore,
    ns: &ProjectNamespace,
    base: &Manifest,
    work_dir: &Path,
    target_tl: u32,
    target_lsn: Lsn,
) -> Result<()> {
    let base_lsn = base.checkpoint_lsn();
    let prefix = ns.delta_prefix_for_timeline(target_tl);
    let mut keys = sim.list_prefix_standard(&prefix).map_err(Error::Store)?;
    keys.sort();

    let mut deltas: Vec<Manifest> = Vec::new();
    let mut delta_paths: Vec<PathBuf> = Vec::new();

    for key in &keys {
        if !key.ends_with("/manifest.bin") {
            continue;
        }
        let rel = key.strip_prefix(&prefix).unwrap_or(key.as_str());
        let lsn_hex = rel.split('/').next().unwrap_or("");
        let Ok(lsn) = Lsn::from_hex(lsn_hex) else {
            continue;
        };
        if lsn <= base_lsn || lsn > target_lsn {
            continue;
        }
        let bytes = sim
            .get_standard(key)
            .map_err(Error::Store)?
            .ok_or_else(|| Error::Other(format!("delta manifest missing: {key}")))?;
        let path = work_dir.join(format!("delta_{lsn_hex}.tikm"));
        let m = Manifest::from_bytes(&bytes, &path).map_err(Error::Store)?;
        delta_paths.push(path);
        deltas.push(m);
    }

    if !deltas.is_empty() {
        base.apply_deltas(&deltas).map_err(Error::Store)?;
    }

    for p in &delta_paths {
        let _ = fs::remove_file(p);
    }

    Ok(())
}

/// Append tikod recovery settings to `postgresql.conf`.
///
/// Uses `recovery_target = 'immediate'` so PG promotes as soon as a consistent
/// state is reached — correct for branching from a shutdown checkpoint where
/// there is no WAL to replay past the checkpoint.
fn write_recovery_conf(conf_path: &Path, restore_command: &str) -> Result<()> {
    let snippet = format!(
        "\n# tikod recovery settings — removed by post_recovery_cleanup\n\
         restore_command = '{restore_command}'\n\
         recovery_target = 'immediate'\n\
         recovery_target_action = 'promote'\n"
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use store::sim_store::SimStore;
    use tempfile::TempDir;

    fn temp_sim() -> (SimStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        (sim, dir)
    }

    fn root_ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 1, 1)
    }

    fn write_delta_manifest(
        sim: &SimStore,
        ns: &ProjectNamespace,
        tl: u32,
        lsn: Lsn,
        timestamp: i64,
    ) {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().join("manifest.tikm");
        let m = Manifest::new(lsn, timestamp, vec![], HashMap::new(), vec![], &tmp).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(&ns.delta_manifest_key(tl, lsn), &bytes)
            .unwrap();
    }

    fn write_base_manifest(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lsn: Lsn) {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().join("manifest.tikm");
        let m = Manifest::new(lsn, 0, vec![], HashMap::new(), vec![], &tmp).unwrap();
        let bytes = m.to_bytes().unwrap();
        sim.put_standard(&ns.base_manifest_key(tl, lsn), &bytes)
            .unwrap();
    }

    /// Write a minimal (uncompressed) tar archive as the pg_state for `(tl, lsn)`.
    ///
    /// The archive contains a single marker file `tiko/pg_state_marker.txt` so
    /// tests can verify extraction succeeded.
    fn write_pg_state(sim: &SimStore, ns: &ProjectNamespace, tl: u32, lsn: Lsn) {
        let content = b"pitr_test";
        let mut ar = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        ar.append_data(&mut header, "tiko/pg_state_marker.txt", &content[..])
            .unwrap();
        let bytes = ar.into_inner().unwrap();
        sim.put_standard(&ns.pg_state_key(tl, lsn), &bytes).unwrap();
    }

    // ── list_restore_points ───────────────────────────────────────────────────

    #[test]
    fn list_restore_points_returns_all_timelines_sorted() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();

        write_delta_manifest(&sim, &ns, 1, Lsn::new(0x1000), 100);
        write_delta_manifest(&sim, &ns, 1, Lsn::new(0x3000), 300);
        write_delta_manifest(&sim, &ns, 2, Lsn::new(0x2000), 200);

        let points = list_restore_points(&sim, &ns).unwrap();
        assert_eq!(points.len(), 3);
        // Sorted by (timeline_id, lsn).
        assert_eq!(
            points[0],
            RestorePoint {
                timeline_id: 1,
                lsn: Lsn::new(0x1000),
                timestamp: 100
            }
        );
        assert_eq!(
            points[1],
            RestorePoint {
                timeline_id: 1,
                lsn: Lsn::new(0x3000),
                timestamp: 300
            }
        );
        assert_eq!(
            points[2],
            RestorePoint {
                timeline_id: 2,
                lsn: Lsn::new(0x2000),
                timestamp: 200
            }
        );
    }

    #[test]
    fn list_restore_points_empty_when_no_deltas() {
        let (sim, _dir) = temp_sim();
        let points = list_restore_points(&sim, &root_ns()).unwrap();
        assert!(points.is_empty());
    }

    // ── prepare_recovery ──────────────────────────────────────────────────────

    #[test]
    fn prepare_recovery_fails_when_target_delta_missing() {
        let (sim, _dir) = temp_sim();
        let pgdata = TempDir::new().unwrap();

        let err = prepare_recovery(
            &sim,
            &root_ns(),
            pgdata.path(),
            pgdata.path(),
            1,
            Lsn::new(0x5000),
        )
        .unwrap_err();
        assert!(matches!(err, Error::TargetNotFound(_)));
    }

    #[test]
    fn prepare_recovery_writes_recovery_manifest_and_conf() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        let pgdata = TempDir::new().unwrap();
        let pgdata_path = pgdata.path();

        let base_lsn = Lsn::new(0x1000);
        let target_lsn = Lsn::new(0x3000);

        write_base_manifest(&sim, &ns, 1, base_lsn);
        write_delta_manifest(&sim, &ns, 1, target_lsn, 999);
        write_pg_state(&sim, &ns, 1, target_lsn);
        fs::write(pgdata_path.join("postgresql.conf"), "# existing\n").unwrap();

        prepare_recovery(&sim, &ns, pgdata_path, pgdata_path, 1, target_lsn).unwrap();

        // recovery_manifest.bin (base_manifest.bin) must exist.
        assert!(
            Manifest::local_manifest_path(pgdata_path).exists(),
            "recovery manifest must be written"
        );
        // recovery.signal must be created.
        assert!(pgdata_path.join("recovery.signal").exists());
        // postgresql.conf must contain the recovery block.
        let conf = fs::read_to_string(pgdata_path.join("postgresql.conf")).unwrap();
        assert!(
            conf.contains("recovery_target = 'immediate'"),
            "recovery_target = 'immediate' missing from conf"
        );
        assert!(
            conf.contains("restore_command"),
            "restore_command missing from conf"
        );
        assert!(
            conf.contains("# existing"),
            "original conf lines must be preserved"
        );
    }

    #[test]
    fn prepare_recovery_extracts_pg_state_into_pgdata() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        let pgdata = TempDir::new().unwrap();
        let pgdata_path = pgdata.path();

        let base_lsn = Lsn::new(0x1000);
        let target_lsn = Lsn::new(0x3000);

        write_base_manifest(&sim, &ns, 1, base_lsn);
        write_delta_manifest(&sim, &ns, 1, target_lsn, 0);
        write_pg_state(&sim, &ns, 1, target_lsn);
        fs::write(pgdata_path.join("postgresql.conf"), "").unwrap();

        prepare_recovery(&sim, &ns, pgdata_path, pgdata_path, 1, target_lsn).unwrap();

        // The marker file embedded in the test tar must appear under PGDATA.
        assert!(
            pgdata_path.join("tiko/pg_state_marker.txt").exists(),
            "pg_state tar must be extracted into PGDATA"
        );
    }

    #[test]
    fn prepare_recovery_writes_recovery_target_immediate_into_conf() {
        let (sim, _dir) = temp_sim();
        let ns = root_ns();
        let pgdata = TempDir::new().unwrap();
        let pgdata_path = pgdata.path();

        let target_lsn = Lsn::new(0x3000000);

        write_base_manifest(&sim, &ns, 1, Lsn::new(0x1000));
        write_delta_manifest(&sim, &ns, 1, target_lsn, 0);
        write_pg_state(&sim, &ns, 1, target_lsn);
        fs::write(pgdata_path.join("postgresql.conf"), "").unwrap();

        prepare_recovery(&sim, &ns, pgdata_path, pgdata_path, 1, target_lsn).unwrap();

        let conf = fs::read_to_string(pgdata_path.join("postgresql.conf")).unwrap();
        assert!(
            conf.contains("recovery_target = 'immediate'"),
            "conf must contain recovery_target = 'immediate': {conf}"
        );
    }
}
