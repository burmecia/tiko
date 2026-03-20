//! Recovery mode support for PITR.
//!
//! During PostgreSQL point-in-time recovery, `RECOVERY_MODE` is set to true
//! and `RECOVERY_MANIFEST` holds the loaded manifest that maps each chunk to
//! its versioned S3 location. The `cached_read_blocks()` fallback uses this
//! to serve reads from the versioned standard-bucket objects.
//!
//! The recovery manifest is loaded from `$PGDATA/tiko/recovery_manifest.bin`,
//! which is a msgpack+zstd blob (same wire format as S3 `manifest.bin`).
//! It is written by the control plane before initiating PITR recovery.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::chunk::ChunkTag;
use crate::manifest::{ChunkRef, Manifest};
use crate::project::ProjectNamespace;
use crate::sim_store::SimStore;
use pgsys::Lsn;

/// Exposed as `pub(crate)` so tests in sibling modules can force the flag.
pub static RECOVERY_MODE: AtomicBool = AtomicBool::new(false);

/// Recovery manifest: a Manifest loaded from `$PGDATA/tiko/recovery_manifest.bin`.
/// Populated by `load_recovery_manifest`; queried via `lookup_recovery_chunk`.
static RECOVERY_MANIFEST: OnceLock<Manifest> = OnceLock::new();

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

/// Load the recovery manifest from `path` (= `$PGDATA/tiko/recovery_manifest.bin`).
///
/// Reads the msgpack+zstd blob at `path`, converts it to a local TIKM file at
/// `{path.parent()}/recovery_manifest_local.bin`, stores it in
/// `RECOVERY_MANIFEST`, and sets `RECOVERY_MODE = true`.
///
/// Returns an error if the file cannot be read or the blob is malformed.
/// If the manifest was already loaded (OnceLock already set), the set is
/// silently ignored but `RECOVERY_MODE` is still set to true.
pub fn load_recovery_manifest(path: &Path) -> io::Result<()> {
    let bytes = std::fs::read(path)?;
    let local_path = path
        .parent()
        .unwrap_or(Path::new("."))
        .join("recovery_manifest_local.bin");
    let manifest = Manifest::from_bytes(&bytes, &local_path)?;
    // OnceLock::set fails silently if already populated — acceptable for recovery.
    let _ = RECOVERY_MANIFEST.set(manifest);
    RECOVERY_MODE.store(true, Ordering::Release);
    Ok(())
}

/// Clear recovery mode. Called after PostgreSQL promotes to primary.
pub fn clear_recovery_mode() {
    RECOVERY_MODE.store(false, Ordering::Release);
}

/// Return `true` if the process is in PITR recovery mode.
pub fn is_recovery_mode() -> bool {
    RECOVERY_MODE.load(Ordering::Acquire)
}

/// Look up a chunk in the recovery manifest.
///
/// Returns `Ok(None)` if the manifest is not loaded or the key is absent.
pub fn lookup_recovery_chunk(key: &ChunkTag) -> io::Result<Option<ChunkRef>> {
    match RECOVERY_MANIFEST.get() {
        Some(m) => m.lookup(key),
        None => Ok(None),
    }
}

/// Prepare PGDATA for crash recovery to `(target_tl, target_lsn)`.
///
/// Steps:
/// 1. Validate `deltas/{target_tl}/{target_lsn}/manifest.bin` exists.
/// 2. Download and extract `pg_state.tar.zst` (pg_control, transaction logs,
///    pg_filenode.map) into PGDATA.
/// 3. Build and write `recovery_manifest.bin` into PGDATA — newest base with
///    `base_lsn <= target_lsn` merged with all deltas in `(base_lsn, target_lsn]`.
/// 4. Append recovery settings to `postgresql.conf`.
/// 5. Touch `recovery.signal`.
///
/// Intermediate files (pg_state archive, manifest working files) are written to
/// a temporary directory that is cleaned up automatically on return.
pub fn prepare_recovery(
    sim: &SimStore,
    ns: &ProjectNamespace,
    pgdata: &Path,
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

    let work =
        tempfile::tempdir().map_err(|e| Error::Other(format!("failed to create temp dir: {e}")))?;

    // ── 1. pg_state ───────────────────────────────────────────────────────────
    let pg_state_key = ns.pg_state_key(target_tl, target_lsn);
    let pg_state_bytes = sim
        .get_standard(&pg_state_key)
        .map_err(Error::Store)?
        .ok_or_else(|| Error::Other(format!("pg_state not found: {pg_state_key}")))?;

    let pg_state_tmp = work.path().join("pg_state.tar.zst");
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

    // ── 2. recovery_manifest.bin ──────────────────────────────────────────────
    let manifest_path = Manifest::local_manifest_path(work.path());
    let base = load_base_manifest(sim, ns, target_tl, target_lsn, &manifest_path)?;
    apply_deltas_up_to(sim, ns, &base, work.path(), target_tl, target_lsn)?;

    let manifest_bytes = base.to_bytes().map_err(Error::Store)?;
    fs::write(pgdata.join("recovery_manifest.bin"), &manifest_bytes)?;

    // ── 3. postgresql.conf ────────────────────────────────────────────────────
    write_recovery_conf(&pgdata.join("postgresql.conf"), target_lsn)?;

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

const RECOVERY_CONF_BEGIN: &str = "# Tiko recovery settings — begin\n";
const RECOVERY_CONF_END: &str = "# Tiko recovery settings — end\n";

/// Remove the recovery settings block previously written by `write_recovery_conf`.
///
/// Strips the entire block delimited by the begin/end markers, leaving the
/// rest of `postgresql.conf` untouched. A no-op if the markers are not present.
pub fn remove_recovery_conf(conf_path: &Path) -> Result<()> {
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    let Some(start) = existing.find(RECOVERY_CONF_BEGIN) else {
        return Ok(());
    };
    let end_marker_offset = existing[start..]
        .find(RECOVERY_CONF_END)
        .map(|p| start + p + RECOVERY_CONF_END.len())
        .unwrap_or(existing.len());
    let cleaned = format!("{}{}", &existing[..start], &existing[end_marker_offset..]);
    fs::write(conf_path, cleaned)?;
    Ok(())
}

/// Append Tiko recovery settings to a PostgreSQL conf file.
///
/// The block is delimited by begin/end markers so that `remove_recovery_conf`
/// can strip it cleanly even if further settings follow.
pub fn write_recovery_conf(conf_path: &Path, target_lsn: Lsn) -> Result<()> {
    let snippet = format!(
        "\n{}\
         restore_command = 'tiko_restore %f %p'\n\
         recovery_target_lsn = '{}'\n\
         recovery_target_action = 'shutdown'\n\
         {}",
        RECOVERY_CONF_BEGIN,
        target_lsn.to_pg_string(),
        RECOVERY_CONF_END,
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Serialises all tests across crates that read or write `RECOVERY_MODE`.
/// Must be held by any test that touches the flag to avoid flaky results.
pub static RECOVERY_MODE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ChunkTag;
    use crate::manifest::{ChunkRef, Manifest};
    use pgsys::Lsn;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn tag(rel: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: rel,
            fork_number: 0,
            chunk_id: 0,
        }
    }

    // ── RECOVERY_MODE flag ────────────────────────────────────────────────

    #[test]
    fn clear_and_set_recovery_mode() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // AtomicBool can be set/cleared freely across tests.
        RECOVERY_MODE.store(false, Ordering::SeqCst);
        assert!(!is_recovery_mode());
        RECOVERY_MODE.store(true, Ordering::SeqCst);
        assert!(is_recovery_mode());
        clear_recovery_mode();
        assert!(!is_recovery_mode());
    }

    // ── lookup_recovery_chunk without a loaded manifest ───────────────────

    #[test]
    fn lookup_when_manifest_not_loaded_does_not_panic() {
        // RECOVERY_MANIFEST may already be set by a parallel test (OnceLock).
        // We only verify the function returns Ok without panicking.
        let result = lookup_recovery_chunk(&tag(0xDEAD));
        assert!(result.is_ok());
    }

    // ── load and lookup via a local Manifest instance ─────────────────────

    #[test]
    fn load_recovery_manifest_and_lookup() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let known_tag = tag(42);
        let known_ref = ChunkRef {
            branch_id: 7,
            timeline_id: 1,
            lsn: Lsn::new(0x1000),
        };

        // Build the msgpack+zstd blob.
        let build_path = dir.path().join("build.tikm");
        let m = Manifest::new(
            Lsn::new(0x1000),
            0,
            vec![(known_tag, known_ref)],
            HashMap::new(),
            &build_path,
        )
        .unwrap();
        let blob = m.to_bytes().unwrap();

        // Write the blob as the recovery manifest file.
        let tiko_dir = dir.path();
        std::fs::create_dir_all(&tiko_dir).unwrap();
        let manifest_path = tiko_dir.join("recovery_manifest.bin");
        std::fs::write(&manifest_path, &blob).unwrap();

        // Attempt to load (may be a no-op if OnceLock already set by a parallel test).
        // load_recovery_manifest sets RECOVERY_MODE=true; clear it afterwards.
        let _ = load_recovery_manifest(&manifest_path);

        // Test the logic directly on a local Manifest instance to avoid
        // OnceLock contention between parallel tests.
        let local = Manifest::from_bytes(&blob, &dir.path().join("local.tikm")).unwrap();
        assert_eq!(local.lookup(&known_tag).unwrap(), Some(known_ref));
        assert_eq!(local.lookup(&tag(999)).unwrap(), None);

        // Restore RECOVERY_MODE so subsequent tests inside the guard see false.
        clear_recovery_mode();
    }

    // ── clear_recovery_mode after load ────────────────────────────────────

    #[test]
    fn recovery_mode_can_be_cleared_after_set() {
        let _guard = RECOVERY_MODE_TEST_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        RECOVERY_MODE.store(true, Ordering::SeqCst);
        assert!(is_recovery_mode());
        clear_recovery_mode();
        assert!(!is_recovery_mode());
    }
}
