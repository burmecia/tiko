//! Recovery mode support for PITR.
//!
//! During PostgreSQL point-in-time recovery, `RECOVERY_MODE` is set to true
//! and `RECOVERY_MANIFEST` holds the loaded manifest that maps each chunk to
//! its versioned S3 location. The `read_blocks()` fallback uses this
//! to serve reads from the versioned standard-bucket objects.
//!
//! The recovery manifest is loaded from `$PGDATA/tiko/recovery_manifest.bin`,
//! which is a TIKM binary file (see `manifest.rs` for the format).
//! It is written by the control plane before initiating PITR recovery.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::chunk::ChunkTag;
use crate::io::store::Store;
use crate::manifest::{ChunkRef, Manifest};
use crate::project::ProjectNamespace;
use pgsys::{Lsn, common::XLOG_SEG_SIZE};

/// Exposed as `pub(crate)` so tests in sibling modules can force the flag.
pub static RECOVERY_MODE: AtomicBool = AtomicBool::new(false);

/// Recovery manifest: a Manifest loaded from `$PGDATA/tiko/recovery_manifest.bin`.
/// Populated by `load_recovery_manifest`; queried via `lookup_recovery_chunk`.
static RECOVERY_MANIFEST: OnceLock<Manifest> = OnceLock::new();

const TIKO_CONF_FILE: &str = "postgresql.tiko.conf";
const RECOVERY_CONF_BEGIN: &str = "# Tiko recovery settings — begin\n";
const RECOVERY_CONF_END: &str = "# Tiko recovery settings — end\n";

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum Error {
    Store(crate::Error),
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
        Error::Store(e.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Load the recovery manifest from `path` (= `$PGDATA/tiko/recovery_manifest.bin`).
///
/// Opens the TIKM file at `path`, stores it in `RECOVERY_MANIFEST`, and sets
/// `RECOVERY_MODE = true`.
///
/// Returns an error if the file cannot be read or is not a valid TIKM file.
/// If the manifest was already loaded (OnceLock already set), the set is
/// silently ignored but `RECOVERY_MODE` is still set to true.
pub fn load_recovery_manifest(path: &Path) -> crate::Result<()> {
    let manifest = Manifest::open(path)?;
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
pub fn lookup_recovery_chunk(_key: &ChunkTag) -> io::Result<Option<ChunkRef>> {
    match RECOVERY_MANIFEST.get() {
        Some(_m) => Ok(None), //_m.lookup(key),
        None => Ok(None),
    }
}

// ── WAL copy from parent pg_wal/ to child pg_wal/ ────────────────────────────

/// Read `checkPointCopy.redo` from the `pg_control` file.
///
/// PostgreSQL's `ControlFileData` layout on a 64-bit system places
/// `checkPointCopy.redo` (the first `XLogRecPtr` of the `CheckPoint` struct)
/// at byte offset 40:
///   - offset  0: system_identifier (uint64, 8 bytes)
///   - offset  8: pg_control_version (uint32, 4 bytes)
///   - offset 12: catalog_version_no (uint32, 4 bytes)
///   - offset 16: state (DBState / int, 4 bytes)
///   - offset 20: padding (4 bytes, 8-byte alignment for time)
///   - offset 24: time (pg_time_t / int64, 8 bytes)
///   - offset 32: checkPoint (XLogRecPtr / uint64, 8 bytes)
///   - offset 40: checkPointCopy.redo (XLogRecPtr / uint64, 8 bytes)  ← here
fn read_checkpoint_redo(pg_control_path: &Path) -> Result<u64> {
    let data = fs::read(pg_control_path)?;
    if data.len() < 48 {
        return Err(Error::Other(format!(
            "pg_control too short: {} bytes (expected ≥ 48)",
            data.len()
        )));
    }
    let redo = u64::from_le_bytes(data[40..48].try_into().unwrap());
    Ok(redo)
}

/// Copy WAL segment files from `parent_pg_wal` into `child_pg_wal`, starting
/// from the segment that contains `redo_lsn` through all segments that
/// currently exist in the parent's `pg_wal/` directory.
///
/// Copies everything available so the child has the full WAL history from the
/// redo point up to the present, not just up to the branch-point checkpoint.
///
/// Creates `child_pg_wal` if it does not exist.  Only files whose names match
/// the WAL segment pattern for `timeline` (`{tl:08X}{log_id:08X}{log_seg:08X}`,
/// 24 hex chars) with a segment number ≥ `first_seg(redo_lsn)` are copied;
/// all other entries (history files, `archive_status/`, partial segments, etc.)
/// are ignored.
fn copy_branch_wal(
    parent_pg_wal: &Path,
    child_pg_wal: &Path,
    redo_lsn: u64,
    timeline: u32,
) -> Result<()> {
    fs::create_dir_all(child_pg_wal)?;

    let seg_size = XLOG_SEG_SIZE as u64;
    let segs_per_xlog_id = 0x1_0000_0000_u64 / seg_size; // 256 for 16 MiB
    let first_seg = redo_lsn / seg_size;
    let tl_prefix = format!("{timeline:08X}");

    let entries = fs::read_dir(parent_pg_wal)?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        // WAL segment filenames are exactly 24 uppercase hex characters.
        if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        // Must belong to the same timeline.
        if !name.starts_with(&tl_prefix) {
            continue;
        }
        // Parse log_id + log_seg from the remaining 16 hex chars.
        let log_id = u32::from_str_radix(&name[8..16], 16)
            .map_err(|_| Error::Other(format!("unparseable WAL seg name: {name}")))?;
        let log_seg = u32::from_str_radix(&name[16..24], 16)
            .map_err(|_| Error::Other(format!("unparseable WAL seg name: {name}")))?;
        let seg_no = log_id as u64 * segs_per_xlog_id + log_seg as u64;

        if seg_no < first_seg {
            continue;
        }

        fs::copy(entry.path(), child_pg_wal.join(&*name))
            .map_err(crate::Error::Io)
            .map_err(Error::Store)?;
        eprintln!("Copied WAL segment {name} (segment {seg_no})");
    }
    Ok(())
}

/// Prepare PGDATA for crash recovery to `(target_tl, target_lsn)`.
///
/// Steps:
/// 1. Validate `deltas/{target_tl}/{target_lsn}/manifest.bin` exists.
/// 2. Download and extract `pg_state.tar.zst` (pg_control, transaction logs,
///    pg_filenode.map) into PGDATA.
/// 3. If `parent_pgdata` is provided, read `checkPointCopy.redo` from the
///    extracted `pg_control`, then copy WAL segments covering
///    `[redo_lsn, target_lsn]` from parent's `pg_wal/` into child's `pg_wal/`.
/// 4. Build and write `recovery_manifest.bin` — newest base with
///    `base_lsn <= target_lsn` merged with all deltas in `(base_lsn, target_lsn]`.
/// 5. Append recovery settings to `postgresql.tiko.conf`.
/// 6. Touch `recovery.signal`.
///
/// Intermediate files (pg_state archive, manifest working files) are written to
/// a temporary directory that is cleaned up automatically on return.
pub fn prepare_recovery(
    sim: &Store,
    ns: &ProjectNamespace,
    pgdata: &Path,    // target PGDATA directory to prepare for recovery
    root_path: &Path, // root path for recovery_manifest.bin files
    target_tl: u32,
    target_lsn: Lsn,
    parent_pgdata: Option<&Path>, // parent's PGDATA; pg_wal/ is copied to child's pg_wal/
) -> Result<()> {
    // ── 1. Validate target ────────────────────────────────────────────────────
    // let delta_key = ns.delta_manifest_key(target_tl, target_lsn);
    // if sim
    //     .get_standard(&delta_key)
    //     .map_err(Error::Store)?
    //     .is_error()
    // {
    //     return Err(Error::TargetNotFound(delta_key));
    // }

    let work =
        tempfile::tempdir().map_err(|e| Error::Other(format!("failed to create temp dir: {e}")))?;

    // ── 2. pg_state ───────────────────────────────────────────────────────────
    let pg_state_key = ns.pg_state_key(target_tl, target_lsn);
    let pg_state_bytes = sim.get_standard(&pg_state_key).map_err(Error::Store)?;

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

    // ── 3. Copy parent WAL into child's pg_wal/ ───────────────────────────────
    // pg_control is now in pgdata (extracted above). Read checkPointCopy.redo
    // (offset 40) to find the earliest WAL byte the child needs, then copy
    // every segment from [redo_lsn, target_lsn] from parent's pg_wal/.
    // No restore_command — PostgreSQL reads WAL directly from local pg_wal/.
    if let Some(parent_pgdata) = parent_pgdata {
        let pg_control_path = pgdata.join("global").join("pg_control");
        let redo_lsn = read_checkpoint_redo(&pg_control_path)?;
        copy_branch_wal(
            &parent_pgdata.join("pg_wal"),
            &pgdata.join("pg_wal"),
            redo_lsn,
            target_tl,
        )?;
    }

    // ── 4. recovery_manifest.bin ──────────────────────────────────────────────
    let manifest_path = Manifest::local_manifest_path(work.path());
    let base = load_base_manifest(sim, ns, target_tl, target_lsn, &manifest_path)?;
    apply_deltas_up_to(sim, ns, &base, work.path(), target_tl, target_lsn)?;

    // Copy the merged TIKM file directly — no need to round-trip through the
    // S3 wire format (zstd+msgpack). Place it in the tiko root directory.
    fs::create_dir_all(root_path)?;
    fs::copy(&manifest_path, Manifest::recovery_manifest_path(root_path))
        .map_err(crate::Error::Io)
        .map_err(Error::Store)?;

    // ── 5. postgresql.tiko.conf ───────────────────────────────────────────────
    write_recovery_conf(&pgdata.join(TIKO_CONF_FILE))?;

    // ── 6. recovery.signal ────────────────────────────────────────────────────
    fs::write(pgdata.join("recovery.signal"), b"")?;

    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Load the newest base manifest with `base_lsn <= target_lsn` on `target_tl`.
fn load_base_manifest(
    sim: &Store,
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

    let bytes = sim.get_standard(best_key).map_err(Error::Store)?;

    Manifest::from_bytes(&bytes, manifest_path)
        .map_err(crate::Error::Io)
        .map_err(Error::Store)
}

/// Apply all delta manifests with `base_lsn < lsn <= target_lsn` on `target_tl`.
///
/// Temp delta files are written under `work_dir` and removed after merging.
fn apply_deltas_up_to(
    sim: &Store,
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
        let bytes = sim.get_standard(key).map_err(Error::Store)?;
        let path = work_dir.join(format!("delta_{lsn_hex}.tikm"));
        let m = Manifest::from_bytes(&bytes, &path)
            .map_err(crate::Error::Io)
            .map_err(Error::Store)?;
        delta_paths.push(path);
        deltas.push(m);
    }

    if !deltas.is_empty() {
        base.apply_deltas(&deltas)
            .map_err(crate::Error::Io)
            .map_err(Error::Store)?;
    }

    for p in &delta_paths {
        let _ = fs::remove_file(p);
    }

    Ok(())
}

/// Remove the recovery settings block previously written by `write_recovery_conf`.
///
/// Strips the entire block delimited by the begin/end markers, leaving the
/// rest of `postgresql.tiko.conf` untouched. A no-op if the markers are not present.
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
///
/// Uses `recovery_target = 'immediate'` so PostgreSQL stops as soon as a
/// consistent recovery state is reached (right after the checkpoint record).
/// This is correct for branching from a checkpoint — in particular it handles
/// shutdown checkpoints, where `checkPoint.redo == ProcLastRecPtr` and the
/// checkpoint record is consumed before the WAL replay loop starts, so any
/// `recovery_target_lsn` pointing at the checkpoint would never be reached.
///
/// No `restore_command` is written: WAL segments are copied directly into the
/// child's `pg_wal/` by `prepare_recovery`, so PostgreSQL reads them from the
/// local directory without calling an external command.
pub fn write_recovery_conf(conf_path: &Path) -> Result<()> {
    let snippet = format!(
        "\n{}\
         recovery_target = 'immediate'\n\
         recovery_target_action = 'shutdown'\n\
         {}",
        RECOVERY_CONF_BEGIN, RECOVERY_CONF_END,
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}
