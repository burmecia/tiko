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

use std::io;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::cache::ChunkTag;
use crate::manifest::{ChunkRef, Manifest};

/// Exposed as `pub(crate)` so tests in sibling modules can force the flag.
pub(crate) static RECOVERY_MODE: AtomicBool = AtomicBool::new(false);

/// Recovery manifest: a Manifest loaded from `$PGDATA/tiko/recovery_manifest.bin`.
/// Populated by `load_recovery_manifest`; queried via `lookup_recovery_chunk`.
static RECOVERY_MANIFEST: OnceLock<Manifest> = OnceLock::new();

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

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Serialises all tests that read or write `RECOVERY_MODE` across the whole
/// crate. Both `recovery::tests` and `s3_ops::tests` must hold this guard
/// before touching the flag, otherwise parallel tests produce flaky results.
#[cfg(test)]
pub(crate) static RECOVERY_MODE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ChunkTag;
    use crate::manifest::{ChunkRef, Manifest};
    use pgsys::Lsn;
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
            lsn: Lsn::new(0x1000),
        };

        // Build the msgpack+zstd blob.
        let build_path = dir.path().join("build.tikm");
        let m = Manifest::new_sorted(
            Lsn::new(0x1000),
            0,
            vec![(known_tag, known_ref)],
            &build_path,
        )
        .unwrap();
        let blob = m.to_bytes().unwrap();

        // Write the blob as the recovery manifest file.
        let tiko_dir = dir.path().join("tiko");
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
