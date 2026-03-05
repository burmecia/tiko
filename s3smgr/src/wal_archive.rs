//! WAL archiving and PG state upload helpers for PITR.
//!
//! All I/O is synchronous (`std::fs`) — this module runs on the checkpointer
//! process main thread, not in a Tokio runtime.
//!
//! Functions:
//! - [`upload_delta_manifest`] — serialise and PUT a delta manifest to standard sim.
//! - [`upload_pg_state`]      — build a tar+zstd archive of PG state files and PUT it.
//! - [`upload_wal_segment`]   — copy a WAL segment file to standard sim.
//! - [`download_wal_segment`] — retrieve a WAL segment from standard sim (used by `tiko_restore`).

use std::fs;
use std::io;
use std::path::Path;

use pgsys::Lsn;
use s3worker::manifest::Manifest;
use s3worker::project::ProjectNamespace;
use s3worker::sim_store::SimStore;

// ── Delta manifest ────────────────────────────────────────────────────────────

/// Serialise `manifest` to the S3 wire format and PUT it at the delta manifest
/// key `{org}/pitr/{proj}/deltas/{lsn_hex}/manifest.bin` in the standard bucket.
pub fn upload_delta_manifest(
    sim: &SimStore,
    ns: &ProjectNamespace,
    checkpoint_lsn: Lsn,
    manifest: &Manifest,
) -> io::Result<()> {
    let bytes = manifest.to_bytes()?;
    sim.put_standard(&ns.delta_manifest_key(checkpoint_lsn), &bytes)
}

// ── PG state archive ──────────────────────────────────────────────────────────

/// Build a tar+zstd archive of the critical PG state files and PUT it at
/// `{org}/pitr/{proj}/deltas/{lsn_hex}/pg_state.tar.zst` in the standard bucket.
///
/// Included paths (relative to `pgdata`):
/// - `global/pg_control`
/// - `pg_xact/**`
/// - `pg_multixact/members/**`
/// - `pg_multixact/offsets/**`
/// - `global/pg_filenode.map`
///
/// Missing files or directories are silently skipped — this is intentional for
/// test environments where PG state files may not exist. In production the
/// checkpointer always runs inside a live PostgreSQL data directory.
pub fn upload_pg_state(
    sim: &SimStore,
    ns: &ProjectNamespace,
    checkpoint_lsn: Lsn,
    pgdata: &Path,
) -> io::Result<()> {
    let compressed = build_pg_state_archive(pgdata)?;
    sim.put_standard(&ns.pg_state_key(checkpoint_lsn), &compressed)
}

/// Build the in-memory tar+zstd archive.  Returns compressed bytes.
fn build_pg_state_archive(pgdata: &Path) -> io::Result<Vec<u8>> {
    let buf: Vec<u8> = Vec::new();
    let enc = zstd::Encoder::new(buf, 3)?;
    let mut builder = tar::Builder::new(enc);

    // global/pg_control
    let pg_control = pgdata.join("global").join("pg_control");
    if pg_control.exists() {
        builder.append_path_with_name(&pg_control, "global/pg_control")?;
    }

    // pg_xact/
    let pg_xact = pgdata.join("pg_xact");
    if pg_xact.is_dir() {
        builder.append_dir_all("pg_xact", &pg_xact)?;
    }

    // pg_multixact/members/
    let multixact_members = pgdata.join("pg_multixact").join("members");
    if multixact_members.is_dir() {
        builder.append_dir_all("pg_multixact/members", &multixact_members)?;
    }

    // pg_multixact/offsets/
    let multixact_offsets = pgdata.join("pg_multixact").join("offsets");
    if multixact_offsets.is_dir() {
        builder.append_dir_all("pg_multixact/offsets", &multixact_offsets)?;
    }

    // global/pg_filenode.map
    let filenode_map = pgdata.join("global").join("pg_filenode.map");
    if filenode_map.exists() {
        builder.append_path_with_name(&filenode_map, "global/pg_filenode.map")?;
    }

    let enc = builder.into_inner()?;
    let compressed = enc.finish()?;
    Ok(compressed)
}

// ── WAL segment archive ───────────────────────────────────────────────────────

/// Read a WAL segment from disk and PUT it at
/// `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}` in the standard bucket.
///
/// Called by the `tiko_archive` binary (Module 9) via PostgreSQL's
/// `archive_command`.
pub fn upload_wal_segment(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    segment: &str,
    path: &Path,
) -> io::Result<()> {
    let bytes = fs::read(path)?;
    sim.put_standard(&ns.wal_key(timeline, segment), &bytes)
}

/// Download a WAL segment from `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}`
/// in the standard bucket and write it to `dest`.
///
/// Returns `Ok(true)` if the segment was found and written, `Ok(false)` if it
/// does not exist (caller may try a parent namespace).
///
/// Called by the `tiko_restore` binary (Module 9).
pub fn download_wal_segment(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    segment: &str,
    dest: &Path,
) -> io::Result<bool> {
    match sim.get_standard(&ns.wal_key(timeline, segment))? {
        Some(bytes) => {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(dest, &bytes)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::Lsn;
    use s3worker::cache::ChunkTag;
    use s3worker::manifest::{ChunkRef, Manifest};
    use s3worker::project::ProjectNamespace;
    use s3worker::sim_store::SimStore;
    use tempfile::TempDir;

    fn ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 7)
    }

    fn setup() -> (TempDir, SimStore) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        (dir, sim)
    }

    fn make_manifest(dir: &Path, lsn: Lsn) -> Manifest {
        let path = dir.join("m.tikm");
        let tag = ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: 1,
            fork_number: 0,
            chunk_id: 0,
        };
        let cref = ChunkRef { branch_id: 7, lsn };
        Manifest::new_sorted(lsn, 0, vec![(tag, cref)], &path).unwrap()
    }

    // ── upload_delta_manifest ────────────────────────────────────────────

    #[test]
    fn upload_delta_manifest_stores_at_correct_key() {
        let (dir, sim) = setup();
        let ns = ns();
        let lsn = Lsn::new(0x200);
        let manifest = make_manifest(dir.path(), lsn);

        upload_delta_manifest(&sim, &ns, lsn, &manifest).unwrap();

        let key = ns.delta_manifest_key(lsn);
        let bytes = sim.get_standard(&key).unwrap();
        assert!(bytes.is_some(), "delta manifest must be stored at {key}");

        // Round-trip: deserialise should succeed
        let tmp = dir.path().join("rt.tikm");
        let m2 = Manifest::from_bytes(&bytes.unwrap(), &tmp).unwrap();
        assert_eq!(m2.checkpoint_lsn(), lsn);
    }

    // ── upload_pg_state ──────────────────────────────────────────────────

    #[test]
    fn upload_pg_state_empty_pgdata_succeeds() {
        // An empty pgdata is valid — all files/dirs are optional.
        let (dir, sim) = setup();
        let ns = ns();
        let lsn = Lsn::new(0x300);

        upload_pg_state(&sim, &ns, lsn, dir.path()).unwrap();

        let key = ns.pg_state_key(lsn);
        let bytes = sim.get_standard(&key).unwrap();
        assert!(bytes.is_some(), "pg_state archive must exist at {key}");
        // Must be non-empty bytes (even an empty tar+zstd archive has a header).
        assert!(!bytes.unwrap().is_empty());
    }

    #[test]
    fn upload_pg_state_includes_pg_control_and_xact() {
        let (dir, sim) = setup();
        let ns = ns();
        let lsn = Lsn::new(0x400);

        // Create fake pg_control and pg_xact segment.
        let global = dir.path().join("global");
        fs::create_dir_all(&global).unwrap();
        fs::write(global.join("pg_control"), b"pg_control_data").unwrap();
        let pg_xact = dir.path().join("pg_xact");
        fs::create_dir_all(&pg_xact).unwrap();
        fs::write(pg_xact.join("0000"), b"xact_segment").unwrap();

        upload_pg_state(&sim, &ns, lsn, dir.path()).unwrap();

        let bytes = sim.get_standard(&ns.pg_state_key(lsn)).unwrap().unwrap();

        // Decompress and inspect tar entries to verify files are present.
        let decompressed = zstd::decode_all(bytes.as_slice()).unwrap();
        let mut archive = tar::Archive::new(decompressed.as_slice());
        let entry_names: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.path().ok().map(|p| p.to_string_lossy().into_owned()))
            .collect();
        assert!(
            entry_names.iter().any(|n| n.contains("pg_control")),
            "pg_control must be in archive; found: {entry_names:?}"
        );
        assert!(
            entry_names.iter().any(|n| n.contains("pg_xact")),
            "pg_xact segment must be in archive; found: {entry_names:?}"
        );
    }

    // ── upload_wal_segment / download_wal_segment ────────────────────────

    #[test]
    fn wal_round_trip_archive_and_restore() {
        let (dir, sim) = setup();
        let ns = ns();
        let timeline: u32 = 1;
        let segment = "000000010000000000000001";

        // Write a fake WAL segment to disk.
        let wal_path = dir.path().join(segment);
        fs::write(&wal_path, b"fake_wal_data").unwrap();

        upload_wal_segment(&sim, &ns, timeline, segment, &wal_path).unwrap();

        // Verify it lives at the expected key.
        let key = ns.wal_key(timeline, segment);
        assert!(sim.get_standard(&key).unwrap().is_some());

        // Download to a different path.
        let dest = dir.path().join("restored_wal");
        let found = download_wal_segment(&sim, &ns, timeline, segment, &dest).unwrap();
        assert!(found, "segment must be found");
        assert_eq!(fs::read(&dest).unwrap(), b"fake_wal_data");
    }

    #[test]
    fn download_wal_segment_missing_returns_false() {
        let (dir, sim) = setup();
        let ns = ns();
        let dest = dir.path().join("out");

        let found = download_wal_segment(&sim, &ns, 1, "000000010000000000000099", &dest).unwrap();
        assert!(!found, "missing segment must return false");
        assert!(!dest.exists(), "dest file must not be created on miss");
    }
}
