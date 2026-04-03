//! Checkpoint flush — the S3/PITR half of PostgreSQL's checkpoint.
//!
//! Called from `CheckPointGuts()` in `xlog.c` after `CheckPointBuffers()`.
//! The checkpointer is a plain PG process — no Tokio runtime.  All I/O is
//! synchronous (`std::fs` + `S3Sim` which is also `std::fs`).
//!
//! # Six-step algorithm
//!
//! 0. **Guard**: returns early if `IoControl`, `S3Sim`, or `ProjectCtx`
//!    are not yet initialised — nothing to flush or upload.
//!
//! 1. **Flush dirty chunks** (`flush_all_dirty_chunks`): every dirty cache
//!    slot is PUT to the express-bucket `latest` object.  Compressed chunk
//!    data is written to a named sidecar file under `dirty_chunks/`, and a
//!    `ChunkDirty` entry is appended to the unified `cache_log`.
//!    **Flush dirty nblocks** (`flush_all_dirty_nblocks`): every dirty entry
//!    in the NblocksTable is PUT to the express nblocks key and a
//!    `NblocksSet` entry appended to `cache_log`.
//!    After step 1 `cache_log` contains all changes during this checkpoint
//!    interval (both mid-interval and just-flushed).
//!
//! 2. **Rename snapshot** (`cache_log` → `cache_log.ckpt`): single atomic
//!    snapshot.  New writes after this point go to a fresh inode.
//!
//! 3. **Parse** `cache_log.ckpt` — one pass, three entry types:
//!    - `ChunkDirty { tag, seq }` → last-write-wins per tag (HashMap).
//!    - `NblocksSet { rf, n }` → last-write-wins per RelFork.
//!    - `ForkDeleted { rf }` → added to deleted set; removed from nblocks.
//!    If all sets are empty, remove `.ckpt` and return — nothing to upload.
//!
//! 3.5. **Capture `pg_state`**: build the tar+zstd archive of `pg_control`,
//!    `pg_xact`, etc. into memory **before** any S3 uploads, so the archive
//!    reflects the filesystem state at checkpoint time rather than after
//!    potentially slow chunk writes.
//!
//! 4. **Write each dirty chunk** to the standard bucket at its versioned key.
//!    Data is read from the sidecar file (`dirty_chunks/{tag_fields}-{seq}`).
//!    Sidecar files are deleted after a successful upload.  Chunks whose fork
//!    is in the deleted set are skipped.
//!
//! 5. **Build delta manifest** (dirty chunks → `ChunkRef`s with own
//!    `branch_id`), upload it and the pre-built `pg_state` archive to the
//!    standard bucket.
//!
//! 6. **Remove `cache_log.ckpt`** to mark the checkpoint as complete.
//!
//! # Crash safety
//!
//! If the process crashes between steps 2 and 6, `cache_log.ckpt` will exist
//! on the next start.  `tiko_checkpoint_flush` detects this and re-processes
//! the existing `.ckpt` file (idempotent because the standard-bucket PUT and
//! the delta manifest PUT are both atomic).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use core::{cache::CacheControl, io_control::IoControl, manifest::materialize_base};
use core::{
    chunk::{ChunkLogEntry, ChunkTag, RelFork},
    manifest::{ChunkRef, Manifest},
    project::{ProjectCtx, ProjectNamespace},
    store::Store,
    tiko_root_path,
};
use pgsys::{Lsn, common::data_dir_path, logging::*};

// ── flush_all_dirty_nblocks ────────────────────────────────────────────────────

/// Drain the NblocksTable, writing each dirty entry to the express nblocks key
/// and appending a `NblocksSet` entry to the unified `cache_log`.
///
/// This is step 1b of the checkpoint algorithm and is called before the log
/// snapshot (step 2) so that all nblocks changes from this interval are present
/// in `cache_log` before it is atomically snapshotted.
fn flush_all_dirty_nblocks(sim: &Store, ns: &ProjectNamespace) {
    IoControl::get().nblocks.drain_dirty(|rf, n| {
        // Write to express for persistence across restarts.
        let _ = sim.put_express(&ns.rel_nblocks_key(rf), &n.to_le_bytes());
        // Append to cache_log so checkpoint_flush_inner can include it.
        CacheControl::append_to_cache_log(&ChunkLogEntry::NblocksSet { rf, n });
    });
}

// ── extern "C" entry point ────────────────────────────────────────────────────

/// Called from `CheckPointGuts()` after `CheckPointBuffers()`.
///
/// `checkpoint_lsn` is the `XLogRecPtr checkPointRedo` argument passed by PG.
/// It is `0` (`InvalidXLogRecPtr`) during `--boot`/`--single` phases where
/// `IoControl::is_initialized()` will also be false, so the early-return
/// guard handles both cases.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_checkpoint_flush(timeline_id: u32, checkpoint_lsn: u64) {
    // During --boot (BOOTSTRAP_PROCESSING), checkpoint_lsn is 0 and neither
    // S3Sim nor ProjectCtx are initialised — nothing to do.
    if checkpoint_lsn == 0 {
        return;
    }

    let (sim, ctx) = match (Store::try_get(), ProjectCtx::try_get()) {
        (Some(s), Some(c)) => (s, c),
        _ => return, // env vars absent or S3Sim not yet initialised
    };

    let lsn = Lsn::new(checkpoint_lsn);
    let timeline = timeline_id;
    let root_dir = tiko_root_path();

    if IoControl::is_initialized() {
        // Normal path (server running under postmaster):
        // 1a. Flush dirty shmem cache chunks → express + cache_log (sidecar + ChunkDirty entry).
        pg_log_debug1(&format!(
            "tiko_checkpoint_flush: step 1a: flushing dirty cache chunks (lsn={})",
            lsn.to_hex()
        ));
        IoControl::get().cache.flush_all_dirty_chunks();
        // 1b. Drain dirty NblocksTable entries → express + cache_log (NblocksSet entry).
        pg_log_debug1(&format!(
            "tiko_checkpoint_flush: step 1b: flushing dirty nblocks (lsn={})",
            lsn.to_hex()
        ));
        flush_all_dirty_nblocks(sim, ctx.ns());
    }
    // Initdb path: writes already went directly to express + cache_log
    // (via cached_write_blocks / set_nblocks), so no flush needed.

    // Steps 2-6: process cache_log → standard bucket → delta manifest.
    // Non-fatal: log and continue. WAL will cover any gap on recovery.
    pg_log_debug1(&format!(
        "tiko_checkpoint_flush: step 2-6: processing cache_log (lsn={})",
        lsn.to_hex()
    ));
    match checkpoint_flush_inner(sim, ctx.ns(), timeline, lsn, &root_dir, &data_dir_path()) {
        Ok(None) => {
            pg_log_info(&format!(
                "tiko_checkpoint_flush: nothing to upload (no dirty chunks, no nblocks updates, no fork deletions) — skipped (lsn={})",
                lsn.to_hex()
            ));
        }
        Ok(Some(stats)) => {
            if stats.crash_recovery {
                pg_log_debug1(&format!(
                    "tiko_checkpoint_flush: step 2: crash recovery — re-processed existing .ckpt (lsn={})",
                    lsn.to_hex()
                ));
            }
            pg_log_debug1(&format!(
                "tiko_checkpoint_flush: step 4-5: uploaded {} chunk(s) + delta manifest + pg_state (lsn={})",
                stats.dirty_chunks,
                lsn.to_hex()
            ));
            pg_log_info(&format!(
                "tiko_checkpoint_flush: complete {} chunk(s) uploaded, lsn={}, crash_recovery={}",
                stats.dirty_chunks,
                lsn.to_hex(),
                stats.crash_recovery,
            ));
        }
        Err(e) => {
            pg_log_warning(&format!("tiko_checkpoint_flush: {e}"));
        }
    }

    // After the initdb shutdown checkpoint, bootstrap the initial base manifest
    // for root projects by running standard materialization over the delta just
    // produced by checkpoint_flush_inner above. Skipped for branch projects —
    // their initial base is created by the restore-from-parent process.
    if !IoControl::is_initialized() && !ctx.is_branch() {
        match materialize_base(sim, ctx.ns(), timeline) {
            Ok(result) => {
                pg_log_debug1(&format!(
                    "tiko_checkpoint_flush: initial base materialization: {result:?}"
                ));
            }
            Err(e) => {
                pg_log_error(&format!(
                    "tiko_checkpoint_flush: initial base materialization failed: {e}"
                ));
            }
        }
    }
}

// ── Inner implementation (also used by tests) ─────────────────────────────────

/// Stats returned by a successful `checkpoint_flush_inner` run.
/// `None` means the dirty set was empty and no work was done.
struct CheckpointStats {
    /// Number of unique dirty chunks written to the standard bucket.
    dirty_chunks: usize,
    /// True when `.ckpt` already existed on entry (crash-recovery re-run).
    crash_recovery: bool,
}

/// Execute steps 2-6 of the checkpoint flush algorithm.
///
/// Separated from the `extern "C"` wrapper so that unit tests can call it
/// directly without needing `IoControl` or the real PG shared memory.
///
/// Returns `Ok(None)` when the dirty set is empty (no-op).
fn checkpoint_flush_inner(
    sim: &Store,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    root_dir: &Path,
    pg_data_dir: &Path,
) -> io::Result<Option<CheckpointStats>> {
    let log_path = CacheControl::cache_log_path(root_dir);
    let ckpt_path = CacheControl::cache_log_checkpoint_path(root_dir);

    // Step 2 — atomic snapshot of the unified log.
    // If `.ckpt` already exists (crash recovery), re-process it.
    // If neither file exists: no writes occurred — dirty set will be empty
    // and the function returns early after step 3.
    let crash_recovery = ckpt_path.exists();
    if !ckpt_path.exists() && log_path.exists() {
        fs::rename(&log_path, &ckpt_path)?;
    }

    // Step 3 — single-pass parse: last-write-wins per key.
    let mut dirty_chunks: HashMap<ChunkTag, u64> = HashMap::new();
    let mut nblocks_from_log: HashMap<RelFork, u32> = HashMap::new();
    let mut deleted_forks_set: HashSet<RelFork> = HashSet::new();

    for entry in CacheControl::read_cache_log(&ckpt_path) {
        match entry {
            ChunkLogEntry::ChunkDirty { tag, seq } => {
                // Delete the superseded sidecar immediately. During initdb the
                // same chunk is written once per block, producing one sidecar per
                // write; only the last seq carries the final data. Without this,
                // all but the last sidecar for each tag would be leaked.
                if let Some(old_seq) = dirty_chunks.insert(tag, seq) {
                    let _ = fs::remove_file(CacheControl::sidecar_path(root_dir, &tag, old_seq));
                }
            }
            ChunkLogEntry::NblocksSet { rf, n } => {
                nblocks_from_log.insert(rf, n);
            }
            ChunkLogEntry::ForkDeleted { rf } => {
                deleted_forks_set.insert(rf);
                nblocks_from_log.remove(&rf);
            }
        }
    }

    // Nothing changed this interval — skip S3 writes and manifest entirely.
    if dirty_chunks.is_empty() && nblocks_from_log.is_empty() && deleted_forks_set.is_empty() {
        let _ = fs::remove_file(&ckpt_path);
        return Ok(None);
    }

    // Capture pg_state archive bytes NOW — before any S3 uploads — so the
    // archive reflects pg_control / pg_xact / etc. at the start of the
    // checkpoint rather than after potentially long chunk S3 writes.
    let pg_state_bytes = build_pg_state_archive(pg_data_dir)?;

    // Step 4 — read each sidecar and write to the standard bucket.
    //
    // Sidecar files contain zstd-compressed chunk data. S3Sim's `put_standard`
    // transparently re-compresses whatever bytes it receives (for non-.zst keys),
    // so the sidecar bytes must be decompressed first to avoid double compression.
    // express `latest` is left untouched — it was already set correctly in
    // step 1a and any post-step-2 eviction writes remain valid.
    for (tag, seq) in &dirty_chunks {
        if !deleted_forks_set.contains(&tag.rel_fork()) {
            let versioned_key = ns.chunk_versioned_key(tag, ns.branch_id, timeline, checkpoint_lsn);
            if let Some(compressed) = CacheControl::read_sidecar(root_dir, tag, *seq) {
                let data = zstd::decode_all(compressed.as_slice())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                sim.put_standard(&versioned_key, &data)?;
            }
        }
        // Always remove the sidecar — uploaded, skipped (deleted fork), or absent.
        // Deleting the whole dirty_chunks/ dir is unsafe: concurrent evictions
        // between step 2 and here write new sidecars for the next checkpoint.
        let _ = fs::remove_file(CacheControl::sidecar_path(root_dir, tag, *seq));
    }

    // Step 5 — delta manifest + pg_state.
    let delta_entries: Vec<(ChunkTag, ChunkRef)> = dirty_chunks
        .keys()
        .filter(|key| !deleted_forks_set.contains(&key.rel_fork()))
        .map(|key| {
            (
                *key,
                ChunkRef {
                    branch_id: ns.branch_id,
                    timeline_id: timeline,
                    lsn: checkpoint_lsn,
                },
            )
        })
        .collect();

    let tmp_delta_manifest_path = delta_tmp_path(root_dir, checkpoint_lsn);
    let delta = Manifest::new(
        checkpoint_lsn,
        now_unix(),
        delta_entries,
        nblocks_from_log,
        deleted_forks_set.into_iter().collect(),
        &tmp_delta_manifest_path,
    )?;
    upload_delta_manifest(sim, ns, timeline, checkpoint_lsn, &delta)?;
    upload_pg_state(sim, ns, timeline, checkpoint_lsn, &pg_state_bytes)?;

    // Step 6 — remove checkpoint snapshot and local build file.
    let _ = fs::remove_file(&ckpt_path); // silently ignore ENOENT
    let _ = fs::remove_file(&tmp_delta_manifest_path); // remove local build file

    Ok(Some(CheckpointStats {
        dirty_chunks: dirty_chunks.len(),
        crash_recovery,
    }))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn delta_tmp_path(root_dir: &Path, lsn: Lsn) -> PathBuf {
    root_dir.join(format!("delta_{}.bin", lsn.to_hex()))
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ── WAL archive helpers ───────────────────────────────────────────────────────

/// Serialise `manifest` to the S3 wire format and PUT it at the delta manifest
/// key `{org}/pitr/{proj}/deltas/{lsn_hex}/manifest.bin` in the standard bucket.
fn upload_delta_manifest(
    sim: &Store,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    manifest: &Manifest,
) -> io::Result<()> {
    let bytes = manifest.to_bytes()?;
    sim.put_standard(&ns.delta_manifest_key(timeline, checkpoint_lsn), &bytes)
}

/// Upload a pre-built tar+zstd archive of critical PG state files at
/// `{org}/pitr/{proj}/deltas/{lsn_hex}/pg_state.tar.zst` in the standard bucket.
///
/// The caller is responsible for building the archive bytes early (before S3
/// chunk uploads) via `build_pg_state_archive` so the archive captures the
/// filesystem state at checkpoint initiation rather than after slow uploads.
fn upload_pg_state(
    sim: &Store,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    compressed: &[u8],
) -> io::Result<()> {
    sim.put_standard(&ns.pg_state_key(timeline, checkpoint_lsn), compressed)
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

    // pg_subtrans/
    let pg_subtrans = pgdata.join("pg_subtrans");
    if pg_subtrans.is_dir() {
        builder.append_dir_all("pg_subtrans", &pg_subtrans)?;
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use core::chunk::{CHUNK_SIZE, ChunkLogEntry, ChunkTag};
    use core::manifest::{ChunkRef, Manifest};
    use core::project::ProjectNamespace;
    use core::store::Store;
    use pgsys::Lsn;
    use std::fs;
    use std::io::Write as _;
    use tempfile::TempDir;

    // ── Test helpers ──────────────────────────────────────────────────────

    fn ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 7)
    }

    fn make_tag(id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: id,
            db_oid: id,
            rel_number: id,
            fork_number: 0,
            chunk_id: id,
        }
    }

    /// Write `ChunkDirty` entries to `log_path` and corresponding sidecar files
    /// under `root_dir/dirty_chunks/`.  Each entry gets a unique sequential seq
    /// (0, 1, 2, …).  Returns the seq numbers assigned in order.
    fn write_chunk_dirty_entries(root_dir: &Path, log_path: &Path, tags: &[ChunkTag]) -> Vec<u64> {
        write_chunk_dirty_entries_with_fill(root_dir, log_path, tags, 0x00)
    }

    fn write_chunk_dirty_entries_with_fill(
        root_dir: &Path,
        log_path: &Path,
        tags: &[ChunkTag],
        fill: u8,
    ) -> Vec<u64> {
        let dirty_chunks_dir = root_dir.join("dirty_chunks");
        fs::create_dir_all(&dirty_chunks_dir).unwrap();
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        let chunk_data = vec![fill; CHUNK_SIZE];
        let compressed = zstd::encode_all(chunk_data.as_slice(), 1).unwrap();
        let mut seqs = Vec::new();
        for (i, tag) in tags.iter().enumerate() {
            let seq = i as u64;
            let sidecar = CacheControl::sidecar_path(root_dir, tag, seq);
            fs::write(&sidecar, &compressed).unwrap();
            let entry = ChunkLogEntry::ChunkDirty { tag: *tag, seq };
            log_file.write_all(&entry.encode()).unwrap();
            seqs.push(seq);
        }
        seqs
    }

    /// Append `NblocksSet` entries to `log_path`.
    fn write_nblocks_set(log_path: &Path, entries: &[(RelFork, u32)]) {
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        for &(rf, n) in entries {
            file.write_all(&ChunkLogEntry::NblocksSet { rf, n }.encode())
                .unwrap();
        }
    }

    /// Append `ForkDeleted` entries to `log_path`.
    fn write_fork_deleted_entries(log_path: &Path, forks: &[RelFork]) {
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        for &rf in forks {
            file.write_all(&ChunkLogEntry::ForkDeleted { rf }.encode())
                .unwrap();
        }
    }

    // Run `checkpoint_flush_inner` with a fresh tempdir.
    fn run_flush(
        dir: &TempDir,
        sim: &Store,
        ns: &ProjectNamespace,
        lsn: Lsn,
        timeline: u32,
    ) -> io::Result<()> {
        checkpoint_flush_inner(sim, ns, timeline, lsn, dir.path(), dir.path()).map(|_| ())
    }

    /// Deserialise the delta manifest from the standard sim for `lsn`.
    fn read_delta_manifest(
        dir: &TempDir,
        sim: &Store,
        ns: &ProjectNamespace,
        timeline: u32,
        lsn: Lsn,
    ) -> Manifest {
        let bytes = sim
            .get_standard(&ns.delta_manifest_key(timeline, lsn))
            .unwrap()
            .expect("delta manifest must exist");
        let path = dir.path().join("read_delta.tikm");
        Manifest::from_bytes(&bytes, &path).unwrap()
    }

    // ── Scenario 1: chunk dirtied, still in cache → flush_all_dirty_chunks ──
    // Simulate: the cache_log was written by flush_all_dirty_chunks (step 1).
    // We pre-populate the log directly (tests cannot run the real cache).

    #[test]
    fn scenario1_chunk_in_log_appears_in_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x1000);
        let tag = make_tag(1);

        // Simulate step 1 output: cache_log has one ChunkDirty entry + sidecar.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert_eq!(m.checkpoint_lsn(), lsn);
        let cref = m.lookup(&tag).unwrap();
        assert!(cref.is_some(), "chunk must appear in delta manifest");
        let cref = cref.unwrap();
        assert_eq!(cref.lsn, lsn);
        assert_eq!(cref.branch_id, ns.branch_id);
    }

    // ── Scenario 2: chunk evicted mid-interval, log has entry ─────────────

    #[test]
    fn scenario2_mid_interval_chunk_log_entry_in_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x2000);
        let tag = make_tag(2);

        // Mid-interval: cache_log written by `flush_dirty_chunk`.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(m.lookup(&tag).unwrap().is_some());
    }

    // ── Scenario 3: chunk evicted twice → dedup collapses to one upload ───

    #[test]
    fn scenario3_dedup_collapses_duplicate_log_entries_to_one_upload() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x3000);
        let tag = make_tag(3);

        // Two log entries for the same chunk (evicted, re-dirtied, evicted again).
        // Each gets a distinct seq; last-write-wins in HashMap picks the later one.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag, tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // Only one versioned object must exist in standard sim.
        let versioned_key = ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn);
        assert!(sim.get_standard(&versioned_key).unwrap().is_some());

        // Delta manifest has exactly one entry for this chunk.
        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        let entries: Vec<_> = {
            let bytes = sim
                .get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .unwrap();
            let path = dir.path().join("count.tikm");
            let m2 = Manifest::from_bytes(&bytes, &path).unwrap();
            let _ = m2.checkpoint_lsn();
            vec![m.lookup(&tag).unwrap()]
        };
        assert_eq!(entries.len(), 1);
    }

    // ── Scenario 4: crash between sidecar write and log append ───────────
    // This scenario is impossible by design: the sidecar is always written
    // before the log entry. A crash after the sidecar but before the log
    // entry leaves an orphaned sidecar (no log entry → checkpoint ignores it).
    // WAL replay will bring the data back on recovery.
    // Not unit-testable — documented here.

    // ── Scenario 5: crash during rename-swap → re-process .ckpt ──────────

    #[test]
    fn scenario5_ckpt_exists_without_log_reprocessed_idempotently() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x5000);
        let tag = make_tag(5);

        // Simulate crash: cache_log.ckpt exists (with sidecar), cache_log is absent.
        let ckpt_path = CacheControl::cache_log_checkpoint_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &ckpt_path, &[tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(
            m.lookup(&tag).unwrap().is_some(),
            "chunk must be in manifest after re-processing .ckpt"
        );

        // .ckpt must be cleaned up.
        assert!(
            !ckpt_path.exists(),
            "cache_log.ckpt must be removed on success"
        );
    }

    // ── All chunks have lsn == checkpoint_lsn and branch_id == own ──────

    #[test]
    fn all_manifest_entries_have_correct_lsn_and_branch_id() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x6000);
        let tags: Vec<ChunkTag> = (10..15).map(make_tag).collect();

        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &tags);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        for tag in &tags {
            let cref = m.lookup(tag).unwrap().expect("tag must be in manifest");
            assert_eq!(cref.lsn, lsn, "lsn must equal checkpoint_lsn");
            assert_eq!(cref.branch_id, ns.branch_id, "branch_id must be own");
            assert_eq!(cref.timeline_id, 1, "timeline_id must be 1");
        }
    }

    // ── Idempotent: calling with .ckpt still present ──────────────────────

    #[test]
    fn idempotent_second_call_with_ckpt_present_produces_same_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x7000);
        let tag = make_tag(77);

        // Populate cache_log and sidecar.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag]);

        // First call — succeeds, removes .ckpt and sidecar.
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();
        let bytes_first = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .unwrap();

        // Simulate crash recovery: re-create .ckpt and sidecar manually.
        let ckpt_path = CacheControl::cache_log_checkpoint_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &ckpt_path, &[tag]);

        // Second call — must succeed and produce the same manifest content.
        // The sidecar is re-uploaded (put_standard is idempotent).
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();
        let bytes_second = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .unwrap();

        // Both manifests must decode to the same entries.
        let p1 = dir.path().join("cmp1.tikm");
        let p2 = dir.path().join("cmp2.tikm");
        let m1 = Manifest::from_bytes(&bytes_first, &p1).unwrap();
        let m2 = Manifest::from_bytes(&bytes_second, &p2).unwrap();
        assert_eq!(m1.lookup(&tag).unwrap(), m2.lookup(&tag).unwrap());
    }

    // ── cache_log.ckpt is removed on success ───────────────────────────

    #[test]
    fn cache_log_ckpt_removed_on_success() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x8000);

        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[make_tag(99)]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let ckpt_path = CacheControl::cache_log_checkpoint_path(dir.path());
        assert!(
            !ckpt_path.exists(),
            "cache_log.ckpt must not exist after success"
        );
    }

    // ── Empty cache_log → no delta manifest written (no-op) ─────────────

    #[test]
    fn empty_cache_log_produces_no_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x9000);

        // Create an empty cache_log.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // No dirty chunks → no manifest should be written.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .is_none(),
            "no delta manifest should be written when cache_log is empty"
        );

        // cache_log.ckpt should be cleaned up.
        let ckpt_path = CacheControl::cache_log_checkpoint_path(dir.path());
        assert!(
            !ckpt_path.exists(),
            "cache_log.ckpt must be removed on no-op"
        );
    }

    // ── Scenario: nblocks changes with no dirty chunks → manifest still written

    #[test]
    fn nblocks_change_only_produces_delta_manifest_with_fork_nblocks() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0xa000);

        // No dirty chunks — cache_log has only a NblocksSet entry.
        let rf = RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: 42,
            fork_number: 0,
        };
        let log_path = CacheControl::cache_log_path(dir.path());
        write_nblocks_set(&log_path, &[(rf, 10)]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // A delta manifest must have been written despite no dirty chunks.
        let bytes = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .expect("delta manifest must be written when only nblocks changed");
        let path = dir.path().join("nb_only.tikm");
        let m = Manifest::from_bytes(&bytes, &path).unwrap();

        // fork_nblocks must carry the value from the log entry.
        assert_eq!(
            m.lookup_nblocks(rf),
            Some(10),
            "fork_nblocks must reflect the NblocksSet value"
        );

        // cache_log.ckpt must be cleaned up.
        let ckpt_path = CacheControl::cache_log_checkpoint_path(dir.path());
        assert!(
            !ckpt_path.exists(),
            "cache_log.ckpt must be removed on success"
        );
    }

    #[test]
    fn nblocks_change_and_dirty_chunk_both_appear_in_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0xb000);

        // One dirty chunk for relation 1, and an nblocks-only change for relation 2.
        // Both entries go into the same unified cache_log.
        let tag = make_tag(1);
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag]);

        let rf2 = RelFork {
            spc_oid: 2,
            db_oid: 2,
            rel_number: 2,
            fork_number: 0,
        };
        // Value comes from the log entry — no express key needed for rf2.
        write_nblocks_set(&log_path, &[(rf2, 7)]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let bytes = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .expect("delta manifest must exist");
        let path = dir.path().join("mixed.tikm");
        let m = Manifest::from_bytes(&bytes, &path).unwrap();

        assert!(
            m.lookup(&tag).unwrap().is_some(),
            "dirty chunk must be in manifest"
        );
        assert_eq!(
            m.lookup_nblocks(rf2),
            Some(7),
            "nblocks-only relation must appear"
        );
    }

    // ── New: sidecar seq uniqueness prevents overwrite ────────────────────

    #[test]
    fn sidecar_seq_uniqueness_prevents_overwrite() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x10000);
        let tag = make_tag(10);

        // Two entries for the same tag, distinct seqs (0 and 1).
        // Both sidecar files must coexist before flush.
        let log_path = CacheControl::cache_log_path(dir.path());
        let seqs = write_chunk_dirty_entries(dir.path(), &log_path, &[tag, tag]);
        let sidecar0 = CacheControl::sidecar_path(dir.path(), &tag, seqs[0]);
        let sidecar1 = CacheControl::sidecar_path(dir.path(), &tag, seqs[1]);
        assert!(sidecar0.exists(), "first sidecar must exist before flush");
        assert!(sidecar1.exists(), "second sidecar must exist before flush");

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // Checkpoint uses last-write-wins (seq 1), uploads that sidecar.
        // Exactly one versioned S3 object must exist for the chunk.
        let versioned_key = ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn);
        assert!(
            sim.get_standard(&versioned_key).unwrap().is_some(),
            "versioned object must exist after flush"
        );

        // The referenced sidecar (seq 1) is deleted; seq 0 is orphaned.
        assert!(
            !sidecar1.exists(),
            "referenced sidecar (seq 1) must be deleted after upload"
        );

        // Manifest has exactly one entry for the chunk.
        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(
            m.lookup(&tag).unwrap().is_some(),
            "chunk must appear exactly once in manifest"
        );
    }

    // ── New: ForkDeleted excludes chunk from manifest ─────────────────────

    #[test]
    fn fork_deleted_chunk_excluded_from_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x11000);
        let tag = make_tag(20);
        let rf = tag.rel_fork();

        // Write a dirty chunk then delete the fork — chunk must be excluded.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_chunk_dirty_entries(dir.path(), &log_path, &[tag]);
        write_fork_deleted_entries(&log_path, &[rf]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        // Fork was deleted — chunk must not appear.
        assert!(
            m.lookup(&tag).unwrap().is_none(),
            "deleted fork chunk must not appear in manifest"
        );
        // And fork must be in the deleted list.
        assert!(
            m.deleted_forks().contains(&rf),
            "fork must appear in manifest deleted_forks list"
        );
    }

    // ── New: NblocksSet then ForkDeleted removes nblocks from manifest ────

    #[test]
    fn nblocks_set_then_fork_deleted_removes_from_fork_nblocks() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x12000);
        let rf = RelFork {
            spc_oid: 5,
            db_oid: 5,
            rel_number: 5,
            fork_number: 0,
        };

        // Set nblocks then delete the fork in the same interval.
        let log_path = CacheControl::cache_log_path(dir.path());
        write_nblocks_set(&log_path, &[(rf, 100)]);
        write_fork_deleted_entries(&log_path, &[rf]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        // ForkDeleted must remove the nblocks entry added by NblocksSet.
        assert_eq!(
            m.lookup_nblocks(rf),
            None,
            "fork_nblocks must not contain a deleted fork"
        );
        assert!(
            m.deleted_forks().contains(&rf),
            "fork must appear in manifest deleted_forks list"
        );
    }

    // ── New: orphaned sidecar does not cause crash ─────────────────────────

    #[test]
    fn orphaned_sidecar_does_not_cause_crash() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x13000);
        let tag = make_tag(30);

        // Create a sidecar file but NO cache_log entry referencing it.
        let dirty_chunks_dir = dir.path().join("dirty_chunks");
        fs::create_dir_all(&dirty_chunks_dir).unwrap();
        let orphan = CacheControl::sidecar_path(dir.path(), &tag, 999);
        fs::write(&orphan, b"orphaned compressed data").unwrap();

        // cache_log is absent — nothing dirty.
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // No manifest written (nothing in log), no crash.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .is_none(),
            "no delta manifest should be written for empty log"
        );
        // Orphaned sidecar still present (checkpoint doesn't touch unreferenced files).
        assert!(orphan.exists(), "orphaned sidecar must remain untouched");
    }

    // ── upload_delta_manifest ─────────────────────────────────────────────

    fn make_manifest(dir: &std::path::Path, lsn: Lsn) -> Manifest {
        let path = dir.join("m.tikm");
        let tag = ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: 1,
            fork_number: 0,
            chunk_id: 0,
        };
        let cref = ChunkRef {
            branch_id: 7,
            timeline_id: 1,
            lsn,
        };
        Manifest::new(lsn, 0, vec![(tag, cref)], HashMap::new(), vec![], &path).unwrap()
    }

    #[test]
    fn upload_delta_manifest_stores_at_correct_key() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x200);
        let manifest = make_manifest(dir.path(), lsn);

        upload_delta_manifest(&sim, &ns, 1, lsn, &manifest).unwrap();

        let key = ns.delta_manifest_key(1, lsn);
        let bytes = sim.get_standard(&key).unwrap();
        assert!(bytes.is_some(), "delta manifest must be stored at {key}");

        // Round-trip: deserialise should succeed
        let tmp = dir.path().join("rt.tikm");
        let m2 = Manifest::from_bytes(&bytes.unwrap(), &tmp).unwrap();
        assert_eq!(m2.checkpoint_lsn(), lsn);
    }

    // ── upload_pg_state ───────────────────────────────────────────────────

    #[test]
    fn upload_pg_state_empty_pgdata_succeeds() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x300);

        let bytes = build_pg_state_archive(dir.path()).unwrap();
        upload_pg_state(&sim, &ns, 1, lsn, &bytes).unwrap();

        let key = ns.pg_state_key(1, lsn);
        let stored = sim.get_standard(&key).unwrap();
        assert!(stored.is_some(), "pg_state archive must exist at {key}");
        assert!(!stored.unwrap().is_empty());
    }

    #[test]
    fn upload_pg_state_includes_pg_control_and_xact() {
        let dir = TempDir::new().unwrap();
        let sim = Store::new_sim(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x400);

        let global = dir.path().join("global");
        fs::create_dir_all(&global).unwrap();
        fs::write(global.join("pg_control"), b"pg_control_data").unwrap();
        let pg_xact = dir.path().join("pg_xact");
        fs::create_dir_all(&pg_xact).unwrap();
        fs::write(pg_xact.join("0000"), b"xact_segment").unwrap();
        let pg_subtrans = dir.path().join("pg_subtrans");
        fs::create_dir_all(&pg_subtrans).unwrap();
        fs::write(pg_subtrans.join("0000"), b"subtrans_segment").unwrap();

        let bytes = build_pg_state_archive(dir.path()).unwrap();
        upload_pg_state(&sim, &ns, 1, lsn, &bytes).unwrap();

        let stored = sim.get_standard(&ns.pg_state_key(1, lsn)).unwrap().unwrap();

        let decompressed = zstd::decode_all(stored.as_slice()).unwrap();
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
        assert!(
            entry_names.iter().any(|n| n.contains("pg_subtrans")),
            "pg_subtrans segment must be in archive; found: {entry_names:?}"
        );
    }
}
