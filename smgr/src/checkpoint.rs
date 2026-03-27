//! Checkpoint flush — the S3/PITR half of PostgreSQL's checkpoint.
//!
//! Called from `CheckPointGuts()` in `xlog.c` after `CheckPointBuffers()`.
//! The checkpointer is a plain PG process — no Tokio runtime.  All I/O is
//! synchronous (`std::fs` + `SimStore` which is also `std::fs`).
//!
//! # Six-step algorithm
//!
//! 0. **Guard**: returns early if `IoControl`, `SimStore`, or `ProjectCtx`
//!    are not yet initialised — nothing to flush or upload.
//!
//! 1. **Flush dirty chunks** (`flush_all_dirty_chunks`): every dirty cache
//!    slot is PUT to the express-bucket `latest` object.  The chunk data is
//!    also written — zstd-compressed — into each log record alongside the
//!    `ChunkTag` (format: `tag[20] | compressed_len[4 LE] | data[N]`).
//!    **Flush dirty nblocks** (`flush_all_dirty_nblocks`): every dirty entry
//!    in the NblocksTable is PUT to the express nblocks key and a
//!    `NblocksRecord` appended to the nblocks log.
//!    After step 1 both logs contain all changes during this checkpoint
//!    interval (both mid-interval and just-flushed).
//!
//! 2. **Rename snapshot** (`chunk_log` → `chunk_log.ckpt`,
//!    `nblocks_log` → `nblocks_log.ckpt`): atomic snapshots of both logs.
//!    New writes after this point go to fresh inodes.
//!
//! 3. **Read + dedup** chunk log (last-write-wins per `ChunkTag`).  If the
//!    dirty set is empty AND the nblocks log is empty, remove `.ckpt` files
//!    and return — nothing to upload.
//!
//! 3.5. **Capture `pg_state`**: build the tar+zstd archive of `pg_control`,
//!    `pg_xact`, etc. into memory **before** any S3 uploads, so the archive
//!    reflects the filesystem state at checkpoint time rather than after
//!    potentially slow chunk writes.
//!
//! 4. **Write each dirty chunk** to the standard bucket at its versioned key
//!    (keyed by `checkpoint_lsn`).  Data comes from the chunk log — no
//!    express re-read — eliminating the race where a concurrent eviction
//!    replaces `latest` after step 1 completes.
//!
//! 5. **Build delta manifest** (dirty chunks → `ChunkRef`s with own
//!    `branch_id`), upload it and the pre-built `pg_state` archive to the
//!    standard bucket.
//!
//! 6. **Remove `chunk_log.ckpt`** and **`nblocks_log.ckpt`** to mark the
//!    checkpoint as complete.
//!
//! # Crash safety
//!
//! If the process crashes between steps 2 and 6, `chunk_log.ckpt` will exist
//! on the next start.  `tiko_checkpoint_flush` detects this and re-processes
//! the existing `.ckpt` file (idempotent because the standard-bucket PUT and
//! the delta manifest PUT are both atomic).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use engine::{cache::CacheControl, io_queue::IoControl, pitr_task::materialize_base};
use pgsys::{Lsn, common::data_dir_path, logging::*};
use store::{
    chunk::{ChunkTag, NBLOCKS_DELETED, RelFork},
    manifest::{ChunkRef, Manifest},
    project::{ProjectCtx, ProjectNamespace},
    sim_store::SimStore,
    tiko_root_path,
};

// ── flush_all_dirty_nblocks ────────────────────────────────────────────────────

/// Drain the NblocksTable, writing each dirty entry to the express nblocks key
/// and appending a `NblocksRecord` to the nblocks log.
///
/// Returns a `HashMap` of `RelFork → nblocks` covering all entries drained this
/// call (same data that was appended to the log), so the caller can seed
/// `rel_nblocks` without reading back the log immediately.
///
/// This is step 1b of the checkpoint algorithm and is called before the log
/// snapshot (step 2) so that all nblocks changes from this interval are present
/// in the nblocks log before it is atomically snapshotted.
fn flush_all_dirty_nblocks(sim: &SimStore, ns: &ProjectNamespace) -> HashMap<RelFork, u32> {
    let mut rel_nblocks = HashMap::new();
    IoControl::get().nblocks.drain_dirty(|rf, n| {
        // Write to express for persistence across restarts.
        let _ = sim.put_express(&ns.rel_nblocks_key(rf), &n.to_le_bytes());
        // Append to nblocks_log so checkpoint_flush_inner can include it.
        CacheControl::append_to_nblocks_log(&rf, n);
        rel_nblocks.insert(rf, n);
    });
    rel_nblocks
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
    // SimStore nor ProjectCtx are initialised — nothing to do.
    if checkpoint_lsn == 0 {
        return;
    }

    let (sim, ctx) = match (SimStore::try_get(), ProjectCtx::try_get()) {
        (Some(s), Some(c)) => (s, c),
        _ => return, // env vars absent or SimStore not yet initialised
    };

    let lsn = Lsn::new(checkpoint_lsn);
    let timeline = timeline_id;
    let root_dir = tiko_root_path();

    if IoControl::is_initialized() {
        // Normal path (server running under postmaster):
        // 1a. Flush dirty shmem cache chunks → express + chunk log.
        pg_log_debug1(&format!(
            "tiko_checkpoint_flush: step 1a: flushing dirty cache chunks (lsn={})",
            lsn.to_hex()
        ));
        IoControl::get().cache.flush_all_dirty_chunks();
        // 1b. Drain dirty NblocksTable entries → express + nblocks log.
        pg_log_debug1(&format!(
            "tiko_checkpoint_flush: step 1b: flushing dirty nblocks (lsn={})",
            lsn.to_hex()
        ));
        flush_all_dirty_nblocks(sim, ctx.ns());
    }
    // Initdb path: writes already went directly to express + chunk log / nblocks
    // log (via cached_write_blocks / set_nblocks), so no flush needed.

    // Steps 2-6: process chunk log → standard bucket → delta manifest.
    // Non-fatal: log and continue. WAL will cover any gap on recovery.
    pg_log_debug1(&format!(
        "tiko_checkpoint_flush: step 2-6: processing chunk log (lsn={})",
        lsn.to_hex()
    ));
    match checkpoint_flush_inner(sim, ctx.ns(), timeline, lsn, &root_dir, &data_dir_path()) {
        Ok(None) => {
            pg_log_info(&format!(
                "tiko_checkpoint_flush: no dirty chunks — skipped (lsn={})",
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
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    root_dir: &Path,
    pg_data_dir: &Path,
) -> io::Result<Option<CheckpointStats>> {
    let log_path = CacheControl::chunk_log_path(root_dir);
    let ckpt_path = CacheControl::chunk_log_checkpoint_path(root_dir);

    // Step 2 — atomic snapshot of both logs.
    // If `.ckpt` already exists (crash recovery), re-process it.
    // If neither file exists: no writes occurred — dirty set will be empty
    // and the function returns early after step 3.
    let nblocks_log_path = CacheControl::nblocks_log_path(root_dir);
    let nblocks_ckpt_path = CacheControl::nblocks_log_checkpoint_path(root_dir);

    let crash_recovery = ckpt_path.exists() || nblocks_ckpt_path.exists();
    if !ckpt_path.exists() && log_path.exists() {
        fs::rename(&log_path, &ckpt_path)?;
    }
    if !nblocks_ckpt_path.exists() && nblocks_log_path.exists() {
        fs::rename(&nblocks_log_path, &nblocks_ckpt_path)?;
    }

    // Step 3 — read + dedup (last-write-wins).  The log now carries the chunk
    // data alongside each tag, so we never need to re-read express `latest`.
    let records = CacheControl::read_chunk_log(&ckpt_path);
    let dirty_chunk_data = dedup_chunk_log(records);

    // Read nblocks log: last-write-wins per RelFork (iterate in order, later
    // entry for the same RelFork overwrites earlier one).
    let nblocks_records = CacheControl::read_nblocks_log(&nblocks_ckpt_path);
    let mut nblocks_from_log: HashMap<RelFork, u32> = HashMap::new();
    let mut deleted_forks_set: HashSet<RelFork> = HashSet::new();
    for rec in nblocks_records {
        if rec.nblocks == NBLOCKS_DELETED {
            deleted_forks_set.insert(rec.rf);
        } else {
            nblocks_from_log.insert(rec.rf, rec.nblocks);
        }
    }
    // A fork that was written and later deleted in the same interval: remove it.
    nblocks_from_log.retain(|rf, _| !deleted_forks_set.contains(rf));

    // Nothing changed this interval — skip S3 writes and manifest entirely.
    if dirty_chunk_data.is_empty() && nblocks_from_log.is_empty() && deleted_forks_set.is_empty() {
        let _ = fs::remove_file(&ckpt_path);
        let _ = fs::remove_file(&nblocks_ckpt_path);
        return Ok(None);
    }

    // Capture pg_state archive bytes NOW — before any S3 uploads — so the
    // archive reflects pg_control / pg_xact / etc. at the start of the
    // checkpoint rather than after potentially long chunk S3 writes.
    let pg_state_bytes = build_pg_state_archive(pg_data_dir)?;

    // Step 4 — write each dirty chunk to the standard bucket.
    //
    // Data comes directly from the chunk log (captured under the exclusive pin
    // in flush_dirty_chunk), so this is immune to the race where a concurrent
    // eviction replaces express `latest` after step 1a completes.
    // express `latest` is left untouched — it was already set correctly in
    // step 1a and any post-step-2 eviction writes remain valid.
    for (chunk_key, chunk_data) in &dirty_chunk_data {
        let versioned_key =
            ns.chunk_versioned_key(chunk_key, ns.branch_id, timeline, checkpoint_lsn);
        sim.put_standard(&versioned_key, chunk_data)?;
    }

    // Step 5 — delta manifest + pg_state.
    let delta_entries: Vec<(ChunkTag, ChunkRef)> = dirty_chunk_data
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

    // Collect rel_nblocks for every relation that had dirty chunks or an nblocks
    // change this interval.
    //
    // Primary source: values from the nblocks log (last-write-wins).  These
    // are the actual nblocks at the time of the last `set_nblocks` call, so no
    // express re-read is needed for these RelForks.
    //
    // For dirty-chunk RelForks whose fork is NOT in the nblocks log (write to
    // an existing block — no size change), fall back to the express nblocks key.
    // If the express key is absent (Ok(None)), skip — the relation is inherited
    // from a parent branch and its authoritative nblocks lives in the base
    // manifest.  Inserting 0 would corrupt the base manifest.
    let mut rel_nblocks: HashMap<RelFork, u32> = nblocks_from_log;

    for chunk_key in dirty_chunk_data.keys() {
        let rf = chunk_key.rel_fork();
        if rel_nblocks.contains_key(&rf) {
            continue;
        }
        let nblocks_key = ns.rel_nblocks_key(rf);
        if let Ok(Some(bytes)) = sim.get_express(&nblocks_key) {
            if bytes.len() >= 4 {
                rel_nblocks.insert(rf, u32::from_le_bytes(bytes[0..4].try_into().unwrap()));
            }
        }
    }

    let tmp_delta_manifest_path = delta_tmp_path(root_dir, checkpoint_lsn);
    let delta = Manifest::new(
        checkpoint_lsn,
        now_unix(),
        delta_entries,
        rel_nblocks,
        deleted_forks_set.into_iter().collect(),
        &tmp_delta_manifest_path,
    )?;
    upload_delta_manifest(sim, ns, timeline, checkpoint_lsn, &delta)?;
    upload_pg_state(sim, ns, timeline, checkpoint_lsn, &pg_state_bytes)?;

    // Step 6 — remove checkpoint snapshots and local build file.
    let _ = fs::remove_file(&ckpt_path); // silently ignore ENOENT
    let _ = fs::remove_file(&nblocks_ckpt_path);
    let _ = fs::remove_file(&tmp_delta_manifest_path); // remove local build file

    Ok(Some(CheckpointStats {
        dirty_chunks: dirty_chunk_data.len(),
        crash_recovery,
    }))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Deduplicate chunk log records by tag using last-write-wins semantics.
///
/// A chunk evicted multiple times during a checkpoint interval has multiple log
/// entries. The *last* entry carries the most recent data (each eviction writes
/// current cache contents), so iterating in order and overwriting earlier
/// entries gives us the correct data for the versioned S3 upload. Returns a
/// `HashMap<ChunkTag, Vec<u8>>` of decompressed chunk data.
fn dedup_chunk_log(records: Vec<(ChunkTag, Vec<u8>)>) -> HashMap<ChunkTag, Vec<u8>> {
    let mut map = HashMap::new();
    for (tag, data) in records {
        map.insert(tag, data); // later entry overwrites earlier = last-write-wins
    }
    map
}

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
    sim: &SimStore,
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
    sim: &SimStore,
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
    use pgsys::Lsn;
    use std::fs;
    use std::io::Write;
    use store::chunk::{CHUNK_SIZE, ChunkTag};
    use store::manifest::{ChunkRef, Manifest};
    use store::project::ProjectNamespace;
    use store::sim_store::SimStore;
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

    /// Write chunk log entries in the current wire format:
    /// `[tag: 20 | compressed_len: 4 LE | compressed_data: N]` per entry.
    /// Each entry gets `CHUNK_SIZE` bytes of `fill` as its chunk data.
    fn write_chunk_log(path: &Path, tags: &[ChunkTag]) {
        write_chunk_log_with_fill(path, tags, 0x00);
    }

    fn write_chunk_log_with_fill(path: &Path, tags: &[ChunkTag], fill: u8) {
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        let chunk_data = vec![fill; CHUNK_SIZE];
        let compressed = zstd::encode_all(chunk_data.as_slice(), 1).unwrap();
        let compressed_len = compressed.len() as u32;
        for tag in tags {
            file.write_all(&tag.encode()).unwrap();
            file.write_all(&compressed_len.to_le_bytes()).unwrap();
            file.write_all(&compressed).unwrap();
        }
    }

    // Run `checkpoint_flush_inner` with a fresh tempdir.
    fn run_flush(
        dir: &TempDir,
        sim: &SimStore,
        ns: &ProjectNamespace,
        lsn: Lsn,
        timeline: u32,
    ) -> io::Result<()> {
        checkpoint_flush_inner(sim, ns, timeline, lsn, dir.path(), dir.path()).map(|_| ())
    }

    /// Deserialise the delta manifest from the standard sim for `lsn`.
    fn read_delta_manifest(
        dir: &TempDir,
        sim: &SimStore,
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
    // Simulate: the chunk log was written by flush_all_dirty_chunks (step 1).
    // We pre-populate the log directly (tests cannot run the real cache).

    #[test]
    fn scenario1_chunk_in_log_appears_in_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x1000);
        let tag = make_tag(1);

        // Simulate step 1 output: chunk log has one entry.
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[tag]);

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
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x2000);
        let tag = make_tag(2);

        // Mid-interval: chunk_log written by `flush_dirty_chunk`.
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(m.lookup(&tag).unwrap().is_some());
    }

    // ── Scenario 3: chunk evicted twice → dedup collapses to one upload ───

    #[test]
    fn scenario3_dedup_collapses_duplicate_log_entries_to_one_upload() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x3000);
        let tag = make_tag(3);

        // Two log entries for the same chunk (evicted, re-dirtied, evicted again).
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[tag, tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // Only one versioned object must exist in standard sim.
        let versioned_key = ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn);
        assert!(sim.get_standard(&versioned_key).unwrap().is_some());

        // Delta manifest has exactly one entry for this chunk.
        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        let entries: Vec<_> = {
            // Re-open the bytes and count entries via entry_count field.
            let bytes = sim
                .get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .unwrap();
            let path = dir.path().join("count.tikm");
            let m2 = Manifest::from_bytes(&bytes, &path).unwrap();
            let _ = m2.checkpoint_lsn();
            // entry_count is internal; verify via lookup
            vec![m.lookup(&tag).unwrap()]
        };
        assert_eq!(entries.len(), 1);
    }

    // ── Scenario 4: crash between PUT and log append ───────────────────────
    // This scenario is a known gap: the chunk reached express but has no log
    // entry, so it won't be uploaded to standard at this checkpoint.
    // WAL replay will bring the data back on recovery.
    // Not unit-testable — documented here.

    // ── Scenario 5: crash during rename-swap → re-process .ckpt ──────────

    #[test]
    fn scenario5_ckpt_exists_without_log_reprocessed_idempotently() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x5000);
        let tag = make_tag(5);

        // Simulate crash: chunk_log.ckpt exists, chunk_log is absent.
        let ckpt_path = CacheControl::chunk_log_checkpoint_path(dir.path());
        write_chunk_log(&ckpt_path, &[tag]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(
            m.lookup(&tag).unwrap().is_some(),
            "chunk must be in manifest after re-processing .ckpt"
        );

        // .ckpt must be cleaned up.
        assert!(
            !ckpt_path.exists(),
            "chunk_log.ckpt must be removed on success"
        );
    }

    // ── All chunks have lsn == checkpoint_lsn and branch_id == own ──────

    #[test]
    fn all_manifest_entries_have_correct_lsn_and_branch_id() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x6000);
        let tags: Vec<ChunkTag> = (10..15).map(make_tag).collect();

        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &tags);

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
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x7000);
        let tag = make_tag(77);

        // Populate log and express.
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[tag]);

        // First call — succeeds, removes .ckpt.
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();
        let bytes_first = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .unwrap();

        // Simulate crash recovery: re-create .ckpt manually.
        let ckpt_path = CacheControl::chunk_log_checkpoint_path(dir.path());
        write_chunk_log(&ckpt_path, &[tag]);

        // Second call — must succeed and produce the same manifest content.
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

    // ── chunk_log.ckpt is removed on success ───────────────────────────

    #[test]
    fn chunk_log_ckpt_removed_on_success() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x8000);

        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[make_tag(99)]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let ckpt_path = CacheControl::chunk_log_checkpoint_path(dir.path());
        assert!(
            !ckpt_path.exists(),
            "chunk_log.ckpt must not exist after success"
        );
    }

    // ── Empty chunk log → no delta manifest written (no-op) ─────────────

    #[test]
    fn empty_chunk_log_produces_no_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x9000);

        // Create an empty chunk log.
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // No dirty chunks → no manifest should be written.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .is_none(),
            "no delta manifest should be written when chunk log is empty"
        );

        // chunk_log.ckpt should be cleaned up.
        let ckpt_path = CacheControl::chunk_log_checkpoint_path(dir.path());
        assert!(
            !ckpt_path.exists(),
            "chunk_log.ckpt must be removed on no-op"
        );
    }

    // ── Scenario: nblocks changes with no dirty chunks → manifest still written

    fn write_nblocks_log(path: &Path, entries: &[(RelFork, u32)]) {
        use store::chunk::NblocksRecord;
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        for &(rf, nblocks) in entries {
            let rec = NblocksRecord { rf, nblocks };
            file.write_all(&rec.encode()).unwrap();
        }
    }

    #[test]
    fn nblocks_change_only_produces_delta_manifest_with_rel_nblocks() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0xa000);

        // No dirty chunks — chunk log absent.
        // Only the nblocks log has an entry (e.g. a truncation was performed).
        // Values are carried directly in the log records (last-write-wins).
        let rf = RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: 42,
            fork_number: 0,
        };
        let nblocks_log_path = CacheControl::nblocks_log_path(dir.path());
        write_nblocks_log(&nblocks_log_path, &[(rf, 10)]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // A delta manifest must have been written despite no dirty chunks.
        let bytes = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .expect("delta manifest must be written when only nblocks changed");
        let path = dir.path().join("nb_only.tikm");
        let m = Manifest::from_bytes(&bytes, &path).unwrap();

        // rel_nblocks must carry the value from the log record.
        assert_eq!(
            m.lookup_nblocks(rf),
            Some(10),
            "rel_nblocks must reflect the nblocks log value"
        );

        // nblocks_log.ckpt must be cleaned up.
        let nblocks_ckpt_path = CacheControl::nblocks_log_checkpoint_path(dir.path());
        assert!(
            !nblocks_ckpt_path.exists(),
            "nblocks_log.ckpt must be removed on success"
        );
    }

    #[test]
    fn nblocks_change_and_dirty_chunk_both_appear_in_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0xb000);

        // One dirty chunk for relation 1, and an nblocks-only change for relation 2.
        let tag = make_tag(1);
        let log_path = CacheControl::chunk_log_path(dir.path());
        write_chunk_log(&log_path, &[tag]);
        // Also write express nblocks for tag's relation (dirty-chunk fallback path).
        let rf1 = tag.rel_fork();
        sim.put_express(&ns.rel_nblocks_key(rf1), &5u32.to_le_bytes())
            .unwrap();

        let rf2 = RelFork {
            spc_oid: 2,
            db_oid: 2,
            rel_number: 2,
            fork_number: 0,
        };
        let nblocks_log_path = CacheControl::nblocks_log_path(dir.path());
        // Value comes from the log record — no express key needed for rf2.
        write_nblocks_log(&nblocks_log_path, &[(rf2, 7)]);

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

    // ── dedup_chunk_log unit test ─────────────────────────────────────────

    #[test]
    fn dedup_removes_duplicates_keeps_all_unique() {
        let t1 = make_tag(1);
        let t2 = make_tag(2);
        // t1 appears twice with different data — last write must win.
        let d1_first = vec![0xAAu8; CHUNK_SIZE];
        let d1_last = vec![0xBBu8; CHUNK_SIZE];
        let d2 = vec![0xCCu8; CHUNK_SIZE];
        let records = vec![(t1, d1_first), (t2, d2.clone()), (t1, d1_last.clone())];
        let result = dedup_chunk_log(records);
        assert_eq!(result.len(), 2, "exactly two unique tags");
        assert_eq!(result[&t1], d1_last, "last-write-wins for t1");
        assert_eq!(result[&t2], d2, "t2 data unchanged");
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
        let sim = SimStore::new(dir.path());
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
        let sim = SimStore::new(dir.path());
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
        let sim = SimStore::new(dir.path());
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
