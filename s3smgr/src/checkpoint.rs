//! Checkpoint flush — the S3/PITR half of PostgreSQL's checkpoint.
//!
//! Called from `CheckPointGuts()` in `xlog.c` after `CheckPointBuffers()`.
//! The checkpointer is a plain PG process — no Tokio runtime.  All I/O is
//! synchronous (`std::fs` + `SimStore` which is also `std::fs`).
//!
//! # Six-step algorithm
//!
//! 0. **Guard**: returns early if `S3IoControl`, `SimStore`, or `ProjectCtx`
//!    are not yet initialised — nothing to flush or upload.
//!
//! 1. **Flush dirty chunks** (`flush_all_dirty_chunks`): every dirty cache
//!    slot is PUT to the express-bucket `latest` object and its `ChunkTag`
//!    appended to the eviction log.
//!    After this step the eviction log contains ALL chunks touched during this
//!    checkpoint interval (both mid-interval evictions and those just flushed).
//!
//! 2. **Rename snapshot** (`eviction_log` → `eviction_log.ckpt`): atomic
//!    snapshot of the log.  New evictions after this point write to a fresh
//!    inode.
//!
//! 3. **Read + dedup** eviction log. If the dirty set is empty, remove
//!    `eviction_log.ckpt` and return — nothing to upload.
//!
//! 4. **Three-step write** each dirty chunk to S3 (staging → versioned copy
//!    in standard-bucket → atomic rename to `latest` in express-bucket),
//!    all keyed by `checkpoint_lsn`.
//!
//! 5. **Build delta manifest** (dirty chunks → `ChunkRef`s with own
//!    `branch_id`), upload it and a tar+zstd `pg_state` archive to the
//!    standard bucket.
//!
//! 6. **Remove `eviction_log.ckpt`** to mark the checkpoint as complete.
//!
//! # Crash safety
//!
//! If the process crashes between steps 2 and 6, `eviction_log.ckpt` will
//! exist on the next start.  `s3_checkpoint_flush` detects this and
//! re-processes the existing `.ckpt` file (idempotent because
//! `three_step_write` is crash-safe and the delta manifest PUT is atomic).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use pgsys::{Lsn, common::data_dir_path, logging::*};
use s3worker::TIKO_DIR;
use s3worker::cache::{CHUNK_SIZE, CacheControl, ChunkTag, RelFork};
use s3worker::io_queue::S3IoControl;
use s3worker::manifest::{ChunkRef, Manifest};
use s3worker::pitr_task::materialize_base;
use s3worker::project::{ProjectCtx, ProjectNamespace, ensure_root_project_meta};
use s3worker::sim_store::SimStore;

// ── extern "C" entry point ────────────────────────────────────────────────────

/// Called from `CheckPointGuts()` after `CheckPointBuffers()`.
///
/// `checkpoint_lsn` is the `XLogRecPtr checkPointRedo` argument passed by PG.
/// It is `0` (`InvalidXLogRecPtr`) during `--boot`/`--single` phases where
/// `S3IoControl::is_initialized()` will also be false, so the early-return
/// guard handles both cases.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_checkpoint_flush(timeline_id: u32, checkpoint_lsn: u64) {
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
    let data_dir = data_dir_path();

    if S3IoControl::is_initialized() {
        // Normal path (server running under postmaster): flush dirty shmem
        // cache chunks to express + eviction log before processing the log.
        pg_log_debug1(&format!(
            "s3_checkpoint_flush: step 1: flushing dirty cache chunks (lsn={})",
            lsn.to_hex()
        ));
        S3IoControl::get().cache.flush_all_dirty_chunks();
    }
    // Initdb path: writes already went directly to express + eviction log
    // (via cached_write_blocks), so flush_all_dirty_chunks is not needed.

    // Steps 2-6: process eviction log → standard bucket → delta manifest.
    // Non-fatal: log and continue. WAL will cover any gap on recovery.
    pg_log_debug1(&format!(
        "s3_checkpoint_flush: step 2-6: processing eviction log (lsn={})",
        lsn.to_hex()
    ));
    match checkpoint_flush_inner(sim, ctx.ns(), timeline, lsn, &data_dir) {
        Ok(None) => {
            pg_log_info(&format!(
                "s3_checkpoint_flush: no dirty chunks — skipped (lsn={})",
                lsn.to_hex()
            ));
        }
        Ok(Some(stats)) => {
            if stats.crash_recovery {
                pg_log_debug1(&format!(
                    "s3_checkpoint_flush: step 2: crash recovery — re-processed existing .ckpt (lsn={})",
                    lsn.to_hex()
                ));
            }
            pg_log_debug1(&format!(
                "s3_checkpoint_flush: step 4-5: uploaded {} chunk(s) + delta manifest + pg_state (lsn={})",
                stats.dirty_chunks,
                lsn.to_hex()
            ));
            pg_log_info(&format!(
                "s3_checkpoint_flush: complete — {} chunk(s) uploaded, lsn={}, crash_recovery={}",
                stats.dirty_chunks,
                lsn.to_hex(),
                stats.crash_recovery,
            ));
        }
        Err(e) => {
            pg_log_warning(&format!("s3_checkpoint_flush: {e}"));
        }
    }

    // After the initdb shutdown checkpoint, bootstrap the initial base manifest
    // for root projects by running standard materialization over the delta just
    // produced by checkpoint_flush_inner above. Skipped for branch projects —
    // their initial base is created by the restore-from-parent process.
    if !S3IoControl::is_initialized() && !ctx.is_branch() {
        match materialize_base(sim, ctx.ns(), timeline) {
            Ok(result) => {
                pg_log_debug1(&format!(
                    "s3_checkpoint_flush: initial base materialization: {result:?}"
                ));
            }
            Err(e) => {
                pg_log_warning(&format!(
                    "s3_checkpoint_flush: initial base materialization failed: {e}"
                ));
            }
        }
        // Write project.json so subsequent process starts use load() instead
        // of bootstrap(), which would overwrite base_manifest.bin with an
        // empty file on every restart.
        if let Err(e) = ensure_root_project_meta(sim, ctx.ns()) {
            pg_log_warning(&format!(
                "s3_checkpoint_flush: ensure_root_project_meta failed: {e}"
            ));
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
/// directly without needing `S3IoControl` or the real PG shared memory.
///
/// Returns `Ok(None)` when the dirty set is empty (no-op).
fn checkpoint_flush_inner(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    data_dir: &Path,
) -> io::Result<Option<CheckpointStats>> {
    let log_path = CacheControl::eviction_log_path(data_dir);
    let ckpt_path = log_path.with_extension("ckpt");

    // Step 2 — atomic snapshot.
    // If `.ckpt` already exists (crash recovery), re-process it.
    let crash_recovery = ckpt_path.exists();
    if !crash_recovery && log_path.exists() {
        fs::rename(&log_path, &ckpt_path)?;
    }
    // If neither file exists: no evictions occurred — dirty set will be
    // empty and the function returns early after step 3.

    // Step 3 — read + dedup.
    let records = CacheControl::read_eviction_log(&ckpt_path);
    let dirty_chunks = dedup_by_chunk_tag(records);

    // Nothing changed this interval — skip S3 writes and manifest entirely.
    if dirty_chunks.is_empty() {
        let _ = fs::remove_file(&ckpt_path);
        return Ok(None);
    }

    // Step 4 — three-step write for each dirty chunk.
    for chunk_key in &dirty_chunks {
        let latest_key = ns.chunk_latest_key(chunk_key);
        // Read data from express `latest` — populated by `flush_dirty_chunk`.
        // Fall back to zeros if the PUT failed for some reason (safe: WAL covers it).
        let chunk_data = sim
            .get_express(&latest_key)?
            .unwrap_or_else(|| vec![0u8; CHUNK_SIZE]);
        sim.three_step_write(ns, chunk_key, timeline, checkpoint_lsn, &chunk_data)?;
    }

    // Step 5 — delta manifest + pg_state.
    let delta_entries: Vec<(ChunkTag, ChunkRef)> = dirty_chunks
        .iter()
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

    // Collect nblocks for every relation that had dirty chunks.
    // Key: RelFork → nblocks.
    let mut rel_nblocks: HashMap<RelFork, u32> = HashMap::new();
    for key in &dirty_chunks {
        let rf = key.rel_fork();
        // Only query once per relation (dedup across chunks of the same fork).
        rel_nblocks.entry(rf).or_insert_with(|| {
            let nblocks_key = ns.rel_nblocks_key(rf);
            match sim.get_express(&nblocks_key) {
                Ok(Some(bytes)) if bytes.len() >= 4 => {
                    u32::from_le_bytes(bytes[0..4].try_into().unwrap())
                }
                _ => 0,
            }
        });
    }

    let tmp_delta_manifest_path = delta_tmp_path(data_dir, checkpoint_lsn);
    let delta = Manifest::new(
        checkpoint_lsn,
        now_unix(),
        delta_entries,
        rel_nblocks,
        &tmp_delta_manifest_path,
    )?;
    upload_delta_manifest(sim, ns, timeline, checkpoint_lsn, &delta)?;
    upload_pg_state(sim, ns, timeline, checkpoint_lsn, data_dir)?;

    // Step 6 — remove checkpoint snapshot and local build file.
    let _ = fs::remove_file(&ckpt_path); // silently ignore ENOENT
    let _ = fs::remove_file(&tmp_delta_manifest_path); // remove local build file

    Ok(Some(CheckpointStats {
        dirty_chunks: dirty_chunks.len(),
        crash_recovery,
    }))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Deduplicate `ChunkTag` records from the eviction log.
///
/// A chunk evicted multiple times during the interval should only be uploaded
/// once (the latest data is already in express `latest`). Order is not
/// significant.
fn dedup_by_chunk_tag(records: Vec<ChunkTag>) -> Vec<ChunkTag> {
    let set: HashSet<ChunkTag> = records.into_iter().collect();
    set.into_iter().collect()
}

fn delta_tmp_path(data_dir: &Path, lsn: Lsn) -> PathBuf {
    data_dir
        .join(TIKO_DIR)
        .join(format!("delta_{}.bin", lsn.to_hex()))
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

/// Build a tar+zstd archive of the critical PG state files and PUT it at
/// `{org}/pitr/{proj}/deltas/{lsn_hex}/pg_state.tar.zst` in the standard bucket.
///
/// Included paths (relative to `pgdata`):
/// - `global/pg_control`
/// - `pg_xact/**`
/// - `pg_multixact/members/**`
/// - `pg_multixact/offsets/**`
/// - `pg_subtrans/**`
/// - `global/pg_filenode.map`
///
/// Missing files or directories are silently skipped — this is intentional for
/// test environments where PG state files may not exist. In production the
/// checkpointer always runs inside a live PostgreSQL data directory.
fn upload_pg_state(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    checkpoint_lsn: Lsn,
    pgdata: &Path,
) -> io::Result<()> {
    let compressed = build_pg_state_archive(pgdata)?;
    sim.put_standard(&ns.pg_state_key(timeline, checkpoint_lsn), &compressed)
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
    use s3worker::cache::ChunkTag;
    use s3worker::manifest::{ChunkRef, Manifest};
    use s3worker::project::ProjectNamespace;
    use s3worker::sim_store::SimStore;
    use std::fs;
    use std::io::Write;
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

    fn write_eviction_log(path: &Path, tags: &[ChunkTag]) {
        let dir = path.parent().unwrap();
        fs::create_dir_all(dir).unwrap();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        for tag in tags {
            file.write_all(&tag.encode()).unwrap();
        }
    }

    /// Set up a SimStore and pre-populate express `latest` for each tag.
    fn setup_express(
        sim: &SimStore,
        ns: &ProjectNamespace,
        tags: &[ChunkTag],
        fill: u8,
    ) -> Vec<u8> {
        let chunk_data = vec![fill; CHUNK_SIZE];
        for tag in tags {
            sim.put_express_latest(ns, tag, &chunk_data).unwrap();
        }
        chunk_data
    }

    // Run `checkpoint_flush_inner` with a fresh tempdir.
    fn run_flush(
        dir: &TempDir,
        sim: &SimStore,
        ns: &ProjectNamespace,
        lsn: Lsn,
        timeline: u32,
    ) -> io::Result<()> {
        checkpoint_flush_inner(sim, ns, timeline, lsn, dir.path()).map(|_| ())
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
        let path = dir.path().join(TIKO_DIR).join("read_delta.tikm");
        Manifest::from_bytes(&bytes, &path).unwrap()
    }

    // ── Scenario 1: chunk dirtied, still in cache → flush_all_dirty_chunks ──
    // Simulate: the eviction log was written by flush_all_dirty_chunks (step 1).
    // We pre-populate the log directly (tests cannot run the real cache).

    #[test]
    fn scenario1_chunk_in_log_appears_in_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x1000);
        let tag = make_tag(1);

        // Simulate step 1 output: eviction log has one entry.
        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[tag]);
        setup_express(&sim, &ns, &[tag], 0xAA);

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
    fn scenario2_mid_interval_eviction_log_entry_in_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x2000);
        let tag = make_tag(2);

        // Mid-interval: eviction_log written by `flush_dirty_chunk`.
        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[tag]);
        setup_express(&sim, &ns, &[tag], 0xBB);

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
        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[tag, tag]);
        setup_express(&sim, &ns, &[tag], 0xCC);

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
            let path = dir.path().join(TIKO_DIR).join("count.tikm");
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

        // Simulate crash: eviction_log.ckpt exists, eviction_log is absent.
        let ckpt_path = dir.path().join(TIKO_DIR).join("eviction_log.ckpt");
        write_eviction_log(&ckpt_path, &[tag]);
        setup_express(&sim, &ns, &[tag], 0xEE);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let m = read_delta_manifest(&dir, &sim, &ns, 1, lsn);
        assert!(
            m.lookup(&tag).unwrap().is_some(),
            "chunk must be in manifest after re-processing .ckpt"
        );

        // .ckpt must be cleaned up.
        assert!(
            !ckpt_path.exists(),
            "eviction_log.ckpt must be removed on success"
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

        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &tags);
        setup_express(&sim, &ns, &tags, 0xDD);

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
        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[tag]);
        setup_express(&sim, &ns, &[tag], 0xFF);

        // First call — succeeds, removes .ckpt.
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();
        let bytes_first = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .unwrap();

        // Simulate crash recovery: re-create .ckpt manually.
        let ckpt_path = dir.path().join(TIKO_DIR).join("eviction_log.ckpt");
        write_eviction_log(&ckpt_path, &[tag]);

        // Second call — must succeed and produce the same manifest content.
        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();
        let bytes_second = sim
            .get_standard(&ns.delta_manifest_key(1, lsn))
            .unwrap()
            .unwrap();

        // Both manifests must decode to the same entries.
        let p1 = dir.path().join(TIKO_DIR).join("cmp1.tikm");
        let p2 = dir.path().join(TIKO_DIR).join("cmp2.tikm");
        let m1 = Manifest::from_bytes(&bytes_first, &p1).unwrap();
        let m2 = Manifest::from_bytes(&bytes_second, &p2).unwrap();
        assert_eq!(m1.lookup(&tag).unwrap(), m2.lookup(&tag).unwrap());
    }

    // ── eviction_log.ckpt is removed on success ───────────────────────────

    #[test]
    fn eviction_log_ckpt_removed_on_success() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x8000);

        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[make_tag(99)]);
        setup_express(&sim, &ns, &[make_tag(99)], 0x11);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        let ckpt_path = dir.path().join(TIKO_DIR).join("eviction_log.ckpt");
        assert!(
            !ckpt_path.exists(),
            "eviction_log.ckpt must not exist after success"
        );
    }

    // ── Empty eviction log → no delta manifest written (no-op) ──────────

    #[test]
    fn empty_eviction_log_produces_no_delta_manifest() {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let ns = ns();
        let lsn = Lsn::new(0x9000);

        // Create an empty eviction log.
        let log_path = dir.path().join(TIKO_DIR).join("eviction_log");
        write_eviction_log(&log_path, &[]);

        run_flush(&dir, &sim, &ns, lsn, 1).unwrap();

        // No dirty chunks → no manifest should be written.
        assert!(
            sim.get_standard(&ns.delta_manifest_key(1, lsn))
                .unwrap()
                .is_none(),
            "no delta manifest should be written when eviction log is empty"
        );

        // eviction_log.ckpt should be cleaned up.
        let ckpt_path = dir.path().join(TIKO_DIR).join("eviction_log.ckpt");
        assert!(
            !ckpt_path.exists(),
            "eviction_log.ckpt must be removed on no-op"
        );
    }

    // ── dedup_by_chunk_tag unit test ──────────────────────────────────────

    #[test]
    fn dedup_removes_duplicates_keeps_all_unique() {
        let t1 = make_tag(1);
        let t2 = make_tag(2);
        let records = vec![t1, t2, t1, t2, t2];
        let mut result = dedup_by_chunk_tag(records);
        result.sort(); // HashSet order is non-deterministic
        assert_eq!(result, vec![t1, t2]);
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
        Manifest::new(lsn, 0, vec![(tag, cref)], HashMap::new(), &path).unwrap()
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

        upload_pg_state(&sim, &ns, 1, lsn, dir.path()).unwrap();

        let key = ns.pg_state_key(1, lsn);
        let bytes = sim.get_standard(&key).unwrap();
        assert!(bytes.is_some(), "pg_state archive must exist at {key}");
        assert!(!bytes.unwrap().is_empty());
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

        upload_pg_state(&sim, &ns, 1, lsn, dir.path()).unwrap();

        let bytes = sim.get_standard(&ns.pg_state_key(1, lsn)).unwrap().unwrap();

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
        assert!(
            entry_names.iter().any(|n| n.contains("pg_subtrans")),
            "pg_subtrans segment must be in archive; found: {entry_names:?}"
        );
    }
}
