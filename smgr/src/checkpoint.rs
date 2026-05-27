//! Checkpoint flush — the S3/PITR half of PostgreSQL's checkpoint.
//!
//! Called from `CheckPointGuts()` in `xlog.c` after `CheckPointBuffers()`.
//! The checkpointer is a plain PG process — no Tokio runtime.  All I/O is
//! synchronous (`std::fs` + `S3Sim` which is also `std::fs`).
//!
//! # Algorithm
//!
//! 0. **Guard**: returns early if `IoControl`, `S3Sim`, or `ProjectCtx`
//!    are not yet initialised — nothing to flush or upload.
//!
//! 1. **Flush dirty chunks** (`flush_all_dirty_chunks`): every dirty cache
//!    slot is PUT to the express-bucket `latest` object.
//!    **Flush dirty nblocks** (`flush_all_dirty_nblocks`): every dirty entry
//!    in the fork-meta table is PUT to the express nblocks key.
//!    Returns `(nblocks_map, deleted_set)` from in-memory fork-meta.
//!
//! 2. **Scan express** for chunk `latest` keys (`{org}/{proj}/chunks/…/latest`)
//!    to build the dirty chunk set for this checkpoint interval.
//!    Also scan for `/.deleted` markers to catch mid-interval evictions.
//!
//! 2.5. **Capture `pg_state`**: build the tar+zstd archive of `pg_control`,
//!    `pg_xact`, etc. into memory **before** any S3 uploads.
//!
//! 3. **Write each dirty chunk** to the standard bucket at its versioned key.
//!    Data is read from the express `latest` object and decompressed.
//!    Chunks whose fork is in the deleted set are skipped.
//!
//! 4. **Build delta manifest** (dirty chunks → `ChunkRef`s with own
//!    `branch_id`), upload it and the pre-built `pg_state` archive to the
//!    standard bucket.
//!
//! 5. **Clean up `/.deleted` markers** from express after a successful upload.
//!
//! # Crash safety
//!
//! The checkpoint is naturally idempotent: rescanning express after a crash
//! reproduces the same dirty-chunk set because express is consistent.
//! Standard-bucket PUT and delta manifest PUT are both atomic.

use std::path::Path;

use core::{error::Result, io::store::Store, io::timeline::Checkpoint};
use pgsys::{Lsn, common::data_dir_path, logging::*, timeline_id::TimelineId};

/// Called from Postgres `CreateCheckPoint()`.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_perform_checkpoint(
    timeline_id: u32,
    checkpoint_lsn: u64,
    redo_lsn: u64,
    is_shutdown: bool,
) {
    let ckpt = Checkpoint::new(TimelineId::new(timeline_id), Lsn::new(checkpoint_lsn));
    let redo_ckpt = Checkpoint::new(TimelineId::new(timeline_id), Lsn::new(redo_lsn));

    pg_log_info(format!(
        "tiko: tiko_perform_checkpoint: checkpoint {ckpt}, redo {redo_ckpt}, is_shutdown {is_shutdown}"
    ));

    let store = match Store::try_get() {
        Ok(s) => s,
        Err(_) => return,
    };

    let pgdata_dir = data_dir_path();

    // Segment-based commit protocol: flush dirty chunks, write-lock fence,
    // set redo_ckpt, drain backend drafts, write segment, advance active
    // window, persist DbMeta.
    if let Err(e) = store.run_commit_protocol(&ckpt, &redo_ckpt) {
        pg_log_error(&format!(
            "tiko: tiko_perform_checkpoint: run_commit_protocol failed at {ckpt}, redo {redo_ckpt}: {e}"
        ));
    }

    // Shutdown checkpoint: fold accumulated segments into the base manifest
    // inline. The Tiko bgworker (which normally runs compaction) is killed
    // in `PM_STOP_BACKENDS` before the checkpointer reaches
    // `PM_WAIT_XLOG_SHUTDOWN`, so there is no in-process compactor to race
    // with. Cross-process compactors are handled by the existing
    // `CompactionResult::Raced` detection inside `run_compaction`. Failure
    // is non-fatal — shutdown still completes; the next startup picks up
    // the extra segments via the normal hydrate path.
    if is_shutdown {
        if let Err(e) = store.run_compaction() {
            pg_log_warning(format!(
                "tiko: tiko_perform_checkpoint: shutdown compaction failed: {e}"
            ));
        }
    }

    // Capture pg_state archive bytes — so the
    // archive reflects pg_control / pg_xact / etc. at the start of the
    // checkpoint rather than after potentially long chunk S3 writes.
    if let Ok(pg_state_bytes) = build_pg_state_archive(&pgdata_dir) {
        //upload_pg_state(store, ns, timeline, lsn, &pg_state_bytes)?;
        pg_log_debug1(format!(
            "tiko: pg_state archive {} bytes",
            pg_state_bytes.len()
        ));
    } else {
        pg_log_error("tiko: tiko_perform_checkpoint: Failed to build pg_state archive");
    }
}

/// Build the in-memory tar+zstd archive.  Returns compressed bytes.
fn build_pg_state_archive(pgdata: &Path) -> Result<Vec<u8>> {
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
