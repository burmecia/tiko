use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::Store;
use super::wal::is_base_usable;
use crate::{
    db::DbNamespace,
    error::{Error, Result},
    manifest::Manifest,
    timeline::Checkpoint,
};
use pgsys::{logging::pg_log_warning, lsn::Lsn, timeline_id::TimelineId};

/// One row returned by [`Store::list_checkpoints`] — a recovery-target
/// candidate flattened from the timeline segment files.
#[derive(Debug, Clone)]
pub struct CheckpointRow {
    pub ckpt: Checkpoint,
    pub redo_ckpt: Checkpoint,
    pub created_at: i64,
    pub n_chunks: usize,
}

/// One row returned by [`Store::list_backups`] — a base backup taken via
/// `pg_basebackup` and stored under the `backup/` prefix.
#[derive(Debug, Clone)]
pub struct BackupRow {
    pub ckpt: Checkpoint,
    pub redo_ckpt: Checkpoint,
    pub created_at: i64,
}

/// JSON sidecar stored next to each base-backup tarball (`{...}.json`) so that
/// time-based selection does not require downloading the tarball.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupMeta {
    checkpoint: Checkpoint,
    redo_ckpt: Checkpoint,
    timeline_id: u32,
    created_at: i64,
}

/// The recoverable window reported by [`Store::recovery_window`], bounded by
/// archived-WAL coverage.
///
/// A single period `[earliest, latest_lsn]`: `earliest` is the oldest base
/// *backup* (a `pg_basebackup` tarball under `backup/`) whose recovery WAL
/// (`[redo, checkpoint]`) lies inside the contiguous archived-WAL run, and
/// `latest_lsn` is the end of that run (the WAL archiving head, T2). A PITR
/// target must fall within `[earliest_ckpt, latest_lsn]` (and
/// `[earliest_ts, latest_ts]`). Recovery selects the latest backup at or before
/// the target as the restore anchor.
#[derive(Debug, Clone)]
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint,
    pub latest_ts: i64,
    /// End of the contiguous archived-WAL run (highest recoverable LSN).
    pub latest_lsn: Lsn,
    pub timeline: TimelineId,
}

impl Store {
    /// Delete every timeline segment file on storage.
    ///
    /// Used by PITR recovery prep (`tiko_pitr recover`) after installing the
    /// backup's base manifest: the recovered instance anchors all reads on
    /// that manifest (+ WAL replay, then the new timeline's segments after
    /// promote). The pre-recovery segments hold the OLD timeline's history
    /// ABOVE the backup LSN — leaving them in place would let the read path's
    /// segment scan resolve "future" chunk versions after promote. Removing
    /// them makes recovery self-contained on the base manifest.
    pub fn delete_all_segments(&self) -> Result<()> {
        let ids = self.list_all_segments()?;
        for sid in ids {
            let key = self.ns.timeline_segment(&sid);
            match self.storage_delete(&key) {
                Ok(_) => {}
                Err(e) if e.is_not_found() => {}
                Err(e) => pg_log_warning(format!(
                    "tiko: failed to delete timeline segment {key}: {e}",
                )),
            }
        }
        Ok(())
    }

    /// List every checkpoint recorded across all timeline segment files,
    /// flattened and sorted ascending by `(created_at, ckpt)`. Read-only;
    /// used by `tiko_pitr list` to present recovery targets.
    pub fn list_checkpoints(&self) -> Result<Vec<CheckpointRow>> {
        let segment_ids = self.list_all_segments()?;
        let mut rows: Vec<CheckpointRow> = Vec::new();
        for sid in &segment_ids {
            let seg = self.load_segment(sid)?;
            for sc in &seg.checkpoints {
                rows.push(CheckpointRow {
                    ckpt: sc.ckpt,
                    redo_ckpt: sc.redo_ckpt,
                    created_at: sc.created_at,
                    n_chunks: sc.chunks.len(),
                });
            }
        }
        rows.sort_by_key(|r| (r.created_at, r.ckpt));
        Ok(rows)
    }

    // ── Base backups (pg_basebackup-based PITR anchors) ──────────────────

    /// Upload a base-backup tarball and its metadata sidecar to the `backup/`
    /// prefix. `tar_bytes` is the compressed (`tar.zst`) output of a
    /// `pg_basebackup` run whose checkpoint is `ckpt`.
    pub fn put_backup(
        &self,
        ckpt: Checkpoint,
        redo_ckpt: Checkpoint,
        created_at: i64,
        tar_bytes: &[u8],
    ) -> Result<()> {
        let tar_key = self.ns.backup_object(&ckpt);
        self.storage_put(&tar_key, tar_bytes)?;

        let meta = BackupMeta {
            checkpoint: ckpt,
            redo_ckpt,
            timeline_id: ckpt.timeline_id.as_u32(),
            created_at,
        };
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| Error::other(format!("failed to serialize backup meta: {e}")))?;
        self.storage_put(&self.ns.backup_meta(&ckpt), &meta_bytes)?;
        Ok(())
    }

    /// List every base backup on storage, parsed into [`BackupRow`]s (read from
    /// the `.json` sidecars) and sorted ascending by checkpoint.
    pub fn list_backups(&self) -> Result<Vec<BackupRow>> {
        let prefix = self.ns.backup_dir();
        let keys = match self.storage_list_prefix(&prefix) {
            Ok(k) => k,
            Err(e) if e.is_not_found() => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut rows: Vec<BackupRow> = Vec::new();
        for key in &keys {
            // Only the `.json` sidecars carry metadata; skip the tarballs.
            let Some(rel) = key.strip_prefix(&prefix) else {
                continue;
            };
            if !rel.ends_with(".json") {
                continue;
            }
            let bytes = match self.storage_get(key) {
                Ok(b) => b,
                Err(e) if e.is_not_found() => continue,
                Err(e) => return Err(e),
            };
            let meta: BackupMeta = serde_json::from_slice(&bytes)
                .map_err(|e| Error::other(format!("failed to parse backup meta {key}: {e}")))?;
            rows.push(BackupRow {
                ckpt: meta.checkpoint,
                redo_ckpt: meta.redo_ckpt,
                created_at: meta.created_at,
            });
        }
        rows.sort_by_key(|r| r.ckpt);
        Ok(rows)
    }

    /// Select the newest base backup with `ckpt <= target` and return its
    /// `(checkpoint, tar_bytes)`. Standard PITR anchor: the closest backup at
    /// or before the target so the minimum WAL must be replayed.
    pub fn load_backup_at_or_before(&self, target: Checkpoint) -> Result<(Checkpoint, Vec<u8>)> {
        let ckpt = self
            .list_backups()?
            .into_iter()
            .filter(|r| r.ckpt <= target)
            .map(|r| r.ckpt)
            .max_by_key(|c| *c)
            .ok_or_else(|| {
                Error::other(format!("no base backup at or before checkpoint {target}"))
            })?;
        let bytes = self.storage_get(&self.ns.backup_object(&ckpt))?;
        Ok((ckpt, bytes))
    }

    /// Select the newest base backup with `created_at <= target_ts` on
    /// `timeline` and return its `(checkpoint, tar_bytes)`.
    pub fn load_backup_before_time(
        &self,
        target_ts: i64,
        timeline: TimelineId,
    ) -> Result<(Checkpoint, Vec<u8>)> {
        let ckpt = self
            .list_backups()?
            .into_iter()
            .filter(|r| r.ckpt.timeline_id == timeline && r.created_at <= target_ts)
            .max_by_key(|r| (r.created_at, r.ckpt))
            .map(|r| r.ckpt)
            .ok_or_else(|| {
                Error::other(format!(
                    "no base backup at or before time {target_ts} on timeline {timeline}"
                ))
            })?;
        let bytes = self.storage_get(&self.ns.backup_object(&ckpt))?;
        Ok((ckpt, bytes))
    }

    /// Install the live `$TIKO_ROOT/base_manifest.tikm` for recovering to
    /// `ckpt`: download the newest base manifest at or before `ckpt` and write
    /// it as the live TIKM cache file.
    ///
    /// The basebackup checkpoint's own segment sits a little above the base
    /// manifest produced by compaction (compaction folds strictly below the
    /// checkpoint's redo), so the manifest at *exactly* `ckpt` may not exist.
    /// The newest manifest `<= ckpt` is the correct anchor: the recovering smgr
    /// seeds `base_ckpt` from it and supplements with the segments above it
    /// (including the backup checkpoint's own segment) to resolve chunks at the
    /// backup LSN. The atomic publish (per-PID tmp + rename inside
    /// `Manifest::from_bytes` → `write_tikm`) means a crash never leaves a
    /// partial file.
    pub fn materialize_base_manifest_at(&self, ckpt: Checkpoint) -> Result<()> {
        let keys = self.storage_list_prefix(&self.ns.bases_dir())?;
        let target_base = keys
            .iter()
            .filter_map(|k| self.ns.parse_base_manifest(k))
            .filter(|c| *c <= ckpt)
            .max_by_key(|c| *c)
            .ok_or_else(|| {
                Error::other(format!(
                    "no base manifest at or before {ckpt} to anchor recovery"
                ))
            })?;
        let key = self.ns.base_manifest(&target_base);
        let bytes = self.storage_get(&key)?;
        // `from_bytes` writes the TIKM at `local_root/base_manifest.tikm`.
        let manifest = Manifest::from_bytes(&bytes, &self.local_root)?;
        // Keep this process's cached snapshot in sync (harmless in the CLI,
        // which exits after this call; required if ever called in-process).
        *self.base_manifest.lock()? = Arc::new(manifest);
        Ok(())
    }

    /// Seed a branch namespace by copying the parent database's newest base
    /// manifest at or before `ckpt` into the `branch_ns` namespace.
    ///
    /// `parent_db_id` identifies the parent database within this store's org
    /// (`self.ns.org_id`); the branch shares the org, so only `db_id` differs.
    ///
    /// The manifest bytes are copied as-is, so the branch's manifest carries
    /// `ChunkRef.db_id = parent_db_id` — which is exactly the copy-on-write
    /// invariant: the branch's smgr resolves those chunks against the parent's
    /// namespace (shared via `TIKO_STORAGE_ROOT`), and only materializes its own
    /// chunk versions (carrying `db_id = branch_db_id`) when it dirties them.
    ///
    /// `ckpt` is typically the `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint; the
    /// manifest may be keyed slightly below it (if the basebackup segment had
    /// no dirty data, `run_compaction_through` keys at the previous checkpoint
    /// — which represents the same state).
    pub fn seed_branch_base_manifest(
        &self,
        parent_db_id: u64,
        branch_ns: DbNamespace,
        ckpt: Checkpoint,
    ) -> Result<()> {
        let parent_ns = self.ns.for_db(parent_db_id);
        let keys = self.storage_list_prefix(&parent_ns.bases_dir())?;
        let target_base = keys
            .iter()
            .filter_map(|k| parent_ns.parse_base_manifest(k))
            .filter(|c| *c <= ckpt)
            .max_by_key(|c| *c)
            .ok_or_else(|| {
                Error::other(format!(
                    "no base manifest at or before {ckpt} in parent db_id={parent_db_id} to seed the branch"
                ))
            })?;

        let parent_key = parent_ns.base_manifest(&target_base);
        let bytes = self.storage_get(&parent_key)?;
        let branch_key = branch_ns.base_manifest(&target_base);
        self.storage_put(&branch_key, &bytes)
    }

    /// Compute the PITR-recoverable window bounded by archived-WAL coverage:
    /// `earliest` = the oldest base *backup* whose recovery WAL (`[redo,
    /// checkpoint]`) is inside the contiguous archived run; `latest_lsn` = the
    /// end of that run. Errors with a clear message when nothing is recoverable
    /// yet.
    pub fn recovery_window(&self) -> Result<RecoveryWindow> {
        // 1. Timeline = newest checkpoint's timeline.
        let rows = self.list_checkpoints()?;
        let newest = rows
            .last()
            .ok_or_else(|| Error::other("no checkpoints found; nothing is recoverable yet"))?;
        let timeline = newest.ckpt.timeline_id;

        // 2. Contiguous archived-WAL run for this timeline.
        let (w_lo, w_hi) = self.archived_wal_run(timeline)?;

        // 3. Earliest usable base backup: ascending by checkpoint, first whose
        //    recovery WAL is inside the archived run. PITR anchors on
        //    pg_basebackup tarballs (`backup/`), not base manifests.
        let mut backups = self.list_backups()?;
        backups.retain(|b| b.ckpt.timeline_id == timeline);
        backups.sort_by_key(|b| b.ckpt);

        let chosen = backups
            .into_iter()
            .find(|b| is_base_usable(b.ckpt.lsn.as_u64(), b.redo_ckpt.lsn.as_u64(), w_lo, w_hi))
            .ok_or_else(|| {
                Error::other("no base backup's WAL is archived; nothing is recoverable yet")
            })?;
        let earliest_ckpt = chosen.ckpt;
        let earliest_ts = chosen.created_at;

        // 4. Latest: run end, and the newest checkpoint time within the run.
        //    If no checkpoint sits at/below w_hi the time window collapses to
        //    `earliest_ts` (never over-promises); the LSN window can still be
        //    wider than the time window in that edge.
        let latest_lsn = Lsn::new(w_hi);
        let latest_ts = rows
            .iter()
            .filter(|r| r.ckpt.lsn.as_u64() <= w_hi)
            .map(|r| r.created_at)
            .max()
            .unwrap_or(earliest_ts);

        Ok(RecoveryWindow {
            earliest_ts,
            earliest_ckpt,
            latest_ts,
            latest_lsn,
            timeline,
        })
    }
}
