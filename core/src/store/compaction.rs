use std::sync::Arc;

use super::Store;
use crate::{
    error::Result,
    io_control::IoControl,
    timeline::{Checkpoint, CheckpointSummary},
};
use pgsys::logging::{pg_log_debug1, pg_log_warning};

/// Outcome of one [`Store::run_compaction`] call. Returned to the compactor
/// task in `worker` for logging and metrics.
#[derive(Debug)]
pub enum CompactionResult {
    /// No `IoControl` (initdb/single-user, or pre-postmaster startup).
    Skipped,
    /// No segment checkpoints exist in the eligible range yet.
    NoNewSegments,
    /// Another compactor advanced `base_ckpt` while we were preparing the
    /// new base manifest; our work was discarded.
    Raced,
    /// Successfully applied `count` segment checkpoints and advanced
    /// `base_ckpt` to `new_base_ckpt`.
    Applied {
        base_ckpt: Checkpoint,
        new_base_ckpt: Checkpoint,
        count: usize,
    },
}

impl Store {
    /// Outcome of a single [`Store::run_compaction`] call.
    ///
    /// Run the segment-based compactor. Picks a target checkpoint
    /// `< redo_ckpt` (or `<= head_ckpt` if `redo_ckpt` hasn't been set yet),
    /// merges every `CheckpointSummary` in `(base_ckpt, target]` into the
    /// base manifest, writes the new base, advances `base_ckpt`, and
    /// deletes segment files whose entire LSN range falls below the new
    /// `base_ckpt` (those are now fully represented in the base manifest).
    ///
    /// Idempotent: with no eligible segments the call returns
    /// [`CompactionResult::NoNewSegments`] without changing any state.
    pub fn run_compaction(&self) -> Result<CompactionResult> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(CompactionResult::Skipped),
        };

        // Snapshot relevant fields under the read lock.
        let (base_ckpt, redo_ckpt, head_ckpt) = {
            let _guard = io_control.timeline.lock.read();
            (
                io_control.timeline.base_ckpt,
                io_control.timeline.redo_ckpt,
                io_control.timeline.head_ckpt,
            )
        };

        // Pick the upper bound. Once PG passes a real `CheckPoint.redo`
        // through, `redo_ckpt` becomes the natural ceiling. Until then it
        // is set equal to the latest commit, so use `head_ckpt` instead.
        let upper_ckpt = if redo_ckpt.lsn.as_u64() == 0 {
            head_ckpt
        } else {
            redo_ckpt
        };
        if upper_ckpt <= base_ckpt {
            return Ok(CompactionResult::NoNewSegments);
        }

        let segments = self.list_segments_in_range(base_ckpt, upper_ckpt)?;
        let mut to_apply: Vec<CheckpointSummary> = Vec::new();
        for sid in &segments {
            let seg = self.load_segment(sid)?;
            for sc in &seg.checkpoints {
                if sc.ckpt > base_ckpt && sc.ckpt < upper_ckpt {
                    to_apply.push(sc.clone());
                }
            }
        }

        if to_apply.is_empty() {
            return Ok(CompactionResult::NoNewSegments);
        }

        // Apply in ascending `Checkpoint` order — `(timeline_id, lsn)` —
        // so last-write-wins is correct across timeline transitions.
        to_apply.sort_by_key(|s| s.ckpt);

        // Merge chunks + relfork meta into the base manifest. Sequence
        // ensures the locally-visible TIKM file is never ahead of S3 — if
        // the S3 PUT fails, the local TIKM stays at the old state.
        //
        //   1. `apply_segments`: pure compute; returns merged state + bytes.
        //   2. `storage.put`: publish the new base manifest to S3.
        //   3. Under the timeline write lock: re-check `base_ckpt` (a raced
        //      compactor discards here, before touching the local file),
        //      `commit_applied` to atomically rewrite the local TIKM, then
        //      advance `base_ckpt`. Holding the lock across the rename makes
        //      the check and the local publish atomic w.r.t. other
        //      compactors, so the on-disk file always matches the shmem base.
        //      We then swap the new Manifest into `base_manifest`; existing
        //      `Arc<Manifest>` readers keep using the old file via their FD
        //      until they drop their `Arc`.
        let current = self.base_manifest()?;
        let new_base_ckpt = to_apply.last().unwrap().ckpt;
        let key = self.lctr.base_manifest(&new_base_ckpt);

        let applied = current.apply_segments(&to_apply, self.ns.db_id)?;
        self.storage.put(&key, &applied.bytes)?;

        let new_manifest = {
            let _write_guard = io_control.timeline.lock.write();
            if io_control.timeline.base_ckpt != base_ckpt {
                pg_log_warning("tiko: compaction raced; another compactor advanced base_ckpt");
                return Ok(CompactionResult::Raced);
            }
            let new_manifest = Arc::new(current.commit_applied(applied)?);
            io_control.timeline.set_base_ckpt(new_base_ckpt);
            new_manifest
        };

        // Swap the fresh Manifest in so this process's next
        // `base_manifest()` call short-circuits instead of re-loading.
        *self.base_manifest.lock().unwrap() = new_manifest;

        // Delete segment files whose entire LSN range is now covered by the
        // base manifest. The segment that contains `new_base_ckpt` itself
        // straddles the boundary and is retained — it still has
        // checkpoints above `base_ckpt`. Comparison uses the derived
        // `SegmentId` Ord (timeline_id then index), so this correctly
        // catches superseded segments from older timelines.
        let new_base_seg = new_base_ckpt.to_segment_id();
        for sid in segments.iter().take_while(|s| **s < new_base_seg) {
            let seg_key = self.lctr.timeline_segment(sid);
            match self.storage.delete(&seg_key) {
                Ok(_) => {}
                Err(e) if e.is_not_found() => {}
                Err(e) => {
                    pg_log_warning(format!(
                        "tiko: failed to delete superseded segment {seg_key}: {e}",
                    ));
                }
            }
        }

        let count = to_apply.len();
        pg_log_debug1(format!(
            "tiko: compaction applied {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt}"
        ));
        Ok(CompactionResult::Applied {
            base_ckpt,
            new_base_ckpt,
            count,
        })
    }

    /// Like [`run_compaction`], but folds every segment checkpoint up to and
    /// **including** `target` into the base manifest, advancing `base_ckpt` to
    /// the resulting checkpoint.
    ///
    /// Used by the `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint to form a base
    /// manifest AT the backup LSN (`target` = the basebackup commit
    /// checkpoint). That lets PITR anchor recovery on a single, complete base
    /// manifest instead of base + segments above it — so the recovering smgr
    /// never has to consult "future" segments (which would leak post-target
    /// state).
    ///
    /// The manifest is keyed/headered at `applied.checkpoint` (the highest
    /// folded checkpoint that actually changed data, ≤ `target`), keeping the
    /// storage key, TIKM header and shmem `base_ckpt` consistent. If the
    /// backup's own segment had no dirty data, `applied.checkpoint` is the
    /// previous checkpoint — which already represents the backup's state, so
    /// `materialize_base_manifest_at(target)` still resolves correctly.
    pub fn run_compaction_through(&self, target: Checkpoint) -> Result<CompactionResult> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(CompactionResult::Skipped),
        };

        let base_ckpt = {
            let _guard = io_control.timeline.lock.read();
            io_control.timeline.base_ckpt
        };
        if target <= base_ckpt {
            return Ok(CompactionResult::NoNewSegments);
        }

        // Fold every segment checkpoint in (base_ckpt, target] (inclusive of
        // `target`, unlike `run_compaction` which is exclusive of redo_ckpt).
        let segments = self.list_segments_in_range(base_ckpt, target)?;
        let mut to_apply: Vec<CheckpointSummary> = Vec::new();
        for sid in &segments {
            let seg = self.load_segment(sid)?;
            for sc in &seg.checkpoints {
                if sc.ckpt > base_ckpt && sc.ckpt <= target {
                    to_apply.push(sc.clone());
                }
            }
        }
        if to_apply.is_empty() {
            return Ok(CompactionResult::NoNewSegments);
        }
        to_apply.sort_by_key(|s| s.ckpt);

        let current = self.base_manifest()?;
        let applied = current.apply_segments(&to_apply, self.ns.db_id)?;
        // Key/header/base_ckpt all at `applied.checkpoint` for consistency.
        let new_base_ckpt = applied.checkpoint;
        let key = self.lctr.base_manifest(&new_base_ckpt);
        self.storage.put(&key, &applied.bytes)?;

        // Same protocol as `run_compaction`: re-check `base_ckpt` and
        // publish the local TIKM atomically under the write lock, so a raced
        // compactor discards before touching the local file.
        let new_manifest = {
            let _write_guard = io_control.timeline.lock.write();
            if io_control.timeline.base_ckpt != base_ckpt {
                pg_log_warning(
                    "tiko: compaction-through raced; another compactor advanced base_ckpt",
                );
                return Ok(CompactionResult::Raced);
            }
            let new_manifest = Arc::new(current.commit_applied(applied)?);
            io_control.timeline.set_base_ckpt(new_base_ckpt);
            new_manifest
        };
        *self.base_manifest.lock().unwrap() = new_manifest;

        // Delete superseded segment files entirely below the new base.
        let new_base_seg = new_base_ckpt.to_segment_id();
        for sid in segments.iter().take_while(|s| **s < new_base_seg) {
            let seg_key = self.lctr.timeline_segment(sid);
            match self.storage.delete(&seg_key) {
                Ok(_) => {}
                Err(e) if e.is_not_found() => {}
                Err(e) => pg_log_warning(format!(
                    "tiko: failed to delete superseded segment {seg_key}: {e}",
                )),
            }
        }

        let count = to_apply.len();
        pg_log_debug1(format!(
            "tiko: compaction-through applied {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt} (target {target})"
        ));
        Ok(CompactionResult::Applied {
            base_ckpt,
            new_base_ckpt,
            count,
        })
    }
}
