use std::sync::Arc;

use super::Store;
use crate::{
    error::{Error, Result},
    io_control::IoControl,
    timeline::{Checkpoint, CheckpointSummary, SegmentId},
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
    /// new base manifest; our work was discarded. Says nothing about how far
    /// the winner advanced — callers needing a specific coverage point must
    /// re-check (see [`Store::run_compaction_through`]).
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
        let timeline = &io_control.timeline;

        // Snapshot relevant fields under the read lock.
        let (redo_ckpt, head_ckpt) = {
            let _guard = timeline.lock.read();
            (timeline.redo_ckpt, timeline.head_ckpt)
        };

        // Pick the upper bound. Once PG passes a real `CheckPoint.redo`
        // through, `redo_ckpt` becomes the natural ceiling. Until then it
        // is set equal to the latest commit, so use `head_ckpt` instead.
        let upper_ckpt = if redo_ckpt.lsn.is_invalid() {
            head_ckpt
        } else {
            redo_ckpt
        };
        self.compact_impl(upper_ckpt, false)
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
    /// The manifest is keyed/headered at the highest folded checkpoint
    /// (≤ `target`; normally the backup's own commit summary), so the
    /// storage key, TIKM header and shmem `base_ckpt` always agree and
    /// `materialize_base_manifest_at(target)` resolves it via "newest key
    /// ≤ target".
    /// The anchor is only useful if it COVERS `target`, so this verifies
    /// coverage before returning: a `Raced` run is retried (the winner may
    /// have folded to a lower checkpoint — e.g. a tick bounded by redo), and
    /// any outcome that still leaves `base_ckpt < target` is an error.
    /// Bounded retries suffice: head/redo are frozen while the basebackup
    /// checkpoint is in progress, so a lower-upper racer can win at most
    /// once before the retry applies.
    pub fn run_compaction_through(&self, target: Checkpoint) -> Result<CompactionResult> {
        const MAX_RACES: u32 = 3;
        let mut races = 0;
        loop {
            let result = self.compact_impl(target, true)?;
            let base_now = IoControl::try_get().map(|c| {
                let _guard = c.timeline.lock.read();
                c.timeline.base_ckpt
            });
            // None: no shmem state to verify against (initdb/single-user).
            let covered = base_now.is_none_or(|b| b >= target);
            match result {
                CompactionResult::Raced if !covered && races < MAX_RACES => {
                    races += 1;
                    continue;
                }
                _ if covered => return Ok(result),
                _ => {
                    return Err(Error::other(format!(
                        "compaction through {target} incomplete: base_ckpt {} does not cover the target",
                        base_now.unwrap_or_default()
                    )));
                }
            }
        }
    }

    // Shared fold-and-publish body. `inclusive` (basebackup) includes `upper`
    // itself; exclusive (tick) stops below `upper`. Both key the new base at
    // `applied.checkpoint` — the last folded summary — so the storage key,
    // TIKM header and shmem `base_ckpt` always agree.
    fn compact_impl(&self, upper: Checkpoint, inclusive: bool) -> Result<CompactionResult> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(CompactionResult::Skipped),
        };
        let timeline = &io_control.timeline;
        let op = if inclusive {
            "compaction-through"
        } else {
            "compaction"
        };

        let base_ckpt = {
            let _guard = timeline.lock.read();
            timeline.base_ckpt
        };
        if upper <= base_ckpt {
            return Ok(CompactionResult::NoNewSegments);
        }

        let segments = self.list_segments_in_range(base_ckpt, upper)?;
        let mut to_apply: Vec<CheckpointSummary> = Vec::new();
        let mut missing: Option<SegmentId> = None;
        for sid in &segments {
            let Some(seg) = self.try_load_segment(sid)? else {
                // A concurrent compactor deleted this segment after we listed
                // it. Its contents are only gone because a newer base manifest
                // already covers them, so confirm `base_ckpt` moved and report
                // a race (the caller retries/verifies coverage); otherwise fail
                // loudly rather than publish a partial merge.
                missing = Some(*sid);
                break;
            };
            for sc in &seg.checkpoints {
                let in_range = if inclusive {
                    sc.ckpt <= upper
                } else {
                    sc.ckpt < upper
                };
                if sc.ckpt > base_ckpt && in_range {
                    to_apply.push(sc.clone());
                }
            }
        }

        if let Some(sid) = missing {
            let base_now = {
                let _guard = timeline.lock.read();
                timeline.base_ckpt
            };
            if base_now != base_ckpt {
                return Ok(CompactionResult::Raced);
            }
            return Err(Error::other(format!(
                "timeline segment {sid} disappeared while base_ckpt stayed at {base_ckpt}"
            )));
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
        let applied = current.apply_segments(&to_apply, self.ns.db_id)?;
        let new_base_ckpt = applied.checkpoint;
        let key = self.ns.base_manifest(&new_base_ckpt);
        self.storage.put(&key, &applied.bytes)?;

        let new_manifest = {
            let _write_guard = timeline.lock.write();
            if timeline.base_ckpt != base_ckpt {
                pg_log_warning(format!(
                    "tiko: {op} raced; another compactor advanced base_ckpt"
                ));
                return Ok(CompactionResult::Raced);
            }
            let new_manifest = Arc::new(current.commit_applied(applied)?);
            debug_assert_eq!(new_manifest.checkpoint(), new_base_ckpt);
            timeline.set_base_ckpt(new_base_ckpt);
            new_manifest
        };

        // Swap the fresh Manifest in so this process's next
        // `base_manifest()` call short-circuits instead of re-loading.
        *self.base_manifest.lock()? = new_manifest;

        // Delete segment files whose entire LSN range is now covered by the
        // base manifest. The segment that contains `new_base_ckpt` itself
        // straddles the boundary and is retained — it still has
        // checkpoints above `base_ckpt`. Comparison uses the derived
        // `SegmentId` Ord (timeline_id then index), so this correctly
        // catches superseded segments from older timelines.
        let new_base_seg = new_base_ckpt.to_segment_id();
        for sid in segments.iter().take_while(|s| **s < new_base_seg) {
            let seg_key = self.ns.timeline_segment(sid);
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
        if inclusive {
            pg_log_debug1(format!(
                "tiko: {op} applied {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt} (target {upper})"
            ));
        } else {
            pg_log_debug1(format!(
                "tiko: {op} applied {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt}"
            ));
        }
        Ok(CompactionResult::Applied {
            base_ckpt,
            new_base_ckpt,
            count,
        })
    }
}
