use super::Store;
use crate::{
    error::Result,
    timeline::draft::DraftFrame,
    io_control::IoControl,
    timeline::{Checkpoint, CheckpointSummary, TimelineSegment},
};
use pgsys::logging::pg_log_debug1;

impl Store {
    /// Build a [`CheckpointSummary`] from the drained drafts and append it
    /// to the appropriate timeline segment file (load existing or init new).
    ///
    /// Called by [`Store::run_commit_protocol`] while the timeline write
    /// lock is held.
    fn commit_segment(
        &self,
        commit_ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        redo_ckpt: Checkpoint,
        drained: DraftFrame,
    ) -> Result<CheckpointSummary> {
        let segment_id = commit_ckpt.to_segment_id();
        let mut seg = match self.load_segment(&segment_id) {
            Ok(existing) => existing,
            Err(e) if e.is_not_found() => TimelineSegment::new(segment_id),
            Err(e) => return Err(e),
        };
        let summary = CheckpointSummary::new(
            commit_ckpt,
            prev_ckpt,
            redo_ckpt,
            drained.chunks,
            drained.relforks,
        );
        seg.push(summary.clone());

        // Write `segment` to storage (overwriting any previous version at the
        // same key). Subsequent commits in the same segment LSN range will
        // re-read this file and append to it.
        let key = self.lctr.timeline_segment(&segment_id);
        let bytes = seg.to_bytes()?;
        self.storage.put(&key, &bytes)?;

        Ok(summary)
    }

    /// Run the segment-based commit protocol — entry point called by the
    /// smgr checkpoint hook on every PG checkpoint.
    ///
    /// No-op if `IoControl` is unavailable (e.g. very early in startup).
    /// Otherwise:
    ///
    /// 1. `cache.flush_dirty()` — flush dirty chunks and relfork meta to
    ///    the storage layer via the normal read-lock path
    ///    ([`Store::patch_chunk`] / [`Store::put_meta`]). Runs before the
    ///    write lock below so it doesn't re-enter the timeline lock.
    /// 2. Acquire `timeline.lock.write()`. This is the fence: it blocks
    ///    until every in-flight reader (the flush above, plus any
    ///    concurrent backend evictions) has dropped its read lock.
    /// 3. Capture `prev_ckpt = head_ckpt` (path prefix for chunks written
    ///    during the interval ending at `commit_ckpt`) and set `redo_ckpt`.
    /// 4. Drain the cluster-wide shmem [`DraftBuffer`] (chunks + relforks
    ///    zones) plus its on-disk spill file. All backends record into this
    ///    one shared buffer, so the drain captures the full interval in a
    ///    single pass.
    /// 5. Build a `CheckpointSummary` from the drained state and append it
    ///    to the appropriate segment file via [`Store::commit_segment`].
    /// 6. `push_active(commit_ckpt, prev_ckpt, chunks, relforks)` updates
    ///    the active window, advances `head_ckpt`, and bumps `generation`.
    /// 7. Update the `DbMeta` JSON on storage to record the new checkpoint.
    /// 8. Drop the write guard implicitly at function exit.
    pub fn run_commit_protocol(
        &self,
        commit_ckpt: &Checkpoint,
        redo_ckpt: &Checkpoint,
    ) -> Result<()> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(()), // initdb / single-user — handled separately.
        };

        // 1. Flush dirty cache state under the normal read-lock path.
        //    `io_control` is non-None (early-returned above), so the cache
        //    is reachable.
        io_control.cache.flush_dirty()?;

        // 2. Acquire the write lock. Waits for all in-flight read-lock
        //    holders (the flush above, concurrent backend evictions) to
        //    drain.
        let _write_guard = io_control.timeline.lock.write();

        let prev_ckpt = io_control.timeline.head_ckpt;
        let timeline = &io_control.timeline;
        timeline.set_redo_ckpt(*redo_ckpt);

        // Drain the centralized shmem draft ring + its on-disk spill file.
        let drained = timeline.draft.drain(&self.draft_spill_path)?;
        let summary = self.commit_segment(*commit_ckpt, prev_ckpt, *redo_ckpt, drained)?;

        timeline.push_active(
            *commit_ckpt,
            prev_ckpt,
            summary.chunks.iter().copied(),
            summary.relforks.iter().map(|(rf, meta)| (*rf, *meta)),
        );

        // Update DbMeta JSON
        self.update_db_meta(commit_ckpt)?;

        pg_log_debug1(format!(
            "tiko: run_commit_protocol at {commit_ckpt}: prev={prev_ckpt} chunks={} relforks={}",
            summary.chunks.len(),
            summary.relforks.len(),
        ));

        Ok(())
    }
}
