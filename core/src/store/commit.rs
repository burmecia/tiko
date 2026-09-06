use super::Store;
use crate::{
    error::Result,
    io_control::IoControl,
    timeline::draft::DraftFrame,
    timeline::{Checkpoint, CheckpointSummary, TimelineSegment},
};
use pgsys::logging::pg_log_debug1;

impl Store {
    /// Build a [`CheckpointSummary`] from the drained drafts and append it
    /// to the pre-loaded timeline segment file, then PUT it back.
    ///
    /// Called by [`Store::run_commit_protocol`] while the timeline write
    /// lock is held. `seg` was pre-loaded outside the lock — see the caller
    /// for why that is race-free.
    fn commit_segment(
        &self,
        commit_ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        redo_ckpt: Checkpoint,
        seg: &mut TimelineSegment,
        drained: DraftFrame,
    ) -> Result<CheckpointSummary> {
        let cs = CheckpointSummary::new(
            commit_ckpt,
            prev_ckpt,
            redo_ckpt,
            drained.chunks,
            drained.relforks,
        );
        seg.push(cs.clone());

        // Write `segment` to storage (overwriting any previous version at the
        // same key). Subsequent commits in the same segment LSN range will
        // re-read this file and append to it.
        let key = self.ns.timeline_segment(&seg.segment_id);
        let bytes = seg.to_bytes()?;
        self.storage.put(&key, &bytes)?;

        Ok(cs)
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
    /// 2. Pre-load the target segment (storage GET + deserialize) *outside*
    ///    the write lock. Race-free: only the checkpointer appends to
    ///    segment files (PG serialises checkpoints), and compaction never
    ///    deletes the head segment (the boundary segment straddling
    ///    `base_ckpt` is always retained).
    /// 3. Acquire `timeline.lock.write()`. This is the fence: it blocks
    ///    until every in-flight reader (the flush above, plus any
    ///    concurrent backend evictions) has dropped its read lock.
    /// 4. Under the lock: capture `prev_ckpt = head_ckpt`, set `redo_ckpt`,
    ///    drain the cluster-wide shmem [`DraftBuffer`] (plus its on-disk
    ///    spill file), append a `CheckpointSummary` to the pre-loaded
    ///    segment and PUT it, then `push_active` (advances `head_ckpt`,
    ///    bumps `generation`) and `commit_drain` (discards the spill file
    ///    only once the segment PUT is durable — a failure before that
    ///    point retries with the same drained contents).
    /// 5. Update the `DbMeta` JSON sidecar — after the lock: it touches no
    ///    timeline state.
    ///
    /// The drain → segment PUT → push_active → commit_drain sequence must
    /// stay inside the write lock. The draft is presence-only and a summary
    /// attributes all its chunks to the single `prev_ckpt` prefix; if
    /// producers could write while a drained-but-uncommitted frame was in
    /// flight, their chunk data would land at the old head prefix yet be
    /// attributed to the next interval's prefix (reads would then miss it),
    /// and a producer spilling mid-commit would lose records to
    /// `commit_drain`'s spill-file delete.
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

        // 2. Pre-load the target segment outside the write lock (see the
        //    doc comment for why this is race-free).
        let segment_id = commit_ckpt.to_segment_id();
        let mut seg = match self.load_segment(&segment_id) {
            Ok(existing) => existing,
            Err(e) if e.is_not_found() => TimelineSegment::new(segment_id),
            Err(e) => return Err(e),
        };

        let cs = {
            // 3. Acquire the write lock. Waits for all in-flight read-lock
            //    holders (the flush above, concurrent backend evictions) to
            //    drain.
            let timeline = &io_control.timeline;
            let _write_guard = timeline.lock.write();

            let prev_ckpt = timeline.head_ckpt;
            timeline.set_redo_ckpt(*redo_ckpt);

            // 4. Drain the centralized shmem draft ring + its on-disk spill
            //    file. Non-destructive: the spill file survives until the
            //    segment PUT is durable, so a failed commit retries with the
            //    same drained contents.
            let drained = timeline.draft.drain(&self.draft_spill)?;
            let cs = self.commit_segment(*commit_ckpt, prev_ckpt, *redo_ckpt, &mut seg, drained)?;

            timeline.push_active(
                *commit_ckpt,
                prev_ckpt,
                cs.chunks.iter().copied(),
                cs.relforks.iter().map(|(rf, meta)| (*rf, *meta)),
            );

            // Segment is durable; discard the drained draft.
            timeline.draft.commit_drain(&self.draft_spill)?;

            cs
        };

        // 5. Update DbMeta JSON — outside the write lock; it touches no
        //    timeline state.
        self.update_db_meta(commit_ckpt)?;

        pg_log_debug1(format!(
            "tiko: run_commit_protocol at {commit_ckpt}: prev={} chunks={} relforks={}",
            cs.prev_ckpt,
            cs.chunks.len(),
            cs.relforks.len(),
        ));

        Ok(())
    }
}
