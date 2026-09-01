use super::Store;
use crate::{
    error::{Error, Result},
    io_control::IoControl,
    relfork::{RelFork, RelForkMeta},
    timeline::{Checkpoint, RelForkLookup},
};
use pgsys::common::BlockNumber;

impl Store {
    // ── RelFork meta operations ──────────────────────────────────────────────────

    pub(crate) fn get_meta(&self, rf: &RelFork) -> Result<RelForkMeta> {
        if let Some(io_control) = IoControl::try_get() {
            let _guard = io_control.timeline.lock.read();

            let timeline = &io_control.timeline;
            let head_ckpt = timeline.head_ckpt;
            let base_ckpt = timeline.base_ckpt;

            // 1. Live interval: shmem draft buffer is the sole source of
            //    truth for uncommitted writes. Falls back to the spill file
            //    transparently if the in-memory zone has been drained.
            if let Some(meta) = timeline.draft.get_relfork(rf, &self.draft_spill)? {
                return Ok(meta);
            }

            // 2. Active window newest → oldest, gated by inline relfork index.
            //    A `Hit` returns directly; a `DefinitiveMiss` means the relfork
            //    was not touched in that checkpoint (safe to skip).
            //    An `Inconclusive` (index overflowed) means the relfork *may*
            //    have been written in this checkpoint — we must stop the
            //    in-memory walk and let the segment scan find the truth.
            //    Continuing past an Inconclusive would risk returning a stale
            //    `Hit` from an older active checkpoint while a newer write
            //    sits unread in the overflowed checkpoint's segment file.
            let mut oldest_active_ckpt: Option<Checkpoint> = None;
            for ac in timeline.iter_active() {
                oldest_active_ckpt = Some(ac.ckpt);
                match ac.relfork_index.get(rf) {
                    RelForkLookup::Hit(meta) => return Ok(meta),
                    RelForkLookup::DefinitiveMiss => continue,
                    RelForkLookup::Inconclusive => break,
                }
            }

            // 3. Segment scan up to `oldest_active_ckpt` inclusive.
            //    - If the loop broke on Inconclusive at K, `oldest_active_ckpt`
            //      is K and we need K's segment file (it may carry the rf
            //      even though K's inline index didn't expose it). Active
            //      checkpoints newer than K reported DefinitiveMiss, and
            //      since a non-overflowed `RelForkIndex` mirrors its
            //      segment's relfork map exactly, their segments don't
            //      carry the rf either — no need to re-read them.
            //    - If every active checkpoint reported DefinitiveMiss, the
            //      loop ran to completion and `oldest_active_ckpt` is the
            //      oldest active checkpoint. Its segment will be
            //      re-confirmed empty by the segment scan, which then
            //      continues down to `base_ckpt`.
            let seg_top_ckpt = oldest_active_ckpt.unwrap_or(head_ckpt);
            if let Some(meta) = self.read_relfork_from_segments(rf, base_ckpt, seg_top_ckpt)? {
                return Ok(meta);
            }
        }

        // 3. Base manifest fallback.
        if let Some(meta) = self.base_manifest()?.lookup_relfork_meta(rf)? {
            return Ok(meta);
        }

        Err(Error::not_found("relfork not found"))
    }

    pub(crate) fn put_meta(&self, rf: &RelFork, meta: &RelForkMeta) -> Result<()> {
        // The draft buffer is the sole source of truth for live-interval
        // relfork meta. The meta is captured into the next segment when the
        // commit protocol drains the draft.
        //
        // Hold the timeline read lock across the draft record so the entry
        // is observed by `get_meta` callers within this interval's window.
        // The checkpointer's write lock waits for all in-flight read-lock
        // holders to drain (the fence — see plan, "Commit protocol"); the
        // checkpointer flushes dirty cache state *before* acquiring its
        // write lock, so no re-entrancy risk.
        //
        // `IoControl::try_get()` is always `Some` here: `tiko_init` (via
        // `smgrinit`) runs in every mode that can reach this code path —
        // bootstrap, single-user, and runtime — and `init_or_attach` has
        // succeeded by then.
        let io_control = IoControl::get();
        let _timeline_guard = io_control.timeline.lock.read();
        self.record_relfork_eviction(*rf, *meta);
        Ok(())
    }

    pub(crate) fn get_nblocks(&self, rf: &RelFork) -> Result<BlockNumber> {
        let meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        Ok(meta.nblocks)
    }

    pub(crate) fn put_nblocks(&self, rf: &RelFork, nblocks: BlockNumber) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.nblocks = nblocks;
        self.put_meta(rf, &meta)
    }

    pub(crate) fn get_deleted(&self, rf: &RelFork) -> Result<bool> {
        let meta = self.get_meta(rf)?;
        Ok(meta.deleted)
    }

    pub(crate) fn create_relfork(&self, rf: &RelFork) -> Result<()> {
        match self.get_meta(rf) {
            Ok(meta) => {
                if !meta.deleted {
                    return Err(Error::already_exists("relfork already exists"));
                }
                self.put_meta(rf, &RelForkMeta::default())
            }
            Err(err) if err.is_not_found() => self.put_meta(rf, &RelForkMeta::default()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn delete_relfork(&self, rf: &RelFork) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.deleted = true;
        self.put_meta(rf, &meta)
    }
}
