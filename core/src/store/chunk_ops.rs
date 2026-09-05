use super::Store;
use crate::{
    chunk::{CHUNK_SIZE, ChunkTag},
    error::{Error, Result},
    io_control::IoControl,
    timeline::Checkpoint,
};
use pgsys::common::BLCKSZ;

impl Store {
    // ── Chunk operations ──────────────────────────────────────────────────

    pub(crate) fn get_chunk(&self, tag: &ChunkTag, dst: &mut [u8]) -> Result<()> {
        debug_assert_eq!(dst.len(), CHUNK_SIZE);

        if let Some(io_control) = IoControl::try_get() {
            let _guard = io_control.timeline.lock.read();
            let timeline = &io_control.timeline;

            let head_ckpt = timeline.head_ckpt;
            let base_ckpt = timeline.base_ckpt;

            // 1. Probe the current head prefix only if the draft buffer
            //    reports the tag is recorded for this interval. Without this
            //    gate, every `get_chunk` would speculatively GET head-prefix
            //    even when the chunk wasn't touched in this interval.
            //    `contains_chunk` is conservative — false positives degrade
            //    to the legacy speculative-GET behavior; false negatives are
            //    impossible (a recorded chunk is either in-memory or in the
            //    spill file, and `contains_chunk` returns true in both cases).
            if timeline.draft.contains_chunk(tag) && self.try_read_chunk_at(tag, &head_ckpt, dst)? {
                return Ok(());
            }

            // 2. Active window newest → oldest, gated by Bloom filter. Bloom
            //    false positives fall through to the next entry; false
            //    negatives are impossible.
            let mut oldest_active_ckpt: Option<Checkpoint> = None;
            for ac in timeline.iter_active() {
                oldest_active_ckpt = Some(ac.ckpt);
                if !ac.chunk_bloom.maybe_contains(tag) {
                    continue;
                }
                if self.try_read_chunk_at(tag, &ac.prev_ckpt, dst)? {
                    return Ok(());
                }
            }

            // 3. On-disk segments below the active window, down to base_ckpt.
            //    `oldest_active_ckpt` is exclusive — its data was already
            //    probed via the active-window Bloom walk above.
            let seg_top_ckpt = oldest_active_ckpt.unwrap_or(head_ckpt);
            if self.read_chunk_from_segments(tag, base_ckpt, seg_top_ckpt, dst)? {
                return Ok(());
            }
        }

        // 4. Base manifest fallback.
        let chunk_ref = self.base_manifest()?.lookup(tag)?;
        if let Some(chunk_ref) = chunk_ref {
            let key = self.ns.chunk_base(tag, &chunk_ref);
            let src = self.storage_get(&key)?;
            dst.copy_from_slice(&src);
            return Ok(());
        }

        Err(Error::not_found("chunk not found in storage"))
    }

    pub(crate) fn patch_chunk(&self, tag: &ChunkTag, block_idx: u32, data: &[u8]) -> Result<()> {
        debug_assert!(!data.is_empty());
        debug_assert_eq!(data.len() % BLCKSZ, 0);

        let byte_offset = block_idx as usize * BLCKSZ;
        debug_assert!(byte_offset + data.len() <= CHUNK_SIZE);

        let is_full_chunk = byte_offset == 0 && data.len() == CHUNK_SIZE;

        // Eviction-flush path: hold the timeline read lock across
        // (read head_ckpt → PUT → record into draft). The checkpointer
        // flushes dirty cache state *before* acquiring its write lock,
        // so this read lock never re-enters from the commit side.
        //
        // `IoControl::get()` is always valid here: `tiko_init` ran via
        // `smgrinit` for every mode that can call `patch_chunk`.
        let io_control = IoControl::get();
        let timeline = &io_control.timeline;
        let _timeline_guard = timeline.lock.read();

        let head_ckpt = timeline.head_ckpt;
        let key = self.ns.chunk(tag, &head_ckpt);

        if is_full_chunk {
            self.storage_put(&key, data)?;
        } else {
            let mut merged = vec![0u8; CHUNK_SIZE];
            match self.get_chunk(tag, &mut merged) {
                Ok(()) => {}
                Err(e) if e.is_not_found() => {} // chunk absent → treat as zeros
                Err(e) => return Err(e),
            }
            merged[byte_offset..byte_offset + data.len()].copy_from_slice(data);
            self.storage_put(&key, &merged)?;
        };

        // Record the chunk into the draft buffer
        self.record_chunk_eviction(*tag);

        Ok(())
    }
}
