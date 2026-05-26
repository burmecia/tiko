use super::timeline::{Checkpoint, SegmentId};
use crate::{chunk::ChunkTag, db::DbNamespace, manifest::ChunkRef};
use pgsys::timeline_id::TimelineId;

pub(crate) struct Locator {
    ns: DbNamespace,
}

impl Locator {
    pub(crate) fn new(ns: DbNamespace) -> Self {
        Self { ns }
    }

    pub(super) fn db_meta(&self) -> String {
        format!("{ns}/db_meta.json", ns = self.ns)
    }

    pub(super) fn chunk(&self, tag: &ChunkTag, ckpt: &Checkpoint) -> String {
        let rf = tag.relfork();
        format!(
            "{ns}/chunks/{ckpt}/{rf}/{chunk_id}",
            ns = self.ns,
            ckpt = ckpt.to_path_string(),
            rf = rf,
            chunk_id = tag.chunk_id
        )
    }

    pub(super) fn chunk_base(&self, tag: &ChunkTag, chunk_ref: &ChunkRef) -> String {
        // The base manifest references chunks at their physical express
        // prefix — the checkpoint LSN at which the chunk version was
        // sealed. ChunkRef.timeline_id + ChunkRef.lsn reconstructs that
        // prefix; the layout matches `chunk()` so reads share the path.
        let ckpt = Checkpoint::new(TimelineId::from(chunk_ref.timeline_id), chunk_ref.lsn);
        self.chunk(tag, &ckpt)
    }

    pub(super) fn bases_dir(&self) -> String {
        format!("{ns}/bases/", ns = self.ns)
    }

    pub(super) fn base_manifest(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{ns}/bases/{tl}/{lsn}.manifest",
            ns = self.ns,
            tl = ckpt.timeline_id,
            lsn = ckpt.lsn.to_hex(),
        )
    }

    /// S3/storage key for a timeline segment file: `{ns}/timeline/{segment_id}.segment`.
    pub(crate) fn timeline_segment(&self, segment_id: &SegmentId) -> String {
        format!("{ns}/timeline/{segment_id}.segment", ns = self.ns)
    }

    /// Listing prefix for all timeline segment files: `{ns}/timeline/`.
    pub(crate) fn timeline_segments_dir(&self) -> String {
        format!("{ns}/timeline/", ns = self.ns)
    }
}
