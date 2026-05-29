use super::timeline::{Checkpoint, SegmentId};
use crate::{chunk::ChunkTag, db::DbNamespace, manifest::ChunkRef};
use pgsys::timeline_id::TimelineId;

pub struct Locator {
    ns: DbNamespace,
}

impl Locator {
    pub(crate) fn new(ns: DbNamespace) -> Self {
        Self { ns }
    }

    // ── Database meta key ────────────────────────────

    pub(super) fn db_meta(&self) -> String {
        format!("{ns}/db_meta.json", ns = self.ns)
    }

    // ── Chunk keys ────────────────────────────

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

    // ── Base manifest keys ────────────────────────────

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

    // ── Timeline segment keys ────────────────────────────

    /// S3/storage key for a timeline segment file: `{ns}/timeline/{segment}`.
    pub(crate) fn timeline_segment(&self, segment_id: &SegmentId) -> String {
        format!(
            "{ns}/timeline/{segment}",
            ns = self.ns,
            segment = segment_id.to_path_string()
        )
    }

    /// Listing prefix for all timeline segment files: `{ns}/timeline/`.
    pub(crate) fn timeline_segments_dir(&self) -> String {
        format!("{ns}/timeline/", ns = self.ns)
    }

    // ── WAL and metadata keys ────────────────────────────

    pub fn wal_segment(&self, timeline_id: TimelineId, wal_segment: &str) -> String {
        format!(
            "{ns}/wal/{timeline_id}/{wal_segment}",
            ns = self.ns,
            timeline_id = timeline_id.to_hex(),
            wal_segment = wal_segment
        )
    }

    /// Prefix for all 256 KiB chunk objects belonging to one in-flight segment.
    ///
    /// `{ns}/wal/{timeline_id}/{wal_segment}.chunks/`
    ///
    /// The `.chunks` suffix distinguishes the chunk directory from the sealed
    /// segment object (`{wal_segment}`) stored at the same parent prefix.
    pub fn wal_chunk_prefix(&self, timeline_id: TimelineId, wal_segment: &str) -> String {
        format!(
            "{ns}/wal/{timeline_id}/{wal_segment}.chunks/",
            ns = self.ns,
            timeline_id = timeline_id.to_hex(),
            wal_segment = wal_segment
        )
    }

    /// Key for one 256 KiB streaming chunk within an in-flight WAL segment.
    ///
    /// `{ns}/wal/{timeline_id}/{wal_segment}.chunks/{byte_offset:016X}`
    pub fn wal_chunk_key(
        &self,
        timeline_id: TimelineId,
        wal_segment: &str,
        byte_offset: usize,
    ) -> String {
        format!(
            "{}/{:016X}",
            self.wal_chunk_prefix(timeline_id, wal_segment),
            byte_offset
        )
    }
}
