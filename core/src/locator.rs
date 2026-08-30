use crate::timeline::{Checkpoint, SegmentId};
use crate::{
    chunk::{ChunkRef, ChunkTag},
    db::DbNamespace,
    relfork::RelFork,
};
use pgsys::lsn::Lsn;
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
        let rf = RelFork::from(tag);
        format!(
            "{ns}/chunks/{ckpt}/{rf}/{chunk_id}",
            ns = self.ns,
            ckpt = ckpt.to_path_string(),
            rf = rf,
            chunk_id = tag.chunk_id
        )
    }

    /// Like [`chunk`](Self::chunk) but addressing an arbitrary `db_id` within
    /// this locator's org. Used by [`chunk_base`](Self::chunk_base) for
    /// copy-on-write reads of chunks owned by another database (e.g. a branch
    /// reading its parent's chunks via `ChunkRef.db_id`).
    fn chunk_in_db(&self, tag: &ChunkTag, ckpt: &Checkpoint, db_id: u64) -> String {
        let rf = RelFork::from(tag);
        format!(
            "{org}/{db}/chunks/{ckpt}/{rf}/{chunk_id}",
            org = self.ns.org_id,
            db = db_id,
            ckpt = ckpt.to_path_string(),
            rf = rf,
            chunk_id = tag.chunk_id
        )
    }

    pub(super) fn chunk_base(&self, tag: &ChunkTag, chunk_ref: &ChunkRef) -> String {
        // The base manifest references a chunk version at the checkpoint LSN
        // at which it was sealed (ChunkRef.timeline_id + ChunkRef.lsn). COW:
        // the chunk lives in the OWNING database's namespace — `chunk_ref.db_id`
        // — so a branch's base manifest (seeded from the parent) resolves
        // shared chunks from the parent's namespace. `db_id` is always the
        // real ENV_DB_ID of the writing database (never a placeholder).
        let ckpt = Checkpoint::new(TimelineId::from(chunk_ref.timeline_id), chunk_ref.lsn);
        self.chunk_in_db(tag, &ckpt, chunk_ref.db_id)
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

    // ── Base backup keys ────────────────────────────

    /// Listing prefix for all base backups: `{ns}/backup/`.
    pub(super) fn backup_dir(&self) -> String {
        format!("{ns}/backup/", ns = self.ns)
    }

    /// Storage key for a base-backup tarball: `{ns}/backup/{tl}/{lsn}.tar.zst`.
    pub(super) fn backup_object(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{ns}/backup/{tl}/{lsn}.tar.zst",
            ns = self.ns,
            tl = ckpt.timeline_id,
            lsn = ckpt.lsn.to_hex(),
        )
    }

    /// Storage key for a base-backup metadata sidecar: `{ns}/backup/{tl}/{lsn}.json`.
    pub(super) fn backup_meta(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{ns}/backup/{tl}/{lsn}.json",
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

    /// Listing prefix for one timeline's WAL objects: `{ns}/wal/{tl:08X}/`.
    pub(crate) fn wal_timeline_dir(&self, timeline_id: TimelineId) -> String {
        format!("{ns}/wal/{tl}/", ns = self.ns, tl = timeline_id.to_hex())
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

/// Parse a base-manifest storage key into its `Checkpoint`.
///
/// Keys have the shape `{bases_prefix}{tl_hex}/{lsn_hex}.manifest`, where
/// `bases_prefix` is `Locator::bases_dir()` (`{ns}/bases/`). Returns `None` for
/// any key that doesn't match (so callers can `filter_map` over a raw listing).
pub(crate) fn parse_base_manifest_ckpt(key: &str, bases_prefix: &str) -> Option<Checkpoint> {
    let rel = key.strip_prefix(bases_prefix)?;
    let (tl_hex, rest) = rel.split_once('/')?;
    let lsn_hex = rest.strip_suffix(".manifest")?;
    let timeline_id = TimelineId::from_hex(tl_hex).ok()?;
    let lsn = Lsn::from_hex(lsn_hex).ok()?;
    Some(Checkpoint::new(timeline_id, lsn))
}

#[cfg(test)]
mod base_select_tests {
    use super::*;

    fn ckpt(tl: u32, lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(tl), Lsn::new(lsn))
    }

    #[test]
    fn parses_valid_key_and_rejects_others() {
        let prefix = "12/5/bases/";
        let key = "12/5/bases/00000001/0000000003000028.manifest";
        assert_eq!(
            parse_base_manifest_ckpt(key, prefix),
            Some(ckpt(1, 0x3000028))
        );

        assert_eq!(
            parse_base_manifest_ckpt("12/5/bases/00000001", prefix),
            None
        );
        assert_eq!(
            parse_base_manifest_ckpt("12/5/other/x.manifest", prefix),
            None
        );
        assert_eq!(
            parse_base_manifest_ckpt("12/5/bases/zz/0000000000000001.manifest", prefix),
            None
        );
    }
}
