use crate::{
    checkpoint_history::CheckpointVersion, chunk::ChunkTag, db::DbNamespace, manifest::ChunkRef,
    relfork::RelFork,
};
use pgsys::Lsn;

pub(super) struct Locator {
    ns: DbNamespace,
}

impl Locator {
    pub(super) fn new(ns: DbNamespace) -> Self {
        Self { ns }
    }

    pub(super) fn db_meta(&self) -> String {
        format!("{ns}/db_meta.json", ns = self.ns)
    }

    fn relfork_meta_key(&self, rf: &RelFork, timeline_id: u32, lsn: Lsn) -> String {
        format!(
            "{ns}/chunks/{tl}/{lsn}/{rf}/relfork_meta.json",
            ns = self.ns,
            tl = timeline_id,
            lsn = lsn.to_hex(),
            rf = rf
        )
    }

    pub(super) fn relfork_meta_versioned(
        &self,
        rf: &RelFork,
        versions: &[CheckpointVersion],
    ) -> Vec<String> {
        versions
            .iter()
            .map(|version| self.relfork_meta_key(rf, version.timeline_id, version.lsn))
            .collect()
    }

    fn chunk_key(&self, tag: &ChunkTag, timeline_id: u32, lsn: Lsn) -> String {
        let rf = tag.relfork();
        format!(
            "{ns}/chunks/{tl}/{lsn}/{rf}/{chunk_id}",
            ns = self.ns,
            tl = timeline_id,
            lsn = lsn.to_hex(),
            rf = rf,
            chunk_id = tag.chunk_id
        )
    }

    pub(super) fn chunk_versioned(
        &self,
        tag: &ChunkTag,
        versions: &[CheckpointVersion],
    ) -> Vec<String> {
        versions
            .iter()
            .map(|version| self.chunk_key(tag, version.timeline_id, version.lsn))
            .collect()
    }

    pub(super) fn chunk_base(&self, _tag: &ChunkTag, _chunk_ref: &ChunkRef) -> String {
        todo!("standard chunk keys not yet implemented");
    }
}
