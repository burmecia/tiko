//! Database namespace and object-storage key layout.
//!
//! [`DbNamespace`] (`{org}/{db}`, built from `TIKO_ORG_ID`/`TIKO_DB_ID`) is
//! the root of every storage key and carries the single definition of the key
//! layout — only `org_id` and `db_id` ever appear in keys, and `for_db`
//! derives a sibling namespace in the same org (the COW mechanism). [`DbMeta`]
//! is the `db_meta.json` sidecar tracking the database's latest checkpoint
//! and branch lineage.

use serde::{Deserialize, Serialize};
use std::{fmt, sync::Mutex};

use crate::chunk::{ChunkRef, ChunkTag};
use crate::env;
use crate::error::Result;
use crate::relfork::RelFork;
use crate::timeline::{Checkpoint, SegmentId};
use pgsys::Lsn;
use pgsys::timeline_id::TimelineId;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DbNamespace {
    pub org_id: u64,
    pub db_id: u64,
}

impl DbNamespace {
    pub fn new(org_id: u64, db_id: u64) -> Self {
        Self { org_id, db_id }
    }

    pub(crate) fn new_from_env() -> Result<Self> {
        let org_id = env::read_u64(env::ENV_ORG_ID)?;
        let db_id = env::read_u64(env::ENV_DB_ID)?;
        Ok(DbNamespace::new(org_id, db_id))
    }
}

/// Single definition of the object-storage key layout under a namespace
/// (`{org}/{db}/`): `db_meta.json`, `chunks/{ckpt}/{rf}/{chunk_id}`,
/// `bases/{tl}/{lsn}.manifest`, `backup/{tl}/{lsn}.tar.zst` (+ `.json`
/// sidecar), `timeline/{segment}`, `wal/{tl}/{segment}[.chunks/{byte_offset:016X}]`.
/// Directory prefixes are defined once and composed into full keys, so
/// builders and parsers cannot drift apart.
impl DbNamespace {
    /// Derive the namespace of another database in the same org — the COW
    /// mechanism: a branch resolves chunks owned by its parent
    /// (`ChunkRef.db_id`) straight from the parent's namespace.
    pub(crate) fn for_db(&self, db_id: u64) -> DbNamespace {
        DbNamespace::new(self.org_id, db_id)
    }

    // ── Database meta key ────────────────────────────

    pub(crate) fn db_meta(&self) -> String {
        format!("{ns}/db_meta.json", ns = self)
    }

    // ── Chunk keys ────────────────────────────

    pub(crate) fn chunk(&self, tag: &ChunkTag, ckpt: &Checkpoint) -> String {
        let rf = RelFork::from(tag);
        format!(
            "{ns}/chunks/{ckpt}/{rf}/{chunk_id}",
            ns = self,
            ckpt = ckpt.to_path_string(),
            rf = rf,
            chunk_id = tag.chunk_id
        )
    }

    /// Chunk key for a base-manifest reference. The base manifest references a
    /// chunk version at the checkpoint LSN at which it was sealed
    /// (`ChunkRef.timeline_id` + `ChunkRef.lsn`), and the chunk lives in the
    /// OWNING database's namespace (`chunk_ref.db_id`, always the real
    /// ENV_DB_ID of the writing database), so a branch's base manifest seeded
    /// from the parent resolves shared chunks from the parent's namespace.
    pub(crate) fn chunk_base(&self, tag: &ChunkTag, chunk_ref: &ChunkRef) -> String {
        let ckpt = Checkpoint::new(TimelineId::from(chunk_ref.timeline_id), chunk_ref.lsn);
        self.for_db(chunk_ref.db_id).chunk(tag, &ckpt)
    }

    // ── Base manifest keys ────────────────────────────

    /// Listing prefix for all base manifests: `{ns}/bases/`.
    pub(crate) fn bases_dir(&self) -> String {
        format!("{ns}/bases/", ns = self)
    }

    /// Storage key for a base manifest: `{ns}/bases/{tl}/{lsn}.manifest`.
    pub(crate) fn base_manifest(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{}{}/{}.manifest",
            self.bases_dir(),
            ckpt.timeline_id,
            ckpt.lsn.to_hex(),
        )
    }

    /// Parse a base-manifest key back into its `Checkpoint`. Returns `None` for
    /// any key not under `bases_dir()` or not matching the expected shape, so
    /// callers can `filter_map` over a raw listing.
    pub(crate) fn parse_base_manifest(&self, key: &str) -> Option<Checkpoint> {
        let rel = key.strip_prefix(&self.bases_dir())?;
        let (tl_hex, rest) = rel.split_once('/')?;
        let lsn_hex = rest.strip_suffix(".manifest")?;
        let timeline_id = TimelineId::from_hex(tl_hex).ok()?;
        let lsn = Lsn::from_hex(lsn_hex).ok()?;
        Some(Checkpoint::new(timeline_id, lsn))
    }

    // ── Base backup keys ────────────────────────────

    /// Listing prefix for all base backups: `{ns}/backup/`.
    pub(crate) fn backup_dir(&self) -> String {
        format!("{ns}/backup/", ns = self)
    }

    /// Storage key for a base-backup tarball: `{ns}/backup/{tl}/{lsn}.tar.zst`.
    pub(crate) fn backup_object(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{}{}/{}.tar.zst",
            self.backup_dir(),
            ckpt.timeline_id,
            ckpt.lsn.to_hex(),
        )
    }

    /// Storage key for a base-backup metadata sidecar: `{ns}/backup/{tl}/{lsn}.json`.
    pub(crate) fn backup_meta(&self, ckpt: &Checkpoint) -> String {
        format!(
            "{}{}/{}.json",
            self.backup_dir(),
            ckpt.timeline_id,
            ckpt.lsn.to_hex(),
        )
    }

    // ── Timeline segment keys ────────────────────────────

    /// Listing prefix for all timeline segment files: `{ns}/timeline/`.
    pub(crate) fn timeline_segments_dir(&self) -> String {
        format!("{ns}/timeline/", ns = self)
    }

    /// Storage key for a timeline segment file: `{ns}/timeline/{segment}`.
    pub(crate) fn timeline_segment(&self, segment_id: &SegmentId) -> String {
        format!(
            "{}{}",
            self.timeline_segments_dir(),
            segment_id.to_path_string()
        )
    }

    // ── WAL keys ────────────────────────────

    /// Listing prefix for one timeline's WAL objects: `{ns}/wal/{tl:08X}/`.
    pub(crate) fn wal_timeline_dir(&self, timeline_id: TimelineId) -> String {
        format!("{ns}/wal/{tl}/", ns = self, tl = timeline_id.to_hex())
    }

    /// Storage key for a sealed WAL segment: `{ns}/wal/{tl}/{wal_segment}`.
    pub fn wal_segment(&self, timeline_id: TimelineId, wal_segment: &str) -> String {
        format!("{}{}", self.wal_timeline_dir(timeline_id), wal_segment)
    }

    /// Prefix for all 256 KiB chunk objects belonging to one in-flight segment:
    /// `{ns}/wal/{tl}/{wal_segment}.chunks/`. The `.chunks` suffix distinguishes
    /// the chunk directory from the sealed segment object (`{wal_segment}`)
    /// stored at the same parent prefix.
    pub fn wal_chunk_prefix(&self, timeline_id: TimelineId, wal_segment: &str) -> String {
        format!(
            "{}{}.chunks/",
            self.wal_timeline_dir(timeline_id),
            wal_segment
        )
    }

    /// Key for one 256 KiB streaming chunk within an in-flight WAL segment:
    /// `{ns}/wal/{tl}/{wal_segment}.chunks/{byte_offset:016X}`.
    pub fn wal_chunk_key(
        &self,
        timeline_id: TimelineId,
        wal_segment: &str,
        byte_offset: usize,
    ) -> String {
        format!(
            "{}{:016X}",
            self.wal_chunk_prefix(timeline_id, wal_segment),
            byte_offset
        )
    }
}

impl fmt::Display for DbNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.org_id, self.db_id)
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct DbMetaInner {
    #[serde(flatten)]
    ns: DbNamespace,
    parent_db_id: Option<u64>,
    parent_checkpoint_lsn: Option<Lsn>,
    parent_timeline_id: Option<u32>,
    checkpoint: Checkpoint,
    created_at: i64,
    status: String,
    deleted_at: Option<i64>,
}

pub(crate) struct DbMeta {
    inner: Mutex<DbMetaInner>,
}

impl DbMeta {
    pub(crate) fn new(ns: DbNamespace) -> Self {
        let inner = DbMetaInner {
            ns,
            parent_db_id: None,
            parent_checkpoint_lsn: None,
            parent_timeline_id: None,
            checkpoint: Checkpoint::default(),
            // created_at: chrono::Utc::now().timestamp(),
            created_at: 0,
            status: "active".to_string(),
            deleted_at: None,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(crate) fn set_checkpoint_lsn(&self, ckpt: &Checkpoint) {
        let mut inner = self.inner.lock().unwrap();
        inner.checkpoint = *ckpt;
    }

    pub(crate) fn load_from_json_bytes(&self, bytes: &[u8]) {
        let inner: DbMetaInner = serde_json::from_slice(bytes).expect("failed to load DbMetaInner");
        let mut guard = self.inner.lock().unwrap();
        *guard = inner;
    }

    pub(crate) fn to_json_bytes(&self) -> Vec<u8> {
        let inner = self.inner.lock().unwrap();
        serde_json::to_vec(&*inner).expect("failed to serialize DbMetaInner")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ckpt(tl: u32, lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(tl), Lsn::new(lsn))
    }

    fn ns() -> DbNamespace {
        DbNamespace::new(12, 5)
    }

    #[test]
    fn parses_valid_base_manifest_key_and_rejects_others() {
        let ns = ns();
        assert_eq!(
            ns.parse_base_manifest("12/5/bases/00000001/0000000003000028.manifest"),
            Some(ckpt(1, 0x3000028))
        );
        assert_eq!(ns.parse_base_manifest("12/5/bases/00000001"), None);
        assert_eq!(ns.parse_base_manifest("12/5/other/x.manifest"), None);
        assert_eq!(
            ns.parse_base_manifest("12/5/bases/zz/0000000000000001.manifest"),
            None
        );

        // Round-trip with the builder.
        let key = ns.base_manifest(&ckpt(1, 0x3000028));
        assert_eq!(ns.parse_base_manifest(&key), Some(ckpt(1, 0x3000028)));
    }

    #[test]
    fn for_db_addresses_sibling_namespace() {
        let tag = ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 2619,
            fork_number: 0,
            chunk_id: 42,
        };
        let chunk_ref = ChunkRef {
            db_id: 7,
            timeline_id: 1,
            lsn: Lsn::new(0x3000028),
        };
        assert_eq!(
            ns().chunk_base(&tag, &chunk_ref),
            "12/7/chunks/00000001/0000000003000028/1663/5/2619.0/42"
        );
    }
}
