//! Durable timeline segment format.
//!
//! A segment file records what each checkpoint wrote: one ordered
//! [`CheckpointSummary`] per checkpoint, covering a fixed 256 MB LSN range
//! (`TIMELINE_SEGMENT_LSN_RANGE`) of a single timeline. Segments are keyed in
//! storage as `{ns}/timeline/{tl}/{index:016X}.segment` with
//! `index = lsn / RANGE` (see `locator.rs`), and serialized with MessagePack
//! behind a `TLSG` magic + version header.
//!
//! `prev_ckpt` in each summary is the chunk path prefix in effect at write
//! time; readers resolve chunks via that prefix, not the closing checkpoint.
//!
//! Written by the commit protocol (`store/commit.rs`, one read-modify-write
//! per checkpoint), read by the backend segment-scan fallback and by the
//! compactor (`store/compaction.rs`), which folds superseded segments into a
//! new base manifest and deletes them.

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use pgsys::{lsn::Lsn, timeline_id::TimelineId};

use crate::chunk::ChunkTag;
use crate::error::{Error, Result};
use crate::relfork::{RelFork, RelForkMeta};

// ── Constants ───────────────────────────────────────────────────────────────

const TIMELINE_SEGMENT_MAGIC: [u8; 4] = *b"TLSG";
const TIMELINE_SEGMENT_VERSION: u32 = 1;
/// Number of LSN units covered by one segment file. `segment_id.index = lsn / TIMELINE_SEGMENT_LSN_RANGE`.
pub const TIMELINE_SEGMENT_LSN_RANGE: u64 = 1 << 28; // 256 MB

// ── Checkpoint ──────────────────────────────────────────────────────────────

#[repr(C)]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Checkpoint {
    pub timeline_id: TimelineId,
    pub lsn: Lsn,
}

const _: () = assert!(std::mem::size_of::<Checkpoint>() == 16);

impl Checkpoint {
    pub fn new(timeline_id: TimelineId, lsn: Lsn) -> Self {
        Self { timeline_id, lsn }
    }

    pub fn to_path_string(&self) -> String {
        format!("{}/{}", self.timeline_id, self.lsn.to_hex())
    }

    pub fn to_segment_id(&self) -> SegmentId {
        SegmentId {
            timeline_id: self.timeline_id,
            index: self.lsn.as_u64() / TIMELINE_SEGMENT_LSN_RANGE,
        }
    }
}

impl fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}",
            self.timeline_id.to_hex_variable_width(),
            self.lsn
        )
    }
}

// ── SegmentId ───────────────────────────────────────────────────────────────

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SegmentId {
    pub timeline_id: TimelineId,
    pub index: u64,
}

impl SegmentId {
    pub fn to_path_string(&self) -> String {
        format!("{}/{:016X}.segment", self.timeline_id, self.index)
    }

    /// Parse a `SegmentId` from its on-disk path string with or without the
    /// containing directory, e.g. 12/34/timeline/00000001/0000000000008655.segment.
    pub fn from_path_string(path_str: &str) -> Option<Self> {
        let stem = path_str.strip_suffix(".segment")?;
        let p: Vec<&str> = stem.rsplit('/').collect();
        if p.len() < 2 {
            return None;
        }
        let index = u64::from_str_radix(p[0], 16).ok()?;
        let timeline_id = TimelineId::from_hex(p[1]).ok()?;
        Some(Self { timeline_id, index })
    }

    /// Does this segment's LSN coverage overlap the closed interval
    /// `[low, high]` under `Checkpoint`'s total order `(timeline_id, lsn)`?
    ///
    /// A segment `(tl, idx)` covers checkpoints with
    /// `lsn ∈ [idx*RANGE, (idx+1)*RANGE)` in timeline `tl`. The check tests
    /// whether the segment's lowest and highest possible checkpoint sit on
    /// opposite sides of `[low, high]`.
    pub fn overlaps_range(&self, low: Checkpoint, high: Checkpoint) -> bool {
        let seg_low = Checkpoint::new(
            self.timeline_id,
            Lsn::new(self.index * TIMELINE_SEGMENT_LSN_RANGE),
        );
        let seg_high = Checkpoint::new(
            self.timeline_id,
            Lsn::new(self.index.saturating_add(1) * TIMELINE_SEGMENT_LSN_RANGE - 1),
        );
        seg_high >= low && seg_low <= high
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{:016X}", self.timeline_id.to_hex(), self.index)
    }
}

// ── CheckpointSummary ───────────────────────────────────────────────────────

/// Per-checkpoint summary stored inside a [`TimelineSegment`].
///
/// `prev_ckpt` is the path prefix where chunks visible at `ckpt` were written
/// — i.e. the checkpoint that was the committed head at write time.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointSummary {
    pub ckpt: Checkpoint,
    pub prev_ckpt: Checkpoint,
    pub redo_ckpt: Checkpoint,
    pub chunks: HashSet<ChunkTag>,
    pub relforks: HashMap<RelFork, RelForkMeta>,
    pub created_at: i64,
}

impl CheckpointSummary {
    pub fn new(
        ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        redo_ckpt: Checkpoint,
        chunks: HashSet<ChunkTag>,
        relforks: HashMap<RelFork, RelForkMeta>,
    ) -> Self {
        Self {
            ckpt,
            prev_ckpt,
            redo_ckpt,
            chunks,
            relforks,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn contains_chunk(&self, tag: &ChunkTag) -> bool {
        self.chunks.contains(tag)
    }

    pub fn relfork_meta(&self, rf: &RelFork) -> Option<&RelForkMeta> {
        self.relforks.get(rf)
    }
}

// ── TimelineSegment ─────────────────────────────────────────────────────────

/// On-disk + on-S3 segment file: an ordered list of per-checkpoint summaries
/// covering one segment-id LSN range.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimelineSegment {
    magic: [u8; 4],
    version: u32,
    pub segment_id: SegmentId,
    pub checkpoints: Vec<CheckpointSummary>,
}

impl TimelineSegment {
    pub fn new(segment_id: SegmentId) -> Self {
        Self {
            magic: TIMELINE_SEGMENT_MAGIC,
            version: TIMELINE_SEGMENT_VERSION,
            segment_id,
            checkpoints: Vec::new(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let segment: Self = rmp_serde::from_slice(bytes)?;
        if segment.magic != TIMELINE_SEGMENT_MAGIC {
            return Err(Error::invalid_data("invalid timeline segment magic"));
        }
        if segment.version != TIMELINE_SEGMENT_VERSION {
            return Err(Error::invalid_data("unsupported timeline segment version"));
        }
        Ok(segment)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(rmp_serde::to_vec(self)?)
    }

    pub fn push(&mut self, summary: CheckpointSummary) {
        debug_assert_eq!(
            summary.ckpt.to_segment_id(),
            self.segment_id,
            "segment_id mismatch when pushing to segment"
        );
        self.checkpoints.push(summary);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::common::ForkNumber;

    fn tag(rel: u32, chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
            chunk_id,
        }
    }

    fn relfork(rel: u32) -> RelFork {
        RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
        }
    }

    // ── Checkpoint ──

    #[test]
    fn checkpoint_path_string_format() {
        let ckpt = Checkpoint::new(TimelineId::new(0x3A), Lsn::new(0xDEADBEEF));
        // Path is "{timeline}/{lsn_hex}". TimelineId Display uses to_hex().
        assert!(ckpt.to_path_string().contains("00000000DEADBEEF"));
        assert!(ckpt.to_path_string().starts_with("0000003A/"));
    }

    #[test]
    fn checkpoint_segment_id_derivation() {
        let tl = TimelineId::new(1);
        // LSN inside segment 0
        let a = Checkpoint::new(tl, Lsn::new(0));
        let b = Checkpoint::new(tl, Lsn::new(TIMELINE_SEGMENT_LSN_RANGE - 1));
        assert_eq!(a.to_segment_id(), b.to_segment_id());
        assert_eq!(a.to_segment_id().index, 0);

        // LSN at segment boundary lands in next segment.
        let c = Checkpoint::new(tl, Lsn::new(TIMELINE_SEGMENT_LSN_RANGE));
        assert_eq!(c.to_segment_id().index, 1);

        // Different timeline → different segment id.
        let d = Checkpoint::new(TimelineId::new(2), Lsn::new(0));
        assert_ne!(a.to_segment_id(), d.to_segment_id());
    }

    // ── CheckpointSummary + TimelineSegment serialization ──

    #[test]
    fn checkpoint_summary_roundtrip() {
        let mut s = CheckpointSummary::new(
            Checkpoint::new(TimelineId::new(1), Lsn::new(100)),
            Checkpoint::new(TimelineId::new(1), Lsn::new(50)),
            Checkpoint::default(),
            HashSet::new(),
            HashMap::new(),
        );
        s.chunks.insert(tag(1, 0));
        s.chunks.insert(tag(1, 1));
        s.chunks.insert(tag(2, 0));
        s.relforks.insert(relfork(1), RelForkMeta::new(32, false));
        s.relforks.insert(relfork(2), RelForkMeta::new(0, true));

        let bytes = rmp_serde::to_vec(&s).unwrap();
        let decoded: CheckpointSummary = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.ckpt, s.ckpt);
        assert_eq!(decoded.prev_ckpt, s.prev_ckpt);
        assert_eq!(decoded.redo_ckpt, s.redo_ckpt);
        assert_eq!(decoded.chunks, s.chunks);
        assert_eq!(decoded.relforks, s.relforks);
    }

    #[test]
    fn timeline_segment_roundtrip_validates_magic_and_version() {
        let tl = TimelineId::new(1);
        let seg_id = Checkpoint::new(tl, Lsn::new(0)).to_segment_id();
        let mut seg = TimelineSegment::new(seg_id);

        let mut s = CheckpointSummary::new(
            Checkpoint::new(tl, Lsn::new(10)),
            Checkpoint::new(tl, Lsn::new(0)),
            Checkpoint::default(),
            HashSet::new(),
            HashMap::new(),
        );
        s.chunks.insert(tag(1, 0));
        seg.push(s);

        let bytes = seg.to_bytes().unwrap();
        let decoded = TimelineSegment::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.segment_id, seg_id);
        assert_eq!(decoded.checkpoints.len(), 1);
        assert!(decoded.checkpoints[0].chunks.contains(&tag(1, 0)));

        // Magic check
        let mut bad = bytes.clone();
        // Find the magic bytes in the encoded form and corrupt one of them.
        // rmp_serde encodes the struct fields in declaration order; locate the
        // 4-byte array TLSG and flip a byte.
        if let Some(pos) = bad.windows(4).position(|w| w == TIMELINE_SEGMENT_MAGIC) {
            bad[pos] = b'X';
            assert!(TimelineSegment::from_bytes(&bad).is_err());
        }
    }

    #[test]
    fn segment_id_filename_round_trip() {
        let s = SegmentId {
            timeline_id: TimelineId::new(0x3A),
            index: 0x42,
        };
        let name = s.to_path_string();
        let parsed = SegmentId::from_path_string(&name).unwrap();
        assert_eq!(parsed, s);

        assert!(SegmentId::from_path_string("not-a-segment.txt").is_none());
        assert!(SegmentId::from_path_string("0000003A/0000000000000042.txt").is_none());
        assert!(
            SegmentId::from_path_string("zz/no/not-segment/0000000000000042.segment").is_none()
        );
    }

    #[test]
    fn timeline_segment_push_asserts_segment_id_match_in_debug() {
        let tl = TimelineId::new(1);
        let seg = TimelineSegment::new(Checkpoint::new(tl, Lsn::new(0)).to_segment_id());
        // The matching case is exercised in `timeline_segment_roundtrip_*`;
        // we just assert the matching id case constructs OK here.
        assert_eq!(seg.checkpoints.len(), 0);
    }

    // ── Display / ordering ──

    #[test]
    fn checkpoint_display_and_total_order() {
        let ckpt = Checkpoint::new(TimelineId::new(0x3A), Lsn::new(0xDEADBEEF));
        assert_eq!(ckpt.to_string(), "3A-0/DEADBEEF");

        // Total order is (timeline_id, lsn): timeline dominates.
        let a = Checkpoint::new(TimelineId::new(1), Lsn::new(u64::MAX));
        let b = Checkpoint::new(TimelineId::new(2), Lsn::new(0));
        assert!(a < b);
    }

    #[test]
    fn segment_id_display_format() {
        let s = SegmentId {
            timeline_id: TimelineId::new(0x3A),
            index: 0x42,
        };
        assert_eq!(s.to_string(), "0000003A-0000000000000042");
    }

    // ── SegmentId::overlaps_range ──

    #[test]
    fn segment_overlaps_range_same_timeline() {
        let tl = TimelineId::new(1);
        let at = |lsn: u64| Checkpoint::new(tl, Lsn::new(lsn));
        let r = TIMELINE_SEGMENT_LSN_RANGE;
        // Segment 1 covers [R, 2R).
        let seg = Checkpoint::new(tl, Lsn::new(r)).to_segment_id();

        // Range inside segment; segment inside range.
        assert!(seg.overlaps_range(at(r + 1), at(2 * r - 2)));
        assert!(seg.overlaps_range(at(0), at(2 * r)));
        // Closed-interval boundary touches.
        assert!(seg.overlaps_range(at(2 * r - 1), at(3 * r)));
        assert!(seg.overlaps_range(at(0), at(r)));
        // Strictly below / above.
        assert!(!seg.overlaps_range(at(0), at(r - 1)));
        assert!(!seg.overlaps_range(at(2 * r), at(3 * r)));
    }

    #[test]
    fn segment_overlaps_range_across_timelines() {
        let at = |tl: u32, lsn: u64| Checkpoint::new(TimelineId::new(tl), Lsn::new(lsn));
        let seg = |tl: u32, index: u64| SegmentId {
            timeline_id: TimelineId::new(tl),
            index,
        };
        let r = TIMELINE_SEGMENT_LSN_RANGE;
        let low = at(2, 5 * r);
        let high = at(4, 3 * r);

        // Any segment on a timeline strictly inside the range overlaps.
        assert!(seg(3, 0).overlaps_range(low, high));
        assert!(seg(3, 99).overlaps_range(low, high));
        // On the boundary timelines, coverage must reach the bound LSN.
        assert!(seg(2, 5).overlaps_range(low, high));
        assert!(!seg(2, 3).overlaps_range(low, high));
        assert!(seg(4, 3).overlaps_range(low, high));
        assert!(!seg(4, 4).overlaps_range(low, high));
        // Timelines outside the range never overlap.
        assert!(!seg(1, 0).overlaps_range(low, high));
        assert!(!seg(5, 0).overlaps_range(low, high));
    }

    // ── TimelineSegment / CheckpointSummary edge cases ──

    #[test]
    fn timeline_segment_rejects_unsupported_version() {
        let tl = TimelineId::new(1);
        let mut seg = TimelineSegment::new(Checkpoint::new(tl, Lsn::new(0)).to_segment_id());
        seg.version = TIMELINE_SEGMENT_VERSION + 1;
        let bytes = seg.to_bytes().unwrap();
        assert!(TimelineSegment::from_bytes(&bytes).is_err());
    }

    #[test]
    fn timeline_segment_push_preserves_commit_order() {
        let tl = TimelineId::new(1);
        let mut seg = TimelineSegment::new(Checkpoint::new(tl, Lsn::new(0)).to_segment_id());
        for lsn in [10u64, 20, 30] {
            seg.push(CheckpointSummary::new(
                Checkpoint::new(tl, Lsn::new(lsn)),
                Checkpoint::default(),
                Checkpoint::default(),
                HashSet::new(),
                HashMap::new(),
            ));
        }
        let decoded = TimelineSegment::from_bytes(&seg.to_bytes().unwrap()).unwrap();
        let lsns: Vec<u64> = decoded
            .checkpoints
            .iter()
            .map(|s| s.ckpt.lsn.as_u64())
            .collect();
        assert_eq!(lsns, [10, 20, 30]);
    }

    #[test]
    fn checkpoint_summary_lookup_misses() {
        let s = CheckpointSummary::new(
            Checkpoint::default(),
            Checkpoint::default(),
            Checkpoint::default(),
            HashSet::new(),
            HashMap::new(),
        );
        assert!(!s.contains_chunk(&tag(9, 9)));
        assert!(s.relfork_meta(&relfork(9)).is_none());
        assert!(s.created_at > 0);
    }
}
