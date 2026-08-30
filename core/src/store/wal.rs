use super::Store;
use crate::error::{Error, Result};
use pgsys::{common::XLOG_SEG_SIZE, timeline_id::TimelineId};

/// One WAL segment's coverage on a timeline, in absolute LSN. `full` = a sealed
/// segment covering its entire `XLOG_SEG_SIZE` range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegEntry {
    seg_no: u64,
    lo: u64,
    hi: u64,
    full: bool,
}

/// Parse a WAL object key under `wal_prefix` (= `{ns}/wal/{tl}/`) into its
/// segment number and, for chunk objects, the chunk byte offset.
///
/// Sealed segment: `{wal_prefix}{segname}`                      → (seg_no, None)
/// Chunk:          `{wal_prefix}{segname}.chunks/{offset:016X}` → (seg_no, Some(offset))
/// `segname` is 24 hex chars; `seg_no` is hex chars [8..24). `None` for non-matches.
fn parse_wal_key(key: &str, wal_prefix: &str) -> Option<(u64, Option<usize>)> {
    let rel = key.strip_prefix(wal_prefix)?;
    if let Some((segname, offpart)) = rel.split_once(".chunks/") {
        if segname.len() != 24 || !segname.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let seg_no = u64::from_str_radix(&segname[8..24], 16).ok()?;
        let off = usize::from_str_radix(offpart, 16).ok()?;
        Some((seg_no, Some(off)))
    } else {
        if rel.len() != 24 || !rel.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let seg_no = u64::from_str_radix(&rel[8..24], 16).ok()?;
        Some((seg_no, None))
    }
}

/// Compute the contiguous archived-WAL run that reaches the highest segment in
/// `entries`. Returns `(w_lo, w_hi)` absolute LSN, or `None` if empty.
///
/// The highest segment anchors the run end (`w_hi`). The run extends down
/// through consecutive segments whose coverage is contiguous: `cur` must cover
/// from its own segment start (no mid-segment front gap inside `cur`), and the
/// next-lower segment's coverage end (`hi`) must reach `cur`'s segment start.
/// Both sealed segments and chunks-only segments qualify — the streaming WAL
/// receiver writes chunks contiguously, so a chunks-only segment whose `hi`
/// reaches the next segment's boundary is fully covered up to that point.
fn wal_contiguous_run(entries: &[SegEntry]) -> Option<(u64, u64)> {
    let seg = XLOG_SEG_SIZE as u64;
    let mut sorted: Vec<SegEntry> = entries.to_vec();
    sorted.sort_unstable_by_key(|e| std::cmp::Reverse(e.seg_no)); // descending
    let top = *sorted.first()?;
    let (mut w_lo, w_hi) = (top.lo, top.hi);
    let mut cur = top;
    let mut idx = 1;
    loop {
        // Segment 0 is the lowest possible — nothing below it. (Also guards the
        // `cur.seg_no - 1` below against debug-mode underflow.)
        if cur.seg_no == 0 {
            break;
        }
        // Can only extend below if `cur` covers from its own segment start
        // (no mid-segment front gap inside `cur`).
        if cur.lo != cur.seg_no * seg {
            break;
        }
        let Some(next) = sorted.get(idx).copied() else {
            break;
        };
        // next must be the immediately-lower segment whose coverage reaches
        // cur's segment start. Sealed or chunks-only both qualify here.
        if next.seg_no != cur.seg_no - 1 || next.hi != cur.lo {
            break;
        }
        w_lo = next.lo;
        cur = next;
        idx += 1;
    }
    Some((w_lo, w_hi))
}

/// A base manifest is usable as a PITR anchor if its recovery WAL fits inside
/// the contiguous archived run `[w_lo, w_hi]`: the replay start (`redo`) must be
/// archived, and its checkpoint record must be within coverage.
pub(super) fn is_base_usable(ckpt_lsn: u64, redo_lsn: u64, w_lo: u64, w_hi: u64) -> bool {
    redo_lsn >= w_lo && ckpt_lsn <= w_hi
}

impl Store {
    /// Compute the contiguous archived-WAL run `[w_lo, w_hi]` (absolute LSN) for
    /// `timeline`, reaching the highest archived segment. Lists `{ns}/wal/{tl}/`,
    /// classifies sealed segments vs partial chunks, and GETs the highest
    /// segment's last chunk for its byte length when that segment is partial.
    pub(super) fn archived_wal_run(&self, timeline: TimelineId) -> Result<(u64, u64)> {
        let seg = XLOG_SEG_SIZE as u64;
        let prefix = self.lctr.wal_timeline_dir(timeline);
        let keys = match self.storage_list_prefix(&prefix) {
            Ok(k) => k,
            Err(e) if e.is_not_found() => Vec::new(),
            Err(e) => return Err(e),
        };

        struct Acc {
            sealed: bool,
            min_off: Option<usize>,
            max_off: Option<usize>,
        }
        let mut segs: std::collections::BTreeMap<u64, Acc> = std::collections::BTreeMap::new();
        for key in &keys {
            let Some((seg_no, off)) = parse_wal_key(key, &prefix) else {
                continue;
            };
            let acc = segs.entry(seg_no).or_insert(Acc {
                sealed: false,
                min_off: None,
                max_off: None,
            });
            match off {
                None => acc.sealed = true,
                Some(o) => {
                    acc.min_off = Some(acc.min_off.map_or(o, |m| m.min(o)));
                    acc.max_off = Some(acc.max_off.map_or(o, |m| m.max(o)));
                }
            }
        }
        if segs.is_empty() {
            return Err(Error::other(
                "no archived WAL for timeline; nothing is recoverable yet",
            ));
        }

        let mut entries: Vec<SegEntry> = Vec::with_capacity(segs.len());
        for (&seg_no, acc) in &segs {
            if acc.sealed {
                // Sealed is authoritative even if leftover chunks exist.
                entries.push(SegEntry {
                    seg_no,
                    lo: seg_no * seg,
                    hi: (seg_no + 1) * seg,
                    full: true,
                });
            } else {
                let min_off = acc.min_off.unwrap_or(0);
                let lo = seg_no * seg + min_off as u64;
                // Compute the coverage end (hi) for EVERY chunks-only segment
                // (not just the highest) so chunks-only segments can bridge the
                // contiguous run. hi needs the length of the last chunk; a
                // missing last chunk falls back to `max_off` (len 0).
                let max_off = acc.max_off.unwrap_or(0);
                let name = format!("{}{:016X}", timeline.to_hex(), seg_no);
                let chunk_key = self.lctr.wal_chunk_key(timeline, &name, max_off);
                let last_len = match self.storage_get(&chunk_key) {
                    Ok(b) => b.len() as u64,
                    Err(_) => 0,
                };
                let hi = seg_no * seg + max_off as u64 + last_len;
                entries.push(SegEntry {
                    seg_no,
                    lo,
                    hi,
                    full: false,
                });
            }
        }

        wal_contiguous_run(&entries)
            .ok_or_else(|| Error::other("no archived WAL for timeline; nothing is recoverable yet"))
    }
}

#[cfg(test)]
mod wal_coverage_tests {
    use super::{SegEntry, is_base_usable, parse_wal_key, wal_contiguous_run};
    use pgsys::common::XLOG_SEG_SIZE;

    const SEG: u64 = XLOG_SEG_SIZE as u64;

    fn sealed(seg_no: u64) -> SegEntry {
        SegEntry {
            seg_no,
            lo: seg_no * SEG,
            hi: (seg_no + 1) * SEG,
            full: true,
        }
    }

    #[test]
    fn parse_wal_key_sealed_and_chunk() {
        let p = "12/34/wal/00000001/";
        assert_eq!(
            parse_wal_key("12/34/wal/00000001/000000010000000000000002", p),
            Some((2, None))
        );
        assert_eq!(
            parse_wal_key(
                "12/34/wal/00000001/000000010000000000000002.chunks/000000000001F898",
                p
            ),
            Some((2, Some(0x1F898)))
        );
        assert_eq!(parse_wal_key("12/34/wal/00000001/not-a-segment", p), None);
        assert_eq!(parse_wal_key("12/34/other/x", p), None);
    }

    #[test]
    fn contiguous_run_sealed_chain() {
        let entries = vec![sealed(0), sealed(1), sealed(2)];
        assert_eq!(wal_contiguous_run(&entries), Some((0, 3 * SEG)));
    }

    #[test]
    fn contiguous_run_partial_top_over_sealed() {
        let top = SegEntry {
            seg_no: 2,
            lo: 2 * SEG,
            hi: 2 * SEG + 0x500,
            full: false,
        };
        let entries = vec![sealed(0), sealed(1), top];
        assert_eq!(wal_contiguous_run(&entries), Some((0, 2 * SEG + 0x500)));
    }

    #[test]
    fn contiguous_run_midsegment_start_no_extend() {
        let top = SegEntry {
            seg_no: 2,
            lo: 2 * SEG + 0x1F898,
            hi: 2 * SEG + 0x5F898,
            full: false,
        };
        assert_eq!(
            wal_contiguous_run(&[top]),
            Some((2 * SEG + 0x1F898, 2 * SEG + 0x5F898))
        );
    }

    #[test]
    fn contiguous_run_chunks_only_bridge() {
        // All chunks-only (never sealed). seg4 is the top with a partial tail;
        // seg3 covers fully up to the seg4 boundary; seg2 began mid-stream
        // (its redo/archive starts partway in). The run should bridge down to
        // seg2's mid-stream start.
        let seg4 = SegEntry {
            seg_no: 4,
            lo: 4 * SEG,
            hi: 4 * SEG + 0x500,
            full: false,
        };
        let seg3 = SegEntry {
            seg_no: 3,
            lo: 3 * SEG,
            hi: 4 * SEG, // reaches the seg4 boundary
            full: false,
        };
        let seg2 = SegEntry {
            seg_no: 2,
            lo: 2 * SEG + 0x41EE8, // mid-stream start
            hi: 3 * SEG,           // reaches the seg3 boundary
            full: false,
        };
        assert_eq!(
            wal_contiguous_run(&[seg2, seg3, seg4]),
            Some((2 * SEG + 0x41EE8, 4 * SEG + 0x500))
        );

        // A chunks-only segment that does NOT reach the boundary must stop the
        // walk (gap between seg3's end and seg4's start).
        let seg3_short = SegEntry {
            seg_no: 3,
            lo: 3 * SEG,
            hi: 4 * SEG - 1,
            full: false,
        };
        assert_eq!(
            wal_contiguous_run(&[seg3_short, seg4]),
            Some((4 * SEG, 4 * SEG + 0x500))
        );
    }

    #[test]
    fn contiguous_run_gap_stops_walk() {
        let entries = vec![sealed(1), sealed(3)];
        assert_eq!(wal_contiguous_run(&entries), Some((3 * SEG, 4 * SEG)));
    }

    #[test]
    fn contiguous_run_empty() {
        assert_eq!(wal_contiguous_run(&[]), None);
    }

    #[test]
    fn base_usability() {
        assert!(is_base_usable(150, 120, 100, 200));
        assert!(!is_base_usable(150, 90, 100, 200));
        assert!(!is_base_usable(250, 120, 100, 200));
    }
}
