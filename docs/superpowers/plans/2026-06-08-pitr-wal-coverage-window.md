# PITR WAL-Coverage Recovery Window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Store::recovery_window` report the actually-PITR-recoverable range, bounded by the contiguous archived-WAL run and anchored at the oldest base manifest whose recovery WAL is archived — so `tiko_pitr list` shows a truthful window and `recover` refuses targets the WAL can't back.

**Architecture:** Add pure helpers (WAL-key parsing, contiguous-run computation, base usability) plus a `Store::archived_wal_run` that lists `{ns}/wal/{tl}/`, computes the run (GET-ing the top partial chunk for its length), then `recovery_window` selects the oldest usable base. `RecoveryWindow.latest_ckpt` becomes `latest_lsn` (= run end). The binary's `list`/`recover` consume the revised window.

**Tech Stack:** Rust (edition 2024); the project's `core` storage layer (`Store`, `Locator`, `Manifest`), `pgsys` (`Lsn`, `TimelineId`, `XLOG_SEG_SIZE`).

**Reference spec:** `docs/superpowers/specs/2026-06-08-pitr-wal-coverage-window-design.md`

**Conventions (project memory / CLAUDE.md):**
- Build: `cargo build -p core -p cli` must succeed. Tests: `cargo test -p core`.
- `cargo clippy` is blocked by pre-existing `pgsys` lint errors (unrelated); verify lint-cleanliness via a warning-free build.
- Commit after each task. Branch `pitr2`. Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

**Verified facts (rely on these; report BLOCKED with the exact compiler error if one is wrong):**
- `core/src/io/store.rs` imports `pgsys::{lsn::Lsn, timeline_id::TimelineId, common::{BLCKSZ, BlockNumber}, logging::...}` and `super::timeline::{Checkpoint, ...}`. `XLOG_SEG_SIZE` is **not** yet imported. `parse_base_manifest_ckpt(key, prefix) -> Option<Checkpoint>` is a module-scope fn.
- `Lsn`: `pub fn new(u64)`, `pub const fn as_u64(self) -> u64`, `to_pg_string()`. `Checkpoint` has public `timeline_id: TimelineId`, `lsn: Lsn`, is `Copy`/`Ord`.
- `Manifest`: `redo_ckpt() -> Checkpoint` (carries `#[allow(dead_code)]`), `checkpoint() -> Checkpoint`, `timestamp() -> i64`, `from_bytes(&[u8], &Path) -> Result<Self>` (all public).
- `Locator` (`core/src/io/locator.rs`): `wal_chunk_key(timeline_id, wal_segment: &str, byte_offset: usize) -> String`, `bases_dir()`. `TimelineId::to_hex()` → 8 hex chars.
- `Store`: `storage_list_prefix(&str) -> Result<Vec<String>>`, `storage_get(&str) -> Result<Vec<u8>>`, `list_checkpoints() -> Result<Vec<CheckpointRow>>` (sorted ascending; `CheckpointRow{ ckpt, redo_ckpt, created_at, n_chunks }`). `self.lctr` is the `Locator`.
- `RecoveryWindow` is consumed only by `cli/src/bin/tiko_pitr.rs` (`run_list`, `run_recover`).

**Sequencing:** Task 1 adds pure helpers (additive; their `dead_code` warning under plain build is expected until Task 2 — do NOT add `#[allow(dead_code)]`; tests reference them). Task 2 is the atomic integration (struct field change + `recovery_window` rewrite + locator helper + binary updates) in one commit so the workspace stays green.

---

### Task 1: Pure WAL-coverage helpers (TDD)

**Files:** Modify `core/src/io/store.rs`.

- [ ] **Step 1: Add the `XLOG_SEG_SIZE` import**

In `core/src/io/store.rs`, change the `pgsys::common` import line inside the existing `use pgsys::{ ... };` block from:
```rust
    common::{BLCKSZ, BlockNumber},
```
to:
```rust
    common::{BLCKSZ, BlockNumber, XLOG_SEG_SIZE},
```

- [ ] **Step 2: Write the failing tests**

Append to the END of `core/src/io/store.rs`:
```rust
#[cfg(test)]
mod wal_coverage_tests {
    use super::{SegEntry, is_base_usable, parse_wal_key, wal_contiguous_run};
    use pgsys::common::XLOG_SEG_SIZE;

    const SEG: u64 = XLOG_SEG_SIZE as u64;

    fn sealed(seg_no: u64) -> SegEntry {
        SegEntry { seg_no, lo: seg_no * SEG, hi: (seg_no + 1) * SEG, full: true }
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
        let top = SegEntry { seg_no: 2, lo: 2 * SEG, hi: 2 * SEG + 0x500, full: false };
        let entries = vec![sealed(0), sealed(1), top];
        assert_eq!(wal_contiguous_run(&entries), Some((0, 2 * SEG + 0x500)));
    }

    #[test]
    fn contiguous_run_midsegment_start_no_extend() {
        // single chunks-only segment starting mid-segment: no extension below.
        let top = SegEntry { seg_no: 2, lo: 2 * SEG + 0x1F898, hi: 2 * SEG + 0x5F898, full: false };
        assert_eq!(
            wal_contiguous_run(&[top]),
            Some((2 * SEG + 0x1F898, 2 * SEG + 0x5F898))
        );
    }

    #[test]
    fn contiguous_run_gap_stops_walk() {
        // top seg 3 sealed, seg 1 sealed, seg 2 MISSING → run is only seg 3.
        let entries = vec![sealed(1), sealed(3)];
        assert_eq!(wal_contiguous_run(&entries), Some((3 * SEG, 4 * SEG)));
    }

    #[test]
    fn contiguous_run_empty() {
        assert_eq!(wal_contiguous_run(&[]), None);
    }

    #[test]
    fn base_usability() {
        assert!(is_base_usable(150, 120, 100, 200)); // redo & ckpt inside
        assert!(!is_base_usable(150, 90, 100, 200)); // redo before w_lo
        assert!(!is_base_usable(250, 120, 100, 200)); // ckpt after w_hi
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p core wal_coverage_tests`
Expected: FAIL — `cannot find ... SegEntry / parse_wal_key / wal_contiguous_run / is_base_usable`.

- [ ] **Step 4: Implement the helpers**

Add at module scope in `core/src/io/store.rs` (a good spot is right after `select_newest_base_at_or_before`):
```rust
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
/// Sealed segment: `{wal_prefix}{segname}`                         → (seg_no, None)
/// Chunk:          `{wal_prefix}{segname}.chunks/{offset:016X}`    → (seg_no, Some(offset))
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
/// The highest segment anchors the run end. The run extends down through
/// consecutive `full` (sealed) segments while contiguous; it stops at the first
/// missing/partial lower segment, or as soon as a segment does not start at its
/// own segment boundary (a mid-segment front gap).
fn wal_contiguous_run(entries: &[SegEntry]) -> Option<(u64, u64)> {
    let seg = XLOG_SEG_SIZE as u64;
    let mut sorted: Vec<SegEntry> = entries.to_vec();
    sorted.sort_unstable_by(|a, b| b.seg_no.cmp(&a.seg_no)); // descending
    let top = *sorted.first()?;
    let (mut w_lo, w_hi) = (top.lo, top.hi);
    let mut cur = top;
    let mut idx = 1;
    loop {
        // Can only extend below if `cur` covers from its own segment start.
        if cur.lo != cur.seg_no * seg {
            break;
        }
        let Some(next) = sorted.get(idx).copied() else {
            break;
        };
        // Must be the immediately-lower segment, full, and contiguous.
        if next.seg_no != cur.seg_no - 1 || !next.full || next.hi != cur.lo {
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
fn is_base_usable(ckpt_lsn: u64, redo_lsn: u64, w_lo: u64, w_hi: u64) -> bool {
    redo_lsn >= w_lo && ckpt_lsn <= w_hi
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p core wal_coverage_tests`
Expected: PASS (7 passed).

- [ ] **Step 6: Commit**

```bash
git add core/src/io/store.rs
git commit -m "feat(core): add pure WAL-coverage helpers for PITR window"
```

(Expected `dead_code` warnings for `SegEntry`/`parse_wal_key`/`wal_contiguous_run`/`is_base_usable` under plain `cargo build -p core` until Task 2 — do NOT suppress; tests keep `cargo test -p core` clean.)

---

### Task 2: WAL-coverage-bounded `recovery_window` + binary updates (atomic)

This is one commit: the locator helper, `Store::archived_wal_run`, the `RecoveryWindow` field change, the `recovery_window` rewrite, and the `tiko_pitr` `list`/`recover` updates — together so the workspace compiles.

**Files:** Modify `core/src/io/locator.rs`, `core/src/io/store.rs`, `core/src/manifest.rs`, `cli/src/bin/tiko_pitr.rs`.

- [ ] **Step 1: Add the locator helper**

In `core/src/io/locator.rs`, add (next to the other `wal_*` helpers):
```rust
    /// Listing prefix for one timeline's WAL objects: `{ns}/wal/{tl:08X}/`.
    pub(crate) fn wal_timeline_dir(&self, timeline_id: TimelineId) -> String {
        format!("{ns}/wal/{tl}/", ns = self.ns, tl = timeline_id.to_hex())
    }
```

- [ ] **Step 2: Drop the stale `#[allow(dead_code)]` on `Manifest::redo_ckpt`**

In `core/src/manifest.rs`, `redo_ckpt()` gains a real caller in this task. Remove the `#[allow(dead_code)]` attribute (and any preceding "not yet called" comment line) immediately above `pub fn redo_ckpt(&self) -> Checkpoint`, leaving the `///` doc and the method.

- [ ] **Step 3: Change the `RecoveryWindow` struct**

In `core/src/io/store.rs`, replace:
```rust
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint,
    pub latest_ts: i64,
    pub latest_ckpt: Checkpoint,
    pub timeline: TimelineId,
}
```
with:
```rust
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint,
    pub latest_ts: i64,
    /// End of the contiguous archived-WAL run (highest recoverable LSN).
    pub latest_lsn: Lsn,
    pub timeline: TimelineId,
}
```

- [ ] **Step 4: Add the `archived_wal_run` method**

Add inside `impl Store { ... }` (e.g. just before `recovery_window`):
```rust
    /// Compute the contiguous archived-WAL run `[w_lo, w_hi]` (absolute LSN) for
    /// `timeline`, reaching the highest archived segment. Lists `{ns}/wal/{tl}/`,
    /// classifies sealed segments vs partial chunks, and GETs the highest
    /// segment's last chunk for its byte length when that segment is partial.
    fn archived_wal_run(&self, timeline: TimelineId) -> Result<(u64, u64)> {
        let seg = XLOG_SEG_SIZE as u64;
        let prefix = self.lctr.wal_timeline_dir(timeline);
        let keys = match self.storage_list_prefix(&prefix) {
            Ok(k) => k,
            Err(e) if e.is_not_found() => Vec::new(),
            Err(e) => return Err(e),
        };

        // Per seg_no: sealed flag + min/max chunk offset.
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
        let Some(&highest) = segs.keys().next_back() else {
            return Err(Error::other(
                "no archived WAL for timeline; nothing is recoverable yet",
            ));
        };

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
                // Only the highest segment's exact end matters; lower partial
                // segments never extend the run, so their `hi` is unused.
                let hi = if seg_no == highest {
                    let max_off = acc.max_off.unwrap_or(0);
                    let name = format!("{}{:016X}", timeline.to_hex(), seg_no);
                    let chunk_key = self.lctr.wal_chunk_key(timeline, &name, max_off);
                    let len = self.storage_get(&chunk_key)?.len();
                    seg_no * seg + max_off as u64 + len as u64
                } else {
                    lo
                };
                entries.push(SegEntry {
                    seg_no,
                    lo,
                    hi,
                    full: false,
                });
            }
        }

        wal_contiguous_run(&entries).ok_or_else(|| {
            Error::other("no archived WAL for timeline; nothing is recoverable yet")
        })
    }
```

- [ ] **Step 5: Rewrite `recovery_window`**

Replace the entire existing `pub fn recovery_window(&self) -> Result<RecoveryWindow> { ... }` with:
```rust
    /// Compute the PITR-recoverable window bounded by archived-WAL coverage:
    /// `earliest` = the oldest base manifest whose recovery WAL is inside the
    /// contiguous archived run; `latest_lsn` = the end of that run. Errors with
    /// a clear message when nothing is recoverable yet.
    pub fn recovery_window(&self) -> Result<RecoveryWindow> {
        // 1. Timeline = newest checkpoint's timeline.
        let rows = self.list_checkpoints()?;
        let newest = rows
            .last()
            .ok_or_else(|| Error::other("no checkpoints found; nothing is recoverable yet"))?;
        let timeline = newest.ckpt.timeline_id;

        // 2. Contiguous archived-WAL run for this timeline.
        let (w_lo, w_hi) = self.archived_wal_run(timeline)?;

        // 3. Oldest usable base: ascending by checkpoint, first whose redo WAL
        //    is archived. Candidates are bases on this timeline with checkpoint
        //    within the run.
        let bases_prefix = self.lctr.bases_dir();
        let base_keys = self.storage_list_prefix(&bases_prefix)?;
        let mut candidates: Vec<(Checkpoint, String)> = base_keys
            .iter()
            .filter_map(|k| parse_base_manifest_ckpt(k, &bases_prefix).map(|c| (c, k.clone())))
            .filter(|(c, _)| {
                c.timeline_id == timeline
                    && c.lsn.as_u64() >= w_lo
                    && c.lsn.as_u64() <= w_hi
            })
            .collect();
        candidates.sort_by_key(|(c, _)| *c);

        let tmp = tempfile::tempdir()?;
        let mut chosen: Option<(Checkpoint, i64)> = None;
        for (ckpt, key) in &candidates {
            let bytes = self.storage_get(key)?;
            let base = Manifest::from_bytes(&bytes, tmp.path())?;
            if is_base_usable(ckpt.lsn.as_u64(), base.redo_ckpt().lsn.as_u64(), w_lo, w_hi) {
                chosen = Some((base.checkpoint(), base.timestamp()));
                break;
            }
        }
        let (earliest_ckpt, earliest_ts) = chosen.ok_or_else(|| {
            Error::other("no base manifest's WAL is archived; nothing is recoverable yet")
        })?;

        // 4. Latest: run end, and the newest checkpoint time within the run.
        let latest_lsn = Lsn::new(w_hi);
        let latest_ts = rows
            .iter()
            .filter(|r| r.ckpt.lsn.as_u64() <= w_hi)
            .map(|r| r.created_at)
            .max()
            .unwrap_or(earliest_ts);

        Ok(RecoveryWindow {
            earliest_ts,
            earliest_ckpt,
            latest_ts,
            latest_lsn,
            timeline,
        })
    }
```

- [ ] **Step 6: Update `run_list` in the binary**

In `cli/src/bin/tiko_pitr.rs`, replace the entire `run_list` function with:
```rust
fn run_list(store: &Store) -> Result<()> {
    let w = store.recovery_window()?;
    let fmt_ts = |ts: i64| {
        DateTime::<Utc>::from_timestamp(ts, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| ts.to_string())
    };
    println!("recoverable window (timeline {}):", w.timeline.to_hex());
    println!(
        "  earliest: {}   lsn {}",
        fmt_ts(w.earliest_ts),
        w.earliest_ckpt.lsn.to_pg_string()
    );
    println!(
        "  latest:   {}   lsn {}",
        fmt_ts(w.latest_ts),
        w.latest_lsn.to_pg_string()
    );
    Ok(())
}
```

- [ ] **Step 7: Update the `--lsn` bounds check in `run_recover`**

In `cli/src/bin/tiko_pitr.rs`, in `run_recover`, change the `--lsn` bounds check from:
```rust
        if l < window.earliest_ckpt.lsn || l > window.latest_ckpt.lsn {
```
to:
```rust
        if l < window.earliest_ckpt.lsn || l > window.latest_lsn {
```
(Leave the `--time` check — `target_ts < window.earliest_ts || target_ts > window.latest_ts` — unchanged. Leave base selection unchanged.)

- [ ] **Step 8: Build + test**

Run: `cargo build -p core -p cli`
Expected: clean build, no warnings (all Task-1 helpers are now used; the `redo_ckpt` allow removed without reintroducing a warning).
Run: `cargo test -p core`
Expected: all pass, including `wal_coverage_tests` and the existing `base_select_tests`.

- [ ] **Step 9: Smoke-check the CLI compiles + help**

Run: `cargo run -p cli --bin tiko_pitr -- --help`
Expected: shows `list` and `recover`.

- [ ] **Step 10: Commit**

```bash
git add core/src/io/locator.rs core/src/io/store.rs core/src/manifest.rs cli/src/bin/tiko_pitr.rs
git commit -m "feat(pitr): bound recovery window by archived-WAL coverage"
```

---

### Task 3: Full build + test gate

**Files:** none (verification only)

- [ ] **Step 1: Build**

Run: `cargo build -p core -p cli`
Expected: clean.

- [ ] **Step 2: Warning count**

Run: `cargo build -p core -p cli 2>&1 | grep -c warning`
Expected: `0`.

- [ ] **Step 3: Tests**

Run: `cargo test -p core`
Expected: all pass (incl. `wal_coverage_tests::*`).

---

## Notes for the implementer

- **Integration (out of band):** the full `recovery_window` (listing + GETs) and end-to-end `list`/`recover` against a live archive are verified with the PITR test, per project convention — including that the current single-256-KiB-chunk test data now correctly yields "nothing is recoverable yet".
- **Candidate filter vs `is_base_usable`:** the candidate filter keeps bases with `checkpoint ∈ [w_lo, w_hi]`; `is_base_usable` additionally requires `redo >= w_lo`. A usable base has `redo >= w_lo` ⟹ `checkpoint >= redo >= w_lo`, so the filter never drops a usable base. Ascending iteration returns the oldest usable one.
- **Only one chunk GET:** only the highest segment's last chunk is GET-ed (for its length); lower partial segments never extend the run, so their exact end is irrelevant.
- **`RecoveryWindow` consumers:** only `cli/src/bin/tiko_pitr.rs`. If `cargo build -p cli` reports a use of `latest_ckpt`, it's a missed spot — update it to `latest_lsn`.
