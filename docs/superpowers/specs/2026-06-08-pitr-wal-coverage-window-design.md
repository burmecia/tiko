# PITR recovery window bounded by archived-WAL coverage

**Date:** 2026-06-08
**Status:** Approved (design)

## Goal

Make `Store::recovery_window` (used by both `tiko_pitr list` and `tiko_pitr
recover`) report the **actually PITR-recoverable** range — bounded by what
archived WAL covers — instead of `[oldest base manifest, newest checkpoint]`
regardless of WAL. `list` then shows a truthful recoverable time/LSN window, and
`recover` refuses targets the WAL can't back (preventing the PostgreSQL
`could not locate a valid checkpoint record` PANIC observed when a base's
recovery WAL was never archived).

## Background

PITR to target `T` requires a base manifest `B` with `B.checkpoint <= T` whose
recovery WAL `[B.redo, B.checkpoint]` is archived, plus continuous archived WAL
from `B.redo` through `T`. The current `recovery_window` ignores WAL coverage:
`earliest` = oldest base, `latest` = newest segment checkpoint. Observed failure:
the archive held a single 256 KiB WAL chunk while six base manifests sat outside
it, yet `list` advertised a wide window and `recover` proceeded into a PG PANIC.

WAL archive layout (from `core/src/io/locator.rs`): per timeline under
`{ns}/wal/{tl:08X}/`, a sealed segment is `{segname}` (full `XLOG_SEG_SIZE` =
16 MiB), and partial segments are `{segname}.chunks/{offset:016X}`. Listing
returns keys only (no sizes), so the end LSN of a chunks-only segment needs one
`GET` of its highest-offset chunk for the byte length. `Manifest::redo_ckpt()`,
`checkpoint()`, `timestamp()` are public.

## Design decisions (resolved during brainstorming)

1. **Window meaning: base-anchored recoverable range.** `earliest` = the oldest
   base whose `[redo, checkpoint]` lies inside the contiguous archived-WAL run;
   `latest` (LSN) = the end of that run. Not the raw WAL byte span.
2. **Shared window: redefine `Store::recovery_window`.** Both `list` and
   `recover` use it, so they agree and `recover` stops over-promising.
3. **Scope: the latest timeline only** (the newest checkpoint's timeline,
   matching `recover`'s default). Multi-timeline coverage is future work.

## `RecoveryWindow` struct (revised)

In `core/src/io/store.rs`:
```rust
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint, // oldest usable base's checkpoint
    pub latest_ts: i64,            // newest checkpoint time with lsn <= latest_lsn
    pub latest_lsn: Lsn,           // W_hi: end of the contiguous archived-WAL run
    pub timeline: TimelineId,
}
```
(`latest_ckpt: Checkpoint` is replaced by `latest_lsn: Lsn`, because the upper
bound is the WAL-run end, not necessarily a checkpoint.)

## Algorithm — `Store::recovery_window` (rewritten)

1. **Timeline.** `let rows = self.list_checkpoints()?;` newest =
   `rows.last()`; none ⇒ `Error::other("no checkpoints found; nothing is
   recoverable yet")`. `timeline = newest.ckpt.timeline_id`.

2. **Contiguous archived-WAL run for `timeline`.**
   - List `self.lctr.wal_timeline_dir(timeline)` (new locator helper →
     `{ns}/wal/{tl:08X}/`). Empty/not-found ⇒ "nothing recoverable".
   - Classify each key:
     - sealed segment (24-hex name, no `.chunks`) → `seg_no` (hex chars 8..24);
       coverage = full `[seg_no*SEG, (seg_no+1)*SEG)`.
     - chunk (`{segname}.chunks/{offset:016X}`) → `(seg_no, offset)`; track per
       `seg_no` the min and max chunk offset.
   - If a `seg_no` has **both** a sealed object and leftover chunks (best-effort
     compaction may strand chunks), classify it as **Sealed** (full) — the
     sealed object is authoritative, matching `tiko_restore`'s preference.
   - Determine the highest `seg_no` present and its covered end `W_hi`:
     - sealed → `(seg_no+1)*SEG`.
     - chunks-only → GET the highest-offset chunk of that segment for its
       decompressed length `len`; `W_hi = seg_no*SEG + max_off + len`.
   - Build per-segment entries `{ seg_no, lo, hi, full }` (absolute LSN):
     - sealed: `lo = seg_no*SEG`, `hi = (seg_no+1)*SEG`, `full = true`.
     - top chunks-only segment: `lo = seg_no*SEG + min_off`, `hi = W_hi`
       (computed above), `full = false`.
     - any lower chunks-only segment: `lo = seg_no*SEG + min_off`, `hi`
       unused (set to `lo`), `full = false` — the walk never includes a
       non-`full` lower segment, so its `hi` is irrelevant.
     Call the pure helper `wal_contiguous_run` (below) → `(W_lo, W_hi)`.

3. **Oldest usable base.** List `self.lctr.bases_dir()` (filter to `timeline`
   via `parse_base_manifest_ckpt`), keep checkpoints with `ckpt.lsn ∈ [W_lo,
   W_hi]`, sort ascending, and `storage_get` + `Manifest::from_bytes` each in
   ascending order; return the first whose `redo_ckpt().lsn >= W_lo`
   (`is_base_usable`, below). That base → `earliest_ts = base.timestamp()`,
   `earliest_ckpt = base.checkpoint()`. None ⇒ "nothing recoverable yet".

4. **Latest.** `latest_lsn = W_hi`; `latest_ts` = the `created_at` of the newest
   `CheckpointRow` with `ckpt.lsn <= W_hi` (fallback to `earliest_ts` if none).

## Decomposition

### Pure helpers (unit-tested, in `core/src/io/store.rs`)

- **`wal_contiguous_run(entries: &[SegEntry]) -> Option<(u64, u64)>`** where
  `struct SegEntry { seg_no: u64, lo: u64, hi: u64, full: bool }`:
  - sort by `seg_no` descending; `top = entries[0]`; `(w_lo, w_hi) = (top.lo,
    top.hi)`.
  - walk down: while the current segment starts at its boundary
    (`cur.lo == cur.seg_no * SEG`), the next-lower `seg_no` exists, is `full`,
    and is contiguous (`next.hi == cur.lo`): set `w_lo = next.lo`, `cur = next`;
    else stop.
  - return `(w_lo, w_hi)`. `None` if `entries` empty.
  - `SEG` = `XLOG_SEG_SIZE as u64`.

- **`is_base_usable(ckpt_lsn: u64, redo_lsn: u64, w_lo: u64, w_hi: u64) -> bool`**
  = `redo_lsn >= w_lo && ckpt_lsn <= w_hi`. (Oldest-ness comes from the ascending
  walk in `recovery_window`.)

### Locator helper (in `core/src/io/locator.rs`)
- **`wal_timeline_dir(&self, timeline_id: TimelineId) -> String`** →
  `{ns}/wal/{tl:08X}/`, `pub(crate)` (callable from `store.rs`).

### Store I/O glue
`recovery_window` does the listing, the single chunk `GET` for `W_hi`, and the
ascending base `GET`s, calling the pure helpers. A small private parser maps a
WAL key to `(seg_no, Option<chunk_offset>)` (reuse the 24-hex / `.chunks/`
conventions; mirror `tiko_restore`'s parsing).

## CLI changes (`cli/src/bin/tiko_pitr.rs`)

- **`run_list`**: print the window as both time and LSN ranges:
  ```
  recoverable window (timeline 00000001):
    earliest: <RFC3339>   lsn <X/Y>
    latest:   <RFC3339>   lsn <X/Y>
  ```
  using `earliest_ts`/`earliest_ckpt.lsn` and `latest_ts`/`latest_lsn`.
- **`run_recover`**: the two bounds checks change `window.latest_ckpt.lsn` →
  `window.latest_lsn`; `window.earliest_ckpt.lsn` stays. (The `--time` upper
  bound stays `window.latest_ts`.) Base selection is unchanged: any base newer
  than `earliest_ckpt` has `redo >= earliest base's redo >= W_lo`, so a target
  within the window selects a usable base.

## Error handling / "nothing recoverable"

`recovery_window` returns a clear `Error::other("...nothing is recoverable
yet")` when there are no checkpoints, no archived WAL, or no base whose recovery
WAL is within the run. `list` and `recover` surface it as
`tiko_pitr: <message>` and exit 1 — no bogus window, no PG PANIC.

## Testing

- **Unit tests** (in `core/src/io/store.rs`, the existing test module style):
  - `wal_contiguous_run`: sealed-only chain (segments 0,1,2 → full span);
    sealed below + partial top (top chunks reaching `max_off+len`); a
    mid-segment-start top (single chunks-only segment with `min_off > 0` →
    run = `[seg*SEG+min_off, seg*SEG+max_off+len)`, no extension below); a gap
    (missing lower segment stops the walk); empty → `None`.
  - `is_base_usable`: redo before `W_lo` → false; checkpoint after `W_hi` →
    false; both inside → true.
- **Integration** (out of band, per project convention): the full
  `recovery_window` (listing + GETs) and the end-to-end `list`/`recover` against
  a live archive are verified with the PITR test — including the current
  test-data case, where the new window correctly reports "nothing recoverable
  yet" (the lone 256 KiB chunk contains no base).

## Out of scope

- Multi-timeline coverage (history spanning timelines from prior promotions).
- Range-GET of just the TIKM/chunk header to avoid transferring `pg_state` /
  full chunks when reading `redo`/length — a future efficiency optimization.
- Detecting/representing multiple disjoint archived runs; v1 reports the single
  contiguous run that reaches the head.
- Changes to the WAL archiver, checkpoint path, or `recover`'s lifecycle beyond
  the two bounds-check expressions.
