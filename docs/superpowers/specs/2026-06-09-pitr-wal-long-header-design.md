# Synthesize the WAL long-header page in tiko_restore

**Date:** 2026-06-09
**Status:** Approved (design)

## Goal

Let PostgreSQL recover from a tiko base whose WAL segment was streamed
mid-segment (so its first page — the "long header" — was never archived) by
having `tiko_restore` synthesize a valid long-header page when it assembles
such a segment.

## Problem (root cause, evidence-backed)

After the base-backup shaping landed, PG accepts the base and begins backup
recovery, but FATALs:

```
restored log file "000000010000000000000002" from archive
invalid magic number 0000 in WAL segment 000000010000000000000002, LSN 0/2000000, offset 0
invalid checkpoint record
could not locate required checkpoint record at 0/208BD30
```

Mechanism (PG18 source): on first access to a segment, `ReadPageInternal`
([xlogreader.c:1042-1060](../../../postgres/src/backend/access/transam/xlogreader.c))
**also reads that segment's page 0 to validate the long header** (magic,
`xlp_sysid`, seg size, blcksz) — even when the target record is on a later page.
tiko's WAL stream began mid-segment-2 (slot created at byte offset `0x1F820`),
so segment 2's page 0 was never archived; `tiko_restore` returns it zero-filled;
PG reads page 0, sees magic `0000`, and can't open the segment to read the
checkpoint. Verified by extracting the archive: segment 2 chunks cover
`[0x1F820, 0x8C428)` — the checkpoint page `0x8A000` is **valid** (magic
`0xD118`, `xlp_pageaddr=0x0208A000`), only page 0 is missing.

Crucially, PG does **not** need the WAL *records* before the base redo (replay
starts at the redo, which the recoverable window already guarantees is within
archived coverage). It only needs the segment's long-header **descriptor** page
to be valid. tiko knows every field that descriptor requires.

## Design decisions (resolved during brainstorming)

1. **Synthesize the long-header page in `tiko_restore`** (restore-time; fixes
   already-archived partial segments). Sealed segments and segments whose page 0
   is archived are untouched.
2. **Source `system_identifier` from `global/pg_control`** (cwd = PGDATA during
   `restore_command`); on failure, warn and skip synthesis (no worse than today).
3. No change to the recoverable window — `is_base_usable` (`redo ≥ w_lo`) is
   already correct; this only makes PG able to *open* the segment.

## PG18 facts (confirmed via `offsetof`/headers in this build)

- `XLOG_PAGE_MAGIC = 0xD118`, `XLP_LONG_HEADER = 0x0002`, `SizeOfXLogLongPHD = 40`.
- `XLogLongPageHeaderData` layout: `xlp_magic@0` (u16), `xlp_info@2` (u16),
  `xlp_tli@4` (u32), `xlp_pageaddr@8` (u64), `xlp_rem_len@16` (u32), pad@20,
  `xlp_sysid@24` (u64), `xlp_seg_size@32` (u32), `xlp_xlog_blcksz@36` (u32).
- `XLogReaderValidatePageHeader` long-header checks: `xlp_sysid == state->system_identifier`
  (when nonzero — it is, during recovery), `xlp_seg_size == wal_segment_size`,
  `xlp_xlog_blcksz == XLOG_BLCKSZ`, plus the generic checks (magic, `xlp_pageaddr`
  == the requested page LSN = segment start).
- `system_identifier@0` (u64) in `ControlFileData`.
- `XLOG_BLCKSZ` default = 8192; tiko assumes `XLOG_SEG_SIZE = 16 MiB` throughout.

## New `core/src/pgcontrol.rs` helpers (pure, unit-tested)

```rust
const XLOG_PAGE_MAGIC: u16 = 0xD118;
const XLP_LONG_HEADER: u16 = 0x0002;
const XLOG_BLCKSZ: u32 = 8192;
const SIZE_OF_XLOG_LONG_PHD: usize = 40;
```

- **`wal_long_header(tli: TimelineId, seg_no: u64, system_identifier: u64) -> [u8; 40]`**
  — build the descriptor: magic, `XLP_LONG_HEADER`, `tli`,
  `xlp_pageaddr = seg_no * XLOG_SEG_SIZE`, `rem_len = 0`, `sysid`,
  `seg_size = XLOG_SEG_SIZE`, `blcksz = XLOG_BLCKSZ`. All little-endian (same
  platform assumption as the existing pg_control code).
- **`parse_wal_seg_no(name: &str) -> Option<u64>`** — inverse of `xlog_file_name`:
  for a 24-hex name, `logid = hex[8..16]`, `logseg = hex[16..24]`,
  `seg_no = logid * (2^32 / XLOG_SEG_SIZE) + logseg` (= `logid*256 + logseg`).
  `None` for malformed names.
- **`read_system_identifier(ctl: &[u8]) -> Result<u64>`** — version-guarded
  (reuse `check_version`), read `u64` LE at offset 0.

## `tiko_restore` wiring (`cli/src/bin/tiko_restore.rs`)

In the chunk-assembly path, after the chunk-copy loop fills `buf` and before
`write_atomic`:

1. If `buf[0..2] == 0xD118` (LE) a real long header is present (offset 0 was
   covered) → do nothing.
2. Otherwise synthesize:
   - `seg_no = pgcontrol::parse_wal_seg_no(name)` (skip if `None`).
   - `sysid`: read `global/pg_control` (cwd-relative); `pgcontrol::read_system_identifier`.
     On any error, `eprintln!` a warning (goes to the PG log) and skip synthesis.
   - `buf[0..40].copy_from_slice(&pgcontrol::wal_long_header(timeline_id, seg_no, sysid))`.
3. `write_atomic(dest, &buf)` as today.

The sealed-segment path is unchanged (real page 0). Only the chunks-only path —
and within it only segments missing page 0 — synthesize.

## Error handling

- Missing/unreadable/bad-version `global/pg_control`, or unparseable segment
  name → warn to stderr, skip synthesis, return the assembled (page-0-zero)
  segment. PG then fails exactly as it does today, but the warning explains why.
- No panics: fixed-size slice writes into the 16 MiB `buf`; `parse_wal_seg_no`
  returns `Option`.

## Testing

- **Unit (core):**
  - `wal_long_header` byte-exact: for `(tli=1, seg_no=2, sysid=0x0123456789ABCDEF)`
    assert magic `0xD118`@0, `0x0002`@2, tli@4, `pageaddr = 2*XLOG_SEG_SIZE`@8,
    sysid@24, `seg_size = XLOG_SEG_SIZE`@32, `blcksz = 8192`@36.
  - `parse_wal_seg_no`: `"000000010000000000000002"` → `2`,
    `"000000010000000100000000"` → `256`; malformed → `None`. (Round-trips with
    `xlog_file_name`.)
  - `read_system_identifier`: reads a planted value from a synthetic
    version-1800 control buffer; rejects a wrong-version / short buffer.
- **Integration (out of band):** the live `tiko_pitr recover` re-run — PG opens
  the partial base segment (synthesized long header passes validation), reads
  the checkpoint, reaches consistency at the base checkpoint, replays to the
  target, and shuts down. (`tiko_restore`'s assembly is integration-verified, per
  project convention.)

## Out of scope

- Backfilling the real pre-stream WAL prefix from the primary's `pg_wal` (not
  needed — replay never reads pages before the base redo).
- Timeline `.history` / promotion (separate, still deferred).
- The tiko-vs-PG segment-name divergence for `seg_no ≥ 256` (separate latent
  issue).
