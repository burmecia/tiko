# WAL Long-Header Synthesis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `tiko_restore` synthesize a valid WAL long-header page at offset 0 when it assembles a segment whose archived coverage started mid-segment, so PostgreSQL can open it during PITR.

**Architecture:** Add three pure helpers to `core/src/pgcontrol.rs` (build the long-header bytes, parse a segment name to a segment number, read `system_identifier` from a `pg_control` buffer), then wire them into `tiko_restore`'s chunk-assembly path: if the assembled first page has no real magic, read `system_identifier` from `global/pg_control` (cwd = PGDATA during `restore_command`) and write a synthesized long header into `buf[0..40]`. Best-effort: any missing input warns and leaves the buffer unchanged.

**Tech Stack:** Rust (edition 2024); `core` crate (`pgcontrol`), `cli` crate binary `tiko_restore`; `pgsys` (`TimelineId`, `XLOG_SEG_SIZE`).

**Spec:** `docs/superpowers/specs/2026-06-09-pitr-wal-long-header-design.md`

---

## File Structure

- `core/src/pgcontrol.rs` — **modify.** Add `wal_long_header`, `parse_wal_seg_no`, `read_system_identifier` plus their constants and unit tests, alongside the existing pg_control helpers. `SEGS_PER_LOGID`, `check_version`, `XLOG_SEG_SIZE`, `TimelineId`, `Result`, `Error` are already in scope here.
- `cli/src/bin/tiko_restore.rs` — **modify.** Add a `maybe_synthesize_long_header` helper and call it in `restore()` right before `write_atomic` in the chunk-assembly path.

---

## Task 1: `core/src/pgcontrol.rs` synthesis helpers

**Files:**
- Modify: `core/src/pgcontrol.rs` (add constants + 3 functions near the existing helpers; add tests in the existing `#[cfg(test)] mod tests`)

Context: `core/src/pgcontrol.rs` already defines `const SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;`, `fn check_version(ctl: &[u8]) -> Result<()>` (returns `Err` if `ctl.len() < OFF_CRC + 4` or the version field ≠ 1800), `fn xlog_file_name(tli, seg_no)`, and imports `pgsys::common::XLOG_SEG_SIZE`, `pgsys::timeline_id::TimelineId`, `crate::error::{Error, Result}`. `TimelineId` has `as_u32()` and `new(u32)`. `system_identifier` is the first field of `ControlFileData` (offset 0, `u64`).

- [ ] **Step 1: Write the failing tests**

Add these three tests inside the existing `mod tests` block (after the existing tests, before its closing `}`):

```rust
    #[test]
    fn wal_long_header_bytes() {
        let h = wal_long_header(TimelineId::new(1), 2, 0x0123_4567_89AB_CDEF);
        assert_eq!(u16::from_le_bytes(h[0..2].try_into().unwrap()), XLOG_PAGE_MAGIC);
        assert_eq!(u16::from_le_bytes(h[2..4].try_into().unwrap()), XLP_LONG_HEADER);
        assert_eq!(u32::from_le_bytes(h[4..8].try_into().unwrap()), 1); // tli
        assert_eq!(
            u64::from_le_bytes(h[8..16].try_into().unwrap()),
            2 * XLOG_SEG_SIZE as u64 // xlp_pageaddr = segment start
        );
        assert_eq!(u32::from_le_bytes(h[16..20].try_into().unwrap()), 0); // rem_len
        assert_eq!(u64::from_le_bytes(h[24..32].try_into().unwrap()), 0x0123_4567_89AB_CDEF); // sysid
        assert_eq!(u32::from_le_bytes(h[32..36].try_into().unwrap()), XLOG_SEG_SIZE as u32);
        assert_eq!(u32::from_le_bytes(h[36..40].try_into().unwrap()), XLOG_BLCKSZ);
    }

    #[test]
    fn parse_wal_seg_no_values() {
        assert_eq!(parse_wal_seg_no("000000010000000000000002"), Some(2));
        assert_eq!(parse_wal_seg_no("000000010000000100000000"), Some(256));
        // Round-trips with xlog_file_name.
        assert_eq!(parse_wal_seg_no(&xlog_file_name(TimelineId::new(1), 700)), Some(700));
        assert_eq!(parse_wal_seg_no("short"), None);
        assert_eq!(parse_wal_seg_no("00000001.history"), None);
    }

    #[test]
    fn read_system_identifier_reads_offset_zero() {
        let mut c = synthetic_control();
        c[0..8].copy_from_slice(&0xDEAD_BEEF_0000_0001u64.to_le_bytes());
        assert_eq!(read_system_identifier(&c).unwrap(), 0xDEAD_BEEF_0000_0001);
        // Rejects wrong version and too-short buffers (via check_version).
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&1700u32.to_le_bytes());
        assert!(read_system_identifier(&c).is_err());
        assert!(read_system_identifier(&[0u8; 8]).is_err());
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p core pgcontrol`
Expected: FAIL — compile errors `cannot find function/value` for `wal_long_header`, `parse_wal_seg_no`, `read_system_identifier`, `XLOG_PAGE_MAGIC`, `XLP_LONG_HEADER`, `XLOG_BLCKSZ`.

- [ ] **Step 3: Add the constants**

Add these constants next to the existing `const SEGS_PER_LOGID` / `const OFF_*` block in `core/src/pgcontrol.rs`:

```rust
/// WAL page magic for this PG major (`XLOG_PAGE_MAGIC`, PG18).
const XLOG_PAGE_MAGIC: u16 = 0xD118;
/// `XLP_LONG_HEADER` — set in `xlp_info` on the first page of each segment.
const XLP_LONG_HEADER: u16 = 0x0002;
/// `XLOG_BLCKSZ` — WAL block size (PostgreSQL default, what this build uses).
const XLOG_BLCKSZ: u32 = 8192;
/// `SizeOfXLogLongPHD` — bytes in `XLogLongPageHeaderData`.
const SIZE_OF_XLOG_LONG_PHD: usize = 40;
```

- [ ] **Step 4: Implement the three functions**

Add these to `core/src/pgcontrol.rs` (e.g. after `xlog_file_name`):

```rust
/// Build a WAL `XLogLongPageHeaderData` — the descriptor on page 0 of every
/// segment that PostgreSQL validates (`XLogReaderValidatePageHeader`) on first
/// access. Synthesized when a mid-stream-start segment never archived its
/// page 0. Field offsets match the PG18 C layout; values are little-endian
/// (same single-platform assumption as the rest of this module).
pub fn wal_long_header(
    tli: TimelineId,
    seg_no: u64,
    system_identifier: u64,
) -> [u8; SIZE_OF_XLOG_LONG_PHD] {
    let mut h = [0u8; SIZE_OF_XLOG_LONG_PHD];
    // XLogPageHeaderData (short header, first 24 bytes):
    h[0..2].copy_from_slice(&XLOG_PAGE_MAGIC.to_le_bytes()); // xlp_magic
    h[2..4].copy_from_slice(&XLP_LONG_HEADER.to_le_bytes()); // xlp_info
    h[4..8].copy_from_slice(&tli.as_u32().to_le_bytes()); // xlp_tli
    let pageaddr = seg_no * XLOG_SEG_SIZE as u64; // segment start LSN
    h[8..16].copy_from_slice(&pageaddr.to_le_bytes()); // xlp_pageaddr
    // h[16..20] xlp_rem_len = 0; h[20..24] alignment padding = 0.
    // XLogLongPageHeaderData extra fields:
    h[24..32].copy_from_slice(&system_identifier.to_le_bytes()); // xlp_sysid
    h[32..36].copy_from_slice(&(XLOG_SEG_SIZE as u32).to_le_bytes()); // xlp_seg_size
    h[36..40].copy_from_slice(&XLOG_BLCKSZ.to_le_bytes()); // xlp_xlog_blcksz
    h
}

/// Inverse of [`xlog_file_name`]: parse a 24-hex WAL segment name into its
/// segment number (`logid * SEGS_PER_LOGID + logseg`). `None` for any name that
/// is not exactly 24 hex digits.
pub fn parse_wal_seg_no(name: &str) -> Option<u64> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let logid = u64::from_str_radix(&name[8..16], 16).ok()?;
    let logseg = u64::from_str_radix(&name[16..24], 16).ok()?;
    Some(logid * SEGS_PER_LOGID + logseg)
}

/// Read `system_identifier` (first field, offset 0) from a `pg_control` buffer.
/// Version-guarded so an unknown layout is rejected rather than misread.
pub fn read_system_identifier(ctl: &[u8]) -> Result<u64> {
    check_version(ctl)?;
    Ok(u64::from_le_bytes(ctl[0..8].try_into().unwrap()))
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p core pgcontrol`
Expected: PASS — all `pgcontrol` tests, including `wal_long_header_bytes`, `parse_wal_seg_no_values`, `read_system_identifier_reads_offset_zero`.

- [ ] **Step 6: Commit**

```bash
git add core/src/pgcontrol.rs
git commit -m "feat: add WAL long-header synthesis helpers to pgcontrol"
```

---

## Task 2: wire synthesis into `tiko_restore`

**Files:**
- Modify: `cli/src/bin/tiko_restore.rs` (imports; new `maybe_synthesize_long_header`; one call site in `restore()`)

Context: `restore()` assembles the segment into `let mut buf = vec![0u8; XLOG_SEG_SIZE];` from chunk objects, then calls `write_atomic(&args.dest_path, &buf)?;` and returns `Ok(Outcome::Restored)` (current file lines ~96–116). The sealed-segment path (returns earlier) is unchanged — sealed segments carry a real page 0. `timeline_id` and `name` are already bound locals in `restore()`. `fs` (`std::fs`) is already imported; `core::pgcontrol` is a public module of the `core` crate (already used by `tiko_pitr`).

- [ ] **Step 1: Add the `core::pgcontrol` import**

In the `use core::{...}` line, add `pgcontrol`:

```rust
use core::{error::Result, io::store::Store, pgcontrol};
```

- [ ] **Step 2: Add the synthesis helper**

Add this function to `cli/src/bin/tiko_restore.rs` (e.g. after `chunk_offset`):

```rust
/// Little-endian bytes of `XLOG_PAGE_MAGIC` — what a real WAL page 0 begins with.
const XLOG_PAGE_MAGIC_LE: [u8; 2] = 0xD118u16.to_le_bytes();

/// On first access to a segment, PostgreSQL reads page 0's long header to
/// validate the segment (`XLogReaderValidatePageHeader`) even when the target
/// record is on a later page. A segment streamed mid-segment never archived its
/// page 0, so the assembled buffer starts zero-filled and PG sees magic `0000`.
/// If page 0 is missing (no real magic), synthesize a valid long header in
/// place. Best-effort: on any missing input, warn (to the PG log) and leave the
/// buffer unchanged — no worse than before.
fn maybe_synthesize_long_header(buf: &mut [u8], tli: TimelineId, name: &str) {
    if buf[0..2] == XLOG_PAGE_MAGIC_LE {
        return; // offset 0 was archived — real long header already present
    }
    let Some(seg_no) = pgcontrol::parse_wal_seg_no(name) else {
        return; // not a parseable segment name; nothing to synthesize
    };
    // restore_command runs with cwd = the data directory, so global/pg_control
    // is the live control file holding this cluster's system_identifier.
    let sysid = match fs::read("global/pg_control") {
        Ok(ctl) => match pgcontrol::read_system_identifier(&ctl) {
            Ok(id) => id,
            Err(e) => {
                eprintln!(
                    "tiko_restore: bad global/pg_control ({e}); \
                     leaving segment {name} page 0 unsynthesized"
                );
                return;
            }
        },
        Err(e) => {
            eprintln!(
                "tiko_restore: cannot read global/pg_control ({e}); \
                 leaving segment {name} page 0 unsynthesized"
            );
            return;
        }
    };
    let hdr = pgcontrol::wal_long_header(tli, seg_no, sysid);
    buf[0..hdr.len()].copy_from_slice(&hdr);
}
```

- [ ] **Step 3: Call it in the chunk-assembly path**

In `restore()`, replace the final two lines of the chunk-assembly path:

```rust
    write_atomic(&args.dest_path, &buf)?;
    Ok(Outcome::Restored)
}
```

with:

```rust
    maybe_synthesize_long_header(&mut buf, timeline_id, name);

    write_atomic(&args.dest_path, &buf)?;
    Ok(Outcome::Restored)
}
```

(This is the chunk-assembly path's tail — the `}` closes `fn restore`. The sealed-segment branch above, which `return`s earlier, is untouched.)

- [ ] **Step 4: Build the cli crate**

Run: `cargo build -p cli`
Expected: builds cleanly — no unused-import or type errors. (`tiko_restore`'s assembly is integration-verified, per project convention; there are no unit tests for the binary.)

- [ ] **Step 5: Commit**

```bash
git add cli/src/bin/tiko_restore.rs
git commit -m "feat: synthesize WAL long-header for mid-stream segments in tiko_restore"
```

---

## Verification (whole feature)

- [ ] `cargo test -p core` — all core tests pass (includes the new `pgcontrol` tests).
- [ ] `cargo build -p cli` — `tiko_restore` builds.
- [ ] Integration (out of band, per project convention): re-run `tiko_pitr recover` against the failing base. Expected — PG restores `000000010000000000000002`, opens it (synthesized long header passes validation), locates the checkpoint, reaches consistency at the base checkpoint, replays to the target, and shuts down cleanly (exit 0). The previous `invalid magic number 0000 ... could not locate required checkpoint record` FATAL no longer appears.
