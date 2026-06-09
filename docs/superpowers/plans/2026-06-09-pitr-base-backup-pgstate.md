# Base-Backup-Shaped pg_state Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `tiko_pitr recover` shape the restored PGDATA (write a `backup_label` + patch `pg_control`) so PostgreSQL treats tiko's checkpoint snapshot as a base backup whose consistency point is the base checkpoint — enabling PITR to any target at/after that checkpoint.

**Architecture:** A new pure, unit-tested `core/src/pgcontrol.rs` reads checkpoint LSNs from a `pg_control` buffer, patches it (`state=DB_IN_ARCHIVE_RECOVERY`, `minRecoveryPoint=checkpoint`, recompute CRC-32C), and builds a `backup_label` (standby-style, no `XLOG_BACKUP_END` needed). `tiko_pitr recover` calls these at restore time, after extracting `pg_state` and before writing the recovery conf.

**Tech Stack:** Rust (edition 2024); `pgsys` (`Lsn`, `TimelineId`, `XLOG_SEG_SIZE`); `chrono`.

**Reference spec:** `docs/superpowers/specs/2026-06-09-pitr-base-backup-pgstate-design.md`

**Conventions (project memory / CLAUDE.md):**
- Build: `cargo build -p core -p cli`. Tests: `cargo test -p core`.
- `cargo clippy` is blocked by pre-existing `pgsys` lint errors (unrelated); verify lint-cleanliness via a warning-free build.
- Commit after each task. Branch `pitr2`. Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

**Verified facts:**
- PG18 `ControlFileData` (confirmed via `offsetof` against the build's headers, `PG_CONTROL_VERSION=1800`, `sizeof=296`): `pg_control_version@8` (u32), `state@16` (u32), `checkPoint@32` (u64), `checkPointCopy.redo@40` (u64), `checkPointCopy.ThisTimeLineID@48` (u32), `minRecoveryPoint@136` (u64), `minRecoveryPointTLI@144` (u32), `crc@292` (u32, last field). CRC-32C is computed over bytes `[0, 292)`. `DB_IN_ARCHIVE_RECOVERY = 5`.
- `XLOG_SEG_SIZE = 16 MiB` (`pgsys::common::XLOG_SEG_SIZE: usize`). `Lsn`: `new(u64)`, `as_u64()`, `to_pg_string()` (→ `X/Y`). `TimelineId`: `new(u32)`, `as_u32()`. `chrono` and `crate::error::{Error, Result}` available in `core`.
- `tiko_pitr` `recover_inner` (in `cli/src/bin/tiko_pitr.rs`) currently does: `extract_pg_state(...)?; pitr::write_pitr_recovery_conf(...)?; std::fs::write(pgdata.join("recovery.signal"), b"")?; <run postgres>`. The binary imports `chrono::{DateTime, Utc}`, `core::pitr`, `std::path::Path`, `core::error::{Error, Result}`.

---

### Task 1: `pgcontrol.rs` — `backup_label` + `xlog_file_name` (TDD)

**Files:** Create `core/src/pgcontrol.rs`; modify `core/src/lib.rs`.

- [ ] **Step 1: Wire the module**

In `core/src/lib.rs`, add alongside the other `pub mod` lines (e.g. after `pub mod pitr;`):
```rust
pub mod pgcontrol;
```

- [ ] **Step 2: Create `core/src/pgcontrol.rs` with the pure builders + failing tests**

Create `core/src/pgcontrol.rs`:
```rust
//! Read/patch a PostgreSQL `pg_control` and synthesize a `backup_label`, so a
//! tiko checkpoint snapshot can be recovered as a base backup (consistency at
//! the base checkpoint). PG18 (`PG_CONTROL_VERSION` 1800) layout; all patching
//! is guarded at runtime by the version field so an unknown layout is never
//! modified.

use chrono::{DateTime, Utc};
use pgsys::common::XLOG_SEG_SIZE;
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

/// WAL segments per logical xlog id: 2^32 / XLOG_SEG_SIZE (= 256 for 16 MiB).
const SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;

/// PostgreSQL WAL segment file name: `{tli:08X}{logid:08X}{logseg:08X}`, where
/// `logid = seg_no / SEGS_PER_LOGID`, `logseg = seg_no % SEGS_PER_LOGID`.
pub fn xlog_file_name(tli: TimelineId, seg_no: u64) -> String {
    format!(
        "{:08X}{:08X}{:08X}",
        tli.as_u32(),
        seg_no / SEGS_PER_LOGID,
        seg_no % SEGS_PER_LOGID
    )
}

/// Build a `backup_label` presenting a tiko checkpoint snapshot as a base
/// backup. Uses the standby end-of-backup path (`BACKUP FROM: standby`), so
/// recovery reaches consistency at `pg_control.minRecoveryPoint` (set to the
/// base checkpoint by [`shape_for_backup_recovery`]) with no `XLOG_BACKUP_END`
/// record. Mirrors PostgreSQL's `build_backup_content` line format.
pub fn backup_label(
    redo: Lsn,
    checkpoint: Lsn,
    tli: TimelineId,
    start_time: DateTime<Utc>,
) -> String {
    let seg = xlog_file_name(tli, redo.as_u64() / XLOG_SEG_SIZE as u64);
    format!(
        "START WAL LOCATION: {redo} (file {seg})\n\
         CHECKPOINT LOCATION: {ckpt}\n\
         BACKUP METHOD: streamed\n\
         BACKUP FROM: standby\n\
         START TIME: {time}\n\
         LABEL: tiko_pitr\n\
         START TIMELINE: {tl}\n",
        redo = redo.to_pg_string(),
        seg = seg,
        ckpt = checkpoint.to_pg_string(),
        time = start_time.format("%Y-%m-%d %H:%M:%S UTC"),
        tl = tli.as_u32(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlog_file_name_format() {
        let tl = TimelineId::new(1);
        assert_eq!(xlog_file_name(tl, 2), "000000010000000000000002");
        assert_eq!(xlog_file_name(tl, 256), "000000010000000100000000");
    }

    #[test]
    fn backup_label_lines() {
        let t = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let s = backup_label(
            Lsn::new(0x2038660),
            Lsn::new(0x20386B8),
            TimelineId::new(1),
            t,
        );
        assert!(s.contains("START WAL LOCATION: 0/2038660 (file 000000010000000000000002)"));
        assert!(s.contains("CHECKPOINT LOCATION: 0/20386B8"));
        assert!(s.contains("BACKUP METHOD: streamed"));
        assert!(s.contains("BACKUP FROM: standby"));
        assert!(s.contains("START TIMELINE: 1"));
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

(These are pure builders landed with their tests; the meaningful RED→GREEN TDD
cycle is in Task 2, where the tests reference not-yet-defined functions.)
Run: `cargo test -p core pgcontrol::tests`
Expected: PASS (2 passed: `xlog_file_name_format`, `backup_label_lines`).

- [ ] **Step 4: Build (clean)**

Run: `cargo build -p core`
Expected: clean (the two `pub fn`s are public API of a `pub mod`, so no dead-code warnings even before a non-test caller exists).

- [ ] **Step 5: Commit**

```bash
git add core/src/pgcontrol.rs core/src/lib.rs
git commit -m "feat(core): add pgcontrol backup_label + xlog_file_name builders"
```

---

### Task 2: `pgcontrol.rs` — CRC-32C + read/patch `pg_control` (TDD)

**Files:** Modify `core/src/pgcontrol.rs`.

- [ ] **Step 1: Add the offset constants + imports**

In `core/src/pgcontrol.rs`, add to the imports at the top:
```rust
use crate::error::{Error, Result};
```
And add these module-scope constants (after `SEGS_PER_LOGID`):
```rust
// PG18 ControlFileData layout (PG_CONTROL_VERSION 1800), confirmed via offsetof
// against the build's headers. `crc` is the last field; CRC covers [0, OFF_CRC).
const PG_CONTROL_VERSION_PG18: u32 = 1800;
const OFF_VERSION: usize = 8;
const OFF_STATE: usize = 16;
const OFF_CHECKPOINT: usize = 32;
const OFF_REDO: usize = 40;
const OFF_THIS_TLI: usize = 48;
const OFF_MIN_RECOVERY: usize = 136;
const OFF_MIN_RECOVERY_TLI: usize = 144;
const OFF_CRC: usize = 292;
/// `DBState::DB_IN_ARCHIVE_RECOVERY`.
const DB_IN_ARCHIVE_RECOVERY: u32 = 5;
```

- [ ] **Step 2: Add failing tests**

Add inside the existing `#[cfg(test)] mod tests` block in `core/src/pgcontrol.rs`:
```rust
    #[test]
    fn crc32c_check_value() {
        // Standard CRC-32C (Castagnoli) check value for "123456789".
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    fn synthetic_control() -> Vec<u8> {
        let mut c = vec![0u8; 8192];
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&PG_CONTROL_VERSION_PG18.to_le_bytes());
        c[OFF_CHECKPOINT..OFF_CHECKPOINT + 8].copy_from_slice(&0x20386B8u64.to_le_bytes());
        c[OFF_REDO..OFF_REDO + 8].copy_from_slice(&0x2038660u64.to_le_bytes());
        c[OFF_THIS_TLI..OFF_THIS_TLI + 4].copy_from_slice(&1u32.to_le_bytes());
        c
    }

    #[test]
    fn read_checkpoint_lsns_reads_fields() {
        let c = synthetic_control();
        let (ckpt, redo, tli) = read_checkpoint_lsns(&c).unwrap();
        assert_eq!(ckpt.as_u64(), 0x20386B8);
        assert_eq!(redo.as_u64(), 0x2038660);
        assert_eq!(tli.as_u32(), 1);
    }

    #[test]
    fn read_checkpoint_lsns_rejects_bad_version() {
        let mut c = synthetic_control();
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&1700u32.to_le_bytes());
        assert!(read_checkpoint_lsns(&c).is_err());
        assert!(read_checkpoint_lsns(&[0u8; 8]).is_err()); // too short
    }

    #[test]
    fn shape_sets_fields_and_self_consistent_crc() {
        let mut c = synthetic_control();
        shape_for_backup_recovery(&mut c, Lsn::new(0x20386B8), TimelineId::new(1)).unwrap();
        assert_eq!(
            u32::from_le_bytes(c[OFF_STATE..OFF_STATE + 4].try_into().unwrap()),
            DB_IN_ARCHIVE_RECOVERY
        );
        assert_eq!(
            u64::from_le_bytes(c[OFF_MIN_RECOVERY..OFF_MIN_RECOVERY + 8].try_into().unwrap()),
            0x20386B8
        );
        assert_eq!(
            u32::from_le_bytes(c[OFF_MIN_RECOVERY_TLI..OFF_MIN_RECOVERY_TLI + 4].try_into().unwrap()),
            1
        );
        // Stored CRC matches a fresh CRC over [0, OFF_CRC).
        let stored = u32::from_le_bytes(c[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
        assert_eq!(stored, crc32c(&c[..OFF_CRC]));
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p core pgcontrol::`
Expected: FAIL — `cannot find function 'crc32c' / 'read_checkpoint_lsns' / 'shape_for_backup_recovery'`.

- [ ] **Step 4: Implement CRC + read/patch**

Add these functions at module scope in `core/src/pgcontrol.rs` (before the `#[cfg(test)]` block):
```rust
/// CRC-32C (Castagnoli), matching PostgreSQL's `pg_crc32c`: reflected,
/// polynomial `0x82F63B78`, init/xorout `0xFFFFFFFF`.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Validate that `ctl` is a PG18 control file we know the layout of.
fn check_version(ctl: &[u8]) -> Result<()> {
    if ctl.len() < OFF_CRC + 4 {
        return Err(Error::other(format!(
            "pg_control too short: {} bytes",
            ctl.len()
        )));
    }
    let v = u32::from_le_bytes(ctl[OFF_VERSION..OFF_VERSION + 4].try_into().unwrap());
    if v != PG_CONTROL_VERSION_PG18 {
        return Err(Error::other(format!(
            "unsupported pg_control_version {v} (expected {PG_CONTROL_VERSION_PG18})"
        )));
    }
    Ok(())
}

/// Read `(checkpoint, redo, timeline)` from a `pg_control` buffer.
pub fn read_checkpoint_lsns(ctl: &[u8]) -> Result<(Lsn, Lsn, TimelineId)> {
    check_version(ctl)?;
    let checkpoint = Lsn::new(u64::from_le_bytes(
        ctl[OFF_CHECKPOINT..OFF_CHECKPOINT + 8].try_into().unwrap(),
    ));
    let redo = Lsn::new(u64::from_le_bytes(
        ctl[OFF_REDO..OFF_REDO + 8].try_into().unwrap(),
    ));
    let tli = TimelineId::new(u32::from_le_bytes(
        ctl[OFF_THIS_TLI..OFF_THIS_TLI + 4].try_into().unwrap(),
    ));
    Ok((checkpoint, redo, tli))
}

/// Patch a `pg_control` buffer in place so PostgreSQL treats the snapshot as a
/// base backup whose consistency point is `min_recovery`: set
/// `state = DB_IN_ARCHIVE_RECOVERY`, `minRecoveryPoint`/`minRecoveryPointTLI`,
/// then recompute the trailing CRC-32C over `[0, OFF_CRC)`.
pub fn shape_for_backup_recovery(
    ctl: &mut [u8],
    min_recovery: Lsn,
    min_recovery_tli: TimelineId,
) -> Result<()> {
    check_version(ctl)?;
    ctl[OFF_STATE..OFF_STATE + 4].copy_from_slice(&DB_IN_ARCHIVE_RECOVERY.to_le_bytes());
    ctl[OFF_MIN_RECOVERY..OFF_MIN_RECOVERY + 8]
        .copy_from_slice(&min_recovery.as_u64().to_le_bytes());
    ctl[OFF_MIN_RECOVERY_TLI..OFF_MIN_RECOVERY_TLI + 4]
        .copy_from_slice(&min_recovery_tli.as_u32().to_le_bytes());
    let crc = crc32c(&ctl[..OFF_CRC]);
    ctl[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p core pgcontrol::`
Expected: PASS (all 6: the 2 from Task 1 + `crc32c_check_value`, `read_checkpoint_lsns_reads_fields`, `read_checkpoint_lsns_rejects_bad_version`, `shape_sets_fields_and_self_consistent_crc`).

- [ ] **Step 6: Build (clean)**

Run: `cargo build -p core`
Expected: clean, no warnings (`crc32c` is used by `shape_for_backup_recovery`; the rest are public API).

- [ ] **Step 7: Commit**

```bash
git add core/src/pgcontrol.rs
git commit -m "feat(core): add pg_control read + base-backup patch (CRC-32C)"
```

---

### Task 3: Wire base-backup shaping into `tiko_pitr recover`

**Files:** Modify `cli/src/bin/tiko_pitr.rs`.

- [ ] **Step 1: Add the import**

In `cli/src/bin/tiko_pitr.rs`, add alongside the existing `use core::pitr;`:
```rust
use core::pgcontrol;
```
(`chrono::Utc` is already imported.)

- [ ] **Step 2: Add the `shape_base_backup` helper**

Add this function near the other helpers (e.g. just after `extract_pg_state`):
```rust
/// Shape the extracted PGDATA so PostgreSQL recovers it as a base backup whose
/// consistency point is the base checkpoint: write a `backup_label` and patch
/// `global/pg_control` (state + minRecoveryPoint + CRC). The checkpoint/redo/
/// timeline are read from the just-extracted `pg_control`.
fn shape_base_backup(pgdata: &Path) -> Result<()> {
    let ctl_path = pgdata.join("global").join("pg_control");
    let mut ctl = std::fs::read(&ctl_path)
        .map_err(|e| Error::other(format!("read {}: {e}", ctl_path.display())))?;

    let (checkpoint, redo, tli) = pgcontrol::read_checkpoint_lsns(&ctl)?;

    let label = pgcontrol::backup_label(redo, checkpoint, tli, Utc::now());
    std::fs::write(pgdata.join("backup_label"), label)?;

    // Consistency point = the base checkpoint (tiko's snapshot is atomic there).
    pgcontrol::shape_for_backup_recovery(&mut ctl, checkpoint, tli)?;
    std::fs::write(&ctl_path, &ctl)
        .map_err(|e| Error::other(format!("write {}: {e}", ctl_path.display())))?;
    Ok(())
}
```

- [ ] **Step 3: Call it in `recover_inner`**

In `recover_inner`, insert the call between `extract_pg_state` and the conf write. Change:
```rust
    extract_pg_state(pg_state, pgdata)?;
    pitr::write_pitr_recovery_conf(conf, timeline, target, tiko_restore)?;
```
to:
```rust
    extract_pg_state(pg_state, pgdata)?;
    // Make the extracted snapshot look like a base backup so PG reaches
    // consistency at the base checkpoint and can stop at an earlier target.
    shape_base_backup(pgdata)?;
    pitr::write_pitr_recovery_conf(conf, timeline, target, tiko_restore)?;
```

- [ ] **Step 4: Build + smoke**

Run: `cargo build -p core -p cli`
Expected: clean build, no warnings.
Run: `cargo run -p cli --bin tiko_pitr -- --help`
Expected: shows `list` and `recover` (no behavior change to the CLI surface).

- [ ] **Step 5: Commit**

```bash
git add cli/src/bin/tiko_pitr.rs
git commit -m "feat(cli): shape PGDATA as a base backup before PITR recovery"
```

---

### Task 4: Full build + test gate

**Files:** none (verification only)

- [ ] **Step 1: Build**

Run: `cargo build -p core -p cli`
Expected: clean.

- [ ] **Step 2: Warning count**

Run: `cargo build -p core -p cli 2>&1 | grep -c warning`
Expected: `0`.

- [ ] **Step 3: Tests**

Run: `cargo test -p core`
Expected: all pass, including `pgcontrol::tests::*`.

---

## Notes for the implementer

- **Integration (out of band, the real proof):** run `tiko_pitr recover --time <T>` (T inside the window) against the live instance. Expected in the PG log: it reads the checkpoint, replays from the base redo, reaches the end-of-backup at the base checkpoint, declares consistency, then stops at the target and shuts down — no more `requested recovery stop point is before consistent recovery point`. The PGDATA backup/restore-on-failure path already reverts `pg_control` and removes the new `backup_label`; on success PostgreSQL renames `backup_label` → `backup_label.old` itself.
- **Why `minRecoveryPoint = checkpoint` (not redo):** tiko's snapshot is atomic at the checkpoint flush, so the data is consistent at the checkpoint LSN. PG replays redo→checkpoint idempotently and reaches consistency at the checkpoint.
- **Version guard:** `shape_for_backup_recovery`/`read_checkpoint_lsns` refuse any `pg_control` whose `pg_control_version != 1800`, so a PG-major upgrade fails loudly instead of corrupting the control file. If PG is upgraded, re-confirm the offsets via `offsetof` and bump the constant.
- **Out of scope:** timeline `.history`/promotion; the tiko-vs-PG WAL segment-name divergence for `seg_no ≥ 256`.
