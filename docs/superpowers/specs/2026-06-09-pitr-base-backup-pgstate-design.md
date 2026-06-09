# Make tiko's pg_state a PostgreSQL-recoverable base backup

**Date:** 2026-06-09
**Status:** Approved (design)

## Goal

Make `tiko_pitr recover` shape the restored PGDATA so PostgreSQL treats tiko's
checkpoint snapshot as a **base backup** whose consistency point is the base
checkpoint — enabling PITR to any target at or after that checkpoint, instead of
only to end-of-WAL.

## Problem (root cause, evidence-backed)

tiko's base manifest embeds a **verbatim running-primary `pg_control`**
(`state = in production`, `minRecoveryPoint = 0/0`, no backup fields — confirmed
via `pg_controldata` on an extracted base). PostgreSQL therefore performs
**crash recovery**: `CheckRecoveryConsistency` early-returns while
`minRecoveryPoint` is invalid ([xlogrecovery.c:2195](../../../postgres/src/backend/access/transam/xlogrecovery.c)),
so `reachedConsistency` only becomes true after replaying **all** WAL. A
`recovery_target_time/lsn` earlier than end-of-WAL is then rejected:
`requested recovery stop point is before consistent recovery point`.

A real `pg_basebackup` avoids this with a `backup_label` (START WAL LOCATION =
checkpoint redo; CHECKPOINT LOCATION = checkpoint record) plus an end-of-backup
signal. tiko already has both LSNs (the base manifest / the extracted
`pg_control` carry `redo` and the checkpoint record LSN).

Key insight: tiko's snapshot is **atomic at checkpoint `C`** (the checkpoint
flush writes all dirty chunks and the manifest pins those versions), so its
consistency point is exactly `C` — unlike `pg_basebackup`, whose copy spans
start→stop. We therefore use the **standby-style** consistency path, which
reaches consistency at `minRecoveryPoint` with **no `XLOG_BACKUP_END` record**.

## PostgreSQL mechanism (from PG18 source)

- `backup_label` format: [xlogbackup.c:28-94](../../../postgres/src/backend/access/transam/xlogbackup.c).
  `read_backup_label` ([xlogrecovery.c:1216-1349](../../../postgres/src/backend/access/transam/xlogrecovery.c))
  requires `START WAL LOCATION` and `CHECKPOINT LOCATION`; `BACKUP METHOD: streamed`
  sets `backupEndRequired = true`; `BACKUP FROM: standby` sets `backupFromStandby`.
- With a backup_label, recovery reads the starting checkpoint from
  `CHECKPOINT LOCATION` (must be readable from archived WAL), sets
  `backupStartPoint = checkPoint.redo`, and — for `BACKUP FROM: standby` —
  `backupEndPoint = ControlFile->minRecoveryPoint` ([xlogrecovery.c:997](../../../postgres/src/backend/access/transam/xlogrecovery.c)).
  It FATALs "backup_label contains data inconsistent with control file" unless
  `state ∈ {DB_IN_ARCHIVE_RECOVERY, DB_SHUTDOWNED_IN_RECOVERY}`.
- Consistency gate ([xlogrecovery.c:2210, 2239](../../../postgres/src/backend/access/transam/xlogrecovery.c)):
  when `backupEndPoint` (= `minRecoveryPoint`) is reached, `ReachedEndOfBackup`
  clears `backupEndRequired`; then `!reachedConsistency && !backupEndRequired &&
  minRecoveryPoint <= lastReplayed` → `reachedConsistency = true`. So consistency
  is established at `minRecoveryPoint` with no end-of-backup WAL record.
- `pg_control` CRC: `INIT/COMP/FIN_CRC32C` over `[0, offsetof(ControlFileData, crc))`
  ([xlog.c:4292-4296](../../../postgres/src/backend/access/transam/xlog.c)); `crc`
  is the last field.

## Design decisions (resolved during brainstorming)

1. **Restore-time shaping** in `tiko_pitr` (fixes all already-archived bases;
   capture path stays a plain snapshot).
2. **`backup_label` + `pg_control` patch**, standby-style (consistency at the
   base checkpoint, no `XLOG_BACKUP_END` required).
3. **Defer** timeline `.history` / new-timeline promotion to a separate effort.

## Restore flow (`tiko_pitr recover`, after `extract_pg_state`)

In `recover_inner`, between extracting `pg_state` and writing the recovery conf,
shape PGDATA as a base backup:

1. Read `PGDATA/global/pg_control` bytes; via `pgcontrol::read_checkpoint_lsns`
   obtain `C` (checkpoint record LSN), `R` (redo), `tl` (timeline). `pg_control`
   is the source of truth (we patch it anyway).
2. Write `PGDATA/backup_label` (`pgcontrol::backup_label`).
3. Patch `PGDATA/global/pg_control` via `pgcontrol::shape_for_backup_recovery`
   (set `state`, `minRecoveryPoint = C`, `minRecoveryPointTLI = tl`; recompute
   CRC) and write it back.
4. (Existing) write the PITR recovery conf, touch `recovery.signal`, run
   `postgres`.

Recovery then: `backupStartPoint = R`; replays from `R`; at `C` the end-of-backup
branch fires (`backupEndPoint = minRecoveryPoint = C`), clears
`backupEndRequired`; `reachedConsistency = true` at `C`; replay continues to the
recovery target (already `>= C` by the window) and stops. The
"before consistent recovery point" FATAL only triggers for targets earlier than
`C`, which the recoverable window excludes.

On failure, the existing PGDATA-restore reverts `pg_control` and removes the new
`backup_label` (it's not in the pre-restore backup). On success, PostgreSQL
itself renames `backup_label` → `backup_label.old`; no extra cleanup needed.

## New module: `core/src/pgcontrol.rs`

Pure, unit-testable. PG18 `ControlFileData` field offsets (LP64; **verified at
runtime** by a `pg_control_version` guard so we never patch an unknown layout):

```
pg_control_version   @ 8   (u32)   // must == 1800 (PG18)
state                @ 16  (u32)   // DBState; DB_IN_ARCHIVE_RECOVERY = 5
checkPoint           @ 32  (u64)   // checkpoint record LSN  → C
checkPointCopy.redo  @ 40  (u64)   // redo                   → R
checkPointCopy.ThisTimeLineID @ 48 (u32) // timeline         → tl
minRecoveryPoint     @ 136 (u64)
minRecoveryPointTLI  @ 144 (u32)
crc                  = last field; CRC computed over [0, offsetof(crc))
```
(Offsets are taken from `postgres/src/include/catalog/pg_control.h` for this
build; implementation MUST confirm them against that header — e.g. a one-off
`offsetof` probe — and the runtime version guard refuses any non-1800 file.)

API:
- `pub fn read_checkpoint_lsns(ctl: &[u8]) -> Result<(Lsn /*checkpoint*/, Lsn /*redo*/, TimelineId)>`
  — version-guard, then read `checkPoint`, `checkPointCopy.redo`, `ThisTimeLineID`.
- `pub fn shape_for_backup_recovery(ctl: &mut [u8], min_recovery: Lsn, min_recovery_tli: TimelineId) -> Result<()>`
  — version-guard; set `state = 5`, `minRecoveryPoint = min_recovery`,
  `minRecoveryPointTLI = min_recovery_tli`; recompute and store the CRC-32C over
  `[0, offsetof(crc))`. Errors (not panics) on a too-short buffer / bad version.
- `pub fn backup_label(redo: Lsn, checkpoint: Lsn, tli: TimelineId, start_time: chrono::DateTime<Utc>) -> String`
  — emits the 7 lines above; `BACKUP METHOD: streamed`, `BACKUP FROM: standby`,
  `LABEL: tiko_pitr`. The `(file …)` token uses PG's `XLogFileName`
  (`{tli:08X}{(segno/256):08X}{(segno%256):08X}`, `segno = redo/XLOG_SEG_SIZE`)
  via a small `xlog_file_name` helper.
- `fn crc32c(data: &[u8]) -> u32` — CRC-32C (Castagnoli), matching PG's
  `pg_crc32c` (reflected poly `0x82F63B78`), table built once.

`DBState::DB_IN_ARCHIVE_RECOVERY = 5` (from `pg_control.h`).

## tiko_pitr integration

`recover_inner` gains a step calling a small binary-local
`shape_base_backup(pgdata: &Path) -> Result<()>` that performs flow steps 1-3
using the `core::pgcontrol` functions. No change to base selection, the
recovery window, the conf writer, or the failure/restore path.

## Error handling

- Unparseable/short/wrong-version `pg_control` → `Error` from
  `read_checkpoint_lsns` / `shape_for_backup_recovery`; `recover_inner` returns
  `Err`, triggering the existing restore-from-backup + leave-PG-stopped path.
- Empty `pg_state` already errors earlier (`extract_pg_state`).

## Testing

- **Unit (core):**
  - `crc32c` matches the standard CRC-32C check value (`"123456789"` →
    `0xE3069283`).
  - `backup_label` emits the expected lines (START WAL/CHECKPOINT LSNs in
    `X/Y`, `BACKUP FROM: standby`, `START TIMELINE`), and `xlog_file_name`
    matches PG's format for sample `(tli, segno)`.
  - `read_checkpoint_lsns` reads planted `checkPoint`/`redo`/`tli` from a
    synthetic 8192-byte control buffer (version 1800).
  - `shape_for_backup_recovery` round-trip: shape a synthetic control → the
    stored CRC equals a fresh `crc32c` over `[0, offsetof(crc))`, and `state` /
    `minRecoveryPoint` / `minRecoveryPointTLI` read back as set; a buffer with a
    non-1800 version is rejected.
- **Integration (out of band):** the live `tiko_pitr recover --time/--lsn` to a
  target inside the window reaches consistency, stops at the target, and
  restarts — verified against the running instance, per project convention.

## Out of scope

- Timeline `.history` generation/archival and post-PITR promotion to a new
  timeline (separate effort).
- The pre-existing divergence between tiko's archive WAL naming
  (`{tl:08X}{segno:016X}`) and PG's `XLogFileName` for `segno ≥ 256` (>4 GiB of
  WAL); harmless at current scale. (The `backup_label` `(file …)` token itself
  uses PG's format, which is what PG expects.)
- Changes to base selection, the recoverable window, the conf writer, or the
  WAL archiver.
