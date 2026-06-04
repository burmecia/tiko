# tiko_pitr — PITR recovery automation CLI

**Date:** 2026-06-04
**Status:** Approved (design)

## Goal

A new standalone binary `tiko_pitr` that automates the full point-in-time
recovery (PITR) lifecycle against Tiko's remote storage:

1. List all available timeline-segment checkpoints on remote so the user can
   choose a recovery target.
2. Alternatively, accept a target checkpoint directly via command-line args.
3. Pick the nearest base manifest at or before the target checkpoint.
4. Extract that base manifest's embedded `pg_state` into the PostgreSQL data
   directory.
5. Adjust the PostgreSQL conf to drive recovery to the target checkpoint.
6. Run recovery, then restart the instance normally once it completes.

## Background / existing infrastructure

- **`cli/src/bin/tiko_restore.rs`** — the `restore_command` helper PostgreSQL
  invokes to fetch WAL segments from remote during archive recovery. `tiko_pitr`
  reuses this as the recovery WAL source (`restore_command = 'tiko_restore %f %p'`).
- **`cli/src/bin/tiko_tlseg_viewer.rs`** — parses one `.segment` file and prints
  its `SegmentCheckpoint`s. `tiko_pitr list` generalizes this across all segments.
- **`core/src/recovery.rs`** — has the building blocks: `load_base_manifest`
  (newest base with `base_lsn <= target_lsn`, currently private),
  `write_recovery_conf` / `remove_recovery_conf` (conf block delimited by shared
  begin/end markers), `TIKO_CONF_FILE = "postgresql.tiko.conf"`.
  NOTE: the existing `write_recovery_conf` is built for *branching*
  (`recovery_target = 'immediate'`, no `restore_command`) — `tiko_pitr` needs a
  different conf writer (see below).
- **`core/src/manifest.rs`** — `Manifest` (TIKM format). Base manifests embed a
  self-contained `pg_state` (`pg_state.tar.zst`) in the trailer, accessible via
  `Manifest::pg_state()`. Recent commits made base manifests self-contained
  (redo checkpoint + pg_state) precisely for PITR.
- **`core/src/io/timeline.rs`** — `TimelineSegment` (`.segment` files under
  `{ns}/timeline/{tl:08X}/{idx:016X}.segment`), each holding ordered
  `SegmentCheckpoint`s (`ckpt`, `prev_ckpt`, `redo_ckpt`, `chunks`, `relforks`,
  `pg_state`, `created_at`).
- **`core/src/io/store.rs`** — `Store::init()` (singleton, configured from env),
  `storage_get` (auto-decompresses), `storage_list_prefix`, and the private
  `list_all_segments` listing pattern.

## Design decisions (resolved during brainstorming)

1. **Recovery model:** `restore_command` + `recovery_target_lsn` +
   `recovery_target_action = 'shutdown'`. True remote PITR; does NOT reuse
   `prepare_recovery`'s WAL-copy-from-parent path.
2. **PG lifecycle:** driven by `pg_ctl`; `action = 'shutdown'` so PG shuts itself
   down on reaching the target — no polling/libpq connection needed.
3. **Scope:** extract `pg_state` only. Do NOT build `recovery_manifest.bin`.
4. **List UX:** `list` subcommand prints a table and exits; `recover` takes an
   explicit target. (List-only, re-run with args — scriptable.)
5. **Code split:** binary stays thin; reusable logic (base-manifest selection,
   PITR conf writer) lives in `core` for sharing and unit testing.

## CLI surface (clap subcommands)

```
tiko_pitr list
    Print a numbered table of all checkpoints found across timeline segments:
    index, timeline, LSN, created_at (RFC3339), #chunks. Read-only; no PGDATA.

tiko_pitr recover --timeline <TL> --lsn <LSN> [--pgdata <DIR>] [--pg-ctl <PATH>]
    Recover the instance to the given checkpoint, then restart normally.
```

Storage configured via env, identical to `tiko_restore` (`Store::init()`):
`TIKO_ROOT_PATH`/`PGDATA`, `TIKO_ORG_ID`, `TIKO_DB_ID`, `TIKO_PROJECT_ID`.
`--pgdata` defaults to `$PGDATA`; `--pg-ctl` defaults to `pg_ctl` on `PATH`.

The binary uses `extern crate cli;` for `pg_stubs` (same as `tiko_restore`),
since `core`'s undefined PG symbols must resolve in a standalone process.

## Components

### core additions (new/exposed, unit-tested)

- **Base-manifest selection** — expose the logic in `recovery.rs::load_base_manifest`
  (newest base with `base_lsn <= target_lsn` on the target timeline; error if
  none covers the target) as a reusable function callable from the binary.
- **PITR conf writer** — a new function alongside `write_recovery_conf` that
  appends a Tiko PITR block to `postgresql.tiko.conf`, delimited by the SAME
  begin/end markers `remove_recovery_conf` already strips:
  ```
  restore_command = 'tiko_restore %f %p'
  recovery_target_lsn = '<target_lsn>'
  recovery_target_timeline = '<target_tl>'
  recovery_target_inclusive = on
  recovery_target_action = 'shutdown'
  ```
  Marker constants stay shared so `remove_recovery_conf` cleans it identically.

### binary (`cli/src/bin/tiko_pitr.rs`)

Thin orchestration over the `core` helpers and `pg_ctl`/`tar`.

## Data flow

### `list`

1. `Store::init()`.
2. List timeline-segment objects under the timeline prefix.
3. For each segment key: `storage_get` (auto-decompresses) →
   `TimelineSegment::from_bytes` → iterate `checkpoints`.
4. Collect `(ckpt.timeline_id, ckpt.lsn, created_at, chunks.len())`, sort by
   `(created_at, ckpt)`, print numbered table.

### `recover`

1. `Store::init()`; resolve `pgdata` and `pg_ctl` path.
2. Validate the target `(timeline, lsn)` appears in the listed checkpoints
   (fail fast before touching PGDATA).
3. **Pick base manifest:** newest base with `base_lsn <= target_lsn` on the
   target timeline (shared `core` helper). Clear error if none covers the target.
4. **Extract pg_state:** `Manifest::pg_state()` → tempfile → `tar -xf` into
   `pgdata`. Lays down the base checkpoint's `pg_control` + xlog state.
5. **Write PITR conf** to `postgresql.tiko.conf` (new conf writer above).
6. **Touch `recovery.signal`.**
7. **Run recovery:** `pg_ctl -D <pgdata> start` and wait for the postmaster to
   exit. With `action = 'shutdown'`, PG replays WAL (pulling segments via
   `tiko_restore`) up to `recovery_target_lsn`, then shuts itself down. Exact
   wait mechanism (spawn + wait for postmaster exit, or poll `postmaster.pid`)
   to be settled during planning; contract is "block until PG finishes recovery
   and exits."
8. **Clean up:** `remove_recovery_conf(postgresql.tiko.conf)` + delete
   `recovery.signal`.
9. **Restart normally:** `pg_ctl -D <pgdata> -w start` (new timeline at promotion).

## Error handling

- Every step returns `Result`; failures print `tiko_pitr: <context>: <err>` to
  stderr and `exit(1)`.
- Fail fast: validate the target exists in the checkpoint list before any PGDATA
  mutation (step 2).
- **Recovery failure (step 7):** still attempt conf/`recovery.signal` cleanup
  (step 8) so a half-written conf doesn't wedge the next start, but do NOT
  auto-start normally — leave PG down and report, since the data-dir state is
  uncertain.

## Testing

- Unit tests in `core`:
  - Base-manifest selection: picks newest `<= target_lsn`; returns the
    no-coverage error when none qualifies.
  - PITR conf writer: emitted block round-trips cleanly through
    `remove_recovery_conf` (write → remove leaves the file as before).
- Binary orchestration (`pg_ctl`, `tar`, full recovery) is integration-level and
  verified separately, per project workflow.

## Out of scope

- Building `recovery_manifest.bin` (data-file chunk-version resolution during
  replay) — explicitly deferred.
- `recovery_target_time`/named-restore-point targets — target is a checkpoint
  `(timeline, lsn)`.
- Interactive checkpoint picker — `list` is print-and-exit.
