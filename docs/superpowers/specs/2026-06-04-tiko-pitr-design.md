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

## Source-of-truth model (segment-based)

The current architecture is **segment-based**. `core/src/recovery.rs` is a
legacy file that still references a delta-manifest model (`delta_manifest_key`,
`apply_deltas`, `pg_state_key`, `base_prefix_for_timeline`) and must be treated
as reference only — it is refactored as part of this work. The authoritative
logic lives in `core/src/manifest.rs`, `core/src/io/timeline.rs`, and
`core/src/io/store.rs`.

Data layout (from `core/src/io/locator.rs`):

- **Checkpoints** are `SegmentCheckpoint`s inside `TimelineSegment` files under
  `timeline_segments_dir()` = `{ns}/timeline/`, keyed
  `{ns}/timeline/{tl:08X}/{idx:016X}.segment`. Each `SegmentCheckpoint` carries
  `ckpt`, `prev_ckpt`, `redo_ckpt`, `chunks`, `relforks`, `pg_state`,
  `created_at` (`core/src/io/timeline.rs`). This is what `list` enumerates.
- **Base manifests** are produced by the compactor (`Store::run_compaction`),
  listed under `bases_dir()` = `{ns}/bases/`, keyed
  `{ns}/bases/{tl}/{lsn_hex}.manifest`. Each `Manifest` carries its own
  `checkpoint()` and an embedded self-contained `pg_state` (`pg_state.tar.zst`),
  accessible via `Manifest::pg_state()` (public, `core/src/manifest.rs`).

Relevant existing pieces:

- **`cli/src/bin/tiko_restore.rs`** — the `restore_command` helper PostgreSQL
  invokes to fetch WAL segments from remote. `tiko_pitr` reuses it as the
  recovery WAL source (`restore_command = 'tiko_restore %f %p'`).
- **`cli/src/bin/tiko_tlseg_viewer.rs`** — parses one `.segment` file and prints
  its `SegmentCheckpoint`s. `tiko_pitr list` generalizes this across all segments.
- **`core/src/io/store.rs`** — `Store::init()` (singleton, configured from env,
  works standalone as `tiko_restore` proves), `storage_get` (auto-decompresses),
  `storage_list_prefix`, `locator()`, and the **private** `list_all_segments` /
  `load_segment` listing pattern. The locator key helpers (`bases_dir`,
  `timeline_segments_dir`, `timeline_segment`, `base_manifest`) are
  `pub(crate)`/`pub(super)` — not reachable from the `cli` crate, which forces
  the reusable logic to be exposed as public `Store` methods.
- **`core/src/recovery.rs`** — legacy. Provides the conf begin/end marker
  constants and `remove_recovery_conf` (worth keeping), but its
  `write_recovery_conf` is branch-oriented (`recovery_target = 'immediate'`, no
  `restore_command`) and its base-manifest/delta code is dead under the segment
  model. Refactored here (see below).

## Design decisions (resolved during brainstorming)

1. **Recovery model:** `restore_command` + `recovery_target_lsn` +
   `recovery_target_action = 'shutdown'`. True remote PITR; does NOT reuse
   `prepare_recovery`'s WAL-copy-from-parent path.
2. **PG lifecycle:** driven by `pg_ctl`; `action = 'shutdown'` so PG shuts itself
   down on reaching the target — no polling/libpq connection needed.
3. **Scope:** extract `pg_state` only. Do NOT build `recovery_manifest.bin`.
4. **List UX:** `list` subcommand prints a table and exits; `recover` takes an
   explicit target. (List-only, re-run with args — scriptable.)
5. **Code split:** binary stays thin; reusable logic (checkpoint listing,
   base-manifest selection, PITR conf writer) lives in `core` for sharing and
   unit testing. This requires exposing new public `Store` methods, since the
   underlying locator/segment helpers are crate-private.
6. **recovery.rs refactor:** drop the dead delta-manifest code paths and align
   the surviving conf helpers with the segment-based PITR flow (details below).

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

New public methods on `Store` (in `core/src/io/store.rs`), built on the existing
private `list_all_segments` / `load_segment` and `storage_*` primitives:

- **`list_checkpoints()`** — scan `timeline_segments_dir()`, load each
  `TimelineSegment` (`storage_get` auto-decompresses → `TimelineSegment::from_bytes`),
  flatten all `SegmentCheckpoint`s into a `Vec` of lightweight rows
  (`ckpt: Checkpoint`, `redo_ckpt`, `created_at: i64`, `n_chunks: usize`), sorted
  ascending by `(created_at, ckpt)`. Read-only.
- **`load_base_manifest_at_or_before(target: Checkpoint) -> Result<Manifest>`** —
  list `bases_dir()`, parse each key's `{tl}/{lsn_hex}.manifest` into a
  `Checkpoint`, pick the newest with `base_ckpt <= target`, `storage_get` it, and
  return `Manifest::from_bytes(...)`. Distinct, clear error if no base covers the
  target. This is the segment-model replacement for the legacy
  `recovery.rs::load_base_manifest`.

### recovery.rs refactor (conf helpers)

- **Keep:** the begin/end marker constants and `remove_recovery_conf` (clean,
  marker-delimited strip — reused verbatim for cleanup).
- **Add:** `write_pitr_recovery_conf(conf_path, target_tl, target_lsn)` that
  appends a Tiko PITR block to `postgresql.tiko.conf`, delimited by the SAME
  markers so `remove_recovery_conf` strips it identically:
  ```
  restore_command = 'tiko_restore %f %p'
  recovery_target_lsn = '<target_lsn>'
  recovery_target_timeline = '<target_tl>'
  recovery_target_inclusive = on
  recovery_target_action = 'shutdown'
  ```
- **Remove:** the dead delta-manifest code paths (`prepare_recovery`'s
  delta/`pg_state_key` logic, `apply_deltas_up_to`, `load_base_manifest`) that
  no longer fit the segment model. Scope the removal to what is genuinely dead;
  do not touch unrelated recovery-mode runtime hooks still in use.

### binary (`cli/src/bin/tiko_pitr.rs`)

Thin orchestration over the `core` helpers and `pg_ctl`/`tar`. Uses
`extern crate cli;` for `pg_stubs` (same as `tiko_restore`).

## Data flow

### `list`

1. `Store::init()`.
2. `store.list_checkpoints()` (scans all timeline segments, flattens
   `SegmentCheckpoint`s, sorts by `(created_at, ckpt)`).
3. Print a numbered table: index, timeline, LSN, created_at (RFC3339 via
   `chrono`), #chunks.

### `recover`

1. `Store::init()`; resolve `pgdata` and `pg_ctl` path.
2. Validate the target `(timeline, lsn)` appears in `list_checkpoints()`
   (fail fast before touching PGDATA).
3. **Pick base manifest:** `store.load_base_manifest_at_or_before(target)` —
   newest base with `base_ckpt <= target`. Clear error if none covers the target.
4. **Extract pg_state:** `Manifest::pg_state()` → tempfile → `tar -xf` into
   `pgdata`. Lays down the base checkpoint's `pg_control` + xlog state; PG
   replays WAL forward from there to the target.
5. **Write PITR conf:** `write_pitr_recovery_conf(pgdata/postgresql.tiko.conf,
   target_tl, target_lsn)`.
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
  - `load_base_manifest_at_or_before`: picks newest base `<= target`; returns
    the no-coverage error when none qualifies. (Use an `S3Sim`-backed `Store`
    over a temp dir, as existing `core` storage tests do.)
  - Base-manifest key → `Checkpoint` parsing round-trip.
  - `write_pitr_recovery_conf`: emitted block contains the expected directives
    and round-trips cleanly through `remove_recovery_conf` (write → remove
    leaves the file as before).
- `list_checkpoints` and binary orchestration (`pg_ctl`, `tar`, full recovery)
  are integration-level and verified separately, per project workflow.

## Out of scope

- Building `recovery_manifest.bin` (data-file chunk-version resolution during
  replay) — explicitly deferred.
- `recovery_target_time`/named-restore-point targets — target is a checkpoint
  `(timeline, lsn)`.
- Interactive checkpoint picker — `list` is print-and-exit.
