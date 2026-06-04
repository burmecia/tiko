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
- **`core/src/recovery.rs`** — fully orphaned dead code: `pub mod recovery;` is
  commented out in `core/src/lib.rs:9`, it references types that no longer exist
  (`ProjectNamespace`, `pg_state_key`, `apply_deltas`, `recovery_manifest_path`),
  and the only references to it are in `cli/legacy/` (also disabled). It does not
  compile and nothing in the build depends on it. The conf marker constants,
  `remove_recovery_conf`, and the `recovery_target_action`/conf-block approach
  are good and worth salvaging. Refactor = delete the dead file and lift the
  salvageable conf logic into a new, compiled, tested module (see below).

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
6. **recovery.rs refactor:** `recovery.rs` is orphaned dead code (uncompiled,
   references nonexistent types). Delete it and lift the salvageable conf logic
   (markers, `remove_recovery_conf`) into a new compiled module `core/src/pitr.rs`
   alongside the new PITR conf writer (details below).

## CLI surface (clap subcommands)

```
tiko_pitr list
    Print a numbered table of all checkpoints found across timeline segments:
    index, timeline, LSN, created_at (RFC3339), #chunks. Read-only; no PGDATA.

tiko_pitr recover --timeline <TL> --lsn <LSN>
                  [--pgdata <DIR>] [--pg-ctl <PATH>] [--postgres <PATH>]
    Recover the instance to the given checkpoint, then restart normally.
```

Storage configured via env, identical to `tiko_restore` (`Store::init()`):
`TIKO_ROOT_PATH`/`PGDATA`, `TIKO_ORG_ID`, `TIKO_DB_ID`, `TIKO_PROJECT_ID`.
`--lsn` accepts PG `X/Y` or hex (`Lsn::parse_either`). `--pgdata` defaults to
`$PGDATA`; `--pg-ctl` defaults to `pg_ctl` on `PATH`; `--postgres` defaults to
the `postgres` binary sibling of `--pg-ctl` (falling back to `postgres` on `PATH`).

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
- **`load_base_pg_state_at_or_before(target: Checkpoint) -> Result<(Checkpoint, Vec<u8>)>`** —
  list `bases_dir()`, parse each key's `{tl}/{lsn_hex}.manifest` into a
  `Checkpoint`, pick the newest with `base_ckpt <= target`, `storage_get` it,
  `Manifest::from_bytes` it into a throwaway `tempdir` (so the process's live
  `local_root` TIKM cache is not clobbered), and return
  `(manifest.checkpoint(), manifest.pg_state().to_vec())`. `pg_state` and
  `checkpoint` are in-memory fields, so they remain valid after the tempdir is
  dropped. Distinct, clear `Error` (`Error::other(...)`) if no base covers the
  target. Segment-model replacement for the legacy `recovery.rs::load_base_manifest`.

Plus a small filesystem helper in `core/src/pitr.rs` (so it is unit-testable in
`core`, where tests reliably run):

- **`backup_dir_excluding(src, dst, exclude_name)` / `restore_dir(backup, dst, exclude_name)`** —
  recursively copy a directory to a sibling backup, skipping a named
  subdirectory (`tiko/`) and the backup dir itself; and restore from it.
  Portable Rust walk (not `cp`/`rsync`, which can't cleanly exclude or may be
  absent). Used for the step-5 PGDATA snapshot.

### New module `core/src/pitr.rs` (conf helpers) + delete `recovery.rs`

- **Delete** `core/src/recovery.rs` (orphaned, uncompiled, references nonexistent
  types; nothing in the build depends on it).
- **Create `core/src/pitr.rs`** (wired via `pub mod pitr;` in `lib.rs`) holding:
  - The begin/end marker constants and `remove_recovery_conf` (lifted from
    recovery.rs — clean, marker-delimited strip).
  - `write_pitr_recovery_conf(conf_path, target_tl: TimelineId, target_lsn: Lsn)`
    that appends a Tiko PITR block delimited by the SAME markers so
    `remove_recovery_conf` strips it identically:
    ```
    restore_command = 'tiko_restore %f %p'
    recovery_target_lsn = '<lsn.to_pg_string()>'
    recovery_target_timeline = '<tl.as_u32()>'
    recovery_target_inclusive = on
    recovery_target_action = 'shutdown'
    ```
- These helpers are pure string/file ops with no `Store` dependency, so they are
  directly unit-testable.

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
3. **Pick base pg_state:** `store.load_base_pg_state_at_or_before(target)` →
   `(base_ckpt, pg_state_bytes)`, newest base with `base_ckpt <= target`. Clear
   error if none covers the target.
4. **Stop PostgreSQL:** `pg_ctl -D <pgdata> -m fast -w stop`, so the data dir is
   quiesced before it is copied or mutated. Tolerate an already-stopped
   instance (treat "not running" as success).
5. **Back up PGDATA (excluding `tiko/`):** with PG stopped, recursively copy
   `pgdata` to a sibling backup dir (e.g. `{pgdata}.tiko_pitr_bak`), skipping the
   `tiko/` directory (the bulk data/cache backed by remote storage) and the
   backup dir itself. This is the crash-safety snapshot — cheap because the large
   data files live in `tiko/`, which is omitted. If a backup dir already exists
   (a prior interrupted run), abort and tell the user to inspect it rather than
   overwriting.
6. **Extract pg_state:** write `pg_state_bytes` → tempfile → `tar -xf` into
   `pgdata`. Lays down the base checkpoint's `pg_control` + xlog state; PG
   replays WAL forward from there to the target.
7. **Write PITR conf:** `write_pitr_recovery_conf(pgdata/postgresql.tiko.conf,
   target_tl, target_lsn)`.
8. **Touch `recovery.signal`.**
9. **Run recovery:** run the `postgres` server binary in the **foreground** —
   `postgres -D <pgdata>` via `Command::status()` — and wait for it to exit.
   With `recovery_target_action = 'shutdown'`, PG replays WAL (pulling segments
   via `tiko_restore`) up to `recovery_target_lsn`, then shuts itself down and
   the process exits **0**. If it cannot reach the target (e.g. WAL ends first,
   bad target, crash) PG exits **non-zero**. So the foreground exit status is a
   reliable success/failure signal — cleaner than scripting `pg_ctl start -w`,
   which polls for "ready to accept connections" that never happens in the
   shutdown case. The `postgres` binary path is derived as a sibling of the
   `pg_ctl` path, overridable via `--postgres`.
10. **On success:** `remove_recovery_conf(postgresql.tiko.conf)`, delete
   `recovery.signal`, delete the PGDATA backup, then **restart normally**:
   `pg_ctl -D <pgdata> -w start` (new timeline at promotion).
11. **On failure:** see Error handling below — restore PGDATA from the backup.

## Error handling

- Every step returns `Result`; failures print `tiko_pitr: <context>: <err>` to
  stderr and `exit(1)`.
- Fail fast: validate the target exists in the checkpoint list before stopping
  PG or touching PGDATA (steps 2–3, before the stop/backup in steps 4–5).
- **Recovery failure (step 9)** — i.e. recovery does not complete successfully
  (non-zero/abnormal postmaster exit, or the target is not reached):
  1. Ensure PG is stopped.
  2. **Restore PGDATA from the backup** taken in step 5: delete the mutated
     PGDATA contents (excluding `tiko/`) and move the backup's contents back,
     returning `pg_control`, `pg_wal/`, and conf files to their pre-recovery
     state. This subsumes conf/`recovery.signal` cleanup — the restored conf has
     no recovery block and no `recovery.signal`.
  3. Do NOT auto-start normally — leave PG down and report, so the operator can
     inspect before retrying.
  The backup is only deleted on success (step 10); on failure it is consumed by
  the restore. If the restore itself fails, leave the backup in place and report
  loudly with its path.

## Testing

- Unit tests in `core`:
  - `load_base_pg_state_at_or_before`: picks newest base `<= target` and returns
    its `(checkpoint, pg_state)`; returns the no-coverage error when none
    qualifies. (Use an `S3Sim`-backed `Store` over a temp dir, as existing `core`
    storage tests do.)
  - Base-manifest key → `Checkpoint` parsing round-trip.
  - `write_pitr_recovery_conf`: emitted block contains the expected directives
    and round-trips cleanly through `remove_recovery_conf` (write → remove
    leaves the file as before).
  - `backup_dir_excluding` / `restore_dir`: over a temp dir tree, the excluded
    subdir is skipped, and backup → mutate → restore returns the tree to its
    original contents.
- `list_checkpoints` and binary orchestration (`pg_ctl`, `tar`, full recovery)
  are integration-level and verified separately, per project workflow.

## Out of scope

- Building `recovery_manifest.bin` (data-file chunk-version resolution during
  replay) — explicitly deferred.
- `recovery_target_time`/named-restore-point targets — target is a checkpoint
  `(timeline, lsn)`.
- Interactive checkpoint picker — `list` is print-and-exit.
