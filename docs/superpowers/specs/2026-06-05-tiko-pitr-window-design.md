# tiko_pitr — window-based recovery targets

**Date:** 2026-06-05
**Status:** Approved (design)

## Goal

Change `tiko_pitr` from selecting a recovery target out of a discrete list of
checkpoints to **picking any point within a recoverable time window**. This makes
the user-facing recovery target robust against compaction: the compactor deletes
old timeline segment files after folding them into the base manifest
([store.rs](../../../core/src/io/store.rs) `run_compaction`), so a target list
sourced from segments silently loses entries that are still physically
recoverable.

End users will use `tiko_pitr list` to see the recoverable window and then
`tiko_pitr recover --time <TS>` (or `--lsn`) to recover to any point inside it.

## Background

Replay only needs three things, all in keyspaces compaction does NOT touch:
base manifests (`{ns}/bases/...`, carry `pg_state` + chunk map + a `timestamp`),
WAL segments (`{ns}/wal/...`, fetched via `tiko_restore`), and chunk objects
(retained while a base references them). Timeline segment files
(`{ns}/timeline/...`) are an internal read-path/compaction artifact and a poor
source of truth for a user-facing catalog.

Therefore the recoverable window is derived from base manifests + the newest
segment checkpoint — both bounded by retention, not by compaction.

This supersedes the exact-checkpoint target model in the existing (unmerged)
`tiko_pitr` (`docs/superpowers/specs/2026-06-04-tiko-pitr-design.md`): the
`r.ckpt == target` validation is removed in favor of window-bounds validation.

## Design decisions (resolved during brainstorming)

1. **Target input:** `--time <TS>` and `--lsn <LSN>`, mutually exclusive,
   exactly one required. `--time` is the primary UX; `--lsn` is the precise
   alternative.
2. **`latest` bound:** the newest `SegmentCheckpoint.created_at`. Knowable from
   data already loaded and conservative (everything up to the last checkpoint is
   definitely replayable); slightly behind the true WAL tail, which is acceptable.
3. **Timeline:** default to the newest checkpoint's timeline; `--timeline <HEX>`
   overrides. `recovery_target_timeline` is written accordingly.
4. **Window/base-selection mechanism (v1):** read base-manifest headers via
   on-demand `storage_get`. No new storage keyspace. Future optimization:
   range-GET the 48-byte TIKM header (needs backend range-read support; real S3
   yes, S3Sim no) or a lightweight `{ns}` window index.

## Window model

The recoverable window is `[earliest, latest]`:

- **earliest** = the oldest retained base manifest's `timestamp` and `checkpoint`.
  A time/LSN target before this cannot be reached (no base covers it; PG reaches
  consistency at the base checkpoint before it could stop earlier).
- **latest** = the newest `SegmentCheckpoint.created_at` (and its checkpoint/LSN).
- **default timeline** = the newest checkpoint's `timeline_id`.

`earliest`/`latest` LSNs are also retained (for `--lsn` bounds checking).

Base manifest `timestamp` is the max merged-checkpoint time
(`Manifest::apply_segments` sets `last_ts`), i.e. the time of the base's
checkpoint — suitable for ordering bases by time. LSN order and time order are
both monotonic, so "newest base with `timestamp <= T`" is well defined.

## CLI surface

```
tiko_pitr list
    Print the recoverable window: earliest (RFC3339), latest (RFC3339), and the
    default timeline (hex). Read-only; no PGDATA.

tiko_pitr recover (--time <TS> | --lsn <LSN>) [--timeline <HEX>]
                  [--pgdata DIR] [--pg-ctl PATH] [--postgres PATH]
    Recover to a point in the window, then restart normally. Exactly one of
    --time / --lsn is required (mutually exclusive).
```

- `--time` accepts a PostgreSQL-style timestamp string (e.g.
  `'2026-06-04 10:00:00'` or RFC3339). It is parsed to a Unix timestamp (via
  `chrono`; interpreted as UTC when no offset is given) **only** to compare
  against the window and to select the base, and is otherwise passed through to
  `recovery_target_time` verbatim — PostgreSQL does the authoritative parse
  during replay. A string `chrono` cannot parse is a clear error before any
  mutation.
- `--lsn` accepts PG `X/Y` or hex (`Lsn::parse_either`).
- `--timeline` defaults to the window's default timeline.
- `--pgdata`/`--pg-ctl`/`--postgres` as in the existing `recover`.

Storage config via env, identical to `tiko_restore` (`Store::init()`).

## Components

### core additions (unit-tested where pure)

- **`Manifest::timestamp(&self) -> i64`** — accessor for the existing TIKM header
  `timestamp` field.

- **`RecoveryWindow`** (pub struct in `core/src/io/store.rs`):
  ```
  pub struct RecoveryWindow {
      pub earliest_ts: i64,
      pub earliest_ckpt: Checkpoint,
      pub latest_ts: i64,
      pub latest_ckpt: Checkpoint,
      pub timeline: TimelineId,
  }
  ```

- **`Store::recovery_window(&self) -> Result<RecoveryWindow>`** —
  `earliest` from the oldest base manifest (lowest-LSN key under `bases_dir()`;
  fetch it, read `checkpoint()`/`timestamp()`); `latest` + `timeline` from the
  newest `CheckpointRow` of `list_checkpoints()`. If no base manifest exists, or
  no checkpoints exist, return a clear `Error::other`.

- **`Store::load_base_pg_state_before_time(target_ts: i64, timeline: TimelineId) -> Result<(Checkpoint, Vec<u8>)>`** —
  list base keys, iterate newest→oldest (by parsed LSN), fetch each, and return
  the first whose `timestamp() <= target_ts` and whose checkpoint timeline
  matches `timeline`, as `(checkpoint, pg_state)`. Clear error if none qualifies.
  (Parallel to the existing `load_base_pg_state_at_or_before` used by `--lsn`.)

- **Generalized conf writer** in `core/src/pitr.rs`:
  ```
  pub enum RecoveryTarget { Lsn(Lsn), Time(String) }

  pub fn write_pitr_recovery_conf(
      conf_path: &Path,
      timeline: TimelineId,
      target: &RecoveryTarget,
  ) -> Result<()>
  ```
  Emits `restore_command`, `recovery_target_timeline = '<tl.as_u32()>'`,
  `recovery_target_inclusive = on`, `recovery_target_action = 'shutdown'`, plus
  exactly one of `recovery_target_lsn = '<lsn.to_pg_string()>'` or
  `recovery_target_time = '<ts>'`. Same begin/end markers so
  `remove_recovery_conf` strips it unchanged.

### binary (`cli/src/bin/tiko_pitr.rs`)

- `run_list`: call `recovery_window()`, print earliest/latest (RFC3339 via
  `chrono`) and default timeline.
- `run_recover`: parse exactly one of `--time`/`--lsn`; resolve timeline
  (override or window default); validate the target is within the window
  (time or LSN bounds) **before** any PGDATA mutation; select base
  (`load_base_pg_state_before_time` for time, `load_base_pg_state_at_or_before`
  for LSN); build the matching `RecoveryTarget`; then the unchanged lifecycle
  (stop → backup excl `tiko/` → extract pg_state → write conf → foreground
  `postgres` → success cleanup+restart / failure restore).

`clap`: make `--time`/`--lsn` a mutually-exclusive, required group (e.g. an
`ArgGroup` or `Option<String>` fields validated in code with a clear error if
zero or both are given).

## Data flow

### `list`
1. `Store::init()`.
2. `store.recovery_window()`.
3. Print: `earliest <RFC3339>`, `latest <RFC3339>`, `timeline <hex>`.

### `recover`
1. Parse exactly one of `--time`/`--lsn` (error if zero or both).
2. `store.recovery_window()`; resolve `timeline` (override or `window.timeline`).
3. Validate target within window:
   - `--time T`: `window.earliest_ts <= parse(T) <= window.latest_ts`.
   - `--lsn L`: `window.earliest_ckpt.lsn <= L <= window.latest_ckpt.lsn`
     (on the resolved timeline).
   Out-of-window → error, no mutation.
4. Select base + `pg_state`:
   - time → `load_base_pg_state_before_time(T_unix, timeline)`.
   - lsn → `load_base_pg_state_at_or_before(Checkpoint::new(timeline, L))`.
5. Build `RecoveryTarget::Time(raw_ts_string)` or `RecoveryTarget::Lsn(L)`.
6. Stop PG → backup → extract pg_state → `write_pitr_recovery_conf(conf,
   timeline, &target)` → touch `recovery.signal` → foreground `postgres` →
   success (clean conf/signal, drop backup, restart) / failure (restore from
   backup, leave PG down). Unchanged from the existing recover lifecycle.

## Error handling

- Exactly-one-of `--time`/`--lsn`: clear error if neither or both supplied.
- `--time` that fails the comparison parse, or out-of-window target: error before
  any PGDATA mutation.
- No base manifest / no checkpoints: `recovery_window()` returns a clear error.
- Unchanged: backup-before-mutation, restore-on-failure, leave-PG-stopped-on-failure.

## Testing

- Unit tests in `core`:
  - `recovery_window` computation: given fixtures (a set of base headers + a set
    of segment checkpoints), earliest = oldest base, latest = newest checkpoint,
    timeline = newest checkpoint's; error paths (no base, no checkpoints). Pure
    helper extraction where possible so it tests without the `Store` singleton;
    `Store`-level method itself is build/integration-verified.
  - time-based base selection: newest base with `timestamp <= T` (pure selection
    helper over `(timestamp, checkpoint, key)` tuples, mirroring Task-4 style);
    none-before-T error; timeline filter.
  - `write_pitr_recovery_conf` for both `RecoveryTarget` variants: correct
    directive emitted; round-trips cleanly through `remove_recovery_conf`.
- `list`/`recover` orchestration is integration-verified separately, per project
  convention.

## Out of scope

- Named restore points (`pg_create_restore_point`) in the window output —
  future enhancement.
- A persisted recovery-window/recovery-point index and its GC coupling (Option B)
  — future; v1 derives the window on demand.
- Range-GET header optimization (Option C) — future, when real S3 lands.
- Mapping `--time` to an LSN ourselves — PostgreSQL handles `recovery_target_time`
  during replay; we only compare against the window.
