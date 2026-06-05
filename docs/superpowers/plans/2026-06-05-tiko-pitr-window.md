# tiko_pitr Window-Based Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `tiko_pitr` to recover to any point within a recoverable time window (compaction-stable), instead of selecting from a discrete list of checkpoints.

**Architecture:** Derive a recoverable window `[earliest, latest]` from base manifests (oldest base = earliest) + the newest segment checkpoint (latest), neither of which compaction destroys. `recover` takes `--time` or `--lsn`, validates it against the window, picks the base by time or LSN, and writes `recovery_target_time`/`recovery_target_lsn` accordingly. New logic lives in `core` (`Store` methods + pure helpers in `pitr.rs`/`store.rs`); the binary stays thin.

**Tech Stack:** Rust (edition 2024), `clap` v4 (derive + `ArgGroup`), `chrono`, `tempfile`, PostgreSQL `pg_ctl`/`postgres`.

**Reference spec:** `docs/superpowers/specs/2026-06-05-tiko-pitr-window-design.md`

**Conventions (project memory / CLAUDE.md):**
- Build: `cargo build -p core` (and `-p cli` once the binary changes land). Workspace stays green after every task.
- Tests: `cargo test -p core`. `cargo clippy` is blocked by pre-existing `pgsys` lint errors (unrelated) — verify lint-cleanliness via a warning-free build instead.
- Commit after each task. Branch `pitr2` (already checked out).
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

**Sequencing note:** Tasks 1–4 are purely additive (no existing signature changes), so `cargo build -p core -p cli` stays green throughout. Task 5 changes the `write_pitr_recovery_conf` signature AND rewrites the binary in one commit, so the workspace compiles at that commit too. Some private helpers added in Tasks 2–4 will emit an expected `dead_code` warning under the non-test build until their consumer lands (test build is clean since tests reference them); do NOT add `#[allow(dead_code)]`.

---

### Task 1: `Manifest::timestamp()` accessor

**Files:** Modify `core/src/manifest.rs`

The TIKM header `timestamp` field exists but has no public accessor; the window needs it. This mirrors the existing sibling getters `checkpoint()` / `pg_state()`.

- [ ] **Step 1: Add the accessor**

In `core/src/manifest.rs`, immediately after the `pub fn checkpoint(&self) -> Checkpoint { self.checkpoint }` method (around line 465-467), add:
```rust
    /// Return the TIKM header timestamp (unix seconds) — the time of this base
    /// manifest's checkpoint. Used to order base manifests by time for PITR.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
```

- [ ] **Step 2: Build**

Run: `cargo build -p core`
Expected: clean build (a `pub fn` getter, no dead-code warning).

- [ ] **Step 3: Commit**

```bash
git add core/src/manifest.rs
git commit -m "feat(core): add Manifest::timestamp accessor"
```

---

### Task 2: `parse_pg_timestamp` in `core/src/pitr.rs` (TDD)

**Files:** Modify `core/src/pitr.rs` (+ inline test)

A pure helper that parses a `--time` string to a Unix timestamp for window comparison and base selection. Interprets bare timestamps as UTC; full offset strings are honored.

- [ ] **Step 1: Add a failing test**

In `core/src/pitr.rs`, inside the existing `#[cfg(test)] mod tests` block, add:
```rust
    #[test]
    fn parse_pg_timestamp_handles_common_formats() {
        assert_eq!(parse_pg_timestamp("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-02").unwrap(), 86_400);
        // RFC3339 with offset: 01:00+01:00 == 00:00 UTC == epoch.
        assert_eq!(parse_pg_timestamp("1970-01-01T01:00:00+01:00").unwrap(), 0);
        assert!(parse_pg_timestamp("not a timestamp").is_err());
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p core parse_pg_timestamp_handles_common_formats`
Expected: FAIL (function `parse_pg_timestamp` not found).

- [ ] **Step 3: Implement**

In `core/src/pitr.rs`, add this function at module scope (e.g. after `remove_recovery_conf`, before the backup helpers). Add `use chrono::{DateTime, NaiveDate, NaiveDateTime};` to the imports at the top of the file:
```rust
/// Parse a `--time` recovery-target string to a Unix timestamp (seconds).
///
/// Accepts RFC3339/ISO with an explicit offset (honored), or a bare
/// `YYYY-MM-DD[ T]HH:MM[:SS]` / `YYYY-MM-DD` which is interpreted as UTC. Used
/// only to compare a target against the recoverable window and to select the
/// base manifest; PostgreSQL re-parses `recovery_target_time` authoritatively
/// during replay.
pub fn parse_pg_timestamp(s: &str) -> Result<i64> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(ndt.and_utc().timestamp());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
    }
    Err(Error::other(format!(
        "could not parse --time '{s}'; use 'YYYY-MM-DD HH:MM:SS' or an RFC3339 timestamp"
    )))
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p core parse_pg_timestamp_handles_common_formats`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/src/pitr.rs
git commit -m "feat(core): add parse_pg_timestamp for window-based PITR targets"
```

---

### Task 3: `select_base_by_time` pure helper in `core/src/io/store.rs` (TDD)

**Files:** Modify `core/src/io/store.rs` (+ inline test)

A pure selector mirroring the existing `select_newest_base_at_or_before`: given `(timestamp, checkpoint, key)` candidates, pick the newest base at or before a target time on a given timeline.

- [ ] **Step 1: Add a failing test**

Append to the existing `#[cfg(test)] mod base_select_tests` block at the end of `core/src/io/store.rs`:
```rust
    #[test]
    fn selects_base_by_time_newest_before_target_on_timeline() {
        // (timestamp, checkpoint, key) candidates on timeline 1.
        let cands = vec![
            (100i64, ckpt(1, 0x1000), "k100".to_string()),
            (200i64, ckpt(1, 0x2000), "k200".to_string()),
            (300i64, ckpt(1, 0x3000), "k300".to_string()),
            // A base on a different timeline that must be ignored.
            (250i64, ckpt(2, 0x2500), "k250tl2".to_string()),
        ];
        let tl = TimelineId::new(1);

        // Target between 200 and 300 → pick the ts=200 base.
        let got = select_base_by_time(&cands, 250, tl).unwrap();
        assert_eq!(got, (ckpt(1, 0x2000), "k200".to_string()));

        // Exact match on a base timestamp → inclusive pick.
        let got = select_base_by_time(&cands, 200, tl).unwrap();
        assert_eq!(got.0, ckpt(1, 0x2000));

        // Target before the earliest base on this timeline → none.
        assert!(select_base_by_time(&cands, 50, tl).is_none());

        // The tl=2 base is never chosen for tl=1 even when its ts qualifies.
        let got = select_base_by_time(&cands, 260, tl).unwrap();
        assert_eq!(got.0, ckpt(1, 0x2000));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p core selects_base_by_time_newest_before_target_on_timeline`
Expected: FAIL (function `select_base_by_time` not found).

- [ ] **Step 3: Implement**

In `core/src/io/store.rs`, add at module scope right after `select_newest_base_at_or_before` (around line 55):
```rust
/// From `(timestamp, checkpoint, key)` candidates, select the newest base
/// manifest on `timeline` whose `timestamp <= target_ts`. Returns
/// `(checkpoint, key)`. Ordering uses `(timestamp, checkpoint)`; both are
/// monotonic, so this is the latest base at or before the target time.
fn select_base_by_time(
    candidates: &[(i64, Checkpoint, String)],
    target_ts: i64,
    timeline: TimelineId,
) -> Option<(Checkpoint, String)> {
    candidates
        .iter()
        .filter(|(ts, c, _)| *ts <= target_ts && c.timeline_id == timeline)
        .max_by_key(|(ts, c, _)| (*ts, *c))
        .map(|(_, c, k)| (*c, k.clone()))
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p core selects_base_by_time_newest_before_target_on_timeline`
Expected: PASS. (A `dead_code` warning for `select_base_by_time` under the non-test build is expected until Task 4 — do not suppress it.)

- [ ] **Step 5: Commit**

```bash
git add core/src/io/store.rs
git commit -m "feat(core): add select_base_by_time helper for window PITR"
```

---

### Task 4: `RecoveryWindow` + `Store::recovery_window` + `Store::load_base_pg_state_before_time`

**Files:** Modify `core/src/io/store.rs`

Public `Store` API the binary needs. These touch storage/`Store`, so they are build/integration-verified (the logic they rely on — `select_base_by_time`, parsing — is unit-tested in Tasks 2–3).

- [ ] **Step 1: Add the `RecoveryWindow` struct**

In `core/src/io/store.rs`, add at module scope just after the `CheckpointRow` struct (around line 65):
```rust
/// The recoverable time window reported by [`Store::recovery_window`].
///
/// `earliest` is the oldest retained base manifest (replay floor); `latest` is
/// the newest segment checkpoint. A PITR target must fall within `[earliest,
/// latest]`. Derived from base manifests + segments, both governed by
/// retention rather than by compaction.
#[derive(Debug, Clone)]
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint,
    pub latest_ts: i64,
    pub latest_ckpt: Checkpoint,
    pub timeline: TimelineId,
}
```

- [ ] **Step 2: Add the two `Store` methods**

Inside `impl Store { ... }`, just after `load_base_pg_state_at_or_before` (search for it), add:
```rust
    /// Compute the recoverable PITR window: `earliest` from the oldest retained
    /// base manifest, `latest`/`timeline` from the newest segment checkpoint.
    pub fn recovery_window(&self) -> Result<RecoveryWindow> {
        let prefix = self.lctr.bases_dir();
        let keys = self.storage_list_prefix(&prefix)?;
        let (_oldest_ckpt, oldest_key) = keys
            .iter()
            .filter_map(|k| parse_base_manifest_ckpt(k, &prefix).map(|c| (c, k.clone())))
            .min_by_key(|(c, _)| *c)
            .ok_or_else(|| Error::other("no base manifest found; nothing is recoverable yet"))?;

        let bytes = self.storage_get(&oldest_key)?;
        let tmp = tempfile::tempdir()?;
        let base = Manifest::from_bytes(&bytes, tmp.path())?;
        let earliest_ts = base.timestamp();
        let earliest_ckpt = base.checkpoint();

        let rows = self.list_checkpoints()?;
        let newest = rows
            .last()
            .ok_or_else(|| Error::other("no checkpoints found; nothing is recoverable yet"))?;

        Ok(RecoveryWindow {
            earliest_ts,
            earliest_ckpt,
            latest_ts: newest.created_at,
            latest_ckpt: newest.ckpt,
            timeline: newest.ckpt.timeline_id,
        })
    }

    /// Find the newest base manifest with `timestamp <= target_ts` on `timeline`
    /// and return its `(checkpoint, pg_state)`.
    ///
    /// Reads each base manifest's header to learn its timestamp (few base
    /// manifests; cold CLI path). Future optimization: range-GET the 48-byte
    /// TIKM header to avoid transferring the embedded `pg_state`.
    pub fn load_base_pg_state_before_time(
        &self,
        target_ts: i64,
        timeline: TimelineId,
    ) -> Result<(Checkpoint, Vec<u8>)> {
        let prefix = self.lctr.bases_dir();
        let keys = self.storage_list_prefix(&prefix)?;
        let tmp = tempfile::tempdir()?;

        let mut candidates: Vec<(i64, Checkpoint, String)> = Vec::new();
        for key in &keys {
            if parse_base_manifest_ckpt(key, &prefix).is_none() {
                continue;
            }
            let bytes = self.storage_get(key)?;
            let m = Manifest::from_bytes(&bytes, tmp.path())?;
            candidates.push((m.timestamp(), m.checkpoint(), key.clone()));
        }

        let (ckpt, key) = select_base_by_time(&candidates, target_ts, timeline).ok_or_else(|| {
            Error::other(format!(
                "no base manifest at or before time {target_ts} on timeline {timeline}"
            ))
        })?;

        let bytes = self.storage_get(&key)?;
        let manifest = Manifest::from_bytes(&bytes, tmp.path())?;
        Ok((ckpt, manifest.pg_state().to_vec()))
    }
```

- [ ] **Step 3: Build and test**

Run: `cargo build -p core -p cli && cargo test -p core`
Expected: clean build (the Task-3 `dead_code` warning is now resolved — `select_base_by_time` is used here); all tests pass.

- [ ] **Step 4: Commit**

```bash
git add core/src/io/store.rs
git commit -m "feat(core): add Store::recovery_window and load_base_pg_state_before_time"
```

---

### Task 5: Generalize the conf writer + switch the binary to window/time targets

**Files:** Modify `core/src/pitr.rs`, `cli/src/bin/tiko_pitr.rs`

This is the cohesive feature switch. It changes `write_pitr_recovery_conf`'s signature (so the binary must change with it in the same commit) and rewrites the binary's `list`/`recover`.

- [ ] **Step 1: Generalize the conf writer (core/src/pitr.rs)**

Replace the existing `write_pitr_recovery_conf` function (the `pub fn write_pitr_recovery_conf(conf_path, target_tl, target_lsn)` and its doc comment) with:
```rust
/// A PITR recovery target: stop replay at a specific LSN, or at a timestamp.
#[derive(Debug, Clone)]
pub enum RecoveryTarget {
    Lsn(Lsn),
    Time(String),
}

/// Append a Tiko PITR recovery block to `conf_path`, delimited by begin/end
/// markers so [`remove_recovery_conf`] can strip it cleanly later.
///
/// Drives archive recovery up to `target` on `timeline`, pulling WAL segments
/// from remote via `tiko_restore`. `recovery_target_action='shutdown'` makes
/// PostgreSQL shut itself down the instant it reaches the target.
///
/// Note: this function does **not** check for an existing block; callers should
/// call [`remove_recovery_conf`] first if the file may already contain one.
pub fn write_pitr_recovery_conf(
    conf_path: &Path,
    timeline: TimelineId,
    target: &RecoveryTarget,
) -> Result<()> {
    let target_line = match target {
        RecoveryTarget::Lsn(lsn) => format!("recovery_target_lsn = '{}'\n", lsn.to_pg_string()),
        RecoveryTarget::Time(ts) => format!("recovery_target_time = '{ts}'\n"),
    };
    let snippet = format!(
        "\n{begin}\
         restore_command = 'tiko_restore %f %p'\n\
         {target_line}\
         recovery_target_timeline = '{tl}'\n\
         recovery_target_inclusive = on\n\
         recovery_target_action = 'shutdown'\n\
         {end}",
        begin = RECOVERY_CONF_BEGIN,
        end = RECOVERY_CONF_END,
        tl = timeline.as_u32(),
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}
```

- [ ] **Step 2: Update the existing conf test + add a time-variant test (core/src/pitr.rs)**

In the `#[cfg(test)] mod tests` block, replace the existing `pitr_conf_round_trips_through_remove` test body's `write_pitr_recovery_conf` call and assertions with the LSN-variant call, and add a time-variant test:
```rust
    #[test]
    fn pitr_conf_round_trips_through_remove() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(TIKO_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        write_pitr_recovery_conf(
            &conf,
            TimelineId::new(2),
            &RecoveryTarget::Lsn(Lsn::new(0x3000028)),
        )
        .unwrap();
        let with = fs::read_to_string(&conf).unwrap();
        assert!(with.contains("restore_command = 'tiko_restore %f %p'"));
        assert!(with.contains("recovery_target_lsn = '0/3000028'"));
        assert!(with.contains("recovery_target_timeline = '2'"));
        assert!(with.contains("recovery_target_action = 'shutdown'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }

    #[test]
    fn pitr_conf_time_target_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(TIKO_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        write_pitr_recovery_conf(
            &conf,
            TimelineId::new(1),
            &RecoveryTarget::Time("2026-06-04 10:00:00".to_string()),
        )
        .unwrap();
        let with = fs::read_to_string(&conf).unwrap();
        assert!(with.contains("recovery_target_time = '2026-06-04 10:00:00'"));
        assert!(!with.contains("recovery_target_lsn"));
        assert!(with.contains("recovery_target_timeline = '1'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }
```

- [ ] **Step 3: Verify core tests pass**

Run: `cargo test -p core`
Expected: PASS (including both conf tests).

- [ ] **Step 4: Rewrite the binary's args, `list`, `recover`, and `recover_inner` (cli/src/bin/tiko_pitr.rs)**

(a) Update the module doc comment lines 3-8 to:
```rust
//! Two subcommands:
//!   * `list` — print the recoverable time window from remote.
//!   * `recover (--time <TS> | --lsn <LSN>) [--timeline <HEX>]` — stop the
//!     instance, snapshot PGDATA (excluding `tiko/`), recover to the target
//!     point in the window, then restart normally. On failure, PGDATA is
//!     restored from the snapshot and the instance is left stopped.
```

(b) Replace the `Cmd::List` doc and the entire `RecoverArgs` struct (lines 40-64) with:
```rust
    /// Print the recoverable time window on remote.
    List,
    /// Recover the instance to a point in the window, then restart normally.
    Recover(RecoverArgs),
}

#[derive(Args)]
#[command(group(
    clap::ArgGroup::new("target").required(true).args(["time", "lsn"])
))]
struct RecoverArgs {
    /// Target time, e.g. `'2026-06-04 10:00:00'` or RFC3339 (mutually
    /// exclusive with --lsn).
    #[arg(long)]
    time: Option<String>,
    /// Target LSN, PostgreSQL `X/Y` or hex (mutually exclusive with --time).
    #[arg(long)]
    lsn: Option<String>,
    /// Target timeline id in hex (e.g. `00000001`). Defaults to the window's
    /// latest timeline.
    #[arg(long)]
    timeline: Option<String>,
    /// PostgreSQL data directory. Defaults to `$PGDATA`.
    #[arg(long, env = "PGDATA")]
    pgdata: PathBuf,
    /// Path to `pg_ctl`. Defaults to `pg_ctl` on `PATH`.
    #[arg(long, default_value = "pg_ctl")]
    pg_ctl: PathBuf,
    /// Path to the `postgres` server binary. Defaults to the sibling of
    /// `--pg-ctl`, falling back to `postgres` on `PATH`.
    #[arg(long)]
    postgres: Option<PathBuf>,
}
```

(c) Replace the entire `run_list` function with:
```rust
fn run_list(store: &Store) -> Result<()> {
    let w = store.recovery_window()?;
    let fmt_ts = |ts: i64| {
        DateTime::<Utc>::from_timestamp(ts, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| ts.to_string())
    };
    println!("recoverable window:");
    println!("  earliest: {}   (checkpoint {})", fmt_ts(w.earliest_ts), w.earliest_ckpt);
    println!("  latest:   {}   (checkpoint {})", fmt_ts(w.latest_ts), w.latest_ckpt);
    println!("  timeline: {}", w.timeline.to_hex());
    Ok(())
}
```

(d) Replace the entire `run_recover` function with:
```rust
fn run_recover(store: &Store, args: &RecoverArgs) -> Result<()> {
    // 1. Determine the recoverable window and resolve the target timeline.
    let window = store.recovery_window()?;
    let timeline = match &args.timeline {
        Some(s) => TimelineId::from_hex(s)
            .map_err(|e| Error::other(format!("invalid --timeline '{s}': {e}")))?,
        None => window.timeline,
    };

    // 2. Resolve the target (time or lsn), validate it is within the window,
    //    and select the base pg_state to recover from. clap guarantees exactly
    //    one of --time / --lsn is set.
    let (base_ckpt, pg_state, target, target_label) = if let Some(time_str) = &args.time {
        let target_ts = pitr::parse_pg_timestamp(time_str)?;
        if target_ts < window.earliest_ts || target_ts > window.latest_ts {
            return Err(Error::other(format!(
                "target time '{time_str}' is outside the recoverable window; run `tiko_pitr list`"
            )));
        }
        let (bc, pg) = store.load_base_pg_state_before_time(target_ts, timeline)?;
        (bc, pg, pitr::RecoveryTarget::Time(time_str.clone()), format!("time '{time_str}'"))
    } else {
        let l = Lsn::parse_either(args.lsn.as_ref().unwrap()).map_err(Error::other)?;
        // LSN bounds use the window's latest-timeline range; base selection +
        // PostgreSQL validate the precise reachability for an older --timeline.
        if l < window.earliest_ckpt.lsn || l > window.latest_ckpt.lsn {
            return Err(Error::other(format!(
                "target LSN {} is outside the recoverable window; run `tiko_pitr list`",
                l.to_pg_string()
            )));
        }
        let (bc, pg) = store.load_base_pg_state_at_or_before(Checkpoint::new(timeline, l))?;
        (bc, pg, pitr::RecoveryTarget::Lsn(l), format!("lsn {}", l.to_pg_string()))
    };
    eprintln!("tiko_pitr: recovering to {target_label} on timeline {} from base checkpoint {base_ckpt}", timeline.to_hex());

    let pgdata = args.pgdata.as_path();
    let pg_ctl = args.pg_ctl.as_path();
    let postgres = args
        .postgres
        .clone()
        .unwrap_or_else(|| sibling_postgres(pg_ctl));
    let conf = pgdata.join(pitr::TIKO_CONF_FILE);
    let backup = backup_path(pgdata);

    // 3. Stop PostgreSQL so the data dir is quiesced before copy/mutation.
    stop_pg(pg_ctl, pgdata)?;

    // 4. Snapshot PGDATA (excluding the bulk `tiko/` dir).
    pitr::backup_dir_excluding(pgdata, &backup, "tiko")?;

    // 5. Mutate + run recovery. On any failure, restore from the snapshot.
    match recover_inner(&conf, pgdata, &pg_state, timeline, &target, &postgres) {
        Ok(()) => {
            pitr::remove_recovery_conf(&conf)?;
            let _ = std::fs::remove_file(pgdata.join("recovery.signal"));
            std::fs::remove_dir_all(&backup)?;
            start_pg(pg_ctl, pgdata)?;
            eprintln!("tiko_pitr: recovery to {target_label} complete; database restarted");
            Ok(())
        }
        Err(e) => {
            eprintln!("tiko_pitr: recovery failed: {e}");
            eprintln!("tiko_pitr: restoring PGDATA from backup {}", backup.display());
            // Best-effort stop before restoring. The foreground `postgres` run
            // has already exited by the time we reach this arm, so PG is
            // normally down already; this just guards against a stray process.
            // We ignore the result and proceed to restore regardless.
            let _ = stop_pg(pg_ctl, pgdata);
            if let Err(re) = pitr::restore_dir(&backup, pgdata, "tiko") {
                eprintln!(
                    "tiko_pitr: RESTORE FAILED ({re}); backup left in place at {}",
                    backup.display()
                );
                return Err(re);
            }
            std::fs::remove_dir_all(&backup)?;
            eprintln!("tiko_pitr: PGDATA restored; database left stopped");
            Err(e)
        }
    }
}
```

(e) Replace the `recover_inner` function signature and its `write_pitr_recovery_conf` call. Change the signature line and the conf-write line:
```rust
fn recover_inner(
    conf: &Path,
    pgdata: &Path,
    pg_state: &[u8],
    timeline: TimelineId,
    target: &pitr::RecoveryTarget,
    postgres: &Path,
) -> Result<()> {
    extract_pg_state(pg_state, pgdata)?;
    pitr::write_pitr_recovery_conf(conf, timeline, target)?;
    std::fs::write(pgdata.join("recovery.signal"), b"")?;
```
(Leave the rest of `recover_inner` — the foreground `postgres` run and exit-status check — unchanged.)

- [ ] **Step 5: Build the binary and smoke-test**

Run:
```bash
cargo build -p core -p cli
cargo run -p cli --bin tiko_pitr -- recover --help
```
Expected: clean build; `recover --help` shows `--time`, `--lsn`, `--timeline`, `--pgdata`, `--pg-ctl`, `--postgres`, and indicates `--time`/`--lsn` are part of a required mutually-exclusive group (supplying neither, or both, errors).

- [ ] **Step 6: Verify the arg group is enforced**

Run:
```bash
cargo run -p cli --bin tiko_pitr -- recover --pgdata /tmp/x ; echo "exit=$?"
cargo run -p cli --bin tiko_pitr -- recover --time 'x' --lsn 0/1 --pgdata /tmp/x ; echo "exit=$?"
```
Expected: both fail with a clap error (neither/both target args) and non-zero exit — before any store/PGDATA work.

- [ ] **Step 7: Commit**

```bash
git add core/src/pitr.rs cli/src/bin/tiko_pitr.rs
git commit -m "feat(cli): window-based PITR targets (--time/--lsn) in tiko_pitr"
```

---

### Task 6: Full build + test + gate

**Files:** none (verification only)

- [ ] **Step 1: Build**

Run: `cargo build -p core -p cli`
Expected: clean, no warnings from changed files.

- [ ] **Step 2: Test**

Run: `cargo test -p core`
Expected: all pass, including `parse_pg_timestamp_handles_common_formats`, `selects_base_by_time_newest_before_target_on_timeline`, `pitr_conf_round_trips_through_remove`, `pitr_conf_time_target_round_trips`, and the existing `base_select_tests`.

- [ ] **Step 3: Confirm no warnings introduced**

Run: `cargo build -p core -p cli 2>&1 | grep -c warning`
Expected: `0` (clippy is blocked by pre-existing `pgsys` lint errors — unrelated; verify cleanliness via the warning-free build).

---

## Notes for the implementer

- **Integration testing (out of band):** the `recover` flow (pg_ctl/postgres/tar + real PITR to a time/LSN) is verified separately against a live instance, per project convention — not by automated tests here.
- **Compaction-stability is the point:** the window comes from base manifests (oldest) + the newest segment checkpoint, neither of which compaction deletes out from under a recoverable target. Do not source the window or target validation from the full segment list.
- **`--time` timezone:** bare timestamps are interpreted as UTC for window comparison; the raw string is passed to `recovery_target_time` for PostgreSQL's authoritative parse. This is intentional (see spec).
- **LSN bounds with an older `--timeline`:** the `--lsn` window check uses the latest-timeline LSN range; base selection (`load_base_pg_state_at_or_before`) and PostgreSQL enforce precise reachability. This approximation is acceptable for v1 (single-timeline is the common case).
```
