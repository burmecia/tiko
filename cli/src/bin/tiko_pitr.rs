//! `tiko_pitr` — automate Tiko point-in-time recovery (PITR).
//!
//! Three subcommands:
//!   * `list` — list available base backups (with timestamps) and the
//!     recoverable window.
//!   * `backup` — run `pg_basebackup` against the live instance and upload the
//!     (small) base backup to Tiko storage under `backup/`. The basebackup
//!     checkpoint (`CHECKPOINT_CAUSE_BASEBACKUP`) makes the checkpointer form a
//!     base manifest at the checkpoint LSN, so the tarball is paired with the
//!     chunk-ref map at the same LSN.
//!   * `recover (--time <TS> | --lsn <LSN>) [--timeline <HEX>]` — restore the
//!     latest backup at/before the target, install its base manifest, replay
//!     WAL to the target, then promote. On failure, PGDATA + the prior base
//!     manifest are restored and the instance is left stopped.
//!
//! **Output model:** every subcommand emits a single JSON object on stdout
//! (pretty-printed) so HTTP consumers (tikoguest's `/pitr/*` routes) can parse
//! the result directly instead of screen-scraping. Human-readable progress and
//! diagnostics go to stderr. On any failure the stdout object is
//! `{"error":{"message":"..."}}` and the process exits non-zero.
//!
//! Storage is configured from the environment exactly as `tiko_restore`
//! expects (`Store::init()`): `TIKO_STORAGE_ROOT`/`TIKO_LOCAL_PATH`/`PGDATA`,
//! `TIKO_ORG_ID`, `TIKO_DB_ID`, `TIKO_PROJECT_ID`.

use std::path::{Path, PathBuf};
use std::process::exit;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use core::error::{Error, Result};
use core::io::store::Store;
use core::io::timeline::Checkpoint;
use core::pitr;
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

/// On-disk filename of the live base manifest cache (mirror of the constant in
/// `core::manifest`). The recovering smgr reads chunk refs from this file.
const BASE_MANIFEST_FILE_NAME: &str = "base_manifest.tikm";

// Standalone process (not loaded into the postmaster); `cli::pg_stubs` supplies
// the PG symbols that `core` transitively references. See `tiko_restore`.
extern crate cli;

#[derive(Parser)]
#[command(name = "tiko_pitr", about = "Automate Tiko point-in-time recovery")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List available base backups (with timestamps) and the recoverable window.
    List,
    /// Take a base backup (`pg_basebackup`) and upload it to Tiko storage.
    Backup(BackupArgs),
    /// Recover the instance to a point in the window, then restart normally.
    Recover(RecoverArgs),
}

#[derive(Args)]
struct BackupArgs {
    /// Host to connect to (`pg_basebackup -h`). Empty = local unix socket.
    #[arg(long, env = "PGHOST", default_value = "")]
    host: String,
    /// Port to connect to (`pg_basebackup -p`).
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    port: u16,
    /// User to connect as (`pg_basebackup -U`). Defaults to the current OS
    /// user (pg_basebackup's own default).
    #[arg(long, env = "PGUSER")]
    user: Option<String>,
    /// Path to the `pg_basebackup` binary. Defaults to `pg_basebackup` on PATH.
    #[arg(long, default_value = "pg_basebackup")]
    pg_basebackup: PathBuf,
    /// Checkpoint mode passed to `pg_basebackup -c`: `fast` or `spread`.
    #[arg(long, default_value = "fast")]
    checkpoint: String,
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
    /// Port the recovering PostgreSQL listens on (used to poll for promotion).
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    port: u16,
    /// Path to `psql`, used to poll for recovery completion (promotion).
    /// Defaults to the sibling of `--pg-ctl`, falling back to `psql` on PATH.
    #[arg(long)]
    psql: Option<PathBuf>,
    /// Path to the `tiko_restore` binary used as PostgreSQL's `restore_command`.
    /// Defaults to the sibling of this `tiko_pitr` executable, falling back to
    /// `tiko_restore` on `PATH`.
    #[arg(long)]
    tiko_restore: Option<PathBuf>,
    /// Seconds to wait for PostgreSQL to reach the recovery target and
    /// promote before declaring failure.
    #[arg(long, default_value_t = 300)]
    recovery_timeout: u64,
}

// ── JSON output DTOs ─────────────────────────────────────────────────────────
//
// The CLI emits one of these on stdout for each subcommand. Fields use the
// human-readable PostgreSQL forms (RFC3339 timestamps, `X/Y` LSNs, hex
// timeline ids) so consumers can display them without reformatting.

/// `list` response: every base backup (newest-first) plus the recoverable
/// window. `window` is `null` (and `window_error` set) when archived WAL
/// coverage isn't available yet.
#[derive(Serialize)]
struct ListOutput {
    backups: Vec<BackupDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    window: Option<WindowDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    window_error: Option<String>,
}

#[derive(Serialize)]
struct BackupDto {
    /// RFC3339 creation timestamp.
    created_at: String,
    /// Fixed-width hex timeline id (e.g. `"00000001"`).
    timeline: String,
    /// PostgreSQL `X/Y` checkpoint LSN the backup was taken at.
    checkpoint_lsn: String,
    /// PostgreSQL `X/Y` REDO LSN (where WAL replay starts).
    redo_lsn: String,
}

#[derive(Serialize)]
struct WindowDto {
    timeline: String,
    /// RFC3339 lower bound of the recoverable window.
    earliest_ts: String,
    /// RFC3339 upper bound (WAL head).
    latest_ts: String,
    /// PostgreSQL `X/Y` earliest recoverable LSN.
    earliest_lsn: String,
    /// PostgreSQL `X/Y` latest recoverable LSN.
    latest_lsn: String,
}

/// `backup` response: coordinates of the just-uploaded base backup.
#[derive(Serialize)]
struct BackupOutput {
    timeline: String,
    checkpoint_lsn: String,
    redo_lsn: String,
    created_at: String,
    /// Compressed tarball size in bytes.
    bytes_compressed: usize,
}

/// `recover` response: where recovery landed.
#[derive(Serialize)]
struct RecoverOutput {
    /// Always `"recovered"` (only emitted on success).
    status: String,
    /// `"time"` or `"lsn"` — which target form was used.
    target_kind: String,
    /// RFC3339 timestamp (time target) or PostgreSQL `X/Y` (lsn target).
    target_value: String,
    timeline: String,
    /// Checkpoint LSN of the base backup recovered from.
    base_checkpoint_lsn: String,
}

/// Render a Unix-seconds timestamp as RFC3339 UTC, falling back to the raw
/// integer if the instant is out of `chrono`'s representable range.
fn fmt_unix_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}

/// Pretty-print a DTO as JSON on stdout.
fn print_json<T: Serialize>(value: &T) -> Result<()> {
    let s = serde_json::to_string_pretty(value)
        .map_err(|e| Error::other(format!("json serialize failed: {e}")))?;
    println!("{s}");
    Ok(())
}

fn run_list(store: &Store) -> Result<()> {
    // All base backups, newest-first (across every timeline).
    let mut backups = store.list_backups()?;
    backups.sort_by(|a, b| b.ckpt.cmp(&a.ckpt));
    let backups_dto: Vec<BackupDto> = backups
        .iter()
        .map(|b| BackupDto {
            created_at: fmt_unix_ts(b.created_at),
            timeline: b.ckpt.timeline_id.to_hex(),
            checkpoint_lsn: b.ckpt.lsn.to_pg_string(),
            redo_lsn: b.redo_ckpt.lsn.to_pg_string(),
        })
        .collect();

    // The single recoverable window [earliest backup, WAL head]. Best-effort:
    // `list` still shows the backups above even when WAL coverage isn't
    // available yet (e.g. right after a backup, before the WAL tail archives).
    let (window, window_error) = match store.recovery_window() {
        Ok(w) => (
            Some(WindowDto {
                timeline: w.timeline.to_hex(),
                earliest_ts: fmt_unix_ts(w.earliest_ts),
                latest_ts: fmt_unix_ts(w.latest_ts),
                earliest_lsn: w.earliest_ckpt.lsn.to_pg_string(),
                latest_lsn: w.latest_lsn.to_pg_string(),
            }),
            None,
        ),
        Err(e) => (None, Some(e.to_string())),
    };

    print_json(&ListOutput {
        backups: backups_dto,
        window,
        window_error,
    })
}

// ── backup ───────────────────────────────────────────────────────────────────

/// Run `pg_basebackup` against the live instance and upload the base backup to
/// Tiko storage under the `backup/` prefix. The basebackup checkpoint
/// (`CHECKPOINT_CAUSE_BASEBACKUP`) makes the checkpointer form a base manifest
/// at the checkpoint LSN, so the uploaded tarball is paired with the chunk-ref
/// map at the same LSN for later recovery.
fn run_backup(store: &Store, args: &BackupArgs) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let backup_dir = tmp.path();

    // 1. Run pg_basebackup. `-X none`: WAL is archived separately by the Tiko
    //    worker. `-c fast`: forces the basebackup checkpoint that the Tiko
    //    checkpointer hooks to run compaction (forming the base manifest).
    cli::pgops::run_pg_basebackup(
        &cli::pgops::BasebackupOpts {
            pg_basebackup: &args.pg_basebackup,
            host: &args.host,
            port: args.port,
            user: args.user.as_deref(),
            checkpoint: &args.checkpoint,
            wal_method: "none",
        },
        backup_dir,
    )?;

    // 2. Parse backup_label: CHECKPOINT LOCATION == the base manifest key
    //    (both are ControlFile->checkPoint == ProcLastRecPtr at the checkpoint
    //    that ran at backup start). START WAL LOCATION is the redo point.
    let label_path = backup_dir.join("backup_label");
    let label = std::fs::read_to_string(&label_path)
        .map_err(|e| Error::other(format!("read {}: {e}", label_path.display())))?;
    let (checkpoint_lsn, redo_lsn, timeline) = cli::pgops::parse_backup_label(&label)?;
    let ckpt = Checkpoint::new(timeline, checkpoint_lsn);
    let redo_ckpt = Checkpoint::new(timeline, redo_lsn);
    eprintln!("tiko_pitr: base backup at checkpoint {ckpt}, redo {redo_ckpt}");

    // 3. Pack the backup directory (tar + zstd).
    let tar_zst = cli::pgops::tar_dir_to_zst(backup_dir)?;

    // 4. Upload the tarball + metadata sidecar.
    let created_at = chrono::Utc::now().timestamp();
    store.put_backup(ckpt, redo_ckpt, created_at, &tar_zst)?;
    eprintln!(
        "tiko_pitr: uploaded base backup at {ckpt} ({} bytes compressed)",
        tar_zst.len()
    );

    print_json(&BackupOutput {
        timeline: ckpt.timeline_id.to_hex(),
        checkpoint_lsn: ckpt.lsn.to_pg_string(),
        redo_lsn: redo_ckpt.lsn.to_pg_string(),
        created_at: fmt_unix_ts(created_at),
        bytes_compressed: tar_zst.len(),
    })
}

fn run_recover(store: &Store, args: &RecoverArgs) -> Result<()> {
    // 1. Recoverable window + target timeline.
    let window = store.recovery_window()?;
    let timeline = match &args.timeline {
        Some(s) => TimelineId::from_hex(s)
            .map_err(|e| Error::other(format!("invalid --timeline '{s}': {e}")))?,
        None => window.timeline,
    };

    // 2. Resolve the target (time or lsn), validate it is within the window,
    //    and select the base backup to recover from — the latest backup at or
    //    before the target (standard PITR: minimises WAL replay). clap
    //    guarantees exactly one of --time / --lsn is set.
    let (base_ckpt, tar_bytes, target, target_kind, target_value) = if let Some(time_str) = &args.time {
        let target_ts = pitr::parse_pg_timestamp(time_str)?;
        if target_ts < window.earliest_ts || target_ts > window.latest_ts {
            return Err(Error::other(format!(
                "target time '{time_str}' is outside the recoverable window; run `tiko_pitr list`"
            )));
        }
        let (bc, bytes) = store.load_backup_before_time(target_ts, timeline)?;
        // Carry the parsed UTC instant (not the raw string) so PostgreSQL's
        // recovery_target_time is rendered as explicit UTC and can't be
        // reinterpreted in the server timezone.
        (
            bc,
            bytes,
            pitr::RecoveryTarget::Time(target_ts),
            "time",
            fmt_unix_ts(target_ts),
        )
    } else {
        let l = Lsn::parse_either(args.lsn.as_ref().unwrap()).map_err(Error::other)?;
        // LSN bounds use the window's latest-timeline range; backup selection +
        // PostgreSQL validate the precise reachability for an older --timeline.
        if l < window.earliest_ckpt.lsn || l > window.latest_lsn {
            return Err(Error::other(format!(
                "target LSN {} is outside the recoverable window; run `tiko_pitr list`",
                l.to_pg_string()
            )));
        }
        let (bc, bytes) = store.load_backup_at_or_before(Checkpoint::new(timeline, l))?;
        (
            bc,
            bytes,
            pitr::RecoveryTarget::Lsn(l),
            "lsn",
            l.to_pg_string(),
        )
    };
    let target_label = format!("{target_kind} {target_value}");
    eprintln!(
        "tiko_pitr: recovering to {target_label} on timeline {} from base backup {base_ckpt}",
        timeline
    );

    let pgdata = args.pgdata.as_path();
    let pg_ctl = args.pg_ctl.as_path();
    let psql = args
        .psql
        .clone()
        .unwrap_or_else(|| cli::pgops::sibling_binary(pg_ctl, "psql"));
    let tiko_restore = args
        .tiko_restore
        .clone()
        .unwrap_or_else(default_tiko_restore);
    let conf = pgdata.join(pitr::RECOVERY_CONF_FILE);
    let backup = backup_path(pgdata);

    // The live base manifest lives under the tiko root (excluded from the
    // PGDATA snapshot). Snapshot it separately so a failed recovery can restore
    // the manifest that matches the rolled-back PGDATA.
    let manifest_path = core::local_path().join(BASE_MANIFEST_FILE_NAME);
    let rollback = tempfile::tempdir()?;
    let manifest_backup = rollback.path().join(BASE_MANIFEST_FILE_NAME);

    // 3. Stop PostgreSQL so the data dir is quiesced before mutation.
    cli::pgops::stop_pg(pg_ctl, pgdata)?;

    // 4. Snapshot PGDATA (excluding the bulk `tiko/` dir) + the live manifest.
    pitr::backup_dir_excluding(pgdata, &backup, "tiko")?;
    if manifest_path.exists() {
        std::fs::copy(&manifest_path, &manifest_backup)?;
    }

    // 5. Wipe PGDATA, restore the base backup, install its base manifest, and
    //    run recovery. On any failure, roll PGDATA + the manifest back.
    let outcome = recover_inner(
        store,
        &conf,
        pgdata,
        &tar_bytes,
        base_ckpt,
        timeline,
        &target,
        pg_ctl,
        args.port,
        &psql,
        &tiko_restore,
        args.recovery_timeout,
    );
    match outcome {
        Ok(()) => {
            pitr::remove_recovery_conf(&conf)?;
            let _ = std::fs::remove_file(pgdata.join("recovery.signal"));
            let _ = std::fs::remove_dir_all(&backup);
            eprintln!(
                "tiko_pitr: recovery to {target_label} complete; database promoted and running"
            );
            print_json(&RecoverOutput {
                status: "recovered".to_string(),
                target_kind: target_kind.to_string(),
                target_value: target_value.clone(),
                timeline: timeline.to_hex(),
                base_checkpoint_lsn: base_ckpt.lsn.to_pg_string(),
            })
        }
        Err(e) => {
            eprintln!("tiko_pitr: recovery failed: {e}");
            eprintln!("tiko_pitr: restoring PGDATA + base manifest from rollback snapshot");
            // Best-effort stop before restoring; ignore the result.
            let _ = cli::pgops::stop_pg(pg_ctl, pgdata);
            if let Err(re) = pitr::restore_dir(&backup, pgdata, "tiko") {
                eprintln!(
                    "tiko_pitr: PGDATA RESTORE FAILED ({re}); backup left in place at {}",
                    backup.display()
                );
                return Err(re);
            }
            if manifest_backup.exists() {
                let _ = std::fs::copy(&manifest_backup, &manifest_path);
            }
            let _ = std::fs::remove_dir_all(&backup);
            eprintln!("tiko_pitr: PGDATA + manifest restored; database left stopped");
            Err(e)
        }
    }
}

/// Wipe PGDATA (keeping `tiko/`), extract the base backup tarball, install the
/// base manifest at `base_ckpt`, write the recovery conf, start PostgreSQL,
/// and wait for promotion. Returns `Ok` only if PostgreSQL reached the target
/// and promoted within the timeout.
fn recover_inner(
    store: &Store,
    conf: &Path,
    pgdata: &Path,
    tar_bytes: &[u8],
    base_ckpt: Checkpoint,
    timeline: TimelineId,
    target: &pitr::RecoveryTarget,
    pg_ctl: &Path,
    port: u16,
    psql: &Path,
    tiko_restore: &Path,
    recovery_timeout: u64,
) -> Result<()> {
    // Wipe everything except `tiko/` and replace with the base backup. The
    // tarball already carries a real `backup_label` from pg_basebackup, so no
    // pg_control shaping is needed.
    pitr::wipe_dir_excluding(pgdata, "tiko")?;
    cli::pgops::extract_backup(tar_bytes, pgdata)?;

    // Install the base manifest valid at the backup checkpoint as the live
    // `$TIKO_ROOT/base_manifest.tikm`. This closes the cross-LSN cache-miss
    // gap: the recovering smgr resolves chunk refs at L_b, not the newest base.
    store.materialize_base_manifest_at(base_ckpt)?;

    // Delete the pre-recovery timeline segments. They hold the OLD timeline's
    // history above the backup LSN; leaving them would let post-promote reads
    // resolve "future" chunk versions via the active window / segment scan
    // (for chunks that were evicted/drafted on the old timeline). The recovered
    // instance anchors on the base manifest (+ WAL replay, then the new
    // timeline's segments after promote).
    store.delete_all_segments()?;

    pitr::write_pitr_recovery_conf(conf, timeline, target, tiko_restore, true)?;
    std::fs::write(pgdata.join("recovery.signal"), b"")?;

    // Start in the background and poll until promotion. With
    // recovery_target_action='promote', postgres does not exit on its own; it
    // ends recovery by promoting and continuing as a primary.
    cli::pgops::start_pg(pg_ctl, pgdata)?;
    if let Err(e) = cli::pgops::wait_for_promotion(psql, port, recovery_timeout) {
        let _ = cli::pgops::stop_pg(pg_ctl, pgdata);
        return Err(e);
    }
    Ok(())
}

/// Default `tiko_restore` path: the sibling of this `tiko_pitr` executable
/// (they are built/installed together), falling back to `tiko_restore` on
/// `PATH` if the current exe path can't be resolved.
fn default_tiko_restore() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("tiko_restore")))
        .unwrap_or_else(|| PathBuf::from("tiko_restore"))
}

/// Sibling backup dir path: `{pgdata}.tiko_pitr_bak`.
fn backup_path(pgdata: &Path) -> PathBuf {
    let mut s = pgdata.as_os_str().to_os_string();
    s.push(".tiko_pitr_bak");
    PathBuf::from(s)
}

fn main() {
    let cli = Cli::parse();

    let res = (|| -> Result<()> {
        let store = Store::init()?;
        match &cli.command {
            Cmd::List => run_list(&store)?,
            Cmd::Backup(args) => run_backup(&store, args)?,
            Cmd::Recover(args) => run_recover(&store, args)?,
        }
        Ok(())
    })();

    if let Err(e) = res {
        // Emit a structured JSON error on stdout (so HTTP consumers can parse
        // the reason directly) and a human-readable line on stderr. Exit
        // non-zero so tikoguest's run_pitr maps this to a 5xx.
        let body = serde_json::json!({ "error": { "message": e.to_string() } });
        let stdout = serde_json::to_string(&body)
            .unwrap_or_else(|_| r#"{"error":{"message":"unknown error"}}"#.to_string());
        println!("{stdout}");
        eprintln!("tiko_pitr: {e}");
        exit(1);
    }
}
