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
//! Storage is configured from the environment exactly as `tiko_restore`
//! expects (`Store::init()`): `TIKO_ROOT_PATH`/`PGDATA`, `TIKO_ORG_ID`,
//! `TIKO_DB_ID`, `TIKO_PROJECT_ID`.

use std::path::{Path, PathBuf};
use std::process::{Command, exit};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};

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

fn run_list(store: &Store) -> Result<()> {
    let fmt_ts = |ts: i64| {
        DateTime::<Utc>::from_timestamp(ts, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| ts.to_string())
    };

    // All base backups, newest-first (across every timeline).
    let mut backups = store.list_backups()?;
    if backups.is_empty() {
        println!("no base backups found");
        return Ok(());
    }
    backups.sort_by(|a, b| b.ckpt.cmp(&a.ckpt));
    println!("base backups ({}):", backups.len());
    for b in &backups {
        println!(
            "  {}  timeline {}  checkpoint {}  redo {}",
            fmt_ts(b.created_at),
            b.ckpt.timeline_id.to_hex(),
            b.ckpt.lsn.to_pg_string(),
            b.redo_ckpt.lsn.to_pg_string(),
        );
    }

    // The single recoverable window [earliest backup, WAL head]. Best-effort:
    // `list` still shows the backups above even when WAL coverage isn't
    // available yet (e.g. right after a backup, before the WAL tail archives).
    match store.recovery_window() {
        Ok(w) => println!(
            "\nrecoverable window (timeline {}): {} .. {}\n  lsn {} .. {}",
            w.timeline.to_hex(),
            fmt_ts(w.earliest_ts),
            fmt_ts(w.latest_ts),
            w.earliest_ckpt.lsn.to_pg_string(),
            w.latest_lsn.to_pg_string(),
        ),
        Err(e) => println!("\nrecoverable window unavailable: {e}"),
    }
    Ok(())
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
    run_pg_basebackup(args, backup_dir)?;

    // 2. Parse backup_label: CHECKPOINT LOCATION == the base manifest key
    //    (both are ControlFile->checkPoint == ProcLastRecPtr at the checkpoint
    //    that ran at backup start). START WAL LOCATION is the redo point.
    let label_path = backup_dir.join("backup_label");
    let label = std::fs::read_to_string(&label_path)
        .map_err(|e| Error::other(format!("read {}: {e}", label_path.display())))?;
    let (checkpoint_lsn, redo_lsn, timeline) = parse_backup_label(&label)?;
    let ckpt = Checkpoint::new(timeline, checkpoint_lsn);
    let redo_ckpt = Checkpoint::new(timeline, redo_lsn);
    eprintln!("tiko_pitr: base backup at checkpoint {ckpt}, redo {redo_ckpt}");

    // 3. Pack the backup directory (tar + zstd).
    let tar_zst = tar_dir_to_zst(backup_dir)?;

    // 4. Upload the tarball + metadata sidecar.
    let created_at = chrono::Utc::now().timestamp();
    store.put_backup(ckpt, redo_ckpt, created_at, &tar_zst)?;
    eprintln!(
        "tiko_pitr: uploaded base backup at {ckpt} ({} bytes compressed)",
        tar_zst.len()
    );
    Ok(())
}

/// Invoke `pg_basebackup` to produce a plain-format base backup in `dest`.
fn run_pg_basebackup(args: &BackupArgs, dest: &Path) -> Result<()> {
    let mut cmd = Command::new(&args.pg_basebackup);
    cmd.arg("-D").arg(dest);
    cmd.args(["-X", "none"]); // WAL archived separately by Tiko.
    cmd.args(["-F", "p"]); // plain format (directory)
    cmd.args(["-c", &args.checkpoint]); // triggers CHECKPOINT_CAUSE_BASEBACKUP
    cmd.args(["--no-password", "--no-manifest"]);
    if !args.host.is_empty() {
        cmd.args(["-h", &args.host]);
    }
    cmd.args(["-p", &args.port.to_string()]);
    if let Some(user) = &args.user {
        cmd.args(["-U", user]);
    }
    let status = cmd
        .status()
        .map_err(|e| Error::other(format!("failed to spawn pg_basebackup: {e}")))?;
    if !status.success() {
        return Err(Error::other(format!(
            "pg_basebackup failed (exit: {status})"
        )));
    }
    Ok(())
}

/// Parse `backup_label` into `(checkpoint_lsn, redo_lsn, timeline)`.
///
/// Relevant lines (see `build_backup_content` in `xlogbackup.c`):
///   `START WAL LOCATION: X/Y (file ...)`   ← redo point
///   `CHECKPOINT LOCATION: X/Y`             ← checkpoint record LSN (base key)
///   `START TIMELINE: N`                     ← timeline id (decimal)
fn parse_backup_label(label: &str) -> Result<(Lsn, Lsn, TimelineId)> {
    let checkpoint_lsn = parse_label_lsn(label, "CHECKPOINT LOCATION:")?;
    let redo_lsn = parse_label_lsn(label, "START WAL LOCATION:")?;
    let timeline = parse_label_tli(label, "START TIMELINE:")?;
    Ok((checkpoint_lsn, redo_lsn, timeline))
}

fn parse_label_lsn(label: &str, prefix: &str) -> Result<Lsn> {
    let token = first_token_after(label, prefix)
        .ok_or_else(|| Error::other(format!("backup_label missing '{prefix}' line")))?;
    Lsn::parse_either(token).map_err(Error::other)
}

fn parse_label_tli(label: &str, prefix: &str) -> Result<TimelineId> {
    let token = first_token_after(label, prefix)
        .ok_or_else(|| Error::other(format!("backup_label missing '{prefix}' line")))?;
    let t = u32::from_str_radix(token, 10)
        .map_err(|_| Error::other(format!("invalid timeline in backup_label: '{token}'")))?;
    Ok(TimelineId::new(t))
}

/// Return the first whitespace-delimited token following `prefix` on any line.
fn first_token_after<'a>(label: &'a str, prefix: &str) -> Option<&'a str> {
    for line in label.lines() {
        if let Some(rest) = line.trim_start().strip_prefix(prefix) {
            return rest.trim().split_whitespace().next();
        }
    }
    None
}

/// Pack a directory into a compressed `tar.zst` blob in memory.
fn tar_dir_to_zst(src: &Path) -> Result<Vec<u8>> {
    let tar_buf: Vec<u8> = Vec::new();
    let mut builder = tar::Builder::new(tar_buf);
    builder.append_dir_all(".", src)?;
    builder.finish()?;
    let tar_buf = builder
        .into_inner()
        .map_err(|e| Error::other(format!("tar finalize: {e}")))?;
    zstd::encode_all(tar_buf.as_slice(), 3)
        .map_err(|e| Error::other(format!("zstd compress: {e}")))
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
    let (base_ckpt, tar_bytes, target, target_label) = if let Some(time_str) = &args.time {
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
            format!("time '{time_str}'"),
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
            format!("lsn {}", l.to_pg_string()),
        )
    };
    eprintln!(
        "tiko_pitr: recovering to {target_label} on timeline {} from base backup {base_ckpt}",
        timeline.to_hex()
    );

    let pgdata = args.pgdata.as_path();
    let pg_ctl = args.pg_ctl.as_path();
    let psql = args
        .psql
        .clone()
        .unwrap_or_else(|| sibling_binary(pg_ctl, "psql"));
    let tiko_restore = args
        .tiko_restore
        .clone()
        .unwrap_or_else(default_tiko_restore);
    let conf = pgdata.join(pitr::RECOVERY_CONF_FILE);
    let backup = backup_path(pgdata);

    // The live base manifest lives under the tiko root (excluded from the
    // PGDATA snapshot). Snapshot it separately so a failed recovery can restore
    // the manifest that matches the rolled-back PGDATA.
    let manifest_path = core::tiko_root_path().join(BASE_MANIFEST_FILE_NAME);
    let rollback = tempfile::tempdir()?;
    let manifest_backup = rollback.path().join(BASE_MANIFEST_FILE_NAME);

    // 3. Stop PostgreSQL so the data dir is quiesced before mutation.
    stop_pg(pg_ctl, pgdata)?;

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
            Ok(())
        }
        Err(e) => {
            eprintln!("tiko_pitr: recovery failed: {e}");
            eprintln!("tiko_pitr: restoring PGDATA + base manifest from rollback snapshot");
            // Best-effort stop before restoring; ignore the result.
            let _ = stop_pg(pg_ctl, pgdata);
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
    psql: &Path,
    tiko_restore: &Path,
    recovery_timeout: u64,
) -> Result<()> {
    // Wipe everything except `tiko/` and replace with the base backup. The
    // tarball already carries a real `backup_label` from pg_basebackup, so no
    // pg_control shaping is needed.
    pitr::wipe_dir_excluding(pgdata, "tiko")?;
    extract_backup(tar_bytes, pgdata)?;

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
    start_pg(pg_ctl, pgdata)?;
    if let Err(e) = wait_for_promotion(psql, recovery_timeout) {
        let _ = stop_pg(pg_ctl, pgdata);
        return Err(e);
    }
    Ok(())
}

/// Decompress (zstd) and extract a base-backup tarball into `pgdata`.
fn extract_backup(tar_zst: &[u8], pgdata: &Path) -> Result<()> {
    let tar_buf = zstd::decode_all(tar_zst)
        .map_err(|e| Error::other(format!("zstd decompress base backup: {e}")))?;
    let mut arch = tar::Archive::new(tar_buf.as_slice());
    arch.unpack(pgdata)
        .map_err(|e| Error::other(format!("tar unpack base backup: {e}")))
}

/// Poll `SELECT pg_is_in_recovery()` once per second until it returns `f`
/// (promotion complete) or `timeout_secs` elapses.
fn wait_for_promotion(psql: &Path, timeout_secs: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match run_psql(psql, "SELECT pg_is_in_recovery()") {
            Ok(out) if out.trim() == "f" => return Ok(()),
            // "t" = still in recovery; a query error = not accepting connections
            // yet (early startup) or a transient blip. Keep polling until the
            // deadline; if the server died, `pg_ctl start` already returned Err.
            _ => {}
        }
        if Instant::now() >= deadline {
            return Err(Error::other(format!(
                "PostgreSQL did not promote within {timeout_secs}s"
            )));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Run `psql -d postgres -Atqc <sql>` and return stdout.
fn run_psql(psql: &Path, sql: &str) -> Result<String> {
    let out = Command::new(psql)
        .args(["-d", "postgres"])
        .args(["-Atqc", sql])
        .output()
        .map_err(|e| Error::other(format!("failed to spawn psql: {e}")))?;
    if !out.status.success() {
        return Err(Error::other(format!(
            "psql failed (exit: {}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `pg_ctl -D <pgdata> -m fast -w stop`, tolerating an already-stopped instance.
///
/// Assumes this process is the sole orchestrator of the target PGDATA (no
/// concurrent `pg_ctl`): a non-zero exit with an absent `postmaster.pid` is
/// treated as "already stopped".
fn stop_pg(pg_ctl: &Path, pgdata: &Path) -> Result<()> {
    let status = Command::new(pg_ctl)
        .arg("stop")
        .arg("-D")
        .arg(pgdata)
        .args(["-m", "fast", "-w"])
        .status()
        .map_err(|e| Error::other(format!("failed to spawn pg_ctl: {e}")))?;
    if status.success() {
        return Ok(());
    }
    // A non-zero exit may just mean "not running". Confirm via postmaster.pid.
    if !pgdata.join("postmaster.pid").exists() {
        return Ok(());
    }
    Err(Error::other(
        "pg_ctl stop failed and postmaster.pid is still present",
    ))
}

/// `pg_ctl -D <pgdata> -w start` for normal startup.
fn start_pg(pg_ctl: &Path, pgdata: &Path) -> Result<()> {
    let status = Command::new(pg_ctl)
        .arg("start")
        .arg("-D")
        .arg(pgdata)
        .arg("-w")
        .status()
        .map_err(|e| Error::other(format!("failed to spawn pg_ctl: {e}")))?;
    if !status.success() {
        return Err(Error::other(format!(
            "pg_ctl start failed (exit: {status})"
        )));
    }
    Ok(())
}

/// Derive a sibling binary (e.g. `psql`, `postgres`) of `pg_ctl`: same parent
/// directory. Falls back to `name` on `PATH` if `pg_ctl` has no parent.
fn sibling_binary(pg_ctl: &Path, name: &str) -> PathBuf {
    match pg_ctl.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(name),
        _ => PathBuf::from(name),
    }
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
    let store = match Store::init() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("tiko_pitr: store init failed: {e}");
            exit(1);
        }
    };

    let res = match &cli.command {
        Cmd::List => run_list(store),
        Cmd::Backup(args) => run_backup(store, args),
        Cmd::Recover(args) => run_recover(store, args),
    };

    if let Err(e) = res {
        eprintln!("tiko_pitr: {e}");
        exit(1);
    }
}
