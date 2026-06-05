//! `tiko_pitr` — automate Tiko point-in-time recovery (PITR).
//!
//! Two subcommands:
//!   * `list` — print the recoverable time window from remote.
//!   * `recover (--time <TS> | --lsn <LSN>) [--timeline <HEX>]` — stop the
//!     instance, snapshot PGDATA (excluding `tiko/`), recover to the target
//!     point in the window, then restart normally. On failure, PGDATA is
//!     restored from the snapshot and the instance is left stopped.
//!
//! Storage is configured from the environment exactly as `tiko_restore`
//! expects (`Store::init()`): `TIKO_ROOT_PATH`/`PGDATA`, `TIKO_ORG_ID`,
//! `TIKO_DB_ID`, `TIKO_PROJECT_ID`.

use std::path::{Path, PathBuf};
use std::process::{Command, exit};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};

use core::error::{Error, Result};
use core::io::store::Store;
use core::io::timeline::Checkpoint;
use core::pitr;
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

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

/// Extract pg_state, write the PITR conf, touch recovery.signal, and run
/// `postgres` in the foreground. Returns `Ok` only if recovery reached the
/// target (postgres exited 0).
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

    // Foreground run: with recovery_target_action='shutdown', postgres replays
    // WAL to the target then shuts down and exits 0. If it can't reach the
    // target it exits non-zero — so the exit status is the success signal.
    let status = Command::new(postgres)
        .arg("-D")
        .arg(pgdata)
        .status()
        .map_err(|e| Error::other(format!("failed to spawn postgres ({}): {e}", postgres.display())))?;
    if !status.success() {
        return Err(Error::other(format!(
            "postgres recovery did not reach the target (exit: {status})"
        )));
    }
    Ok(())
}

/// Extract a `pg_state.tar.zst` archive into `pgdata` via the `tar` CLI.
fn extract_pg_state(pg_state: &[u8], pgdata: &Path) -> Result<()> {
    if pg_state.is_empty() {
        return Err(Error::other(
            "selected base manifest has empty pg_state; cannot recover",
        ));
    }
    let tmp = tempfile::tempdir()?;
    let archive = tmp.path().join("pg_state.tar.zst");
    std::fs::write(&archive, pg_state)?;
    let status = Command::new("tar")
        .arg("-xf")
        .arg(&archive)
        .arg("-C")
        .arg(pgdata)
        .status()
        .map_err(|e| Error::other(format!("failed to spawn tar: {e}")))?;
    if !status.success() {
        return Err(Error::other("pg_state tar extraction failed"));
    }
    Ok(())
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
        return Err(Error::other(format!("pg_ctl start failed (exit: {status})")));
    }
    Ok(())
}

/// Derive the `postgres` binary path as a sibling of `pg_ctl`.
fn sibling_postgres(pg_ctl: &Path) -> PathBuf {
    match pg_ctl.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join("postgres"),
        _ => PathBuf::from("postgres"),
    }
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
        Cmd::Recover(args) => run_recover(store, args),
    };

    if let Err(e) = res {
        eprintln!("tiko_pitr: {e}");
        exit(1);
    }
}
