//! `tiko_pitr` — automate Tiko point-in-time recovery (PITR).
//!
//! Two subcommands:
//!   * `list` — print available recovery checkpoints from remote.
//!   * `recover --timeline <TL> --lsn <LSN>` — stop the instance, snapshot
//!     PGDATA (excluding `tiko/`), recover to the target checkpoint, then
//!     restart normally. On failure, PGDATA is restored from the snapshot and
//!     the instance is left stopped.
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
    /// List available recovery checkpoints on remote.
    List,
    /// Recover the instance to a target checkpoint, then restart normally.
    Recover(RecoverArgs),
}

#[derive(Args)]
struct RecoverArgs {
    /// Target timeline id, in hex as shown by `list` (e.g. `00000001`).
    #[arg(long)]
    timeline: String,
    /// Target LSN, PostgreSQL `X/Y` or hex form (e.g. `0/3000028`).
    #[arg(long)]
    lsn: String,
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
    let rows = store.list_checkpoints()?;
    if rows.is_empty() {
        println!("no checkpoints found");
        return Ok(());
    }
    println!(
        "{:>4}  {:>10}  {:>18}  {:>26}  {:>7}",
        "#", "timeline", "lsn", "created_at", "chunks"
    );
    for (i, r) in rows.iter().enumerate() {
        let ts = DateTime::<Utc>::from_timestamp(r.created_at, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| r.created_at.to_string());
        println!(
            "{:>4}  {:>10}  {:>18}  {:>26}  {:>7}",
            i,
            r.ckpt.timeline_id.to_hex(),
            r.ckpt.lsn.to_pg_string(),
            ts,
            r.n_chunks
        );
    }
    Ok(())
}

fn run_recover(store: &Store, args: &RecoverArgs) -> Result<()> {
    let tl = TimelineId::from_hex(&args.timeline)
        .map_err(|e| Error::other(format!("invalid --timeline '{}': {e}", args.timeline)))?;
    let lsn = Lsn::parse_either(&args.lsn).map_err(Error::other)?;
    let target = Checkpoint::new(tl, lsn);

    // 2. Validate the target is a real, available checkpoint.
    let rows = store.list_checkpoints()?;
    if !rows.iter().any(|r| r.ckpt == target) {
        return Err(Error::other(format!(
            "target {target} is not an available checkpoint; run `tiko_pitr list`"
        )));
    }

    // 3. Pick the base pg_state to recover from (newest base <= target).
    let (base_ckpt, pg_state) = store.load_base_pg_state_at_or_before(target)?;
    eprintln!("tiko_pitr: recovering to {target} from base checkpoint {base_ckpt}");

    let pgdata = args.pgdata.as_path();
    let pg_ctl = args.pg_ctl.as_path();
    let postgres = args
        .postgres
        .clone()
        .unwrap_or_else(|| sibling_postgres(pg_ctl));
    let conf = pgdata.join(pitr::TIKO_CONF_FILE);
    let backup = backup_path(pgdata);

    // 4. Stop PostgreSQL so the data dir is quiesced before copy/mutation.
    stop_pg(pg_ctl, pgdata)?;

    // 5. Snapshot PGDATA (excluding the bulk `tiko/` dir).
    pitr::backup_dir_excluding(pgdata, &backup, "tiko")?;

    // 6-9. Mutate + run recovery. On any failure, restore from the snapshot.
    match recover_inner(&conf, pgdata, &pg_state, tl, lsn, &postgres) {
        Ok(()) => {
            // 10. Success: clean up recovery artifacts, drop backup, restart.
            pitr::remove_recovery_conf(&conf)?;
            let _ = std::fs::remove_file(pgdata.join("recovery.signal"));
            std::fs::remove_dir_all(&backup)?;
            start_pg(pg_ctl, pgdata)?;
            eprintln!("tiko_pitr: recovery to {target} complete; database restarted");
            Ok(())
        }
        Err(e) => {
            // 11. Failure: ensure stopped, restore PGDATA, leave PG down.
            eprintln!("tiko_pitr: recovery failed: {e}");
            eprintln!("tiko_pitr: restoring PGDATA from backup {}", backup.display());
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

/// Steps 6-9: extract pg_state, write the PITR conf, touch recovery.signal, and
/// run `postgres` in the foreground. Returns `Ok` only if recovery reached the
/// target (postgres exited 0).
fn recover_inner(
    conf: &Path,
    pgdata: &Path,
    pg_state: &[u8],
    tl: TimelineId,
    lsn: Lsn,
    postgres: &Path,
) -> Result<()> {
    extract_pg_state(pg_state, pgdata)?;
    pitr::write_pitr_recovery_conf(conf, tl, lsn)?;
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
