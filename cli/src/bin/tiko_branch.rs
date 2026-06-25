//! `tiko_branch` — create a database branch by copying a parent database at a
//! point in time, copy-on-write over the shared storage.
//!
//! Runs with the PARENT's environment (`TIKO_DB_ID` etc.). It:
//!   1. runs `pg_basebackup -X stream` against the running parent (the
//!      `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint forms a base manifest at the
//!      backup LSN);
//!   2. seeds the branch's storage namespace with that base manifest — its
//!      `ChunkRef.db_id = parent`, so the branch copy-on-write reads the
//!      parent's chunks through the shared `TIKO_STORAGE_ROOT`;
//!   3. extracts the backup into a fresh branch PGDATA;
//!   4. starts the branch PostgreSQL under the branch's `TIKO_DB_ID`
//!      (`TIKO_STORAGE_ROOT` shared with the parent, `TIKO_LOCAL_PATH` per
//!      branch). `backup_label` drives recovery to the backup's consistency
//!      point and the branch promotes — no `recovery.signal`/target needed.
//!
//! The parent keeps running untouched; the branch diverges from the backup LSN.

use std::path::PathBuf;
use std::process::{Command, exit};

use clap::{Args, Parser};

use core::env;
use core::error::{Error, Result};
use core::io::store::Store;
use core::io::timeline::Checkpoint;
use core::{DbNamespace, storage_root_path};

// Standalone process (not loaded into the postmaster); `cli::pg_stubs` supplies
// the PG symbols that `core` transitively references. See `tiko_restore`.
extern crate cli;

#[derive(Parser)]
#[command(
    name = "tiko_branch",
    about = "Create a Tiko database branch (copy-on-write) from a running parent"
)]
struct Cli {
    #[command(flatten)]
    parent: ParentArgs,
    #[command(flatten)]
    branch: BranchArgs,
}

/// Connection to the running PARENT database (for `pg_basebackup`).
#[derive(Args)]
struct ParentArgs {
    /// Host of the running parent (`pg_basebackup -h`). Empty = local socket.
    #[arg(long, env = "PGHOST", default_value = "")]
    host: String,
    /// Port of the running parent (`pg_basebackup -p`).
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    port: u16,
    /// User to connect as (`pg_basebackup -U`). Defaults to current OS user.
    #[arg(long, env = "PGUSER")]
    user: Option<String>,
    /// Path to `pg_basebackup`. Defaults to `pg_basebackup` on PATH.
    #[arg(long, default_value = "pg_basebackup")]
    pg_basebackup: PathBuf,
    /// `pg_basebackup -c`: `fast` or `spread`.
    #[arg(long, default_value = "fast")]
    checkpoint: String,
}

/// The NEW branch database identity + location.
#[derive(Args)]
struct BranchArgs {
    /// The NEW database id for the branch (`TIKO_DB_ID`). Required.
    #[arg(long)]
    db_id: u64,
    /// The NEW project id for the branch (`TIKO_PROJECT_ID`). Required.
    #[arg(long)]
    project_id: u64,
    /// Branch PostgreSQL data directory (created if absent).
    #[arg(long)]
    pgdata: PathBuf,
    /// Port the branch PostgreSQL listens on. MUST differ from the parent.
    #[arg(long, default_value_t = 5433)]
    branch_port: u16,
    /// Per-branch local cache path (`TIKO_LOCAL_PATH`: `base_manifest.tikm`,
    /// `draft.spill`, `chunk_cache`). Defaults to `<pgdata>/tiko`.
    #[arg(long)]
    local_path: Option<PathBuf>,
    /// Path to `pg_ctl`. Defaults to `pg_ctl` on PATH.
    #[arg(long, default_value = "pg_ctl")]
    pg_ctl: PathBuf,
    /// Path to `psql` for polling promotion. Defaults to sibling of `--pg-ctl`.
    #[arg(long)]
    psql: Option<PathBuf>,
    /// Seconds to wait for the branch to reach consistency and promote.
    #[arg(long, default_value_t = 300)]
    recovery_timeout: u64,
}

fn main() {
    let cli = Cli::parse();
    // This is an ephemeral standalone CLI process (no real PG `DataDir`), so
    // `local_path()` would default to a relative "tiko/" in the CWD — polluting
    // the working directory and clobbering any existing sample. Redirect the
    // tool's own local cache to a temp dir. (The branch PG is spawned with its
    // own explicit `TIKO_LOCAL_PATH`, so this only affects the tool process.)
    let local_temp = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("tiko_branch: failed to create temp dir: {e}");
        exit(1);
    });
    // SAFETY: single-threaded CLI before any other thread reads the env.
    unsafe {
        std::env::set_var(env::ENV_TIKO_LOCAL_PATH, local_temp.path());
    }

    // `Store::init` reads the PARENT's TIKO_ORG_ID/DB_ID/PROJECT_ID (shared org).
    let store = match Store::init() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("tiko_branch: parent store init failed: {e}");
            exit(1);
        }
    };
    let result = run_branch(store, &cli.parent, &cli.branch);

    // Clean up the tool's temp cache (the branch PG has its own TIKO_LOCAL_PATH).
    drop(local_temp);

    if let Err(e) = result {
        eprintln!("tiko_branch: {e}");
        exit(1);
    }
}

fn run_branch(store: &Store, parent: &ParentArgs, branch: &BranchArgs) -> Result<()> {
    // A branch shares the parent's org; only db_id/project_id differ.
    let org_id = env::read_u64(env::ENV_ORG_ID);
    let branch_ns = DbNamespace::new(org_id, branch.db_id, branch.project_id);
    let storage_root = storage_root_path();
    let branch_local = branch
        .local_path
        .clone()
        .unwrap_or_else(|| branch.pgdata.join("tiko"));
    eprintln!(
        "tiko_branch: branching parent into db_id={} project_id={} (port {})",
        branch.db_id, branch.project_id, branch.branch_port
    );

    // 1. pg_basebackup -X stream against the parent. The checkpoint auto-forms
    //    the parent's base manifest at the backup LSN.
    let tmp = tempfile::tempdir()?;
    let backup_dir = tmp.path();
    cli::pgops::run_pg_basebackup(
        &cli::pgops::BasebackupOpts {
            pg_basebackup: &parent.pg_basebackup,
            host: &parent.host,
            port: parent.port,
            user: parent.user.as_deref(),
            checkpoint: &parent.checkpoint,
            wal_method: "stream",
        },
        backup_dir,
    )?;

    // 2. Parse backup_label → backup checkpoint LSN (== base manifest key).
    let label_path = backup_dir.join("backup_label");
    let label = std::fs::read_to_string(&label_path)
        .map_err(|e| Error::other(format!("read {}: {e}", label_path.display())))?;
    let (checkpoint_lsn, _redo_lsn, timeline) = cli::pgops::parse_backup_label(&label)?;
    let ckpt = Checkpoint::new(timeline, checkpoint_lsn);
    eprintln!("tiko_branch: parent backup checkpoint {ckpt}");

    // 3. Seed the branch namespace with the parent's base manifest at the
    //    backup LSN. ChunkRef.db_id = parent is preserved, so the branch
    //    copy-on-write reads shared chunks from the parent's namespace.
    store.seed_branch_base_manifest(branch_ns.clone(), ckpt)?;
    eprintln!("tiko_branch: seeded branch namespace {branch_ns} from {ckpt}");

    // 4. Pack + extract the backup into the branch PGDATA. Then drop any
    //    parent-local `tiko` cache pg_basebackup copied (it belongs to the
    //    parent's db_id); the branch re-derives its own from the seeded ns.
    std::fs::create_dir_all(&branch.pgdata)?;
    let tar_zst = cli::pgops::tar_dir_to_zst(backup_dir)?;
    cli::pgops::extract_backup(&tar_zst, &branch.pgdata)?;
    let branch_tiko = branch.pgdata.join("tiko");
    if branch_tiko.exists() {
        let _ = std::fs::remove_dir_all(&branch_tiko);
    }
    // PostgreSQL requires PGDATA mode 0700/0750; create_dir_all + tar unpack
    // leave it at the process umask (typically 0755), so fix it explicitly.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&branch.pgdata, PermissionsExt::from_mode(0o700))
            .map_err(|e| Error::other(format!("set branch PGDATA permissions: {e}")))?;
    }

    // 5. Start the branch PostgreSQL. backup_label drives archive recovery to
    //    the backup's consistency point, then it promotes — no recovery.signal
    //    or recovery_target needed. The branch runs under its own db_id/project
    //    with the parent's shared storage root and its own local cache path.
    std::fs::create_dir_all(&branch_local)?;
    let psql = branch
        .psql
        .clone()
        .unwrap_or_else(|| cli::pgops::sibling_binary(&branch.pg_ctl, "psql"));
    let log_path = branch.pgdata.join("branch.log");

    // Absolutize the Tiko paths passed to the branch PG: postgres changes its
    // CWD to PGDATA on startup, so a relative path would resolve under PGDATA
    // (creating spurious nested dirs) instead of relative to this tool's CWD.
    // `Path::join` keeps already-absolute paths as-is.
    let cwd = std::env::current_dir().unwrap_or_default();
    let storage_root_abs = cwd.join(&storage_root);
    let branch_local_abs = cwd.join(&branch_local);

    let status = Command::new(&branch.pg_ctl)
        .arg("start")
        .arg("-D")
        .arg(&branch.pgdata)
        .arg("-l")
        .arg(&log_path)
        .arg("-w")
        .arg("-o")
        .arg(format!("-c port={}", branch.branch_port))
        .env("TIKO_DB_ID", branch.db_id.to_string())
        .env("TIKO_PROJECT_ID", branch.project_id.to_string())
        .env(
            "TIKO_STORAGE_ROOT",
            storage_root_abs.to_string_lossy().to_string(),
        )
        .env(
            "TIKO_LOCAL_PATH",
            branch_local_abs.to_string_lossy().to_string(),
        )
        .status()
        .map_err(|e| Error::other(format!("failed to spawn pg_ctl: {e}")))?;
    if !status.success() {
        return Err(Error::other(format!(
            "branch pg_ctl start failed (exit: {status}); see {}",
            log_path.display()
        )));
    }

    // 6. Wait for the branch to reach consistency and promote.
    cli::pgops::wait_for_promotion(&psql, branch.branch_port, branch.recovery_timeout)?;
    eprintln!(
        "tiko_branch: branch db_id={} is up on port {} (copy-on-write on parent storage)",
        branch.db_id, branch.branch_port
    );
    Ok(())
}
