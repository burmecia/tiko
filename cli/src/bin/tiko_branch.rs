//! `tiko_branch` — create a database branch by copying a parent database at a
//! point in time, copy-on-write over the shared storage.
//!
//! Three subcommands:
//!   * `backup` — run `pg_basebackup -X stream` against the running parent (the
//!     `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint forms a base manifest at the
//!     backup LSN), then pack the backup directory into a compressed `tar.zst`
//!     file at `--pack <path>`. No Tiko storage access, no TIKO_* env required.
//!   * `restore` — read a pack file produced by `backup`, unpack it into a fresh
//!     branch PGDATA, seed the branch's storage namespace with the parent's base
//!     manifest at the backup checkpoint LSN (its `ChunkRef.db_id = parent`, so
//!     the branch copy-on-write reads the parent's chunks through the shared
//!     `TIKO_STORAGE_ROOT`), and start the branch PostgreSQL under the branch's
//!     `TIKO_DB_ID` to drive archive recovery via `backup_label`. Once the
//!     branch reaches the backup's consistency point and promotes, it is
//!     **stopped** — run `restart` to bring it back up.
//!   * `restart` — start the branch PostgreSQL left stopped by a successful
//!     `restore`. Just `pg_ctl start` against the already-recovered branch
//!     PGDATA with the branch's Tiko environment.
//!
//! The parent keeps running untouched; the branch diverges from the backup LSN.
//!
//! The `backup` subcommand runs in the PARENT's context (it connects to the
//! running parent for `pg_basebackup`). The `restore` and `restart` subcommands
//! run in the CHILD/branch's context. The split lets the caller bring the
//! branch online only when ready (e.g. after wiring up the API endpoint).
//!
//! **Output model:** every subcommand emits a single JSON object on stdout
//! (pretty-printed) so HTTP consumers (tikoguest's `/branch/*` routes) can
//! parse the result directly instead of screen-scraping. Human-readable
//! progress and diagnostics go to stderr. On any failure the stdout object is
//! `{"error":{"message":"..."}}` and the process exits non-zero.

use std::path::{Path, PathBuf};
use std::process::{Command, exit};

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use core::env;
use core::error::{Error, Result};
use core::io::store::Store;
use core::io::timeline::Checkpoint;
use core::{DbNamespace, storage_root_path};

// Standalone process (not loaded into the postmaster); `cli::pg_stubs` supplies
// the PG symbols that `core` transitively references. See `tiko_restore`.
extern crate cli;

/// On-disk filename of the live base-manifest cache under `TIKO_LOCAL_PATH`
/// (mirror of the private constant in `core::manifest`). `Store::init()` reads
/// this as a fast path, so a stale leftover would shadow the seeded manifest.
const BASE_MANIFEST_FILE_NAME: &str = "base_manifest.tikm";

#[derive(Parser)]
#[command(
    name = "tiko_branch",
    about = "Create a Tiko database branch (copy-on-write) from a running parent"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run `pg_basebackup` against the parent and pack the result into a
    /// compressed `tar.zst` file. No Tiko storage access required.
    Backup(BackupArgs),
    /// Read a pack file, unpack it into a branch PGDATA, seed the branch
    /// namespace from the parent's base manifest, and run recovery to the
    /// backup's consistency point. The branch promotes and is left **stopped**
    /// — run `restart` to bring it back up.
    Restore(RestoreArgs),
    /// Start the branch PostgreSQL left stopped by a successful `restore`.
    Restart(RestartArgs),
}

/// `backup`: connection to the running PARENT database (for `pg_basebackup`) +
/// the output pack file path.
#[derive(Args)]
struct BackupArgs {
    /// Path to write the compressed `tar.zst` pack file (base backup tarball).
    #[arg(long)]
    pack: PathBuf,
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

/// `restore`: the pack file to read + the NEW branch database identity/location.
#[derive(Args)]
struct RestoreArgs {
    /// Path to the compressed `tar.zst` pack file (produced by `backup`).
    #[arg(long)]
    pack: PathBuf,
    /// The PARENT database id to branch from. The branch copy-on-write reads
    /// the parent's chunks through the shared `TIKO_STORAGE_ROOT`, so the
    /// branch namespace is seeded from the parent's base manifest at the backup
    /// checkpoint LSN.
    #[arg(long)]
    parent_db_id: u64,
    /// The NEW database id for the branch (`TIKO_DB_ID`). Required.
    #[arg(long)]
    db_id: u64,
    /// The NEW project id for the branch (`TIKO_PROJECT_ID`). Defaults to
    /// `db_id` when omitted.
    #[arg(long)]
    project_id: Option<u64>,
    /// Branch PostgreSQL data directory (created if absent).
    #[arg(long)]
    pgdata: PathBuf,
    /// Port the branch PostgreSQL listens on. MUST differ from the parent.
    #[arg(long, default_value_t = 5432)]
    branch_port: u16,
    /// Per-branch local cache path (`base_manifest.tikm`, `draft.spill`,
    /// `chunk_cache`). Defaults to the `TIKO_LOCAL_PATH` env var if set, else
    /// `<pgdata>/tiko`. Resolved by clap at parse time (before the tool
    /// process overrides its own `TIKO_LOCAL_PATH` for `Store::init`).
    #[arg(long, env = "TIKO_LOCAL_PATH")]
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

/// `restart`: the branch database identity/location. Just enough to start the
/// already-recovered branch PostgreSQL with its Tiko environment.
#[derive(Args)]
struct RestartArgs {
    /// The branch database id (`TIKO_DB_ID`). Required.
    #[arg(long)]
    db_id: u64,
    /// The branch project id (`TIKO_PROJECT_ID`). Defaults to `db_id`.
    #[arg(long)]
    project_id: Option<u64>,
    /// Branch PostgreSQL data directory.
    #[arg(long)]
    pgdata: PathBuf,
    /// Port the branch PostgreSQL listens on.
    #[arg(long, default_value_t = 5432)]
    branch_port: u16,
    /// Per-branch local cache path (`TIKO_LOCAL_PATH`). Defaults to
    /// `<pgdata>/tiko`.
    #[arg(long)]
    local_path: Option<PathBuf>,
    /// Path to `pg_ctl`. Defaults to `pg_ctl` on PATH.
    #[arg(long, default_value = "pg_ctl")]
    pg_ctl: PathBuf,
}

// ── JSON output DTOs ─────────────────────────────────────────────────────────
//
// Each subcommand emits one of these on stdout. Fields use the human-readable
// PostgreSQL forms (`X/Y` LSNs, hex timeline ids) so HTTP consumers can display
// them without reformatting.

/// `backup` response: the pack file just written and the backup checkpoint it
/// was taken at.
#[derive(Serialize)]
struct BackupOutput {
    /// Always `"backed_up"` (only emitted on success).
    status: String,
    /// Absolute path to the written `tar.zst` pack file.
    pack: String,
    /// Fixed-width hex timeline id (e.g. `"00000001"`).
    timeline: String,
    /// PostgreSQL `X/Y` checkpoint LSN the backup was taken at.
    checkpoint_lsn: String,
    /// PostgreSQL `X/Y` REDO LSN (where WAL replay starts).
    redo_lsn: String,
    /// Compressed pack size in bytes.
    bytes_compressed: usize,
}

/// `restore` response: the branch was seeded and promoted (then stopped).
#[derive(Serialize)]
struct RestoreOutput {
    /// Always `"restored"` (only emitted on success).
    status: String,
    /// The NEW branch database id.
    db_id: u64,
    /// The branch project id.
    project_id: u64,
    /// The PARENT database id the branch was copied from.
    parent_db_id: u64,
    /// Fixed-width hex timeline id (e.g. `"00000001"`).
    timeline: String,
    /// PostgreSQL `X/Y` checkpoint LSN the branch was seeded at (the parent's
    /// backup checkpoint == base manifest key).
    checkpoint_lsn: String,
}

/// `restart` response: the branch PostgreSQL was started.
#[derive(Serialize)]
struct RestartOutput {
    /// Always `"started"` (only emitted on success).
    status: String,
    /// The branch database id.
    db_id: u64,
    /// Port the branch PostgreSQL listens on.
    branch_port: u16,
}

/// Pretty-print a DTO as JSON on stdout.
fn print_json<T: Serialize>(value: &T) -> Result<()> {
    let s = serde_json::to_string_pretty(value)
        .map_err(|e| Error::other(format!("json serialize failed: {e}")))?;
    println!("{s}");
    Ok(())
}

fn main() {
    let cli = Cli::parse();

    let result = (|| -> Result<()> {
        match cli.command {
            Cmd::Backup(args) => run_backup(&args),
            // `restart` is just `pg_ctl start` on the already-recovered branch
            // PGDATA; it doesn't touch Tiko storage, so don't require `Store::init`
            // (or the TIKO_* env) to succeed for it.
            Cmd::Restart(args) => run_restart(&args),
            Cmd::Restore(args) => {
                // This is an ephemeral standalone CLI process (no real PG
                // `DataDir`), so `local_path()` would default to a relative
                // "tiko/" in the CWD — polluting the working directory and
                // clobbering any existing sample. Redirect the tool's own local
                // cache to a temp dir. (The branch PG is spawned with its own
                // explicit `TIKO_LOCAL_PATH`, so this only affects the tool
                // process.)
                //
                // NB: `args.local_path` (the BRANCH's local path) was already
                // resolved by clap at `Cli::parse()` above, from the original
                // `TIKO_LOCAL_PATH` env — so overriding the env below does NOT
                // change it. Reading `TIKO_LOCAL_PATH` here in `main()` instead
                // would wrongly capture this temp dir.
                let local_temp = tempfile::tempdir().map_err(|e| {
                    Error::other(format!("tiko_branch: failed to create temp dir: {e}"))
                })?;
                // SAFETY: single-threaded CLI before any other thread reads the env.
                unsafe {
                    std::env::set_var(env::ENV_TIKO_LOCAL_PATH, local_temp.path());
                }

                // `Store::init` reads the shared `TIKO_ORG_ID`/`TIKO_STORAGE_ROOT`
                // (and requires `TIKO_DB_ID`/`TIKO_PROJECT_ID` to be set, though
                // their values are irrelevant here — the parent's db_id comes from
                // `--parent-db-id`). `TIKO_ORG_ID` identifies the shared org the
                // parent and branch live in.
                let store = Store::init()?;
                let res = run_restore(&store, &args);

                // Clean up the tool's temp cache (the branch PG has its own
                // TIKO_LOCAL_PATH).
                drop(local_temp);
                res
            }
        }
    })();

    if let Err(e) = result {
        // Emit a structured JSON error on stdout (so HTTP consumers can parse
        // the reason directly) and a human-readable line on stderr. Exit
        // non-zero so tikoguest's run_external maps this to a 5xx.
        let body = serde_json::json!({ "error": { "message": e.to_string() } });
        let stdout = serde_json::to_string(&body)
            .unwrap_or_else(|_| r#"{"error":{"message":"unknown error"}}"#.to_string());
        println!("{stdout}");
        eprintln!("tiko_branch: {e}");
        exit(1);
    }
}

/// `backup` subcommand: run `pg_basebackup -X stream` against the parent, pack
/// the result into a compressed `tar.zst` file at `args.pack`. No Tiko storage
/// access — the pack file is a self-contained base backup (carries
/// `backup_label` + WAL).
fn run_backup(args: &BackupArgs) -> Result<()> {
    // 1. pg_basebackup -X stream against the parent. The checkpoint auto-forms
    //    the parent's base manifest at the backup LSN.
    let tmp = tempfile::tempdir()?;
    let backup_dir = tmp.path();
    cli::pgops::run_pg_basebackup(
        &cli::pgops::BasebackupOpts {
            pg_basebackup: &args.pg_basebackup,
            host: &args.host,
            port: args.port,
            user: args.user.as_deref(),
            checkpoint: &args.checkpoint,
            wal_method: "stream",
        },
        backup_dir,
    )?;

    // 2. Parse backup_label → backup checkpoint LSN (== base manifest key).
    //    Informational only here; `restore` re-parses from the extracted
    //    PGDATA so the pack file stays self-contained (no sidecar metadata).
    let label_path = backup_dir.join("backup_label");
    let label = std::fs::read_to_string(&label_path)
        .map_err(|e| Error::other(format!("read {}: {e}", label_path.display())))?;
    let (checkpoint_lsn, redo_lsn, timeline) = cli::pgops::parse_backup_label(&label)?;
    let ckpt = Checkpoint::new(timeline, checkpoint_lsn);
    let redo_ckpt = Checkpoint::new(timeline, redo_lsn);
    eprintln!("tiko_branch: parent backup checkpoint {ckpt}");

    // 3. Pack the backup directory (tar + zstd) and write it to --pack.
    let tar_zst = cli::pgops::tar_dir_to_zst(backup_dir)?;
    if let Some(parent) = args.pack.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::other(format!("create {}: {e}", parent.display())))?;
    }
    std::fs::write(&args.pack, &tar_zst)
        .map_err(|e| Error::other(format!("write {}: {e}", args.pack.display())))?;
    eprintln!(
        "tiko_branch: wrote pack to {} ({} bytes compressed) at checkpoint {ckpt}",
        args.pack.display(),
        tar_zst.len()
    );

    print_json(&BackupOutput {
        status: "backed_up".to_string(),
        pack: args.pack.to_string_lossy().to_string(),
        timeline: ckpt.timeline_id.to_hex(),
        checkpoint_lsn: ckpt.lsn.to_pg_string(),
        redo_lsn: redo_ckpt.lsn.to_pg_string(),
        bytes_compressed: tar_zst.len(),
    })
}

/// `restore` subcommand: read a pack file, unpack it into the branch PGDATA,
/// seed the branch namespace from the parent's base manifest at the backup
/// checkpoint LSN, then start the branch PostgreSQL to drive archive recovery
/// via `backup_label`. Once the branch promotes it is **stopped** — leaving it
/// quiesced for `tiko_branch restart`.
fn run_restore(store: &Store, branch: &RestoreArgs) -> Result<()> {
    // A branch shares the parent's org; only db_id/project_id differ.
    let org_id = env::read_u64(env::ENV_ORG_ID);
    // Default the branch's project_id to its db_id when not specified.
    let project_id = branch.project_id.unwrap_or(branch.db_id);
    let branch_ns = DbNamespace::new(org_id, branch.db_id, project_id);
    let branch_local = branch
        .local_path
        .clone()
        .unwrap_or_else(|| branch.pgdata.join("tiko"));
    eprintln!(
        "tiko_branch: restoring branch db_id={} project_id={} (port {}) from pack {} (parent db_id={})",
        branch.db_id,
        project_id,
        branch.branch_port,
        branch.pack.display(),
        branch.parent_db_id,
    );

    // 1. Read the pack file and unpack it into the branch PGDATA. Then drop
    //    any parent-local `tiko` cache pg_basebackup copied (it belongs to the
    //    parent's db_id); the branch re-derives its own from the seeded ns.
    let tar_zst = std::fs::read(&branch.pack)
        .map_err(|e| Error::other(format!("read {}: {e}", branch.pack.display())))?;
    std::fs::create_dir_all(&branch.pgdata)?;
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

    // 2. Parse backup_label → backup checkpoint LSN (== base manifest key)
    //    from the extracted PGDATA. The pack file is self-contained, so no
    //    sidecar metadata is needed.
    let label_path = branch.pgdata.join("backup_label");
    let label = std::fs::read_to_string(&label_path)
        .map_err(|e| Error::other(format!("read {}: {e}", label_path.display())))?;
    let (checkpoint_lsn, _redo_lsn, timeline) = cli::pgops::parse_backup_label(&label)?;
    let ckpt = Checkpoint::new(timeline, checkpoint_lsn);
    eprintln!("tiko_branch: parent backup checkpoint {ckpt}");

    // 3. Seed the branch namespace with the parent's base manifest at the
    //    backup LSN. ChunkRef.db_id = parent is preserved, so the branch
    //    copy-on-write reads shared chunks from the parent's namespace.
    store.seed_branch_base_manifest(branch.parent_db_id, branch_ns.clone(), ckpt)?;
    eprintln!("tiko_branch: seeded branch namespace {branch_ns} from {ckpt}");

    // 4. Clear any stale local base-manifest cache so the branch PostgreSQL
    //    re-derives it from the freshly-seeded shared-storage namespace.
    //    `Store::init()` prefers the on-disk `base_manifest.tikm` fast path; a
    //    leftover from a previous run (or the tool's temp store) would shadow
    //    the seeded manifest and pollute the branch's view.
    let manifest_path = branch_local.join(BASE_MANIFEST_FILE_NAME);
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path)
            .map_err(|e| Error::other(format!("remove {}: {e}", manifest_path.display())))?;
        eprintln!(
            "tiko_branch: cleared stale local base manifest at {}",
            manifest_path.display()
        );
    }

    // 5. Start the branch PostgreSQL to drive archive recovery to the backup's
    //    consistency point, then it promotes — no recovery.signal or
    //    recovery_target needed. The branch runs under its own db_id/project
    //    with the parent's shared storage root and its own local cache path.
    start_branch_pg(
        &branch.pg_ctl,
        &branch.pgdata,
        branch.branch_port,
        branch.db_id,
        project_id,
        &branch_local,
    )?;

    // 6. Wait for the branch to reach consistency and promote.
    let psql = branch
        .psql
        .clone()
        .unwrap_or_else(|| cli::pgops::sibling_binary(&branch.pg_ctl, "psql"));
    cli::pgops::wait_for_promotion(&psql, branch.branch_port, branch.recovery_timeout)?;
    eprintln!(
        "tiko_branch: branch db_id={} promoted; stopping (run `tiko_branch restart` to start it)",
        branch.db_id
    );

    // 7. Stop the branch PostgreSQL — leave it quiesced for `restart`. Splitting
    //    restore/restart lets the caller bring the branch online only when ready
    //    (and keeps `restore` from leaving a running primary the caller may not
    //    be ready to serve).
    cli::pgops::stop_pg(&branch.pg_ctl, &branch.pgdata)?;
    print_json(&RestoreOutput {
        status: "restored".to_string(),
        db_id: branch.db_id,
        project_id,
        parent_db_id: branch.parent_db_id,
        timeline: ckpt.timeline_id.to_hex(),
        checkpoint_lsn: ckpt.lsn.to_pg_string(),
    })
}

/// `restart` subcommand: start the branch PostgreSQL left stopped by a
/// successful `restore`. Just `pg_ctl start` against the already-recovered
/// branch PGDATA with the branch's Tiko environment — no recovery, no promotion
/// wait.
fn run_restart(args: &RestartArgs) -> Result<()> {
    let project_id = args.project_id.unwrap_or(args.db_id);
    let branch_local = args
        .local_path
        .clone()
        .unwrap_or_else(|| args.pgdata.join("tiko"));
    start_branch_pg(
        &args.pg_ctl,
        &args.pgdata,
        args.branch_port,
        args.db_id,
        project_id,
        &branch_local,
    )?;
    eprintln!(
        "tiko_branch: branch db_id={} is up on port {} (copy-on-write on parent storage)",
        args.db_id, args.branch_port
    );
    print_json(&RestartOutput {
        status: "started".to_string(),
        db_id: args.db_id,
        branch_port: args.branch_port,
    })
}

/// Start the branch PostgreSQL via `pg_ctl start` with the branch's Tiko
/// environment (`TIKO_DB_ID`/`TIKO_PROJECT_ID`/`TIKO_STORAGE_ROOT`/
/// `TIKO_LOCAL_PATH`) and the given listen port. Used by both `restore` (to
/// drive recovery) and `restart` (to bring the branch back up).
///
/// Absolutizes the Tiko paths passed to the branch PG: postgres changes its CWD
/// to PGDATA on startup, so a relative path would resolve under PGDATA (creating
/// spurious nested dirs) instead of relative to this tool's CWD. `Path::join`
/// keeps already-absolute paths as-is.
fn start_branch_pg(
    pg_ctl: &Path,
    pgdata: &Path,
    branch_port: u16,
    db_id: u64,
    project_id: u64,
    local_path: &Path,
) -> Result<()> {
    std::fs::create_dir_all(local_path)?;
    let log_path = pgdata.join("branch.log");
    let cwd = std::env::current_dir().unwrap_or_default();
    let storage_root_abs = cwd.join(storage_root_path());
    let branch_local_abs = cwd.join(local_path);

    let org_id = env::read_u64(env::ENV_ORG_ID);
    let status = Command::new(pg_ctl)
        .arg("start")
        .arg("-D")
        .arg(pgdata)
        .arg("-l")
        .arg(&log_path)
        .arg("-w")
        .arg("-o")
        .arg(format!("-c port={}", branch_port))
        .env("TIKO_ORG_ID", org_id.to_string())
        .env("TIKO_DB_ID", db_id.to_string())
        .env("TIKO_PROJECT_ID", project_id.to_string())
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
    Ok(())
}
