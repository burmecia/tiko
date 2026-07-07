//! Shared PostgreSQL/backup helper ops used by the `tiko_pitr` and
//! `tiko_branch` operator binaries: `pg_basebackup` invocation, `backup_label`
//! parsing, tar.zst pack/unpack, `pg_ctl` start/stop, and promotion polling.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use core::error::{Error, Result};
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

/// Connection + mode options for [`run_pg_basebackup`].
pub struct BasebackupOpts<'a> {
    pub pg_basebackup: &'a Path,
    /// `pg_basebackup -h`; empty means the local unix socket.
    pub host: &'a str,
    pub port: u16,
    /// `pg_basebackup -U`; `None` = pg_basebackup's default (current OS user).
    pub user: Option<&'a str>,
    /// `pg_basebackup -c`: `"fast"` or `"spread"`.
    pub checkpoint: &'a str,
    /// `pg_basebackup -X`: `"none"`, `"stream"`, or `"fetch"`.
    pub wal_method: &'a str,
}

/// Invoke `pg_basebackup` to produce a plain-format base backup in `dest`.
///
/// `-c fast` triggers the `CHECKPOINT_CAUSE_BASEBACKUP` checkpoint that the
/// Tiko checkpointer hooks to form a base manifest at the backup LSN.
pub fn run_pg_basebackup(opts: &BasebackupOpts<'_>, dest: &Path) -> Result<()> {
    let mut cmd = Command::new(opts.pg_basebackup);
    cmd.arg("-D").arg(dest);
    cmd.args(["-X", opts.wal_method]);
    cmd.args(["-F", "p"]); // plain format (directory)
    cmd.args(["-c", opts.checkpoint]); // triggers CHECKPOINT_CAUSE_BASEBACKUP
    cmd.args(["--no-password", "--no-manifest"]);
    if !opts.host.is_empty() {
        cmd.args(["-h", opts.host]);
    }
    cmd.args(["-p", &opts.port.to_string()]);
    if let Some(user) = opts.user {
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
pub fn parse_backup_label(label: &str) -> Result<(Lsn, Lsn, TimelineId)> {
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
pub fn tar_dir_to_zst(src: &Path) -> Result<Vec<u8>> {
    let tar_buf: Vec<u8> = Vec::new();
    let mut builder = tar::Builder::new(tar_buf);
    builder.append_dir_all(".", src)?;
    builder.finish()?;
    let tar_buf = builder
        .into_inner()
        .map_err(|e| Error::other(format!("tar finalize: {e}")))?;
    zstd::encode_all(tar_buf.as_slice(), 3).map_err(|e| Error::other(format!("zstd compress: {e}")))
}

/// Decompress (zstd) and extract a base-backup tarball into `dest`.
pub fn extract_backup(tar_zst: &[u8], dest: &Path) -> Result<()> {
    let tar_buf = zstd::decode_all(tar_zst)
        .map_err(|e| Error::other(format!("zstd decompress base backup: {e}")))?;
    let mut arch = tar::Archive::new(tar_buf.as_slice());
    arch.unpack(dest)
        .map_err(|e| Error::other(format!("tar unpack base backup: {e}")))
}

/// Poll `SELECT pg_is_in_recovery()` once per second until it returns `f`
/// (promotion complete) or `timeout_secs` elapses. Connects to the given
/// `port` over the local socket.
pub fn wait_for_promotion(psql: &Path, port: u16, timeout_secs: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match run_psql(psql, port, "SELECT pg_is_in_recovery()") {
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

/// Run `psql -p <port> -d postgres -Atqc <sql>` and return stdout.
pub fn run_psql(psql: &Path, port: u16, sql: &str) -> Result<String> {
    let out = Command::new(psql)
        .args(["-p", &port.to_string()])
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
pub fn stop_pg(pg_ctl: &Path, pgdata: &Path) -> Result<()> {
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
    if !pgdata.join("postmaster.pid").exists() {
        return Ok(());
    }
    Err(Error::other(
        "pg_ctl stop failed and postmaster.pid is still present",
    ))
}

/// `pg_ctl -D <pgdata> [-l <log_file>] -w start` for normal startup.
///
/// When `log_file` is `Some`, the postmaster's stdout/stderr are redirected to
/// that file via pg_ctl's `-l` (which appends with `>> ... 2>&1`). When it is
/// `None`, pg_ctl leaves the postmaster's stderr attached to *this* process's
/// stderr — which, per `pg_ctl.c`'s `start_postmaster()` (no `-l` branch), is
/// not redirected to `/dev/null`. That is rarely what you want: postgres
/// `log_min_messages=debug1` output would spill to the caller's stderr, and in
/// `tiko_pitr`'s case end up folded into tikoguest's HTTP error responses.
/// Pass a log file unless you have a reason not to.
pub fn start_pg(pg_ctl: &Path, pgdata: &Path, log_file: Option<&Path>) -> Result<()> {
    let mut cmd = Command::new(pg_ctl);
    cmd.arg("start").arg("-D").arg(pgdata);
    if let Some(log) = log_file {
        cmd.arg("-l").arg(log);
    }
    cmd.arg("-w");
    let status = cmd
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
pub fn sibling_binary(pg_ctl: &Path, name: &str) -> PathBuf {
    match pg_ctl.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(name),
        _ => PathBuf::from(name),
    }
}
