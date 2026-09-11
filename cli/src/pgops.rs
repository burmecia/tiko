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
/// Starting a base backup always requests a `CHECKPOINT_CAUSE_BASEBACKUP`
/// checkpoint (`do_pg_backup_start` in xlog.c), which the Tiko checkpointer hooks
/// to form a base manifest at the backup LSN. The `-c` mode only affects how
/// that checkpoint runs: `fast` adds `CHECKPOINT_IMMEDIATE`, `spread` (the
/// pg_basebackup default) does not. Both trigger the hook.
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
    let t = token
        .parse::<u32>()
        .map_err(|_| Error::other(format!("invalid timeline in backup_label: '{token}'")))?;
    Ok(TimelineId::new(t))
}

/// Return the first whitespace-delimited token following `prefix` on any line.
fn first_token_after<'a>(label: &'a str, prefix: &str) -> Option<&'a str> {
    for line in label.lines() {
        if let Some(rest) = line.trim_start().strip_prefix(prefix) {
            return rest.split_whitespace().next();
        }
    }
    None
}

/// Pack a directory into a compressed `tar.zst` blob in memory.
///
/// The tar stream is fed straight into the zstd encoder, so the uncompressed
/// archive is never buffered in full (only the compressed result is).
/// `follow_symlinks(false)` stores symlinks as links instead of expanding their
/// targets (e.g. `pg_tblspc/*` tablespace links), and special files
/// (sockets/FIFOs) become placeholder entries rather than being read.
pub fn tar_dir_to_zst(src: &Path) -> Result<Vec<u8>> {
    let encoder = zstd::Encoder::new(Vec::new(), 3)
        .map_err(|e| Error::other(format!("zstd encoder init: {e}")))?;
    let mut builder = tar::Builder::new(encoder);
    builder.follow_symlinks(false);
    builder.append_dir_all(".", src)?;
    builder.finish()?;
    let encoder = builder
        .into_inner()
        .map_err(|e| Error::other(format!("tar finalize: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::other(format!("zstd compress: {e}")))
}

/// Decompress (zstd) and extract a base-backup tarball into `dest`.
///
/// Unpacks over `dest`, creating it if needed. Pre-existing entries are replaced
/// by archive members, but entries *absent* from the archive are left in place.
/// Callers that need a pristine tree must clear `dest` first — and must not
/// clear any directory intentionally preserved across the restore (e.g.
/// `PGDATA/tiko`, which can hold the block store when `TIKO_STORAGE_ROOT` is
/// unset).
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
///
/// Fails fast if `pgdata/postmaster.pid` disappears (a cleanly-exited postmaster
/// aborts recovery by removing it) instead of burning the whole timeout. Note
/// the caller is assumed to own `port`: if some other primary is already
/// listening there, `pg_ctl start` would have failed to bind, so a successful
/// poll is taken to mean our recovering instance.
pub fn wait_for_promotion(psql: &Path, pgdata: &Path, port: u16, timeout_secs: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match run_psql(psql, port, "SELECT pg_is_in_recovery()") {
            Ok(out) if out.trim() == "f" => return Ok(()),
            // "t" = still in recovery; a query error = not accepting connections
            // yet (early startup) or a transient blip. Keep polling until the
            // deadline.
            _ => {}
        }
        if !pgdata.join("postmaster.pid").exists() {
            return Err(Error::other(
                "PostgreSQL postmaster is no longer running during recovery",
            ));
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
///
/// `PGCONNECT_TIMEOUT` bounds connection establishment so a hung or unreachable
/// server can't block the poll loop indefinitely.
fn run_psql(psql: &Path, port: u16, sql: &str) -> Result<String> {
    let out = Command::new(psql)
        .args(["-p", &port.to_string()])
        .args(["-d", "postgres"])
        .args(["-Atqc", sql])
        .env("PGCONNECT_TIMEOUT", "5")
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
///
/// `-s` keeps pg_ctl's informational output off stdout: the CLI emits a single
/// JSON object there, and pg_ctl would otherwise interleave `waiting for server
/// to shut down...`/`server stopped` (pg_ctl.c `print_msg`), breaking consumers.
pub fn stop_pg(pg_ctl: &Path, pgdata: &Path) -> Result<()> {
    let status = Command::new(pg_ctl)
        .arg("stop")
        .arg("-s")
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

/// Options for [`start_pg`].
pub struct StartPgOpts<'a> {
    pub pg_ctl: &'a Path,
    pub pgdata: &'a Path,
    /// Postmaster log file (`pg_ctl -l`); `None` leaves the postmaster's
    /// stderr attached to this process's stderr. See [`start_pg`].
    pub log_file: Option<&'a Path>,
    /// Seconds to wait for the postmaster to become ready (`pg_ctl -t`).
    /// `None` uses pg_ctl's default (60 s). Recovery can take longer than that
    /// to reach a consistent, connectable state, so pass the recovery timeout.
    pub wait_secs: Option<u64>,
    /// Extra postmaster options (`pg_ctl -o`), e.g. `-c port=5433`.
    pub server_opts: Option<&'a str>,
    /// Extra environment variables for the started postmaster (e.g. `TIKO_*`).
    pub envs: &'a [(&'a str, String)],
}

/// `pg_ctl -s -D <pgdata> [-l <log_file>] [-t <secs>] [-o <opts>] -w start`.
///
/// When `log_file` is `Some`, the postmaster's stdout/stderr are redirected to
/// that file via pg_ctl's `-l` (which appends with `>> ... 2>&1`). When it is
/// `None`, pg_ctl leaves the postmaster's stderr attached to *this* process's
/// stderr — which, per `pg_ctl.c`'s `start_postmaster()` (no `-l` branch), is
/// not redirected to `/dev/null`. That is rarely what you want: postgres
/// `log_min_messages=debug1` output would spill to the caller's stderr, and in
/// `tiko_pitr`'s case end up folded into tikoguest's HTTP error responses.
/// Pass a log file unless you have a reason not to.
///
/// `-s` keeps pg_ctl's informational output off stdout: the CLI emits a single
/// JSON object there, and pg_ctl would otherwise interleave `waiting for server
/// to start...`/`server started` (pg_ctl.c `print_msg`), breaking consumers.
/// `-t` is forwarded so a slow recovery isn't capped at pg_ctl's 60 s default.
pub fn start_pg(opts: &StartPgOpts<'_>) -> Result<()> {
    let mut cmd = Command::new(opts.pg_ctl);
    cmd.arg("start").arg("-s").arg("-D").arg(opts.pgdata);
    if let Some(log) = opts.log_file {
        cmd.arg("-l").arg(log);
    }
    if let Some(secs) = opts.wait_secs {
        cmd.arg("-t").arg(secs.to_string());
    }
    if let Some(server_opts) = opts.server_opts {
        cmd.arg("-o").arg(server_opts);
    }
    for (key, value) in opts.envs {
        cmd.env(key, value);
    }
    cmd.arg("-w");
    let status = cmd
        .status()
        .map_err(|e| Error::other(format!("failed to spawn pg_ctl: {e}")))?;
    if !status.success() {
        return Err(Error::other(match opts.log_file {
            Some(log) => format!(
                "pg_ctl start failed (exit: {status}); see {}",
                log.display()
            ),
            None => format!("pg_ctl start failed (exit: {status})"),
        }));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const LABEL: &str = "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n\
                         CHECKPOINT LOCATION: 0/2000080\n\
                         BACKUP METHOD: streamed\n\
                         BACKUP FROM: primary\n\
                         START TIME: 2026-01-01 00:00:00 UTC\n\
                         LABEL: tiko\n\
                         START TIMELINE: 3\n";

    #[test]
    fn parse_backup_label_extracts_all_fields() {
        let (checkpoint, redo, timeline) = parse_backup_label(LABEL).unwrap();
        assert_eq!(checkpoint, Lsn::from_pg_string("0/2000080").unwrap());
        assert_eq!(redo, Lsn::from_pg_string("0/2000028").unwrap());
        assert_eq!(timeline, TimelineId::new(3));
    }

    #[test]
    fn parse_backup_label_rejects_missing_lines() {
        assert!(parse_backup_label("LABEL: tiko\n").is_err());

        let no_timeline = LABEL
            .lines()
            .filter(|l| !l.starts_with("START TIMELINE"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse_backup_label(&no_timeline).is_err());
    }

    #[test]
    fn first_token_after_stops_at_whitespace() {
        assert_eq!(first_token_after(LABEL, "START TIMELINE:"), Some("3"));
        assert_eq!(
            first_token_after(LABEL, "CHECKPOINT LOCATION:"),
            Some("0/2000080")
        );
        assert_eq!(first_token_after(LABEL, "MISSING:"), None);
    }

    #[test]
    fn sibling_binary_uses_pg_ctl_directory() {
        assert_eq!(
            sibling_binary(Path::new("/opt/pg/bin/pg_ctl"), "psql"),
            PathBuf::from("/opt/pg/bin/psql")
        );
        // A bare name has no parent dir, so it falls back to PATH lookup.
        assert_eq!(
            sibling_binary(Path::new("pg_ctl"), "psql"),
            PathBuf::from("psql")
        );
    }

    #[cfg(unix)]
    #[test]
    fn tar_round_trips_directory_and_preserves_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let src = root.path().join("pgdata");
        fs::create_dir_all(src.join("global")).unwrap();
        fs::write(src.join("PG_VERSION"), b"18\n").unwrap();
        fs::write(src.join("global/pg_control"), b"ctl").unwrap();
        std::os::unix::fs::symlink("PG_VERSION", src.join("version.link")).unwrap();

        let packed = tar_dir_to_zst(&src).unwrap();
        let dst = root.path().join("restored");
        extract_backup(&packed, &dst).unwrap();

        assert_eq!(fs::read(dst.join("PG_VERSION")).unwrap(), b"18\n");
        assert_eq!(fs::read(dst.join("global/pg_control")).unwrap(), b"ctl");
        assert!(
            fs::symlink_metadata(dst.join("version.link"))
                .unwrap()
                .file_type()
                .is_symlink(),
            "symlinks must round-trip as links, not be expanded"
        );
    }

    #[test]
    fn extract_backup_leaves_entries_absent_from_the_archive() {
        // Documents the non-clearing contract: callers that need a pristine
        // tree (e.g. branch restore) clear dest themselves.
        let root = tempfile::tempdir().unwrap();
        let src = root.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("from_archive"), b"new").unwrap();
        let packed = tar_dir_to_zst(&src).unwrap();

        let dst = root.path().join("dst");
        fs::create_dir_all(&dst).unwrap();
        fs::write(dst.join("stale"), b"old").unwrap();
        extract_backup(&packed, &dst).unwrap();

        assert_eq!(fs::read(dst.join("from_archive")).unwrap(), b"new");
        assert_eq!(fs::read(dst.join("stale")).unwrap(), b"old");
    }
}
