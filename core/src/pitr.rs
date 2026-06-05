//! Point-in-time recovery (PITR) helpers used by the `tiko_pitr` CLI.
//!
//! Two concerns live here, both pure filesystem/string operations with no
//! dependency on the running `Store`, so they are unit-testable directly:
//!
//! 1. Editing `postgresql.tiko.conf`: writing a marker-delimited PITR recovery
//!    block (`write_pitr_recovery_conf`) and stripping it again
//!    (`remove_recovery_conf`).
//! 2. Crash-safe PGDATA snapshot/restore that excludes the bulk `tiko/`
//!    directory (`backup_dir_excluding` / `restore_dir`).

use std::ffi::OsStr;
use std::fs;
use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

use crate::error::{Error, Result};

/// Name of the Tiko-managed include file under PGDATA. PostgreSQL must already
/// `include`/`include_if_exists` this from `postgresql.conf`.
pub const TIKO_CONF_FILE: &str = "postgresql.tiko.conf";

const RECOVERY_CONF_BEGIN: &str = "# Tiko recovery settings — begin\n";
const RECOVERY_CONF_END: &str = "# Tiko recovery settings — end\n";

/// Append a Tiko PITR recovery block to `conf_path`, delimited by begin/end
/// markers so [`remove_recovery_conf`] can strip it cleanly later.
///
/// Drives archive recovery up to `target_lsn` on `target_tl`, pulling WAL
/// segments from remote via `tiko_restore`. `recovery_target_action='shutdown'`
/// makes PostgreSQL shut itself down the instant it reaches the target.
///
/// Note: this function does **not** check for an existing block; callers should
/// call [`remove_recovery_conf`] first if the file may already contain one.
pub fn write_pitr_recovery_conf(
    conf_path: &Path,
    target_tl: TimelineId,
    target_lsn: Lsn,
) -> Result<()> {
    let snippet = format!(
        "\n{begin}\
         restore_command = 'tiko_restore %f %p'\n\
         recovery_target_lsn = '{lsn}'\n\
         recovery_target_timeline = '{tl}'\n\
         recovery_target_inclusive = on\n\
         recovery_target_action = 'shutdown'\n\
         {end}",
        begin = RECOVERY_CONF_BEGIN,
        end = RECOVERY_CONF_END,
        lsn = target_lsn.to_pg_string(),
        tl = target_tl.as_u32(),
    );
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    fs::write(conf_path, format!("{existing}{snippet}"))?;
    Ok(())
}

/// Remove the marker-delimited block previously written by
/// [`write_pitr_recovery_conf`]. No-op if the markers are absent.
pub fn remove_recovery_conf(conf_path: &Path) -> Result<()> {
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    let Some(begin_off) = existing.find(RECOVERY_CONF_BEGIN) else {
        return Ok(());
    };
    // Also consume the preceding newline that write_pitr_recovery_conf inserts.
    let start = if begin_off > 0 && existing.as_bytes()[begin_off - 1] == b'\n' {
        begin_off - 1
    } else {
        begin_off
    };
    let end_off = existing[begin_off..]
        .find(RECOVERY_CONF_END)
        .map(|p| begin_off + p + RECOVERY_CONF_END.len())
        .unwrap_or(existing.len());
    let cleaned = format!("{}{}", &existing[..start], &existing[end_off..]);
    fs::write(conf_path, cleaned)?;
    Ok(())
}

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

/// Recursively copy `src` into a fresh `dst`, skipping any top-level entry named
/// `exclude_name` (e.g. `"tiko"`, the bulk data dir backed by remote storage).
///
/// Errors if `dst` already exists, so a stale backup from an interrupted run is
/// never silently overwritten.
pub fn backup_dir_excluding(src: &Path, dst: &Path, exclude_name: &str) -> Result<()> {
    if dst.exists() {
        return Err(Error::already_exists(format!(
            "backup dir already exists: {} (inspect/remove it before retrying)",
            dst.display()
        )));
    }
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_name() == OsStr::new(exclude_name) {
            continue;
        }
        copy_recursive(&entry.path(), &dst.join(entry.file_name()))?;
    }
    Ok(())
}

/// Restore `dst` from `backup`: delete every top-level entry in `dst` except
/// `exclude_name`, then copy the backup's contents back in. After this, `dst`
/// matches the snapshot for everything except the preserved `exclude_name` dir.
///
/// # Failure handling
///
/// This is not atomic: it deletes then re-copies in place, so an error or
/// crash partway through leaves `dst` torn (some originals gone, some backup
/// entries not yet written). The caller MUST keep `backup` intact until this
/// returns `Ok`, and re-run the restore on failure rather than deleting the
/// backup. (`tiko_pitr` only removes the backup after a successful restore.)
pub fn restore_dir(backup: &Path, dst: &Path, exclude_name: &str) -> Result<()> {
    for entry in fs::read_dir(dst)? {
        let entry = entry?;
        if entry.file_name() == OsStr::new(exclude_name) {
            continue;
        }
        let p = entry.path();
        if fs::symlink_metadata(&p)?.file_type().is_dir() {
            fs::remove_dir_all(&p)?;
        } else {
            fs::remove_file(&p)?;
        }
    }
    for entry in fs::read_dir(backup)? {
        let entry = entry?;
        copy_recursive(&entry.path(), &dst.join(entry.file_name()))?;
    }
    Ok(())
}

/// Recursively copy `from` to `to`, creating directories as needed. Symlinks
/// are dereferenced (copied as their target's contents) — adequate for a PGDATA
/// minus `tiko/`, which holds no tablespace symlinks in this deployment.
fn copy_recursive(from: &Path, to: &Path) -> Result<()> {
    let ft = fs::symlink_metadata(from)?.file_type();
    if ft.is_dir() {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            copy_recursive(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else {
        fs::copy(from, to)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pitr_conf_round_trips_through_remove() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(TIKO_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        write_pitr_recovery_conf(&conf, TimelineId::new(2), Lsn::new(0x3000028)).unwrap();
        let with = fs::read_to_string(&conf).unwrap();
        assert!(with.contains("restore_command = 'tiko_restore %f %p'"));
        assert!(with.contains("recovery_target_lsn = '0/3000028'"));
        assert!(with.contains("recovery_target_timeline = '2'"));
        assert!(with.contains("recovery_target_action = 'shutdown'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }

    #[test]
    fn parse_pg_timestamp_handles_common_formats() {
        assert_eq!(parse_pg_timestamp("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-02").unwrap(), 86_400);
        // RFC3339 with offset: 01:00+01:00 == 00:00 UTC == epoch.
        assert_eq!(parse_pg_timestamp("1970-01-01T01:00:00+01:00").unwrap(), 0);
        assert!(parse_pg_timestamp("not a timestamp").is_err());
    }

    #[test]
    fn backup_excludes_tiko_and_restore_round_trips() {
        let root = tempfile::tempdir().unwrap();
        let pgdata = root.path().join("pgdata");
        fs::create_dir_all(pgdata.join("global")).unwrap();
        fs::write(pgdata.join("PG_VERSION"), "16\n").unwrap();
        fs::write(pgdata.join("global/pg_control"), b"orig").unwrap();
        fs::create_dir_all(pgdata.join("tiko/s3sim")).unwrap();
        fs::write(pgdata.join("tiko/s3sim/blob"), b"bigdata").unwrap();

        let bak = root.path().join("pgdata.tiko_pitr_bak");
        backup_dir_excluding(&pgdata, &bak, "tiko").unwrap();
        assert!(bak.join("PG_VERSION").exists());
        assert!(bak.join("global/pg_control").exists());
        assert!(!bak.join("tiko").exists(), "tiko/ must be excluded from backup");

        // A second backup must refuse to overwrite.
        assert!(backup_dir_excluding(&pgdata, &bak, "tiko").is_err());

        // Mutate PGDATA as a recovery run would.
        fs::write(pgdata.join("global/pg_control"), b"MUTATED").unwrap();
        fs::write(pgdata.join("recovery.signal"), b"").unwrap();

        restore_dir(&bak, &pgdata, "tiko").unwrap();
        assert_eq!(fs::read(pgdata.join("global/pg_control")).unwrap(), b"orig");
        assert!(!pgdata.join("recovery.signal").exists(), "restore must drop new files");
        assert!(pgdata.join("tiko/s3sim/blob").exists(), "tiko/ must be left untouched");
    }
}
