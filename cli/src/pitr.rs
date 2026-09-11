//! Point-in-time recovery (PITR) helpers used by the `tiko_pitr` CLI.
//!
//! Two concerns live here, both pure filesystem/string operations with no
//! dependency on the running `Store`, so they are unit-testable directly:
//!
//! 1. Editing `postgresql.auto.conf`: writing a marker-delimited PITR recovery
//!    block (`write_pitr_recovery_conf`) and stripping it again
//!    (`remove_recovery_conf`).
//! 2. PGDATA snapshot/restore that excludes the bulk `tiko/` directory
//!    (`backup_dir_excluding` / `restore_dir`). Neither is atomic: on failure
//!    the caller must keep the snapshot and re-run the restore.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

use core::error::{Error, Result};

/// File under PGDATA that the PITR recovery block is written to.
///
/// `postgresql.auto.conf` is always read by PostgreSQL (processed last, so it
/// has the highest precedence) and exists in every data dir, so the recovery
/// settings take effect without requiring any `include` in `postgresql.conf`.
/// The block is delimited by markers and stripped by [`remove_recovery_conf`]
/// after recovery, leaving the file's other (ALTER SYSTEM) contents intact.
pub const RECOVERY_CONF_FILE: &str = "postgresql.auto.conf";

const RECOVERY_CONF_BEGIN: &str = "# Tiko recovery settings — begin\n";
const RECOVERY_CONF_END: &str = "# Tiko recovery settings — end\n";

/// A PITR recovery target: stop replay at a specific LSN, or at a timestamp.
///
/// `Time` carries a Unix-seconds instant (UTC), not a wall-clock string, so the
/// target is timezone-unambiguous end to end (`parse_pg_timestamp` produces it,
/// `format_recovery_target_time` renders it with an explicit offset for PG).
#[derive(Debug, Clone, Copy)]
pub enum RecoveryTarget {
    Lsn(Lsn),
    Time(i64),
}

/// Render a Unix-seconds instant as a PostgreSQL `timestamptz` literal in UTC
/// with an explicit `+00:00` offset.
///
/// `recovery_target_time` is a `timestamptz`: a literal **without** an offset is
/// interpreted in the server's `timezone`, which would silently disagree with
/// the UTC instant `tiko_pitr` selected (and that `tiko_pitr list` displays).
/// Emitting the offset makes the target unambiguous regardless of server tz.
pub fn format_recovery_target_time(unix_ts: i64) -> Result<String> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(unix_ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%:z").to_string())
        .ok_or_else(|| Error::other(format!("invalid recovery target timestamp: {unix_ts}")))
}

/// Append a Tiko PITR recovery block to `conf_path`, delimited by begin/end
/// markers so [`remove_recovery_conf`] can strip it cleanly later.
///
/// Drives archive recovery up to `target` on `timeline`, pulling WAL segments
/// from remote via `restore_bin` (the `tiko_restore` binary). When `promote`
/// is true, `recovery_target_action='promote'` so PostgreSQL ends recovery by
/// creating a new timeline and continuing as a writable primary; otherwise
/// `'shutdown'` halts at the target.
///
/// `restore_bin` is emitted as an absolute path so PostgreSQL's shell finds it
/// regardless of `PATH`; it is double-quoted to tolerate spaces in the path.
///
/// Any block previously written by this function is stripped first, so calling
/// it repeatedly can't stack duplicate recovery blocks. The file is replaced
/// atomically (temp + rename) to avoid leaving a torn `postgresql.auto.conf`.
pub fn write_pitr_recovery_conf(
    conf_path: &Path,
    timeline: TimelineId,
    target: &RecoveryTarget,
    restore_bin: &Path,
    promote: bool,
) -> Result<()> {
    let target_line = match target {
        RecoveryTarget::Lsn(lsn) => format!("recovery_target_lsn = '{}'\n", lsn.to_pg_string()),
        RecoveryTarget::Time(ts) => {
            format!(
                "recovery_target_time = '{}'\n",
                format_recovery_target_time(*ts)?
            )
        }
    };
    let action = if promote { "promote" } else { "shutdown" };
    let snippet = format!(
        "\n{begin}\
         restore_command = '\"{restore}\" %f %p'\n\
         {target_line}\
         recovery_target_timeline = '{tl}'\n\
         recovery_target_inclusive = on\n\
         recovery_target_action = '{action}'\n\
         {end}",
        begin = RECOVERY_CONF_BEGIN,
        end = RECOVERY_CONF_END,
        restore = restore_bin.display(),
        tl = timeline.as_u32(),
    );
    remove_recovery_conf(conf_path)?;
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    write_atomic(conf_path, format!("{existing}{snippet}").as_bytes())?;
    Ok(())
}

/// Remove the marker-delimited block previously written by
/// [`write_pitr_recovery_conf`]. No-op if the markers are absent.
///
/// Errors (rather than stripping to EOF) if a begin marker has no matching end
/// marker: a torn/stray block must not silently discard trailing settings.
pub fn remove_recovery_conf(conf_path: &Path) -> Result<()> {
    let existing = fs::read_to_string(conf_path).unwrap_or_default();
    let Some(begin_off) = existing.find(RECOVERY_CONF_BEGIN) else {
        return Ok(());
    };
    let Some(end_rel) = existing[begin_off..].find(RECOVERY_CONF_END) else {
        return Err(Error::other(
            "postgresql.auto.conf has a Tiko recovery block with no end marker",
        ));
    };
    // Also consume the preceding newline that write_pitr_recovery_conf inserts.
    let start = if begin_off > 0 && existing.as_bytes()[begin_off - 1] == b'\n' {
        begin_off - 1
    } else {
        begin_off
    };
    let end_off = begin_off + end_rel + RECOVERY_CONF_END.len();
    let cleaned = format!("{}{}", &existing[..start], &existing[end_off..]);
    write_atomic(conf_path, cleaned.as_bytes())?;
    Ok(())
}

/// Write `data` to `path` atomically: a sibling temp file followed by a rename,
/// preserving the existing file's permissions if it exists. A crash mid-write
/// must never leave a torn `postgresql.auto.conf`.
fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = temp_sibling(path);
    fs::write(&tmp, data)?;
    if let Ok(meta) = fs::metadata(path) {
        let _ = fs::set_permissions(&tmp, meta.permissions());
    }
    match fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e.into())
        }
    }
}

/// Build a unique temp path next to `path` (same directory, so rename is atomic).
fn temp_sibling(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".tiko.{}.tmp", std::process::id()));
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(name),
        _ => PathBuf::from(name),
    }
}

/// Parse a `--time` recovery-target string to a Unix timestamp (seconds).
///
/// Accepts RFC3339/ISO with an explicit offset (honored) — with either a `T` or
/// space date/time separator — or a bare `YYYY-MM-DD[ T]HH:MM[:SS[.fff]]` /
/// `YYYY-MM-DD` which is interpreted as UTC. Sub-second digits are accepted but
/// truncated (targets are second-resolution). Used only to compare a target
/// against the recoverable window and to select the base manifest; PostgreSQL
/// re-parses `recovery_target_time` authoritatively during replay.
pub fn parse_pg_timestamp(s: &str) -> Result<i64> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    // Space-separated date/time with an explicit offset (PostgreSQL's own
    // display form), with optional fractional seconds.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M%:z"] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Ok(dt.timestamp());
        }
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
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
/// never silently overwritten. Not atomic: a crash mid-copy leaves a partial
/// `dst` that the `exists` guard then blocks until an operator removes it.
///
/// `pg_wal/` is included on purpose: the snapshot must be a restartable PGDATA,
/// and crash recovery on restart needs the WAL from the shutdown checkpoint — it
/// is not re-fetched (no `restore_command` is configured outside recovery). The
/// default `TIKO_STORAGE_ROOT`/`TIKO_LOCAL_PATH` live under `PGDATA/tiko`, which
/// the caller excludes; a differently-named in-PGDATA store would be copied.
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

/// Delete every top-level entry in `dst` except `exclude_name`. Used by PITR
/// restore to clear PGDATA while preserving the bulk `tiko/` directory.
pub fn wipe_dir_excluding(dst: &Path, exclude_name: &str) -> Result<()> {
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
    wipe_dir_excluding(dst, exclude_name)?;
    for entry in fs::read_dir(backup)? {
        let entry = entry?;
        copy_recursive(&entry.path(), &dst.join(entry.file_name()))?;
    }
    Ok(())
}

/// Recursively copy `from` to `to`, creating directories as needed.
///
/// Symlinks are recreated as links (so `pg_tblspc/*` and similar survive), and
/// directory modes are preserved. Special files (sockets/FIFOs/devices) hold no
/// snapshot-worthy data and are skipped.
fn copy_recursive(from: &Path, to: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(from)?;
    let ft = meta.file_type();
    if ft.is_dir() {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            copy_recursive(&entry.path(), &to.join(entry.file_name()))?;
        }
        // Set the mode after populating the dir: a read-only source dir would
        // otherwise prevent writing its children.
        fs::set_permissions(to, meta.permissions())?;
    } else if ft.is_symlink() {
        copy_symlink(from, to)?;
    } else if ft.is_file() {
        // `fs::copy` preserves the file's mode bits.
        fs::copy(from, to)?;
    }
    Ok(())
}

#[cfg(unix)]
fn copy_symlink(from: &Path, to: &Path) -> Result<()> {
    let target = fs::read_link(from)?;
    // Recreating a link fails if the path already exists; clear a stale entry.
    let _ = fs::remove_file(to);
    std::os::unix::fs::symlink(&target, to)?;
    Ok(())
}

#[cfg(not(unix))]
fn copy_symlink(from: &Path, to: &Path) -> Result<()> {
    // No portable link recreation here; dereference as a fallback.
    fs::copy(from, to)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::common::RECOVERY_SIGNAL_FILE;

    #[test]
    fn pitr_conf_round_trips_through_remove() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(RECOVERY_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        write_pitr_recovery_conf(
            &conf,
            TimelineId::new(2),
            &RecoveryTarget::Lsn(Lsn::new(0x3000028)),
            Path::new("/opt/tiko/bin/tiko_restore"),
            false,
        )
        .unwrap();
        let with = fs::read_to_string(&conf).unwrap();
        assert!(with.contains("restore_command = '\"/opt/tiko/bin/tiko_restore\" %f %p'"));
        assert!(with.contains("recovery_target_lsn = '0/3000028'"));
        assert!(with.contains("recovery_target_timeline = '2'"));
        assert!(with.contains("recovery_target_action = 'shutdown'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }

    #[test]
    fn pitr_conf_time_target_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(RECOVERY_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        // 0 = 1970-01-01 00:00:00 UTC; rendered with an explicit +00:00 offset.
        write_pitr_recovery_conf(
            &conf,
            TimelineId::new(1),
            &RecoveryTarget::Time(0),
            Path::new("/opt/tiko/bin/tiko_restore"),
            false,
        )
        .unwrap();
        let with = fs::read_to_string(&conf).unwrap();
        assert!(with.contains("recovery_target_time = '1970-01-01 00:00:00+00:00'"));
        assert!(!with.contains("recovery_target_lsn"));
        assert!(with.contains("recovery_target_timeline = '1'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }

    #[test]
    fn format_recovery_target_time_is_explicit_utc() {
        assert_eq!(
            format_recovery_target_time(0).unwrap(),
            "1970-01-01 00:00:00+00:00"
        );
        // parse (bare → UTC) and format (UTC + explicit offset) are inverse, so
        // PostgreSQL reads back the exact instant tiko_pitr selected.
        let ts = parse_pg_timestamp("2026-06-08 13:40:00").unwrap();
        assert_eq!(
            format_recovery_target_time(ts).unwrap(),
            "2026-06-08 13:40:00+00:00"
        );
    }

    #[test]
    fn parse_pg_timestamp_handles_common_formats() {
        assert_eq!(parse_pg_timestamp("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-02").unwrap(), 86_400);
        // Minute-precision (no seconds), both separators → UTC.
        assert_eq!(parse_pg_timestamp("1970-01-01 00:01").unwrap(), 60);
        assert_eq!(parse_pg_timestamp("1970-01-01T00:01").unwrap(), 60);
        // RFC3339 with offset: 01:00+01:00 == 00:00 UTC == epoch.
        assert_eq!(parse_pg_timestamp("1970-01-01T01:00:00+01:00").unwrap(), 0);
        // Space-separated date/time with an offset (PostgreSQL display form).
        assert_eq!(parse_pg_timestamp("1970-01-01 01:00:00+01:00").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-01 00:01+00:00").unwrap(), 60);
        // Sub-second precision is accepted and truncated to seconds.
        assert_eq!(parse_pg_timestamp("1970-01-01 00:00:00.500").unwrap(), 0);
        assert_eq!(parse_pg_timestamp("1970-01-01T00:00:00.999").unwrap(), 0);
        assert!(parse_pg_timestamp("not a timestamp").is_err());
    }

    #[test]
    fn remove_recovery_conf_noop_without_markers() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(RECOVERY_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        remove_recovery_conf(&conf).unwrap();
        assert_eq!(
            fs::read_to_string(&conf).unwrap(),
            "shared_buffers = 128MB\n"
        );
    }

    #[test]
    fn write_pitr_recovery_conf_replaces_existing_block() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(RECOVERY_CONF_FILE);
        fs::write(&conf, "shared_buffers = 128MB\n").unwrap();
        let before = fs::read_to_string(&conf).unwrap();

        for lsn in [0x3000028u64, 0x4000028] {
            write_pitr_recovery_conf(
                &conf,
                TimelineId::new(1),
                &RecoveryTarget::Lsn(Lsn::new(lsn)),
                Path::new("/opt/tiko/bin/tiko_restore"),
                true,
            )
            .unwrap();
        }
        let with = fs::read_to_string(&conf).unwrap();
        assert_eq!(
            with.matches(RECOVERY_CONF_BEGIN).count(),
            1,
            "a repeated write must not stack blocks"
        );
        assert!(with.contains("recovery_target_lsn = '0/4000028'"));

        remove_recovery_conf(&conf).unwrap();
        assert_eq!(fs::read_to_string(&conf).unwrap(), before);
    }

    #[test]
    fn remove_recovery_conf_dangling_begin_errors() {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join(RECOVERY_CONF_FILE);
        let content = format!("a = 1\n{RECOVERY_CONF_BEGIN}restore_command = 'x'\n");
        fs::write(&conf, &content).unwrap();

        assert!(remove_recovery_conf(&conf).is_err());
        // The trailing settings must be left untouched on error.
        assert_eq!(fs::read_to_string(&conf).unwrap(), content);
    }

    #[cfg(unix)]
    #[test]
    fn backup_restore_preserves_symlinks_and_dir_modes() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let pgdata = root.path().join("pgdata");
        fs::create_dir_all(pgdata.join("pg_tblspc")).unwrap();
        fs::write(pgdata.join("PG_VERSION"), "18\n").unwrap();
        std::os::unix::fs::symlink("/mnt/ts1", pgdata.join("pg_tblspc/12345")).unwrap();
        fs::set_permissions(pgdata.join("pg_tblspc"), fs::Permissions::from_mode(0o700)).unwrap();

        let bak = root.path().join("bak");
        backup_dir_excluding(&pgdata, &bak, "tiko").unwrap();
        let link = bak.join("pg_tblspc/12345");
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_link(&link).unwrap(), PathBuf::from("/mnt/ts1"));

        wipe_dir_excluding(&pgdata, "tiko").unwrap();
        restore_dir(&bak, &pgdata, "tiko").unwrap();

        let restored = pgdata.join("pg_tblspc/12345");
        assert!(
            fs::symlink_metadata(&restored)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_link(&restored).unwrap(), PathBuf::from("/mnt/ts1"));
        assert_eq!(
            fs::metadata(pgdata.join("pg_tblspc"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "directory mode must round-trip"
        );
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
        assert!(
            !bak.join("tiko").exists(),
            "tiko/ must be excluded from backup"
        );

        // A second backup must refuse to overwrite.
        assert!(backup_dir_excluding(&pgdata, &bak, "tiko").is_err());

        // Mutate PGDATA as a recovery run would.
        fs::write(pgdata.join("global/pg_control"), b"MUTATED").unwrap();
        fs::write(pgdata.join(RECOVERY_SIGNAL_FILE), b"").unwrap();

        restore_dir(&bak, &pgdata, "tiko").unwrap();
        assert_eq!(fs::read(pgdata.join("global/pg_control")).unwrap(), b"orig");
        assert!(
            !pgdata.join(RECOVERY_SIGNAL_FILE).exists(),
            "restore must drop new files"
        );
        assert!(
            pgdata.join("tiko/s3sim/blob").exists(),
            "tiko/ must be left untouched"
        );
    }
}
