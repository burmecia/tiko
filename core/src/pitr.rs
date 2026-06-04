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

use std::fs;
use std::path::Path;

use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

use crate::error::Result;

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
}
