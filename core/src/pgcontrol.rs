//! Read/patch a PostgreSQL `pg_control` and synthesize a `backup_label`, so a
//! tiko checkpoint snapshot can be recovered as a base backup (consistency at
//! the base checkpoint). PG18 (`PG_CONTROL_VERSION` 1800) layout; all patching
//! is guarded at runtime by the version field so an unknown layout is never
//! modified.

use chrono::{DateTime, Utc};
use pgsys::common::XLOG_SEG_SIZE;
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

/// WAL segments per logical xlog id: 2^32 / XLOG_SEG_SIZE (= 256 for 16 MiB).
const SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;

/// PostgreSQL WAL segment file name: `{tli:08X}{logid:08X}{logseg:08X}`, where
/// `logid = seg_no / SEGS_PER_LOGID`, `logseg = seg_no % SEGS_PER_LOGID`.
pub fn xlog_file_name(tli: TimelineId, seg_no: u64) -> String {
    format!(
        "{:08X}{:08X}{:08X}",
        tli.as_u32(),
        seg_no / SEGS_PER_LOGID,
        seg_no % SEGS_PER_LOGID
    )
}

/// Build a `backup_label` presenting a tiko checkpoint snapshot as a base
/// backup. Uses the standby end-of-backup path (`BACKUP FROM: standby`), so
/// recovery reaches consistency at `pg_control.minRecoveryPoint` (set to the
/// base checkpoint by [`shape_for_backup_recovery`]) with no `XLOG_BACKUP_END`
/// record. Mirrors PostgreSQL's `build_backup_content` line format.
pub fn backup_label(
    redo: Lsn,
    checkpoint: Lsn,
    tli: TimelineId,
    start_time: DateTime<Utc>,
) -> String {
    let seg = xlog_file_name(tli, redo.as_u64() / XLOG_SEG_SIZE as u64);
    format!(
        "START WAL LOCATION: {redo} (file {seg})\n\
         CHECKPOINT LOCATION: {ckpt}\n\
         BACKUP METHOD: streamed\n\
         BACKUP FROM: standby\n\
         START TIME: {time}\n\
         LABEL: tiko_pitr\n\
         START TIMELINE: {tl}\n",
        redo = redo.to_pg_string(),
        seg = seg,
        ckpt = checkpoint.to_pg_string(),
        time = start_time.format("%Y-%m-%d %H:%M:%S UTC"),
        tl = tli.as_u32(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlog_file_name_format() {
        let tl = TimelineId::new(1);
        assert_eq!(xlog_file_name(tl, 2), "000000010000000000000002");
        assert_eq!(xlog_file_name(tl, 256), "000000010000000100000000");
    }

    #[test]
    fn backup_label_lines() {
        let t = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let s = backup_label(
            Lsn::new(0x2038660),
            Lsn::new(0x20386B8),
            TimelineId::new(1),
            t,
        );
        assert!(s.contains("START WAL LOCATION: 0/2038660 (file 000000010000000000000002)"));
        assert!(s.contains("CHECKPOINT LOCATION: 0/20386B8"));
        assert!(s.contains("BACKUP METHOD: streamed"));
        assert!(s.contains("BACKUP FROM: standby"));
        assert!(s.contains("START TIMELINE: 1"));
    }
}
