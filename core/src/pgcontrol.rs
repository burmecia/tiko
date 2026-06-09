//! Read/patch a PostgreSQL `pg_control` and synthesize a `backup_label`, so a
//! tiko checkpoint snapshot can be recovered as a base backup (consistency at
//! the base checkpoint). PG18 (`PG_CONTROL_VERSION` 1800) layout; all patching
//! is guarded at runtime by the version field so an unknown layout is never
//! modified.

use chrono::{DateTime, Utc};
use pgsys::common::XLOG_SEG_SIZE;
use pgsys::lsn::Lsn;
use pgsys::timeline_id::TimelineId;

use crate::error::{Error, Result};

/// WAL segments per logical xlog id: 2^32 / XLOG_SEG_SIZE (= 256 for 16 MiB).
const SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;

// PG18 ControlFileData layout (PG_CONTROL_VERSION 1800), confirmed via offsetof
// against the build's headers. `crc` is the last field; CRC covers [0, OFF_CRC).
const PG_CONTROL_VERSION_PG18: u32 = 1800;
const OFF_VERSION: usize = 8;
const OFF_STATE: usize = 16;
const OFF_CHECKPOINT: usize = 32;
const OFF_REDO: usize = 40;
const OFF_THIS_TLI: usize = 48;
const OFF_MIN_RECOVERY: usize = 136;
const OFF_MIN_RECOVERY_TLI: usize = 144;
const OFF_CRC: usize = 292;
/// `DBState::DB_IN_ARCHIVE_RECOVERY`.
const DB_IN_ARCHIVE_RECOVERY: u32 = 5;

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

/// CRC-32C (Castagnoli), matching PostgreSQL's `pg_crc32c`: reflected,
/// polynomial `0x82F63B78`, init/xorout `0xFFFFFFFF`.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Validate that `ctl` is a PG18 control file we know the layout of.
fn check_version(ctl: &[u8]) -> Result<()> {
    if ctl.len() < OFF_CRC + 4 {
        return Err(Error::other(format!(
            "pg_control too short: {} bytes",
            ctl.len()
        )));
    }
    let v = u32::from_le_bytes(ctl[OFF_VERSION..OFF_VERSION + 4].try_into().unwrap());
    if v != PG_CONTROL_VERSION_PG18 {
        return Err(Error::other(format!(
            "unsupported pg_control_version {v} (expected {PG_CONTROL_VERSION_PG18})"
        )));
    }
    Ok(())
}

/// Read `(checkpoint, redo, timeline)` from a `pg_control` buffer.
pub fn read_checkpoint_lsns(ctl: &[u8]) -> Result<(Lsn, Lsn, TimelineId)> {
    check_version(ctl)?;
    let checkpoint = Lsn::new(u64::from_le_bytes(
        ctl[OFF_CHECKPOINT..OFF_CHECKPOINT + 8].try_into().unwrap(),
    ));
    let redo = Lsn::new(u64::from_le_bytes(
        ctl[OFF_REDO..OFF_REDO + 8].try_into().unwrap(),
    ));
    let tli = TimelineId::new(u32::from_le_bytes(
        ctl[OFF_THIS_TLI..OFF_THIS_TLI + 4].try_into().unwrap(),
    ));
    Ok((checkpoint, redo, tli))
}

/// Patch a `pg_control` buffer in place so PostgreSQL treats the snapshot as a
/// base backup whose consistency point is `min_recovery`: set
/// `state = DB_IN_ARCHIVE_RECOVERY`, `minRecoveryPoint`/`minRecoveryPointTLI`,
/// then recompute the trailing CRC-32C over `[0, OFF_CRC)`.
pub fn shape_for_backup_recovery(
    ctl: &mut [u8],
    min_recovery: Lsn,
    min_recovery_tli: TimelineId,
) -> Result<()> {
    check_version(ctl)?;
    ctl[OFF_STATE..OFF_STATE + 4].copy_from_slice(&DB_IN_ARCHIVE_RECOVERY.to_le_bytes());
    ctl[OFF_MIN_RECOVERY..OFF_MIN_RECOVERY + 8]
        .copy_from_slice(&min_recovery.as_u64().to_le_bytes());
    ctl[OFF_MIN_RECOVERY_TLI..OFF_MIN_RECOVERY_TLI + 4]
        .copy_from_slice(&min_recovery_tli.as_u32().to_le_bytes());
    let crc = crc32c(&ctl[..OFF_CRC]);
    ctl[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
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
    fn crc32c_check_value() {
        // Standard CRC-32C (Castagnoli) check value for "123456789".
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    fn synthetic_control() -> Vec<u8> {
        let mut c = vec![0u8; 8192];
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&PG_CONTROL_VERSION_PG18.to_le_bytes());
        c[OFF_CHECKPOINT..OFF_CHECKPOINT + 8].copy_from_slice(&0x20386B8u64.to_le_bytes());
        c[OFF_REDO..OFF_REDO + 8].copy_from_slice(&0x2038660u64.to_le_bytes());
        c[OFF_THIS_TLI..OFF_THIS_TLI + 4].copy_from_slice(&1u32.to_le_bytes());
        c
    }

    #[test]
    fn read_checkpoint_lsns_reads_fields() {
        let c = synthetic_control();
        let (ckpt, redo, tli) = read_checkpoint_lsns(&c).unwrap();
        assert_eq!(ckpt.as_u64(), 0x20386B8);
        assert_eq!(redo.as_u64(), 0x2038660);
        assert_eq!(tli.as_u32(), 1);
    }

    #[test]
    fn read_checkpoint_lsns_rejects_bad_version() {
        let mut c = synthetic_control();
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&1700u32.to_le_bytes());
        assert!(read_checkpoint_lsns(&c).is_err());
        assert!(read_checkpoint_lsns(&[0u8; 8]).is_err()); // too short
    }

    #[test]
    fn shape_sets_fields_and_self_consistent_crc() {
        let mut c = synthetic_control();
        shape_for_backup_recovery(&mut c, Lsn::new(0x20386B8), TimelineId::new(1)).unwrap();
        assert_eq!(
            u32::from_le_bytes(c[OFF_STATE..OFF_STATE + 4].try_into().unwrap()),
            DB_IN_ARCHIVE_RECOVERY
        );
        assert_eq!(
            u64::from_le_bytes(c[OFF_MIN_RECOVERY..OFF_MIN_RECOVERY + 8].try_into().unwrap()),
            0x20386B8
        );
        assert_eq!(
            u32::from_le_bytes(c[OFF_MIN_RECOVERY_TLI..OFF_MIN_RECOVERY_TLI + 4].try_into().unwrap()),
            1
        );
        let stored = u32::from_le_bytes(c[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
        assert_eq!(stored, crc32c(&c[..OFF_CRC]));
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
