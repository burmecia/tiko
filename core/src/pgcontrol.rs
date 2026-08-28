//! WAL segment metadata helpers and safe reads of PostgreSQL `pg_control`,
//! used by `tiko_restore` outside a running postmaster. PG18
//! (`PG_CONTROL_VERSION` 1800) layout; all reads are guarded at runtime by the
//! version field so an unknown layout is never misinterpreted.

use pgsys::common::XLOG_SEG_SIZE;
use pgsys::timeline_id::TimelineId;

use crate::error::{Error, Result};

/// WAL segments per logical xlog id: 2^32 / XLOG_SEG_SIZE (= 256 for 16 MiB).
const SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;
/// WAL page magic for this PG major (`XLOG_PAGE_MAGIC`, PG18).
const XLOG_PAGE_MAGIC: u16 = 0xD118;
/// `XLP_LONG_HEADER` — set in `xlp_info` on the first page of each segment.
const XLP_LONG_HEADER: u16 = 0x0002;
/// `XLOG_BLCKSZ` — WAL block size (PostgreSQL default, what this build uses).
const XLOG_BLCKSZ: u32 = 8192;
/// `SizeOfXLogLongPHD` — bytes in `XLogLongPageHeaderData`.
const SIZE_OF_XLOG_LONG_PHD: usize = 40;

// PG18 ControlFileData layout (PG_CONTROL_VERSION 1800), confirmed via offsetof
// against the build's headers. pg_control is native-endian; the
// from_le_bytes/to_le_bytes below assume a little-endian host (arm64/x86-64),
// which is the only supported platform.
const PG_CONTROL_VERSION_PG18: u32 = 1800;
const OFF_VERSION: usize = 8;
const OFF_CRC: usize = 292;

/// Build a WAL `XLogLongPageHeaderData` — the descriptor on page 0 of every
/// segment that PostgreSQL validates (`XLogReaderValidatePageHeader`) on first
/// access. Synthesized when a mid-stream-start segment never archived its
/// page 0. Field offsets match the PG18 C layout; values are little-endian
/// (same single-platform assumption as the rest of this module).
pub fn wal_long_header(
    tli: TimelineId,
    seg_no: u64,
    system_identifier: u64,
) -> [u8; SIZE_OF_XLOG_LONG_PHD] {
    let mut h = [0u8; SIZE_OF_XLOG_LONG_PHD];
    // XLogPageHeaderData (short header, first 24 bytes):
    h[0..2].copy_from_slice(&XLOG_PAGE_MAGIC.to_le_bytes()); // xlp_magic
    h[2..4].copy_from_slice(&XLP_LONG_HEADER.to_le_bytes()); // xlp_info
    h[4..8].copy_from_slice(&tli.as_u32().to_le_bytes()); // xlp_tli
    let pageaddr = seg_no * XLOG_SEG_SIZE as u64; // segment start LSN
    h[8..16].copy_from_slice(&pageaddr.to_le_bytes()); // xlp_pageaddr
    // h[16..20] xlp_rem_len = 0; h[20..24] alignment padding = 0.
    // XLogLongPageHeaderData extra fields:
    h[24..32].copy_from_slice(&system_identifier.to_le_bytes()); // xlp_sysid
    h[32..36].copy_from_slice(&(XLOG_SEG_SIZE as u32).to_le_bytes()); // xlp_seg_size
    h[36..40].copy_from_slice(&XLOG_BLCKSZ.to_le_bytes()); // xlp_xlog_blcksz
    h
}

/// Parse a 24-hex WAL segment name into its
/// segment number (`logid * SEGS_PER_LOGID + logseg`). `None` for any name that
/// is not exactly 24 hex digits. The timeline prefix is ignored — segment
/// numbers are timeline-independent; callers needing the timeline parse it
/// separately.
pub fn parse_wal_seg_no(name: &str) -> Option<u64> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let logid = u64::from_str_radix(&name[8..16], 16).ok()?;
    let logseg = u64::from_str_radix(&name[16..24], 16).ok()?;
    Some(logid * SEGS_PER_LOGID + logseg)
}

/// Read `system_identifier` (first field, offset 0) from a `pg_control` buffer.
/// Version-guarded so an unknown layout is rejected rather than misread.
pub fn read_system_identifier(ctl: &[u8]) -> Result<u64> {
    check_version(ctl)?;
    Ok(u64::from_le_bytes(ctl[0..8].try_into().unwrap()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_long_header_bytes() {
        let h = wal_long_header(TimelineId::new(1), 2, 0x0123_4567_89AB_CDEF);
        assert_eq!(
            u16::from_le_bytes(h[0..2].try_into().unwrap()),
            XLOG_PAGE_MAGIC
        );
        assert_eq!(
            u16::from_le_bytes(h[2..4].try_into().unwrap()),
            XLP_LONG_HEADER
        );
        assert_eq!(u32::from_le_bytes(h[4..8].try_into().unwrap()), 1); // tli
        assert_eq!(
            u64::from_le_bytes(h[8..16].try_into().unwrap()),
            2 * XLOG_SEG_SIZE as u64 // xlp_pageaddr = segment start
        );
        assert_eq!(u32::from_le_bytes(h[16..20].try_into().unwrap()), 0); // rem_len
        assert_eq!(
            u64::from_le_bytes(h[24..32].try_into().unwrap()),
            0x0123_4567_89AB_CDEF
        ); // sysid
        assert_eq!(
            u32::from_le_bytes(h[32..36].try_into().unwrap()),
            XLOG_SEG_SIZE as u32
        );
        assert_eq!(
            u32::from_le_bytes(h[36..40].try_into().unwrap()),
            XLOG_BLCKSZ
        );
    }

    #[test]
    fn parse_wal_seg_no_values() {
        assert_eq!(parse_wal_seg_no("000000010000000000000002"), Some(2));
        assert_eq!(parse_wal_seg_no("000000010000000100000000"), Some(256));
        assert_eq!(parse_wal_seg_no("0000000100000000000002BC"), Some(700));
        assert_eq!(parse_wal_seg_no("short"), None);
        assert_eq!(parse_wal_seg_no("00000001.history"), None);
    }

    #[test]
    fn read_system_identifier_reads_offset_zero() {
        let mut c = vec![0u8; 8192];
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&PG_CONTROL_VERSION_PG18.to_le_bytes());
        c[0..8].copy_from_slice(&0xDEAD_BEEF_0000_0001u64.to_le_bytes());
        assert_eq!(read_system_identifier(&c).unwrap(), 0xDEAD_BEEF_0000_0001);
        // Rejects wrong version and too-short buffers (via check_version).
        c[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&1700u32.to_le_bytes());
        assert!(read_system_identifier(&c).is_err());
        assert!(read_system_identifier(&[0u8; 8]).is_err());
    }
}
