//! WAL segment metadata helpers and safe reads of PostgreSQL `pg_control`,
//! used by `tiko_restore` outside a running postmaster. PG18
//! (`PG_CONTROL_VERSION` 1800) layout; all reads are guarded at runtime by the
//! version field and the trailing CRC so an unknown layout or a torn buffer is
//! never misinterpreted.

use pgsys::timeline_id::TimelineId;
use pgsys::version::{
    PG_CONTROL_OFF_CRC, PG_CONTROL_OFF_VERSION, PG_CONTROL_VERSION, SIZE_OF_XLOG_LONG_PHD,
    XLOG_BLCKSZ, XLOG_PAGE_MAGIC, XLOG_SEG_SIZE, XLOG_SEGS_PER_LOGID, XLP_LONG_HEADER,
};

use core::error::{Error, Result};

/// Reflected CRC-32C (Castagnoli) polynomial, matching `pg_crc32c`.
const CRC32C_POLY: u32 = 0x82F6_3B78;

/// Byte-at-a-time CRC-32C table, built at compile time.
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

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

/// Parse a 24-hex WAL segment name into its timeline id (high 8 hex digits)
/// and segment number (`logid * XLOG_SEGS_PER_LOGID + logseg`). `None` for any
/// name that is not exactly 24 hex digits. Segment numbers are
/// timeline-independent; the timeline is returned too so callers don't have to
/// re-parse the name.
pub fn parse_wal_segment_name(name: &str) -> Option<(TimelineId, u64)> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let timeline = TimelineId::from_hex(&name[..8]).ok()?;
    let logid = u64::from_str_radix(&name[8..16], 16).ok()?;
    let logseg = u64::from_str_radix(&name[16..24], 16).ok()?;
    Some((timeline, logid * XLOG_SEGS_PER_LOGID + logseg))
}

/// Segment number of a 24-hex WAL segment name. See [`parse_wal_segment_name`].
pub fn parse_wal_seg_no(name: &str) -> Option<u64> {
    parse_wal_segment_name(name).map(|(_, seg_no)| seg_no)
}

/// Read `system_identifier` (first field, offset 0) from a `pg_control` buffer.
///
/// Version- and CRC-guarded: an unknown layout or a torn/corrupt buffer is
/// rejected rather than misread. The CRC matters because the recovering
/// postmaster rewrites `pg_control` in place while `restore_command` reads it.
pub fn read_system_identifier(ctl: &[u8]) -> Result<u64> {
    check_version(ctl)?;
    check_crc(ctl)?;
    Ok(u64::from_le_bytes(ctl[0..8].try_into().unwrap()))
}

/// Verify the trailing `pg_crc32c` over `ControlFileData[0..PG_CONTROL_OFF_CRC]`.
fn check_crc(ctl: &[u8]) -> Result<()> {
    let stored = u32::from_le_bytes(
        ctl[PG_CONTROL_OFF_CRC..PG_CONTROL_OFF_CRC + 4]
            .try_into()
            .unwrap(),
    );
    let computed = crc32c(&ctl[..PG_CONTROL_OFF_CRC]);
    if stored != computed {
        return Err(Error::other(format!(
            "pg_control CRC mismatch: stored {stored:08X}, computed {computed:08X}"
        )));
    }
    Ok(())
}

/// Compute the CRC-32C of `data`, matching PostgreSQL's `INIT_CRC32C` /
/// `COMP_CRC32C` / `FIN_CRC32C` sequence.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc = CRC32C_TABLE[((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Validate that `ctl` is a PG18 control file we know the layout of.
fn check_version(ctl: &[u8]) -> Result<()> {
    if ctl.len() < PG_CONTROL_OFF_CRC + 4 {
        return Err(Error::other(format!(
            "pg_control too short: {} bytes",
            ctl.len()
        )));
    }
    let v = u32::from_le_bytes(
        ctl[PG_CONTROL_OFF_VERSION..PG_CONTROL_OFF_VERSION + 4]
            .try_into()
            .unwrap(),
    );
    if v != PG_CONTROL_VERSION {
        return Err(Error::other(format!(
            "unsupported pg_control_version {v} (expected {PG_CONTROL_VERSION})"
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
    fn parse_wal_segment_name_values() {
        assert_eq!(
            parse_wal_segment_name("000000010000000000000002"),
            Some((TimelineId::new(1), 2))
        );
        assert_eq!(
            parse_wal_segment_name("000000020000000100000000"),
            Some((TimelineId::new(2), 256))
        );
        assert_eq!(
            parse_wal_segment_name("0000000100000000000002BC"),
            Some((TimelineId::new(1), 700))
        );
        assert_eq!(parse_wal_segment_name("short"), None);
        assert_eq!(parse_wal_segment_name("00000001.history"), None);
        // Segment number is the same as the timeline-agnostic accessor.
        assert_eq!(parse_wal_seg_no("0000000100000000000002BC"), Some(700));
    }

    #[test]
    fn parse_wal_segment_name_accepts_uppercase_hex() {
        assert_eq!(
            parse_wal_segment_name("0000000A000000010000000A"),
            Some((TimelineId::new(10), XLOG_SEGS_PER_LOGID + 10))
        );
    }

    #[test]
    fn parse_wal_segment_name_max_fields() {
        // The largest logical xlog id and segment offset must combine without
        // overflowing u64.
        assert_eq!(
            parse_wal_segment_name("00000001FFFFFFFF00000000"),
            Some((TimelineId::new(1), 0xFFFF_FFFFu64 * XLOG_SEGS_PER_LOGID))
        );
        assert_eq!(
            parse_wal_segment_name("0000000100000000FFFFFFFF"),
            Some((TimelineId::new(1), 0xFFFF_FFFF))
        );
    }

    #[test]
    fn parse_wal_segment_name_rejects_malformed_names() {
        assert_eq!(parse_wal_segment_name(""), None);
        assert_eq!(parse_wal_segment_name(&"0".repeat(23)), None);
        assert_eq!(parse_wal_segment_name(&"0".repeat(25)), None);
        assert_eq!(
            parse_wal_segment_name(&format!("{}g", "0".repeat(23))),
            None
        );
        assert_eq!(
            parse_wal_segment_name("000000010000000000000001.partial"),
            None
        );
        // 24 bytes but containing a multi-byte UTF-8 char: slicing must not
        // panic (the all-ASCII-hex guard runs before any byte-range slice).
        let multibyte = format!("{}{}", "0".repeat(22), "\u{e9}");
        assert_eq!(multibyte.len(), 24);
        assert_eq!(parse_wal_segment_name(&multibyte), None);
        assert_eq!(parse_wal_seg_no(&multibyte), None);
    }

    #[test]
    fn wal_long_header_zero_segment() {
        let h = wal_long_header(TimelineId::new(0xFFFF_FFFF), 0, 0);
        assert_eq!(u32::from_le_bytes(h[4..8].try_into().unwrap()), 0xFFFF_FFFF);
        // xlp_pageaddr = 0 at segment 0.
        assert_eq!(u64::from_le_bytes(h[8..16].try_into().unwrap()), 0);
        // Bytes 16..24 are xlp_rem_len plus alignment padding, both zero.
        assert_eq!(h[16..24], [0u8; 8]);
        assert_eq!(
            u16::from_le_bytes(h[2..4].try_into().unwrap()),
            XLP_LONG_HEADER
        );
    }

    #[test]
    fn wal_long_header_pageaddr_spans_logical_xlog_ids() {
        // Segment numbers are global across logical xlog ids, so pageaddr must
        // be the full 64-bit segment-start LSN (not truncated to 32 bits).
        let seg_no = XLOG_SEGS_PER_LOGID * 2 + 3;
        let h = wal_long_header(TimelineId::new(3), seg_no, 0);
        assert_eq!(
            u64::from_le_bytes(h[8..16].try_into().unwrap()),
            seg_no * XLOG_SEG_SIZE as u64
        );
    }

    fn write_valid_crc(c: &mut [u8]) {
        let crc = crc32c(&c[..PG_CONTROL_OFF_CRC]);
        c[PG_CONTROL_OFF_CRC..PG_CONTROL_OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    }

    fn valid_control(system_identifier: u64) -> Vec<u8> {
        let mut c = vec![0u8; 8192];
        c[PG_CONTROL_OFF_VERSION..PG_CONTROL_OFF_VERSION + 4]
            .copy_from_slice(&PG_CONTROL_VERSION.to_le_bytes());
        c[0..8].copy_from_slice(&system_identifier.to_le_bytes());
        write_valid_crc(&mut c);
        c
    }

    #[test]
    fn crc32c_matches_reference_vectors() {
        // Standard CRC-32C check values, confirmed against PostgreSQL's
        // pg_comp_crc32c_sb8 (INIT/COMP/FIN_CRC32C).
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"a"), 0xC1D0_4330);
        assert_eq!(crc32c(b"abc"), 0x364B_3FB7);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(
            crc32c(b"The quick brown fox jumps over the lazy dog"),
            0x2262_0404
        );
        assert_eq!(crc32c(&[0u8; 16]), 0x4270_9AEA);
    }

    #[test]
    fn read_system_identifier_reads_offset_zero() {
        let mut c = valid_control(0xDEAD_BEEF_0000_0001);
        assert_eq!(read_system_identifier(&c).unwrap(), 0xDEAD_BEEF_0000_0001);

        // A covered byte changed under a stale CRC (a torn/corrupt read) is
        // rejected even though the version field is intact.
        c[0] ^= 0xFF;
        assert!(read_system_identifier(&c).is_err());
        c[0] ^= 0xFF; // restore the byte; the CRC is valid again

        // Rejects wrong version and too-short buffers (via check_version).
        c[PG_CONTROL_OFF_VERSION..PG_CONTROL_OFF_VERSION + 4]
            .copy_from_slice(&1700u32.to_le_bytes());
        assert!(read_system_identifier(&c).is_err());
        assert!(read_system_identifier(&[0u8; 8]).is_err());
    }

    #[test]
    fn read_system_identifier_min_length_boundary() {
        // Exactly through the CRC field is the minimum accepted length.
        let mut c = vec![0u8; PG_CONTROL_OFF_CRC + 4];
        c[PG_CONTROL_OFF_VERSION..PG_CONTROL_OFF_VERSION + 4]
            .copy_from_slice(&PG_CONTROL_VERSION.to_le_bytes());
        c[0..8].copy_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
        write_valid_crc(&mut c);
        assert_eq!(read_system_identifier(&c).unwrap(), 0x1122_3344_5566_7788);

        // One byte short of the CRC field is rejected.
        assert!(read_system_identifier(&c[..PG_CONTROL_OFF_CRC + 3]).is_err());
    }

    #[test]
    fn read_system_identifier_rejects_version_mismatch_with_valid_crc() {
        let mut c = valid_control(7);
        c[PG_CONTROL_OFF_VERSION..PG_CONTROL_OFF_VERSION + 4]
            .copy_from_slice(&1700u32.to_le_bytes());
        write_valid_crc(&mut c); // CRC is valid, but the version isn't ours
        assert!(read_system_identifier(&c).is_err());
    }

    #[test]
    fn read_system_identifier_rejects_corrupt_crc_field() {
        let mut c = valid_control(0xABCD);
        c[PG_CONTROL_OFF_CRC] ^= 0xFF; // corrupt the stored CRC itself
        assert!(read_system_identifier(&c).is_err());
    }

    #[test]
    fn read_system_identifier_crc_covers_last_header_byte() {
        let mut c = valid_control(0xABCD);
        c[PG_CONTROL_OFF_CRC - 1] ^= 0xFF; // last byte under the CRC
        assert!(read_system_identifier(&c).is_err());
    }

    #[test]
    fn read_system_identifier_ignores_bytes_past_crc() {
        let mut c = valid_control(0x1234_5678_9ABC_DEF0);
        c[PG_CONTROL_OFF_CRC + 4] ^= 0xFF; // zero padding after the CRC
        assert_eq!(read_system_identifier(&c).unwrap(), 0x1234_5678_9ABC_DEF0);
    }
}
