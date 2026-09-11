//! PostgreSQL version-specific constants.
//!
//! Everything here must be re-checked when the vendored Postgres in
//! `postgres/` is upgraded. Sources:
//!   - PG_VERSION_NUM:               src/include/pg_config.h
//!   - TABLESPACE_VERSION_DIRECTORY: src/include/common/relpath.h
//!     ("PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO) + catalog/catversion.h
//!   - MAX_IO_WORKERS / NUM_AUXILIARY_PROCS: src/include/storage/proc.h
//!   - XLOG_PAGE_MAGIC / XLP_LONG_HEADER / SizeOfXLogLongPHD:
//!     src/include/access/xlog_internal.h
//!   - XLOG_BLCKSZ: src/include/pg_config.h (`--with-wal-blocksize`)
//!   - DEFAULT_XLOG_SEG_SIZE: src/include/pg_config_manual.h
//!   - PG_CONTROL_VERSION / ControlFileData offsets: src/include/catalog/pg_control.h

use std::ffi::c_int;

/// PostgreSQL 18.6
pub const PG_VERSION_NUM: c_int = 180006;

/// Version-specific subdirectory name inside pg_tblspc/<spc_oid>/.
/// PG 18, CATALOG_VERSION_NO 202506291.
pub const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202506291";

/// Maximum number of I/O worker processes (proc.h: MAX_IO_WORKERS).
/// Compile-time upper bound; actual count is controlled by the `io_workers` GUC.
pub const MAX_IO_WORKERS: c_int = 32;

/// Number of auxiliary process slots (proc.h: NUM_AUXILIARY_PROCS).
/// = 6 traditional (Startup, BgWriter, Checkpointer, WalWriter, WalReceiver, WalSummarizer) + MAX_IO_WORKERS (up to 32 I/O worker processes).
/// These have ProcNumbers from MaxBackends to MaxBackends + NUM_AUXILIARY_PROCS - 1.
pub const NUM_AUXILIARY_PROCS: c_int = 6 + MAX_IO_WORKERS;

// ── WAL and pg_control on-disk format (PG 18) ────────────────────────────────

/// WAL page magic (xlog_internal.h: XLOG_PAGE_MAGIC). Bumped whenever the WAL
/// page format changes, so it doubles as a WAL format version indicator.
pub const XLOG_PAGE_MAGIC: u16 = 0xD118;

/// `XLP_LONG_HEADER` flag in `xlp_info`, set on the first page of a segment
/// (xlog_internal.h).
pub const XLP_LONG_HEADER: u16 = 0x0002;

/// WAL block size (pg_config.h: XLOG_BLCKSZ). Build-time value
/// (`--with-wal-blocksize`, default 8 KiB); changing it requires an initdb.
pub const XLOG_BLCKSZ: u32 = 8192;

/// Default WAL segment size in bytes (pg_config_manual.h:
/// DEFAULT_XLOG_SEG_SIZE). This build/initdb uses the 16 MiB default.
pub const XLOG_SEG_SIZE: usize = 16 * 1024 * 1024;

/// WAL segments per logical xlog id (xlog_internal.h:
/// XLogSegmentsPerXLogId): 2^32 / XLOG_SEG_SIZE (= 256 at 16 MiB).
pub const XLOG_SEGS_PER_LOGID: u64 = (1u64 << 32) / XLOG_SEG_SIZE as u64;

/// `SizeOfXLogLongPHD` (xlog_internal.h): MAXALIGN'd size of
/// `XLogLongPageHeaderData`, 40 bytes on 64-bit.
pub const SIZE_OF_XLOG_LONG_PHD: usize = 40;

/// `PG_CONTROL_VERSION` (catalog/pg_control.h).
pub const PG_CONTROL_VERSION: u32 = 1800;

/// Offset of `ControlFileData.pg_control_version` (catalog/pg_control.h).
/// Layout assumes a 64-bit little-endian host (arm64/x86-64).
pub const PG_CONTROL_OFF_VERSION: usize = 8;

/// Offset of `ControlFileData.crc`, the trailing `pg_crc32c` field
/// (catalog/pg_control.h), over which the CRC is computed.
pub const PG_CONTROL_OFF_CRC: usize = 292;
