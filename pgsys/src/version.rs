//! PostgreSQL version-specific constants.
//!
//! Everything here must be re-checked when the vendored Postgres in
//! `postgres/` is upgraded. Sources:
//!   - PG_VERSION_NUM:               src/include/pg_config.h
//!   - TABLESPACE_VERSION_DIRECTORY: src/include/common/relpath.h
//!     ("PG_" PG_MAJORVERSION "_" CATALOG_VERSION_NO) + catalog/catversion.h
//!   - MAX_IO_WORKERS / NUM_AUXILIARY_PROCS: src/include/storage/proc.h

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
/// = 6 traditional (Startup, BgWriter, Checkpointer, WalWriter, WalReceiver, WalSummarizer)
///   + MAX_IO_WORKERS (up to 32 I/O worker processes).
/// These have ProcNumbers from MaxBackends to MaxBackends + NUM_AUXILIARY_PROCS - 1.
pub const NUM_AUXILIARY_PROCS: c_int = 6 + MAX_IO_WORKERS;
