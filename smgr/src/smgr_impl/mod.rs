mod close;
mod create;
mod exists;
mod extend;
mod fd;
mod immedsync;
mod init;
mod maxcombine;
mod nblocks;
mod open;
mod prefetch;
mod readv;
mod registersync;
mod shutdown;
mod startreadv;
mod truncate;
mod unlink;
mod writeback;
mod writev;
mod zeroextend;

use pgsys::common::{TABLESPACE_VERSION_DIRECTORY, data_dir_path};

/// Build the path of the per-relation marker file that Tiko maintains
/// inside the standard `pg_tblspc` directory structure.
///
/// PostgreSQL's `DROP TABLESPACE` checks whether the tablespace directory is
/// empty by trying to `rmdir` each per-database subdirectory under
/// `pg_tblspc/<spc>/<ver>/<db>/`. With Tiko's custom storage backend the
/// actual relation data lives in the chunk-cache / S3-Sim and never touches
/// `pg_tblspc`, so PG would incorrectly conclude the tablespace is empty and
/// allow a premature `DROP TABLESPACE`.
///
/// To prevent this, `tiko_create` creates a zero-byte marker file at this
/// standard path and `tiko_unlink` removes it.  The file contains no data;
/// its sole purpose is to make the directory appear non-empty to PG's
/// emptiness check.
fn marker_path(spc_oid: u32, db_oid: u32, rel_number: u32) -> std::path::PathBuf {
    data_dir_path()
        .join("pg_tblspc")
        .join(spc_oid.to_string())
        .join(TABLESPACE_VERSION_DIRECTORY)
        .join(db_oid.to_string())
        .join(rel_number.to_string())
}
