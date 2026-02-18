use pgsys::{logging::pg_log_error, smgr::*};
use s3worker::s3_ops;

/// Get the number of blocks stored in a relation fork.
///
/// Unlike `mdnblocks` which iterates across segments and opens file
/// descriptors, S3 uses a single file per fork — just `file_size / BLCKSZ`.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_nblocks(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
) -> BlockNumber {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    match s3_ops::file_nblocks(loc.spc_oid, loc.db_oid, loc.rel_number, forknum) {
        Ok(n) => n,
        Err(errno) => {
            pg_log_error(&format!(
                "s3_nblocks: failed for rel {}/{}/{} fork {}: errno {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum, errno
            ));
            0
        }
    }
}
