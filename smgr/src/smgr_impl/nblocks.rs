use pgsys::{logging::pg_log_error, smgr::*};
use store::chunk::RelFork;
use worker::s3_ops;

/// Get the number of blocks stored in a relation fork.
///
/// Returns `max(file_nblocks, cache_max)` — the backing file may lag behind
/// the cache under the write-back policy, so we must also check the cache for
/// blocks that have been written but not yet evicted to the S3-sim file.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_nblocks(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
) -> BlockNumber {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    match s3_ops::cached_file_nblocks(RelFork {
        spc_oid: loc.spc_oid,
        db_oid: loc.db_oid,
        rel_number: loc.rel_number,
        fork_number: forknum,
    }) {
        Ok(n) => n,
        Err(errno) => {
            pg_log_error(&format!(
                "tiko_nblocks: failed for rel {}/{}/{} fork {}: errno {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum, errno
            ));
            0
        }
    }
}
