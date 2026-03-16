use pgsys::{common::in_recovery, logging::pg_log_error, smgr::*};
use worker::cache::RelFork;
use worker::s3_ops;

/// Truncate a relation fork to the given number of blocks.
///
/// Unlike `mdtruncate` which iterates segments and closes excess file
/// descriptors, S3 uses a single file per fork — just `ftruncate` to
/// `nblocks * BLCKSZ`.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_truncate(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    old_blocks: BlockNumber,
    nblocks: BlockNumber,
) {
    // Matches mdtruncate: bogus request if nblocks > current size,
    // but silently tolerate during recovery (WAL replay may see stale sizes).
    if nblocks > old_blocks {
        if in_recovery() {
            return;
        }
        pg_log_error(&format!(
            "tiko_truncate: cannot truncate to {} blocks, only {} blocks now",
            nblocks, old_blocks
        ));
        return;
    }
    if nblocks == old_blocks {
        return; // no work
    }

    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    if let Err(errno) = s3_ops::cached_truncate_file(
        RelFork {
            spc_oid: loc.spc_oid,
            db_oid: loc.db_oid,
            rel_number: loc.rel_number,
            fork_number: forknum,
        },
        nblocks,
    ) {
        pg_log_error(&format!(
            "tiko_truncate: failed for rel {}/{}/{} fork {} nblocks {}: errno {}",
            loc.spc_oid, loc.db_oid, loc.rel_number, forknum, nblocks, errno
        ));
    }
}
