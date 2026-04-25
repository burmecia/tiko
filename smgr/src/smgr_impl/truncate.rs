use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{BlockNumber, ForkNumber, in_recovery},
    logging::pg_log_error,
    smgr::*,
};

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

    let relfork = RelFork::from_rel(reln, forknum);

    if let Err(err) = ops::truncate_relfork(&relfork, nblocks) {
        pg_log_error(&format!(
            "tiko_truncate: failed for relfork {relfork} nblocks {nblocks}: {err}",
        ));
    }
}
