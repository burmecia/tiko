use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{BlockNumber, ForkNumber},
    logging::pg_log_error,
    smgr::*,
};

/// Get the number of blocks stored in a relation fork.
///
/// Returns `max(nblocks, cache_max)` — the backing file may lag behind
/// the cache under the write-back policy, so we must also check the cache for
/// blocks that have been written but not yet evicted to the S3-sim file.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_nblocks(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
) -> BlockNumber {
    let relfork = RelFork::from_rel(reln, forknum);

    match ops::get_nblocks(&relfork) {
        Ok(n) => n,
        Err(err) => {
            pg_log_error(&format!(
                "tiko_nblocks: failed for relfork {relfork}: {err}",
            ));
            0
        }
    }
}
