use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{BlockNumber, ForkNumber, INVALID_BLOCK_NUMBER},
    logging::pg_log_error,
    smgr::*,
};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_extend(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: *const std::ffi::c_void,
    _skip_fsync: bool,
) {
    let relfork = RelFork::from_rel(reln, forknum);

    if blocknum == INVALID_BLOCK_NUMBER {
        pg_log_error(&format!(
            "tiko_extend: cannot extend relfork {relfork} beyond {} blocks",
            INVALID_BLOCK_NUMBER
        ));
        return;
    }

    if let Err(err) = ops::write_blocks(&relfork, blocknum, 1, buffer as *const u8) {
        pg_log_error(&format!(
            "tiko_extend: failed for relfork {relfork} block {blocknum}: {err}",
        ));
    }
}
