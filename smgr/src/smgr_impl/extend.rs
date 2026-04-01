use core::chunk::RelFork;
use core::s3_ops;
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
    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    if blocknum == INVALID_BLOCK_NUMBER {
        pg_log_error(&format!(
            "tiko_extend: cannot extend rel {}/{}/{} fork {} beyond {} blocks",
            loc.spc_oid, loc.db_oid, loc.rel_number, forknum, INVALID_BLOCK_NUMBER
        ));
        return;
    }

    if let Err(errno) = s3_ops::cached_write_blocks(
        RelFork {
            spc_oid: loc.spc_oid,
            db_oid: loc.db_oid,
            rel_number: loc.rel_number,
            fork_number: forknum,
        },
        blocknum,
        1,
        buffer as *const u8,
    ) {
        pg_log_error(&format!(
            "tiko_extend: write failed for rel {}/{}/{} fork {} block {}: errno {}",
            loc.spc_oid, loc.db_oid, loc.rel_number, forknum, blocknum, errno
        ));
    }
}
