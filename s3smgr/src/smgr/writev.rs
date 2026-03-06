use pgsys::{common::in_recovery, logging::pg_log_error, smgr::*};
use s3worker::cache::RelFork;
use s3worker::s3_ops;

use crate::buffers;

/// POSIX ENOENT (No such file or directory)
const ENOENT: i32 = 2;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_writev(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *const *const std::ffi::c_void,
    nblocks: BlockNumber,
    _skip_fsync: bool,
) {
    // Guard against invalid nblocks
    if nblocks == 0 {
        return;
    }

    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    let rf = RelFork {
        spc_oid: loc.spc_oid,
        db_oid: loc.db_oid,
        rel_number: loc.rel_number,
        fork_number: forknum,
    };
    let iov = unsafe { buffers::buffers_to_iov(buffers, nblocks) };

    let mut block_offset: u32 = 0;
    for entry in &iov {
        let run_nblocks = (entry.iov_len / pgsys::common::BLCKSZ) as u32;

        if let Err(errno) = s3_ops::cached_write_blocks(
            rf,
            blocknum + block_offset,
            run_nblocks,
            entry.iov_base as *const u8,
        ) {
            // During recovery, silently tolerate ENOENT (no-op is correct)
            if !(in_recovery() && errno == ENOENT) {
                pg_log_error(&format!(
                    "s3_writev: write failed for rel {}/{}/{} fork {} block {} nblocks {}: errno {}",
                    loc.spc_oid,
                    loc.db_oid,
                    loc.rel_number,
                    forknum,
                    blocknum + block_offset,
                    run_nblocks,
                    errno
                ));
            }
        }

        block_offset += run_nblocks;
    }
}
