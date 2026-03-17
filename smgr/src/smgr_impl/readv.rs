use engine::s3_ops;
use pgsys::{
    common::{BlockNumber, ForkNumber, in_recovery},
    logging::pg_log_error,
    smgr::*,
};
use store::chunk::RelFork;

use crate::buffers;

/// POSIX ENOENT (No such file or directory)
const ENOENT: i32 = 2;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_readv(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut std::ffi::c_void,
    nblocks: BlockNumber,
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
    let iov = unsafe { buffers::buffers_to_iov(buffers as *const *const _, nblocks) };

    let mut block_offset: u32 = 0;
    for entry in &iov {
        let run_nblocks = (entry.iov_len / pgsys::common::BLCKSZ) as u32;

        if let Err(errno) = s3_ops::cached_read_blocks(
            rf,
            blocknum + block_offset,
            run_nblocks,
            entry.iov_base as *mut u8,
        ) {
            // During recovery, silently tolerate ENOENT
            if in_recovery() && errno == ENOENT {
                let buffer_ptr = entry.iov_base as *mut u8;
                unsafe {
                    std::ptr::write_bytes(buffer_ptr, 0, entry.iov_len);
                }
            } else {
                pg_log_error(&format!(
                    "tiko_readv: read failed for rel {}/{}/{} fork {} block {} nblocks {}: errno {}",
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
