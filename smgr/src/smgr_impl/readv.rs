use core::{chunk::RelFork, ops};
use pgsys::{
    common::{BlockNumber, ForkNumber},
    logging::pg_log_error,
    smgr::*,
};

use crate::buffers;

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

    let relfork = RelFork::from_rel(reln, forknum);
    let iov = unsafe { buffers::buffers_to_iov(buffers as *const *const _, nblocks) };

    let mut block_offset: u32 = 0;
    for entry in &iov {
        let run_nblocks = (entry.iov_len / pgsys::common::BLCKSZ) as u32;

        if let Err(err) = ops::read_blocks(
            &relfork,
            blocknum + block_offset,
            run_nblocks,
            entry.iov_base as *mut u8,
        ) {
            pg_log_error(&format!(
                "tiko_readv: failed for relfork {relfork} block {blocknum} nblocks {run_nblocks}: {err}",
                blocknum = blocknum + block_offset,
            ));
        }

        block_offset += run_nblocks;
    }
}
