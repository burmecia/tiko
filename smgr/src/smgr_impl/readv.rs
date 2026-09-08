use core::relfork::{RelFork, ops};
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
        let entry_blocknum = blocknum + block_offset;

        match ops::read_blocks(
            &relfork,
            entry_blocknum,
            run_nblocks,
            entry.iov_base as *mut u8,
        ) {
            // A short read means the range runs past EOF (read_blocks clips
            // at nblocks); mdreadv errors in this case.
            Ok(n) if n < run_nblocks => {
                pg_log_error(format!(
                    "tiko_readv: could not read blocks {entry_blocknum}..{} of relfork {relfork}: read only {n} of {run_nblocks} blocks",
                    entry_blocknum + run_nblocks - 1,
                ));
            }
            Err(err) => {
                pg_log_error(format!(
                    "tiko_readv: failed for relfork {relfork} block {entry_blocknum} nblocks {run_nblocks}: {err}",
                ));
            }
            Ok(_) => {}
        }

        block_offset += run_nblocks;
    }
}
