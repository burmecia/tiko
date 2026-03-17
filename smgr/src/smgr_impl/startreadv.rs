use pgsys::{
    aio::*,
    common::{BlockNumber, ForkNumber, get_my_proc_number},
    logging,
    smgr::*,
};

use crate::buffers;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_startreadv(
    ioh: *mut PgAioHandle,
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut std::ffi::c_void,
    nblocks: BlockNumber,
) {
    unsafe {
        let loc = &(*reln).smgr_rlocator.locator;
        let proc_num = get_my_proc_number();
        logging::pg_log_debug2(&format!(
            "tiko_startreadv({}): rel {} fork {} block {} nblocks {}",
            proc_num, loc.rel_number, forknum, blocknum, nblocks
        ));

        // 1. Coalesce adjacent buffers into contiguous runs
        let coalesced = buffers::buffers_to_iov(buffers as *const *const _, nblocks);

        // 2. Copy coalesced iovecs into PG shared memory iov array
        let mut iov: *mut IoVec = std::ptr::null_mut();
        let max_iovcnt = pgaio_io_get_iovec(ioh, &mut iov);
        assert!((coalesced.len() as i32) <= max_iovcnt);
        for (j, entry) in coalesced.iter().enumerate() {
            let pg_entry = &mut *iov.add(j);
            pg_entry.iov_base = entry.iov_base;
            pg_entry.iov_len = entry.iov_len;
        }
        let iovcnt = coalesced.len() as i32;

        // 3. Set buffered flag, target identity, and md completion callback
        pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);
        pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks as i32, false);
        pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);

        // 4. Stage as PGAIO_OP_TIKO_READV — returns immediately.
        //    During initdb the perform function handles I/O directly via s3_ops;
        //    under the postmaster, IO workers submit to the s3worker pipeline.
        pgaio_io_start_tiko_readv(ioh, iovcnt);
    }
}
