use pgsys::{
    aio::*,
    common::BLCKSZ,
    common::{get_my_proc_number, is_under_postmaster},
    logging::pg_log_debug1,
    smgr::*,
};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_startreadv(
    ioh: *mut PgAioHandle,
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut std::ffi::c_void,
    nblocks: BlockNumber,
) {
    unsafe {
        // During initdb / single-user mode there is no s3worker — fall back to md.
        if !is_under_postmaster() {
            mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
            return;
        }

        let loc = &(*reln).smgr_rlocator.locator;
        let proc_num = get_my_proc_number();
        pg_log_debug1(&format!(
            "s3_startreadv({}): rel {} fork {} block {} nblocks {}",
            proc_num, loc.rel_number, forknum, blocknum, nblocks
        ));

        // 1. Get iovec array from PG shared memory
        let mut iov: *mut IoVec = std::ptr::null_mut();
        let max_iovcnt = pgaio_io_get_iovec(ioh, &mut iov);
        assert!((nblocks as i32) <= max_iovcnt);

        // 2. Fill iovecs from buffer pointers, coalescing adjacent buffers
        //    (reimplements md.c's static buffers_to_iovec)
        let mut iovcnt: i32 = 0;
        for i in 0..nblocks as usize {
            let base = *buffers.add(i);
            if iovcnt > 0 {
                let prev = &mut *iov.add(iovcnt as usize - 1);
                let prev_end = (prev.iov_base as usize) + prev.iov_len;
                if prev_end == base as usize {
                    prev.iov_len += BLCKSZ;
                    continue;
                }
            }
            let entry = &mut *iov.add(iovcnt as usize);
            entry.iov_base = base;
            entry.iov_len = BLCKSZ;
            iovcnt += 1;
        }

        // 3. Set buffered flag, target identity, and md completion callback
        pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);
        pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks as i32, false);
        pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);

        // 4. Stage as PGAIO_OP_S3_READV — returns immediately, IO worker picks it up
        pgaio_io_start_s3_readv(ioh, iovcnt);
    }
}
