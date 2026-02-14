use pgsys::{common::get_my_proc_number, logging::pg_log_debug1, smgr::*};

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
        let loc = &(*reln).smgr_rlocator.locator;
        let proc_num = get_my_proc_number();
        pg_log_debug1(&format!(
            "s3_startreadv({}): rel {} fork {} block {} nblocks {}",
            proc_num, loc.rel_number, forknum, blocknum, nblocks
        ));

        // TODO: route through async S3 IO pipeline instead of md passthrough
        mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
    }
}
