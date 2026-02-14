use pgsys::smgr::*;

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
        mdstartreadv(ioh, reln, forknum, blocknum, buffers, nblocks);
    }
}
