use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_extend(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: *const std::ffi::c_void,
    skip_fsync: bool,
) {
    unsafe {
        mdextend(reln, forknum, blocknum, buffer, skip_fsync);
    }
}
