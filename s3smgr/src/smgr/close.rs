use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_close(reln: *mut SMgrRelationData, forknum: ForkNumber) {
    unsafe {
        mdclose(reln, forknum);
    }
}
