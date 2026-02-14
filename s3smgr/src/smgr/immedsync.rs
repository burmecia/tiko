use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_immedsync(reln: *mut SMgrRelationData, forknum: ForkNumber) {
    unsafe {
        mdimmedsync(reln, forknum);
    }
}
