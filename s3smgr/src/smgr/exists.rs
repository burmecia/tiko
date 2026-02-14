use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_exists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool {
    unsafe { mdexists(reln, forknum) }
}
