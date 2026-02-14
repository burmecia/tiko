use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_create(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    is_redo: bool,
) {
    unsafe {
        mdcreate(reln, forknum, is_redo);
    }
}
