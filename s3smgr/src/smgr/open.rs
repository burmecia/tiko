use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_open(reln: *mut SMgrRelationData) {
    unsafe {
        mdopen(reln);
    }
}
