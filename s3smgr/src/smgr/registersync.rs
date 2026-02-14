use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_registersync(reln: *mut SMgrRelationData, forknum: ForkNumber) {
    unsafe {
        mdregistersync(reln, forknum);
    }
}
