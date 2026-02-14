use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_unlink(
    rlocator: *mut RelFileLocatorBackend,
    forknum: ForkNumber,
    is_redo: bool,
) {
    unsafe {
        mdunlink(rlocator, forknum, is_redo);
    }
}
