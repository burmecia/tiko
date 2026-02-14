use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_fd(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    off: *mut u32,
) -> i32 {
    unsafe { mdfd(reln, forknum, blocknum, off) }
}
