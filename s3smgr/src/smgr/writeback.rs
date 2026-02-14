use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_writeback(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
) {
    unsafe {
        mdwriteback(reln, forknum, blocknum, nblocks);
    }
}
