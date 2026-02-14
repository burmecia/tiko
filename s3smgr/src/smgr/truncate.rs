use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_truncate(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    old_blocks: BlockNumber,
    nblocks: BlockNumber,
) {
    unsafe {
        mdtruncate(reln, forknum, old_blocks, nblocks);
    }
}
