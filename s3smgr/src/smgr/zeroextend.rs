use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_zeroextend(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) {
    unsafe {
        mdzeroextend(reln, forknum, blocknum, nblocks, skip_fsync);
    }
}
