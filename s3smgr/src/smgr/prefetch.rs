use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_prefetch(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
) -> bool {
    unsafe { mdprefetch(reln, forknum, blocknum, nblocks) }
}
