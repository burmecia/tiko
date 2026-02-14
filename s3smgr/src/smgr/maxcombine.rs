use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_maxcombine(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> u32 {
    unsafe { mdmaxcombine(reln, forknum, blocknum) }
}
