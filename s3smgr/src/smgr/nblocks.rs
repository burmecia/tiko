use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_nblocks(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
) -> BlockNumber {
    unsafe { mdnblocks(reln, forknum) }
}
