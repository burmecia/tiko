use pgsys::smgr::*;
use s3worker::s3_ops;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_exists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    s3_ops::file_exists(loc.spc_oid, loc.db_oid, loc.rel_number, forknum)
}
