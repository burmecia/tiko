use pgsys::smgr::*;
use worker::cache::RelFork;
use worker::s3_ops;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_exists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    s3_ops::store_exists(RelFork {
        spc_oid: loc.spc_oid,
        db_oid: loc.db_oid,
        rel_number: loc.rel_number,
        fork_number: forknum,
    })
}
