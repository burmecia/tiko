use engine::s3_ops;
use pgsys::smgr::*;
use store::chunk::RelFork;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_exists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    s3_ops::store_exists(RelFork {
        spc_oid: loc.spc_oid,
        db_oid: loc.db_oid,
        rel_number: loc.rel_number,
        fork_number: forknum,
    })
}
