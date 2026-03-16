use engine::s3_ops;
use pgsys::{logging::pg_log_error, smgr::*};
use store::chunk::RelFork;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_create(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    match s3_ops::store_create(RelFork {
        spc_oid: loc.spc_oid,
        db_oid: loc.db_oid,
        rel_number: loc.rel_number,
        fork_number: forknum,
    }) {
        Ok(true) => {}             // newly created
        Ok(false) if is_redo => {} // exists, WAL replay — OK
        Ok(false) => {
            pg_log_error(&format!(
                "tiko_create: file already exists for rel {}/{}/{} fork {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum
            ));
        }
        Err(errno) => {
            pg_log_error(&format!(
                "tiko_create: failed for rel {}/{}/{} fork {}: errno {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum, errno
            ));
        }
    }
}
