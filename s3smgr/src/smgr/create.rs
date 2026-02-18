use pgsys::{logging::pg_log_error, smgr::*};
use s3worker::s3_ops;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_create(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };

    match s3_ops::create_file(loc.spc_oid, loc.db_oid, loc.rel_number, forknum) {
        Ok(true) => {}             // newly created
        Ok(false) if is_redo => {} // exists, WAL replay — OK
        Ok(false) => {
            pg_log_error(&format!(
                "s3_create: file already exists for rel {}/{}/{} fork {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum
            ));
        }
        Err(errno) => {
            pg_log_error(&format!(
                "s3_create: failed for rel {}/{}/{} fork {}: errno {}",
                loc.spc_oid, loc.db_oid, loc.rel_number, forknum, errno
            ));
        }
    }
}
