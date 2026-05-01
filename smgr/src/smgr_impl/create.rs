use super::marker_path;
use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{DEFAULTTABLESPACE_OID, ForkNumber, GLOBALTABLESPACE_OID, MAIN_FORKNUM},
    logging::pg_log_error,
    smgr::*,
};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_create(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let relfork = RelFork::from_rel(reln, forknum);
    match ops::create(&relfork) {
        Ok(true) => {}             // newly created
        Ok(false) if is_redo => {} // exists, WAL replay — OK
        Ok(false) => {
            pg_log_error(&format!("tiko_create: relfork already exists {relfork}",));
        }
        Err(err) => {
            pg_log_error(&format!("tiko_create: failed for relfork {relfork}: {err}",));
        }
    }

    // For non-default tablespaces, maintain a per-relation marker file in the
    // standard pg_tblspc directory so that PostgreSQL's DROP TABLESPACE
    // emptiness check (destroy_tablespace_directories) sees the tablespace as
    // non-empty while relations still exist in it.
    let loc = unsafe { (*reln).smgr_rlocator.locator };
    if loc.spc_oid != DEFAULTTABLESPACE_OID
        && loc.spc_oid != GLOBALTABLESPACE_OID
        && forknum == MAIN_FORKNUM
    {
        // Create the per-database subdirectory (idempotent).
        unsafe { TablespaceCreateDbspace(loc.spc_oid, loc.db_oid, is_redo) };

        // Create the zero-byte marker file.
        let path = marker_path(loc.spc_oid, loc.db_oid, loc.rel_number);
        if let Err(err) = std::fs::File::create(&path) {
            pg_log_error(&format!(
                "tiko_create: failed to create tablespace marker {path:?}: {err}"
            ));
        }
    }
}
