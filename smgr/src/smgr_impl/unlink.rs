use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{
        DEFAULTTABLESPACE_OID, ForkNumber, GLOBALTABLESPACE_OID, INVALID_FORK_NUMBER, MAX_FORKNUM,
    },
    logging::pg_log_warning,
    smgr::*,
};

use super::marker_path;

/// Delete a relation's physical storage.
///
/// If `forknum` is `InvalidForkNumber`, all forks are removed.
/// Otherwise, only the specified fork is removed.
///
/// Unlike `mdunlink` which truncates-then-unlinks to reclaim disk space
/// from other backends' open FDs, defers main fork unlinks to avoid
/// relfilenumber reuse hazards, and iterates segments — S3 has none of
/// these concerns. Just delete the file(s), ignoring ENOENT.
///
/// Errors are reported as WARNING (not ERROR), matching `mdunlink`'s
/// convention — this is usually called outside a transaction.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_unlink(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    _is_redo: bool,
) {
    if forknum == INVALID_FORK_NUMBER {
        for fork in 0..=MAX_FORKNUM {
            unlink_fork(&rlocator, fork);
        }
    } else {
        unlink_fork(&rlocator, forknum);
    }

    // Remove the tablespace marker file when unlinking a non-default tablespace
    // relation. We create one marker per relation (not per fork), so remove it
    // when the main fork goes away (rewrite/move) or on full relation unlink.
    let rloc = &rlocator.locator;
    if rloc.spc_oid != DEFAULTTABLESPACE_OID
        && rloc.spc_oid != GLOBALTABLESPACE_OID
        && (forknum == INVALID_FORK_NUMBER || forknum == pgsys::common::MAIN_FORKNUM)
    {
        let path = marker_path(rloc.spc_oid, rloc.db_oid, rloc.rel_number);
        // Ignore ENOENT — the marker may not exist if the relation was created
        // before this fix was applied or in a non-tablespace context.
        let _ = std::fs::remove_file(&path);
    }
}

fn unlink_fork(rlocator: &RelFileLocatorBackend, forknum: ForkNumber) {
    let rloc = &rlocator.locator;
    let relfork = RelFork::new(rloc.spc_oid, rloc.db_oid, rloc.rel_number, forknum);

    match ops::delete_fork(&relfork) {
        Ok(()) => {}
        Err(err) if err.is_not_found() => {
            // Ignore ENOENT: caller may have already removed the file, or it may not exist at all.
        }
        Err(err) => {
            pg_log_warning(&format!("tiko_unlink: failed for relfork {relfork}: {err}",));
        }
    }
}
