use pgsys::{logging::pg_log_warning, smgr::*};
use s3worker::s3_ops;

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
pub extern "C-unwind" fn s3_unlink(
    rlocator: *mut RelFileLocatorBackend,
    forknum: ForkNumber,
    _is_redo: bool,
) {
    if forknum == INVALID_FORK_NUMBER {
        for fork in 0..=MAX_FORKNUM {
            unlink_fork(rlocator, fork);
        }
    } else {
        unlink_fork(rlocator, forknum);
    }
}

fn unlink_fork(rlocator: *mut RelFileLocatorBackend, forknum: ForkNumber) {
    let loc = unsafe { &(*rlocator).locator };

    if let Err(errno) = s3_ops::cached_delete_file(loc.spc_oid, loc.db_oid, loc.rel_number, forknum)
    {
        pg_log_warning(&format!(
            "s3_unlink: could not remove rel {}/{}/{} fork {}: errno {}",
            loc.spc_oid, loc.db_oid, loc.rel_number, forknum, errno
        ));
    }
}
