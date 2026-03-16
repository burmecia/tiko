use pgsys::smgr::*;
use worker::io_queue::IoControl;

/// Immediately flush dirty cache chunks for a relation fork to backing files.
///
/// Called by `smgrdosyncall()` when PostgreSQL explicitly requests an
/// immediate sync (e.g. `FlushRelationBuffers`, `DROP TABLE`). Unlike the
/// normal checkpoint path (handled by `s3_checkpoint_flush`), this targets
/// only the given relation fork.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_immedsync(reln: *mut SMgrRelationData, forknum: ForkNumber) {
    if !IoControl::is_initialized() {
        return;
    }
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    IoControl::get().cache.flush_dirty_chunks_for_relation(
        loc.spc_oid,
        loc.db_oid,
        loc.rel_number,
        forknum,
    );
}
