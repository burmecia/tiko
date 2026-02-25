use pgsys::smgr::*;
use s3worker::io_queue::S3IoControl;

/// Immediately flush dirty cache chunks for a relation fork to backing files.
///
/// Called by `smgrdosyncall()` when PostgreSQL explicitly requests an
/// immediate sync (e.g. `FlushRelationBuffers`, `DROP TABLE`). Unlike the
/// normal checkpoint path (handled by `s3_checkpoint_flush`), this targets
/// only the given relation fork.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_immedsync(reln: *mut SMgrRelationData, forknum: ForkNumber) {
    if !S3IoControl::is_initialized() {
        return;
    }
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    S3IoControl::get().cache.flush_dirty_chunks_for_relation(
        loc.spc_oid,
        loc.db_oid,
        loc.rel_number,
        forknum,
    );
}
