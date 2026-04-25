use core::chunk::RelFork;
use core::io_control::IoControl;
use core::ops;
use pgsys::{common::ForkNumber, smgr::*};

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
    let relfork = RelFork::from_rel(reln, forknum);
    ops::flush_dirty_for_relfork(&relfork).ok();
}
