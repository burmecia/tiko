use pgsys::{common::ForkNumber, smgr::*};

/// Mark a whole relation fork as needing fsync.
///
/// No-op for S3 — `mdregistersync` registers dirty segments with the
/// checkpointer's sync request queue for local filesystem durability.
/// S3 guarantees durability on PUT, so no sync registration is needed.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_registersync(_reln: *mut SMgrRelationData, _forknum: ForkNumber) {}
