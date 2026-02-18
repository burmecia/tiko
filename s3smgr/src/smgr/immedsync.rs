use pgsys::smgr::*;

/// Immediately sync a relation fork to stable storage.
///
/// No-op for S3 — durability is guaranteed by the S3 PUT operation itself.
/// No local fsync needed (unlike mdimmedsync which fsyncs all segments).
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_immedsync(_reln: *mut SMgrRelationData, _forknum: ForkNumber) {}
