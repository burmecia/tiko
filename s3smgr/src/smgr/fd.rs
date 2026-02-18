use pgsys::{common::BLCKSZ, smgr::*};

/// Return a raw file descriptor and byte offset for a given block.
///
/// Used by PG18 AIO when IO workers need to re-open the fd in their own
/// process. S3 operations use `PGAIO_OP_S3_READV`/`PGAIO_OP_S3_WRITEV`
/// which bypass `smgrfd()` entirely (see smgr.c), so this should never
/// be called in practice. Returns -1 (invalid fd) as a safety measure.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_fd(
    _reln: *mut SMgrRelationData,
    _forknum: ForkNumber,
    blocknum: BlockNumber,
    off: *mut u32,
) -> i32 {
    unsafe {
        *off = blocknum * BLCKSZ as u32;
    }
    -1
}
