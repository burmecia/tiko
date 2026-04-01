use core::chunk::RelFork;
use core::s3_ops;
use pgsys::{
    common::{BlockNumber, ForkNumber, INVALID_BLOCK_NUMBER},
    logging::pg_log_error,
    smgr::*,
};

/// Extend a relation fork with zero-filled blocks.
///
/// Unlike `mdzeroextend` which uses `posix_fallocate` / `FileZero` and
/// iterates across segments, S3 uses a single file per fork —
/// `ftruncate` to `(blocknum + nblocks) * BLCKSZ` zero-fills the
/// extended region on POSIX.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_zeroextend(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    _skip_fsync: bool,
) {
    let loc = unsafe { &(*reln).smgr_rlocator.locator };
    let nblocks_u32 = nblocks as u32;

    // Check for overflow: matches mdzeroextend's boundary check
    if (blocknum as u64) + (nblocks_u32 as u64) >= INVALID_BLOCK_NUMBER as u64 {
        pg_log_error(&format!(
            "tiko_zeroextend: cannot extend rel {}/{}/{} fork {} beyond block {} (requested {} + {})",
            loc.spc_oid,
            loc.db_oid,
            loc.rel_number,
            forknum,
            INVALID_BLOCK_NUMBER,
            blocknum,
            nblocks_u32
        ));
        return;
    }

    if let Err(errno) = s3_ops::cached_zeroextend(
        RelFork {
            spc_oid: loc.spc_oid,
            db_oid: loc.db_oid,
            rel_number: loc.rel_number,
            fork_number: forknum,
        },
        blocknum,
        nblocks_u32,
    ) {
        pg_log_error(&format!(
            "tiko_zeroextend: failed for rel {}/{}/{} fork {} block {} nblocks {}: errno {}",
            loc.spc_oid, loc.db_oid, loc.rel_number, forknum, blocknum, nblocks, errno
        ));
    }
}
