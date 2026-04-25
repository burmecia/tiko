use core::chunk::RelFork;
use core::ops;
use pgsys::{
    common::{BLCKSZ, BlockNumber, ForkNumber, INVALID_BLOCK_NUMBER},
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
    let relfork = RelFork::from_rel(reln, forknum);
    let nblocks_u32 = nblocks as u32;

    // Check for overflow: matches mdzeroextend's boundary check
    if (blocknum as u64) + (nblocks_u32 as u64) >= INVALID_BLOCK_NUMBER as u64 {
        pg_log_error(&format!(
            "tiko_zeroextend: cannot extend relfork {relfork} beyond block {} (requested {} + {})",
            INVALID_BLOCK_NUMBER, blocknum, nblocks_u32
        ));
        return;
    }

    let buf = vec![0u8; nblocks as usize * BLCKSZ];

    if let Err(err) = ops::write_blocks(&relfork, blocknum, nblocks_u32, buf.as_ptr()) {
        pg_log_error(&format!(
            "tiko_zeroextend: failed for relfork {relfork} block {blocknum} nblocks {nblocks_u32}: {err}",
        ));
    }
}
