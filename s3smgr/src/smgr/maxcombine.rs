use pgsys::smgr::*;

/// Return the maximum number of blocks that can be combined in a single
/// I/O starting at `blocknum`.
///
/// `mdmaxcombine` returns `RELSEG_SIZE - blocknum % RELSEG_SIZE` because
/// md splits files into 1 GB segments. S3 has no segments (one object per
/// fork), so there is no segment boundary — return the theoretical max.
/// PG's `PG_IOV_MAX` and buffer manager logic cap the actual I/O size.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_maxcombine(
    _reln: *mut SMgrRelationData,
    _forknum: ForkNumber,
    blocknum: BlockNumber,
) -> u32 {
    u32::MAX - blocknum
}
