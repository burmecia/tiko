use pgsys::smgr::*;

/// Tell the kernel to write pages back to storage.
///
/// No-op for S3 — `mdwriteback` uses `posix_fadvise(DONTNEED)` or
/// `sync_file_range` to hint the OS to flush dirty pages from the page
/// cache. S3 writes go via PUT which guarantees durability immediately;
/// no OS page cache is involved.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_writeback(
    _reln: *mut SMgrRelationData,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: BlockNumber,
) {
}
