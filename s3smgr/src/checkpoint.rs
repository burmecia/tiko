use s3worker::io_queue::S3IoControl;

/// Flush all dirty cache chunks to backing files at checkpoint time.
///
/// Called directly from `CheckPointGuts()` in xlog.c after `CheckPointBuffers()`
/// has written all dirty buffer pool pages into the cache via `s3_writev()`.
/// Flushing here ensures every dirty block is in the S3-sim backing files
/// before the checkpoint WAL record is written, so that a crash and WAL
/// replay from this checkpoint yields a fully consistent image.
///
/// The `S3IoControl::is_initialized()` guard makes this safe during initdb
/// (no shared memory) and any other pre-shmem phase.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_checkpoint_flush() {
    if !S3IoControl::is_initialized() {
        return;
    }
    S3IoControl::get().cache.flush_all_dirty_chunks();
}
