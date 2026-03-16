mod aio;
pub(crate) mod buffers;
mod checkpoint;
mod pipeline;
mod smgr;

/// Wait event identifiers for S3 I/O operations, initialized in s3_init()
pub(crate) static mut WAIT_EVENT_S3_IO_READ: u32 = 0;
pub(crate) static mut WAIT_EVENT_S3_IO_WRITE: u32 = 0;

/// Whether to use the s3worker async pipeline for I/O.
///
/// Returns `true` when running under the postmaster AND worker is alive.
/// Returns `false` during initdb, single-user mode, shutdown checkpoint
/// (worker already terminated), or worker crash — callers should fall
/// back to direct `s3_ops` calls.
pub(crate) fn use_pipeline() -> bool {
    use pgsys::smgr::is_under_postmaster;
    use worker::io_queue::IoControl;
    is_under_postmaster() && IoControl::get().is_worker_alive()
}
