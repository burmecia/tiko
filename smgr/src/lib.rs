mod aio;
pub(crate) mod buffers;
mod checkpoint;
mod pipeline;
mod smgr_impl;

/// Wait event identifiers for worker I/O operations, initialized in tiko_init()
pub(crate) static mut WAIT_EVENT_TIKO_IO_READ: u32 = 0;
pub(crate) static mut WAIT_EVENT_TIKO_IO_WRITE: u32 = 0;

/// Whether to use the worker async pipeline for I/O.
///
/// Returns `true` when running under the postmaster AND worker is alive.
/// Returns `false` during initdb, single-user mode, shutdown checkpoint
/// (worker already terminated), or worker crash — callers should fall
/// back to direct `ops` calls.
pub(crate) fn use_pipeline() -> bool {
    use core::io_control::IoControl;
    use pgsys::common::is_under_postmaster;
    is_under_postmaster() && IoControl::get().is_worker_alive()
}
