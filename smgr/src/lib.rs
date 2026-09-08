mod aio;
mod checkpoint;
mod pipeline;
mod smgr_impl;
pub(crate) mod utils;

/// Wait event identifiers for worker I/O operations, initialized in tiko_init()
pub(crate) static mut WAIT_EVENT_TIKO_IO_READ: u32 = 0;
pub(crate) static mut WAIT_EVENT_TIKO_IO_WRITE: u32 = 0;
pub(crate) static mut WAIT_EVENT_TIKO_COMPACTION: u32 = 0;
