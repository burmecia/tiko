mod pipeline;
mod smgr;

/// Wait event identifiers for S3 I/O operations, initialized in s3_init()
pub(crate) static mut WAIT_EVENT_S3_IO_READ: u32 = 0;
pub(crate) static mut WAIT_EVENT_S3_IO_WRITE: u32 = 0;
