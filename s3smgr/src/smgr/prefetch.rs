use pgsys::smgr::*;
use worker::io_queue::IoOpKind;

use crate::{pipeline, use_pipeline};

/// Initiate asynchronous prefetch of blocks.
///
/// Submits a prefetch request through the pipeline to s3worker, which
/// will warm the local cache by fetching blocks from S3.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_prefetch(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
) -> bool {
    if !use_pipeline() {
        return true;
    }

    let result = unsafe {
        pipeline::submit_and_wait(
            IoOpKind::Prefetch,
            reln,
            forknum,
            blocknum,
            nblocks as u32,
            0, // buffer_ptr (unused — cache manages its own buffers)
            crate::WAIT_EVENT_S3_IO_READ,
            "s3_prefetch",
        )
    };

    result.is_some()
}
