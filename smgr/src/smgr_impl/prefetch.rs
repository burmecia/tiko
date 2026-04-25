use core::io_control::IoOpKind;
use pgsys::{
    common::{BlockNumber, ForkNumber},
    smgr::*,
};

use crate::{pipeline, use_pipeline};

/// Initiate asynchronous prefetch of blocks.
///
/// Submits a prefetch request through the pipeline to worker, which
/// will warm the local cache by fetching blocks from S3.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_prefetch(
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
            crate::WAIT_EVENT_TIKO_IO_READ,
            "tiko_prefetch",
        )
    };

    result.is_some()
}
