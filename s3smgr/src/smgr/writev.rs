use pgsys::{
    common::{get_my_proc_number, is_under_postmaster},
    smgr::*,
};
use s3worker::io_queue::S3IoOpKind;

use crate::pipeline;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_writev(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *const *const std::ffi::c_void,
    nblocks: BlockNumber,
    skip_fsync: bool,
) {
    unsafe {
        if is_under_postmaster() {
            let _result = pipeline::submit_and_wait(
                S3IoOpKind::Write,
                reln,
                forknum,
                blocknum,
                nblocks,
                *buffers as u64,
                crate::WAIT_EVENT_S3_IO_WRITE,
                get_my_proc_number(),
                "s3_writev",
            );
        }

        // TODO: remove once io_handler does real I/O
        mdwritev(reln, forknum, blocknum, buffers, nblocks, skip_fsync);
    }
}
