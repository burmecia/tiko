use pgsys::{
    common::{get_my_proc_number, is_under_postmaster},
    smgr::*,
};
use s3worker::io_queue::S3IoOpKind;

use crate::pipeline;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_readv(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut std::ffi::c_void,
    nblocks: BlockNumber,
) {
    unsafe {
        if is_under_postmaster() {
            let _result = pipeline::submit_and_wait(
                S3IoOpKind::Read,
                reln,
                forknum,
                blocknum,
                nblocks,
                *buffers as u64,
                crate::WAIT_EVENT_S3_IO_READ,
                get_my_proc_number(),
                "s3_readv",
            );
        }

        // TODO: remove once io_handler does real I/O
        mdreadv(reln, forknum, blocknum, buffers, nblocks);
    }
}
