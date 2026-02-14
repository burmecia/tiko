use pgsys::common::{BLCKSZ, get_my_proc_number};
use s3worker::io_queue::S3IoOpKind;

use crate::{WAIT_EVENT_S3_IO_READ, WAIT_EVENT_S3_IO_WRITE, pipeline};

/// Common implementation for S3 AIO read/write.
///
/// Called from `pgaio_io_perform_synchronously()` inside `START_CRIT_SECTION()`.
/// **MUST NOT** call `elog(ERROR)` / `pg_log_error` — that would PANIC.
///
/// Walks the iovec entries (each a contiguous buffer range), submitting one
/// request per entry through the s3worker async pipeline via `submit_and_wait_raw`.
///
/// Returns `nblocks * BLCKSZ` on success, or `-errno` on failure.
unsafe fn s3_io_perform(
    op: S3IoOpKind,
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: u32,
    db_oid: u32,
    rel_number: u32,
    fork_number: u32,
    block_number: u32,
    nblocks: i32,
    wait_event: u32,
    label: &str,
) -> isize {
    unsafe {
        let proc_num = get_my_proc_number();
        let mut current_block = block_number;

        for i in 0..iov_length as usize {
            let entry = &*iov.add(i);
            let entry_nblocks = (entry.iov_len / BLCKSZ) as u32;

            // Submit this iovec entry as a separate request to the pipeline, and wait for completion.
            let result = pipeline::submit_and_wait_raw(
                op,
                spc_oid,
                db_oid,
                rel_number,
                fork_number,
                current_block,
                entry_nblocks,
                entry.iov_base as u64,
                wait_event,
                proc_num,
                label,
            );

            match result {
                Ok(_) => {
                    current_block += entry_nblocks;
                }
                Err(errno) => {
                    let blocks_done = current_block - block_number;
                    if blocks_done > 0 {
                        return (blocks_done as isize) * (BLCKSZ as isize);
                    }
                    return -(errno as isize);
                }
            }
        }

        (nblocks as isize) * (BLCKSZ as isize)
    }
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_io_perform_read(
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: u32,
    db_oid: u32,
    rel_number: u32,
    fork_number: u32,
    block_number: u32,
    nblocks: i32,
) -> isize {
    unsafe {
        s3_io_perform(
            S3IoOpKind::Read,
            iov,
            iov_length,
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            WAIT_EVENT_S3_IO_READ,
            "s3_io_perform_read",
        )
    }
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_io_perform_write(
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: u32,
    db_oid: u32,
    rel_number: u32,
    fork_number: u32,
    block_number: u32,
    nblocks: i32,
) -> isize {
    unsafe {
        s3_io_perform(
            S3IoOpKind::Write,
            iov,
            iov_length,
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            WAIT_EVENT_S3_IO_WRITE,
            "s3_io_perform_write",
        )
    }
}
