use core::chunk::RelFork;
use core::{io_control::IoOpKind, ops};
use pgsys::common::{BLCKSZ, BlockNumber, ForkNumber, Oid, RelFileNumber};

use crate::{WAIT_EVENT_TIKO_IO_READ, WAIT_EVENT_TIKO_IO_WRITE, pipeline, use_pipeline};

/// Common implementation for AIO read/write.
///
/// Called from `pgaio_io_perform_synchronously()` inside `START_CRIT_SECTION()`.
/// **MUST NOT** call `elog(ERROR)` / `pg_log_error` — that would PANIC.
///
/// Walks the iovec entries (each a contiguous buffer range). Under the
/// postmaster with worker alive, submits each entry through the worker
/// async pipeline via `submit_and_wait_raw`. When the pipeline is unavailable
/// (initdb, shutdown checkpoint, worker crash), OR when the IO targets a
/// local (temp-table) buffer, performs direct `ops::read_blocks` /
/// `write_blocks` calls instead.
///
/// Local buffers (`is_local_buffer = true`) MUST bypass the pipeline: they
/// live in backend-local memory (`LocalBufferBlockPointers`), which is not
/// mapped in worker's address space. Sending the pointer cross-process
/// causes worker to write into its own copy-on-write fork page, leaving
/// the backend's local buffer zero — producing the
/// "unexpected zero page" error on the next B-tree read.
///
/// Returns `nblocks * BLCKSZ` on success, or `-errno` on failure.
unsafe fn perform_io(
    op: IoOpKind,
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    _nblocks: i32,
    is_local_buffer: bool,
    wait_event: u32,
    label: &str,
) -> isize {
    unsafe {
        let mut current_block = block_number;

        for i in 0..iov_length as usize {
            let entry = &*iov.add(i);
            let entry_nblocks = (entry.iov_len / BLCKSZ) as u32;

            // Local buffers are backend-local memory: never send to worker.
            let result = if use_pipeline() && !is_local_buffer {
                // Normal: submit to worker pipeline and wait for completion
                pipeline::submit_and_wait_raw(
                    op,
                    spc_oid,
                    db_oid,
                    rel_number,
                    fork_number,
                    current_block,
                    entry_nblocks,
                    entry.iov_base as u64,
                    wait_event,
                    label,
                )
                .map(|result| result.nblocks)
            } else {
                // No pipeline (initdb / shutdown / worker dead): direct ops call
                let rf = RelFork {
                    spc_oid,
                    db_oid,
                    rel_number,
                    fork_number,
                };
                match op {
                    IoOpKind::Read => ops::read_blocks(
                        &rf,
                        current_block,
                        entry_nblocks,
                        entry.iov_base as *mut u8,
                    )
                    .map_err(|e| e.to_errno()),
                    IoOpKind::Write => ops::write_blocks(
                        &rf,
                        current_block,
                        entry_nblocks,
                        entry.iov_base as *const u8,
                    )
                    .map_err(|e| e.to_errno()),
                    _ => Err(libc::ENOTSUP),
                }
            };

            match result {
                Ok(result_nblocks) => {
                    current_block += result_nblocks;
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

        (current_block as isize - block_number as isize) * (BLCKSZ as isize)
    }
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_io_perform_read(
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: i32,
    is_local_buffer: bool,
) -> isize {
    unsafe {
        perform_io(
            IoOpKind::Read,
            iov,
            iov_length,
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            is_local_buffer,
            WAIT_EVENT_TIKO_IO_READ,
            "tiko_io_perform_read",
        )
    }
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_io_perform_write(
    iov: *mut pgsys::aio::IoVec,
    iov_length: i32,
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: i32,
) -> isize {
    unsafe {
        perform_io(
            IoOpKind::Write,
            iov,
            iov_length,
            spc_oid,
            db_oid,
            rel_number,
            fork_number,
            block_number,
            nblocks,
            false, /* writes always target shared buffers */
            WAIT_EVENT_TIKO_IO_WRITE,
            "tiko_io_perform_write",
        )
    }
}
