//! I/O request processing for s3worker's Tokio runtime.
//!
//! This module receives dispatched I/O requests and performs the actual
//! block-level I/O via `s3_ops::read_blocks` / `s3_ops::write_blocks`.
//!
//! # Completion Path
//!
//! After I/O completes on a Tokio thread:
//! 1. Write result fields to the slot (`result_status`, `result_nblocks`)
//! 2. Mark slot completed (`mark_completed()` — Release fence)
//! 3. Call `SetLatch(owner_latch)` to wake the backend directly
//!
//! This eliminates the harvest step — Tokio notifies backends directly.

use std::sync::atomic::Ordering;

use pgsys::latch::SetLatch;
use tokio::sync::mpsc;

use crate::dispatcher::IoWorkRequest;
use crate::io_queue::{S3IoControl, S3IoOpKind};
use crate::s3_ops;

/// Main I/O worker loop — receives requests from the dispatcher channel
/// and spawns a Tokio task for each request.
///
/// Each request is processed in its own task for parallel I/O. The receiver
/// shuts down cleanly when the Dispatcher (sender) is dropped.
pub async fn io_worker_loop(mut rx: mpsc::Receiver<IoWorkRequest>) {
    while let Some(request) = rx.recv().await {
        tokio::spawn(process_io_request(request));
    }
}

/// Process a single I/O request.
///
/// Looks up the slot in shared memory, performs the I/O operation,
/// writes results to the slot, marks it completed, and wakes the backend via SetLatch.
async fn process_io_request(request: IoWorkRequest) {
    let control = S3IoControl::get();
    let pool = control.backend_pool(request.backend_id as i32);
    let slot = pool.slot(request.slot_index as usize);

    // Perform I/O based on operation type
    let (status, nblocks) = match slot.op {
        S3IoOpKind::Read => {
            let buffer_ptr = slot.buffer_ptr.load(Ordering::Acquire) as *mut u8;
            match s3_ops::cached_read_blocks(
                slot.spc_oid,
                slot.db_oid,
                slot.rel_number,
                slot.fork_number,
                slot.block_number,
                slot.nblocks,
                buffer_ptr,
            ) {
                Ok(n) => (0u32, n),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::Write => {
            let buffer_ptr = slot.buffer_ptr.load(Ordering::Acquire) as *const u8;
            match s3_ops::cached_write_blocks(
                slot.spc_oid,
                slot.db_oid,
                slot.rel_number,
                slot.fork_number,
                slot.block_number,
                slot.nblocks,
                buffer_ptr,
            ) {
                Ok(n) => (0u32, n),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::Exists => {
            if s3_ops::file_exists(slot.spc_oid, slot.db_oid, slot.rel_number, slot.fork_number) {
                (0u32, 1)
            } else {
                // file doesn't exist — not an error, just report 0 nblocks
                (0u32, 0)
            }
        }
        S3IoOpKind::Create => {
            match s3_ops::create_file(slot.spc_oid, slot.db_oid, slot.rel_number, slot.fork_number)
            {
                Ok(created) => (0u32, if created { 1 } else { 0 }),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::Nblocks => {
            match s3_ops::file_nblocks(slot.spc_oid, slot.db_oid, slot.rel_number, slot.fork_number)
            {
                Ok(n) => (0u32, n),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::Prefetch => {
            // TODO: warm the local cache by fetching blocks from S3.
            // On cache miss: issue S3 GET for the requested block range,
            // write the fetched data into the local cache file. On cache
            // hit: no-op. This allows subsequent s3_readv calls to hit
            // the cache instead of going to S3.
            (0u32, 0u32)
        }
        S3IoOpKind::Truncate => {
            // Target nblocks is stored in block_number
            match s3_ops::truncate_file(
                slot.spc_oid,
                slot.db_oid,
                slot.rel_number,
                slot.fork_number,
                slot.block_number,
            ) {
                Ok(()) => (0u32, 0u32),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::Unlink => {
            match s3_ops::delete_file(slot.spc_oid, slot.db_oid, slot.rel_number, slot.fork_number)
            {
                Ok(()) => (0u32, 0u32),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        S3IoOpKind::ZeroExtend => {
            match s3_ops::zeroextend_file(
                slot.spc_oid,
                slot.db_oid,
                slot.rel_number,
                slot.fork_number,
                slot.block_number,
                slot.nblocks,
            ) {
                Ok(()) => (0u32, 0u32),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        _ => {
            // Unsupported operation
            (libc::ENOTSUP as u32, 0u32)
        }
    };

    // Check generation before writing results — if the slot was recycled by a new
    // backend (attach() bumped generation), discard this stale completion silently.
    let current_gen = slot.generation.load(Ordering::Relaxed);
    if current_gen != request.generation {
        // Slot was recycled. Do NOT write results, mark_completed, or SetLatch.
        // The new backend will have reset this slot to Free state.
        return;
    }

    // Write result fields (must happen before mark_completed)
    slot.result_status.store(status, Ordering::Relaxed);
    slot.result_nblocks.store(nblocks, Ordering::Relaxed);

    // Mark completed (Release fence ensures results visible before state change)
    slot.mark_completed();

    // Wake the backend directly — no main-thread harvest step
    let latch = slot.owner_latch.load(Ordering::Acquire) as *mut pgsys::latch::Latch;
    if !latch.is_null() {
        unsafe {
            SetLatch(latch);
        }
    }
}
