//! I/O request processing for Tiko worker's Tokio runtime.
//!
//! This module receives dispatched I/O requests and performs the actual
//! block-level I/O via `ops::read_blocks` / `ops::write_blocks`.
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
use tokio::sync::mpsc;

use core::chunk::RelFork;
use pgsys::latch::SetLatch;

use core::io_control::{IoControl, IoOpKind, IoWorkRequest};
use core::ops;

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
    let control = IoControl::get();
    let pool = control.backend_pool(request.backend_id as i32);
    let slot = pool.slot(request.slot_index as usize);

    let rf = RelFork {
        spc_oid: slot.spc_oid,
        db_oid: slot.db_oid,
        rel_number: slot.rel_number,
        fork_number: slot.fork_number,
    };

    // Perform I/O based on operation type
    let (status, nblocks) = match slot.op {
        IoOpKind::Read => {
            let buffer_ptr = slot.buffer_ptr.load(Ordering::Acquire) as *mut u8;
            match ops::read_blocks(rf, slot.block_number, slot.nblocks, buffer_ptr) {
                Ok(n) => (0u32, n),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        IoOpKind::Write => {
            let buffer_ptr = slot.buffer_ptr.load(Ordering::Acquire) as *const u8;
            match ops::write_blocks(rf, slot.block_number, slot.nblocks, buffer_ptr) {
                Ok(n) => (0u32, n),
                Err(errno) => (errno as u32, 0u32),
            }
        }
        IoOpKind::Prefetch => match ops::prefetch_blocks(rf, slot.block_number, slot.nblocks) {
            Ok(n) => (0u32, n),
            Err(errno) => (errno as u32, 0u32),
        },
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
