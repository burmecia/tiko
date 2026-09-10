//! I/O request processing for Tiko worker's Tokio runtime.
//!
//! This module receives dispatched I/O requests and performs the actual
//! block-level I/O via `ops::read_blocks` / `ops::write_blocks`.
//!
//! # Completion Path
//!
//! After I/O completes on a Tokio thread:
//! 1. Pin the slot (`try_start_completing()` — CAS (gen, InProgress) → (gen, Completing))
//! 2. Write result fields to the slot (`result_status`, `result_nblocks`)
//! 3. Finish completion (`finish_completing()` — CAS (gen, Completing) → (gen, Completed))
//! 4. Call `SetLatch(owner_latch)` to wake the backend directly
//!
//! The generation is part of the CAS word, so a slot recycled by `attach()`
//! (old backend died, new backend reusing the ProcNumber) can never be matched
//! by a stale completer — even if the new request reached InProgress again.
//!
//! This eliminates the harvest step — Tokio notifies backends directly.
//!
//! The whole request is wrapped in `catch_unwind`: a dropped completion would
//! leave the slot InProgress forever and hang the backend in its wait loop,
//! so a panicking task fails the slot with EIO instead.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

use core::{
    io_control::{IoControl, IoOpKind, IoWorkRequest, SlotState},
    relfork::{RelFork, ops},
};
use pgsys::latch::SetLatch;

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
/// Performs the I/O from the dispatch-time snapshot (never re-reading slot
/// fields), then completes via the pinned protocol and wakes the backend via
/// SetLatch. If the slot was recycled at any point, the result is discarded
/// silently — the new backend's own request will complete on its own.
async fn process_io_request(request: IoWorkRequest) {
    let control = IoControl::get();
    let pool = control.backend_pool(request.backend_id as i32);
    let slot = pool.slot(request.slot_index as usize);

    // Revalidate before touching buffers: if the slot was recycled while this
    // task waited to be scheduled, the snapshot's buffer may have been reused.
    if !slot.is_state(request.generation, SlotState::InProgress) {
        return;
    }

    let (status, nblocks) = match std::panic::catch_unwind(AssertUnwindSafe(|| do_io(&request))) {
        Ok(result) => result,
        Err(_) => (libc::EIO, 0u32),
    };

    // Pin the slot for completion. Fails if attach() recycled it — generation
    // mismatch is part of the CAS word, so a recycled slot can never match even
    // if the new request reached InProgress again.
    if !slot.try_start_completing(request.generation) {
        return;
    }

    // Write result fields (only the pin winner may write these)
    slot.result_status.store(status, Ordering::Relaxed);
    slot.result_nblocks.store(nblocks, Ordering::Relaxed);

    if !slot.finish_completing(request.generation) {
        // Recycled while pinned — discard; the new request completes on its own.
        return;
    }

    // Wake the backend directly — no main-thread harvest step
    let latch = slot.owner_latch.load(Ordering::Acquire) as *mut pgsys::latch::Latch;
    if !latch.is_null() {
        unsafe {
            SetLatch(latch);
        }
    }
}

/// Perform I/O based on operation type, using only the dispatch-time snapshot.
fn do_io(request: &IoWorkRequest) -> (i32, u32) {
    let rf = RelFork {
        spc_oid: request.spc_oid,
        db_oid: request.db_oid,
        rel_number: request.rel_number,
        fork_number: request.fork_number,
    };

    match request.op {
        IoOpKind::Read => {
            let buffer_ptr = request.buffer_ptr as *mut u8;
            match ops::read_blocks(&rf, request.block_number, request.nblocks, buffer_ptr) {
                Ok(n) => (0i32, n),
                Err(e) => (e.to_errno(), 0u32),
            }
        }
        IoOpKind::Write => {
            let buffer_ptr = request.buffer_ptr as *const u8;
            match ops::write_blocks(&rf, request.block_number, request.nblocks, buffer_ptr) {
                Ok(n) => (0i32, n),
                Err(e) => (e.to_errno(), 0u32),
            }
        }
        IoOpKind::Prefetch => {
            match ops::prefetch_blocks(&rf, request.block_number, request.nblocks) {
                Ok(n) => (0i32, n),
                Err(_) => (libc::EIO, 0u32),
            }
        }
        _ => (libc::ENOTSUP, 0u32),
    }
}
