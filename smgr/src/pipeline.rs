//! Shared async I/O pipeline for s3_readv / s3_writev.
//!
//! Both functions follow the same pattern:
//!   claim slot → fill → publish → submit → wake worker → wait completion → read result → release
//!
//! This module extracts that common logic into `submit_and_wait`.

use std::sync::atomic::Ordering;

use core::io_control::*;
use pgsys::{
    common::{BlockNumber, ForkNumber, Oid, RelFileNumber, get_my_proc_number},
    latch::*,
    logging::*,
    smgr::*,
};

/// POSIX ENOENT (No such file or directory) — constant to avoid libc dependency.
const ENOENT: i32 = 2;

/// Result of a completed async I/O request.
#[allow(dead_code)]
pub struct IoResult {
    pub status: u32,
    pub nblocks: u32,
}

/// Submit an I/O request through the async pipeline and block until completion.
///
/// Returns `Some(IoResult)` on normal completion, or `None` if worker died
/// at any point during the pipeline (the caller should handle this — currently
/// we just log and return, matching the pre-refactor behavior).
///
/// # Safety
/// Caller must ensure `reln` is a valid PG SMgrRelation pointer and
/// `buffer_ptr` points to a valid PG buffer page.
pub unsafe fn submit_and_wait(
    op: IoOpKind,
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: u64,
    wait_event: u32,
    label: &str,
) -> Option<IoResult> {
    unsafe {
        let loc = &(*reln).smgr_rlocator.locator;
        let result = submit_and_wait_raw(
            op,
            loc.spc_oid,
            loc.db_oid,
            loc.rel_number,
            forknum,
            blocknum,
            nblocks,
            buffer_ptr,
            wait_event,
            label,
        );
        match result {
            Ok(io_result) => Some(io_result),
            Err(_errno) => {
                pg_log_error(&format!(
                    "{}({}): I/O failed for rel {} fork {} block {}: errno {}",
                    label,
                    get_my_proc_number(),
                    loc.rel_number,
                    forknum,
                    blocknum,
                    _errno
                ));
                None
            }
        }
    }
}

/// Critical-section-safe variant: takes raw relation identity instead of a pointer.
///
/// Returns `Ok(IoResult)` on success, or `Err(errno)` on failure.
///
/// **MUST NOT call `pg_log_error`** — this function may be called from within
/// `pgaio_io_perform_synchronously`'s `START_CRIT_SECTION()`, where `elog(ERROR)`
/// escalates to PANIC. Uses `pg_log_warning` for diagnostics instead.
pub unsafe fn submit_and_wait_raw(
    op: IoOpKind,
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: u64,
    wait_event: u32,
    label: &str,
) -> Result<IoResult, i32> {
    unsafe {
        let control = IoControl::get();
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);

        // 1. Claim a slot from our own pool (zero contention).
        let slot_idx = loop {
            if let Some(idx) = pool.try_claim() {
                break idx;
            }
            if !control.is_worker_alive() {
                pg_log_warning(&format!(
                    "{}({}): worker is not running, cannot process I/O",
                    label, proc_num
                ));
                return Err(ENOENT);
            }
            ResetLatch(MyLatch);
            if let Some(idx) = pool.try_claim() {
                break idx;
            }
            WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, wait_event);
        };
        let slot = pool.slot(slot_idx);

        // 2. Fill request fields via raw pointer (private to this backend, no races).
        let slot_ptr = slot as *const IoSlot as *mut IoSlot;
        (*slot_ptr).op = op;
        (*slot_ptr).spc_oid = spc_oid;
        (*slot_ptr).db_oid = db_oid;
        (*slot_ptr).rel_number = rel_number;
        (*slot_ptr).fork_number = forknum;
        (*slot_ptr).block_number = blocknum;
        (*slot_ptr).nblocks = nblocks;
        slot.owner_proc.store(proc_num, Ordering::Relaxed);
        slot.owner_latch.store(MyLatch as u64, Ordering::Relaxed);
        slot.buffer_ptr.store(buffer_ptr, Ordering::Relaxed);
        slot.result_status.store(0, Ordering::Relaxed);
        slot.result_nblocks.store(0, Ordering::Relaxed);

        // 3. Publish (Filling → Submitted)
        slot.publish();

        // 4. Push to MPSC submit queue.
        while !control.submit_queue.push(proc_num as u32, slot_idx as u8) {
            if !control.is_worker_alive() {
                pool.release(slot_idx);
                pg_log_warning(&format!(
                    "{}({}): worker died while waiting to submit",
                    label, proc_num
                ));
                return Err(ENOENT);
            }
            ResetLatch(MyLatch);
            if control.submit_queue.push(proc_num as u32, slot_idx as u8) {
                break;
            }
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                10,
                wait_event,
            );
        }

        // 5. Wake worker via SetLatch
        let worker_latch = control.worker_latch.load(Ordering::Acquire) as *mut Latch;
        if !worker_latch.is_null() {
            SetLatch(worker_latch);
        }

        pg_log_debug2(&format!(
            "{}({}): submitted {:?} for rel {} fork {} block {} nblocks {}",
            label, proc_num, op, rel_number, forknum, blocknum, nblocks
        ));

        // 6. Wait for completion via WaitLatch
        loop {
            ResetLatch(MyLatch);
            if slot.current_state() == SlotState::Completed {
                break;
            }
            if !control.is_worker_alive() {
                pool.release(slot_idx);
                pg_log_warning(&format!(
                    "{}({}): worker died while waiting for I/O completion",
                    label, proc_num
                ));
                return Err(ENOENT);
            }
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                wait_event,
            );
        }

        // 7. Read result
        let result_status = slot.result_status.load(Ordering::Acquire);
        let result_nblocks = slot.result_nblocks.load(Ordering::Acquire);
        if result_status != 0 {
            pg_log_warning(&format!(
                "{}({}): I/O error for rel {} block {}: status {}, nblocks {}",
                label, proc_num, rel_number, blocknum, result_status, result_nblocks
            ));
        }

        // 8. Release slot back to pool (Completed → Free + set free bit)
        pool.release(slot_idx);

        pg_log_debug2(&format!(
            "{}({}): completed {:?} for rel {} block {} nblocks {}, result: (status {}, nblocks {}), slot {} released",
            label,
            proc_num,
            op,
            rel_number,
            blocknum,
            nblocks,
            result_status,
            result_nblocks,
            slot_idx
        ));

        if result_status != 0 {
            Err(result_status as i32)
        } else {
            Ok(IoResult {
                status: result_status,
                nblocks: result_nblocks,
            })
        }
    }
}
