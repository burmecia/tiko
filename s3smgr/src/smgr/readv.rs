use std::sync::atomic::Ordering;

use pgsys::{common::{get_my_proc_number, is_under_postmaster}, latch::*, logging::*, smgr::*};
use s3worker::io_queue::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_readv(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut std::ffi::c_void,
    nblocks: BlockNumber,
) {
    unsafe {
        // During initdb (bootstrap and single-user modes) there is no postmaster
        // and no s3worker process. Fall back to md.
        if !is_under_postmaster() {
            mdreadv(reln, forknum, blocknum, buffers, nblocks);
            return;
        }

        let control = S3IoControl::get();
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);

        // 1. Claim a slot from our own pool (zero contention).
        //    If all 4 slots are in-flight, wait until one completes.
        //    Tokio calls SetLatch(owner_latch) on completion, waking us.
        let slot_idx = loop {
            if let Some(idx) = pool.try_claim() {
                break idx;
            }
            if !control.is_s3worker_alive() {
                pg_log_error(&format!(
                    "s3_readv({}): s3worker is not running, cannot process I/O",
                    proc_num
                ));
                return;
            }
            // Standard ResetLatch-check-WaitLatch pattern to avoid missed wakeups
            ResetLatch(MyLatch);
            if let Some(idx) = pool.try_claim() {
                break idx;
            }
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                -1,
                crate::WAIT_EVENT_S3_IO_READ,
            );
        };
        let slot = pool.slot(slot_idx);

        // 2. Fill request fields via raw pointer (private to this backend, no races).
        //    The owning backend has exclusive logical access during the Filling phase,
        //    but pool.slot() returns &S3IoSlot, so we use a raw pointer for the
        //    non-atomic fields.
        let loc = &(*reln).smgr_rlocator.locator;
        let slot_ptr = slot as *const S3IoSlot as *mut S3IoSlot;
        (*slot_ptr).op = S3IoOpKind::Read;
        (*slot_ptr).spc_oid = loc.spc_oid;
        (*slot_ptr).db_oid = loc.db_oid;
        (*slot_ptr).rel_number = loc.rel_number;
        (*slot_ptr).fork_number = forknum;
        (*slot_ptr).block_number = blocknum;
        (*slot_ptr).nblocks = nblocks;
        slot.owner_proc.store(proc_num, Ordering::Relaxed);
        slot.owner_latch.store(MyLatch as u64, Ordering::Relaxed);
        slot.buffer_ptr.store(*buffers as u64, Ordering::Relaxed);
        slot.result_status.store(0, Ordering::Relaxed);
        slot.result_nblocks.store(0, Ordering::Relaxed);

        // 3. Publish (Filling → Submitted)
        slot.publish();

        // 4. Push to MPSC submit queue.
        //    If full, wait until s3worker drains entries.
        //    s3worker sets our latch indirectly via slot completions.
        while !control.submit_queue.push(proc_num as u32, slot_idx as u8) {
            if !control.is_s3worker_alive() {
                pool.release(slot_idx);
                pg_log_error(&format!(
                    "s3_readv({}): s3worker died while waiting to submit",
                    proc_num
                ));
                return;
            }
            ResetLatch(MyLatch);
            if control.submit_queue.push(proc_num as u32, slot_idx as u8) {
                break;
            }
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                10, // 10ms retry — submit queue drains fast
                crate::WAIT_EVENT_S3_IO_READ,
            );
        }

        // 5. Wake s3worker via SetLatch
        let s3worker_latch = control.s3worker_latch.load(Ordering::Acquire) as *mut Latch;
        if !s3worker_latch.is_null() {
            SetLatch(s3worker_latch);
        }

        pg_log_debug1(&format!(
            "s3_readv({}): submitted read for rel {} fork {} block {} nblocks {}",
            proc_num, loc.rel_number, forknum, blocknum, nblocks
        ));

        // 6. Wait for completion via WaitLatch
        loop {
            ResetLatch(MyLatch);
            if slot.current_state() == SlotState::Completed {
                break;
            }
            if !control.is_s3worker_alive() {
                pool.release(slot_idx);
                pg_log_error(&format!(
                    "s3_readv({}): s3worker died while waiting for I/O completion",
                    proc_num
                ));
                return;
            }
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000, // 1s timeout to recheck s3worker liveness
                crate::WAIT_EVENT_S3_IO_READ,
            );
        }

        // 7. Read result
        let result_status = slot.result_status.load(Ordering::Acquire);
        let result_nblocks = slot.result_nblocks.load(Ordering::Acquire);
        if result_status != 0 {
            // TODO: proper error handling based on status code (e.g. ENOENT = zero-fill, EIO = error)
            pg_log_warning(&format!(
                "s3_readv({}): I/O error for rel {} block {}: status {}, nblocks {}",
                proc_num, loc.rel_number, blocknum, result_status, result_nblocks
            ));
        }

        // 8. Release slot back to pool (Completed → Free + set free bit)
        pool.release(slot_idx);

        pg_log_debug1(&format!(
            "s3_readv({}): completed read for rel {} block {} nblocks {}, result: (status {}, nblocks {}), slot {} released",
            proc_num, loc.rel_number, blocknum, nblocks, result_status, result_nblocks, slot_idx
        ));
    }
}
