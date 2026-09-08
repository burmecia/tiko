//! Buffer coalescing utilities.
//!
//! PG passes `void **buffers` — an array of per-block buffer pointers that
//! may or may not be contiguous in memory. `buffers_to_iov` coalesces
//! adjacent buffers into contiguous iovec runs, mirroring md.c's
//! `buffers_to_iovec`.

use core::io_control::IoControl;
use pgsys::{
    aio::IoVec,
    common::{BlockNumber, BLCKSZ},
};

/// Whether to use the worker async pipeline for I/O.
///
/// Returns `true` when running under the postmaster AND worker is alive.
/// Returns `false` during initdb, single-user mode, shutdown checkpoint
/// (worker already terminated), or worker crash — callers should fall
/// back to direct `relfork::ops` calls.
pub(crate) fn use_pipeline() -> bool {
    use pgsys::common::is_under_postmaster;
    is_under_postmaster() && IoControl::get().is_worker_alive()
}

/// Coalesce adjacent buffer pointers into contiguous iovec runs.
///
/// Returns a `Vec<IoVec>` where each entry represents a contiguous
/// range of BLCKSZ buffers. Adjacent buffers are merged into a single
/// entry with combined `iov_len`.
///
/// # Safety
/// `buffers` must point to a valid array of at least `nblocks` pointers.
pub(crate) unsafe fn buffers_to_iov(
    buffers: *const *const std::ffi::c_void,
    nblocks: BlockNumber,
) -> Vec<IoVec> {
    let mut iov = Vec::new();

    for i in 0..nblocks as usize {
        let base = unsafe { *buffers.add(i) };

        if let Some(prev) = iov.last_mut() {
            let prev: &mut IoVec = prev;
            let prev_end = (prev.iov_base as usize) + prev.iov_len;
            if prev_end == base as usize {
                prev.iov_len += BLCKSZ;
                continue;
            }
        }

        iov.push(IoVec {
            iov_base: base as *mut _,
            iov_len: BLCKSZ,
        });
    }

    iov
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_case(bases: &[*const std::ffi::c_void], expected: &[(usize, usize)]) {
        let iov = unsafe { buffers_to_iov(bases.as_ptr(), bases.len() as BlockNumber) };
        assert_eq!(iov.len(), expected.len());
        for (entry, &(base, len)) in iov.iter().zip(expected) {
            assert_eq!(entry.iov_base as usize, base);
            assert_eq!(entry.iov_len, len);
        }
    }

    #[test]
    fn empty() {
        let bases: Vec<*const std::ffi::c_void> = Vec::new();
        let iov = unsafe { buffers_to_iov(bases.as_ptr(), 0) };
        assert!(iov.is_empty());
    }

    #[test]
    fn single_block() {
        let buf = [0u8; BLCKSZ];
        let base = buf.as_ptr() as *const std::ffi::c_void;
        run_case(&[base], &[(base as usize, BLCKSZ)]);
    }

    #[test]
    fn fully_contiguous() {
        let buf = vec![0u8; 4 * BLCKSZ];
        let base = buf.as_ptr() as usize;
        let bases: Vec<*const std::ffi::c_void> = (0..4)
            .map(|i| (base + i * BLCKSZ) as *const std::ffi::c_void)
            .collect();
        run_case(&bases, &[(base, 4 * BLCKSZ)]);
    }

    #[test]
    fn fully_discontiguous() {
        let buf = vec![0u8; BLCKSZ];
        let base = buf.as_ptr() as usize;
        let bases: Vec<*const std::ffi::c_void> = (0..3)
            .map(|_| buf.as_ptr() as *const std::ffi::c_void)
            .collect();
        let expected: Vec<(usize, usize)> = (0..3).map(|_| (base, BLCKSZ)).collect();
        run_case(&bases, &expected);
    }

    #[test]
    fn mixed_runs() {
        // Two adjacent blocks (merge into one run), then a lone block from a
        // separate allocation, then one more block back in the first region —
        // but the previous run ends at the lone block, so no merge.
        let contiguous = vec![0u8; 2 * BLCKSZ];
        let lone = [0u8; BLCKSZ];
        let c = contiguous.as_ptr() as usize;
        let l = lone.as_ptr() as usize;
        let bases = vec![
            c as *const std::ffi::c_void,
            (c + BLCKSZ) as *const std::ffi::c_void,
            l as *const std::ffi::c_void,
            (c + BLCKSZ) as *const std::ffi::c_void,
        ];
        run_case(
            &bases,
            &[(c, 2 * BLCKSZ), (l, BLCKSZ), (c + BLCKSZ, BLCKSZ)],
        );
    }

    #[test]
    fn repeated_pointer_breaks_run() {
        // Same buffer twice in a row: prev_end (base + BLCKSZ) != base,
        // so it must not be merged into a single entry.
        let buf = [0u8; BLCKSZ];
        let base = buf.as_ptr() as usize;
        let bases = [
            buf.as_ptr() as *const std::ffi::c_void,
            buf.as_ptr() as *const std::ffi::c_void,
        ];
        run_case(&bases, &[(base, BLCKSZ), (base, BLCKSZ)]);
    }
}
