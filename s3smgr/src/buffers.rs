//! Buffer coalescing utilities.
//!
//! PG passes `void **buffers` — an array of per-block buffer pointers that
//! may or may not be contiguous in memory. `buffers_to_iov` coalesces
//! adjacent buffers into contiguous iovec runs, mirroring md.c's
//! `buffers_to_iovec`.

use pgsys::{
    aio::IoVec,
    common::{BLCKSZ, BlockNumber},
};

/// Coalesce adjacent buffer pointers into contiguous iovec runs.
///
/// Returns a `Vec<IoVec>` where each entry represents a contiguous
/// range of BLCKSZ buffers. Adjacent buffers are merged into a single
/// entry with combined `iov_len`.
///
/// # Safety
/// `buffers` must point to a valid array of at least `nblocks` pointers.
pub unsafe fn buffers_to_iov(
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
