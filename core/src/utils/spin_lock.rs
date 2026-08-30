//! Lightweight RAII spinlock over [`AtomicU32`].
//!
//! Intended for short critical sections in shmem-resident data structures
//! where a full `Mutex` is undesirable (no allocation, no OS futex). Holds
//! `0` = free, `1` = held. Acquisition uses a CAS retry loop with a
//! [`std::hint::spin_loop`] backoff while the lock is held by someone else.

use std::hint;
use std::sync::atomic::{AtomicU32, Ordering};

/// RAII guard returned by [`spin_lock`]. Releases the lock on drop.
pub struct SpinGuard<'a>(&'a AtomicU32);

impl Drop for SpinGuard<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// Acquire the spinlock backing `l`, returning a guard that releases it on
/// drop. The caller is expected to keep the guard for the shortest possible
/// critical section.
pub fn spin_lock(l: &AtomicU32) -> SpinGuard<'_> {
    while l
        .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        while l.load(Ordering::Relaxed) != 0 {
            hint::spin_loop();
        }
    }
    SpinGuard(l)
}
