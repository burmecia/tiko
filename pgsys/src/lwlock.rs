const LWLOCKMODE_LW_EXCLUSIVE: u32 = 0;
const LWLOCKMODE_LW_SHARED: u32 = 1;
const LWLOCKMODE_LW_WAIT_UNTIL_FREE: u32 = 2;

#[repr(C)]
pub struct LWLock {
    // Opaque structure representing a lightweight lock in PostgreSQL
    _private: [u8; 0],
}

unsafe extern "C" {
    fn LWLockAcquire(lock: *mut LWLock, mode: u32) -> bool;
    fn LWLockRelease(lock: *mut LWLock);
}

pub fn acquire_lwlock_exclusive(lock: *mut LWLock) -> bool {
    unsafe { LWLockAcquire(lock, LWLOCKMODE_LW_EXCLUSIVE) }
}

pub fn acquire_lwlock_shared(lock: *mut LWLock) -> bool {
    unsafe { LWLockAcquire(lock, LWLOCKMODE_LW_SHARED) }
}

pub fn acquire_lwlock_wait_until_free(lock: *mut LWLock) -> bool {
    unsafe { LWLockAcquire(lock, LWLOCKMODE_LW_WAIT_UNTIL_FREE) }
}

pub fn release_lwlock(lock: *mut LWLock) {
    unsafe { LWLockRelease(lock) }
}
