use crate::lwlock::LWLock;
use std::ffi::c_void;

unsafe extern "C" {
    pub static process_shared_preload_libraries_in_progress: bool;

    pub static mut shmem_request_hook: Option<unsafe extern "C" fn()>;
    pub static mut shmem_startup_hook: Option<unsafe extern "C" fn()>;

    pub fn RequestAddinShmemSpace(size: usize);

    pub fn LWLockAcquire(lock: *mut LWLock, mode: u32) -> bool;
    pub fn LWLockRelease(lock: *mut LWLock);

    pub fn ShmemInitStruct(name: *const i8, size: usize, foundPtr: *mut bool) -> *mut c_void;

    pub fn rust_get_addin_shmem_init_lock() -> *mut LWLock;
}
