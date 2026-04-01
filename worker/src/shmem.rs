//! Shared memory structures and initialization for Tiko worker
//!
//! This module manages PostgreSQL shared memory for the I/O queue system.
//! Uses the standard PG extension pattern:
//! - `shmem_request_hook` to request memory at startup
//! - `shmem_startup_hook` to initialize structures
//! - `ShmemInitStruct` to create the shared control structure + backend pools

use core::io_queue::IoControl;
use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS},
    logging::*,
    shmem::*,
};

static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;

/// Request shared memory from PostgreSQL.
/// Called via shmem_request_hook. MaxBackends is available at this point.
pub extern "C" fn worker_shmem_request() {
    unsafe {
        // Call previous hook if chained
        if let Some(prev_hook) = PREV_SHMEM_REQUEST_HOOK {
            prev_hook();
        }

        let max_backends = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let size = IoControl::shmem_size(max_backends);
        RequestAddinShmemSpace(size);

        pg_log_debug1(&format!(
            "tiko: requested {} bytes shared memory ({} backend pools, cache {} chunk slots + hash + locks)",
            size,
            max_backends,
            core::cache::CACHE_NUM_SLOTS
        ));
    }
}

/// Startup hook - initialize shared memory after PostgreSQL startup
pub extern "C" fn worker_shmem_startup() {
    unsafe {
        // Call previous hook if chained
        if let Some(prev_hook) = PREV_SHMEM_STARTUP_HOOK {
            prev_hook();
        }

        let max_backends = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        IoControl::init_or_attach(max_backends);

        pg_log_debug1(&format!(
            "tiko: initialized shared memory ({} backend pools, cache {} chunk slots + hash + locks)",
            max_backends,
            core::cache::CACHE_NUM_SLOTS
        ));
    }
}

/// Install hooks for shared memory initialization.
/// Called once from _PG_init at PostgreSQL startup.
pub fn init_shared_memory() {
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = shmem_request_hook;
        PREV_SHMEM_STARTUP_HOOK = shmem_startup_hook;
        shmem_request_hook = Some(worker_shmem_request as _);
        shmem_startup_hook = Some(worker_shmem_startup as _);
    }
}
