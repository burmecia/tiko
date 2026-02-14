//! Shared memory structures and initialization for s3worker
//!
//! This module manages PostgreSQL shared memory for the I/O queue system.
//! Uses the standard PG extension pattern:
//! - `shmem_request_hook` to request memory at startup
//! - `shmem_startup_hook` to initialize structures
//! - `ShmemInitStruct` to create the shared control structure + backend pools

use crate::io_queue::S3IoControl;
use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS},
    logging::*,
    shmem::*,
};

static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;

/// Request shared memory from PostgreSQL.
/// Called via shmem_request_hook. MaxBackends is available at this point.
pub extern "C" fn s3worker_shmem_request() {
    unsafe {
        // Call previous hook if chained
        if let Some(prev_hook) = PREV_SHMEM_REQUEST_HOOK {
            prev_hook();
        }

        let max_backends = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let size = S3IoControl::shmem_size(max_backends);
        RequestAddinShmemSpace(size);

        pg_log_debug1(&format!(
            "s3worker: requested {} bytes shared memory ({} backend pools)",
            size, max_backends
        ));
    }
}

/// Startup hook - initialize shared memory after PostgreSQL startup
pub extern "C" fn s3worker_shmem_startup() {
    unsafe {
        // Call previous hook if chained
        if let Some(prev_hook) = PREV_SHMEM_STARTUP_HOOK {
            prev_hook();
        }

        let max_backends = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        S3IoControl::init_or_attach(max_backends);

        pg_log_debug1(&format!(
            "s3worker: initialized shared memory ({} backend pools)",
            max_backends
        ));
    }
}

/// Install hooks for shared memory initialization.
/// Called once from _PG_init at PostgreSQL startup.
pub fn init_shared_memory() {
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = shmem_request_hook;
        PREV_SHMEM_STARTUP_HOOK = shmem_startup_hook;
        shmem_request_hook = Some(s3worker_shmem_request as _);
        shmem_startup_hook = Some(s3worker_shmem_startup as _);
    }
}
