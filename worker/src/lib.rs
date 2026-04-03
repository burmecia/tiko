pub mod dispatcher;
pub mod log_relay;
pub mod tasks;

// Re-export engine modules (moved from worker to engine crate)
pub use core::{cache, io_queue, s3_ops};

// Re-export the shared store modules
pub use core::{manifest, project, recovery, sim_store};

mod io_handler;
mod main_loop;
mod shmem;
mod thread_pool;

use pgsys::{
    bgworker::*,
    common::{FLOAT8PASSBYVAL, FUNC_MAX_ARGS, INDEX_MAX_KEYS, NAMEDATALEN, PG_VERSION_NUM},
    utils,
};
use std::ffi::{c_char, c_int};

// PostgreSQL extension magic function
#[unsafe(no_mangle)]
pub extern "C-unwind" fn Pg_magic_func() -> &'static PgMagicStruct {
    static MAGIC: PgMagicStruct = PgMagicStruct {
        len: std::mem::size_of::<PgMagicStruct>() as c_int,
        abi_fields: PgAbiValues {
            version: PG_VERSION_NUM as c_int / 100,
            funcmaxargs: FUNC_MAX_ARGS,
            indexmaxkeys: INDEX_MAX_KEYS,
            namedatalen: NAMEDATALEN,
            float8byval: FLOAT8PASSBYVAL,
            abi_extra: {
                let mut arr = [0 as c_char; 32];
                let bytes = b"PostgreSQL";
                let mut i = 0;
                while i < bytes.len() {
                    arr[i] = bytes[i] as c_char;
                    i += 1;
                }
                arr
            },
        },
        name: c"tiko".as_ptr(),
        version: c"1.0".as_ptr(),
    };
    &MAGIC
}

// Entry point for the PostgreSQL extension
#[unsafe(no_mangle)]
pub extern "C-unwind" fn _PG_init() {
    if unsafe { !pgsys::shmem::process_shared_preload_libraries_in_progress } {
        return;
    }

    // install hooks for shared memory initialization
    shmem::init_shared_memory();

    // Create a background worker struct
    let mut worker: BackgroundWorker = unsafe { std::mem::zeroed() };
    utils::copy_str_to_c(&mut worker.bgw_name, "Tiko background worker");
    utils::copy_str_to_c(&mut worker.bgw_type, "tiko");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    utils::copy_str_to_c(&mut worker.bgw_library_name, "libtikoworker");
    utils::copy_str_to_c(&mut worker.bgw_function_name, "worker_main");

    unsafe {
        // Register the background worker
        RegisterBackgroundWorker(&mut worker);
    }
}
