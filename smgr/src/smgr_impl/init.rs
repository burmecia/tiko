use core::io_control::IoControl;
use core::store::Store;
use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS, get_my_proc_number},
    logging::*,
    wait_events::new_wait_event,
};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_init() {
    unsafe {
        // Initialize wait event identifiers for Tiko I/O operations
        crate::WAIT_EVENT_TIKO_IO_READ = new_wait_event(c"TikoIORead".as_ptr());
        crate::WAIT_EVENT_TIKO_IO_WRITE = new_wait_event(c"TikoIOWrite".as_ptr());
    }

    // Initialize Store unconditionally — needed for both initdb and normal run.
    Store::init();

    unsafe {
        // Explicitly attach to shared memory in this backend process.
        // ShmemInitStruct looks up the existing block (found=true), no reinitialization.
        let total_procs = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let control = IoControl::init_or_attach(total_procs);

        // Attach this backend to its slot pool
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);
        pool.attach();

        pg_log_debug1(&format!("tiko_init: backend {} pool attached", proc_num));
    }
}
