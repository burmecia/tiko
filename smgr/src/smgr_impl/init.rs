use core::io::store::Store;
use core::io_control::IoControl;
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

    unsafe {
        // Explicitly attach to shared memory in this backend process.
        // ShmemInitStruct looks up the existing block (found=true), no reinitialization.
        // Must run before `Store::init` so `hydrate_timeline_state` inside
        // `Store::init` can consult `IoControl`.
        let total_procs = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let control = IoControl::init_or_attach(total_procs);

        // Attach this backend to its slot pool
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);
        pool.attach();

        pg_log_debug2(&format!("tiko_init: backend {} pool attached", proc_num));
    }

    // Initialize Store — needed for both initdb and normal run. Also hydrates
    // the timeline state from existing segments on its first call.
    if let Err(e) = Store::init() {
        pg_log_warning(&format!("tiko_init: Store::init failed: {e}"));
    }
}
