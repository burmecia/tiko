use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS, get_my_proc_number},
    logging::*,
    smgr::mdinit,
    wait_events::new_wait_event,
};
use s3worker::io_queue::S3IoControl;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_init() {
    unsafe {
        mdinit();

        // Initialize wait event identifiers for S3 I/O operations
        crate::WAIT_EVENT_S3_IO_READ = new_wait_event(c"S3IORead".as_ptr());
        crate::WAIT_EVENT_S3_IO_WRITE = new_wait_event(c"S3IOWrite".as_ptr());

        // Explicitly attach to shared memory in this backend process.
        // ShmemInitStruct looks up the existing block (found=true), no reinitialization.
        let total_procs = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let control = S3IoControl::init_or_attach(total_procs);

        // Attach this backend to its slot pool
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);
        pool.attach();

        pg_log_debug1(&format!(
            "s3smgr.s3_init: backend {} pool attached",
            proc_num
        ));
    }
}
