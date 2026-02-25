use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS, get_my_proc_number, is_under_postmaster},
    logging::*,
    wait_events::new_wait_event,
};
use s3worker::io_queue::S3IoControl;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_init() {
    unsafe {
        // Initialize wait event identifiers for S3 I/O operations
        crate::WAIT_EVENT_S3_IO_READ = new_wait_event(c"S3IORead".as_ptr());
        crate::WAIT_EVENT_S3_IO_WRITE = new_wait_event(c"S3IOWrite".as_ptr());

        // Skip shared memory attachment in initdb (--boot) and single-user mode.
        // In those modes MyProcNumber is invalid and the S3IoControl shmem block
        // was never sized/initialised via shmem_request_hook.
        if !is_under_postmaster() {
            return;
        }

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
