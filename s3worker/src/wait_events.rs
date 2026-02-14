use pgsys::wait_events::new_wait_event;

/// Wait event identifier for s3worker main loop
static mut WAIT_EVENT_S3WORKER_MAIN: u32 = 0;

// Initialize wait event identifiers for this worker
pub fn init_wait_events() {
    unsafe {
        WAIT_EVENT_S3WORKER_MAIN = new_wait_event(c"S3WorkerMain".as_ptr());
    }
}

#[inline(always)]
pub fn get_wait_event_s3worker_main() -> u32 {
    unsafe { WAIT_EVENT_S3WORKER_MAIN }
}
