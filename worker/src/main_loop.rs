//! Main event loop for Tiko worker background process
//!
//! This module implements the core polling and event loop for the Tiko worker,
//! including signal handling, latch-based waiting, and request processing.
//!
//! The main loop pops entries from the MPSC submit queue and dispatches them
//! to Tokio workers. Completions go directly from Tokio → backend via SetLatch
//! (no harvest step needed on the main thread).

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::dispatcher::Dispatcher;
use crate::log_relay;
use core::io_control::IoControl;
use pgsys::{
    common::{MyProcPid, SIGHUP, SIGTERM},
    cshim::check_for_interrupts,
    latch::*,
    logging::*,
    wait_events::new_wait_event,
};

use crate::io_handler;
use crate::thread_pool;

/// Global flags for managing worker lifecycle and configuration
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static CONFIG_RELOAD_PENDING: AtomicBool = AtomicBool::new(false);

/// Handle SIGTERM (shutdown request from postmaster)
extern "C" fn handle_sigterm(_: c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Handle SIGHUP (config reload request)
extern "C" fn handle_sighup(_: c_int) {
    CONFIG_RELOAD_PENDING.store(true, Ordering::Release);
}

/// Initialize signal handlers
fn setup_signal_handlers() {
    unsafe {
        pqsignal(SIGTERM, Some(handle_sigterm));
        pqsignal(SIGHUP, Some(handle_sighup));
    }

    unsafe {
        pgsys::bgworker::BackgroundWorkerUnblockSignals();
    }

    pg_log_info("tiko: signal handlers installed");
}

/// Wait event identifier for Tiko worker main loop
static mut WAIT_EVENT_TIKO_WORKER_MAIN: u32 = 0;

/// Main event loop for worker
#[unsafe(no_mangle)]
pub extern "C-unwind" fn worker_main(_arg: *mut c_void) {
    pg_log_info("tiko: main loop starting");

    setup_signal_handlers();

    // Initialize wait event identifiers for this worker
    unsafe {
        WAIT_EVENT_TIKO_WORKER_MAIN = new_wait_event(c"TikoWorkerMain".as_ptr());
    }

    // Initialize the log relay channel before spawning any Tokio tasks so that
    // relay_log() calls from Tokio threads are forwarded here via pg_log_*.
    let log_rx = log_relay::init();

    // Initialize Tokio runtime for async I/O
    if let Err(e) = thread_pool::init_tokio_runtime() {
        pg_log_error(&format!(
            "tiko: failed to initialize Tokio runtime: {:?}",
            e
        ));
        return;
    }

    // Initialize dispatcher — work channel from main thread to Tokio
    let (dispatcher, rx) = Dispatcher::new(512);

    // Spawn io_worker_loop on Tokio — receives requests and spawns per-request tasks
    thread_pool::spawn_task(io_handler::io_worker_loop(rx));

    // Spawn the compactor background task now that the runtime and ProjectCtx are initialised.
    thread_pool::spawn_compactor_task();

    // Spawn WAL streaming task.
    //thread_pool::spawn_wal_receiver_task();

    // Get shared memory IO control structure
    let io_control = IoControl::get();

    // Store our PID and latch so backends can check liveness and wake us
    io_control
        .worker_pid
        .store(unsafe { MyProcPid } as u32, Ordering::Relaxed);
    io_control
        .worker_latch
        .store(unsafe { MyLatch } as u64, Ordering::Release);

    // Statistics
    let mut loop_count = 0u64;
    let mut requests_processed = 0u64;

    pg_log_info("tiko: initialized and entering main loop");

    // Main event loop
    while !SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
        loop_count += 1;

        // Check for interrupts (SIGTERM, postmaster death, etc.)
        check_for_interrupts();

        // Forward any log messages queued by Tokio threads.
        log_relay::drain(&log_rx);

        // Pop from submit queue and dispatch to Tokio
        match io_control.poll_submit_queue(|request| dispatcher.send_work(request)) {
            Ok(dispatched) => requests_processed += dispatched,
            Err(_) => break, // fatal: dispatcher disconnected
        }

        // Periodic logging
        if loop_count % 4 == 0 {
            pg_log_debug3(&format!(
                "tiko: loop_count={}, requests={}",
                loop_count, requests_processed
            ));
        }

        // Log cache stats periodically (every 10000 loops)
        if loop_count % 10000 == 0 {
            io_control.stats.log_summary();
        }

        // Wait for new work or timeout
        wait_for_work();
    }

    io_control.stats.log_summary();
    pg_log_info(&format!(
        "tiko: shutting down (loops={}, requests={})",
        loop_count, requests_processed
    ));

    // Clear latch and PID so backends detect shutdown
    io_control.worker_latch.store(0, Ordering::Release);
    io_control.worker_pid.store(0, Ordering::Release);

    thread_pool::shutdown_tokio_runtime();
}

/// Wait for new work using PostgreSQL's latch mechanism
fn wait_for_work() {
    const WAIT_FLAGS: c_int = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH;
    const TIMEOUT_MS: i64 = 1000;

    let latch = LatchGuard::current();
    let rc = latch.wait(WAIT_FLAGS, TIMEOUT_MS, unsafe {
        WAIT_EVENT_TIKO_WORKER_MAIN
    });

    if (rc & WL_LATCH_SET) != 0 {
        latch.reset();
    }
}
