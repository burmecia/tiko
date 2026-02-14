//! PostgreSQL signal and latch-based event notification
//!
//! This module provides FFI bindings for PostgreSQL's signal handling
//! and latch-based event notification used by background workers.

use std::ffi::c_int;

// WaitLatch flags - must match PostgreSQL's values in waiteventset.h
pub const WL_LATCH_SET: c_int = 1 << 0; // 0x01 - Latch was set
pub const WL_SOCKET_READABLE: c_int = 1 << 1; // 0x02 - Socket readable
pub const WL_SOCKET_WRITEABLE: c_int = 1 << 2; // 0x04 - Socket writeable
pub const WL_TIMEOUT: c_int = 1 << 3; // 0x08 - Timeout elapsed
pub const WL_POSTMASTER_DEATH: c_int = 1 << 4; // 0x10 - Postmaster died
pub const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5; // 0x20 - Exit if postmaster dies

/// Opaque latch structure (never accessed directly)
#[repr(C)]
pub struct Latch {
    _private: [u8; 0],
}

/// PostgreSQL signal handler function type
pub type PqSignalT = Option<extern "C" fn(c_int)>;

unsafe extern "C" {
    /// Set up a signal handler
    ///
    /// Backend signal handler setup using sigaction() for reliability.
    /// Wraps the handler to verify process PID and preserve errno.
    ///
    /// # Arguments
    /// * `signum` - Signal number (SIGTERM, SIGHUP, SIGINT, etc.)
    /// * `handler` - Signal handler function, SIG_IGN, or SIG_DFL
    ///
    /// # Returns
    /// Previous signal handler
    ///
    /// # Source
    /// Defined in: src/port/pqsignal.c
    /// Declared in: src/include/port.h
    #[link_name = "pqsignal_be"]
    pub fn pqsignal(signum: c_int, handler: PqSignalT) -> PqSignalT;

    /// Get the current process's latch
    pub static MyLatch: *mut Latch;

    /// Set a latch to wake a process
    ///
    /// This is signal-safe and can be called from any context.
    /// Sets a flag that WaitLatch will detect.
    pub fn SetLatch(latch: *mut Latch);

    /// Reset a latch after WaitLatch returns
    ///
    /// Must be called in the main process context (not signal handler)
    pub fn ResetLatch(latch: *mut Latch);

    /// Wait for a latch or timeout
    ///
    /// # Arguments
    /// * `latch` - Latch to wait on
    /// * `wakeup_events` - Combination of WL_* flags
    /// * `timeout` - Timeout in milliseconds (-1 = no timeout)
    /// * `wait_event_info` - Event ID for pg_stat_activity
    ///
    /// # Returns
    /// Combination of WL_* flags indicating what caused the wakeup
    pub fn WaitLatch(
        latch: *mut Latch,
        wakeup_events: c_int,
        timeout: i64,
        wait_event_info: u32,
    ) -> c_int;
}

/// Rust wrapper for latch operations
pub struct LatchGuard {
    latch: *mut Latch,
}

impl LatchGuard {
    /// Get the current process's latch
    pub fn current() -> Self {
        unsafe { LatchGuard { latch: MyLatch } }
    }

    /// Wait for events on this latch
    pub fn wait(&self, events: c_int, timeout_ms: i64, wait_event_info: u32) -> c_int {
        unsafe { WaitLatch(self.latch, events, timeout_ms, wait_event_info) }
    }

    /// Reset the latch after a wait
    pub fn reset(&self) {
        unsafe {
            ResetLatch(self.latch);
        }
    }

    /// Signal this latch (can be called from anywhere)
    pub fn signal(&self) {
        unsafe {
            SetLatch(self.latch);
        }
    }
}
