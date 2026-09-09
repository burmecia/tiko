// pgsys/src/logging.rs
//! PostgreSQL logging module
//!
//! Thread-aware: PostgreSQL's `elog` may only run on the process's PG thread
//! — it touches process-local state (`ErrorContext`, `CurrentMemoryContext`,
//! `palloc`). [`mark_pg_thread`] designates that thread; [`pg_log`] calls
//! `elog` directly on it and relays `(level, message)` through a bounded
//! queue from any other thread (Tokio runtime threads). The PG thread empties
//! the queue via [`drain_relay`] (the tikoworker main loop calls it every
//! iteration). With no thread marked, `pg_log` is always direct — correct for
//! single-threaded backends/checkpointer and for CLI binaries (where
//! `rust_pg_log` is a no-op stub). Levels pass through verbatim, so a relayed
//! PANIC aborts the process when the PG thread drains it.

use std::collections::VecDeque;
use std::ffi::{CString, c_int};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread::ThreadId;

// PostgreSQL error level constants (from elog.h)
pub const DEBUG5: c_int = 10; // Debugging message (most detailed)
pub const DEBUG4: c_int = 11; // Debugging message
pub const DEBUG3: c_int = 12; // Debugging message
pub const DEBUG2: c_int = 13; // Debugging message
pub const DEBUG1: c_int = 14; // Debugging message (least detailed)
pub const LOG: c_int = 15; // Informational message
pub const INFO: c_int = 17; // Informational message
pub const NOTICE: c_int = 18; // Notice message
pub const WARNING: c_int = 19; // Warning message
pub const ERROR: c_int = 21; // Error message
pub const FATAL: c_int = 22; // Fatal error (terminates the session/process)
pub const PANIC: c_int = 23; // Panic (aborts the process; postmaster crash-restarts the cluster)

// PostgreSQL logging function wrapper
// We define a C wrapper function that will be implemented in PostgreSQL C code
unsafe extern "C" {
    fn rust_pg_log(elevel: c_int, message: *const std::os::raw::c_char);
}

/// The elog-safe thread of this process, designated via [`mark_pg_thread`].
static PG_THREAD: OnceLock<ThreadId> = OnceLock::new();

/// Messages logged off the PG thread, queued for it to emit via [`drain_relay`].
static RELAY: OnceLock<Mutex<VecDeque<(i32, String)>>> = OnceLock::new();

/// Max queued messages before falling back to stderr (mirrors the tikoworker
/// `log_relay` fallback policy — a log call must never block or panic).
const RELAY_CAP: usize = 1024;

/// Designate the calling thread as this process's elog-safe PG thread. A
/// multi-threaded process (tikoworker) must call this on its main PG thread
/// *before* spawning any runtime threads, so their `pg_log` calls relay.
pub fn mark_pg_thread() {
    let _ = PG_THREAD.set(std::thread::current().id());
}

/// True when the calling thread is this process's elog-safe PG thread. An
/// unmarked process (single-threaded backends, CLI binaries) treats every
/// thread as safe. Public for crash-escalation paths that must know whether
/// `elog(PANIC)` applies here or has to be relayed to the PG thread.
pub fn on_pg_thread() -> bool {
    match PG_THREAD.get() {
        Some(id) => *id == std::thread::current().id(),
        None => true,
    }
}

fn relay_queue() -> MutexGuard<'static, VecDeque<(i32, String)>> {
    RELAY
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Log a message to PostgreSQL's logging system. Direct `elog` on the marked
/// PG thread; queued for later emission from any other thread.
///
/// # Arguments
/// * `elevel` - Error level (e.g., LOG, INFO, NOTICE, WARNING, ERROR)
/// * `message` - Message to log
pub fn pg_log(elevel: i32, message: impl AsRef<str>) {
    if on_pg_thread() {
        unsafe {
            let msg = CString::new(message.as_ref()).unwrap_or_else(|_| CString::new("").unwrap());
            rust_pg_log(elevel, msg.as_ptr());
        }
        return;
    }
    let mut q = relay_queue();
    if q.len() < RELAY_CAP {
        q.push_back((elevel, message.as_ref().to_owned()));
    } else {
        eprintln!(
            "tiko: log relay queue full, to stderr: {}",
            message.as_ref()
        );
    }
}

/// Emit every queued message via `elog`. No-op when called off the PG thread.
pub fn drain_relay() {
    if !on_pg_thread() {
        return;
    }
    let pending: Vec<(i32, String)> = relay_queue().drain(..).collect();
    for (elevel, msg) in pending {
        pg_log(elevel, msg);
    }
}

#[inline(always)]
pub fn pg_log_debug5(message: impl AsRef<str>) {
    pg_log(DEBUG5, message);
}

#[inline(always)]
pub fn pg_log_debug4(message: impl AsRef<str>) {
    pg_log(DEBUG4, message);
}

#[inline(always)]
pub fn pg_log_debug3(message: impl AsRef<str>) {
    pg_log(DEBUG3, message);
}

#[inline(always)]
pub fn pg_log_debug2(message: impl AsRef<str>) {
    pg_log(DEBUG2, message);
}

#[inline(always)]
pub fn pg_log_debug1(message: impl AsRef<str>) {
    pg_log(DEBUG1, message);
}

#[inline(always)]
pub fn pg_log_info(message: impl AsRef<str>) {
    pg_log(INFO, message);
}

#[inline(always)]
pub fn pg_log_warning(message: impl AsRef<str>) {
    pg_log(WARNING, message);
}

/// Log at ERROR severity. Like C `elog(ERROR, ...)`, this **longjmps to the
/// innermost PG_CATCH and never returns** — any code after a call is
/// unreachable. Do not add "fallback"/"return on error" paths after calling
/// this; use `pg_log_warning` or a `Result` if execution must continue.
#[inline(always)]
pub fn pg_log_error(message: impl AsRef<str>) {
    pg_log(ERROR, message);
}

#[inline(always)]
pub fn pg_log_notice(message: impl AsRef<str>) {
    pg_log(NOTICE, message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::c_char;

    static LOGGED: OnceLock<Mutex<Vec<(i32, String)>>> = OnceLock::new();

    // Test-binary stand-in for the postmaster-provided symbol (the CLI
    // binaries do the same via `pg_stubs.rs`).
    #[unsafe(no_mangle)]
    extern "C" fn rust_pg_log(elevel: c_int, message: *const c_char) {
        let msg = unsafe { std::ffi::CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned();
        LOGGED
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push((elevel, msg));
    }

    fn logged() -> Vec<(i32, String)> {
        LOGGED
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .clone()
    }

    // Single test function: PG_THREAD and the relay queue are process-global,
    // so parallel tests would interfere with each other.
    #[test]
    fn pg_log_direct_on_pg_thread_relayed_elsewhere() {
        // Unmarked process: always direct (backend/CLI behavior).
        pg_log(INFO, "direct-before-mark");
        assert!(logged().contains(&(INFO, "direct-before-mark".to_string())));

        mark_pg_thread();
        pg_log(WARNING, "direct-on-marked");
        assert!(logged().contains(&(WARNING, "direct-on-marked".to_string())));

        std::thread::scope(|s| {
            s.spawn(|| {
                // Not the marked thread: queued, elog not called; drain off
                // the PG thread is a no-op.
                pg_log(WARNING, "from-tokio-thread");
                drain_relay();
                assert_eq!(relay_queue().len(), 1);
                assert!(!logged().iter().any(|(_, m)| m == "from-tokio-thread"));
            });
        });

        // The PG thread emits the relayed message on drain.
        drain_relay();
        assert!(logged().contains(&(WARNING, "from-tokio-thread".to_string())));
        assert!(relay_queue().is_empty());

        // Overflow falls back to stderr instead of growing unboundedly.
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0..RELAY_CAP + 3 {
                    pg_log(DEBUG1, format!("flood-{i}"));
                }
            });
        });
        assert_eq!(relay_queue().len(), RELAY_CAP);
        let before = logged().len();
        drain_relay();
        assert!(relay_queue().is_empty());
        assert_eq!(logged().len(), before + RELAY_CAP);
    }
}
