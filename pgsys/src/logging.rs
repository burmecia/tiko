// pgsys/src/logging.rs
//! PostgreSQL logging module

use std::ffi::{CString, c_int};

// PostgreSQL error level constants (from elog.h)
pub const DEBUG5: c_int = 10; // Debugging message (most detailed)
pub const DEBUG4: c_int = 11; // Debugging message
pub const DEBUG3: c_int = 12; // Debugging message
pub const DEBUG2: c_int = 13; // Debugging message
pub const DEBUG1: c_int = 14; // Debugging message (least detailed)
pub const LOG: c_int = 15; // Informational message
pub const INFO: c_int = 17; // Informational message
pub const NOTICE: c_int = 18; // Informational message  
pub const WARNING: c_int = 19; // Warning message
pub const ERROR: c_int = 21; // Error message

// PostgreSQL logging function wrapper
// We define a C wrapper function that will be implemented in PostgreSQL C code
unsafe extern "C" {
    fn rust_pg_log(elevel: c_int, message: *const std::os::raw::c_char);
}

// Safe Rust wrapper for PostgreSQL logging
/// Log a message to PostgreSQL's logging system
///
/// # Arguments
/// * `elevel` - Error level (e.g., LOG, INFO, NOTICE, WARNING, ERROR)
/// * `message` - Message to log
pub fn pg_log(elevel: i32, message: &str) {
    unsafe {
        let msg = CString::new(message).unwrap_or_else(|_| CString::new("").unwrap());
        rust_pg_log(elevel, msg.as_ptr());
    }
}

#[inline(always)]
pub fn pg_log_debug5(message: &str) {
    pg_log(DEBUG5, message);
}

#[inline(always)]
pub fn pg_log_debug4(message: &str) {
    pg_log(DEBUG4, message);
}

#[inline(always)]
pub fn pg_log_debug3(message: &str) {
    pg_log(DEBUG3, message);
}

#[inline(always)]
pub fn pg_log_debug2(message: &str) {
    pg_log(DEBUG2, message);
}

#[inline(always)]
pub fn pg_log_debug1(message: &str) {
    pg_log(DEBUG1, message);
}

#[inline(always)]
pub fn pg_log_info(message: &str) {
    pg_log(INFO, message);
}

#[inline(always)]
pub fn pg_log_warning(message: &str) {
    pg_log(WARNING, message);
}

#[inline(always)]
pub fn pg_log_error(message: &str) {
    pg_log(ERROR, message);
}

#[inline(always)]
pub fn pg_log_notice(message: &str) {
    pg_log(NOTICE, message);
}
