// pgsys/src/bgworker.rs
//! PostgreSQL Background Worker FFI bindings

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::common::*;
use std::ffi::{c_char, c_int};

pub const BGW_MAXLEN: usize = 96;
pub const BGW_EXTRALEN: usize = 128;

// Background Worker flags
pub const BGWORKER_SHMEM_ACCESS: c_int = 1 << 0;
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: c_int = 1 << 1;

// Background Worker start time constants
pub const BgWorkerStart_PostmasterStart: c_int = 0;
pub const BgWorkerStart_ConsistentState: c_int = 1;
pub const BgWorkerStart_RecoveryFinished: c_int = 2;

// Default restart time (in seconds)
pub const BGW_NEVER_RESTART: c_int = -1;
pub const BGW_DEFAULT_RESTART_INTERVAL: c_int = 60;

#[repr(C)]
pub struct PgAbiValues {
    pub version: c_int,
    pub funcmaxargs: c_int,
    pub indexmaxkeys: c_int,
    pub namedatalen: c_int,
    pub float8byval: c_int,
    pub abi_extra: [c_char; 32],
}

#[repr(C)]
pub struct PgMagicStruct {
    pub len: c_int,
    pub abi_fields: PgAbiValues,
    pub name: *const c_char,
    pub version: *const c_char,
}

// SAFETY: PgMagicStruct contains only null pointers in static context,
// which is safe to share across threads
unsafe impl Sync for PgMagicStruct {}

// Background Worker Structure
#[repr(C)]
pub struct BackgroundWorker {
    pub bgw_name: [c_char; BGW_MAXLEN],
    pub bgw_type: [c_char; BGW_MAXLEN],
    pub bgw_flags: c_int,
    pub bgw_start_time: c_int,
    pub bgw_restart_time: c_int, // Restart time in seconds, or BGW_NEVER_RESTART
    pub bgw_library_name: [c_char; MAXPGPATH],
    pub bgw_function_name: [c_char; BGW_MAXLEN],
    pub bgw_main_arg: Datum,
    pub bgw_extra: [c_char; BGW_EXTRALEN],
    pub bgw_notify_pid: Pid,
}

unsafe extern "C" {
    // Background worker API
    pub fn RegisterBackgroundWorker(worker: *mut BackgroundWorker);

    // Signal handling for background workers
    pub fn BackgroundWorkerBlockSignals();
    pub fn BackgroundWorkerUnblockSignals();
}
