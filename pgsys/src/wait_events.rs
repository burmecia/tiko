use std::ffi::c_char;

unsafe extern "C" {

    /// Register a new wait event for an extension
    ///
    /// Returns a wait_event_info value in the PG_WAIT_EXTENSION class.
    fn WaitEventExtensionNew(wait_event_name: *const c_char) -> u32;
}

/// # Safety
///
/// `wait_event_name` must be a valid NUL-terminated C string that outlives
/// the call, and the caller must be running in a PostgreSQL backend process.
pub unsafe fn new_wait_event(wait_event_name: *const c_char) -> u32 {
    unsafe { WaitEventExtensionNew(wait_event_name) }
}
