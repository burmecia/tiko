unsafe extern "C" {
    /// Check for interrupts from postmaster (C shim wrapper)
    ///
    /// Calls CHECK_FOR_INTERRUPTS macro from C code to handle:
    /// - Query cancel (SIGINT)
    /// - Shutdown (SIGTERM from postmaster)
    /// - Configuration reload (SIGHUP)
    /// - Various timeout handlers
    /// - Client connection loss
    pub fn rust_check_for_interrupts();
}

/// Check for and process pending interrupts
///
/// This should be called periodically in the main loop to handle:
/// - Query cancellation (SIGINT)
/// - Shutdown signals (SIGTERM)
/// - Configuration reloads (SIGHUP)
/// - Timeout conditions
/// - Client connection loss
pub fn check_for_interrupts() {
    unsafe {
        rust_check_for_interrupts();
    }
}
