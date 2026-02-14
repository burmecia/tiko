//! FFI bindings for PostgreSQL Condition Variables
//!
//! Condition variables are used for process synchronization in PostgreSQL,
//! allowing processes to sleep until a condition becomes true and be woken
//! by other processes when the condition changes.

pub const CONDITION_VARIABLE_SIZE: usize = 16;

/// ConditionVariable
#[repr(C)]
pub struct ConditionVariable {
    _private: [u8; CONDITION_VARIABLE_SIZE],
}

unsafe extern "C" {
    /// Initialize a condition variable
    /// Must be called before first use
    ///
    /// # Safety
    /// cv must point to valid memory
    pub fn ConditionVariableInit(cv: *mut ConditionVariable);

    /// Sleep on a condition variable until signaled
    /// Releases any LWLocks held and re-acquires them after waking
    /// Can be interrupted by query cancellation or other signals
    ///
    /// # Safety
    /// cv must be a properly initialized ConditionVariable
    /// wait_event_info should be a valid wait event ID
    pub fn ConditionVariableSleep(cv: *mut ConditionVariable, wait_event_info: u32);

    /// Sleep on a condition variable with a timeout
    /// Returns true if signaled, false if timed out
    ///
    /// # Safety
    /// cv must be a properly initialized ConditionVariable
    /// timeout is in milliseconds
    pub fn ConditionVariableTimedSleep(
        cv: *mut ConditionVariable,
        timeout: i64,
        wait_event_info: u32,
    ) -> bool;

    /// Wake up one process sleeping on this condition variable
    /// Used when only one waiter needs to be woken (e.g., work queue)
    ///
    /// # Safety
    /// cv must be a properly initialized ConditionVariable
    pub fn ConditionVariableSignal(cv: *mut ConditionVariable);

    /// Wake up all processes sleeping on this condition variable
    /// Used when all waiters need to be woken (e.g., I/O completion)
    ///
    /// # Safety
    /// cv must be a properly initialized ConditionVariable
    pub fn ConditionVariableBroadcast(cv: *mut ConditionVariable);

    pub fn ConditionVariableCancelSleep();

    /// Prepare to sleep on a condition variable
    /// Must be called before checking the condition you're waiting for
    /// Prevents race conditions where the signal arrives before you sleep
    ///
    /// # Safety
    /// cv must be a properly initialized ConditionVariable
    pub fn ConditionVariablePrepareToSleep(cv: *mut ConditionVariable);
}

impl ConditionVariable {
    /// Create a new zeroed condition variable
    /// You must call init() before using it
    pub const fn new() -> Self {
        Self {
            _private: [0; CONDITION_VARIABLE_SIZE],
        }
    }

    /// Initialize this condition variable
    /// Must be called before first use
    pub fn init(&mut self) {
        unsafe {
            ConditionVariableInit(self);
        }
    }

    /// Sleep until signaled
    ///
    /// Takes `&self` because ConditionVariable uses interior mutability
    /// (internal spinlock + proclist), matching the semantics of atomics.
    pub fn sleep(&self, wait_event: u32) {
        unsafe {
            ConditionVariableSleep(self as *const Self as *mut Self, wait_event);
        }
    }

    /// Sleep with timeout, returns true if signaled, false if timed out
    pub fn timed_sleep(&self, timeout_ms: i64, wait_event: u32) -> bool {
        unsafe {
            ConditionVariableTimedSleep(self as *const Self as *mut Self, timeout_ms, wait_event)
        }
    }

    /// Wake one waiter
    pub fn signal(&self) {
        unsafe {
            ConditionVariableSignal(self as *const Self as *mut Self);
        }
    }

    /// Wake all waiters
    pub fn broadcast(&self) {
        unsafe {
            ConditionVariableBroadcast(self as *const Self as *mut Self);
        }
    }

    /// Prepare to sleep (call before checking condition)
    pub fn prepare_to_sleep(&self) {
        unsafe {
            ConditionVariablePrepareToSleep(self as *const Self as *mut Self);
        }
    }
}

// ConditionVariable can be safely shared across processes via shared memory
unsafe impl Send for ConditionVariable {}
unsafe impl Sync for ConditionVariable {}
