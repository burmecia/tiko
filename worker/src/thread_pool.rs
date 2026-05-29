#![allow(dead_code)]
//! Tokio runtime and thread pool management
//!
//! This module initializes and manages the Tokio async runtime used by Tiko worker.
//! Configuration:
//! - 4 worker threads for async I/O operations
//! - 8 blocking threads for CPU-bound work (if needed)

use pgsys::logging::*;
use std::sync::{Once, OnceLock};

use crate::tasks::compactor::compactor_task;
use crate::tasks::wal_receiver::{WalReceiverConfig, wal_receiver_task};
use core::{
    //project::{ProjectCtx, ProjectNamespace},
    io::store::Store,
};

/// Spawn the PITR background task on the Tokio runtime.
///
/// Does nothing if:
/// - The runtime has not been initialised.
/// - `Store` has not been initialised.
///
/// Call this from `worker_main` after both `init_tokio_runtime` and
/// `init_project_ctx` have completed.
pub(crate) fn spawn_compactor_task() {
    let Some(runtime) = TOKIO_RUNTIME.get() else {
        pg_log_warning("tiko: spawn_compactor_task called before runtime init; skipping");
        return;
    };

    let Ok(store) = Store::try_get() else {
        pg_log_warning("tiko: Store not initialised; skipping compactor task");
        return;
    };

    runtime.spawn(compactor_task(store));
}

/// Spawn the WAL receiver task on the Tokio runtime.
///
/// Does nothing if the runtime, or `Store` are not yet initialised.
pub(crate) fn spawn_wal_receiver_task() {
    let Some(runtime) = TOKIO_RUNTIME.get() else {
        pg_log_warning("tiko: spawn_wal_receiver_task called before runtime init; skipping");
        return;
    };

    let Ok(store) = Store::try_get() else {
        pg_log_warning("tiko: Store not initialised; skipping WAL receiver task");
        return;
    };

    runtime.spawn(wal_receiver_task(store, WalReceiverConfig::default()));
    pg_log_info("tiko: WAL receiver task spawned");
}

/// The global Tokio runtime handle stored safely using OnceLock
static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static RUNTIME_INIT: Once = Once::new();

/// Create and initialize the Tokio runtime
///
/// Sets up:
/// - Worker thread pool (4 threads for async work)
/// - Blocking thread pool (8 threads for blocking operations)
/// - Proper naming and lifecycle management
pub(crate) fn init_tokio_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let mut init_error: Option<Box<dyn std::error::Error>> = None;

    RUNTIME_INIT.call_once(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(8)
            .thread_name("worker-tokio")
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                let _ = TOKIO_RUNTIME.set(runtime);
                pg_log_info("tiko: Tokio runtime initialized (4 workers, 8 blocking)");
            }
            Err(e) => {
                init_error = Some(Box::new(e));
            }
        }
    });

    if let Some(err) = init_error {
        Err(err)
    } else {
        Ok(())
    }
}

/// Get a reference to the global Tokio runtime
///
/// # Panics
/// If called before `init_tokio_runtime()`
pub(crate) fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME.get().expect("Tokio runtime not initialized")
}

/// Spawn a task on the Tokio runtime
///
/// # Arguments
/// * `task` - Async function to execute
pub(crate) fn spawn_task<F>(task: F)
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_runtime().spawn(task);
}

/// Gracefully shutdown the Tokio runtime
///
/// Note: With OnceLock, we cannot directly shutdown the runtime.
/// The runtime will shutdown when the process exits.
pub(crate) fn shutdown_tokio_runtime() {
    pg_log_info("tiko: Tokio runtime will shutdown with process termination");
}

/// Configuration for the thread pool
#[derive(Debug, Clone)]
pub(crate) struct ThreadPoolConfig {
    pub worker_threads: usize,
    pub blocking_threads: usize,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        ThreadPoolConfig {
            worker_threads: 4,
            blocking_threads: 8,
        }
    }
}

/// Create a runtime with custom configuration
pub(crate) fn init_tokio_runtime_with_config(
    config: ThreadPoolConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut init_error: Option<Box<dyn std::error::Error>> = None;

    RUNTIME_INIT.call_once(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .max_blocking_threads(config.blocking_threads)
            .thread_name("worker-tokio")
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                let _ = TOKIO_RUNTIME.set(runtime);
                pg_log_info(&format!(
                    "tiko: Tokio runtime initialized ({} workers, {} blocking)",
                    config.worker_threads, config.blocking_threads
                ));
            }
            Err(e) => {
                init_error = Some(Box::new(e));
            }
        }
    });

    if let Some(err) = init_error {
        Err(err)
    } else {
        Ok(())
    }
}
