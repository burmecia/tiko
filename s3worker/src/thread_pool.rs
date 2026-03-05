#![allow(dead_code)]
//! Tokio runtime and thread pool management
//!
//! This module initializes and manages the Tokio async runtime used by s3worker.
//! Configuration:
//! - 4 worker threads for async I/O operations
//! - 8 blocking threads for CPU-bound work (if needed)

use pgsys::logging::*;
use std::sync::{Arc, Once, OnceLock};

use crate::pitr_task::{PitrConfig, pitr_background_task};
use crate::project::{ProjectCtx, ProjectNamespace};
use crate::sim_store::SimStore;

/// Arc<SimStore> shared with the PITR background task.
/// Also accessible from Module 5's eviction path via `pitr_sim_store()`.
static PITR_SIM_STORE: OnceLock<Arc<SimStore>> = OnceLock::new();

/// Return the shared `Arc<SimStore>` used by the PITR task, if initialised.
pub fn pitr_sim_store() -> Option<&'static Arc<SimStore>> {
    PITR_SIM_STORE.get()
}

/// Spawn the PITR background task on the Tokio runtime.
///
/// Does nothing if:
/// - The runtime has not been initialised.
/// - `ProjectCtx` is not yet loaded (env vars absent).
///
/// Call this from `s3worker_main` after both `init_tokio_runtime` and
/// `init_project_ctx` have completed.
pub fn spawn_pitr_task(data_dir: &std::path::Path) {
    let Some(runtime) = TOKIO_RUNTIME.get() else {
        pg_log_warning("s3worker: spawn_pitr_task called before runtime init; skipping");
        return;
    };

    let Some(ctx) = ProjectCtx::try_get() else {
        pg_log_info("s3worker: ProjectCtx not initialised; skipping PITR background task");
        return;
    };

    let ns: ProjectNamespace = ctx.ns().clone();
    let sim = Arc::new(SimStore::new(data_dir));
    let cfg = PitrConfig::from_env();

    let _ = PITR_SIM_STORE.set(Arc::clone(&sim));

    runtime.spawn(pitr_background_task(sim, ns, cfg));
    pg_log_info("s3worker: PITR background task spawned");
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
pub fn init_tokio_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let mut init_error: Option<Box<dyn std::error::Error>> = None;

    RUNTIME_INIT.call_once(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(8)
            .thread_name("s3worker-tokio")
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                let _ = TOKIO_RUNTIME.set(runtime);
                pg_log_info("s3worker: Tokio runtime initialized (4 workers, 8 blocking)");
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
pub fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME.get().expect("Tokio runtime not initialized")
}

/// Spawn a task on the Tokio runtime
///
/// # Arguments
/// * `task` - Async function to execute
pub fn spawn_task<F>(task: F)
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
pub fn shutdown_tokio_runtime() {
    pg_log_info("s3worker: Tokio runtime will shutdown with process termination");
}

/// Configuration for the thread pool
#[derive(Debug, Clone)]
pub struct ThreadPoolConfig {
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
pub fn init_tokio_runtime_with_config(
    config: ThreadPoolConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut init_error: Option<Box<dyn std::error::Error>> = None;

    RUNTIME_INIT.call_once(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .max_blocking_threads(config.blocking_threads)
            .thread_name("s3worker-tokio")
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                let _ = TOKIO_RUNTIME.set(runtime);
                pg_log_info(&format!(
                    "s3worker: Tokio runtime initialized ({} workers, {} blocking)",
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
