pub mod dispatcher;
pub mod extension;
pub mod log_relay;
pub mod tasks;

// Re-export engine modules (moved from worker to engine crate)
pub use core::{cache, io_control, relfork::ops};

// Re-export the shared store modules
pub use core::{manifest, s3, s3_sim, storage};

mod io_handler;
mod main_loop;
mod shmem;
mod thread_pool;
