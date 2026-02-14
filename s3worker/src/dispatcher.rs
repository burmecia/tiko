//! Dispatcher coordination between main thread and Tokio worker threads
//!
//! This module manages the work channel from the main thread to Tokio workers.
//! Completions go directly from Tokio → backend via SetLatch (no completion channel).
//!
//! The Dispatcher holds the sender half of a `tokio::sync::mpsc` channel.
//! The receiver half is returned from `Dispatcher::new()` and passed to `io_worker_loop`.

use pgsys::logging::*;
use tokio::sync::mpsc;

/// Work request sent from main thread to Tokio workers.
///
/// Identifies a slot by its backend pool and slot index.
/// The Tokio worker uses these to look up the actual S3IoSlot from S3IoControl.
///
/// `backend_id` is a ProcNumber (u32) — can be up to MAX_BACKENDS (262143).
/// `slot_index` is 0..SLOTS_PER_BACKEND-1 (currently 0..3).
/// `generation` is a snapshot of the slot's generation at dispatch time — used to
/// detect stale completions when a backend dies and its ProcNumber is recycled.
#[derive(Debug, Clone)]
pub struct IoWorkRequest {
    pub backend_id: u32,
    pub slot_index: u8,
    pub generation: u32,
}

/// Dispatcher state — holds the sender half of a bounded tokio mpsc channel.
///
/// The receiver half is returned from `new()` and passed to `io_worker_loop`.
pub struct Dispatcher {
    work_sender: mpsc::Sender<IoWorkRequest>,
}

impl Dispatcher {
    /// Create a new dispatcher with a bounded work channel.
    ///
    /// Returns the Dispatcher (sender) and the Receiver for `io_worker_loop`.
    ///
    /// # Arguments
    /// * `work_queue_size` - Backpressure limit for pending work (typically 256-512)
    pub fn new(work_queue_size: usize) -> (Self, mpsc::Receiver<IoWorkRequest>) {
        let (work_sender, work_receiver) = mpsc::channel(work_queue_size);

        pg_log_info(&format!(
            "s3worker: dispatcher created with work_queue_size={}",
            work_queue_size,
        ));

        (Dispatcher { work_sender }, work_receiver)
    }

    /// Send a work request to the Tokio thread pool.
    ///
    /// Returns `Err` with the request if the bounded queue is full (backpressure)
    /// or if the receiver has been dropped (shutdown).
    pub fn send_work(
        &self,
        request: IoWorkRequest,
    ) -> Result<(), mpsc::error::TrySendError<IoWorkRequest>> {
        self.work_sender.try_send(request)
    }
}
