//! Dispatcher coordination between main thread and Tokio worker threads.
//!
//! The Dispatcher holds the sender half of a bounded tokio mpsc channel.
//! The receiver half is returned from `Dispatcher::new()` and passed to
//! `io_worker_loop`.

use tokio::sync::mpsc;

use core::{error::Result, io_control::IoWorkRequest};
use pgsys::logging::*;

/// Dispatcher state — holds the sender half of a bounded tokio mpsc channel.
pub struct Dispatcher {
    work_sender: mpsc::Sender<IoWorkRequest>,
}

impl Dispatcher {
    /// Create a new dispatcher with a bounded work channel.
    ///
    /// Returns the Dispatcher (sender) and the Receiver for `io_worker_loop`.
    pub fn new(work_queue_size: usize) -> (Self, mpsc::Receiver<IoWorkRequest>) {
        let (work_sender, work_receiver) = mpsc::channel(work_queue_size);

        pg_log_debug2(&format!(
            "tiko: dispatcher created with work_queue_size={}",
            work_queue_size,
        ));

        (Dispatcher { work_sender }, work_receiver)
    }

    /// Send a work request to the Tokio thread pool.
    ///
    /// Returns `Err` with the request if the bounded queue is full or closed.
    pub fn send_work(&self, request: IoWorkRequest) -> Result<()> {
        self.work_sender.try_send(request)?;
        Ok(())
    }
}
