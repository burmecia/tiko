//! Thread-safe log relay for Tokio threads.
//!
//! Tokio threads cannot call `pg_log_*` directly — those functions call into
//! PostgreSQL's `elog()` which requires PG process-local state (ErrorContext,
//! CurrentMemoryContext, etc.) that only exists on the main PG thread.
//!
//! Instead, Tokio tasks call [`relay_log`] which sends `(elevel, message)`
//! pairs through a bounded channel.  The tiko worker main loop drains the channel
//! with [`drain`] on every iteration and forwards each message to `pg_log`.
//!
//! If the channel is full or not yet initialised the message falls back to
//! `eprintln!` so nothing is silently dropped.

use std::sync::OnceLock;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};

use pgsys::logging::{self, pg_log};

static LOG_TX: OnceLock<SyncSender<(i32, String)>> = OnceLock::new();

/// Initialise the relay channel.  Returns the [`Receiver`] for the main thread
/// to pass to [`drain`] each loop iteration.
///
/// Must be called once from the main PG thread before any Tokio tasks are
/// spawned.
pub fn init() -> Receiver<(i32, String)> {
    let (tx, rx) = mpsc::sync_channel(256);
    let _ = LOG_TX.set(tx);
    rx
}

/// Send a log message from any thread.
///
/// Falls back to `eprintln!` if the channel is not yet initialised or is full.
pub fn relay_log(elevel: i32, msg: impl Into<String>) {
    let msg = msg.into();
    if let Some(tx) = LOG_TX.get() {
        match tx.try_send((elevel, msg.clone())) {
            Ok(()) => return,
            Err(TrySendError::Full(_)) => {
                eprintln!("tiko: log_relay: channel full, dropping to stderr: {msg}");
            }
            Err(TrySendError::Disconnected(_)) => {
                eprintln!("tiko: log_relay: channel disconnected, dropping to stderr: {msg}");
            }
        }
    } else {
        // Channel not yet initialised (startup race).
        eprintln!("tiko: log_relay: channel not initialised, dropping to stderr: {msg}");
    }
}

/// Drain all pending log messages and forward them to PostgreSQL's logger.
///
/// Call this from the main PG thread on every main-loop iteration.
pub fn drain(rx: &Receiver<(i32, String)>) {
    while let Ok((elevel, msg)) = rx.try_recv() {
        pg_log(elevel, &msg);
    }
}

/// Convenience wrappers mirroring the `pg_log_*` naming convention.
pub fn relay_debug1(msg: impl Into<String>) {
    relay_log(logging::DEBUG1, msg);
}

pub fn relay_debug2(msg: impl Into<String>) {
    relay_log(logging::DEBUG2, msg);
}

pub fn relay_debug3(msg: impl Into<String>) {
    relay_log(logging::DEBUG3, msg);
}

pub fn relay_debug4(msg: impl Into<String>) {
    relay_log(logging::DEBUG4, msg);
}

pub fn relay_debug5(msg: impl Into<String>) {
    relay_log(logging::DEBUG5, msg);
}

pub fn relay_info(msg: impl Into<String>) {
    relay_log(logging::INFO, msg);
}

pub fn relay_warning(msg: impl Into<String>) {
    relay_log(logging::WARNING, msg);
}

pub fn relay_error(msg: impl Into<String>) {
    relay_log(logging::ERROR, msg);
}
