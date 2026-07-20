//! tikoblk — host block-storage daemon.
//!
//! `tikoblkd` serves Linux ublk block devices backed by pluggable
//! [`backend::BlockBackend`] implementations. Phase 1 ships a loop-file
//! backend ([`backend::FileBackend`]); the chunked S3 Files engine slots in
//! behind the same trait later.
//!
//! - [`device`]: ublk device lifecycle on `libublk` (add/recover/delete,
//!   target params, `/dev` node-link bridge for ublk2-named kernels).
//! - [`volume`]: volume manager driving the device<->backend wiring.
//! - [`registry`]: persistent volume registry (`registry.json`, atomic).
//! - [`control`]: minimal HTTP/1.1-over-UDS control API.

pub mod backend;
pub mod cache;
pub mod chunk;
pub mod chunkstore;
pub mod control;
pub mod crc32;
pub mod device;
pub mod gc;
pub mod map;
pub mod metrics;
pub mod registry;
pub mod volume;

/// Errors returned by the tikoblk daemon.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// libublk / kernel ublk failure.
    #[error("ublk error: {0}")]
    Ublk(String),
    /// No such volume.
    #[error("volume not found: {0}")]
    NotFound(String),
    /// Volume id already registered.
    #[error("volume already exists: {0}")]
    AlreadyExists(String),
    /// Device is in use (mounted / held open).
    #[error("volume {0} is busy (device in use)")]
    Busy(String),
    /// Operation not valid for the volume's current state.
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// Malformed request input.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// Not enough free space on the data directory filesystem.
    #[error("insufficient free space: need {need} bytes, have {have}")]
    InsufficientSpace {
        /// Bytes required (including safety headroom).
        need: u64,
        /// Bytes available.
        have: u64,
    },
    /// Timed out waiting for an async step (device start/stop).
    #[error("timeout: {0}")]
    Timeout(String),
}

impl From<libublk::UblkError> for Error {
    fn from(e: libublk::UblkError) -> Self {
        Error::Ublk(e.to_string())
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;
