use std::io::{Error as IoError, ErrorKind as IoErrorKind};

/// Crate-level error type for `core`.
#[derive(Debug, thiserror::Error)]

pub enum Error {
    /// Wraps a low-level I/O error from the OS or file operations.
    #[error("io: {0}")]
    Io(#[from] IoError),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("store not available")]
    StoreNotAvailable,

    #[error("eviction sweep exhausted")]
    EvictionSweepExhausted,

    /// A catch-all for errors that don't fit a specific variant.
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Error::Io(IoError::new(IoErrorKind::NotFound, msg.into()))
    }

    pub fn already_exists(msg: impl Into<String>) -> Self {
        Error::Io(IoError::new(IoErrorKind::AlreadyExists, msg.into()))
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Error::Io(IoError::new(IoErrorKind::PermissionDenied, msg.into()))
    }

    pub fn invalid_data(msg: impl Into<String>) -> Self {
        Error::Io(IoError::new(IoErrorKind::InvalidData, msg.into()))
    }

    pub fn unexpected_eof(msg: impl Into<String>) -> Self {
        Error::Io(IoError::new(IoErrorKind::UnexpectedEof, msg.into()))
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::Io(e) if e.kind() == IoErrorKind::NotFound)
    }

    pub fn is_already_exists(&self) -> bool {
        matches!(self, Error::Io(e) if e.kind() == IoErrorKind::AlreadyExists)
    }

    /// Map this error to a POSIX errno value suitable for passing through
    /// the shared-memory I/O slot (`result_status` field).
    ///
    /// `0` is reserved for success; callers should treat any non-zero value
    /// as failure and reconstruct an appropriate error on the backend side.
    pub fn to_errno(&self) -> i32 {
        match self {
            Error::Io(e) => match e.kind() {
                IoErrorKind::NotFound => libc::ENOENT,
                IoErrorKind::PermissionDenied => libc::EACCES,
                IoErrorKind::AlreadyExists => libc::EEXIST,
                IoErrorKind::UnexpectedEof => libc::EINVAL,
                IoErrorKind::InvalidData => libc::EINVAL,
                _ => e.raw_os_error().unwrap_or(libc::EIO),
            },
            Error::Json(_) => libc::EINVAL,
            Error::StoreNotAvailable => libc::ENODEV,
            Error::EvictionSweepExhausted => libc::EAGAIN,
            Error::Other(_) => libc::EIO,
        }
    }
}

/// Convenience `Result` alias using this crate's [`Error`] type.
pub type Result<T> = std::result::Result<T, Error>;
