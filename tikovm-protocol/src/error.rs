//! Errors shared across the protocol boundary.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors raised by protocol-level operations (framing, (de)serialization).
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("malformed frame: {0}")]
    MalformedFrame(String),

    #[error("frame too large: {0} bytes (limit {1})")]
    FrameTooLarge(usize, usize),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Structured error envelope carried over both the vsock RPC channel and the
/// HTTP control API: `{"error":{"kind":..., "message":..., "detail":...}}`.
/// Both sides use this shape so errors round-trip losslessly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Machine-readable error category (e.g. `"not_found"`, `"invalid_state"`).
    pub kind: String,
    /// Human-readable message.
    pub message: String,
    /// Optional extra detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl ErrorEnvelope {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                kind: kind.into(),
                message: message.into(),
                detail: None,
            },
        }
    }

    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.error.detail = Some(detail);
        self
    }
}
