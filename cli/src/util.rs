//! Small shared formatting helpers for the operator CLI binaries.

use chrono::{DateTime, Utc};

/// Render a Unix-seconds timestamp as RFC3339 UTC, falling back to the raw
/// integer if the instant is out of `chrono`'s representable range.
pub fn fmt_unix_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}
