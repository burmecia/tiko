//! Traffic routing rules (design §11). The host's proxy/router matches an
//! incoming connection against these to select a target VM, then wakes it
//! (restore-on-demand) and forwards.

use serde::{Deserialize, Serialize};

/// How external traffic is matched to a VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutingRule {
    /// HTTP reverse proxy: match by Host header, path prefix, or a custom
    /// header. Forwards to the workload's `expose.http_port` via the guest proxy.
    Http {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path_prefix: Option<String>,
        /// Header name/value used as the VM selector (generalizes the
        /// `X-Tiko-Endpoint: vm-N` trick).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        header: Option<HeaderMatch>,
    },
    /// Dedicated host TCP listener port, passthrough (for wire protocols).
    Tcp { listener_port: u16 },
    /// First-bytes token selector (generalizes libpq `options`-based routing).
    Token { token: String },
}

/// A `Name: Value` header match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderMatch {
    pub name: String,
    /// The literal value, or `"{vm_id}"` to match the VM id derived from the
    /// header value (e.g. header `X-Tiko-Endpoint: vm-3` selects vm-3).
    pub value: String,
}
