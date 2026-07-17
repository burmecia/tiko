//! vsock RPC messages between host and guest (design §7).
//!
//! Framed with [`crate::codec`]. The host drives lifecycle (`Start`, `Stop`,
//! `PreSuspend`, `PostRestore`); the guest reports runtime status
//! (`Ready`, `HealthReport`) and requests lifecycle actions (`SuspendRequest`,
//! `ShutdownRequest`). The host also answers its own `GetNetworkStats` (the
//! proxy is the authoritative source of VM-scoped traffic stats for the guest's
//! `host_network` idle probe).

use serde::{Deserialize, Serialize};

use crate::vm::VmId;

/// Default vsock port the guest agent listens on.
pub const GUEST_VSOCK_PORT: u32 = 9000;

/// Host → guest requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostToGuest {
    /// Start (or ensure started) the workload process.
    Start,
    /// Stop the workload process.
    /// mode: "graceful" (SIGTERM+wait) | "force" (SIGKILL).
    Stop { mode: StopMode },
    /// About to suspend: run quiesce hooks, then ack.
    PreSuspend,
    /// Just restored: run resume hooks.
    PostRestore,
    /// Query current health status.
    GetHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    Graceful,
    Force,
}

/// Guest → host messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestToHost {
    /// Announced on boot: the agent is up and the manifest has been loaded.
    Ready {
        vm_id: VmId,
        workload: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        http_port: Option<u16>,
    },
    /// Periodic health report.
    HealthReport {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        healthy: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    /// "I'm idle — please suspend me." (design §8; host obeys via pause→suspend.)
    SuspendRequest { vm_id: VmId },
    /// "I'm done — please destroy me." (ephemeral/scheduled completion.)
    ShutdownRequest { vm_id: VmId },
}

/// Response envelope for a `HostToGuest` request. Used when the host expects a
/// reply (e.g. `GetHealth`, `PreSuspend` ack).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestResponse {
    Ok,
    Health {
        healthy: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    Error {
        message: String,
    },
}

/// VM-scoped network statistics, served by the host to the guest's
/// `host_network` idle probe. VM-scoped = aggregated across all ports, so the
/// guest needs no port config (design §8).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Currently established client connections to this VM.
    pub established_conns: u64,
    /// Seconds since the last data byte flowed in either direction
    /// (`u64::MAX` when no traffic has ever been seen).
    pub last_data_age_secs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl NetworkStats {
    /// True if there is no sign of recent network activity.
    pub fn is_idle(&self) -> bool {
        self.established_conns == 0 && self.last_data_age_secs > 0
    }
}
