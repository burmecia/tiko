//! vsock RPC messages between host and guest (design §7).
//!
//! Two directions over virtio-vsock:
//!
//! - **Guest → host (control):** the guest connects (AF_VSOCK) to the host
//!   (CID 2) on [`HOST_CTRL_PORT`] to pull network stats and request lifecycle
//!   actions. The host listens on a **per-VM** AF_UNIX socket
//!   (`{vsock_uds}_HOST_CTRL_PORT`), so it derives the target VM from *which
//!   socket* the connection arrived on — the messages carry no `vm_id`.
//! - **Host → guest (commands):** the host connects to the guest's AF_VSOCK
//!   listener on [`GUEST_VSOCK_PORT`] (future: `Start`/`Stop`/`PreSuspend`/
//!   `PostRestore`).
//!
//! All messages are framed with [`crate::codec`] (length-delimited JSON).

use serde::{Deserialize, Serialize};

/// AF_VSOCK port the guest agent listens on (host→guest commands).
pub const GUEST_VSOCK_PORT: u32 = 9000;

/// AF_VSOCK port the guest connects to on the host (guest→host control).
/// Firecracker forwards a guest connection to CID 2:`HOST_CTRL_PORT` to the
/// host's AF_UNIX socket at `{vsock_uds}_HOST_CTRL_PORT`.
pub const HOST_CTRL_PORT: u32 = 9001;

/// The host's well-known vsock CID (guests connect here to reach the host).
pub const HOST_CID: u32 = 2;

/// Guest → host control request (carried on the guest→host channel).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestToHost {
    /// Pull VM-scoped network statistics from the host (idle probe).
    GetNetworkStats,
    /// "I'm idle — please suspend me." Host obeys via pause → suspend.
    Suspend,
    /// "I'm done — please destroy me." (ephemeral/scheduled completion.)
    Shutdown,
    /// Announced on boot: the agent is up and the manifest has been loaded.
    Ready {
        workload: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        http_port: Option<u16>,
    },
    /// Periodic health report.
    HealthReport {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        healthy: Option<bool>,
    },
}

/// Host → guest reply to a [`GuestToHost`] request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostReply {
    /// Reply to `GetNetworkStats`.
    Stats(NetworkStats),
    /// Reply to `Suspend`: the VM is being suspended; `pause_epoch` bumped.
    Suspended { pause_epoch: u64 },
    /// Generic ack.
    Ok,
    /// Error reply.
    Error { message: String },
}

/// Host → guest command. The host connects to the guest's AF_VSOCK listener on
/// [`GUEST_VSOCK_PORT`] (via the vsock UDS + `CONNECT`) to drive these.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostToGuest {
    Start,
    Stop { mode: StopMode },
    /// About to suspend: run `[suspend].pre_suspend_cmd`, then ack. Sent while
    /// the VM is still running (before the host pauses).
    PreSuspend,
    /// Just restored+resumed: run `[suspend].post_restore_cmd`.
    PostRestore,
    GetHealth,
}

/// Guest → host reply to a [`HostToGuest`] command.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestReply {
    Ok,
    Health { healthy: bool },
    Error { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    Graceful,
    Force,
}

/// VM-scoped network statistics, served by the host to the guest's idle probe.
/// VM-scoped = aggregated across all ports, so the guest needs no port config.
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
