//! VM lifecycle types: the state machine (design §6) and provision request.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::manifest::{SchedulePolicy, WorkloadManifest};
use crate::routing::RoutingRule;

/// Unique identifier for a VM instance.
pub type VmId = String;

/// The full VM state machine. Tracked by the host's control registry; the
/// `Vmm` backend reports only coarse live states (`Created`/`Started`/`Paused`/
/// `Destroyed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmState {
    // --- transitional ---
    Creating,
    Starting,
    Pausing,
    Resuming,
    Suspending,
    Restoring,
    Destroying,
    // --- stable ---
    Created,
    Started,
    Paused,
    Suspended,
    Destroyed,
}

impl VmState {
    /// Whether this is a stable (resting) state vs a transitional one.
    pub fn is_stable(self) -> bool {
        matches!(
            self,
            VmState::Created | VmState::Started | VmState::Paused | VmState::Suspended | VmState::Destroyed
        )
    }

    /// Whether a live VM process exists in this state.
    pub fn is_live(self) -> bool {
        matches!(self, VmState::Started | VmState::Paused | VmState::Creating | VmState::Starting | VmState::Pausing | VmState::Resuming)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            VmState::Creating => "creating",
            VmState::Created => "created",
            VmState::Starting => "starting",
            VmState::Started => "started",
            VmState::Pausing => "pausing",
            VmState::Paused => "paused",
            VmState::Resuming => "resuming",
            VmState::Suspending => "suspending",
            VmState::Suspended => "suspended",
            VmState::Restoring => "restoring",
            VmState::Destroying => "destroying",
            VmState::Destroyed => "destroyed",
        }
    }
}

impl std::fmt::Display for VmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The result of validating a state transition: either the new state or an
/// error describing why the transition is illegal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalTransition {
    pub from: VmState,
    pub op: LifecycleOp,
}

/// High-level lifecycle operations, used to validate transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOp {
    Create,
    Start,
    Pause,
    Resume,
    Suspend,
    Restore,
    Destroy,
}

impl LifecycleOp {
    /// Compute the target stable state for this op given the current state.
    /// Returns `Err(IllegalTransition)` if the op isn't valid from `from`.
    ///
    /// Implements the transition table from design §6:
    /// - create:  –          → Created
    /// - start:   Created     → Started
    /// - pause:   Started     → Paused
    /// - resume:  Paused      → Started
    /// - suspend: Paused      → Suspended
    /// - restore: Suspended   → Started
    /// - destroy: any stable  → Destroyed
    pub fn transition(self, from: VmState) -> Result<VmState, IllegalTransition> {
        use LifecycleOp::*;
        use VmState::*;
        let ok = matches!(
            (self, from),
            (Create, _) | (Start, Created) | (Pause, Started) | (Resume, Paused) | (Suspend, Paused) | (Restore, Suspended)
        ) || (self == Destroy && from.is_stable());
        if !ok {
            return Err(IllegalTransition { from, op: self });
        }
        let target = match self {
            Create => Created,
            Start => Started,
            Pause => Paused,
            Resume => Started,
            Suspend => Suspended,
            Restore => Started,
            Destroy => Destroyed,
        };
        Ok(target)
    }

    /// The transitional state the VM passes through while this op is in flight.
    pub fn transitional(self) -> VmState {
        match self {
            LifecycleOp::Create => VmState::Creating,
            LifecycleOp::Start => VmState::Starting,
            LifecycleOp::Pause => VmState::Pausing,
            LifecycleOp::Resume => VmState::Resuming,
            LifecycleOp::Suspend => VmState::Suspending,
            LifecycleOp::Restore => VmState::Restoring,
            LifecycleOp::Destroy => VmState::Destroying,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LifecycleOp::Create => "create",
            LifecycleOp::Start => "start",
            LifecycleOp::Pause => "pause",
            LifecycleOp::Resume => "resume",
            LifecycleOp::Suspend => "suspend",
            LifecycleOp::Restore => "restore",
            LifecycleOp::Destroy => "destroy",
        }
    }
}

/// Resource allocation for a VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub memory_mb: u64,
    pub vcpus: u8,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self { memory_mb: 512, vcpus: 2 }
    }
}

/// Kernel + initrd to boot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelSpec {
    pub kernel_path: PathBuf,
    #[serde(default)]
    pub kernel_cmdline: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initrd_path: Option<PathBuf>,
}

/// Network placement. If `guest_ip` is omitted the host derives addressing
/// deterministically from the VM id (as the current `tikod` does).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_ip: Option<IpAddr>,
}

/// Reference to the base rootfs image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootfsRef {
    pub path: PathBuf,
    /// Read-only shared base (the common case). When false, the host copies the
    /// image per-VM instead of overlaying.
    #[serde(default = "default_true")]
    pub read_only_base: bool,
}

fn default_true() -> bool {
    true
}

/// The provision request (design §5.2). Host/infra placement: what to run and
/// how external traffic reaches it. Idle policy is *not* here (it lives in the
/// manifest and is guest-driven).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmSpec {
    pub vm_id: VmId,
    pub rootfs: RootfsRef,
    #[serde(default)]
    pub resources: ResourceConfig,
    pub kernel: KernelSpec,
    #[serde(default)]
    pub network: NetworkSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routing: Vec<RoutingRule>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// Authoritative manifest; host reads only `volumes` + `schedule`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<WorkloadManifest>,
    /// Overrides the manifest's `[schedule]` if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<SchedulePolicy>,
}

/// A live VM summary (returned by inventory / `GET /vms`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmInfo {
    pub vm_id: VmId,
    pub state: VmState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_ip: Option<IpAddr>,
    /// Workload label from the manifest, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
    /// Latest guest-reported health (true = healthy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        use LifecycleOp as O;
        use VmState::*;
        assert_eq!(O::Start.transition(Created).unwrap(), Started);
        assert_eq!(O::Pause.transition(Started).unwrap(), Paused);
        assert_eq!(O::Resume.transition(Paused).unwrap(), Started);
        assert_eq!(O::Suspend.transition(Paused).unwrap(), Suspended);
        assert_eq!(O::Restore.transition(Suspended).unwrap(), Started);
        assert_eq!(O::Destroy.transition(Suspended).unwrap(), Destroyed);
        assert_eq!(O::Destroy.transition(Started).unwrap(), Destroyed);
    }

    #[test]
    fn illegal_transitions() {
        use LifecycleOp as O;
        use VmState::*;
        // suspend only from Paused (not Started)
        assert!(O::Suspend.transition(Started).is_err());
        // restore only from Suspended
        assert!(O::Restore.transition(Paused).is_err());
        // start only from Created
        assert!(O::Start.transition(Started).is_err());
        // pause only from Started
        assert!(O::Pause.transition(Paused).is_err());
        // destroy only from stable (Pausing is transitional)
        assert!(O::Destroy.transition(Pausing).is_err());
    }

    #[test]
    fn state_predicate_roundtrip() {
        use VmState::*;
        assert!(Created.is_stable());
        assert!(!Creating.is_stable());
        assert!(Started.is_live());
        assert!(!Suspended.is_live());
        assert_eq!(Paused.to_string(), "paused");
    }
}
