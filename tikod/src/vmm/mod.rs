//! VMM abstraction layer.
//!
//! Defines the [`Vmm`] trait that abstracts the hypervisor backend, allowing
//! `tikod` to run on macOS (Apple Virtualization Framework) for development
//! and on Linux (Firecracker) for production.
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │ node/ module  ─── uses ──→  trait Vmm            │
//! │                               ▲         ▲        │
//! │                   ┌───────────┘         │        │
//! │              AppleVzVmm            FirecrackerVmm│
//! │              (macOS dev)           (Linux prod)  │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod firecracker;

#[cfg(target_os = "macos")]
pub mod apple_vz;

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Unique identifier for a running (or paused) VM instance.
pub type VmId = String;

/// Lifecycle state of a VM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmState {
    /// VM is being created / booted.
    Starting,
    /// VM is running and accepting connections.
    Running,
    /// VM is paused (frozen); memory state preserved.
    Paused,
    /// VM is being snapshotted.
    Snapshotting,
    /// VM is being restored from a snapshot.
    Restoring,
    /// VM has been shut down or destroyed.
    Stopped,
}

impl std::fmt::Display for VmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmState::Starting => write!(f, "starting"),
            VmState::Running => write!(f, "running"),
            VmState::Paused => write!(f, "paused"),
            VmState::Snapshotting => write!(f, "snapshotting"),
            VmState::Restoring => write!(f, "restoring"),
            VmState::Stopped => write!(f, "stopped"),
        }
    }
}

/// Configuration for creating a new VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmConfig {
    /// Unique identifier for this VM instance.
    pub vm_id: VmId,
    /// Path to the Linux kernel image (e.g. `vmlinux` / `bzImage`).
    pub kernel_path: PathBuf,
    /// Kernel command-line arguments.
    pub kernel_cmdline: String,
    /// Path to the root filesystem image (read-only, shared across VMs).
    pub rootfs_path: PathBuf,
    /// VM memory in megabytes.
    pub memory_mb: u64,
    /// Number of virtual CPUs.
    pub vcpus: u8,
    /// Extra writable block devices (e.g. per-VM PGDATA/cache scratch).
    #[serde(default)]
    pub drives: Vec<DriveConfig>,
    /// Optional initrd path.
    #[serde(default)]
    pub initrd_path: Option<PathBuf>,
}

/// Configuration for an extra writable block device attached to a VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveConfig {
    /// Identifier for this drive within the VM (e.g. "pgdata").
    pub drive_id: String,
    /// Path to the backing file on the host.
    pub path: PathBuf,
    /// Whether the drive is read-only.
    pub read_only: bool,
}

/// A snapshot of a paused VM — the source for `restore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// ID of the VM this snapshot belongs to.
    pub vm_id: VmId,
    /// Path to the microVM-state file on the host (Firecracker `snapshot_path`).
    pub state_path: PathBuf,
    /// Path to the guest-memory file on the host (Firecracker `mem_file_path`).
    ///
    /// Carried explicitly so `restore` does not depend on a path-naming
    /// convention (the snapshot can be moved/renamed without breaking it).
    pub mem_path: PathBuf,
    /// VM config at the time of snapshot (needed for restore).
    pub config: VmConfig,
}

/// Errors returned by VMM operations.
#[derive(Debug, thiserror::Error)]
pub enum VmmError {
    #[error("VM not found: {0}")]
    VmNotFound(VmId),
    #[error("invalid state transition: VM {vm_id} is {current}, expected {expected}")]
    InvalidState {
        vm_id: VmId,
        current: VmState,
        expected: VmState,
    },
    #[error("VMM backend error: {0}")]
    Backend(String),
    #[error("snapshot not found for VM: {0}")]
    SnapshotNotFound(VmId),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type VmmResult<T> = Result<T, VmmError>;

/// Abstract hypervisor backend.
///
/// Each implementation wraps a specific VMM:
/// - [`apple_vz::AppleVzVmm`] — Apple Virtualization Framework (macOS dev)
/// - [`firecracker::FirecrackerVmm`] — Firecracker microVM (Linux prod)
///
/// The backend owns the VM state internally, keyed by [`VmId`]. All methods
/// are idempotent where reasonable and safe.
#[async_trait]
pub trait Vmm: Send + Sync {
    /// Create and register a new VM (does not start it).
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId>;

    /// Start a created (or paused) VM.
    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Pause a running VM (freeze execution, preserve memory).
    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Resume a paused VM.
    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Snapshot a paused VM to disk. Returns the snapshot descriptor.
    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot>;

    /// Restore a VM from a snapshot. The restored VM is in `Paused` state;
    /// call [`start_vm`](Vmm::start_vm) or [`resume_vm`](Vmm::resume_vm) to
    /// resume execution.
    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId>;

    /// Shut down and destroy a VM, releasing all resources.
    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Query the current lifecycle state of a VM.
    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<VmState>;

    /// Guest IP address if available (for proxy forwarding).
    async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<std::net::IpAddr>>;
}

/// Returns the platform-default VMM backend.
#[cfg(target_os = "macos")]
pub fn default_vmm(snapshot_dir: std::path::PathBuf) -> Box<dyn Vmm> {
    Box::new(apple_vz::AppleVzVmm::new(snapshot_dir))
}

/// Returns the platform-default VMM backend.
#[cfg(target_os = "linux")]
pub fn default_vmm(snapshot_dir: std::path::PathBuf) -> Box<dyn Vmm> {
    Box::new(firecracker::FirecrackerVmm::new(snapshot_dir))
}
