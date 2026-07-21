//! VMM abstraction layer (design §6).
//!
//! The `Vmm` trait abstracts the hypervisor backend. The control registry /
//! `Node` express the full 13-state machine on top of it; the backend reports
//! only coarse live states via [`BackendState`].
//!
//! ```text
//!  node.rs  ─── uses ──→  trait Vmm
//!                            ▲
//!                            │
//!                  FirecrackerBackend   (Linux prod; not yet ported)
//!                  StubBackend          (non-Linux / dev / tests)
//! ```

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tikovm_protocol::vm::VmId;
use tikovm_protocol::volume::VolumeTier;

#[cfg(target_os = "linux")]
pub mod firecracker;
pub mod mock;

/// Coarse live state as observed by a backend. The control layer maps the full
/// fine-grained [`tikovm_protocol::vm::VmState`] onto these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendState {
    /// Registered/configured, not booted.
    Created,
    /// Booted and running.
    Started,
    /// Warm-paused (in memory).
    Paused,
    /// Torn down.
    Destroyed,
}

/// Low-level VM configuration handed to a backend at create time. Derived from
/// a [`tikovm_protocol::vm::VmSpec`] by the control layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmConfig {
    pub vm_id: VmId,
    pub kernel_path: PathBuf,
    pub kernel_cmdline: String,
    pub rootfs_path: PathBuf,
    pub memory_mb: u64,
    pub vcpus: u8,
    /// Extra writable block devices (per-VM overlay + declared volumes).
    #[serde(default)]
    pub drives: Vec<DriveConfig>,
    #[serde(default)]
    pub initrd_path: Option<PathBuf>,
    /// Guest vsock CID.
    #[serde(default)]
    pub guest_cid: Option<u32>,
}

/// An extra block device attached to a VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveConfig {
    pub drive_id: String,
    pub path: PathBuf,
    pub read_only: bool,
    /// Image size in MiB — the host creates the (sparse) ext4 file if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_mb: Option<u64>,
    /// Where the image lives (and thus its lifetime):
    /// - `LocalFast` -> under the host snapshot dir; per-VM + ephemeral on
    ///   destroy unless `persist_key` is set (then a shared local-fast store
    ///   keyed by it, persistent across destroy).
    /// - `RemoteSlow` -> under `source` (a host-mounted remote FS; persists).
    #[serde(default)]
    pub tier: VolumeTier,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// `LocalFast` only: stable persist-across-destroy identity (see
    /// [`tikovm_protocol::volume::VolumeDecl::persist_key`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist_key: Option<String>,
}

/// A snapshot of a paused VM — the source for `restore` / the artifact of
/// `suspend` (snapshot + destroy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub vm_id: VmId,
    pub state_path: PathBuf,
    pub mem_path: PathBuf,
    /// Config at snapshot time (needed for restore).
    pub config: VmConfig,
}

/// Errors returned by VMM operations.
#[derive(Debug, thiserror::Error)]
pub enum VmmError {
    #[error("VM not found: {0}")]
    VmNotFound(VmId),
    #[error("invalid state transition: VM {vm_id} is {current}, op requires {required}")]
    InvalidState {
        vm_id: VmId,
        current: &'static str,
        required: &'static str,
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

/// Abstract hypervisor backend. Owns live VM state internally, keyed by `VmId`.
///
/// These are **low-level** primitives; the high-level lifecycle operations
/// (suspend = snapshot+destroy, restore = restore+resume) are composed in
/// [`crate::node::Node`].
#[async_trait]
pub trait Vmm: Send + Sync {
    /// Create and register a new VM (does not boot it).
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId>;

    /// Boot a created (or restore-from-snapshot) VM.
    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Warm-pause a running VM (preserve memory).
    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Resume a warm-paused VM.
    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Snapshot a paused VM to disk. Returns the snapshot descriptor.
    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot>;

    /// Restore a VM from a snapshot. The restored VM is `Paused`; call
    /// `resume_vm` to run it.
    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId>;

    /// Shut down and destroy a VM, releasing resources.
    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()>;

    /// Query the coarse live state of a VM.
    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<BackendState>;

    /// Guest IP address if available (for proxy forwarding).
    async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<IpAddr>>;

    /// List every live VM the backend currently knows about.
    async fn list_vms(&self) -> VmmResult<Vec<(VmId, BackendState)>>;

    /// Path to the virtio-vsock host-side UDS for `vm_id`, if the VM has a
    /// vsock device. The control plane binds `{path}_HOST_CTRL_PORT` to receive
    /// guest→host connections (Firecracker forwards CID 2:port there).
    async fn vsock_uds_path(&self, _vm_id: &VmId) -> VmmResult<Option<PathBuf>> {
        Ok(None)
    }

    /// Release durable-per-VM host resources on a **terminal** destroy (not
    /// suspend — suspend must keep volumes so the VM can restore). Default: noop.
    async fn cleanup_vm(&self, _vm_id: &VmId) -> VmmResult<()> {
        Ok(())
    }
}

/// Returns the platform-default VMM backend.
///
/// On non-Linux platforms there is no hypervisor, so this returns a
/// [`StubBackend`] whose every operation fails — letting the rest of the binary
/// (config, API, proxy) compile and run for development.
/// Returns the platform-default VMM backend.
///
/// On Linux this is the Firecracker backend (with volume provisioning from
/// the storage config); elsewhere a [`StubBackend`] whose every operation
/// fails (lets the rest of the binary compile/run for dev).
#[cfg(target_os = "linux")]
pub fn default_vmm(snapshot_dir: PathBuf, storage: &crate::config::StorageConfig) -> Arc<dyn Vmm> {
    Arc::new(firecracker::FirecrackerVmm::new(snapshot_dir, storage))
}

/// Returns the platform-default VMM backend (non-Linux: stub).
#[cfg(not(target_os = "linux"))]
pub fn default_vmm(_snapshot_dir: PathBuf, _storage: &crate::config::StorageConfig) -> Arc<dyn Vmm> {
    Arc::new(StubBackend)
}

/// No-op backend for platforms/dev without a real hypervisor. Every operation
/// returns [`VmmError::Backend`].
#[derive(Default)]
pub struct StubBackend;

fn unsupported() -> VmmError {
    VmmError::Backend(
        "no VMM backend available on this platform (Firecracker requires Linux/KVM); \
         using StubBackend"
            .into(),
    )
}

#[async_trait]
impl Vmm for StubBackend {
    async fn create_vm(&self, _config: VmConfig) -> VmmResult<VmId> {
        Err(unsupported())
    }
    async fn start_vm(&self, _vm_id: &VmId) -> VmmResult<()> {
        Err(unsupported())
    }
    async fn pause_vm(&self, _vm_id: &VmId) -> VmmResult<()> {
        Err(unsupported())
    }
    async fn resume_vm(&self, _vm_id: &VmId) -> VmmResult<()> {
        Err(unsupported())
    }
    async fn snapshot_vm(&self, _vm_id: &VmId) -> VmmResult<Snapshot> {
        Err(unsupported())
    }
    async fn restore_vm(&self, _snapshot: &Snapshot) -> VmmResult<VmId> {
        Err(unsupported())
    }
    async fn destroy_vm(&self, _vm_id: &VmId) -> VmmResult<()> {
        Err(unsupported())
    }
    async fn vm_state(&self, _vm_id: &VmId) -> VmmResult<BackendState> {
        Err(unsupported())
    }
    async fn vm_guest_ip(&self, _vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        Err(unsupported())
    }
    async fn list_vms(&self) -> VmmResult<Vec<(VmId, BackendState)>> {
        Err(unsupported())
    }
}
