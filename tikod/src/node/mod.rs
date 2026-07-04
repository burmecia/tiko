//! Node-level VM lifecycle management.
//!
//! Wraps a [`Vmm`] backend with higher-level orchestration:
//! - Provisioning (create + start)
//! - Scale-to-zero (pause → snapshot → release resources)
//! - Scale-from-zero (restore → resume)
//! - VM endpoint discovery (for proxy forwarding)
//!
//! ```text
//! proxy/ ──→ node::Node ──→ Vmm trait ──→ backend (AppleVz / Firecracker)
//!                │
//!                ├── snapshot cache (local NVMe / disk)
//!                └── per-VM artifact dirs (serial logs, sockets)
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use crate::vmm::{Snapshot, VmConfig, VmId, VmInfo, VmState, Vmm};

/// High-level VM lifecycle manager.
///
/// Owns the VMM backend and adds orchestration logic for scale-to-zero,
/// snapshot caching, and endpoint discovery.
pub struct Node {
    /// The VMM backend (AppleVzVmm on macOS, FirecrackerVmm on Linux).
    vmm: Arc<dyn Vmm>,
    /// Directory for snapshot files.
    snapshot_dir: PathBuf,
}

impl Node {
    /// Create a new node manager wrapping a VMM backend.
    pub fn new(vmm: Arc<dyn Vmm>, snapshot_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&snapshot_dir).ok();
        Self { vmm, snapshot_dir }
    }

    /// Provision a new VM: create and start it.
    pub async fn provision(&self, config: VmConfig) -> Result<VmId, crate::vmm::VmmError> {
        let vm_id = self.vmm.create_vm(config).await?;
        self.vmm.start_vm(&vm_id).await?;
        info!(vm_id = %vm_id, "VM provisioned");
        Ok(vm_id)
    }

    /// Scale to zero: pause → snapshot → destroy (release RAM/CPU).
    ///
    /// The VM's state is preserved in the snapshot file on local disk.
    /// Call [`scale_from_zero`] to resume.
    pub async fn scale_to_zero(&self, vm_id: &VmId) -> Result<Snapshot, crate::vmm::VmmError> {
        info!(vm_id = %vm_id, "scaling to zero");

        // Ensure VM is paused before snapshotting.
        let state = self.vmm.vm_state(vm_id).await?;
        if state == VmState::Running {
            self.vmm.pause_vm(vm_id).await?;
        }

        let snapshot = self.vmm.snapshot_vm(vm_id).await?;
        self.vmm.destroy_vm(vm_id).await?;

        info!(vm_id = %vm_id, "scaled to zero");
        Ok(snapshot)
    }

    /// Scale from zero: restore from snapshot → resume.
    pub async fn scale_from_zero(&self, snapshot: &Snapshot) -> Result<VmId, crate::vmm::VmmError> {
        info!(vm_id = %snapshot.vm_id, "scaling from zero");

        let vm_id = self.vmm.restore_vm(snapshot).await?;
        self.vmm.resume_vm(&vm_id).await?;

        info!(vm_id = %vm_id, "scaled from zero");
        Ok(vm_id)
    }

    /// Ensure a VM is running. If it's paused, resume it.
    /// Returns the VM ID once it's in `Running` state.
    pub async fn ensure_running(&self, vm_id: &VmId) -> Result<(), crate::vmm::VmmError> {
        let state = self.vmm.vm_state(vm_id).await?;
        match state {
            VmState::Running => Ok(()),
            VmState::Paused => {
                self.vmm.resume_vm(vm_id).await?;
                Ok(())
            }
            other => Err(crate::vmm::VmmError::InvalidState {
                vm_id: vm_id.clone(),
                current: other,
                expected: VmState::Running,
            }),
        }
    }

    /// Get the guest IP address of a VM (for proxy forwarding).
    pub async fn guest_ip(
        &self,
        vm_id: &VmId,
    ) -> Result<Option<std::net::IpAddr>, crate::vmm::VmmError> {
        self.vmm.vm_guest_ip(vm_id).await
    }

    /// Destroy a VM permanently.
    pub async fn destroy(&self, vm_id: &VmId) -> Result<(), crate::vmm::VmmError> {
        self.vmm.destroy_vm(vm_id).await
    }

    /// Query VM state.
    pub async fn state(&self, vm_id: &VmId) -> Result<VmState, crate::vmm::VmmError> {
        self.vmm.vm_state(vm_id).await
    }

    /// List every live VM the backend knows about. This is the authoritative
    /// per-node inventory; the control plane merges it with its registry.
    pub async fn list_vms(&self) -> Result<Vec<VmInfo>, crate::vmm::VmmError> {
        self.vmm.list_vms().await
    }

    /// Reference to the underlying VMM backend.
    pub fn vmm(&self) -> &Arc<dyn Vmm> {
        &self.vmm
    }

    /// Snapshot directory path.
    pub fn snapshot_dir(&self) -> &PathBuf {
        &self.snapshot_dir
    }
}
