//! Node-level VM lifecycle management.
//!
//! Wraps a [`Vmm`] backend with higher-level orchestration:
//! - Provisioning (create + start)
//! - Freeze (pause → snapshot → release resources)
//! - Thaw (restore → resume)
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
/// Owns the VMM backend and adds orchestration logic for freeze,
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

    /// Freeze: pause → snapshot → destroy (release RAM/CPU).
    ///
    /// The VM's state is preserved in the snapshot file on local disk.
    /// Call [`thaw`] to resume.
    ///
    /// This is the *immediate* (cold) path, bypassing the warm-pause window.
    /// Used by the manual `PUT /freeze` route.
    pub async fn freeze(&self, vm_id: &VmId) -> Result<Snapshot, crate::vmm::VmmError> {
        info!(vm_id = %vm_id, "freezing");

        // Ensure VM is paused before snapshotting.
        let state = self.vmm.vm_state(vm_id).await?;
        if state == VmState::Running {
            self.vmm.pause_vm(vm_id).await?;
        }

        let snapshot = self.vmm.snapshot_vm(vm_id).await?;
        self.vmm.destroy_vm(vm_id).await?;

        info!(vm_id = %vm_id, "frozen");
        Ok(snapshot)
    }

    /// Warm-pause: freeze the VM but keep it in memory. TCP connections
    /// survive (frozen, not broken). The VM can be transparently resumed by
    /// the proxy's wake-on-stale. If the warm window expires without activity,
    /// the caller transitions to [`cold_freeze`].
    ///
    /// [`cold_freeze`]: Node::cold_freeze
    pub async fn warm_pause(&self, vm_id: &VmId) -> Result<(), crate::vmm::VmmError> {
        info!(vm_id = %vm_id, "warm-pausing (freeze, keep in memory)");
        if self.vmm.vm_state(vm_id).await? == VmState::Running {
            self.vmm.pause_vm(vm_id).await?;
        }
        Ok(())
    }

    /// Cold freeze: snapshot + destroy a paused VM, releasing RAM/CPU.
    /// The caller must have already warm-paused the VM and cancelled its
    /// connections. Stores the resulting snapshot in the registry.
    pub async fn cold_freeze(&self, vm_id: &VmId) -> Result<Snapshot, crate::vmm::VmmError> {
        info!(vm_id = %vm_id, "cold freeze (snapshot + destroy)");

        // Ensure paused (idempotent — warm_pause may have already done it).
        let state = self.vmm.vm_state(vm_id).await?;
        if state == VmState::Running {
            self.vmm.pause_vm(vm_id).await?;
        }

        let snapshot = self.vmm.snapshot_vm(vm_id).await?;
        self.vmm.destroy_vm(vm_id).await?;

        info!(vm_id = %vm_id, "cold-frozen");
        Ok(snapshot)
    }

    /// Thaw: restore from snapshot → resume.
    pub async fn thaw(&self, snapshot: &Snapshot) -> Result<VmId, crate::vmm::VmmError> {
        info!(vm_id = %snapshot.vm_id, "thawing");

        let vm_id = self.vmm.restore_vm(snapshot).await?;
        self.vmm.resume_vm(&vm_id).await?;

        info!(vm_id = %vm_id, "thawed");
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

    /// Wake a VM for forwarding: ensure it is `Running`, regardless of current
    /// state. Used by the proxy's wake-on-connect and the HTTP restore route.
    ///
    /// - `Running` → no-op.
    /// - `Paused`  → resume.
    /// - `Stopped` / not-present → restore from the registry-stored snapshot
    ///   (`thaw`). This is the true thaw path that
    ///   [`ensure_running`] does not cover.
    ///
    /// The cold-restore path is **single-flighted** via [`Control::restore_lock`]:
    /// concurrent callers (e.g. many clients hitting a cold VM at once) share
    /// one restore. After acquiring the lock the leader re-checks state, so a
    /// waiter that lost the race observes the VM as `Running` and returns
    /// without restoring again.
    pub async fn wake(
        &self,
        vm_id: &VmId,
        control: &crate::control::Control,
    ) -> Result<(), crate::vmm::VmmError> {
        // Fast path: present and runnable.
        match self.vmm.vm_state(vm_id).await {
            Ok(VmState::Running) => return Ok(()),
            Ok(VmState::Paused) => {
                self.vmm.resume_vm(vm_id).await?;
                // Fresh cancel signal: a prior freeze attempt (if any)
                // is now stale — new connections should not be cancelled.
                control.reset_cancellers(vm_id);
                // Clear warm-paused so the proxy re-enables keepalive and stops
                // treating the backend as stale.
                control.clear_warm_paused(vm_id);
                return Ok(());
            }
            Ok(other) => {
                return Err(crate::vmm::VmmError::InvalidState {
                    vm_id: vm_id.clone(),
                    current: other,
                    expected: VmState::Running,
                });
            }
            Err(crate::vmm::VmmError::VmNotFound(_)) => {}
            Err(e) => return Err(e),
        }

        // Cold path: VM is gone — restore under a single-flight lock.
        let lock = control.restore_lock(vm_id);
        let _guard = lock.lock().await;

        // Re-check after acquiring the lock: another caller may have restored
        // the VM while we were waiting.
        if let Ok(VmState::Running) = self.vmm.vm_state(vm_id).await {
            return Ok(());
        }

        let snapshot = control
            .get_snapshot(vm_id)
            .ok_or_else(|| crate::vmm::VmmError::SnapshotNotFound(vm_id.clone()))?;
        self.thaw(&snapshot).await?;
        // Fresh cancel signal for the restored VM.
        control.reset_cancellers(vm_id);
        // Clear warm-paused (cold restore produces a Running VM).
        control.clear_warm_paused(vm_id);
        // No epoch bump here — the pause epoch was already bumped at pause
        // time (when the VM was warm-paused before snapshotting). The guest
        // detects the stale mismatch on its first tick after restore.
        Ok(())
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
