//! Control plane: VM registry.
//!
//! Tracks VM state and metadata. In v1 this is entirely in-memory and
//! single-node — no persistence, no clustering.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │ Control                                             │
//! │  ┌─────────────────────────────────────────────────┐│
//! │  │ VM Registry: { vm_id → VmRecord }              ││
//! │  │  - tenant_id, branch_id                         ││
//! │  │  - state: Running | Paused | Stopped            ││
//! │  │  - snapshot_id (if paused)                      ││
//! │  │  - last_report_at / last_metrics (from agent)   ││
//! │  │  - snapshot_requested (idempotency guard)       ││
//! │  └─────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────┘
//! ```

use std::time::Instant;

use dashmap::DashMap;
use tracing::{debug, info};

use crate::vmm::VmId;

/// Metadata for a registered VM.
#[derive(Debug, Clone)]
pub struct VmRecord {
    /// Owning tenant.
    pub tenant_id: String,
    /// Branch identifier (Tiko branching).
    pub branch_id: String,
    /// PostgreSQL port inside the VM (always 5432 in v1).
    pub pg_port: u16,
    /// Last time a client connection was active.
    pub last_active_at: Instant,
    /// Whether the VM currently has active client connections.
    pub connection_count: u32,
    /// Snapshot ID if the VM is paused/snapshotted.
    pub snapshot_id: Option<String>,
    /// Last time the guest agent pushed a metrics report.
    pub last_report_at: Option<Instant>,
    /// Last metrics report body (raw JSON from the agent).
    pub last_metrics: Option<serde_json::Value>,
    /// Idempotency guard: true while a snapshot-request from the agent is being
    /// processed (between ack and scale_to_zero completion).
    pub snapshot_requested: bool,
}

/// In-memory VM registry.
pub struct Control {
    /// VM records keyed by VmId.
    vms: DashMap<VmId, VmRecord>,
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

impl Control {
    pub fn new() -> Self {
        Self {
            vms: DashMap::new(),
        }
    }

    /// Register a new VM in the control plane.
    pub fn register(
        &self,
        vm_id: VmId,
        tenant_id: String,
        branch_id: String,
        pg_port: u16,
    ) {
        info!(vm_id = %vm_id, tenant = %tenant_id, "registering VM");
        self.vms.insert(
            vm_id,
            VmRecord {
                tenant_id,
                branch_id,
                pg_port,
                last_active_at: Instant::now(),
                connection_count: 0,
                snapshot_id: None,
                last_report_at: None,
                last_metrics: None,
                snapshot_requested: false,
            },
        );
    }

    /// Unregister a VM.
    pub fn unregister(&self, vm_id: &VmId) {
        self.vms.remove(vm_id);
    }

    /// Record that a client connected to a VM (increments connection counter,
    /// updates last-active timestamp).
    pub fn on_connect(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.connection_count += 1;
            rec.last_active_at = Instant::now();
            debug!(vm_id = %vm_id, conns = rec.connection_count, "client connected");
        }
    }

    /// Record that a client disconnected from a VM.
    pub fn on_disconnect(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            if rec.connection_count > 0 {
                rec.connection_count -= 1;
            }
            rec.last_active_at = Instant::now();
            debug!(vm_id = %vm_id, conns = rec.connection_count, "client disconnected");
        }
    }

    /// Get the record for a VM.
    pub fn get(&self, vm_id: &VmId) -> Option<VmRecord> {
        self.vms.get(vm_id).map(|r| r.clone())
    }

    /// List all registered VMs.
    pub fn list(&self) -> Vec<(VmId, VmRecord)> {
        self.vms
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Mark a VM as having a snapshot (after scale-to-zero).
    pub fn set_snapshot(&self, vm_id: &VmId, snapshot_id: String) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.snapshot_id = Some(snapshot_id);
        }
    }

    /// Store a metrics report from the guest agent. Returns `true` if the VM
    /// was found in the registry, `false` if it's unknown (caller should 404).
    pub fn record_report(&self, vm_id: &VmId, metrics: serde_json::Value) -> bool {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.last_report_at = Some(Instant::now());
            rec.last_metrics = Some(metrics);
            debug!(vm_id = %vm_id, "recorded metrics report");
            true
        } else {
            false
        }
    }

    /// Try to mark a snapshot-request as in-progress. Returns:
    /// - `None` — VM not found in the registry (caller returns 404).
    /// - `Some(true)` — already requested (idempotent — caller returns 202 but
    ///   does NOT spawn another scale_to_zero).
    /// - `Some(false)` — new request, flag now set (caller spawns scale_to_zero).
    pub fn try_mark_snapshot_requested(&self, vm_id: &VmId) -> Option<bool> {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            let was = rec.snapshot_requested;
            rec.snapshot_requested = true;
            Some(was)
        } else {
            None
        }
    }

    /// Clear the snapshot-requested flag (after scale_to_zero completes or
    /// fails).
    pub fn clear_snapshot_requested(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.snapshot_requested = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_request_idempotency() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        // Unknown VM → None.
        assert!(ctrl.try_mark_snapshot_requested(&"vm-ghost".into()).is_none());

        // First request → Some(false) (new).
        assert_eq!(
            ctrl.try_mark_snapshot_requested(&"vm-1".into()),
            Some(false)
        );

        // Second request → Some(true) (already requested — idempotent).
        assert_eq!(
            ctrl.try_mark_snapshot_requested(&"vm-1".into()),
            Some(true)
        );

        // After clearing, next request is new again.
        ctrl.clear_snapshot_requested(&"vm-1".into());
        assert_eq!(
            ctrl.try_mark_snapshot_requested(&"vm-1".into()),
            Some(false)
        );
    }
}
