//! Control plane: VM registry and idle policy.
//!
//! Tracks VM state and enforces the auto-pause (scale-to-zero) policy.
//! In v1 this is entirely in-memory and single-node — no persistence,
//! no clustering.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │ Control                                             │
//! │  ┌─────────────────────────────────────────────────┐│
//! │  │ VM Registry: { vm_id → VmRecord }              ││
//! │  │  - tenant_id, branch_id                         ││
//! │  │  - state: Running | Paused | Stopped            ││
//! │  │  - last_active_at (for idle detection)          ││
//! │  │  - snapshot_id (if paused)                      ││
//! │  └─────────────────────────────────────────────────┘│
//! │  ┌─────────────────────────────────────────────────┐│
//! │  │ Idle Policy:                                    ││
//! │  │  - idle_timeout_secs (default 300 / 5 min)      ││
//! │  │  - checks last_active_at; triggers pause        ││
//! │  └─────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────┘
//! ```

use std::time::{Duration, Instant};

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
}

/// Configuration for the idle (auto-pause) policy.
#[derive(Debug, Clone)]
pub struct IdlePolicy {
    /// Seconds of inactivity before auto-pausing a VM.
    pub idle_timeout_secs: u64,
}

impl Default for IdlePolicy {
    fn default() -> Self {
        Self {
            idle_timeout_secs: 300, // 5 minutes
        }
    }
}

/// In-memory VM registry with idle tracking.
pub struct Control {
    /// VM records keyed by VmId.
    vms: DashMap<VmId, VmRecord>,
    /// Idle policy configuration.
    idle_policy: IdlePolicy,
}

impl Control {
    pub fn new(idle_policy: IdlePolicy) -> Self {
        Self {
            vms: DashMap::new(),
            idle_policy,
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

    /// Returns VM IDs that have been idle longer than the policy timeout
    /// and have zero active connections. These are candidates for
    /// scale-to-zero.
    pub fn idle_vms(&self) -> Vec<VmId> {
        let timeout = Duration::from_secs(self.idle_policy.idle_timeout_secs);
        let now = Instant::now();

        self.vms
            .iter()
            .filter(|entry| {
                entry.connection_count == 0 && now.duration_since(entry.last_active_at) > timeout
            })
            .map(|entry| entry.key().clone())
            .collect()
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
}
