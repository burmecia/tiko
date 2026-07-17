//! Control registry (design §6, §14). The in-memory source of VM records.
//!
//! Backed by a [`DashMap`]; each value is an `Arc<RwLock<VmRecord>>` so a record
//! can be held across `.await` points in [`crate::node::Node`] without holding a
//! shard guard. The registry is the **hot read path**; the durable source of
//! truth is the [`crate::store::StateStore`] (write-through, design §14).

use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use dashmap::DashMap;
use tokio::sync::Notify;
use tikovm_protocol::vm::{VmId, VmInfo, VmSpec, VmState};

use crate::vmm::Snapshot;

/// A VM's full control-plane record.
#[derive(Debug, Clone)]
pub struct VmRecord {
    pub spec: VmSpec,
    pub state: VmState,
    pub snapshot: Option<Snapshot>,
    pub guest_ip: Option<IpAddr>,
    pub vsock_cid: Option<u32>,
    pub last_activity: Option<SystemTime>,
    pub pause_epoch: u64,
    pub last_metrics: Option<serde_json::Value>,
    /// Latest guest-reported health (transient: in-memory only, not persisted).
    pub healthy: Option<bool>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl VmRecord {
    pub fn new(spec: VmSpec, state: VmState) -> Self {
        let now = SystemTime::now();
        Self {
            spec,
            state,
            snapshot: None,
            guest_ip: None,
            vsock_cid: None,
            last_activity: None,
            pause_epoch: 0,
            last_metrics: None,
            healthy: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn workload_label(&self) -> Option<&str> {
        self.spec.manifest.as_ref().map(|m| m.workload.as_str())
    }

    pub fn to_info(&self, vm_id: &VmId) -> VmInfo {
        VmInfo {
            vm_id: vm_id.clone(),
            state: self.state,
            guest_ip: self.guest_ip,
            workload: self.workload_label().map(|s| s.to_string()),
            healthy: self.healthy,
        }
    }
}

/// In-memory VM registry + coordination primitives.
pub struct Control {
    vms: DashMap<VmId, Arc<RwLock<VmRecord>>>,
    /// Single-flight locks: only one restore per VM at a time. `tokio::sync`
    /// because the guard is held across `.await` in `Node::restore`.
    restore_locks: DashMap<VmId, Arc<tokio::sync::Mutex<()>>>,
    /// Per-VM cancel signal for warm-pause countdowns.
    cancels: DashMap<VmId, Arc<Notify>>,
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
            restore_locks: DashMap::new(),
            cancels: DashMap::new(),
        }
    }

    /// Insert/replace a record.
    pub fn register(&self, record: VmRecord) {
        let vm_id = record.spec.vm_id.clone();
        self.cancels.entry(vm_id.clone()).or_default();
        self.vms.insert(vm_id, Arc::new(RwLock::new(record)));
    }

    /// Get a handle to a record (cloned Arc).
    pub fn get(&self, vm_id: &VmId) -> Option<Arc<RwLock<VmRecord>>> {
        self.vms.get(vm_id).map(|r| r.clone())
    }

    /// Find a VM by its guest IP (the guest signals over the TAP network
    /// knowing only its own IP, not its vm_id).
    pub fn find_by_guest_ip(&self, ip: IpAddr) -> Option<(VmId, Arc<RwLock<VmRecord>>)> {
        self.vms
            .iter()
            .find(|r| r.value().read().unwrap().guest_ip == Some(ip))
            .map(|r| (r.key().clone(), r.value().clone()))
    }

    /// Remove a record (and its coordination primitives).
    pub fn remove(&self, vm_id: &VmId) -> Option<Arc<RwLock<VmRecord>>> {
        self.restore_locks.remove(vm_id);
        self.cancels.remove(vm_id);
        self.vms.remove(vm_id).map(|(_, v)| v)
    }

    /// Snapshot of all VM ids.
    pub fn ids(&self) -> Vec<VmId> {
        self.vms.iter().map(|r| r.key().clone()).collect()
    }

    /// Inventory for `GET /vms`.
    pub fn list(&self) -> Vec<VmInfo> {
        self.vms
            .iter()
            .map(|r| {
                let rec = r.value().read().unwrap();
                rec.to_info(r.key())
            })
            .collect()
    }

    /// The single-flight restore lock for a VM.
    pub fn restore_lock(&self, vm_id: &VmId) -> Arc<tokio::sync::Mutex<()>> {
        self.restore_locks.entry(vm_id.clone()).or_default().clone()
    }

    /// The warm-pause countdown cancel signal for a VM.
    pub fn cancel(&self, vm_id: &VmId) -> Arc<Notify> {
        self.cancels.entry(vm_id.clone()).or_default().clone()
    }
}
