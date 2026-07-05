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

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, info};

use crate::vmm::{Snapshot, VmId};

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
    /// Full snapshot descriptor if the VM is paused/snapshotted (set after
    /// `scale_to_zero`). Kept here so clients can restore/scale-from-zero by
    /// `vm_id` alone — they never need to know `state_path`/`mem_path`/config.
    pub snapshot: Option<Snapshot>,
    /// Last time the guest agent pushed a metrics report.
    pub last_report_at: Option<Instant>,
    /// Last metrics report body (raw JSON from the agent).
    pub last_metrics: Option<serde_json::Value>,
    /// Idempotency guard: true while a snapshot-request from the agent is being
    /// processed (between ack and scale_to_zero completion).
    pub snapshot_requested: bool,
    /// Monotonic counter bumped on every successful cold restore
    /// (`scale_from_zero`). Guest agents poll it via
    /// `GET /vms/{vm_id}/restore-epoch` to detect that they were restored from
    /// a snapshot and reset stale in-memory state (e.g. the scaler's
    /// `requested` flag / `idle_ticks`). See [`Control::bump_restore_epoch`].
    pub restore_epoch: u64,
}

/// In-memory VM registry.
pub struct Control {
    /// VM records keyed by VmId.
    vms: DashMap<VmId, VmRecord>,
    /// Single-flight restore locks, keyed by VmId. The first caller to wake a
    /// cold (Stopped) VM becomes the leader and performs the restore while
    /// holding the mutex; concurrent callers await the same lock and then
    /// observe the VM as Running (re-checked in [`crate::node::Node::wake`]).
    /// Entries persist (one per VM that was ever cold-restored) — bounded by
    /// the number of VMs and cheap (an empty async mutex each).
    restores: DashMap<VmId, Arc<Mutex<()>>>,
    /// Per-VM connection-cancellation signals. Each active proxied connection
    /// holds a clone of the `Arc<Notify>` and waits on `.notified()` while
    /// splicing. When [`cancel_vm_connections`] fires, all waiters wake and
    /// close their client sockets promptly — so the client sees a connection
    /// reset and reconnects through the wake path, instead of hanging on a
    /// frozen/destroyed backend. [`reset_cancellers`] swaps in a fresh Notify
    /// after a successful wake so new connections aren't affected by a stale
    /// signal.
    cancellers: DashMap<VmId, Arc<Notify>>,
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
            restores: DashMap::new(),
            cancellers: DashMap::new(),
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
                snapshot: None,
                last_report_at: None,
                last_metrics: None,
                snapshot_requested: false,
                restore_epoch: 0,
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

    /// Store the full snapshot descriptor for a VM (after scale-to-zero). The
    /// snapshot is later retrieved by [`get_snapshot`] so clients can restore /
    /// scale-from-zero by `vm_id` alone — no snapshot details cross the wire.
    pub fn set_snapshot(&self, vm_id: &VmId, snapshot: Snapshot) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.snapshot = Some(snapshot);
        }
    }

    /// Retrieve the stored snapshot for a VM (for `PUT /vms/{vm_id}/restore`
    /// and `PUT /vms/{vm_id}/scale-from-zero`). Returns `None` if the VM isn't
    /// registered or has no snapshot (never scaled to zero).
    pub fn get_snapshot(&self, vm_id: &VmId) -> Option<Snapshot> {
        self.vms.get(vm_id).and_then(|r| r.snapshot.clone())
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

    /// Bump the restore epoch for `vm_id`. Called after a successful cold
    /// restore (the leader in `Node::wake`'s single-flight path). Guest agents
    /// polling [`restore_epoch`] detect the change and reset stale state.
    ///
    /// No-op if the VM isn't registered (a restore of an unknown VM is
    /// impossible in practice — the snapshot is looked up from the registry).
    ///
    /// [`restore_epoch`]: Control::restore_epoch
    pub fn bump_restore_epoch(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.restore_epoch = rec.restore_epoch.saturating_add(1);
            debug!(vm_id = %vm_id, epoch = rec.restore_epoch, "bumped restore epoch");
        }
    }

    /// Current restore epoch for `vm_id`, or `None` if the VM isn't registered.
    pub fn restore_epoch(&self, vm_id: &VmId) -> Option<u64> {
        self.vms.get(vm_id).map(|r| r.restore_epoch)
    }

    /// Per-VM single-flight restore lock. Callers hold the returned mutex while
    /// performing a cold restore so that concurrent connections to the same
    /// Stopped VM share one restore rather than racing. See [`Node::wake`].
    pub fn restore_lock(&self, vm_id: &VmId) -> Arc<Mutex<()>> {
        self.restores
            .entry(vm_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Get this VM's connection-cancel signal (creating one if absent). Each
    /// proxied connection clones the returned `Arc<Notify>` and waits on
    /// `.notified()` while splicing. See [`Self::cancel_vm_connections`].
    pub fn subscribe_cancel(&self, vm_id: &VmId) -> Arc<Notify> {
        self.cancellers
            .entry(vm_id.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Wake all connections currently waiting on `vm_id`'s cancel signal, so
    /// they close their client sockets promptly. Called at the start of
    /// scale-to-zero, *before* pausing the VM. Connections that subscribe
    /// after this call (and before [`reset_cancellers`]) will simply fail on
    /// the dead backend and reconnect through wake.
    pub fn cancel_vm_connections(&self, vm_id: &VmId) {
        if let Some(notify) = self.cancellers.get(vm_id) {
            debug!(vm_id = %vm_id, "cancelling active connections");
            notify.notify_waiters();
        }
    }

    /// Swap in a fresh cancel signal for `vm_id`. Called after a successful
    /// wake (resume / restore) so that connections to the now-running VM are
    /// not affected by a prior scale-to-zero cancel. Replacing (rather than
    /// reusing) the `Notify` means stale waiters from the cancelled generation
    /// are never spuriously woken by later activity.
    pub fn reset_cancellers(&self, vm_id: &VmId) {
        self.cancellers.insert(vm_id.clone(), Arc::new(Notify::new()));
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

    #[test]
    fn restore_epoch_starts_zero_and_increments() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        // Freshly registered → epoch 0.
        assert_eq!(ctrl.restore_epoch(&"vm-1".into()), Some(0));

        // Each bump increments by one.
        ctrl.bump_restore_epoch(&"vm-1".into());
        ctrl.bump_restore_epoch(&"vm-1".into());
        assert_eq!(ctrl.restore_epoch(&"vm-1".into()), Some(2));
    }

    #[test]
    fn restore_epoch_unknown_vm_is_none() {
        let ctrl = Control::new();
        assert_eq!(ctrl.restore_epoch(&"vm-ghost".into()), None);
        // Bumping an unknown VM is a no-op (not a panic).
        ctrl.bump_restore_epoch(&"vm-ghost".into());
        assert_eq!(ctrl.restore_epoch(&"vm-ghost".into()), None);
    }
}
