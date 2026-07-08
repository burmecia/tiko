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
//! │  │  - pause_requested (idempotency guard)          ││
//! │  │  - pause_epoch (incremented on each pause)      ││
//! │  └─────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{watch, Mutex, Notify};
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
    /// `freeze`). Kept here so clients can restore/thaw by
    /// `vm_id` alone — they never need to know `state_path`/`mem_path`/config.
    pub snapshot: Option<Snapshot>,
    /// Last time the guest agent pushed a metrics report.
    pub last_report_at: Option<Instant>,
    /// Last metrics report body (raw JSON from the agent).
    pub last_metrics: Option<serde_json::Value>,
    /// Idempotency guard: true while a pause-request from the agent is being
    /// processed (between ack and warm-window completion).
    pub pause_requested: bool,
    /// Monotonic counter bumped on every pause (`POST /pause-request`). The
    /// guest detects the change via the `pause_epoch` field returned by
    /// `POST /reports` and resets stale in-memory state (e.g. the scaler's
    /// `idle_ticks`). Covers both pause→resume (traffic during warm window)
    /// and pause→snapshot→restore cycles, because the bump happens at pause
    /// time — before the guest is frozen — so the guest's stale local copy
    /// always mismatches on the first tick after resume/restore.
    pub pause_epoch: u64,
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
    /// Per-VM thermal-state signals (`false` = Running, `true` = WarmPaused).
    /// The proxy subscribes and uses it to (a) toggle TCP keepalive on/off and
    /// (b) wake-on-stale (resume the VM before forwarding client data). See
    /// [`mark_warm_paused`] / [`clear_warm_paused`].
    ///
    /// [`mark_warm_paused`]: Control::mark_warm_paused
    /// [`clear_warm_paused`]: Control::clear_warm_paused
    warm: DashMap<VmId, watch::Sender<bool>>,
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
            warm: DashMap::new(),
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
                pause_requested: false,
                pause_epoch: 0,
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

    /// Store the full snapshot descriptor for a VM (after freeze). The
    /// snapshot is later retrieved by [`get_snapshot`] so clients can restore /
    /// thaw by `vm_id` alone — no snapshot details cross the wire.
    pub fn set_snapshot(&self, vm_id: &VmId, snapshot: Snapshot) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.snapshot = Some(snapshot);
        }
    }

    /// Retrieve the stored snapshot for a VM (for `PUT /vms/{vm_id}/restore`
    /// and `PUT /vms/{vm_id}/thaw`). Returns `None` if the VM isn't
    /// registered or has no snapshot (never frozen).
    pub fn get_snapshot(&self, vm_id: &VmId) -> Option<Snapshot> {
        self.vms.get(vm_id).and_then(|r| r.snapshot.clone())
    }

    /// Store a metrics report from the guest agent. Returns `Some(pause_epoch)`
    /// if the VM was found (so the caller can include it in the response for
    /// the guest's epoch-mismatch detection), or `None` if it's unknown (caller
    /// should 404).
    pub fn record_report(&self, vm_id: &VmId, metrics: serde_json::Value) -> Option<u64> {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.last_report_at = Some(Instant::now());
            rec.last_metrics = Some(metrics);
            debug!(vm_id = %vm_id, "recorded metrics report");
            Some(rec.pause_epoch)
        } else {
            None
        }
    }

    /// Try to mark a pause-request as in-progress. Returns:
    /// - `None` — VM not found in the registry (caller returns 404).
    /// - `Some(true)` — already requested (idempotent — caller returns 202 but
    ///   does NOT trigger another pause).
    /// - `Some(false)` — new request, flag now set (caller pauses the VM).
    pub fn try_mark_pause_requested(&self, vm_id: &VmId) -> Option<bool> {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            let was = rec.pause_requested;
            rec.pause_requested = true;
            Some(was)
        } else {
            None
        }
    }

    /// Clear the pause-requested flag (after warm-window completes or fails).
    pub fn clear_pause_requested(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.pause_requested = false;
        }
    }

    /// Bump the pause epoch for `vm_id`. Called each time tikod pauses the VM
    /// (on receiving a new pause-request). The guest detects the change via
    /// the `pause_epoch` field in the `POST /reports` response and resets
    /// stale state.
    ///
    /// No-op if the VM isn't registered.
    pub fn bump_pause_epoch(&self, vm_id: &VmId) {
        if let Some(mut rec) = self.vms.get_mut(vm_id) {
            rec.pause_epoch = rec.pause_epoch.saturating_add(1);
            debug!(vm_id = %vm_id, epoch = rec.pause_epoch, "bumped pause epoch");
        }
    }

    /// Current pause epoch for `vm_id`, or `None` if the VM isn't registered.
    pub fn pause_epoch(&self, vm_id: &VmId) -> Option<u64> {
        self.vms.get(vm_id).map(|r| r.pause_epoch)
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
    /// freeze, *before* pausing the VM. Connections that subscribe
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
    /// not affected by a prior freeze cancel. Replacing (rather than
    /// reusing) the `Notify` means stale waiters from the cancelled generation
    /// are never spuriously woken by later activity.
    pub fn reset_cancellers(&self, vm_id: &VmId) {
        self.cancellers.insert(vm_id.clone(), Arc::new(Notify::new()));
    }

    /// Subscribe to `vm_id`'s thermal-state signal. Returns a `watch::Receiver`
    /// that yields `true` while the VM is warm-paused and `false` while
    /// running. The proxy uses this to toggle keepalive and to wake-on-stale.
    pub fn subscribe_warm(&self, vm_id: &VmId) -> watch::Receiver<bool> {
        self.warm
            .entry(vm_id.clone())
            .or_insert_with(|| watch::channel(false).0)
            .subscribe()
    }

    /// Mark `vm_id` as warm-paused. Notifies all subscribers (proxied
    /// connections) so they can disable TCP keepalive before the kernel's
    /// keepalive probe would kill the frozen connection.
    ///
    /// Uses `send_replace` (not `send`): `watch::Sender::send` is a silent
    /// no-op when the channel has no live receivers, so it would fail to store
    /// `true` when the proxied connection that created the watch has since
    /// closed — leaving `is_warm_paused` reading a stale `false` and causing
    /// the warm timer to skip cold-freeze. `send_replace` always updates the
    /// stored value and notifies any live receivers.
    pub fn mark_warm_paused(&self, vm_id: &VmId) {
        if let Some(tx) = self.warm.get(vm_id) {
            tx.send_replace(true);
        } else {
            let (tx, _rx) = watch::channel(true);
            self.warm.insert(vm_id.clone(), tx);
        }
        debug!(vm_id = %vm_id, "marked warm-paused");
    }

    /// Mark `vm_id` as running (clear warm-paused). Called after a successful
    /// wake (resume). Notifies subscribers to re-enable keepalive.
    pub fn clear_warm_paused(&self, vm_id: &VmId) {
        if let Some(tx) = self.warm.get(vm_id) {
            tx.send_replace(false);
            debug!(vm_id = %vm_id, "cleared warm-paused");
        }
    }

    /// Read the current thermal state: `true` if warm-paused, `false`
    /// otherwise (running or unknown). Used by the warm timer to decide
    /// whether to proceed to cold freeze.
    pub fn is_warm_paused(&self, vm_id: &VmId) -> bool {
        self.warm.get(vm_id).is_some_and(|tx| *tx.borrow())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_request_idempotency() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        // Unknown VM → None.
        assert!(ctrl.try_mark_pause_requested(&"vm-ghost".into()).is_none());

        // First request → Some(false) (new).
        assert_eq!(
            ctrl.try_mark_pause_requested(&"vm-1".into()),
            Some(false)
        );

        // Second request → Some(true) (already requested — idempotent).
        assert_eq!(
            ctrl.try_mark_pause_requested(&"vm-1".into()),
            Some(true)
        );

        // After clearing, next request is new again.
        ctrl.clear_pause_requested(&"vm-1".into());
        assert_eq!(
            ctrl.try_mark_pause_requested(&"vm-1".into()),
            Some(false)
        );
    }

    #[test]
    fn pause_epoch_starts_zero_and_increments() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        // Freshly registered → epoch 0.
        assert_eq!(ctrl.pause_epoch(&"vm-1".into()), Some(0));

        // Each bump increments by one.
        ctrl.bump_pause_epoch(&"vm-1".into());
        ctrl.bump_pause_epoch(&"vm-1".into());
        assert_eq!(ctrl.pause_epoch(&"vm-1".into()), Some(2));
    }

    #[test]
    fn pause_epoch_unknown_vm_is_none() {
        let ctrl = Control::new();
        assert_eq!(ctrl.pause_epoch(&"vm-ghost".into()), None);
        // Bumping an unknown VM is a no-op (not a panic).
        ctrl.bump_pause_epoch(&"vm-ghost".into());
        assert_eq!(ctrl.pause_epoch(&"vm-ghost".into()), None);
    }

    /// Regression: `mark_warm_paused` must store `true` even when the proxied
    /// connection that created the watch has closed (no live receivers). The
    /// naive `watch::Sender::send` is a silent no-op when the channel is
    /// closed, which left `is_warm_paused` reading a stale `false` and caused
    /// the warm timer to skip cold-freeze for VMs with no active connection.
    #[test]
    fn warm_paused_stored_without_subscribers() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        // A proxied connection subscribed, then closed (receiver dropped).
        let rx = ctrl.subscribe_warm(&"vm-1".into());
        drop(rx);
        assert!(!ctrl.is_warm_paused(&"vm-1".into())); // still false

        // Marking warm-paused must take effect despite no live receiver.
        ctrl.mark_warm_paused(&"vm-1".into());
        assert!(ctrl.is_warm_paused(&"vm-1".into()));

        // Clearing must likewise take effect without a live receiver.
        ctrl.clear_warm_paused(&"vm-1".into());
        assert!(!ctrl.is_warm_paused(&"vm-1".into()));
    }

    /// `mark_warm_paused` on a VM with no prior watch entry (no connection was
    /// ever proxied) must still store `true` for `is_warm_paused`.
    #[test]
    fn warm_paused_marked_with_no_prior_entry() {
        let ctrl = Control::new();
        ctrl.register("vm-1".into(), "acme".into(), "main".into(), 5432);

        assert!(!ctrl.is_warm_paused(&"vm-1".into()));
        ctrl.mark_warm_paused(&"vm-1".into());
        assert!(ctrl.is_warm_paused(&"vm-1".into()));
    }
}
