//! Lifecycle orchestration (design §6). `Node` wraps a [`Vmm`] backend and the
//! [`Control`] registry, enforcing the 13-state transition table via
//! [`tikovm_protocol::vm::LifecycleOp`]. It composes the high-level ops:
//!
//! - `suspend`  = `snapshot_vm` → `destroy_vm`  (Paused → Suspended)
//! - `restore`  = `restore_vm`  → `resume_vm`   (Suspended → Started, single-flight)
//! - `freeze`   = `pause` → `suspend`           (Started → Suspended; scale-to-zero)
//!
//! State mutations are written through to the registry; persistence (design §14)
//! hooks in via `store` once implemented.

use std::sync::Arc;
use std::time::SystemTime;

use tikovm_protocol::vm::{LifecycleOp, VmId, VmSpec, VmState};

use crate::control::{Control, VmRecord};
use crate::store::{PersistedVmRecord, StateStore};
use crate::vmm::{VmConfig, Vmm, VmmError, VmmResult};

pub struct Node {
    vmm: Arc<dyn Vmm>,
    control: Arc<Control>,
    store: Option<Arc<dyn StateStore>>,
}

impl Node {
    pub fn new(vmm: Arc<dyn Vmm>, control: Arc<Control>) -> Self {
        Self { vmm, control, store: None }
    }

    /// Attach a durable store; state changes write through to it.
    pub fn with_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn control(&self) -> &Control {
        &self.control
    }

    pub fn vmm(&self) -> &dyn Vmm {
        self.vmm.as_ref()
    }

    /// Persist the current record to the store (if attached).
    fn persist(&self, vm_id: &VmId) {
        let Some(store) = &self.store else { return };
        let Some(rec) = self.control.get(vm_id) else { return };
        let g = rec.read().unwrap();
        let _ = store.upsert(&PersistedVmRecord::from_record(&g));
    }

    fn persist_delete(&self, vm_id: &VmId) {
        if let Some(store) = &self.store {
            let _ = store.delete(vm_id);
        }
    }

    /// Current fine-grained state, if registered.
    pub fn state_of(&self, vm_id: &VmId) -> Option<VmState> {
        let rec = self.control.get(vm_id)?;
        let g = rec.read().unwrap();
        Some(g.state)
    }

    // ---- transition helpers (synchronous, no await) -----------------------

    fn invalid(vm_id: &VmId, op: LifecycleOp, from: VmState) -> VmmError {
        VmmError::InvalidState {
            vm_id: vm_id.clone(),
            current: from.as_str(),
            required: op.as_str(),
        }
    }

    /// Validate `op` against the current state, then set the transitional state.
    /// Returns the previous state for revert-on-error.
    fn begin(&self, vm_id: &VmId, op: LifecycleOp) -> Result<VmState, VmmError> {
        let rec = self.control.get(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
        let prev = {
            let g = rec.read().unwrap();
            LifecycleOp::transition(op, g.state).map_err(|it| Self::invalid(vm_id, it.op, it.from))?;
            g.state
        };
        {
            let mut g = rec.write().unwrap();
            g.state = op.transitional();
            g.updated_at = SystemTime::now();
        }
        Ok(prev)
    }

    fn commit(&self, vm_id: &VmId, target: VmState) {
        if let Some(rec) = self.control.get(vm_id) {
            let mut g = rec.write().unwrap();
            g.state = target;
            g.updated_at = SystemTime::now();
        }
        self.persist(vm_id);
    }

    fn touch_activity(&self, vm_id: &VmId) {
        if let Some(rec) = self.control.get(vm_id) {
            let mut g = rec.write().unwrap();
            g.last_activity = Some(SystemTime::now());
        }
    }

    // ---- lifecycle ops ----------------------------------------------------

    /// Register + create a VM (does not boot). Records begin in `Created`.
    pub async fn create(&self, config: VmConfig, spec: VmSpec) -> VmmResult<VmId> {
        let vm_id = config.vm_id.clone();
        if self.control.get(&vm_id).is_some() {
            return Err(VmmError::InvalidConfig(format!("vm {vm_id} already exists")));
        }
        let mut record = VmRecord::new(spec, VmState::Creating);
        record.vsock_cid = config.guest_cid;
        self.control.register(record);
        match self.vmm.create_vm(config).await {
            Ok(id) => {
                self.commit(&vm_id, VmState::Created);
                Ok(id)
            }
            Err(e) => {
                self.control.remove(&vm_id);
                Err(e)
            }
        }
    }

    pub async fn start(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Start)?;
        match self.vmm.start_vm(vm_id).await {
            Ok(_) => {
                self.commit(vm_id, VmState::Started);
                self.touch_activity(vm_id);
                Ok(())
            }
            Err(e) => {
                self.commit(vm_id, prev);
                Err(e)
            }
        }
    }

    pub async fn pause(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Pause)?;
        match self.vmm.pause_vm(vm_id).await {
            Ok(_) => {
                self.commit(vm_id, VmState::Paused);
                Ok(())
            }
            Err(e) => {
                self.commit(vm_id, prev);
                Err(e)
            }
        }
    }

    pub async fn resume(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Resume)?;
        match self.vmm.resume_vm(vm_id).await {
            Ok(_) => {
                self.commit(vm_id, VmState::Started);
                self.touch_activity(vm_id);
                Ok(())
            }
            Err(e) => {
                self.commit(vm_id, prev);
                Err(e)
            }
        }
    }

    /// Cold-suspend: snapshot a paused VM then tear it down (Paused → Suspended).
    pub async fn suspend(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Suspend)?;
        let snap = match self.vmm.snapshot_vm(vm_id).await {
            Ok(s) => s,
            Err(e) => {
                self.commit(vm_id, prev);
                return Err(e);
            }
        };
        if let Some(rec) = self.control.get(vm_id) {
            let mut g = rec.write().unwrap();
            g.snapshot = Some(snap);
        }
        match self.vmm.destroy_vm(vm_id).await {
            Ok(_) => {
                self.commit(vm_id, VmState::Suspended);
                Ok(())
            }
            Err(e) => {
                self.commit(vm_id, prev);
                Err(e)
            }
        }
    }

    /// Wake a suspended VM (Suspended → Started). Single-flight per VM.
    pub async fn restore(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Restore)?;
        let lock = self.control.restore_lock(vm_id);
        let _guard = lock.lock().await;
        let snap = {
            let rec = self.control.get(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
            let g = rec.read().unwrap();
            g.snapshot.clone().ok_or_else(|| VmmError::SnapshotNotFound(vm_id.clone()))?
        };
        if let Err(e) = self.vmm.restore_vm(&snap).await {
            self.commit(vm_id, prev);
            return Err(e);
        }
        match self.vmm.resume_vm(vm_id).await {
            Ok(_) => {
                self.commit(vm_id, VmState::Started);
                self.touch_activity(vm_id);
                Ok(())
            }
            Err(e) => {
                self.commit(vm_id, prev);
                Err(e)
            }
        }
    }

    /// Wake a VM if it isn't running: `Paused` → resume, `Suspended` → restore.
    /// No-op when already `Started`.
    pub async fn ensure_running(&self, vm_id: &VmId) -> VmmResult<()> {
        match self.state_of(vm_id) {
            Some(VmState::Started) => Ok(()),
            Some(VmState::Paused) => self.resume(vm_id).await,
            Some(VmState::Suspended) => self.restore(vm_id).await,
            Some(s) => Err(VmmError::InvalidState {
                vm_id: vm_id.clone(),
                current: s.as_str(),
                required: "started|paused|suspended",
            }),
            None => Err(VmmError::VmNotFound(vm_id.clone())),
        }
    }

    /// Scale-to-zero convenience: Started → Paused → Suspended.
    pub async fn freeze(&self, vm_id: &VmId) -> VmmResult<()> {
        if self.state_of(vm_id) == Some(VmState::Started) {
            self.pause(vm_id).await?;
        }
        self.suspend(vm_id).await
    }

    /// Tear down a VM from any stable state. Removes the registry record.
    pub async fn destroy(&self, vm_id: &VmId) -> VmmResult<()> {
        let prev = self.begin(vm_id, LifecycleOp::Destroy)?;
        if prev.is_live() {
            self.vmm.destroy_vm(vm_id).await.inspect_err(|_e| {
                self.commit(vm_id, prev);
            })?;
        }
        self.control.remove(vm_id);
        self.persist_delete(vm_id);
        Ok(())
    }

    /// Bump and return the pause epoch (coordinated with the guest scaler loop).
    pub fn bump_pause_epoch(&self, vm_id: &VmId) -> Option<u64> {
        let rec = self.control.get(vm_id)?;
        let mut g = rec.write().unwrap();
        g.pause_epoch += 1;
        Some(g.pause_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{reconcile, SqliteStore};
    use crate::vmm::mock::MockVmm;
    use tikovm_protocol::manifest::WorkloadManifest;

    fn node(tmp: &std::path::Path) -> Node {
        Node::new(
            Arc::new(MockVmm::new(tmp.join("snaps"))),
            Arc::new(Control::new()),
        )
    }

    fn cfg(id: &str) -> VmConfig {
        VmConfig {
            vm_id: id.into(),
            kernel_path: "/k".into(),
            kernel_cmdline: "console=ttyS0".into(),
            rootfs_path: "/r".into(),
            memory_mb: 512,
            vcpus: 2,
            drives: vec![],
            initrd_path: None,
            guest_cid: Some(3),
        }
    }

    fn spec(id: &str) -> VmSpec {
        VmSpec {
            vm_id: id.into(),
            rootfs: tikovm_protocol::vm::RootfsRef { path: "/r".into(), read_only_base: true },
            resources: tikovm_protocol::vm::ResourceConfig::default(),
            kernel: tikovm_protocol::vm::KernelSpec {
                kernel_path: "/k".into(),
                kernel_cmdline: "console=ttyS0".into(),
                initrd_path: None,
            },
            network: tikovm_protocol::vm::NetworkSpec::default(),
            routing: vec![],
            env: Default::default(),
            manifest: Some(WorkloadManifest::empty("echo")),
            schedule: None,
        }
    }

    #[tokio::test]
    async fn full_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let n = node(tmp.path());
        n.create(cfg("vm-1"), spec("vm-1")).await.unwrap();
        assert_eq!(n.state_of(&"vm-1".to_string()), Some(VmState::Created));
        n.start(&"vm-1".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-1".to_string()), Some(VmState::Started));
        n.pause(&"vm-1".to_string()).await.unwrap();
        n.suspend(&"vm-1".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-1".to_string()), Some(VmState::Suspended));
        n.restore(&"vm-1".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-1".to_string()), Some(VmState::Started));
        n.destroy(&"vm-1".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-1".to_string()), None);
    }

    #[tokio::test]
    async fn freeze_and_ensure_running() {
        let tmp = tempfile::tempdir().unwrap();
        let n = node(tmp.path());
        n.create(cfg("vm-2"), spec("vm-2")).await.unwrap();
        n.start(&"vm-2".to_string()).await.unwrap();
        n.freeze(&"vm-2".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-2".to_string()), Some(VmState::Suspended));
        n.ensure_running(&"vm-2".to_string()).await.unwrap();
        assert_eq!(n.state_of(&"vm-2".to_string()), Some(VmState::Started));
    }

    #[tokio::test]
    async fn illegal_suspend_from_started() {
        let tmp = tempfile::tempdir().unwrap();
        let n = node(tmp.path());
        n.create(cfg("vm-3"), spec("vm-3")).await.unwrap();
        n.start(&"vm-3".to_string()).await.unwrap();
        // suspend requires Paused, not Started
        assert!(n.suspend(&"vm-3".to_string()).await.is_err());
        // state unchanged
        assert_eq!(n.state_of(&"vm-3".to_string()), Some(VmState::Started));
    }

    #[tokio::test]
    async fn crash_recovery_write_through_and_reconcile() {
        // Simulate: hostd provisions+suspends a VM with a store, then "crashes"
        // (we drop the Node). On restart, reconcile() rebuilds the registry and
        // a fresh Node can restore the VM.
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("tikovm.db");

        // --- first "process lifetime" ---
        let store = Arc::new(SqliteStore::open(&db).unwrap());
        {
            let n = Node::new(
                Arc::new(MockVmm::new(tmp.path().join("snaps"))),
                Arc::new(Control::new()),
            )
            .with_store(store.clone());
            n.create(cfg("vm-cr"), spec("vm-cr")).await.unwrap();
            n.start(&"vm-cr".to_string()).await.unwrap();
            n.pause(&"vm-cr".to_string()).await.unwrap();
            n.suspend(&"vm-cr".to_string()).await.unwrap();
            assert_eq!(n.state_of(&"vm-cr".to_string()), Some(VmState::Suspended));
            // `n` and the registry drop here — simulating a crash. `store` persists.
        }

        // --- "restart": new registry, reconcile from the store ---
        let new_control = Arc::new(Control::new());
        let recovered = reconcile(&new_control, &*store).unwrap();
        assert_eq!(recovered, 1);
        assert_eq!(
            new_control.get(&"vm-cr".to_string()).unwrap().read().unwrap().state,
            VmState::Suspended
        );

        // A fresh Node on top of the recovered registry can restore it.
        let n2 = Node::new(
            Arc::new(MockVmm::new(tmp.path().join("snaps"))),
            new_control,
        )
        .with_store(store);
        n2.restore(&"vm-cr".to_string()).await.unwrap();
        assert_eq!(n2.state_of(&"vm-cr".to_string()), Some(VmState::Started));
    }
}
