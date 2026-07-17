//! An in-memory `Vmm` implementation for tests and dev. Simulates the full
//! lifecycle state machine (including snapshot/restore) without any hypervisor.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tikovm_protocol::vm::VmId;

use super::{BackendState, DriveConfig, Snapshot, VmConfig, Vmm, VmmError, VmmResult};

#[derive(Debug)]
struct MockVm {
    state: BackendState,
    config: VmConfig,
    snapshot: Option<Snapshot>,
}

/// A fully in-memory backend. Created VMs start in `Created`; `snapshot_vm`
/// requires `Paused` and records a fake on-disk snapshot descriptor.
#[derive(Default)]
pub struct MockVmm {
    inner: Arc<Mutex<HashMap<VmId, MockVm>>>,
    snapshot_dir: PathBuf,
}

impl MockVmm {
    pub fn new(snapshot_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            snapshot_dir,
        }
    }

    fn snapshot_paths(&self, vm_id: &VmId) -> (PathBuf, PathBuf) {
        (
            self.snapshot_dir.join(format!("{vm_id}.snapshot")),
            self.snapshot_dir.join(format!("{vm_id}.mem")),
        )
    }
}

#[async_trait]
impl Vmm for MockVmm {
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId> {
        let vm_id = config.vm_id.clone();
        let mut map = self.inner.lock().unwrap();
        if map.contains_key(&vm_id) {
            return Err(VmmError::InvalidConfig(format!("vm {vm_id} already exists")));
        }
        map.insert(vm_id.clone(), MockVm { state: BackendState::Created, config, snapshot: None });
        Ok(vm_id)
    }

    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut map = self.inner.lock().unwrap();
        let vm = map.get_mut(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
        vm.state = BackendState::Started;
        Ok(())
    }

    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut map = self.inner.lock().unwrap();
        let vm = map.get_mut(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
        if vm.state != BackendState::Started {
            return Err(VmmError::InvalidState {
                vm_id: vm_id.clone(),
                current: "not started",
                required: "started",
            });
        }
        vm.state = BackendState::Paused;
        Ok(())
    }

    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut map = self.inner.lock().unwrap();
        let vm = map.get_mut(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
        if vm.state != BackendState::Paused {
            return Err(VmmError::InvalidState {
                vm_id: vm_id.clone(),
                current: "not paused",
                required: "paused",
            });
        }
        vm.state = BackendState::Started;
        Ok(())
    }

    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        let mut map = self.inner.lock().unwrap();
        let vm = map.get_mut(vm_id).ok_or(VmmError::VmNotFound(vm_id.clone()))?;
        if vm.state != BackendState::Paused {
            return Err(VmmError::InvalidState {
                vm_id: vm_id.clone(),
                current: "not paused",
                required: "paused",
            });
        }
        let (state_path, mem_path) = self.snapshot_paths(vm_id);
        let snap = Snapshot {
            vm_id: vm_id.clone(),
            state_path,
            mem_path,
            config: vm.config.clone(),
        };
        vm.snapshot = Some(snap.clone());
        Ok(snap)
    }

    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId> {
        let vm_id = snapshot.vm_id.clone();
        let mut map = self.inner.lock().unwrap();
        map.insert(
            vm_id.clone(),
            MockVm {
                state: BackendState::Paused,
                config: snapshot.config.clone(),
                snapshot: Some(snapshot.clone()),
            },
        );
        Ok(vm_id)
    }

    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        let mut map = self.inner.lock().unwrap();
        map.remove(vm_id);
        Ok(())
    }

    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<BackendState> {
        let map = self.inner.lock().unwrap();
        map.get(vm_id).map(|v| v.state).ok_or(VmmError::VmNotFound(vm_id.clone()))
    }

    async fn vm_guest_ip(&self, _vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        Ok(None)
    }

    async fn list_vms(&self) -> VmmResult<Vec<(VmId, BackendState)>> {
        let map = self.inner.lock().unwrap();
        Ok(map.iter().map(|(id, v)| (id.clone(), v.state)).collect())
    }
}

/// A drive spec useful in tests.
pub fn test_drive(id: &str, path: &str) -> DriveConfig {
    DriveConfig {
        drive_id: id.into(),
        path: path.into(),
        read_only: false,
        size_mb: None,
        tier: tikovm_protocol::volume::VolumeTier::LocalFast,
        source: None,
    }
}
