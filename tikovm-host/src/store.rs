//! Durable state store + crash recovery (design §14).
//!
//! The DB is the durable source of truth; the in-memory [`Control`] registry is
//! the hot read path and is reconstructed from the DB on boot. [`Node`] writes
//! through on every stable state change. [`reconcile`] is the boot-time recovery
//! pass that applies the **restore-on-demand** policy: a VM that was live at
//! crash time is collapsed to `Suspended` (from its last snapshot) and lazily
//! restored on next access.
//!
//! The current backend is **SQLite** (`rusqlite`, bundled). [`StateStore`] is the
//! generic seam for a future Postgres/etcd impl (multi-node).

use std::net::IpAddr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tikovm_protocol::vm::{VmSpec, VmState};

use crate::control::{Control, VmRecord};
use crate::vmm::Snapshot;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Serializable view of a [`VmRecord`]. Timestamps are epoch millis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedVmRecord {
    pub vm_id: String,
    pub spec: VmSpec,
    pub state: VmState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Snapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_ip: Option<IpAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vsock_cid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity_ms: Option<i64>,
    #[serde(default)]
    pub pause_epoch: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl PersistedVmRecord {
    pub fn from_record(rec: &VmRecord) -> Self {
        Self {
            vm_id: rec.spec.vm_id.clone(),
            spec: rec.spec.clone(),
            state: rec.state,
            snapshot: rec.snapshot.clone(),
            guest_ip: rec.guest_ip,
            vsock_cid: rec.vsock_cid,
            last_activity_ms: rec.last_activity.map(to_ms),
            pause_epoch: rec.pause_epoch,
            created_at_ms: to_ms(rec.created_at),
            updated_at_ms: to_ms(rec.updated_at),
        }
    }

    pub fn to_record(&self) -> VmRecord {
        let mut r = VmRecord::new(self.spec.clone(), self.state);
        r.snapshot = self.snapshot.clone();
        r.guest_ip = self.guest_ip;
        r.vsock_cid = self.vsock_cid;
        r.last_activity = self.last_activity_ms.map(from_ms);
        r.pause_epoch = self.pause_epoch;
        r.created_at = from_ms(self.created_at_ms);
        r.updated_at = from_ms(self.updated_at_ms);
        r
    }
}

fn to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn from_ms(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

/// Generic durable store. Implementations are synchronous; SQLite calls are
/// brief and local. (For very high throughput, wrap in `spawn_blocking`.)
pub trait StateStore: Send + Sync {
    fn upsert(&self, rec: &PersistedVmRecord) -> StoreResult<()>;
    fn get(&self, vm_id: &str) -> StoreResult<Option<PersistedVmRecord>>;
    fn list(&self) -> StoreResult<Vec<PersistedVmRecord>>;
    fn delete(&self, vm_id: &str) -> StoreResult<()>;
}

/// SQLite-backed store. Each record is stored as a JSON blob keyed by `vm_id`.
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (or create) the store at `path`. Creates the schema if absent.
    pub fn open(path: &Path) -> StoreResult<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS vms (
                vm_id TEXT PRIMARY KEY,
                json  TEXT NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// In-memory store (useful for tests).
    pub fn in_memory() -> StoreResult<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS vms (
                vm_id TEXT PRIMARY KEY,
                json  TEXT NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl StateStore for SqliteStore {
    fn upsert(&self, rec: &PersistedVmRecord) -> StoreResult<()> {
        let json = serde_json::to_string(rec)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO vms (vm_id, json) VALUES (?1, ?2)",
            rusqlite::params![rec.vm_id, json],
        )?;
        Ok(())
    }

    fn get(&self, vm_id: &str) -> StoreResult<Option<PersistedVmRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT json FROM vms WHERE vm_id = ?1")?;
        let mut rows = stmt.query(rusqlite::params![vm_id])?;
        if let Some(row) = rows.next()? {
            let json: String = row.get(0)?;
            return Ok(Some(serde_json::from_str(&json)?));
        }
        Ok(None)
    }

    fn list(&self) -> StoreResult<Vec<PersistedVmRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT json FROM vms")?;
        let rows = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        })?;
        let mut out = Vec::new();
        for r in rows {
            let json = r?;
            out.push(serde_json::from_str(&json)?);
        }
        Ok(out)
    }

    fn delete(&self, vm_id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM vms WHERE vm_id = ?1", rusqlite::params![vm_id])?;
        Ok(())
    }
}

/// Boot-time reconciliation (design §14). Loads all records from the store into
/// the registry and applies the **restore-on-demand** policy: a VM that was
/// `Started`/`Paused` at crash time is assumed dead (its Firecracker child did
/// not survive) and collapsed to `Suspended` if a snapshot exists (so it can be
/// lazily restored on next access), or dropped if no snapshot exists.
///
/// Returns the number of records recovered.
pub fn reconcile(control: &Control, store: &dyn StateStore) -> StoreResult<usize> {
    let records = store.list()?;
    let mut n = 0;
    for prec in records {
        let mut rec = prec.to_record();
        match rec.state {
            VmState::Started | VmState::Paused => {
                if rec.snapshot.is_some() {
                    rec.state = VmState::Suspended;
                } else {
                    // No snapshot to restore from: the VM is lost.
                    tracing::warn!(
                        vm_id = %rec.spec.vm_id,
                        "live VM had no snapshot at crash; dropping"
                    );
                    continue;
                }
            }
            // Created/Suspended/Destroyed and all transitional: keep as-is.
            _ => {}
        }
        let vm_id = rec.spec.vm_id.clone();
        control.register(rec);
        // Persist the normalized state back.
        if let Some(updated) = control.get(&vm_id) {
            let g = updated.read().unwrap();
            let _ = store.upsert(&PersistedVmRecord::from_record(&g));
        }
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikovm_protocol::manifest::WorkloadManifest;

    fn spec(id: &str) -> VmSpec {
        VmSpec {
            vm_id: id.into(),
            rootfs: tikovm_protocol::vm::RootfsRef {
                path: "/r".into(),
                read_only_base: true,
            },
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

    #[test]
    fn upsert_get_list_delete() {
        let store = SqliteStore::in_memory().unwrap();
        let rec = VmRecord::new(spec("vm-1"), VmState::Created);
        let prec = PersistedVmRecord::from_record(&rec);
        store.upsert(&prec).unwrap();
        assert!(store.get("vm-1").unwrap().is_some());
        assert_eq!(store.list().unwrap().len(), 1);
        store.delete("vm-1").unwrap();
        assert!(store.get("vm-1").unwrap().is_none());
    }

    #[test]
    fn round_trip_preserves_state() {
        let store = SqliteStore::in_memory().unwrap();
        let mut rec = VmRecord::new(spec("vm-2"), VmState::Suspended);
        rec.pause_epoch = 7;
        store.upsert(&PersistedVmRecord::from_record(&rec)).unwrap();
        let back = store.get("vm-2").unwrap().unwrap().to_record();
        assert_eq!(back.state, VmState::Suspended);
        assert_eq!(back.pause_epoch, 7);
    }

    #[test]
    fn reconcile_collapses_live_to_suspended() {
        let store = SqliteStore::in_memory().unwrap();
        let control = Control::new();

        // A "live at crash" VM with a snapshot.
        let mut rec = VmRecord::new(spec("vm-3"), VmState::Started);
        rec.snapshot = Some(Snapshot {
            vm_id: "vm-3".into(),
            state_path: "/s".into(),
            mem_path: "/m".into(),
            config: crate::vmm::VmConfig {
                vm_id: "vm-3".into(),
                kernel_path: "/k".into(),
                kernel_cmdline: "".into(),
                rootfs_path: "/r".into(),
                memory_mb: 512,
                vcpus: 2,
                drives: vec![],
                initrd_path: None,
                guest_cid: Some(3),
            },
        });
        store.upsert(&PersistedVmRecord::from_record(&rec)).unwrap();

        let n = reconcile(&control, &store).unwrap();
        assert_eq!(n, 1);
        let recovered = control.get(&"vm-3".to_string()).unwrap();
        assert_eq!(recovered.read().unwrap().state, VmState::Suspended);
    }
}
