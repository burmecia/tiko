//! Persistent volume registry: `<data-dir>/registry.json`.
//!
//! Maps `vol_id` to its metadata, including the **reserved** ublk device id.
//! A volume keeps the same `dev_id` for its whole lifetime (even across
//! daemon restarts and detach/attach cycles) so its `/dev/ublkbN` node is
//! stable — required later for Firecracker snapshot restore, which records
//! drive paths in the snapshot.
//!
//! Writes are atomic: serialize to a temp file in the same directory, fsync,
//! then rename over the target.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Lifecycle state of a registered volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeState {
    /// Registered, backing file exists, no live ublk device.
    Created,
    /// ublk device exists and is (or should be) served by this daemon.
    Attached,
}

/// Storage engine backing a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    /// Phase-1 loop file (default for backward compatibility).
    #[default]
    File,
    /// Phase-2 chunked engine (chunkstore on the S3 Files mount + NVMe
    /// journal/read-cache).
    Chunk,
}

/// Persistent per-volume metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMeta {
    /// Caller-chosen volume id.
    pub vol_id: String,
    /// Reserved ublk device id (stable across restarts).
    pub dev_id: u32,
    /// Device size in bytes.
    pub size_bytes: u64,
    /// Backing path: loop file for `file`, store volume dir for `chunk`.
    pub backing_path: PathBuf,
    /// Current state.
    pub state: VolumeState,
    /// Storage engine.
    #[serde(default)]
    pub backend: BackendKind,
    /// Chunk size in bytes (chunk backend only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RegistryFile {
    volumes: BTreeMap<String, VolumeMeta>,
}

/// The on-disk registry plus its in-memory view.
pub struct Registry {
    path: PathBuf,
    volumes: BTreeMap<String, VolumeMeta>,
}

impl Registry {
    /// Load from `path`; a missing file means an empty registry.
    pub fn load(path: &Path) -> Result<Self> {
        let volumes = match std::fs::read(path) {
            Ok(bytes) => {
                let f: RegistryFile = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::InvalidInput(format!("corrupt registry.json: {e}")))?;
                f.volumes
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path: path.to_path_buf(),
            volumes,
        })
    }

    /// Atomically persist the current view (tmp file + fsync + rename).
    pub fn save(&self) -> Result<()> {
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&RegistryFile {
            volumes: self.volumes.clone(),
        })
        .map_err(|e| Error::InvalidInput(format!("registry serialize: {e}")))?;
        std::fs::write(&tmp, &bytes)?;
        // fsync the temp file before renaming it into place.
        let f = std::fs::File::open(&tmp)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Allocate the lowest free device id >= 1.
    ///
    /// Id 0 is deliberately never used: on the original bring-up host it is
    /// poisoned by a wedged daemon until reboot, and avoiding 0 costs nothing.
    pub fn alloc_dev_id(&self) -> u32 {
        let mut id = 1u32;
        while self.volumes.values().any(|v| v.dev_id == id) {
            id += 1;
        }
        id
    }

    /// Insert (or replace) a volume entry.
    pub fn insert(&mut self, meta: VolumeMeta) {
        self.volumes.insert(meta.vol_id.clone(), meta);
    }

    /// Remove and return a volume entry.
    pub fn remove(&mut self, vol_id: &str) -> Option<VolumeMeta> {
        self.volumes.remove(vol_id)
    }

    /// Look up a volume.
    pub fn get(&self, vol_id: &str) -> Option<&VolumeMeta> {
        self.volumes.get(vol_id)
    }

    /// Look up a volume mutably (caller must [`Registry::save`]).
    pub fn get_mut(&mut self, vol_id: &str) -> Option<&mut VolumeMeta> {
        self.volumes.get_mut(vol_id)
    }

    /// All volumes, ordered by id.
    pub fn list(&self) -> Vec<VolumeMeta> {
        self.volumes.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_registry(tag: &str) -> (PathBuf, Registry) {
        let dir = std::env::temp_dir().join(format!("tikoblk-reg-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.json");
        let reg = Registry::load(&path).unwrap();
        (dir, reg)
    }

    fn meta(vol_id: &str, dev_id: u32) -> VolumeMeta {
        VolumeMeta {
            vol_id: vol_id.to_string(),
            dev_id,
            size_bytes: 1 << 20,
            backing_path: PathBuf::from(format!("/tmp/{vol_id}.img")),
            state: VolumeState::Created,
            backend: BackendKind::File,
            chunk_size: None,
        }
    }

    #[test]
    fn phase1_registry_without_backend_fields_still_loads() {
        let dir = std::env::temp_dir().join(format!("tikoblk-reg-compat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.json");
        std::fs::write(
            &path,
            r#"{"volumes":{"v1":{"vol_id":"v1","dev_id":1,"size_bytes":1048576,"backing_path":"/x/v1.img","state":"created"}}}"#,
        )
        .unwrap();
        let reg = Registry::load(&path).unwrap();
        let m = reg.get("v1").unwrap();
        assert_eq!(m.backend, BackendKind::File);
        assert_eq!(m.chunk_size, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn roundtrip_and_atomic_write() {
        let (dir, mut reg) = tmp_registry("rt");
        reg.insert(meta("a", 1));
        reg.insert(meta("b", 2));
        reg.save().unwrap();

        // No temp file left behind; the target parses back identically.
        assert!(!dir.join("registry.json.tmp").exists());
        let reg2 = Registry::load(&dir.join("registry.json")).unwrap();
        let list = reg2.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].vol_id, "a");
        assert_eq!(list[1].dev_id, 2);

        // Second save over an existing file also works (rename replaces).
        reg2.save().unwrap();
        let reg3 = Registry::load(&dir.join("registry.json")).unwrap();
        assert_eq!(reg3.list().len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_is_empty_registry() {
        let dir = std::env::temp_dir().join(format!("tikoblk-reg-miss-{}", std::process::id()));
        let reg = Registry::load(&dir.join("nope.json")).unwrap();
        assert!(reg.list().is_empty());
    }

    #[test]
    fn dev_id_allocation_lowest_free_and_never_zero() {
        let (dir, mut reg) = tmp_registry("alloc");
        assert_eq!(reg.alloc_dev_id(), 1);
        reg.insert(meta("a", reg.alloc_dev_id()));
        reg.insert(meta("b", reg.alloc_dev_id()));
        reg.insert(meta("c", reg.alloc_dev_id()));
        assert_eq!(reg.get("a").unwrap().dev_id, 1);
        assert_eq!(reg.get("b").unwrap().dev_id, 2);
        assert_eq!(reg.get("c").unwrap().dev_id, 3);

        // Holes are reused: remove dev 2, next alloc gets 2 back.
        reg.remove("b");
        assert_eq!(reg.alloc_dev_id(), 2);
        // Id 0 is never handed out even when the map is empty.
        reg.remove("a");
        reg.remove("c");
        assert_eq!(reg.alloc_dev_id(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }
}
