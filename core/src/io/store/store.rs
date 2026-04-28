use std::path::Path;
use std::sync::{Mutex, OnceLock};

use super::{backend::ObjectStore, locator::Locator, s3_sim::S3Sim};
use crate::{
    checkpoint_history::{CheckpointHistory, CheckpointVersion},
    chunk::{CHUNK_SIZE, ChunkTag, RelFork},
    db::{DbMeta, DbNamespace},
    error::{Error, Result},
    io::checkpoint_history::CkptHistSnapshot,
    io_control::IoControl,
    manifest::Manifest,
    relfork::RelForkMeta,
    tiko_root_path,
};
use pgsys::{
    common::{BLCKSZ, BlockNumber},
    logging::pg_log_debug1,
    lsn::Lsn,
};

// ── Store ─────────────────────────────────────────────────────────────────────

static STORE: OnceLock<Store> = OnceLock::new();

/// Top-level store object.
///
/// Holds a concrete `ObjectStore` backend (`S3Sim` or `S3`) and provides:
/// - The same primitive two-bucket operations via forwarding methods.
///   built entirely from `ObjectStore` primitives.
/// - A process-global singleton (`init` / `get` / `try_get`).
pub struct Store {
    backend: Box<dyn ObjectStore + Send + Sync>,
    ns: DbNamespace,
    loc: Locator,
    base_manifest: Manifest,
    /// Per-backend cache of the shared checkpoint version list.
    /// `Mutex` satisfies the `Sync` bound required by `static STORE`.
    /// Only the owning backend process acquires this lock; it is never
    /// contended within a single process.
    ckpt_hist: Mutex<CkptHistSnapshot>,
}

impl Store {
    /// Create a `Store` backed by the local filesystem S3 simulation.
    fn new(root_path: &Path, ns: DbNamespace) -> Self {
        Store {
            backend: Box::new(S3Sim::new(root_path)),
            ns: ns.clone(),
            loc: Locator::new(ns.clone()),
            base_manifest: Manifest::empty(&root_path.join("base_manifest")).unwrap(),
            ckpt_hist: Mutex::new(CkptHistSnapshot::default()),
        }
    }

    /// Scan the express bucket for existing checkpoint folders and populate
    /// `target` with them, oldest-first, so the ring
    /// buffer ends up with the newest entry at the logical top.
    ///
    /// Key structure: `{ns}/chunks/{tl}/{lsn_hex}/…`
    /// We extract the `{tl}` and `{lsn_hex}` segments (indices 2 and 3 of
    /// the `/`-split, relative to `ns`).
    ///
    /// Called once from `Store::init()` after `IoControl` has been
    /// initialised, so `IoControl::is_initialized()` is guaranteed true.
    pub(crate) fn load_checkpoint_history(&self, target: &mut CheckpointHistory) {
        let prefix = format!("{}/chunks/", self.ns);

        let keys = match self.backend.list_prefix_express(&prefix) {
            Ok(k) => k,
            Err(_) => return, // storage not reachable; history starts empty
        };

        // Collect unique (tl, lsn) pairs from `{ns}/chunks/{tl}/{lsn_hex}/…`
        let mut versions: Vec<(u32, Lsn)> = {
            use std::collections::BTreeMap;
            let mut seen: BTreeMap<u64, (u32, Lsn)> = BTreeMap::new();
            for key in &keys {
                // Strip the namespace prefix; remaining: `chunks/{tl}/{lsn_hex}/…`
                let rel = key.strip_prefix(&prefix).unwrap_or(key.as_str());
                let mut parts = rel.splitn(3, '/');
                let tl_str = match parts.next() {
                    Some(s) => s,
                    None => continue,
                };
                let lsn_hex = match parts.next() {
                    Some(s) => s,
                    None => continue,
                };
                let tl: u32 = match tl_str.parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let lsn: Lsn = match Lsn::from_hex(lsn_hex) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // Deduplicate by raw LSN value; BTreeMap keeps insertion order by key.
                seen.entry(lsn.into()).or_insert((tl, lsn));
            }
            seen.into_values().collect()
        };

        // Sort oldest-first so successive push() calls build up newest-at-top.
        versions.sort_by_key(|(_, lsn)| *lsn);

        // Push each version into the target history.
        for (tl, lsn) in &versions {
            target.push(*tl, *lsn);
        }

        pg_log_debug1(format!(
            "tiko: load_checkpoint_history loaded {} versions: {:?}",
            versions.len(),
            versions
        ));
    }

    pub fn perform_checkpoint(&self, timeline_id: u32, lsn: Lsn) -> Result<()> {
        let db = DbMeta::new(self.ns.clone());
        let key = self.loc.db_meta();

        // Load existing DbMeta if it exists.
        match self.get_express(&[key.clone()]) {
            Ok(json_bytes) => db.load_from_json_bytes(&json_bytes),
            Err(err) if err.is_not_found() => {} // no existing meta; treat as default
            Err(err) => return Err(err),
        }

        db.set_checkpoint_lsn(timeline_id, lsn);

        // Write DbMeta json file
        let json_bytes = db.to_json_bytes();
        self.put_express(&key, &json_bytes)?;

        // Append to the shared versioned history so all backends can find
        // chunks written in this checkpoint via the express-bucket read path.
        if let Some(io_control) = IoControl::try_get() {
            io_control.ckpt_hist.push(timeline_id, lsn);
        }

        Ok(())
    }

    // ── Global singleton ──────────────────────────────────────────────────

    /// Initialise the global `Store` with a local sim backend and return a
    /// `'static` reference to it. Subsequent calls are silently ignored
    /// (OnceLock semantics).
    pub fn init() -> &'static Self {
        let root_path = tiko_root_path();
        let ns = DbNamespace::new_from_env();
        let _ = STORE.set(Self::new(&root_path, ns));
        Self::get()
    }

    /// Return a `'static` reference to the global `Store`.
    ///
    /// # Panics
    /// Panics if `Store::init` has not been called.
    pub fn get() -> &'static Self {
        STORE
            .get()
            .expect("Store::get() called before Store::init()")
    }

    /// Return the global `Store`, or `None` if not yet initialised.
    pub fn try_get() -> Result<&'static Self> {
        STORE.get().ok_or_else(|| Error::StoreNotAvailable)
    }

    // ── Version-scanning helpers ──────────────────────────────────────────────

    /// Core helper: iterate checkpoint versions newest-first and map each to
    /// an S3 key string via `f(namespace, timeline_id, lsn_hex)`.
    ///
    /// When `IoControl` is not yet initialised (unit tests, initdb), falls back
    /// to a single key derived from `DbMeta`'s current checkpoint LSN.
    fn versioned_keys<F>(&self, f: F) -> Vec<String>
    where
        F: Fn(&[CheckpointVersion]) -> Vec<String>,
    {
        if IoControl::is_initialized() {
            let shared_ckpt_hist = &IoControl::get().ckpt_hist;
            let mut ckpt_hist = self.ckpt_hist.lock().unwrap();
            let versions = ckpt_hist.get_or_refresh(shared_ckpt_hist);
            if versions.is_empty() {
                f(&[CheckpointVersion::default()])
            } else {
                f(versions)
            }
        } else {
            // Fallback: default checkpoint version (no shared memory or empty history).
            f(&[CheckpointVersion::default()])
        }
    }

    // ── RelFork meta operations ──────────────────────────────────────────────────

    pub(crate) fn get_meta(&self, rf: &RelFork) -> Result<RelForkMeta> {
        // Build the list of express keys to probe, newest checkpoint first.
        let keys = self.versioned_keys(|versions: &[CheckpointVersion]| {
            self.loc.relfork_meta_versioned(rf, versions)
        });
        match self.get_express(&keys) {
            Ok(bytes) => {
                let meta = serde_json::from_slice::<RelForkMeta>(&bytes)?;
                Ok(meta)
            }
            Err(err) if err.is_not_found() => match self.base_manifest.lookup_relfork_meta(rf) {
                Some(meta) => Ok(meta),
                None => Err(Error::not_found("relfork not found")),
            },
            Err(err) => Err(err),
        }
    }

    pub(crate) fn put_meta(&self, rf: &RelFork, meta: &RelForkMeta) -> Result<()> {
        // Always write to the current checkpoint version.
        let keys = self.versioned_keys(|versions: &[CheckpointVersion]| {
            debug_assert!(
                !versions.is_empty(),
                "put_meta requires at least one checkpoint version"
            );
            self.loc.relfork_meta_versioned(rf, &versions[..1])
        });
        let key = keys.first().cloned().unwrap();
        let json_bytes = meta.to_json_bytes();
        self.put_express(&key, &json_bytes)
    }

    pub(crate) fn get_nblocks(&self, rf: &RelFork) -> Result<BlockNumber> {
        let meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        Ok(meta.nblocks)
    }

    pub(crate) fn put_nblocks(&self, rf: &RelFork, nblocks: BlockNumber) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.nblocks = nblocks;
        self.put_meta(rf, &meta)
    }

    pub(crate) fn get_deleted(&self, rf: &RelFork) -> Result<bool> {
        let meta = self.get_meta(rf)?;
        Ok(meta.deleted)
    }

    pub(crate) fn create_relfork(&self, rf: &RelFork) -> Result<()> {
        match self.get_meta(rf) {
            Ok(meta) => {
                if !meta.deleted {
                    return Err(Error::already_exists("relfork already exists"));
                }
                self.put_meta(rf, &RelForkMeta::default())
            }
            Err(err) if err.is_not_found() => self.put_meta(rf, &RelForkMeta::default()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn delete_relfork(&self, rf: &RelFork) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.deleted = true;
        self.put_meta(rf, &meta)
    }

    // ── Chunk operations ──────────────────────────────────────────────────

    pub(crate) fn get_chunk(&self, tag: &ChunkTag, dst: &mut [u8]) -> Result<()> {
        debug_assert_eq!(dst.len(), CHUNK_SIZE);

        let keys = self.versioned_keys(|versions: &[CheckpointVersion]| {
            self.loc.chunk_versioned(tag, versions)
        });
        match self.get_express(&keys) {
            Ok(src) => {
                dst.copy_from_slice(&src);
                Ok(())
            }
            Err(err) if err.is_not_found() => {
                let chunk_ref = self.base_manifest.lookup(tag)?;
                if let Some(chunk_ref) = chunk_ref {
                    let key = self.loc.chunk_base(tag, &chunk_ref);
                    let src = self.get_standard(&key)?;
                    dst.copy_from_slice(&src);
                    Ok(())
                } else {
                    Err(Error::not_found("chunk not found in store"))
                }
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn patch_chunk(&self, tag: &ChunkTag, block_offset: u32, data: &[u8]) -> Result<()> {
        debug_assert!(!data.is_empty());
        debug_assert_eq!(data.len() % BLCKSZ, 0);
        let byte_offset = block_offset as usize * BLCKSZ;
        debug_assert!(byte_offset + data.len() <= CHUNK_SIZE);
        let is_full_chunk = byte_offset == 0 && data.len() == CHUNK_SIZE;

        let keys = self.versioned_keys(|versions: &[CheckpointVersion]| {
            debug_assert!(
                !versions.is_empty(),
                "patch_chunk requires at least one checkpoint version"
            );
            self.loc.chunk_versioned(tag, &versions[..1])
        });
        let key = keys.first().cloned().unwrap();

        if is_full_chunk {
            self.put_express(&key, data)
        } else {
            let mut merged = vec![0u8; CHUNK_SIZE];
            match self.get_chunk(tag, &mut merged) {
                Ok(()) => {}
                Err(e) if e.is_not_found() => {} // chunk absent → treat as zeros
                Err(e) => return Err(e),
            }
            merged[byte_offset..byte_offset + data.len()].copy_from_slice(data);
            self.put_express(&key, &merged)
        }
    }

    // ── Primitive forwarding methods ──────────────────────────────────────

    pub fn get_express(&self, keys: &[String]) -> Result<Vec<u8>> {
        for key in keys {
            match self.backend.get_express(key) {
                Ok(data) => {
                    IoControl::try_get().map(|io_control| {
                        io_control.stats.store_express.inc_gets(data.len());
                    });
                    return Ok(data);
                }
                Err(err) if err.is_not_found() => {
                    IoControl::try_get().map(|io_control| {
                        io_control.stats.store_express.inc_gets(0);
                    });
                    // try the next key in next loop iteration
                }
                Err(err) => return Err(err),
            }
        }
        Err(Error::not_found("not found in express bucket"))
    }

    pub fn put_express(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_express(key, data)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.store_express.inc_puts(data.len());
        });
        Ok(())
    }

    pub fn get_standard(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.backend.get_standard(key)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.store_standard.inc_gets(data.len());
        });
        Ok(data)
    }

    pub fn put_standard(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_standard(key, data)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.store_standard.inc_puts(data.len());
        });
        Ok(())
    }

    // ------ to retire from below -------

    /// Create a `Store` backed by the local filesystem S3 simulation.
    /// Intended for tests — production code should use `Store::init`.
    pub fn new_sim(root_dir: &Path) -> Self {
        Self::new(root_dir, DbNamespace::new(0, 0, 0))
    }

    // ── Primitive forwarding methods ──────────────────────────────────────

    pub fn rename_express(&self, src_key: &str, dst_key: &str) -> Result<()> {
        self.backend.rename_express(src_key, dst_key)
    }
    pub fn delete_express(&self, key: &str) -> Result<()> {
        self.backend.delete_express(key)
    }
    pub fn list_prefix_express(&self, prefix: &str) -> Result<Vec<String>> {
        self.backend.list_prefix_express(prefix)
    }

    pub fn delete_standard(&self, key: &str) -> Result<()> {
        self.backend.delete_standard(key)
    }
    pub fn remove_dir_standard(&self, prefix: &str) -> Result<()> {
        self.backend.remove_dir_standard(prefix)
    }
    pub fn list_prefix_standard(&self, prefix: &str) -> Result<Vec<String>> {
        self.backend.list_prefix_standard(prefix)
    }
    pub fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> Result<()> {
        self.backend.copy_express_to_standard(src_key, dst_key)
    }
}
