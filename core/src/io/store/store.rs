use std::path::Path;
use std::sync::OnceLock;

use super::{backend::ObjectStore, s3_sim::S3Sim};
use crate::{
    chunk::{CHUNK_SIZE, ChunkTag, RelFork},
    db::{DbMeta, DbNamespace},
    error::{Error, Result},
    io_control::IoControl,
    manifest::Manifest,
    relfork::RelForkMeta,
    tiko_root_path,
};
use pgsys::Lsn;
use pgsys::common::{BLCKSZ, BlockNumber};

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
    db: DbMeta,
    base_manifest: Manifest,
}

impl Store {
    /// Create a `Store` backed by the local filesystem S3 simulation.
    fn new(root_path: &Path, ns: DbNamespace) -> Self {
        Store {
            backend: Box::new(S3Sim::new(root_path)),
            db: DbMeta::new(ns),
            base_manifest: Manifest::empty(&root_path.join("base_manifest")).unwrap(),
        }
    }

    pub fn do_checkpoint(&self, lsn: Lsn) -> Result<()> {
        self.db.set_checkpoint_lsn(lsn);
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

    // ── RelFork meta operations ──────────────────────────────────────────────────

    pub(crate) fn get_meta(&self, rf: &RelFork) -> Result<RelForkMeta> {
        let key = self.db.relfork_meta_key(rf);
        let result = self.get_express(&key)?;
        match result {
            Some(bytes) => {
                let meta = serde_json::from_slice::<RelForkMeta>(&bytes)?;
                Ok(meta)
            }
            None => match self.base_manifest.lookup_relfork_meta(rf) {
                Some(meta) => Ok(meta),
                None => Err(Error::not_found("relfork not found")),
            },
        }
    }

    pub(crate) fn put_meta(&self, rf: &RelFork, meta: &RelForkMeta) -> Result<()> {
        let key = self.db.relfork_meta_key(rf);
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

        let key = self.db.relfork_chunk_key(tag);
        let result = self.get_express(&key)?;
        match result {
            Some(src) => {
                dst.copy_from_slice(&src);
                Ok(())
            }
            None => {
                let chunk_ref = self.base_manifest.lookup(tag)?;
                if let Some(chunk_ref) = chunk_ref {
                    let key = self.db.chunk_key_standard(tag, &chunk_ref);
                    if let Some(src) = self.get_standard(&key)? {
                        dst.copy_from_slice(&src);
                        Ok(())
                    } else {
                        Err(Error::not_found("chunk not found in store"))
                    }
                } else {
                    Err(Error::not_found("chunk not found in store"))
                }
            }
        }
    }

    pub(crate) fn patch_chunk(&self, tag: &ChunkTag, block_offset: u32, data: &[u8]) -> Result<()> {
        debug_assert!(!data.is_empty());
        debug_assert_eq!(data.len() % BLCKSZ, 0);
        let byte_offset = block_offset as usize * BLCKSZ;
        debug_assert!(byte_offset + data.len() <= CHUNK_SIZE);
        let is_full_chunk = byte_offset == 0 && data.len() == CHUNK_SIZE;
        let key = self.db.relfork_chunk_key(tag);

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

    pub fn get_express(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let result = self.backend.get_express(key)?;
        if let Some(ref data) = result {
            IoControl::get().stats.store_express.inc_gets(data.len());
        } else {
            IoControl::get().stats.store_express.inc_gets(0);
        }
        Ok(result)
    }

    pub fn put_express(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_express(key, data)?;
        IoControl::get().stats.store_express.inc_puts(data.len());
        Ok(())
    }

    pub fn get_standard(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let result = self.backend.get_standard(key)?;
        if let Some(ref data) = result {
            IoControl::get().stats.store_standard.inc_gets(data.len());
        } else {
            IoControl::get().stats.store_standard.inc_gets(0);
        }
        Ok(result)
    }

    pub fn put_standard(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put_standard(key, data)?;
        IoControl::get().stats.store_standard.inc_puts(data.len());
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
