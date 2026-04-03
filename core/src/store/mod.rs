pub mod backend;
pub mod ops;
pub mod s3;
pub mod s3_sim;

use std::io;
use std::path::Path;
use std::sync::OnceLock;

use pgsys::Lsn;

use crate::chunk::ChunkTag;
use crate::project::ProjectNamespace;

use self::backend::ObjectStore;
use self::s3_sim::S3Sim;

// ── Store ─────────────────────────────────────────────────────────────────────

/// Top-level store object.
///
/// Holds a concrete `ObjectStore` backend (`S3Sim` or `S3`) and provides:
/// - The same primitive two-bucket operations via forwarding methods.
/// - Higher-level compound operations (`three_step_write`, `put_express_latest`)
///   built entirely from `ObjectStore` primitives.
/// - A process-global singleton (`init` / `get` / `try_get`).
pub struct Store {
    inner: Box<dyn ObjectStore + Send + Sync>,
}

static STORE: OnceLock<Store> = OnceLock::new();

// SAFETY: `Store` contains a `Box<dyn ObjectStore + Send + Sync>` which is
// already `Send + Sync` by the trait bound.
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Store {
    // ── Constructors ──────────────────────────────────────────────────────

    /// Create a `Store` backed by the local filesystem S3 simulation.
    pub fn new_sim(tiko_root_dir: &Path) -> Self {
        Store {
            inner: Box::new(S3Sim::new(tiko_root_dir)),
        }
    }

    /// Create a `Store` backed by the local sim using the `TIKO_ROOT_PATH`
    /// environment variable.
    ///
    /// # Panics
    /// Panics if `TIKO_ROOT_PATH` is not set.
    pub fn new_sim_from_env() -> Self {
        Store {
            inner: Box::new(S3Sim::new_from_env()),
        }
    }

    // ── Global singleton ──────────────────────────────────────────────────

    /// Initialise the global `Store` with a local sim backend and return a
    /// `'static` reference to it.  Subsequent calls are silently ignored
    /// (OnceLock semantics).
    pub fn init(tiko_root_dir: &Path) -> &'static Self {
        let _ = STORE.set(Self::new_sim(tiko_root_dir));
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
    pub fn try_get() -> Option<&'static Self> {
        STORE.get()
    }

    // ── Primitive forwarding methods ──────────────────────────────────────

    pub fn put_express(&self, key: &str, data: &[u8]) -> io::Result<()> {
        self.inner.put_express(key, data)
    }
    pub fn get_express(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.inner.get_express(key)
    }
    pub fn rename_express(&self, src_key: &str, dst_key: &str) -> io::Result<()> {
        self.inner.rename_express(src_key, dst_key)
    }
    pub fn delete_express(&self, key: &str) -> io::Result<()> {
        self.inner.delete_express(key)
    }
    pub fn list_prefix_express(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list_prefix_express(prefix)
    }
    pub fn put_standard(&self, key: &str, data: &[u8]) -> io::Result<()> {
        self.inner.put_standard(key, data)
    }
    pub fn get_standard(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.inner.get_standard(key)
    }
    pub fn delete_standard(&self, key: &str) -> io::Result<()> {
        self.inner.delete_standard(key)
    }
    pub fn remove_dir_standard(&self, prefix: &str) -> io::Result<()> {
        self.inner.remove_dir_standard(prefix)
    }
    pub fn list_prefix_standard(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list_prefix_standard(prefix)
    }
    pub fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> io::Result<()> {
        self.inner.copy_express_to_standard(src_key, dst_key)
    }
    pub fn put_template(&self, filename: &str, data: &[u8]) -> io::Result<()> {
        self.inner.put_template(filename, data)
    }
    pub fn get_template(&self, filename: &str) -> io::Result<Option<Vec<u8>>> {
        self.inner.get_template(filename)
    }
    pub fn copy_org_data(
        &self,
        src_standard: &Path,
        src_express: &Path,
        src_org_id: u64,
        dst_org_id: u64,
    ) -> io::Result<()> {
        self.inner
            .copy_org_data(src_standard, src_express, src_org_id, dst_org_id)
    }

    // ── Compound operations (built from ObjectStore primitives) ───────────

    /// Three-step checkpoint write:
    /// 1. PUT staging file to express bucket
    /// 2. COPY staging → versioned object in standard bucket
    /// 3. Atomic RENAME staging → `latest` in express bucket
    ///
    /// Used **only** at checkpoint time.  Mid-interval evictions use
    /// [`put_express_latest`] instead.
    pub fn three_step_write(
        &self,
        ns: &ProjectNamespace,
        key: &ChunkTag,
        timeline: u32,
        checkpoint_lsn: Lsn,
        data: &[u8],
    ) -> io::Result<()> {
        let staging = ns.chunk_staging_key(key, checkpoint_lsn);
        let versioned = ns.chunk_versioned_key(key, ns.branch_id, timeline, checkpoint_lsn);
        let latest = ns.chunk_latest_key(key, timeline);
        self.inner.put_express(&staging, data)?;
        self.inner.copy_express_to_standard(&staging, &versioned)?;
        self.inner.rename_express(&staging, &latest)?;
        Ok(())
    }

    /// Eviction write: plain PUT to express-bucket `latest`.
    /// No staging, no standard-bucket copy — those happen at checkpoint.
    pub fn put_express_latest(
        &self,
        ns: &ProjectNamespace,
        key: &ChunkTag,
        timeline: u32,
        data: &[u8],
    ) -> io::Result<()> {
        self.inner
            .put_express(&ns.chunk_latest_key(key, timeline), data)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::Lsn;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Store) {
        let dir = TempDir::new().unwrap();
        let store = Store::new_sim(dir.path());
        (dir, store)
    }

    fn ns() -> ProjectNamespace {
        ProjectNamespace::new(1, 42, 7)
    }

    fn chunk_tag() -> ChunkTag {
        ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 1000,
            fork_number: 0,
            chunk_id: 3,
        }
    }

    // ── put_express_latest ────────────────────────────────────────────────

    #[test]
    fn put_express_latest_writes_only_latest() {
        let (_dir, store) = setup();
        let ns = ns();
        let tag = chunk_tag();
        let lsn = Lsn::new(0x100);

        store
            .put_express_latest(&ns, &tag, 1, b"chunk-data")
            .unwrap();

        let latest_key = ns.chunk_latest_key(&tag, 1);
        assert_eq!(
            store.get_express(&latest_key).unwrap(),
            Some(b"chunk-data".to_vec())
        );

        // no staging file
        let staging_key = ns.chunk_staging_key(&tag, lsn);
        assert_eq!(store.get_express(&staging_key).unwrap(), None);

        // no versioned object in standard
        let versioned_key = ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn);
        assert_eq!(store.get_standard(&versioned_key).unwrap(), None);
    }

    // ── three_step_write ──────────────────────────────────────────────────

    #[test]
    fn three_step_write_full_success() {
        let (_dir, store) = setup();
        let ns = ns();
        let tag = chunk_tag();
        let lsn = Lsn::new(0x200);

        store
            .three_step_write(&ns, &tag, 1, lsn, b"block-data")
            .unwrap();

        assert_eq!(
            store.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            Some(b"block-data".to_vec())
        );
        assert_eq!(
            store
                .get_standard(&ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn))
                .unwrap(),
            Some(b"block-data".to_vec())
        );
        assert_eq!(
            store.get_express(&ns.chunk_staging_key(&tag, lsn)).unwrap(),
            None
        );
    }

    #[test]
    fn three_step_crash_after_step1_old_latest_unchanged() {
        let (_dir, store) = setup();
        let ns = ns();
        let tag = chunk_tag();
        let lsn = Lsn::new(0x300);

        store.put_express_latest(&ns, &tag, 1, b"old-data").unwrap();

        let staging = ns.chunk_staging_key(&tag, lsn);
        store.put_express(&staging, b"new-data").unwrap();

        assert_eq!(
            store.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            Some(b"old-data".to_vec())
        );
        assert_eq!(
            store
                .get_standard(&ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn))
                .unwrap(),
            None
        );
    }

    #[test]
    fn three_step_crash_after_step2_old_latest_unchanged_versioned_valid() {
        let (_dir, store) = setup();
        let ns = ns();
        let tag = chunk_tag();
        let lsn = Lsn::new(0x400);

        store.put_express_latest(&ns, &tag, 1, b"old-data").unwrap();

        let staging = ns.chunk_staging_key(&tag, lsn);
        store.put_express(&staging, b"new-data").unwrap();
        store
            .copy_express_to_standard(
                &staging,
                &ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn),
            )
            .unwrap();

        assert_eq!(
            store.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            Some(b"old-data".to_vec())
        );
        assert_eq!(
            store
                .get_standard(&ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn))
                .unwrap(),
            Some(b"new-data".to_vec())
        );
    }
}
