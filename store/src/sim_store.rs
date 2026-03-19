//! S3 simulation store — local filesystem backend.
//!
//! Mirrors S3 Express One Zone (hot mutable objects) and Standard S3
//! (versioned immutable objects) using the local filesystem under
//! `{DataDir}/tiko/sim/`. The key structure is identical to the real S3
//! layout, so switching to `aws-sdk-s3` later is a drop-in replacement
//! of this file only.
//!
//! # Key conventions
//!
//! All keys are relative to the bucket root (express or standard). Callers
//! must not include a leading `/`. `list_prefix_*` returns keys relative
//! to the same root, so the returned strings can be passed directly back
//! to `get_*`/`delete_*`.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use pgsys::Lsn;

use crate::chunk::ChunkTag;
use crate::project::ProjectNamespace;

// ── Globals ───────────────────────────────────────────────────────────────────

/// Sim store initialised by `SimStore::init` at s3worker startup.
/// Accessed from `cached_read_blocks` via `try_fetch_chunk_from_s3`.
pub(crate) static SIM_STORE: OnceLock<SimStore> = OnceLock::new();

// ── SimStore ─────────────────────────────────────────────────────────────────

/// Local-filesystem simulation of S3 Express + Standard buckets.
#[derive(Debug)]
pub struct SimStore {
    /// `{DataDir}/tiko/sim/express`
    express_root: PathBuf,
    /// `{DataDir}/tiko/sim/standard`
    standard_root: PathBuf,
}

impl SimStore {
    /// Initialise the sim store.
    ///
    /// Must be called once from `s3worker_main()` before `ProjectCtx::load()`.
    /// Subsequent calls are silently ignored (OnceLock semantics).
    pub fn init(tiko_root_dir: &Path) -> &'static Self {
        let _ = SIM_STORE.set(SimStore::new(tiko_root_dir));
        Self::get()
    }

    /// Return the global `SimStore`.
    ///
    /// # Panics
    /// Panics if `SimStore::init` has not been called.
    pub fn get() -> &'static Self {
        SIM_STORE
            .get()
            .expect("SimStore::get() called before SimStore::init()")
    }

    /// Return the global `SimStore`, or `None` if not yet initialised.
    pub fn try_get() -> Option<&'static Self> {
        SIM_STORE.get()
    }

    /// Create a new `SimStore` instance with the given root directory.
    pub fn new(tiko_root_dir: &Path) -> Self {
        let base = tiko_root_dir.join("sim");
        SimStore {
            express_root: base.join("express"),
            standard_root: base.join("standard"),
        }
    }

    /// Create a new `SimStore` instance using the `TIKO_ROOT_PATH` environment variable.
    ///
    /// # Panics
    /// Panics if `TIKO_ROOT_PATH` is not set.
    pub fn new_from_env() -> Self {
        let root = std::env::var(crate::ENV_TIKO_ROOT_PATH)
            .expect("TIKO_ROOT_PATH environment variable is not set");
        Self::new(Path::new(&root))
    }

    // ── Primitive helpers ─────────────────────────────────────────────────

    pub fn put_express(&self, key: &str, data: &[u8]) -> io::Result<()> {
        write_file(&self.express_root.join(key), data)
    }

    pub fn put_standard(&self, key: &str, data: &[u8]) -> io::Result<()> {
        write_file(&self.standard_root.join(key), data)
    }

    /// Returns `None` if the key does not exist.
    pub fn get_express(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        read_optional(&self.express_root.join(key))
    }

    /// Returns `None` if the key does not exist.
    pub fn get_standard(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        read_optional(&self.standard_root.join(key))
    }

    /// Copy an express-bucket object to the standard bucket.
    pub fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> io::Result<()> {
        let dst = self.standard_root.join(dst_key);
        ensure_parent(&dst)?;
        fs::copy(self.express_root.join(src_key), dst)?;
        Ok(())
    }

    /// Atomically rename within the express bucket.
    /// Equivalent to S3 Express `RenameObject` — atomic on POSIX filesystems.
    pub fn rename_express(&self, src_key: &str, dst_key: &str) -> io::Result<()> {
        let dst = self.express_root.join(dst_key);
        ensure_parent(&dst)?;
        fs::rename(self.express_root.join(src_key), dst)
    }

    /// Delete from the express bucket; silently succeeds if key is absent.
    pub fn delete_express(&self, key: &str) -> io::Result<()> {
        remove_optional(&self.express_root.join(key))
    }

    /// Delete from the standard bucket; silently succeeds if key is absent.
    pub fn delete_standard(&self, key: &str) -> io::Result<()> {
        remove_optional(&self.standard_root.join(key))
    }

    // ── Template helpers ──────────────────────────────────────────────────

    fn template_key(&self, filename: &str) -> PathBuf {
        self.standard_root.join("template").join(filename)
    }

    /// Store a PGDATA template tarball in the standard bucket at `template/{filename}`.
    pub fn put_template(&self, filename: &str, data: &[u8]) -> io::Result<()> {
        write_file(&self.template_key(filename), data)
    }

    /// Retrieve a PGDATA template tarball from the standard bucket.
    /// Returns `None` if not found.
    pub fn get_template(&self, filename: &str) -> io::Result<Option<Vec<u8>>> {
        read_optional(&self.template_key(filename))
    }

    /// Copy all objects from `src_standard` and `src_express` into this SimStore,
    /// rewriting the leading org component from `src_org_id` to `dst_org_id`.
    ///
    /// Files are copied at the raw filesystem level (preserving on-disk encoding)
    /// rather than going through the put/get layer to avoid double-compression.
    ///
    /// Used by `create_org` to seed a new org from a template's embedded SimStore.
    pub fn copy_org_data(
        &self,
        src_standard: &Path,
        src_express: &Path,
        src_org_id: u64,
        dst_org_id: u64,
    ) -> io::Result<()> {
        let prefix = src_org_id.to_string();
        copy_rekey(src_standard, &self.standard_root, &prefix, dst_org_id)?;
        copy_rekey(src_express, &self.express_root, &prefix, dst_org_id)?;
        Ok(())
    }

    /// List all keys in the express bucket that start with `prefix`.
    /// Returns keys relative to the express root.
    pub fn list_prefix_express(&self, prefix: &str) -> io::Result<Vec<String>> {
        list_under_prefix(&self.express_root, prefix)
    }

    /// List all keys in the standard bucket that start with `prefix`.
    /// Returns keys relative to the standard root.
    pub fn list_prefix_standard(&self, prefix: &str) -> io::Result<Vec<String>> {
        list_under_prefix(&self.standard_root, prefix)
    }

    // ── Compound operations ───────────────────────────────────────────────

    /// Three-step checkpoint write:
    /// 1. PUT staging file to express bucket
    /// 2. COPY staging → versioned object in standard bucket
    /// 3. Atomic RENAME staging → `latest` in express bucket
    ///
    /// Used **only** at checkpoint time. Mid-interval evictions use
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
        self.put_express(&staging, data)?;
        self.copy_express_to_standard(&staging, &versioned)?;
        self.rename_express(&staging, &latest)?;
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
        self.put_express(&ns.chunk_latest_key(key, timeline), data)
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn ensure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    ensure_parent(path)?;
    let mut f = File::create(path)?;
    if is_json_file(path) {
        f.write_all(data)
    } else {
        let compressed =
            zstd::encode_all(data, 1).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        f.write_all(&compressed)
    }
}

fn read_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let raw = match fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if is_json_file(path) {
        Ok(Some(raw))
    } else {
        let data = zstd::decode_all(raw.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Some(data))
    }
}

fn is_json_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
}

fn remove_optional(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Recursively collect all file paths under `root/prefix`, returning them
/// relative to `root`.
fn list_under_prefix(root: &Path, prefix: &str) -> io::Result<Vec<String>> {
    let base = root.join(prefix);
    let mut keys = Vec::new();
    collect_files(root, &base, &mut keys)?;
    keys.sort();
    Ok(keys)
}

/// Walk all files under `src_bucket/{src_org_prefix}/`, and copy each one to
/// `dst_bucket/{dst_org_id}/{relative_path}` using raw `fs::copy` to preserve
/// the on-disk encoding (zstd-compressed for non-JSON files).
fn copy_rekey(
    src_bucket: &Path,
    dst_bucket: &Path,
    src_org_prefix: &str,
    dst_org_id: u64,
) -> io::Result<()> {
    let src_dir = src_bucket.join(src_org_prefix);
    if !src_dir.exists() {
        return Ok(());
    }
    let mut files = Vec::new();
    collect_files(&src_dir, &src_dir, &mut files)?;
    for rel in files {
        let src_path = src_dir.join(&rel);
        let dst_path = dst_bucket.join(dst_org_id.to_string()).join(&rel);
        ensure_parent(&dst_path)?;
        fs::copy(&src_path, &dst_path)?;
    }
    Ok(())
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) -> io::Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_files(root, &entry.path(), out)?;
        } else if let Ok(rel) = entry.path().strip_prefix(root) {
            if let Some(s) = rel.to_str() {
                out.push(s.to_owned());
            }
        }
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ProjectNamespace;
    use pgsys::Lsn;
    use tempfile::TempDir;

    fn setup() -> (TempDir, SimStore) {
        let dir = TempDir::new().unwrap();
        let store = SimStore::new(dir.path());
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

    // ── PUT / GET round-trips ─────────────────────────────────────────────

    #[test]
    fn put_get_express_round_trip() {
        let (_dir, store) = setup();
        store.put_express("a/b/c", b"hello").unwrap();
        assert_eq!(store.get_express("a/b/c").unwrap(), Some(b"hello".to_vec()));
    }

    #[test]
    fn put_get_standard_round_trip() {
        let (_dir, store) = setup();
        store.put_standard("x/y/z", b"world").unwrap();
        assert_eq!(
            store.get_standard("x/y/z").unwrap(),
            Some(b"world".to_vec())
        );
    }

    #[test]
    fn put_get_json_standard_round_trip_without_compression() {
        let (_dir, store) = setup();
        let key = "1/metadata/42/project.json";
        let payload = br#"{"project_id":42,"status":"active"}"#;

        store.put_standard(key, payload).unwrap();

        assert_eq!(store.get_standard(key).unwrap(), Some(payload.to_vec()));

        let raw_on_disk = fs::read(store.standard_root.join(key)).unwrap();
        assert_eq!(raw_on_disk, payload);
    }

    #[test]
    fn get_missing_returns_none() {
        let (_dir, store) = setup();
        assert_eq!(store.get_express("does/not/exist").unwrap(), None);
        assert_eq!(store.get_standard("does/not/exist").unwrap(), None);
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

        // latest exists
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

        // latest in express
        assert_eq!(
            store.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            Some(b"block-data".to_vec())
        );
        // versioned in standard
        assert_eq!(
            store
                .get_standard(&ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn))
                .unwrap(),
            Some(b"block-data".to_vec())
        );
        // staging cleaned up
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

        // Simulate pre-existing latest
        store.put_express_latest(&ns, &tag, 1, b"old-data").unwrap();

        // Step 1 only: staging written
        let staging = ns.chunk_staging_key(&tag, lsn);
        store.put_express(&staging, b"new-data").unwrap();

        // latest unchanged, no versioned object
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

        // Steps 1 + 2: staging + versioned written, rename not done
        let staging = ns.chunk_staging_key(&tag, lsn);
        store.put_express(&staging, b"new-data").unwrap();
        store
            .copy_express_to_standard(
                &staging,
                &ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn),
            )
            .unwrap();

        // latest still old
        assert_eq!(
            store.get_express(&ns.chunk_latest_key(&tag, 1)).unwrap(),
            Some(b"old-data".to_vec())
        );
        // versioned is valid
        assert_eq!(
            store
                .get_standard(&ns.chunk_versioned_key(&tag, ns.branch_id, 1, lsn))
                .unwrap(),
            Some(b"new-data".to_vec())
        );
    }

    // ── Key prefix formatting ─────────────────────────────────────────────

    #[test]
    fn key_format_chunk_latest() {
        let ns = ProjectNamespace::new(1, 42, 7);
        let tag = chunk_tag();
        assert_eq!(
            ns.chunk_latest_key(&tag, 1),
            "1/42/chunks/1663/5/1000.0/3/00000001/latest"
        );
    }

    #[test]
    fn key_format_chunk_staging() {
        let ns = ProjectNamespace::new(1, 42, 7);
        let tag = chunk_tag();
        assert_eq!(
            ns.chunk_staging_key(&tag, Lsn::new(0x3A000028)),
            "1/42/chunks/1663/5/1000.0/3/.staging_000000003A000028"
        );
    }

    #[test]
    fn key_format_chunk_versioned_uses_branch_id_not_project_id() {
        let ns = ProjectNamespace::new(1, 42, 7);
        let tag = chunk_tag();
        // branch_id=7, project_id=42 — key must use 7; timeline=1 (00000001)
        let key = ns.chunk_versioned_key(&tag, ns.branch_id, 1, Lsn::new(0x100));
        assert!(key.contains("/7/"), "expected branch_id 7 in key: {key}");
        assert!(
            !key.contains("/42/chunks"),
            "must not use project_id in versioned key"
        );
        assert_eq!(key, "1/chunks/7/1663/5/1000.0/3/00000001/0000000000000100");
    }

    #[test]
    fn key_format_delta_manifest() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(
            ns.delta_manifest_key(1, Lsn::new(0x200)),
            "1/pitr/42/deltas/00000001/0000000000000200/manifest.bin"
        );
    }

    #[test]
    fn key_format_base_manifest() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(
            ns.base_manifest_key(1, Lsn::new(0x100)),
            "1/pitr/42/bases/00000001/0000000000000100/manifest.bin"
        );
    }

    #[test]
    fn key_format_pg_state() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(
            ns.pg_state_key(1, Lsn::new(0x300)),
            "1/pitr/42/deltas/00000001/0000000000000300/pg_state.tar.zst"
        );
    }

    #[test]
    fn key_format_wal() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(
            ns.wal_key(1, "000000010000000000000001"),
            "1/pitr/42/wal/00000001/000000010000000000000001"
        );
    }

    #[test]
    fn key_format_project_meta() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(ns.project_meta_key(), "1/metadata/42/project.json");
    }

    #[test]
    fn key_format_prefixes() {
        let ns = ProjectNamespace::new(1, 42, 7);
        assert_eq!(ns.delta_prefix(), "1/pitr/42/deltas/");
        assert_eq!(ns.base_prefix(), "1/pitr/42/bases/");
    }

    // ── list_prefix_standard ──────────────────────────────────────────────

    #[test]
    fn list_prefix_standard_returns_correct_subset() {
        let (_dir, store) = setup();
        store.put_standard("a/b/c", b"1").unwrap();
        store.put_standard("a/b/d", b"2").unwrap();
        store.put_standard("a/x/e", b"3").unwrap();

        let keys = store.list_prefix_standard("a/b/").unwrap();
        assert_eq!(keys, vec!["a/b/c", "a/b/d"]);

        let all = store.list_prefix_standard("a/").unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn list_prefix_standard_empty_prefix_dir_returns_empty() {
        let (_dir, store) = setup();
        let keys = store.list_prefix_standard("no/such/prefix/").unwrap();
        assert!(keys.is_empty());
    }

    // ── delete ────────────────────────────────────────────────────────────

    #[test]
    fn delete_missing_key_is_noop() {
        let (_dir, store) = setup();
        store.delete_express("no/such/key").unwrap();
        store.delete_standard("no/such/key").unwrap();
    }
}
