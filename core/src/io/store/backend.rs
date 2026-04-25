//! `ObjectStore` trait — abstraction for two-bucket object storage.
//!
//! Both the local filesystem simulator (`S3Sim`) and the remote S3 backend
//! (`S3`) implement this trait.  `Store` holds one concrete implementation
//! and delegates all primitive I/O through these methods.

use crate::error::Result;

/// Two-bucket object storage abstraction.
///
/// Mirrors the S3 Express One Zone (hot mutable, express bucket) +
/// Standard S3 (versioned immutable, standard bucket) structure.
/// All keys are bucket-relative without a leading `/`.
pub trait ObjectStore {
    // ── Express bucket ────────────────────────────────────────────────────

    fn put_express(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Returns `None` if the key does not exist.
    fn get_express(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Atomically rename within the express bucket.
    /// Equivalent to S3 Express `RenameObject` — atomic on POSIX filesystems.
    fn rename_express(&self, src_key: &str, dst_key: &str) -> Result<()>;

    /// Delete from the express bucket; silently succeeds if key is absent.
    fn delete_express(&self, key: &str) -> Result<()>;

    /// List all keys in the express bucket that start with `prefix`.
    /// Returns keys relative to the bucket root.
    fn list_prefix_express(&self, prefix: &str) -> Result<Vec<String>>;

    // ── Standard bucket ───────────────────────────────────────────────────

    fn put_standard(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Returns `None` if the key does not exist.
    fn get_standard(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Delete from the standard bucket; silently succeeds if key is absent.
    fn delete_standard(&self, key: &str) -> Result<()>;

    /// Remove an empty directory under the standard bucket.
    /// Silently succeeds if the directory does not exist or is not empty.
    fn remove_dir_standard(&self, prefix: &str) -> Result<()>;

    /// List all keys in the standard bucket that start with `prefix`.
    /// Returns keys relative to the bucket root.
    fn list_prefix_standard(&self, prefix: &str) -> Result<Vec<String>>;

    // ── Cross-bucket ──────────────────────────────────────────────────────

    /// Copy an express-bucket object to the standard bucket.
    fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> Result<()>;
}
