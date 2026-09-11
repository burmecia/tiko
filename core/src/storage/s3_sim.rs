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

use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::ObjectStorage;
use crate::error::Result;

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

// ── S3Sim ─────────────────────────────────────────────────────────────────

/// Local-filesystem simulation of S3 bucket.
#[derive(Debug)]
pub(super) struct S3Sim {
    /// `{DataDir}/tiko/s3sim`
    root: PathBuf,
}

impl S3Sim {
    /// Create a new `S3Sim` instance with the given root directory.
    pub(super) fn new(root_path: &Path) -> Self {
        let root = root_path.join("s3sim");
        S3Sim { root }
    }
}

// ── ObjectStorage impl ──────────────────────────────────────────────────────────

impl ObjectStorage for S3Sim {
    // Stage under `.tmp/` (outside every listed key prefix, same filesystem)
    // and rename, so readers never observe a partial object; fsync file and
    // directory so a power loss can't leave one either.
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.root.join(key);
        ensure_parent(&path)?;
        let tmp = self.root.join(".tmp").join(format!(
            "{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        ensure_parent(&tmp)?;
        let result = (|| {
            let mut f = File::create(&tmp)?;
            if skip_compression(&path) {
                f.write_all(data)?;
            } else {
                let compressed = zstd::encode_all(data, 1).map_err(io::Error::other)?;
                f.write_all(&compressed)?;
            }
            f.sync_all()?;
            fs::rename(&tmp, &path)?;
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.root.join(key);
        let raw = fs::read(&path)?;
        decode_object(&path, raw)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.root.join(key);
        if path.is_dir() {
            match fs::remove_dir_all(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        } else {
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let base = self.root.join(prefix);
        let mut keys = Vec::new();
        collect_files(&self.root, &base, &mut keys)?;
        keys.sort();
        Ok(keys)
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Returns true for file types that should be stored as-is without zstd compression:
/// - `.json` — human-readable metadata, small, already uncompressed
/// - `.zst` — already compressed archives
fn skip_compression(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json") || ext.eq_ignore_ascii_case("zst"))
}

/// zstd frame magic (little-endian), as emitted by `zstd::encode_all`.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Decode an object's stored bytes using the canonical encoding rule: `.json`
/// and `.zst` objects (and empty files) are stored uncompressed, everything
/// else is a zstd frame. Corrupt compressed data is an error.
///
/// Shared by `S3Sim::get` and the inspector CLIs so the storage format stays
/// single-sourced.
pub fn decode_object(path: &Path, raw: Vec<u8>) -> Result<Vec<u8>> {
    if raw.is_empty() || skip_compression(path) {
        Ok(raw)
    } else {
        Ok(zstd::decode_all(raw.as_slice())?)
    }
}

/// Like [`decode_object`], but tolerates an uncompressed object whose extension
/// is not in the skip list (e.g. an inspector reading a decompressed `.segment`
/// or an object written by a backend that doesn't compress). A non-zstd frame
/// is returned as-is; a zstd frame is always decompressed, so corruption still
/// surfaces as an error.
pub fn decode_object_autodetect(path: &Path, raw: Vec<u8>) -> Result<Vec<u8>> {
    if raw.is_empty() || skip_compression(path) || !raw.starts_with(&ZSTD_MAGIC) {
        Ok(raw)
    } else {
        Ok(zstd::decode_all(raw.as_slice())?)
    }
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_files(root, &entry.path(), out)?;
        } else if let Ok(rel) = entry.path().strip_prefix(root)
            && let Some(s) = rel.to_str()
        {
            out.push(s.to_owned());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_object_round_trips_and_autodetects_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("00000000.segment");
        let raw = b"not compressed WAL metadata".to_vec();
        let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();

        // A zstd frame decodes under both rules.
        assert_eq!(decode_object(&seg, compressed.clone()).unwrap(), raw);
        assert_eq!(decode_object_autodetect(&seg, compressed).unwrap(), raw);

        // Plain bytes on a non-skip extension: strict errors, autodetect passes.
        assert!(decode_object(&seg, raw.clone()).is_err());
        assert_eq!(decode_object_autodetect(&seg, raw.clone()).unwrap(), raw);

        // Skip-compression extensions and empty inputs are raw either way.
        let json = dir.path().join("meta.json");
        assert_eq!(decode_object(&json, raw.clone()).unwrap(), raw);
        assert_eq!(decode_object_autodetect(&json, raw.clone()).unwrap(), raw);
        assert_eq!(decode_object(&seg, Vec::new()).unwrap(), Vec::<u8>::new());
    }
}
