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

use super::backend::ObjectStore;
use crate::error::Result;

// ── S3Sim ─────────────────────────────────────────────────────────────────

/// Local-filesystem simulation of S3 Express + Standard buckets.
#[derive(Debug)]
pub(super) struct S3Sim {
    /// `{DataDir}/tiko/sim/express`
    express_root: PathBuf,
    /// `{DataDir}/tiko/sim/standard`
    standard_root: PathBuf,
}

impl S3Sim {
    /// Create a new `S3Sim` instance with the given root directory.
    pub(super) fn new(root_path: &Path) -> Self {
        let base = root_path.join("sim");
        S3Sim {
            express_root: base.join("express"),
            standard_root: base.join("standard"),
        }
    }
}

// ── ObjectStore impl ──────────────────────────────────────────────────────────

impl ObjectStore for S3Sim {
    fn put_express(&self, key: &str, data: &[u8]) -> Result<()> {
        write_file(&self.express_root.join(key), data)
    }
    fn get_express(&self, key: &str) -> Result<Vec<u8>> {
        read_file(&self.express_root.join(key))
    }
    fn rename_express(&self, src_key: &str, dst_key: &str) -> Result<()> {
        let dst = self.express_root.join(dst_key);
        ensure_parent(&dst)?;
        fs::rename(self.express_root.join(src_key), dst)?;
        Ok(())
    }
    fn delete_express(&self, key: &str) -> Result<()> {
        remove_file(&self.express_root.join(key))
    }
    fn list_prefix_express(&self, prefix: &str) -> Result<Vec<String>> {
        list_under_prefix(&self.express_root, prefix)
    }
    fn put_standard(&self, key: &str, data: &[u8]) -> Result<()> {
        write_file(&self.standard_root.join(key), data)
    }
    fn get_standard(&self, key: &str) -> Result<Vec<u8>> {
        read_file(&self.standard_root.join(key))
    }
    fn delete_standard(&self, key: &str) -> Result<()> {
        remove_file(&self.standard_root.join(key))
    }
    fn remove_dir_standard(&self, prefix: &str) -> Result<()> {
        let path = self.standard_root.join(prefix);
        match fs::remove_dir(&path) {
            Ok(()) => Ok(()),
            Err(e)
                if e.kind() == io::ErrorKind::NotFound
                    || e.kind() == io::ErrorKind::DirectoryNotEmpty =>
            {
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
    fn list_prefix_standard(&self, prefix: &str) -> Result<Vec<String>> {
        list_under_prefix(&self.standard_root, prefix)
    }
    fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> Result<()> {
        let dst = self.standard_root.join(dst_key);
        ensure_parent(&dst)?;
        fs::copy(self.express_root.join(src_key), dst)?;
        Ok(())
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

fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    ensure_parent(path)?;
    let mut f = File::create(path)?;
    if skip_compression(path) {
        f.write_all(data)?;
    } else {
        let compressed =
            zstd::encode_all(data, 1).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        f.write_all(&compressed)?;
    }
    Ok(())
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    let raw = fs::read(path)?;
    if raw.is_empty() || skip_compression(path) {
        Ok(raw)
    } else {
        let data = zstd::decode_all(raw.as_slice())?;
        Ok(data)
    }
}

fn remove_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
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
        } else if let Ok(rel) = entry.path().strip_prefix(root) {
            if let Some(s) = rel.to_str() {
                out.push(s.to_owned());
            }
        }
    }
    Ok(())
}

/// Recursively collect all file paths under `root/prefix`, returning them
/// relative to `root`.
fn list_under_prefix(root: &Path, prefix: &str) -> Result<Vec<String>> {
    let base = root.join(prefix);
    let mut keys = Vec::new();
    collect_files(root, &base, &mut keys)?;
    keys.sort();
    Ok(keys)
}
