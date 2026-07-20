//! Pluggable block backends.
//!
//! The ublk serve loop talks to a [`BlockBackend`]; Phase 1 provides
//! [`FileBackend`] (a loop file under `<data-dir>/backing/<vol_id>.img`).
//! Phase 2's chunked S3 Files engine implements the same trait.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// A random-access block device backing store.
///
/// Implementations must be thread-safe: queue threads issue calls
/// concurrently (Phase 1 uses a single queue, but the trait does not
/// guarantee that).
pub trait BlockBackend: Send + Sync {
    /// Read exactly `buf.len()` bytes at byte offset `off`.
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// Write exactly `buf.len()` bytes at byte offset `off`.
    fn write_at(&self, off: u64, buf: &[u8]) -> io::Result<usize>;
    /// Durably flush all prior writes (fsync semantics).
    fn flush(&self) -> io::Result<()>;
    /// Device size in bytes.
    fn size(&self) -> u64;
}

/// Loop-file backend: `pread`/`pwrite`/`fsync` on one backing file.
///
/// The file is fully preallocated at creation time so a guest write can
/// never hit host `ENOSPC` mid-I/O (which surfaces as guest write errors).
pub struct FileBackend {
    file: File,
    path: PathBuf,
    size: u64,
}

impl FileBackend {
    /// Create a new backing file of `size` bytes, fully preallocated.
    ///
    /// Fails with [`io::ErrorKind::AlreadyExists`] if the file exists.
    pub fn create(path: &Path, size: u64) -> io::Result<Self> {
        use std::os::unix::io::AsRawFd;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        // Preallocate so host ENOSPC cannot surface as guest write errors.
        let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            let _ = std::fs::remove_file(path);
            return Err(err);
        }
        file.sync_all()?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            size,
        })
    }

    /// Open an existing backing file. The recorded size is the file length.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            file,
            path: path.to_path_buf(),
            size,
        })
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl BlockBackend for FileBackend {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read_exact_at(buf, off)?;
        Ok(buf.len())
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all_at(buf, off)?;
        Ok(buf.len())
    }

    fn flush(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_backend_read_write_flush() {
        let dir = std::env::temp_dir().join(format!("tikoblk-be-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.img");

        let be = FileBackend::create(&path, 1 << 20).unwrap();
        assert_eq!(be.size(), 1 << 20);

        let data = vec![0xABu8; 4096];
        assert_eq!(be.write_at(8192, &data).unwrap(), 4096);
        let mut out = vec![0u8; 4096];
        assert_eq!(be.read_at(8192, &mut out).unwrap(), 4096);
        assert_eq!(out, data);
        be.flush().unwrap();

        // Reopen sees the same data and size.
        drop(be);
        let be = FileBackend::open(&path).unwrap();
        assert_eq!(be.size(), 1 << 20);
        let mut out = vec![0u8; 4096];
        be.read_at(8192, &mut out).unwrap();
        assert_eq!(out, data);

        // create_new on an existing file fails.
        assert!(FileBackend::create(&path, 1 << 20).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_backend_is_actually_preallocated() {
        let dir = std::env::temp_dir().join(format!("tikoblk-be2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("v.img");
        let _be = FileBackend::create(&path, 4 << 20).unwrap();
        let blocks = std::fs::metadata(&path).unwrap().len();
        assert_eq!(blocks, 4 << 20);
        drop(_be);
        std::fs::remove_dir_all(&dir).ok();
    }
}
