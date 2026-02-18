//! S3 block-level read/write operations.
//!
//! Provides `read_blocks()` and `write_blocks()` — synchronous functions that
//! perform actual file I/O. Called from two contexts:
//!
//! 1. **s3worker io_handler** (Tokio): `process_io_request` calls these for
//!    Read/Write slot operations.
//! 2. **Backend during initdb** (sync): called directly when no s3worker exists.
//!
//! Uses S3-style path layout on local filesystem:
//! `{DataDir}/pico/{spc_oid}/{db_oid}/{rel_number}.{fork}`
//!
//! Will be replaced by real S3 GET/PUT operations once the S3 client is added.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use pgsys::common::{BLCKSZ, BlockNumber, DataDir, ForkNumber, Oid, RelFileNumber};

/// Build the local file path for a relation fork.
///
/// Layout: `{DataDir}/pico/{spc_oid}/{db_oid}/{rel_number}.{fork}`
///
/// Mirrors the future S3 key structure:
/// `s3://{bucket}/{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}`
pub fn block_path(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> PathBuf {
    let data_dir = unsafe { std::ffi::CStr::from_ptr(DataDir).to_str().unwrap_or("") };

    PathBuf::from(data_dir)
        .join("pico")
        .join(spc_oid.to_string())
        .join(db_oid.to_string())
        .join(format!("{}.{}", rel_number, fork_number))
}

/// Map `std::io::Error` to a raw errno value.
fn io_err_to_errno(e: &io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

/// Check if a relation fork file exists.
///
/// # Returns
/// - `true` if the file exists
/// - `false` otherwise
pub fn file_exists(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> bool {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);
    path.exists()
}

/// Create a relation fork file. Creates parent directories if needed.
///
/// # Returns
/// - `Ok(false)` if the file already existed
/// - `Ok(true)` if a new file was created
/// - `Err(errno)` on failure
pub fn create_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<bool, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let created = !path.exists();

    OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    Ok(created)
}

/// Get the number of blocks in a relation fork file.
///
/// Unlike `mdnblocks` which iterates across segments, S3 uses a single file
/// per fork — just `file_size / BLCKSZ`. Returns 0 if the file doesn't exist.
///
/// # Returns
/// - `Ok(nblocks)` — number of whole blocks in the file
/// - `Err(errno)` on I/O failure (other than file-not-found)
pub fn file_nblocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    match fs::metadata(&path) {
        Ok(meta) => Ok(meta.len() as u32 / BLCKSZ as u32),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(io_err_to_errno(&e)),
    }
}

/// Read blocks from a relation data file into a buffer.
///
/// Implements retry loop for short reads, matching PostgreSQL's FileReadV
/// behavior. Continues reading until all requested blocks are transferred
/// or EOF/error occurs.
///
/// # Returns
/// - `Ok(nblocks)` on full read
/// - `Ok(partial)` on EOF (fewer blocks than requested)
/// - `Err(errno)` on I/O failure
pub fn read_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *mut u8,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    let file = File::open(&path).map_err(|e| io_err_to_errno(&e))?;

    let mut total_blocks_read = 0u32;
    let mut remaining = nblocks;

    // Retry loop: handle short reads (partial transfers)
    while remaining > 0 {
        let offset = (block_number + total_blocks_read) as u64 * BLCKSZ as u64;
        let bytes_to_read = remaining as usize * BLCKSZ;
        let buf_offset = total_blocks_read as usize * BLCKSZ;
        let buf =
            unsafe { std::slice::from_raw_parts_mut(buffer_ptr.add(buf_offset), bytes_to_read) };

        match file.read_at(buf, offset) {
            Ok(0) => break, // EOF reached
            Ok(bytes_read) => {
                let blocks_read = bytes_read as u32 / BLCKSZ as u32;
                total_blocks_read += blocks_read;
                remaining -= blocks_read;

                // Partial block at EOF — shouldn't happen with aligned I/O,
                // but handle it gracefully (matches md behavior)
                if bytes_read % BLCKSZ != 0 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue, // EINTR: retry
            Err(e) => return Err(io_err_to_errno(&e)),
        }
    }

    Ok(total_blocks_read)
}

/// Write blocks from a buffer to a relation data file.
///
/// Creates parent directories if they don't exist. Uses `write_at` (pwrite),
/// which extends the file and zero-fills gaps if `block_number` is beyond EOF —
/// same semantics as `mdextend`'s `FileWrite`. In the future S3 implementation
/// this becomes a PUT to a per-block key, so extend vs overwrite is irrelevant.
///
/// Implements retry loop for short writes, matching PostgreSQL's FileWriteV
/// behavior. Continues writing until all requested blocks are transferred
/// or an error occurs.
///
/// # Returns
/// - `Ok(nblocks)` on full write
/// - `Err(errno)` on I/O failure (short writes are retried until completion)
pub fn write_blocks(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
    buffer_ptr: *const u8,
) -> Result<BlockNumber, i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    let mut total_blocks_written = 0u32;
    let mut remaining = nblocks;

    // Retry loop: handle short writes (partial transfers)
    while remaining > 0 {
        let offset = (block_number + total_blocks_written) as u64 * BLCKSZ as u64;
        let bytes_to_write = remaining as usize * BLCKSZ;
        let buf_offset = total_blocks_written as usize * BLCKSZ;
        let buf = unsafe { std::slice::from_raw_parts(buffer_ptr.add(buf_offset), bytes_to_write) };

        match file.write_at(buf, offset) {
            Ok(0) => {
                // Short write with 0 bytes written — likely ENOSPC (disk full)
                // Return an error like md does
                return Err(libc::ENOSPC);
            }
            Ok(bytes_written) => {
                let blocks_written = bytes_written as u32 / BLCKSZ as u32;
                total_blocks_written += blocks_written;
                remaining -= blocks_written;

                // Partial block write — shouldn't happen with aligned I/O,
                // but handle it as potential ENOSPC
                if bytes_written % BLCKSZ != 0 && remaining > 0 {
                    return Err(libc::ENOSPC);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue, // EINTR: retry
            Err(e) => return Err(io_err_to_errno(&e)),
        }
    }

    Ok(total_blocks_written)
}

/// Extend a relation fork file with zero-filled blocks.
///
/// Uses `File::set_len()` (ftruncate) to extend the file to
/// `(blocknum + nblocks) * BLCKSZ`. On POSIX, `ftruncate` zero-fills
/// the extended region. Creates the file and parent directories if
/// they don't exist (matching `mdzeroextend`'s `EXTENSION_CREATE`).
///
/// # Returns
/// - `Ok(())` on success
/// - `Err(errno)` on failure
pub fn zeroextend_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    block_number: BlockNumber,
    nblocks: BlockNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err_to_errno(&e))?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io_err_to_errno(&e))?;

    let new_len = (block_number as u64 + nblocks as u64) * BLCKSZ as u64;
    file.set_len(new_len).map_err(|e| io_err_to_errno(&e))
}

/// Truncate a relation fork file to the given number of blocks.
///
/// Uses `File::set_len()` (ftruncate) to shrink the file. If the file
/// doesn't exist, this is a no-op (the relation was already dropped or
/// never created).
///
/// # Returns
/// - `Ok(())` on success or if the file doesn't exist
/// - `Err(errno)` on failure
pub fn truncate_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
    nblocks: BlockNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    let file = match OpenOptions::new().write(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err_to_errno(&e)),
    };

    let new_len = nblocks as u64 * BLCKSZ as u64;
    file.set_len(new_len).map_err(|e| io_err_to_errno(&e))
}

/// Delete a relation fork file.
///
/// Silently ignores ENOENT — the file may not exist (e.g. non-main forks
/// that were never created, or WAL redo replaying a drop).
///
/// # Returns
/// - `Ok(())` on success or if the file doesn't exist
/// - `Err(errno)` on failure
pub fn delete_file(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    fork_number: ForkNumber,
) -> Result<(), i32> {
    let path = block_path(spc_oid, db_oid, rel_number, fork_number);

    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err_to_errno(&e)),
    }
}
