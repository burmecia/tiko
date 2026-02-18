// pgsys/src/aio.rs
//! PostgreSQL AIO (Async I/O) FFI bindings

use crate::common::{BlockNumber, ForkNumber};
use crate::smgr::{PgAioHandle, SMgrRelationData};
use std::ffi::c_int;

/// POSIX iovec — matches `struct iovec` from <sys/uio.h>.
#[repr(C)]
pub struct IoVec {
    pub iov_base: *mut std::ffi::c_void,
    pub iov_len: usize,
}

// PgAioHandleCallbackID values (from aio.h enum)
pub const PGAIO_HCB_MD_READV: c_int = 1;

// PgAioHandleFlags values (from aio.h enum)
pub const PGAIO_HF_BUFFERED: c_int = 1 << 2;

unsafe extern "C" {
    /// Get the iovec array from a PgAioHandle (in shared memory).
    /// Returns max iov count; sets *iov to point into shared memory.
    pub fn pgaio_io_get_iovec(ioh: *mut PgAioHandle, iov: *mut *mut IoVec) -> c_int;

    /// Register completion callbacks on the IO handle.
    pub fn pgaio_io_register_callbacks(ioh: *mut PgAioHandle, cb_id: c_int, cb_data: u8);

    /// Set the SMGR target on an IO handle (stores relation identity for reopen + callbacks).
    pub fn pgaio_io_set_target_smgr(
        ioh: *mut PgAioHandle,
        smgr: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: c_int,
        skip_fsync: bool,
    );

    /// Set a flag on the IO handle.
    pub fn pgaio_io_set_flag(ioh: *mut PgAioHandle, flag: c_int);

    /// Stage the IO as PGAIO_OP_S3_READV. Only needs iovcnt (no fd/offset).
    pub fn pgaio_io_start_s3_readv(ioh: *mut PgAioHandle, iovcnt: c_int);

    /// Stage the IO as PGAIO_OP_S3_WRITEV. Only needs iovcnt (no fd/offset).
    pub fn pgaio_io_start_s3_writev(ioh: *mut PgAioHandle, iovcnt: c_int);
}
