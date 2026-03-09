use std::ffi::{CStr, c_char, c_int};
use std::path::{Path, PathBuf};

// pid_t is typically a signed integer type in C, so we use i32 here
pub type Pid = i32;

pub type Datum = u64;

// Define the types that match PostgreSQL's C types
pub type BlockNumber = u32;
pub type ForkNumber = i32;
pub type RelFileNumber = u32; // typedef Oid RelFileNumber in PostgreSQL
pub type Oid = u32; // typedef unsigned int Oid

pub const INVALID_BLOCK_NUMBER: BlockNumber = 0xFFFFFFFF;
pub const MAX_BLOCK_NUMBER: BlockNumber = 0xFFFFFFFE;

// Fork number constants (relpath.h)
pub const INVALID_FORK_NUMBER: ForkNumber = -1;
pub const MAIN_FORKNUM: ForkNumber = 0;
pub const MAX_FORKNUM: ForkNumber = 3; // INIT_FORKNUM

pub const PG_VERSION_NUM: c_int = 180001; // in src/include/pg_config.h
pub const MAXPGPATH: usize = 1024;
pub const FUNC_MAX_ARGS: c_int = 100;
pub const INDEX_MAX_KEYS: c_int = 32;
pub const NAMEDATALEN: c_int = 64;
pub const FLOAT8PASSBYVAL: c_int = 1;

pub const BLCKSZ: usize = 8192;

/// Maximum number of I/O worker processes (proc.h: MAX_IO_WORKERS).
/// Compile-time upper bound; actual count is controlled by the `io_workers` GUC.
pub const MAX_IO_WORKERS: c_int = 32;

/// Number of auxiliary process slots in PG18 (proc.h: NUM_AUXILIARY_PROCS).
/// = 6 traditional (Startup, BgWriter, Checkpointer, WalWriter, WalReceiver, WalSummarizer)
///   + MAX_IO_WORKERS (up to 32 I/O worker processes).
/// These have ProcNumbers from MaxBackends to MaxBackends + NUM_AUXILIARY_PROCS - 1.
pub const NUM_AUXILIARY_PROCS: c_int = 6 + MAX_IO_WORKERS;

/// PostgreSQL ProcNumber type (typedef int ProcNumber).
///
/// This is a dense index into the PGPROC array (0..MaxBackends+NUM_AUXILIARY_PROCS-1),
/// NOT the OS process ID. Assigned when a backend attaches to shared
/// memory and recycled when it exits.
pub type ProcNumber = c_int;

// Signal constants
pub const SIGTERM: c_int = 15;
pub const SIGHUP: c_int = 1;
pub const SIGINT: c_int = 2;
pub const SIGALRM: c_int = 14;
pub const SIGUSR1: c_int = 10;
pub const SIGUSR2: c_int = 12;

/// PostgreSQL processing mode (miscadmin.h).
/// Process-local global — no shared memory access needed.
pub type ProcessingMode = c_int;
pub const BOOTSTRAP_PROCESSING: ProcessingMode = 0;
pub const INIT_PROCESSING: ProcessingMode = 1;
pub const NORMAL_PROCESSING: ProcessingMode = 2;

unsafe extern "C" {
    pub static MyProcNumber: ProcNumber;

    /// OS process ID of the current backend (pid_t, set at process start)
    pub static MyProcPid: Pid;

    /// Maximum number of backends (set during postmaster startup, available at shmem hook time)
    pub static MaxBackends: c_int;

    // PostgreSQL's data directory path (global variable)
    pub static DataDir: *const c_char;

    /// Current processing mode (miscadmin.h: extern ProcessingMode Mode).
    /// BootstrapProcessing during initdb --boot, InitProcessing during startup,
    /// NormalProcessing once the postmaster is fully running.
    pub static Mode: ProcessingMode;

    /// True when this backend was forked by the postmaster (miscadmin.h).
    /// False during initdb (both --boot and --single phases).
    /// This is the authoritative check for whether background workers can exist.
    pub static IsUnderPostmaster: bool;

    /// True during WAL replay (crash recovery or standby).
    /// Set by the startup process; other processes see false (xlogutils.h).
    pub static InRecovery: bool;

}

#[inline(always)]
pub fn is_normal_processing() -> bool {
    unsafe { Mode == NORMAL_PROCESSING }
}

/// True only when running as a postmaster-forked backend.
/// When false, there is no postmaster, no bgworker launcher, and
/// therefore no s3worker — I/O must fall back to md.
#[inline(always)]
pub fn is_under_postmaster() -> bool {
    unsafe { IsUnderPostmaster }
}

/// True during WAL replay (crash recovery or standby).
#[inline(always)]
pub fn in_recovery() -> bool {
    unsafe { InRecovery }
}

/// Return the current backend's ProcNumber.
///
/// Dense index 0..MaxBackends+NUM_AUXILIARY_PROCS-1 into the PGPROC
/// shared memory array. NOT the OS PID.
#[inline(always)]
pub fn get_my_proc_number() -> ProcNumber {
    unsafe { MyProcNumber }
}

/// Return the PostgreSQL data directory path.
pub fn data_dir_path() -> PathBuf {
    let data_dir = unsafe { CStr::from_ptr(DataDir).to_str().unwrap_or("") };
    Path::new(data_dir).to_path_buf()
}
