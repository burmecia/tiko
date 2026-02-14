// pgsys/src/smgr.rs
//! PostgreSQL Storage Manager (SMgr) FFI types and bindings

// Define the types that match PostgreSQL's C types
pub type BlockNumber = u32;
pub type ForkNumber = u32;
pub type RelFileNumber = u32; // typedef Oid RelFileNumber in PostgreSQL
pub type Oid = u32; // typedef unsigned int Oid
pub type ProcNumber = i32; // typedef int ProcNumber

// MAX_FORKNUM is INIT_FORKNUM which is 3
const MAX_FORKNUM: usize = 3;

// Corresponds to PostgreSQL's RelFileLocator struct
#[repr(C)]
pub struct RelFileLocator {
    pub spc_oid: Oid,              // tablespace OID
    pub db_oid: Oid,               // database OID
    pub rel_number: RelFileNumber, // relation filenode number
}

// Corresponds to PostgreSQL's RelFileLocatorBackend struct
#[repr(C)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

// Corresponds to PostgreSQL's SMgrRelationData struct
// This must match the C struct layout
#[repr(C)]
pub struct SMgrRelationData {
    pub smgr_rlocator: RelFileLocatorBackend,
    pub smgr_targblock: BlockNumber,
    pub smgr_cached_nblocks: [BlockNumber; MAX_FORKNUM + 1],
    // We don't need to define the private fields (smgr_which, md_num_open_segs, etc.)
    // since we only access the public fields above
}

// Opaque handle type for async I/O
#[repr(C)]
pub struct PgAioHandle {
    _private: [u8; 0],
}

// External C functions from the MD (magnetic disk) storage manager
unsafe extern "C" {

    pub fn mdinit();
    pub fn mdopen(reln: *mut SMgrRelationData);
    pub fn mdclose(reln: *mut SMgrRelationData, forknum: ForkNumber);
    pub fn mdcreate(reln: *mut SMgrRelationData, forknum: ForkNumber, isRedo: bool);
    pub fn mdexists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool;
    pub fn mdunlink(rlocator: *mut RelFileLocatorBackend, forknum: ForkNumber, isRedo: bool);
    pub fn mdextend(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: *const std::ffi::c_void,
        skipFsync: bool,
    );
    pub fn mdzeroextend(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: i32,
        skipFsync: bool,
    );
    pub fn mdprefetch(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: i32,
    ) -> bool;
    pub fn mdmaxcombine(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
    ) -> u32;
    pub fn mdreadv(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: *mut *mut std::ffi::c_void,
        nblocks: BlockNumber,
    );
    pub fn mdstartreadv(
        ioh: *mut PgAioHandle,
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: *mut *mut std::ffi::c_void,
        nblocks: BlockNumber,
    );
    pub fn mdwritev(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: *const *const std::ffi::c_void,
        nblocks: BlockNumber,
        skipFsync: bool,
    );
    pub fn mdwriteback(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: BlockNumber,
    );
    pub fn mdnblocks(reln: *mut SMgrRelationData, forknum: ForkNumber) -> BlockNumber;
    pub fn mdtruncate(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        old_blocks: BlockNumber,
        nblocks: BlockNumber,
    );
    pub fn mdimmedsync(reln: *mut SMgrRelationData, forknum: ForkNumber);
    pub fn mdregistersync(reln: *mut SMgrRelationData, forknum: ForkNumber);
    pub fn mdfd(
        reln: *mut SMgrRelationData,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        off: *mut u32,
    ) -> i32;
}
