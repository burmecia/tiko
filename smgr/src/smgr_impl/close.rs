use pgsys::{common::ForkNumber, smgr::*};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_close(_reln: *mut SMgrRelationData, _forknum: ForkNumber) {
    // NO_OP, NO NEED TO CALL mdclose() BECAUSE:
    // mdclose() only cleans up file descriptor tracking; no I/O is performed.
    // S3 connection cleanup is handled at the pipeline/worker layer.
    // Therefore, no S3-specific logic is needed here.
    //mdclose(reln, forknum);
}
