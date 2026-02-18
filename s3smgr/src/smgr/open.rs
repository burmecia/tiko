use pgsys::smgr::*;

#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_open(_reln: *mut SMgrRelationData) {
    // NO_OP, NO NEED TO CALL mdopen() BECAUSE:
    // mdopen() only initializes relation state (resets open segment counters)
    // and performs no I/O operations. Therefore, S3-specific logic is not needed.
    //mdopen(reln);
}
