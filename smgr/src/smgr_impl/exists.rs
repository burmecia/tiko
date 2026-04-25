use core::chunk::RelFork;
use core::ops;
use pgsys::{common::ForkNumber, logging::pg_log_error, smgr::*};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_exists(reln: *mut SMgrRelationData, forknum: ForkNumber) -> bool {
    let relfork = RelFork::from_rel(reln, forknum);
    match ops::exists(&relfork) {
        Ok(exists) => exists,
        Err(err) => {
            pg_log_error(&format!("tiko_exists: failed for relfork {relfork}: {err}",));
            false
        }
    }
}
