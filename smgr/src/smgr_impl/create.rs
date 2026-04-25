use core::chunk::RelFork;
use core::ops;
use pgsys::{common::ForkNumber, logging::pg_log_error, smgr::*};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_create(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let relfork = RelFork::from_rel(reln, forknum);
    match ops::create(&relfork) {
        Ok(true) => {}             // newly created
        Ok(false) if is_redo => {} // exists, WAL replay — OK
        Ok(false) => {
            pg_log_error(&format!("tiko_create: relfork already exists {relfork}",));
        }
        Err(err) => {
            pg_log_error(&format!("tiko_create: failed for relfork {relfork}: {err}",));
        }
    }
}
