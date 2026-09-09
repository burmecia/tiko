use core::io_control::IoControl;
use core::relfork::RelFork;
use core::{env, local_path, relfork::ops, storage_root_path};
use pgsys::{
    common::{BlockNumber, ForkNumber},
    logging::pg_log_error,
    smgr::*,
};

/// Get the number of blocks in a relation fork.
///
/// Reads from the shared-memory meta cache, which tracks every write that
/// extended the fork (including dirty chunks not yet committed/evicted).
/// Falls back to the committed store state when the cache is unavailable
/// (e.g. initdb).
#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_nblocks(
    reln: *mut SMgrRelationData,
    forknum: ForkNumber,
) -> BlockNumber {
    let relfork = RelFork::from_rel(reln, forknum);

    match ops::get_nblocks(&relfork) {
        Ok(n) => n,
        Err(err) => {
            // Dump env + cache state so failures (often a missing/wrong
            // namespace or an uninitialised cache during early startup /
            // shutdown) are diagnosable from the log alone.
            let env_val = |name: &str| std::env::var(name).unwrap_or_else(|_| "<unset>".into());
            pg_log_error(format!(
                "tiko_nblocks: failed for relfork {relfork}: {err} \
                 [cache_available={} \
                  TIKO_ORG_ID={} TIKO_DB_ID={} \
                  storage_root={} local_path={}]",
                IoControl::cache_is_available(),
                env_val(env::ENV_ORG_ID),
                env_val(env::ENV_DB_ID),
                storage_root_path().display(),
                local_path().display(),
            ));
            0
        }
    }
}
