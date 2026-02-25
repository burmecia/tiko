/// smgr shutdown hook — called via `smgrshutdown()` as an `on_proc_exit` callback
/// in every process that opened the smgr subsystem.
///
/// Cache flushing is intentionally not done here. `s3_checkpoint_flush()` is
/// called from `CheckPointGuts()` after `CheckPointBuffers()`, which guarantees
/// all dirty chunks are written to backing files before the checkpoint WAL record
/// is written. By the time this hook fires — whether in a regular backend exiting
/// during `PM_STOP_BACKENDS` or in the checkpointer after the shutdown checkpoint —
/// the cache is already clean.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_shutdown() {}
