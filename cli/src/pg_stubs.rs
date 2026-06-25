//! PostgreSQL symbol stubs for standalone (non-PG) CLI binaries.
//!
//! `core` transitively references a handful of symbols that only exist inside
//! a postmaster-loaded extension — they're declared `extern` in `pgsys` and
//! resolved at load time against the running postgres process. A standalone
//! binary statically links `core`, so those symbols would be undefined at link
//! time unless something in the binary's link graph defines them.
//!
//! This module provides no-op definitions. It lives in the `cli` library crate
//! (not in `pgsys`) on purpose: `pgsys` is also linked into the postgres
//! extension, where these symbols are supplied by postgres itself — defining
//! them there would collide. Confining the stubs to the CLI lib keeps them out
//! of the extension's link graph entirely.
//!
//! The symbols are `#[no_mangle]` and referenced by `core`/`pgsys`, so the
//! linker pulls them into every `cli` binary automatically; no explicit `use`
//! is required at the call site.

use std::os::raw::{c_char, c_int};

/// `char *DataDir` — read by [`pgsys::common::data_dir_path`] only when both
/// `TIKO_STORAGE_ROOT` and `TIKO_LOCAL_PATH` are unset. Points at an empty C
/// string so a stray read can't dereference null.
#[unsafe(no_mangle)]
pub static mut DataDir: *const c_char = c"".as_ptr();

/// Logging trampoline that `core`/`pgsys` call into; a no-op outside the
/// postmaster. PostgreSQL already captures `restore_command` stderr, so log
/// lines from standalone binaries are dropped here intentionally.
#[unsafe(no_mangle)]
pub extern "C" fn rust_pg_log(_elevel: c_int, _message: *const c_char) {}
