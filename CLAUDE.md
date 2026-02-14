# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pico replaces PostgreSQL's magnetic disk (`md`) storage manager with S3-backed block storage. A local file cache sits in front of S3 as the source of truth. The project is written in Rust and compiled as PostgreSQL shared libraries (extensions).

## Build & Test

Requires Rust 1.88+ (edition 2024). PostgreSQL is a git submodule under `postgres/`.

```bash
# Build everything and run tests
./run_test.sh

# Build individual crates
cd s3smgr && cargo build --release    # produces target/release/libs3smgr.a (staticlib)
cd s3worker && cargo build --release  # produces target/release/libs3worker.dylib (cdylib)

# Run the PostgreSQL regression test
cd postgres/src/test/modules/test_pico && make check \
  PG_TEST_INITDB_EXTRA_OPTS='-c log_min_messages=debug1 -c shared_preload_libraries=libs3worker'
```

PostgreSQL configure (from `note.txt`) — minimal debug build:
```bash
cd postgres && ./configure --prefix $(realpath ../)/target/pg-install \
    --enable-debug --enable-cassert --without-openssl --without-systemd \
    --without-libxml --without-libxslt --without-llvm --without-selinux
```

## Architecture

Three workspace crates with a clear dependency chain:

```
s3smgr ──→ s3worker ──→ pgsys
  │                       │
  └───────────────────────┘
```

### `pgsys` — PostgreSQL FFI bindings
Raw `extern "C"` declarations for PG internals: smgr (md* functions), background workers, shared memory, LWLocks, latches, condition variables, logging. No build.rs/bindgen — bindings are hand-written `#[repr(C)]` structs matching PG's C layout. Symbols resolve at load time against the running postgres process.

### `s3smgr` — Storage manager interface (staticlib)
Implements the PG `smgr` interface with `s3_*` functions (`s3_readv`, `s3_writev`, `s3_open`, etc.) that are registered as the storage manager. Currently most functions delegate to `md*` (magnetic disk) as a passthrough while the S3 path is being built. `s3_readv` and `s3_writev` have the full async I/O path: claim slot from per-backend pool → fill → publish → push to submit queue → SetLatch s3worker → WaitLatch for completion → release. Both guard against non-normal modes (initdb, shutdown) via `is_under_postmaster()` with md fallback.

### `s3worker` — Background worker process (cdylib)
Loaded via `shared_preload_libraries`. `_PG_init` registers a background worker that calls `s3worker_main`. Internal structure:
- **`main_loop`** — the PG-process main thread: polls submit queue, dispatches to Tokio, sleeps via `WaitLatch`
- **`thread_pool`** — initializes Tokio runtime (4 async workers + 8 blocking threads)
- **`dispatcher`** — bounded `sync_channel` for work requests from main thread to Tokio (no completion channel — Tokio notifies backends directly via SetLatch)
- **`io_handler`** — async S3 GET/PUT and local cache read/write with SetLatch completion path
- **`io_queue`** (pub) — the shared memory data structures, also used by `s3smgr`
- **`shmem`** — hooks into `shmem_request_hook`/`shmem_startup_hook` for PG shared memory init

### Shared Memory IPC

`S3IoControl` lives in PG shared memory (allocated via `ShmemInitStruct`). Layout:

- **Per-backend slot pools** (`BackendSlotPool`): Each backend owns 4 `S3IoSlot`s (64 bytes each). Slot claiming uses a local bitmask — zero contention, no CAS races. Pools are dynamically sized to `MaxBackends` and follow `S3IoControl` in shared memory via pointer arithmetic.
- **MPSC submit queue** (`SubmitQueue`): Backends push `(backend_id, slot_idx)` entries via `fetch_add`. s3worker pops and dispatches. Strict ordering, no advisory hints. 1024 entries, power-of-2 ring buffer. Zero sentinel handles producer write delays.
- **SetLatch completion**: Tokio workers call `SetLatch(owner_latch)` directly on the backend's latch after marking a slot Completed. No harvest step, no main-thread scan.

### Slot State Machine

`Free → Filling → Submitted → InProgress → Completed → Free`

| Transition | Who | Mechanism |
|---|---|---|
| Free → Filling | Backend | Claim from own pool (bit clear) |
| Filling → Submitted | Backend | `slot.publish()` (Release store) |
| Submitted → InProgress | s3worker | `slot.try_start_processing()` (CAS) |
| InProgress → Completed | Tokio | `slot.mark_completed()` + `SetLatch(owner_latch)` |
| Completed → Free | Backend | `slot.release()` + `pool.release()` |

### Shutdown & Non-Normal Mode Handling

PostgreSQL kills all `B_BG_WORKER` processes (including s3worker) in `PM_STOP_BACKENDS`, **before** the checkpointer performs the shutdown checkpoint in `PM_WAIT_XLOG_SHUTDOWN`. There is no `bgw_flags` value to keep a bgworker alive past this phase.

**Current guards** (`s3_readv`/`s3_writev`):
- `is_under_postmaster()` — false during initdb (both `--boot` and `--single` phases), falls back to md. Checked via PG's `IsUnderPostmaster` global (process-local, no shared memory).
- `is_s3worker_alive()` — checked at slot claim, submit queue push, and completion wait. Falls back to md if s3worker is dead (shutdown, crash). Uses `kill(pid, 0)` on PID stored in shared memory.

**Long-term solution — synchronous local cache write fallback**:
Once md is fully replaced, the md fallback path won't exist. Instead, `s3_writev`/`s3_readv` should have two paths:
1. **Async path** (s3worker alive): submit queue → Tokio → S3 + local cache (current implementation)
2. **Sync path** (s3worker dead): direct `pwrite()`/`pread()` to local cache files inline, no submit queue or Tokio needed

This handles initdb, shutdown checkpoint, and s3worker crash without depending on md. Pages land in the local cache (persistent), WAL guarantees recoverability, and on next startup s3worker reconciles cache-dirty pages with S3.

### Future Improvements

- **Local cache hit short-circuit**: Cache hits bypass the shared memory queue entirely — `s3_readv` checks the cache index and does a direct `pread()`. Only cache misses go through the async pipeline.
- **S3 request coalescing**: Batch adjacent block reads into single S3 range GET requests to amortize per-request latency.

### Thread Safety Rules

Tokio threads **CAN**: read/write shared memory atomics, `memcpy` to `buffer_ptr`, file/network I/O, `SetLatch`.
Tokio threads **CANNOT**: call `ConditionVariable*`, `LWLock*`, `ereport`/`elog`, `palloc`/`pfree` — these require PG process-local state and must only run on the main thread.

## Key Conventions

- `s3worker/build.rs` uses `-undefined dynamic_lookup` (macOS) so PG symbols resolve at extension load time
- All PG-facing functions use `extern "C-unwind"` and `#[unsafe(no_mangle)]`
- Shared memory pointers stored in `OnceLock<*mut T>` with Send/Sync wrapper types
- PG hook chaining: always save and call `prev_*_hook` before installing custom hooks
