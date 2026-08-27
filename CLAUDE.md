# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tiko is a serverless Postgres proof-of-concept: an S3-backed storage engine for PostgreSQL.
It replaces PostgreSQL's magnetic-disk (`md`) storage manager with an S3-backed block store,
supports copy-on-write branching, and streams WAL to S3 for point-in-time recovery. Written in
Rust, compiled as PostgreSQL shared libraries (extensions) plus standalone CLI binaries. The
compute layer (running Postgres in Firecracker microVMs that scale to zero when idle) lives in
the separate [tikovm](https://github.com/burmecia/tikovm) project. This is experimental, not
production software.

## Build & Test

Requires Rust 1.88+ (edition 2024). PostgreSQL 18 is a git submodule under `postgres/`, patched
for Tiko's custom AIO opcodes.

```bash
./scripts/build_postgres.sh   # build vendored/patched Postgres
./scripts/run_test.sh         # build smgr+worker, build Postgres, run the smoke test (make check)

# Other test scripts
./scripts/run_large_data_test.sh  # large-data test
./scripts/run_pg_test.sh      # PostgreSQL regression test
./scripts/run_pitr_test.sh    # PITR test
./scripts/run_branch_test.sh  # COW branching test

# Build individual crates
cargo build -p smgr    # produces target/{debug,release}/libtikosmgr.a (staticlib+rlib)
cargo build -p worker  # produces target/{debug,release}/libtikoworker.{dylib,so} (cdylib+rlib)
```

`run_test.sh` sets `TIKO_ORG_ID`/`TIKO_DB_ID`/`TIKO_PROJECT_ID`/`TIKO_PITR_INTERVAL_SECS`, builds
`smgr`, builds Postgres, builds `worker`, copies `libtikoworker` into
`postgres/src/test/modules/test_tiko/`, then runs `make check` there with
`shared_preload_libraries=libtikoworker`.

Clippy note: `cargo clippy` run against `core`/`cli` aborts on pre-existing lint errors in `pgsys`
(FFI bindings) — verify changes with `cargo build`/`cargo test` instead of clippy.

## Architecture

### Workspace layout

```
tiko/
├── postgres/     # vendored PostgreSQL 18 (git submodule) + Tiko patches
├── pgsys/        # hand-written PostgreSQL FFI bindings
├── core/         # storage layer: chunks, manifests, store, I/O engine
├── smgr/         # tikosmgr — PostgreSQL storage manager
├── worker/       # tikoworker — background worker (AIO, WAL receiver, compactor)
├── cli/          # operator CLIs: tiko_pitr, tiko_branch, tiko_restore, tiko_tlseg_viewer
```

```
pgsys ──→ core ──→ smgr (tikosmgr)  ──→ postgres
              └───→ worker (tikoworker) ──→ postgres
                └──→ cli (tiko_pitr, tiko_branch, tiko_restore, ...)
```

The compute layer is **not in this repo**. VM orchestration (Firecracker lifecycle,
snapshot/restore, connection proxying) lives in [tikovm](https://github.com/burmecia/tikovm):
`hostd` runs on the KVM host and `guestd` inside each VM (starts/stops Postgres via `pg_ctl`,
reports idleness). Tiko's crates have no Rust dependency on tikovm — integration is by
convention: `guestd` spawns Tiko's CLI binaries / `pg_ctl` inside the VM and exposes HTTP
routes that consume their JSON output.

### `pgsys` — PostgreSQL FFI bindings
Raw `extern "C"` declarations for PG internals: smgr, background workers, shared memory, LWLocks,
latches, condition variables, logging, PG18 AIO (`aio.rs`). No build.rs/bindgen — bindings are
hand-written `#[repr(C)]` structs matching PG's C layout. Symbols resolve at load time against the
running postgres process.

### `core` — storage layer (library, no PG dependency)
Chunks, manifests, the object-store abstraction, and the shared-memory I/O/cache engine. Key
modules:
- `db.rs` — `DbNamespace { org_id, db_id,
  project_id }`, built from `TIKO_ORG_ID`/`TIKO_DB_ID`/`TIKO_PROJECT_ID` env vars. Only
  `org_id`/`db_id` currently appear in storage keys.
- `io/locator.rs` — `Locator`: builds S3 object keys, e.g. `{org}/{db}/chunks/{ckpt}/{relfork}/{chunk_id}`,
  `{org}/{db}/bases/{tl}/{lsn}.manifest`, `{org}/{db}/backup/{tl}/{lsn}.tar.zst`,
  `{org}/{db}/timeline/{segment}`, `{org}/{db}/wal/{tl}/{segment}[.chunks/{offset}]`,
  `{org}/{db}/db_meta.json`. `chunk_in_db()` addresses another `db_id` in the same org — this is
  the COW mechanism (see below).
- `manifest.rs` — `ChunkRef { db_id, timeline_id, lsn, ... }`. A chunk reference can point at a
  *parent* database's namespace, so a branch's base manifest resolves shared chunks straight from
  the parent's storage without copying.
- `io/storage/` — `trait ObjectStorage { put, get, delete, list_prefix }`. `storage.rs` wraps a
  `Box<dyn ObjectStorage>`; `s3.rs` is a stub (`todo!()`) for a real networked S3 client; `s3_sim.rs`
  (`S3Sim`) is the **active backend today** — a local-filesystem simulation of S3 rooted at
  `{root_path}/s3sim`, zstd-compressing everything except `.json`/`.zst` objects. In production this
  filesystem root is itself an NFSv4.2-mounted S3 Files share, so despite the name, `S3Sim` is the
  real production storage path, not just a test double.
- `io/cache/` — shared-memory write-back `ChunkCache`/`MetaCache` (256 KB chunks, per-fork nblocks
  and deletion state). There is **no local backing-file cache** anymore — reads/writes flow PG
  buffer → shmem cache → `Store` → `Storage` (S3Sim) directly on eviction/flush.
- `io/store.rs` — `Store` ties cache + locator + storage together (`get_chunk`, `patch_chunk`,
  `run_compaction`).
- `pitr.rs` — recovery-config helpers (`postgresql.auto.conf` recovery block), crash-safe PGDATA
  snapshot/restore excluding the bulk `tiko/` dir.
- `env.rs` — env var parsing, incl. `TIKO_LOCAL_PATH` for the small local state dir (base-manifest
  cache file, draft spill file — not block data).

### `smgr` (crate `smgr`, lib `tikosmgr`) — storage manager interface (staticlib+rlib)
Implements the PG `smgr` interface (`smgr_impl/*.rs`: open, close, create, exists, extend, nblocks,
prefetch, readv, writev, truncate, unlink, zeroextend, startreadv, ...). Two I/O paths:
- **Sync path**: calls `core::ops` (read/write blocks) directly in the backend process. Correct
  because sync smgr callers may pass backend-local memory (palloc'd pages, local buffers,
  stack-local aligned blocks) that the worker process cannot access cross-process.
- **Async path** (`tiko_startreadv` → `aio.rs::perform_io`): uses the shared-memory pipeline to
  `tikoworker`. Falls back to direct `core::ops` calls when the worker/pipeline is unavailable
  (initdb, shutdown checkpoint, worker crash).
- `checkpoint.rs` — `tiko_perform_checkpoint()`: normal checkpoints flush dirty cache chunks;
  `CHECKPOINT_CAUSE_BASEBACKUP` additionally materializes a base manifest at that LSN (paired with
  `tiko_pitr backup`); shutdown checkpoints fold everything into the base manifest inline.

### `worker` (crate `worker`, lib `tikoworker`) — background worker process (cdylib+rlib)
Loaded via `shared_preload_libraries`. `_PG_init` registers a background worker running
`main_loop`. Structure:
- **`main_loop`** — PG-process main thread: polls submit queue, dispatches to Tokio, sleeps via
  `WaitLatch`.
- **`thread_pool`** — Tokio runtime init.
- **`dispatcher`** / **`io_handler`** — shared-memory submit queue from backends to Tokio, async
  S3 GET/PUT + local cache I/O, completion via `SetLatch` on the backend's latch.
- **`shmem`** — `shmem_request_hook`/`shmem_startup_hook` for PG shared memory init.
- **`tasks/wal_receiver.rs`** — streams WAL from the local postmaster via the PG physical
  streaming-replication protocol over a Unix socket (hand-rolled wire protocol; `tokio-postgres`
  lacks `CopyBoth`), uploading 256 KiB WAL chunk objects near-realtime and sealing full segments on
  switch.
- **`tasks/compactor.rs`** — folds superseded timeline segments into a new base manifest and
  deletes the now-redundant segment objects (the only GC-like behavior currently implemented; see
  Roadmap below — full chunk/retention GC is not yet built).

### Shared Memory IPC & Slot State Machine
`S3IoControl`-style shared struct lives in PG shared memory. Per-backend slot pools (small fixed
slots per backend, bitmask claiming — no CAS races on claim), an MPSC submit queue backends push
into and the worker pops from, and direct `SetLatch` completion (no harvest step, no main-thread
scan).

Slot lifecycle: `Free → Filling → Submitted → InProgress → Completed → Free` — backend claims and
fills, backend publishes (release store), worker claims for processing (CAS), Tokio marks complete
and sets the backend's latch, backend releases back to its pool.

### PG18 AIO Integration
The vendored `postgres/` submodule is patched with custom AIO opcodes `PGAIO_OP_TIKO_READV` /
`PGAIO_OP_TIKO_WRITEV` (`postgres/src/include/storage/aio.h`, `.../tiko.h`), wired into
`aio_io.c`/`aio_funcs.c`/`smgr.c`'s core dispatch switches. This is a small, contained patch — no
I/O method replacement, no custom completion callbacks beyond the normal bufmgr chain.

Flow: `smgr::startreadv::tiko_startreadv` sets up iovecs, registers callbacks, calls
`pgaio_io_start_tiko_readv` (no `PGAIO_HF_SYNCHRONOUS` flag, so PG's IO worker pool picks it up,
keeping the backend non-blocking). The IO worker calls `pgaio_io_perform_synchronously()`, which
hits `smgr::aio::perform_io()` — this submits into the Tiko shared-memory pipeline to `tikoworker`
(or falls back to direct `core::ops` calls when the pipeline isn't available) and waits on the
latch. Normal PG AIO completion callbacks (md validation, `BM_VALID`, checksums) run unmodified.

Thread safety: Tokio threads **can** read/write shared memory atomics, `memcpy` into buffers, do
file/network I/O, and `SetLatch`. They **cannot** call `ConditionVariable*`, `LWLock*`,
`ereport`/`elog`, or `palloc`/`pfree` — those require PG process-local state and must only run on
the main thread.

### Shutdown & Non-Normal Mode Handling
PostgreSQL kills all `B_BG_WORKER` processes (including `tikoworker`) in `PM_STOP_BACKENDS`,
**before** the checkpointer's shutdown checkpoint. A `use_pipeline()`-style guard (checks
`IsUnderPostmaster` and whether the worker PID in shared memory is alive) falls back to direct
`core::ops` calls when the async path isn't available — initdb, shutdown checkpoint, worker crash.
Sync smgr functions always call `core::ops` directly regardless, so pages land in the shmem cache /
get flushed to storage, WAL guarantees recoverability, and on restart the worker reconciles any
cache-dirty state.

### `cli` — operator CLI binaries
- `tiko_pitr` — `list` (available recovery points), `backup` (runs `pg_basebackup`, uploads
  tarball under the `backup/` key prefix), `recover --time|--lsn [--timeline]` (installs the
  backup's base manifest, replays WAL, promotes, leaves the instance stopped), `restart`.
- `tiko_branch` — `backup` (runs `pg_basebackup -X stream` against the running parent, forming a
  base manifest at that LSN via `CHECKPOINT_CAUSE_BASEBACKUP`, packs into `tar.zst`), `restore`
  (unpacks into a fresh branch PGDATA and seeds the branch's namespace with the parent's base
  manifest — `ChunkRef.db_id = parent`, so shared chunks resolve from the parent's storage — then
  starts the branch's Postgres to replay to consistency and stops it), `restart`.
- `tiko_restore` — implements PostgreSQL's `restore_command` contract (`tiko_restore %f %p`),
  reading sealed-segment or in-flight `.chunks/` WAL objects written by `wal_receiver`.
- `tiko_tlseg_viewer` — inspects timeline/segment objects.
- `pg_stubs.rs` — standalone binaries statically link `core`/`pgsys`, which declare `extern "C"`
  symbols normally resolved by the running postmaster (e.g. `DataDir`, `rust_pg_log`). `pg_stubs.rs`
  provides no-op definitions so these binaries link outside of a running Postgres process.

### Compute layer (tikovm)
The compute half of the stack — running Postgres in Firecracker microVMs that scale to zero —
lives in the separate [tikovm](https://github.com/burmecia/tikovm) project and is out of scope
for this repo. In brief: `hostd` (on the KVM host) owns VM lifecycle, snapshot/restore, and the
client-facing proxy that wakes a frozen VM on connect; `guestd` (inside each VM) manages the
Postgres process via `pg_ctl`, runs Tiko's CLI binaries (`tiko_branch`, `tiko_pitr`, ...) and
reports idleness so `hostd` knows when to freeze.

### Copy-on-write branching
Every database is a branch of a seed database. A chunk's `ChunkRef` can reference the *parent*
database's `db_id`, so a freshly restored branch shares all inherited chunks without copying —
only newly written/modified blocks land under the branch's own `db_id`. Driven end-to-end by
`tiko_branch backup`/`restore` (in a deployment, invoked inside the VM by tikovm's `guestd`).

### Point-in-time recovery
WAL streams to S3 in near-real-time via `worker::tasks::wal_receiver`. `tiko_pitr recover
--time|--lsn` replays to a target point and promotes. `tiko_restore` implements the
`restore_command` contract PG calls during recovery.

## Key Conventions

- `worker/build.rs` uses `-undefined dynamic_lookup` (macOS) so PG symbols resolve at extension
  load time.
- All PG-facing functions use `extern "C-unwind"` and `#[unsafe(no_mangle)]`.
- Shared memory pointers stored in `OnceLock<*mut T>` with Send/Sync wrapper types.
- PG hook chaining: always save and call `prev_*_hook` before installing custom hooks.

## Roadmap / Not Yet Implemented

Per the README's own roadmap and verified absent from the code:
- **Garbage collection**: no chunk/retention GC exists. `worker::tasks::compactor` only deletes
  timeline segments once folded into a new base manifest — there is no delta-manifest GC,
  base-manifest GC, WAL GC, or orphaned-chunk GC. There is also no org deletion
  mechanism anymore (`org.rs` and its `deleted_at` soft-delete field were removed).
- **Real S3 backend**: `core::io::storage::s3::S3` is a stub (`todo!()`); `S3Sim` (local
  filesystem, potentially NFS-mounted) is the only working backend today.
