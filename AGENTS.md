# AGENTS.md

Guide for AI agents working in this repo.

## Project Overview

Tiko is a serverless Postgres proof-of-concept: an S3-backed storage engine for
PostgreSQL. It replaces PostgreSQL's magnetic-disk (`md`) storage manager with
an S3-backed block store, supports copy-on-write branching, and streams WAL to
S3 for point-in-time recovery. Written in Rust, compiled as PostgreSQL shared
libraries (extensions) plus standalone CLI binaries. The compute layer (running
Postgres in Firecracker microVMs that scale to zero when idle) lives in the
separate [tikovm](https://github.com/burmecia/tikovm) project. This is
experimental, not production software.

## Build & test commands

Requires Rust 1.88+ (edition 2024). PostgreSQL 18 is a git submodule under
`postgres/`, patched for Tiko's custom AIO opcodes — it must be initialized.

```bash
./scripts/build_postgres.sh   # build vendored/patched PG into target/pg-install (run once)
./scripts/run_test.sh         # primary smoke test: builds smgr + PG + worker, runs make check
```

Other suites: `run_large_data_test.sh` (large data), `run_test4.sh`, `run_pg_test.sh`
(PG regression), `run_pitr_test.sh` (PITR), `run_branch_test.sh` (COW branching).

Individual crates:

```bash
cargo build -p smgr    # target/{debug,release}/libtikosmgr.a (staticlib+rlib)
cargo build -p worker  # target/{debug,release}/libtikoworker.{dylib,so} (cdylib+rlib)
```

`run_test.sh` is the **integration test** and encodes a required build order:
build `smgr` (staticlib, linked into PG) → `make && make install` in `postgres/`
→ build `worker` (cdylib) → copy `libtikoworker.{dylib,so}` into
`postgres/src/test/modules/test_tiko/worker/` → `make check` there with
`shared_preload_libraries=libtikoworker`. Don't reorder these.

Unit tests run per-crate with `cargo test -p <crate>` (e.g. `core`, `pgsys`).

## Gotchas an agent will hit

- **Do NOT run `cargo clippy`** on `core`/`smgr`/`worker`/`cli`/`pgsys`. Pre-existing
  lint errors in the hand-written FFI bindings (`pgsys`) abort the build. Verify
  changes with `cargo build` / `cargo test` instead.
- **`build_postgres.sh`** installs deps via `apt-get` on Linux and `brew` on
  macOS (auto-detected). On macOS it also checks for Xcode Command Line Tools.
- **Required env vars**: `run_test.sh` sets `TIKO_ORG_ID`/`TIKO_DB_ID`/
  `TIKO_PROJECT_ID`/`TIKO_PITR_INTERVAL_SECS`. It also `unset`s
  `TIKO_STORAGE_ROOT`/`TIKO_LOCAL_PATH` (the smoke test uses defaults). In a VM
  these are provisioned by the tikovm guest image (sourced from
  `/var/lib/postgresql/tiko_env.sh`).
- **macOS System V shmem leak**: `run_test.sh` cleans orphaned `ipcs -m` segments
  first because macOS caps `kern.sysv.shmmni` at 32 and each killed postgres leaks
  one. If `make check` hangs/fails on shmem, clear them manually.

## Architecture

### Workspace layout

```
tiko/
├── postgres/     # vendored PostgreSQL 18 (git submodule) + Tiko patches
├── pgsys/        # hand-written PostgreSQL FFI bindings
├── core/         # storage layer: chunks, manifests, store, I/O engine
├── smgr/         # tikosmgr — PostgreSQL storage manager (staticlib+rlib, linked into PG)
├── worker/       # tikoworker — background worker (cdylib+rlib, shared_preload_libraries)
├── cli/          # operator CLIs: tiko_pitr, tiko_branch, tiko_restore, tiko_tlseg_viewer
```

```
pgsys ──→ core ──→ smgr (tikosmgr)  ──→ postgres
              └───→ worker (tikoworker) ──→ postgres
                └──→ cli (tiko_pitr, tiko_branch, tiko_restore, ...)
```

### `pgsys` — PostgreSQL FFI bindings
Raw `extern "C"` declarations for PG internals: smgr, background workers, shared
memory, LWLocks, latches, condition variables, logging, PG18 AIO (`aio.rs`). No
build.rs/bindgen — bindings are hand-written `#[repr(C)]` structs matching PG's
C layout. Symbols resolve at load time against the running postgres process.

### `core` — storage layer (library, no PG dependency)
Chunks, manifests, the object-store abstraction, and the shared-memory
I/O/cache engine. Key modules:
- `db.rs` — `DbNamespace { org_id, db_id }`, built from `TIKO_ORG_ID`/`TIKO_DB_ID`
  env vars. Only these two appear in storage keys.
- `io/locator.rs` — `Locator`: builds S3 object keys under `{org}/{db}/`:
  `chunks/...`, `bases/{tl}/{lsn}.manifest`, `backup/{tl}/{lsn}.tar.zst` (+ a
  `{lsn}.json` meta sidecar), `timeline/{segment}`,
  `wal/{tl}/{segment}[.chunks/{byte_offset:016X}]`, `db_meta.json`.
  `chunk_in_db()` addresses another `db_id` in the same org — the COW mechanism.
- `manifest.rs` — `Manifest` (file-backed sorted TIKM manifest) and
  `ChunkRef { db_id, timeline_id, lsn }`. A chunk reference can point at a
  *parent* database's namespace, so a branch's base manifest resolves shared
  chunks straight from the parent's storage without copying.
- `io/storage/` — `trait ObjectStorage { put, get, delete, list_prefix }`.
  `s3.rs` is a `todo!()` stub (real networked S3 not implemented). `s3_sim.rs`
  (`S3Sim`) is the **active backend** — a local-filesystem simulation rooted at
  `{root_path}/s3sim`, zstd-compressing everything except `.json`/`.zst`
  objects. It is **not just a test double**: in production its root is an
  NFSv4.2-mounted S3 Files share, so this is the real storage path.
- `io/cache/` — shared-memory write-back `ChunkCache`/`MetaCache` (256 KB
  chunks, per-fork nblocks and deletion state). There is **no local
  backing-file cache** anymore — reads/writes flow PG buffer → shmem cache →
  `Store` → `Storage` (S3Sim) directly on eviction/flush.
- `io/store.rs` — `Store` ties cache + locator + storage together (`get_chunk`,
  `patch_chunk`, `run_compaction`).
- `pitr.rs` — recovery-config helpers (`postgresql.auto.conf` recovery block),
  crash-safe PGDATA snapshot/restore excluding the bulk `tiko/` dir.
- `env.rs` — env var parsing, incl. `TIKO_LOCAL_PATH` for the small local state
  dir (base-manifest cache file, draft spill file — not block data).

### `smgr` (crate `smgr`, lib `tikosmgr`) — storage manager interface
Implements the PG `smgr` interface (`smgr_impl/*.rs`: open, close, create,
exists, extend, nblocks, prefetch, readv, writev, truncate, unlink, zeroextend,
startreadv, immedsync, ...). Two I/O paths:
- **Sync path**: calls `core::relfork_ops` (read/write blocks) directly in the
  backend process. Correct because sync smgr callers may pass backend-local
  memory (palloc'd pages, local buffers, stack-local aligned blocks) that the
  worker process cannot access cross-process.
- **Async path** (`tiko_startreadv` → `aio.rs::perform_io`): uses the
  shared-memory pipeline to `tikoworker`. Falls back to direct
  `core::relfork_ops` calls when the worker/pipeline is unavailable (initdb,
  shutdown checkpoint, worker crash).
- `checkpoint.rs` — `tiko_perform_checkpoint()`: normal checkpoints flush dirty
  cache chunks; `CHECKPOINT_CAUSE_BASEBACKUP` additionally materializes a base
  manifest at that LSN (paired with `tiko_pitr backup`); shutdown checkpoints
  fold everything into the base manifest inline.

### `worker` (crate `worker`, lib `tikoworker`) — background worker process
Loaded via `shared_preload_libraries`. `_PG_init` registers a background worker
running `main_loop`. Structure:
- **`main_loop`** — PG-process main thread: polls submit queue, dispatches to
  Tokio, sleeps via `WaitLatch`.
- **`thread_pool`** — Tokio runtime init.
- **`dispatcher`** / **`io_handler`** — shared-memory submit queue from backends
  to Tokio, async S3 GET/PUT + local cache I/O, completion via `SetLatch` on the
  backend's latch.
- **`shmem`** — `shmem_request_hook`/`shmem_startup_hook` for PG shared memory
  init.
- **`tasks/wal_receiver.rs`** — streams WAL from the local postmaster via the PG
  physical streaming-replication protocol over a Unix socket (hand-rolled wire
  protocol; `tokio-postgres` lacks `CopyBoth`), uploading 256 KiB WAL chunk
  objects near-realtime and sealing full segments on switch.
- **`tasks/compactor.rs`** — folds superseded timeline segments into a new base
  manifest and deletes the now-redundant segment objects. This is the only
  GC-like behavior: **no chunk/WAL/orphan GC exists**, and no org deletion
  mechanism (the old `org.rs` soft-delete module was removed).

### Shared Memory IPC & Slot State Machine
An `IoControl`-style shared struct lives in PG shared memory. Per-backend slot
pools (small fixed slots per backend, bitmask claiming — no CAS races on
claim), an MPSC submit queue backends push into and the worker pops from, and
direct `SetLatch` completion (no harvest step, no main-thread scan).

Slot lifecycle: `Free → Filling → Submitted → InProgress → Completed → Free`
(`SlotState` in `core/src/io/io_control.rs`) — backend claims and fills, backend
publishes (release store), worker claims for processing (CAS), Tokio marks
complete and sets the backend's latch, backend releases back to its pool.

### PG18 AIO Integration
The vendored `postgres/` submodule is patched with custom AIO opcodes
`PGAIO_OP_TIKO_READV` / `PGAIO_OP_TIKO_WRITEV`
(`postgres/src/include/storage/aio.h`, `.../tiko.h`), wired into
`aio_io.c`/`aio_funcs.c` and `backend/storage/smgr/smgr.c`'s core dispatch
switches. This is a small, contained patch — no I/O method replacement, no
custom completion callbacks beyond the normal bufmgr chain.

Flow: `smgr::startreadv::tiko_startreadv` sets up iovecs, registers callbacks,
calls `pgaio_io_start_tiko_readv` (no `PGAIO_HF_SYNCHRONOUS` flag, so PG's IO
worker pool picks it up, keeping the backend non-blocking). The IO worker calls
`pgaio_io_perform_synchronously()`, which hits `smgr::aio::perform_io()` — this
submits into the Tiko shared-memory pipeline to `tikoworker` (or falls back to
direct `core::relfork_ops` calls when the pipeline isn't available) and waits on
the latch. Normal PG AIO completion callbacks (md validation, `BM_VALID`,
checksums) run unmodified.

Thread safety: Tokio threads **can** read/write shared memory atomics, `memcpy`
into buffers, do file/network I/O, and `SetLatch`. They **cannot** call
`ConditionVariable*`, `LWLock*`, `ereport`/`elog`, or `palloc`/`pfree` — those
require PG process-local state and must only run on the main thread.

### Shutdown & Non-Normal Mode Handling
PostgreSQL kills all `B_BG_WORKER` processes (including `tikoworker`) in
`PM_STOP_BACKENDS`, **before** the checkpointer's shutdown checkpoint. A
`use_pipeline()`-style guard (checks `IsUnderPostmaster` and whether the worker
PID in shared memory is alive) falls back to direct `core::relfork_ops` calls
when the async path isn't available — initdb, shutdown checkpoint, worker crash.
Sync smgr functions always call `core::relfork_ops` directly regardless, so
pages land in the shmem cache / get flushed to storage, WAL guarantees
recoverability, and on restart the worker reconciles any cache-dirty state.

### `cli` — operator CLI binaries
- `tiko_pitr` — `list` (available recovery points), `backup` (runs
  `pg_basebackup`, uploads tarball under the `backup/` key prefix),
  `recover --time|--lsn [--timeline]` (installs the backup's base manifest,
  replays WAL, promotes, leaves the instance stopped), `restart`.
- `tiko_branch` — `backup` (runs `pg_basebackup -X stream` against the running
  parent, forming a base manifest at that LSN via `CHECKPOINT_CAUSE_BASEBACKUP`,
  packs into `tar.zst`), `restore` (unpacks into a fresh branch PGDATA and seeds
  the branch's namespace with the parent's base manifest — `ChunkRef.db_id` =
  parent, so shared chunks resolve from the parent's storage — then starts the
  branch's Postgres to replay to consistency and stops it), `restart`.
- `tiko_restore` — implements PostgreSQL's `restore_command` contract
  (`tiko_restore %f %p`), reading sealed-segment or in-flight `.chunks/` WAL
  objects written by `wal_receiver`.
- `tiko_tlseg_viewer` — inspects timeline/segment objects.
- `pg_stubs.rs` — standalone binaries statically link `core`/`pgsys`, which
  declare `extern "C"` symbols normally resolved by the running postmaster
  (e.g. `DataDir`, `rust_pg_log`). `pg_stubs.rs` provides no-op definitions so
  these binaries link outside of a running Postgres process.

### Compute layer (tikovm)
The compute layer is **not in this repo**. VM orchestration (Firecracker
lifecycle, snapshot/restore, connection proxying) lives in
[tikovm](https://github.com/burmecia/tikovm): `hostd` runs on the KVM host and
`guestd` inside each VM (starts/stops Postgres via `pg_ctl`, runs Tiko's CLI
binaries, reports idleness so `hostd` knows when to freeze). Tiko's crates have
no Rust dependency on tikovm — integration is by convention: `guestd` spawns
Tiko's CLI binaries / `pg_ctl` inside the VM and exposes HTTP routes that
consume their JSON output.

### Copy-on-write branching
Every database is a branch of a seed database. A chunk's `ChunkRef` can
reference the *parent* database's `db_id`, so a freshly restored branch shares
all inherited chunks without copying — only newly written/modified blocks land
under the branch's own `db_id`. Driven end-to-end by `tiko_branch
backup`/`restore` (in a deployment, invoked inside the VM by tikovm's
`guestd`).

### Point-in-time recovery
WAL streams to S3 in near-real-time via `worker::tasks::wal_receiver`.
`tiko_pitr recover --time|--lsn` replays to a target point and promotes.
`tiko_restore` implements the `restore_command` contract PG calls during
recovery.

## Conventions

- All PG-facing functions use `extern "C-unwind"` and `#[unsafe(no_mangle)]`.
- `worker/build.rs` emits `-undefined dynamic_lookup` on macOS so PG symbols
  resolve at extension load time (don't change this).
- Shared-memory pointers are stored in `OnceLock<*mut T>` with hand-rolled
  Send/Sync wrappers; per-backend slot pools use bitmask claiming (no CAS races).
- Tokio worker threads may touch shmem atomics, `memcpy` buffers, do I/O, and
  `SetLatch` — they must **not** call `ConditionVariable*`, `LWLock*`,
  `ereport`/`elog`, or `palloc`/`pfree` (those are PG process-local).
- Hook chaining: always save and call the `prev_*_hook` before installing your own.

## Notes that differ from defaults

- Through the tikovm proxy, `psql` authenticates with a per-VM JWT:
  `options='-c tikovm_token=<jwt>'` (routed by tikovm's `hostd`). Tiko's own test
  scripts talk to Postgres directly, no proxy involved.
- Minimum Rust **1.88, edition 2024** (no `rust-toolchain.toml`).
- No CI workflows are defined; `./scripts/run_test.sh` is the canonical check.

## Code Style & Comments

- **Minimize comments**: Only add comments when absolutely necessary
- **Comment only the "why"**, never the "what" (code should be self-documenting)
- Keep comments **short, concise, and to the point** (1-2 lines maximum)
- Prefer meaningful variable/function names over explanatory comments
- **Do not** add doc comments (`///`) for every function - only for public API surfaces
- **Do not** add inline comments for obvious operations (e.g., `// increment counter`)
- Use `//` for single-line comments, avoid block comments `/* */` unless necessary
