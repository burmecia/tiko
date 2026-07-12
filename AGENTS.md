# AGENTS.md

Guide for AI agents working in this repo. `CLAUDE.md` has the full architecture
write-up — read it for deep context (crate boundaries, smgr/worker IPC, AIO
integration, COW branching, PITR). This file captures only what you'd likely get
wrong without being told.

## Build & test commands

```bash
./scripts/build_postgres.sh   # build vendored/patched PG into target/pg-install (run once)
./scripts/run_test.sh         # primary smoke test: builds smgr + PG + worker, runs make check
```

Other suites: `run_large_data_test.sh` (large data), `run_test4.sh`, `run_pg_test.sh`
(PG regression), `run_pitr_test.sh` (PITR), `run_branch_test.sh` (COW branching).

`run_test.sh` is the **integration test** and encodes a required build order:
build `smgr` (staticlib, linked into PG) → `make && make install` in `postgres/`
→ build `worker` (cdylib) → copy `libtikoworker.{dylib,so}` into
`postgres/src/test/modules/test_tiko/worker/` → `make check` there with
`shared_preload_libraries=libtikoworker`. Don't reorder these.

Unit tests run per-crate with `cargo test -p <crate>` (e.g. `core`, `pgsys`,
`tikoguest`). `tikod`'s integration tests (`tikod/tests/`) only build on Linux.

## Gotchas an agent will hit

- **Do NOT run `cargo clippy`** on `core`/`smgr`/`worker`/`cli`/`pgsys`. Pre-existing
  lint errors in the hand-written FFI bindings (`pgsys`) abort the build. Verify
  changes with `cargo build` / `cargo test` instead.
- **`tikod` does NOT build on macOS** (it's Linux/Firecracker-only — `default_vmm`
  is `#[cfg(target_os = "linux")]` but `main.rs` imports it unconditionally). The
  CLAUDE.md mention of an Apple Virtualization Framework macOS backend is stale;
  `tikod/src/vmm/` contains only `firecracker.rs`. On macOS you can build/test
  `core`, `pgsys`, `smgr`, `worker`, `cli`, `tikoguest`, but not `tikod`.
- **`build_postgres.sh` is Linux-only** (`sudo apt-get`). On macOS, configure
  `postgres/` manually once before running `run_test.sh`.
- **Required env vars**: `run_test.sh` sets `TIKO_ORG_ID`/`TIKO_DB_ID`/
  `TIKO_PROJECT_ID`/`TIKO_PITR_INTERVAL_SECS`. It also `unset`s
  `TIKO_STORAGE_ROOT`/`TIKO_LOCAL_PATH` (the smoke test uses defaults). In a VM
  these come from `/var/lib/postgresql/tiko.env` (see `scripts/tiko_env.sh`).
- **macOS System V shmem leak**: `run_test.sh` cleans orphaned `ipcs -m` segments
  first because macOS caps `kern.sysv.shmmni` at 32 and each killed postgres leaks
  one. If `make check` hangs/fails on shmem, clear them manually.

## Architecture facts that aren't obvious from filenames

- **Storage backend today is `S3Sim`** (`core/src/io/storage/s3_sim.rs`) — a
  local-filesystem zstd-compressed stand-in. It is **not just a test double**: in
  production its root is an NFSv4.2-mounted S3 Files share, so this is the real
  storage path. `core/src/io/storage/s3.rs` is a `todo!()` stub (real networked S3
  is not implemented).
- **No garbage collector exists.** `worker::tasks::compactor` only folds
  superseded timeline segments into a base manifest and deletes the redundant
  segment objects. There is no chunk/delta-manifest/WAL/orphan GC. Org soft-delete
  (`OrgMeta.deleted_at`) is tracked but nothing reclaims the data.
- **crate-type matters**: `smgr` (`tikosmgr`) = `staticlib`+`rlib`, linked *into*
  postgres at build time. `worker` (`tikoworker`) = `cdylib`+`rlib`, loaded at
  runtime via `shared_preload_libraries`. `cli`/`tikod`/`tikoguest` are binaries.
- **`tikod` and `tikoguest` have NO Rust dependency on `core`/`smgr`/`worker`** —
  they orchestrate by spawning CLI binaries / `pg_ctl` and talking HTTP.
- **Two smgr I/O paths**: sync smgr functions call `core::ops` directly in the
  backend (correct because callers may pass backend-local memory the worker can't
  reach cross-process); the async path (`tiko_startreadv`) goes through the
  shmem submit-queue to `tikoworker`, with a fallback to direct `core::ops` calls
  when the worker is unavailable (initdb, shutdown checkpoint, worker crash).
- **PG18 is patched** with custom AIO opcodes (`PGAIO_OP_TIKO_READV`/`WRITEV`) in
  the vendored `postgres/` submodule. The submodule must be initialized.

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
- `cli/legacy/` is dead code (commented out of `Cargo.toml`'s `[[bin]]` list).

## Notes that differ from defaults

- `psql` selects a database via `options='-c tiko.endpoint=vm-N'` (routed by `tikod`).
- There is no `rust-toolchain.toml`; minimum is Rust **1.88, edition 2024**.
- No CI workflows are defined; `./scripts/run_test.sh` is the canonical check.
