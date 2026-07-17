# AGENTS.md

Guide for AI agents working in this repo. `CLAUDE.md` has the full architecture
write-up — read it for deep context (crate boundaries, smgr/worker IPC, AIO
integration, COW branching, PITR, and the tikovm microVM platform). This file
captures only what you'd likely get wrong without being told.

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

### tikovm (independent of Postgres — no PG submodule needed)

```bash
cargo build -p tikovm-protocol -p tikovm-host -p tikovm-guest
cargo test  -p tikovm-protocol -p tikovm-host -p tikovm-guest   # unit tests, no KVM
cargo clippy -p tikovm-protocol -p tikovm-host -p tikovm-guest  # clippy IS fine on these
./scripts/tikovm/run_e2e.sh    # full E2E on real KVM/Firecracker (provision→scale-to-zero→
                               #   lifecycle→crash recovery→metrics; 17 PASS/FAIL checks)
```

## Gotchas an agent will hit

- **Do NOT run `cargo clippy`** on `core`/`smgr`/`worker`/`cli`/`pgsys`. Pre-existing
  lint errors in the hand-written FFI bindings (`pgsys`) abort the build. Verify
  changes with `cargo build` / `cargo test` instead. (Exception: clippy is **fine**
  on the `tikovm-*` crates — they have no `pgsys` dependency.)
- **`tikod` builds on macOS but cannot run VMs.** `default_vmm` has a
  `#[cfg(target_os = "linux")]` Firecracker branch and a `#[cfg(not(target_os = "linux"))]`
  branch that returns an `UnsupportedVmm` stub (every `Vmm` op errors with
  `VmmError::Backend`). So on macOS `tikod` compiles/starts (useful for working on
  config/HTTP API/proxy) but no VM can be created. The CLAUDE.md mention of an Apple
  Virtualization Framework macOS backend is stale; `tikod/src/vmm/` ships only
  `firecracker.rs` (Linux prod) plus the in-`mod.rs` `UnsupportedVmm` stub. On macOS you
  can build/test `core`, `pgsys`, `smgr`, `worker`, `cli`, `tikoguest`, and now `tikod`
  (VM ops aside).
- **`build_postgres.sh`** installs deps via `apt-get` on Linux and `brew` on
  macOS (auto-detected). On macOS it also checks for Xcode Command Line Tools.
- **Required env vars**: `run_test.sh` sets `TIKO_ORG_ID`/`TIKO_DB_ID`/
  `TIKO_PROJECT_ID`/`TIKO_PITR_INTERVAL_SECS`. It also `unset`s
  `TIKO_STORAGE_ROOT`/`TIKO_LOCAL_PATH` (the smoke test uses defaults). In a VM
  these come from `/var/lib/postgresql/tiko.env` (see `scripts/tiko_env.sh`).
- **macOS System V shmem leak**: `run_test.sh` cleans orphaned `ipcs -m` segments
  first because macOS caps `kern.sysv.shmmni` at 32 and each killed postgres leaks
  one. If `make check` hangs/fails on shmem, clear them manually.
- **`tikovm-hostd` compiles anywhere but only runs VMs on Linux + KVM.**
  `default_vmm()` returns `FirecrackerVmm` under `#[cfg(target_os="linux")]` and a
  `StubBackend` (every `Vmm` op errors with `VmmError::Backend`) elsewhere. So on
  macOS you can build/test the `tikovm-*` crates (unit tests + `MockVmm` API tests
  pass) but cannot create real VMs. Real runs need `/dev/kvm` + the `FIRECRACKER_BIN`
  binary + passwordless sudo (for TAP/iptables/mount).
- **`tikovm-*` crates have NO dependency on `core`/`smgr`/`worker`/`pgsys`** —
  they are cleanly liftable into a standalone repo. The only inter-crate Cargo edge
  is `tikovm-host`/`tikovm-guest` → `tikovm-protocol`.
- **`run_e2e.sh` rebuilds the echo rootfs each run.**
  `scripts/tikovm/build_echo_rootfs.sh` loop-mounts the ubuntu base rootfs and
  injects the freshly-built `tikovm-guestd` + `echo-server` + `workload.toml`, so a
  stale rootfs is never the bug. It expects assets under `tikod/assets/`
  (`vmlinux-6.1`, `ubuntu-24.04-rootfs.ext4`, `tiko-initramfs.cpio.gz`).

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
- **`tikovm` is a separate platform** (3 crates: `tikovm-protocol`/`-host`/`-guest`)
  generalized from `tikod`/`tikoguest`. It has its own daemon (`tikovm-hostd`), its
  own guest agent (`tikovm-guestd`), and its own design doc
  (`docs/tikovm-design.md`). Idle detection is **guest-authoritative**: the guest's
  `IdleEvaluator` signals the host over vsock to `freeze` (pause→snapshot→destroy);
  an inbound proxy connection triggers `restore`+`resume` (single-flight). `suspend`
  in tikovm = snapshot+destroy (no checkpoint file beyond the snapshot).
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
- The `tikovm` proxy routes via `X-Tiko-Endpoint: vm-N` header (or a configured
  default VM); `tikovm-hostd` is started with `--proxy-default-vm`/`--proxy-default-port`.
- There is no `rust-toolchain.toml`; minimum is Rust **1.88, edition 2024**.
- No CI workflows are defined; `./scripts/run_test.sh` is the canonical PG check,
  `./scripts/tikovm/run_e2e.sh` the canonical tikovm check.
