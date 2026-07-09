# Tiko

**Serverless PostgreSQL on S3-backed storage, orchestrated by microVMs.**

Tiko runs each PostgreSQL database inside its own Firecracker microVM and stores
its data on S3-compatible object storage. A local file cache sits in front of S3
as the source of truth for hot blocks, while a custom storage manager replaces
PostgreSQL's magnetic-disk (`md`) layer entirely. The result is a database that
scales to zero (freeze a VM to a snapshot, restore on first connection), supports
point-in-time recovery and copy-on-write branching, and keeps per-database cost
close to raw object storage.

The project is written in Rust and compiled as PostgreSQL shared libraries
(extensions) plus standalone binaries. PostgreSQL is vendored as a git submodule
under `postgres/`, with a small patch set integrating Tiko's storage manager and
PG18's asynchronous I/O subsystem.

---

## Most important features

### S3-backed block storage with a local cache
- A custom storage manager (`smgr`) implements PostgreSQL's `smgr` interface with
  `s3_*` functions (`s3_readv`, `s3_writev`, `s3_open`, …), so the database never
  touches a local magnetic disk for relation files.
- Relation data is split into **256 KB chunks** (32 × 8 KB blocks). Chunks are
  content-addressed, versioned per checkpoint LSN, and stored under
  `{storage_root}/s3sim/{org}/{db}/chunks/`.
- A **shared-memory block cache** fronts S3: hot blocks are served from a
  per-database local cache, dirty chunks are flushed at checkpoint time, and a
  background worker reconciles cache-dirty pages with object storage.

### Asynchronous I/O pipeline
- Sync smgr paths call block I/O directly in the backend process (safe for
  backend-local memory).
- Async reads go through a **shared-memory pipeline** to a Tokio-powered
  background worker that handles cache hits (`pread`), S3 fetches on misses, and
  writes — completing backends via `SetLatch` with zero contention slot pools.
- Integrates with **PostgreSQL 18's AIO subsystem** via a custom `PGAIO_OP_S3_READV`
  op, keeping the backend non-blocking.

### Compute control plane (`tikod`)
- **microVM lifecycle**: provision / start / pause / resume / snapshot / destroy,
  backed by Firecracker on Linux (production) and Apple Virtualization Framework on
  macOS (development).
- **PG wire-protocol proxy** with **wake-on-connect**: a frozen VM is transparently
  restored on the first client connection, so databases can scale to zero without
  dropping clients.
- **Warm-pause + cold-freeze**: idle VMs are warm-paused (TCP connections survive),
  then snapshot-and-destroyed after an idle window to reclaim RAM/CPU.
- A single HTTP control API fans every operation out to the right guest agent.

### Point-in-time recovery (PITR)
- WAL is streamed to object storage in near-realtime via the PostgreSQL physical
  streaming-replication protocol, uploaded as 256 KB chunk objects and sealed
  segment objects.
- Periodic **base backups** (`pg_basebackup`) are uploaded alongside base manifests
  (chunk-reference maps), bounding WAL replay during recovery.
- `tiko_pitr recover` restores the latest usable backup, replays WAL to a target
  time or LSN, and promotes — all automated.

### Copy-on-write database branching
- A new database can be created from an **org-level bootstrap pack** in a single
  call (`POST /dbs`): provision a VM, restore the pack, start Postgres.
- Branches share chunks with their parent through copy-on-write `ChunkRef`s, so a
  fork costs only the new/modified blocks.
- `tiko_branch` provides `backup` / `restore` / `restart` for branch lifecycle.

### Compaction & retention (GC)
- A **compactor** periodically folds timeline segments into a new base manifest,
  advancing the base checkpoint and deleting covered segment files.
- Retention is **checkpoint-count-based** (not time-based): inactive projects keep
  their full history, while busy ones age out old manifests, WAL, and orphan
  chunks past a configurable window.

### Observability & guest agent (`tikoguest`)
- An in-guest agent exposes HTTP routes for Postgres control (`/pg/*`), PITR
  (`/pitr/*`), branching (`/branch/*`), and health.
- A scaler loop pushes metrics to `tikod`, evaluates idle policy, and requests
  pause when a database goes quiet — driving the scale-to-zero behavior.

---

## Repository layout

```
tiko/
├── postgres/        # vendored PostgreSQL (git submodule) + Tiko patches
├── firecracker/     # vendored Firecracker microVMM (git submodule)
├── pgsys/           # hand-written PostgreSQL FFI bindings (extern "C")
├── core/            # storage layer: chunks, manifests, store, I/O engine
├── smgr/            # tikosmgr — PostgreSQL storage manager (s3_* functions)
├── worker/          # tikoworker — background worker (AIO, WAL receiver, compactor)
├── cli/             # operator CLIs: tiko_pitr, tiko_branch, tiko_restore, tiko_tlseg_viewer
├── tikod/           # control plane: proxy, node/VMM lifecycle, HTTP API
└── tikoguest/       # in-VM agent: pg control, observability, scaler, freeze
```

### Dependency chain

```
pgsys ──→ core ──→ smgr (tikosmgr) ──→ postgres
              └────→ worker (tikoworker) ──→ postgres
                  └→ cli (tiko_pitr, tiko_branch, …)
```

`tikod` and `tikoguest` are standalone binaries with no internal Rust deps: they
orchestrate Postgres and the storage layer by spawning CLIs / `pg_ctl` and over
HTTP, not by linking the storage crates.

---

## Getting started

Requires **Rust 1.88+** (edition 2024) and the PostgreSQL submodule initialized:

```bash
git submodule update --init postgres
```

### Build & run the storage tests

```bash
./run_test.sh
```

This builds the Rust crates, compiles the patched PostgreSQL, and runs the
`test_pico` regression module:

```bash
cd postgres/src/test/modules/test_pico && make check \
  PG_TEST_INITDB_EXTRA_OPTS='-c log_min_messages=debug1 -c shared_preload_libraries=libs3worker'
```

### Build individual crates

```bash
cargo build -p smgr      # storage manager (loaded into postgres)
cargo build -p worker    # background worker (cdylib, shared_preload_libraries)
cargo build -p tikod     # control plane binary
cargo build -p tikoguest # guest agent binary
cargo build -p cli       # operator CLIs (tiko_pitr, tiko_branch, …)
```

### Run the control-plane tests

```bash
cargo test -p tikod
cargo test -p tikoguest
cargo test -p core
```

---

## Architecture

```
                              ┌──────────────────────────────────────────┐
   SQL client ──PG wire──→    │  tikod (control plane)                    │
                              │   proxy ─→ control ─→ node ─→ Vmm backend │
                              └───────────────┬──────────────────────────┘
                                              │ HTTP (guest IP:9000)
                                  ┌───────────▼───────────┐
                                  │  tikoguest (in-VM)     │
                                  │   pg_ctl / scaler      │
                                  └───────────┬───────────┘
                                              │ shared_preload_libraries
                                  ┌───────────▼───────────┐
                                  │  PostgreSQL + Tiko ext │
                                  │  smgr (tikosmgr)       │
                                  │  worker (tikoworker)   │
                                  └───────────┬───────────┘
                                              │ chunks / WAL / manifests
                                  ┌───────────▼───────────┐
                                  │  S3-compatible storage │
                                  │  (local cache in front)│
                                  └───────────────────────┘
```

- The **storage manager** turns every block read/write into chunk-level object
  operations, transparent to SQL.
- The **worker** owns the async pipeline, streams WAL, and runs compaction.
- **tikod** owns VM lifecycle and proxies client traffic, freezing/restoring VMs
  on demand so idle databases cost nothing.

---

## License

Apache-2.0.
