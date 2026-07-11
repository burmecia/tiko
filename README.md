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
  backed by Firecracker on Linux (production).
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

Clone Tiko repo

```bash
git clone https://github.com/burmecia/tiko.git
cd tiko
```

Build Postgres:

```bash
./scripts/build_postgres.sh
```

## Postgres compute-storage separation

Run the smoke test

```bash
./scripts/run_test.sh
```

Other test scripts you can try run:

- `./scripts/run_test2.sh` -- Large data test
- `./scripts/run_pg_test.sh` -- Postgres regression test
- `./scripts/run_pitr_test.sh` -- PITR test
- `./scripts/run_branch_test.sh` -- Branching test

## MicroVM orchestration

### Build Firecracker

Tiko runs each PostgreSQL database inside a Firecracker microVM. The `firecracker` binary must be available on the host (via the `FIRECRACKER_BIN` environment variables).

- A KVM-enabled Linux host is required (`/dev/kvm`).
- Build Firecracker needs Docker to be installed (https://docs.docker.com/engine/install/ubuntu/)

```bash
git clone https://github.com/firecracker-microvm/firecracker
cd firecracker
tools/devtool build

export FIRECRACKER_BIN=$(realpath ./build/cargo_target/x86_64-unknown-linux-musl/debug/firecracker)
```

### Set up AWS S3 Files

Tiko uses [AWS S3 Files](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files.html) as its object-storage backend. The guest VM mounts an S3 Files file system via NFSv4.2 (TLS + IAM), requiring:

- An S3 Files file system and mount target in the host's AZ.
- Static IAM credentials for the guest (the guest has no instance metadata
  service). Store them in `tikod/assets/s3files-creds.env` (gitignored).

See `tikod/docs/s3-files-setup.md` for the full setup runbook.

Once the S3 Files is ready, update S3 Files config in `scripts/mount_s3files_vm.sh` to let guest know where to access the S3 Files.

Copy S3 Files creds file and fill in with your AWS access credentials:

```bash
cp ./scripts/s3files-creds.env.sample ./tikod/assets/s3files-creds.env
```

### Prepare MicroVM, tikod and initialise seed db

Prepare kernel and rootfs:

```bash
./scripts/download_kernel.sh
./scripts/build_initramfs.sh
./scripts/create_rootfs.sh
```

Start tikod server:

```bash
RUST_LOG=tikod=debug cargo run -p tikod
```

Open another terminal and run `vmtop` to monitor the VM swarm:

```bash
./scripts/vmtop.py
```

Open another terminal again to create seed db:

```bash
# create an initial vm
curl -X POST localhost:9000/vms/provision

# create the seed db (this will take several minutes, be patient)
curl -X POST localhost:9000/vms/vm-0/db/init

# start the seed db
curl -X POST localhost:9000/vms/vm-0/db/start

# add some seed data
psql -d "host=localhost user=postgres dbname=postgres options='-c tiko.endpoint=vm-0'" \
    -c 'create table tt(a int); insert into tt values(123);'

# take a base backup of the seed db
curl -X PUT localhost:9000/vms/vm-0/branch/backup
```

Ok now, all the preparation works are done.

### Scale to zero

Let's create 8 databases:

```bash
./scripts/stress_create_dbs.sh 8
```

Now go back to `vmtop` terminal and watch each db's status change.

- `running` - normal status
- `paused` - db is paused but still in memory, 2 minutes without activities from `running`
- `frozen` - db is destroyed after snapshot, 2 minutes without new connection from `paused`

When a db is in `paused` or `frozen`, open a new psql connection will bring it back to `running`.

```bash
# wake up vm-2
psql -d "host=localhost user=postgres dbname=postgres options='-c tiko.endpoint=vm-2'" -c 'select * from tt'

# wake up vm-5
psql -d "host=localhost user=postgres dbname=postgres options='-c tiko.endpoint=vm-5'" -c 'select * from tt'
```

Notes:

- we use connection options, like `tiko.endpoint=vm-2`, to distiguish which db to wake up.
- for demo purpose, we currently use fixed-time inactivity checking policy, that is, it will always be treated inactive after 2 minutes regardless having active connection or not.

### Copy-on-write(COW) database branching

### PITR

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
