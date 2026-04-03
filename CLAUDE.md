# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tiko replaces PostgreSQL's magnetic disk (`md`) storage manager with S3-backed block storage. A local file cache sits in front of S3 as the source of truth. The project is written in Rust and compiled as PostgreSQL shared libraries (extensions).

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
Implements the PG `smgr` interface with `s3_*` functions (`s3_readv`, `s3_writev`, `s3_open`, etc.) that are registered as the storage manager. Two I/O paths:

- **Sync path** (all smgr functions except `s3_startreadv` and `s3_prefetch`): Calls `store_ops` directly in the backend process (`pread`/`pwrite` on local cache files). This is correct because sync smgr callers may pass backend-local memory pointers (`PageSetChecksumCopy` palloc'd pages, `LocalBufferBlockPointers`, stack-local `PGIOAlignedBlock`) that s3worker cannot access cross-process.
- **Async path** (`s3_startreadv` → `s3_io_perform_read`/`s3_io_perform_write`): Uses the shared-memory pipeline to s3worker. Buffers are always in shared memory (`BufferBlocks`), so cross-process access is safe. Also used by `s3_prefetch` for local cache warming (no `buffer_ptr`).

The `use_pipeline()` helper (combines `is_under_postmaster()` + `is_s3worker_alive()`) guards the async path in `aio.rs` and `prefetch.rs`, falling back to direct `store_ops` when unavailable (initdb, shutdown checkpoint, s3worker crash).

### `s3worker` — Background worker process (cdylib)
Loaded via `shared_preload_libraries`. `_PG_init` registers a background worker that calls `s3worker_main`. Internal structure:
- **`main_loop`** — the PG-process main thread: polls submit queue, dispatches to Tokio, sleeps via `WaitLatch`
- **`thread_pool`** — initializes Tokio runtime (4 async workers + 8 blocking threads)
- **`dispatcher`** — bounded `sync_channel` for work requests from main thread to Tokio (no completion channel — Tokio notifies backends directly via SetLatch)
- **`io_handler`** — async S3 GET/PUT and local cache read/write with SetLatch completion path
- **`io_control`** (pub) — the shared memory data structures, also used by `s3smgr`
- **`store_ops`** (pub) — synchronous block-level file I/O (`read_blocks`, `write_blocks`, `create_file`, etc.). Called directly by s3smgr sync functions and by s3worker's `io_handler`. Uses S3-style path layout: `{DataDir}/pico/{spc_oid}/{db_oid}/{rel_number}.{fork}`
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

**`use_pipeline()` guard** (used in `aio.rs` and `prefetch.rs`):
- `is_under_postmaster()` — false during initdb (both `--boot` and `--single` phases). Checked via PG's `IsUnderPostmaster` global (process-local, no shared memory).
- `is_s3worker_alive()` — uses `kill(pid, 0)` on PID stored in shared memory. Returns false if s3worker is dead (shutdown, crash).
- When `use_pipeline()` returns false, the AIO path falls back to direct `store_ops` calls (same as the sync smgr functions).

All sync smgr functions (`s3_readv`, `s3_writev`, `s3_extend`, `s3_create`, etc.) always call `store_ops` directly — no pipeline, no fallback needed. This handles initdb, shutdown checkpoint, and s3worker crash. Pages land in the local cache (persistent), WAL guarantees recoverability, and on next startup s3worker reconciles cache-dirty pages with S3.

### PG18 AIO Integration

PG18 introduces an asynchronous I/O subsystem built around `PgAioHandle` — a shared-memory object tracking each I/O operation through a state machine: `IDLE → HANDED_OUT → DEFINED → STAGED → SUBMITTED → COMPLETED_IO → COMPLETED_SHARED → COMPLETED_LOCAL → IDLE`. The smgr interface adds `startreadv` alongside the existing synchronous `readv`.

**Design: Custom `PgAioOp` for S3 reads**

Add `PGAIO_OP_S3_READV` to the `PgAioOp` enum and handle it in `pgaio_io_perform_synchronously()`. This plugs into PG18 AIO with a small, contained patch — no I/O method replacement, no custom callbacks needed.

**Data flow:**

```
Backend              PG IO Worker              s3worker (Tokio)          S3
═══════              ════════════              ════════════════          ══
s3_startreadv()
  set up iovec
  pgaio_io_stage(PGAIO_OP_S3_READV)
  submit to IO worker queue
  return immediately ✓
                     wakes up
(doing other work)   pgaio_io_perform_synchronously()
                       case PGAIO_OP_S3_READV:
                       submit_and_wait() ────→  claim slot, fill, publish
                       WaitLatch                dispatch to Tokio
                                                  cache hit → pread
                                                  cache miss ──────→ GET
                                                              ◄───── data
                                                              write cache
                                                memcpy to buffer_ptr
                                                mark_completed + SetLatch
                     WaitLatch returns ◄────────┘
                     result = nblocks * BLCKSZ
                     pgaio_io_process_completion()
                       callbacks (md_readv_complete, buffer_readv_complete)
                       ConditionVariableBroadcast
wref_wait returns ◄──┘
```

**Key design decisions:**

- **No `PGAIO_HF_SYNCHRONOUS` flag**: Without this flag, `pgaio_io_stage` submits the handle to PG's IO worker pool (not the backend). The IO worker calls `pgaio_io_perform_synchronously()` which hits our `PGAIO_OP_S3_READV` switch case. The backend remains non-blocking — true async from its perspective.
- **IO worker reuses `submit_and_wait()`**: The IO worker is a regular PG process with a valid `MyProcNumber` and `MyLatch`, counted in `MaxBackends`. It gets its own `BackendSlotPool` in `S3IoControl`. From Tiko's perspective, it's just another backend — claims slots, publishes requests, waits on latch. No Tokio runtime needed in the IO worker.
- **s3worker handles all S3 I/O**: The IO worker never touches S3 directly. It submits to the Tiko shared-memory pipeline; s3worker's Tokio runtime handles cache checks, S3 fetches, and writes data to the shared-memory buffer pages.
- **Bufmgr callbacks work unmodified**: `pgaio_io_process_completion` runs the normal callback chain (md byte validation, bufmgr `BM_VALID` flag setting, checksum checks) — no custom AIO callbacks needed.

**PG patch scope** (~6 switch cases + 1 function + enum update):

| File | Change |
|------|--------|
| `include/storage/aio.h` | Add `PGAIO_OP_S3_READV` to `PgAioOp`, update `PGAIO_OP_COUNT` |
| `aio_io.c` `pgaio_io_perform_synchronously` | Add case calling `s3_io_perform_read()` |
| `aio_io.c` `pgaio_io_get_op_name` | Add `"s3_readv"` |
| `aio_io.c` `pgaio_io_uses_fd` | Return `false` (no fd to track) |
| `aio_io.c` `pgaio_io_get_iovec_length` | Return from `op_data.read.iov_length` |
| `aio_io.c` | Add `pgaio_io_start_s3_readv()` (sets up op_data, calls `pgaio_io_stage`) |
| `aio_funcs.c` `pg_get_aios` | Display offset/length for debug view |
| `method_io_uring.c` | Not needed — IO worker path handles it via `pgaio_io_perform_synchronously` |

**Rust side** (`s3smgr`):

- `s3_startreadv`: Set up iovec from buffers, register md callbacks, set smgr target, call `pgaio_io_start_s3_readv()`
- `s3_io_perform_read` (`extern "C"`): Decode relation info from `target_data`, call `submit_and_wait()` with iov buffer addresses, return `nblocks * BLCKSZ` or `-errno`

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

## PITR Support Design

### High-Level Architecture

Since Tiko already uses S3-backed storage for data files, PITR primarily requires:
1. **WAL archiving to S3** with lifecycle policies
2. **Metadata tracking** for recovery points
3. **Restore coordination** using PostgreSQL's built-in recovery

### Implementation Plan

#### Phase 1: WAL Archiving to S3

**1.1 Add WAL Archiver Module** (`s3worker/src/wal_archiver.rs`):

```rust
//! WAL archiver - uploads completed WAL segments to S3
//!
//! Integrates with PostgreSQL's archive_command to upload WAL files to S3
//! with automatic lifecycle management (7-day retention).

use std::path::Path;
use tokio::fs;
use aws_sdk_s3::Client as S3Client;

pub struct WalArchiver {
    s3_client: S3Client,
    bucket: String,
    prefix: String,  // e.g., "wal/{cluster_id}/"
    retention_days: u32,
}

impl WalArchiver {
    /// Archive a WAL segment to S3
    pub async fn archive_segment(&self, wal_path: &Path) -> Result<(), String> {
        let wal_filename = wal_path.file_name()
            .ok_or("Invalid WAL path")?
            .to_str()
            .ok_or("Invalid filename")?;
        
        // S3 key: wal/{cluster_id}/{timeline}/{segment}
        let s3_key = format!("{}{}", self.prefix, wal_filename);
        
        let body = fs::read(wal_path).await
            .map_err(|e| format!("Failed to read WAL: {}", e))?;
        
        // Upload to S3 with lifecycle tag
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(body.into())
            .tagging(&format!("retention-days={}", self.retention_days))
            .send()
            .await
            .map_err(|e| format!("S3 upload failed: {}", e))?;
        
        Ok(())
    }
    
    /// Restore a WAL segment from S3
    pub async fn restore_segment(&self, wal_filename: &str, dest_path: &Path) -> Result<(), String> {
        let s3_key = format!("{}{}", self.prefix, wal_filename);
        
        let response = self.s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await
            .map_err(|e| format!("S3 download failed: {}", e))?;
        
        let body = response.body.collect().await
            .map_err(|e| format!("Failed to read S3 body: {}", e))?
            .into_bytes();
        
        fs::write(dest_path, &body).await
            .map_err(|e| format!("Failed to write WAL: {}", e))?;
        
        Ok(())
    }
}
```

**1.2 PostgreSQL Configuration** (`postgresql.conf`):

```ini
# Enable WAL archiving
wal_level = replica
archive_mode = on
archive_timeout = 300  # Force archive every 5 minutes

# Archive command - calls Tiko's WAL archiver
archive_command = '/path/to/pico_archive %p %f'

# For 7-day PITR, keep enough WAL locally too
max_wal_size = 10GB
min_wal_size = 1GB
wal_keep_size = 1GB
```

**1.3 Archive Command Helper** (`pico_archive` binary):

```rust
// s3worker/src/bin/pico_archive.rs
//! Archive command wrapper
//! Usage: pico_archive <wal_path> <wal_filename>

use tokio::runtime::Runtime;
use s3worker::wal_archiver::WalArchiver;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: pico_archive <wal_path> <wal_filename>");
        std::process::exit(1);
    }
    
    let wal_path = &args[1];
    let wal_filename = &args[2];
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let archiver = WalArchiver::from_env();
        match archiver.archive_segment(wal_path.as_ref()).await {
            Ok(_) => {
                // PostgreSQL expects exit code 0 on success
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("Archive failed: {}", e);
                std::process::exit(1);
            }
        }
    });
}
```

#### Phase 2: Continuous Base Backup Tracking

**2.1 Add Snapshot Metadata Tracker** (`s3worker/src/snapshot.rs`):

```rust
//! Snapshot metadata for PITR
//!
//! Since Tiko data files are already in S3-style local cache, we track
//! "recovery points" rather than full backups.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub struct RecoveryPoint {
    pub timestamp: i64,  // Unix timestamp
    pub wal_lsn: String,  // LSN at snapshot time (e.g., "0/3000000")
    pub checkpoint_lsn: String,  // Last checkpoint LSN
    pub timeline_id: u32,
    pub cache_generation: u64,  // Cache generation number
}

pub struct SnapshotTracker {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,  // e.g., "snapshots/{cluster_id}/"
}

impl SnapshotTracker {
    /// Record a recovery point (called periodically, e.g., every hour)
    pub async fn record_recovery_point(&self, point: &RecoveryPoint) -> Result<(), String> {
        let key = format!("{}recovery_point_{}.json", self.prefix, point.timestamp);
        
        let json = serde_json::to_string_pretty(point)
            .map_err(|e| format!("JSON error: {}", e))?;
        
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(json.into_bytes().into())
            .send()
            .await
            .map_err(|e| format!("S3 upload failed: {}", e))?;
        
        Ok(())
    }
    
    /// List available recovery points within retention window
    pub async fn list_recovery_points(&self, since: i64) -> Result<Vec<RecoveryPoint>, String> {
        // List S3 objects with prefix, filter by timestamp
        // Parse JSON and return sorted list
        todo!()
    }
}
```

**2.2 Background Snapshot Task** (add to `s3worker`):

```rust
// In s3worker/src/main_loop.rs or new file

use tokio::time::{interval, Duration};

/// Periodic task to record recovery points
pub async fn snapshot_task(tracker: Arc<SnapshotTracker>) {
    let mut interval = interval(Duration::from_secs(3600)); // Every hour
    
    loop {
        interval.tick().await;
        
        // Get current checkpoint LSN from PostgreSQL
        let recovery_point = unsafe {
            let lsn = pgsys::xlog::GetInsertRecPtr();
            let checkpoint_lsn = pgsys::xlog::GetLastCheckpointRecPtr();
            
            RecoveryPoint {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                wal_lsn: format!("{:X}/{:X}", 
                    (lsn >> 32) as u32, 
                    (lsn & 0xFFFFFFFF) as u32),
                checkpoint_lsn: format!("{:X}/{:X}",
                    (checkpoint_lsn >> 32) as u32,
                    (checkpoint_lsn & 0xFFFFFFFF) as u32),
                timeline_id: pgsys::xlog::GetRecoveryTargetTLI(),
                cache_generation: CacheControl::global().generation(),
            }
        };
        
        if let Err(e) = tracker.record_recovery_point(&recovery_point).await {
            eprintln!("Failed to record recovery point: {}", e);
        }
    }
}
```

#### Phase 3: Cache Sync to S3

**Status: checkpoint flush is implemented; real S3 client is future work.**

The cache currently uses local S3-sim files as the backing store
(`{DataDir}/pico/{spc_oid}/{db_oid}/{rel_number}.{fork}`). Dirty cache
blocks are flushed to these files at checkpoint time. When a real S3 client
is added, `store_ops::write_blocks` and `read_blocks` will be replaced with
S3 PUT/GET calls.

**3.1 Checkpoint Flush** (`s3smgr/src/checkpoint.rs`) — **already implemented**:

```rust
/// Called from CheckPointGuts() in xlog.c after CheckPointBuffers().
/// Flushes all dirty cache chunks to the S3-sim backing files before
/// the checkpoint WAL record is written.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn s3_checkpoint_flush() {
    if !S3IoControl::is_initialized() {
        return;
    }
    S3IoControl::get().cache.flush_all_dirty_chunks();
}
```

`flush_all_dirty_chunks` scans every cache slot, spins to pin each dirty
slot exclusively, calls `flush_dirty_chunk` (which reads each dirty block
from the cache file and writes it to the backing relation file via
`store_ops::write_blocks`), clears `dirty_blocks`, then unpins. This is a
synchronous, blocking flush — it runs on the checkpointer process main thread
before the checkpoint WAL record is written.

`s3_shutdown` (smgr shutdown hook) is **intentionally empty**: by the time it
fires, the shutdown checkpoint has already flushed all dirty chunks.

**3.2 Future: Replace S3-sim with real S3** (modify `core/src/store_ops.rs`):

Chunk granularity is 256 KB (32 blocks). The S3 key layout mirrors the local
path structure but uses `chunk_id` as the object key suffix:

```
S3 key:   {spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}
Local:    {DataDir}/pico/{spc_oid}/{db_oid}/{rel_number}.{fork}
```

`read_blocks` becomes a S3 GET for the chunk containing the requested block;
`write_blocks` (called from `flush_dirty_chunk` on eviction or checkpoint)
becomes a S3 PUT. The cache layer above is unchanged — it operates in terms
of `read_blocks`/`write_blocks` regardless of the backing store.

#### Phase 4: Recovery (Restore) Process

**4.1 Restore Command Helper** (`pico_restore` binary):

```rust
// s3worker/src/bin/pico_restore.rs
//! Restore command for archive recovery
//! Usage: pico_restore %f %p

use tokio::runtime::Runtime;
use s3worker::wal_archiver::WalArchiver;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: pico_restore <wal_filename> <dest_path>");
        std::process::exit(1);
    }
    
    let wal_filename = &args[1];
    let dest_path = PathBuf::from(&args[2]);
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let archiver = WalArchiver::from_env();
        match archiver.restore_segment(wal_filename, &dest_path).await {
            Ok(_) => std::process::exit(0),
            Err(e) => {
                eprintln!("Restore failed: {}", e);
                std::process::exit(1);
            }
        }
    });
}
```

**4.2 Recovery Configuration**:

To restore to a point in time (within last 7 days):

```bash
# 1. Stop the database
pg_ctl stop

# 2. Clear local cache (will be rebuilt from S3)
rm -rf $PGDATA/pico/cache*

# 3. Configure recovery
cat >> $PGDATA/postgresql.conf <<EOF
restore_command = '/path/to/pico_restore %f %p'
recovery_target_time = '2026-02-20 10:00:00'
recovery_target_action = 'promote'
EOF

touch $PGDATA/recovery.signal

# 4. Start recovery
pg_ctl start

# PostgreSQL will:
# - Read checkpoint from pg_control
# - Download WAL from S3 via restore_command
# - Replay WAL until target time
# - On cache miss, s3worker downloads blocks from S3
```

### S3 Lifecycle Policy (7-Day Retention)

Configure S3 bucket lifecycle rules:

```json
{
  "Rules": [
    {
      "Id": "ExpireOldWAL",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "wal/"
      },
      "Expiration": {
        "Days": 7
      }
    },
    {
      "Id": "ExpireOldSnapshots",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "snapshots/"
      },
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

### Implementation Checklist

```rust
// Add to Cargo.toml dependencies:
// [dependencies]
// aws-config = "1.0"
// aws-sdk-s3 = "1.0"
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
```

**Files to create:**
- [ ] `s3worker/src/wal_archiver.rs` - WAL upload/download
- [ ] `s3worker/src/snapshot.rs` - Recovery point tracking
- [ ] `s3worker/src/s3_client.rs` - S3 client initialization
- [ ] `s3worker/src/bin/pico_archive.rs` - Archive command binary
- [ ] `s3worker/src/bin/pico_restore.rs` - Restore command binary

**Files to modify:**
- [ ] `s3worker/src/lib.rs` - Export new modules
- [ ] `s3worker/src/main_loop.rs` - Add snapshot task
- [ ] `core/src/store_ops.rs` - Add S3 sync functions
- [ ] `postgres/src/test/modules/test_pico/test_pico.c` - Add PITR tests

### Testing Plan

```sql
-- 1. Setup test database
CREATE TABLE test_pitr (id int, data text, ts timestamp default now());
INSERT INTO test_pitr VALUES (1, 'before', now());

-- 2. Create restore point
SELECT pg_create_restore_point('before_delete');

-- 3. Make changes to recover from
INSERT INTO test_pitr VALUES (2, 'deleted', now());
DELETE FROM test_pitr WHERE id = 1;

-- 4. Perform PITR to before_delete
-- (follow recovery steps above)

-- 5. Verify recovery
SELECT * FROM test_pitr;  -- Should show id=1, not id=2
```

### Advantages of This Design

1. **Minimal PostgreSQL patches** - Uses standard `archive_command`/`restore_command`
2. **Leverages existing infra** - s3worker Tokio runtime handles S3 I/O
3. **Checkpoint-count retention** - Retention is activity-based, not time-based (see GC Policy below)
4. **No full backups needed** - Tiko's block-level S3 storage + WAL = complete PITR
5. **Fast recovery** - Only download blocks accessed during WAL replay

### GC Policy (Retention Enforcement)

GC is run by `tikod` (control plane), not by `s3worker`. The entry point is
`enforce_retention_org(org_id, max_checkpoints)` in `tikod/src/gc.rs`.

**Retention is checkpoint-count-based, not time-based.**
A time-based cutoff would delete data from inactive projects (a paused project
still has a valid current state). Counting checkpoints ties retention to database
activity: a busy project fills the window quickly; an idle one keeps all its history
indefinitely until it generates enough new checkpoints to trigger cleanup.

Policy: keep the last `max_checkpoints` (default 500) delta manifests per project.

**Cutoff derivation:**
```
all_delta_lsns = sorted ascending list of delta manifest LSNs for the project
if len > max_checkpoints:
    cutoff_lsn = all_delta_lsns[len - max_checkpoints]
else:
    skip (nothing to GC)
```

**Four GC phases (run in order):**

| Phase | What is deleted |
|---|---|
| Delta manifest GC | Manifests + `pg_state.tar.zst` with LSN < `cutoff_lsn` |
| Base manifest GC | All bases with `base_lsn < cutoff_lsn`, except the newest one (needed as recovery anchor) |
| WAL GC | Segments whose end LSN is entirely before `cutoff_lsn` |
| Chunk GC | Versioned `{lsn_hex}` objects not referenced by any retained base or delta manifest; zero branch is permanent |

In a multi-server cluster, GC acquires a per-org GC lease (`{org}/gc_lease.json`)
before running — only one server runs GC for a given org at a time.
