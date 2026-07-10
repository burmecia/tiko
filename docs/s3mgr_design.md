## Summary: S3 Storage Manager for PostgreSQL (Tiko Project)

### Project Goal

Replace PostgreSQL's magnetic disk (`md`) storage with S3-backed block storage. The system uses a local file cache with S3 as the source of truth.

### Architecture Overview

Two Rust crates, both compiled as PostgreSQL shared libraries:

1. **`s3smgr` crate** — Implements the `smgr` interface. Functions (`s3_readv`, `s3_writev`, `s3_startreadv`, etc.) are called by PostgreSQL backends in place of `md*` functions. Currently delegates to `md*`; will be changed to submit async I/O requests to the shared memory ring.

2. **`s3worker` crate** — A background worker process registered via `_PG_init` / `RegisterBackgroundWorker`. Owns the I/O processing loop. Uses one PG process with an internal Tokio thread pool for concurrent S3 and local cache I/O.

### Shared Memory Design

Registered via `shmem_request_hook` + `shmem_startup_hook` + `ShmemInitStruct` (standard PG extension pattern).

**Multi-queue ring buffer:**

- **`NUM_IO_QUEUES = 8`** independent queues (power of 2).
- **`SLOTS_PER_QUEUE = 128`** slots per queue (power of 2), giving 1024 total in-flight capacity.
- Each backend has **queue affinity**: `primary_queue = MyProcNumber % NUM_IO_QUEUES`, with overflow to adjacent queues when full.
- Each `S3IoSlot` is **128-byte cache-line aligned** to prevent false sharing.

**Slot state machine (lock-free, atomic `u8`):**
```
Free → Filling → Submitted → InProgress → Completed → Free
```
- `Free → Filling`: Backend claims slot via `fetch_add` on queue head + CAS on state.
- `Filling → Submitted`: Backend publishes request with `Release` fence.
- `Submitted → InProgress`: s3worker main thread claims via atomic store.
- `InProgress → Completed`: s3worker main thread writes results + signals.
- `Completed → Free`: Backend harvests result and releases slot.

**Per-slot `ConditionVariable`** for completion notification (same pattern as PG 18's `PgAioHandle.cv`). One global `ConditionVariable` (`cv_work_available`) for waking s3worker when work is submitted. Generation counter per slot prevents ABA problems.

**Queue structure:** Each queue has a separated `head` (producer, atomic `fetch_add`) and `tail` (consumer, only s3worker writes) on different cache lines.

### Signaling / Notification

| Direction | Mechanism |
|---|---|
| Backend → s3worker (new request) | `ConditionVariableSignal(&ctl.cv_work_available)` |
| s3worker → Backend (completion) | `ConditionVariableBroadcast(&slot.cv)` |
| Tokio thread → s3worker main thread | `SetLatch(s3worker_latch)` (signal-safe, thread-safe) |
| Backend waiting for result | `ConditionVariableSleep(&slot.cv, ...)` loop checking `slot.state == Completed` |

### s3worker Internal Threading Model

**One PG background worker process, multiple Rust threads:**

- **Main thread** — Owns PGPROC / MyLatch / MyProcNumber. Polls all queues for Submitted requests, dispatches work to Tokio via `std::sync::mpsc::sync_channel`, drains completions, writes results to slots, calls `ConditionVariableBroadcast` (only safe from PG process context). Sleeps via `WaitLatch` with timeout fallback when idle.

- **Tokio runtime threads (4 worker + 8 blocking)** — Perform async S3 GET/PUT and local file cache I/O. Write block data directly to `buffer_ptr` (pinned shared buffer page) via `memcpy`. Send completions back via channel. Wake main thread via `SetLatch`.

**Thread safety rules:**
- Threads **CAN**: read/write shared memory atomics, `memcpy` to `buffer_ptr`, do file/network I/O, call `SetLatch`.
- Threads **CANNOT**: call `ConditionVariable*`, `LWLock*`, `ereport`/`elog`, `palloc`/`pfree` — these all depend on process-local PG state.

### Naming

- `s3worker` — the background worker crate. The process that performs S3 and local cache I/O.
- `s3smgr` — the smgr interface crate. Functions prefixed `s3_*` (e.g., `s3_readv`, `s3_writev`). Implements the PostgreSQL storage manager interface targeting S3.

### Key Design Decisions and Rationale

| Decision | Rationale |
|---|---|
| Multi-queue over single queue | Eliminates producer contention; backends spread across N queues |
| Affinity-based queue selection over random | Preserves per-backend ordering; better cache locality for sequential scans |
| Per-slot ConditionVariable over process Latch | Precise per-I/O wakeup; no thundering herd; same as PG 18 AIO design |
| Fixed ring in static shmem over DSM-per-request | Zero allocation overhead per I/O; survives crash/restart cleanly |
| 1 process + N threads over N BGWorker processes | PG BGWorker slots are scarce; S3 latency needs high concurrency; Tokio multiplexes hundreds of async tasks on few OS threads |
| `SetLatch` for thread→main-thread wakeup | Explicitly signal/thread-safe in PG; just does atomic + `kill(pid, SIGUSR1)` |
| Bounded channels for dispatch/completion | Provides backpressure when threads are saturated |