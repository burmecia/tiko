# WAL Streaming Design

## Problem

File-based archiving (`archive_command`) only uploads a WAL segment after the segment is **complete** — after PostgreSQL switches away from it. A segment is 16 MB; with `archive_timeout = 0` a low-traffic database may never switch segments at all.

This creates an **archiving gap**: when a branch is created mid-segment, the parent's current segment has never been archived. The branch's recovery process needs WAL from that segment to reach a consistent recovery point, calls `tiko_restore`, gets exit 1, and fails with:

```
FATAL: requested recovery stop point is before consistent recovery point
```

## Solution: Replace `archive_command` with Physical Replication Streaming

Tiko runs an in-process WAL streaming receiver (Tokio task inside s3worker) that connects to the local postmaster via the **physical streaming replication protocol** and uploads WAL chunks to the sim/S3 store in near-realtime — before segment completion. **`archive_command` is removed entirely.**

A **physical replication slot** with `RESERVE_WAL` prevents the primary from recycling WAL before upload. `confirmed_flush_lsn` is advanced only at sealed segment boundaries, so the slot's `restart_lsn` always points to the start of the current in-flight segment — PG retains at most one unsealable segment (16 MB) of extra WAL. On s3worker restart, streaming always resumes from a clean segment boundary; no mid-segment recovery logic is needed.

### Architecture Overview

```
PostgreSQL primary (walsender process)
      │
      │  physical streaming replication protocol (Unix socket)
      │  IDENTIFY_SYSTEM  →  (timeline, flush_lsn)
      │  CREATE_REPLICATION_SLOT tiko_wal_stream PHYSICAL RESERVE_WAL IF NOT EXISTS
      │  SELECT confirmed_flush_lsn FROM pg_replication_slots  →  resume_lsn
      │  START_REPLICATION SLOT tiko_wal_stream PHYSICAL {resume_lsn} TIMELINE {tl}
      ▼
wal_streaming Tokio task  (inside s3worker, CopyBoth mode)
      │
      │  tokio::select! {
      │      msg  ← CopyBoth stream   (XLogData | PrimaryKeepalive)
      │      tick ← keepalive_interval (every 10 s)
      │  }
      │
      │  XLogData → append to per-segment in-memory buffer (Vec<u8>, max 16 MB)
      │
      ├─ buf.len() - chunks_uploaded >= chunk_bytes
      │      → tokio::spawn(spawn_blocking(sim.put_standard(chunk_key, slice)))
      │        tracked in JoinSet<Result<()>>
      │
      └─ segment switch detected  (start_lsn / XLogSegSize != cur_seg_no)
               → chunk_tasks.join_all()          ← wait for inflight chunk PUTs
               → buf.resize(XLogSegSize, 0)       ← zero-pad to 16 MB
               → spawn_blocking(sim.put_standard(seg_key, &buf)).await  ← must complete
               → send StandbyStatusUpdate(flush_lsn = next seg start)   ← advance slot
               → tokio::spawn(delete_chunks_for_seg)  ← best-effort compaction
               → reset buf, chunks_uploaded, chunk_tasks for new segment
```

---

## S3 Key Layout

Two object types under the same WAL prefix:

```
# Sealed segment  (PUT on segment switch, authoritative)
{org}/pitr/{proj}/wal/{tl:08X}/{seg24}
  e.g.  456/pitr/42/wal/00000001/000000010000000000000002

# In-flight chunks  (PUT every 256 KB while segment is active)
{org}/pitr/{proj}/wal/{tl:08X}/{seg24}.chunks/{start_byte:016X}
  e.g.  456/pitr/42/wal/00000001/000000010000000000000002.chunks/0000000000000000
        456/pitr/42/wal/00000001/000000010000000000000002.chunks/0000000000040000
```

Chunk size: **256 KB** — matches existing cache chunk granularity. Once the sealed object exists it supersedes its chunks; chunks are deleted after sealing (best-effort). The `.chunks/` suffix prevents key collisions between `{seg24}` and its chunk prefix (already implemented in `ProjectNamespace::wal_chunk_prefix`).

---

## `tiko_restore` Lookup Order

Unchanged — already implemented in `cli/src/bin/tiko_restore.rs`:

```
1. GET sealed segment from own namespace              → found: write, exit 0
2. LIST {own_ns}/wal/{tl}/{seg}.chunks/               → found chunks: assemble + zero-pad, exit 0
3. GET sealed segment from parent namespace           → found: write, exit 0
4. LIST {parent_ns}/wal/{tl}/{seg}.chunks/            → found chunks: assemble + zero-pad, exit 0
5. All misses → exit 1
```

Zero-padding the assembled segment to 16 MB is safe: PostgreSQL treats zero pages as end-of-WAL and requests the next segment via `restore_command`.

---

## Streaming Receiver: `engine/src/wal_streaming.rs`

### Config

```rust
pub struct WalStreamConfig {
    /// libpq connstring — must include `replication=true`.
    /// e.g. "host=/tmp port=5432 dbname=postgres replication=true"
    pub connstr: String,
    /// Physical replication slot name.
    pub slot_name: String,
    /// Bytes to accumulate before uploading a chunk. Default: 256 * 1024.
    pub chunk_bytes: usize,
}
```

No `pgdata` (no filesystem reads), no `poll_ms` (event-driven from walsender), no `start_lsn`
(always derived from slot's `confirmed_flush_lsn` on each reconnect), no `timeline` (from
`IDENTIFY_SYSTEM`).

### Per-segment state

```rust
struct SegState {
    seg_no: u64,
    timeline: u32,
    /// Raw WAL bytes received so far for this segment. Capacity: XLogSegSize.
    buf: Vec<u8>,
    /// Bytes covered by completed chunk PUTs (multiple of chunk_bytes).
    chunks_uploaded: usize,
    /// In-flight chunk PUT tasks. Joined before sealing.
    chunk_tasks: JoinSet<Result<()>>,
}
```

Reset entirely on each reconnect. `confirmed_lsn: u64` (last `flush_lsn` sent in
`StandbyStatusUpdate`) is re-derived from `pg_replication_slots` on every reconnect — not
persisted in-process.

### Startup sequence (per connection attempt)

```
1. tokio_postgres::connect(connstr)  — replication=true mode
2. IDENTIFY_SYSTEM
        → sys_id, timeline, flush_lsn, dbname
3. CREATE_REPLICATION_SLOT {slot_name} PHYSICAL RESERVE_WAL IF NOT EXISTS
        RESERVE_WAL: retains WAL from slot creation, not just from first START_REPLICATION
        IF NOT EXISTS: idempotent; "slot already exists" is not an error
4. SELECT confirmed_flush_lsn FROM pg_replication_slots
        WHERE slot_name = '{slot_name}'
        → resume_lsn  (NULL on first run → '0/0'; walsender chooses the start)
5. START_REPLICATION SLOT {slot_name} PHYSICAL {resume_lsn} TIMELINE {timeline}
        TIMELINE clause: pins stream to current timeline; avoids surprises at
        timeline boundaries (promotion/failover restarts s3worker anyway)
        → enters CopyBoth mode; SegState initialised with seg_no from resume_lsn
```

### Main loop

```
keepalive_interval = tokio::time::interval(10s)

loop:
    tokio::select! {
        msg = copyboth_stream.next() => {
            match msg {
                XLogData { start_lsn, end_lsn, data } => ingest(start_lsn, data),
                PrimaryKeepalive { reply_requested: true }  => send_standby_status(),
                PrimaryKeepalive { reply_requested: false } => {}
                None => return Err("stream closed"),
            }
        }
        _ = keepalive_interval.tick() => send_standby_status(),
    }
```

**Why `tokio::select!` with a 10s timer:** walsender closes the connection after
`wal_sender_timeout` (default 60s) with no reply. Sending proactively every 10s is conservative
and avoids relying solely on `reply_requested` flags.

### `ingest(start_lsn, data)`

```
seg_no_new = start_lsn / XLogSegSize

if seg_no_new != state.seg_no:
    # XLogData never crosses a segment boundary (walsender splits at boundaries),
    # so data belongs entirely to the new segment.  Seal the old one first.
    seal_segment(&mut state).await?
    state = SegState::new(seg_no_new, timeline)

state.buf.extend_from_slice(&data)

# Fire chunk PUTs for any newly complete 256 KB windows — non-blocking.
while state.buf.len() - state.chunks_uploaded >= chunk_bytes:
    offset = state.chunks_uploaded
    slice  = state.buf[offset .. offset + chunk_bytes].to_vec()
    key    = ns.wal_chunk_key(timeline, seg_name, offset)
    state.chunk_tasks.spawn(
        tokio::task::spawn_blocking(move || sim.put_standard(&key, &slice))
    )
    state.chunks_uploaded += chunk_bytes
```

**Why parallel chunk uploads:** `spawn_blocking` fires each chunk PUT without awaiting it.
WAL ingestion from the walsender is never stalled by upload latency. The `JoinSet` collects
all futures; they are joined only immediately before sealing, ensuring all chunks exist before
the sealed segment object is PUT.

### `seal_segment(state)`

```
# 1. Wait for all in-flight chunk PUTs to complete.
#    Any error here is fatal — bubble up to reconnect loop.
while let Some(result) = state.chunk_tasks.join_next().await:
    result??

# 2. Upload any tail bytes not yet covered by a full chunk.
if state.buf.len() > state.chunks_uploaded:
    tail_offset = state.chunks_uploaded
    tail        = state.buf[tail_offset..].to_vec()
    key         = ns.wal_chunk_key(timeline, seg_name, tail_offset)
    spawn_blocking(move || sim.put_standard(&key, &tail)).await??

# 3. Zero-pad buffer to exactly XLogSegSize, then PUT the sealed segment.
state.buf.resize(XLogSegSize, 0)
seg_key = ns.wal_key(timeline, seg_name)
let sealed = state.buf.clone()   # 16 MB; clone avoids borrow across await
spawn_blocking(move || sim.put_standard(&seg_key, &sealed)).await??

log::info("tiko: wal_streaming: sealed {seg_name} ({} B)", state.buf.len())

# 4. Advance slot: send flush_lsn = start of next segment.
#    This is the ONLY place confirmed_lsn advances.
confirmed_lsn = (state.seg_no + 1) * XLogSegSize
send_standby_status()

# 5. Best-effort compaction: delete chunk objects.
#    Fire-and-forget — stranded chunks are harmless (tiko_restore prefers the
#    sealed object in step 1 before trying chunks in step 2).
let prefix = ns.wal_chunk_prefix(timeline, seg_name)
tokio::spawn(async move {
    if let Ok(keys) = sim.list_prefix_standard(&prefix) {
        for key in keys { let _ = sim.delete_standard(&key); }
    }
})
```

### Error handling and reconnect loop

Any error in `run_streaming` (connection drop, parse error, chunk PUT failure, seal PUT failure)
returns `Err` immediately. The outer reconnect loop:

```
backoff = 1s
loop:
    match run_streaming(sim, &ns, &config).await:
        Ok(())  => backoff = 1s   # clean disconnect — reconnect quickly
        Err(e)  =>
            log::warn("tiko: wal_streaming: {e}, reconnecting in {backoff:?}")
            sleep(backoff).await
            backoff = min(backoff * 2, 60s)
```

On reconnect, `run_streaming` re-reads `confirmed_flush_lsn` from `pg_replication_slots` (step 4
of startup). Because `confirmed_flush_lsn` only advances at sealed segment boundaries, this is
always a clean segment start — `SegState` is initialised fresh. Any partially-uploaded chunks for
the aborted segment are replayed from scratch; `put_standard` is idempotent, so re-uploading is safe.

**Chunk PUT failure specifically:** does not retry in-place. Error bubbles to reconnect loop.
On reconnect the segment replays from the beginning — all chunk keys are overwritten with identical
data, then sealed normally.

### Slot invalidation (unrecoverable WAL gap)

If `max_slot_wal_keep_size` is exceeded, PostgreSQL invalidates the slot. The next
`START_REPLICATION` (or mid-stream receive) fails with:

```
ERROR: replication slot "tiko_wal_stream" is invalid
```

Recovery procedure:
```
1. DROP REPLICATION SLOT {slot_name}  (if it exists)
2. CREATE_REPLICATION_SLOT {slot_name} PHYSICAL RESERVE_WAL IF NOT EXISTS
3. resume_lsn = current server flush_lsn  (from IDENTIFY_SYSTEM)
4. START_REPLICATION from resume_lsn
5. log::error("tiko: wal_streaming: slot invalidated — WAL gap before {resume_lsn}; \
               segments in this range are unrecoverable")
```

This is the only known state where PITR data is permanently lost. The error is logged at
`ERROR` level and emitted via `log_relay` so it surfaces in the PostgreSQL server log.

---

## Wire Formats

### `StandbyStatusUpdate` (client → server, type `'r'`)

| Offset | Size | Value |
|--------|------|-------|
| 0 | 1 | `'r'` |
| 1 | 8 | `write_lsn` — big-endian u64, same as `flush_lsn` |
| 9 | 8 | `flush_lsn` — big-endian u64, end of last sealed segment |
| 17 | 8 | `apply_lsn` — big-endian u64, same as `flush_lsn` |
| 25 | 8 | `client_time` — big-endian i64, μs since 2000-01-01 |
| 33 | 1 | `reply_requested` — `0` |

`flush_lsn` is what the server uses to advance `confirmed_flush_lsn` (and thus `restart_lsn`) on
the slot. `write_lsn` and `apply_lsn` are set equal to `flush_lsn` — physical replication has no
separate apply phase.

### `XLogData` (server → client, type `'w'`)

| Offset | Size | Value |
|--------|------|-------|
| 0 | 1 | `'w'` |
| 1 | 8 | `start_lsn` — big-endian u64 |
| 9 | 8 | `end_lsn` — big-endian u64 |
| 17 | 8 | `server_time` — big-endian i64, μs since 2000-01-01 |
| 25 | N | WAL bytes |

### `PrimaryKeepalive` (server → client, type `'k'`)

| Offset | Size | Value |
|--------|------|-------|
| 0 | 1 | `'k'` |
| 1 | 8 | `end_lsn` — big-endian u64 |
| 9 | 8 | `server_time` — big-endian i64 |
| 17 | 1 | `reply_requested` — `1` if server wants immediate reply |

---

## LSN → Segment Arithmetic

```
XLogSegSize  = 16 * 1024 * 1024      // 16 MB, 1 << 24
seg_no       = lsn / XLogSegSize
seg_offset   = lsn % XLogSegSize
seg_name     = format!("{timeline:08X}{seg_no:016X}")
next_seg_lsn = (seg_no + 1) * XLogSegSize
```

The walsender never sends a single `XLogData` message that crosses a segment boundary — it splits
at boundaries internally. A segment switch is therefore: `start_lsn / XLogSegSize != state.seg_no`.

---

## PostgreSQL Configuration

### Required settings

```ini
# Required for physical streaming replication (change requires restart)
wal_level             = replica
max_wal_senders       = 2      # ≥ 1 for the streaming task; +1 spare
max_replication_slots = 2      # ≥ 1 for tiko_wal_stream; +1 spare

# Safety valve: bounds pg_wal disk usage if streaming falls behind.
# When hit, the slot is invalidated and a WAL gap results — size accordingly.
max_slot_wal_keep_size = 1GB
```

`archive_mode`, `archive_command`, and `archive_timeout` are **not set**.

### `pg_hba.conf`

The streaming task connects via Unix socket with `replication=true`:

```
local   replication   all   trust
```

### Generated recovery config (`prepare_recovery` → `postgresql.tiko.conf`)

Unchanged:

```ini
# Tiko recovery settings — begin
restore_command = '... tiko_restore %f %p'
recovery_target = 'immediate'
recovery_target_action = 'promote'
# Tiko recovery settings — end
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TIKO_WAL_STREAM_SLOT` | `tiko_wal_stream` | Physical replication slot name |
| `TIKO_WAL_STREAM_CONNSTR` | `host=/tmp port=5432 dbname=postgres replication=true` | Must include `replication=true` |
| `TIKO_WAL_CHUNK_BYTES` | `262144` | Chunk upload size in bytes |

---

## Recovery Flow

```
tiko_restore: segment=000000010000000000000002

step 1: GET 456/pitr/43/wal/00000001/...002          → 404 (own ns, new branch)
step 2: LIST 456/pitr/43/wal/00000001/...002.chunks/ → empty
step 3: GET 456/pitr/42/wal/00000001/...002          → 404 (parent ns, mid-segment: not yet sealed)
step 4: LIST 456/pitr/42/wal/00000001/...002.chunks/ → chunks found (streaming uploaded them)
        assemble + zero-pad to 16 MB → write to pg_wal/

WAL replay:
  0/2038128  redo starts
  0/2038178  RUNNING_XACTS: xid 752 in-flight
  0/20XXXX   xid 752 COMMIT  ← reachable inside assembled segment

consistent recovery point reached → promote → branch is writable
```

---

## Files to Create / Modify

| File | Change |
|---|---|
| `engine/src/wal_streaming.rs` | **Full rewrite** — physical replication protocol, `IDENTIFY_SYSTEM`, slot creation, `START_REPLICATION TIMELINE`, `tokio::select!` main loop, parallel chunk uploads with `JoinSet`, `seal_segment`, slot invalidation handling |
| `engine/Cargo.toml` | Confirm `tokio-postgres` feature flags include `runtime-tokio` for `CopyBothDuplex` replication support |
| `worker/src/thread_pool.rs` | No structural change — `WalStreamConfig::from_env()` reads updated env vars |
| `store/src/project.rs` | No change — `wal_key`, `wal_chunk_prefix`, `wal_chunk_key` already correct |
| `cli/src/bin/tiko_restore.rs` | No change — chunk assembly + parent fallback already implemented |
| `store/src/recovery.rs` | No change — `write_recovery_conf` already omits `archive_mode`/`archive_command` |
| `postgresql.conf.sample` | Set `wal_level`, `max_wal_senders`, `max_replication_slots`, `max_slot_wal_keep_size` |
