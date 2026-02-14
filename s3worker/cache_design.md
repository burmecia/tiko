# Local Cache Design

The s3worker uses local files as a cache in front of S3 (the source of truth).
The total cache size is configurable. PostgreSQL's WAL provides crash recovery.

## Position in the I/O Stack

```
PostgreSQL shared buffers  (hot pages, managed by PG buffer manager)
         |
    smgr interface  (s3_readv / s3_writev)
         |
   +-----------+
   | Local Cache |  <-- this design
   +-----------+
         |
        S3       (source of truth)
```

The local cache sits **below** PostgreSQL's shared buffers. Access patterns are
already filtered by PG's buffer manager:
- **Reads** happen on buffer cache misses
- **Writes** happen during checkpoints / bgwriter flushes

## 1. Cache Layout — Fixed-Slot Block Array

A single pre-allocated file divided into fixed 8 KB slots:

```
cache_file:  [slot 0][slot 1][slot 2]...[slot N-1]
              8 KB    8 KB    8 KB       8 KB

N = cache_size_bytes / 8192
```

**Why not a directory tree** (one file per relation like `md.c`):
eviction becomes complex — tracking per-block recency across thousands of files
and deleting individual blocks from the middle of a file is expensive.
A flat slot array gives clean O(1) eviction.

## 2. Index — In-Memory Hash Map

```
BlockTag { spc_oid, db_oid, rel_oid, fork, blkno }  -->  slot_index
```

- Stored in **shared memory** (accessible by s3worker + backends via smgr path)
- O(1) lookup for cache hits
- Rebuilt on startup by scanning slot metadata (or persisted in a small index file)

## 3. Eviction — Clock-Sweep

Same algorithm PostgreSQL uses for its own buffer manager — proven for database
workloads:

- Each slot has: `usage_count` (0–5), `dirty` bit, `pin_count`
- Clock hand sweeps the array; decrements `usage_count` on each pass
- Evicts first unpinned slot with `usage_count == 0`
- Dirty pages flushed to S3 **before** eviction (write-back)

**Why clock-sweep over LRU**: no per-access linked-list manipulation. A single
atomic increment on `usage_count` per access is all that's needed.
Scan-resistant naturally — sequential scans only set `usage_count = 1`, which
decays quickly.

## 4. Write Policy — Write-Back with Async S3 Flush

```
PG write  -->  local cache slot (immediate)  -->  S3 (async background)
```

- Writes to local cache are fast (local SSD I/O)
- Background flusher in Tokio runtime uploads dirty pages to S3
- **Crash safety**: WAL replay handles any pages not yet flushed to S3
- **Batching**: group dirty pages by relation for multi-block S3 PUTs
  (S3 charges per request, not per byte)

## 5. S3 Object Granularity

One 8 KB block per S3 object is wasteful — S3 latency per GET/PUT is ~50–100 ms
regardless of size.

- **Chunk size: 1 MB** (128 blocks per S3 object)
- S3 key: `s3://{bucket}/{spc_oid}/{db_oid}/{rel_oid}.{fork}/{chunk_id}`
- **Read miss**: fetch entire 1 MB chunk, populate up to 128 cache slots (prefetch)
- **Write flush**: upload dirty blocks as a full chunk (merge with existing clean blocks)

This amortizes S3 latency and aligns with S3's cost model.

## 6. Component Summary

| Component      | Location        | Structure                                       |
|----------------|-----------------|--------------------------------------------------|
| Slot data      | Local file      | `N x 8 KB` pre-allocated                        |
| Slot metadata  | Shared memory   | `[tag, usage_count, dirty, pin_count]` per slot  |
| Hash index     | Shared memory   | `BlockTag -> slot_index`                         |
| Clock hand     | Shared memory   | Single `AtomicU32`                               |
| Dirty queue    | s3worker        | List of slots pending S3 upload                  |
| S3 objects     | S3              | 1 MB chunks (128 blocks each)                    |

## 7. Shutdown & Fallback Path

PostgreSQL kills all `B_BG_WORKER` processes (including s3worker) in
`PM_STOP_BACKENDS`, **before** the checkpointer performs its shutdown
checkpoint in `PM_WAIT_XLOG_SHUTDOWN`. No `bgw_flags` value can change this
ordering.

This means `s3_writev`/`s3_readv` must handle three scenarios where
s3worker is unavailable:

| Scenario | Detection | Fallback |
|---|---|---|
| initdb (bootstrap + single-user) | `!IsUnderPostmaster` | Sync path |
| Shutdown checkpoint | `is_s3worker_alive() == false` | Sync path |
| s3worker crash | `is_s3worker_alive() == false` | Sync path |

### Synchronous local cache fallback (long-term)

When the async pipeline is unavailable, `s3_writev`/`s3_readv` perform
**direct synchronous I/O** to the local cache file:

```
s3worker alive?
  ├── YES → async pipeline (submit queue → Tokio → S3 + local cache)
  └── NO  → sync pwrite()/pread() to local cache file inline
```

The sync path:
1. Looks up the slot via the shared memory hash index
2. On write: `pwrite()` the page into the cache slot, mark dirty
3. On read (cache hit): `pread()` from the cache slot
4. On read (cache miss): zero-fill or error (S3 not reachable without Tokio)

**No data loss** because:
- Pages land in the local cache (persistent on local SSD)
- WAL guarantees crash recoverability
- On next startup, s3worker reconciles cache-dirty pages with S3

**Note**: During initdb, the cache file and shared memory index may not
exist yet. In this case the sync path falls back further to `md*` functions
(which will eventually be removed once the cache is initialized before
first use in all code paths).

## 8. Open Questions

1. **Cache file: pre-allocated vs sparse?**
   Pre-allocated avoids fragmentation but takes full space upfront.

2. **Index persistence**: rebuild from slot metadata on startup (simpler)
   vs persist to disk (faster restart)?

3. **S3 chunk size**: 1 MB is a reasonable default — should it be configurable?

4. **Dirty flush trigger**: time-based (every N seconds), count-based
   (every N dirty pages), or piggyback on PG checkpoints?
