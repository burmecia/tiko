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

## 2. Index — Fixed-Size Hash Table in Shared Memory

```
BlockTag { spc_oid, db_oid, rel_oid, fork, blkno }  -->  slot_index
```

- **Fixed-size open-addressing hash table** allocated in PG shared memory at
  startup via `shmem_request_hook`. Sized to `2 * N` entries (2x cache slots)
  for low collision rates. Same approach PG uses internally for its buffer
  mapping table (`BufMappingPartition`).
- Shared memory is fixed at startup — the table cannot grow dynamically.
  If the cache size is changed, the hash table must be resized at next restart.
- O(1) lookup for cache hits on the read path (short-circuit, no s3worker).
- **Partitioned locking**: the table is divided into partitions (e.g., 128),
  each protected by a lightweight lock. Lookups only hold a shared lock on one
  partition. Insertions/evictions take an exclusive lock on the affected
  partition. This minimizes contention across concurrent backends.

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

### Concurrency protocol

- **Clock hand**: single `AtomicU32`, advanced via `fetch_add`. Multiple
  backends can sweep concurrently — each atomically claims the next slot index
  to inspect, avoiding duplicate work.
- **Slot pinning**: `pin_count` is an `AtomicU32`. Backends increment before
  use, decrement after. A pinned slot (`pin_count > 0`) is skipped during
  eviction — the sweeper moves to the next slot.
- **Eviction sequence**: to evict slot `i`, a backend must:
  1. CAS `pin_count` from 0 → 1 (claim exclusive eviction access)
  2. If dirty: submit async S3 flush, wait for completion, clear dirty bit
  3. Remove old `BlockTag → i` mapping from hash table (exclusive partition lock)
  4. Insert new `BlockTag → i` mapping (exclusive partition lock)
  5. Perform the I/O (read from S3 into slot)
  6. Set new tag, set `usage_count = 1`, unpin

  If the CAS fails (another backend pinned it), skip and advance the clock hand.

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
- **Write flush**: upload dirty blocks as a full chunk

### Chunk merge strategy for writes

S3 objects are immutable — no partial updates. To flush dirty blocks within
a 1 MB chunk, the full chunk must be uploaded. Two approaches:

1. **Local-only merge (preferred)**: the cache file already holds all blocks
   for a given chunk (clean + dirty). To flush, read the full chunk's worth of
   slots from the local cache file, assemble into a 1 MB buffer, and PUT to S3.
   No S3 read needed — the merge is entirely local I/O.

2. **S3 read-modify-write (fallback)**: if the cache doesn't hold all 128
   blocks for the chunk (partial population), GET the existing chunk from S3,
   overlay dirty blocks, PUT back. Adds one S3 round trip.

In practice, approach (1) dominates because the read-miss path prefetches the
entire chunk, so all blocks are typically resident. Approach (2) is only needed
for chunks that were never fully fetched.

## 6. Crash Recovery — Volatile Cache

The cache is a **performance optimization, not a durability layer**. S3 + WAL
handle durability.

**On crash:**
- Shared memory (hash index, slot metadata) is **lost**
- Cache file survives on disk but blocks are **unidentifiable** (raw 8 KB
  pages with no embedded metadata indicating which relation/block they belong to)

**Recovery strategy: discard and rebuild**
1. On startup, **discard the entire cache file** (or truncate to zero)
2. WAL replay reconstructs all committed data from the last checkpoint
3. Cache warms up organically through normal read misses

**Why not persist cache metadata:**
- Adds complexity (sync metadata file on every eviction/dirty-mark)
- Marginal benefit — cold cache penalty is temporary and WAL replay is fast
- Avoids metadata-vs-data consistency bugs entirely

**Alternative (future optimization)**: persist a lightweight metadata file
alongside the cache (slot → BlockTag mapping), synced at checkpoint boundaries.
On startup, validate each slot against S3 checksums and repopulate the hash
index. Only worth doing if cold-start latency becomes a production concern.

## 7. Read-Miss Flow

Full path for a cache miss, including eviction cascade:

```
s3_readv(block B)
  │
  ├─ hash index lookup(B)
  │   ├─ HIT  → pread() from cache slot, bump usage_count, return
  │   └─ MISS ↓
  │
  ├─ find a free slot:
  │   ├─ free slot available → claim it
  │   └─ no free slot → clock-sweep eviction:
  │       ├─ found clean slot (usage_count=0, !dirty) → evict immediately
  │       └─ found dirty slot (usage_count=0, dirty):
  │           └─ flush to S3 first (async) → wait → clear dirty → evict
  │
  ├─ fetch 1 MB chunk from S3 (contains block B + up to 127 neighbors)
  │   └─ populate multiple cache slots (prefetch)
  │
  └─ pread() block B from cache slot, return
```

**Worst case**: evict dirty + fetch from S3 = two S3 round trips (~100–200 ms).
The prefetch amortizes this — subsequent reads within the same chunk hit the
cache.

## 8. Shutdown & Fallback Path

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

## 9. Component Summary

| Component      | Location        | Structure                                        |
|----------------|-----------------|--------------------------------------------------|
| Slot data      | Local file      | `N x 8 KB` pre-allocated                        |
| Slot metadata  | Shared memory   | `[tag, usage_count, dirty, pin_count]` per slot  |
| Hash index     | Shared memory   | Fixed-size open-addressing, `2*N` entries, partitioned locks |
| Clock hand     | Shared memory   | Single `AtomicU32`, CAS-advanced                 |
| Dirty queue    | s3worker        | List of slots pending S3 upload                  |
| S3 objects     | S3              | 1 MB chunks (128 blocks each)                    |

## 10. Open Questions

1. **Cache file: pre-allocated vs sparse?**
   Pre-allocated avoids fragmentation but takes full space upfront.

2. **S3 chunk size**: 1 MB is a reasonable default — should it be configurable?

3. **Dirty flush trigger**: time-based (every N seconds), count-based
   (every N dirty pages), or piggyback on PG checkpoints?

4. **Hash table partition count**: 128 partitions is a reasonable default
   for up to ~100 concurrent backends. Should scale with `MaxBackends`?

5. **Prefetch strategy on read miss**: always fetch full 1 MB chunk, or
   adaptive (fetch only requested block for random access patterns)?
