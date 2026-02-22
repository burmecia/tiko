# Local Cache Design

The s3worker uses local files as a cache in front of S3 (the source of truth).
The total cache size is configurable via a GUC (`pico.cache_size`) set at
server startup. PostgreSQL's WAL provides crash recovery.

## Position in the I/O Stack

```
PostgreSQL shared buffers  (hot pages, managed by PG buffer manager)
         |
    smgr interface  (s3_readv / s3_writev)
         |
   +-----------+
   | Local Cache |  <-- this design (write-back, chunk-level)
   +-----------+
         |
   S3-sim files    (source of truth, future: real S3)
```

The local cache sits **below** PostgreSQL's shared buffers. Access patterns are
already filtered by PG's buffer manager:
- **Reads** happen on buffer cache misses
- **Writes** happen during checkpoints / bgwriter flushes

## 1. Cache Layout — Chunk-Slot Array

A single pre-allocated file divided into fixed 256 KB chunk slots. Each chunk
holds 32 contiguous 8 KB blocks, matching the S3 object size:

```
cache_file:  [chunk_0 (256KB)][chunk_1 (256KB)]...[chunk_1023 (256KB)]

Block B within chunk slot S is at byte offset:
  S * CHUNK_SIZE + (B % BLOCKS_PER_CHUNK) * BLCKSZ

N = 1024 chunk slots = 256 MB default cache
```

**Why chunk-level instead of block-level**: S3 latency per GET/PUT is ~50-100 ms
regardless of size. One 8 KB block per S3 object is wasteful. 256 KB chunks
(32 blocks) amortize S3 request overhead. Read misses prefetch entire chunks,
and dirty evictions flush only modified blocks within the chunk.

## 2. Index — Hash Table in PG Shared Memory

```
ChunkTag { spc_oid, db_oid, rel_number, fork_number, chunk_id }  -->  slot_index
```

Where `chunk_id = blkno / 32`.

- **Fixed-size open-addressing hash table** in PG shared memory (trailing
  arrays after `S3IoControl`). Sized to `2 * N` entries (2048 for 1024 slots)
  for low collision rates.
- All processes access the same shared memory region — writes from one
  process are immediately visible to others.
- O(1) lookup for cache hits on the read path (short-circuit, no s3worker).
- **Partitioned locking**: the table is divided into 128 partitions,
  each protected by a spin-based `AtomicRWLock` in shared memory.
  Lookups hold a shared (read) lock. Insertions/evictions take an exclusive
  (write) lock.

### Per-block tracking via bitmasks

Each `CacheSlotMeta` has two `AtomicU32` bitmasks:
- `valid_blocks`: bit N set = block N is populated in this chunk slot
- `dirty_blocks`: bit N set = block N has been modified (needs flush on eviction)

Slot is "occupied" when `valid_blocks != 0`. Slot is "dirty" when `dirty_blocks != 0`.

### Shared memory layout (trailing arrays)

```
S3IoControl { header, submit_queue, stats, cache: CacheControl }
[align 64] BackendSlotPool[0..max_backends]
[align 4]  AtomicRWLock[0..128]          partition locks (512 bytes)
[align 4]  CacheSlotMeta[0..1024]        chunk slot metadata (~36 KB)
[align 4]  CacheHashEntry[0..2048]       hash table (~52 KB)
```

Total cache metadata: ~88 KB.

## 3. Eviction — Clock-Sweep

Same algorithm PostgreSQL uses for its own buffer manager — proven for database
workloads:

- Each slot has: `usage_count` (0–5), `valid_blocks` / `dirty_blocks` bitmasks, `pin_count`
- Clock hand sweeps the array; decrements `usage_count` on each pass
- Evicts first unpinned slot with `usage_count == 0`
- **Dirty chunks flushed before eviction**: iterates `dirty_blocks` bitmask,
  writes each dirty block to the S3-sim file via `s3_ops::write_blocks()`

**Why clock-sweep over LRU**: no per-access linked-list manipulation. A single
atomic increment on `usage_count` per access is all that's needed.
Scan-resistant naturally — sequential scans only set `usage_count = 1`, which
decays quickly.

### Concurrency protocol

- **Clock hand**: single `AtomicU32`, advanced via `fetch_add`.
- **Slot pinning**: `pin_count` is an `AtomicU32`. Pinned slots (`pin_count > 0`)
  are skipped during eviction.
- **Eviction sequence**: CAS `pin_count` 0 → 1, flush dirty blocks if any,
  remove from hash table, clear metadata, return slot.

## 4. Write Policy — Write-Back

```
PG write  -->  local cache slot (immediate, set dirty bit)
                                                |
                            eviction trigger  -->  flush dirty blocks to S3-sim
```

- Writes go to cache only — **no write-through** to backing files
- Dirty blocks are flushed to S3-sim files on eviction
- **Crash safety**: WAL replay handles any pages not yet flushed
- Per-block dirty tracking via `dirty_blocks` bitmask means only modified
  blocks within a chunk are written back on eviction

## 5. S3 Object Granularity

One cache chunk = one S3 object = 256 KB (32 blocks):

- S3 key: `s3://{bucket}/{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}`
- **Read miss**: fetch entire 256 KB chunk, populate cache slot (prefetch)
- **Dirty eviction**: flush only dirty blocks to S3-sim file

## 6. Crash Recovery — Volatile Cache

The cache is a **performance optimization, not a durability layer**. S3 + WAL
handle durability.

**On crash:**
- PG shared memory (all cache metadata) is **lost**
- Cache data file survives but blocks are **unidentifiable** without the
  in-memory hash table

**Recovery strategy: discard and rebuild**
1. On startup, PG shared memory is re-initialized (all cache metadata starts
   fresh — empty hash table, zeroed slot metadata)
2. WAL replay reconstructs all committed data from the last checkpoint
3. Cache warms up organically through normal read misses

## 7. Read-Miss Flow (Chunk-Level)

Full path for a cache miss:

```
s3_readv(block B)
  │
  ├─ compute chunk_id = B / 32, block_offset = B % 32
  ├─ hash index lookup(ChunkTag)
  │   ├─ CHUNK HIT:
  │   │   ├─ block valid   → pread() from cache slot, return
  │   │   └─ block invalid → read single block from S3-sim, populate, return
  │   │
  │   └─ CHUNK MISS:
  │       ├─ insert(chunk_tag) → evicts old chunk (flush if dirty)
  │       ├─ pread full chunk from S3-sim file (up to 32 blocks)
  │       ├─ write all fetched blocks to cache slot, set valid_blocks
  │       └─ read block from cache slot, return
```

## 8. Write Flow (Write-Back)

```
s3_writev(block B)
  │
  ├─ compute chunk_id = B / 32, block_offset = B % 32
  ├─ hash index lookup(ChunkTag)
  │   ├─ CHUNK HIT  → pin
  │   └─ CHUNK MISS → insert empty chunk (no S3-sim fetch)
  │
  ├─ pwrite block to cache slot
  ├─ set valid_blocks bit
  ├─ set dirty_blocks bit
  └─ unpin

  // NO write-through — dirty blocks flushed on eviction
```

## 9. Shutdown & Fallback Path

PostgreSQL kills all `B_BG_WORKER` processes (including s3worker) in
`PM_STOP_BACKENDS`, **before** the checkpointer performs its shutdown
checkpoint in `PM_WAIT_XLOG_SHUTDOWN`.

| Scenario | Detection | Fallback |
|---|---|---|
| initdb (bootstrap + single-user) | `!IsUnderPostmaster` | Direct `s3_ops` |
| Shutdown checkpoint | `is_s3worker_alive() == false` | Direct `s3_ops` |
| s3worker crash | `is_s3worker_alive() == false` | Direct `s3_ops` |

When cache is not initialized (initdb), reads/writes fall back to raw
`read_blocks`/`write_blocks` directly on S3-sim files.

## 10. Component Summary

| Component      | Location              | Structure                                        |
|----------------|-----------------------|--------------------------------------------------|
| Control        | PG shared memory      | `CacheControl` (~16 bytes): num_slots, clock_hand |
| Chunk data     | Local file            | `1024 x 256 KB` pre-allocated (`{DataDir}/pico/cache`, 256 MB) |
| Slot metadata  | PG shared memory      | `[ChunkTag, valid_blocks, dirty_blocks, usage_count, pin_count]` per slot (~36 KB) |
| Hash index     | PG shared memory      | Fixed-size open-addressing, 2048 entries (~52 KB) |
| Partition locks| PG shared memory      | `AtomicRWLock[128]` (512 bytes)                   |
| Clock hand     | PG shared memory      | Single `AtomicU32` in `CacheControl`              |
| S3-sim files   | Local filesystem      | Per-relation files (`{DataDir}/pico/{spc}/{db}/{rel}.{fork}`) |
