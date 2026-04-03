# Local Cache Design

The s3worker uses local files as a cache in front of S3 (the source of truth).
The total cache size is configurable via a GUC (`tiko.cache_size`) set at
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
- **`AtomicRWLock` instead of PG LWLocks**: Tokio threads in s3worker also
  access the hash table (`cached_read_blocks`/`cached_write_blocks` in
  `io_handler`). LWLocks require per-process state (`MyProc`) and are
  not safe to call from Tokio threads, so spin-based atomics are used.
- **`ChunkTag` hashed via FNV-1a**: fast 32-bit hash over all 5 fields.

### Per-block tracking via bitmasks

Each `CacheSlotMeta` has two `AtomicU32` bitmasks:
- `valid_blocks`: bit N set = block N is populated in this chunk slot
- `dirty_blocks`: bit N set = block N has been modified (needs flush on eviction)

Slot is "occupied" when `valid_blocks != 0`. Slot is "dirty" when `dirty_blocks != 0`.

### Shared memory layout (trailing arrays)

```
S3IoControl { num_backend_pools, s3worker_pid, s3worker_latch,
              submit_queue, cache: CacheControl, stats: S3IoStats }
[aligned]  BackendSlotPool[0..max_backends]
[aligned]  CacheSlotMeta[0..1024]        chunk slot metadata (~36 KB)
[aligned]  CacheHashEntry[0..2048]       hash table (~56 KB)
[aligned]  AtomicRWLock[0..128]          partition locks (512 bytes)
```

Note: `cache` (CacheControl) comes before `stats` (S3IoStats) in the
`S3IoControl` struct. Trailing arrays are laid out in order: slot metadata,
hash entries, then partition locks — each aligned to its natural alignment.

Total cache metadata: ~92 KB.

### `cache_is_available()` guard

Used in `store_ops` to gate cache access:
```rust
fn cache_is_available() -> bool {
    is_under_postmaster() && S3IoControl::is_initialized()
}
```

- `is_under_postmaster()` — false during initdb (`--boot`/`--single`), where
  `MyProcNumber` is invalid and `S3IoControl` was never sized via
  `shmem_request_hook`.
- `S3IoControl::is_initialized()` — false if the shmem startup hook has not
  yet run in this process (e.g. very early in backend startup).

When unavailable, all cache-aware functions fall back directly to raw
`read_blocks`/`write_blocks`.

## 3. Eviction — Clock-Sweep

Same algorithm PostgreSQL uses for its own buffer manager — proven for database
workloads:

- Each slot has: `usage_count` (0–5), `valid_blocks` / `dirty_blocks` bitmasks, `pin_count`
- Clock hand sweeps the array; decrements `usage_count` on each pass
- Evicts first unpinned slot with `usage_count == 0`
- **Dirty chunks flushed before eviction**: iterates `dirty_blocks` bitmask,
  writes each dirty block to the S3-sim file via `store_ops::write_blocks()`

**Why clock-sweep over LRU**: no per-access linked-list manipulation. A single
atomic increment on `usage_count` per access is all that's needed.
Scan-resistant naturally — sequential scans only set `usage_count = 1`, which
decays quickly.

### Concurrency protocol

- **Clock hand**: single `AtomicU32`, advanced via `fetch_add`.
- **Slot pinning**: `pin_count` is an `AtomicU32`. Pinned slots (`pin_count > 0`)
  are skipped during eviction.
- **Eviction sequence**: CAS `pin_count` 0 → 1, check `valid_blocks` (empty
  slots claimed immediately), decrement `usage_count` if > 0, then on
  `usage_count == 0`: flush dirty blocks, remove from hash table via tombstone
  (`HashStatus::Deleted`), clear metadata, return slot.
- **Eviction stats**: `S3IoControl::stats.evictions` and `.dirty_evictions`
  counters incremented on each eviction.

## 4. Write Policy — Write-Back

```
PG write  -->  local cache slot (immediate, set dirty bit)
                                                |
                            eviction trigger  -->  flush dirty blocks to S3-sim
```

- Writes go to cache only — **no write-through** to backing files
- Dirty blocks are flushed to S3-sim files on eviction
- **Crash safety**: WAL replay handles any pages not yet flushed to backing files
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
cached_read_blocks(block B)
  │
  ├─ compute chunk_id = B / 32, block_offset = B % 32
  ├─ hash index lookup(ChunkTag)
  │   ├─ CHUNK HIT:
  │   │   ├─ block valid   → pin, read_block() from cache slot, touch, unpin
  │   │   └─ block invalid → pin, read 1 block from S3-sim, write to cache,
  │   │                       set_block_valid, touch, unpin
  │   │
  │   └─ CHUNK MISS:
  │       ├─ insert(chunk_tag) → evicts old chunk (flush if dirty), returns pinned
  │       ├─ query file_nblocks() for available blocks in this chunk
  │       ├─ read up to 32 blocks from S3-sim file (whole chunk prefetch)
  │       ├─ write_blocks_to_slot(), set_valid_blocks_mask(valid_mask)
  │       ├─ read requested block from cache (or zero-fill if beyond EOF)
  │       └─ touch, unpin
```

## 8. Write Flow (Write-Back)

```
cached_write_blocks(block B)
  │
  ├─ compute chunk_id = B / 32, block_offset = B % 32
  ├─ hash index lookup(ChunkTag)
  │   ├─ CHUNK HIT  → pin
  │   └─ CHUNK MISS → insert empty chunk (no S3-sim fetch), returns pinned
  │
  ├─ write_block() to cache slot
  ├─ set_block_valid()
  ├─ mark_dirty()
  ├─ touch()
  └─ unpin()

  // NO write-through — dirty blocks flushed on eviction
```

## 9. Prefetch Flow (`warm_cache_blocks`)

Used by `s3_prefetch` (async via `S3IoOpKind::Prefetch`) to warm the cache
without copying data to a caller buffer:

```
warm_cache_blocks(block_number, nblocks)
  │
  ├─ iterate chunk-by-chunk over [first_chunk..=last_chunk]
  │   ├─ CACHE HIT  → pin, touch, unpin  (already warm)
  │   └─ CACHE MISS → insert empty slot (pinned)
  │                    prefetch full chunk from S3-sim
  │                    write_blocks_to_slot(), set_valid_blocks_mask()
  │                    touch, unpin
  └─ no-op when cache unavailable (returns Ok(0))
```

## 10. Cache Invalidation (`invalidate_range`)

Used by `cached_truncate_file` and `cached_delete_file` to evict stale
blocks **before** modifying the backing file:

- **Truncate** (`first_block = nblocks`): chunks fully beyond the new EOF are
  reset (removed from hash + metadata cleared). Chunks partially overlapping
  the truncation point have their `valid_blocks`/`dirty_blocks` masks trimmed.
- **Delete** (`first_block = 0`): all chunks for the relation are reset.

Order matters: invalidating **before** the backing file operation prevents
dirty blocks from being flushed back to a truncated or deleted file.

## 11. Cache-Aware File Operations

All smgr-facing operations have cache-aware wrappers in `store_ops`:

| Function | Cache interaction |
|---|---|
| `cached_read_blocks` | Chunk lookup + prefetch on miss |
| `cached_write_blocks` | Chunk lookup + insert on miss (no prefetch) |
| `cached_file_nblocks` | `max(file_nblocks, cache.max_block_for_relation())` |
| `cached_truncate_file` | `invalidate_range` then `truncate_file` |
| `cached_delete_file` | `invalidate_range(first=0)` then `delete_file` |
| `warm_cache_blocks` | Prefetch without copying to caller |

`cached_file_nblocks` is needed because write-back caching means the backing
file may not yet reflect blocks written since the last eviction.

## 12. Checkpoint and Shutdown Flush

### Checkpoint flush (`s3_checkpoint_flush`)

Called directly from `CheckPointGuts()` in `xlog.c` **after**
`CheckPointBuffers()` has written all dirty buffer pool pages into the cache.
Calls `cache.flush_all_dirty_chunks()`, which:
- Scans all slots, spins to pin each dirty slot
- Calls `flush_dirty_chunk()` for each dirty slot
- Clears `dirty_blocks` after flush

This guarantees every dirty block is in the S3-sim backing files before the
checkpoint WAL record is written, so WAL replay from this checkpoint yields
a fully consistent image.

### Shutdown (`s3_shutdown`)

**Empty** — deliberately performs no cache flush. By the time `s3_shutdown`
fires (either a regular backend exiting during `PM_STOP_BACKENDS` or the
checkpointer after the shutdown checkpoint), `s3_checkpoint_flush` has already
ensured all dirty chunks are written. Flushing again would be redundant.

### Relation-level flush (`flush_dirty_chunks_for_relation`)

Called from `s3_immedsync()` when PostgreSQL requests an immediate sync for
a relation (e.g. `smgrdosyncall` during explicit buffer flush). Scans all
slots, pinning and flushing only those matching the given relation fork.

## 13. I/O Statistics (`S3IoStats`)

Counters in `S3IoControl::stats` (all `AtomicU64`, live in PG shared memory):

| Counter | Meaning |
|---|---|
| `total_reads` | Total blocks requested via `cached_read_blocks` |
| `total_writes` | Total blocks requested via `cached_write_blocks` |
| `cache_hits` | Chunk found in hash table on read or write |
| `cache_misses` | Chunk not found; new slot allocated |
| `evictions` | Total evictions (clean or dirty) |
| `dirty_evictions` | Evictions that required flushing dirty blocks |
| `s3_gets` | S3 GET requests (future: real S3 reads) |
| `s3_puts` | S3 PUT requests (future: real S3 writes) |
| `queue_full_waits` | Times submit queue was full; backend spun waiting |

## 14. Shutdown & Fallback Path

PostgreSQL kills all `B_BG_WORKER` processes (including s3worker) in
`PM_STOP_BACKENDS`, **before** the checkpointer performs its shutdown
checkpoint in `PM_WAIT_XLOG_SHUTDOWN`.

| Scenario | Detection | Fallback |
|---|---|---|
| initdb (bootstrap + single-user) | `!is_under_postmaster()` | Direct `store_ops` |
| shmem not yet initialized | `!S3IoControl::is_initialized()` | Direct `store_ops` |
| Shutdown checkpoint | `is_s3worker_alive() == false` | Direct `store_ops` |
| s3worker crash | `is_s3worker_alive() == false` | Direct `store_ops` |

When cache is not initialized (initdb), reads/writes fall back to raw
`read_blocks`/`write_blocks` directly on S3-sim files. `invalidate_range`
and related cache operations are no-ops.

## 15. Component Summary

| Component      | Location              | Structure                                        |
|----------------|-----------------------|--------------------------------------------------|
| Control        | PG shared memory      | `CacheControl` (~40 bytes): num_slots, clock_hand, array pointers |
| Chunk data     | Local file            | `1024 x 256 KB` pre-allocated (`{DataDir}/tiko/cache`, 256 MB) |
| Slot metadata  | PG shared memory      | `CacheSlotMeta[1024]`: tag(20B) + valid_blocks + dirty_blocks + usage_count + pad + pin_count (~36 KB) |
| Hash index     | PG shared memory      | `CacheHashEntry[2048]`: open-addressing, FNV-1a hash, tombstone delete (~56 KB) |
| Partition locks| PG shared memory      | `AtomicRWLock[128]` spin reader-writer locks (512 bytes) |
| Clock hand     | PG shared memory      | Single `AtomicU32` in `CacheControl`              |
| Stats          | PG shared memory      | `S3IoStats`: 9 × `AtomicU64` counters (~72 bytes) |
| S3-sim files   | Local filesystem      | Per-relation files (`{DataDir}/tiko/{spc}/{db}/{rel}.{fork}`) |
