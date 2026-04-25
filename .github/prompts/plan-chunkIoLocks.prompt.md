## Plan: Add Slot I/O Guards For Chunk Flush Consistency

Add a new shared-memory array chunk_io_locks with one lock per chunk slot, then use it to serialize byte-level slot reads and writes so checkpoint flush reads a coherent chunk snapshot even during concurrent backend writes. Keep the dirty-bit drain/requeue logic unchanged.

**Steps**
1. Phase 1: Shared memory layout extension
2. Update [core/src/io/cache.rs](core/src/io/cache.rs) CacheControl struct to include chunk_io_locks_base and add accessor helper for a slot lock index.
3. Update CacheControl::init in [core/src/io/cache.rs](core/src/io/cache.rs) to accept chunk_io_locks pointer and initialize CHUNK_NUM_SLOTS locks.
4. Update shared memory size and trailing-array offsets in [core/src/io/io_control.rs](core/src/io/io_control.rs): insert a new chunk_io_locks_offset after existing chunk bucket-lock array and before fork-meta arrays. Recompute downstream offsets and final shmem_size accordingly.
5. Update IoControl::init_or_attach in [core/src/io/io_control.rs](core/src/io/io_control.rs) to compute chunk_io_locks pointer and pass it into CacheControl::init. Depends on steps 2-4.
6. Phase 2: Apply lock discipline to I/O paths
7. In [core/src/io/cache.rs](core/src/io/cache.rs), wrap byte write operations in write_block and write_blocks_to_slot with slot-level shared lock (read mode) so multiple writers can proceed while excluding flush snapshot capture.
8. In [core/src/io/cache.rs](core/src/io/cache.rs), update flush_dirty_chunk to acquire slot-level exclusive lock only for the local chunk read into buffer, then release before remote put_express call. Depends on step 7.
9. Keep slot-level lock hold time short: no network call while holding lock; only pread/pwrite and immediate metadata operations.
10. Phase 3: Lock ordering and safety rules
11. Document lock-order policy in [core/src/io/cache.rs](core/src/io/cache.rs): when both are needed, pin slot first, then slot I/O lock; do not hold bucket-chain locks while waiting for slot I/O locks.
12. Audit callsites that may combine bucket operations and slot I/O operations to ensure no new inversion or deadlock path; adjust comments where needed.
13. Phase 4: Verification
14. Run focused compile check for core crate and full workspace check.
15. Add a concurrency test in [core/src/io/cache.rs](core/src/io/cache.rs) test module or neighboring cache test file: concurrent writer thread repeatedly writes and marks dirty on one slot while flush_all_dirty_chunks runs; assert no dirty-bit loss and no panic.
16. Add a coherence-oriented test (or stress helper) where flush captures chunk buffer while writes happen; verify each uploaded chunk image corresponds to a coherent slot state boundary (no torn block image for a single 8KB block).

**Relevant files**
- [/Users/bolu/supabase/tiko/core/src/io/cache.rs](/Users/bolu/supabase/tiko/core/src/io/cache.rs) — add chunk_io_locks pointer, accessor, init wiring, slot-level lock usage in write and flush paths, and lock-order comments.
- [/Users/bolu/supabase/tiko/core/src/io/io_control.rs](/Users/bolu/supabase/tiko/core/src/io/io_control.rs) — update trailing shared-memory layout offsets and total size; wire pointer in init_or_attach.
- [/Users/bolu/supabase/tiko/worker/src/shmem.rs](/Users/bolu/supabase/tiko/worker/src/shmem.rs) — optional debug log size notes if startup logging references total shmem sizing.

**Verification**
1. cargo check -p core
2. cargo check
3. Run targeted concurrency test repeatedly (for example 100 iterations) to catch timing bugs.
4. Observe checkpoint path logs and confirm flush_all_dirty_chunks reports stable behavior under write load.

**Decisions**
- Lock cardinality: one chunk_io_lock per slot (CHUNK_NUM_SLOTS), as confirmed.
- Lock type: reuse AtomicRWLock for consistency with existing shared-memory lock primitives.
- Scope included: coherent slot snapshot protection for chunk byte I/O and shared-memory wiring.
- Scope excluded: redesigning bucket lock strategy, replacing AtomicRWLock implementation, and changing fork-meta locking.

**Further Considerations**
1. Throughput tuning after correctness: if contention appears, consider lock striping as a follow-up, not in initial fix.
2. Optional bounded retry in flush_all_dirty_chunks can reduce lag for slots dirtied again during the same checkpoint cycle.