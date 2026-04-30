use pgsys::common::{BLCKSZ, BlockNumber};
use pgsys::logging::{pg_log_debug2, pg_log_warning};
use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU32, Ordering};

use super::{CHAIN_NIL, MAX_USAGE_COUNT};
use crate::io::rwlock::AtomicRWLock;
use crate::{
    chunk::{CHUNK_SIZE, ChunkTag, RelFork},
    error::{Error, Result},
    io::{io_control::IoControl, store::Store},
};

pub const CHUNK_NUM_SLOTS: u32 = 1024;
pub(crate) const CHUNK_NUM_BUCKETS: u32 = CHUNK_NUM_SLOTS * 2;

thread_local! {
    static LOCAL_CHUNK_CLOCK_HAND: Cell<u32> = const { Cell::new(0) };
}

static CACHE_FILE: OnceLock<File> = OnceLock::new();

/// Field-wise atomic representation of a `ChunkTag`.
///
/// Each field is loaded/stored atomically with `Relaxed` ordering. An optimistic
/// reader without a lock may observe a "frankenstein" tag mixing fields from
/// different writer generations — this is identical in effect to the former
/// non-atomic torn read, and callers re-verify under a bucket lock before
/// acting. Synchronisation of the tag against other writes (dirty, chain
/// linkage) is provided by the bucket lock's Release/Acquire pair, not by the
/// ordering on these fields.
///
/// Layout matches `ChunkTag` (5 × 4 bytes = 20 bytes) so `ChunkSlot` keeps its
/// 36-byte size.
#[repr(C)]
struct AtomicChunkTag {
    spc_oid: AtomicU32,
    db_oid: AtomicU32,
    rel_number: AtomicU32,
    fork_number: AtomicI32,
    chunk_id: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<AtomicChunkTag>() == 20);

impl AtomicChunkTag {
    fn load(&self) -> ChunkTag {
        ChunkTag {
            spc_oid: self.spc_oid.load(Ordering::Relaxed),
            db_oid: self.db_oid.load(Ordering::Relaxed),
            rel_number: self.rel_number.load(Ordering::Relaxed),
            fork_number: self.fork_number.load(Ordering::Relaxed),
            chunk_id: self.chunk_id.load(Ordering::Relaxed),
        }
    }

    fn store(&self, t: ChunkTag) {
        self.spc_oid.store(t.spc_oid, Ordering::Relaxed);
        self.db_oid.store(t.db_oid, Ordering::Relaxed);
        self.rel_number.store(t.rel_number, Ordering::Relaxed);
        self.fork_number.store(t.fork_number, Ordering::Relaxed);
        self.chunk_id.store(t.chunk_id, Ordering::Relaxed);
    }
}

/// Metadata for a single cache slot. Lives in shared memory; one entry per slot.
///
/// The cache file stores the actual block data at `slot_index * CHUNK_SIZE`.
/// This struct tracks the slot's identity, eviction state, and dirty status.
///
/// Global lock-order invariant: no code path holds `io_lock` while acquiring a
/// bucket lock. `insert` deliberately releases `io_lock` after writing the
/// cache file and before taking the bucket write lock to preserve this, so
/// truncate's `bucket -> io_lock` order cannot deadlock against anything else.
///
/// Locking rules:
/// - `tag` is stable while `pin_count > 0`; modifications require the bucket write lock.
/// - `dirty` and data in the cache file are protected by the per-slot `io_lock`.
/// - `pin_count` is atomic. It is incremented either under a bucket lock
///   (`lookup_and_pin`, eviction) or without any bucket lock
///   (`flush_dirty_chunks`). Eviction's re-check under the bucket write lock
///   does NOT exclude flush pins; eviction and flush coexist safely because
///   both serialise on `io_lock` inside `try_flush_dirty_chunk` and race on
///   the `dirty` flag via atomic swap (only the winner actually PUTs).
/// - `usage_count` is atomic and may be modified without a lock.
/// - `next` is written only under the bucket write lock; reads are under the
///   bucket read or write lock.
#[repr(C)]
pub(crate) struct ChunkSlot {
    /// Identity of the chunk held in this slot. A default (all-zero) tag means the slot is empty.
    ///
    /// Stored as field-wise atomics so optimistic lock-free reads during eviction
    /// scans and truncation are well-defined (no data race). Writes happen under
    /// the bucket write lock; reads under a bucket lock see a consistent tag,
    /// while optimistic readers may see a mixed tag and must re-verify under
    /// the lock before acting.
    tag: AtomicChunkTag,
    /// True if the chunk has been written but not yet flushed to the store.
    /// The cache operates on full chunks, so a single flag is sufficient.
    /// Protected by the per-slot `io_lock`.
    dirty: AtomicBool,
    /// Clock-sweep age counter. Incremented on access (up to MAX_USAGE_COUNT), decremented
    /// by the eviction sweep. A slot with usage_count > 0 is skipped during eviction.
    usage_count: AtomicU8,
    _pad: [u8; 6],
    /// Number of active users holding a reference to this slot. A slot with pin_count > 0
    /// cannot be evicted. Modified atomically; eviction checks under the bucket write lock.
    pin_count: AtomicU32,
    /// Index of the next slot in the same bucket chain, or CHAIN_NIL if this is the tail.
    next: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<ChunkSlot>() == 36);

impl ChunkSlot {
    fn init(&mut self) {
        self.clear();
        self._pad = [0; 6];
        self.pin_count.store(0, Ordering::Relaxed);
    }

    /// Reset the slot for reuse. Zeroes the tag (marking it unoccupied) and
    /// all transient fields. Does NOT touch `pin_count`.
    ///
    /// Callers must hold the per-slot `io_lock` write (or be in one-time
    /// `init()`) to serialise against a flush_dirty_chunks thread that may
    /// still hold io_lock after losing the dirty.swap race in
    /// try_flush_dirty_chunk. Callers must additionally ensure the slot is
    /// either unlinked-and-pinned or held under the bucket write lock with
    /// pin_count == 0.
    fn clear(&self) {
        self.write_tag(ChunkTag::default());
        self.dirty.store(false, Ordering::Relaxed);
        self.usage_count.store(0, Ordering::Relaxed);
        self.next.store(CHAIN_NIL, Ordering::Relaxed);
    }

    /// Read the tag atomically field-by-field.
    ///
    /// Caller requirements:
    /// - a bucket lock (read or write) is held for this slot's bucket, or
    /// - the slot is pinned (`pin_count > 0`), or
    /// - the read is an optimistic probe that will be re-verified under a lock.
    ///
    /// Only the third case may observe a mixed tag (fields from different
    /// writer generations); the first two see a consistent tag because no
    /// writer can run concurrently.
    fn read_tag(&self) -> ChunkTag {
        self.tag.load()
    }

    /// Write the tag atomically field-by-field.
    ///
    /// Caller must hold the bucket write lock for the target bucket.
    fn write_tag(&self, new_tag: ChunkTag) {
        self.tag.store(new_tag)
    }

    /// Returns true if this slot holds a real chunk (tag is non-default).
    /// A freshly initialised or just-cleared slot has a zeroed tag and is considered empty.
    ///
    /// Same caller requirements as `read_tag`.
    fn is_occupied(&self) -> bool {
        self.read_tag() != ChunkTag::default()
    }
}

struct PinnedSlot<'a> {
    cache: &'a ChunkCache,
    slot_index: u32,
}
impl Drop for PinnedSlot<'_> {
    fn drop(&mut self) {
        self.cache.unpin(self.slot_index);
    }
}

#[repr(C)]
pub(crate) struct ChunkCache {
    slots_base: *const ChunkSlot,
    buckets_base: *const AtomicU32,
    bucket_locks_base: *const AtomicRWLock,
    io_locks_base: *const AtomicRWLock,
}

impl ChunkCache {
    pub(super) fn init(
        &mut self,
        slots: *mut ChunkSlot,
        buckets: *mut AtomicU32,
        bucket_locks: *mut AtomicRWLock,
        io_locks: *mut AtomicRWLock,
    ) {
        self.slots_base = slots;
        self.buckets_base = buckets;
        self.bucket_locks_base = bucket_locks;
        self.io_locks_base = io_locks;

        unsafe {
            for i in 0..CHUNK_NUM_SLOTS as usize {
                (*slots.add(i)).init();
                (*io_locks.add(i)).init();
            }
            for i in 0..CHUNK_NUM_BUCKETS as usize {
                (*buckets.add(i)).store(CHAIN_NIL, Ordering::Relaxed);
                (*bucket_locks.add(i)).init();
            }
        }

        let _ = Self::cache_file();
    }

    // --- Cache file access ---

    fn cache_file_path() -> PathBuf {
        crate::tiko_root_path().join("chunk_cache")
    }

    fn cache_file() -> &'static File {
        CACHE_FILE.get_or_init(|| {
            let path = Self::cache_file_path();
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap_or_else(|_| panic!("failed to open cache file {}", path.display()));

            let expected_size = CHUNK_NUM_SLOTS as u64 * CHUNK_SIZE as u64;
            if let Ok(meta) = file.metadata() {
                if meta.len() < expected_size {
                    file.set_len(expected_size)
                        .expect("failed to pre-allocate cache file");
                }
            }
            file
        })
    }

    #[inline(always)]
    fn chunk_offset_in_file(&self, slot_index: u32) -> u64 {
        slot_index as u64 * CHUNK_SIZE as u64
    }

    // --- Slot and bucket accessors ---

    fn slot(&self, slot_index: u32) -> &ChunkSlot {
        debug_assert!(slot_index < CHUNK_NUM_SLOTS);
        unsafe { &*self.slots_base.add(slot_index as usize) }
    }

    fn bucket_head(&self, bucket: u32) -> &AtomicU32 {
        debug_assert!(bucket < CHUNK_NUM_BUCKETS);
        unsafe { &*self.buckets_base.add(bucket as usize) }
    }

    fn bucket_lock(&self, bucket: u32) -> &AtomicRWLock {
        debug_assert!(bucket < CHUNK_NUM_BUCKETS);
        unsafe { &*self.bucket_locks_base.add(bucket as usize) }
    }

    fn io_lock(&self, slot_index: u32) -> &AtomicRWLock {
        debug_assert!(slot_index < CHUNK_NUM_SLOTS);
        unsafe { &*self.io_locks_base.add(slot_index as usize) }
    }

    fn bucket(&self, tag: &ChunkTag) -> (u32, &AtomicRWLock) {
        let bucket = tag.hash() % CHUNK_NUM_BUCKETS;
        (bucket, self.bucket_lock(bucket))
    }

    // ---- Chunk read/write interface ----

    pub(super) fn get_chunk(&self, tag: &ChunkTag, dst: &mut [u8]) -> Result<()> {
        debug_assert_eq!(dst.len(), CHUNK_SIZE);

        loop {
            if let Some(pinned) = self.lookup_and_pin(tag) {
                // Cache hit: read directly into caller's buffer under io_lock.
                let _guard = self.io_lock(pinned.slot_index).read();
                self.read_chunk_from_file(pinned.slot_index, dst)?;
                self.touch(pinned.slot_index);
                return Ok(());
            }

            // Cache miss: fetch from store, then insert into cache.
            let store = Store::try_get()?;
            let mut chunk_data = vec![0u8; CHUNK_SIZE];
            store.get_chunk(tag, &mut chunk_data)?;
            if self.insert(tag, &chunk_data, false)? {
                // New slot: data already written to cache file by insert().
                dst.copy_from_slice(&chunk_data);
                return Ok(());
            }

            // Another thread inserted first — read from the cache again in next loop
            // since it may hold newer data from a concurrent put_chunk.
        }
    }

    /// Write `data` into the chunk identified by `tag` at `block_offset`.
    ///
    /// When `block_offset == 0 && data.len() == CHUNK_SIZE` the chunk is
    /// overwritten wholesale (no read needed). Otherwise an atomic
    /// read-merge-write is performed under a single `io_lock` hold so
    /// concurrent partial writes to the same chunk cannot lose each other's
    /// updates.
    ///
    /// `data.len()` must be a non-zero multiple of `BLCKSZ` and the patched
    /// range must fit entirely within the chunk.
    ///
    /// On a full-chunk cache miss the chunk is inserted directly (no store
    /// fetch). On a partial-chunk cache miss the full chunk is fetched from
    /// the store; if the chunk does not exist yet (write beyond EOF / hole
    /// fill), it is treated as all-zeros before the patch is applied.
    pub(super) fn patch_chunk(&self, tag: &ChunkTag, block_offset: u32, data: &[u8]) -> Result<()> {
        debug_assert!(!data.is_empty());
        debug_assert_eq!(data.len() % BLCKSZ, 0);
        let byte_offset = block_offset as usize * BLCKSZ;
        debug_assert!(byte_offset + data.len() <= CHUNK_SIZE);
        let is_full_chunk = byte_offset == 0 && data.len() == CHUNK_SIZE;

        loop {
            // Cache hit: write under a single io_lock hold.
            if let Some(pinned) = self.lookup_and_pin(tag) {
                let slot_index = pinned.slot_index;
                let slot = self.slot(slot_index);
                let _guard = self.io_lock(slot_index).write();

                if is_full_chunk {
                    // Wholesale overwrite — no read needed.
                    self.write_chunk_to_file(slot_index, data)?;
                } else {
                    // Read existing chunk, merge, write back. The RMW runs
                    // under io_lock so concurrent patches cannot lose each
                    // other's updates.
                    let mut chunk = vec![0u8; CHUNK_SIZE];
                    self.read_chunk_from_file(slot_index, &mut chunk)?;
                    chunk[byte_offset..byte_offset + data.len()].copy_from_slice(data);
                    self.write_chunk_to_file(slot_index, &chunk)?;
                }
                slot.dirty.store(true, Ordering::Release);
                self.touch(slot_index);
                return Ok(());
            }

            // Cache miss: insert, possibly after merging with store content.
            // `insert` is atomic; if another thread inserted the same tag
            // concurrently, insert returns Ok(false) and we loop back — the
            // cache-hit branch will then apply our write on top of their
            // entry under io_lock.
            if is_full_chunk {
                if self.insert(tag, data, true)? {
                    return Ok(());
                }
            } else {
                let store = Store::try_get()?;
                let mut chunk = vec![0u8; CHUNK_SIZE];
                match store.get_chunk(tag, &mut chunk) {
                    Ok(()) => {}
                    Err(e) if e.is_not_found() => {} // chunk absent → treat as zeros
                    Err(e) => return Err(e),
                }
                chunk[byte_offset..byte_offset + data.len()].copy_from_slice(data);
                if self.insert(tag, &chunk, true)? {
                    return Ok(());
                }
            }
        }
    }

    // --- Block and Chunk IO operations ---

    // Raw full-chunk read. Does NOT acquire the io_lock — the caller must already
    // hold it (in any mode). Acquiring a read_lock from inside a write_lock would
    // deadlock because AtomicRWLock requires state == 0 to grant a read_lock.
    fn read_chunk_from_file(&self, slot_index: u32, buf: &mut [u8]) -> Result<()> {
        debug_assert_eq!(buf.len(), CHUNK_SIZE);

        let base_offset = self.chunk_offset_in_file(slot_index);
        let file = Self::cache_file();
        let mut done = 0;
        while done < buf.len() {
            let read = file.read_at(&mut buf[done..], base_offset + done as u64)?;
            if read == 0 {
                return Err(Error::unexpected_eof(format!(
                    "unexpected EOF while reading chunk cache slot {}, offset {}",
                    slot_index,
                    base_offset + done as u64
                )));
            }
            done += read;
        }

        Ok(())
    }

    // Raw full-chunk write. Does NOT acquire the io_lock — the caller must
    // already hold it in write mode. Does NOT modify the dirty flag.
    // Uses a progressive write loop because write_at is not guaranteed to
    // write all bytes in a single call.
    fn write_chunk_to_file(&self, slot_index: u32, buf: &[u8]) -> Result<()> {
        debug_assert_eq!(buf.len(), CHUNK_SIZE);
        debug_assert!(
            self.io_lock(slot_index).is_in_write(),
            "write_chunk_to_file requires a write io_lock on slot {slot_index}",
        );

        let base_offset = self.chunk_offset_in_file(slot_index);
        let file = Self::cache_file();
        let mut done = 0;
        while done < buf.len() {
            let written = file.write_at(&buf[done..], base_offset + done as u64);
            match written {
                Ok(0) => {
                    return Err(Error::unexpected_eof(format!(
                        "unexpected EOF while writing chunk cache slot {slot_index}, offset {}",
                        base_offset + done as u64
                    )));
                }
                Ok(n) => done += n,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    // --- Cache operations ---

    fn unpin(&self, slot_index: u32) {
        let slot = self.slot(slot_index);
        let prev = slot.pin_count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "unpin on slot {} with pin_count 0", slot_index);
    }

    fn touch(&self, slot_index: u32) {
        let slot = self.slot(slot_index);
        let _ = slot
            .usage_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if c < MAX_USAGE_COUNT {
                    Some(c + 1)
                } else {
                    None
                }
            });
    }

    /// Atomically decrement usage_count (clamped at 0). Returns the value
    /// *before* the decrement so the caller can tell whether the slot was
    /// already at zero.
    fn untouch(&self, slot_index: u32) -> u8 {
        let slot = self.slot(slot_index);
        match slot
            .usage_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if c > 0 { Some(c - 1) } else { None }
            }) {
            Ok(prev) => prev,
            Err(prev) => prev,
        }
    }

    fn lookup_and_pin<'a>(&'a self, tag: &ChunkTag) -> Option<PinnedSlot<'a>> {
        let (bucket, lock) = self.bucket(tag);

        let _guard = lock.read();
        let mut cur = self.bucket_head(bucket).load(Ordering::Acquire);
        let mut found = None;
        while cur != CHAIN_NIL {
            let slot = self.slot(cur);
            // Safety: bucket read lock is held.
            if slot.read_tag() == *tag {
                slot.pin_count.fetch_add(1, Ordering::Relaxed);
                found = Some(cur);
                break;
            }
            cur = slot.next.load(Ordering::Acquire);
        }
        drop(_guard);

        if found.is_some() {
            IoControl::get().stats.chunk_cache.inc_hits();
        } else {
            IoControl::get().stats.chunk_cache.inc_misses();
        }

        found.map(|slot_index| PinnedSlot {
            cache: self,
            slot_index,
        })
    }

    // Unlink the slot at slot_index from its bucket chain.
    // Caller must hold the bucket write lock.
    fn unlink_from_chain(&self, slot_index: u32, bucket: u32) {
        let head_ref = self.bucket_head(bucket);
        let head = head_ref.load(Ordering::Acquire);

        if head == CHAIN_NIL {
            return;
        }

        if head == slot_index {
            let next = self.slot(slot_index).next.load(Ordering::Acquire);
            head_ref.store(next, Ordering::Release);
            return;
        }

        let mut prev = head;
        loop {
            let prev_slot = self.slot(prev);
            let next = prev_slot.next.load(Ordering::Acquire);
            if next == CHAIN_NIL {
                return;
            }
            if next == slot_index {
                let after = self.slot(slot_index).next.load(Ordering::Acquire);
                prev_slot.next.store(after, Ordering::Release);
                return;
            }
            prev = next;
        }
    }

    /// Find a victim slot, flush it (in place, without unlinking) if dirty,
    /// then unlink and clear it for reuse. Returns the cleared slot pinned.
    ///
    /// The flush happens while the slot is **still in the bucket chain** so
    /// concurrent `lookup_and_pin` callers can hit the cache during the
    /// flush rather than falling through to the store. Their `io_lock.read`
    /// (in `get_chunk`) blocks behind `try_flush_dirty_chunk`'s
    /// `io_lock.write` and proceeds with the cached chunk once the flush
    /// returns. This closes a race where a chunk that has only ever lived
    /// in the cache (newly written, never flushed) would surface to readers
    /// as `chunk not found in store` while its first flush was in flight.
    /// Important here because chunks are 256 KB — the flush window over the
    /// store is wider than for meta.
    ///
    /// Lock-order invariant `bucket -> io_lock` is preserved: we never hold
    /// `io_lock` while acquiring a `bucket` lock. The bucket lock is taken,
    /// then released, before each `io_lock`-using step.
    ///
    /// Three phases:
    ///   1. Pin the candidate under `bucket.write` (slot stays in chain).
    ///   2. Flush dirty (if any) via `try_flush_dirty_chunk` — `io_lock.write`
    ///      only, no bucket lock held.
    ///   3. Re-acquire `bucket.write`, verify nothing else pinned the slot or
    ///      re-dirtied it, unlink, then `clear()` under `io_lock.write`.
    ///
    /// On any abort (flush failure, concurrent pin, slot re-dirtied), the slot
    /// is left in the chain with its current contents and the sweep moves on.
    /// No data loss path remains — the previous "evict-then-relink-on-failure"
    /// dance with duplicate detection is gone.
    fn evict_and_pin(&self) -> Result<u32> {
        let start = LOCAL_CHUNK_CLOCK_HAND.with(|h| h.get());

        for i in 0..(CHUNK_NUM_SLOTS * MAX_USAGE_COUNT as u32) {
            let slot_index = (start + i) % CHUNK_NUM_SLOTS;
            let slot = self.slot(slot_index);

            // Early check pin count before acquiring the lock to avoid
            // unnecessary locking of hot slots; re-verified under the lock.
            if slot.pin_count.load(Ordering::Relaxed) != 0 {
                continue;
            }

            // Decrement usage count; only consider slots that aged to 0.
            if self.untouch(slot_index) != 0 {
                continue;
            }

            // Optimistic tag read; re-verified under bucket.write below.
            let tag = slot.read_tag();
            let (bucket, lock) = self.bucket(&tag);

            // ── Phase 1: claim the slot (pin) under bucket.write. Slot
            //    remains linked in the bucket chain. ──────────────────────
            {
                let _guard = lock.write();

                if slot.pin_count.load(Ordering::Relaxed) != 0 {
                    continue;
                }
                if slot.read_tag() != tag {
                    continue;
                }
                slot.pin_count.fetch_add(1, Ordering::Relaxed);
            }

            // ── Phase 2: flush dirty (if any) WITHOUT holding any bucket
            //    lock. try_flush_dirty_chunk takes io_lock.write itself.
            //    Concurrent readers can pin via lookup_and_pin and will
            //    block on io_lock.read until the flush returns, then read
            //    the still-valid cached chunk. ────────────────────────────
            let flush_result = if slot.is_occupied() {
                self.try_flush_dirty_chunk(slot_index)
            } else {
                Ok(false)
            };

            match flush_result {
                Ok(true) => {
                    IoControl::get().stats.chunk_cache.inc_dirty_evictions();
                }
                Ok(false) => {
                    // Empty slot, or dirty was already false.
                }
                Err(e) => {
                    // Flush failed. try_flush_dirty_chunk restored dirty=true.
                    // Slot stays linked with its current contents — no data
                    // loss. Bump usage so the next sweep doesn't immediately
                    // retry the same failing slot, then move on.
                    self.touch(slot_index);
                    self.unpin(slot_index);
                    pg_log_warning(&format!(
                        "tiko: evict flush failed for slot {slot_index}, \
                         leaving in-chain for retry: {e}"
                    ));
                    continue;
                }
            }

            // ── Phase 3: re-acquire bucket.write, verify nothing
            //    interfered with the slot during phase 2, then unlink. ────
            let unlinked = {
                let _guard = lock.write();

                // Concurrent readers may have pinned the slot while we
                // flushed. If still pinned by anyone other than us, abort —
                // a future sweep can evict the slot for free (it's clean).
                if slot.pin_count.load(Ordering::Relaxed) != 1 {
                    false
                }
                // A concurrent patch_chunk (cache hit on this tag) may have
                // re-set dirty=true after our flush. Don't drop that data:
                // leave the slot in-chain.
                else if slot.dirty.load(Ordering::Acquire) {
                    false
                } else {
                    self.unlink_from_chain(slot_index, bucket);
                    true
                }
            };

            if !unlinked {
                self.touch(slot_index);
                self.unpin(slot_index);
                continue;
            }

            if slot.is_occupied() {
                IoControl::get().stats.chunk_cache.inc_evictions();
            }

            // Clear the slot for reuse. io_lock serialises against any
            // concurrent flush_dirty_chunks that may still hold io_lock on
            // this slot after losing its own dirty.swap race.
            {
                let _io_guard = self.io_lock(slot_index).write();
                slot.clear();
            }

            LOCAL_CHUNK_CLOCK_HAND.with(|h| h.set((slot_index + 1) % CHUNK_NUM_SLOTS));

            return Ok(slot_index);
        }

        Err(Error::EvictionSweepExhausted)
    }

    /// Insert `tag` with `chunk_data` into the cache atomically.
    ///
    /// The chunk data is written to the cache file **before** the slot is
    /// linked into the hash chain, so concurrent readers via `lookup_and_pin`
    /// can never observe stale file data from a previous occupant.
    ///
    /// Returns:
    /// - `Ok(true)` — new slot inserted, tag and file data are committed.
    /// - `Ok(false)` — another thread already inserted the same tag; nothing written.
    /// - `Err(...)` — file I/O error during the cache-file write.
    fn insert(&self, tag: &ChunkTag, chunk_data: &[u8], mark_dirty: bool) -> Result<bool> {
        debug_assert_eq!(chunk_data.len(), CHUNK_SIZE);

        let slot_index = self.evict_and_pin()?;
        let slot = self.slot(slot_index);

        // Write chunk data to the cache file BEFORE linking into the chain.
        // The slot is pinned but not in any chain, so no other thread can
        // discover it via lookup — the io_lock acquired here is uncontested.
        // We do NOT set dirty; that happens under the bucket lock below
        // (after writing the tag) so that flush_dirty_chunks never sees
        // dirty=true with a zeroed tag.
        {
            let _io_guard = self.io_lock(slot_index).write();
            if let Err(e) = self.write_chunk_to_file(slot_index, chunk_data) {
                drop(_io_guard);
                self.unpin(slot_index);
                return Err(e);
            }
        }

        let (bucket, lock) = self.bucket(tag);
        let guard = lock.write();

        // Check if another thread inserted the same tag while we were evicting.
        let old_head = self.bucket_head(bucket).load(Ordering::Acquire);
        let mut cur = old_head;
        while cur != CHAIN_NIL {
            let cur_slot = self.slot(cur);
            // Safety: bucket write lock is held.
            if cur_slot.read_tag() == *tag {
                drop(guard);
                self.unpin(slot_index);
                return Ok(false);
            }
            cur = cur_slot.next.load(Ordering::Acquire);
        }

        // No duplicate — commit the slot into the chain.
        // Safety: bucket write lock is held, slot is pinned with no other observers.
        slot.write_tag(*tag);
        if mark_dirty {
            slot.dirty.store(true, Ordering::Release);
        }
        self.touch(slot_index);
        slot.next.store(old_head, Ordering::Release);
        self.bucket_head(bucket)
            .store(slot_index, Ordering::Release);

        drop(guard);
        self.unpin(slot_index);

        Ok(true)
    }

    // Attempt to flush the chunk to the store if dirty. Returns:
    // - Ok(true) if flush succeeded and the chunk is now clean
    // - Ok(false) if the chunk was already clean (dirty was false)
    // - Err if the flush failed (e.g. store put_chunk returned an error)
    //
    // Safety invariant: the caller MUST have incremented slot.pin_count before calling this
    // function and must decrement it afterward. slot.tag is accessed while the io_lock is held;
    // this is safe because a pinned slot (pin_count > 0) cannot be evicted — eviction checks
    // pin_count == 0 under the bucket write_lock before claiming a slot, so the tag is stable.
    //
    // The io_lock is held across the entire operation (swap + read + PUT) to serialise against
    // concurrent truncations: truncate_relfork clears dirty under the same io_lock, so either
    // truncate runs first (was_dirty == false, we return Ok(false)) or flush runs first
    // (truncate waits until the PUT completes). This prevents a just-deleted S3 object from
    // being re-created by a flush that lost the race.
    fn try_flush_dirty_chunk(&self, slot_index: u32) -> Result<bool> {
        let store = Store::try_get()?;
        let _guard = self.io_lock(slot_index).write();

        let slot = self.slot(slot_index);
        debug_assert!(
            slot.pin_count.load(Ordering::Relaxed) > 0,
            "try_flush_dirty_chunk requires a pinned slot (slot {slot_index})",
        );
        let was_dirty = slot.dirty.swap(false, Ordering::AcqRel);
        if !was_dirty {
            return Ok(false);
        }

        let mut chunk_data = vec![0u8; CHUNK_SIZE];
        if let Err(e) = self.read_chunk_from_file(slot_index, &mut chunk_data) {
            slot.dirty.store(true, Ordering::Release);
            return Err(e);
        }

        // PUT while still holding io_lock — prevents a concurrent truncation from
        // clearing dirty and then having us re-create the S3 object afterward.
        // Safety: slot is pinned (pin_count > 0) and io_lock is held.
        let flush_tag = slot.read_tag();
        match store.patch_chunk(&flush_tag, 0, &chunk_data) {
            Ok(_) => Ok(true),
            Err(e) => {
                slot.dirty.store(true, Ordering::Release);
                pg_log_debug2(&format!(
                    "tiko: try_flush_dirty_chunk failed for slot {slot_index}: {e}",
                ));
                Err(e)
            }
        }
    }

    /// Flush all dirty chunks in the cache.
    ///
    /// Returns `Err` on the first flush failure, matching `mdimmedsync`'s
    /// behaviour of raising an error immediately on `fsync` failure.
    pub(super) fn flush_dirty_chunks(&self, for_relfork: Option<&RelFork>) -> Result<u32> {
        let mut flushed_chunk_cnt = 0;

        for slot_index in 0..CHUNK_NUM_SLOTS {
            let slot = self.slot(slot_index);

            // Quick check to skip non-dirty slots without pinning them
            if !slot.dirty.load(Ordering::Relaxed) {
                continue;
            }

            slot.pin_count.fetch_add(1, Ordering::Release);

            // Check dirty again after pinning to avoid flushing
            // a chunk that became clean after the first check
            if !slot.dirty.load(Ordering::Acquire) {
                self.unpin(slot_index);
                continue;
            }

            if let Some(rf) = for_relfork {
                // Safety: slot is pinned (pin_count > 0), which prevents eviction
                // from changing the tag.
                if slot.read_tag().relfork() != *rf {
                    self.unpin(slot_index);
                    continue;
                }
            }

            let result = self.try_flush_dirty_chunk(slot_index);
            self.unpin(slot_index);
            if let Ok(true) = result {
                flushed_chunk_cnt += 1;
            }
            result?;
        }

        Ok(flushed_chunk_cnt)
    }

    /// Optimistically truncate all chunks belonging to RelFork `rf`
    /// with blocks >= `first_block` in the chunk cache.
    /// Note that it is no-op if the refork has no chunks in the cache.
    pub(super) fn truncate_relfork(&self, rf: &RelFork, first_block: BlockNumber) {
        for slot_index in 0..CHUNK_NUM_SLOTS {
            let slot = self.slot(slot_index);

            // Optimistic read without a lock. The returned tag may mix fields
            // from different writer generations; we re-verify under the bucket
            // write lock below before acting.
            let tag = slot.read_tag();
            if tag.relfork() != *rf {
                continue;
            }

            let (bucket, lock) = self.bucket(&tag);
            // Guard is dropped at end of each iteration or on `continue`.
            let _guard = lock.write();

            // Re-verify: the slot may have been evicted and reused since the
            // optimistic read above.
            // Safety: bucket write lock is held.
            if slot.read_tag() != tag {
                continue;
            }

            let chunk_start = tag.start_block();
            let chunk_end = tag.end_block_exclusive();

            if first_block <= chunk_start {
                // Full overlap: the entire chunk is at or beyond the truncation point.
                // Always unlink so no new lookup can find stale truncated data.
                // Any pending dirty writes for this chunk are intentionally
                // discarded — the blocks are being truncated away.
                self.unlink_from_chain(slot_index, bucket);

                // io_lock serialises against a concurrent flush_dirty_chunks
                // that may still hold io_lock on this slot after losing the
                // dirty.swap race. Lock order bucket -> io_lock matches
                // evict_and_pin.
                let _io_guard = self.io_lock(slot_index).write();
                if slot.pin_count.load(Ordering::Relaxed) == 0 {
                    slot.clear();
                } else {
                    // Slot is in use — clear dirty to serialise with
                    // try_flush_dirty_chunk's swap+read+PUT sequence, preventing a
                    // just-deleted S3 object from being re-created by a racing flush.
                    // The slot is now orphaned (unlinked but tag still set); eviction
                    // will reclaim it after the pinner releases.
                    slot.dirty.store(false, Ordering::Release);
                }
            } else if first_block < chunk_end {
                // Partial overlap: the chunk straddles the truncation boundary.
                // The cache operates on full chunks, so dirty cannot be partially
                // cleared. Leave the flag as-is; the next flush will PUT the full
                // chunk including bytes for blocks >= first_block. The store is
                // responsible for discarding or overwriting those bytes when its
                // own truncation is applied.
            }
        }
    }
}
