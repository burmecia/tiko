use pgsys::logging::{pg_log_debug2, pg_log_warning};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU32, Ordering};

use super::{CHAIN_NIL, MAX_USAGE_COUNT};
use crate::{
    chunk::RelFork,
    error::{Error, Result},
    io::{io_control::IoControl, rwlock::AtomicRWLock, store::Store},
    relfork::RelForkMeta,
};
use pgsys::common::BlockNumber;

pub const META_NUM_SLOTS: u32 = 1024;
pub const META_NUM_BUCKETS: u32 = META_NUM_SLOTS * 2;

thread_local! {
    static LOCAL_META_CLOCK_HAND: Cell<u32> = const { Cell::new(0) };
}

/// Field-wise atomic representation of a `RelFork`.
///
/// Each field is loaded/stored atomically with `Relaxed` ordering. An
/// optimistic reader without a lock may observe a tag mixing fields from
/// different writer generations; callers re-verify under a lock before
/// acting. Synchronisation against other writes (dirty, nblocks, deleted,
/// chain linkage) is provided by the bucket lock and `io_lock`'s
/// Release/Acquire pairs — not by the ordering on these fields.
///
/// Layout matches `RelFork` (4 × 4 bytes = 16 bytes).
#[repr(C)]
struct AtomicRelFork {
    spc_oid: AtomicU32,
    db_oid: AtomicU32,
    rel_number: AtomicU32,
    fork_number: AtomicI32,
}

const _: () = assert!(std::mem::size_of::<AtomicRelFork>() == 16);

impl AtomicRelFork {
    fn load(&self) -> RelFork {
        RelFork {
            spc_oid: self.spc_oid.load(Ordering::Relaxed),
            db_oid: self.db_oid.load(Ordering::Relaxed),
            rel_number: self.rel_number.load(Ordering::Relaxed),
            fork_number: self.fork_number.load(Ordering::Relaxed),
        }
    }

    fn store(&self, t: RelFork) {
        self.spc_oid.store(t.spc_oid, Ordering::Relaxed);
        self.db_oid.store(t.db_oid, Ordering::Relaxed);
        self.rel_number.store(t.rel_number, Ordering::Relaxed);
        self.fork_number.store(t.fork_number, Ordering::Relaxed);
    }
}

/// Metadata for a single meta-cache slot. Lives in shared memory; one entry
/// per slot. Unlike ChunkSlot there is no backing cache file — nblocks and
/// deleted live directly in the slot.
///
/// Global lock-order invariant: no code path holds `io_lock` while acquiring
/// a bucket lock. The legal orderings are `bucket -> io_lock` (insert) or
/// `io_lock` alone (try_flush_dirty_meta, get_meta / put_meta / put_deleted
/// cache hits, eviction's flush and clear). Eviction releases its bucket
/// lock before each `io_lock`-using step and re-acquires bucket separately
/// to unlink — bucket and io_lock are never held together during eviction.
///
/// Locking rules:
/// - `tag` is stable while `pin_count > 0`; modifications require the bucket
///   write lock held together with `io_lock` write.
/// - `nblocks`, `deleted`, `dirty` are protected by `io_lock`.
/// - `pin_count` is atomic. It is incremented either under a bucket lock
///   (`lookup_and_pin`, eviction) or without any bucket lock
///   (`flush_dirty_metas`). Eviction's re-check under the bucket write lock
///   does NOT exclude flush pins; eviction and flush coexist safely because
///   both serialise on `io_lock` inside `try_flush_dirty_meta` and race on
///   the `dirty` flag via atomic swap (only the winner PUTs).
/// - `usage_count` is atomic and may be modified without a lock.
/// - `next` is written only under the bucket write lock; reads are under the
///   bucket read or write lock.
#[repr(C)]
pub(crate) struct MetaSlot {
    /// Identity of the relfork held in this slot. A default (all-zero) tag
    /// means the slot is empty. Stored as field-wise atomics so optimistic
    /// lock-free reads during eviction scans are well-defined (no data race,
    /// though a mixed tag is possible and callers must re-verify under a
    /// lock before acting).
    tag: AtomicRelFork,
    /// Number of blocks in the relfork. Protected by `io_lock`.
    nblocks: AtomicU32,
    /// True if the relfork has been marked deleted. Protected by `io_lock`.
    deleted: AtomicBool,
    /// True if the slot has been written but not yet flushed to the store.
    /// Protected by `io_lock`. Atomic swap on `dirty` is the serialisation
    /// point that decides which of eviction/flush actually performs the PUT.
    dirty: AtomicBool,
    /// Clock-sweep age counter. Incremented on access (up to MAX_USAGE_COUNT),
    /// decremented by the eviction sweep. Atomic; no lock required.
    usage_count: AtomicU8,
    _pad: [u8; 1],
    /// Number of active users holding a reference to this slot. A slot with
    /// pin_count > 0 cannot be evicted. Modified atomically; see struct-level
    /// note about which code paths hold a bucket lock when doing so.
    pin_count: AtomicU32,
    /// Index of the next slot in the same bucket chain, or CHAIN_NIL if this
    /// is the tail.
    next: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<MetaSlot>() == 32);

impl MetaSlot {
    fn init(&mut self) {
        self.clear();
        self._pad = [0; 1];
        self.pin_count.store(0, Ordering::Relaxed);
    }

    /// Reset the slot for reuse. Zeroes the tag and all transient fields.
    /// Does NOT touch `pin_count`.
    ///
    /// Callers must be the sole accessor: either during one-time `init()`, or
    /// from the eviction path while holding the per-slot `io_lock` write
    /// (to serialise against a flush_dirty_metas thread that may still hold
    /// io_lock after losing the dirty.swap race in try_flush_dirty_meta).
    fn clear(&self) {
        self.write_tag(RelFork::default());
        self.nblocks.store(0, Ordering::Relaxed);
        self.deleted.store(false, Ordering::Relaxed);
        self.dirty.store(false, Ordering::Relaxed);
        self.usage_count.store(0, Ordering::Relaxed);
        self.next.store(CHAIN_NIL, Ordering::Relaxed);
    }

    fn read_tag(&self) -> RelFork {
        self.tag.load()
    }

    fn write_tag(&self, new_tag: RelFork) {
        self.tag.store(new_tag)
    }

    fn is_occupied(&self) -> bool {
        self.read_tag() != RelFork::default()
    }
}

struct PinnedSlot<'a> {
    cache: &'a MetaCache,
    slot_index: u32,
}
impl Drop for PinnedSlot<'_> {
    fn drop(&mut self) {
        self.cache.unpin(self.slot_index);
    }
}

#[repr(C)]
pub(crate) struct MetaCache {
    slots_base: *const MetaSlot,
    buckets_base: *const AtomicU32,
    bucket_locks_base: *const AtomicRWLock,
    io_locks_base: *const AtomicRWLock,
}

impl MetaCache {
    pub(super) fn init(
        &mut self,
        slots: *mut MetaSlot,
        buckets: *mut AtomicU32,
        bucket_locks: *mut AtomicRWLock,
        io_locks: *mut AtomicRWLock,
    ) {
        self.slots_base = slots;
        self.buckets_base = buckets;
        self.bucket_locks_base = bucket_locks;
        self.io_locks_base = io_locks;

        unsafe {
            for i in 0..META_NUM_SLOTS as usize {
                (*slots.add(i)).init();
                (*io_locks.add(i)).init();
            }
            for i in 0..META_NUM_BUCKETS as usize {
                (*buckets.add(i)).store(CHAIN_NIL, Ordering::Relaxed);
                (*bucket_locks.add(i)).init();
            }
        }
    }

    // --- Slot and bucket accessors ---

    fn slot(&self, slot_index: u32) -> &MetaSlot {
        debug_assert!(slot_index < META_NUM_SLOTS);
        unsafe { &*self.slots_base.add(slot_index as usize) }
    }

    fn bucket_head(&self, bucket: u32) -> &AtomicU32 {
        debug_assert!(bucket < META_NUM_BUCKETS);
        unsafe { &*self.buckets_base.add(bucket as usize) }
    }

    fn bucket_lock(&self, bucket: u32) -> &AtomicRWLock {
        debug_assert!(bucket < META_NUM_BUCKETS);
        unsafe { &*self.bucket_locks_base.add(bucket as usize) }
    }

    fn io_lock(&self, slot_index: u32) -> &AtomicRWLock {
        debug_assert!(slot_index < META_NUM_SLOTS);
        unsafe { &*self.io_locks_base.add(slot_index as usize) }
    }

    fn bucket(&self, tag: &RelFork) -> (u32, &AtomicRWLock) {
        let bucket = tag.hash() % META_NUM_BUCKETS;
        (bucket, self.bucket_lock(bucket))
    }

    // ---- Meta read/write interface ----

    fn get_meta(&self, tag: &RelFork) -> Result<RelForkMeta> {
        loop {
            if let Some(pinned) = self.lookup_and_pin(tag) {
                // Cache hit: read meta fields and update usage count.
                let _guard = self.io_lock(pinned.slot_index).read();
                let slot = self.slot(pinned.slot_index);
                let nblocks = slot.nblocks.load(Ordering::Relaxed);
                let deleted = slot.deleted.load(Ordering::Relaxed);
                self.touch(pinned.slot_index);
                return Ok(RelForkMeta::new(nblocks, deleted));
            }

            // Cache miss: fetch from store and insert. If another thread raced
            // and inserted first (inserted == false), loop back so the cache-hit
            // branch reads their entry, which may hold newer data from a
            // concurrent put_nblocks or put_deleted.
            let (meta, inserted) = self.load_from_store(tag)?;
            if inserted {
                return Ok(meta);
            }
        }
    }

    pub(super) fn get_nblocks(&self, tag: &RelFork) -> Result<BlockNumber> {
        self.get_meta(tag).and_then(|meta| {
            if meta.deleted {
                Err(Error::not_found("relfork meta is deleted"))
            } else {
                Ok(meta.nblocks)
            }
        })
    }

    /// Update `nblocks` for an existing, non-deleted relfork. Preserves the
    /// `deleted` flag.
    ///
    /// Returns `Err::not_found` if the relfork is missing from both cache
    /// and store, or if it exists in the deleted state. The read-modify-write
    /// is performed under `io_lock` so concurrent `put_*`/`put_deleted`
    /// updates cannot race.
    pub(super) fn put_nblocks(&self, tag: &RelFork, nblocks: BlockNumber) -> Result<()> {
        loop {
            if let Some(pinned) = self.lookup_and_pin(tag) {
                let _guard = self.io_lock(pinned.slot_index).write();
                let slot = self.slot(pinned.slot_index);
                if slot.deleted.load(Ordering::Relaxed) {
                    return Err(Error::not_found("relfork is deleted"));
                }
                slot.nblocks.store(nblocks, Ordering::Relaxed);
                slot.dirty.store(true, Ordering::Release);
                self.touch(pinned.slot_index);
                return Ok(());
            }

            // Cache miss: consult the store. If the store has no record the
            // get_meta call propagates Err::not_found. Otherwise populate the
            // cache and loop back to apply the update under io_lock.
            self.load_from_store(tag)?;
        }
    }

    #[inline(always)]
    pub(super) fn get_deleted(&self, tag: &RelFork) -> Result<bool> {
        self.get_meta(tag).map(|meta| meta.deleted)
    }

    /// Atomically toggle the `deleted` flag. When transitioning to
    /// `deleted=true`, `nblocks` is zeroed; when transitioning to
    /// `deleted=false`, the current `nblocks` is preserved.
    ///
    /// The read-modify-write is performed under `io_lock` so concurrent
    /// `put_nblocks`/`put_meta` calls cannot have their update lost.
    pub(super) fn put_deleted(&self, tag: &RelFork, deleted: bool) -> Result<()> {
        loop {
            if let Some(pinned) = self.lookup_and_pin(tag) {
                let _guard = self.io_lock(pinned.slot_index).write();
                let slot = self.slot(pinned.slot_index);
                if slot.deleted.load(Ordering::Relaxed) {
                    return Err(Error::not_found("relfork meta is already deleted"));
                }
                if deleted {
                    slot.nblocks.store(0, Ordering::Relaxed);
                }
                slot.deleted.store(deleted, Ordering::Relaxed);
                slot.dirty.store(true, Ordering::Release);
                self.touch(pinned.slot_index);
                return Ok(());
            }

            // Cache miss: fetch from store and insert, then retry under io_lock.
            self.load_from_store(tag)?;

            // Loop back: lookup_and_pin will find either our insert or another
            // thread's concurrent insert, and the modification is performed
            // under io_lock.
        }
    }

    /// Create a relfork. On success the cache holds `nblocks=0, deleted=false`
    /// for `tag`, marked dirty so it will be flushed to the store.
    ///
    /// - If the relfork already exists as live (`deleted=false`): returns
    ///   `Err::already_exists` with no state change.
    /// - If it exists as `deleted=true`: transitions to
    ///   `deleted=false, nblocks=0` (recreate).
    /// - If it does not exist in either cache or store: inserts a fresh live
    ///   entry.
    ///
    /// The check-and-modify is performed under `io_lock` so two concurrent
    /// `create` calls on the same tag cannot both succeed — the loser sees
    /// the winner's commit and returns `Err::already_exists`.
    pub(super) fn create_relfork(&self, tag: &RelFork) -> Result<()> {
        loop {
            // Path A: tag already in cache. Decide under io_lock.
            if let Some(pinned) = self.lookup_and_pin(tag) {
                let _guard = self.io_lock(pinned.slot_index).write();
                let slot = self.slot(pinned.slot_index);
                if !slot.deleted.load(Ordering::Relaxed) {
                    return Err(Error::already_exists("relfork already exists"));
                }
                // Was deleted=true — recreate with nblocks=0, mark dirty so
                // the transition is persisted.
                slot.nblocks.store(0, Ordering::Relaxed);
                slot.deleted.store(false, Ordering::Relaxed);
                slot.dirty.store(true, Ordering::Release);
                self.touch(pinned.slot_index);
                return Ok(());
            }

            // Path B: cache miss — consult the store.
            match self.load_from_store(tag) {
                Ok(_) => {
                    // Populate cache and loop back to decide under io_lock
                    // in Path A.
                }
                Err(e) if e.is_not_found() => {
                    // Truly new. Insert deleted=false, nblocks=0, dirty=true.
                    // If another thread raced and inserted first, insert
                    // returns Ok(false) and we loop back to verify via Path A.
                    let fresh = RelForkMeta::new(0, false);
                    if self.insert(tag, &fresh, true)? {
                        return Ok(());
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    // --- Cache operations ---

    fn unpin(&self, slot_index: u32) {
        let slot = self.slot(slot_index);
        let prev = slot.pin_count.fetch_sub(1, Ordering::Release);
        assert!(prev > 0, "unpin on slot {} with pin_count 0", slot_index);
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

    fn lookup_and_pin<'a>(&'a self, tag: &RelFork) -> Option<PinnedSlot<'a>> {
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
            IoControl::get().stats.meta_cache.inc_hits();
        } else {
            IoControl::get().stats.meta_cache.inc_misses();
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
    /// blocks behind `try_flush_dirty_meta`'s `io_lock.write` and proceeds
    /// with the cached value once the flush returns. This closes a race
    /// where a first-time-create relfork (no prior version on disk) would
    /// surface to readers as `relfork not found` while its first flush was
    /// in flight.
    ///
    /// Lock-order invariant `bucket -> io_lock` is preserved: we never hold
    /// `io_lock` while acquiring a `bucket` lock. The bucket lock is taken,
    /// then released, before each `io_lock`-using step.
    ///
    /// Three phases:
    ///   1. Pin the candidate under `bucket.write` (slot stays in chain).
    ///   2. Flush dirty (if any) via `try_flush_dirty_meta` — `io_lock.write`
    ///      only, no bucket lock held.
    ///   3. Re-acquire `bucket.write`, verify nothing else pinned the slot or
    ///      re-dirtied it, unlink, then `clear()` under `io_lock.write`.
    ///
    /// On any abort (flush failure, concurrent pin, slot re-dirtied), the slot
    /// is left in the chain with its current contents and the sweep moves on.
    /// No data loss path remains — the previous "evict-then-relink-on-failure"
    /// dance with duplicate detection is gone.
    fn evict_and_pin(&self) -> Result<u32> {
        let start = LOCAL_META_CLOCK_HAND.with(|h| h.get());

        for i in 0..(META_NUM_SLOTS * MAX_USAGE_COUNT as u32) {
            let slot_index = (start + i) % META_NUM_SLOTS;
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
            //    lock. try_flush_dirty_meta takes io_lock.write itself.
            //    Concurrent readers can pin via lookup_and_pin and will
            //    block on io_lock.read until the flush returns, then read
            //    valid cached values. ─────────────────────────────────────
            let flush_result = if slot.is_occupied() {
                self.try_flush_dirty_meta(slot_index)
            } else {
                Ok(false)
            };

            match flush_result {
                Ok(true) => {
                    IoControl::get().stats.meta_cache.inc_dirty_evictions();
                }
                Ok(false) => {
                    // Empty slot, or dirty was already false.
                }
                Err(e) => {
                    // Flush failed. try_flush_dirty_meta restored dirty=true.
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
                // A concurrent put_nblocks / put_deleted (cache hit on this
                // tag) may have updated the slot and re-set dirty=true after
                // our flush. Don't drop that data: leave the slot in-chain.
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
                IoControl::get().stats.meta_cache.inc_evictions();
            }

            // Clear the slot for reuse. io_lock serialises against any
            // concurrent flush_dirty_metas that may still hold io_lock on
            // this slot after losing its own dirty.swap race.
            {
                let _io_guard = self.io_lock(slot_index).write();
                slot.clear();
            }

            LOCAL_META_CLOCK_HAND.with(|h| h.set((slot_index + 1) % META_NUM_SLOTS));

            return Ok(slot_index);
        }

        Err(Error::EvictionSweepExhausted)
    }

    /// Insert `tag` with `meta` into the cache atomically.
    ///
    /// Returns:
    /// - `Ok(true)` — new slot inserted.
    /// - `Ok(false)` — another thread already inserted the same tag; nothing written.
    /// - `Err(...)` — other error.
    fn insert(&self, tag: &RelFork, meta: &RelForkMeta, mark_dirty: bool) -> Result<bool> {
        let slot_index = self.evict_and_pin()?;
        let slot = self.slot(slot_index);

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

        // No duplicate — commit the slot.
        //
        // tag/nblocks/deleted/dirty are written under io_lock so a concurrent
        // flush_dirty_metas (which iterates slots by index and can pin this
        // slot even before it is linked into the chain) cannot observe a
        // partially-committed state, and cannot race try_flush_dirty_meta
        // against our writes. Lock order bucket -> io_lock is consistent
        // with evict_and_pin's clear() path.
        //
        // Chain pointers (next, bucket_head) are modified outside io_lock —
        // they're protected by the bucket write lock, which is the only
        // discipline chain readers follow.
        //
        // Safety: bucket write lock is held; the slot is pinned. Until
        // bucket_head is published, lookup_and_pin cannot find this slot,
        // but flush_dirty_metas may still reach it by index — hence the
        // io_lock around the meta writes.
        {
            let _io_guard = self.io_lock(slot_index).write();
            slot.write_tag(*tag);
            slot.nblocks.store(meta.nblocks, Ordering::Relaxed);
            slot.deleted.store(meta.deleted, Ordering::Relaxed);
            if mark_dirty {
                slot.dirty.store(true, Ordering::Release);
            }
        }
        self.touch(slot_index);
        slot.next.store(old_head, Ordering::Release);
        self.bucket_head(bucket)
            .store(slot_index, Ordering::Release);

        drop(guard);
        self.unpin(slot_index);

        Ok(true)
    }

    /// Load the meta for `tag` from the store and insert it into the cache.
    /// Fetch meta from the store and insert into cache.
    ///
    /// Returns `(meta, inserted)` where `inserted` is `true` if this call
    /// populated the cache slot, or `false` if another thread raced and
    /// inserted first. Callers that need to return the freshest value should
    /// loop back to the cache-hit path when `inserted == false`.
    fn load_from_store(&self, tag: &RelFork) -> Result<(RelForkMeta, bool)> {
        let store = Store::try_get()?;
        let meta = store.get_meta(tag)?;
        let inserted = self.insert(tag, &meta, false)?;
        Ok((meta, inserted))
    }

    // Attempt to flush the dirty slot to the store. Returns:
    // - Ok(true) if flush succeeded and the slot is now clean.
    // - Ok(false) if `dirty` was already false (another thread — typically a
    //   racing eviction or flush — won the `dirty.swap`).
    // - Err if Store::put_meta failed. On error, `dirty` is restored to true
    //   so a later flush attempt will retry.
    //
    // Caller MUST have incremented `pin_count` before calling and decrement it
    // afterward. The `dirty.swap` + PUT pair runs under `io_lock` write so
    // only one of several concurrent flushers (from eviction or
    // flush_dirty_metas) actually performs the PUT.
    fn try_flush_dirty_meta(&self, slot_index: u32) -> Result<bool> {
        let store = Store::try_get()?;
        let _guard = self.io_lock(slot_index).write();

        let slot = self.slot(slot_index);
        debug_assert!(
            slot.pin_count.load(Ordering::Relaxed) > 0,
            "try_flush_dirty_meta requires a pinned slot (slot {slot_index})",
        );
        let was_dirty = slot.dirty.swap(false, Ordering::AcqRel);
        if !was_dirty {
            return Ok(false);
        }

        // Read tag/nblocks/deleted and PUT while still holding io_lock: this
        // serialises against insert and put_meta / put_deleted writers, which
        // also take io_lock, so the values PUT are internally consistent.
        // Tag is stable because the slot is pinned.
        let flush_tag = slot.read_tag();
        let flush_meta = RelForkMeta::new(
            slot.nblocks.load(Ordering::Relaxed),
            slot.deleted.load(Ordering::Relaxed),
        );
        match store.put_meta(&flush_tag, &flush_meta) {
            Ok(_) => Ok(true),
            Err(e) => {
                slot.dirty.store(true, Ordering::Release);
                pg_log_debug2(&format!(
                    "tiko: try_flush_dirty_meta failed for slot {slot_index}: {e}",
                ));
                Err(e)
            }
        }
    }

    /// Flush all dirty meta slots in the cache.
    ///
    /// Returns `Err` on the first flush failure, matching `mdimmedsync`'s
    /// behaviour of raising an error immediately on `fsync` failure.
    pub(super) fn flush_dirty_metas(&self, for_relfork: Option<&RelFork>) -> Result<u32> {
        let mut flushed_meta_cnt = 0;

        for slot_index in 0..META_NUM_SLOTS {
            let slot = self.slot(slot_index);

            // Quick check to skip non-dirty slots without pinning them
            if !slot.dirty.load(Ordering::Relaxed) {
                continue;
            }

            slot.pin_count.fetch_add(1, Ordering::Release);

            // Re-check dirty after pinning: a concurrent flush/eviction may have
            // already cleared it via `dirty.swap` between our pre-check and our
            // pin. Acquire pairs with the Release on whoever set/cleared dirty.
            if !slot.dirty.load(Ordering::Acquire) {
                self.unpin(slot_index);
                continue;
            }

            if let Some(rf) = for_relfork {
                // Optimistic filter read. Eviction can still pin this slot
                // concurrently and rewrite the tag to default via clear() —
                // pin_count > 0 does NOT exclude eviction from that. A
                // mismatched (possibly mixed) tag just means we skip; if a
                // mixed tag happens to match, try_flush_dirty_meta's
                // dirty.swap will bail when a concurrent flusher wins.
                if slot.read_tag() != *rf {
                    self.unpin(slot_index);
                    continue;
                }
            }

            let result = self.try_flush_dirty_meta(slot_index);
            self.unpin(slot_index);
            if let Ok(true) = result {
                flushed_meta_cnt += 1;
            }
            result?;
        }

        Ok(flushed_meta_cnt)
    }
}
