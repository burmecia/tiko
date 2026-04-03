//! Write-back per-fork nblocks map in IoControl shared memory.
//!
//! A fixed-size open-addressing hash table storing the live block count for
//! each relation fork, symmetric with the chunk cache ([`CacheControl`]).
//! All PostgreSQL backends share one copy via PG shared memory.
//!
//! # Design
//!
//! - **4096 entries, 64 partitions**: enough for a busy workload; 96 KB of
//!   shmem. At checkpoint time dirty entries are drained to express + log.
//! - **Write-back**: `set` writes to shmem only (no express I/O).
//!   Express is written by `drain_dirty` at checkpoint time.
//! - **`set_clean`**: populate from a cold express read without marking dirty.
//! - **Hash function**: FNV-1a over the four `RelFork` fields, matching
//!   [`ChunkTag::hash`] minus the `chunk_id` component.
//! - **Overflow eviction**: when a partition is full, the first valid
//!   non-dirty entry is evicted. If every entry in the partition is dirty,
//!   one dirty entry is force-evicted (caller is responsible for flushing it
//!   to express before the next checkpoint snapshot).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::chunk::{REL_FORK_SIZE, RelFork};

use super::cache::AtomicRWLock;

// ── Constants ──

/// Number of entries in the nblocks hash table.
pub const NBLOCKS_NUM_ENTRIES: u32 = 4096;

/// Number of hash-table partitions (lock granularity).
pub const NBLOCKS_NUM_PARTITIONS: u32 = 64;

/// Entries per partition.
const ENTRIES_PER_PARTITION: u32 = NBLOCKS_NUM_ENTRIES / NBLOCKS_NUM_PARTITIONS;

// ── NblocksEntry ──

/// One slot in the nblocks hash table. Lives in PG shared memory.
///
/// Size = 16 (RelFork) + 4 (nblocks) + 1 (dirty) + 1 (valid) + 2 (pad) = 24 bytes.
#[repr(C)]
pub struct NblocksEntry {
    /// Key: the relation fork whose nblocks this slot tracks.
    pub tag: RelFork,
    /// Current block count (little-endian u32 in shmem).
    pub nblocks: AtomicU32,
    /// True when this entry has been written since the last `drain_dirty`.
    pub dirty: AtomicBool,
    /// True when this slot is occupied (valid key + nblocks).
    pub valid: AtomicBool,
    pub _pad: [u8; 2],
}

const _: () = assert!(std::mem::size_of::<NblocksEntry>() == 24);

impl NblocksEntry {
    fn init(&self) {
        self.nblocks.store(0, Ordering::Relaxed);
        self.dirty.store(false, Ordering::Relaxed);
        self.valid.store(false, Ordering::Relaxed);
    }
}

// ── NblocksControl ──

/// Shared-memory control block for the nblocks hash table.
///
/// Embedded in [`IoControl`]. The actual entry and lock arrays follow the
/// `IoControl` struct in the shared memory allocation (pointer arithmetic).
#[repr(C)]
pub struct NblocksControl {
    pub num_entries: u32,
    pub num_partitions: u32,
    entries_base: *const NblocksEntry,
    locks_base: *const AtomicRWLock,
}

// Safety: lives in PG shared memory, mapped at the same VA in all processes.
unsafe impl Send for NblocksControl {}
unsafe impl Sync for NblocksControl {}

impl NblocksControl {
    /// Initialise the control block and zero all entries + locks.
    ///
    /// `entries` and `locks` must point into the shared memory region
    /// immediately following `IoControl` (same allocation).
    pub fn init(&mut self, entries: *mut NblocksEntry, locks: *mut AtomicRWLock) {
        self.num_entries = NBLOCKS_NUM_ENTRIES;
        self.num_partitions = NBLOCKS_NUM_PARTITIONS;
        self.entries_base = entries;
        self.locks_base = locks;

        unsafe {
            for i in 0..NBLOCKS_NUM_ENTRIES as usize {
                (*entries.add(i)).init();
            }
            // AtomicRWLock::init is available via the cache module's pub(crate) path.
            // We initialise by zeroing — state=0 means "unlocked".
            std::ptr::write_bytes(
                locks as *mut u8,
                0,
                NBLOCKS_NUM_PARTITIONS as usize * std::mem::size_of::<AtomicRWLock>(),
            );
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn entry(&self, idx: usize) -> &NblocksEntry {
        debug_assert!(idx < NBLOCKS_NUM_ENTRIES as usize);
        unsafe { &*self.entries_base.add(idx) }
    }

    fn lock(&self, partition: usize) -> &AtomicRWLock {
        debug_assert!(partition < NBLOCKS_NUM_PARTITIONS as usize);
        unsafe { &*self.locks_base.add(partition) }
    }

    /// FNV-1a hash over the four RelFork fields.
    fn hash(rf: RelFork) -> u32 {
        const FNV_OFFSET: u32 = 2166136261;
        const FNV_PRIME: u32 = 16777619;
        let mut h = FNV_OFFSET;
        for &byte in &rf.spc_oid.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &rf.db_oid.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &rf.rel_number.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        for &byte in &rf.fork_number.to_le_bytes() {
            h ^= byte as u32;
            h = h.wrapping_mul(FNV_PRIME);
        }
        h
    }

    /// Return the partition index and the start index within `entries_base`
    /// for the first slot of the given `RelFork`.
    fn partition_and_start(rf: RelFork) -> (usize, usize) {
        let h = Self::hash(rf);
        let partition = (h % NBLOCKS_NUM_PARTITIONS) as usize;
        let start = partition * ENTRIES_PER_PARTITION as usize;
        (partition, start)
    }

    // ── Public API ────────────────────────────────────────────────────────

    /// Insert or update `nblocks` for `rf`, marking the entry dirty.
    ///
    /// Dirty entries are written to express and the nblocks log at checkpoint
    /// time by [`drain_dirty`]. This is the normal write path (smgr extend /
    /// truncate / create).
    pub fn set(&self, rf: RelFork, nblocks: u32) {
        self.upsert(rf, nblocks, true);
    }

    /// Populate an entry from a cold express read without marking it dirty.
    ///
    /// Used by `file_nblocks` to cache a value retrieved from express,
    /// avoiding future round-trips without dirtying the entry.
    pub fn set_clean(&self, rf: RelFork, nblocks: u32) {
        self.upsert(rf, nblocks, false);
    }

    fn upsert(&self, rf: RelFork, nblocks: u32, dirty: bool) {
        let (partition, start) = Self::partition_and_start(rf);
        let lock = self.lock(partition);
        lock.write_lock();

        // Linear probe within the partition.
        let mut evict_idx: Option<usize> = None;
        for i in 0..ENTRIES_PER_PARTITION as usize {
            let idx = start + i;
            let entry = self.entry(idx);

            if !entry.valid.load(Ordering::Relaxed) {
                // Empty slot: claim it.
                self.write_entry(entry, rf, nblocks, dirty);
                lock.write_unlock();
                return;
            }
            if Self::tags_equal(entry, rf) {
                // Update in place.
                entry.nblocks.store(nblocks, Ordering::Relaxed);
                if dirty {
                    entry.dirty.store(true, Ordering::Relaxed);
                }
                lock.write_unlock();
                return;
            }
            // Candidate for eviction: prefer non-dirty.
            if evict_idx.is_none() && !entry.dirty.load(Ordering::Relaxed) {
                evict_idx = Some(idx);
            }
        }

        // Partition full — evict a slot.
        let evict_idx = match evict_idx {
            Some(i) => i,
            None => {
                // All slots dirty: force-evict first slot (slow path, rare).
                start
            }
        };
        self.write_entry(self.entry(evict_idx), rf, nblocks, dirty);
        lock.write_unlock();
    }

    fn write_entry(&self, entry: &NblocksEntry, rf: RelFork, nblocks: u32, dirty: bool) {
        // Write tag fields individually (RelFork is not Copy-atomic).
        let encoded = rf.encode();
        unsafe {
            let tag_ptr = &entry.tag as *const RelFork as *mut u8;
            std::ptr::copy_nonoverlapping(encoded.as_ptr(), tag_ptr, REL_FORK_SIZE);
        }
        entry.nblocks.store(nblocks, Ordering::Relaxed);
        entry.dirty.store(dirty, Ordering::Relaxed);
        entry.valid.store(true, Ordering::Release);
    }

    fn tags_equal(entry: &NblocksEntry, rf: RelFork) -> bool {
        // Read under the partition lock (shared in get(), exclusive in upsert/remove).
        // No atomics needed for the tag fields: the lock provides the ordering.
        entry.tag.spc_oid == rf.spc_oid
            && entry.tag.db_oid == rf.db_oid
            && entry.tag.rel_number == rf.rel_number
            && entry.tag.fork_number == rf.fork_number
    }

    /// Lookup `nblocks` for `rf`. Returns `Some(n)` on hit, `None` on miss.
    pub fn get(&self, rf: RelFork) -> Option<u32> {
        let (partition, start) = Self::partition_and_start(rf);
        let lock = self.lock(partition);
        lock.read_lock();

        let mut result = None;
        for i in 0..ENTRIES_PER_PARTITION as usize {
            let entry = self.entry(start + i);
            if !entry.valid.load(Ordering::Acquire) {
                break; // Empty slot — key is absent (linear probe invariant).
            }
            if Self::tags_equal(entry, rf) {
                result = Some(entry.nblocks.load(Ordering::Relaxed));
                break;
            }
        }

        lock.read_unlock();
        result
    }

    /// Remove the entry for `rf` (on relation delete / drop).
    ///
    /// Compacts the partition to maintain the linear-probe invariant.
    pub fn remove(&self, rf: RelFork) {
        let (partition, start) = Self::partition_and_start(rf);
        let lock = self.lock(partition);
        lock.write_lock();

        let mut found_at: Option<usize> = None;
        for i in 0..ENTRIES_PER_PARTITION as usize {
            let idx = start + i;
            let entry = self.entry(idx);
            if !entry.valid.load(Ordering::Relaxed) {
                break;
            }
            if Self::tags_equal(entry, rf) {
                found_at = Some(i);
                break;
            }
        }

        if let Some(pos) = found_at {
            // Find the end of the contiguous occupied run starting at pos+1.
            let mut run_end = pos + 1;
            while run_end < ENTRIES_PER_PARTITION as usize {
                if !self.entry(start + run_end).valid.load(Ordering::Relaxed) {
                    break;
                }
                run_end += 1;
            }
            // Shift entries left to fill the gap at `pos`.
            for i in pos..run_end - 1 {
                let src = start + i + 1;
                let dst = start + i;
                let src_entry = self.entry(src);
                let dst_entry = self.entry(dst);
                let encoded = src_entry.tag.encode();
                unsafe {
                    let tag_ptr = &dst_entry.tag as *const RelFork as *mut u8;
                    std::ptr::copy_nonoverlapping(encoded.as_ptr(), tag_ptr, REL_FORK_SIZE);
                }
                dst_entry
                    .nblocks
                    .store(src_entry.nblocks.load(Ordering::Relaxed), Ordering::Relaxed);
                dst_entry
                    .dirty
                    .store(src_entry.dirty.load(Ordering::Relaxed), Ordering::Relaxed);
                dst_entry.valid.store(true, Ordering::Release);
            }
            // Mark the now-vacated tail slot invalid.
            self.entry(start + run_end - 1)
                .valid
                .store(false, Ordering::Release);
        }

        lock.write_unlock();
    }

    /// Drain all dirty entries: call `f(rf, nblocks)` for each, then clear dirty.
    ///
    /// Called at checkpoint time to flush nblocks to express and the nblocks log.
    /// Acquires each partition's write lock for the entire partition scan so that
    /// the dirty-clear is atomic with the value snapshot, and the partition is not
    /// mutated mid-scan.  Concurrent `set` calls on other partitions proceed
    /// without blocking.
    pub fn drain_dirty(&self, mut f: impl FnMut(RelFork, u32)) {
        for partition in 0..NBLOCKS_NUM_PARTITIONS as usize {
            let lock = self.lock(partition);
            lock.write_lock();

            let start = partition * ENTRIES_PER_PARTITION as usize;
            let mut dirty_entries: [(RelFork, u32); ENTRIES_PER_PARTITION as usize] = [(
                RelFork {
                    spc_oid: 0,
                    db_oid: 0,
                    rel_number: 0,
                    fork_number: 0,
                },
                0,
            );
                ENTRIES_PER_PARTITION as usize];
            let mut n_dirty = 0;

            for i in 0..ENTRIES_PER_PARTITION as usize {
                let entry = self.entry(start + i);
                if !entry.valid.load(Ordering::Acquire) {
                    break;
                }
                if entry.dirty.load(Ordering::Relaxed) {
                    dirty_entries[n_dirty] = (entry.tag, entry.nblocks.load(Ordering::Relaxed));
                    n_dirty += 1;
                    entry.dirty.store(false, Ordering::Relaxed);
                }
            }

            lock.write_unlock();

            // Call f outside the lock so that it can do I/O without blocking
            // concurrent readers.  The dirty flag has already been cleared above.
            for i in 0..n_dirty {
                let (rf, nblocks) = dirty_entries[i];
                f(rf, nblocks);
            }
        }
    }
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::RelFork;

    fn make_control() -> Box<(NblocksControl, Vec<NblocksEntry>, Vec<AtomicRWLock>)> {
        let mut entries: Vec<NblocksEntry> = (0..NBLOCKS_NUM_ENTRIES)
            .map(|_| NblocksEntry {
                tag: RelFork {
                    spc_oid: 0,
                    db_oid: 0,
                    rel_number: 0,
                    fork_number: 0,
                },
                nblocks: AtomicU32::new(0),
                dirty: AtomicBool::new(false),
                valid: AtomicBool::new(false),
                _pad: [0; 2],
            })
            .collect();
        let mut locks: Vec<AtomicRWLock> = (0..NBLOCKS_NUM_PARTITIONS)
            .map(|_| AtomicRWLock::new_unlocked())
            .collect();
        let mut ctrl = NblocksControl {
            num_entries: 0,
            num_partitions: 0,
            entries_base: std::ptr::null(),
            locks_base: std::ptr::null(),
        };
        ctrl.init(entries.as_mut_ptr(), locks.as_mut_ptr());
        // Return all three so the vecs stay alive.
        Box::new((ctrl, entries, locks))
    }

    fn rf(rel_number: u32) -> RelFork {
        RelFork {
            spc_oid: 1663,
            db_oid: 5,
            rel_number,
            fork_number: 0,
        }
    }

    #[test]
    fn set_get_round_trip() {
        let b = make_control();
        let ctrl = &b.0;
        ctrl.set(rf(1), 42);
        assert_eq!(ctrl.get(rf(1)), Some(42));
    }

    #[test]
    fn set_clean_not_dirty() {
        let b = make_control();
        let ctrl = &b.0;
        ctrl.set_clean(rf(2), 10);
        assert_eq!(ctrl.get(rf(2)), Some(10));

        // drain_dirty should yield nothing.
        let mut count = 0;
        ctrl.drain_dirty(|_, _| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn drain_dirty_yields_and_clears() {
        let b = make_control();
        let ctrl = &b.0;
        ctrl.set(rf(3), 7);
        ctrl.set(rf(4), 9);

        let mut seen: Vec<(u32, u32)> = Vec::new();
        ctrl.drain_dirty(|r, n| seen.push((r.rel_number, n)));
        assert_eq!(seen.len(), 2);
        seen.sort();
        assert_eq!(seen, vec![(3, 7), (4, 9)]);

        // Second drain: nothing dirty.
        let mut count = 0;
        ctrl.drain_dirty(|_, _| count += 1);
        assert_eq!(count, 0);

        // Values still readable after drain.
        assert_eq!(ctrl.get(rf(3)), Some(7));
    }

    #[test]
    fn remove_clears_entry() {
        let b = make_control();
        let ctrl = &b.0;
        ctrl.set(rf(5), 20);
        assert_eq!(ctrl.get(rf(5)), Some(20));
        ctrl.remove(rf(5));
        assert_eq!(ctrl.get(rf(5)), None);
    }

    #[test]
    fn update_in_place() {
        let b = make_control();
        let ctrl = &b.0;
        ctrl.set(rf(6), 100);
        ctrl.set(rf(6), 200);
        assert_eq!(ctrl.get(rf(6)), Some(200));
    }

    #[test]
    fn collision_handling() {
        // Insert multiple RelForks that hash to the same partition.
        let b = make_control();
        let ctrl = &b.0;
        // These have different rel_numbers so they are distinct keys.
        let (partition_a, _) = NblocksControl::partition_and_start(rf(100));
        // Find another rel_number that maps to the same partition.
        let mut partner = 101u32;
        loop {
            if NblocksControl::partition_and_start(rf(partner)).0 == partition_a {
                break;
            }
            partner += 1;
        }
        ctrl.set(rf(100), 55);
        ctrl.set(rf(partner), 77);
        assert_eq!(ctrl.get(rf(100)), Some(55));
        assert_eq!(ctrl.get(rf(partner)), Some(77));
    }
}
