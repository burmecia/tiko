use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

#[cfg(test)]
use pgsys::{lsn::Lsn, timeline_id::TimelineId};

use super::segment::Checkpoint;
use crate::chunk::ChunkTag;
use crate::io::draft::DraftBuffer;
use crate::relfork::{RelFork, RelForkMeta};
use crate::utils::rw_lock::AtomicRWLock;

/// Number of recent checkpoints kept fully indexed in the shmem active window.
pub const ACTIVE_WINDOW_SIZE: usize = 64;

/// Per-active-checkpoint Bloom filter size in bytes. 16 KiB = 128 Ki bits.
/// At ~12 K dirty chunks per checkpoint and 7 hash functions, false-positive
/// rate is ~1 %. With K = 64 active slots, total Bloom footprint is ~1 MiB.
pub const CHUNK_BLOOM_BYTES: usize = 16 * 1024;
const CHUNK_BLOOM_BITS: u32 = (CHUNK_BLOOM_BYTES * 8) as u32;
const CHUNK_BLOOM_HASHES: u32 = 7;

// ── ChunkBloom ──────────────────────────────────────────────────────────────

/// Fixed-size Bloom filter living in shared memory. Stores the set of
/// [`ChunkTag`]s present in one active-window checkpoint.
///
/// False positives fall through to an on-disk segment lookup, so they only
/// affect read-path cost on a rare miss path, not correctness.
#[repr(C)]
pub struct ChunkBloom {
    bits: [u8; CHUNK_BLOOM_BYTES],
}

impl ChunkBloom {
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }

    pub fn insert(&mut self, tag: &ChunkTag) {
        let (h1, h2) = double_hash(tag);
        for i in 0..CHUNK_BLOOM_HASHES {
            let bit = combined_hash(h1, h2, i) % CHUNK_BLOOM_BITS;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    pub fn maybe_contains(&self, tag: &ChunkTag) -> bool {
        let (h1, h2) = double_hash(tag);
        for i in 0..CHUNK_BLOOM_HASHES {
            let bit = combined_hash(h1, h2, i) % CHUNK_BLOOM_BITS;
            if self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

#[inline]
fn double_hash(tag: &ChunkTag) -> (u32, u32) {
    // FNV-1a from ChunkTag, mixed two ways to get two independent hashes
    // for the double-hashing Bloom scheme (Kirsch & Mitzenmacher).
    let h1 = tag.hash();
    let h2 = (h1 ^ 0x9E37_79B9_u32).wrapping_mul(0x85EB_CA6B_u32);
    (h1, h2)
}

#[inline]
fn combined_hash(h1: u32, h2: u32, i: u32) -> u32 {
    h1.wrapping_add(i.wrapping_mul(h2))
        .wrapping_add(i.wrapping_mul(i))
}

// ── RelforkIndex ────────────────────────────────────────────────────────────

/// Maximum number of relforks indexed per active-checkpoint inline index.
/// Sized so the index footprint (`RELFORK_INDEX_CAP × 24 B` = 3 KiB) plus
/// `ChunkBloom` (16 KiB) stays well under 20 KiB per slot.
pub const RELFORK_INDEX_CAP: usize = 128;

/// One entry of [`RelforkIndex`]. `RelForkMeta` is inlined as primitive
/// fields to keep the entry `Copy` and avoid a non-`Copy` field in a
/// shmem-resident fixed-size array.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct RelforkEntry {
    pub rf: RelFork,
    pub nblocks: u32,
    pub deleted: bool,
    _pad: [u8; 3],
}

/// Result of probing a single [`RelforkIndex`].
#[derive(Debug)]
pub enum RelforkLookup {
    /// The relfork was modified in this checkpoint; here is its meta.
    Hit(RelForkMeta),
    /// The relfork was definitely not modified in this checkpoint.
    /// Safe to skip this checkpoint's on-disk segment entry for `rf`.
    DefinitiveMiss,
    /// The index overflowed: the relfork *might* be in this checkpoint
    /// but isn't in the kept portion. Caller must consult the segment file.
    Inconclusive,
}

/// Sorted inline index of [`RelFork`] → [`RelForkMeta`] for one active
/// checkpoint. Replaces an on-disk segment GET for the common case where
/// fewer than [`RELFORK_INDEX_CAP`] relforks were touched in the checkpoint.
///
/// Lookup is binary-search over the sorted prefix `entries[..len]`. If the
/// originating checkpoint touched more than [`RELFORK_INDEX_CAP`] relforks,
/// `overflowed` is set and a miss on the inline index is *inconclusive* —
/// the read path must fall through to the segment file.
#[repr(C)]
pub struct RelforkIndex {
    len: u32,
    overflowed: bool,
    _pad: [u8; 3],
    entries: [RelforkEntry; RELFORK_INDEX_CAP],
}

impl RelforkIndex {
    pub fn clear(&mut self) {
        self.len = 0;
        self.overflowed = false;
    }

    /// Populate from an iterator of `(rf, meta)`. Duplicates must already be
    /// resolved by the caller (the commit-protocol drain uses a
    /// `HashMap<RelFork, RelForkMeta>`, so each `RelFork` appears at most
    /// once). Entries are sorted by `RelFork`'s natural order; if the input
    /// exceeds [`RELFORK_INDEX_CAP`], the index is marked `overflowed` and
    /// keeps the first `RELFORK_INDEX_CAP` sorted entries.
    pub fn populate(&mut self, relforks: impl IntoIterator<Item = (RelFork, RelForkMeta)>) {
        let mut buf: Vec<(RelFork, RelForkMeta)> = relforks.into_iter().collect();
        buf.sort_unstable_by_key(|a| a.0);

        self.overflowed = buf.len() > RELFORK_INDEX_CAP;
        let n = buf.len().min(RELFORK_INDEX_CAP);
        for (i, (rf, meta)) in buf.into_iter().take(n).enumerate() {
            self.entries[i] = RelforkEntry {
                rf,
                nblocks: meta.nblocks,
                deleted: meta.deleted,
                _pad: [0; 3],
            };
        }
        self.len = n as u32;
    }

    pub fn get(&self, rf: &RelFork) -> RelforkLookup {
        let slice = &self.entries[..self.len as usize];
        match slice.binary_search_by(|e| e.rf.cmp(rf)) {
            Ok(i) => RelforkLookup::Hit(RelForkMeta {
                nblocks: slice[i].nblocks,
                deleted: slice[i].deleted,
            }),
            Err(_) => {
                if self.overflowed {
                    RelforkLookup::Inconclusive
                } else {
                    RelforkLookup::DefinitiveMiss
                }
            }
        }
    }
}

// ── ActiveCheckpoint ────────────────────────────────────────────────────────

/// One entry of the shmem active window. Carries the checkpoint identity,
/// the path prefix to use for S3 reads at this checkpoint, a chunk presence
/// Bloom filter, and an inline relfork-meta index.
#[repr(C)]
pub struct ActiveCheckpoint {
    pub ckpt: Checkpoint,
    pub prev_ckpt: Checkpoint,
    pub chunk_bloom: ChunkBloom,
    pub relfork_index: RelforkIndex,
}

impl ActiveCheckpoint {
    pub fn reset(&mut self) {
        self.ckpt = Checkpoint::default();
        self.prev_ckpt = Checkpoint::default();
        self.chunk_bloom.clear();
        self.relfork_index.clear();
    }

    pub fn populate(
        &mut self,
        ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        chunks: impl IntoIterator<Item = ChunkTag>,
        relforks: impl IntoIterator<Item = (RelFork, RelForkMeta)>,
    ) {
        self.ckpt = ckpt;
        self.prev_ckpt = prev_ckpt;
        self.chunk_bloom.clear();
        for tag in chunks {
            self.chunk_bloom.insert(&tag);
        }
        self.relfork_index.populate(relforks);
    }
}

// ── TimelineState ───────────────────────────────────────────────────────────

/// Consolidated shmem state for the timeline subsystem.
///
/// Layout discipline: all mutable fields except `generation` are protected by
/// `lock`. `generation` is bumped (Release) on every commit; backends read it
/// lock-free (Acquire) to decide whether to refresh their local snapshot.
///
/// Invariant: `base_ckpt < redo_ckpt <= head_ckpt`.
///
/// `lock` fences all checkpoint-interval mutations: it serialises advances
/// to `head_ckpt` / `active_window` against `draft` drains. Read-lock
/// holders may mutate `draft` (its own per-shard spinlocks handle producer
/// concurrency); only the write-lock holder may drain it.
#[repr(C)]
pub struct TimelineState {
    pub(crate) lock: AtomicRWLock,
    pub generation: AtomicU64,
    /// Set once by the first process to run [`Store::hydrate_timeline_state`]
    /// after `IoControl` is initialised. Subsequent backends observe this
    /// and skip the hydration scan.
    pub hydrated: AtomicBool,
    pub base_ckpt: Checkpoint,
    pub head_ckpt: Checkpoint,
    pub redo_ckpt: Checkpoint,
    /// Number of valid entries in `active_window` (0..=ACTIVE_WINDOW_SIZE).
    active_count: u32,
    /// Index of the next write slot (mod ACTIVE_WINDOW_SIZE). The newest
    /// active checkpoint sits at `(active_head - 1) mod ACTIVE_WINDOW_SIZE`.
    active_head: u32,
    active_window: [ActiveCheckpoint; ACTIVE_WINDOW_SIZE],
    /// Live-interval draft buffer. Backends record into it under
    /// `lock.read()`; the checkpointer drains it under `lock.write()` as
    /// part of the commit fence.
    pub draft: DraftBuffer,
    /// Set by the checkpointer during a `CHECKPOINT_CAUSE_BASEBACKUP`
    /// checkpoint so the background compactor skips its tick. The
    /// checkpointer then runs compaction itself (after draining any
    /// in-flight compactor run, see `compaction_in_progress`) to form a
    /// base manifest at the basebackup LSN without racing the compactor.
    pub compaction_paused: AtomicBool,
    /// Bumped by any caller around an in-flight `run_compaction` so a
    /// pausing checkpointer can `drain_compaction()` (spin until zero)
    /// before running its own compaction.
    pub compaction_in_progress: AtomicU32,
}

impl TimelineState {
    /// Initialise the structure in-place. Call once when allocating shmem.
    pub fn init(&mut self) {
        self.lock.init();
        self.generation.store(0, Ordering::Relaxed);
        self.hydrated.store(false, Ordering::Relaxed);
        self.base_ckpt = Checkpoint::default();
        self.head_ckpt = Checkpoint::default();
        self.redo_ckpt = Checkpoint::default();
        self.active_count = 0;
        self.active_head = 0;
        for slot in self.active_window.iter_mut() {
            slot.reset();
        }
        self.draft.init();
        self.compaction_paused.store(false, Ordering::Relaxed);
        self.compaction_in_progress.store(0, Ordering::Relaxed);
    }

    /// Push a new active-window entry. Caller must hold `lock.write()` —
    /// this method takes `&self` and casts internally, the standard
    /// convention for shmem-resident types.
    /// Bumps `generation` (Release) on success; updates `head_ckpt`.
    pub fn push_active(
        &self,
        ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        chunks: impl IntoIterator<Item = ChunkTag>,
        relforks: impl IntoIterator<Item = (RelFork, RelForkMeta)>,
    ) {
        // SAFETY: caller holds the exclusive write lock on `self.lock`, so
        // there are no concurrent readers or writers of any field below.
        unsafe {
            let me = self as *const Self as *mut Self;
            let head = (*me).active_head as usize;
            debug_assert!(head < ACTIVE_WINDOW_SIZE);
            (*me).active_window[head].populate(ckpt, prev_ckpt, chunks, relforks);
            (*me).active_head = ((head + 1) % ACTIVE_WINDOW_SIZE) as u32;
            if ((*me).active_count as usize) < ACTIVE_WINDOW_SIZE {
                (*me).active_count += 1;
            }
            (*me).head_ckpt = ckpt;
        }
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Set `redo_ckpt`. Caller must hold `lock.write()`. Same `&self`
    /// convention as [`push_active`].
    pub fn set_redo_ckpt(&self, redo_ckpt: Checkpoint) {
        // SAFETY: caller holds the exclusive write lock.
        unsafe {
            let me = self as *const Self as *mut Self;
            (*me).redo_ckpt = redo_ckpt;
        }
    }

    /// Set `base_ckpt`. Caller must hold `lock.write()`. Used by the
    /// compactor to advance the base point and by startup hydration to
    /// recover the value from the base manifest.
    pub fn set_base_ckpt(&self, base_ckpt: Checkpoint) {
        // SAFETY: caller holds the exclusive write lock.
        unsafe {
            let me = self as *const Self as *mut Self;
            (*me).base_ckpt = base_ckpt;
        }
        self.generation.fetch_add(1, Ordering::Release);
    }

    // ── Basebackup compaction coordination ───────────────────────────────
    //
    // The checkpointer's `CHECKPOINT_CAUSE_BASEBACKUP` path wants to run
    // compaction itself to form a base manifest at the basebackup LSN.
    // Mutual exclusion with the background compactor:
    //   1. Checkpointer: `pause_compaction()` (store), then
    //      `drain_compaction()` — spin until `compaction_in_progress` is 0.
    //   2. Compactor: `begin_compaction()` (bump) FIRST, then checks
    //      `is_compaction_paused()` and backs off if set.
    // Ordering argument: either the compactor's bump precedes the
    // checkpointer's drain read (drain waits for the compactor to finish),
    // or the pause store precedes the compactor's flag load (it backs off).
    // Checking the flag before bumping would leave a window where both run.
    //   3. The checkpointer wraps its own compaction in begin/end too, so
    //      any concurrent drainer observes it.

    /// Request the background compactor to skip its ticks. Process-local
    /// shmem flag; safe to call from any process sharing `IoControl`.
    pub fn pause_compaction(&self) {
        self.compaction_paused.store(true, Ordering::Release);
    }

    /// Clear the pause flag. Cheap to call unconditionally.
    pub fn resume_compaction(&self) {
        self.compaction_paused.store(false, Ordering::Release);
    }

    /// Whether the background compactor should skip its current tick.
    pub fn is_compaction_paused(&self) -> bool {
        self.compaction_paused.load(Ordering::Acquire)
    }

    /// Record the start of a compaction run. Callers MUST pair this with
    /// [`end_compaction`], and MUST bump before checking
    /// [`Self::is_compaction_paused`] — see the section comment above.
    pub fn begin_compaction(&self) {
        self.compaction_in_progress.fetch_add(1, Ordering::Release);
    }

    /// Record the end of a `run_compaction` call.
    pub fn end_compaction(&self) {
        self.compaction_in_progress.fetch_sub(1, Ordering::Release);
    }

    /// Spin until no `run_compaction` is in flight. Bounded by the compactor
    /// interval; a crashed compactor leaves the counter at 0 (the `begin`/
    /// `end` pair is tight around the synchronous `run_compaction`).
    pub fn drain_compaction(&self) {
        while self.compaction_in_progress.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }
    }

    /// Iterate active-window entries newest-first. Caller must hold a read
    /// (or write) lock.
    pub fn iter_active(&self) -> impl Iterator<Item = &ActiveCheckpoint> {
        let count = self.active_count as usize;
        let head = self.active_head as usize;
        (0..count).map(move |i| {
            let slot = (head + ACTIVE_WINDOW_SIZE - 1 - i) % ACTIVE_WINDOW_SIZE;
            &self.active_window[slot]
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::common::ForkNumber;

    fn tag(rel: u32, chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
            chunk_id,
        }
    }

    fn relfork(rel: u32) -> RelFork {
        RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
        }
    }

    // ── ChunkBloom ──

    fn new_bloom() -> Box<ChunkBloom> {
        // Heap-allocate zeroed bytes and reinterpret to avoid stack-allocating
        // a 16 KB array.
        let v = vec![0u8; CHUNK_BLOOM_BYTES].into_boxed_slice();
        let raw = Box::into_raw(v) as *mut ChunkBloom;
        unsafe { Box::from_raw(raw) }
    }

    #[test]
    fn bloom_empty_contains_nothing() {
        let b = new_bloom();
        assert!(!b.maybe_contains(&tag(1, 0)));
        assert!(!b.maybe_contains(&tag(99, 99)));
    }

    #[test]
    fn bloom_no_false_negatives() {
        let mut b = new_bloom();
        let mut inserted = Vec::new();
        for r in 0..50u32 {
            for c in 0..10u32 {
                let t = tag(r, c);
                b.insert(&t);
                inserted.push(t);
            }
        }
        for t in &inserted {
            assert!(
                b.maybe_contains(t),
                "false negative for {:?}: Bloom must report membership for everything inserted",
                t
            );
        }
    }

    #[test]
    fn bloom_false_positive_rate_is_reasonable() {
        // Insert N items, then probe M items that were NOT inserted.
        // With CHUNK_BLOOM_BITS=128Ki and 7 hashes, optimal load is around
        // 12700 items @ 1% FP. We test well below that capacity.
        let mut b = new_bloom();
        const INSERTED: u32 = 1_000;
        const PROBED: u32 = 10_000;
        for i in 0..INSERTED {
            b.insert(&tag(0, i));
        }
        let mut fp = 0u32;
        for i in INSERTED..(INSERTED + PROBED) {
            if b.maybe_contains(&tag(0, i)) {
                fp += 1;
            }
        }
        // At ~1k items / 128k bits / 7 hashes, FP rate is well under 0.1%.
        // Allow generous headroom for hash-quality variation.
        assert!(
            fp < PROBED / 100,
            "false-positive rate too high: {}/{} ({:.2}%)",
            fp,
            PROBED,
            fp as f64 * 100.0 / PROBED as f64
        );
    }

    #[test]
    fn bloom_clear_resets_state() {
        let mut b = new_bloom();
        let t = tag(7, 7);
        b.insert(&t);
        assert!(b.maybe_contains(&t));
        b.clear();
        assert!(!b.maybe_contains(&t));
    }

    // ── ActiveCheckpoint ──

    fn new_active_checkpoint() -> Box<ActiveCheckpoint> {
        let layout = std::alloc::Layout::new::<ActiveCheckpoint>();
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut ActiveCheckpoint;
            (*raw).reset();
            Box::from_raw(raw)
        }
    }

    #[test]
    fn active_checkpoint_populate_then_probe() {
        let mut ac = new_active_checkpoint();
        let ckpt = Checkpoint::new(TimelineId::new(1), Lsn::new(100));
        let prev = Checkpoint::new(TimelineId::new(1), Lsn::new(50));
        let tags = vec![tag(1, 0), tag(1, 1), tag(2, 5)];
        ac.populate(ckpt, prev, tags.clone(), std::iter::empty());

        assert_eq!(ac.ckpt, ckpt);
        assert_eq!(ac.prev_ckpt, prev);
        for t in &tags {
            assert!(ac.chunk_bloom.maybe_contains(t));
        }
        // A tag that was not inserted is *probably* absent.
        assert!(!ac.chunk_bloom.maybe_contains(&tag(999, 999)));
    }

    // ── TimelineState ──

    fn new_timeline_state() -> Box<TimelineState> {
        let layout = std::alloc::Layout::new::<TimelineState>();
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut TimelineState;
            (*raw).init();
            Box::from_raw(raw)
        }
    }

    fn ckpt(lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(1), Lsn::new(lsn))
    }

    #[test]
    fn timeline_state_initial_state_is_empty() {
        let s = new_timeline_state();
        assert_eq!(s.active_count, 0);
        assert_eq!(s.active_head, 0);
        assert_eq!(s.head_ckpt, Checkpoint::default());
        assert_eq!(s.base_ckpt, Checkpoint::default());
        assert_eq!(s.redo_ckpt, Checkpoint::default());
        assert_eq!(s.generation.load(Ordering::Relaxed), 0);
        assert!(s.iter_active().next().is_none());
    }

    #[test]
    fn timeline_state_push_active_bumps_generation_and_head() {
        let s = new_timeline_state();
        s.push_active(ckpt(100), ckpt(0), [tag(1, 0)], std::iter::empty());
        assert_eq!(s.head_ckpt, ckpt(100));
        assert_eq!(s.active_count, 1);
        assert_eq!(s.generation.load(Ordering::Relaxed), 1);

        s.push_active(ckpt(200), ckpt(100), [tag(2, 0)], std::iter::empty());
        assert_eq!(s.head_ckpt, ckpt(200));
        assert_eq!(s.active_count, 2);
        assert_eq!(s.generation.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn timeline_state_iter_active_is_newest_first() {
        let s = new_timeline_state();
        for i in 1..=5u64 {
            s.push_active(
                ckpt(i * 100),
                ckpt((i - 1) * 100),
                [tag(1, i as u32)],
                std::iter::empty(),
            );
        }
        let lsns: Vec<u64> = s.iter_active().map(|ac| ac.ckpt.lsn.as_u64()).collect();
        assert_eq!(lsns, vec![500, 400, 300, 200, 100]);
    }

    #[test]
    fn timeline_state_active_window_wraps_when_full() {
        let s = new_timeline_state();
        // Push ACTIVE_WINDOW_SIZE + 5 entries to force eviction of oldest.
        let total = ACTIVE_WINDOW_SIZE as u64 + 5;
        for i in 1..=total {
            s.push_active(
                ckpt(i * 100),
                ckpt((i - 1) * 100),
                [tag(1, i as u32)],
                std::iter::empty(),
            );
        }
        assert_eq!(s.active_count as usize, ACTIVE_WINDOW_SIZE);
        // Newest entry sits at the front of iter_active().
        let first = s.iter_active().next().unwrap();
        assert_eq!(first.ckpt.lsn.as_u64(), total * 100);
        // Oldest retained entry is at the end; the very first 5 pushes were
        // evicted out of the ring buffer.
        let last = s.iter_active().last().unwrap();
        assert_eq!(
            last.ckpt.lsn.as_u64(),
            (total - ACTIVE_WINDOW_SIZE as u64 + 1) * 100
        );
    }

    #[test]
    fn timeline_state_size_is_within_expected_bounds() {
        // Sanity check the shmem footprint. ActiveCheckpoint × ACTIVE_WINDOW_SIZE
        // dominates; the rest of TimelineState is small.
        let size = std::mem::size_of::<TimelineState>();
        assert!(
            size >= ACTIVE_WINDOW_SIZE * (32 + CHUNK_BLOOM_BYTES),
            "TimelineState ({} bytes) smaller than the active_window minimum",
            size,
        );
        assert!(
            size < 2 * 1024 * 1024,
            "TimelineState ({} bytes) exceeded 2 MiB; check the layout",
            size,
        );
    }

    // ── RelforkIndex ──

    fn new_relfork_index() -> Box<RelforkIndex> {
        let layout = std::alloc::Layout::new::<RelforkIndex>();
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut RelforkIndex;
            (*raw).clear();
            Box::from_raw(raw)
        }
    }

    #[test]
    fn relfork_index_hit_and_definitive_miss() {
        let mut idx = new_relfork_index();
        idx.populate([
            (relfork(1), RelForkMeta::new(32, false)),
            (relfork(3), RelForkMeta::new(0, true)),
            (relfork(2), RelForkMeta::new(64, false)),
        ]);

        match idx.get(&relfork(2)) {
            RelforkLookup::Hit(m) => {
                assert_eq!(m.nblocks, 64);
                assert!(!m.deleted);
            }
            other => panic!("expected hit, got {other:?}"),
        }
        match idx.get(&relfork(3)) {
            RelforkLookup::Hit(m) => assert!(m.deleted),
            other => panic!("expected hit, got {other:?}"),
        }
        match idx.get(&relfork(99)) {
            RelforkLookup::DefinitiveMiss => {}
            other => panic!("expected definitive miss, got {other:?}"),
        }
    }

    #[test]
    fn relfork_index_overflow_returns_inconclusive_on_miss() {
        let mut idx = new_relfork_index();
        let entries: Vec<_> = (0..(RELFORK_INDEX_CAP as u32 + 5))
            .map(|i| (relfork(i), RelForkMeta::new(i, false)))
            .collect();
        idx.populate(entries);

        // Sorted-keep retains the first RELFORK_INDEX_CAP rels by RelFork
        // order. With our `relfork(i)` helper, rel_number == i, so rels
        // 0..CAP are kept and CAP..CAP+5 are dropped.
        match idx.get(&relfork(0)) {
            RelforkLookup::Hit(m) => assert_eq!(m.nblocks, 0),
            other => panic!("expected hit for kept entry, got {other:?}"),
        }
        // The dropped rel was in the input but not in the kept window —
        // lookup must be Inconclusive, not DefinitiveMiss.
        match idx.get(&relfork(RELFORK_INDEX_CAP as u32 + 1)) {
            RelforkLookup::Inconclusive => {}
            other => panic!("expected inconclusive, got {other:?}"),
        }
    }

    #[test]
    fn relfork_index_clear_resets_state() {
        let mut idx = new_relfork_index();
        idx.populate([(relfork(1), RelForkMeta::new(10, false))]);
        assert!(matches!(idx.get(&relfork(1)), RelforkLookup::Hit(_)));
        idx.clear();
        assert!(matches!(
            idx.get(&relfork(1)),
            RelforkLookup::DefinitiveMiss
        ));
    }

    #[test]
    fn timeline_state_active_bloom_carries_tags() {
        let s = new_timeline_state();
        let tags = vec![tag(7, 0), tag(7, 1), tag(8, 0)];
        s.push_active(ckpt(100), ckpt(0), tags.clone(), std::iter::empty());
        let entry = s.iter_active().next().unwrap();
        for t in &tags {
            assert!(entry.chunk_bloom.maybe_contains(t));
        }
        assert!(!entry.chunk_bloom.maybe_contains(&tag(99, 99)));
    }
}
