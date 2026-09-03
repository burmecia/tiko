use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use pgsys::{lsn::Lsn, timeline_id::TimelineId};

use super::segment::Checkpoint;
use crate::chunk::ChunkTag;
use crate::relfork::{RelFork, RelForkMeta};
use crate::timeline::draft::DraftBuffer;
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

// ── RelForkIndex ────────────────────────────────────────────────────────────

/// Maximum number of relforks indexed per active-checkpoint inline index.
/// Sized so the index footprint (`REL_FORK_INDEX_CAP × 24 B` = 3 KiB) plus
/// `ChunkBloom` (16 KiB) stays well under 20 KiB per slot.
pub const REL_FORK_INDEX_CAP: usize = 128;

/// One entry of [`RelForkIndex`].
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct RelForkEntry {
    pub rf: RelFork,
    pub meta: RelForkMeta,
    _pad: [u8; 3],
}

/// Result of probing a single [`RelForkIndex`].
#[derive(Debug)]
pub enum RelForkLookup {
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
/// fewer than [`REL_FORK_INDEX_CAP`] relforks were touched in the checkpoint.
///
/// Lookup is binary-search over the sorted prefix `entries[..len]`. If the
/// originating checkpoint touched more than [`REL_FORK_INDEX_CAP`] relforks,
/// `overflowed` is set and a miss on the inline index is *inconclusive* —
/// the read path must fall through to the segment file.
#[repr(C)]
pub struct RelForkIndex {
    len: u32,
    overflowed: bool,
    _pad: [u8; 3],
    entries: [RelForkEntry; REL_FORK_INDEX_CAP],
}

impl RelForkIndex {
    pub fn clear(&mut self) {
        self.len = 0;
        self.overflowed = false;
    }

    /// Populate from an iterator of `(rf, meta)`. Duplicates must already be
    /// resolved by the caller (the commit-protocol drain uses a
    /// `HashMap<RelFork, RelForkMeta>`, so each `RelFork` appears at most
    /// once). Entries are sorted by `RelFork`'s natural order; if the input
    /// exceeds [`REL_FORK_INDEX_CAP`], the index is marked `overflowed` and
    /// keeps the first `REL_FORK_INDEX_CAP` sorted entries.
    pub fn populate(&mut self, relforks: impl IntoIterator<Item = (RelFork, RelForkMeta)>) {
        let mut buf: Vec<(RelFork, RelForkMeta)> = relforks.into_iter().collect();
        buf.sort_unstable_by_key(|a| a.0);

        self.overflowed = buf.len() > REL_FORK_INDEX_CAP;
        let n = buf.len().min(REL_FORK_INDEX_CAP);
        for (i, (rf, meta)) in buf.into_iter().take(n).enumerate() {
            self.entries[i] = RelForkEntry {
                rf,
                meta,
                _pad: [0; 3],
            };
        }
        self.len = n as u32;
    }

    pub fn get(&self, rf: &RelFork) -> RelForkLookup {
        let slice = &self.entries[..self.len as usize];
        match slice.binary_search_by(|e| e.rf.cmp(rf)) {
            Ok(i) => RelForkLookup::Hit(slice[i].meta),
            Err(_) => {
                if self.overflowed {
                    RelForkLookup::Inconclusive
                } else {
                    RelForkLookup::DefinitiveMiss
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
    pub relfork_index: RelForkIndex,
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

// ── CompactionRequest ───────────────────────────────────────────────────────

/// Compaction completed successfully.
pub const COMPACTION_STATUS_OK: u32 = 0;
/// The worker-side compaction run returned an error.
pub const COMPACTION_STATUS_ERROR: u32 = 1;

/// Single-slot basebackup→worker compaction request in shared memory.
///
/// The tikoworker compactor is the sole routine compactor; a basebackup
/// checkpointer that needs a base manifest at its checkpoint LSN publishes a
/// request here and waits on its own latch instead of running compaction
/// itself. Publication uses generation counters: the requester writes the
/// payload with `Relaxed` stores and publishes with a `Release` bump of
/// `request_gen`; the worker pairs an `Acquire` load of `request_gen` with
/// `Relaxed` payload reads and completes with `status` (`Relaxed`) followed
/// by a `Release` store to `done_gen`.
///
/// Invariant: at most one request is outstanding. Guaranteed structurally —
/// `CreateCheckPoint` is serialised by PG's CheckpointLock and the requester
/// waits for completion before returning.
#[repr(C)]
pub struct CompactionRequest {
    /// Bumped by the requester to publish a request (payload written first).
    request_gen: AtomicU64,
    /// Set to the completed generation by the worker (payload `status` first).
    done_gen: AtomicU64,
    /// Compact through this checkpoint (inclusive).
    target_timeline: AtomicU32,
    target_lsn: AtomicU64,
    /// Requester's latch as a pointer value; the worker SetLatches it on
    /// completion. A spurious set on a recycled latch is harmless.
    requester_latch: AtomicU64,
    /// `COMPACTION_STATUS_*`; valid once `done_gen` reaches the request gen.
    status: AtomicU32,
}

impl CompactionRequest {
    fn init(&self) {
        self.request_gen.store(0, Ordering::Relaxed);
        self.done_gen.store(0, Ordering::Relaxed);
        self.target_timeline.store(0, Ordering::Relaxed);
        self.target_lsn.store(0, Ordering::Relaxed);
        self.requester_latch.store(0, Ordering::Relaxed);
        self.status.store(0, Ordering::Relaxed);
    }
}

/// A pending compaction request observed by the worker.
#[derive(Clone, Copy)]
pub struct PendingCompaction {
    /// Generation to pass back to [`TimelineState::complete_compaction`].
    pub generation: u64,
    /// Checkpoint to compact through (inclusive).
    pub target: Checkpoint,
    /// Requester's latch (pointer value) to SetLatch on completion.
    pub requester_latch: u64,
}

// ── TimelineState ───────────────────────────────────────────────────────────

/// Consolidated shmem state for the timeline subsystem.
///
/// Layout discipline: the plain checkpoint fields and `active_window` are
/// protected by `lock`. `generation`, `hydrated`, `draft` and
/// `compaction_request` are internally synchronised and safe to access
/// lock-free. `generation` is bumped (Release) on every commit; backends
/// read it lock-free (Acquire) to decide whether to refresh their local
/// snapshot.
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
    /// Basebackup compaction request slot: the checkpointer publishes a
    /// compact-through request here and the tikoworker compactor executes it
    /// (the worker is the sole routine compactor). See [`CompactionRequest`].
    pub compaction_request: CompactionRequest,
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
        self.compaction_request.init();
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

    // ── Basebackup compaction requests ─────────────────────────────────────
    //
    // The tikoworker compactor is the sole routine compaction executor. A
    // basebackup checkpointer needing a base manifest at its checkpoint LSN
    // publishes a request and waits on its latch; the worker's compactor task
    // runs `run_compaction_through` and completes the request. No mutual
    // exclusion is needed between the two: only the worker ever compacts.
    // Escape hatches (worker dead / request timed out) fall back to running
    // compaction in the checkpointer, which is safe because
    // `run_compaction*` re-checks `base_ckpt` under the write lock and
    // discards raced runs.

    /// Publish a compaction request for `target`. Returns the generation to
    /// poll with [`Self::compaction_result`]. Caller must then SetLatch the
    /// worker latch to wake the compactor promptly.
    ///
    /// At most one request may be outstanding (see [`CompactionRequest`]).
    pub fn request_compaction(&self, target: Checkpoint, requester_latch: u64) -> u64 {
        let req = &self.compaction_request;
        req.target_timeline
            .store(target.timeline_id.as_u32(), Ordering::Relaxed);
        req.target_lsn.store(target.lsn.as_u64(), Ordering::Relaxed);
        req.requester_latch
            .store(requester_latch, Ordering::Relaxed);
        req.status.store(0, Ordering::Relaxed);
        // Release publishes the payload above.
        req.request_gen.fetch_add(1, Ordering::Release) + 1
    }

    /// Worker side: observe the pending request, if any. Double-reads
    /// `request_gen` so a torn read can only delay the request to the next
    /// poll, never mix fields from two generations.
    pub fn pending_compaction_request(&self) -> Option<PendingCompaction> {
        let req = &self.compaction_request;
        let generation = req.request_gen.load(Ordering::Acquire);
        if generation == req.done_gen.load(Ordering::Acquire) {
            return None;
        }
        let pending = PendingCompaction {
            generation,
            target: Checkpoint::new(
                TimelineId::new(req.target_timeline.load(Ordering::Relaxed)),
                Lsn::new(req.target_lsn.load(Ordering::Relaxed)),
            ),
            requester_latch: req.requester_latch.load(Ordering::Relaxed),
        };
        if req.request_gen.load(Ordering::Acquire) != generation {
            return None;
        }
        Some(pending)
    }

    /// Worker side: mark `generation` complete. The caller SetLatches
    /// `requester_latch` afterwards to wake the waiting requester.
    pub fn complete_compaction(&self, generation: u64, status: u32) {
        let req = &self.compaction_request;
        req.status.store(status, Ordering::Relaxed);
        // Release publishes `status`.
        req.done_gen.store(generation, Ordering::Release);
    }

    /// Requester side: the completion status once `generation` has been
    /// serviced.
    pub fn compaction_result(&self, generation: u64) -> Option<u32> {
        let req = &self.compaction_request;
        if req.done_gen.load(Ordering::Acquire) == generation {
            Some(req.status.load(Ordering::Relaxed))
        } else {
            None
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
    use pgsys::{common::ForkNumber, lsn::Lsn, timeline_id::TimelineId};

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

    // ── RelForkIndex ──

    fn new_relfork_index() -> Box<RelForkIndex> {
        let layout = std::alloc::Layout::new::<RelForkIndex>();
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut RelForkIndex;
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
            RelForkLookup::Hit(m) => {
                assert_eq!(m.nblocks, 64);
                assert!(!m.deleted);
            }
            other => panic!("expected hit, got {other:?}"),
        }
        match idx.get(&relfork(3)) {
            RelForkLookup::Hit(m) => assert!(m.deleted),
            other => panic!("expected hit, got {other:?}"),
        }
        match idx.get(&relfork(99)) {
            RelForkLookup::DefinitiveMiss => {}
            other => panic!("expected definitive miss, got {other:?}"),
        }
    }

    #[test]
    fn relfork_index_overflow_returns_inconclusive_on_miss() {
        let mut idx = new_relfork_index();
        let entries: Vec<_> = (0..(REL_FORK_INDEX_CAP as u32 + 5))
            .map(|i| (relfork(i), RelForkMeta::new(i, false)))
            .collect();
        idx.populate(entries);

        // Sorted-keep retains the first REL_FORK_INDEX_CAP rels by RelFork
        // order. With our `relfork(i)` helper, rel_number == i, so rels
        // 0..CAP are kept and CAP..CAP+5 are dropped.
        match idx.get(&relfork(0)) {
            RelForkLookup::Hit(m) => assert_eq!(m.nblocks, 0),
            other => panic!("expected hit for kept entry, got {other:?}"),
        }
        // The dropped rel was in the input but not in the kept window —
        // lookup must be Inconclusive, not DefinitiveMiss.
        match idx.get(&relfork(REL_FORK_INDEX_CAP as u32 + 1)) {
            RelForkLookup::Inconclusive => {}
            other => panic!("expected inconclusive, got {other:?}"),
        }
    }

    #[test]
    fn relfork_index_clear_resets_state() {
        let mut idx = new_relfork_index();
        idx.populate([(relfork(1), RelForkMeta::new(10, false))]);
        assert!(matches!(idx.get(&relfork(1)), RelForkLookup::Hit(_)));
        idx.clear();
        assert!(matches!(
            idx.get(&relfork(1)),
            RelForkLookup::DefinitiveMiss
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

    // ── CompactionRequest ──

    #[test]
    fn compaction_request_round_trip() {
        let s = new_timeline_state();
        assert!(s.pending_compaction_request().is_none());
        assert!(s.compaction_result(1).is_none());

        let generation = s.request_compaction(ckpt(500), 0xDEAD);
        assert_eq!(generation, 1);

        let pending = s.pending_compaction_request().expect("request pending");
        assert_eq!(pending.generation, 1);
        assert_eq!(pending.target, ckpt(500));
        assert_eq!(pending.requester_latch, 0xDEAD);
        // Still pending until completed.
        assert!(s.compaction_result(1).is_none());

        s.complete_compaction(1, COMPACTION_STATUS_OK);
        assert!(s.pending_compaction_request().is_none());
        assert_eq!(s.compaction_result(1), Some(COMPACTION_STATUS_OK));
    }

    #[test]
    fn compaction_request_generations_are_monotonic() {
        let s = new_timeline_state();
        let g1 = s.request_compaction(ckpt(100), 1);
        s.complete_compaction(g1, COMPACTION_STATUS_OK);
        let g2 = s.request_compaction(ckpt(200), 2);
        assert_eq!(g2, g1 + 1);
        // The previous generation stays complete; the new one is pending.
        assert_eq!(s.compaction_result(g1), Some(COMPACTION_STATUS_OK));
        assert!(s.compaction_result(g2).is_none());

        s.complete_compaction(g2, COMPACTION_STATUS_ERROR);
        assert_eq!(s.compaction_result(g2), Some(COMPACTION_STATUS_ERROR));
    }
}
