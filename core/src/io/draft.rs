//! Centralized shmem draft buffer.
//!
//! Two-zone hash table in shared memory that records every chunk tag and
//! relfork-meta update written during the current PG checkpoint interval.
//! Backends record directly; readers ([`crate::io::store::Store::get_chunk`],
//! [`crate::io::store::Store::get_meta`]) probe via [`DraftBuffer::contains_chunk`]
//! / [`DraftBuffer::get_relfork`]. At commit time the checkpointer drains
//! both zones plus any on-disk spill overflow and folds them into the new
//! `SegmentCheckpoint`.
//!
//! Layout:
//! - [`ChunkZone`]: `CHUNK_NUM_SHARDS` sharded open-addressed hash sets of
//!   [`ChunkTag`] (presence-only; chunk data lives at the S3 head-prefix).
//! - [`RelforkZone`]: single open-addressed hash table of
//!   [`RelFork`] → [`RelForkMeta`] (last write wins on overwrite).
//! - Each chunk shard has its own spinlock; the relfork zone has one
//!   spinlock. Spill drains are serialised by a global [`AtomicRWLock`].
//! - When a shard / zone crosses the [`DRAFT_SPILL_WATERMARK_PCT`] load
//!   factor the producer triggers a non-blocking spill. When a shard /
//!   zone fills the producer blocks on a synchronous spill before retrying.
//!
//! Spill file format:
//! ```text
//! Repeated:
//!   u32 LE  frame_len
//!   u8 × frame_len  msgpack(DraftFrame)
//! ```

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::chunk::ChunkTag;
use crate::error::{Error, Result};
use crate::io::utils::rw_lock::AtomicRWLock;
use crate::io::utils::spin_lock::spin_lock;
use crate::relfork::{RelFork, RelForkMeta};

/// A set of recorded chunk tags + relfork-meta updates.
///
/// Used in two roles with the same on-the-wire and in-memory shape:
/// - one spill-file frame on disk (msgpack-encoded);
/// - the merged result returned by [`DraftBuffer::drain`].
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DraftFrame {
    pub chunks: HashSet<ChunkTag>,
    pub relforks: HashMap<RelFork, RelForkMeta>,
}

impl DraftFrame {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.relforks.is_empty()
    }

    fn merge_frame(&mut self, frame: DraftFrame) {
        for tag in frame.chunks {
            self.chunks.insert(tag);
        }
        for (rf, meta) in frame.relforks {
            // Last-write-wins; frames are merged in file order (oldest →
            // newest), then with the in-memory residue last.
            self.relforks.insert(rf, meta);
        }
    }
}

// ── Sizing ──────────────────────────────────────────────────────────────────

/// Number of shards in [`ChunkZone`]. Each shard is independently locked.
pub const CHUNK_NUM_SHARDS: usize = 16;
/// Slot capacity of each [`ChunkShard`].
pub const CHUNK_SHARD_CAP: usize = 256;
/// Total chunk-zone capacity across all shards.
pub const CHUNK_TOTAL_CAP: usize = CHUNK_NUM_SHARDS * CHUNK_SHARD_CAP;
const _: () = assert!(CHUNK_SHARD_CAP.is_power_of_two());
const _: () = assert!(CHUNK_NUM_SHARDS.is_power_of_two());

/// Slot capacity of [`RelforkZone`].
pub const RELFORK_ZONE_CAP: usize = 8192;
const _: () = assert!(RELFORK_ZONE_CAP.is_power_of_two());

/// Non-blocking spill is triggered when a shard / zone load reaches this
/// percentage of capacity.
pub const DRAFT_SPILL_WATERMARK_PCT: u32 = 75;

/// Per-shard watermark for [`ChunkShard`] (in slots).
pub const CHUNK_SHARD_WATERMARK: usize = CHUNK_SHARD_CAP * DRAFT_SPILL_WATERMARK_PCT as usize / 100;

/// Watermark for [`RelforkZone`] (in slots).
pub const RELFORK_ZONE_WATERMARK: usize =
    RELFORK_ZONE_CAP * DRAFT_SPILL_WATERMARK_PCT as usize / 100;

/// Filename of the on-disk overflow file used by [`DraftBuffer::spill_to_file`].
/// Lives under the tiko root path, one per cluster.
pub const DRAFT_SPILL_FILE_NAME: &str = "draft.spill";

// ── Slot entries ────────────────────────────────────────────────────────────

const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct ChunkSlotEntry {
    state: u8,
    _pad: [u8; 3],
    tag: ChunkTag,
}
const _: () = assert!(std::mem::size_of::<ChunkSlotEntry>() == 24);

#[repr(C)]
#[derive(Clone, Copy)]
struct RelforkSlotEntry {
    state: u8,
    _pad: [u8; 3],
    rf: RelFork,
    nblocks: u32,
    deleted: bool,
    _pad2: [u8; 7],
}
const _: () = assert!(std::mem::size_of::<RelforkSlotEntry>() == 32);

// ── ChunkShard / ChunkZone ──────────────────────────────────────────────────

/// One shard of [`ChunkZone`]. Linear-probing open-addressed hash set
/// guarded by `lock`.
#[repr(C, align(64))]
pub struct ChunkShard {
    lock: AtomicU32,
    len: AtomicU32,
    _pad: [u8; 56],
    slots: UnsafeCell<[ChunkSlotEntry; CHUNK_SHARD_CAP]>,
}

// SAFETY: all access to `slots` is gated by `lock`. The shmem residency of
// this struct is no different than the previous ring's `DraftSlot` array.
unsafe impl Sync for ChunkShard {}

impl ChunkShard {
    fn init(&self) {
        self.lock.store(0, Ordering::Relaxed);
        self.len.store(0, Ordering::Relaxed);
        // SAFETY: idempotent init prior to publication.
        unsafe {
            let slots = &mut *self.slots.get();
            for s in slots.iter_mut() {
                s.state = SLOT_EMPTY;
            }
        }
    }

    /// Returns `Ok(over_watermark)` on insert (or no-op if already present);
    /// returns `Err(())` if the shard is full.
    fn insert(&self, tag: ChunkTag) -> std::result::Result<bool, ()> {
        let _g = spin_lock(&self.lock);
        let start = (tag.hash() as usize) % CHUNK_SHARD_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        for i in 0..CHUNK_SHARD_CAP {
            let idx = (start + i) % CHUNK_SHARD_CAP;
            let slot = &mut slots[idx];
            match slot.state {
                SLOT_EMPTY => {
                    slot.tag = tag;
                    slot.state = SLOT_OCCUPIED;
                    let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                    return Ok(new_len as usize >= CHUNK_SHARD_WATERMARK);
                }
                SLOT_OCCUPIED if slot.tag == tag => return Ok(false),
                _ => continue,
            }
        }
        Err(())
    }

    fn contains(&self, tag: &ChunkTag) -> bool {
        let _g = spin_lock(&self.lock);
        let start = (tag.hash() as usize) % CHUNK_SHARD_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &*self.slots.get() };
        for i in 0..CHUNK_SHARD_CAP {
            let idx = (start + i) % CHUNK_SHARD_CAP;
            match slots[idx].state {
                SLOT_EMPTY => return false,
                SLOT_OCCUPIED if slots[idx].tag == *tag => return true,
                _ => continue,
            }
        }
        false
    }

    fn drain_into(&self, dst: &mut HashSet<ChunkTag>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        for s in slots.iter_mut() {
            if s.state == SLOT_OCCUPIED {
                dst.insert(s.tag);
                s.state = SLOT_EMPTY;
            }
        }
        self.len.store(0, Ordering::Relaxed);
    }
}

/// Sharded hash set of [`ChunkTag`]s. Presence-only — chunk data lives at the
/// S3 head-prefix and is read by [`crate::io::store::Store::get_chunk`].
#[repr(C, align(128))]
pub struct ChunkZone {
    shards: [ChunkShard; CHUNK_NUM_SHARDS],
}

impl ChunkZone {
    fn init(&self) {
        for shard in self.shards.iter() {
            shard.init();
        }
    }

    fn shard_for(&self, tag: &ChunkTag) -> &ChunkShard {
        let s = (tag.hash() as usize) % CHUNK_NUM_SHARDS;
        &self.shards[s]
    }

    fn insert(&self, tag: ChunkTag) -> std::result::Result<bool, ()> {
        self.shard_for(&tag).insert(tag)
    }

    fn contains(&self, tag: &ChunkTag) -> bool {
        self.shard_for(tag).contains(tag)
    }

    fn drain_into(&self, dst: &mut HashSet<ChunkTag>) {
        for shard in self.shards.iter() {
            shard.drain_into(dst);
        }
    }
}

// ── RelforkZone ─────────────────────────────────────────────────────────────

/// Open-addressed hash table of `RelFork → RelForkMeta`. Single global
/// spinlock. Overwriting an existing entry preserves last-write-wins
/// semantics — required by `Store::get_meta` correctness.
#[repr(C, align(128))]
pub struct RelforkZone {
    lock: AtomicU32,
    len: AtomicU32,
    _pad: [u8; 56],
    slots: UnsafeCell<[RelforkSlotEntry; RELFORK_ZONE_CAP]>,
}

// SAFETY: all access to `slots` is gated by `lock`.
unsafe impl Sync for RelforkZone {}

impl RelforkZone {
    fn init(&self) {
        self.lock.store(0, Ordering::Relaxed);
        self.len.store(0, Ordering::Relaxed);
        // SAFETY: idempotent init prior to publication.
        unsafe {
            let slots = &mut *self.slots.get();
            for s in slots.iter_mut() {
                s.state = SLOT_EMPTY;
            }
        }
    }

    /// `meta` overwrites any existing entry for the same `rf`. Returns
    /// `Ok(over_watermark)` on insert; returns `Err(())` if the zone is full.
    fn insert(&self, rf: RelFork, meta: RelForkMeta) -> std::result::Result<bool, ()> {
        let _g = spin_lock(&self.lock);
        let start = (rf.hash() as usize) % RELFORK_ZONE_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        for i in 0..RELFORK_ZONE_CAP {
            let idx = (start + i) % RELFORK_ZONE_CAP;
            let slot = &mut slots[idx];
            match slot.state {
                SLOT_EMPTY => {
                    slot.rf = rf;
                    slot.nblocks = meta.nblocks;
                    slot.deleted = meta.deleted;
                    slot.state = SLOT_OCCUPIED;
                    let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                    return Ok(new_len as usize >= RELFORK_ZONE_WATERMARK);
                }
                SLOT_OCCUPIED if slot.rf == rf => {
                    slot.nblocks = meta.nblocks;
                    slot.deleted = meta.deleted;
                    return Ok(false);
                }
                _ => continue,
            }
        }
        Err(())
    }

    fn get(&self, rf: &RelFork) -> Option<RelForkMeta> {
        let _g = spin_lock(&self.lock);
        let start = (rf.hash() as usize) % RELFORK_ZONE_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &*self.slots.get() };
        for i in 0..RELFORK_ZONE_CAP {
            let idx = (start + i) % RELFORK_ZONE_CAP;
            match slots[idx].state {
                SLOT_EMPTY => return None,
                SLOT_OCCUPIED if slots[idx].rf == *rf => {
                    return Some(RelForkMeta {
                        nblocks: slots[idx].nblocks,
                        deleted: slots[idx].deleted,
                    });
                }
                _ => continue,
            }
        }
        None
    }

    fn drain_into(&self, dst: &mut HashMap<RelFork, RelForkMeta>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        for s in slots.iter_mut() {
            if s.state == SLOT_OCCUPIED {
                dst.insert(
                    s.rf,
                    RelForkMeta {
                        nblocks: s.nblocks,
                        deleted: s.deleted,
                    },
                );
                s.state = SLOT_EMPTY;
            }
        }
        self.len.store(0, Ordering::Relaxed);
    }
}

// ── DraftBuffer ─────────────────────────────────────────────────────────────

/// Process-wide two-zone draft buffer in shared memory.
#[repr(C, align(128))]
pub struct DraftBuffer {
    /// Serialises in-shmem → spill-file drains. Exclusive only.
    spill_lock: AtomicRWLock,
    /// Bumped on each successful spill. Exposed for tests / debug.
    pub spill_seq: AtomicU64,
    /// Set on each spill drain (when a frame is appended); cleared by `drain`
    /// (at commit). Used by [`Self::contains_chunk`] to return
    /// conservative-yes and by [`Self::get_relfork`] to gate the spill-file
    /// scan — avoids touching the file on lookup hot paths when no spill has
    /// occurred since the last commit.
    has_spilled: AtomicBool,
    chunks: ChunkZone,
    relforks: RelforkZone,
}

// SAFETY: every field is internally synchronised.
unsafe impl Sync for DraftBuffer {}

impl DraftBuffer {
    /// In-place initialise. Safe to call once when allocating shmem and
    /// idempotent (zero-initialised memory is already a valid state).
    pub fn init(&self) {
        self.spill_lock.init();
        self.spill_seq.store(0, Ordering::Relaxed);
        self.has_spilled.store(false, Ordering::Relaxed);
        self.chunks.init();
        self.relforks.init();
    }

    /// Record an evicted chunk tag (presence-only; idempotent).
    pub fn record_chunk(&self, tag: ChunkTag, spill_path: &Path) -> Result<()> {
        loop {
            match self.chunks.insert(tag) {
                Ok(over_watermark) => {
                    if over_watermark {
                        let _ = self.try_spill_to_file(spill_path);
                    }
                    return Ok(());
                }
                Err(()) => {
                    // Shard full → blocking spill, then retry.
                    self.spill_to_file(spill_path)?;
                }
            }
        }
    }

    /// Record a relfork-meta update. Same `rf` overwrites the existing entry
    /// (last-write-wins).
    pub fn record_relfork(&self, rf: RelFork, meta: RelForkMeta, spill_path: &Path) -> Result<()> {
        loop {
            match self.relforks.insert(rf, meta.clone()) {
                Ok(over_watermark) => {
                    if over_watermark {
                        let _ = self.try_spill_to_file(spill_path);
                    }
                    return Ok(());
                }
                Err(()) => {
                    self.spill_to_file(spill_path)?;
                }
            }
        }
    }

    /// Conservative presence check.
    ///
    /// Returns `true` if `tag` is in the in-memory chunk zone. If a spill has
    /// occurred since the last `drain` and `tag` is not in the in-memory
    /// zone, returns `true` conservatively rather than scanning the spill
    /// file: the caller ([`crate::io::store::Store::get_chunk`]) treats a
    /// `true` as a hint to probe the head-prefix and falls through if the
    /// object is absent. False negatives, on the other hand, would silently
    /// skip the head-prefix probe and lose data.
    pub fn contains_chunk(&self, tag: &ChunkTag) -> bool {
        if self.chunks.contains(tag) {
            return true;
        }
        self.has_spilled.load(Ordering::Acquire)
    }

    /// Return the latest recorded [`RelForkMeta`] for `rf`, or `None`.
    ///
    /// If a spill has occurred since the last `drain` and `rf` isn't in the
    /// in-memory zone, the spill file is scanned for the most recent write.
    /// Frames in the spill file are merged in file order (oldest → newest),
    /// so last-write-wins is preserved across spill boundaries.
    pub fn get_relfork(&self, rf: &RelFork, spill_path: &Path) -> Result<Option<RelForkMeta>> {
        if let Some(meta) = self.relforks.get(rf) {
            return Ok(Some(meta));
        }
        if !self.has_spilled.load(Ordering::Acquire) {
            return Ok(None);
        }
        let merged = read_spill_file(spill_path)?;
        Ok(merged.relforks.get(rf).cloned())
    }

    /// Blocking spill: drains both zones into a single spill frame.
    pub fn spill_to_file(&self, spill_path: &Path) -> Result<()> {
        let _guard = self.spill_lock.write();
        self.spill_locked(spill_path)
    }

    /// Non-blocking spill. Returns `Ok(true)` if the spill ran, `Ok(false)`
    /// if another spill is already in progress.
    pub fn try_spill_to_file(&self, spill_path: &Path) -> Result<bool> {
        match self.spill_lock.try_write() {
            Some(_guard) => {
                self.spill_locked(spill_path)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn spill_locked(&self, spill_path: &Path) -> Result<()> {
        let mut frame = DraftFrame::default();
        self.chunks.drain_into(&mut frame.chunks);
        self.relforks.drain_into(&mut frame.relforks);
        if frame.is_empty() {
            return Ok(());
        }
        append_spill_frame(spill_path, &frame)?;
        self.has_spilled.store(true, Ordering::Release);
        self.spill_seq.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Drain everything: in-memory zones + spill file → merged [`DraftFrame`].
    /// After this call the buffer is empty and the spill file is gone.
    ///
    /// Caller must hold an external mutual-exclusion guard (typically
    /// `timeline.lock.write()`) so no producer can race in.
    pub fn drain(&self, spill_path: &Path) -> Result<DraftFrame> {
        self.spill_to_file(spill_path)?;
        let merged = read_spill_file(spill_path)?;
        delete_spill_file(spill_path)?;
        self.has_spilled.store(false, Ordering::Release);
        Ok(merged)
    }
}

// ── Spill file I/O ──────────────────────────────────────────────────────────

fn append_spill_frame(path: &Path, frame: &DraftFrame) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let bytes = rmp_serde::to_vec(frame)?;
    let len = bytes.len() as u32;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&bytes)?;
    file.flush()?;
    Ok(())
}

fn read_spill_file(path: &Path) -> Result<DraftFrame> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DraftFrame::default());
        }
        Err(e) => return Err(Error::Io(e)),
    };
    let mut merged = DraftFrame::default();
    loop {
        let mut len_buf = [0u8; 4];
        match file.read(&mut len_buf)? {
            0 => break,
            n if n < 4 => {
                return Err(Error::invalid_data(format!(
                    "truncated draft.spill frame length: got {n} bytes"
                )));
            }
            _ => {}
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        file.read_exact(&mut bytes)?;
        let frame: DraftFrame = rmp_serde::from_slice(&bytes)?;
        merged.merge_frame(frame);
    }
    Ok(merged)
}

fn delete_spill_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Io(e)),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::common::ForkNumber;
    use tempfile::tempdir;

    fn tag(rel: u32, chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
            chunk_id,
        }
    }

    fn rf(rel: u32) -> RelFork {
        RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
        }
    }

    fn meta(nblocks: u32) -> RelForkMeta {
        RelForkMeta {
            nblocks,
            deleted: false,
        }
    }

    fn deleted_meta() -> RelForkMeta {
        RelForkMeta {
            nblocks: 0,
            deleted: true,
        }
    }

    fn new_buffer() -> Box<DraftBuffer> {
        let layout = std::alloc::Layout::new::<DraftBuffer>();
        // SAFETY: `alloc_zeroed` returns a properly aligned allocation; zero
        // is a valid initial value for every field (atomics start at 0, slot
        // bytes start at 0 = SLOT_EMPTY).
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut DraftBuffer;
            assert!(!raw.is_null(), "DraftBuffer allocation failed");
            (*raw).init();
            Box::from_raw(raw)
        }
    }

    #[test]
    fn draft_buffer_single_producer_drain_roundtrip() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        for c in 0..50u32 {
            buf.record_chunk(tag(1, c), &spill).unwrap();
        }
        buf.record_relfork(rf(1), meta(32), &spill).unwrap();
        buf.record_relfork(rf(2), meta(48), &spill).unwrap();

        let merged = buf.drain(&spill).unwrap();
        assert_eq!(merged.chunks.len(), 50);
        for c in 0..50u32 {
            assert!(merged.chunks.contains(&tag(1, c)));
        }
        assert_eq!(merged.relforks.get(&rf(1)).unwrap().nblocks, 32);
        assert_eq!(merged.relforks.get(&rf(2)).unwrap().nblocks, 48);
        assert!(!spill.exists(), "drain should remove the spill file");
    }

    #[test]
    fn draft_buffer_relfork_last_write_wins() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_relfork(rf(7), meta(10), &spill).unwrap();
        buf.record_relfork(rf(7), meta(20), &spill).unwrap();
        buf.record_relfork(rf(7), deleted_meta(), &spill).unwrap();
        buf.record_relfork(rf(7), meta(30), &spill).unwrap();

        let merged = buf.drain(&spill).unwrap();
        let m = merged.relforks.get(&rf(7)).unwrap();
        assert_eq!(m.nblocks, 30);
        assert!(!m.deleted);
    }

    #[test]
    fn draft_buffer_spill_on_full() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        // Write 2× the total capacity. With uniform hash distribution
        // every shard's load far exceeds CHUNK_SHARD_CAP, so at least one
        // (almost certainly many) blocking spills must occur.
        let total = (CHUNK_TOTAL_CAP * 2) as u32;
        for c in 0..total {
            buf.record_chunk(tag(2, c), &spill).unwrap();
        }
        assert!(
            buf.spill_seq.load(Ordering::Acquire) >= 1,
            "expected at least one spill",
        );

        let merged = buf.drain(&spill).unwrap();
        assert_eq!(merged.chunks.len() as u32, total);
        for c in 0..total {
            assert!(merged.chunks.contains(&tag(2, c)), "missing chunk {c}");
        }
    }

    #[test]
    fn draft_buffer_spill_on_watermark_drains_zones() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        // Write enough chunks to be confident that at least one shard
        // crosses its 75 % watermark and triggers a non-blocking spill,
        // which drains the entire zone.
        let n = (CHUNK_TOTAL_CAP * 90 / 100) as u32;
        for c in 0..n {
            buf.record_chunk(tag(3, c), &spill).unwrap();
        }
        assert!(buf.spill_seq.load(Ordering::Acquire) >= 1);
        assert!(
            spill.exists(),
            "spill file should exist after watermark spill"
        );

        let merged = buf.drain(&spill).unwrap();
        assert_eq!(merged.chunks.len() as u32, n);
    }

    #[test]
    fn draft_buffer_relfork_last_write_wins_across_spills() {
        // Force multiple spill frames and confirm last-write-wins still
        // holds when the same RelFork is updated across spill boundaries.
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_relfork(rf(99), meta(10), &spill).unwrap();
        buf.spill_to_file(&spill).unwrap();
        buf.record_relfork(rf(99), meta(20), &spill).unwrap();
        buf.spill_to_file(&spill).unwrap();
        buf.record_relfork(rf(99), meta(30), &spill).unwrap();

        let merged = buf.drain(&spill).unwrap();
        assert_eq!(
            merged.relforks.get(&rf(99)).unwrap().nblocks,
            30,
            "latest in-memory write must win over spilled frames",
        );
    }

    #[test]
    fn draft_buffer_get_relfork_returns_recorded_meta() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_relfork(rf(1), meta(32), &spill).unwrap();
        buf.record_relfork(rf(2), meta(64), &spill).unwrap();

        let got1 = buf
            .get_relfork(&rf(1), &spill)
            .unwrap()
            .expect("rf(1) should be present");
        assert_eq!(got1.nblocks, 32);
        assert!(!got1.deleted);

        let got2 = buf
            .get_relfork(&rf(2), &spill)
            .unwrap()
            .expect("rf(2) should be present");
        assert_eq!(got2.nblocks, 64);

        assert!(buf.get_relfork(&rf(3), &spill).unwrap().is_none());
    }

    #[test]
    fn draft_buffer_get_relfork_overwrite_returns_latest() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_relfork(rf(5), meta(10), &spill).unwrap();
        buf.record_relfork(rf(5), meta(20), &spill).unwrap();
        buf.record_relfork(rf(5), deleted_meta(), &spill).unwrap();

        let got = buf
            .get_relfork(&rf(5), &spill)
            .unwrap()
            .expect("rf(5) should be present");
        assert!(
            got.deleted,
            "last write wins: must reflect the deleted update"
        );
    }

    #[test]
    fn draft_buffer_contains_chunk_returns_true_for_recorded_tag() {
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_chunk(tag(1, 0), &spill).unwrap();
        buf.record_chunk(tag(1, 7), &spill).unwrap();

        assert!(buf.contains_chunk(&tag(1, 0)));
        assert!(buf.contains_chunk(&tag(1, 7)));
        // Pre-spill: definitive miss for tags never recorded.
        assert!(!buf.contains_chunk(&tag(1, 999)));
        assert!(!buf.contains_chunk(&tag(2, 0)));
    }

    #[test]
    fn draft_buffer_lookups_survive_spill_to_disk() {
        // After a spill, in-memory zones are drained. `contains_chunk` then
        // returns conservative-yes (false positives are absorbed by
        // Store::get_chunk's fall-through); `get_relfork` scans the spill
        // file because it must return the correct meta value.
        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        buf.record_chunk(tag(4, 0), &spill).unwrap();
        buf.record_relfork(rf(9), meta(100), &spill).unwrap();
        buf.spill_to_file(&spill).unwrap();

        assert!(spill.exists(), "spill file must exist after explicit spill");
        assert!(
            buf.contains_chunk(&tag(4, 0)),
            "recorded chunk must still be visible (conservative-yes after spill)",
        );
        let got = buf
            .get_relfork(&rf(9), &spill)
            .unwrap()
            .expect("relfork recorded before spill must still be visible");
        assert_eq!(got.nblocks, 100);
    }

    #[test]
    fn draft_buffer_concurrent_producers() {
        const THREADS: u32 = 4;
        const PER_THREAD: u32 = 5_000;

        let dir = tempdir().unwrap();
        let spill = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let buf = new_buffer();

        std::thread::scope(|s| {
            for t in 0..THREADS {
                let buf_ref: &DraftBuffer = &buf;
                let spill_path: &Path = &spill;
                s.spawn(move || {
                    for i in 0..PER_THREAD {
                        // Thread `t`, chunk i — unique tag per (t, i).
                        buf_ref
                            .record_chunk(tag(t, i), spill_path)
                            .expect("record_chunk");
                    }
                });
            }
        });

        let merged = buf.drain(&spill).unwrap();
        let expected = (THREADS * PER_THREAD) as usize;
        assert_eq!(merged.chunks.len(), expected);
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                assert!(
                    merged.chunks.contains(&tag(t, i)),
                    "missing chunk t={t} i={i}",
                );
            }
        }
    }
}
