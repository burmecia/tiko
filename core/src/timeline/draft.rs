//! Centralized shmem draft buffer.
//!
//! Two-zone hash table in shared memory that records every chunk tag and
//! relfork-meta update written during the current PG checkpoint interval.
//! Backends record directly; readers ([`crate::store::Store::get_chunk`],
//! [`crate::store::Store::get_meta`]) probe via [`DraftBuffer::contains_chunk`]
//! / [`DraftBuffer::get_relfork`]. At commit time the checkpointer drains
//! both zones plus any on-disk spill overflow and folds them into the new
//! `CheckpointSummary`.
//!
//! Layout:
//! - [`ChunkZone`]: `CHUNK_NUM_SHARDS` sharded open-addressed hash sets of
//!   [`ChunkTag`] (presence-only; chunk data lives at the S3 head-prefix).
//! - [`RelForkZone`]: single open-addressed hash table of
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::chunk::ChunkTag;
use crate::error::{Error, Result};
use crate::relfork::{RelFork, RelForkMeta};
use crate::utils::rw_lock::AtomicRWLock;
use crate::utils::spin_lock::spin_lock;

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

/// Slot capacity of [`RelForkZone`].
pub const REL_FORK_ZONE_CAP: usize = 8192;
const _: () = assert!(REL_FORK_ZONE_CAP.is_power_of_two());

/// Non-blocking spill is triggered when a shard / zone load reaches this
/// percentage of capacity.
pub const DRAFT_SPILL_WATERMARK_PCT: u32 = 75;

/// Per-shard watermark for [`ChunkShard`] (in slots).
pub const CHUNK_SHARD_WATERMARK: usize = CHUNK_SHARD_CAP * DRAFT_SPILL_WATERMARK_PCT as usize / 100;

/// Watermark for [`RelForkZone`] (in slots).
pub const REL_FORK_ZONE_WATERMARK: usize =
    REL_FORK_ZONE_CAP * DRAFT_SPILL_WATERMARK_PCT as usize / 100;

/// Filename of the on-disk overflow file used by [`DraftBuffer::spill_to_file`].
/// Lives under the tiko root path, one per cluster.
pub const DRAFT_SPILL_FILE_NAME: &str = "draft.spill";

/// Upper bound for one serialized spill frame. A legit frame holds at most
/// `CHUNK_TOTAL_CAP + REL_FORK_ZONE_CAP` entries (well under 2 MiB
/// serialized); the cap exists so a corrupt length prefix can't trigger a
/// huge allocation before the read fails.
const MAX_SPILL_FRAME_LEN: usize = 16 * 1024 * 1024;

// ── Slot entries ────────────────────────────────────────────────────────────

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Empty = 0,
    Occupied = 1,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ChunkSlotEntry {
    state: SlotState,
    _pad: [u8; 3],
    tag: ChunkTag,
}
const _: () = assert!(std::mem::size_of::<ChunkSlotEntry>() == 24);

#[repr(C)]
#[derive(Clone, Copy)]
struct RelForkSlotEntry {
    state: SlotState,
    _pad: [u8; 3],
    rf: RelFork,
    meta: RelForkMeta,
}
const _: () = assert!(std::mem::size_of::<RelForkSlotEntry>() == 28);

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
                s.state = SlotState::Empty;
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
                SlotState::Empty => {
                    slot.tag = tag;
                    slot.state = SlotState::Occupied;
                    let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                    return Ok(new_len as usize >= CHUNK_SHARD_WATERMARK);
                }
                SlotState::Occupied if slot.tag == tag => return Ok(false),
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
                SlotState::Empty => return false,
                SlotState::Occupied if slots[idx].tag == *tag => return true,
                _ => continue,
            }
        }
        false
    }

    fn collect_into(&self, dst: &mut HashSet<ChunkTag>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &*self.slots.get() };
        for s in slots.iter() {
            if s.state == SlotState::Occupied {
                dst.insert(s.tag);
            }
        }
    }

    /// Remove slots whose tag is in `collected`, then rebuild the shard.
    /// Exact because the caller holds the exclusive spill lock from collect
    /// to clear: no occupied slot can be emptied in between, so concurrent
    /// inserts only claim still-empty slots and never duplicate a collected
    /// tag. Rebuild (rather than in-place clearing) keeps probe chains
    /// intact: entries inserted after collection may sit behind collected
    /// slots, and clearing those in place would cut their chains.
    fn clear_collected(&self, collected: &HashSet<ChunkTag>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        let mut kept: Vec<ChunkTag> = Vec::new();
        for s in slots.iter_mut() {
            if s.state == SlotState::Occupied {
                if !collected.contains(&s.tag) {
                    kept.push(s.tag);
                }
                s.state = SlotState::Empty;
            }
        }
        for tag in &kept {
            let start = (tag.hash() as usize) % CHUNK_SHARD_CAP;
            for i in 0..CHUNK_SHARD_CAP {
                let slot = &mut slots[(start + i) % CHUNK_SHARD_CAP];
                if slot.state == SlotState::Empty {
                    slot.tag = *tag;
                    slot.state = SlotState::Occupied;
                    break;
                }
            }
        }
        self.len.store(kept.len() as u32, Ordering::Relaxed);
    }
}

/// Sharded hash set of [`ChunkTag`]s. Presence-only — chunk data lives at the
/// S3 head-prefix and is read by [`crate::store::Store::get_chunk`].
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

    fn collect_into(&self, dst: &mut HashSet<ChunkTag>) {
        for shard in self.shards.iter() {
            shard.collect_into(dst);
        }
    }

    fn clear_collected(&self, collected: &HashSet<ChunkTag>) {
        for shard in self.shards.iter() {
            shard.clear_collected(collected);
        }
    }
}

// ── RelForkZone ─────────────────────────────────────────────────────────────

/// Open-addressed hash table of `RelFork → RelForkMeta`. Single global
/// spinlock. Overwriting an existing entry preserves last-write-wins
/// semantics — required by `Store::get_meta` correctness.
#[repr(C, align(128))]
pub struct RelForkZone {
    lock: AtomicU32,
    len: AtomicU32,
    _pad: [u8; 56],
    slots: UnsafeCell<[RelForkSlotEntry; REL_FORK_ZONE_CAP]>,
}

// SAFETY: all access to `slots` is gated by `lock`.
unsafe impl Sync for RelForkZone {}

impl RelForkZone {
    fn init(&self) {
        self.lock.store(0, Ordering::Relaxed);
        self.len.store(0, Ordering::Relaxed);
        // SAFETY: idempotent init prior to publication.
        unsafe {
            let slots = &mut *self.slots.get();
            for s in slots.iter_mut() {
                s.state = SlotState::Empty;
            }
        }
    }

    /// `meta` overwrites any existing entry for the same `rf`. Returns
    /// `Ok(over_watermark)` on insert; returns `Err(())` if the zone is full.
    fn insert(&self, rf: RelFork, meta: RelForkMeta) -> std::result::Result<bool, ()> {
        let _g = spin_lock(&self.lock);
        let start = (rf.hash() as usize) % REL_FORK_ZONE_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        for i in 0..REL_FORK_ZONE_CAP {
            let idx = (start + i) % REL_FORK_ZONE_CAP;
            let slot = &mut slots[idx];
            match slot.state {
                SlotState::Empty => {
                    slot.rf = rf;
                    slot.meta = meta;
                    slot.state = SlotState::Occupied;
                    let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                    return Ok(new_len as usize >= REL_FORK_ZONE_WATERMARK);
                }
                SlotState::Occupied if slot.rf == rf => {
                    slot.meta = meta;
                    return Ok(false);
                }
                _ => continue,
            }
        }
        Err(())
    }

    fn get(&self, rf: &RelFork) -> Option<RelForkMeta> {
        let _g = spin_lock(&self.lock);
        let start = (rf.hash() as usize) % REL_FORK_ZONE_CAP;
        // SAFETY: lock held.
        let slots = unsafe { &*self.slots.get() };
        for i in 0..REL_FORK_ZONE_CAP {
            let idx = (start + i) % REL_FORK_ZONE_CAP;
            match slots[idx].state {
                SlotState::Empty => return None,
                SlotState::Occupied if slots[idx].rf == *rf => {
                    return Some(slots[idx].meta);
                }
                _ => continue,
            }
        }
        None
    }

    fn collect_into(&self, dst: &mut HashMap<RelFork, RelForkMeta>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &*self.slots.get() };
        for s in slots.iter() {
            if s.state == SlotState::Occupied {
                dst.insert(s.rf, s.meta);
            }
        }
    }

    /// Remove slots collected into `collected` (skipping entries whose meta
    /// changed since collection — a concurrent overwrite must survive,
    /// last-write-wins), then rebuild the zone so probe chains of surviving
    /// entries stay intact (see [`ChunkShard::clear_collected`]).
    fn clear_collected(&self, collected: &HashMap<RelFork, RelForkMeta>) {
        let _g = spin_lock(&self.lock);
        // SAFETY: lock held.
        let slots = unsafe { &mut *self.slots.get() };
        let mut kept: Vec<(RelFork, RelForkMeta)> = Vec::new();
        for s in slots.iter_mut() {
            if s.state == SlotState::Occupied {
                if collected.get(&s.rf) != Some(&s.meta) {
                    kept.push((s.rf, s.meta));
                }
                s.state = SlotState::Empty;
            }
        }
        for (rf, meta) in &kept {
            let start = (rf.hash() as usize) % REL_FORK_ZONE_CAP;
            for i in 0..REL_FORK_ZONE_CAP {
                let slot = &mut slots[(start + i) % REL_FORK_ZONE_CAP];
                if slot.state == SlotState::Empty {
                    slot.rf = *rf;
                    slot.meta = *meta;
                    slot.state = SlotState::Occupied;
                    break;
                }
            }
        }
        self.len.store(kept.len() as u32, Ordering::Relaxed);
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
    /// Set once a spill frame is durably appended (before the collected
    /// slots are cleared, so readers never lose sight of an entry);
    /// cleared by `drain` (at commit). Used by [`Self::contains_chunk`] to return
    /// conservative-yes and by [`Self::get_relfork`] to gate the spill-file
    /// scan — avoids touching the file on lookup hot paths when no spill has
    /// occurred since the last commit.
    has_spilled: AtomicBool,
    chunks: ChunkZone,
    relforks: RelForkZone,
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
    pub fn record_chunk(&self, tag: ChunkTag, spill: &SpillFile) -> Result<()> {
        loop {
            match self.chunks.insert(tag) {
                Ok(over_watermark) => {
                    if over_watermark {
                        let _ = self.try_spill_to_file(spill);
                    }
                    return Ok(());
                }
                Err(()) => {
                    // Shard full → blocking spill, then retry.
                    self.spill_to_file(spill)?;
                }
            }
        }
    }

    /// Record a relfork-meta update. Same `rf` overwrites the existing entry
    /// (last-write-wins).
    pub fn record_relfork(&self, rf: RelFork, meta: RelForkMeta, spill: &SpillFile) -> Result<()> {
        loop {
            match self.relforks.insert(rf, meta) {
                Ok(over_watermark) => {
                    if over_watermark {
                        let _ = self.try_spill_to_file(spill);
                    }
                    return Ok(());
                }
                Err(()) => {
                    self.spill_to_file(spill)?;
                }
            }
        }
    }

    /// Conservative presence check.
    ///
    /// Returns `true` if `tag` is in the in-memory chunk zone. If a spill has
    /// occurred since the last `drain` and `tag` is not in the in-memory
    /// zone, returns `true` conservatively rather than scanning the spill
    /// file: the caller ([`crate::store::Store::get_chunk`]) treats a
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
    pub fn get_relfork(&self, rf: &RelFork, spill: &SpillFile) -> Result<Option<RelForkMeta>> {
        if let Some(meta) = self.relforks.get(rf) {
            return Ok(Some(meta));
        }
        if !self.has_spilled.load(Ordering::Acquire) {
            return Ok(None);
        }
        // Read guard: a later spill may be mid-append while this flag is
        // still set from an earlier one; scanning without the guard could
        // hit a partially written frame.
        let _guard = self.spill_lock.read();
        let merged = spill.read()?;
        Ok(merged.relforks.get(rf).cloned())
    }

    /// Blocking spill: drains both zones into a single spill frame.
    pub fn spill_to_file(&self, spill: &SpillFile) -> Result<()> {
        let _guard = self.spill_lock.write();
        self.spill_locked(spill)
    }

    /// Non-blocking spill. Returns `Ok(true)` if the spill ran, `Ok(false)`
    /// if another spill is already in progress.
    pub fn try_spill_to_file(&self, spill: &SpillFile) -> Result<bool> {
        match self.spill_lock.try_write() {
            Some(_guard) => {
                self.spill_locked(spill)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn spill_locked(&self, spill: &SpillFile) -> Result<()> {
        let mut frame = DraftFrame::default();
        self.chunks.collect_into(&mut frame.chunks);
        self.relforks.collect_into(&mut frame.relforks);
        if frame.is_empty() {
            return Ok(());
        }
        // On error nothing was cleared — the entries stay in memory, no
        // rollback needed.
        spill.append_frame(&frame)?;
        // Set after the append but before clearing: entries are visible in
        // the zones until now, and in the file once this flag is up — a
        // reader never loses sight of them.
        self.has_spilled.store(true, Ordering::Release);
        self.chunks.clear_collected(&frame.chunks);
        self.relforks.clear_collected(&frame.relforks);
        self.spill_seq.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Drain everything: in-memory zones + spill file → merged [`DraftFrame`].
    ///
    /// Non-destructive: the spill file is kept so a failed commit can be
    /// retried — the re-run re-merges the same file (plus any new in-memory
    /// entries, which are prefix-consistent because `head_ckpt` cannot have
    /// advanced while the caller's commit was unfinished). Call
    /// [`Self::commit_drain`] only after the drained frame is durably
    /// committed.
    ///
    /// Caller must hold an external mutual-exclusion guard (typically
    /// `timeline.lock.write()`) so no producer can race in.
    pub fn drain(&self, spill: &SpillFile) -> Result<DraftFrame> {
        self.spill_to_file(spill)?;
        spill.read()
    }

    /// Discard the drained state after it has been durably committed.
    pub fn commit_drain(&self, spill: &SpillFile) -> Result<()> {
        spill.delete_durable()?;
        self.has_spilled.store(false, Ordering::Release);
        Ok(())
    }
}

// ── SpillFile ───────────────────────────────────────────────────────────────

/// On-disk overflow file for [`DraftBuffer`]. Owns the frame format and all
/// durability rules (staging-file rename, file + directory fsync).
///
/// Per-process handle holding a heap `PathBuf` — must never be stored in
/// shared memory; [`DraftBuffer`] methods take it by reference instead.
pub struct SpillFile {
    path: PathBuf,
}

impl SpillFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Append a frame by rewriting the file: copy the current contents into
    /// a sibling staging file, append the new frame, fsync, then atomically
    /// rename over the live file. A crash or write error mid-append can only
    /// leave a stale staging file behind — the live file is always a
    /// complete generation, never a torn tail that would poison every later
    /// read.
    fn append_frame(&self, frame: &DraftFrame) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = rmp_serde::to_vec(frame)?;
        let staging = self.staging_path();
        {
            let mut out = File::create(&staging)?;
            match File::open(&self.path) {
                Ok(mut old) => {
                    std::io::copy(&mut old, &mut out)?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::Io(e)),
            }
            out.write_all(&(bytes.len() as u32).to_le_bytes())?;
            out.write_all(&bytes)?;
            out.sync_all()?;
        }
        std::fs::rename(&staging, &self.path)?;
        self.sync_parent_dir()
    }

    /// Merge all frames in file order (oldest → newest). A missing file
    /// reads as an empty frame.
    fn read(&self) -> Result<DraftFrame> {
        let mut file = match File::open(&self.path) {
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
            if len > MAX_SPILL_FRAME_LEN {
                return Err(Error::invalid_data(format!(
                    "draft.spill frame length {len} exceeds cap {MAX_SPILL_FRAME_LEN}"
                )));
            }
            let mut bytes = vec![0u8; len];
            file.read_exact(&mut bytes)?;
            let frame: DraftFrame = rmp_serde::from_slice(&bytes)?;
            merged.merge_frame(frame);
        }
        Ok(merged)
    }

    /// Delete the file and fsync the parent dir so the removal survives an
    /// OS crash — a resurrected file would be re-merged by the next drain
    /// under an advanced head_ckpt. Falls back to truncation (which merges
    /// as an empty frame) if removal fails.
    fn delete_durable(&self) -> Result<()> {
        if let Err(del_err) = self.delete() {
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.path)
                .map_err(|_| del_err)?;
        }
        self.sync_parent_dir()
    }

    fn delete(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    fn staging_path(&self) -> PathBuf {
        let mut name = self.path.file_name().unwrap_or_default().to_owned();
        name.push(".tmp");
        self.path.with_file_name(name)
    }

    /// fsync the parent directory so a rename / removal is durable.
    fn sync_parent_dir(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
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
        // bytes start at 0 = SlotState::Empty).
        unsafe {
            let raw = std::alloc::alloc_zeroed(layout) as *mut DraftBuffer;
            assert!(!raw.is_null(), "DraftBuffer allocation failed");
            (*raw).init();
            Box::from_raw(raw)
        }
    }

    /// Zeroed + init'd heap allocation; valid for every zone type since zero
    /// is a valid initial state for all fields.
    ///
    /// # Safety
    /// `T` must be valid in the all-zero-bytes state and idempotently
    /// `init`-able (holds for `ChunkShard` / `RelForkZone`).
    unsafe fn new_zeroed<T: Sized>(init: impl Fn(&T)) -> Box<T> {
        unsafe {
            let raw = std::alloc::alloc_zeroed(std::alloc::Layout::new::<T>()) as *mut T;
            assert!(!raw.is_null());
            init(&*raw);
            Box::from_raw(raw)
        }
    }

    /// Find `n` chunk_ids whose tags share a home slot in a shard, forming
    /// one deterministic probe chain.
    fn colliding_chunk_ids(n: usize) -> Vec<u32> {
        let mut by_home: HashMap<usize, Vec<u32>> = HashMap::new();
        for c in 0..100_000u32 {
            let home = (tag(9, c).hash() as usize) % CHUNK_SHARD_CAP;
            let group = by_home.entry(home).or_default();
            group.push(c);
            if group.len() == n {
                return group.clone();
            }
        }
        panic!("no {n}-way collision found");
    }

    /// Same for relforks in the relfork zone.
    fn colliding_rel_ids(n: usize) -> Vec<u32> {
        let mut by_home: HashMap<usize, Vec<u32>> = HashMap::new();
        for r in 0..100_000u32 {
            let home = (rf(r).hash() as usize) % REL_FORK_ZONE_CAP;
            let group = by_home.entry(home).or_default();
            group.push(r);
            if group.len() == n {
                return group.clone();
            }
        }
        panic!("no {n}-way collision found");
    }

    #[test]
    fn chunk_shard_clear_collected_keeps_probe_chains() {
        // a, b, c share a home slot: a occupies it, b and c sit right behind
        // it on the probe chain. Clearing a in place would cut b and c off.
        let ids = colliding_chunk_ids(3);
        let (a, b, c) = (tag(9, ids[0]), tag(9, ids[1]), tag(9, ids[2]));

        let shard = unsafe { new_zeroed::<ChunkShard>(|s| s.init()) };
        shard.insert(a).unwrap();
        shard.insert(b).unwrap();
        shard.insert(c).unwrap();

        let collected: HashSet<ChunkTag> = [a].into_iter().collect();
        shard.clear_collected(&collected);

        assert!(!shard.contains(&a));
        assert!(shard.contains(&b), "probe chain cut at b");
        assert!(shard.contains(&c), "probe chain cut at c");
        assert_eq!(shard.len.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn relfork_zone_clear_collected_keeps_probe_chains() {
        let ids = colliding_rel_ids(3);
        let (a, b, c) = (rf(ids[0]), rf(ids[1]), rf(ids[2]));

        let zone = unsafe { new_zeroed::<RelForkZone>(|z| z.init()) };
        zone.insert(a, meta(1)).unwrap();
        zone.insert(b, meta(2)).unwrap();
        zone.insert(c, meta(3)).unwrap();

        let collected: HashMap<RelFork, RelForkMeta> = [(a, meta(1))].into_iter().collect();
        zone.clear_collected(&collected);

        assert!(zone.get(&a).is_none());
        assert_eq!(zone.get(&b).unwrap().nblocks, 2, "probe chain cut at b");
        assert_eq!(zone.get(&c).unwrap().nblocks, 3, "probe chain cut at c");
        assert_eq!(zone.len.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn relfork_zone_clear_collected_keeps_newer_meta() {
        // Collected with meta(1), then overwritten with meta(999) before the
        // clear: the newer write must survive (last-write-wins).
        let zone = unsafe { new_zeroed::<RelForkZone>(|z| z.init()) };
        zone.insert(rf(1), meta(1)).unwrap();
        zone.insert(rf(2), meta(2)).unwrap();

        let collected: HashMap<RelFork, RelForkMeta> = [(rf(1), meta(1))].into_iter().collect();
        zone.insert(rf(1), meta(999)).unwrap();
        zone.clear_collected(&collected);

        assert_eq!(zone.get(&rf(1)).unwrap().nblocks, 999);
        assert_eq!(zone.get(&rf(2)).unwrap().nblocks, 2);
        assert_eq!(zone.len.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn draft_buffer_single_producer_drain_roundtrip() {
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        buf.commit_drain(&spill).unwrap();
        assert!(
            !spill_path.exists(),
            "commit_drain should remove the spill file"
        );
    }

    #[test]
    fn draft_buffer_drain_is_non_destructive_until_commit() {
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        let buf = new_buffer();

        buf.record_chunk(tag(1, 0), &spill).unwrap();
        buf.record_relfork(rf(1), meta(32), &spill).unwrap();

        // A failed commit is retried by re-draining: the re-run must
        // reproduce the same frame.
        let first = buf.drain(&spill).unwrap();
        assert!(spill_path.exists(), "drain must keep the spill file");
        let second = buf.drain(&spill).unwrap();
        assert_eq!(first.chunks, second.chunks);
        assert_eq!(first.relforks, second.relforks);

        // Entries recorded between the failed attempt and the retry are
        // picked up too.
        buf.record_chunk(tag(1, 1), &spill).unwrap();
        let third = buf.drain(&spill).unwrap();
        assert_eq!(third.chunks.len(), 2);

        buf.commit_drain(&spill).unwrap();
        assert!(!spill_path.exists());
        let empty = buf.drain(&spill).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn draft_buffer_spill_survives_stale_tmp_file() {
        // Simulates a crash mid-append: a garbage tmp file is left behind,
        // the live file untouched. The next spill must overwrite it cleanly.
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        std::fs::write(spill.staging_path(), b"garbage").unwrap();

        let buf = new_buffer();
        buf.record_relfork(rf(1), meta(32), &spill).unwrap();
        buf.spill_to_file(&spill).unwrap();

        assert!(!spill.staging_path().exists(), "tmp should be renamed away");
        let merged = buf.drain(&spill).unwrap();
        assert_eq!(merged.relforks.get(&rf(1)).unwrap().nblocks, 32);
    }

    #[test]
    fn draft_buffer_drain_rejects_corrupt_spill_file() {
        // With copy-append-rename a corrupt live file means genuine on-disk
        // corruption; reads must fail loudly, not silently skip data.
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        let buf = new_buffer();

        // Truncated frame length (< 4 bytes).
        std::fs::write(&spill_path, b"ab").unwrap();
        assert!(buf.drain(&spill).is_err());

        // Valid length, short body.
        let mut bad = 10u32.to_le_bytes().to_vec();
        bad.extend_from_slice(b"abc");
        std::fs::write(&spill_path, &bad).unwrap();
        assert!(buf.drain(&spill).is_err());

        // Valid length, body that isn't a DraftFrame.
        let mut bad = 3u32.to_le_bytes().to_vec();
        bad.extend_from_slice(&[0xff, 0xff, 0xff]);
        std::fs::write(&spill_path, &bad).unwrap();
        assert!(buf.drain(&spill).is_err());

        // Absurd length: rejected by the sanity cap before any allocation.
        std::fs::write(&spill_path, u32::MAX.to_le_bytes()).unwrap();
        assert!(buf.drain(&spill).is_err());
    }

    #[test]
    fn draft_buffer_commit_drain_without_spill_file() {
        // Nothing recorded, no spill file: drain reads empty and
        // commit_drain's delete tolerates the missing file.
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        let buf = new_buffer();

        let merged = buf.drain(&spill).unwrap();
        assert!(merged.is_empty());
        buf.commit_drain(&spill).unwrap();
        assert!(!spill_path.exists());
    }

    #[test]
    fn draft_buffer_relfork_last_write_wins() {
        let dir = tempdir().unwrap();
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
            spill_path.exists(),
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        let buf = new_buffer();

        buf.record_chunk(tag(4, 0), &spill).unwrap();
        buf.record_relfork(rf(9), meta(100), &spill).unwrap();
        buf.spill_to_file(&spill).unwrap();

        assert!(
            spill_path.exists(),
            "spill file must exist after explicit spill"
        );
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
        let spill_path = dir.path().join(DRAFT_SPILL_FILE_NAME);
        let spill = SpillFile::new(spill_path.clone());
        let buf = new_buffer();

        std::thread::scope(|s| {
            for t in 0..THREADS {
                let buf_ref: &DraftBuffer = &buf;
                let spill_ref: &SpillFile = &spill;
                s.spawn(move || {
                    for i in 0..PER_THREAD {
                        // Thread `t`, chunk i — unique tag per (t, i).
                        buf_ref
                            .record_chunk(tag(t, i), spill_ref)
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
