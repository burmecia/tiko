use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

use crate::{
    chunk::{CHUNK_SIZE, ChunkTag, RelFork},
    db::{DbMeta, DbNamespace},
    error::{Error, Result},
    io::{
        draft::{DRAFT_SPILL_FILE_NAME, DraftFrame},
        locator::Locator,
        storage::Storage,
        timeline::{ACTIVE_WINDOW_SIZE, RelforkLookup},
    },
    io_control::IoControl,
    manifest::Manifest,
    relfork::RelForkMeta,
    tiko_root_path,
};
use pgsys::{
    common::{BLCKSZ, BlockNumber, XLOG_SEG_SIZE},
    logging::{pg_log_debug1, pg_log_debug2, pg_log_info, pg_log_warning},
    lsn::Lsn,
    timeline_id::TimelineId,
};

use super::timeline::{Checkpoint, SegmentCheckpoint, SegmentId, TimelineSegment};

/// Parse a base-manifest storage key into its `Checkpoint`.
///
/// Keys have the shape `{bases_prefix}{tl_hex}/{lsn_hex}.manifest`, where
/// `bases_prefix` is `Locator::bases_dir()` (`{ns}/bases/`). Returns `None` for
/// any key that doesn't match (so callers can `filter_map` over a raw listing).
fn parse_base_manifest_ckpt(key: &str, bases_prefix: &str) -> Option<Checkpoint> {
    let rel = key.strip_prefix(bases_prefix)?;
    let (tl_hex, rest) = rel.split_once('/')?;
    let lsn_hex = rest.strip_suffix(".manifest")?;
    let timeline_id = TimelineId::from_hex(tl_hex).ok()?;
    let lsn = Lsn::from_hex(lsn_hex).ok()?;
    Some(Checkpoint::new(timeline_id, lsn))
}

/// From a raw listing of base-manifest keys, select the newest base manifest
/// whose checkpoint is at or before `target`. Returns `(checkpoint, key)`.
fn select_newest_base_at_or_before(
    keys: &[String],
    bases_prefix: &str,
    target: Checkpoint,
) -> Option<(Checkpoint, String)> {
    keys.iter()
        .filter_map(|k| parse_base_manifest_ckpt(k, bases_prefix).map(|c| (c, k.clone())))
        .filter(|(c, _)| *c <= target)
        .max_by_key(|(c, _)| *c)
}

/// From `(timestamp, checkpoint, key)` candidates, select the newest base
/// manifest on `timeline` whose `timestamp <= target_ts`. Returns
/// `(checkpoint, key)`. Ordering uses `(timestamp, checkpoint)`; both are
/// monotonic, so this is the latest base at or before the target time.
fn select_base_by_time(
    candidates: &[(i64, Checkpoint, String)],
    target_ts: i64,
    timeline: TimelineId,
) -> Option<(Checkpoint, String)> {
    candidates
        .iter()
        .filter(|(ts, c, _)| *ts <= target_ts && c.timeline_id == timeline)
        .max_by_key(|(ts, c, _)| (*ts, *c))
        .map(|(_, c, k)| (*c, k.clone()))
}

/// One WAL segment's coverage on a timeline, in absolute LSN. `full` = a sealed
/// segment covering its entire `XLOG_SEG_SIZE` range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegEntry {
    seg_no: u64,
    lo: u64,
    hi: u64,
    full: bool,
}

/// Parse a WAL object key under `wal_prefix` (= `{ns}/wal/{tl}/`) into its
/// segment number and, for chunk objects, the chunk byte offset.
///
/// Sealed segment: `{wal_prefix}{segname}`                      → (seg_no, None)
/// Chunk:          `{wal_prefix}{segname}.chunks/{offset:016X}` → (seg_no, Some(offset))
/// `segname` is 24 hex chars; `seg_no` is hex chars [8..24). `None` for non-matches.
fn parse_wal_key(key: &str, wal_prefix: &str) -> Option<(u64, Option<usize>)> {
    let rel = key.strip_prefix(wal_prefix)?;
    if let Some((segname, offpart)) = rel.split_once(".chunks/") {
        if segname.len() != 24 || !segname.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let seg_no = u64::from_str_radix(&segname[8..24], 16).ok()?;
        let off = usize::from_str_radix(offpart, 16).ok()?;
        Some((seg_no, Some(off)))
    } else {
        if rel.len() != 24 || !rel.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let seg_no = u64::from_str_radix(&rel[8..24], 16).ok()?;
        Some((seg_no, None))
    }
}

/// Compute the contiguous archived-WAL run that reaches the highest segment in
/// `entries`. Returns `(w_lo, w_hi)` absolute LSN, or `None` if empty.
///
/// The highest segment anchors the run end. The run extends down through
/// consecutive `full` (sealed) segments while contiguous; it stops at the first
/// missing/partial lower segment, or as soon as a segment does not start at its
/// own segment boundary (a mid-segment front gap).
fn wal_contiguous_run(entries: &[SegEntry]) -> Option<(u64, u64)> {
    let seg = XLOG_SEG_SIZE as u64;
    let mut sorted: Vec<SegEntry> = entries.to_vec();
    sorted.sort_unstable_by(|a, b| b.seg_no.cmp(&a.seg_no)); // descending
    let top = *sorted.first()?;
    let (mut w_lo, w_hi) = (top.lo, top.hi);
    let mut cur = top;
    let mut idx = 1;
    loop {
        // Segment 0 is the lowest possible — nothing below it. (Also guards the
        // `cur.seg_no - 1` below against debug-mode underflow.)
        if cur.seg_no == 0 {
            break;
        }
        // Can only extend below if `cur` covers from its own segment start.
        if cur.lo != cur.seg_no * seg {
            break;
        }
        let Some(next) = sorted.get(idx).copied() else {
            break;
        };
        // Must be the immediately-lower segment, full, and contiguous.
        if next.seg_no != cur.seg_no - 1 || !next.full || next.hi != cur.lo {
            break;
        }
        w_lo = next.lo;
        cur = next;
        idx += 1;
    }
    Some((w_lo, w_hi))
}

/// A base manifest is usable as a PITR anchor if its recovery WAL fits inside
/// the contiguous archived run `[w_lo, w_hi]`: the replay start (`redo`) must be
/// archived, and its checkpoint record must be within coverage.
fn is_base_usable(ckpt_lsn: u64, redo_lsn: u64, w_lo: u64, w_hi: u64) -> bool {
    redo_lsn >= w_lo && ckpt_lsn <= w_hi
}

/// One row returned by [`Store::list_checkpoints`] — a recovery-target
/// candidate flattened from the timeline segment files.
#[derive(Debug, Clone)]
pub struct CheckpointRow {
    pub ckpt: Checkpoint,
    pub redo_ckpt: Checkpoint,
    pub created_at: i64,
    pub n_chunks: usize,
}

/// The recoverable window reported by [`Store::recovery_window`], bounded by
/// archived-WAL coverage.
///
/// `earliest` is the oldest base manifest whose recovery WAL (`[redo,
/// checkpoint]`) lies inside the contiguous archived-WAL run; `latest_lsn` is
/// the end of that run (the highest recoverable LSN). A PITR target must fall
/// within `[earliest_ckpt, latest_lsn]` (and `[earliest_ts, latest_ts]`).
#[derive(Debug, Clone)]
pub struct RecoveryWindow {
    pub earliest_ts: i64,
    pub earliest_ckpt: Checkpoint,
    pub latest_ts: i64,
    /// End of the contiguous archived-WAL run (highest recoverable LSN).
    pub latest_lsn: Lsn,
    pub timeline: TimelineId,
}

/// Outcome of one [`Store::run_compaction`] call. Returned to the compactor
/// task in `worker` for logging and metrics.
#[derive(Debug)]
pub enum CompactionResult {
    /// No `IoControl` (initdb/single-user, or pre-postmaster startup).
    Skipped,
    /// No segment checkpoints exist in the eligible range yet.
    NoNewSegments,
    /// Another compactor advanced `base_ckpt` while we were preparing the
    /// new base manifest; our work was discarded.
    Raced,
    /// Successfully applied `count` segment checkpoints and advanced
    /// `base_ckpt` to `new_base_ckpt`.
    Applied {
        base_ckpt: Checkpoint,
        new_base_ckpt: Checkpoint,
        count: usize,
    },
}

// ── Store ─────────────────────────────────────────────────────────────────────

static STORE: OnceLock<Store> = OnceLock::new();

/// Top-level store object.
///
/// Holds a concrete `ObjectStore` backend (`S3Sim` or `S3`) and provides:
/// - The same primitive two-bucket operations via forwarding methods.
///   built entirely from `ObjectStore` primitives.
/// - A process-global singleton (`init` / `get` / `try_get`).
pub struct Store {
    ns: DbNamespace,
    lctr: Locator,
    /// Current base-manifest snapshot. Readers grab an `Arc<Manifest>` under
    /// the `Mutex` (briefly) and use it lock-free. The `Manifest` is
    /// immutable; the compactor produces a fresh one via
    /// [`Manifest::commit_applied`] and swaps the `Arc` in here. Cross-process
    /// staleness is detected by comparing
    /// `IoControl::get().timeline.base_ckpt` to the current `Manifest`'s own
    /// [`Manifest::checkpoint`].
    base_manifest: Mutex<Arc<Manifest>>,
    storage: Storage,
    /// Local root path used to materialise the base-manifest TIKM cache file
    /// on reload (and the draft spill file). One per process.
    local_root: PathBuf,
    /// On-disk overflow file for the centralized shmem [`DraftBuffer`].
    /// One per cluster at `{tiko_root}/draft.spill`.
    draft_spill_path: PathBuf,
}

impl Store {
    pub fn locator(&self) -> &Locator {
        &self.lctr
    }

    /// Update the DbMeta JSON object on storage with the latest checkpoint
    /// LSN. Internal helper called from [`Store::run_commit_protocol`].
    fn update_db_meta(&self, ckpt: &Checkpoint) -> Result<()> {
        let db = DbMeta::new(self.ns.clone());
        let key = self.lctr.db_meta();

        // Load existing DbMeta if it exists.
        match self.storage.get(&key) {
            Ok(json_bytes) => db.load_from_json_bytes(&json_bytes),
            Err(err) if err.is_not_found() => {} // no existing meta; treat as default
            Err(err) => return Err(err),
        }

        db.set_checkpoint_lsn(ckpt);
        let json_bytes = db.to_json_bytes();
        self.storage_put(&key, &json_bytes)?;

        Ok(())
    }

    // ── Global singleton ──────────────────────────────────────────────────

    /// Initialise the global `Store` with a local sim backend and return a
    /// `'static` reference to it. Subsequent calls are silently ignored
    /// (OnceLock semantics).
    pub fn init() -> Result<&'static Self> {
        if let Some(store) = STORE.get() {
            return Ok(store);
        }

        let local_root = tiko_root_path();
        let ns = DbNamespace::new_from_env();
        let lctr = Locator::new(ns.clone());
        let storage = Storage::new(&local_root);

        // Local fast path: reuse the on-disk TIKM file if a previous
        // invocation (this process or a sibling) already published it. The
        // local file is kept up to date by the compactor's `commit_applied`
        // (atomic tmp + rename) and by S3-fallback reloads, so it's at worst
        // stale by one compaction cycle — the normal staleness check on
        // subsequent `base_manifest()` calls catches up.
        //
        // Falls back to an S3 list + GET if the local file is missing or
        // unreadable (fresh data dir, or after a `tiko_root` reset).
        let initial: Manifest = match Manifest::open_local(&local_root) {
            Ok(manifest) => {
                pg_log_debug2(format!(
                    "tiko: Store::init(): opened local base manifest at {}",
                    manifest.checkpoint()
                ));
                manifest
            }
            Err(_) => {
                let mut bases = storage.list_prefix(&lctr.bases_dir())?;
                bases.sort_unstable();
                if let Some(key) = bases.last() {
                    let bytes = storage.get(key)?;
                    let manifest = Manifest::from_bytes(&bytes, &local_root)?;
                    pg_log_debug1(format!(
                        "tiko: Store::init(): downloaded base manifest {key} at {}",
                        manifest.checkpoint()
                    ));
                    manifest
                } else {
                    pg_log_debug1(
                        "tiko: Store::init(): no base manifests found; starting with an empty one",
                    );
                    Manifest::empty(&local_root)?
                }
            }
        };

        let draft_spill_path = local_root.join(DRAFT_SPILL_FILE_NAME);
        let store = Store {
            ns,
            lctr,
            base_manifest: Mutex::new(Arc::new(initial)),
            storage: Storage::new(&local_root),
            local_root,
            draft_spill_path,
        };

        let _ = STORE.set(store); // ignore duplicate init attempts
        let store = Self::try_get()?;

        // Hydrate the timeline state from existing segments. Idempotent —
        // the `hydrated` flag in shmem gates the work to the first caller;
        // subsequent backends short-circuit. Requires `IoControl` to be
        // attached (no-op otherwise); `tiko_init` calls
        // `IoControl::init_or_attach` before `Store::init`. Failure is
        // logged but doesn't abort startup — readers fall back to
        // base-manifest + segment scan on demand.
        if let Err(e) = store.hydrate_timeline_state() {
            pg_log_warning(format!(
                "tiko: Store::init(): hydrate_timeline_state failed: {e}"
            ));
        }

        Ok(store)
    }

    /// Return a snapshot of the current base manifest, fresh w.r.t. the
    /// shmem `timeline.base_ckpt`. Fast path: one `Mutex` lock + `Arc::clone`.
    /// Slow path (compactor has advanced `base_ckpt` since our last load):
    /// reload from S3 inside the lock so concurrent reloaders serialise on
    /// the local TIKM file write.
    pub(crate) fn base_manifest(&self) -> Result<Arc<Manifest>> {
        let target = IoControl::try_get()
            .map(|c| c.timeline.base_ckpt)
            .unwrap_or_default();

        {
            let guard = self.base_manifest.lock().unwrap();
            if guard.checkpoint() == target {
                return Ok(guard.clone());
            }
        }

        // Slow path: reload from S3 (or local TIKM via open_local).
        let new = Arc::new(self.load_manifest_at(target)?);
        let mut guard = self.base_manifest.lock().unwrap();
        if guard.checkpoint() != target {
            *guard = new.clone();
            return Ok(new);
        }
        Ok(guard.clone())
    }

    /// Load a fresh `Manifest` for the given checkpoint.
    ///
    /// Fast path: open the existing local TIKM file in-place via
    /// [`Manifest::open_local`]; if its header matches `ckpt`, no network
    /// I/O occurs. The compactor publishes the TIKM file atomically (tmp +
    /// rename inside `write_tikm`) so seeing a complete file here means it
    /// matches some checkpoint — we just verify it's the one we want.
    ///
    /// Slow path: GET the msgpack blob from S3 and materialise a fresh
    /// local TIKM file via [`Manifest::from_bytes`].
    ///
    /// For the default checkpoint (no base manifest yet) returns an empty
    /// manifest.
    fn load_manifest_at(&self, ckpt: Checkpoint) -> Result<Manifest> {
        if ckpt == Checkpoint::default() {
            return Manifest::empty(&self.local_root);
        }

        if let Ok(manifest) = Manifest::open_local(&self.local_root) {
            if manifest.checkpoint() == ckpt {
                return Ok(manifest);
            }
        }

        // S3 fallback. `Manifest::from_bytes` materialises the local TIKM
        // file as a side effect (also via tmp + rename inside `write_tikm`).
        let key = self.lctr.base_manifest(&ckpt);
        let bytes = self.storage.get(&key)?;
        Manifest::from_bytes(&bytes, &self.local_root)
    }

    /// Return a `'static` reference to the global `Store`.
    ///
    /// # Panics
    /// Panics if `Store::init` has not been called.
    pub fn get() -> &'static Self {
        STORE
            .get()
            .expect("Store::get() called before Store::init()")
    }

    /// Return the global `Store`, or `None` if not yet initialised.
    pub fn try_get() -> Result<&'static Self> {
        STORE.get().ok_or_else(|| Error::StoreNotAvailable)
    }

    // ── RelFork meta operations ──────────────────────────────────────────────────

    pub(crate) fn get_meta(&self, rf: &RelFork) -> Result<RelForkMeta> {
        if let Some(io_control) = IoControl::try_get() {
            let _guard = io_control.timeline.lock.read();

            let timeline = &io_control.timeline;
            let head_ckpt = timeline.head_ckpt;
            let base_ckpt = timeline.base_ckpt;

            // 1. Live interval: shmem draft buffer is the sole source of
            //    truth for uncommitted writes. Falls back to the spill file
            //    transparently if the in-memory zone has been drained.
            if let Some(meta) = timeline.draft.get_relfork(rf, &self.draft_spill_path)? {
                return Ok(meta);
            }

            // 2. Active window newest → oldest, gated by inline relfork index.
            //    A `Hit` returns directly; a `DefinitiveMiss` means the relfork
            //    was not touched in that checkpoint (safe to skip).
            //    An `Inconclusive` (index overflowed) means the relfork *may*
            //    have been written in this checkpoint — we must stop the
            //    in-memory walk and let the segment scan find the truth.
            //    Continuing past an Inconclusive would risk returning a stale
            //    `Hit` from an older active checkpoint while a newer write
            //    sits unread in the overflowed checkpoint's segment file.
            let mut oldest_active_ckpt: Option<Checkpoint> = None;
            for ac in timeline.iter_active() {
                oldest_active_ckpt = Some(ac.ckpt);
                match ac.relfork_index.get(rf) {
                    RelforkLookup::Hit(meta) => return Ok(meta),
                    RelforkLookup::DefinitiveMiss => continue,
                    RelforkLookup::Inconclusive => break,
                }
            }

            // 3. Segment scan up to `oldest_active_ckpt` inclusive.
            //    - If the loop broke on Inconclusive at K, `oldest_active_ckpt`
            //      is K and we need K's segment file (it may carry the rf
            //      even though K's inline index didn't expose it). Active
            //      checkpoints newer than K reported DefinitiveMiss, and
            //      since a non-overflowed `RelforkIndex` mirrors its
            //      segment's relfork map exactly, their segments don't
            //      carry the rf either — no need to re-read them.
            //    - If every active checkpoint reported DefinitiveMiss, the
            //      loop ran to completion and `oldest_active_ckpt` is the
            //      oldest active checkpoint. Its segment will be
            //      re-confirmed empty by the segment scan, which then
            //      continues down to `base_ckpt`.
            let seg_top_ckpt = oldest_active_ckpt.unwrap_or(head_ckpt);
            if let Some(meta) = self.read_relfork_from_segments(rf, base_ckpt, seg_top_ckpt)? {
                return Ok(meta);
            }
        }

        // 3. Base manifest fallback.
        if let Some(meta) = self.base_manifest()?.lookup_relfork_meta(rf)? {
            return Ok(meta);
        }

        Err(Error::not_found("relfork not found"))
    }

    pub(crate) fn put_meta(&self, rf: &RelFork, meta: &RelForkMeta) -> Result<()> {
        // The draft buffer is the sole source of truth for live-interval
        // relfork meta. The meta is captured into the next segment when the
        // commit protocol drains the draft.
        //
        // Hold the timeline read lock across the draft record so the entry
        // is observed by `get_meta` callers within this interval's window.
        // The checkpointer's write lock waits for all in-flight read-lock
        // holders to drain (the fence — see plan, "Commit protocol"); the
        // checkpointer flushes dirty cache state *before* acquiring its
        // write lock, so no re-entrancy risk.
        //
        // `IoControl::try_get()` is always `Some` here: `tiko_init` (via
        // `smgrinit`) runs in every mode that can reach this code path —
        // bootstrap, single-user, and runtime — and `init_or_attach` has
        // succeeded by then.
        let io_control = IoControl::get();
        let _timeline_guard = io_control.timeline.lock.read();
        self.record_relfork_eviction(*rf, meta.clone());
        Ok(())
    }

    pub(crate) fn get_nblocks(&self, rf: &RelFork) -> Result<BlockNumber> {
        let meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        Ok(meta.nblocks)
    }

    pub(crate) fn put_nblocks(&self, rf: &RelFork, nblocks: BlockNumber) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.nblocks = nblocks;
        self.put_meta(rf, &meta)
    }

    pub(crate) fn get_deleted(&self, rf: &RelFork) -> Result<bool> {
        let meta = self.get_meta(rf)?;
        Ok(meta.deleted)
    }

    pub(crate) fn create_relfork(&self, rf: &RelFork) -> Result<()> {
        match self.get_meta(rf) {
            Ok(meta) => {
                if !meta.deleted {
                    return Err(Error::already_exists("relfork already exists"));
                }
                self.put_meta(rf, &RelForkMeta::default())
            }
            Err(err) if err.is_not_found() => self.put_meta(rf, &RelForkMeta::default()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn delete_relfork(&self, rf: &RelFork) -> Result<()> {
        let mut meta = self.get_meta(rf)?;
        if meta.deleted {
            return Err(Error::not_found("relfork is deleted"));
        }
        meta.deleted = true;
        self.put_meta(rf, &meta)
    }

    // ── Chunk operations ──────────────────────────────────────────────────

    pub(crate) fn get_chunk(&self, tag: &ChunkTag, dst: &mut [u8]) -> Result<()> {
        debug_assert_eq!(dst.len(), CHUNK_SIZE);

        if let Some(io_control) = IoControl::try_get() {
            let _guard = io_control.timeline.lock.read();
            let timeline = &io_control.timeline;

            let head_ckpt = timeline.head_ckpt;
            let base_ckpt = timeline.base_ckpt;

            // 1. Probe the current head prefix only if the draft buffer
            //    reports the tag is recorded for this interval. Without this
            //    gate, every `get_chunk` would speculatively GET head-prefix
            //    even when the chunk wasn't touched in this interval.
            //    `contains_chunk` is conservative — false positives degrade
            //    to the legacy speculative-GET behavior; false negatives are
            //    impossible (a recorded chunk is either in-memory or in the
            //    spill file, and `contains_chunk` returns true in both cases).
            if timeline.draft.contains_chunk(tag) && self.try_read_chunk_at(tag, &head_ckpt, dst)? {
                return Ok(());
            }

            // 2. Active window newest → oldest, gated by Bloom filter. Bloom
            //    false positives fall through to the next entry; false
            //    negatives are impossible.
            let mut oldest_active_ckpt: Option<Checkpoint> = None;
            for ac in timeline.iter_active() {
                oldest_active_ckpt = Some(ac.ckpt);
                if !ac.chunk_bloom.maybe_contains(tag) {
                    continue;
                }
                if self.try_read_chunk_at(tag, &ac.prev_ckpt, dst)? {
                    return Ok(());
                }
            }

            // 3. On-disk segments below the active window, down to base_ckpt.
            //    `oldest_active_ckpt` is exclusive — its data was already
            //    probed via the active-window Bloom walk above.
            let seg_top_ckpt = oldest_active_ckpt.unwrap_or(head_ckpt);
            if self.read_chunk_from_segments(tag, base_ckpt, seg_top_ckpt, dst)? {
                return Ok(());
            }
        }

        // 4. Base manifest fallback.
        let chunk_ref = self.base_manifest()?.lookup(tag)?;
        if let Some(chunk_ref) = chunk_ref {
            let key = self.lctr.chunk_base(tag, &chunk_ref);
            let src = self.storage_get(&key)?;
            dst.copy_from_slice(&src);
            return Ok(());
        }

        Err(Error::not_found("chunk not found in storage"))
    }

    pub(crate) fn patch_chunk(&self, tag: &ChunkTag, block_offset: u32, data: &[u8]) -> Result<()> {
        debug_assert!(!data.is_empty());
        debug_assert_eq!(data.len() % BLCKSZ, 0);

        let byte_offset = block_offset as usize * BLCKSZ;
        debug_assert!(byte_offset + data.len() <= CHUNK_SIZE);

        let is_full_chunk = byte_offset == 0 && data.len() == CHUNK_SIZE;

        // Eviction-flush path: hold the timeline read lock across
        // (read head_ckpt → PUT → record into draft). The checkpointer
        // flushes dirty cache state *before* acquiring its write lock,
        // so this read lock never re-enters from the commit side.
        //
        // `IoControl::get()` is always valid here: `tiko_init` ran via
        // `smgrinit` for every mode that can call `patch_chunk`.
        let io_control = IoControl::get();
        let _timeline_guard = io_control.timeline.lock.read();

        let head_ckpt = io_control.timeline.head_ckpt;
        let key = self.lctr.chunk(tag, &head_ckpt);

        if is_full_chunk {
            self.storage_put(&key, data)?;
        } else {
            let mut merged = vec![0u8; CHUNK_SIZE];
            match self.get_chunk(tag, &mut merged) {
                Ok(()) => {}
                Err(e) if e.is_not_found() => {} // chunk absent → treat as zeros
                Err(e) => return Err(e),
            }
            merged[byte_offset..byte_offset + data.len()].copy_from_slice(data);
            self.storage_put(&key, &merged)?;
        };

        self.record_chunk_eviction(*tag);
        Ok(())
    }

    // ── Primitive forwarding methods ──────────────────────────────────────

    pub fn storage_put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.storage.put(key, data)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.storage.inc_puts(data.len());
        });
        Ok(())
    }

    pub fn storage_get(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.storage.get(key)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.storage.inc_gets(data.len());
        });
        Ok(data)
    }

    pub fn storage_list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let ret = self.storage.list_prefix(prefix)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.storage.inc_lists();
        });
        Ok(ret)
    }

    pub fn storage_delete(&self, key: &str) -> Result<()> {
        self.storage.delete(key)?;
        IoControl::try_get().map(|io_control| {
            io_control.stats.storage.inc_deletes();
        });
        Ok(())
    }

    // ── Backend draft (eviction-flush recording) ──────────────────────────

    fn record_chunk_eviction(&self, tag: ChunkTag) {
        let Some(io_control) = IoControl::try_get() else {
            return;
        };
        if let Err(e) = io_control
            .timeline
            .draft
            .record_chunk(tag, &self.draft_spill_path)
        {
            pg_log_warning(format!("tiko: failed to record chunk eviction: {e}"));
        }
    }

    fn record_relfork_eviction(&self, rf: RelFork, meta: RelForkMeta) {
        let Some(io_control) = IoControl::try_get() else {
            return;
        };
        if let Err(e) = io_control
            .timeline
            .draft
            .record_relfork(rf, meta, &self.draft_spill_path)
        {
            pg_log_warning(format!("tiko: failed to record relfork eviction: {e}"));
        }
    }

    // ── Commit protocol ──────────────────────────────────────────────────

    /// List every segment file under the timeline directory, parsed into
    /// `SegmentId`s and sorted ascending by `(timeline_id, index)` (the
    /// natural derived order). Returns an empty vec if the directory does
    /// not exist yet.
    fn list_all_segments(&self) -> Result<Vec<SegmentId>> {
        let prefix = self.lctr.timeline_segments_dir();
        let keys = match self.storage.list_prefix(&prefix) {
            Ok(k) => k,
            Err(e) if e.is_not_found() => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut ids: Vec<SegmentId> = keys
            .iter()
            .filter_map(|path_str| SegmentId::from_path_string(path_str))
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// Return every segment whose LSN coverage overlaps `[low_ckpt, high_ckpt]`,
    /// sorted ascending by `(timeline_id, index)`. Both `timeline_id` and
    /// `lsn` are monotonic so each candidate segment is positioned uniquely
    /// in this total order — no merging across timelines is needed.
    ///
    /// A segment `(tl, idx)` covers LSNs `[idx * RANGE, (idx + 1) * RANGE)`
    /// in timeline `tl`. The filter keeps a segment if any LSN in its
    /// coverage could fall inside `[low_ckpt, high_ckpt]` under `Checkpoint`'s
    /// derived total order.
    fn list_segments_in_range(
        &self,
        low_ckpt: Checkpoint,
        high_ckpt: Checkpoint,
    ) -> Result<Vec<SegmentId>> {
        let mut ids = self.list_all_segments()?;
        ids.retain(|sid| sid.overlaps_range(low_ckpt, high_ckpt));
        Ok(ids)
    }

    /// Try to read the chunk for `tag` at the prefix derived from `ckpt`.
    /// Returns `Ok(true)` on hit (data copied into `dst`), `Ok(false)` on
    /// not-found, propagates other storage errors.
    fn try_read_chunk_at(&self, tag: &ChunkTag, ckpt: &Checkpoint, dst: &mut [u8]) -> Result<bool> {
        let key = self.lctr.chunk(tag, ckpt);
        match self.storage.get(&key) {
            Ok(data) => {
                dst.copy_from_slice(&data);
                Ok(true)
            }
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Try to load the segment at `segment_id` from storage. Returns
    /// `Ok(None)` if no segment file exists (e.g. that LSN range hasn't been
    /// committed to yet).
    fn load_segment(&self, segment_id: &SegmentId) -> Result<TimelineSegment> {
        let key = self.lctr.timeline_segment(segment_id);
        let seg_bytes = self.storage.get(&key)?;
        TimelineSegment::from_bytes(&seg_bytes)
    }

    /// Walk on-disk segments newest → oldest covering the half-open
    /// checkpoint range `[low_ckpt, high_ckpt_excl)`. On the first checkpoint
    /// whose summary contains `tag`, fetch the chunk into `dst` at the
    /// prefix recorded in `prev_ckpt`. Returns `Ok(true)` on hit,
    /// `Ok(false)` if no segment yields the chunk.
    ///
    /// `high_ckpt_excl` is exclusive because the caller has already covered
    /// `[oldest_active_ckpt, head_ckpt]` via the in-memory active-window
    /// Bloom walk, and the segment file for the oldest active checkpoint
    /// would re-cover the same data.
    fn read_chunk_from_segments(
        &self,
        tag: &ChunkTag,
        low_ckpt: Checkpoint,
        high_ckpt_excl: Checkpoint,
        dst: &mut [u8],
    ) -> Result<bool> {
        if high_ckpt_excl <= low_ckpt {
            return Ok(false);
        }
        // List one slot wider than the exclusive bound — the inner filter
        // drops checkpoints at `high_ckpt_excl` and above.
        let segments = self.list_segments_in_range(low_ckpt, high_ckpt_excl)?;
        for sid in segments.iter().rev() {
            let seg = self.load_segment(sid)?;
            for sc in seg.checkpoints.iter().rev() {
                if sc.ckpt < low_ckpt || sc.ckpt >= high_ckpt_excl {
                    continue;
                }
                if sc.contains_chunk(tag) && self.try_read_chunk_at(tag, &sc.prev_ckpt, dst)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Walk on-disk segments newest → oldest covering the closed checkpoint
    /// range `[low_ckpt, high_ckpt]`, returning the most recent
    /// `RelForkMeta` for `rf` embedded in any segment checkpoint, or
    /// `Ok(None)` if none. Both endpoints are inclusive.
    fn read_relfork_from_segments(
        &self,
        rf: &RelFork,
        low_ckpt: Checkpoint,
        high_ckpt: Checkpoint,
    ) -> Result<Option<RelForkMeta>> {
        if high_ckpt < low_ckpt {
            return Ok(None);
        }
        let segments = self.list_segments_in_range(low_ckpt, high_ckpt)?;
        for sid in segments.iter().rev() {
            let seg = self.load_segment(sid)?;
            for sc in seg.checkpoints.iter().rev() {
                if sc.ckpt < low_ckpt || sc.ckpt > high_ckpt {
                    continue;
                }
                if let Some(meta) = sc.relfork_meta(rf) {
                    return Ok(Some(meta.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Build a [`SegmentCheckpoint`] from the drained drafts and append it
    /// to the appropriate timeline segment file (load existing or init new).
    ///
    /// Called by [`Store::run_commit_protocol`] while the timeline write
    /// lock is held.
    fn commit_segment(
        &self,
        commit_ckpt: Checkpoint,
        prev_ckpt: Checkpoint,
        redo_ckpt: Checkpoint,
        drained: DraftFrame,
        pg_state_bytes: &[u8],
    ) -> Result<SegmentCheckpoint> {
        let segment_id = commit_ckpt.to_segment_id();
        let mut seg = match self.load_segment(&segment_id) {
            Ok(existing) => existing,
            Err(e) if e.is_not_found() => TimelineSegment::new(segment_id),
            Err(e) => return Err(e),
        };
        let summary = SegmentCheckpoint::new(
            commit_ckpt,
            prev_ckpt,
            redo_ckpt,
            drained.chunks,
            drained.relforks,
            pg_state_bytes,
        );
        seg.push(summary.clone());

        // Write `segment` to storage (overwriting any previous version at the
        // same key). Subsequent commits in the same segment LSN range will
        // re-read this file and append to it.
        let key = self.lctr.timeline_segment(&segment_id);
        let bytes = seg.to_bytes()?;
        self.storage.put(&key, &bytes)?;

        Ok(summary)
    }

    /// Outcome of a single [`Store::run_compaction`] call.
    ///
    /// Run the segment-based compactor. Picks a target checkpoint
    /// `< redo_ckpt` (or `<= head_ckpt` if `redo_ckpt` hasn't been set yet),
    /// merges every `SegmentCheckpoint` in `(base_ckpt, target]` into the
    /// base manifest, writes the new base, advances `base_ckpt`, and
    /// deletes segment files whose entire LSN range falls below the new
    /// `base_ckpt` (those are now fully represented in the base manifest).
    ///
    /// Idempotent: with no eligible segments the call returns
    /// [`CompactionResult::NoNewSegments`] without changing any state.
    pub fn run_compaction(&self) -> Result<CompactionResult> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(CompactionResult::Skipped),
        };

        // Snapshot relevant fields under the read lock.
        let (base_ckpt, redo_ckpt, head_ckpt) = {
            let _guard = io_control.timeline.lock.read();
            (
                io_control.timeline.base_ckpt,
                io_control.timeline.redo_ckpt,
                io_control.timeline.head_ckpt,
            )
        };

        // Pick the upper bound. Once PG passes a real `CheckPoint.redo`
        // through, `redo_ckpt` becomes the natural ceiling. Until then it
        // is set equal to the latest commit, so use `head_ckpt` instead.
        let upper_ckpt = if redo_ckpt.lsn.as_u64() == 0 {
            head_ckpt
        } else {
            redo_ckpt
        };
        if upper_ckpt <= base_ckpt {
            return Ok(CompactionResult::NoNewSegments);
        }

        let segments = self.list_segments_in_range(base_ckpt, upper_ckpt)?;
        let mut to_apply: Vec<SegmentCheckpoint> = Vec::new();
        for sid in &segments {
            let seg = self.load_segment(sid)?;
            for sc in &seg.checkpoints {
                if sc.ckpt > base_ckpt && sc.ckpt < upper_ckpt {
                    to_apply.push(sc.clone());
                }
            }
        }

        if to_apply.is_empty() {
            return Ok(CompactionResult::NoNewSegments);
        }

        // Apply in ascending `Checkpoint` order — `(timeline_id, lsn)` —
        // so last-write-wins is correct across timeline transitions.
        to_apply.sort_by_key(|s| s.ckpt);

        // Merge chunks + relfork meta into the base manifest. Three-step
        // sequence ensures the locally-visible TIKM file is never ahead of
        // S3 — if the S3 PUT fails, the local TIKM stays at the old state.
        //
        //   1. `apply_segments`: pure compute; returns merged state + bytes.
        //   2. `storage.put`: publish the new base manifest to S3.
        //   3. `commit_applied`: atomically rewrite the local TIKM file and
        //      return a fresh `Manifest`. We swap it into `base_manifest`;
        //      existing `Arc<Manifest>` readers keep using the old file via
        //      their FD until they drop their `Arc`.
        let current = self.base_manifest()?;
        let new_base_ckpt = to_apply.last().unwrap().ckpt;
        let key = self.lctr.base_manifest(&new_base_ckpt);

        let applied = current.apply_segments(&to_apply)?;
        self.storage.put(&key, &applied.bytes)?;
        let new_manifest = Arc::new(current.commit_applied(applied)?);

        // Advance `base_ckpt` in shmem under the write lock.
        {
            let _write_guard = io_control.timeline.lock.write();
            if io_control.timeline.base_ckpt != base_ckpt {
                pg_log_warning(
                    "tiko: compaction raced; another compactor advanced base_ckpt".to_string(),
                );
                return Ok(CompactionResult::Raced);
            }
            io_control.timeline.set_base_ckpt(new_base_ckpt);
        }

        // Swap the fresh Manifest in so this process's next
        // `base_manifest()` call short-circuits instead of re-loading.
        *self.base_manifest.lock().unwrap() = new_manifest;

        // Delete segment files whose entire LSN range is now covered by the
        // base manifest. The segment that contains `new_base_ckpt` itself
        // straddles the boundary and is retained — it still has
        // checkpoints above `base_ckpt`. Comparison uses the derived
        // `SegmentId` Ord (timeline_id then index), so this correctly
        // catches superseded segments from older timelines.
        let new_base_seg = new_base_ckpt.to_segment_id();
        for sid in segments.iter().take_while(|s| **s < new_base_seg) {
            let seg_key = self.lctr.timeline_segment(sid);
            match self.storage.delete(&seg_key) {
                Ok(_) => {}
                Err(e) if e.is_not_found() => {}
                Err(e) => {
                    pg_log_warning(format!(
                        "tiko: failed to delete superseded segment {seg_key}: {e}",
                    ));
                }
            }
        }

        let count = to_apply.len();
        pg_log_debug1(format!(
            "tiko: compaction applied {count} segment checkpoint(s); {base_ckpt} → {new_base_ckpt}"
        ));
        Ok(CompactionResult::Applied {
            base_ckpt,
            new_base_ckpt,
            count,
        })
    }

    /// Populate the shmem [`TimelineState`] from existing on-storage
    /// segments. Idempotent — the first caller does the work, subsequent
    /// calls observe `hydrated` and return immediately.
    ///
    /// Called from `tiko_init` after `IoControl::init_or_attach` so that
    /// the first backend (typically the postmaster) hydrates before any
    /// other backend services a query.
    pub fn hydrate_timeline_state(&self) -> Result<()> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(()),
        };

        // Fast-path: someone else already hydrated.
        if io_control.timeline.hydrated.load(Ordering::Acquire) {
            return Ok(());
        }

        let _write_guard = io_control.timeline.lock.write();

        // Double-check under the lock — another process may have raced us
        // through the fast-path window.
        if io_control.timeline.hydrated.load(Ordering::Relaxed) {
            return Ok(());
        }

        // List every segment file across every timeline. The helper sorts
        // ascending by `(timeline_id, index)` (derived `SegmentId` Ord),
        // which is also the natural ordering of checkpoints because both
        // `timeline_id` and `lsn` are monotonic across PG's lifetime.
        let segment_ids = self.list_all_segments()?;

        // Collect most-recent ACTIVE_WINDOW_SIZE SegmentCheckpoints by
        // walking segments newest-first, then within each segment newest
        // checkpoint first. Stop once we have enough.
        let mut newest_first: Vec<SegmentCheckpoint> = Vec::new();
        'outer: for segment_id in segment_ids.iter().rev() {
            let seg = self.load_segment(segment_id)?;
            for sc in seg.checkpoints.iter().rev() {
                newest_first.push(sc.clone());
                if newest_first.len() >= ACTIVE_WINDOW_SIZE {
                    break 'outer;
                }
            }
        }

        // Replay oldest-first so the ring buffer ends up newest-at-front.
        for sc in newest_first.iter().rev() {
            io_control.timeline.push_active(
                sc.ckpt,
                sc.prev_ckpt,
                sc.chunks.iter().copied(),
                sc.relforks.iter().map(|(rf, meta)| (*rf, meta.clone())),
            );
        }

        // Recover base_ckpt from the loaded base manifest (if any). The
        // manifest carries its own `Checkpoint`. Fresh clusters (no
        // segments, no base) leave base_ckpt at default. Read from the
        // cached snapshot directly — this runs once at hydration and shmem
        // base_ckpt isn't yet populated, so we can't go through
        // `base_manifest()`.
        let base_ckpt = self.base_manifest.lock().unwrap().checkpoint();
        if base_ckpt != Checkpoint::default() {
            io_control.timeline.set_base_ckpt(base_ckpt);
        }

        if let Some(newest) = newest_first.first() {
            pg_log_info(format!(
                "tiko: hydrated timeline state: {} active checkpoint(s), head={}, base={}",
                newest_first.len(),
                newest.ckpt,
                io_control.timeline.base_ckpt,
            ));
        } else {
            pg_log_info("tiko: hydrated timeline state: no existing segments");
        }

        io_control.timeline.hydrated.store(true, Ordering::Release);
        Ok(())
    }

    /// List every checkpoint recorded across all timeline segment files,
    /// flattened and sorted ascending by `(created_at, ckpt)`. Read-only;
    /// used by `tiko_pitr list` to present recovery targets.
    pub fn list_checkpoints(&self) -> Result<Vec<CheckpointRow>> {
        let segment_ids = self.list_all_segments()?;
        let mut rows: Vec<CheckpointRow> = Vec::new();
        for sid in &segment_ids {
            let seg = self.load_segment(sid)?;
            for sc in &seg.checkpoints {
                rows.push(CheckpointRow {
                    ckpt: sc.ckpt,
                    redo_ckpt: sc.redo_ckpt,
                    created_at: sc.created_at,
                    n_chunks: sc.chunks.len(),
                });
            }
        }
        rows.sort_by_key(|r| (r.created_at, r.ckpt));
        Ok(rows)
    }

    /// Find the newest base manifest with `base_ckpt <= target` and return its
    /// `(checkpoint, pg_state)`. The manifest is materialised into a throwaway
    /// tempdir so the process's live `local_root` TIKM cache is not clobbered;
    /// `checkpoint` and `pg_state` are in-memory fields and stay valid after the
    /// tempdir is dropped.
    pub fn load_base_pg_state_at_or_before(
        &self,
        target: Checkpoint,
    ) -> Result<(Checkpoint, Vec<u8>)> {
        let prefix = self.lctr.bases_dir();
        let keys = self.storage_list_prefix(&prefix)?;
        let (_base_ckpt, key) = select_newest_base_at_or_before(&keys, &prefix, target)
            .ok_or_else(|| {
                Error::other(format!("no base manifest at or before checkpoint {target}"))
            })?;
        let bytes = self.storage_get(&key)?;
        let tmp = tempfile::tempdir()?;
        let manifest = Manifest::from_bytes(&bytes, tmp.path())?;
        Ok((manifest.checkpoint(), manifest.pg_state().to_vec()))
    }

    /// Compute the contiguous archived-WAL run `[w_lo, w_hi]` (absolute LSN) for
    /// `timeline`, reaching the highest archived segment. Lists `{ns}/wal/{tl}/`,
    /// classifies sealed segments vs partial chunks, and GETs the highest
    /// segment's last chunk for its byte length when that segment is partial.
    fn archived_wal_run(&self, timeline: TimelineId) -> Result<(u64, u64)> {
        let seg = XLOG_SEG_SIZE as u64;
        let prefix = self.lctr.wal_timeline_dir(timeline);
        let keys = match self.storage_list_prefix(&prefix) {
            Ok(k) => k,
            Err(e) if e.is_not_found() => Vec::new(),
            Err(e) => return Err(e),
        };

        struct Acc {
            sealed: bool,
            min_off: Option<usize>,
            max_off: Option<usize>,
        }
        let mut segs: std::collections::BTreeMap<u64, Acc> = std::collections::BTreeMap::new();
        for key in &keys {
            let Some((seg_no, off)) = parse_wal_key(key, &prefix) else {
                continue;
            };
            let acc = segs.entry(seg_no).or_insert(Acc {
                sealed: false,
                min_off: None,
                max_off: None,
            });
            match off {
                None => acc.sealed = true,
                Some(o) => {
                    acc.min_off = Some(acc.min_off.map_or(o, |m| m.min(o)));
                    acc.max_off = Some(acc.max_off.map_or(o, |m| m.max(o)));
                }
            }
        }
        let Some(&highest) = segs.keys().next_back() else {
            return Err(Error::other(
                "no archived WAL for timeline; nothing is recoverable yet",
            ));
        };

        let mut entries: Vec<SegEntry> = Vec::with_capacity(segs.len());
        for (&seg_no, acc) in &segs {
            if acc.sealed {
                // Sealed is authoritative even if leftover chunks exist.
                entries.push(SegEntry {
                    seg_no,
                    lo: seg_no * seg,
                    hi: (seg_no + 1) * seg,
                    full: true,
                });
            } else {
                let min_off = acc.min_off.unwrap_or(0);
                let lo = seg_no * seg + min_off as u64;
                // Only the highest segment's exact end matters; lower partial
                // segments never extend the run, so their `hi` is unused.
                let hi = if seg_no == highest {
                    let max_off = acc.max_off.unwrap_or(0);
                    let name = format!("{}{:016X}", timeline.to_hex(), seg_no);
                    let chunk_key = self.lctr.wal_chunk_key(timeline, &name, max_off);
                    let len = self.storage_get(&chunk_key)?.len();
                    seg_no * seg + max_off as u64 + len as u64
                } else {
                    lo
                };
                entries.push(SegEntry {
                    seg_no,
                    lo,
                    hi,
                    full: false,
                });
            }
        }

        wal_contiguous_run(&entries)
            .ok_or_else(|| Error::other("no archived WAL for timeline; nothing is recoverable yet"))
    }

    /// Compute the PITR-recoverable window bounded by archived-WAL coverage:
    /// `earliest` = the oldest base manifest whose recovery WAL is inside the
    /// contiguous archived run; `latest_lsn` = the end of that run. Errors with
    /// a clear message when nothing is recoverable yet.
    pub fn recovery_window(&self) -> Result<RecoveryWindow> {
        // 1. Timeline = newest checkpoint's timeline.
        let rows = self.list_checkpoints()?;
        let newest = rows
            .last()
            .ok_or_else(|| Error::other("no checkpoints found; nothing is recoverable yet"))?;
        let timeline = newest.ckpt.timeline_id;

        // 2. Contiguous archived-WAL run for this timeline.
        let (w_lo, w_hi) = self.archived_wal_run(timeline)?;

        // 3. Oldest usable base: ascending by checkpoint, first whose redo WAL
        //    is archived. Candidates are bases on this timeline with checkpoint
        //    within the run.
        let bases_prefix = self.lctr.bases_dir();
        let base_keys = self.storage_list_prefix(&bases_prefix)?;
        let mut candidates: Vec<(Checkpoint, String)> = base_keys
            .iter()
            .filter_map(|k| parse_base_manifest_ckpt(k, &bases_prefix).map(|c| (c, k.clone())))
            .filter(|(c, _)| {
                c.timeline_id == timeline && c.lsn.as_u64() >= w_lo && c.lsn.as_u64() <= w_hi
            })
            .collect();
        candidates.sort_by_key(|(c, _)| *c);

        let tmp = tempfile::tempdir()?;
        let mut chosen: Option<(Checkpoint, i64)> = None;
        // Ascending, stop at the first usable base. Worst case (all in-window
        // bases unusable) GETs every candidate; acceptable on this cold CLI path.
        for (ckpt, key) in &candidates {
            let bytes = self.storage_get(key)?;
            let base = Manifest::from_bytes(&bytes, tmp.path())?;
            if is_base_usable(ckpt.lsn.as_u64(), base.redo_ckpt().lsn.as_u64(), w_lo, w_hi) {
                chosen = Some((base.checkpoint(), base.timestamp()));
                break;
            }
        }
        let (earliest_ckpt, earliest_ts) = chosen.ok_or_else(|| {
            Error::other("no base manifest's WAL is archived; nothing is recoverable yet")
        })?;

        // 4. Latest: run end, and the newest checkpoint time within the run.
        //    If no checkpoint sits at/below w_hi the time window collapses to
        //    `earliest_ts` (never over-promises); the LSN window can still be
        //    wider than the time window in that edge.
        let latest_lsn = Lsn::new(w_hi);
        let latest_ts = rows
            .iter()
            .filter(|r| r.ckpt.lsn.as_u64() <= w_hi)
            .map(|r| r.created_at)
            .max()
            .unwrap_or(earliest_ts);

        Ok(RecoveryWindow {
            earliest_ts,
            earliest_ckpt,
            latest_ts,
            latest_lsn,
            timeline,
        })
    }

    /// Find the newest base manifest with `timestamp <= target_ts` on `timeline`
    /// and return its `(checkpoint, pg_state)`.
    ///
    /// Reads each base manifest's header to learn its timestamp (few base
    /// manifests; cold CLI path). Future optimization: range-GET the 48-byte
    /// TIKM header to avoid transferring the embedded `pg_state`.
    pub fn load_base_pg_state_before_time(
        &self,
        target_ts: i64,
        timeline: TimelineId,
    ) -> Result<(Checkpoint, Vec<u8>)> {
        let prefix = self.lctr.bases_dir();
        let keys = self.storage_list_prefix(&prefix)?;
        let tmp = tempfile::tempdir()?;

        let mut candidates: Vec<(i64, Checkpoint, String)> = Vec::new();
        for key in &keys {
            if parse_base_manifest_ckpt(key, &prefix).is_none() {
                continue;
            }
            let bytes = self.storage_get(key)?;
            let m = Manifest::from_bytes(&bytes, tmp.path())?;
            candidates.push((m.timestamp(), m.checkpoint(), key.clone()));
        }

        let (ckpt, key) =
            select_base_by_time(&candidates, target_ts, timeline).ok_or_else(|| {
                Error::other(format!(
                    "no base manifest at or before time {target_ts} on timeline {timeline}"
                ))
            })?;

        let bytes = self.storage_get(&key)?;
        let manifest = Manifest::from_bytes(&bytes, tmp.path())?;
        Ok((ckpt, manifest.pg_state().to_vec()))
    }

    /// Run the segment-based commit protocol — entry point called by the
    /// smgr checkpoint hook on every PG checkpoint.
    ///
    /// No-op if `IoControl` is unavailable (e.g. very early in startup).
    /// Otherwise:
    ///
    /// 1. `cache.flush_dirty()` — flush dirty chunks and relfork meta to
    ///    the storage layer via the normal read-lock path
    ///    ([`Store::patch_chunk`] / [`Store::put_meta`]). Runs before the
    ///    write lock below so it doesn't re-enter the timeline lock.
    /// 2. Acquire `timeline.lock.write()`. This is the fence: it blocks
    ///    until every in-flight reader (the flush above, plus any
    ///    concurrent backend evictions) has dropped its read lock.
    /// 3. Capture `prev_ckpt = head_ckpt` (path prefix for chunks written
    ///    during the interval ending at `commit_ckpt`) and set `redo_ckpt`.
    /// 4. Drain the cluster-wide shmem [`DraftBuffer`] (chunks + relforks
    ///    zones) plus its on-disk spill file. All backends record into this
    ///    one shared buffer, so the drain captures the full interval in a
    ///    single pass.
    /// 5. Build a `SegmentCheckpoint` from the drained state and append it
    ///    to the appropriate segment file via [`Store::commit_segment`].
    /// 6. `push_active(commit_ckpt, prev_ckpt, chunks, relforks)` updates
    ///    the active window, advances `head_ckpt`, and bumps `generation`.
    /// 7. Update the `DbMeta` JSON on storage to record the new checkpoint.
    /// 8. Drop the write guard implicitly at function exit.
    pub fn run_commit_protocol(
        &self,
        commit_ckpt: &Checkpoint,
        redo_ckpt: &Checkpoint,
        pg_state_bytes: &[u8],
    ) -> Result<()> {
        let io_control = match IoControl::try_get() {
            Some(c) => c,
            None => return Ok(()), // initdb / single-user — handled separately.
        };

        // 1. Flush dirty cache state under the normal read-lock path.
        //    `io_control` is non-None (early-returned above), so the cache
        //    is reachable.
        io_control.cache.flush_dirty()?;

        // 2. Acquire the write lock. Waits for all in-flight read-lock
        //    holders (the flush above, concurrent backend evictions) to
        //    drain.
        let _write_guard = io_control.timeline.lock.write();

        let prev_ckpt = io_control.timeline.head_ckpt;
        let timeline = &io_control.timeline;
        timeline.set_redo_ckpt(*redo_ckpt);

        // Drain the centralized shmem draft ring + its on-disk spill file.
        let drained = timeline.draft.drain(&self.draft_spill_path)?;
        let summary =
            self.commit_segment(*commit_ckpt, prev_ckpt, *redo_ckpt, drained, pg_state_bytes)?;

        timeline.push_active(
            *commit_ckpt,
            prev_ckpt,
            summary.chunks.iter().copied(),
            summary
                .relforks
                .iter()
                .map(|(rf, meta)| (*rf, meta.clone())),
        );

        // Update DbMeta JSON
        self.update_db_meta(commit_ckpt)?;

        pg_log_debug1(format!(
            "tiko: run_commit_protocol at {commit_ckpt}: prev={prev_ckpt} chunks={} relforks={}",
            summary.chunks.len(),
            summary.relforks.len(),
        ));

        Ok(())
    }
}

#[cfg(test)]
mod base_select_tests {
    use super::*;

    fn ckpt(tl: u32, lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(tl), Lsn::new(lsn))
    }

    #[test]
    fn parses_valid_key_and_rejects_others() {
        let prefix = "12/5/bases/";
        let key = "12/5/bases/00000001/0000000003000028.manifest";
        assert_eq!(
            parse_base_manifest_ckpt(key, prefix),
            Some(ckpt(1, 0x3000028))
        );

        assert_eq!(
            parse_base_manifest_ckpt("12/5/bases/00000001", prefix),
            None
        );
        assert_eq!(
            parse_base_manifest_ckpt("12/5/other/x.manifest", prefix),
            None
        );
        assert_eq!(
            parse_base_manifest_ckpt("12/5/bases/zz/0000000000000001.manifest", prefix),
            None
        );
    }

    #[test]
    fn selects_newest_at_or_before_target() {
        let prefix = "12/5/bases/";
        let keys = vec![
            "12/5/bases/00000001/0000000000001000.manifest".to_string(),
            "12/5/bases/00000001/0000000003000028.manifest".to_string(),
            "12/5/bases/00000001/0000000009000000.manifest".to_string(),
        ];
        // Target between the 2nd and 3rd base → pick the 2nd.
        let got = select_newest_base_at_or_before(&keys, prefix, ckpt(1, 0x5000000));
        assert_eq!(got.unwrap().0, ckpt(1, 0x3000028));

        // Target exactly on a base → inclusive pick.
        let got = select_newest_base_at_or_before(&keys, prefix, ckpt(1, 0x3000028));
        assert_eq!(got.unwrap().0, ckpt(1, 0x3000028));

        // Target before the earliest base → none.
        assert!(select_newest_base_at_or_before(&keys, prefix, ckpt(1, 0x10)).is_none());
    }

    #[test]
    fn selects_base_by_time_newest_before_target_on_timeline() {
        // (timestamp, checkpoint, key) candidates on timeline 1.
        let cands = vec![
            (100i64, ckpt(1, 0x1000), "k100".to_string()),
            (200i64, ckpt(1, 0x2000), "k200".to_string()),
            (300i64, ckpt(1, 0x3000), "k300".to_string()),
            // A base on a different timeline that must be ignored.
            (250i64, ckpt(2, 0x2500), "k250tl2".to_string()),
        ];
        let tl = TimelineId::new(1);

        // Target between 200 and 300 → pick the ts=200 base.
        let got = select_base_by_time(&cands, 250, tl).unwrap();
        assert_eq!(got, (ckpt(1, 0x2000), "k200".to_string()));

        // Exact match on a base timestamp → inclusive pick.
        let got = select_base_by_time(&cands, 200, tl).unwrap();
        assert_eq!(got.0, ckpt(1, 0x2000));

        // Target before the earliest base on this timeline → none.
        assert!(select_base_by_time(&cands, 50, tl).is_none());

        // The tl=2 base is never chosen for tl=1 even when its ts qualifies.
        let got = select_base_by_time(&cands, 260, tl).unwrap();
        assert_eq!(got.0, ckpt(1, 0x2000));
    }
}

#[cfg(test)]
mod wal_coverage_tests {
    use super::{SegEntry, is_base_usable, parse_wal_key, wal_contiguous_run};
    use pgsys::common::XLOG_SEG_SIZE;

    const SEG: u64 = XLOG_SEG_SIZE as u64;

    fn sealed(seg_no: u64) -> SegEntry {
        SegEntry {
            seg_no,
            lo: seg_no * SEG,
            hi: (seg_no + 1) * SEG,
            full: true,
        }
    }

    #[test]
    fn parse_wal_key_sealed_and_chunk() {
        let p = "12/34/wal/00000001/";
        assert_eq!(
            parse_wal_key("12/34/wal/00000001/000000010000000000000002", p),
            Some((2, None))
        );
        assert_eq!(
            parse_wal_key(
                "12/34/wal/00000001/000000010000000000000002.chunks/000000000001F898",
                p
            ),
            Some((2, Some(0x1F898)))
        );
        assert_eq!(parse_wal_key("12/34/wal/00000001/not-a-segment", p), None);
        assert_eq!(parse_wal_key("12/34/other/x", p), None);
    }

    #[test]
    fn contiguous_run_sealed_chain() {
        let entries = vec![sealed(0), sealed(1), sealed(2)];
        assert_eq!(wal_contiguous_run(&entries), Some((0, 3 * SEG)));
    }

    #[test]
    fn contiguous_run_partial_top_over_sealed() {
        let top = SegEntry {
            seg_no: 2,
            lo: 2 * SEG,
            hi: 2 * SEG + 0x500,
            full: false,
        };
        let entries = vec![sealed(0), sealed(1), top];
        assert_eq!(wal_contiguous_run(&entries), Some((0, 2 * SEG + 0x500)));
    }

    #[test]
    fn contiguous_run_midsegment_start_no_extend() {
        let top = SegEntry {
            seg_no: 2,
            lo: 2 * SEG + 0x1F898,
            hi: 2 * SEG + 0x5F898,
            full: false,
        };
        assert_eq!(
            wal_contiguous_run(&[top]),
            Some((2 * SEG + 0x1F898, 2 * SEG + 0x5F898))
        );
    }

    #[test]
    fn contiguous_run_gap_stops_walk() {
        let entries = vec![sealed(1), sealed(3)];
        assert_eq!(wal_contiguous_run(&entries), Some((3 * SEG, 4 * SEG)));
    }

    #[test]
    fn contiguous_run_empty() {
        assert_eq!(wal_contiguous_run(&[]), None);
    }

    #[test]
    fn base_usability() {
        assert!(is_base_usable(150, 120, 100, 200));
        assert!(!is_base_usable(150, 90, 100, 200));
        assert!(!is_base_usable(250, 120, 100, 200));
    }
}
