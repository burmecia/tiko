use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};

use crate::{
    chunk::ChunkTag,
    db::{DbMeta, DbNamespace},
    error::{Error, Result},
    io_control::IoControl,
    local_path,
    manifest::Manifest,
    relfork::{RelFork, RelForkMeta},
    storage::Storage,
    storage_root_path,
    timeline::draft::{DRAFT_SPILL_FILE_NAME, SpillFile},
    timeline::{ACTIVE_WINDOW_SIZE, Checkpoint, CheckpointSummary, SegmentId, TimelineSegment},
};
use pgsys::{
    common::data_dir_path,
    logging::{pg_log_debug1, pg_log_debug2, pg_log_info, pg_log_warning},
};

static STORE: OnceLock<Store> = OnceLock::new();

/// Top-level store object.
///
/// Holds a concrete `ObjectStore` backend (`S3Sim` or `S3`) and provides:
/// - The same primitive two-bucket operations via forwarding methods.
///   built entirely from `ObjectStore` primitives.
/// - A process-global singleton (`init` / `get` / `try_get`).
pub struct Store {
    pub(super) ns: DbNamespace,
    /// Current base-manifest snapshot. Readers grab an `Arc<Manifest>` under
    /// the `Mutex` (briefly) and use it lock-free. The `Manifest` is
    /// immutable; the compactor produces a fresh one via
    /// [`Manifest::commit_applied`] and swaps the `Arc` in here. Cross-process
    /// staleness is detected by comparing
    /// `IoControl::get().timeline.base_ckpt` to the current `Manifest`'s own
    /// [`Manifest::checkpoint`].
    pub(super) base_manifest: Mutex<Arc<Manifest>>,
    pub(super) storage: Storage,
    /// Local root path used to materialise the base-manifest TIKM cache file
    /// on reload (and the draft spill file). One per process.
    pub(super) local_root: PathBuf,
    /// On-disk overflow file for the centralized shmem [`DraftBuffer`].
    /// One per cluster at `{tiko_root}/draft.spill`.
    pub(super) draft_spill: SpillFile,
}

impl Store {
    pub fn namespace(&self) -> &DbNamespace {
        &self.ns
    }

    /// Update the DbMeta JSON object on storage with the latest checkpoint
    /// LSN. Internal helper called from [`Store::run_commit_protocol`].
    pub(super) fn update_db_meta(&self, ckpt: &Checkpoint) -> Result<()> {
        let db = DbMeta::new(self.ns.clone());
        let key = self.ns.db_meta();

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

        // Storage root is SHARED across databases (the remote object store);
        // local_root is PER-DATABASE (cache/state files that must not collide
        // between parent and branch).
        let storage_root = storage_root_path();
        let local_root = local_path();
        let ns = DbNamespace::new_from_env();
        let storage = Storage::new(&storage_root);

        // Local fast path: reuse the on-disk TIKM file if a previous
        // invocation (this process or a sibling) already published it. The
        // local file is kept up to date by the compactor's `commit_applied`
        // (atomic tmp + rename) and by S3-fallback reloads, so it's at worst
        // stale by one compaction cycle — the normal staleness check on
        // subsequent `base_manifest()` calls catches up.
        //
        // Falls back to an S3 list + GET if the local file is missing or
        // unreadable (fresh data dir, or after a local-path reset).
        let initial: Manifest = match Manifest::open_local(&local_root) {
            Ok(manifest) => {
                pg_log_debug2(format!(
                    "tiko: Store::init(): opened local base manifest at {}",
                    manifest.checkpoint()
                ));
                manifest
            }
            Err(_) => {
                let mut bases = storage.list_prefix(&ns.bases_dir())?;
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

        let draft_spill = SpillFile::new(local_root.join(DRAFT_SPILL_FILE_NAME));
        let store = Store {
            ns,
            base_manifest: Mutex::new(Arc::new(initial)),
            storage,
            local_root,
            draft_spill,
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

        let mut guard = self.base_manifest.lock().unwrap();
        if guard.checkpoint() != target {
            *guard = Arc::new(self.load_manifest_at(target)?);
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

        if let Ok(manifest) = Manifest::open_local(&self.local_root)
            && manifest.checkpoint() == ckpt
        {
            return Ok(manifest);
        }

        // S3 fallback. `Manifest::from_bytes` materialises the local TIKM
        // file as a side effect (also via tmp + rename inside `write_tikm`).
        let key = self.ns.base_manifest(&ckpt);
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
        STORE.get().ok_or(Error::StoreNotAvailable)
    }

    // ── Primitive forwarding methods ──────────────────────────────────────

    pub fn storage_put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.storage.put(key, data)?;
        if let Some(io_control) = IoControl::try_get() {
            io_control.stats.storage.inc_puts(data.len());
        }
        Ok(())
    }

    pub fn storage_get(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.storage.get(key)?;
        if let Some(io_control) = IoControl::try_get() {
            io_control.stats.storage.inc_gets(data.len());
        }
        Ok(data)
    }

    pub fn storage_list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let ret = self.storage.list_prefix(prefix)?;
        if let Some(io_control) = IoControl::try_get() {
            io_control.stats.storage.inc_lists();
        }
        Ok(ret)
    }

    pub fn storage_delete(&self, key: &str) -> Result<()> {
        self.storage.delete(key)?;
        if let Some(io_control) = IoControl::try_get() {
            io_control.stats.storage.inc_deletes();
        }
        Ok(())
    }

    // ── Backend draft (eviction-flush recording) ──────────────────────────

    pub(super) fn record_chunk_eviction(&self, tag: ChunkTag) {
        let Some(io_control) = IoControl::try_get() else {
            return;
        };
        if let Err(e) = io_control
            .timeline
            .draft
            .record_chunk(tag, &self.draft_spill)
        {
            pg_log_warning(format!("tiko: failed to record chunk eviction: {e}"));
        }
    }

    pub(super) fn record_relfork_eviction(&self, rf: RelFork, meta: RelForkMeta) {
        let Some(io_control) = IoControl::try_get() else {
            return;
        };
        if let Err(e) = io_control
            .timeline
            .draft
            .record_relfork(rf, meta, &self.draft_spill)
        {
            pg_log_warning(format!("tiko: failed to record relfork eviction: {e}"));
        }
    }

    // ── Commit protocol ──────────────────────────────────────────────────

    /// List every segment file under the timeline directory, parsed into
    /// `SegmentId`s and sorted ascending by `(timeline_id, index)` (the
    /// natural derived order). Returns an empty vec if the directory does
    /// not exist yet.
    pub(super) fn list_all_segments(&self) -> Result<Vec<SegmentId>> {
        let prefix = self.ns.timeline_segments_dir();
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
    pub(super) fn list_segments_in_range(
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
    pub(super) fn try_read_chunk_at(
        &self,
        tag: &ChunkTag,
        ckpt: &Checkpoint,
        dst: &mut [u8],
    ) -> Result<bool> {
        let key = self.ns.chunk(tag, ckpt);
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
    pub(super) fn load_segment(&self, segment_id: &SegmentId) -> Result<TimelineSegment> {
        let key = self.ns.timeline_segment(segment_id);
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
    pub(super) fn read_chunk_from_segments(
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
    pub(super) fn read_relfork_from_segments(
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
                    return Ok(Some(*meta));
                }
            }
        }
        Ok(None)
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

        // PITR recovery detection. The recovering postmaster is started by
        // `tiko_pitr recover`, which writes `recovery.signal` into PGDATA and
        // installs the backup's base manifest (at the backup LSN) as the live
        // TIKM. In that mode the installed manifest is the COMPLETE anchor
        // snapshot, so we must NOT populate the active window from on-storage
        // segments — those segments hold checkpoints ABOVE the backup LSN
        // (post-target "future" state) that would leak into reads after promote
        // (a cache miss would resolve a future chunk version instead of the
        // anchor version). Reads go through the base manifest; the active
        // window starts empty and fills only with the new timeline's
        // checkpoints after promote.
        let in_recovery = data_dir_path().join("recovery.signal").exists();

        if !in_recovery {
            // Normal startup: populate the active window with the newest
            // checkpoints so the read path can short-circuit segment scans.
            // `list_all_segments` sorts ascending by `(timeline_id, index)`,
            // the natural ordering of checkpoints.
            let segment_ids = self.list_all_segments()?;

            // Collect most-recent ACTIVE_WINDOW_SIZE CheckpointSummarys by
            // walking segments newest-first, then within each segment newest
            // checkpoint first. Stop once we have enough.
            let mut newest_first: Vec<CheckpointSummary> = Vec::new();
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
                    sc.relforks.iter().map(|(rf, meta)| (*rf, *meta)),
                );
            }

            if let Some(newest) = newest_first.first() {
                pg_log_info(format!(
                    "tiko: hydrated timeline state: {} active checkpoint(s), head={}",
                    newest_first.len(),
                    newest.ckpt,
                ));
            } else {
                pg_log_info("tiko: hydrated timeline state: no existing segments");
            }
        }

        // Recover base_ckpt from the loaded base manifest (if any). The
        // manifest carries its own `Checkpoint`. Fresh clusters (no segments,
        // no base) leave base_ckpt at default. Read from the cached snapshot
        // directly — this runs once at hydration and shmem base_ckpt isn't yet
        // populated, so we can't go through `base_manifest()`. In PITR recovery
        // this is the installed backup-L_b manifest, so base_ckpt = the anchor.
        let base_ckpt = self.base_manifest.lock().unwrap().checkpoint();
        if base_ckpt != Checkpoint::default() {
            io_control.timeline.set_base_ckpt(base_ckpt);
        }

        if in_recovery {
            pg_log_info(format!(
                "tiko: PITR recovery — active-window hydration skipped; reads anchor on base manifest at {base_ckpt}"
            ));
        }

        io_control.timeline.hydrated.store(true, Ordering::Release);
        Ok(())
    }
}
