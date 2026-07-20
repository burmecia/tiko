//! `ChunkBackend` — the chunked storage engine behind [`BlockBackend`].
//!
//! Geometry: a volume is `size / chunk_size` immutable chunks (default
//! 1 MiB, per-volume 256..=4096 KiB power of two at create). All mutability
//! funnels through three places, in strict data-before-metadata order:
//!
//! 1. **dirty buffer** (memory, per volume): whole-chunk buffers keyed by
//!    chunk index. `write_at` splits into chunk-aligned pieces and updates
//!    these (sub-chunk writes read-modify-write the current image). Reads
//!    hit it first.
//! 2. **NVMe write journal** (data-dir): `flush()` appends every dirty
//!    chunk not already journaled at its current epoch to the current
//!    journal segment with one sequential write + fsync — the guest-fsync
//!    durability boundary. Replayed into the dirty buffer on open.
//! 3. **chunk store** (S3 Files mount): the daemon-wide flusher thread
//!    turns journaled dirty chunks into immutable chunk files (tmp+fsync+
//!    rename), then appends a map-delta journal file + fsync (chunk file
//!    fsynced BEFORE the map delta referencing it), then reclaims the
//!    rotated NVMe journal segment, folding the map journal periodically.
//!
//! Detach (`drain`) persists everything and folds the map; daemon SIGTERM
//! deliberately does NOT drain — leftover journal segments are replayed on
//! the next open (the tested crash path).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::backend::BlockBackend;
use crate::cache::{self, JournalRecord, ReadCache};
use crate::chunkstore::{ChunkId, ChunkStore, ZERO_ID, new_chunk_id};
use crate::map::{self, ChunkMap};

/// Max dirty bytes buffered per volume before writers wait for the flusher.
const DIRTY_CAP: u64 = 64 << 20;
/// Dirty level at which the flusher also drains un-journaled chunks.
const DRAIN_THRESHOLD: u64 = DIRTY_CAP / 2;
/// Fold the map journal after this many delta files.
const FOLD_EVERY_DELTAS: u32 = 8;

struct DirtyChunk {
    /// Volume-global write sequence at last modification (staleness check
    /// for the flusher's epoch-matched removal).
    epoch: u64,
    /// Full chunk_size image.
    data: Vec<u8>,
}

#[derive(Default)]
struct Inner {
    map: Option<ChunkMap>,
    dirty: HashMap<u64, DirtyChunk>,
    dirty_bytes: u64,
    /// chunk idx -> epoch last journaled (subset of dirty).
    journaled: HashMap<u64, u64>,
    /// Current NVMe journal segment seq (appends go here).
    journal_seg: u64,
    /// Next map-delta journal seq.
    map_delta_seq: u64,
    deltas_since_fold: u32,
    write_seq: u64,
}

/// Statistics for `GET /volumes/{id}`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChunkStats {
    /// Map generation (checkpoint counter; bumped by folds).
    pub generation: u64,
    /// Map epoch (single-attach lease counter).
    pub epoch: u64,
    /// Any data persisted (attach `formatted` signal).
    pub has_data: bool,
    /// Chunks in the dirty buffer.
    pub dirty_chunks: usize,
    /// Bytes in the dirty buffer.
    pub dirty_bytes: u64,
    /// Dirty chunks journaled but not yet in the store.
    pub journaled_chunks: usize,
    /// NVMe journal segment files present.
    pub journal_segments: usize,
    /// Read-cache entries.
    pub cache_entries: usize,
    /// Read-cache bytes used.
    pub cache_bytes: u64,
    /// Read-cache cap.
    pub cache_cap_bytes: u64,
}

/// A volume on the chunked engine.
pub struct ChunkBackend {
    vol_id: String,
    size: u64,
    chunk_size: u32,
    compress: bool,
    store: Arc<ChunkStore>,
    rcache: Arc<ReadCache>,
    jdir: PathBuf,
    inner: Mutex<Inner>,
    /// Writers wait here when the dirty buffer hits DIRTY_CAP.
    dirty_cv: Condvar,
    /// Serializes flush_cycle vs drain (flusher thread vs detach path).
    flush_lock: Mutex<()>,
}

impl ChunkBackend {
    /// Create a brand-new chunk volume on the store (dirs + all-holes map).
    pub fn create(
        store: Arc<ChunkStore>,
        vol_id: &str,
        size: u64,
        chunk_size: u32,
    ) -> io::Result<()> {
        store.create_volume(vol_id)?;
        let map = ChunkMap::new(size, chunk_size);
        if let Err(e) = map.write_atomic(vol_id, &store.map_path(vol_id)) {
            let _ = store.remove_volume(vol_id);
            return Err(e);
        }
        Ok(())
    }

    /// Open a volume: clean crash-leftover tmp chunks, load map + deltas,
    /// replay any leftover NVMe journal segments into the dirty buffer.
    pub fn open(
        store: Arc<ChunkStore>,
        rcache: Arc<ReadCache>,
        journal_root: &std::path::Path,
        vol_id: &str,
        size: u64,
        chunk_size: u32,
        compress: bool,
    ) -> io::Result<Self> {
        // Note: orphan `*.tmp` chunks in the pool are reaped by the GC
        // (grace-period protected), not on open — the pool is shared.
        let (map, max_delta_seq) =
            ChunkMap::load(&store.map_path(vol_id), &store.map_journal_dir(vol_id), size, chunk_size)?;
        if map.volume_size != size || map.chunk_size != chunk_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "map geometry {}x{} != registry {}x{}",
                    map.volume_size, map.chunk_size, size, chunk_size
                ),
            ));
        }

        let jdir = journal_root.join(vol_id);
        let nchunks = map.nchunks();
        let mut inner = Inner {
            map: Some(map),
            map_delta_seq: max_delta_seq + 1,
            ..Default::default()
        };
        let mut max_seg = 0;
        for (seq, path) in cache::list_segments(&jdir)? {
            let applied = cache::replay_segment(&path, |idx, data| {
                if idx < nchunks {
                    inner.write_seq += 1;
                    let epoch = inner.write_seq;
                    inner.dirty_bytes += data.len() as u64;
                    if let Some(old) = inner.dirty.insert(idx, DirtyChunk { epoch, data }) {
                        inner.dirty_bytes -= old.data.len() as u64;
                    }
                    inner.journaled.insert(idx, epoch);
                }
            })?;
            if applied > 0 {
                crate::metrics::add(&crate::metrics::JOURNAL_REPLAYS_TOTAL, applied as u64);
                tracing::info!(vol_id, seg = seq, applied, "replayed journal segment");
            }
            max_seg = max_seg.max(seq);
        }
        inner.journal_seg = max_seg;

        Ok(Self {
            vol_id: vol_id.to_string(),
            size,
            chunk_size,
            compress,
            store,
            rcache,
            jdir,
            inner: Mutex::new(inner),
            dirty_cv: Condvar::new(),
            flush_lock: Mutex::new(()),
        })
    }

    fn map_id(&self, inner: &Inner, idx: u64) -> ChunkId {
        inner.map.as_ref().expect("map").get(idx)
    }

    /// Fetch a whole chunk image (decompressed), using the read cache.
    /// ZERO_ID callers get zeros before reaching here.
    fn materialize(&self, id: ChunkId) -> io::Result<Vec<u8>> {
        if let Some(data) = self.rcache.get(&id) {
            return Ok(data);
        }
        let data = self.store.read_chunk(&id)?;
        crate::metrics::inc(&crate::metrics::CHUNKS_READ_TOTAL);
        self.rcache.insert(&id, &data);
        Ok(data)
    }

    /// Volume id.
    pub fn vol_id(&self) -> &str {
        &self.vol_id
    }

    /// Current map epoch (single-attach lease counter, Phase 3).
    pub fn epoch(&self) -> u64 {
        self.inner.lock().unwrap().map.as_ref().expect("map").epoch
    }

    /// Bump the map epoch and persist the map header. Called on each
    /// successful attach (single-attach lease bookkeeping). The full map is
    /// rewritten atomically; leftover deltas re-apply identically on load.
    pub fn bump_epoch(&self) -> io::Result<u64> {
        let epoch = {
            let mut inner = self.inner.lock().unwrap();
            let map = inner.map.as_mut().expect("map");
            map.epoch += 1;
            map.epoch
        };
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .as_ref()
            .expect("map")
            .write_atomic(&self.vol_id, &self.store.map_path(&self.vol_id))?;
        Ok(epoch)
    }

    /// Current map generation (checkpoint counter; bumped by folds).
    pub fn generation(&self) -> u64 {
        self.inner.lock().unwrap().map.as_ref().expect("map").generation
    }

    /// Whether any chunk index is backed by data (map has a non-hole id).
    /// This is the attach `formatted` signal — unlike `generation` it is
    /// true as soon as any delta applied, even before the next checkpoint.
    pub fn has_data(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .map
            .as_ref()
            .expect("map")
            .ids()
            .iter()
            .any(|id| *id != ZERO_ID)
    }

    /// All chunk ids currently referenced (map + dirty is irrelevant —
    /// dirty chunks get ids only when persisted; here: map ids), used to
    /// prune the read cache on volume delete.
    pub fn referenced_ids(&self) -> Vec<ChunkId> {
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .as_ref()
            .expect("map")
            .ids()
            .iter()
            .copied()
            .filter(|id| *id != ZERO_ID)
            .collect()
    }

    /// Stats snapshot.
    pub fn stats(&self) -> ChunkStats {
        let inner = self.inner.lock().unwrap();
        let (cache_entries, cache_bytes, cache_cap_bytes) = self.rcache.stats();
        let map = inner.map.as_ref().expect("map");
        ChunkStats {
            generation: map.generation,
            epoch: map.epoch,
            has_data: map.ids().iter().any(|id| *id != ZERO_ID),
            dirty_chunks: inner.dirty.len(),
            dirty_bytes: inner.dirty_bytes,
            journaled_chunks: inner.journaled.len(),
            journal_segments: cache::list_segments(&self.jdir).map(|v| v.len()).unwrap_or(0),
            cache_entries,
            cache_bytes,
            cache_cap_bytes,
        }
    }

    /// Flusher entry point: persist what is due. Returns true if it did
    /// work. Serialized against `drain` by `flush_lock`.
    pub fn flush_cycle(&self) -> io::Result<bool> {
        let _guard = self.flush_lock.lock().unwrap();

        // Phase A (locked): snapshot due chunks; rotate the journal segment
        // so appends during the (slow) store writes land in a new segment.
        let (drain_seg, pending) = {
            let mut inner = self.inner.lock().unwrap();
            let due_all = inner.dirty_bytes >= DRAIN_THRESHOLD;
            if inner.journaled.is_empty() && !due_all {
                return Ok(false);
            }
            let drain_seg = if inner.journaled.is_empty() {
                None // un-journaled pressure drain: no segment to reclaim
            } else {
                let seg = inner.journal_seg;
                inner.journal_seg += 1;
                Some(seg)
            };
            let indexes: Vec<u64> = if due_all {
                inner.dirty.keys().copied().collect()
            } else {
                inner.journaled.keys().copied().collect()
            };
            let pending: Vec<(u64, u64, Vec<u8>)> = indexes
                .into_iter()
                .filter_map(|idx| {
                    inner
                        .dirty
                        .get(&idx)
                        .map(|dc| (idx, dc.epoch, dc.data.clone()))
                })
                .collect();
            (drain_seg, pending)
        };
        if pending.is_empty() {
            return Ok(false);
        }

        // Phase B (unlocked): chunk files first — data before metadata.
        let mut deltas = Vec::with_capacity(pending.len());
        for (idx, epoch, data) in &pending {
            let id = new_chunk_id()?;
            self.store.write_chunk(&id, data, self.compress)?;
            crate::metrics::inc(&crate::metrics::CHUNKS_WRITTEN_TOTAL);
            deltas.push((*idx, *epoch, id));
        }

        // Phase C: map-delta journal file + fsync (references only chunks
        // already durable from Phase B).
        let delta_seq = {
            let mut inner = self.inner.lock().unwrap();
            let seq = inner.map_delta_seq;
            inner.map_delta_seq += 1;
            seq
        };
        let delta_records: Vec<map::Delta> = deltas.iter().map(|(i, _, id)| (*i, *id)).collect();
        map::write_delta_file(&self.store.map_journal_dir(&self.vol_id), delta_seq, &delta_records)?;

        // Phase D (locked): apply to the in-memory map, drop drained dirty
        // (only if not re-written since the snapshot), then reclaim the
        // rotated journal segment(s).
        {
            let mut inner = self.inner.lock().unwrap();
            for (idx, epoch, id) in deltas {
                inner.map.as_mut().expect("map").set(idx, id);
                if inner.journaled.get(&idx).is_some_and(|e| *e <= epoch) {
                    inner.journaled.remove(&idx);
                }
                if inner.dirty.get(&idx).is_some_and(|dc| dc.epoch == epoch) {
                    let dc = inner.dirty.remove(&idx).expect("dirty chunk");
                    inner.dirty_bytes -= dc.data.len() as u64;
                }
            }
            inner.deltas_since_fold += 1;
        }
        if let Some(seg) = drain_seg {
            for (seq, path) in cache::list_segments(&self.jdir)? {
                if seq <= seg {
                    let _ = std::fs::remove_file(path);
                }
            }
            let _ = crate::chunkstore::sync_dir(&self.jdir);
        }
        self.dirty_cv.notify_all();

        // Periodic checkpoint.
        let need_fold = self.inner.lock().unwrap().deltas_since_fold >= FOLD_EVERY_DELTAS;
        if need_fold {
            let mut inner = self.inner.lock().unwrap();
            let map = inner.map.as_mut().expect("map");
            map::fold(
                &self.vol_id,
                map,
                &self.store.map_path(&self.vol_id),
                &self.store.map_journal_dir(&self.vol_id),
            )?;
            inner.deltas_since_fold = 0;
        }
        Ok(true)
    }

    /// Detach path: persist ALL dirty chunks, fold the map, remove the
    /// NVMe journal. Waits out any in-flight flush cycle via `flush_lock`.
    pub fn drain(&self) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        self.drain_locked()?;
        tracing::info!(vol_id = %self.vol_id, "volume drained to chunk store");
        Ok(())
    }

    /// Snapshot path: drain (consistent point), then write the full folded
    /// map to `dst` — all under one `flush_lock` hold, so the snapshot is
    /// exactly the post-drain point even if the guest keeps writing.
    pub fn write_snapshot_map(&self, dst: &std::path::Path) -> io::Result<()> {
        let _guard = self.flush_lock.lock().unwrap();
        self.drain_locked()?;
        let inner = self.inner.lock().unwrap();
        inner.map.as_ref().expect("map").write_atomic(&self.vol_id, dst)?;
        tracing::info!(vol_id = %self.vol_id, dst = %dst.display(), "snapshot map written");
        Ok(())
    }

    /// Persist all dirty chunks + fold the map + reclaim the NVMe journal.
    /// Caller must hold `flush_lock`.
    fn drain_locked(&self) -> io::Result<()> {
        loop {
            let pending: Vec<(u64, u64, Vec<u8>)> = {
                let inner = self.inner.lock().unwrap();
                inner
                    .dirty
                    .iter()
                    .map(|(idx, dc)| (*idx, dc.epoch, dc.data.clone()))
                    .collect()
            };
            if pending.is_empty() {
                break;
            }
            let mut deltas = Vec::with_capacity(pending.len());
            for (idx, epoch, data) in &pending {
                let id = new_chunk_id()?;
                self.store.write_chunk(&id, data, self.compress)?;
                crate::metrics::inc(&crate::metrics::CHUNKS_WRITTEN_TOTAL);
                deltas.push((*idx, *epoch, id));
            }
            let delta_seq = {
                let mut inner = self.inner.lock().unwrap();
                let seq = inner.map_delta_seq;
                inner.map_delta_seq += 1;
                seq
            };
            let delta_records: Vec<map::Delta> = deltas.iter().map(|(i, _, id)| (*i, *id)).collect();
            map::write_delta_file(
                &self.store.map_journal_dir(&self.vol_id),
                delta_seq,
                &delta_records,
            )?;
            let mut inner = self.inner.lock().unwrap();
            for (idx, epoch, id) in deltas {
                if inner.dirty.get(&idx).is_some_and(|dc| dc.epoch == epoch) {
                    let dc = inner.dirty.remove(&idx).expect("dirty chunk");
                    inner.dirty_bytes -= dc.data.len() as u64;
                    inner.map.as_mut().expect("map").set(idx, id);
                }
            }
            inner.journaled.clear();
            self.dirty_cv.notify_all();
        }
        // Fold everything into the map and remove the NVMe journal.
        {
            let mut inner = self.inner.lock().unwrap();
            let map = inner.map.as_mut().expect("map");
            map::fold(
                &self.vol_id,
                map,
                &self.store.map_path(&self.vol_id),
                &self.store.map_journal_dir(&self.vol_id),
            )?;
            inner.deltas_since_fold = 0;
        }
        for (_, path) in cache::list_segments(&self.jdir)? {
            let _ = std::fs::remove_file(path);
        }
        if self.jdir.exists() {
            let _ = crate::chunkstore::sync_dir(&self.jdir);
        }
        Ok(())
    }
}

impl BlockBackend for ChunkBackend {
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let cs = self.chunk_size as u64;
        let mut done = 0usize;
        while done < buf.len() {
            let pos = off + done as u64;
            let idx = pos / cs;
            let start = (pos % cs) as usize;
            let len = ((cs as usize) - start).min(buf.len() - done);

            // Dirty first; then id (without holding the lock across the
            // store fetch — chunk files are immutable).
            let (dirty_data, id) = {
                let inner = self.inner.lock().unwrap();
                match inner.dirty.get(&idx) {
                    Some(dc) => (Some(dc.data.clone()), ZERO_ID),
                    None => (None, self.map_id(&inner, idx)),
                }
            };
            let dst = &mut buf[done..done + len];
            if let Some(data) = dirty_data {
                dst.copy_from_slice(&data[start..start + len]);
            } else if id == ZERO_ID {
                dst.fill(0);
            } else {
                let data = self.materialize(id)?;
                dst.copy_from_slice(&data[start..start + len]);
            }
            done += len;
        }
        Ok(buf.len())
    }

    fn write_at(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        let cs = self.chunk_size as u64;
        // Pass 1 (locked): which chunks need their current image fetched
        // for a sub-chunk RMW?
        let mut fetch: Vec<(u64, ChunkId)> = Vec::new();
        {
            let inner = self.inner.lock().unwrap();
            let mut done = 0usize;
            while done < buf.len() {
                let pos = off + done as u64;
                let idx = pos / cs;
                let start = (pos % cs) as usize;
                let len = ((cs as usize) - start).min(buf.len() - done);
                if len < cs as usize && !inner.dirty.contains_key(&idx) {
                    let id = self.map_id(&inner, idx);
                    if id != ZERO_ID {
                        fetch.push((idx, id));
                    }
                }
                done += len;
            }
        }
        // Fetch outside the lock (store IO).
        let mut bases: HashMap<u64, Vec<u8>> = HashMap::new();
        for (idx, id) in fetch {
            bases.insert(idx, self.materialize(id)?);
        }
        // Pass 2 (locked): apply. Re-check dirty — another writer may have
        // materialized the chunk while we fetched.
        let mut inner = self.inner.lock().unwrap();
        let mut done = 0usize;
        while done < buf.len() {
            let pos = off + done as u64;
            let idx = pos / cs;
            let start = (pos % cs) as usize;
            let len = ((cs as usize) - start).min(buf.len() - done);
            if !inner.dirty.contains_key(&idx) {
                let base = bases
                    .remove(&idx)
                    .unwrap_or_else(|| vec![0u8; cs as usize]);
                inner.write_seq += 1;
                let epoch = inner.write_seq;
                inner.dirty_bytes += base.len() as u64;
                inner.dirty.insert(idx, DirtyChunk { epoch, data: base });
            }
            inner.write_seq += 1;
            let epoch = inner.write_seq;
            let dc = inner.dirty.get_mut(&idx).expect("dirty chunk");
            dc.data[start..start + len].copy_from_slice(&buf[done..done + len]);
            dc.epoch = epoch;
            done += len;
        }
        // Backpressure: wait for the flusher when the buffer is full. (The
        // guest can always make progress: the flusher drains journaled
        // chunks and, above DRAIN_THRESHOLD, un-journaled ones too.)
        while inner.dirty_bytes >= DIRTY_CAP {
            inner = self
                .dirty_cv
                .wait_timeout(inner, Duration::from_secs(2))
                .unwrap()
                .0;
        }
        Ok(buf.len())
    }

    fn flush(&self) -> io::Result<()> {
        crate::metrics::inc(&crate::metrics::FLUSHES_TOTAL);
        let (seg, records) = {
            let mut inner = self.inner.lock().unwrap();
            let records: Vec<JournalRecord> = inner
                .dirty
                .iter()
                .filter(|(idx, dc)| {
                    inner.journaled.get(*idx).is_none_or(|e| *e < dc.epoch)
                })
                .map(|(idx, dc)| {
                    let (flags, payload) = if self.compress {
                        let c = zstd::bulk::compress(&dc.data, 3)?;
                        if c.len() < dc.data.len() { (1u8, c) } else { (0u8, dc.data.clone()) }
                    } else {
                        (0u8, dc.data.clone())
                    };
                    Ok(JournalRecord { chunk_idx: *idx, flags, payload })
                })
                .collect::<io::Result<Vec<_>>>()?;
            for r in &records {
                let epoch = inner.dirty[&r.chunk_idx].epoch;
                inner.journaled.insert(r.chunk_idx, epoch);
            }
            (inner.journal_seg, records)
        };
        cache::append_segment(&self.jdir, seg, &records)?;
        Ok(())
    }

    fn size(&self) -> u64 {
        self.size
    }
}

// ------------------------------------------------------------- the flusher

struct FlusherShared {
    state: Mutex<FlusherState>,
    cv: Condvar,
}

#[derive(Default)]
struct FlusherState {
    shutdown: bool,
    volumes: HashMap<String, Weak<ChunkBackend>>,
}

/// One daemon-wide background thread turning journaled dirty chunks into
/// chunkstore files + map deltas.
pub struct Flusher {
    shared: Arc<FlusherShared>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Flusher {
    /// Start the flusher with the given idle tick interval.
    pub fn start(interval: Duration) -> Arc<Self> {
        let shared = Arc::new(FlusherShared {
            state: Mutex::new(FlusherState::default()),
            cv: Condvar::new(),
        });
        let shared_t = shared.clone();
        let join = std::thread::Builder::new()
            .name("tikoblk-flusher".into())
            .spawn(move || loop {
                {
                    let st = shared_t.state.lock().unwrap();
                    let (st, _) = shared_t.cv.wait_timeout(st, interval).unwrap();
                    if st.shutdown {
                        break;
                    }
                }
                let vols: Vec<Arc<ChunkBackend>> = {
                    let mut st = shared_t.state.lock().unwrap();
                    st.volumes.retain(|_, w| w.strong_count() > 0);
                    st.volumes.values().filter_map(Weak::upgrade).collect()
                };
                for vol in vols {
                    if let Err(e) = vol.flush_cycle() {
                        tracing::error!(vol_id = %vol.vol_id(), error = %e, "flush cycle failed (will retry)");
                    }
                }
            })
            .expect("spawn flusher");
        Arc::new(Self {
            shared,
            join: Mutex::new(Some(join)),
        })
    }

    /// Track a live chunk volume.
    pub fn register(&self, be: &Arc<ChunkBackend>) {
        self.shared
            .state
            .lock()
            .unwrap()
            .volumes
            .insert(be.vol_id().to_string(), Arc::downgrade(be));
    }

    /// Stop tracking a volume (before drain/detach).
    pub fn deregister(&self, vol_id: &str) {
        self.shared.state.lock().unwrap().volumes.remove(vol_id);
    }

    /// Wake the flusher immediately (after flush()).
    pub fn kick(&self) {
        self.shared.cv.notify_all();
    }

    /// Stop the thread (daemon shutdown; draining is a per-volume concern).
    pub fn shutdown(&self) {
        self.shared.state.lock().unwrap().shutdown = true;
        self.shared.cv.notify_all();
        if let Some(join) = self.join.lock().unwrap().take() {
            let _ = join.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        dir: PathBuf,
        store: Arc<ChunkStore>,
        rcache: Arc<ReadCache>,
        journal_root: PathBuf,
    }

    fn fixture(tag: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!("tikoblk-chunk-{tag}-{}", std::process::id()));
        let store = Arc::new(ChunkStore::new(&dir.join("store")).unwrap());
        let rcache = Arc::new(ReadCache::new(&dir.join("cache"), 8 << 20).unwrap());
        let journal_root = dir.join("journal");
        Fixture { dir, store, rcache, journal_root }
    }

    fn open(fx: &Fixture, vol: &str, size: u64, cs: u32) -> ChunkBackend {
        ChunkBackend::open(
            fx.store.clone(),
            fx.rcache.clone(),
            &fx.journal_root,
            vol,
            size,
            cs,
            true,
        )
        .unwrap()
    }

    fn pattern(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
    }

    #[test]
    fn read_your_writes_and_subchunk_rmw() {
        let fx = fixture("ryw");
        let vol = "v";
        ChunkBackend::create(fx.store.clone(), vol, 4 << 20, 1 << 20).unwrap();
        let be = open(&fx, vol, 4 << 20, 1 << 20);

        // Holes read as zeros.
        let mut out = vec![0xFFu8; 4096];
        be.read_at(0, &mut out).unwrap();
        assert!(out.iter().all(|&b| b == 0));

        // Full-chunk write, read back (from dirty buffer).
        let d0 = pattern(3, 1 << 20);
        be.write_at(0, &d0).unwrap();
        let mut out = vec![0u8; 1 << 20];
        be.read_at(0, &mut out).unwrap();
        assert_eq!(out, d0);

        // Sub-chunk write into chunk 1 (RMW against a hole base).
        let patch = pattern(9, 1000);
        be.write_at((1 << 20) + 500, &patch).unwrap();
        let mut out = vec![0xAAu8; 2000];
        be.read_at((1 << 20) + 400, &mut out).unwrap();
        assert_eq!(&out[..100], &[0u8; 100]);
        assert_eq!(&out[100..1100], &patch[..]);

        // Read spanning two chunks (chunk 1 carries the patch at 500..1500).
        let mut out = vec![0u8; 8192];
        be.read_at((1 << 20) - 4096, &mut out).unwrap();
        assert_eq!(&out[..4096], &d0[(1 << 20) - 4096..]);
        assert_eq!(&out[4096..4096 + 500], &[0u8; 500]);
        assert_eq!(&out[4096 + 500..4096 + 1500], &patch[..]);
        std::fs::remove_dir_all(&fx.dir).ok();
    }

    #[test]
    fn drain_persists_and_reopen_reads_from_store() {
        let fx = fixture("drain");
        let vol = "v";
        ChunkBackend::create(fx.store.clone(), vol, 4 << 20, 1 << 20).unwrap();
        let d0 = pattern(5, 1 << 20);
        let d1 = pattern(6, 1 << 20);
        {
            let be = open(&fx, vol, 4 << 20, 1 << 20);
            be.write_at(0, &d0).unwrap();
            be.write_at(1 << 20, &d1).unwrap();
            be.flush().unwrap();
            be.drain().unwrap();
            assert!(be.generation() > 0, "checkpoint bumped generation");
        }
        // Reopen with a COLD read cache: data must come from chunk files.
        let rcache2 = Arc::new(ReadCache::new(&fx.dir.join("cache2"), 8 << 20).unwrap());
        let be = ChunkBackend::open(
            fx.store.clone(), rcache2, &fx.journal_root, vol, 4 << 20, 1 << 20, true,
        )
        .unwrap();
        let mut out = vec![0u8; 2 << 20];
        be.read_at(0, &mut out).unwrap();
        assert_eq!(&out[..1 << 20], &d0[..]);
        assert_eq!(&out[1 << 20..], &d1[..]);
        // Journal reclaimed by drain.
        assert!(cache::list_segments(&fx.journal_root.join(vol)).unwrap().is_empty());
        std::fs::remove_dir_all(&fx.dir).ok();
    }

    #[test]
    fn flush_then_crash_reopen_replays() {
        let fx = fixture("crash");
        let vol = "v";
        ChunkBackend::create(fx.store.clone(), vol, 4 << 20, 1 << 20).unwrap();
        let d0 = pattern(7, 1 << 20);
        {
            let be = open(&fx, vol, 4 << 20, 1 << 20);
            be.write_at(0, &d0).unwrap();
            be.flush().unwrap();
            // Simulate crash: drop WITHOUT drain. (mem::forget to be sure
            // no Drop-side drain ever sneaks in.)
            std::mem::forget(be);
        }
        let be = open(&fx, vol, 4 << 20, 1 << 20);
        let mut out = vec![0u8; 1 << 20];
        be.read_at(0, &mut out).unwrap();
        assert_eq!(out, d0, "journaled data survived the crash");
        // Replayed chunks are journaled; a flush cycle persists them.
        be.flush_cycle().unwrap();
        be.drain().unwrap();
        drop(be);
        let rcache2 = Arc::new(ReadCache::new(&fx.dir.join("cache2"), 8 << 20).unwrap());
        let be = ChunkBackend::open(
            fx.store.clone(), rcache2, &fx.journal_root, vol, 4 << 20, 1 << 20, true,
        )
        .unwrap();
        let mut out = vec![0u8; 1 << 20];
        be.read_at(0, &mut out).unwrap();
        assert_eq!(out, d0);
        std::fs::remove_dir_all(&fx.dir).ok();
    }

    #[test]
    fn write_without_flush_is_lost_but_never_torn() {
        let fx = fixture("torn");
        let vol = "v";
        ChunkBackend::create(fx.store.clone(), vol, 4 << 20, 1 << 20).unwrap();
        let d0 = pattern(11, 1 << 20);
        let d1 = pattern(12, 1 << 20);
        {
            let be = open(&fx, vol, 4 << 20, 1 << 20);
            be.write_at(0, &d0).unwrap();
            be.flush().unwrap();
            be.drain().unwrap(); // chunk 0 = d0 durable in store
            be.write_at(0, &d1).unwrap(); // overwrite WITHOUT flush
            std::mem::forget(be); // crash
        }
        let be = open(&fx, vol, 4 << 20, 1 << 20);
        let mut out = vec![0u8; 1 << 20];
        be.read_at(0, &mut out).unwrap();
        assert!(
            out == d0 || out == d1,
            "chunk is whole (old or new), never a mix"
        );
        assert_eq!(out, d0, "un-flushed write was not journaled");
        std::fs::remove_dir_all(&fx.dir).ok();
    }

    #[test]
    fn flusher_thread_drains_and_reclaims_journal() {
        let fx = fixture("flusher");
        let vol = "v";
        ChunkBackend::create(fx.store.clone(), vol, 4 << 20, 1 << 20).unwrap();
        let be = Arc::new(open(&fx, vol, 4 << 20, 1 << 20));
        let flusher = Flusher::start(Duration::from_millis(50));
        flusher.register(&be);

        let d0 = pattern(21, 1 << 20);
        be.write_at(0, &d0).unwrap();
        be.flush().unwrap();
        flusher.kick();
        // Wait for the background drain.
        for _ in 0..100 {
            if be.stats().dirty_chunks == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(be.stats().dirty_chunks, 0, "flusher drained the dirty buffer");
        assert!(
            cache::list_segments(&fx.journal_root.join(vol)).unwrap().is_empty(),
            "journal segment reclaimed"
        );
        flusher.shutdown();
        std::fs::remove_dir_all(&fx.dir).ok();
    }
}
