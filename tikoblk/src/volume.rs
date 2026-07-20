//! Volume manager: wires ublk devices to [`BlockBackend`]s.
//!
//! Each attached volume has one serve thread running `UblkCtrl::run_target`
//! (which spawns its own per-queue threads). The queue handler dispatches
//! READ/WRITE/FLUSH **synchronously** to the backend and completes the ublk
//! command immediately — no io_uring SQEs, no `-EAGAIN` dance. That is the
//! simple Phase 1 model; Phase 2 can move to async SQEs behind the same
//! trait.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use libublk::io::{BufDesc, BufDescList, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{UblkError, UblkIORes};

use crate::backend::{BlockBackend, FileBackend};
use crate::cache::ReadCache;
use crate::chunk::{ChunkBackend, Flusher};
use crate::chunkstore::ChunkStore;
use crate::device;
use crate::registry::{BackendKind, Registry, VolumeMeta, VolumeState};
use crate::{Error, Result};

/// How long to wait for a device to come live (or a serve thread to stop).
const READY_TIMEOUT: Duration = Duration::from_secs(15);

/// How long detach waits for the serve thread after kill_dev before
/// failing safe (leaving the volume attached) instead of cascading into
/// driver delete paths that can wedge.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Global sequence so concurrent attach/detach of the same volume are
/// serialized per-volume by the manager lock anyway; this is only for log
/// correlation.
static SERVE_SEQ: AtomicU64 = AtomicU64::new(0);

/// One live serve thread.
struct Attached {
    join: JoinHandle<()>,
    /// Present for chunk-backend volumes (drain on detach, stats, ids).
    chunk: Option<Arc<ChunkBackend>>,
    /// Single-attach lease: the flock'd `map.lock` fd, held for the attach
    /// lifetime (flock is released automatically on daemon death).
    lease: Option<std::fs::File>,
}

/// The volume manager. Owns the registry, the chunk store/read cache/
/// flusher, and the set of live serve threads.
pub struct VolumeManager {
    data_dir: PathBuf,
    registry: Mutex<Registry>,
    attached: Mutex<HashMap<String, Attached>>,
    store: Arc<ChunkStore>,
    rcache: Arc<ReadCache>,
    flusher: Arc<Flusher>,
    gc_grace: Duration,
    gc_shutdown: Arc<AtomicU64>,
}

/// Manager configuration (daemon flags funneled here).
#[derive(Debug, Clone)]
pub struct ManagerOpts {
    /// Read-cache capacity in bytes.
    pub cache_bytes: u64,
    /// Periodic GC interval in seconds (0 = manual only).
    pub gc_interval_secs: u64,
    /// GC grace period in seconds (young unreferenced chunks are kept).
    pub gc_grace_secs: u64,
}

impl Default for ManagerOpts {
    fn default() -> Self {
        Self {
            cache_bytes: 512 << 20,
            gc_interval_secs: 3600,
            gc_grace_secs: 600,
        }
    }
}

/// Result of a successful attach.
pub struct AttachInfo {
    /// Block device node to use, e.g. `/dev/ublkb3`.
    pub device: String,
    /// Whether the device already carries data (chunk backend: map
    /// generation > 0; file backend: always false — the caller mkfs's).
    pub formatted: bool,
}

/// Options for [`VolumeManager::create`].
#[derive(Debug, Clone)]
pub struct CreateOpts {
    /// Storage engine (default: file loop backend).
    pub backend: BackendKind,
    /// Chunk size in bytes for the chunk backend (256 KiB..=4 MiB, power of
    /// two; default 1 MiB).
    pub chunk_size: u32,
    /// Zero-copy clone source: (src_vol_id, snap_id).
    pub from_snapshot: Option<(String, String)>,
}

impl Default for CreateOpts {
    fn default() -> Self {
        Self {
            backend: BackendKind::File,
            chunk_size: 1 << 20,
            from_snapshot: None,
        }
    }
}

impl VolumeManager {
    /// Open the manager rooted at `data_dir`, with the chunk store at
    /// `store_root`. Spawns the periodic GC task when
    /// `opts.gc_interval_secs > 0` (single-daemon-per-store assumption, see
    /// gc.rs).
    pub fn new(data_dir: &Path, store_root: &Path, opts: &ManagerOpts) -> Result<Self> {
        std::fs::create_dir_all(data_dir.join("backing"))?;
        let registry = Registry::load(&data_dir.join("registry.json"))?;
        let store = Arc::new(ChunkStore::new(store_root)?);
        let rcache = Arc::new(ReadCache::new(&data_dir.join("cache"), opts.cache_bytes)?);
        let flusher = Flusher::start(Duration::from_secs(1));
        let gc_shutdown = Arc::new(AtomicU64::new(0));
        if opts.gc_interval_secs > 0 {
            let store_t = store.clone();
            let grace = Duration::from_secs(opts.gc_grace_secs);
            let interval = Duration::from_secs(opts.gc_interval_secs);
            let shutdown = gc_shutdown.clone();
            std::thread::Builder::new()
                .name("tikoblk-gc".into())
                .spawn(move || loop {
                    let deadline = std::time::Instant::now() + interval;
                    while shutdown.load(Ordering::Relaxed) == 0 {
                        if std::time::Instant::now() >= deadline {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    if shutdown.load(Ordering::Relaxed) != 0 {
                        break;
                    }
                    match crate::gc::run(&store_t, grace) {
                        Ok(s) if s.reclaimed_count > 0 || s.tmp_reaped > 0 => {
                            tracing::info!(reclaimed = s.reclaimed_count, bytes = s.reclaimed_bytes, tmp = s.tmp_reaped, "periodic gc pass")
                        }
                        Ok(_) => {}
                        Err(e) => tracing::error!(error = %e, "periodic gc failed"),
                    }
                })
                .map_err(Error::Io)?;
        }
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            registry: Mutex::new(registry),
            attached: Mutex::new(HashMap::new()),
            store,
            rcache,
            flusher,
            gc_grace: Duration::from_secs(opts.gc_grace_secs),
            gc_shutdown,
        })
    }

    fn backing_path(&self, vol_id: &str) -> PathBuf {
        self.data_dir.join("backing").join(format!("{vol_id}.img"))
    }

    fn journal_root(&self) -> PathBuf {
        self.data_dir.join("journal")
    }

    /// Create a volume and register it.
    ///
    /// `file` backend: preallocate the loop file; refuses when the data-dir
    /// filesystem has less than 2x `size_bytes` free. `chunk` backend:
    /// create the store volume skeleton + all-holes map (or a zero-copy
    /// clone when `opts.from_snapshot` is set); refuses when the data dir
    /// cannot hold journal+cache headroom.
    pub fn create(&self, vol_id: &str, size_bytes: u64, opts: CreateOpts) -> Result<VolumeMeta> {
        validate_vol_id(vol_id)?;
        let mut reg = self.registry.lock().unwrap();
        if reg.get(vol_id).is_some() {
            return Err(Error::AlreadyExists(vol_id.to_string()));
        }
        if opts.from_snapshot.is_some() && opts.backend != BackendKind::Chunk {
            return Err(Error::InvalidInput(
                "from_snapshot requires the chunk backend".into(),
            ));
        }

        let (backing, chunk_size, size_bytes) = match opts.backend {
            BackendKind::File => {
                if size_bytes == 0 || !size_bytes.is_multiple_of(512) {
                    return Err(Error::InvalidInput(
                        "size must be a positive multiple of 512".into(),
                    ));
                }
                check_free_space(&self.data_dir, size_bytes)?;
                let backing = self.backing_path(vol_id);
                FileBackend::create(&backing, size_bytes)?;
                (backing, None, size_bytes)
            }
            BackendKind::Chunk => {
                // Journal + read cache live on the data dir; require
                // comfortable headroom (chunks themselves are on the store).
                let (_, _, cache_cap) = self.rcache.stats();
                check_free_space(&self.data_dir, cache_cap + (256 << 20))?;
                match &opts.from_snapshot {
                    Some((src_vol, snap_id)) => {
                        let (cs, size) =
                            self.create_clone(vol_id, src_vol, snap_id, opts.chunk_size, size_bytes)?;
                        (self.store.vol_dir(vol_id), Some(cs), size)
                    }
                    None => {
                        let cs = opts.chunk_size;
                        if !(256 << 10..=4096 << 10).contains(&cs) || !cs.is_power_of_two() {
                            return Err(Error::InvalidInput(format!(
                                "chunk_size must be a power of two in 256 KiB..=4 MiB, got {cs}"
                            )));
                        }
                        if size_bytes == 0 || !size_bytes.is_multiple_of(cs as u64) {
                            return Err(Error::InvalidInput(format!(
                                "size {size_bytes} is not a multiple of chunk size {cs}"
                            )));
                        }
                        ChunkBackend::create(self.store.clone(), vol_id, size_bytes, cs)
                            .map_err(Error::Io)?;
                        (self.store.vol_dir(vol_id), Some(cs), size_bytes)
                    }
                }
            }
        };

        let meta = VolumeMeta {
            vol_id: vol_id.to_string(),
            dev_id: reg.alloc_dev_id(),
            size_bytes,
            backing_path: backing.clone(),
            state: VolumeState::Created,
            backend: opts.backend,
            chunk_size,
        };
        reg.insert(meta.clone());
        if let Err(e) = reg.save() {
            reg.remove(vol_id);
            match opts.backend {
                BackendKind::File => {
                    let _ = std::fs::remove_file(&backing);
                }
                BackendKind::Chunk => {
                    let _ = self.store.remove_volume(vol_id);
                }
            }
            return Err(e);
        }
        tracing::info!(%vol_id, dev_id = meta.dev_id, size_bytes, backend = ?opts.backend, "volume created");
        Ok(meta)
    }

    /// Zero-copy clone: new volume dir with a copy of the snapshot's map
    /// (same pool chunk ids, fresh epoch/generation). Returns
    /// (chunk_size, size_bytes) from the snapshot geometry.
    fn create_clone(
        &self,
        vol_id: &str,
        src_vol: &str,
        snap_id: &str,
        want_chunk_size: u32,
        want_size: u64,
    ) -> Result<(u32, u64)> {
        validate_vol_id(snap_id)?;
        let snap_map_path = self.store.snapshot_dir(src_vol, snap_id).join("map");
        let snap_map = match std::fs::read(&snap_map_path) {
            Ok(buf) => crate::map::ChunkMap::decode(&buf).map_err(Error::Io)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(format!("snapshot {src_vol}/{snap_id}")));
            }
            Err(e) => return Err(Error::Io(e)),
        };
        if want_chunk_size != 0 && want_chunk_size != snap_map.chunk_size {
            return Err(Error::InvalidInput(format!(
                "chunk_size override {} does not match snapshot {}",
                want_chunk_size, snap_map.chunk_size
            )));
        }
        if want_size != 0 && want_size != snap_map.volume_size {
            return Err(Error::InvalidInput(format!(
                "size override {} does not match snapshot {}",
                want_size, snap_map.volume_size
            )));
        }
        self.store.create_volume(vol_id).map_err(Error::Io)?;
        let clone_map = snap_map.for_clone();
        if let Err(e) = clone_map.write_atomic(vol_id, &self.store.map_path(vol_id)) {
            let _ = self.store.remove_volume(vol_id);
            return Err(Error::Io(e));
        }
        tracing::info!(%vol_id, %src_vol, %snap_id, "created zero-copy clone");
        Ok((snap_map.chunk_size, snap_map.volume_size))
    }

    /// Attach: create (or recover) the ublk device and start serving it.
    /// Idempotent: re-attaching a live volume returns its device path.
    pub fn attach(&self, vol_id: &str) -> Result<AttachInfo> {
        let meta = self
            .registry
            .lock()
            .unwrap()
            .get(vol_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(vol_id.to_string()))?;

        if let Some(att) = self.attached.lock().unwrap().get(vol_id) {
            // Already live in this daemon.
            return Ok(AttachInfo {
                device: device::bdev_path(meta.dev_id),
                formatted: att.chunk.as_ref().is_some_and(|c| c.has_data()),
            });
        }

        // Single-attach lease first: fail before touching any state.
        let lease = match meta.backend {
            BackendKind::Chunk => Some(self.acquire_lease(&meta.vol_id)?),
            BackendKind::File => None,
        };

        let opened = self.open_backend(&meta)?;
        let formatted = opened.formatted;
        let chunk_for_bump = opened.chunk.clone();
        let res = match meta.state {
            VolumeState::Created => self.attach_fresh(meta, opened, formatted, lease),
            // Registry says attached but this daemon isn't serving it:
            // recover if the device survived, else re-create it.
            VolumeState::Attached => self.attach_recover(meta, opened, formatted, lease),
        };
        if res.is_ok()
            && let Some(cb) = chunk_for_bump
        {
            // Lease bookkeeping: epoch advances on every successful attach.
            cb.bump_epoch().map_err(Error::Io)?;
        }
        res
    }

    /// Take the single-attach lease on a chunk volume: flock `map.lock`
    /// exclusively, non-blocking. The fd is the lease — closing it (or
    /// daemon death) releases the lock. Over NFSv4 advisory locks this is
    /// what stops a second host/daemon attaching the same volume.
    fn acquire_lease(&self, vol_id: &str) -> Result<std::fs::File> {
        let path = self.store.lock_path(vol_id);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(Error::Io)?;
        use std::os::unix::io::AsRawFd;
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Err(Error::Busy(format!("{vol_id} already attached (lease held)")));
            }
            return Err(Error::Io(e));
        }
        Ok(f)
    }

    /// Fresh `ADD_DEV` for a volume that has no kernel device.
    fn attach_fresh(
        &self,
        meta: VolumeMeta,
        opened: Opened,
        formatted: bool,
        lease: Option<std::fs::File>,
    ) -> Result<AttachInfo> {
        // Persist state=attached before serving: if we die now, the recovery
        // sweep on next start reattaches this device.
        self.set_state(&meta.vol_id, VolumeState::Attached)?;

        match self.spawn_serve(ServeMode::Add, &meta, opened, lease) {
            Ok(()) => Ok(AttachInfo {
                device: device::bdev_path(meta.dev_id),
                formatted,
            }),
            Err(e) => {
                let _ = device::delete_device(meta.dev_id);
                let _ = self.set_state(&meta.vol_id, VolumeState::Created);
                Err(e)
            }
        }
    }

    /// Reattach to a quiesced device, falling back to a fresh add with the
    /// same id when the kernel no longer has it (e.g. after a reboot).
    fn attach_recover(
        &self,
        meta: VolumeMeta,
        opened: Opened,
        formatted: bool,
        lease: Option<std::fs::File>,
    ) -> Result<AttachInfo> {
        match device::probe(meta.dev_id)? {
            Some(p) if p.is_quiesced() => {
                self.spawn_serve(ServeMode::Recover, &meta, opened, lease)?;
            }
            Some(p) if p.is_live() => {
                return Err(Error::InvalidState(format!(
                    "device {} is live but not served by this daemon",
                    meta.dev_id
                )));
            }
            Some(p) => {
                return Err(Error::InvalidState(format!(
                    "device {} in unexpected kernel state {}",
                    meta.dev_id, p.state
                )));
            }
            None => {
                tracing::info!(dev_id = meta.dev_id, "device gone; fresh ADD_DEV with same id");
                self.spawn_serve(ServeMode::Add, &meta, opened, lease)?;
            }
        }
        Ok(AttachInfo {
            device: device::bdev_path(meta.dev_id),
            formatted,
        })
    }

    /// Spawn the serve thread and wait until the device is live.
    ///
    /// The `UblkCtrl` handle is built INSIDE the serve thread: libublk keeps
    /// a thread-local control io_uring (initialized by the `UblkCtrl`
    /// constructor), so a handle built in one thread and driven from another
    /// panics ("Control ring not initialized") and aborts on drop.
    fn spawn_serve(
        &self,
        mode: ServeMode,
        meta: &VolumeMeta,
        opened: Opened,
        lease: Option<std::fs::File>,
    ) -> Result<()> {
        let (tx, rx) = mpsc::channel::<std::result::Result<u32, String>>();
        let vol_id = meta.vol_id.clone();
        let meta_t = meta.clone();
        let chunk = opened.chunk.clone();
        let backend = opened.backend.clone();
        let seq = SERVE_SEQ.fetch_add(1, Ordering::Relaxed);
        let join = std::thread::Builder::new()
            .name(format!("ublk-{}-{}", meta.vol_id, meta.dev_id))
            .spawn(move || {
                serve(mode, meta_t, backend, tx);
                tracing::info!(%vol_id, seq, "serve thread exited");
            })
            .map_err(Error::Io)?;

        match rx.recv_timeout(READY_TIMEOUT) {
            Ok(Ok(_dev_id)) => {
                self.attached
                    .lock()
                    .unwrap()
                    .insert(meta.vol_id.clone(), Attached { join, chunk, lease });
                device::relax_bdev_perms(meta.dev_id);
                Ok(())
            }
            Ok(Err(e)) => {
                let _ = join.join();
                Err(Error::Ublk(e))
            }
            Err(_) => {
                // Timeout: stop the half-started device; the serve thread's
                // run_target returns on kill_dev.
                let _ = device::delete_device(meta.dev_id);
                let _ = join.join();
                Err(Error::Timeout(format!(
                    "device {} did not come live within {READY_TIMEOUT:?}",
                    meta.dev_id
                )))
            }
        }
    }

    /// Detach: stop serving, drain (chunk backend) and delete the ublk
    /// device. Idempotent. Refuses with [`Error::Busy`] while the block
    /// device has holders.
    pub fn detach(&self, vol_id: &str) -> Result<()> {
        let meta = self
            .registry
            .lock()
            .unwrap()
            .get(vol_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(vol_id.to_string()))?;

        let att = self.attached.lock().unwrap().remove(vol_id);
        match (&meta.state, &att) {
            (VolumeState::Created, None) => return Ok(()), // already detached
            (VolumeState::Created, Some(_)) => {
                return Err(Error::InvalidState(format!(
                    "volume {vol_id} registered detached but has a live serve thread"
                )));
            }
            _ => {}
        }

        if device::device_busy(meta.dev_id) {
            // Put the handle back; nothing was stopped.
            if let Some(a) = att {
                self.attached.lock().unwrap().insert(vol_id.to_string(), a);
            }
            return Err(Error::Busy(vol_id.to_string()));
        }

        // kill_dev makes the serve thread's run_target() return; then drain
        // (chunk backend) and del_dev.
        let kill = libublk::ctrl::UblkCtrl::new_simple(meta.dev_id as i32)
            .and_then(|c| c.kill_dev());
        match kill {
            Ok(_) => {}
            Err(e) if device::is_device_gone(&e) => {}
            Err(e) => tracing::warn!(dev_id = meta.dev_id, error = %e, "kill_dev failed"),
        }
        if let Some(a) = att {
            // Bounded join: if the driver does not stop the queue promptly
            // (observed on ublk2 with a recently-active guest client — the
            // queue never wakes and kernel del_dev then wedges), do NOT
            // cascade into del_dev. Put the entry back (lease retained) and
            // fail; the operator can retry once the driver settles.
            let start = Instant::now();
            while !a.join.is_finished() {
                if start.elapsed() >= STOP_TIMEOUT {
                    self.attached.lock().unwrap().insert(vol_id.to_string(), a);
                    return Err(Error::Timeout(format!(
                        "serve thread for {vol_id} did not stop within {STOP_TIMEOUT:?}; device left attached"
                    )));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let Attached { join, chunk, lease } = a;
            let _ = join.join();
            tracing::debug!(dev_id = meta.dev_id, elapsed_ms = start.elapsed().as_millis(), "serve thread joined");
            // Clean-shutdown durability for the chunk engine: persist all
            // dirty chunks, fold the map, reclaim the NVMe journal — all
            // before the device node goes away.
            if let Some(cb) = chunk {
                self.flusher.deregister(vol_id);
                cb.drain().map_err(Error::Io)?;
            }
            drop(lease); // release the single-attach lease last
        }
        device::delete_device(meta.dev_id)?;
        self.set_state(vol_id, VolumeState::Created)?;
        tracing::info!(%vol_id, dev_id = meta.dev_id, "volume detached");
        Ok(())
    }

    /// Delete a volume entirely: detach if attached, then remove backing
    /// (loop file / store volume dir + NVMe journal) and the registry entry
    /// (freeing its device id). Pool chunks are never touched — they may be
    /// shared with clones/snapshots; the GC reclaims them. Refuses while
    /// snapshots exist (no implicit cascade).
    pub fn delete(&self, vol_id: &str) -> Result<()> {
        let meta = self
            .registry
            .lock()
            .unwrap()
            .get(vol_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(vol_id.to_string()))?;
        if meta.backend == BackendKind::Chunk {
            let snaps = self.store.list_snapshots(vol_id).map_err(Error::Io)?;
            if !snaps.is_empty() {
                return Err(Error::InvalidState(format!(
                    "volume {vol_id} has {} snapshot(s); delete them first",
                    snaps.len()
                )));
            }
        }
        if meta.state == VolumeState::Attached || self.attached.lock().unwrap().contains_key(vol_id)
        {
            self.detach(vol_id)?;
        }
        match meta.backend {
            BackendKind::File => match std::fs::remove_file(&meta.backing_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            },
            BackendKind::Chunk => {
                // Cache entries keyed by chunk id may be shared with clones;
                // leave them for LRU eviction (safe: ids are content-addressed
                // by construction and the GC owns pool reclamation).
                self.store.remove_volume(vol_id).map_err(Error::Io)?;
                let _ = std::fs::remove_dir_all(self.journal_root().join(vol_id));
            }
        }
        let mut reg = self.registry.lock().unwrap();
        reg.remove(vol_id);
        reg.save()?;
        tracing::info!(%vol_id, "volume deleted");
        Ok(())
    }

    // ---- snapshots (chunk backend only) ------------------------------------

    /// Take a COW snapshot: drain to a consistent point and freeze the map
    /// under `snapshots/<snap_id>/map`. Pool chunks are shared, so this is
    /// O(map size). Works attached (drains live state) or detached (state
    /// is already durable).
    pub fn snapshot(&self, vol_id: &str, name: Option<&str>) -> Result<String> {
        let meta = self.get(vol_id)?;
        if meta.backend != BackendKind::Chunk {
            return Err(Error::InvalidInput(
                "snapshots require the chunk backend".into(),
            ));
        }
        let snap_id = match name {
            Some(n) => {
                validate_vol_id(n)?;
                n.to_string()
            }
            None => utc_timestamp(),
        };
        let snap_dir = self.store.snapshot_dir(vol_id, &snap_id);
        if snap_dir.exists() {
            return Err(Error::AlreadyExists(format!("snapshot {vol_id}/{snap_id}")));
        }
        std::fs::create_dir_all(&snap_dir).map_err(Error::Io)?;
        let dst = snap_dir.join("map");
        // Live: drain + freeze under one flush_lock hold. Detached: the
        // detach path already drained+folded; copy the durable map.
        let live_chunk = self
            .attached
            .lock()
            .unwrap()
            .get(vol_id)
            .and_then(|a| a.chunk.clone());
        match live_chunk {
            Some(cb) => cb.write_snapshot_map(&dst).map_err(Error::Io)?,
            None => {
                crate::chunkstore::copy_file_synced(&self.store.map_path(vol_id), &dst)
                    .map_err(Error::Io)?;
            }
        }
        tracing::info!(%vol_id, %snap_id, "snapshot created");
        Ok(snap_id)
    }

    /// List snapshot ids of a volume.
    pub fn list_snapshots(&self, vol_id: &str) -> Result<Vec<String>> {
        let meta = self.get(vol_id)?;
        if meta.backend != BackendKind::Chunk {
            return Ok(Vec::new());
        }
        self.store.list_snapshots(vol_id).map_err(Error::Io)
    }

    /// Delete a snapshot (its frozen map; shared pool chunks are the GC's
    /// business).
    pub fn delete_snapshot(&self, vol_id: &str, snap_id: &str) -> Result<()> {
        let meta = self.get(vol_id)?;
        if meta.backend != BackendKind::Chunk {
            return Err(Error::InvalidInput(
                "snapshots require the chunk backend".into(),
            ));
        }
        let dir = self.store.snapshot_dir(vol_id, snap_id);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(format!("snapshot {vol_id}/{snap_id}")));
            }
            Err(e) => return Err(Error::Io(e)),
        }
        tracing::info!(%vol_id, %snap_id, "snapshot deleted");
        Ok(())
    }

    // ---- GC -----------------------------------------------------------------

    /// Run one mark-and-sweep pass over the store's chunk pool.
    pub fn gc_run(&self) -> Result<crate::gc::GcStats> {
        crate::gc::run(&self.store, self.gc_grace).map_err(Error::Io)
    }

    /// Per-volume metrics snapshot for `GET /metrics`.
    pub fn metrics_snapshot(&self) -> Vec<VolMetrics> {
        let att = self.attached.lock().unwrap();
        self.list()
            .into_iter()
            .map(|meta| {
                let live = att.get(&meta.vol_id).and_then(|a| a.chunk.as_ref());
                let (dirty_bytes, epoch) = match live {
                    Some(cb) => (cb.stats().dirty_bytes, cb.epoch()),
                    None => {
                        let epoch = if meta.backend == BackendKind::Chunk {
                            crate::map::ChunkMap::load(
                                &self.store.map_path(&meta.vol_id),
                                &self.store.map_journal_dir(&meta.vol_id),
                                meta.size_bytes,
                                meta.chunk_size.unwrap_or(1 << 20),
                            )
                            .map(|(m, _)| m.epoch)
                            .unwrap_or(0)
                        } else {
                            0
                        };
                        (0, epoch)
                    }
                };
                let journal_bytes = if meta.backend == BackendKind::Chunk {
                    crate::cache::journal_bytes(&self.journal_root().join(&meta.vol_id))
                } else {
                    0
                };
                VolMetrics {
                    vol_id: meta.vol_id,
                    backend: meta.backend,
                    size_bytes: meta.size_bytes,
                    dirty_bytes,
                    journal_bytes,
                    epoch,
                    attached: live.is_some(),
                }
            })
            .collect()
    }

    /// List all registered volumes.
    pub fn list(&self) -> Vec<VolumeMeta> {
        self.registry.lock().unwrap().list()
    }

    /// Get one volume's metadata.
    pub fn get(&self, vol_id: &str) -> Result<VolumeMeta> {
        self.registry
            .lock()
            .unwrap()
            .get(vol_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(vol_id.to_string()))
    }

    /// Detail view for `GET /volumes/{id}`: registry metadata plus backend
    /// specifics (generation, dirty/journal/cache stats for the chunk
    /// engine).
    pub fn volume_detail(&self, vol_id: &str) -> Result<serde_json::Value> {
        let meta = self.get(vol_id)?;
        let mut v = serde_json::to_value(&meta)
            .map_err(|e| Error::InvalidInput(format!("serialize meta: {e}")))?;
        if meta.backend == BackendKind::Chunk {
            let att = self.attached.lock().unwrap();
            if let Some(a) = att.get(vol_id)
                && let Some(cb) = &a.chunk
            {
                v["stats"] = serde_json::to_value(cb.stats())
                    .map_err(|e| Error::InvalidInput(format!("serialize stats: {e}")))?;
                v["generation"] = serde_json::json!(cb.generation());
                v["epoch"] = serde_json::json!(cb.epoch());
                v["has_data"] = serde_json::json!(cb.has_data());
            } else if let Ok((map, _)) = crate::map::ChunkMap::load(
                &self.store.map_path(vol_id),
                &self.store.map_journal_dir(vol_id),
                meta.size_bytes,
                meta.chunk_size.unwrap_or(1 << 20),
            ) {
                // Not live: report the durable state from the store.
                v["generation"] = serde_json::json!(map.generation);
                v["epoch"] = serde_json::json!(map.epoch);
                v["has_data"] =
                    serde_json::json!(map.ids().iter().any(|id| *id != crate::chunkstore::ZERO_ID));
            }
            drop(att);
            let snaps = self.store.list_snapshots(vol_id).unwrap_or_default();
            v["snapshots"] = serde_json::json!(snaps.len());
        }
        Ok(v)
    }

    /// Startup recovery sweep: for every volume whose device should be live,
    /// reattach (recovery path) or re-create with the same device id. This
    /// is what makes a killed/restarted daemon transparent: USER_RECOVERY
    /// quiesced devices resume serving from the same backing file, and the
    /// chunk engine replays leftover NVMe journal segments on open.
    pub fn recover_attached(&self) {
        let vols: Vec<VolumeMeta> = self
            .registry
            .lock()
            .unwrap()
            .list()
            .into_iter()
            .filter(|v| v.state == VolumeState::Attached)
            .collect();
        for meta in vols {
            match self.attach(&meta.vol_id) {
                Ok(_) => tracing::info!(vol_id = %meta.vol_id, dev_id = meta.dev_id, "recovered attached volume"),
                Err(e) => tracing::error!(vol_id = %meta.vol_id, dev_id = meta.dev_id, error = %e, "recovery failed; will retry on next start/attach"),
            }
        }
    }

    /// Open a volume's backend per its registry entry. For the chunk engine
    /// this replays leftover journal segments and registers the volume with
    /// the daemon flusher.
    fn open_backend(&self, meta: &VolumeMeta) -> Result<Opened> {
        match meta.backend {
            BackendKind::File => {
                let be = FileBackend::open(&meta.backing_path).map_err(|e| {
                    Error::Ublk(format!("open backing {}: {e}", meta.backing_path.display()))
                })?;
                if be.size() != meta.size_bytes {
                    return Err(Error::InvalidState(format!(
                        "backing {} size {} != registry size {}",
                        meta.backing_path.display(),
                        be.size(),
                        meta.size_bytes
                    )));
                }
                Ok(Opened {
                    backend: Arc::new(be),
                    chunk: None,
                    formatted: false,
                })
            }
            BackendKind::Chunk => {
                let cb = ChunkBackend::open(
                    self.store.clone(),
                    self.rcache.clone(),
                    &self.journal_root(),
                    &meta.vol_id,
                    meta.size_bytes,
                    meta.chunk_size.ok_or_else(|| {
                        Error::InvalidState(format!("chunk volume {} lacks chunk_size", meta.vol_id))
                    })?,
                    true,
                )
                .map_err(Error::Io)?;
                let cb = Arc::new(cb);
                self.flusher.register(&cb);
                let formatted = cb.has_data();
                Ok(Opened {
                    backend: cb.clone(),
                    chunk: Some(cb),
                    formatted,
                })
            }
        }
    }

    fn set_state(&self, vol_id: &str, state: VolumeState) -> Result<()> {
        let mut reg = self.registry.lock().unwrap();
        let meta = reg
            .get_mut(vol_id)
            .ok_or_else(|| Error::NotFound(vol_id.to_string()))?;
        meta.state = state;
        reg.save()
    }
}

/// Per-volume metrics row for `GET /metrics`.
#[derive(Debug, Clone)]
pub struct VolMetrics {
    /// Volume id.
    pub vol_id: String,
    /// Storage engine.
    pub backend: BackendKind,
    /// Volume size in bytes.
    pub size_bytes: u64,
    /// Dirty-buffer bytes (live volumes; 0 when detached).
    pub dirty_bytes: u64,
    /// NVMe journal bytes on disk.
    pub journal_bytes: u64,
    /// Map epoch (single-attach lease counter).
    pub epoch: u64,
    /// Served by this daemon right now.
    pub attached: bool,
}

/// A backend opened for attach, plus chunk-engine specifics.
struct Opened {
    backend: Arc<dyn BlockBackend>,
    chunk: Option<Arc<ChunkBackend>>,
    formatted: bool,
}

impl Drop for VolumeManager {
    fn drop(&mut self) {
        self.gc_shutdown.store(1, Ordering::Relaxed);
        self.flusher.shutdown();
    }
}

/// UTC timestamp snapshot id, e.g. `20260719T123045Z` (no chrono dep).
fn utc_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Civil-from-days (Howard Hinnant's algorithm).
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

fn validate_vol_id(vol_id: &str) -> Result<()> {
    let ok = !vol_id.is_empty()
        && vol_id.len() <= 64
        && vol_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!("invalid vol_id: {vol_id:?}")))
    }
}

/// Free-space guard: require >= 2x `size` available on the data-dir fs.
fn check_free_space(dir: &Path, size: u64) -> Result<()> {
    let c = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes())
        .map_err(|_| Error::InvalidInput("data dir path not representable".into()))?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let free = (st.f_bavail as u64) * (st.f_frsize as u64);
    let need = size.saturating_mul(2);
    if free < need {
        return Err(Error::InsufficientSpace { need, have: free });
    }
    Ok(())
}

// ---- serve loop (device <-> backend wiring) ------------------------------

/// How the serve thread obtains its device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeMode {
    /// Fresh ADD_DEV (with one orphan-cleanup retry).
    Add,
    /// START_USER_RECOVERY + RECOVER_DEV reattach to a quiesced device.
    Recover,
}

/// Entry point of one volume's serve thread; blocks in `run_target` until
/// the device is killed/deleted or the process exits.
fn serve(
    mode: ServeMode,
    meta: VolumeMeta,
    backend: Arc<dyn BlockBackend>,
    ready: mpsc::Sender<std::result::Result<u32, String>>,
) {
    // Build the control handle in THIS thread (see spawn_serve docs).
    let ctrl = match mode {
        ServeMode::Add => match device::build_add(&meta) {
            Ok(c) => Ok(c),
            Err(e) => {
                // A device with our id may be orphaned by an earlier crashed
                // daemon whose registry entry was lost. Clean it and retry
                // once.
                tracing::warn!(dev_id = meta.dev_id, error = %e, "ADD_DEV failed; cleaning orphan and retrying");
                let _ = device::delete_device(meta.dev_id);
                device::build_add(&meta)
            }
        },
        ServeMode::Recover => device::build_recover(meta.dev_id),
    };
    let ctrl = match ctrl {
        Ok(c) => c,
        Err(e) => {
            let _ = ready.send(Err(e.to_string()));
            return;
        }
    };

    let recovering = mode == ServeMode::Recover;
    let meta_tgt = meta.clone();
    let q_backend = backend.clone();
    let ready_dev = ready.clone();
    let res = ctrl.run_target(
        move |dev: &mut UblkDev| device::init_target(dev, &meta_tgt, recovering),
        move |qid, dev: &_| {
            if let Err(e) = q_loop(qid, dev, q_backend.clone()) {
                tracing::error!(qid, error = %e, "queue handler failed");
            }
        },
        move |c: &libublk::ctrl::UblkCtrl| {
            let id = c.dev_info().dev_id;
            device::ensure_node_links(id);
            tracing::info!(dev_id = id, cdev = %c.get_cdev_path(), bdev = %c.get_bdev_path(), "device live");
            let _ = ready_dev.send(Ok(id));
        },
    );
    if let Err(e) = res {
        tracing::error!(dev_id = meta.dev_id, error = %e, "run_target failed");
        // If the device never came live, tell the spawner.
        let _ = ready.send(Err(format!("run_target: {e}")));
    }
}

/// One queue's I/O loop: fetch commands and dispatch each to the backend.
fn q_loop(qid: u16, dev: &UblkDev, backend: Arc<dyn BlockBackend>) -> std::result::Result<(), UblkError> {
    let bufs = Rc::new(dev.alloc_queue_io_bufs());
    let bufs_h = bufs.clone();
    let io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let buf = &bufs_h[tag as usize];
        if let Err(e) = handle_io(q, tag, io, buf, backend.as_ref()) {
            tracing::error!(qid, tag, error = %e, "handle_io failed");
        }
    };

    UblkQueue::new(qid, dev)?
        .submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs)))?
        .wait_and_handle_io(io_handler);
    Ok(())
}

/// Dispatch one ublk I/O command to the backend and complete it.
///
/// Fresh commands from the driver have `io.is_tgt_io() == false`. We never
/// submit target SQEs, so a target-io CQE here is unexpected and ignored.
fn handle_io(
    q: &UblkQueue<'_>,
    tag: u16,
    io: &UblkIOCtx,
    buf: &libublk::helpers::IoBuf<u8>,
    backend: &dyn BlockBackend,
) -> std::result::Result<(), UblkError> {
    if io.is_tgt_io() {
        tracing::warn!(tag, "unexpected target-io CQE (we submit no SQEs)");
        return Ok(());
    }
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let len = (iod.nr_sectors << 9) as usize;
    debug_assert!(len <= buf.len());

    let res: i32 = match op {
        libublk::sys::UBLK_IO_OP_READ => {
            // SAFETY: this tag's io buffer is exclusively owned by the
            // in-flight command until we complete it below; no other party
            // reads or writes it concurrently.
            let dst = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len) };
            match backend.read_at(off, dst) {
                Ok(n) => n as i32,
                Err(e) => -errno(&e),
            }
        }
        libublk::sys::UBLK_IO_OP_WRITE => match backend.write_at(off, &buf[..len]) {
            Ok(n) => n as i32,
            Err(e) => -errno(&e),
        },
        libublk::sys::UBLK_IO_OP_FLUSH => match backend.flush() {
            Ok(()) => 0,
            Err(e) => -errno(&e),
        },
        // Discard/write-zeroes are not advertised in our params; succeed
        // silently if the kernel ever sends one anyway.
        libublk::sys::UBLK_IO_OP_DISCARD => 0,
        _ => -libc::EINVAL,
    };

    q.complete_io_cmd_unified(tag, BufDesc::Slice(buf), Ok(UblkIORes::Result(res)))
}

fn errno(e: &std::io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mgr_at(dir: &Path) -> VolumeManager {
        std::fs::create_dir_all(dir).unwrap();
        let opts = ManagerOpts {
            cache_bytes: 8 << 20,
            gc_interval_secs: 0,
            ..Default::default()
        };
        VolumeManager::new(dir, &dir.join("store"), &opts).unwrap()
    }

    #[test]
    fn create_list_get_delete_without_ublk() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm-{}", std::process::id()));
        let mgr = mgr_at(&dir);

        let m = mgr.create("v1", 16 << 20, CreateOpts::default()).unwrap();
        assert_eq!(m.dev_id, 1);
        assert_eq!(m.state, VolumeState::Created);
        assert_eq!(m.backend, BackendKind::File);
        assert!(mgr.backing_path("v1").exists());

        // Duplicate id rejected.
        assert!(matches!(
            mgr.create("v1", 16 << 20, CreateOpts::default()),
            Err(Error::AlreadyExists(_))
        ));
        // Bad ids rejected.
        assert!(matches!(
            mgr.create("../evil", 16 << 20, CreateOpts::default()),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            mgr.create("", 16 << 20, CreateOpts::default()),
            Err(Error::InvalidInput(_))
        ));

        assert_eq!(mgr.list().len(), 1);
        assert_eq!(mgr.get("v1").unwrap().dev_id, 1);
        assert!(matches!(mgr.get("nope"), Err(Error::NotFound(_))));

        // detach/delete of a never-attached volume.
        mgr.detach("v1").unwrap();
        mgr.delete("v1").unwrap();
        assert!(!mgr.backing_path("v1").exists());
        assert!(mgr.list().is_empty());
        assert!(matches!(mgr.delete("v1"), Err(Error::NotFound(_))));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn chunk_create_validation_and_delete_without_ublk() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm3-{}", std::process::id()));
        let mgr = mgr_at(&dir);
        let chunk_opts = CreateOpts {
            backend: BackendKind::Chunk,
            chunk_size: 1 << 20,
            from_snapshot: None,
        };

        let m = mgr.create("c1", 16 << 20, chunk_opts.clone()).unwrap();
        assert_eq!(m.backend, BackendKind::Chunk);
        assert_eq!(m.chunk_size, Some(1 << 20));
        assert!(mgr.store.vol_dir("c1").join("map").exists());
        // Fresh volume: generation 0 (attach would report formatted=false).
        let detail = mgr.volume_detail("c1").unwrap();
        assert_eq!(detail["backend"], "chunk");
        assert_eq!(detail["generation"], 0);
        assert_eq!(detail["has_data"], false);

        // Bad chunk sizes / geometry rejected.
        for (cs, size) in [
            (100u32, 16 << 20),          // not power of two
            (128 << 10, 16 << 20),       // below 256 KiB
            (8 << 20, 16 << 20),         // above 4 MiB
            (4 << 20, 18 << 20),         // size not multiple of chunk
        ] {
            assert!(matches!(
                mgr.create(
                    &format!("bad{cs}"),
                    size,
                    CreateOpts { backend: BackendKind::Chunk, chunk_size: cs, from_snapshot: None }
                ),
                Err(Error::InvalidInput(_))
            ));
        }

        // detach (no-op) + delete removes the store dir.
        mgr.detach("c1").unwrap();
        mgr.delete("c1").unwrap();
        assert!(!mgr.store.vol_dir("c1").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn dev_id_reserved_across_manager_restart() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm2-{}", std::process::id()));
        let id = {
            let mgr = mgr_at(&dir);
            mgr.create("a", 8 << 20, CreateOpts::default()).unwrap();
            mgr.create("b", 8 << 20, CreateOpts::default()).unwrap();
            mgr.delete("a").unwrap();
            mgr.create("c", 8 << 20, CreateOpts::default()).unwrap();
            mgr.get("c").unwrap().dev_id
        };
        assert_eq!(id, 1, "freed dev id is reused (lowest free >= 1)");
        // Registry survives a manager reload.
        let mgr = mgr_at(&dir);
        assert_eq!(mgr.list().len(), 2);
        assert_eq!(mgr.get("c").unwrap().dev_id, 1);
        assert_eq!(mgr.get("b").unwrap().dev_id, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn chunk_opts() -> CreateOpts {
        CreateOpts {
            backend: BackendKind::Chunk,
            chunk_size: 1 << 20,
            from_snapshot: None,
        }
    }

    /// Write `data` at `off` into a chunk volume without involving ublk.
    fn write_vol(mgr: &VolumeManager, vol: &str, off: u64, data: &[u8]) {
        let meta = mgr.get(vol).unwrap();
        let cb = crate::chunk::ChunkBackend::open(
            mgr.store.clone(),
            mgr.rcache.clone(),
            &mgr.journal_root(),
            vol,
            meta.size_bytes,
            meta.chunk_size.unwrap(),
            true,
        )
        .unwrap();
        use crate::backend::BlockBackend;
        cb.write_at(off, data).unwrap();
        cb.flush().unwrap();
        cb.drain().unwrap();
    }

    fn read_vol(mgr: &VolumeManager, vol: &str, off: u64, len: usize) -> Vec<u8> {
        let meta = mgr.get(vol).unwrap();
        let cb = crate::chunk::ChunkBackend::open(
            mgr.store.clone(),
            mgr.rcache.clone(),
            &mgr.journal_root(),
            vol,
            meta.size_bytes,
            meta.chunk_size.unwrap(),
            true,
        )
        .unwrap();
        use crate::backend::BlockBackend;
        let mut out = vec![0u8; len];
        cb.read_at(off, &mut out).unwrap();
        out
    }

    fn vol_map_ids(mgr: &VolumeManager, vol: &str) -> Vec<crate::chunkstore::ChunkId> {
        let meta = mgr.get(vol).unwrap();
        let (map, _) = crate::map::ChunkMap::load(
            &mgr.store.map_path(vol),
            &mgr.store.map_journal_dir(vol),
            meta.size_bytes,
            meta.chunk_size.unwrap(),
        )
        .unwrap();
        map.ids().to_vec()
    }

    #[test]
    fn snapshot_clone_zero_copy_and_isolation() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm4-{}", std::process::id()));
        let mgr = mgr_at(&dir);
        mgr.create("a", 4 << 20, chunk_opts()).unwrap();

        // Write P1, snapshot, overwrite with P2.
        let p1 = vec![0x11u8; 1 << 20];
        let p2 = vec![0x22u8; 1 << 20];
        write_vol(&mgr, "a", 0, &p1);
        let snap = mgr.snapshot("a", Some("s1")).unwrap();
        assert_eq!(snap, "s1");
        assert!(mgr.store.snapshot_dir("a", "s1").join("map").exists());
        let snap_ids = vol_map_ids(&mgr, "a");
        write_vol(&mgr, "a", 0, &p2);

        // Duplicate snapshot name -> 409-ish.
        assert!(matches!(
            mgr.snapshot("a", Some("s1")),
            Err(Error::AlreadyExists(_))
        ));

        // Clone from the snapshot: same chunk ids (zero copy), sees P1.
        let meta_b = mgr
            .create(
                "b",
                0,
                CreateOpts {
                    backend: BackendKind::Chunk,
                    chunk_size: 0,
                    from_snapshot: Some(("a".into(), "s1".into())),
                },
            )
            .unwrap();
        assert_eq!(meta_b.size_bytes, 4 << 20);
        assert_eq!(meta_b.chunk_size, Some(1 << 20));
        assert_eq!(vol_map_ids(&mgr, "b"), snap_ids, "clone shares chunk ids");
        assert_eq!(read_vol(&mgr, "b", 0, 1 << 20), p1, "clone reads snapshot point");
        assert_eq!(read_vol(&mgr, "a", 0, 1 << 20), p2, "origin moved on");

        // Geometry-override mismatches rejected.
        assert!(matches!(
            mgr.create(
                "b2",
                8 << 20,
                CreateOpts {
                    backend: BackendKind::Chunk,
                    chunk_size: 0,
                    from_snapshot: Some(("a".into(), "s1".into())),
                },
            ),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            mgr.create(
                "b3",
                0,
                CreateOpts {
                    backend: BackendKind::Chunk,
                    chunk_size: 2 << 20,
                    from_snapshot: Some(("a".into(), "s1".into())),
                },
            ),
            Err(Error::InvalidInput(_))
        ));

        // Snapshot listing + delete-with-snapshots 409 + cleanup.
        assert_eq!(mgr.list_snapshots("a").unwrap(), vec!["s1".to_string()]);
        let detail = mgr.volume_detail("a").unwrap();
        assert_eq!(detail["snapshots"], 1);
        assert!(matches!(mgr.delete("a"), Err(Error::InvalidState(_))));
        mgr.delete_snapshot("a", "s1").unwrap();
        assert!(matches!(mgr.delete_snapshot("a", "s1"), Err(Error::NotFound(_))));
        mgr.delete("a").unwrap();
        mgr.delete("b").unwrap();
        assert!(!mgr.store.vol_dir("a").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lease_conflict_two_managers_and_fork() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm5-{}", std::process::id()));
        let store = dir.join("store");
        let opts = || ManagerOpts {
            cache_bytes: 8 << 20,
            gc_interval_secs: 0,
            ..Default::default()
        };
        let mgr1 = VolumeManager::new(&dir.join("d1"), &store, &opts()).unwrap();
        let mgr2 = VolumeManager::new(&dir.join("d2"), &store, &opts()).unwrap();
        mgr1.create("v", 4 << 20, chunk_opts()).unwrap();

        let lease1 = mgr1.acquire_lease("v").unwrap();
        // Second manager (same process, own fd) conflicts.
        assert!(matches!(mgr2.acquire_lease("v"), Err(Error::Busy(_))));

        // Forked process with its own fd also conflicts (flock is per
        // open-file-description).
        use std::os::unix::io::AsRawFd;
        let lock_path = mgr1.store.lock_path("v");
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // Child: only async-signal-safe-ish ops, then _exit.
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .unwrap();
            let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            unsafe { libc::_exit(if rc == 0 { 0 } else { 1 }) };
        }
        assert!(pid > 0);
        let mut status = 0;
        let r = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(r, pid);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 1, "child must NOT acquire the held lease");

        // Releasing lets the second manager in.
        drop(lease1);
        let _lease2 = mgr2.acquire_lease("v").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn epoch_bump_persists() {
        let dir = std::env::temp_dir().join(format!("tikoblk-vm6-{}", std::process::id()));
        let mgr = mgr_at(&dir);
        mgr.create("v", 4 << 20, chunk_opts()).unwrap();
        let meta = mgr.get("v").unwrap();
        let cb = crate::chunk::ChunkBackend::open(
            mgr.store.clone(),
            mgr.rcache.clone(),
            &mgr.journal_root(),
            "v",
            meta.size_bytes,
            meta.chunk_size.unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(cb.epoch(), 0);
        assert_eq!(cb.bump_epoch().unwrap(), 1);
        assert_eq!(cb.bump_epoch().unwrap(), 2);
        drop(cb);
        // Re-read the durable map: epoch survived.
        let (map, _) = crate::map::ChunkMap::load(
            &mgr.store.map_path("v"),
            &mgr.store.map_journal_dir("v"),
            meta.size_bytes,
            meta.chunk_size.unwrap(),
        )
        .unwrap();
        assert_eq!(map.epoch, 2);
        std::fs::remove_dir_all(&dir).ok();
    }
}
