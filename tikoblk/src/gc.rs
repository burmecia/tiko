//! Mark-and-sweep garbage collection for the shared chunk pool.
//!
//! Because chunks are immutable and clones share them, files under
//! `<store_root>/chunks/` accumulate: superseded images (every rewrite
//! makes a new chunk), orphans from crash windows (chunk file written,
//! daemon died before its map delta), deleted volumes' data. The live set
//! is the union of all chunk ids over:
//!
//! - every live volume's `map` + applied `map.journal/*.mj` deltas, and
//! - every `snapshots/<snap_id>/map`.
//!
//! Sweep deletes pool files NOT in the live set, but only when older than
//! a grace period (default 10 min): a chunk file is written *before* the
//! map delta that references it becomes visible, so a young unreferenced
//! file may simply be in flight. Orphan `*.tmp` files past the grace
//! period are reaped too.
//!
//! Correctness scope: this assumes a SINGLE daemon owns a given store
//! root (the same assumption as the rest of tikoblk — the per-volume
//! lease protects attach, not store-wide operations). It is not a
//! distributed GC.

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::chunkstore::{ChunkId, ChunkStore, ZERO_ID, id_from_hex, id_hex};
use crate::map::ChunkMap;

/// Outcome of one GC pass.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct GcStats {
    /// Pool files examined (chunks + tmp files).
    pub scanned: u64,
    /// Pool chunk files deleted.
    pub reclaimed_count: u64,
    /// Bytes deleted (chunk + tmp files).
    pub reclaimed_bytes: u64,
    /// Orphan `*.tmp` files deleted.
    pub tmp_reaped: u64,
    /// Live ids in the mark set.
    pub live_ids: u64,
}

/// Compute the live id set: all volume maps (+ unfolded deltas) and all
/// snapshot maps under the store root.
pub fn live_set(store: &ChunkStore) -> io::Result<HashSet<ChunkId>> {
    let mut live = HashSet::new();
    for vol_id in store.list_volumes()? {
        let vol_dir = store.vol_dir(&vol_id);
        let map_path = vol_dir.join("map");
        let journal_dir = vol_dir.join("map.journal");
        // Geometry comes from the map header itself. (A volume dir without
        // its base map is a failed-create leftover with no deltas yet.)
        if map_path.exists() {
            let (map, _) = ChunkMap::load(&map_path, &journal_dir, 0, 1)?;
            for id in map.ids() {
                if *id != ZERO_ID {
                    live.insert(*id);
                }
            }
        }
        // Snapshot maps.
        for snap_id in store.list_snapshots(&vol_id)? {
            let snap_map = store.snapshot_dir(&vol_id, &snap_id).join("map");
            match std::fs::read(&snap_map) {
                Ok(buf) => {
                    let map = ChunkMap::decode(&buf)?;
                    for id in map.ids() {
                        if *id != ZERO_ID {
                            live.insert(*id);
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(live)
}

/// One mark-and-sweep pass over the pool. Files younger than `grace` are
/// never touched.
pub fn run(store: &ChunkStore, grace: Duration) -> io::Result<GcStats> {
    let live = live_set(store)?;
    let mut stats = GcStats {
        live_ids: live.len() as u64,
        ..Default::default()
    };
    let cutoff = SystemTime::now()
        .checked_sub(grace)
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let pool = store.pool_dir();
    for shard1 in read_subdirs(&pool)? {
        for shard2 in read_subdirs(&shard1)? {
            let entries = match std::fs::read_dir(&shard2) {
                Ok(it) => it,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let md = match entry.metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let old_enough = md.modified().map(|t| t < cutoff).unwrap_or(false);
                stats.scanned += 1;

                if let Some(stem) = name.strip_suffix(".tmp") {
                    // Orphan tmp from a crashed chunk write.
                    if old_enough && id_from_hex(stem).is_some() {
                        let bytes = md.len();
                        if std::fs::remove_file(entry.path()).is_ok() {
                            stats.tmp_reaped += 1;
                            stats.reclaimed_bytes += bytes;
                        }
                    }
                    continue;
                }

                let Some(id) = id_from_hex(&name) else {
                    continue; // not a chunk file name
                };
                if live.contains(&id) || !old_enough {
                    continue;
                }
                let bytes = md.len();
                match store.delete_chunk(&id) {
                    Ok(()) => {
                        stats.reclaimed_count += 1;
                        stats.reclaimed_bytes += bytes;
                        tracing::debug!(id = id_hex(&id), "gc reclaimed chunk");
                    }
                    Err(e) => tracing::warn!(id = id_hex(&id), error = %e, "gc delete failed"),
                }
            }
        }
    }
    // fsync swept shard dirs is overkill; the pool root fsync covers us on
    // the next chunk write. Nothing else to do.
    crate::metrics::add(&crate::metrics::GC_RECLAIMED_BYTES_TOTAL, stats.reclaimed_bytes);
    Ok(stats)
}

fn read_subdirs(dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(it) => {
            for e in it.flatten() {
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    out.push(e.path());
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkstore::new_chunk_id;
    use crate::map::write_delta_file;
    use std::os::unix::fs::MetadataExt;

    struct Fx {
        dir: std::path::PathBuf,
        store: ChunkStore,
    }

    fn fixture(tag: &str) -> Fx {
        let dir = std::env::temp_dir().join(format!("tikoblk-gc-{tag}-{}", std::process::id()));
        let store = ChunkStore::new(&dir).unwrap();
        Fx { dir, store }
    }

    fn write_chunk(store: &ChunkStore, tag: u8) -> ChunkId {
        let id = new_chunk_id().unwrap();
        store.write_chunk(&id, &vec![tag; 4096], false).unwrap();
        id
    }

    fn make_map(store: &ChunkStore, vol: &str, ids: &[ChunkId]) {
        store.create_volume(vol).unwrap();
        let mut map = ChunkMap::new((ids.len().max(1) as u64) << 20, 1 << 20);
        for (i, id) in ids.iter().enumerate() {
            map.set(i as u64, *id);
        }
        map.write_atomic(vol, &store.map_path(vol)).unwrap();
    }

    fn age_file(path: &Path, secs: i64) {
        // Backdate mtime so the grace period passes (no utimens in std).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let t = now - secs;
        let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        let times = [
            libc::timespec { tv_sec: t, tv_nsec: 0 },
            libc::timespec { tv_sec: t, tv_nsec: 0 },
        ];
        let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(rc, 0);
        let got = std::fs::metadata(path).unwrap().mtime();
        assert!(got <= t + 1);
    }

    #[test]
    fn live_set_covers_maps_deltas_and_snapshots() {
        let fx = fixture("live");
        let a = write_chunk(&fx.store, 1);
        let b = write_chunk(&fx.store, 2);
        let c = write_chunk(&fx.store, 3);
        let d = write_chunk(&fx.store, 4);

        // Volume v: map references a; delta journal references b.
        fx.store.create_volume("v").unwrap();
        let mut map = ChunkMap::new(4 << 20, 1 << 20);
        map.set(0, a);
        map.write_atomic("v", &fx.store.map_path("v")).unwrap();
        write_delta_file(&fx.store.map_journal_dir("v"), 1, &[(1, b)]).unwrap();

        // Snapshot of v references c.
        crate::chunkstore::copy_file_synced(
            &fx.store.map_path("v"),
            &fx.store.snapshot_dir("v", "s1").join("map"),
        )
        .unwrap();
        let mut snap = ChunkMap::decode(&std::fs::read(fx.store.snapshot_dir("v", "s1").join("map")).unwrap()).unwrap();
        snap.set(2, c);
        snap.write_atomic("v", &fx.store.snapshot_dir("v", "s1").join("map")).unwrap();

        // d is unreferenced.
        let live = live_set(&fx.store).unwrap();
        assert!(live.contains(&a));
        assert!(live.contains(&b), "delta-journal ids are live");
        assert!(live.contains(&c), "snapshot ids are live");
        assert!(!live.contains(&d));
        std::fs::remove_dir_all(&fx.dir).ok();
    }

    #[test]
    fn sweep_respects_grace_and_reaps_tmp() {
        let fx = fixture("sweep");
        let referenced = write_chunk(&fx.store, 1);
        let dead_old = write_chunk(&fx.store, 2);
        let dead_young = write_chunk(&fx.store, 3);
        make_map(&fx.store, "v", &[referenced]);

        // Orphan tmp, backdated.
        let tmp_id = new_chunk_id().unwrap();
        let tmp_path = fx
            .store
            .chunk_path(&tmp_id)
            .with_extension("tmp");
        std::fs::create_dir_all(tmp_path.parent().unwrap()).unwrap();
        std::fs::write(&tmp_path, b"partial").unwrap();

        age_file(&fx.store.chunk_path(&dead_old), 3600);
        age_file(&tmp_path, 3600);

        let stats = run(&fx.store, Duration::from_secs(600)).unwrap();
        assert!(fx.store.chunk_path(&referenced).exists());
        assert!(!fx.store.chunk_path(&dead_old).exists(), "old unreferenced reclaimed");
        assert!(fx.store.chunk_path(&dead_young).exists(), "young unreferenced kept (grace)");
        assert!(!tmp_path.exists(), "old orphan tmp reaped");
        assert_eq!(stats.reclaimed_count, 1);
        assert_eq!(stats.tmp_reaped, 1);
        assert!(stats.scanned >= 4);

        // With grace=0 the young chunk goes too.
        let stats2 = run(&fx.store, Duration::from_secs(0)).unwrap();
        assert!(!fx.store.chunk_path(&dead_young).exists());
        assert_eq!(stats2.reclaimed_count, 1);
        std::fs::remove_dir_all(&fx.dir).ok();
    }
}
