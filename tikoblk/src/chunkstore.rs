//! Immutable file-per-chunk store with a **store-root-wide chunk pool**.
//!
//! Rooted at the daemon-level `--store-root` (production: an S3 Files NFS
//! mount, e.g. `/mnt/s3files/tikoblk`; tests: a local dir). Layout:
//!
//! ```text
//! <store_root>/
//!   chunks/ab/cd/<id>        # shared pool: immutable chunks, sharded by
//!                            #   first 4 hex chars of the 128-bit id
//!   volumes/<vol_id>/
//!     map                    # see map.rs
//!     map.journal/           # see map.rs
//!     map.lock               # single-attach lease (flock, see volume.rs)
//!     snapshots/<snap_id>/map   # frozen COW snapshot maps
//! ```
//!
//! Chunks are immutable, content-independent blobs addressed by a random
//! 128-bit id, so ANY map may reference ANY pool chunk — that is what makes
//! zero-copy COW clones possible. Write = `<id>.tmp` + fsync + atomic
//! rename + fsync parent dir (a reader never sees a partial chunk; NFS
//! rename is atomic). The zero id is reserved as the sparse-hole marker.
//! Reclaiming unreferenced pool chunks is the GC's job (gc.rs), never the
//! volume paths'.
//!
//! On-disk chunk format: 1 flag byte + payload. Flag bit 0 = payload is
//! zstd-compressed. Mixed compressed/raw chunks are fine.

use std::io;
use std::path::{Path, PathBuf};

/// Chunk id: 128-bit random. All-zero is the reserved "hole" id.
pub type ChunkId = [u8; 16];

/// The reserved hole id: reads of a hole return zeros, no fetch happens.
pub const ZERO_ID: ChunkId = [0u8; 16];

/// Chunk file flag: payload is zstd-compressed.
pub const FLAG_ZSTD: u8 = 1;

/// The chunk store root.
pub struct ChunkStore {
    root: PathBuf,
}

/// Generate a fresh random non-zero chunk id (from /dev/urandom).
pub fn new_chunk_id() -> io::Result<ChunkId> {
    let mut id = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut id)?;
    if id == ZERO_ID { id[0] = 1; }
    Ok(id)
}

/// Lowercase hex of a chunk id (32 chars; shard key is the first 4).
pub fn id_hex(id: &ChunkId) -> String {
    let mut s = String::with_capacity(32);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parse a 32-char hex string back into a chunk id.
pub fn id_from_hex(s: &str) -> Option<ChunkId> {
    if s.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(id)
}

use std::io::Read as _;

impl ChunkStore {
    /// Open (creating if needed) the store at `root`.
    pub fn new(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root.join("volumes"))?;
        std::fs::create_dir_all(root.join("chunks"))?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Root directory of the store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Per-volume directory.
    pub fn vol_dir(&self, vol_id: &str) -> PathBuf {
        self.root.join("volumes").join(vol_id)
    }

    /// Store-root-wide chunk pool directory.
    pub fn pool_dir(&self) -> PathBuf {
        self.root.join("chunks")
    }

    /// `map.journal/` dir of a volume.
    pub fn map_journal_dir(&self, vol_id: &str) -> PathBuf {
        self.vol_dir(vol_id).join("map.journal")
    }

    /// `map` file of a volume.
    pub fn map_path(&self, vol_id: &str) -> PathBuf {
        self.vol_dir(vol_id).join("map")
    }

    /// `map.lock` lease file of a volume.
    pub fn lock_path(&self, vol_id: &str) -> PathBuf {
        self.vol_dir(vol_id).join("map.lock")
    }

    /// `snapshots/` dir of a volume.
    pub fn snapshots_dir(&self, vol_id: &str) -> PathBuf {
        self.vol_dir(vol_id).join("snapshots")
    }

    /// Directory of one snapshot (contains its `map`).
    pub fn snapshot_dir(&self, vol_id: &str, snap_id: &str) -> PathBuf {
        self.snapshots_dir(vol_id).join(snap_id)
    }

    /// Path of a pool chunk file (sharded).
    pub fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        let hex = id_hex(id);
        self.pool_dir().join(&hex[..2]).join(&hex[2..4]).join(&hex)
    }

    /// Volume ids present in the store (GC mark phase, snapshot listing).
    pub fn list_volumes(&self) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        match std::fs::read_dir(self.root.join("volumes")) {
            Ok(it) => {
                for e in it.flatten() {
                    if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        out.push(e.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        out.sort();
        Ok(out)
    }

    /// Snapshot ids of a volume (dirs containing a `map` file).
    pub fn list_snapshots(&self, vol_id: &str) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        match std::fs::read_dir(self.snapshots_dir(vol_id)) {
            Ok(it) => {
                for e in it.flatten() {
                    if e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                        && e.path().join("map").exists()
                    {
                        out.push(e.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        out.sort();
        Ok(out)
    }

    /// Create the on-store directory skeleton for a new volume.
    pub fn create_volume(&self, vol_id: &str) -> io::Result<()> {
        let dir = self.vol_dir(vol_id);
        if dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("store volume dir exists: {}", dir.display()),
            ));
        }
        std::fs::create_dir_all(self.map_journal_dir(vol_id))?;
        std::fs::create_dir_all(self.snapshots_dir(vol_id))?;
        std::fs::File::create(dir.join("map.lock"))?;
        sync_dir(&dir)?;
        Ok(())
    }

    /// Remove a volume's entire store directory. Never touches the shared
    /// chunk pool (unreferenced chunks are the GC's job).
    pub fn remove_volume(&self, vol_id: &str) -> io::Result<()> {
        let dir = self.vol_dir(vol_id);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        sync_dir(&self.root.join("volumes"))?;
        Ok(())
    }

    /// Write a chunk immutably into the pool: tmp file + fsync + rename +
    /// fsync dir. `data` is the full (decompressed) chunk payload; the raw
    /// form is kept when zstd does not shrink it.
    pub fn write_chunk(&self, id: &ChunkId, data: &[u8], compress: bool) -> io::Result<()> {
        let mut blob = Vec::with_capacity(data.len() + 1);
        let compressed = compress && {
            let c = zstd::bulk::compress(data, 3)?;
            if c.len() < data.len() {
                blob.push(FLAG_ZSTD);
                blob.extend_from_slice(&c);
                true
            } else {
                false
            }
        };
        if !compressed {
            blob.push(0);
            blob.extend_from_slice(data);
        }

        let final_path = self.chunk_path(id);
        let parent = final_path.parent().expect("shard dir").to_path_buf();
        std::fs::create_dir_all(&parent)?;
        let tmp_path = parent.join(format!("{}.tmp", id_hex(id)));
        std::fs::write(&tmp_path, &blob)?;
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp_path, &final_path)?;
        sync_dir(&parent)?;
        Ok(())
    }

    /// Read and (if flagged) decompress a pool chunk.
    pub fn read_chunk(&self, id: &ChunkId) -> io::Result<Vec<u8>> {
        debug_assert_ne!(*id, ZERO_ID, "holes must be handled by the caller");
        let blob = std::fs::read(self.chunk_path(id))?;
        if blob.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty chunk file"));
        }
        let (flags, payload) = (blob[0], &blob[1..]);
        if flags & FLAG_ZSTD != 0 {
            zstd::decode_all(payload)
        } else {
            Ok(payload.to_vec())
        }
    }

    /// Delete a pool chunk file (GC only).
    pub fn delete_chunk(&self, id: &ChunkId) -> io::Result<()> {
        match std::fs::remove_file(self.chunk_path(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// fsync a directory (durability of entries created/renamed inside it).
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    let f = std::fs::File::open(dir)?;
    f.sync_all()
}

/// Copy a file with fsync + fsync of the parent dir (snapshot map copies).
pub fn copy_file_synced(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dst.parent().expect("parent"))?;
    std::fs::copy(src, dst)?;
    let f = std::fs::File::open(dst)?;
    f.sync_all()?;
    drop(f);
    sync_dir(dst.parent().expect("parent"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_at(tag: &str) -> (PathBuf, ChunkStore) {
        let dir = std::env::temp_dir().join(format!("tikoblk-cs-{tag}-{}", std::process::id()));
        let store = ChunkStore::new(&dir).unwrap();
        (dir, store)
    }

    #[test]
    fn chunk_write_read_roundtrip_raw_and_zstd() {
        let (dir, store) = store_at("rw");
        let id = new_chunk_id().unwrap();

        // Incompressible data: stored raw even with compress=true.
        let mut random = vec![0u8; 1 << 20];
        std::fs::File::open("/dev/urandom")
            .unwrap()
            .read_exact(&mut random)
            .unwrap();
        store.write_chunk(&id, &random, true).unwrap();
        let raw = std::fs::read(store.chunk_path(&id)).unwrap();
        assert_eq!(raw[0] & FLAG_ZSTD, 0, "random data should stay raw");
        assert_eq!(store.read_chunk(&id).unwrap(), random);

        // Compressible data: stored compressed, reads back identical.
        let id2 = new_chunk_id().unwrap();
        let zeros = vec![0u8; 1 << 20];
        store.write_chunk(&id2, &zeros, true).unwrap();
        let blob = std::fs::read(store.chunk_path(&id2)).unwrap();
        assert_eq!(blob[0] & FLAG_ZSTD, FLAG_ZSTD);
        assert!(blob.len() < zeros.len() / 10);
        assert_eq!(store.read_chunk(&id2).unwrap(), zeros);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_leaves_no_tmp_and_shards_path() {
        let (dir, store) = store_at("atomic");
        let id = [0xABu8; 16];
        store.write_chunk(&id, b"hello-chunk", false).unwrap();
        let hex = id_hex(&id);
        let expect = store.pool_dir().join(&hex[..2]).join(&hex[2..4]).join(&hex);
        assert_eq!(store.chunk_path(&id), expect);
        assert!(expect.exists());
        assert!(!expect.with_extension("tmp").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn remove_volume_deletes_only_vol_dir() {
        let (dir, store) = store_at("rmvol");
        store.create_volume("v").unwrap();
        let id = new_chunk_id().unwrap();
        store.write_chunk(&id, b"x", false).unwrap();
        store.remove_volume("v").unwrap();
        assert!(!store.vol_dir("v").exists());
        assert!(store.chunk_path(&id).exists(), "pool chunks survive volume delete");
        // create of an existing volume refuses.
        store.create_volume("w").unwrap();
        assert!(store.create_volume("w").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn hex_roundtrip() {
        let id = new_chunk_id().unwrap();
        assert_eq!(id_from_hex(&id_hex(&id)).unwrap(), id);
        assert!(id_from_hex("xyz").is_none());
    }
}
