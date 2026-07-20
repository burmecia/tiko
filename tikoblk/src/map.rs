//! Per-volume chunk map: which chunk id backs each volume chunk index.
//!
//! The `map` file is the only mutable on-store metadata and is small
//! (64-byte header + 16 bytes per chunk index; a 1 TiB volume with 1 MiB
//! chunks = 16 MiB). Updates never rewrite it in place: the flusher
//! appends small **map-delta journal** files under `map.journal/` and a
//! checkpoint folds them into a freshly written `map` (tmp + fsync +
//! rename), deleting the folded delta files only after the new `map` is
//! durable. Same base+deltas pattern as worker's manifest/segment
//! compactor.
//!
//! Header layout (little-endian, 64 bytes):
//!
//! ```text
//!  0  magic "TBLKMAP1" (8)
//!  8  version u32 (=1)
//! 12  header_len u32 (=64)
//! 16  vol_id tag [u8;16] (vol_id bytes, zero-padded/truncated; sanity tag)
//! 32  volume_size_bytes u64
//! 40  chunk_size_bytes u32
//! 44  flags u32 (0)
//! 48  epoch u64 (reserved for the Phase-3 lease)
//! 56  generation u64 (bumped by every checkpoint that applied deltas;
//!                    `formatted` in the attach response is generation > 0)
//! 64  chunk ids: nchunks * 16 bytes (zero id = sparse hole)
//! ```
//!
//! A map-delta file `map.journal/<seq>.mj` is a run of 24-byte records
//! {chunk_idx u64, chunk_id[16]} plus a 4-byte CRC-32 trailer over all
//! record bytes. Files are written whole (single write + fsync), never
//! appended to.

use std::io;
use std::path::{Path, PathBuf};

use crate::chunkstore::{ChunkId, ZERO_ID};
use crate::crc32::crc32;

const MAGIC: &[u8; 8] = b"TBLKMAP1";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 64;
const ID_LEN: usize = 16;

/// In-memory chunk map plus its header fields.
#[derive(Debug, Clone)]
pub struct ChunkMap {
    /// Volume size in bytes (multiple of chunk_size).
    pub volume_size: u64,
    /// Chunk size in bytes.
    pub chunk_size: u32,
    /// Reserved for the Phase-3 lease.
    pub epoch: u64,
    /// Checkpoint generation (`formatted` = generation > 0).
    pub generation: u64,
    /// Chunk id per chunk index; ZERO_ID = hole.
    ids: Vec<ChunkId>,
}

impl ChunkMap {
    /// A fresh, all-holes map.
    pub fn new(volume_size: u64, chunk_size: u32) -> Self {
        let nchunks = (volume_size / chunk_size as u64) as usize;
        Self {
            volume_size,
            chunk_size,
            epoch: 0,
            generation: 0,
            ids: vec![ZERO_ID; nchunks],
        }
    }

    /// Number of chunk indexes in the volume.
    pub fn nchunks(&self) -> u64 {
        self.ids.len() as u64
    }

    /// Chunk id at `idx` (ZERO_ID for a hole).
    pub fn get(&self, idx: u64) -> ChunkId {
        self.ids[idx as usize]
    }

    /// Point `idx` at `id` (ZERO_ID clears it back to a hole).
    pub fn set(&mut self, idx: u64, id: ChunkId) {
        self.ids[idx as usize] = id;
    }

    /// All ids (for GC/cache bookkeeping).
    pub fn ids(&self) -> &[ChunkId] {
        &self.ids
    }

    /// A copy for a zero-copy clone: same chunk ids, fresh metadata.
    /// The clone gets `epoch = 0` and `generation = 0` (its own history
    /// starts now) while referencing the same pool chunks.
    pub fn for_clone(&self) -> Self {
        let mut m = self.clone();
        m.epoch = 0;
        m.generation = 0;
        m
    }

    /// Serialize: 64-byte header + id array.
    pub fn encode(&self, vol_id: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.ids.len() * ID_LEN);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&(HEADER_LEN as u32).to_le_bytes());
        let mut tag = [0u8; 16];
        let vb = vol_id.as_bytes();
        let n = vb.len().min(16);
        tag[..n].copy_from_slice(&vb[..n]);
        out.extend_from_slice(&tag);
        out.extend_from_slice(&self.volume_size.to_le_bytes());
        out.extend_from_slice(&self.chunk_size.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // flags
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        for id in &self.ids {
            out.extend_from_slice(id);
        }
        out
    }

    /// Parse a map image.
    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
        if buf.len() < HEADER_LEN || &buf[..8] != MAGIC {
            return Err(bad("bad map magic/length"));
        }
        if u32::from_le_bytes(buf[8..12].try_into().unwrap()) != VERSION {
            return Err(bad("unsupported map version"));
        }
        let volume_size = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let chunk_size = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        let epoch = u64::from_le_bytes(buf[48..56].try_into().unwrap());
        let generation = u64::from_le_bytes(buf[56..64].try_into().unwrap());
        if chunk_size == 0 || volume_size % chunk_size as u64 != 0 {
            return Err(bad("bad map geometry"));
        }
        let nchunks = (volume_size / chunk_size as u64) as usize;
        if buf.len() != HEADER_LEN + nchunks * ID_LEN {
            return Err(bad("map length does not match geometry"));
        }
        let mut ids = Vec::with_capacity(nchunks);
        for i in 0..nchunks {
            let off = HEADER_LEN + i * ID_LEN;
            ids.push(buf[off..off + ID_LEN].try_into().unwrap());
        }
        Ok(Self {
            volume_size,
            chunk_size,
            epoch,
            generation,
            ids,
        })
    }

    /// Load `map` + apply any `map.journal/*.mj` deltas in seq order.
    /// If no map file exists yet, starts from an all-holes map of the given
    /// geometry. Returns the map and the highest delta seq seen.
    pub fn load(
        map_path: &Path,
        journal_dir: &Path,
        volume_size: u64,
        chunk_size: u32,
    ) -> io::Result<(Self, u64)> {
        let mut map = match std::fs::read(map_path) {
            Ok(buf) => Self::decode(&buf)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Self::new(volume_size, chunk_size),
            Err(e) => return Err(e),
        };
        let mut max_seq = 0;
        for (seq, path) in list_deltas(journal_dir)? {
            let deltas = read_delta_file(&path)?;
            for (idx, id) in deltas {
                map.set(idx, id);
            }
            max_seq = max_seq.max(seq);
        }
        Ok((map, max_seq))
    }

    /// Write the map atomically (tmp + fsync + rename + fsync dir).
    pub fn write_atomic(&self, vol_id: &str, map_path: &Path) -> io::Result<()> {
        let tmp = map_path.with_extension("tmp");
        std::fs::write(&tmp, self.encode(vol_id))?;
        let f = std::fs::File::open(&tmp)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, map_path)?;
        crate::chunkstore::sync_dir(map_path.parent().expect("vol dir"))
    }
}

/// One applied map delta.
pub type Delta = (u64, ChunkId);

/// Encode and write a delta journal file (records + CRC32 trailer),
/// fsynced, under `journal_dir/<seq>.mj`.
pub fn write_delta_file(journal_dir: &Path, seq: u64, deltas: &[Delta]) -> io::Result<PathBuf> {
    let mut buf = Vec::with_capacity(deltas.len() * 24 + 4);
    for (idx, id) in deltas {
        buf.extend_from_slice(&idx.to_le_bytes());
        buf.extend_from_slice(id);
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let path = journal_dir.join(format!("{seq:020}.mj"));
    std::fs::write(&path, &buf)?;
    let f = std::fs::File::open(&path)?;
    f.sync_all()?;
    drop(f);
    crate::chunkstore::sync_dir(journal_dir)?;
    Ok(path)
}

/// Read and CRC-verify a delta journal file.
pub fn read_delta_file(path: &Path) -> io::Result<Vec<Delta>> {
    let buf = std::fs::read(path)?;
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {m}", path.display()));
    if buf.len() < 4 || (buf.len() - 4) % 24 != 0 {
        return Err(bad("bad delta file length"));
    }
    let (records, crc) = buf.split_at(buf.len() - 4);
    if crc32(records) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(bad("delta file crc mismatch"));
    }
    let mut out = Vec::with_capacity(records.len() / 24);
    for rec in records.chunks_exact(24) {
        let idx = u64::from_le_bytes(rec[..8].try_into().unwrap());
        let id: ChunkId = rec[8..24].try_into().unwrap();
        out.push((idx, id));
    }
    Ok(out)
}

/// List delta journal files as (seq, path), sorted by seq.
pub fn list_deltas(journal_dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    match std::fs::read_dir(journal_dir) {
        Ok(it) => {
            for e in it.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if let Some(stem) = name.strip_suffix(".mj")
                    && let Ok(seq) = stem.parse::<u64>()
                {
                    out.push((seq, e.path()));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// Fold: apply all deltas to the base map, write it atomically with
/// `generation + 1` (if any delta applied), then delete the folded delta
/// files. Returns the number of deltas folded.
pub fn fold(vol_id: &str, map: &mut ChunkMap, map_path: &Path, journal_dir: &Path) -> io::Result<usize> {
    let deltas = list_deltas(journal_dir)?;
    if deltas.is_empty() {
        return Ok(0);
    }
    let mut applied = 0;
    for (_, path) in &deltas {
        for (idx, id) in read_delta_file(path)? {
            map.set(idx, id);
            applied += 1;
        }
    }
    if applied > 0 {
        map.generation += 1;
    }
    map.write_atomic(vol_id, map_path)?;
    // Only now that the new map is durable may the folded deltas go away.
    for (_, path) in deltas {
        let _ = std::fs::remove_file(path);
    }
    crate::chunkstore::sync_dir(journal_dir)?;
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tikoblk-map-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("map.journal")).unwrap();
        dir
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mut m = ChunkMap::new(4 << 20, 1 << 20);
        m.set(2, [7u8; 16]);
        m.generation = 5;
        m.epoch = 9;
        let buf = m.encode("vol-x");
        let m2 = ChunkMap::decode(&buf).unwrap();
        assert_eq!(m2.volume_size, 4 << 20);
        assert_eq!(m2.chunk_size, 1 << 20);
        assert_eq!(m2.nchunks(), 4);
        assert_eq!(m2.get(0), ZERO_ID);
        assert_eq!(m2.get(2), [7u8; 16]);
        assert_eq!(m2.generation, 5);
        assert_eq!(m2.epoch, 9);

        // Corrupt magic / truncated length are rejected.
        let mut bad = buf.clone();
        bad[0] = b'X';
        assert!(ChunkMap::decode(&bad).is_err());
        assert!(ChunkMap::decode(&buf[..buf.len() - 1]).is_err());
    }

    #[test]
    fn delta_roundtrip_and_crc_detection() {
        let dir = dirs("delta");
        let jd = dir.join("map.journal");
        let deltas = vec![(0u64, [1u8; 16]), (3u64, [2u8; 16])];
        let path = write_delta_file(&jd, 7, &deltas).unwrap();
        assert_eq!(read_delta_file(&path).unwrap(), deltas);

        // Flip a payload byte -> crc mismatch.
        let mut buf = std::fs::read(&path).unwrap();
        buf[3] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();
        assert!(read_delta_file(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_applies_deltas_and_fold_checkpoint() {
        let dir = dirs("fold");
        let map_path = dir.join("map");
        let jd = dir.join("map.journal");

        // Base map at generation 1.
        let mut base = ChunkMap::new(4 << 20, 1 << 20);
        base.generation = 1;
        base.set(0, [9u8; 16]);
        base.write_atomic("v", &map_path).unwrap();

        // Two delta files; later seq wins per index.
        write_delta_file(&jd, 1, &[(1, [1u8; 16]), (2, [2u8; 16])]).unwrap();
        write_delta_file(&jd, 2, &[(2, [3u8; 16])]).unwrap();

        let (m, max_seq) = ChunkMap::load(&map_path, &jd, 4 << 20, 1 << 20).unwrap();
        assert_eq!(max_seq, 2);
        assert_eq!(m.get(0), [9u8; 16]);
        assert_eq!(m.get(1), [1u8; 16]);
        assert_eq!(m.get(2), [3u8; 16]);
        assert_eq!(m.generation, 1, "load does not fold");

        // Fold: map updated, generation bumped, delta files gone.
        let mut m2 = ChunkMap::load(&map_path, &jd, 4 << 20, 1 << 20).unwrap().0;
        let applied = fold("v", &mut m2, &map_path, &jd).unwrap();
        assert_eq!(applied, 3);
        assert_eq!(m2.generation, 2);
        assert!(list_deltas(&jd).unwrap().is_empty());
        let (m3, _) = ChunkMap::load(&map_path, &jd, 4 << 20, 1 << 20).unwrap();
        assert_eq!(m3.get(2), [3u8; 16]);
        assert_eq!(m3.generation, 2);

        // Fold with no deltas is a no-op (generation unchanged).
        assert_eq!(fold("v", &mut m3.clone(), &map_path, &jd).unwrap(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
